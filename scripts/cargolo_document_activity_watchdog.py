#!/root/.hermes/hermes-agent/venv-py312/bin/python
"""Script-only watchdog for CARGOLO ASR TMS document-upload monitoring.

Designed for Hermes cron `no_agent=True`:
- empty stdout means silent/no delivery when nothing changed
- non-empty stdout is an operator-readable status for processed events/errors
- non-zero exit lets the scheduler alert on broken monitoring
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEFAULT_REPO_ROOT = Path("/root/.hermes/hermes-agent")
REPO_ROOT = Path(__file__).resolve().parents[1]
if not (REPO_ROOT / "plugins" / "cargolo_ops").exists():
    REPO_ROOT = DEFAULT_REPO_ROOT
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from plugins.cargolo_ops.document_activity_monitor import run_document_activity_monitor  # noqa: E402

ERROR_STATE_PATH = Path.home() / ".hermes" / "cargolo_asr" / "runtime" / "document_activity_monitor_watchdog_error_state.json"
ERROR_NOTIFY_COOLDOWN_SECONDS = int(os.environ.get("HERMES_CARGOLO_DOCUMENT_MONITOR_ERROR_NOTIFY_COOLDOWN", "3600") or "3600")


def _short_path(value: object) -> str:
    text = str(value or "").strip()
    return text.replace(str(Path.home()), "~") if text else "-"


def _format_report(result: dict) -> str:
    lines: list[str] = []
    errors = result.get("errors") if isinstance(result.get("errors"), list) else []
    processed = result.get("processed") if isinstance(result.get("processed"), list) else []

    if result.get("status") == "watchdog_error":
        return str(result.get("operator_message") or "CARGOLO Dokumentenmonitor: temporär nicht erreichbar.").strip()

    if result.get("status") == "baselined":
        return (
            f"CARGOLO Dokumentenmonitor: Baseline gesetzt bis Activity {result.get('baselined_activity_id') or result.get('last_seen_activity_id_after') or 0}; "
            "alte Uploads wurden nicht verarbeitet."
        )

    if errors:
        lines.append(f"CARGOLO Dokumentenmonitor: ⚠️ {len(errors)} Fehler")
        for err in errors[:3]:
            if isinstance(err, dict):
                lines.append(f"- {err.get('order_id') or '-'} / Activity {err.get('activity_id') or '-'}: {err.get('error') or 'unbekannter Fehler'}")
            else:
                lines.append(f"- {err}")

    if processed:
        if not lines:
            lines.append(f"CARGOLO Dokumentenmonitor: {len(processed)} neue TMS-Dokument-Uploads verarbeitet")
        else:
            lines.append(f"Verarbeitet: {len(processed)}")
        for row in processed[:5]:
            if not isinstance(row, dict):
                continue
            note = row.get("notification") if isinstance(row.get("notification"), dict) else {}
            delivered = note.get("delivered") if note else None
            delivery = f", Teams-Notify={delivered}" if delivered is not None else ""
            processor = row.get("processor_result") if isinstance(row.get("processor_result"), dict) else {}
            reconciliation = row.get("document_reconciliation") if isinstance(row.get("document_reconciliation"), dict) else {}
            risk = str(reconciliation.get("risk") or "low").strip().lower()
            review = bool(reconciliation.get("needs_human_review"))
            priority = processor.get("analysis_priority") or ("medium" if review else "low")
            status_bits = [f"Prio={priority}", f"Risk={risk}"]
            if review:
                status_bits.append("Review nötig")
            raw_review_intents = processor.get("document_review_intents") if isinstance(processor, dict) else []
            review_intents = raw_review_intents if isinstance(raw_review_intents, list) else []
            raw_side_effects = processor.get("side_effects") if isinstance(processor, dict) else {}
            side_effects = raw_side_effects if isinstance(raw_side_effects, dict) else {}
            if review_intents:
                status_bits.append(f"Review-Intents={len(review_intents)}")
            if side_effects:
                status_bits.append(f"TMS-Änderungen={side_effects.get('tms_updates', 0)}")
            raw_sections = processor.get("document_message_sections") if isinstance(processor, dict) else {}
            sections = raw_sections if isinstance(raw_sections, dict) else {}
            auffaellig = str(sections.get("auffaellig") or "").strip()
            abgleich = str(sections.get("abgleich") or "").strip()
            summary_text = auffaellig or abgleich
            if summary_text:
                finding_text = f"; Kurz: {summary_text[:160]}"
            else:
                raw_findings = reconciliation.get("findings") if isinstance(reconciliation, dict) else []
                findings = raw_findings if isinstance(raw_findings, list) else []
                first_finding = findings[0] if findings else None
                if isinstance(first_finding, dict):
                    summary = first_finding.get("summary") or first_finding.get("type") or "fachlich prüfen"
                    filename = first_finding.get("filename") or "Dokument"
                    finding_text = f"; Kurz: {filename}: {str(summary)[:120]}"
                else:
                    finding_text = f"; Kurz: {str(first_finding)[:120]}" if first_finding else ""
            lines.append(
                f"- {row.get('order_id') or '-'} / Activity {row.get('activity_id') or '-'} ({', '.join(status_bits)}){delivery}{finding_text}; Report: {_short_path(row.get('report_md_path') or row.get('report_json_path'))}"
            )

    if lines:
        lines.append(f"Cursor: {result.get('last_seen_activity_id_before')} → {result.get('last_seen_activity_id_after')}")
        latest = result.get("latest_run_path") or result.get("state_path")
        if latest:
            lines.append(f"Artefakt: {_short_path(latest)}")
    return "\n".join(lines)


def _load_error_state() -> dict:
    try:
        if ERROR_STATE_PATH.exists():
            data = json.loads(ERROR_STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _save_error_state(payload: dict) -> None:
    ERROR_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ERROR_STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _iso_to_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _human_error_summary(error_text: str, exc: Exception) -> str:
    lowered = error_text.lower()
    if "all connection attempts failed" in lowered or "connecterror" in lowered:
        return "TMS-MCP-Verbindung fehlgeschlagen (All connection attempts failed)."
    if "timed out" in lowered or "timeout" in lowered:
        return "TMS-MCP-Aufruf ist in ein Timeout gelaufen."
    first_line = error_text.splitlines()[0].strip() if error_text.splitlines() else ""
    if not first_line:
        first_line = type(exc).__name__
    return first_line[:220]


def _watchdog_error_payload(exc: Exception) -> tuple[dict, bool]:
    now = datetime.now(timezone.utc)
    error_text = str(exc).strip() or type(exc).__name__
    # Keep the fingerprint stable for repeated transport outages while avoiding
    # huge traceback delivery through Hermes cron/no_agent.
    fingerprint = hashlib.sha256(f"{type(exc).__name__}:{error_text}".encode("utf-8", "ignore")).hexdigest()[:16]
    previous = _load_error_state()
    previous_notified_at = _iso_to_datetime(previous.get("last_notified_at"))
    notify = previous.get("fingerprint") != fingerprint
    if not notify and previous_notified_at is not None:
        notify = (now - previous_notified_at).total_seconds() >= ERROR_NOTIFY_COOLDOWN_SECONDS
    if not previous:
        notify = True
    state = {
        "fingerprint": fingerprint,
        "last_error_at": now.isoformat(timespec="seconds"),
        "last_error_type": type(exc).__name__,
        "last_error": error_text[:2000],
        "repeat_count": int(previous.get("repeat_count") or 0) + 1 if previous.get("fingerprint") == fingerprint else 1,
        "last_notified_at": now.isoformat(timespec="seconds") if notify else previous.get("last_notified_at"),
    }
    _save_error_state(state)
    human_summary = _human_error_summary(error_text, exc)
    operator_message = (
        "CARGOLO Dokumentenmonitor: TMS/MCP gerade nicht erreichbar; "
        "Watchdog wurde bewusst ohne Python-Traceback gedrosselt. "
        f"Fehler: {type(exc).__name__}: {human_summary}"
    )
    payload = {
        "status": "watchdog_error",
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "error_type": type(exc).__name__,
        "error": error_text,
        "fingerprint": fingerprint,
        "repeat_count": state["repeat_count"],
        "operator_message": operator_message,
        "error_state_path": str(ERROR_STATE_PATH),
    }
    return payload, notify


def main() -> int:
    baseline_now = os.environ.get("HERMES_CARGOLO_DOCUMENT_MONITOR_BASELINE_NOW", "").strip().lower() in {"1", "true", "yes", "on"}
    os.environ.setdefault(
        "HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_CMD",
        str(REPO_ROOT / "venv-py312" / "bin" / "python") + " " + str(REPO_ROOT / "scripts" / "cargolo_document_agent_review.py"),
    )
    # Cron kills no_agent scripts at 300s. Keep the optional LLM review
    # comfortably below that budget and process only one fresh upload per tick;
    # older events stay retryable through the activity cursor.
    os.environ.setdefault("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_TIMEOUT", "45")
    # The TMS activity log can be slow without a bounded time window. The
    # monitor also keeps an activity-ID cursor, so a short rolling window is
    # enough for the frequent watchdog while preventing unbounded scans.
    date_from = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(timespec="seconds")
    latest_path = Path.home() / ".hermes" / "cargolo_asr" / "runtime" / "document_activity_monitor_watchdog_latest.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = run_document_activity_monitor(max_events=1, per_page=25, baseline_now=baseline_now, date_from=date_from)
        latest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        text = _format_report(result)
        if text:
            print(text)
        # no_agent cron would otherwise deliver raw traceback/error blobs on
        # every tick. The script itself now reports operational issues in a
        # bounded, human-readable way, so keep the scheduler status clean.
        return 0
    except Exception as exc:
        result, should_notify = _watchdog_error_payload(exc)
        result["latest_path"] = str(latest_path)
        latest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        if should_notify:
            print(_format_report(result))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
