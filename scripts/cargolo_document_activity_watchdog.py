#!/root/.hermes/hermes-agent/venv-py312/bin/python
"""Script-only watchdog for CARGOLO ASR TMS document-upload monitoring.

Designed for Hermes cron `no_agent=True`:
- empty stdout means silent/no delivery when nothing changed
- non-empty stdout is an operator-readable status for processed events/errors
- non-zero exit lets the scheduler alert on broken monitoring
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

DEFAULT_REPO_ROOT = Path("/root/.hermes/hermes-agent")
REPO_ROOT = Path(__file__).resolve().parents[1]
if not (REPO_ROOT / "plugins" / "cargolo_ops").exists():
    REPO_ROOT = DEFAULT_REPO_ROOT
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from plugins.cargolo_ops.document_activity_monitor import run_document_activity_monitor  # noqa: E402


def _short_path(value: object) -> str:
    text = str(value or "").strip()
    return text.replace(str(Path.home()), "~") if text else "-"


def _format_report(result: dict) -> str:
    lines: list[str] = []
    errors = result.get("errors") if isinstance(result.get("errors"), list) else []
    processed = result.get("processed") if isinstance(result.get("processed"), list) else []

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
            findings = reconciliation.get("findings") if isinstance(reconciliation.get("findings"), list) else []
            finding_text = f"; Auffällig: {str(findings[0])[:120]}" if findings else ""
            lines.append(
                f"- {row.get('order_id') or '-'} / Activity {row.get('activity_id') or '-'} ({', '.join(status_bits)}){delivery}{finding_text}; Report: {_short_path(row.get('report_md_path') or row.get('report_json_path'))}"
            )

    if lines:
        lines.append(f"Cursor: {result.get('last_seen_activity_id_before')} → {result.get('last_seen_activity_id_after')}")
        latest = result.get("latest_run_path") or result.get("state_path")
        if latest:
            lines.append(f"Artefakt: {_short_path(latest)}")
    return "\n".join(lines)


def main() -> int:
    result = run_document_activity_monitor(max_events=5, per_page=50)
    latest_path = Path.home() / ".hermes" / "cargolo_asr" / "runtime" / "document_activity_monitor_watchdog_latest.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    text = _format_report(result)
    if text:
        print(text)
    return 0 if not result.get("errors") else 2


if __name__ == "__main__":
    raise SystemExit(main())
