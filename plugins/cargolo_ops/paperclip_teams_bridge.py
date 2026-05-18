"""CARGOLO Teams -> Paperclip Chef bridge.

This module keeps Paperclip as the coordination/audit control plane for
Teams-originated CARGOLO case-assist questions. It deliberately does not perform
productive CARGOLO side effects: no TMS writes, no document uploads, no
customer/partner mail, and no proactive Teams sends. The Teams adapter remains
the only component that sends the gateway response back to Teams.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import Any
import urllib.error
import urllib.request

from .employee_agent import EmployeeRequest, EmployeeResponse
from .models import utc_now_iso


_DEFAULT_COMPANY_ID = "88e0f596-11b4-4987-93d3-20de94d10089"
_DEFAULT_PROJECT_ID = "f49f3bad-5948-4d91-9c76-d0c8ffc2c620"
_DEFAULT_CHEF_AGENT_ID = "23685acf-9da7-4504-b496-66260c51293b"
_DEFAULT_API_BASE = "http://127.0.0.1:3100"
_TRUE_VALUES = {"1", "true", "yes", "y", "on", "enabled"}
_FALSE_VALUES = {"0", "false", "no", "n", "off", "disabled"}
_TERMINAL_RUN_STATUSES = {
    "completed",
    "succeeded",
    "success",
    "failed",
    "cancelled",
    "canceled",
    "errored",
    "timeout",
    "timed_out",
    "skipped",
}
_TERMINAL_ISSUE_STATUSES = {"done", "cancelled", "canceled", "archived"}
_ORDER_RE = re.compile(r"\b(?:AN|BU)-\d{3,}\b", re.IGNORECASE)


@dataclass(frozen=True)
class PaperclipTeamsBridgeConfig:
    enabled: bool = False
    api_base: str = _DEFAULT_API_BASE
    company_id: str = _DEFAULT_COMPANY_ID
    project_id: str = _DEFAULT_PROJECT_ID
    chef_agent_id: str = _DEFAULT_CHEF_AGENT_ID
    issue_priority: str = "high"
    issue_work_mode: str = "standard"
    wait_timeout_seconds: float = 8.0
    poll_interval_seconds: float = 1.5
    terminal_grace_seconds: float = 15.0
    wakeup_after_create: bool = False
    request_timeout_seconds: float = 8.0

    @classmethod
    def from_env(cls, *, enabled: bool | None = None) -> "PaperclipTeamsBridgeConfig":
        raw_enabled = os.getenv("CARGOLO_PAPERCLIP_TEAMS_BRIDGE_ENABLED")
        if raw_enabled is None:
            raw_enabled = os.getenv("CARGOLO_PAPERCLIP_BRIDGE_ENABLED")
        return cls(
            enabled=_parse_bool(raw_enabled, default=False) if enabled is None else bool(enabled),
            api_base=(os.getenv("CARGOLO_PAPERCLIP_API_BASE") or _DEFAULT_API_BASE).rstrip("/"),
            company_id=os.getenv("CARGOLO_PAPERCLIP_COMPANY_ID") or _DEFAULT_COMPANY_ID,
            project_id=os.getenv("CARGOLO_PAPERCLIP_PROJECT_ID") or _DEFAULT_PROJECT_ID,
            chef_agent_id=os.getenv("CARGOLO_PAPERCLIP_CHEF_AGENT_ID") or _DEFAULT_CHEF_AGENT_ID,
            issue_priority=os.getenv("CARGOLO_PAPERCLIP_ISSUE_PRIORITY") or "high",
            issue_work_mode=os.getenv("CARGOLO_PAPERCLIP_ISSUE_WORK_MODE") or "standard",
            wait_timeout_seconds=_parse_float(os.getenv("CARGOLO_PAPERCLIP_WAIT_TIMEOUT_SECONDS"), default=8.0),
            poll_interval_seconds=_parse_float(os.getenv("CARGOLO_PAPERCLIP_POLL_INTERVAL_SECONDS"), default=1.5),
            terminal_grace_seconds=_parse_float(os.getenv("CARGOLO_PAPERCLIP_TERMINAL_GRACE_SECONDS"), default=15.0),
            wakeup_after_create=_parse_bool(os.getenv("CARGOLO_PAPERCLIP_WAKEUP_AFTER_CREATE"), default=False),
            request_timeout_seconds=_parse_float(os.getenv("CARGOLO_PAPERCLIP_REQUEST_TIMEOUT_SECONDS"), default=8.0),
        )


def _parse_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    lowered = str(value).strip().lower()
    if lowered in _TRUE_VALUES:
        return True
    if lowered in _FALSE_VALUES:
        return False
    return default


def _parse_float(value: Any, *, default: float) -> float:
    try:
        parsed = float(str(value).strip())
    except Exception:
        return default
    if parsed < 0:
        return default
    return parsed


def paperclip_teams_bridge_enabled(enabled: bool | None = None) -> bool:
    """Return whether Teams case-assist should be handed to Paperclip."""

    return PaperclipTeamsBridgeConfig.from_env(enabled=enabled).enabled


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _request_json(method: str, url: str, *, payload: dict[str, Any] | None = None, timeout: float = 8.0) -> Any:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:  # noqa: S310 - local trusted Paperclip API
            raw = response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")[:2000]
        raise RuntimeError(f"Paperclip API {method} {url} failed with HTTP {exc.code}: {body}") from exc
    except Exception as exc:
        raise RuntimeError(f"Paperclip API {method} {url} failed: {exc}") from exc
    if not raw.strip():
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


def _safe_title(text: str, order_id: str | None) -> str:
    prefix = f"Teams-Fallfrage {order_id}" if order_id else "Teams-Fallfrage CARGOLO"
    compact = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(compact) > 120:
        compact = compact[:117].rstrip() + "..."
    return f"{prefix}: {compact}" if compact else prefix


def _stable_key(*parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _normalize_order_id(*values: Any) -> str | None:
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue
        match = _ORDER_RE.search(raw)
        if match:
            return match.group(0).upper()
    return None


def _read_json_file(path: Path) -> Any:
    if not path.exists() or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _json_payload_has_content(payload: Any) -> bool:
    if isinstance(payload, dict):
        return any(value not in (None, "", [], {}) for value in payload.values())
    if isinstance(payload, list):
        return bool(payload)
    return payload not in (None, "")


def _json_file_has_content(path: Path) -> bool:
    return _json_payload_has_content(_read_json_file(path))


def _file_has_bytes(path: Path) -> bool:
    try:
        return path.exists() and path.is_file() and path.stat().st_size > 0
    except Exception:
        return False


def _jsonl_has_rows(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                return True
    except Exception:
        return False
    return False


def _registry_has_document_evidence(registry: Any) -> bool:
    if not isinstance(registry, dict):
        return False
    evidence_keys = (
        "received_types",
        "expected_types",
        "received_documents",
        "tms_documents",
        "mirrored_tms_documents",
        "tms_mirroring_gaps",
        "analyzed_documents",
        "missing_types",
    )
    return any(bool(registry.get(key)) for key in evidence_keys)


def _local_case_evidence_status(root: Path, order_id: str) -> dict[str, Any]:
    """Return whether the local case has enough read-only evidence for Paperclip.

    A bare `orders/<AN>/employee` or CaseStore skeleton is not enough. For the
    Paperclip/Chef handoff we want local TMS + mail history + document registry +
    document analysis evidence to be present before the agent answers from the
    local case. Missing evidence triggers the TMS-first lifecycle sync.
    """

    normalized = _normalize_order_id(order_id) or str(order_id or "").strip().upper()
    case_root = root / "orders" / normalized
    registry_path = case_root / "documents" / "registry.json"
    registry = _read_json_file(registry_path)
    tms_ready = _json_file_has_content(case_root / "tms" / "shipment_detail.json") or _json_file_has_content(case_root / "tms_snapshot.json")
    mail_ready = _jsonl_has_rows(case_root / "email_index.jsonl")
    registry_ready = _registry_has_document_evidence(registry)
    analysis_path = None
    if isinstance(registry, dict):
        raw_analysis_path = str(registry.get("document_analysis_summary_path") or "").strip()
        if raw_analysis_path:
            candidate = Path(raw_analysis_path)
            analysis_path = candidate if candidate.is_absolute() else case_root / candidate
    default_analysis_path = case_root / "documents" / "analysis" / "latest_summary.json"
    analysis_ready = _file_has_bytes(default_analysis_path) or bool(analysis_path and _file_has_bytes(analysis_path))
    exists = case_root.exists()
    evidence = {
        "case_folder": exists,
        "tms": tms_ready,
        "mail_history": mail_ready,
        "document_registry": registry_ready,
        "document_analysis": analysis_ready,
    }
    missing = [name for name, present in evidence.items() if not present]
    return {
        "status": "local_ready" if not missing else "incomplete",
        "order_id": normalized,
        "case_root": str(case_root),
        "answerable_from_local": not missing,
        "evidence": evidence,
        "missing": missing,
    }


def _sync_case_lifecycle(order_id: str, **kwargs: Any) -> dict[str, Any]:
    from .case_lifecycle import sync_case_lifecycle

    return sync_case_lifecycle(order_id, **kwargs)


def _run_local_case_preflight(root: Path, order_id: str | None) -> dict[str, Any]:
    if not order_id:
        return {"status": "skipped", "reason": "no_order_id", "answerable_from_local": False}
    before = _local_case_evidence_status(root, order_id)
    if before.get("answerable_from_local"):
        return before
    try:
        sync_result = _sync_case_lifecycle(
            order_id,
            storage_root=root,
            refresh_history=True,
            analyze_documents=True,
        )
    except Exception as exc:
        return {
            "status": "sync_error",
            "order_id": order_id,
            "case_root": before.get("case_root"),
            "answerable_from_local": False,
            "missing_before": before.get("missing") or [],
            "evidence_before": before.get("evidence") or {},
            "error": str(exc),
        }
    if isinstance(sync_result, dict) and sync_result.get("status") == "skipped":
        return {
            "status": "skipped",
            "order_id": order_id,
            "reason": sync_result.get("reason"),
            "message": sync_result.get("message"),
            "answerable_from_local": False,
            "missing_before": before.get("missing") or [],
            "evidence_before": before.get("evidence") or {},
            "sync_result": sync_result,
        }
    after = _local_case_evidence_status(root, order_id)
    return {
        "status": "synced",
        "order_id": order_id,
        "case_root": after.get("case_root") or before.get("case_root"),
        "answerable_from_local": bool(after.get("answerable_from_local")),
        "missing_before": before.get("missing") or [],
        "missing_after": after.get("missing") or [],
        "evidence_before": before.get("evidence") or {},
        "evidence_after": after.get("evidence") or {},
        "sync_result": sync_result,
    }


def _preflight_description(local_case_preflight: dict[str, Any] | None) -> str:
    if not local_case_preflight:
        return "Lokaler Case-Preflight: nicht ausgeführt."
    status = local_case_preflight.get("status") or "unbekannt"
    case_root = local_case_preflight.get("case_root") or "n/a"
    answerable = "ja" if local_case_preflight.get("answerable_from_local") else "nein/teilweise"
    missing = local_case_preflight.get("missing_after") or local_case_preflight.get("missing") or []
    if not isinstance(missing, list):
        missing = [str(missing)]
    sync_result = local_case_preflight.get("sync_result") if isinstance(local_case_preflight.get("sync_result"), dict) else {}
    history_count = sync_result.get("history_sync_count") if isinstance(sync_result, dict) else None
    reason = local_case_preflight.get("reason") or (sync_result.get("reason") if isinstance(sync_result, dict) else None)
    lines = [
        "Lokaler Case-Preflight:",
        f"- Status: {status}",
        f"- Case-Pfad: {case_root}",
        f"- Lokale Antwortbasis vollständig: {answerable}",
        f"- Fehlende Evidenz nach Preflight: {', '.join(str(item) for item in missing) if missing else '-'}",
    ]
    if history_count is not None:
        lines.append(f"- Mail-Historie Sync Count: {history_count}")
    if reason:
        lines.append(f"- Grund/Hinweis: {reason}")
    return "\n".join(lines)


def _build_issue_description(
    *,
    request: EmployeeRequest,
    response: EmployeeResponse,
    channel_id: str,
    message_id: str,
    user_id: str | None,
    user_name: str | None,
    local_case_preflight: dict[str, Any] | None = None,
) -> str:
    order_id = response.order_id or request.order_id or _normalize_order_id(request.text) or "nicht eindeutig"
    actor = user_name or user_id or request.actor or "Teams Operator"
    return f"""CARGOLO Teams-Fallfrage für Paperclip Chef.

Teams-Kontext:
- Originalfrage: {request.text.strip()}
- Actor: {actor}
- User ID: {user_id or 'n/a'}
- Channel/Conversation: {channel_id or 'n/a'}
- Message ID: {message_id or 'n/a'}
- Order/Case: {order_id}

{_preflight_description(local_case_preflight)}

Chef-Auftrag:
1. Koordiniere die passenden CARGOLO-Agenten/Spezialisten (z.B. Case Brain, Document Monitor) für diese operative Fallfrage.
2. Arbeite TMS-first: AN/BU nur bearbeiten, wenn sie live im ASR-TMS plausibel ist; unbekannte AN/BU nicht über Mail/n8n weiter suchen.
3. Nutze CARGOLO/Hermes-Kontext, lokale Case-Folder, TMS/MCP-Kontext, Mail-Historie und Dokumentregistry nur lesend.
4. Gib eine kompakte deutsche Teams-Antwort zurück: Lage, Auffälligkeit, Empfehlung/Nächster Schritt.

Sicherheitsgrenzen / Default-Deny:
- Kein TMS-Write.
- Kein Dokumentupload oder Dokumentstatus-Writeback.
- Keine Kunden-/Partner-Mail und kein externer Versand.
- Keine produktive Teams-Nachricht außerhalb der Gateway-Antwort.
- Wenn eine Änderung sinnvoll erscheint: nur als Review-/Freigabevorschlag formulieren.

Bitte schreibe das Ergebnis als Issue-Kommentar. Wenn möglich, beginne den für Teams gedachten Teil mit:
TEAMS_ANTWORT:

Wichtig: Der TEAMS_ANTWORT-Block ist direkt für Teams sichtbar. Keine internen Notizen, keine Diff-/Audit-Blöcke und keine Debugdetails in diesen Block schreiben.
""".strip()


def _issue_identifier(issue: dict[str, Any]) -> str:
    return str(issue.get("identifier") or issue.get("number") or issue.get("id") or "Paperclip-Issue")


def _create_chef_issue(
    *,
    config: PaperclipTeamsBridgeConfig,
    request: EmployeeRequest,
    response: EmployeeResponse,
    channel_id: str,
    message_id: str,
    user_id: str | None,
    user_name: str | None,
    local_case_preflight: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "projectId": config.project_id,
        "title": _safe_title(request.text, response.order_id or request.order_id),
        "description": _build_issue_description(
            request=request,
            response=response,
            channel_id=channel_id,
            message_id=message_id,
            user_id=user_id,
            user_name=user_name,
            local_case_preflight=local_case_preflight,
        ),
        "assigneeAgentId": config.chef_agent_id,
        "priority": config.issue_priority,
        "workMode": config.issue_work_mode,
        "status": "todo",
    }
    payload = {key: value for key, value in payload.items() if value not in (None, "")}
    created = _request_json(
        "POST",
        f"{config.api_base}/api/companies/{config.company_id}/issues",
        payload=payload,
        timeout=config.request_timeout_seconds,
    )
    if isinstance(created, dict) and isinstance(created.get("issue"), dict):
        return created["issue"]
    return created if isinstance(created, dict) else {"raw": created}


def _wakeup_chef(
    *,
    config: PaperclipTeamsBridgeConfig,
    issue: dict[str, Any],
    message_id: str,
    request: EmployeeRequest,
    response: EmployeeResponse,
) -> dict[str, Any]:
    issue_id = str(issue.get("id") or "")
    payload = {
        "source": "assignment",
        "triggerDetail": "manual",
        "reason": "teams_case_assist_issue_created",
        "payload": {
            "issueId": issue_id,
            "issueIdentifier": issue.get("identifier"),
            "orderId": response.order_id or request.order_id,
            "source": "teams_paperclip_bridge",
        },
        "idempotencyKey": f"teams-bridge:{issue_id or _stable_key(message_id, request.text)}",
    }
    return _request_json(
        "POST",
        f"{config.api_base}/api/agents/{config.chef_agent_id}/wakeup",
        payload=payload,
        timeout=config.request_timeout_seconds,
    )


def _as_list(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("comments", "runs", "heartbeatRuns", "heartbeat_runs", "items", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def _comment_body(comment: Any) -> str:
    if not isinstance(comment, dict):
        return ""
    for key in ("body", "content", "text", "markdown", "message"):
        value = comment.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _strip_code_fence(text: str) -> str:
    cleaned = text.strip()
    match = re.fullmatch(r"```(?:html|markdown|md|text)?\s*(.*?)\s*```", cleaned, flags=re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else cleaned


def _sanitize_teams_answer(text: str) -> str:
    cleaned = _strip_code_fence(text).strip()
    cleaned = re.sub(r"^[-–—]{3,}\s*", "", cleaned).strip()
    # The Teams answer is customer-internal/operator-visible. Drop anything the
    # Chef explicitly labels as not for Teams, and avoid leaking audit/debug tails.
    split_match = re.search(
        r"\n\s*(?:---\s*)?(?:\*\*)?\s*(?:Interne Notiz|Internal note|Audit|Debug)\b.*$",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if split_match:
        cleaned = cleaned[: split_match.start()].rstrip()
    return cleaned.strip()


def _looks_probably_truncated(text: str) -> bool:
    """Detect Paperclip result excerpts that were clipped mid-sentence.

    Paperclip comments can contain the complete final answer while adapter
    resultJson may be capped. Do not deliver a run result that visibly ends in
    the middle of a word; let the poller wait for the agent-authored
    TEAMS_ANTWORT comment instead.
    """

    cleaned = text.rstrip()
    if len(cleaned) < 480:
        return False
    tail = cleaned[-24:]
    if re.search(r"[.!?…\])}>'\"]\s*$", cleaned):
        return False
    return bool(re.search(r"[A-Za-zÄÖÜäöüß]{3,}$", tail))


def _looks_probably_like_truncated_json(text: str) -> bool:
    """Return whether a JSON-looking run payload appears clipped/corrupt.

    Paperclip may expose adapter results either as JSON objects or as stringified
    JSON excerpts. If the stringified form is clipped, we can still use the
    explicit TEAMS_ANTWORT marker for detection, but we must not forward an
    incomplete mid-word answer to Teams.
    """

    cleaned = str(text or "").strip()
    if not cleaned or cleaned[0] not in "[{":
        return False
    try:
        json.loads(cleaned)
        return False
    except json.JSONDecodeError as exc:
        if "unterminated string" in exc.msg.lower():
            return True
        if exc.pos >= max(0, len(cleaned) - 4):
            return True
        if cleaned.count('"') % 2 == 1:
            return True
        return cleaned[-1:] not in "}]"


def _unescape_jsonish_text(text: str) -> str:
    return (
        str(text or "")
        .replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\r", "\n")
        .replace("\\t", "\t")
        .replace('\\"', '"')
        .replace("\\/", "/")
    )


def _extract_marker_segment_from_jsonish_text(text: str) -> str | None:
    candidate = _unescape_jsonish_text(text)
    match = re.search(r"TEAMS_ANTWORT\s*:", candidate, flags=re.IGNORECASE)
    if not match:
        return None
    segment = candidate[match.start() :].strip()
    # If the marker came from a broken JSON string, drop obvious trailing JSON
    # quote/brace noise while keeping the answer itself intact.
    return segment.rstrip().rstrip('"}]}').strip()


def _jsonish_marker_segment_looks_incomplete(text: str) -> bool:
    cleaned = str(text or "").rstrip()
    if not cleaned:
        return True
    if re.search(r"[.!?…\])}>'\"]\s*$", cleaned):
        return False
    return bool(re.search(r"[A-Za-zÄÖÜäöüß]{3,}$", cleaned[-24:]))


def _extract_teams_answer(body: str, *, require_marker: bool = False) -> str | None:
    text = _strip_code_fence(body)
    marker = re.match(r"\s*TEAMS_ANTWORT\s*:\s*", text, flags=re.IGNORECASE)
    if marker:
        answer = _sanitize_teams_answer(text[marker.end() :])
        return answer if answer else None
    if require_marker:
        return None
    if len(text) >= 20 and not re.search(r"\b(?:review diff|diff --git|a//tmp|b//tmp)\b", text, flags=re.IGNORECASE):
        return _sanitize_teams_answer(text)
    return None


def _comment_created_at(comment: Any) -> str:
    if isinstance(comment, dict):
        return str(comment.get("createdAt") or comment.get("updatedAt") or "")
    return ""


def _comment_is_agent_generated(comment: Any) -> bool:
    """Return whether a Paperclip comment is safe to treat as agent output.

    Teams follow-ups must never be sourced from local-board/user comments. Those
    comments are also Paperclip wake triggers and caused the CAR-5 done -> wake ->
    comment loop. Unknown/bare comment rows are not trusted either: if Paperclip
    cannot explicitly label the author as an agent, the bridge waits for a
    matching run_result instead of forwarding a potentially bridge-authored
    marker back to Teams.
    """

    if not isinstance(comment, dict):
        return False
    author_type = str(comment.get("authorType") or comment.get("author_type") or "").lower().strip()
    return author_type == "agent"


def _latest_issue_comment_answer(config: PaperclipTeamsBridgeConfig, issue_id: str) -> str | None:
    if not issue_id:
        return None
    payload = _request_json(
        "GET",
        f"{config.api_base}/api/issues/{issue_id}/comments",
        timeout=config.request_timeout_seconds,
    )
    comments = sorted(_as_list(payload), key=_comment_created_at)
    for comment in reversed(comments):
        if not _comment_is_agent_generated(comment):
            continue
        # Only an explicit TEAMS_ANTWORT block may become a Teams follow-up.
        # Markerless Paperclip comments are often heartbeat summaries, review
        # notes, or debug/audit text and must stay internal.
        answer = _extract_teams_answer(_comment_body(comment), require_marker=True)
        if answer:
            return answer
    return None


def _run_created_at(run: Any) -> str:
    if isinstance(run, dict):
        return str(run.get("finishedAt") or run.get("updatedAt") or run.get("createdAt") or "")
    return ""


def _run_issue_id(run: Any) -> str:
    if not isinstance(run, dict):
        return ""
    context = run.get("contextSnapshot") or run.get("context_snapshot")
    if isinstance(context, dict):
        return str(context.get("issueId") or context.get("issue_id") or "").strip()
    return ""


def _collect_run_result_texts(payload: Any) -> list[str]:
    texts: list[str] = []
    if isinstance(payload, dict):
        for key in ("result", "summary", "text", "message", "answer", "output"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                texts.extend(_collect_run_result_texts(value))
        for value in payload.values():
            if isinstance(value, (dict, list)):
                texts.extend(_collect_run_result_texts(value))
    elif isinstance(payload, list):
        for item in payload:
            texts.extend(_collect_run_result_texts(item))
    elif isinstance(payload, str):
        raw = payload.strip()
        if not raw:
            return texts
        if raw[0] in "[{":
            try:
                decoded = json.loads(raw)
            except json.JSONDecodeError:
                if _looks_probably_like_truncated_json(raw):
                    marker_segment = _extract_marker_segment_from_jsonish_text(raw)
                    if marker_segment and not _jsonish_marker_segment_looks_incomplete(marker_segment):
                        texts.append(marker_segment)
            else:
                texts.extend(_collect_run_result_texts(decoded))
                return texts
        texts.append(raw)
    return texts


def _run_result_texts(run: Any) -> list[str]:
    if not isinstance(run, dict):
        return []
    result_json = run.get("resultJson") or run.get("result_json")
    texts: list[str] = _collect_run_result_texts(result_json)
    for key in ("stdoutExcerpt", "stdout_excerpt"):
        value = run.get(key)
        if isinstance(value, str) and value.strip():
            texts.extend(_collect_run_result_texts(value))
    return texts


def _latest_run_answer(config: PaperclipTeamsBridgeConfig, issue_id: str) -> str | None:
    """Read the final answer directly from the Chef run result if available.

    Paperclip persists adapter resultJson before/around issue-comment materialize.
    Polling it avoids racing the final comment and keeps bridge delivery tied to
    the exact issue rather than the latest Chef run globally.
    """

    if not issue_id:
        return None
    payload = _request_json(
        "GET",
        f"{config.api_base}/api/companies/{config.company_id}/heartbeat-runs?agentId={config.chef_agent_id}&limit=25",
        timeout=config.request_timeout_seconds,
    )
    runs = [run for run in _as_list(payload) if _run_issue_id(run) == issue_id]
    for run in sorted(runs, key=_run_created_at, reverse=True):
        for text in _run_result_texts(run):
            if _looks_probably_truncated(text):
                continue
            answer = _extract_teams_answer(text, require_marker=True)
            if answer:
                return answer
    return None


def _latest_run_status(config: PaperclipTeamsBridgeConfig) -> str | None:
    payload = _request_json(
        "GET",
        f"{config.api_base}/api/companies/{config.company_id}/heartbeat-runs?agentId={config.chef_agent_id}&limit=5",
        timeout=config.request_timeout_seconds,
    )
    runs = _as_list(payload)
    if not runs or not isinstance(runs[0], dict):
        return None
    return str(runs[0].get("status") or runs[0].get("state") or "").lower() or None


def _issue_status(config: PaperclipTeamsBridgeConfig, issue_id: str) -> str | None:
    if not issue_id:
        return None
    payload = _request_json(
        "GET",
        f"{config.api_base}/api/issues/{issue_id}",
        timeout=config.request_timeout_seconds,
    )
    if isinstance(payload, dict):
        return str(payload.get("status") or "").lower() or None
    return None


def poll_paperclip_issue_answer(
    *,
    issue_id: str,
    config: PaperclipTeamsBridgeConfig | None = None,
    timeout_seconds: float = 480.0,
    poll_interval_seconds: float | None = None,
) -> dict[str, Any]:
    """Poll one Paperclip issue until a Teams answer comment appears or timeout.

    This is used by the Teams adapter in a background task so the Bot Framework
    request can be acknowledged immediately while the Chef keeps working.
    """

    bridge_config = config or PaperclipTeamsBridgeConfig.from_env(enabled=True)
    issue_id = str(issue_id or "").strip()
    if not issue_id:
        return {"answer": None, "issue_status": None, "timed_out": False, "reason": "missing_issue_id"}
    interval = poll_interval_seconds if poll_interval_seconds is not None else bridge_config.poll_interval_seconds
    interval = max(1.0, float(interval or 1.0))
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    terminal_deadline: float | None = None
    latest_status: str | None = None
    while time.monotonic() <= deadline:
        # Prefer the matching Chef run result over comments. Paperclip comments
        # are UI/audit material and can contain stale auto-comments or delayed
        # heartbeat summaries; resultJson is tied directly to the issue run.
        try:
            answer = _latest_run_answer(bridge_config, issue_id)
        except RuntimeError:
            answer = None
        if answer:
            return {"answer": answer, "issue_status": latest_status, "timed_out": False, "source": "run_result"}
        answer = _latest_issue_comment_answer(bridge_config, issue_id)
        if answer:
            return {"answer": answer, "issue_status": latest_status, "timed_out": False, "source": "issue_comment"}
        try:
            latest_status = _issue_status(bridge_config, issue_id)
        except RuntimeError:
            latest_status = latest_status or None
        if latest_status in _TERMINAL_ISSUE_STATUSES:
            # A final run result/comment can be committed shortly after status
            # flips to done. Keep polling for a short grace period instead of
            # returning the placeholder immediately.
            if terminal_deadline is None:
                terminal_deadline = min(deadline, time.monotonic() + max(0.0, bridge_config.terminal_grace_seconds))
            if time.monotonic() >= terminal_deadline:
                return {"answer": None, "issue_status": latest_status, "timed_out": False, "reason": "terminal_without_answer"}
        time.sleep(interval)
    return {"answer": None, "issue_status": latest_status, "timed_out": True}


def _wait_for_issue_answer(config: PaperclipTeamsBridgeConfig, issue: dict[str, Any]) -> tuple[str | None, str | None]:
    issue_id = str(issue.get("id") or "")
    if not issue_id or config.wait_timeout_seconds <= 0:
        return None, None
    deadline = time.monotonic() + config.wait_timeout_seconds
    terminal_deadline: float | None = None
    latest_status: str | None = None
    while time.monotonic() <= deadline:
        # Prefer resultJson from the matching Chef run over comments; issue
        # comments are delayed/audit-facing and can contain non-answer material.
        try:
            answer = _latest_run_answer(config, issue_id)
        except RuntimeError:
            answer = None
        if answer:
            return answer, latest_status
        answer = _latest_issue_comment_answer(config, issue_id)
        if answer:
            return answer, latest_status
        try:
            latest_status = _issue_status(config, issue_id)
        except RuntimeError:
            latest_status = latest_status or None
        if latest_status in _TERMINAL_ISSUE_STATUSES:
            # Give Paperclip a grace window to persist adapter resultJson and/or
            # the final agent comment after the issue moves to done/cancelled.
            if terminal_deadline is None:
                terminal_deadline = min(deadline, time.monotonic() + max(0.0, config.terminal_grace_seconds))
            if time.monotonic() >= terminal_deadline:
                return None, latest_status
        time.sleep(max(0.2, config.poll_interval_seconds))
    return None, latest_status


def _fallback_response(issue: dict[str, Any], *, run_status: str | None = None) -> str:
    identifier = _issue_identifier(issue)
    status_line = f" Run-Status: {run_status}." if run_status else ""
    return (
        f"🧭 Paperclip Chef hat den Fall übernommen ({identifier}).{status_line}\n"
        "Lage: Die Teams-Fallfrage ist als CARGOLO Operations Issue angelegt und dem Chef zugewiesen.\n"
        "Sicherheit: Kein TMS-Write, keine Kunden-/Partnermail, kein Dokumentupload.\n"
        "Nächster Schritt: Chef/Agenten liefern das Ergebnis im Paperclip-Issue; ich gebe es über Teams aus, sobald der Polling-Pfad ein Ergebnis findet."
    )


def _progress_response(issue: dict[str, Any], *, order_id: str | None, local_case_preflight: dict[str, Any] | None = None) -> str:
    identifier = _issue_identifier(issue)
    subject = order_id or "die Fallfrage"
    preflight_status = str((local_case_preflight or {}).get("status") or "gestartet")
    return (
        f"Bin dran: {subject} ist im CARGOLO Operations Board angelegt ({identifier}).\n"
        f"Lage: Lokaler Case-Preflight ist {preflight_status}; TMS, Mail-Historie und Dokumente werden read-only geprüft.\n"
        "Sicherheit: Keine TMS-Writes, keine Kunden-/Partnermails, kein Dokumentupload.\n"
        "Ergebnis folgt hier automatisch als Antwort, sobald der CARGOLO Operations Lauf fertig ist."
    )


def handle_paperclip_teams_case_assist(
    *,
    root: Path,
    request: EmployeeRequest,
    response: EmployeeResponse,
    channel_id: str,
    message_id: str,
    user_id: str | None = None,
    user_name: str | None = None,
    config: PaperclipTeamsBridgeConfig | None = None,
) -> dict[str, Any]:
    """Create a Paperclip Chef issue for a Teams case-assist request.

    Returns a Teams-handoff-shaped dict. Side-effect flags are always false; the
    only external action is a local Paperclip control-plane issue/wakeup.
    """

    bridge_config = config or PaperclipTeamsBridgeConfig.from_env()
    started_at = utc_now_iso()
    if not bridge_config.enabled:
        return {"handled": False, "reason": "paperclip_bridge_disabled"}
    order_id = _normalize_order_id(response.order_id, request.order_id, request.text)
    local_case_preflight: dict[str, Any] | None = None

    audit_base: dict[str, Any] = {
        "timestamp": started_at,
        "source": "teams_paperclip_bridge",
        "channel_id": channel_id,
        "message_id": message_id,
        "user_id": user_id,
        "user_name": user_name,
        "order_id": order_id,
        "request_text": request.text,
        "company_id": bridge_config.company_id,
        "project_id": bridge_config.project_id,
        "chef_agent_id": bridge_config.chef_agent_id,
        "should_send_to_teams": False,
        "should_write_tms": False,
        "should_send_customer_message": False,
    }

    try:
        local_case_preflight = _run_local_case_preflight(root, order_id)
        if local_case_preflight.get("status") == "skipped" and local_case_preflight.get("reason") == "shipment_not_found_in_tms":
            result = {
                "handled": True,
                "classification": "shipment_not_found_in_tms",
                "handoff_target": "paperclip_chef",
                "order_id": order_id,
                "response_text": (
                    f"{order_id} finde ich nicht im ASR-TMS.\n"
                    "Lage: Ich stoppe TMS-first und lege kein Paperclip-Issue an; keine Mail-/n8n-Suche.\n"
                    "Sicherheit: Kein TMS-Write, keine Kunden-/Partnermail, kein Dokumentupload.\n"
                    "Nächster Schritt: AN/BU prüfen oder die Sendung zuerst im TMS anlegen/finden."
                ),
                "local_case_preflight": local_case_preflight,
                "paperclip_result_pending": False,
                "suppress_initial_response": False,
                "should_send_to_teams": False,
                "should_write_tms": False,
                "should_send_customer_message": False,
            }
            _append_jsonl(root / "runtime" / "paperclip_teams_bridge.jsonl", {**audit_base, **result, "status": "skipped"})
            return result

        issue = _create_chef_issue(
            config=bridge_config,
            request=request,
            response=response,
            channel_id=channel_id,
            message_id=message_id,
            user_id=user_id,
            user_name=user_name,
            local_case_preflight=local_case_preflight,
        )
        wakeup: dict[str, Any] | None = None
        if bridge_config.wakeup_after_create:
            wakeup = _wakeup_chef(
                config=bridge_config,
                issue=issue,
                message_id=message_id,
                request=request,
                response=response,
            )
        answer, run_status = _wait_for_issue_answer(bridge_config, issue)
        fallback_text = _fallback_response(issue, run_status=run_status)
        response_text = answer or _progress_response(issue, order_id=order_id, local_case_preflight=local_case_preflight)
        result = {
            "handled": True,
            "classification": "paperclip_case_assist",
            "handoff_target": "paperclip_chef",
            "order_id": order_id,
            "response_text": response_text,
            "local_case_preflight": local_case_preflight,
            "paperclip_issue_id": issue.get("id"),
            "paperclip_issue_identifier": issue.get("identifier"),
            "paperclip_issue_status": run_status,
            "paperclip_run_status": run_status,
            "paperclip_answer_ready": bool(answer),
            "paperclip_result_pending": not bool(answer),
            "paperclip_placeholder_text": fallback_text,
            "suppress_initial_response": False,
            "paperclip_wakeup_id": wakeup.get("id") if isinstance(wakeup, dict) else None,
            "should_send_to_teams": False,
            "should_write_tms": False,
            "should_send_customer_message": False,
        }
        _append_jsonl(root / "runtime" / "paperclip_teams_bridge.jsonl", {**audit_base, **result, "status": "ok"})
        return result
    except Exception as exc:
        result = {
            "handled": True,
            "classification": "paperclip_case_assist_error",
            "handoff_target": "paperclip_chef",
            "order_id": order_id,
            "response_text": (
                "⚠️ Paperclip Chef konnte diese Fallfrage gerade nicht sicher übernehmen.\n"
                "Lage: Die lokale Paperclip-Bridge hat einen technischen Fehler gemeldet; Details sind nur intern im Audit-Log erfasst.\n"
                "Sicherheit: Kein TMS-Write, keine Kunden-/Partnermail, kein Dokumentupload."
            ),
            "paperclip_error": str(exc),
            "local_case_preflight": local_case_preflight,
            "should_send_to_teams": False,
            "should_write_tms": False,
            "should_send_customer_message": False,
        }
        _append_jsonl(root / "runtime" / "paperclip_teams_bridge.jsonl", {**audit_base, **result, "status": "error"})
        return result
