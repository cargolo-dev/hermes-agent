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


def _build_issue_description(
    *,
    request: EmployeeRequest,
    response: EmployeeResponse,
    channel_id: str,
    message_id: str,
    user_id: str | None,
    user_name: str | None,
) -> str:
    order_id = response.order_id or request.order_id or "nicht eindeutig"
    actor = user_name or user_id or request.actor or "Teams Operator"
    return f"""CARGOLO Teams-Fallfrage für Paperclip Chef.

Teams-Kontext:
- Originalfrage: {request.text.strip()}
- Actor: {actor}
- User ID: {user_id or 'n/a'}
- Channel/Conversation: {channel_id or 'n/a'}
- Message ID: {message_id or 'n/a'}
- Order/Case: {order_id}

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
        for key in ("comments", "items", "data", "results"):
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
        # Only an explicit TEAMS_ANTWORT block may become a Teams follow-up.
        # Markerless Paperclip comments are often heartbeat summaries, review
        # notes, or debug/audit text and must stay internal.
        answer = _extract_teams_answer(_comment_body(comment), require_marker=True)
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
    latest_status: str | None = None
    while time.monotonic() <= deadline:
        answer = _latest_issue_comment_answer(bridge_config, issue_id)
        if answer:
            return {"answer": answer, "issue_status": latest_status, "timed_out": False}
        try:
            latest_status = _issue_status(bridge_config, issue_id)
        except RuntimeError:
            latest_status = latest_status or None
        if latest_status in _TERMINAL_ISSUE_STATUSES:
            # A final comment can be committed a few seconds after the issue status.
            time.sleep(min(interval, 3.0))
            answer = _latest_issue_comment_answer(bridge_config, issue_id)
            return {"answer": answer, "issue_status": latest_status, "timed_out": False}
        time.sleep(interval)
    return {"answer": None, "issue_status": latest_status, "timed_out": True}


def _wait_for_issue_answer(config: PaperclipTeamsBridgeConfig, issue: dict[str, Any]) -> tuple[str | None, str | None]:
    issue_id = str(issue.get("id") or "")
    if not issue_id or config.wait_timeout_seconds <= 0:
        return None, None
    deadline = time.monotonic() + config.wait_timeout_seconds
    latest_status: str | None = None
    while time.monotonic() <= deadline:
        answer = _latest_issue_comment_answer(config, issue_id)
        if answer:
            return answer, latest_status
        try:
            latest_status = _issue_status(config, issue_id)
        except RuntimeError:
            latest_status = latest_status or None
        if latest_status in _TERMINAL_ISSUE_STATUSES:
            # Give Paperclip one short extra chance to persist the final comment.
            time.sleep(min(config.poll_interval_seconds, 1.0))
            answer = _latest_issue_comment_answer(config, issue_id)
            if answer:
                return answer, latest_status
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

    audit_base: dict[str, Any] = {
        "timestamp": started_at,
        "source": "teams_paperclip_bridge",
        "channel_id": channel_id,
        "message_id": message_id,
        "user_id": user_id,
        "user_name": user_name,
        "order_id": response.order_id or request.order_id,
        "request_text": request.text,
        "company_id": bridge_config.company_id,
        "project_id": bridge_config.project_id,
        "chef_agent_id": bridge_config.chef_agent_id,
        "should_send_to_teams": False,
        "should_write_tms": False,
        "should_send_customer_message": False,
    }

    try:
        issue = _create_chef_issue(
            config=bridge_config,
            request=request,
            response=response,
            channel_id=channel_id,
            message_id=message_id,
            user_id=user_id,
            user_name=user_name,
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
        response_text = answer or _fallback_response(issue, run_status=run_status)
        result = {
            "handled": True,
            "classification": "paperclip_case_assist",
            "handoff_target": "paperclip_chef",
            "order_id": response.order_id or request.order_id,
            "response_text": response_text,
            "paperclip_issue_id": issue.get("id"),
            "paperclip_issue_identifier": issue.get("identifier"),
            "paperclip_issue_status": run_status,
            "paperclip_run_status": run_status,
            "paperclip_answer_ready": bool(answer),
            "paperclip_result_pending": not bool(answer),
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
            "order_id": response.order_id or request.order_id,
            "response_text": (
                "⚠️ Paperclip Chef konnte diese Fallfrage gerade nicht sicher übernehmen.\n"
                "Lage: Die lokale Paperclip-Bridge hat einen technischen Fehler gemeldet; Details sind nur intern im Audit-Log erfasst.\n"
                "Sicherheit: Kein TMS-Write, keine Kunden-/Partnermail, kein Dokumentupload."
            ),
            "paperclip_error": str(exc),
            "should_send_to_teams": False,
            "should_write_tms": False,
            "should_send_customer_message": False,
        }
        _append_jsonl(root / "runtime" / "paperclip_teams_bridge.jsonl", {**audit_base, **result, "status": "error"})
        return result
