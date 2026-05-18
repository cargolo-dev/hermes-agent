"""Read-only Teams-to-employee-runtime handoff contract for CARGOLO.

This module is the safe pre-adapter layer for the future Teams integration.  It
only decides whether an inbound Teams message is eligible for the local employee
runtime, runs that runtime, and writes a local audit row.  It deliberately does
not send Teams messages, write TMS data, or contact customers.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .employee_agent import EmployeeRequest, ResponseMode
from .employee_runtime import run_employee_runtime
from .models import utc_now_iso
from .paperclip_teams_bridge import PaperclipTeamsBridgeConfig, handle_paperclip_teams_case_assist


class TeamsHandoffConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    dedicated_channel_ids: set[str] = Field(default_factory=set)
    mention_patterns: tuple[str, ...] = ("@Hermes CARGOLO", "@Hermes", "Hermes CARGOLO")
    audit_enabled: bool = True
    paperclip_bridge_enabled: bool = False
    paperclip_api_base: str | None = None
    paperclip_company_id: str | None = None
    paperclip_project_id: str | None = None
    paperclip_chef_agent_id: str | None = None
    paperclip_wait_timeout_seconds: float | None = None
    paperclip_poll_interval_seconds: float | None = None
    paperclip_terminal_grace_seconds: float | None = None
    paperclip_request_timeout_seconds: float | None = None
    paperclip_wakeup_after_create: bool | None = None


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _strip_mention(text: str, patterns: tuple[str, ...]) -> tuple[str, bool]:
    cleaned = text.strip()
    for pattern in patterns:
        escaped = re.escape(pattern.strip())
        match = re.match(rf"^\s*{escaped}(?=$|[:,\s-])[:,\s-]*", cleaned, flags=re.IGNORECASE)
        if match:
            return cleaned[match.end() :].strip(), True
    return cleaned, False


def build_cargolo_employee_agent_prompt(text: str, *, order_id: str | None = None, intent: str = "free_chat") -> str:
    """Wrap generic Teams chat with the CARGOLO employee operating contract.

    This keeps normal Hermes chat available in dedicated CARGOLO channels while
    making the fallback answer behave like an internal CARGOLO employee rather
    than an unrelated generic chatbot.
    """
    order_line = f"Case: {order_id}. " if order_id else "Case: nicht eindeutig / keine AN-BU erkannt. "
    return (
        "Rolle: Du bist Hermes CARGOLO in Microsoft Teams — ein interner, proaktiver Mitarbeiter, "
        "nicht ein generischer Bot. Antworte trotzdem ganz normal als KI, wenn es keine Transportfrage ist.\n"
        f"{order_line}Intent: {intent}.\n"
        "Arbeitsweise: Bei Transport-/ASR-Fragen TMS-first. Jede AN/BU muss zuerst live im ASR-TMS existieren; "
        "wenn nicht, kurz sagen `nicht im TMS zu finden` und keine n8n-/Mail-Historie suchen. "
        "Erst nach positivem TMS-Fund bei Bedarf lokale Case-Folder, TMS/MCP-Kontext, Mail-Historie, Dokumentregistry, "
        "Pricing/Billing-Kontext heranziehen.\n"
        "Sicherheit: Teams-Freitext darf keine direkten TMS-, Angebots-, Dokumentupload- oder Kundenmail-Writes auslösen. "
        "Schreib-/Sende-Wünsche nur als Entwurf/Review behandeln.\n"
        "Antwortstil: Deutsch, knapp, menschlich-operativ, CARGOLO-intern. Keine Audit-Dumps, keine Debug-Pipes, keine KI-Floskeln.\n"
        f"Teams-Nachricht: {text.strip()}"
    )


def _paperclip_config_from_handoff(config: TeamsHandoffConfig) -> PaperclipTeamsBridgeConfig:
    env_config = PaperclipTeamsBridgeConfig.from_env(enabled=config.paperclip_bridge_enabled)
    return PaperclipTeamsBridgeConfig(
        enabled=bool(config.paperclip_bridge_enabled),
        api_base=(config.paperclip_api_base or env_config.api_base).rstrip("/"),
        company_id=config.paperclip_company_id or env_config.company_id,
        project_id=config.paperclip_project_id or env_config.project_id,
        chef_agent_id=config.paperclip_chef_agent_id or env_config.chef_agent_id,
        issue_priority=env_config.issue_priority,
        issue_work_mode=env_config.issue_work_mode,
        wait_timeout_seconds=(
            config.paperclip_wait_timeout_seconds
            if config.paperclip_wait_timeout_seconds is not None
            else env_config.wait_timeout_seconds
        ),
        poll_interval_seconds=(
            config.paperclip_poll_interval_seconds
            if config.paperclip_poll_interval_seconds is not None
            else env_config.poll_interval_seconds
        ),
        terminal_grace_seconds=(
            config.paperclip_terminal_grace_seconds
            if config.paperclip_terminal_grace_seconds is not None
            else env_config.terminal_grace_seconds
        ),
        wakeup_after_create=(
            config.paperclip_wakeup_after_create
            if config.paperclip_wakeup_after_create is not None
            else env_config.wakeup_after_create
        ),
        request_timeout_seconds=(
            config.paperclip_request_timeout_seconds
            if config.paperclip_request_timeout_seconds is not None
            else env_config.request_timeout_seconds
        ),
    )


def handle_teams_employee_message(
    *,
    root: Path,
    text: str,
    channel_id: str,
    message_id: str,
    user_id: str | None = None,
    user_name: str | None = None,
    config: TeamsHandoffConfig | None = None,
) -> dict[str, Any]:
    """Route a Teams inbound message to the local employee runtime if eligible.

    Dedicated CARGOLO/Hermes channels treat every message as intended for Hermes.
    Shared channels require a mention so the adapter does not intercept normal
    team chatter.
    """

    handoff_config = config or TeamsHandoffConfig()
    is_dedicated = channel_id in handoff_config.dedicated_channel_ids
    cleaned_text, has_mention = _strip_mention(text, handoff_config.mention_patterns)

    if is_dedicated:
        request_text = cleaned_text
        handoff_mode = "dedicated_channel"
        requires_mention = False
    elif has_mention:
        request_text = cleaned_text
        handoff_mode = "mention"
        requires_mention = True
    else:
        return {"handled": False, "reason": "mention_required", "requires_mention": True}

    employee_request = EmployeeRequest(
        text=request_text,
        channel="teams",
        actor=user_name or user_id,
    )
    runtime_result = run_employee_runtime(
        employee_request,
        root=root,
    )
    response = runtime_result.employee_response
    mode_value = response.mode.value if isinstance(response.mode, ResponseMode) else str(response.mode)
    handled = mode_value != ResponseMode.FREE_CHAT.value
    order_id = getattr(response, "order_id", None)
    bridge_config = _paperclip_config_from_handoff(handoff_config)

    base_row = {
        "timestamp": utc_now_iso(),
        "handoff_mode": handoff_mode,
        "requires_mention": requires_mention,
        "channel_id": channel_id,
        "message_id": message_id,
        "user_id": user_id,
        "user_name": user_name,
        "request_text": request_text,
        "order_id": order_id,
        "runtime": runtime_result.to_audit_row(),
        "paperclip_bridge_enabled": bridge_config.enabled,
    }

    if bridge_config.enabled and mode_value == ResponseMode.CASE_ASSIST.value:
        bridge_result = handle_paperclip_teams_case_assist(
            root=root,
            request=employee_request,
            response=response,
            channel_id=channel_id,
            message_id=message_id,
            user_id=user_id,
            user_name=user_name,
            config=bridge_config,
        )
        row = {
            **base_row,
            **bridge_result,
            "handled": True,
            "classification": bridge_result.get("classification") or "paperclip_case_assist",
            "passthrough_text": None,
            "allow_generic_chat": False,
            "agent_prompt": None,
            "should_send_to_teams": False,
            "should_write_tms": False,
            "should_send_customer_message": False,
        }
        if handoff_config.audit_enabled:
            _append_jsonl(root / "runtime" / "teams_employee_handoff.jsonl", row)
        return row

    # In Paperclip bridge mode, non-operative free chat must remain normal Hermes
    # chat. Do not wrap it in the CARGOLO employee prompt; only CASE_ASSIST goes
    # through Paperclip, while guarded/draft actions stay locally blocked.
    agent_prompt = None
    if not handled and not bridge_config.enabled:
        agent_prompt = build_cargolo_employee_agent_prompt(request_text, order_id=order_id, intent="free_chat")

    row = {
        **base_row,
        "handled": handled,
        "reason": None if handled else "generic_hermes_chat",
        "classification": mode_value,
        "passthrough_text": request_text if not handled else None,
        "allow_generic_chat": not handled,
        "agent_prompt": agent_prompt,
        "response_text": runtime_result.draft_response if handled else None,
        "should_send_to_teams": runtime_result.should_send_to_teams,
        "should_write_tms": runtime_result.should_write_tms,
        "should_send_customer_message": runtime_result.should_send_customer_message,
    }
    if handoff_config.audit_enabled:
        _append_jsonl(root / "runtime" / "teams_employee_handoff.jsonl", row)
    return row
