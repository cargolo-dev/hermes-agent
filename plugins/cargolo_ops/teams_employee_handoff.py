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

from .employee_agent import EmployeeRequest
from .employee_runtime import run_employee_runtime
from .models import utc_now_iso


class TeamsHandoffConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    dedicated_channel_ids: set[str] = Field(default_factory=set)
    mention_patterns: tuple[str, ...] = ("@Hermes CARGOLO", "@Hermes", "Hermes CARGOLO")
    audit_enabled: bool = True


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

    runtime_result = run_employee_runtime(
        EmployeeRequest(
            text=request_text,
            channel="teams",
            actor=user_name or user_id,
        ),
        root=root,
    )
    response = runtime_result.employee_response
    handled = response.mode.value != "free_chat"
    row = {
        "timestamp": utc_now_iso(),
        "handled": handled,
        "reason": None if handled else "generic_hermes_chat",
        "classification": response.mode.value,
        "handoff_mode": handoff_mode,
        "requires_mention": requires_mention,
        "channel_id": channel_id,
        "message_id": message_id,
        "user_id": user_id,
        "user_name": user_name,
        "request_text": request_text,
        "passthrough_text": request_text if not handled else None,
        "order_id": response.order_id,
        "response_text": runtime_result.draft_response if handled else None,
        "should_send_to_teams": runtime_result.should_send_to_teams,
        "should_write_tms": runtime_result.should_write_tms,
        "should_send_customer_message": runtime_result.should_send_customer_message,
        "runtime": runtime_result.to_audit_row(),
    }
    if handoff_config.audit_enabled:
        _append_jsonl(root / "runtime" / "teams_employee_handoff.jsonl", row)
    return row
