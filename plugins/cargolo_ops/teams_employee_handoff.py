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

from .employee_agent import EmployeeRequest, ResponseMode, handle_employee_request
from .employee_runtime import run_employee_runtime
from .teams_ops_router import build_case_evidence_agent_handoff, route_teams_ops_message
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


    # Deterministic non-card action flows (upload/internal/TMS review queues) must
    # win over generic case-assist classification.  They are still review-only and
    # never perform TMS/customer side effects.
    early_ops_review: dict[str, object] = {"handled": False}
    early_action_text = request_text.lower()
    if (
        any(token in early_action_text for token in ("tms", "mrn", "hbl", "mbl", "hawb", "zollreferenz", "offene freig", "pending", "review", "status"))
        or (any(token in early_action_text for token in ("lade", "upload", "hochladen")) and any(token in early_action_text for token in ("dokument", "doc", "ci", "pl", "datei", "commercial invoice", "packing")))
        or any(token in early_action_text for token in ("markier erledigt", "markiere erledigt", "todo erledigt", "notiz", "interne note", "case-note"))
    ):
        early_ops_review = route_teams_ops_message(
            text=request_text,
            root=root,
            chat_id=channel_id,
            user_id=user_id,
            user_name=user_name,
            message_id=message_id,
        )
    early_classification = str(early_ops_review.get("classification") or "")
    if early_ops_review.get("handled") and early_classification in {
        "tms_review_card_prepared",
        "tms_control_without_card_context",
        "document_upload_review_prepared",
        "internal_action_review_prepared",
        "pending_tms_reviews",
        "ops_status",
        "shipment_not_found_in_tms",
    }:
        row = {
            "timestamp": utc_now_iso(),
            "handoff_mode": handoff_mode,
            "requires_mention": requires_mention,
            "channel_id": channel_id,
            "message_id": message_id,
            "user_id": user_id,
            "user_name": user_name,
            "request_text": request_text,
            "order_id": early_ops_review.get("order_id"),
            "runtime": {},
            "handled": True,
            "reason": None,
            "classification": early_classification,
            "passthrough_text": None,
            "allow_generic_chat": False,
            "agent_prompt": None,
            "response_text": early_ops_review.get("response_text"),
            "teams_tms_review_cards": early_ops_review.get("teams_tms_review_cards") or [],
            "teams_upload_review_cards": early_ops_review.get("teams_upload_review_cards") or [],
            "teams_internal_review_cards": early_ops_review.get("teams_internal_review_cards") or [],
            "side_effects": early_ops_review.get("side_effects") or {},
            "should_send_to_teams": bool(early_ops_review.get("should_send_to_teams", False)),
            "should_write_tms": False,
            "should_send_customer_message": False,
        }
        if handoff_config.audit_enabled:
            _append_jsonl(root / "runtime" / "teams_employee_handoff.jsonl", row)
        return row

    employee_request = EmployeeRequest(
        text=request_text,
        channel="teams",
        actor=user_name or user_id,
    )
    response = handle_employee_request(employee_request)
    mode_value = response.mode.value if isinstance(response.mode, ResponseMode) else str(response.mode)
    order_id = getattr(response, "order_id", None)

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
        "runtime": response.to_audit_row(),
    }

    if mode_value in {ResponseMode.GUARDED_ACTION_REQUIRED.value, ResponseMode.DRAFT_ONLY.value}:
        ops_review = route_teams_ops_message(
            text=request_text,
            root=root,
            chat_id=channel_id,
            user_id=user_id,
            user_name=user_name,
            message_id=message_id,
        )
        if ops_review.get("handled"):
            row = {
                **base_row,
                "handled": True,
                "reason": None,
                "classification": str(ops_review.get("classification") or mode_value),
                "passthrough_text": None,
                "allow_generic_chat": False,
                "agent_prompt": None,
                "response_text": ops_review.get("response_text"),
                "teams_tms_review_cards": ops_review.get("teams_tms_review_cards") or [],
                "teams_upload_review_cards": ops_review.get("teams_upload_review_cards") or [],
                "teams_internal_review_cards": ops_review.get("teams_internal_review_cards") or [],
                "side_effects": ops_review.get("side_effects") or {},
                "should_send_to_teams": bool(ops_review.get("should_send_to_teams", False)),
                "should_write_tms": False,
                "should_send_customer_message": False,
            }
            if handoff_config.audit_enabled:
                _append_jsonl(root / "runtime" / "teams_employee_handoff.jsonl", row)
            return row

    if mode_value == ResponseMode.CASE_ASSIST.value and order_id:
        handoff = build_case_evidence_agent_handoff(
            root=root,
            order_id=order_id,
            text=request_text,
            user_name=user_name or user_id,
        )
        row = {
            **base_row,
            "handled": bool(handoff.get("handled")),
            "reason": None if handoff.get("handled") else "agent_case_assist_after_evidence_refresh",
            "classification": str(handoff.get("classification") or "case_assist_agent_handoff"),
            "passthrough_text": request_text,
            "allow_generic_chat": bool(handoff.get("allow_generic_chat")),
            "agent_prompt": handoff.get("agent_prompt"),
            "response_text": handoff.get("response_text") if handoff.get("handled") else None,
            "teams_tms_review_cards": handoff.get("teams_tms_review_cards") or [],
            "teams_upload_review_cards": handoff.get("teams_upload_review_cards") or [],
            "teams_internal_review_cards": handoff.get("teams_internal_review_cards") or [],
            "case_path": handoff.get("case_path"),
            "lifecycle": handoff.get("lifecycle"),
            "should_send_to_teams": bool(handoff.get("should_send_to_teams", False)),
            "should_write_tms": bool(handoff.get("should_write_tms", False)),
            "should_send_customer_message": bool(handoff.get("should_send_customer_message", False)),
        }
        if handoff_config.audit_enabled:
            _append_jsonl(root / "runtime" / "teams_employee_handoff.jsonl", row)
        return row

    runtime_result = run_employee_runtime(
        employee_request,
        root=root,
    )
    response = runtime_result.employee_response
    mode_value = response.mode.value if isinstance(response.mode, ResponseMode) else str(response.mode)
    handled = mode_value != ResponseMode.FREE_CHAT.value
    order_id = getattr(response, "order_id", None)

    base_row = {
        **base_row,
        "order_id": order_id,
        "runtime": runtime_result.to_audit_row(),
    }

    agent_prompt = None
    if not handled:
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
