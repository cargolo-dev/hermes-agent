"""Agent-first CARGOLO employee runtime contract.

This layer is intentionally *not* a rigid workflow bot.  It keeps normal Hermes
chat/drafting behaviour available while marking operational boundaries that need
guards (Teams send, TMS write, customer send).  The coordinator remains the
traceability/guardrail substrate; this module models the employee brain contract.
"""

from __future__ import annotations

from enum import Enum
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .models import normalize_order_ids, utc_now_iso


class ResponseMode(str, Enum):
    FREE_CHAT = "free_chat"
    CASE_ASSIST = "case_assist"
    DRAFT_ONLY = "draft_only"
    GUARDED_ACTION_REQUIRED = "guarded_action_required"


class BoundaryAction(str, Enum):
    NONE = "none"
    TEAMS_SEND = "teams_send"
    TMS_WRITE = "tms_write"
    CUSTOMER_MESSAGE_DRAFT = "customer_message_draft"
    CUSTOMER_MESSAGE_SEND = "customer_message_send"
    DOCUMENT_UPLOAD = "document_upload"
    CRON_OUTBOUND = "cron_outbound"


class ContextNeed(str, Enum):
    CASE_FOLDER = "case_folder"
    MAIL_HISTORY = "mail_history"
    TMS_SNAPSHOT = "tms_snapshot"
    DOCUMENTS = "documents"
    PRICING_KB = "pricing_kb"
    BILLING_CONTEXT = "billing_context"
    TEAMS_THREAD = "teams_thread"


class SpecialistPlan(BaseModel):
    model_config = ConfigDict(extra="allow", use_enum_values=False)

    tasks: list[dict[str, Any]] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"tasks": self.tasks}


class EmployeeRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    text: str
    channel: str = "telegram"
    order_id: str | None = None
    actor: str | None = None
    context_refs: list[str] = Field(default_factory=list)

    @field_validator("order_id", mode="before")
    @classmethod
    def normalize_order_id(cls, value: str | None) -> str | None:
        if not value:
            return None
        matches = normalize_order_ids(value)
        return matches[0] if matches else None


class EmployeeResponse(BaseModel):
    model_config = ConfigDict(extra="allow", use_enum_values=False)

    mode: ResponseMode
    agent_first: bool = True
    order_id: str | None = None
    can_answer_normally: bool = True
    requires_guard: bool = False
    boundary_action: BoundaryAction = BoundaryAction.NONE
    guard_reason: str | None = None
    draft_instruction: str | None = None
    context_needs: list[ContextNeed] = Field(default_factory=list)
    specialist_plan: SpecialistPlan = Field(default_factory=SpecialistPlan)
    safety_notes: list[str] = Field(default_factory=list)
    should_send_to_teams: bool = False
    should_write_tms: bool = False
    should_send_customer_message: bool = False

    def to_audit_row(self) -> dict[str, Any]:
        return {
            "timestamp": utc_now_iso(),
            "mode": self.mode.value,
            "agent_first": self.agent_first,
            "order_id": self.order_id,
            "can_answer_normally": self.can_answer_normally,
            "requires_guard": self.requires_guard,
            "boundary_action": self.boundary_action.value,
            "guard_reason": self.guard_reason,
            "draft_instruction": self.draft_instruction,
            "context_needs": [need.value for need in self.context_needs],
            "specialist_plan": self.specialist_plan.to_dict(),
            "safety_notes": self.safety_notes,
            "should_send_to_teams": self.should_send_to_teams,
            "should_write_tms": self.should_write_tms,
            "should_send_customer_message": self.should_send_customer_message,
        }


def _text(request: EmployeeRequest) -> str:
    return (request.text or "").lower()


def _order_id(request: EmployeeRequest) -> str | None:
    if request.order_id:
        return request.order_id
    matches = normalize_order_ids(request.text)
    return matches[0] if matches else None


def _has_any(text: str, tokens: tuple[str, ...]) -> bool:
    return any(token in text for token in tokens)


def _is_teams_send_request(text: str) -> bool:
    send_semantics = _has_any(text, ("poste", "post ", "schick", "sende", "send", "veröffentliche", "veroeffentliche"))
    return send_semantics and "teams" in text


def _is_tms_write_request(text: str) -> bool:
    imperative_write = _has_any(
        text,
        (
            "eintragen",
            "trage",
            "trag ",
            "schreib",
            "write",
            "ändern",
            "aendern",
            "übernehmen",
            "uebernehmen",
            "setzen",
            "setze",
            "setz",
            "aktualisieren",
            "aktualisier",
        ),
    )
    # Treat English "update" as write intent only when used imperatively at the
    # beginning.  German ops often asks for "ein Update" as a read-only status.
    imperative_update = re.match(r"^\s*update\b", text) is not None
    write_semantics = imperative_write or imperative_update
    tms_context = "tms" in text
    tms_field_context = _has_any(text, ("mrn", "hbl", "mbl", "hawb", "mawb", "customs", "zoll", "reference", "referenz"))
    return write_semantics and (tms_context or tms_field_context)


def _is_customer_draft_request(text: str) -> bool:
    draft_semantics = _has_any(text, ("schreib", "formulier", "entwurf", "draft", "vorschlag"))
    customer_context = _has_any(text, ("kunde", "kunden", "customer", "dienstleister", "partner", "carrier", "reederei", "spedition"))
    return draft_semantics and customer_context


def _is_customer_send_request(text: str) -> bool:
    send_semantics = _has_any(text, ("sende", "send", "antworte", "antworten", "mail dem", "mail an", "email", "e-mail"))
    customer_context = _has_any(text, ("kunde", "kunden", "customer"))
    return send_semantics and customer_context


def _is_document_upload_request(text: str) -> bool:
    upload_semantics = _has_any(text, ("lade", "upload", "hochladen", "hoch", "anhängen", "anhaengen"))
    document_context = _has_any(text, ("dokument", "doc", "ci", "commercial invoice", "pl", "packing list", "awb", "datei"))
    destination_context = _has_any(text, ("tms", "teams", "case", "akte", "sendung"))
    return upload_semantics and document_context and destination_context


def _is_cron_outbound_request(text: str) -> bool:
    cron_context = _has_any(text, ("cron", "regelmäßig", "regelmaessig", "jeden morgen", "täglich", "taeglich", "automatisch"))
    outbound_context = _has_any(text, ("teams", "kunden", "kunde", "mail", "telegram", "melden", "informiert", "posten"))
    return cron_context and outbound_context


def _context_needs_for(text: str, order_id: str | None) -> list[ContextNeed]:
    if not order_id:
        return []
    needs = [ContextNeed.CASE_FOLDER]
    if _has_any(text, ("mail", "historie", "kunde", "kunden", "antwort")):
        needs.append(ContextNeed.MAIL_HISTORY)
    if _has_any(text, ("tms", "status", "stand", "lage")):
        needs.append(ContextNeed.TMS_SNAPSHOT)
    if _has_any(text, ("dokument", "doc", "awb", "ci", "pl", "commercial invoice", "packing list", "komplett")):
        needs.append(ContextNeed.DOCUMENTS)
    if _has_any(text, ("preis", "pricing", "angebot", "rate")):
        needs.append(ContextNeed.PRICING_KB)
    if _has_any(text, ("rechnung", "billing", "invoice", "kosten")):
        needs.append(ContextNeed.BILLING_CONTEXT)
    if _has_any(text, ("teams", "thread", "karte", "card")):
        needs.append(ContextNeed.TEAMS_THREAD)
    return needs


def _task(agent: str, order_id: str | None, *, purpose: str, priority: int, request: EmployeeRequest) -> dict[str, Any]:
    return {
        "agent": agent,
        "mode": "read_only",
        "order_id": order_id,
        "purpose": purpose,
        "priority": priority,
        "source": "employee_agent",
        "context_refs": request.context_refs,
    }


def _specialist_plan_for(request: EmployeeRequest, order_id: str | None, needs: list[ContextNeed]) -> SpecialistPlan:
    tasks: list[dict[str, Any]] = []
    if ContextNeed.CASE_FOLDER in needs:
        tasks.append(_task("case_context", order_id, purpose="collect case folder context", priority=10, request=request))
    if ContextNeed.DOCUMENTS in needs:
        tasks.append(_task("document_analyst", order_id, purpose="analyze document state and discrepancies", priority=20, request=request))
    if ContextNeed.MAIL_HISTORY in needs:
        tasks.append(_task("mail_history", order_id, purpose="summarize latest ASR mail history", priority=30, request=request))
    if ContextNeed.TMS_SNAPSHOT in needs:
        tasks.append(_task("tms_snapshot", order_id, purpose="read current shipment/TMS state", priority=40, request=request))
    if ContextNeed.PRICING_KB in needs:
        tasks.append(_task("pricing_context", order_id, purpose="retrieve pricing knowledge context", priority=50, request=request))
    if ContextNeed.BILLING_CONTEXT in needs:
        tasks.append(_task("billing_context", order_id, purpose="retrieve billing/outcome evidence", priority=60, request=request))
    if ContextNeed.TEAMS_THREAD in needs:
        tasks.append(_task("teams_thread_context", order_id, purpose="read related Teams thread context", priority=70, request=request))
    return SpecialistPlan(tasks=tasks)


def handle_employee_request(request: EmployeeRequest) -> EmployeeResponse:
    """Classify an employee interaction without taking external side effects.

    This function deliberately leaves normal chat/drafting possible.  It only
    marks boundary actions for a later guard/approval layer.
    """

    text = _text(request)
    order_id = _order_id(request)

    if _is_customer_send_request(text):
        return EmployeeResponse(
            mode=ResponseMode.GUARDED_ACTION_REQUIRED,
            order_id=order_id,
            requires_guard=True,
            boundary_action=BoundaryAction.CUSTOMER_MESSAGE_SEND,
            guard_reason="Approval required before sending any customer-facing message.",
            draft_instruction="Prepare a customer-facing draft only; do not send it.",
            safety_notes=["Customer send blocked; Entwurf erlaubt, aber keine Kundenmail auslösen."],
        )

    if _is_customer_draft_request(text):
        draft_needs = _context_needs_for(text, order_id)
        if order_id and (not draft_needs or draft_needs == [ContextNeed.CASE_FOLDER]):
            draft_needs = [ContextNeed.CASE_FOLDER, ContextNeed.MAIL_HISTORY, ContextNeed.TMS_SNAPSHOT]
        return EmployeeResponse(
            mode=ResponseMode.DRAFT_ONLY,
            order_id=order_id,
            boundary_action=BoundaryAction.CUSTOMER_MESSAGE_DRAFT,
            draft_instruction="Draft a business-facing message, but do not send it.",
            context_needs=draft_needs,
            specialist_plan=_specialist_plan_for(request, order_id, draft_needs),
            safety_notes=["Nur Entwurf: nicht senden, keine Kundenmail/Partnernachricht auslösen."],
        )

    if _is_document_upload_request(text):
        return EmployeeResponse(
            mode=ResponseMode.GUARDED_ACTION_REQUIRED,
            order_id=order_id,
            requires_guard=True,
            boundary_action=BoundaryAction.DOCUMENT_UPLOAD,
            guard_reason="Approval required before uploading documents or changing document state.",
            safety_notes=["Document upload blocked by guard; no file uploaded."],
        )

    if _is_cron_outbound_request(text):
        return EmployeeResponse(
            mode=ResponseMode.GUARDED_ACTION_REQUIRED,
            order_id=order_id,
            requires_guard=True,
            boundary_action=BoundaryAction.CRON_OUTBOUND,
            guard_reason="Approval required before creating recurring outbound notifications.",
            safety_notes=["Cron/outbound notification blocked by guard; no schedule created."],
        )

    if _is_tms_write_request(text):
        return EmployeeResponse(
            mode=ResponseMode.GUARDED_ACTION_REQUIRED,
            order_id=order_id,
            requires_guard=True,
            boundary_action=BoundaryAction.TMS_WRITE,
            guard_reason="Approval required before any TMS write intent can be prepared or executed.",
            safety_notes=["TMS write blocked by default-deny guard; no write executed."],
        )

    if _is_teams_send_request(text):
        return EmployeeResponse(
            mode=ResponseMode.GUARDED_ACTION_REQUIRED,
            order_id=order_id,
            requires_guard=True,
            boundary_action=BoundaryAction.TEAMS_SEND,
            guard_reason="Approval required before sending anything to Teams.",
            draft_instruction="Prepare a draft Teams update only; do not send it.",
            safety_notes=["Teams send blocked; draft/review only."],
        )

    needs = _context_needs_for(text, order_id)
    if order_id:
        # Case assist stays dynamic and agent-first: context needs drive the plan,
        # but the human-facing response can still be a normal Hermes answer.
        return EmployeeResponse(
            mode=ResponseMode.CASE_ASSIST,
            order_id=order_id,
            context_needs=needs or [ContextNeed.CASE_FOLDER],
            specialist_plan=_specialist_plan_for(request, order_id, needs or [ContextNeed.CASE_FOLDER]),
            safety_notes=["Read-only case assist; no Teams/TMS/customer side effect."],
        )

    return EmployeeResponse(mode=ResponseMode.FREE_CHAT)
