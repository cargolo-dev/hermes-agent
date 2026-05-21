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


class EmployeeIntent(str, Enum):
    FREE_CHAT = "free_chat"
    CASE_OVERVIEW = "case_overview"
    ETA_STATUS = "eta_status"
    CUSTOMER_REPLY_CHECK = "customer_reply_check"
    DOCUMENT_GAP_CHECK = "document_gap_check"
    CLEANLINESS_CHECK = "cleanliness_check"
    RELEASE_READINESS_CHECK = "release_readiness_check"
    BLOCKER_CHECK = "blocker_check"
    CUSTOMER_OPEN_ITEMS_CHECK = "customer_open_items_check"
    CUSTOMS_READINESS_CHECK = "customs_readiness_check"
    DELAY_REASON_CHECK = "delay_reason_check"
    TODAYS_WORK = "todays_work"
    CUSTOMER_MESSAGE_DRAFT = "customer_message_draft"
    CUSTOMER_MESSAGE_SEND = "customer_message_send"
    TMS_WRITE_REQUEST = "tms_write_request"
    DOCUMENT_UPLOAD_REQUEST = "document_upload_request"
    TEAMS_SEND_REQUEST = "teams_send_request"
    CRON_OUTBOUND_REQUEST = "cron_outbound_request"


class RequestedSource(str, Enum):
    CASE_STATE = "case_state"
    TMS_SNAPSHOT = "tms_snapshot"
    EMAIL_INDEX = "email_index"
    DOCUMENT_REGISTRY = "document_registry"
    DOCUMENT_ANALYSIS = "document_analysis"
    BILLING_CONTEXT = "billing_context"
    PRICING_KB = "pricing_kb"
    TEAMS_THREAD_CONTEXT = "teams_thread_context"


class Urgency(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    TODAY = "today"


class AnswerDepth(str, Enum):
    BRIEF = "brief"
    NORMAL = "normal"
    DETAILED = "detailed"


class StructuredEmployeeIntent(BaseModel):
    model_config = ConfigDict(extra="allow", use_enum_values=False)

    intent: EmployeeIntent = EmployeeIntent.FREE_CHAT
    order_id: str | None = None
    requested_sources: list[RequestedSource] = Field(default_factory=list)
    wants_write: bool = False
    urgency: Urgency = Urgency.NORMAL
    answer_depth: AnswerDepth = AnswerDepth.NORMAL
    needs_customer_draft: bool = False
    needs_internal_recommendation: bool = False
    boundary_action: BoundaryAction = BoundaryAction.NONE
    confidence: float = 1.0
    reasons: list[str] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent.value,
            "order_id": self.order_id,
            "requested_sources": [source.value for source in self.requested_sources],
            "wants_write": self.wants_write,
            "urgency": self.urgency.value,
            "answer_depth": self.answer_depth.value,
            "needs_customer_draft": self.needs_customer_draft,
            "needs_internal_recommendation": self.needs_internal_recommendation,
            "boundary_action": self.boundary_action.value,
            "confidence": self.confidence,
            "reasons": self.reasons,
        }


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
    structured_intent: StructuredEmployeeIntent | None = None

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
            "structured_intent": self.structured_intent.to_dict() if self.structured_intent else None,
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
    read_only_or_negated_write = _has_any(
        text,
        (
            "read-only",
            "readonly",
            "nur lesen",
            "nur lesend",
            "keine tms-write",
            "keine tms write",
            "keine tms-writes",
            "keine tms writes",
            "keine tms-änderung",
            "keine tms aenderung",
            "keine tms-änderungen",
            "keine tms aenderungen",
            "no tms write",
            "no tms-write",
        ),
    )
    if read_only_or_negated_write:
        return False

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
    if _has_any(text, ("geantwortet", "antwort vom kunden", "kundenantwort", "hat der kunde", "hat kunde")):
        return False
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
    if _has_any(text, ("mail", "historie", "kunde", "kunden", "antwort", "alles", "komplett", "übersicht", "uebersicht")):
        needs.append(ContextNeed.MAIL_HISTORY)
    if _has_any(text, ("tms", "status", "stand", "lage", "freigabe", "freigaben", "offen", "alles", "komplett", "übersicht", "uebersicht")):
        needs.append(ContextNeed.TMS_SNAPSHOT)
    if _has_any(text, ("dokument", "doc", "awb", "ci", "pl", "commercial invoice", "packing list", "komplett", "alles")):
        needs.append(ContextNeed.DOCUMENTS)
    if _has_any(text, ("preis", "pricing", "angebot", "rate")):
        needs.append(ContextNeed.PRICING_KB)
    if _has_any(text, ("rechnung", "billing", "invoice", "kosten")):
        needs.append(ContextNeed.BILLING_CONTEXT)
    if _has_any(text, ("teams", "thread", "karte", "card", "freigabe", "freigaben")):
        needs.append(ContextNeed.TEAMS_THREAD)
    return needs



def _norm_text(value: str) -> str:
    text = (value or "").lower()
    return (
        text.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )


def _sources_for_needs(needs: list[ContextNeed]) -> list[RequestedSource]:
    result: list[RequestedSource] = []
    mapping = {
        ContextNeed.CASE_FOLDER: [RequestedSource.CASE_STATE],
        ContextNeed.MAIL_HISTORY: [RequestedSource.EMAIL_INDEX],
        ContextNeed.TMS_SNAPSHOT: [RequestedSource.TMS_SNAPSHOT],
        ContextNeed.DOCUMENTS: [RequestedSource.DOCUMENT_REGISTRY, RequestedSource.DOCUMENT_ANALYSIS],
        ContextNeed.PRICING_KB: [RequestedSource.PRICING_KB],
        ContextNeed.BILLING_CONTEXT: [RequestedSource.BILLING_CONTEXT],
        ContextNeed.TEAMS_THREAD: [RequestedSource.TEAMS_THREAD_CONTEXT],
    }
    for need in needs:
        for source in mapping.get(need, []):
            if source not in result:
                result.append(source)
    return result


def _needs_for_sources(sources: list[RequestedSource]) -> list[ContextNeed]:
    result: list[ContextNeed] = []
    mapping = {
        RequestedSource.CASE_STATE: ContextNeed.CASE_FOLDER,
        RequestedSource.EMAIL_INDEX: ContextNeed.MAIL_HISTORY,
        RequestedSource.TMS_SNAPSHOT: ContextNeed.TMS_SNAPSHOT,
        RequestedSource.DOCUMENT_REGISTRY: ContextNeed.DOCUMENTS,
        RequestedSource.DOCUMENT_ANALYSIS: ContextNeed.DOCUMENTS,
        RequestedSource.PRICING_KB: ContextNeed.PRICING_KB,
        RequestedSource.BILLING_CONTEXT: ContextNeed.BILLING_CONTEXT,
        RequestedSource.TEAMS_THREAD_CONTEXT: ContextNeed.TEAMS_THREAD,
    }
    for source in sources:
        need = mapping.get(source)
        if need and need not in result:
            result.append(need)
    return result


def _intent_sources(*sources: RequestedSource) -> list[RequestedSource]:
    result: list[RequestedSource] = []
    for source in (RequestedSource.CASE_STATE, *sources):
        if source not in result:
            result.append(source)
    return result


def classify_employee_intent(request: EmployeeRequest) -> StructuredEmployeeIntent:
    text = _text(request)
    norm = _norm_text(request.text)
    order_id = _order_id(request)

    if _is_customer_send_request(text):
        return StructuredEmployeeIntent(intent=EmployeeIntent.CUSTOMER_MESSAGE_SEND, order_id=order_id, wants_write=True, needs_customer_draft=True, boundary_action=BoundaryAction.CUSTOMER_MESSAGE_SEND, reasons=["customer_send_guard"])
    if _is_customer_draft_request(text):
        needs = _context_needs_for(text, order_id)
        if order_id and (not needs or needs == [ContextNeed.CASE_FOLDER]):
            needs = [ContextNeed.CASE_FOLDER, ContextNeed.MAIL_HISTORY, ContextNeed.TMS_SNAPSHOT]
        return StructuredEmployeeIntent(intent=EmployeeIntent.CUSTOMER_MESSAGE_DRAFT, order_id=order_id, requested_sources=_sources_for_needs(needs), needs_customer_draft=True, boundary_action=BoundaryAction.CUSTOMER_MESSAGE_DRAFT, reasons=["customer_draft"])
    if _is_document_upload_request(text):
        return StructuredEmployeeIntent(intent=EmployeeIntent.DOCUMENT_UPLOAD_REQUEST, order_id=order_id, wants_write=True, boundary_action=BoundaryAction.DOCUMENT_UPLOAD, reasons=["document_upload_guard"])
    if _is_cron_outbound_request(text):
        return StructuredEmployeeIntent(intent=EmployeeIntent.CRON_OUTBOUND_REQUEST, order_id=order_id, wants_write=True, boundary_action=BoundaryAction.CRON_OUTBOUND, reasons=["cron_outbound_guard"])
    if _is_tms_write_request(text):
        return StructuredEmployeeIntent(intent=EmployeeIntent.TMS_WRITE_REQUEST, order_id=order_id, wants_write=True, boundary_action=BoundaryAction.TMS_WRITE, reasons=["tms_write_guard"])
    if _is_teams_send_request(text):
        return StructuredEmployeeIntent(intent=EmployeeIntent.TEAMS_SEND_REQUEST, order_id=order_id, wants_write=True, boundary_action=BoundaryAction.TEAMS_SEND, reasons=["teams_send_guard"])

    if not order_id and _has_any(norm, ("was muss ich heute machen", "was ist heute dran", "to dos heute", "todos heute", "heute erledigen", "meine offenen")):
        return StructuredEmployeeIntent(
            intent=EmployeeIntent.TODAYS_WORK,
            order_id=None,
            requested_sources=[RequestedSource.TEAMS_THREAD_CONTEXT, RequestedSource.CASE_STATE, RequestedSource.TMS_SNAPSHOT],
            urgency=Urgency.TODAY,
            answer_depth=AnswerDepth.BRIEF,
            needs_internal_recommendation=True,
            reasons=["todays_work_phrase"],
        )

    intent = EmployeeIntent.CASE_OVERVIEW
    sources = _sources_for_needs(_context_needs_for(text, order_id)) if order_id else []
    recommendation = False
    reasons: list[str] = []
    if order_id:
        if _has_any(norm, ("ziehen lassen", "kann ich den ziehen", "kann raus", "freigeben", "release")):
            intent = EmployeeIntent.RELEASE_READINESS_CHECK
            sources = _intent_sources(RequestedSource.TMS_SNAPSHOT, RequestedSource.EMAIL_INDEX, RequestedSource.DOCUMENT_REGISTRY, RequestedSource.DOCUMENT_ANALYSIS, RequestedSource.BILLING_CONTEXT)
            recommendation = True
            reasons.append("release_readiness_phrase")
        elif _has_any(norm, ("blockt da was", "blockiert", "blockt", "stopper", "hakt", "problem")):
            intent = EmployeeIntent.BLOCKER_CHECK
            sources = _intent_sources(RequestedSource.TMS_SNAPSHOT, RequestedSource.EMAIL_INDEX, RequestedSource.DOCUMENT_REGISTRY, RequestedSource.DOCUMENT_ANALYSIS, RequestedSource.TEAMS_THREAD_CONTEXT)
            recommendation = True
            reasons.append("blocker_phrase")
        elif _has_any(norm, ("kundenseite noch offen", "kunde offen", "kunden offen", "warten auf kunde")):
            intent = EmployeeIntent.CUSTOMER_OPEN_ITEMS_CHECK
            sources = _intent_sources(RequestedSource.EMAIL_INDEX, RequestedSource.TMS_SNAPSHOT)
            recommendation = True
            reasons.append("customer_open_items_phrase")
        elif _has_any(norm, ("alles fuer verzollung", "verzollung komplett", "verzollung", "zoll alles", "customs ready")):
            intent = EmployeeIntent.CUSTOMS_READINESS_CHECK
            sources = _intent_sources(RequestedSource.TMS_SNAPSHOT, RequestedSource.DOCUMENT_REGISTRY, RequestedSource.DOCUMENT_ANALYSIS, RequestedSource.EMAIL_INDEX)
            recommendation = True
            reasons.append("customs_readiness_phrase")
        elif _has_any(norm, ("warum haengt", "warum steht", "warum dauert", "wieso haengt", "delay grund")):
            intent = EmployeeIntent.DELAY_REASON_CHECK
            sources = _intent_sources(RequestedSource.TMS_SNAPSHOT, RequestedSource.EMAIL_INDEX, RequestedSource.DOCUMENT_REGISTRY, RequestedSource.TEAMS_THREAD_CONTEXT)
            recommendation = True
            reasons.append("delay_reason_phrase")
        elif _has_any(norm, ("eta", "etd", "ankunft", "liefertermin", "wann kommt")):
            intent = EmployeeIntent.ETA_STATUS
            sources = _intent_sources(RequestedSource.TMS_SNAPSHOT)
            reasons.append("eta_phrase")
        elif _has_any(norm, ("geantwortet", "antwort", "kunde", "kunden", "mail")):
            intent = EmployeeIntent.CUSTOMER_REPLY_CHECK
            if not sources:
                sources = _intent_sources(RequestedSource.EMAIL_INDEX)
            reasons.append("customer_reply_phrase")
        elif _has_any(norm, ("fehlt", "dokument", "doc", "ci", "pl", "packing", "commercial")):
            intent = EmployeeIntent.DOCUMENT_GAP_CHECK
            if not sources or sources == [RequestedSource.CASE_STATE]:
                sources = _intent_sources(RequestedSource.DOCUMENT_REGISTRY, RequestedSource.DOCUMENT_ANALYSIS)
            reasons.append("document_gap_phrase")
        elif _has_any(norm, ("sauber", "alles ok", "risiko", "freigabe")):
            intent = EmployeeIntent.CLEANLINESS_CHECK
            if not sources or sources == [RequestedSource.CASE_STATE]:
                sources = _intent_sources(RequestedSource.TMS_SNAPSHOT, RequestedSource.EMAIL_INDEX, RequestedSource.DOCUMENT_REGISTRY, RequestedSource.DOCUMENT_ANALYSIS, RequestedSource.BILLING_CONTEXT)
            recommendation = True
            reasons.append("cleanliness_phrase")
        if not sources:
            sources = [RequestedSource.CASE_STATE]
        return StructuredEmployeeIntent(intent=intent, order_id=order_id, requested_sources=sources, needs_internal_recommendation=recommendation, reasons=reasons or ["case_order_id"])

    return StructuredEmployeeIntent(intent=EmployeeIntent.FREE_CHAT)

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
    structured = classify_employee_intent(request)
    order_id = structured.order_id

    if structured.boundary_action is BoundaryAction.CUSTOMER_MESSAGE_SEND:
        return EmployeeResponse(
            mode=ResponseMode.GUARDED_ACTION_REQUIRED,
            order_id=order_id,
            requires_guard=True,
            boundary_action=BoundaryAction.CUSTOMER_MESSAGE_SEND,
            guard_reason="Approval required before sending any customer-facing message.",
            draft_instruction="Prepare a customer-facing draft only; do not send it.",
            safety_notes=["Customer send blocked; Entwurf erlaubt, aber keine Kundenmail auslösen."],
            structured_intent=structured,
        )

    if structured.boundary_action is BoundaryAction.CUSTOMER_MESSAGE_DRAFT:
        draft_needs = _needs_for_sources(structured.requested_sources)
        return EmployeeResponse(
            mode=ResponseMode.DRAFT_ONLY,
            order_id=order_id,
            boundary_action=BoundaryAction.CUSTOMER_MESSAGE_DRAFT,
            draft_instruction="Draft a business-facing message, but do not send it.",
            context_needs=draft_needs,
            specialist_plan=_specialist_plan_for(request, order_id, draft_needs),
            safety_notes=["Nur Entwurf: nicht senden, keine Kundenmail/Partnernachricht auslösen."],
            structured_intent=structured,
        )

    if structured.boundary_action is BoundaryAction.DOCUMENT_UPLOAD:
        return EmployeeResponse(
            mode=ResponseMode.GUARDED_ACTION_REQUIRED,
            order_id=order_id,
            requires_guard=True,
            boundary_action=BoundaryAction.DOCUMENT_UPLOAD,
            guard_reason="Approval required before uploading documents or changing document state.",
            safety_notes=["Document upload blocked by guard; no file uploaded."],
            structured_intent=structured,
        )

    if structured.boundary_action is BoundaryAction.CRON_OUTBOUND:
        return EmployeeResponse(
            mode=ResponseMode.GUARDED_ACTION_REQUIRED,
            order_id=order_id,
            requires_guard=True,
            boundary_action=BoundaryAction.CRON_OUTBOUND,
            guard_reason="Approval required before creating recurring outbound notifications.",
            safety_notes=["Cron/outbound notification blocked by guard; no schedule created."],
            structured_intent=structured,
        )

    if structured.boundary_action is BoundaryAction.TMS_WRITE:
        return EmployeeResponse(
            mode=ResponseMode.GUARDED_ACTION_REQUIRED,
            order_id=order_id,
            requires_guard=True,
            boundary_action=BoundaryAction.TMS_WRITE,
            guard_reason="Approval required before any TMS write intent can be prepared or executed.",
            safety_notes=["TMS write blocked by default-deny guard; no write executed."],
            structured_intent=structured,
        )

    if structured.boundary_action is BoundaryAction.TEAMS_SEND:
        return EmployeeResponse(
            mode=ResponseMode.GUARDED_ACTION_REQUIRED,
            order_id=order_id,
            requires_guard=True,
            boundary_action=BoundaryAction.TEAMS_SEND,
            guard_reason="Approval required before sending anything to Teams.",
            draft_instruction="Prepare a draft Teams update only; do not send it.",
            safety_notes=["Teams send blocked; draft/review only."],
            structured_intent=structured,
        )

    needs = _needs_for_sources(structured.requested_sources)
    if order_id:
        # Case assist stays dynamic and agent-first: context needs drive the plan,
        # but the human-facing response can still be a normal Hermes answer.
        return EmployeeResponse(
            mode=ResponseMode.CASE_ASSIST,
            order_id=order_id,
            context_needs=needs or [ContextNeed.CASE_FOLDER],
            specialist_plan=_specialist_plan_for(request, order_id, needs or [ContextNeed.CASE_FOLDER]),
            safety_notes=["Read-only case assist; no Teams/TMS/customer side effect."],
            structured_intent=structured,
        )

    return EmployeeResponse(mode=ResponseMode.FREE_CHAT, structured_intent=structured)
