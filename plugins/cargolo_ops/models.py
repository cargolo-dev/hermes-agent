from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

ORDER_ID_RE = re.compile(r"\b(?:AN|BU)-\d{4,6}\b", re.IGNORECASE)
INTERNAL_DOMAIN_RE = re.compile(r"@cargolo\.com\s*$", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_order_ids(*values: str | None) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        for match in ORDER_ID_RE.findall(value):
            norm = match.upper()
            if norm not in seen:
                found.append(norm)
                seen.add(norm)
    return found


class EmailClassification(str, Enum):
    quote_request = "quote_request"
    booking_request = "booking_request"
    document_submission = "document_submission"
    missing_documents = "missing_documents"
    tracking_request = "tracking_request"
    delay_or_exception = "delay_or_exception"
    complaint = "complaint"
    customs_or_compliance = "customs_or_compliance"
    internal_note = "internal_note"
    unknown = "unknown"


class AttachmentPayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    filename: str = ""
    mime_type: str = "application/octet-stream"
    content_base64: str | None = None
    storage_url: str | None = None
    sha256: str | None = None
    size: int | None = None

    @field_validator("size", mode="before")
    @classmethod
    def parse_size(cls, value: Any) -> int | None:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            raw = value.strip().replace(",", ".")
            m = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([kmgt]?b)?$", raw, re.IGNORECASE)
            if not m:
                digits = re.sub(r"[^0-9]", "", raw)
                return int(digits) if digits else None
            amount = float(m.group(1))
            unit = (m.group(2) or "b").lower()
            factor = {
                "b": 1,
                "kb": 1024,
                "mb": 1024**2,
                "gb": 1024**3,
                "tb": 1024**4,
            }.get(unit, 1)
            return int(amount * factor)
        return None


class IncomingMessagePayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    message_id: str = ""
    graph_id: str | None = None
    conversation_id: str | None = None
    subject: str = ""
    from_email: str = Field(default="", alias="from")
    from_name: str | None = None
    to: list[str] = Field(default_factory=list)
    cc: list[str] = Field(default_factory=list)
    received_at: str | None = None
    sent_at: str | None = None
    body_text: str = ""
    body_html: str | None = None
    body_preview: str | None = None
    has_attachments: bool = False
    attachment_count: int = 0
    attachments: list[AttachmentPayload] = Field(default_factory=list)
    detected_language: str | None = None
    headers: dict[str, Any] = Field(default_factory=dict)
    classification_hint: str | None = None
    customer_hint: str | None = None

    @property
    def is_internal(self) -> bool:
        return bool(self.from_email and INTERNAL_DOMAIN_RE.search(self.from_email))

    @property
    def dedupe_hash(self) -> str:
        raw = "\n".join([
            self.message_id or "",
            self.subject or "",
            self.from_email or "",
            self.received_at or "",
            self.body_text or "",
        ])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class IncomingEmailEvent(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    event_id: str
    received_at: str
    source: str = "email"
    message_id: str
    primary_message_id: str | None = None
    thread_id: str = ""
    subject: str = ""
    from_email: str = Field(default="", alias="from")
    to: list[str] = Field(default_factory=list)
    cc: list[str] = Field(default_factory=list)
    body_text: str = ""
    body_html: str | None = None
    attachments: list[AttachmentPayload] = Field(default_factory=list)
    extracted_order_ids: list[str] = Field(default_factory=list)
    detected_language: str | None = None
    headers: dict[str, Any] = Field(default_factory=dict)
    spam_score: float | None = None
    classification_hint: str | None = None
    customer_hint: str | None = None
    mailbox: str | None = None
    mail_context: str | None = None
    trigger_message_id: str | None = None
    trigger_graph_id: str | None = None
    trigger_conversation_id: str | None = None
    messages: list[IncomingMessagePayload] = Field(default_factory=list)
    raw_payload: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "IncomingEmailEvent":
        messages = [IncomingMessagePayload.model_validate(m) for m in payload.get("messages", [])]
        trigger_message_id = payload.get("trigger_message_id") or payload.get("message_id")
        trigger_graph_id = payload.get("trigger_graph_id")
        primary = None
        for candidate in messages:
            if trigger_message_id and candidate.message_id == trigger_message_id:
                primary = candidate
                break
            if trigger_graph_id and candidate.graph_id == trigger_graph_id:
                primary = candidate
                break
        if primary is None and messages:
            primary = messages[0]

        event_id = (
            payload.get("event_id")
            or payload.get("trigger_message_id")
            or payload.get("message_id")
            or (primary.message_id if primary else None)
            or hashlib.sha256(repr(payload).encode("utf-8")).hexdigest()[:16]
        )
        message_id = (
            payload.get("message_id")
            or payload.get("trigger_message_id")
            or (primary.message_id if primary else None)
            or event_id
        )
        thread_id = (
            payload.get("thread_id")
            or payload.get("trigger_conversation_id")
            or (primary.conversation_id if primary else "")
        )
        subject = payload.get("subject") or (primary.subject if primary else "") or ""
        body_text = payload.get("body_text") or (primary.body_text if primary else "") or ""
        body_html = payload.get("body_html") or (primary.body_html if primary else None)
        from_email = payload.get("from") or (primary.from_email if primary else "") or ""
        to = payload.get("to") or (primary.to if primary else []) or []
        cc = payload.get("cc") or (primary.cc if primary else []) or []
        attachments = payload.get("attachments") or (primary.attachments if primary else []) or []
        received_at = (
            payload.get("received_at")
            or (primary.received_at if primary else None)
            or utc_now_iso()
        )
        order_ids: list[str] = []
        order_ids.extend(normalize_order_ids(
            (primary.subject if primary else None),
            (primary.body_text if primary else None),
        ))
        order_ids.extend([str(item).upper() for item in (payload.get("extracted_order_ids") or []) if str(item or "").strip()])
        order_ids.extend(normalize_order_ids(payload.get("an"), payload.get("bu")))
        order_ids.extend(normalize_order_ids(subject, body_text, payload.get("mail_context")))

        return cls.model_validate({
            "event_id": str(event_id),
            "received_at": received_at,
            "source": payload.get("source", "email"),
            "message_id": str(message_id),
            "primary_message_id": primary.message_id if primary else None,
            "thread_id": thread_id,
            "subject": subject,
            "from": from_email,
            "to": to,
            "cc": cc,
            "body_text": body_text,
            "body_html": body_html,
            "attachments": attachments,
            "extracted_order_ids": list(dict.fromkeys([oid.upper() for oid in order_ids if oid])),
            "detected_language": payload.get("detected_language") or (primary.detected_language if primary else None),
            "headers": payload.get("headers", {}),
            "spam_score": payload.get("spam_score"),
            "classification_hint": payload.get("classification_hint") or (primary.classification_hint if primary else None),
            "customer_hint": payload.get("customer_hint") or (primary.customer_hint if primary else None),
            "mailbox": payload.get("mailbox"),
            "mail_context": payload.get("mail_context"),
            "trigger_message_id": payload.get("trigger_message_id"),
            "trigger_graph_id": payload.get("trigger_graph_id"),
            "trigger_conversation_id": payload.get("trigger_conversation_id"),
            "messages": [m.model_dump(by_alias=True) for m in messages],
            "raw_payload": payload,
        })

    @property
    def order_id(self) -> str | None:
        return self.extracted_order_ids[0] if self.extracted_order_ids else None

    @property
    def primary_message(self) -> IncomingMessagePayload:
        if self.messages:
            if self.primary_message_id:
                for message in self.messages:
                    if message.message_id == self.primary_message_id:
                        return message
            return self.messages[0]
        return IncomingMessagePayload(
            message_id=self.message_id,
            subject=self.subject,
            **{
                "from": self.from_email,
                "to": self.to,
                "cc": self.cc,
                "received_at": self.received_at,
                "body_text": self.body_text,
                "body_html": self.body_html,
                "attachments": self.attachments,
                "attachment_count": len(self.attachments),
                "has_attachments": bool(self.attachments),
                "detected_language": self.detected_language,
                "classification_hint": self.classification_hint,
                "customer_hint": self.customer_hint,
                "conversation_id": self.thread_id,
            },
        )


# ---------------------------------------------------------------------------
# TMS response models (Phase 2 — real read adapter)
# ---------------------------------------------------------------------------


class TMSShipmentSummary(BaseModel):
    """Single row from /admin/shipments_list → items[].

    Field names match the real Xano API response exactly.
    The UUID is returned as ``id`` (not ``shipment_uuid``).
    """
    model_config = ConfigDict(extra="allow")

    id: str = ""  # UUID of the shipment
    booking_id: int | None = None
    shipment_number: str = ""
    status: str = ""
    transport_mode: str = ""
    network: str = ""
    origin_city: str | None = None
    origin_country: str | None = None
    destination_city: str | None = None
    destination_country: str | None = None
    pickup_date: str | None = None
    delivery_date: str | None = None
    total_weight: float | None = None
    total_volume: float | None = None
    price_total: float | None = None
    created_at: str | None = None
    company_id: int | None = None
    company_name: str | None = None
    company_customer_number: str | None = None
    eta_main_carriage: str | None = None
    etd_main_carriage: str | None = None
    customer_reference: str | None = None
    provider_name: str | None = None
    has_customs_auth: bool | None = None


class TMSShipmentDetail(BaseModel):
    """Full detail from /admin/shipment_detail.

    Includes nested objects for sender, recipient, cargo, price,
    tracking_events, status_history, documents, billing_items,
    transport_legs, asr_offer/request, and more.
    """
    model_config = ConfigDict(extra="allow")

    id: str = ""  # UUID
    shipment_number: str = ""
    booking_id: str | None = None
    booking_number: str | None = None
    status: str = ""
    network: str = ""
    service_level: str | None = None
    pickup_date: str | None = None
    estimated_delivery_date: str | None = None
    latest_delivery_date: str | None = None
    actual_delivery_date: str | None = None
    tracking_number: str | None = None
    consignment_number: str | None = None
    mware_number: str | None = None
    route_origin_city: str | None = None
    route_origin_country: str | None = None
    route_destination_city: str | None = None
    route_destination_country: str | None = None
    company_id: int | None = None
    company_name: str | None = None
    company_customer_number: str | None = None
    company_email: str | None = None
    customer_reference: str | None = None
    sender: dict[str, Any] | None = None
    recipient: dict[str, Any] | None = None
    cargo: list[dict[str, Any]] = Field(default_factory=list)
    price: dict[str, Any] | None = None
    tracking_events: list[dict[str, Any]] = Field(default_factory=list)
    status_history: list[dict[str, Any]] = Field(default_factory=list)
    documents: list[dict[str, Any]] = Field(default_factory=list)
    transport_legs: list[dict[str, Any]] = Field(default_factory=list)
    billing_items: list[dict[str, Any]] = Field(default_factory=list)
    container_number: str | None = None
    container_type: str | None = None
    seal_number: str | None = None
    hbl_number: str | None = None
    mbl_number: str | None = None
    hawb_number: str | None = None
    mawb_number: str | None = None
    bl_number: str | None = None
    incoterms: str | None = None
    incoterms_location: str | None = None
    pol_code: str | None = None
    pol_name: str | None = None
    pod_code: str | None = None
    pod_name: str | None = None
    cargo_ready_date: str | None = None
    customs_status: str | None = None
    customs_reference: str | None = None
    internal_notes: list[dict[str, Any]] = Field(default_factory=list)
    billing_status: str | None = None
    billing_total_vk: float | None = None
    billing_total_ek: float | None = None
    billing_margin: float | None = None
    billing_margin_percent: float | None = None
    asr_request_id: int | None = None
    asr_offer_id: int | None = None
    asr_offer: dict[str, Any] | None = None
    asr_request: dict[str, Any] | None = None
    departure_airport: str | None = None
    arrival_airport: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class TMSBillingItem(BaseModel):
    """Single billing item from /admin/shipment_billing_items → items[].

    VK = Verkaufspreis (sales price), EK = Einkaufspreis (cost price).
    """
    model_config = ConfigDict(extra="allow")

    uuid: str | None = None
    sort_order: int | None = None
    name: str = ""
    hint: str | None = None
    quantity: float | None = None
    unit: str | None = None
    vk_price: float | None = None
    ek_price: float | None = None
    ek_basis: str | None = None
    is_adjustment: bool | None = None
    adjustment_reason: str | None = None
    source: str | None = None


class TMSBillingSums(BaseModel):
    """Billing sums from /admin/shipment_billing_items → sums."""
    model_config = ConfigDict(extra="allow")

    total_vk: float = 0.0
    total_ek: float = 0.0
    margin: float = 0.0
    margin_percent: float = 0.0


class TMSSnapshot(BaseModel):
    """Complete TMS snapshot for a shipment — stored in tms_snapshot.json."""
    model_config = ConfigDict(extra="allow")

    order_id: str = ""
    shipment_uuid: str | None = None
    shipment_number: str = ""
    source: str = "mock"  # "mock" or "live"
    status: str = "unknown"
    detail: dict[str, Any] = Field(default_factory=dict)
    billing_items: list[dict[str, Any]] = Field(default_factory=list)
    billing_sums: dict[str, Any] = Field(default_factory=dict)
    stats: dict[str, Any] = Field(default_factory=dict)
    customer_rules: dict[str, Any] = Field(default_factory=dict)
    open_tasks: list[dict[str, Any]] = Field(default_factory=list)
    fetched_at: str = ""
    warnings: list[str] = Field(default_factory=list)


class DeltaAnalysis(BaseModel):
    new_information: list[str] = Field(default_factory=list)
    confirmations: list[str] = Field(default_factory=list)
    contradictions: list[str] = Field(default_factory=list)
    resolved_missing_information: list[str] = Field(default_factory=list)
    remaining_missing_information: list[str] = Field(default_factory=list)
    customer_reply_needed: bool = False
    internal_task_needed: bool = False
    escalation_needed: bool = False
    reasoning: list[str] = Field(default_factory=list)


class EntitiesSnapshot(BaseModel):
    container_numbers: list[str] = Field(default_factory=list)
    reference_numbers: list[str] = Field(default_factory=list)
    stations_ports_airports: list[str] = Field(default_factory=list)
    parties: list[str] = Field(default_factory=list)
    deadlines: list[str] = Field(default_factory=list)
    document_types: list[str] = Field(default_factory=list)
    incoterms: list[str] = Field(default_factory=list)
    transport_mode_candidates: list[str] = Field(default_factory=list)


class CaseState(BaseModel):
    order_id: str
    mode: str = "unknown"
    customer_name: str | None = None
    customer_reference: str | None = None
    current_status: str = "new"
    last_email_at: str | None = None
    last_customer_message_at: str | None = None
    last_internal_action_at: str | None = None
    missing_information: list[str] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    documents_expected: list[str] = Field(default_factory=list)
    documents_received: list[str] = Field(default_factory=list)
    latest_summary: str = ""
    reply_recommended: bool = False
    task_recommended: bool = False
    task_reason: str = ""
    next_best_action: str = "Mail- und TMS-Stand prüfen"
    tms_order_id: str | None = None
    tms_last_sync_at: str | None = None


class TaskProposal(BaseModel):
    order_id: str
    title: str
    description: str
    priority: str = "medium"
    due_at: str | None = None
    task_type: str = "follow_up"
    created: bool = False
    external_task_id: str | None = None


class ASRSpecialistOutput(BaseModel):
    model_config = ConfigDict(extra="allow")

    role: str
    summary: str = ""
    confidence: str = "medium"
    files_used: list[str] = Field(default_factory=list)


class ASRAnalysisRisk(BaseModel):
    model_config = ConfigDict(extra="allow")

    code: str
    severity: str
    reason: str
    evidence: list[str] = Field(default_factory=list)


class ASRAnalysisAction(BaseModel):
    model_config = ConfigDict(extra="allow")

    action: str
    urgency: str
    owner_role: str
    reason: str
    blocking: bool = False


class ASRAnalysisReplyGuidance(BaseModel):
    model_config = ConfigDict(extra="allow")

    reply_recommended: bool = False
    draft_status: str = "needs_revision"
    must_include: list[str] = Field(default_factory=list)
    must_avoid: list[str] = Field(default_factory=list)
    missing_for_reply: list[str] = Field(default_factory=list)
    tone_guidance: str = ""
    revised_internal_reply_brief: str = ""


class ASRAnalysisConfidence(BaseModel):
    model_config = ConfigDict(extra="allow")

    overall: str = "medium"
    why: list[str] = Field(default_factory=list)


class ASRAnalysisBrief(BaseModel):
    model_config = ConfigDict(extra="allow")

    analysis_version: str
    order_id: str
    deterministic_status: str
    case_initialized: bool = False
    message_classification: str = "unknown"
    priority: str = "medium"
    ops_summary: str = ""
    customer_reply_guidance: ASRAnalysisReplyGuidance = Field(default_factory=ASRAnalysisReplyGuidance)
    internal_actions: list[ASRAnalysisAction] = Field(default_factory=list)
    risk_flags: list[ASRAnalysisRisk] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)
    confidence: ASRAnalysisConfidence = Field(default_factory=ASRAnalysisConfidence)
    provenance: dict[str, Any] = Field(default_factory=dict)


class ProcessingResult(BaseModel):
    status: str
    order_id: str | None = None
    case_root: str | None = None
    classification: EmailClassification | None = None
    initialized: bool = False
    duplicate: bool = False
    review_required: bool = False
    timeline_entry: str | None = None
    draft_path: str | None = None
    document_registry_path: str | None = None
    task: TaskProposal | None = None
    history_sync_count: int = 0
    history_sync_status: str | None = None
    history_sync_error: str | None = None
    latest_subject: str | None = None
    latest_sender: str | None = None
    attachment_count: int = 0
    attachment_filenames: list[str] = Field(default_factory=list)
    pending_updates_path: str | None = None
    applied_updates_path: str | None = None
    case_report_path: str | None = None
    pending_action_summary: dict[str, int] = Field(default_factory=dict)
    applied_action_summary: dict[str, int] = Field(default_factory=dict)
    applied_action_targets: list[str] = Field(default_factory=list)
    failed_action_targets: list[str] = Field(default_factory=list)
    skipped_action_targets: list[str] = Field(default_factory=list)
    applied_action_details: list[str] = Field(default_factory=list)
    failed_action_details: list[str] = Field(default_factory=list)
    skipped_action_details: list[str] = Field(default_factory=list)
    analysis_status: str | None = None
    analysis_brief_path: str | None = None
    analysis_priority: str | None = None
    analysis_summary: str | None = None
    internal_note_status: str | None = None
    internal_note_preview: str | None = None
    internal_note_error: str | None = None
    suppress_delivery: bool = False
    message: str = ""
