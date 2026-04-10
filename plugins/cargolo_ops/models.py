from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

ORDER_ID_RE = re.compile(r"\bAN-\d{4,6}\b", re.IGNORECASE)
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
        order_ids = list(payload.get("extracted_order_ids") or [])
        order_ids.extend(normalize_order_ids(payload.get("an"), subject, body_text, payload.get("mail_context")))

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
    next_best_action: str = "Review latest message"
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
    task: TaskProposal | None = None
    history_sync_count: int = 0
    message: str = ""
