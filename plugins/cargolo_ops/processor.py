from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .adapters import MockTMSAdapter, build_mail_history_client_from_env
from .models import (
    CaseState,
    DeltaAnalysis,
    EmailClassification,
    EntitiesSnapshot,
    IncomingEmailEvent,
    IncomingMessagePayload,
    ProcessingResult,
    TaskProposal,
    normalize_order_ids,
    utc_now_iso,
)
from .storage import CaseStore

CLASSIFICATION_KEYWORDS: list[tuple[EmailClassification, tuple[str, ...]]] = [
    (EmailClassification.quote_request, ("quote", "rate request", "offer", "preis")),
    (EmailClassification.booking_request, ("booking", "book", "buchung")),
    (EmailClassification.document_submission, ("attached", "anbei", "invoice", "awb", "bl", "pod", "packing list")),
    (EmailClassification.missing_documents, ("missing document", "please send", "fehlt", "missing docs")),
    (EmailClassification.tracking_request, ("eta", "tracking", "status", "where is", "ankunft")),
    (EmailClassification.delay_or_exception, ("delay", "rolled", "exception", "urgent", "problem", "stuck", "customs hold")),
    (EmailClassification.complaint, ("complaint", "damage", "claim", "unhappy", "beschwerde")),
    (EmailClassification.customs_or_compliance, ("customs", "compliance", "zoll", "mrn", "hs code")),
]

DOCUMENT_HINTS = {
    "invoice": "commercial_invoice",
    "packing": "packing_list",
    "awb": "air_waybill",
    "bill of lading": "bill_of_lading",
    "bl": "bill_of_lading",
    "pod": "proof_of_delivery",
    "customs": "customs_document",
    "mrn": "mrn",
}

MODE_KEYWORDS = {
    "air": ("air", "awb", "iata", "airport", "flight"),
    "ocean": ("ocean", "sea", "bl", "bill of lading", "port", "vessel"),
    "rail": ("rail", "train", "terminal", "wagon"),
}

INCOTERMS = ("EXW", "FCA", "FOB", "CIF", "DAP", "DDP", "CPT", "CIP")


def _body_blob(message: IncomingMessagePayload) -> str:
    return "\n".join(filter(None, [message.subject, message.body_preview or "", message.body_text]))


def classify_email(message: IncomingMessagePayload) -> EmailClassification:
    if message.is_internal:
        return EmailClassification.internal_note
    text = _body_blob(message).lower()
    matches = [
        classification
        for classification, keywords in CLASSIFICATION_KEYWORDS
        if any(keyword in text for keyword in keywords)
    ]
    if not matches:
        return EmailClassification.unknown
    for preferred in (
        EmailClassification.complaint,
        EmailClassification.delay_or_exception,
        EmailClassification.customs_or_compliance,
        EmailClassification.missing_documents,
        EmailClassification.booking_request,
        EmailClassification.quote_request,
        EmailClassification.tracking_request,
        EmailClassification.document_submission,
    ):
        if preferred in matches:
            return preferred
    return matches[0]


def detect_mode(text: str) -> str:
    lowered = text.lower()
    for mode, keywords in MODE_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return mode
    return "unknown"


def extract_entities(event: IncomingEmailEvent) -> EntitiesSnapshot:
    message = event.primary_message
    text = _body_blob(message)
    lowered = text.lower()
    refs = normalize_order_ids(text)

    document_types = sorted({
        label for needle, label in DOCUMENT_HINTS.items() if needle in lowered
    })
    transport_modes = [mode for mode, keywords in MODE_KEYWORDS.items() if any(keyword in lowered for keyword in keywords)]

    ports = []
    for token in text.replace("\n", " ").split():
        if token.isupper() and len(token) in {3, 5} and token.isalpha():
            ports.append(token)

    incoterms = [term for term in INCOTERMS if term in text.upper()]
    parties = []
    if message.from_name:
        parties.append(message.from_name)
    if message.from_email:
        parties.append(message.from_email)

    deadlines = []
    for marker in ("today", "tomorrow", "morgen", "heute", "asap"):
        if marker in lowered:
            deadlines.append(marker)

    return EntitiesSnapshot(
        container_numbers=[],
        reference_numbers=sorted(set(refs)),
        stations_ports_airports=sorted(set(ports))[:10],
        parties=sorted(set(parties)),
        deadlines=sorted(set(deadlines)),
        document_types=document_types,
        incoterms=sorted(set(incoterms)),
        transport_mode_candidates=sorted(set(transport_modes)),
    )


def determine_missing_information(classification: EmailClassification, entities: EntitiesSnapshot, message: IncomingMessagePayload) -> list[str]:
    missing: list[str] = []
    body = _body_blob(message).lower()
    if classification in {EmailClassification.quote_request, EmailClassification.booking_request}:
        if not entities.transport_mode_candidates:
            missing.append("transport_mode")
        if not entities.reference_numbers:
            missing.append("order_reference")
        if not any(term in body for term in ("origin", "pickup", "abholung")):
            missing.append("origin")
        if not any(term in body for term in ("destination", "delivery", "zustellung")):
            missing.append("destination")
    if classification == EmailClassification.missing_documents:
        missing.append("requested_documents")
    if classification == EmailClassification.customs_or_compliance and "mrn" not in body:
        missing.append("mrn")
    return sorted(set(missing))


def build_delta(
    *,
    state: CaseState,
    entities_before: EntitiesSnapshot,
    entities_after: EntitiesSnapshot,
    classification: EmailClassification,
    message: IncomingMessagePayload,
    tms_snapshot: dict[str, Any],
) -> DeltaAnalysis:
    previous_docs = set(state.documents_received)
    current_docs = set(entities_after.document_types)
    new_docs = sorted(current_docs - previous_docs)
    resolved_missing = sorted(set(state.missing_information) - set(determine_missing_information(classification, entities_after, message)))
    remaining_missing = determine_missing_information(classification, entities_after, message)

    contradictions: list[str] = []
    text = _body_blob(message).lower()
    if tms_snapshot.get("status") and any(term in text for term in ("delay", "late", "problem", "stuck")):
        if str(tms_snapshot.get("status", "")).lower() in {"on_track", "delivered", "ok"}:
            contradictions.append("email signals delay/issue while TMS snapshot looks healthy")

    confirmations: list[str] = []
    for ref in entities_after.reference_numbers:
        if ref in entities_before.reference_numbers:
            confirmations.append(f"reference {ref} already known")

    customer_reply_needed = not message.is_internal and classification not in {EmailClassification.document_submission, EmailClassification.internal_note}
    internal_task_needed = bool(contradictions) or classification in {
        EmailClassification.delay_or_exception,
        EmailClassification.complaint,
        EmailClassification.customs_or_compliance,
        EmailClassification.missing_documents,
    }
    escalation_needed = classification in {EmailClassification.complaint, EmailClassification.delay_or_exception} or bool(contradictions)

    reasoning = [
        f"classification={classification.value}",
        f"new_docs={len(new_docs)}",
        f"remaining_missing={len(remaining_missing)}",
        f"contradictions={len(contradictions)}",
    ]

    return DeltaAnalysis(
        new_information=[f"new document type: {doc}" for doc in new_docs],
        confirmations=sorted(set(confirmations)),
        contradictions=contradictions,
        resolved_missing_information=resolved_missing,
        remaining_missing_information=remaining_missing,
        customer_reply_needed=customer_reply_needed,
        internal_task_needed=internal_task_needed,
        escalation_needed=escalation_needed,
        reasoning=reasoning,
    )


def _merge_entities(old: EntitiesSnapshot, new: EntitiesSnapshot) -> EntitiesSnapshot:
    merged = {}
    for field_name in EntitiesSnapshot.model_fields:
        merged[field_name] = sorted(set(getattr(old, field_name)) | set(getattr(new, field_name)))
    return EntitiesSnapshot(**merged)


def _summary_for(message: IncomingMessagePayload, classification: EmailClassification, delta: DeltaAnalysis) -> str:
    parts = [f"{classification.value} from {message.from_email or 'unknown sender'}"]
    if delta.new_information:
        parts.append(f"new: {', '.join(delta.new_information[:3])}")
    if delta.contradictions:
        parts.append(f"contradictions: {', '.join(delta.contradictions[:2])}")
    if delta.remaining_missing_information:
        parts.append(f"missing: {', '.join(delta.remaining_missing_information[:3])}")
    return " | ".join(parts)


def _draft_markdown(order_id: str, classification: EmailClassification, delta: DeltaAnalysis, state: CaseState, message: IncomingMessagePayload) -> str:
    return (
        f"# Draft reply for {order_id}\n\n"
        f"- Classification: {classification.value}\n"
        f"- Sender: {message.from_email}\n"
        f"- Subject: {message.subject}\n\n"
        "## Suggested customer-facing response (DO NOT SEND AUTOMATICALLY)\n\n"
        "Hello,\n\n"
        f"we reviewed your message regarding {order_id}. "
        f"Current internal assessment: {state.current_status}.\n\n"
        + (f"We still need: {', '.join(delta.remaining_missing_information)}.\n\n" if delta.remaining_missing_information else "")
        + "Our operations team will confirm the next step after internal review.\n\nBest regards\nCARGOLO ASR\n"
    )


def _proposed_task(order_id: str, classification: EmailClassification, delta: DeltaAnalysis) -> TaskProposal | None:
    if not delta.internal_task_needed:
        return None
    due_at = (datetime.now(timezone.utc) + timedelta(hours=4)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    priority = "high" if delta.escalation_needed else "medium"
    return TaskProposal(
        order_id=order_id,
        title=f"Review {classification.value} for {order_id}",
        description="; ".join(delta.contradictions or delta.remaining_missing_information or delta.reasoning),
        priority=priority,
        due_at=due_at,
        task_type="exception" if delta.escalation_needed else "follow_up",
    )


def _decode_attachment(content_base64: str | None) -> bytes:
    if not content_base64:
        return b""
    return base64.b64decode(content_base64.encode("utf-8"), validate=True)


def _sync_mail_history(
    store: CaseStore,
    order_id: str,
    state: CaseState,
    mailbox: str | None,
    *,
    exclude_message_ids: set[str] | None = None,
) -> int:
    client = build_mail_history_client_from_env()
    if client is None:
        return 0
    since = state.last_email_at
    result = client.fetch_history(
        order_id,
        first_sync=not bool(since),
        since=since,
        mailbox=mailbox or "asr@cargolo.com",
        include_attachments=True,
        include_html=False,
    )
    count = 0
    exclude_message_ids = exclude_message_ids or set()
    for row in result.get("messages", []):
        message = IncomingMessagePayload.model_validate(row)
        if message.message_id in exclude_message_ids:
            continue
        if store.has_message(order_id, message.message_id, message.dedupe_hash):
            continue
        raw_path = store.store_raw_email(order_id, message, row, prefix="history")
        store.append_email_index(order_id, {
            "message_id": message.message_id,
            "thread_id": message.conversation_id,
            "subject": message.subject,
            "sender": message.from_email,
            "received_at": message.received_at,
            "stored_paths": [str(raw_path)],
            "classification": "history_sync",
            "linked_order_id": order_id,
            "dedupe_hash": message.dedupe_hash,
        })
        count += 1
    return count


def process_email_event(
    payload: dict[str, Any],
    *,
    storage_root: Path | None = None,
    create_task: bool = False,
    refresh_history: bool = True,
) -> ProcessingResult:
    event = IncomingEmailEvent.from_payload(payload)
    store = CaseStore(storage_root)
    message = event.primary_message

    if not event.order_id:
        review_path = store.save_unassigned_event(event.raw_payload, "No AN found in payload")
        return ProcessingResult(
            status="review_queue",
            review_required=True,
            message=f"No order id found. Saved for review at {review_path}",
        )

    order_id = event.order_id
    case_preexisting = store.order_path(order_id).exists()
    case_root = store.ensure_case(order_id)
    if store.has_message(order_id, event.message_id, message.dedupe_hash):
        return ProcessingResult(
            status="duplicate",
            order_id=order_id,
            case_root=str(case_root),
            initialized=not case_preexisting,
            duplicate=True,
            message="Event already processed for this case",
        )

    raw_path = store.store_raw_email(order_id, message, event.model_dump(by_alias=True), prefix="ingest")

    state = store.load_case_state(order_id)
    entities_before = store.load_entities(order_id)
    tms = MockTMSAdapter(store.root)
    tms_snapshot = tms.snapshot_bundle(order_id, event.customer_hint)

    history_count = 0
    if refresh_history:
        try:
            history_count = _sync_mail_history(
                store,
                order_id,
                state,
                event.mailbox,
                exclude_message_ids={message.message_id},
            )
        except Exception:
            history_count = 0

    history_rows = store.list_email_index(order_id)
    entities_new = extract_entities(event)
    entities_after = _merge_entities(entities_before, entities_new)
    classification = classify_email(message)
    delta = build_delta(
        state=state,
        entities_before=entities_before,
        entities_after=entities_after,
        classification=classification,
        message=message,
        tms_snapshot=tms_snapshot,
    )

    attachment_paths: list[str] = []
    for attachment in message.attachments:
        content = _decode_attachment(attachment.content_base64)
        if content:
            attachment_paths.append(str(store.store_attachment(order_id, attachment.filename or "attachment.bin", content)))

    state.mode = entities_after.transport_mode_candidates[0] if entities_after.transport_mode_candidates else detect_mode(_body_blob(message))
    state.current_status = classification.value
    state.last_email_at = message.received_at or event.received_at
    if message.is_internal:
        state.last_internal_action_at = utc_now_iso()
    else:
        state.last_customer_message_at = message.received_at or event.received_at
    state.documents_received = sorted(set(state.documents_received) | set(entities_after.document_types))
    state.missing_information = delta.remaining_missing_information
    state.risks = sorted(set(state.risks) | set(delta.contradictions))
    state.reply_recommended = delta.customer_reply_needed
    state.task_recommended = delta.internal_task_needed
    state.task_reason = "; ".join(delta.contradictions or delta.remaining_missing_information or delta.reasoning)
    state.next_best_action = (
        "Escalate to ASR ops lead" if delta.escalation_needed else
        "Create follow-up task" if delta.internal_task_needed else
        "Review draft and respond manually"
    )
    state.tms_order_id = tms_snapshot.get("order_id")
    state.tms_last_sync_at = utc_now_iso()
    state.latest_summary = _summary_for(message, classification, delta)

    normalized = {
        "plain_text": message.body_text,
        "important_fields": {
            "subject": message.subject,
            "sender": message.from_email,
            "received_at": message.received_at,
            "order_id": order_id,
        },
        "intent": classification.value,
        "detected_entities": entities_new.model_dump(),
        "delta_to_case": delta.model_dump(),
        "history_sync_count": history_count,
        "history_email_count_before_current": len(history_rows),
        "case_initialized": not case_preexisting,
    }
    normalized_path = store.store_normalized_email(order_id, message, normalized)
    draft_path = store.store_draft(order_id, message, _draft_markdown(order_id, classification, delta, state, message))
    entities_path = store.save_entities(order_id, entities_after)
    state_path = store.save_case_state(order_id, state)
    tms_path = store.save_tms_snapshot(order_id, tms_snapshot)
    store.append_email_index(order_id, {
        "message_id": message.message_id,
        "thread_id": message.conversation_id,
        "subject": message.subject,
        "sender": message.from_email,
        "received_at": message.received_at,
        "stored_paths": [str(raw_path), str(normalized_path), str(draft_path), *attachment_paths],
        "classification": classification.value,
        "linked_order_id": order_id,
        "dedupe_hash": message.dedupe_hash,
    })

    task_proposal = _proposed_task(order_id, classification, delta)
    if task_proposal and create_task:
        created = tms.create_task(
            order_id=order_id,
            title=task_proposal.title,
            description=task_proposal.description,
            priority=task_proposal.priority,
            due_at=task_proposal.due_at,
            task_type=task_proposal.task_type,
        )
        task_proposal.created = True
        task_proposal.external_task_id = created.get("task_id")
    if task_proposal:
        store.append_task_log(order_id, task_proposal)

    timeline_path = store.append_timeline(
        order_id,
        heading=f"{classification.value} / {message.subject or 'no subject'}",
        summary=state.latest_summary,
        delta="; ".join(delta.new_information + delta.contradictions + delta.remaining_missing_information) or "no major changes",
        next_step=state.next_best_action,
    )
    store.append_audit(
        order_id,
        action="process_email_event",
        result="ok",
        files=[str(raw_path), str(normalized_path), str(draft_path), str(state_path), str(entities_path), str(tms_path), str(timeline_path), *attachment_paths],
        extra={
            "classification": classification.value,
            "history_sync_count": history_count,
            "history_email_count_before_current": len(history_rows),
            "initialized": not case_preexisting,
            "task_created": bool(task_proposal and task_proposal.created),
        },
    )

    return ProcessingResult(
        status="processed",
        order_id=order_id,
        case_root=str(case_root),
        classification=classification,
        initialized=not case_preexisting,
        draft_path=str(draft_path),
        task=task_proposal,
        history_sync_count=history_count,
        timeline_entry=state.latest_summary,
        message="Processed email event successfully",
    )
