from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .adapters import MockTMSAdapter, build_mail_history_client_from_env, build_tms_client_from_env
from .analysis import run_postprocess_subagent_analysis
from .tms_provider import build_tms_provider_from_env, build_tms_write_provider_from_env
from .writeback_actions import apply_pending_tms_action, supports_field_update_target
from .document_analysis import analyze_case_documents
from .models import (
    CaseState,
    DeltaAnalysis,
    EmailClassification,
    EntitiesSnapshot,
    IncomingEmailEvent,
    IncomingMessagePayload,
    ProcessingResult,
    TMSSnapshot,
    TaskProposal,
    normalize_order_ids,
    utc_now_iso,
)
from .storage import CaseStore

import logging

logger = logging.getLogger(__name__)

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
    "air waybill": "air_waybill",
    "bill of lading": "bill_of_lading",
    "bl": "bill_of_lading",
    "pod": "proof_of_delivery",
    "proof of delivery": "proof_of_delivery",
    "customs": "customs_document",
    "mrn": "mrn",
}

MODE_KEYWORDS = {
    "air": ("air", "awb", "iata", "airport", "flight"),
    "ocean": ("ocean", "sea", "bl", "bill of lading", "port", "vessel"),
    "rail": ("rail", "train", "terminal", "wagon"),
}

INCOTERMS = ("EXW", "FCA", "FOB", "CIF", "DAP", "DDP", "CPT", "CIP")


def _slugify_document_label(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")
    return slug or "document"


def _detect_document_types(*values: str | None) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for value in values:
        lowered = str(value or "").lower()
        if not lowered:
            continue
        for needle, label in DOCUMENT_HINTS.items():
            if needle in lowered and label not in seen:
                seen.add(label)
                found.append(label)
    return found


def _extract_tms_document_records(
    tms_snapshot: dict[str, Any],
    tms_document_requirements: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    requirement_payload = tms_document_requirements if isinstance(tms_document_requirements, dict) else {}
    requirement_documents = requirement_payload.get("documents") if isinstance(requirement_payload.get("documents"), list) else None
    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    detail_documents = detail.get("documents") if isinstance(detail, dict) else None
    documents = requirement_documents if isinstance(requirement_documents, list) else detail_documents
    if not isinstance(documents, list):
        return []

    expected_types_from_requirements = {
        str(value or "").strip()
        for value in (requirement_payload.get("expected_types") or [])
        if str(value or "").strip()
    }

    records: list[dict[str, Any]] = []
    for idx, row in enumerate(documents, start=1):
        if not isinstance(row, dict):
            continue
        label = (
            row.get("document_type")
            or row.get("type")
            or row.get("title")
            or row.get("name")
            or row.get("filename")
            or f"document_{idx}"
        )
        detected = _detect_document_types(label, row.get("description"), row.get("filename"))
        normalized_type = detected[0] if detected else _slugify_document_label(str(label))
        records.append({
            "tms_document_id": row.get("tms_document_id") or row.get("id") or row.get("uuid") or f"tms-doc-{idx}",
            "label": label,
            "document_type": normalized_type,
            "required": bool(
                row.get("required")
                or row.get("is_required")
                or row.get("mandatory")
                or normalized_type in expected_types_from_requirements
            ),
            "status": row.get("status") or row.get("availability") or row.get("state"),
            "filename": row.get("filename"),
            "url": row.get("url") or row.get("download_url"),
        })
    return records


def _build_document_registry(
    *,
    prior_registry: dict[str, Any],
    message: IncomingMessagePayload,
    attachment_records: list[dict[str, Any]],
    tms_snapshot: dict[str, Any],
    tms_document_requirements: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prior_received = [row for row in prior_registry.get("received_documents", []) if isinstance(row, dict)]
    prior_tms = [row for row in prior_registry.get("tms_documents", []) if isinstance(row, dict)]

    requirement_payload = tms_document_requirements if isinstance(tms_document_requirements, dict) else {}
    requirement_expected_types = sorted({
        str(value or "").strip()
        for value in (requirement_payload.get("expected_types") or [])
        if str(value or "").strip()
    })

    received_map: dict[str, dict[str, Any]] = {}
    for row in prior_received + attachment_records:
        key = str(row.get("sha256") or row.get("stored_path") or row.get("filename") or len(received_map))
        received_map[key] = row

    tms_records = _extract_tms_document_records(tms_snapshot, requirement_payload)
    tms_map: dict[str, dict[str, Any]] = {}
    for row in prior_tms + tms_records:
        key = str(row.get("tms_document_id") or row.get("document_type") or row.get("label") or len(tms_map))
        tms_map[key] = row

    received_documents = sorted(
        received_map.values(),
        key=lambda row: (str(row.get("received_at") or ""), str(row.get("filename") or ""), str(row.get("stored_path") or "")),
    )
    tms_documents = sorted(
        tms_map.values(),
        key=lambda row: (str(row.get("document_type") or ""), str(row.get("label") or "")),
    )
    received_types = sorted({
        doc_type
        for row in received_documents
        for doc_type in row.get("detected_types", [])
        if doc_type
    })
    required_types = sorted({
        str(row.get("document_type") or "")
        for row in tms_documents
        if row.get("required") and row.get("document_type")
    })
    expected_types = requirement_expected_types or required_types or sorted({
        str(row.get("document_type") or "")
        for row in tms_documents
        if row.get("document_type")
    })
    missing_types = sorted(set(expected_types) - set(received_types))

    return {
        "registry_version": 1,
        "updated_at": utc_now_iso(),
        "message_id": message.message_id,
        "received_documents": received_documents,
        "tms_documents": tms_documents,
        "received_types": received_types,
        "expected_types": expected_types,
        "missing_types": missing_types,
        "tms_expected_types": requirement_expected_types,
        "tms_document_requirements": requirement_payload,
    }


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


def _derive_action_guidance_from_signals(
    *,
    mode: str,
    contradictions: list[str] | None = None,
    missing_information: list[str] | None = None,
    open_questions: list[str] | None = None,
    analysis_flags: list[str] | None = None,
    escalation_needed: bool = False,
) -> tuple[bool, str, str]:
    contradictions = [str(item).strip() for item in (contradictions or []) if str(item).strip()]
    missing_information = [str(item).strip() for item in (missing_information or []) if str(item).strip()]
    open_questions = [str(item).strip() for item in (open_questions or []) if str(item).strip()]
    analysis_flags = [str(item).strip() for item in (analysis_flags or []) if str(item).strip()]
    mode = str(mode or "").strip().lower()

    if escalation_needed or contradictions:
        return True, "contradictions_or_escalation", "TMS-Status und Mail-/Prozesswiderspruch fachlich prüfen"

    missing_doc_types = sorted({
        item.split(":", 1)[1]
        for item in missing_information
        if item.startswith("document:") and ":" in item
    })

    if mode == "air" and "air_waybill" in missing_doc_types:
        return True, "missing_documents:air_waybill", "AWB/Flugdaten gegen Mail- und Carrier-Stand prüfen"
    if {"commercial_invoice", "packing_list"}.issubset(set(missing_doc_types)):
        return True, "missing_documents:commercial_invoice,packing_list", "Commercial Invoice / Packing List gegen Mail- und TMS-Stand prüfen"
    if "commercial_invoice" in missing_doc_types:
        return True, "missing_documents:commercial_invoice", "Commercial Invoice Verfügbarkeit gegen Mail- und TMS-Stand prüfen"
    if "packing_list" in missing_doc_types:
        return True, "missing_documents:packing_list", "Packing List Bedarf gegen Mail- und TMS-Stand prüfen"
    if "customs_document" in missing_doc_types:
        return True, "missing_documents:customs_document", "Zolldokumente gegen Prozessstand prüfen"

    if analysis_flags:
        first_flag = analysis_flags[0].split(":", 1)[-1]
        return True, f"analysis_flags:{first_flag}", "Analysehinweise fachlich prüfen und operativ einordnen"
    if open_questions:
        return True, "open_questions_present", "Offene Punkte aus Mail-/Dokumentlage fachlich prüfen"

    return False, "", "Auf erste echte ASR-Mail warten"


def _derive_action_guidance_from_pending_actions(
    pending_plan: dict[str, Any],
    *,
    fallback_next_step: str,
    fallback_reason: str,
) -> tuple[bool, str, str]:
    pending_actions = [row for row in (pending_plan.get("pending_actions") or []) if isinstance(row, dict)]
    if not pending_actions:
        return bool(fallback_reason), fallback_reason, fallback_next_step

    targets = [str(row.get("target") or "").strip() for row in pending_actions]
    if any(target in {
        "shipment.review.status_inconsistent_with_analysis",
        "shipment.review.status_inconsistent_with_recent_mail_activity",
        "shipment.review.actual_delivery_date_placeholder",
    } for target in targets):
        return True, "review:shipment_status_validation", "TMS-Status/Meilensteine fachlich prüfen und bei Bedarf korrigieren"
    if "documents.review.air_waybill_missing_but_flight_context_present" in targets:
        return True, "review:air_waybill_context", "AWB/Flugdaten gegen Mail- und Carrier-Stand prüfen"
    if "documents.review.packing_list_expected_but_mail_evidence_unclear" in targets:
        return True, "review:packing_list_context", "Packing List Bedarf gegen Mail- und TMS-Stand prüfen"
    if "documents.review.commercial_invoice_expected_but_mail_evidence_unclear" in targets:
        return True, "review:commercial_invoice_context", "Commercial Invoice gegen Mail- und TMS-Stand prüfen"
    if "documents.review.bill_of_lading_missing_but_ocean_context_present" in targets:
        return True, "review:bill_of_lading_context", "Bill of Lading gegen Mail- und Carrier-Stand prüfen"

    missing_doc_targets = [target for target in targets if target.startswith("documents.") and ".review." not in target]
    if {"documents.commercial_invoice", "documents.packing_list"}.issubset(set(missing_doc_targets)):
        return True, "missing_documents:commercial_invoice,packing_list", "Commercial Invoice / Packing List gegen Mail- und TMS-Stand prüfen"
    if "documents.commercial_invoice" in missing_doc_targets:
        return True, "missing_documents:commercial_invoice", "Commercial Invoice Verfügbarkeit gegen Mail- und TMS-Stand prüfen"
    if "documents.packing_list" in missing_doc_targets:
        return True, "missing_documents:packing_list", "Packing List Bedarf gegen Mail- und TMS-Stand prüfen"
    if "documents.customs_document" in missing_doc_targets:
        return True, "missing_documents:customs_document", "Zolldokumente gegen Prozessstand prüfen"

    top_review = next((row for row in pending_actions if str(row.get("action_status") or "") == "review"), None)
    if top_review:
        return True, f"review:{str(top_review.get('target') or 'generic').strip()}", str(top_review.get("proposed_decision") or fallback_next_step or "Operativen Review durchführen").strip()

    return bool(fallback_reason), fallback_reason, fallback_next_step


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

def _proposed_task(
    order_id: str,
    classification: EmailClassification,
    delta: DeltaAnalysis,
    *,
    missing_document_types: list[str] | None = None,
) -> TaskProposal | None:
    missing_document_types = [doc for doc in (missing_document_types or []) if doc]
    if not (delta.internal_task_needed or delta.escalation_needed or missing_document_types):
        return None
    priority = "high" if delta.escalation_needed else "medium"
    due_at = (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat().replace("+00:00", "Z")
    task_type = "documents" if missing_document_types and not delta.escalation_needed else ("exception" if delta.escalation_needed else "follow_up")
    description_parts = list(delta.reasoning)
    if missing_document_types:
        description_parts.append(f"missing_documents={','.join(missing_document_types)}")
    return TaskProposal(
        order_id=order_id,
        title=f"Review {classification.value} for {order_id}",
        description="; ".join(description_parts),
        priority=priority,
        due_at=due_at,
        task_type=task_type,
    )


def _sync_orders_repo_immediately(order_id: str) -> None:
    """Best-effort immediate git sync for repo-backed ASR case folders.

    The cron job remains as a safety net, but this gives the user immediate
    GitHub visibility right after a case finishes processing.
    """
    script = Path("/root/.hermes/scripts/cargolo_asr_orders_autopush.sh")
    if not script.exists():
        logger.warning("Immediate ASR git sync skipped for %s: script missing at %s", order_id, script)
        return

    try:
        result = subprocess.run(
            [str(script)],
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
    except Exception:
        logger.exception("Immediate ASR git sync crashed for %s", order_id)
        return

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode == 0:
        logger.info("Immediate ASR git sync finished for %s: %s", order_id, stdout or "ok")
    else:
        logger.warning(
            "Immediate ASR git sync failed for %s (exit=%s): stdout=%s stderr=%s",
            order_id,
            result.returncode,
            stdout,
            stderr,
        )


def _latest_history_message_path(history_rows: list[dict[str, Any]]) -> Path | None:
    for row in reversed(history_rows):
        for stored_path_str in row.get("stored_paths") or []:
            candidate = Path(str(stored_path_str))
            if not candidate.exists() or candidate.suffix.lower() != ".json":
                continue
            if "/emails/raw/" in candidate.as_posix():
                return candidate
    return None


def _decode_attachment(content_base64: str | None) -> bytes:
    if not content_base64:
        return b""
    return base64.b64decode(content_base64.encode("utf-8"), validate=True)


def _live_shipment_exists(order_id: str) -> bool | None:
    """Return True/False when live shipment lookup is authoritative, else None.

    None means "unknown / do not enforce skip" (e.g. no live provider configured,
    the lookup itself failed, or a mocked/incomplete provider returned a non-final
    placeholder instead of an actual list response). This lets local/mock setups keep
    working while the production ingest can skip AN/BU references that are not present
    in the ASR shipment list.
    """
    live_provider = build_tms_provider_from_env()
    if live_provider is None:
        return None
    try:
        rows = live_provider.shipments_list(
            transport_category="asr",
            shipment_number=str(order_id or "").strip().upper(),
            limit=20,
        )
    except Exception:
        logger.exception("Live shipment existence check failed for %s", order_id)
        return None

    if not isinstance(rows, list):
        logger.warning(
            "Live shipment existence check for %s returned non-list payload %s; treating as unknown",
            order_id,
            type(rows).__name__,
        )
        return None

    order_upper = str(order_id or "").strip().upper()
    for row in rows:
        if not isinstance(row, dict):
            logger.warning(
                "Live shipment existence check for %s returned non-dict row %s; treating as unknown",
                order_id,
                type(row).__name__,
            )
            return None
        if str((row or {}).get("shipment_number") or "").strip().upper() == order_upper:
            return True
    return False


def _fetch_tms_bundle(
    store: CaseStore,
    order_id: str,
    customer_hint: str | None,
) -> tuple[TMSSnapshot | dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Fetch the main TMS snapshot plus MCP sidecars used by reconciliation."""
    live_provider = build_tms_provider_from_env()
    if live_provider is not None:
        try:
            snapshot = live_provider.snapshot_bundle(order_id, customer_hint)
            tms_dir = store.ensure_case(order_id) / "tms"
            tms_dir.mkdir(parents=True, exist_ok=True)

            document_requirements: dict[str, Any] = {}
            billing_context: dict[str, Any] = {}

            if snapshot.detail:
                (tms_dir / "shipment_detail.json").write_text(
                    json.dumps(snapshot.detail, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            if snapshot.billing_items:
                (tms_dir / "shipment_billing_items.json").write_text(
                    json.dumps(snapshot.billing_items, ensure_ascii=False, indent=2), encoding="utf-8"
                )

            try:
                raw_document_requirements = live_provider.document_requirements(order_id)
                if isinstance(raw_document_requirements, dict):
                    document_requirements = raw_document_requirements
                if document_requirements:
                    (tms_dir / "document_requirements.json").write_text(
                        json.dumps(document_requirements, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
            except Exception:
                logger.exception("TMS document requirements sync failed for %s", order_id)

            try:
                raw_billing_context = live_provider.billing_context(order_id)
                if isinstance(raw_billing_context, dict):
                    billing_context = raw_billing_context
                if billing_context:
                    (tms_dir / "billing_context.json").write_text(
                        json.dumps(billing_context, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
            except Exception:
                logger.exception("TMS billing context sync failed for %s", order_id)

            store.append_tms_sync_log(order_id, {
                "timestamp": utc_now_iso(),
                "phase": "read_sync",
                "action": "fetch_tms_bundle",
                "source": snapshot.source,
                "provider": getattr(snapshot, "provider", None),
                "status": snapshot.status,
                "shipment_uuid": snapshot.shipment_uuid,
                "shipment_number": snapshot.shipment_number,
                "warnings": snapshot.warnings,
                "document_requirements_synced": bool(document_requirements),
                "billing_context_synced": bool(billing_context),
            })
            logger.info(
                "TMS live sync OK for %s (uuid=%s provider=%s)",
                order_id,
                snapshot.shipment_uuid,
                getattr(snapshot, "provider", snapshot.source),
            )
            return snapshot, document_requirements, billing_context
        except Exception:
            logger.exception("TMS live sync failed for %s, falling back to mock", order_id)

    mock = MockTMSAdapter(store.root)
    mock_bundle = mock.snapshot_bundle(order_id, customer_hint)
    return mock_bundle, {}, {}


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
        stored_paths = [str(raw_path)]
        for attachment in message.attachments:
            content = _decode_attachment(attachment.content_base64)
            if not content:
                continue
            stored_path = store.store_attachment(order_id, attachment.filename or "attachment.bin", content)
            stored_paths.append(str(stored_path))
        store.append_email_index(order_id, {
            "message_id": message.message_id,
            "thread_id": message.conversation_id,
            "subject": message.subject,
            "sender": message.from_email,
            "received_at": message.received_at,
            "stored_paths": stored_paths,
            "classification": "history_sync",
            "linked_order_id": order_id,
            "dedupe_hash": message.dedupe_hash,
        })
        count += 1
    return count


def _collect_attachment_records_from_email_index(history_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for row in history_rows:
        if not isinstance(row, dict):
            continue
        for stored_path_str in row.get("stored_paths") or []:
            stored_path = Path(str(stored_path_str or ""))
            if "documents/inbound" not in str(stored_path):
                continue
            if not stored_path.exists() or not stored_path.is_file():
                continue
            content = stored_path.read_bytes()
            sha = hashlib.sha256(content).hexdigest()
            if sha in records:
                continue
            records[sha] = {
                "source": "mail_history_attachment",
                "message_id": row.get("message_id"),
                "received_at": row.get("received_at"),
                "filename": stored_path.name,
                "stored_path": str(stored_path),
                "mime_type": "application/octet-stream",
                "size": len(content),
                "sha256": sha,
                "detected_types": _detect_document_types(stored_path.name),
            }
    return list(records.values())


def _derive_precise_update_candidates(
    *,
    order_id: str,
    tms_snapshot: dict[str, Any],
    history_rows: list[dict[str, Any]],
    document_registry: dict[str, Any],
) -> list[dict[str, Any]]:
    def _normalize_ts(value: Any) -> str | None:
        if value in (None, "", 0):
            return None
        if isinstance(value, (int, float)):
            if value <= 0:
                return None
            return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        return str(value).strip() or None

    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot.get("detail"), dict) else {}
    raw = tms_snapshot.get("raw") if isinstance(tms_snapshot.get("raw"), dict) else {}
    billing_items = tms_snapshot.get("billing_items") if isinstance(tms_snapshot.get("billing_items"), list) else []
    missing_types = set(document_registry.get("missing_types") or [])
    candidates: list[dict[str, Any]] = []

    raw_transport_legs = raw.get("transport_legs") if isinstance(raw.get("transport_legs"), list) else []
    main_leg = next((leg for leg in raw_transport_legs if isinstance(leg, dict) and str(leg.get("leg_type") or "").strip() == "main_carriage"), None)
    milestones = detail.get("milestones") if isinstance(detail.get("milestones"), dict) else {}
    main_atd = _normalize_ts(main_leg.get("atd") if isinstance(main_leg, dict) else None)
    if main_atd and not _normalize_ts(milestones.get("atd_main_carriage")):
        candidates.append({
            "field": "shipment.milestones.atd_main_carriage",
            "suggested_value": main_atd,
            "source": "tms.raw.transport_legs.main_carriage.atd",
            "reason": "Main carriage departure exists in the detailed transport leg data but is missing in the shipment milestone field.",
        })

    cargo_rows = detail.get("cargo") if isinstance(detail.get("cargo"), list) else []
    cargo_weight = round(sum(float(row.get("weight_kg") or 0) for row in cargo_rows if isinstance(row, dict)), 1)
    totals = tms_snapshot.get("totals") if isinstance(tms_snapshot.get("totals"), dict) else {}
    current_total_weight = totals.get("total_weight_kg")
    if cargo_weight and isinstance(current_total_weight, (int, float)) and abs(float(current_total_weight) - cargo_weight) > 1:
        candidates.append({
            "field": "shipment.totals.total_weight_kg",
            "suggested_value": cargo_weight,
            "source": "tms.detail.cargo[].weight_kg",
            "reason": f"Shipment total weight currently shows {current_total_weight}, but the cargo line weights sum to {cargo_weight} kg.",
        })

    notes = raw.get("internal_notes") if isinstance(raw.get("internal_notes"), list) else []
    notes_blob = "\n".join(str(note.get("content") or "") for note in notes if isinstance(note, dict))
    volume_match = re.search(r"Gesamt:\s*[0-9.,]+\s*kg,\s*([0-9.,]+)\s*cbm", notes_blob, re.IGNORECASE)
    if volume_match:
        volume_value = float(volume_match.group(1).replace(".", "").replace(",", "."))
        current_volume = totals.get("total_volume_m3")
        if not isinstance(current_volume, (int, float)) or abs(float(current_volume) - volume_value) > 0.01:
            candidates.append({
                "field": "shipment.totals.total_volume_m3",
                "suggested_value": volume_value,
                "source": "tms.raw.internal_notes[Cargo-Daten]",
                "reason": f"Shipment volume currently shows {current_volume}, but the internal cargo note states a total of {volume_value} cbm.",
            })

    destination_city = (detail.get("destination") or {}).get("city") if isinstance(detail.get("destination"), dict) else None
    for item in billing_items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")
        if "XXX" in name and destination_city:
            candidates.append({
                "field": f"billing_items[{item.get('id', '?')}].name",
                "suggested_value": name.replace("bis XXX", f"bis {destination_city}", 1),
                "source": "tms.billing_items + shipment.destination.city",
                "reason": "Billing line contains a customer-facing placeholder XXX although the shipment destination city is already known.",
            })
            break

    dates = detail.get("dates") if isinstance(detail.get("dates"), dict) else {}
    latest_delivery_date = str(dates.get("latest_delivery_date") or "").strip()
    estimated_delivery_date = str(dates.get("estimated_delivery_date") or "").strip()
    if latest_delivery_date and estimated_delivery_date and latest_delivery_date < estimated_delivery_date:
        candidates.append({
            "field": "shipment.dates.latest_delivery_date",
            "suggested_value": estimated_delivery_date,
            "source": "tms.detail.dates.estimated_delivery_date",
            "reason": f"latest_delivery_date ({latest_delivery_date}) is earlier than estimated_delivery_date ({estimated_delivery_date}) while the shipment is still in transit.",
        })

    detail_transport_legs = detail.get("transport_legs") if isinstance(detail.get("transport_legs"), list) else []
    destination = detail.get("destination") if isinstance(detail.get("destination"), dict) else {}
    destination_city = str(destination.get("city") or "").strip()
    destination_country = str(destination.get("country") or "").strip()
    open_question_blob = "\n".join(str(item) for item in (document_registry.get("analysis_open_questions") or []) if str(item).strip())
    analyzed_docs = [row for row in (document_registry.get("analyzed_documents") or []) if isinstance(row, dict)]
    analyzed_summary_blob = "\n".join(str(row.get("summary") or "") for row in analyzed_docs if str(row.get("summary") or "").strip())
    placeholder_destination = destination_city.lower() in {"", "x", "xx", "xxx", "n/a", "unknown"}
    city_match = re.search(r"im Dokument als\s+([^\)]+?)\s+identifiziert", open_question_blob, re.IGNORECASE)
    if not city_match:
        city_match = re.search(r"nach\s+([A-Za-zÄÖÜäöüß\- ]+?)(?:\s*\(|,\s*(?:CH|DE|Schweiz|Germany)|\.)", analyzed_summary_blob, re.IGNORECASE)
    identified_city = str(city_match.group(1) if city_match else "").strip(" .,")
    if placeholder_destination and identified_city and detail_transport_legs:
        on_carriage_leg = next(
            (
                leg for leg in detail_transport_legs
                if isinstance(leg, dict) and str(leg.get("leg_type") or "").strip().lower() == "on_carriage"
            ),
            detail_transport_legs[-1] if detail_transport_legs else None,
        )
        if isinstance(on_carriage_leg, dict):
            tool_args: dict[str, Any] = {"destination_name": identified_city}
            if destination_country:
                tool_args["destination_country_code"] = destination_country
            if on_carriage_leg.get("leg_uuid"):
                tool_args["leg_uuid"] = on_carriage_leg.get("leg_uuid")
            elif on_carriage_leg.get("sort_order") is not None:
                tool_args["sort_order"] = on_carriage_leg.get("sort_order")
            candidates.append({
                "action_type": "transport_leg_update",
                "target": "transport_leg.destination_name",
                "suggested_value": identified_city,
                "source": "documents.analysis_open_questions",
                "reason": f"TMS destination currently uses placeholder '{destination_city or '-'}', while the document analysis identified the concrete destination as {identified_city}.",
                "tool_args": tool_args,
                "requires_write_access": True,
            })

    history_blob = "\n".join(
        "\n".join(
            [
                str(row.get("subject") or ""),
                str(row.get("body_text") or row.get("body_preview") or row.get("summary") or ""),
                str(row.get("sender") or ""),
            ]
        )
        for row in history_rows
        if isinstance(row, dict)
    )
    history_blob_lower = history_blob.lower()
    history_blob_upper = history_blob.upper()

    freight_details = detail.get("freight_details") if isinstance(detail.get("freight_details"), dict) else {}
    carrier_block = detail.get("carrier") if isinstance(detail.get("carrier"), dict) else {}
    customs_block = detail.get("customs") if isinstance(detail.get("customs"), dict) else {}

    container_match = re.search(r"\b([A-Z]{4}\d{7})\b", history_blob_upper)
    current_container = str(
        freight_details.get("container_number")
        or detail.get("container_number")
        or tms_snapshot.get("container_number")
        or ""
    ).strip().upper()
    if container_match and not current_container:
        candidates.append({
            "field": "shipment.freight_details.container_number",
            "suggested_value": container_match.group(1),
            "source": "mail_history.container_number",
            "reason": "Im Mailverlauf ist eine belastbare Containernummer explizit genannt, im TMS aber noch nicht hinterlegt.",
        })

    hawb_match = re.search(r"\bHAWB\s*[:#]?\s*([A-Z0-9-]{8,24})\b", history_blob_upper)
    current_hawb = str(
        freight_details.get("hawb_number")
        or detail.get("hawb_number")
        or tms_snapshot.get("hawb_number")
        or ""
    ).strip().upper()
    if hawb_match and not current_hawb:
        candidates.append({
            "field": "shipment.freight_details.hawb_number",
            "suggested_value": hawb_match.group(1),
            "source": "mail_history.hawb_number",
            "reason": "Im Mailverlauf ist eine explizite HAWB-Nummer genannt, die im TMS noch fehlt.",
        })

    mrn_match = re.search(r"\bMRN\s*[:#]?\s*([A-Z0-9]{10,24})\b", history_blob_upper)
    current_customs_ref = str(
        customs_block.get("customs_reference")
        or detail.get("customs_reference")
        or tms_snapshot.get("customs_reference")
        or ""
    ).strip().upper()
    if mrn_match and not current_customs_ref:
        candidates.append({
            "field": "shipment.customs.customs_reference",
            "suggested_value": mrn_match.group(1),
            "source": "mail_history.mrn",
            "reason": "Im Mailverlauf ist eine explizite MRN/Zollreferenz genannt, die im TMS noch fehlt.",
        })

    tracking_match = re.search(r"\b(?:TRACKING(?:\s+NUMBER)?|TRACKING\s+NO|TRACKING\s+NR)\s*[:#]?\s*([A-Z0-9-]{8,34})\b", history_blob_upper)
    if not tracking_match:
        tracking_match = re.search(r"\b(1Z[0-9A-Z]{16})\b", history_blob_upper)
    current_tracking = str(
        carrier_block.get("tracking_number")
        or detail.get("tracking_number")
        or tms_snapshot.get("tracking_number")
        or ""
    ).strip().upper()
    if tracking_match and not current_tracking:
        candidates.append({
            "field": "shipment.carrier.tracking_number",
            "suggested_value": tracking_match.group(1),
            "source": "mail_history.tracking_number",
            "reason": "Im Mailverlauf ist eine explizite Tracking-Nummer genannt, die im TMS noch fehlt.",
        })

    carrier_ref_match = re.search(r"\b(?:CARRIER\s+REFERENCE|CARRIER\s+REF|CARRIER\s+REFERENCE\s+NO)\s*[:#]?\s*([A-Z0-9-]{4,24})\b", history_blob_upper)
    current_carrier_ref = str(
        carrier_block.get("carrier_reference")
        or detail.get("carrier_reference")
        or tms_snapshot.get("carrier_reference")
        or ""
    ).strip().upper()
    if carrier_ref_match and not current_carrier_ref:
        candidates.append({
            "field": "shipment.carrier.carrier_reference",
            "suggested_value": carrier_ref_match.group(1),
            "source": "mail_history.carrier_reference",
            "reason": "Im Mailverlauf ist eine explizite Carrier-Referenz genannt, die im TMS noch fehlt.",
        })

    if "commercial_invoice" in missing_types and "invoice" in history_blob_lower:
        candidates.append({
            "field": "documents.commercial_invoice",
            "suggested_value": "import_from_mail_history_attachment",
            "source": "mail_history.subjects/body mentions invoice",
            "reason": "Mail history explicitly references an invoice, so the commercial invoice should be attached and classified into the case/TMS document set.",
        })

    return candidates


def _build_tms_pending_updates(
    *,
    order_id: str,
    tms_snapshot: dict[str, Any],
    history_rows: list[dict[str, Any]],
    document_registry: dict[str, Any],
) -> dict[str, Any]:
    def _slug_hint(value: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")
        return slug or "review"

    def _derive_review_hint_actions() -> list[dict[str, Any]]:
        analyzed_documents = [row for row in (document_registry.get("analyzed_documents") or []) if isinstance(row, dict)]
        actions: list[dict[str, Any]] = []
        seen_targets: set[str] = set()

        def _add_action(*, target: str, review_topic: str, priority: str, reason: str, proposed_decision: str, evidence: list[str], source_filename: str) -> None:
            if target in seen_targets:
                return
            seen_targets.add(target)
            actions.append({
                "action_type": "review_hint",
                "target": target,
                "suggested_value": "review_required",
                "source": "documents.analysis.latest_summary.json",
                "reason": reason,
                "requires_write_access": False,
                "review_topic": review_topic,
                "priority": priority,
                "evidence": [source_filename, *[item for item in evidence if item and item != source_filename]],
                "proposed_decision": proposed_decision,
            })

        for row in analyzed_documents:
            filename = str(row.get("filename") or "document")
            evidence = [
                *[str(item) for item in (row.get("operational_flags") or []) if str(item).strip()],
                *[str(item) for item in (row.get("missing_or_unreadable") or []) if str(item).strip()],
            ]
            evidence_blob = "\n".join(evidence).lower()
            is_blank_customs_template = (
                str(row.get("doc_type") or "") == "customs_document"
                and (
                    "action_required_customer" in evidence_blob
                    or "action_required" in evidence_blob
                    or "aktion erforderlich" in evidence_blob
                    or "unsigned_document" in evidence_blob
                    or "blanko" in evidence_blob
                    or "vom kunden" in evidence_blob and "unterzeichnet" in evidence_blob
                    or "noch ausgefüllt und unterschrieben" in evidence_blob
                    or "noch nicht ausgefüllt" in evidence_blob
                    or "noch ausgefüllt" in evidence_blob
                )
            )
            if is_blank_customs_template:
                _add_action(
                    target="documents.review.customs_template_customer_completion",
                    review_topic="customs_preparation",
                    priority="medium",
                    reason="Die Zollvorlage wirkt wie eine von uns gesendete Kunden-Ausfüllvorlage und nicht wie ein bereits final ausgefülltes Zolldokument.",
                    proposed_decision="Als von uns gesendete Kunden-Ausfüllvorlage behandeln und nur nach ausgefüllter Rücksendung als belastbares Zolldokument werten",
                    evidence=evidence,
                    source_filename=filename,
                )
                continue
            if "mrn" in evidence_blob and "t1-versandverfahren" in evidence_blob:
                _add_action(
                    target="documents.review.mrn_missing_with_t1_reference",
                    review_topic="customs",
                    priority="medium",
                    reason="Im Dokumentensatz wird ein T1-Versandverfahren erwähnt, aber eine MRN ist in den vorliegenden Unterlagen nicht sichtbar oder nicht belastbar dokumentiert.",
                    proposed_decision="MRN im Dokumentensatz oder Mailverlauf prüfen und bei belastbarer Evidenz in den operativen Review übernehmen",
                    evidence=evidence,
                    source_filename=filename,
                )
            if "abfahrtshafen (pol)" in evidence_blob:
                _add_action(
                    target="documents.review.pol_missing",
                    review_topic="routing",
                    priority="medium",
                    reason="Im Dokumentensatz fehlt ein belastbarer Abfahrtshafen (POL), obwohl der Fall bereits operative Export-/Seefrachtbezüge enthält.",
                    proposed_decision="POL aus Handelsrechnung, Carrier-/Booking-Unterlagen oder Mailverlauf verifizieren und danach als Routing-Review bewerten",
                    evidence=evidence,
                    source_filename=filename,
                )
            remaining_quality_questions: list[str] = []
            for question in (row.get("missing_or_unreadable") or []):
                text = str(question or "").strip()
                normalized_text = text.lower()
                if not text:
                    continue
                if text in {"MRN (obwohl T1 erwähnt wird)", "Abfahrtshafen (POL)"}:
                    continue
                if "error code:" in normalized_text or "unsupported mime type" in normalized_text:
                    continue
                if normalized_text.startswith("mrn"):
                    continue
                remaining_quality_questions.append(text)

            if remaining_quality_questions:
                _add_action(
                    target="documents.review.document_quality_bundle",
                    review_topic="document_quality",
                    priority="low",
                    reason="Aus der Dokumentanalyse ergeben sich mehrere formale oder dokumentqualitative Prüfpunkte, die gebündelt fachlich bewertet werden sollten.",
                    proposed_decision="Gebündelte Dokumentqualitäts-Hinweise prüfen und entscheiden, ob Nachforderung, Klarstellung oder Ignorieren angemessen ist",
                    evidence=[*evidence, *remaining_quality_questions],
                    source_filename=filename,
                )
        return actions
    update_candidates = _derive_precise_update_candidates(
        order_id=order_id,
        tms_snapshot=tms_snapshot,
        history_rows=history_rows,
        document_registry=document_registry,
    )
    missing_types = [str(item) for item in (document_registry.get("missing_types") or []) if str(item).strip()]
    tms_match_summary = [row for row in (document_registry.get("tms_match_summary") or []) if isinstance(row, dict)]
    open_questions = [str(item) for item in (document_registry.get("analysis_open_questions") or []) if str(item).strip()]
    tms_documents = [row for row in (document_registry.get("tms_documents") or []) if isinstance(row, dict)]

    def _normalize_doc_type(value: Any) -> str:
        return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")

    tms_uploaded_types = {
        _normalize_doc_type(row.get("document_type") or row.get("label"))
        for row in tms_documents
        if _normalize_doc_type(row.get("document_type") or row.get("label"))
        and str(row.get("status") or "").strip().lower() == "uploaded"
    }
    actionable_missing_types = [
        doc_type for doc_type in missing_types if _normalize_doc_type(doc_type) not in tms_uploaded_types
    ]
    pending_actions: list[dict[str, Any]] = []
    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot.get("detail"), dict) else {}

    def _has_analysis_failures() -> bool:
        if any("Dokumentanalyse fehlgeschlagen" in question for question in open_questions):
            return True
        return any(
            isinstance(row, dict) and str(row.get("analysis_status") or "") == "error"
            for row in (document_registry.get("received_documents") or [])
        )

    def _customs_not_yet_due() -> bool:
        detail_dates = detail.get("dates") if isinstance(detail.get("dates"), dict) else {}
        shipment_status = str(tms_snapshot.get("status") or detail.get("status") or "").strip().lower()
        estimated_delivery_date = str(detail_dates.get("estimated_delivery_date") or "").strip()
        if shipment_status in {"addresses_pending", "confirmed", "pickup_scheduled", "picked_up", "pending_confirmation"}:
            return True
        if estimated_delivery_date:
            try:
                eta_date = datetime.fromisoformat(estimated_delivery_date).date()
                if (eta_date - datetime.now(timezone.utc).date()).days >= 10:
                    return True
            except Exception:
                pass
        return False

    def _actual_delivery_date_looks_like_default_placeholder() -> bool:
        shipment_status = str(tms_snapshot.get("status") or detail.get("status") or "").strip().lower()
        detail_dates = detail.get("dates") if isinstance(detail.get("dates"), dict) else {}
        actual_delivery_date = str(detail_dates.get("actual_delivery_date") or "").strip()
        latest_delivery_date = str(detail_dates.get("latest_delivery_date") or "").strip()
        estimated_delivery_date = str(detail_dates.get("estimated_delivery_date") or "").strip()
        pickup_date = str(detail_dates.get("pickup_date") or "").strip()
        wish_date = str(detail_dates.get("wish_date") or "").strip()
        if not actual_delivery_date:
            return False
        if shipment_status in {"delivered", "cancelled"}:
            return False

        early_status = shipment_status in {"addresses_pending", "confirmed", "pickup_scheduled", "picked_up", "pending_confirmation"}
        same_as_early_dates = actual_delivery_date in {date for date in {pickup_date, wish_date, latest_delivery_date} if date}
        contradictory_eta = bool(estimated_delivery_date and actual_delivery_date < estimated_delivery_date)

        milestones = detail.get("milestones") if isinstance(detail.get("milestones"), dict) else {}
        ata_main_carriage = milestones.get("ata_main_carriage")
        no_arrival_milestone = ata_main_carriage in {None, 0, "", "0"}

        return (same_as_early_dates and early_status) or (contradictory_eta and no_arrival_milestone)

    def _recent_mail_status_conflict_action() -> dict[str, Any] | None:
        shipment_status = str(tms_snapshot.get("status") or detail.get("status") or "").strip().lower()
        if shipment_status not in {"delivered", "cancelled"}:
            return None

        recent_rows = [row for row in history_rows[-5:] if isinstance(row, dict)]
        if not recent_rows:
            return None

        strong_conflict_markers = (
            "status delivered passt noch nicht",
            "not delivered",
            "nicht zugestellt",
            "zustellung noch nicht",
            "delivery status wrong",
            "falscher status",
        )
        active_flow_markers = (
            "dgr",
            "un38.3",
            "un 38.3",
            "verpack",
            "abholung",
            "pickup",
            "booking",
            "awb",
            "air waybill",
            "bill of lading",
            "packing list",
            "invoice",
            "freigabe",
            "documentation",
            "dokument",
            "rate",
        )
        delivered_evidence_markers = (
            "proof of delivery",
            "pod",
            "zugestellt",
            "abgeliefert",
            "delivered to consignee",
        )

        evidence: list[str] = []
        active_signal = False
        explicit_conflict = False
        delivered_evidence = False
        for row in recent_rows:
            subject = str(row.get("subject") or "").strip()
            body = str(row.get("body_text") or row.get("body_preview") or row.get("summary") or "").strip()
            blob = f"{subject}\n{body}".lower()
            if any(marker in blob for marker in strong_conflict_markers):
                explicit_conflict = True
            if any(marker in blob for marker in active_flow_markers):
                active_signal = True
            if any(marker in blob for marker in delivered_evidence_markers):
                delivered_evidence = True
            if subject:
                evidence.append(f"subject: {subject}")

        if not explicit_conflict and (not active_signal or delivered_evidence):
            return None

        reason = (
            "TMS-Status wirkt fachlich inkonsistent: Der Vorgang steht auf delivered/cancelled, "
            "obwohl der aktuelle Mailverlauf noch laufende operative Abstimmung oder einen expliziten Statuskonflikt zeigt."
        )
        return {
            "action_type": "review_hint",
            "target": "shipment.review.status_inconsistent_with_recent_mail_activity",
            "suggested_value": "review_required",
            "source": "mail_history.latest_messages + tms.status",
            "reason": reason,
            "requires_write_access": False,
            "review_topic": "status_validation",
            "priority": "high",
            "evidence": [
                f"shipment_status={shipment_status}",
                *evidence[:3],
            ],
            "proposed_decision": "TMS-Status vor weiteren Dokument-/Meilensteinentscheidungen manuell prüfen und auf den realen Prozessstand zurücksetzen",
        }

    status_conflict_action = _recent_mail_status_conflict_action()
    delivered_status_mail_conflict = bool(status_conflict_action)

    def _classify_action(action: dict[str, Any]) -> str:
        action_type = str(action.get("action_type") or "").strip().lower()
        target = str(action.get("target") or "").strip().lower()
        if action_type == "field_update":
            return "write_now" if supports_field_update_target(str(action.get("target") or "")) else "review"
        if action_type in {"document_upload", "status_update"}:
            return "write_now"
        if action_type == "document_gap":
            if delivered_status_mail_conflict:
                return "not_yet_knowable"
            if target.endswith("customs_document") and _customs_not_yet_due():
                return "not_yet_due"
            if target.endswith(("commercial_invoice", "packing_list", "invoice")) and _has_analysis_failures():
                return "not_yet_knowable"
            return "review"
        return "review"

    for candidate in update_candidates:
        action = {
            "action_type": "field_update",
            "target": candidate.get("field"),
            "suggested_value": candidate.get("suggested_value"),
            "source": candidate.get("source"),
            "reason": candidate.get("reason"),
            "requires_write_access": True,
        }
        action["action_status"] = _classify_action(action)
        pending_actions.append(action)

    if status_conflict_action:
        status_conflict_action["action_status"] = _classify_action(status_conflict_action)
        pending_actions.append(status_conflict_action)

    shipment_status = str(tms_snapshot.get("status") or detail.get("status") or "").strip().lower()
    detail_dates = detail.get("dates") if isinstance(detail.get("dates"), dict) else {}
    actual_delivery_date = str(detail_dates.get("actual_delivery_date") or "").strip()
    if actual_delivery_date and shipment_status not in {"delivered", "cancelled"}:
        if _actual_delivery_date_looks_like_default_placeholder():
            pending_actions.append({
                "action_type": "review_hint",
                "target": "shipment.review.actual_delivery_date_placeholder",
                "suggested_value": "review_required",
                "source": "tms.detail.dates.actual_delivery_date",
                "reason": (
                    f"actual_delivery_date {actual_delivery_date} wirkt wie ein Default-/Platzhalterdatum "
                    "und darf den Sendungsstatus nicht automatisch auf delivered setzen."
                ),
                "requires_write_access": False,
                "review_topic": "status_validation",
                "priority": "high",
                "evidence": [
                    f"status={shipment_status or '-'}",
                    f"actual_delivery_date={actual_delivery_date}",
                    f"latest_delivery_date={str(detail_dates.get('latest_delivery_date') or '').strip() or '-'}",
                    f"estimated_delivery_date={str(detail_dates.get('estimated_delivery_date') or '').strip() or '-'}",
                    f"pickup_date={str(detail_dates.get('pickup_date') or '').strip() or '-'}",
                ],
                "proposed_decision": "Lieferstatus nur nach belastbarer Zustellbestätigung oder konsistenten Milestones manuell prüfen",
                "action_status": "review",
            })
        else:
            action = {
                "action_type": "status_update",
                "target": "shipment.status",
                "suggested_value": "delivered",
                "source": "tms.detail.dates.actual_delivery_date",
                "reason": f"actual_delivery_date {actual_delivery_date} ist gesetzt, daher sollte der Sendungsstatus auf delivered stehen.",
                "requires_write_access": True,
            }
            action["action_status"] = _classify_action(action)
            pending_actions.append(action)

    received_documents = [row for row in (document_registry.get("received_documents") or []) if isinstance(row, dict)]
    uploaded_doc_types: set[str] = set()
    for doc_type in actionable_missing_types:
        matching_doc = next(
            (
                row for row in received_documents
                if doc_type in [str(item) for item in (row.get("detected_types") or []) if str(item).strip()]
                and Path(str(row.get("stored_path") or "")).exists()
            ),
            None,
        )
        if matching_doc:
            action = {
                "action_type": "document_upload",
                "target": f"documents.{doc_type}",
                "document_type": doc_type,
                "file_name": matching_doc.get("filename") or Path(str(matching_doc.get("stored_path") or "document.bin")).name,
                "source_path": str(matching_doc.get("stored_path") or ""),
                "mime_type": matching_doc.get("mime_type"),
                "suggested_value": "upload_local_case_document_to_tms",
                "source": str(matching_doc.get("stored_path") or "documents/inbound"),
                "reason": f"Der erwartete Dokumenttyp '{doc_type}' liegt bereits lokal im Fall vor und kann direkt ins TMS hochgeladen werden.",
                "requires_write_access": True,
            }
            action["action_status"] = _classify_action(action)
            pending_actions.append(action)
            uploaded_doc_types.add(doc_type)

    history_blob = "\n".join(
        "\n".join(
            [
                str(row.get("subject") or ""),
                str(row.get("body_text") or row.get("body_preview") or row.get("summary") or ""),
                str(row.get("sender") or ""),
            ]
        )
        for row in history_rows
        if isinstance(row, dict)
    ).lower()

    contextual_document_targets: set[str] = set()
    shipment_network = str(detail.get("network") or tms_snapshot.get("network") or "").strip().lower()
    if "air_waybill" in actionable_missing_types and shipment_network == "air":
        if any(token in history_blob for token in ("flight", "awb", "air waybill", "carrier", "lh", "lx", "tk")):
            action = {
                "action_type": "review_hint",
                "target": "documents.review.air_waybill_missing_but_flight_context_present",
                "suggested_value": "review_required",
                "source": "mail_history.latest_messages",
                "reason": "Im Mailverlauf liegt bereits klarer Luftfracht-/Flugkontext vor, aber ein belastbarer AWB ist im Fall/TMS noch nicht sichtbar.",
                "requires_write_access": False,
                "review_topic": "air_waybill",
                "priority": "high",
                "evidence": [
                    row.get("subject")
                    for row in history_rows[-3:]
                    if isinstance(row, dict) and str(row.get("subject") or "").strip()
                ],
                "proposed_decision": "AWB bzw. Carrier-Referenz gegen Mail- und Carrier-Stand prüfen und danach gezielt nachtragen oder nachfordern",
            }
            action["action_status"] = _classify_action(action)
            pending_actions.append(action)
            contextual_document_targets.add("documents.air_waybill")

    if "packing_list" in actionable_missing_types and shipment_network in {"air", "ocean", "rail"}:
        action = {
            "action_type": "review_hint",
            "target": "documents.review.packing_list_expected_but_mail_evidence_unclear",
            "suggested_value": "review_required",
            "source": "mail_history.latest_messages + document_registry.missing_types",
            "reason": "Die Packing List ist laut Erwartung offen, aber aus dem aktuellen Mailverlauf ist noch nicht belastbar ableitbar, ob sie bereits vorliegt, nachgereicht wird oder fachlich noch zu klären ist.",
            "requires_write_access": False,
            "review_topic": "packing_list",
            "priority": "medium",
            "evidence": [
                row.get("subject")
                for row in history_rows[-3:]
                if isinstance(row, dict) and str(row.get("subject") or "").strip()
            ] or ["mail_history_without_clear_packing_list_confirmation"],
            "proposed_decision": "Packing List Bedarf gegen Mail- und TMS-Stand prüfen und nur bei belastbarer Evidenz aktiv nachfordern oder hochladen",
        }
        action["action_status"] = _classify_action(action)
        pending_actions.append(action)
        contextual_document_targets.add("documents.packing_list")

    if "commercial_invoice" in actionable_missing_types and shipment_network in {"air", "ocean", "rail"}:
        action = {
            "action_type": "review_hint",
            "target": "documents.review.commercial_invoice_expected_but_mail_evidence_unclear",
            "suggested_value": "review_required",
            "source": "mail_history.latest_messages + document_registry.missing_types",
            "reason": "Die Commercial Invoice ist laut Erwartung offen, aber aus dem aktuellen Mailverlauf ist noch nicht belastbar ableitbar, ob sie bereits vorliegt, nachgereicht wird oder fachlich noch zu klären ist.",
            "requires_write_access": False,
            "review_topic": "commercial_invoice",
            "priority": "medium",
            "evidence": [
                row.get("subject")
                for row in history_rows[-3:]
                if isinstance(row, dict) and str(row.get("subject") or "").strip()
            ] or ["mail_history_without_clear_invoice_confirmation"],
            "proposed_decision": "Commercial Invoice gegen Mail- und TMS-Stand prüfen und nur bei belastbarer Evidenz aktiv nachfordern oder hochladen",
        }
        action["action_status"] = _classify_action(action)
        pending_actions.append(action)
        contextual_document_targets.add("documents.commercial_invoice")

    if "bill_of_lading" in actionable_missing_types and shipment_network in {"sea", "ocean"}:
        if any(token in history_blob for token in ("vessel", "ocean", "sea freight", "bl pending", "bill of lading", "port", "etd")):
            action = {
                "action_type": "review_hint",
                "target": "documents.review.bill_of_lading_missing_but_ocean_context_present",
                "suggested_value": "review_required",
                "source": "mail_history.latest_messages",
                "reason": "Im Mailverlauf liegt bereits klarer Seefracht-/Vessel-Kontext vor, aber ein belastbarer Bill of Lading ist im Fall/TMS noch nicht sichtbar.",
                "requires_write_access": False,
                "review_topic": "bill_of_lading",
                "priority": "high",
                "evidence": [
                    row.get("subject")
                    for row in history_rows[-3:]
                    if isinstance(row, dict) and str(row.get("subject") or "").strip()
                ],
                "proposed_decision": "B/L gegen Mail- und Carrier-Stand prüfen und danach gezielt nachtragen oder nachfordern",
            }
            action["action_status"] = _classify_action(action)
            pending_actions.append(action)
            contextual_document_targets.add("documents.bill_of_lading")

    for doc_type in actionable_missing_types:
        if doc_type in uploaded_doc_types:
            continue
        if f"documents.{doc_type}" in contextual_document_targets:
            continue
        action = {
            "action_type": "document_gap",
            "target": f"documents.{doc_type}",
            "suggested_value": "missing_after_mail_tms_reconciliation",
            "source": "document_registry.missing_types",
            "reason": f"Der erwartete Dokumenttyp '{doc_type}' fehlt nach Mail-/TMS-Abgleich weiterhin und muss im TMS nachgezogen oder aktiv angefordert werden.",
            "requires_write_access": True,
        }
        action["action_status"] = _classify_action(action)
        pending_actions.append(action)

    for action in _derive_review_hint_actions():
        action["action_status"] = _classify_action(action)
        pending_actions.append(action)

    action_summary = {
        "write_now": sum(1 for row in pending_actions if row.get("action_status") == "write_now"),
        "review": sum(1 for row in pending_actions if row.get("action_status") == "review"),
        "not_yet_due": sum(1 for row in pending_actions if row.get("action_status") == "not_yet_due"),
        "not_yet_knowable": sum(1 for row in pending_actions if row.get("action_status") == "not_yet_knowable"),
    }

    return {
        "version": 1,
        "generated_at": utc_now_iso(),
        "order_id": order_id,
        "shipment_uuid": tms_snapshot.get("shipment_uuid") or detail.get("id"),
        "shipment_number": tms_snapshot.get("shipment_number") or order_id,
        "status": "pending_write_access",
        "requires_write_access": True,
        "received_types": list(document_registry.get("received_types") or []),
        "expected_types": list(document_registry.get("expected_types") or []),
        "missing_types": missing_types,
        "document_matches": tms_match_summary,
        "field_update_candidates": update_candidates,
        "open_questions": open_questions,
        "action_summary": action_summary,
        "pending_actions": pending_actions,
    }


def _augment_pending_updates_with_analysis(pending_plan: dict[str, Any], analysis_brief: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(pending_plan, dict) or not isinstance(analysis_brief, dict):
        return pending_plan

    pending_actions = [dict(row) for row in (pending_plan.get("pending_actions") or []) if isinstance(row, dict)]
    if not pending_actions and not analysis_brief:
        return pending_plan

    internal_actions = [row for row in (analysis_brief.get("internal_actions") or []) if isinstance(row, dict)]
    risk_flags = [row for row in (analysis_brief.get("risk_flags") or []) if isinstance(row, dict)]
    ops_summary = str(analysis_brief.get("ops_summary") or "").strip()
    status_conflict = False

    for risk in risk_flags:
        code = str(risk.get("code") or "").strip().upper()
        reason = str(risk.get("reason") or "").strip().lower()
        if code == "DATA_INCONSISTENCY_STATUS" or ("status" in reason and any(token in reason for token in ("zustellung", "delivered", "inkonsistent", "falsch"))):
            status_conflict = True
            break
    if not status_conflict:
        for action in internal_actions:
            action_text = str(action.get("action") or "").strip().lower()
            reason = str(action.get("reason") or "").strip().lower()
            if "status" in action_text or "status" in reason:
                status_conflict = True
                break

    if not status_conflict:
        return pending_plan

    updated = dict(pending_plan)
    if not any(str(row.get("target") or "") == "shipment.review.status_inconsistent_with_analysis" for row in pending_actions):
        pending_actions.insert(0, {
            "action_type": "review_hint",
            "target": "shipment.review.status_inconsistent_with_analysis",
            "suggested_value": "review_required",
            "source": "analysis/latest_brief.json",
            "reason": "Die vertiefte Fallanalyse sieht einen belastbaren Widerspruch zwischen TMS-Status und realem Prozessstand.",
            "requires_write_access": False,
            "review_topic": "status_validation",
            "priority": "high",
            "evidence": [
                *[str(row.get("code") or row.get("reason") or "").strip() for row in risk_flags[:3] if str(row.get("code") or row.get("reason") or "").strip()],
                *[str(row.get("action") or "").strip() for row in internal_actions[:2] if str(row.get("action") or "").strip()],
            ],
            "proposed_decision": "TMS-Status manuell gegen Mailverlauf und reale Transportphase prüfen und erst danach weitere Dokument-/Meilensteinbewertungen fortsetzen",
            "action_status": "review",
        })

    for action in pending_actions:
        if str(action.get("action_type") or "") == "document_gap" and str(action.get("action_status") or "") == "review":
            action["action_status"] = "not_yet_knowable"
            reason = str(action.get("reason") or "").strip()
            if "Statuskonflikt" not in reason:
                action["reason"] = (
                    f"{reason} Statuskonflikt laut Analyse zuerst klären; vorher ist der Dokumentbedarf operativ noch nicht belastbar.".strip()
                )

    updated["pending_actions"] = pending_actions
    updated["analysis_summary"] = ops_summary or updated.get("analysis_summary")
    updated["action_summary"] = {
        "write_now": sum(1 for row in pending_actions if row.get("action_status") == "write_now"),
        "review": sum(1 for row in pending_actions if row.get("action_status") == "review"),
        "not_yet_due": sum(1 for row in pending_actions if row.get("action_status") == "not_yet_due"),
        "not_yet_knowable": sum(1 for row in pending_actions if row.get("action_status") == "not_yet_knowable"),
    }
    return updated


def _render_tms_pending_updates_markdown(plan: dict[str, Any]) -> str:
    lines = [
        f"# TMS Pending Updates {plan.get('order_id')}",
        "",
        f"- Generated at: {plan.get('generated_at')}",
        f"- Shipment UUID: {plan.get('shipment_uuid') or '-'}",
        f"- Status: {plan.get('status')}",
        "",
        "## Pending Actions",
    ]
    pending_actions = plan.get("pending_actions") or []
    if pending_actions:
        for action in pending_actions:
            lines.append(
                f"- [{action.get('action_type')}/{action.get('action_status')}] {action.get('target')}: {action.get('suggested_value')} | Quelle: {action.get('source')} | Grund: {action.get('reason')}"
            )
    else:
        lines.append("- Keine unmittelbaren TMS-Schreibaktionen abgeleitet.")

    lines.extend(["", "## Reconciled Documents"])
    matches = plan.get("document_matches") or []
    if matches:
        for match in matches:
            lines.append(
                f"- {match.get('received_filename')} -> {match.get('tms_document_id')} ({match.get('tms_filename')}) | Basis: {', '.join(match.get('match_basis') or [])}"
            )
    else:
        lines.append("- Keine eindeutigen Dokument-Matches vorhanden.")

    lines.extend(["", "## Open Questions"])
    open_questions = plan.get("open_questions") or []
    if open_questions:
        lines.extend(f"- {item}" for item in open_questions)
    else:
        lines.append("- Keine offenen Fragen.")
    return "\n".join(lines) + "\n"


def _build_tms_applied_updates(*, pending_plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": 1,
        "generated_at": utc_now_iso(),
        "order_id": pending_plan.get("order_id"),
        "shipment_uuid": pending_plan.get("shipment_uuid"),
        "shipment_number": pending_plan.get("shipment_number"),
        "status": "awaiting_write_access",
        "derived_from_pending_updates": "tms/pending_updates.json",
        "requires_write_access": True,
        "applied_actions": [],
        "failed_actions": [],
        "skipped_actions": [],
        "dry_run_actions": [],
        "last_attempted_at": None,
        "applied_at": None,
    }


def _execute_write_now_actions(
    *,
    order_id: str,
    pending_plan: dict[str, Any],
    pending_updates_path: Path,
) -> dict[str, Any]:
    applied_payload = _build_tms_applied_updates(pending_plan=pending_plan)
    now = utc_now_iso()
    write_now_actions = [
        row for row in (pending_plan.get("pending_actions") or [])
        if isinstance(row, dict) and str(row.get("action_status") or "") == "write_now"
    ]
    applied_payload["last_attempted_at"] = now if write_now_actions else None

    context = {
        "order_id": order_id,
        "shipment_uuid": pending_plan.get("shipment_uuid"),
        "shipment_number": pending_plan.get("shipment_number") or order_id,
        "pending_updates_path": str(pending_updates_path),
    }
    for action in write_now_actions:
        try:
            result = apply_pending_tms_action(action, context)
            result_status = str((result or {}).get("status") or "applied").strip().lower()
            entry = {
                "attempted_at": now,
                "action": action,
                "result": result,
            }
            if result_status == "failed":
                applied_payload["failed_actions"].append(entry)
            elif result_status == "skipped":
                applied_payload["skipped_actions"].append(entry)
            else:
                applied_payload["applied_actions"].append(entry)
        except Exception as exc:  # pragma: no cover - result shape validated by tests
            applied_payload["failed_actions"].append({
                "attempted_at": now,
                "action": action,
                "error": str(exc),
            })

    if applied_payload["failed_actions"] and applied_payload["applied_actions"]:
        applied_payload["status"] = "partial"
    elif applied_payload["failed_actions"]:
        applied_payload["status"] = "failed"
    elif applied_payload["applied_actions"]:
        applied_payload["status"] = "applied"
        applied_payload["applied_at"] = now
    elif applied_payload["skipped_actions"]:
        applied_payload["status"] = "skipped"
    return applied_payload


def _build_transport_internal_note(
    *,
    order_id: str,
    run_type: str,
    tms_snapshot: dict[str, Any],
    state: CaseState,
    pending_summary: dict[str, Any] | None,
    applied_summary: dict[str, Any] | None,
    applied_targets: list[str] | None,
    history_sync_count: int,
    history_sync_status: str | None,
    history_sync_error: str | None,
    latest_subject: str | None = None,
    analysis_summary: str | None = None,
) -> str:
    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot.get("detail"), dict) else {}
    status = str(tms_snapshot.get("status") or detail.get("status") or state.current_status or "-").strip() or "-"
    network = str(detail.get("network") or tms_snapshot.get("network") or state.mode or "-").strip() or "-"
    origin = str((detail.get("origin") or {}).get("city") or detail.get("origin_city") or tms_snapshot.get("origin_city") or "-").strip() or "-"
    destination = str((detail.get("destination") or {}).get("city") or detail.get("destination_city") or tms_snapshot.get("destination_city") or "-").strip() or "-"
    next_step = re.sub(r"\s+", " ", str(state.next_best_action or "-").strip())[:140] or "-"
    latest_subject_text = re.sub(r"\s+", " ", str(latest_subject or "").strip())[:120]
    analysis_text = re.sub(r"\s+", " ", str(analysis_summary or "").strip())[:180]
    applied_targets_list = [str(item).strip() for item in (applied_targets or []) if str(item).strip()]

    route_text = f"{origin} → {destination}" if origin != "-" or destination != "-" else "-"

    if run_type == "bootstrap_case":
        first_line = f"Initialer Stand für {order_id}: Status {status}, Verkehr {network}, Route {route_text}."
    elif run_type == "process_event":
        first_line = f"Update zu {order_id}: Status {status}, Verkehr {network}, Route {route_text}."
    else:
        first_line = f"Stand {order_id}: Status {status}, Verkehr {network}, Route {route_text}."

    lines = [first_line]

    if latest_subject_text:
        lines.append(f"Letzter relevanter Betreff: {latest_subject_text}.")

    if history_sync_error:
        lines.append(f"Mailhistorie konnte nicht sauber geladen werden: {history_sync_error}.")
    elif str(history_sync_status or "").strip().lower() == "ok":
        lines.append(f"In diesem Lauf wurden {history_sync_count} weitere Nachricht(en) berücksichtigt.")
    elif str(history_sync_status or "").strip().lower() == "skipped":
        lines.append("Mailhistorie wurde in diesem Lauf nicht erneut abgefragt.")

    if isinstance(pending_summary, dict):
        pending_parts = []
        if int(pending_summary.get("write_now", 0) or 0) > 0:
            pending_parts.append(f"{int(pending_summary.get('write_now', 0))} direkte TMS-Anpassung(en) möglich")
        if int(pending_summary.get("review", 0) or 0) > 0:
            pending_parts.append(f"{int(pending_summary.get('review', 0))} Punkt(e) noch zur fachlichen Prüfung")
        if int(pending_summary.get("not_yet_due", 0) or 0) > 0:
            pending_parts.append(f"{int(pending_summary.get('not_yet_due', 0))} Punkt(e) aktuell noch nicht fällig")
        if int(pending_summary.get("not_yet_knowable", 0) or 0) > 0:
            pending_parts.append(f"{int(pending_summary.get('not_yet_knowable', 0))} Punkt(e) derzeit noch nicht belastbar")
        if pending_parts:
            lines.append("Offen sind aktuell " + "; ".join(pending_parts) + ".")

    if isinstance(applied_summary, dict):
        applied = int(applied_summary.get("applied", 0) or 0)
        failed = int(applied_summary.get("failed", 0) or 0)
        skipped = int(applied_summary.get("skipped", 0) or 0)
        if applied or failed or skipped:
            status_bits = []
            if applied:
                status_bits.append(f"{applied} Änderung(en) wurden im TMS übernommen")
            if failed:
                status_bits.append(f"{failed} Änderung(en) konnten nicht geschrieben werden")
            if skipped:
                status_bits.append(f"{skipped} Änderung(en) wurden bewusst übersprungen")
            lines.append("TMS-Rückmeldung: " + "; ".join(status_bits) + ".")
    if applied_targets_list:
        pretty_targets = ", ".join(applied_targets_list[:4])
        if len(applied_targets_list) > 4:
            pretty_targets += f" und {len(applied_targets_list) - 4} weitere"
        lines.append(f"Übernommen: {pretty_targets}.")

    lines.append(f"Nächster Schritt aus operativer Sicht: {next_step}.")
    if analysis_text:
        lines.append(f"Einschätzung: {analysis_text}.")
    return "\n".join(lines)


def _normalize_transport_note_content(content: str) -> str:
    return re.sub(r"\s+", " ", str(content or "").strip())


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"", "0", "false", "no", "off"}


def _transport_note_hash(content: str) -> str:
    return hashlib.sha256(_normalize_transport_note_content(content).encode("utf-8")).hexdigest()


def _transport_note_already_recorded(store: CaseStore, order_id: str, *, source_key: str, note_hash: str) -> bool:
    rows = store.list_audit_events(order_id)
    if not rows:
        return False
    for row in reversed(rows[-25:]):
        existing_source = str(row.get("internal_note_source_key") or "").strip()
        existing_hash = str(row.get("internal_note_hash") or "").strip()
        existing_status = str(row.get("internal_note_status") or "").strip().lower()
        if existing_status not in {"applied", "skipped_duplicate"}:
            continue
        if source_key and existing_source == source_key:
            return True
        if note_hash and existing_hash == note_hash:
            return True
    return False


def _add_transport_internal_note(store: CaseStore, order_id: str, content: str, *, source_key: str) -> dict[str, Any]:
    normalized_content = _normalize_transport_note_content(content)
    preview = normalized_content[:240]
    note_hash = _transport_note_hash(normalized_content)
    if _transport_note_already_recorded(store, order_id, source_key=source_key, note_hash=note_hash):
        return {
            "status": "skipped_duplicate",
            "preview": preview,
            "error": None,
            "source_key": source_key,
            "note_hash": note_hash,
        }
    if not _env_flag("HERMES_CARGOLO_ASR_ENABLE_TMS_INTERNAL_NOTES", False):
        return {
            "status": "skipped",
            "preview": preview,
            "error": "tms_internal_notes_disabled",
            "source_key": source_key,
            "note_hash": note_hash,
        }
    provider = build_tms_write_provider_from_env()
    if provider is None:
        return {
            "status": "skipped",
            "preview": preview,
            "error": "tms_write_provider_not_configured",
            "source_key": source_key,
            "note_hash": note_hash,
        }
    try:
        response = provider.add_internal_note(an=order_id, admin_user_id=106, content=content)
        status = "applied" if str((response or {}).get("status") or "").strip().lower() == "ok" else "failed"
        return {
            "status": status,
            "preview": preview,
            "error": None if status == "applied" else str(response),
            "response": response,
            "source_key": source_key,
            "note_hash": note_hash,
        }
    except Exception as exc:
        logger.exception("Could not write internal transport note for %s", order_id)
        return {
            "status": "failed",
            "preview": preview,
            "error": str(exc),
            "source_key": source_key,
            "note_hash": note_hash,
        }


def _build_speditionsanalyse_payload(
    *,
    order_id: str,
    state: CaseState,
    tms_snapshot: dict[str, Any],
    document_registry: dict[str, Any],
    history_rows: list[dict[str, Any]],
    pending_updates: dict[str, Any],
) -> dict[str, Any]:
    tms_detail = tms_snapshot.get("detail") if isinstance(tms_snapshot.get("detail"), dict) else {}
    doc_summary_path = document_registry.get("document_analysis_summary_path")
    unique_senders = sorted({str(row.get("sender") or "").strip() for row in history_rows if str(row.get("sender") or "").strip()})
    latest_subjects = [str(row.get("subject") or "") for row in history_rows[-5:] if str(row.get("subject") or "").strip()]
    reconciliation_summary = {
        "integrity_findings": [],
        "document_matches": list(document_registry.get("tms_match_summary") or []),
        "pending_actions": list(pending_updates.get("pending_actions") or []),
        "open_questions": list(document_registry.get("analysis_open_questions") or []),
    }
    billing_items = tms_snapshot.get("billing_items") if isinstance(tms_snapshot.get("billing_items"), list) else []
    billing_blob = "\n".join(
        f"{item.get('name', '')} {item.get('hint', '')}"
        for item in billing_items
        if isinstance(item, dict)
    )
    if "xxx" in billing_blob.lower():
        reconciliation_summary["integrity_findings"].append("billing_contains_placeholder_xxx")
    transport_legs = tms_detail.get("transport_legs") if isinstance(tms_detail.get("transport_legs"), list) else []
    if transport_legs and any((not leg.get("etd") and not leg.get("eta")) for leg in transport_legs if isinstance(leg, dict)):
        reconciliation_summary["integrity_findings"].append("transport_legs_missing_schedule_data")
    if not reconciliation_summary["integrity_findings"]:
        reconciliation_summary["integrity_findings"].extend(
            action.get("target")
            for action in (pending_updates.get("field_update_candidates") or [])
            if action.get("target")
        )
    return {
        "version": 1,
        "output_type": "speditionsanalyse",
        "generated_at": utc_now_iso(),
        "order_id": order_id,
        "source_artifacts": {
            "tms_snapshot_path": "tms_snapshot.json",
            "document_requirements_path": "tms/document_requirements.json",
            "billing_context_path": "tms/billing_context.json",
            "email_index_path": "email_index.jsonl",
            "document_registry_path": "documents/registry.json",
            "document_analysis_summary_path": doc_summary_path,
            "pending_updates_path": "tms/pending_updates.json",
            "applied_updates_path": "tms/applied_updates.json",
        },
        "sections": {
            "tms_mcp": {
                "shipment": {
                    "shipment_number": {"value": tms_snapshot.get("shipment_number") or order_id, "source": "tms_snapshot.detail/shipment snapshot"},
                    "shipment_uuid": {"value": tms_snapshot.get("shipment_uuid"), "source": "tms_snapshot.detail/shipment snapshot"},
                    "status": {"value": tms_snapshot.get("status"), "source": "tms_snapshot.status"},
                    "network": {"value": tms_detail.get("network"), "source": "tms_snapshot.detail.network"},
                    "incoterms": {"value": tms_detail.get("incoterms"), "source": "tms_snapshot.detail.incoterms"},
                    "origin_city": {"value": tms_detail.get("route_origin_city") or (tms_detail.get("origin") or {}).get("city"), "source": "tms_snapshot.detail.route_origin_city"},
                    "destination_city": {"value": tms_detail.get("route_destination_city") or (tms_detail.get("destination") or {}).get("city"), "source": "tms_snapshot.detail.route_destination_city"},
                    "customer_name": {"value": tms_detail.get("company_name"), "source": "tms_snapshot.detail.company_name"},
                },
                "expected_documents": {"value": list(document_registry.get("expected_types") or []), "source": "tms/document_requirements.json + document registry"},
                "tms_documents": {"value": list(document_registry.get("tms_documents") or []), "source": "tms/document_requirements.json"},
            },
            "mail_history": {
                "email_count_total": {"value": len(history_rows), "source": "email_index.jsonl"},
                "unique_senders": {"value": unique_senders, "source": "email_index.jsonl"},
                "latest_subjects": {"value": latest_subjects, "source": "email_index.jsonl"},
                "first_received_at": {"value": min((str(row.get('received_at') or '') for row in history_rows if str(row.get('received_at') or '').strip()), default=None), "source": "email_index.jsonl"},
                "last_received_at": {"value": max((str(row.get('received_at') or '') for row in history_rows if str(row.get('received_at') or '').strip()), default=None), "source": "email_index.jsonl"},
            },
            "documents": {
                "received_types": {"value": list(document_registry.get("received_types") or []), "source": "documents/registry.json"},
                "missing_types": {"value": list(document_registry.get("missing_types") or []), "source": "documents/registry.json"},
                "tms_match_summary": {"value": list(document_registry.get("tms_match_summary") or []), "source": "documents/registry.json"},
                "analyzed_documents": {"value": list(document_registry.get("analyzed_documents") or []), "source": doc_summary_path or "documents/analysis/latest_summary.json"},
            },
            "reconciliation": {
                "integrity_findings": {"value": reconciliation_summary["integrity_findings"], "source": "tms/pending_updates.json::field_update_candidates"},
                "document_matches": {"value": reconciliation_summary["document_matches"], "source": "documents/registry.json::tms_match_summary"},
                "open_questions": {"value": reconciliation_summary["open_questions"], "source": "documents/registry.json::analysis_open_questions"},
                "pending_actions": {"value": reconciliation_summary["pending_actions"], "source": "tms/pending_updates.json"},
                "next_best_action": {"value": state.next_best_action, "source": "case_state.json"},
            },
        },
    }


def _render_case_report_markdown(payload: dict[str, Any]) -> str:
    sections = payload.get("sections") or {}
    tms = sections.get("tms_mcp") or {}
    mail = sections.get("mail_history") or {}
    docs = sections.get("documents") or {}
    recon = sections.get("reconciliation") or {}
    lines = [
        f"# Fallbericht {payload.get('order_id')}",
        "",
        f"- Generiert am: {payload.get('generated_at')}",
        "",
        "## TMS / MCP",
        f"- Shipment: {(tms.get('shipment') or {}).get('shipment_number', {}).get('value')}",
        f"- Status: {(tms.get('shipment') or {}).get('status', {}).get('value')}",
        f"- Netzwerk: {(tms.get('shipment') or {}).get('network', {}).get('value')}",
        f"- Route: {(tms.get('shipment') or {}).get('origin_city', {}).get('value')} -> {(tms.get('shipment') or {}).get('destination_city', {}).get('value')}",
        f"- Erwartete Dokumente: {', '.join((tms.get('expected_documents') or {}).get('value') or []) or '-'}",
        "",
        "## Mailhistorie",
        f"- E-Mails gesamt: {(mail.get('email_count_total') or {}).get('value')}",
        f"- Sender: {', '.join((mail.get('unique_senders') or {}).get('value') or []) or '-'}",
        "",
        "## Dokumentanalyse",
        f"- Empfangene Typen: {', '.join((docs.get('received_types') or {}).get('value') or []) or '-'}",
        f"- Fehlende Typen: {', '.join((docs.get('missing_types') or {}).get('value') or []) or '-'}",
        "",
        "## Reconciliation / TMS-Pflege",
    ]
    for action in (recon.get('pending_actions') or {}).get('value') or []:
        lines.append(f"- [{action.get('action_type')}] {action.get('target')}: {action.get('suggested_value')} | Quelle: {action.get('source')}")
    lines.extend(["", "## Quellen",])
    for key, value in (payload.get('source_artifacts') or {}).items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def _write_bootstrap_summary(
    *,
    case_root: Path,
    order_id: str,
    state: CaseState,
    tms_snapshot: dict[str, Any],
    document_registry: dict[str, Any],
    history_rows: list[dict[str, Any]],
    history_count: int,
) -> tuple[Path, Path]:
    def _normalize_timestamp(value: Any) -> str | None:
        if value in (None, "", 0):
            return None
        if isinstance(value, (int, float)):
            if value <= 0:
                return None
            return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        text = str(value).strip()
        return text or None

    def _contains_city(text_value: Any, city_value: Any) -> bool:
        text = str(text_value or "").strip().lower()
        city = str(city_value or "").strip().lower()
        return bool(text and city and city in text)

    tms_detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    if not isinstance(tms_detail, dict):
        tms_detail = {}
    latest_history_subjects = [
        str(row.get("subject") or "")
        for row in history_rows[-5:]
        if str(row.get("subject") or "").strip()
    ]
    history_subject_blob = "\n".join(latest_history_subjects).upper()
    history_sender_values = sorted({
        str(row.get("sender") or "").strip()
        for row in history_rows
        if str(row.get("sender") or "").strip()
    })
    history_received_values = [
        str(row.get("received_at") or "").strip()
        for row in history_rows
        if str(row.get("received_at") or "").strip()
    ]
    first_received_at = min(history_received_values) if history_received_values else None
    last_received_at = max(history_received_values) if history_received_values else None
    history_matches_shipment_number = bool(order_id and order_id.upper() in history_subject_blob)
    history_has_customer_reference = bool(
        state.customer_reference and str(state.customer_reference).strip() and str(state.customer_reference).upper() in history_subject_blob
    )
    expected_types = list(document_registry.get("expected_types") or [])
    missing_types = list(document_registry.get("missing_types") or [])
    findings: list[str] = []
    if not history_rows:
        findings.append("customer_present_but_history_empty" if tms_detail.get("company_name") else "history_empty")
    if history_matches_shipment_number:
        findings.append("shipment_number_seen_in_history_subjects")
    if history_has_customer_reference:
        findings.append("customer_reference_seen_in_history_subjects")
    if expected_types and missing_types:
        findings.append("expected_documents_still_missing_after_history_sync")
    if tms_snapshot.get("warnings"):
        findings.append("tms_snapshot_contains_warnings")
    if not findings:
        findings.append("no_material_deltas_detected")

    comparison = {
        "history_email_count_total": len(history_rows),
        "history_sync_count": history_count,
        "history_latest_subjects": latest_history_subjects,
        "history_unique_senders": history_sender_values,
        "history_matches_shipment_number": history_matches_shipment_number,
        "history_matches_customer_reference": history_has_customer_reference,
        "tms_customer_available": bool(tms_detail.get("company_name")),
        "tms_customer_reference_available": bool(tms_detail.get("customer_reference")),
        "expected_document_count": len(expected_types),
        "missing_document_count": len(missing_types),
        "findings": findings,
    }

    transport_legs = tms_detail.get("transport_legs") if isinstance(tms_detail.get("transport_legs"), list) else []
    normalized_legs: list[dict[str, Any]] = []
    for leg in transport_legs:
        if not isinstance(leg, dict):
            continue
        normalized_legs.append({
            "leg_type": str(leg.get("leg_type") or "").strip() or None,
            "transport_mode": str(leg.get("transport_mode") or "").strip() or None,
            "origin": str(leg.get("origin") or "").strip() or None,
            "destination": str(leg.get("destination") or "").strip() or None,
            "status": str(leg.get("status") or "").strip() or None,
            "carrier": str(leg.get("carrier") or "").strip() or None,
            "etd": _normalize_timestamp(leg.get("etd")),
            "eta": _normalize_timestamp(leg.get("eta")),
        })

    milestones = tms_detail.get("milestones") if isinstance(tms_detail.get("milestones"), dict) else {}
    normalized_milestones = {key: _normalize_timestamp(value) for key, value in milestones.items()}

    integrity_findings: list[str] = []
    billing_items = tms_snapshot.get("billing_items") if isinstance(tms_snapshot.get("billing_items"), list) else []
    billing_blob = "\n".join(
        f"{item.get('name', '')} {item.get('hint', '')}"
        for item in billing_items
        if isinstance(item, dict)
    )
    if "xxx" in billing_blob.lower():
        integrity_findings.append("billing_contains_placeholder_xxx")
    if normalized_legs:
        first_leg = normalized_legs[0]
        last_leg = normalized_legs[-1]
        if not _contains_city(first_leg.get("origin"), summary_origin_city := (tms_detail.get("route_origin_city") or (tms_detail.get("origin") or {}).get("city"))):
            integrity_findings.append("first_leg_origin_differs_from_tms_origin")
        if not _contains_city(last_leg.get("destination"), summary_destination_city := (tms_detail.get("route_destination_city") or (tms_detail.get("destination") or {}).get("city"))):
            integrity_findings.append("last_leg_destination_differs_from_tms_destination")
        if any((not leg.get("etd") and not leg.get("eta")) for leg in normalized_legs):
            integrity_findings.append("transport_legs_missing_schedule_data")
    else:
        integrity_findings.append("no_transport_legs_in_tms")
    if not integrity_findings:
        integrity_findings.append("no_transport_integrity_flags")

    reconciliation = {
        "mail_history": {
            "email_count_total": len(history_rows),
            "first_received_at": first_received_at,
            "last_received_at": last_received_at,
            "latest_subjects": latest_history_subjects,
            "unique_senders": history_sender_values,
            "shipment_number_seen_in_subjects": history_matches_shipment_number,
            "customer_reference_seen_in_subjects": history_has_customer_reference,
        },
        "tms_transport": {
            "shipment_status": tms_snapshot.get("status"),
            "transport_mode": tms_detail.get("transport_mode") or tms_detail.get("network"),
            "origin_city": tms_detail.get("route_origin_city") or (tms_detail.get("origin") or {}).get("city"),
            "destination_city": tms_detail.get("route_destination_city") or (tms_detail.get("destination") or {}).get("city"),
            "transport_leg_count": len(normalized_legs),
            "legs": normalized_legs,
            "milestones": normalized_milestones,
        },
        "documents": {
            "expected_types": expected_types,
            "missing_types": missing_types,
        },
        "integrity_findings": integrity_findings,
        "update_candidates": _derive_precise_update_candidates(
            order_id=order_id,
            tms_snapshot=tms_snapshot,
            history_rows=history_rows,
            document_registry=document_registry,
        ),
    }
    summary_payload = {
        "order_id": order_id,
        "generated_at": utc_now_iso(),
        "bootstrap": {
            "history_sync_count": history_count,
            "history_email_count_total": len(history_rows),
            "latest_history_subjects": latest_history_subjects,
        },
        "tms": {
            "status": tms_snapshot.get("status"),
            "source": tms_snapshot.get("source"),
            "provider": tms_snapshot.get("provider"),
            "shipment_uuid": tms_snapshot.get("shipment_uuid"),
            "shipment_number": tms_snapshot.get("shipment_number") or order_id,
            "network": tms_detail.get("network"),
            "transport_mode": tms_detail.get("transport_mode"),
            "company_name": tms_detail.get("company_name"),
            "customer_reference": tms_detail.get("customer_reference"),
            "incoterms": tms_detail.get("incoterms"),
            "origin": {
                "city": tms_detail.get("route_origin_city") or (tms_detail.get("origin") or {}).get("city"),
                "country": tms_detail.get("route_origin_country") or (tms_detail.get("origin") or {}).get("country"),
            },
            "destination": {
                "city": tms_detail.get("route_destination_city") or (tms_detail.get("destination") or {}).get("city"),
                "country": tms_detail.get("route_destination_country") or (tms_detail.get("destination") or {}).get("country"),
            },
            "warnings": list(tms_snapshot.get("warnings") or []),
        },
        "documents": {
            "received_types": list(document_registry.get("received_types") or []),
            "expected_types": expected_types,
            "missing_types": missing_types,
            "open_questions": list(document_registry.get("analysis_open_questions") or []),
        },
        "comparison": comparison,
        "reconciliation": reconciliation,
        "case_state": {
            "mode": state.mode,
            "current_status": state.current_status,
            "customer_name": state.customer_name,
            "customer_reference": state.customer_reference,
            "missing_information": list(state.missing_information or []),
            "open_questions": list(state.open_questions or []),
            "next_best_action": state.next_best_action,
            "latest_summary": state.latest_summary,
        },
    }
    summary_json_path = case_root / "bootstrap_summary.json"
    summary_json_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_lines = [
        f"Auftrag: {order_id}",
        f"TMS-Status: {state.current_status}",
        f"Modus: {state.mode}",
        f"Kunde: {state.customer_name or '-'}",
        f"Kundenreferenz: {state.customer_reference or '-'}",
        f"Route: {(summary_payload['tms']['origin']['city'] or '-')}/{(summary_payload['tms']['origin']['country'] or '-')} -> {(summary_payload['tms']['destination']['city'] or '-')}/{(summary_payload['tms']['destination']['country'] or '-')}",
        f"Incoterms: {summary_payload['tms']['incoterms'] or '-'}",
        f"Mailhistorie gesynct: +{history_count} (gesamt {len(history_rows)})",
        f"Erwartete Dokumente: {', '.join(summary_payload['documents']['expected_types']) or '-'}",
        f"Fehlende Dokumente: {', '.join(summary_payload['documents']['missing_types']) or '-'}",
        f"Offene Punkte: {', '.join(summary_payload['documents']['open_questions']) or '-'}",
        f"Nächste Aktion: {state.next_best_action or '-'}",
        "Abgleich Mailhistorie vs. TMS:",
        f"- Mails gesamt: {comparison['history_email_count_total']}",
        f"- Sendungsnummer in Betreffhistorie erkannt: {'ja' if comparison['history_matches_shipment_number'] else 'nein'}",
        f"- Kundenreferenz in Betreffhistorie erkannt: {'ja' if comparison['history_matches_customer_reference'] else 'nein'}",
        f"- Findings: {', '.join(comparison['findings']) or '-'}",
    ]
    if comparison["findings"]:
        summary_lines.extend(f"- {finding}" for finding in comparison["findings"])
    summary_lines.extend([
        "Detaillierter Abgleich Mailverlauf vs. TMS:",
        f"- Erster Mailzeitpunkt: {reconciliation['mail_history']['first_received_at'] or '-'}",
        f"- Letzter Mailzeitpunkt: {reconciliation['mail_history']['last_received_at'] or '-'}",
        f"- Transportlegs laut TMS: {reconciliation['tms_transport']['transport_leg_count']}",
    ])
    if reconciliation["integrity_findings"]:
        summary_lines.extend(f"- {finding}" for finding in reconciliation["integrity_findings"])
    if reconciliation["update_candidates"]:
        summary_lines.append("Konkrete TMS-Pflegekandidaten:")
        for candidate in reconciliation["update_candidates"]:
            summary_lines.append(
                f"- {candidate.get('field')}: {candidate.get('suggested_value')} | Quelle: {candidate.get('source')}"
            )
    if latest_history_subjects:
        summary_lines.append("Letzte Mail-Betreffs:")
        summary_lines.extend(f"- {subject}" for subject in latest_history_subjects)
    summary_txt_path = case_root / "summary.txt"
    summary_txt_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    return summary_json_path, summary_txt_path


def bootstrap_case(
    order_id: str,
    *,
    storage_root: Path | None = None,
    refresh_history: bool = True,
    mailbox: str = "asr@cargolo.com",
    write_internal_note: bool = False,
) -> ProcessingResult:
    order_id = str(order_id or "").strip().upper()
    if not order_id:
        raise ValueError("bootstrap_case requires a non-empty shipment number")
    store = CaseStore(storage_root)
    case_dir_existed = store.order_path(order_id).exists()
    case_root = store.ensure_case(order_id)
    state = store.load_case_state(order_id)
    entities = store.load_entities(order_id)
    prior_document_registry = store.load_document_registry(order_id)

    tms_snapshot_obj, tms_document_requirements, tms_billing_context = _fetch_tms_bundle(store, order_id, None)
    tms_snapshot = tms_snapshot_obj.model_dump(mode="json") if isinstance(tms_snapshot_obj, TMSSnapshot) else tms_snapshot_obj
    if tms_document_requirements:
        tms_snapshot["document_requirements"] = tms_document_requirements
    if tms_billing_context:
        tms_snapshot["billing_context"] = tms_billing_context

    history_count = 0
    history_sync_error: str | None = None
    try:
        history_count = _sync_mail_history(
            store,
            order_id,
            state,
            mailbox,
            exclude_message_ids=set(),
        )
    except Exception as exc:
        history_count = 0
        history_sync_error = f"mail_history_sync_failed: {exc}"

    history_rows = store.list_email_index(order_id)
    history_attachment_records = _collect_attachment_records_from_email_index(history_rows)

    bootstrap_message = IncomingMessagePayload(
        message_id=f"bootstrap:{order_id}",
        subject=f"Bootstrap {order_id}",
        from_email="asr-bootstrap@cargolo.internal",
        received_at=utc_now_iso(),
        body_text="",
        attachments=[],
    )
    document_registry = _build_document_registry(
        prior_registry=prior_document_registry,
        message=bootstrap_message,
        attachment_records=history_attachment_records,
        tms_snapshot=tms_snapshot,
        tms_document_requirements=tms_document_requirements,
    )
    document_registry, document_open_questions = analyze_case_documents(
        order_id=order_id,
        case_root=case_root,
        registry=document_registry,
        tms_snapshot=tms_snapshot,
    )

    tms_detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    network = str((tms_detail or {}).get("network") or "").lower()
    network_to_mode = {"air": "air", "sea": "ocean", "rail": "rail", "road": "road", "asr": "air"}
    state.mode = network_to_mode.get(network, state.mode or "unknown")
    state.current_status = str(tms_snapshot.get("status") or state.current_status or "bootstrapped")
    state.documents_received = sorted(set(document_registry.get("received_types", [])))
    state.documents_expected = sorted(set(document_registry.get("expected_types", [])))
    document_missing_flags = [f"document:{doc_type}" for doc_type in document_registry.get("missing_types", []) if doc_type]
    state.missing_information = sorted(set(document_missing_flags))
    state.open_questions = sorted(
        set(document_open_questions)
        | set(document_registry.get("analysis_open_questions", []))
        | ({history_sync_error} if history_sync_error else set())
    )
    state.risks = sorted(
        set(state.risks)
        | set(tms_snapshot.get("warnings", []))
        | ({history_sync_error} if history_sync_error else set())
    )
    state.reply_recommended = False
    task_recommended, task_reason, next_best_action = _derive_action_guidance_from_signals(
        mode=state.mode,
        missing_information=state.missing_information,
        open_questions=state.open_questions,
        analysis_flags=state.risks,
    )
    state.task_recommended = task_recommended
    state.task_reason = task_reason
    state.next_best_action = next_best_action
    state.tms_order_id = tms_snapshot.get("order_id")
    state.tms_last_sync_at = utc_now_iso()
    if tms_detail:
        if tms_detail.get("company_name"):
            state.customer_name = tms_detail.get("company_name")
        if tms_detail.get("customer_reference"):
            state.customer_reference = tms_detail.get("customer_reference")
    state.latest_summary = (
        f"Bootstrap für {order_id}: TMS-Status {state.current_status}, "
        f"Mailhistorie +{history_count}, erwartete Dokumente {len(state.documents_expected)}, "
        f"empfangene Dokumente {len(history_attachment_records)}"
    )

    entities_path = store.save_entities(order_id, entities)
    state_path = store.save_case_state(order_id, state)
    tms_path = store.save_tms_snapshot(order_id, tms_snapshot)
    document_registry_path = store.save_document_registry(order_id, document_registry)
    pending_updates_payload = _build_tms_pending_updates(
        order_id=order_id,
        tms_snapshot=tms_snapshot,
        history_rows=history_rows,
        document_registry=document_registry,
    )
    pending_updates_json_path, pending_updates_md_path = store.save_tms_pending_updates(
        order_id,
        pending_updates_payload,
        _render_tms_pending_updates_markdown(pending_updates_payload),
    )
    state.task_recommended, state.task_reason, state.next_best_action = _derive_action_guidance_from_pending_actions(
        pending_updates_payload,
        fallback_next_step=state.next_best_action,
        fallback_reason=state.task_reason,
    )
    state_path = store.save_case_state(order_id, state)
    applied_updates_payload = _execute_write_now_actions(
        order_id=order_id,
        pending_plan=pending_updates_payload,
        pending_updates_path=pending_updates_json_path,
    )
    applied_updates_path = store.save_tms_applied_updates(order_id, applied_updates_payload)
    case_report_payload = _build_speditionsanalyse_payload(
        order_id=order_id,
        state=state,
        tms_snapshot=tms_snapshot,
        document_registry=document_registry,
        history_rows=history_rows,
        pending_updates=pending_updates_payload,
    )
    case_report_json_path, case_report_md_path = store.save_case_report(
        order_id,
        case_report_payload,
        _render_case_report_markdown(case_report_payload),
    )
    store.append_tms_sync_log(order_id, {
        "timestamp": utc_now_iso(),
        "phase": "planning_artifacts_created",
        "action": "bootstrap_case",
        "shipment_uuid": pending_updates_payload.get("shipment_uuid"),
        "shipment_number": pending_updates_payload.get("shipment_number"),
        "pending_updates_path": str(pending_updates_json_path),
        "applied_updates_path": str(applied_updates_path),
        "case_report_path": str(case_report_json_path),
    })
    summary_json_path, summary_txt_path = _write_bootstrap_summary(
        case_root=case_root,
        order_id=order_id,
        state=state,
        tms_snapshot=tms_snapshot,
        document_registry=document_registry,
        history_rows=history_rows,
        history_count=history_count,
    )
    analysis_status: str | None = None
    analysis_brief_path: str | None = None
    analysis_priority: str | None = None
    analysis_summary: str | None = None
    timeline_path = store.append_timeline(
        order_id,
        heading="bootstrap / initial baseline",
        summary=state.latest_summary,
        delta=(
            f"history_sync_count={history_count}; expected_docs={','.join(document_registry.get('expected_types', [])) or '-'}; "
            f"missing_docs={','.join(document_registry.get('missing_types', [])) or '-'}"
        ),
        next_step=state.next_best_action,
    )
    bootstrap_note_source_key = f"bootstrap_case:{order_id}"
    internal_note = (
        _add_transport_internal_note(
            store,
            order_id,
            _build_transport_internal_note(
                order_id=order_id,
                run_type="bootstrap_case",
                tms_snapshot=tms_snapshot,
                state=state,
                pending_summary=pending_updates_payload.get("action_summary"),
                applied_summary={
                    "applied": len(applied_updates_payload.get("applied_actions") or []),
                    "failed": len(applied_updates_payload.get("failed_actions") or []),
                    "skipped": len(applied_updates_payload.get("skipped_actions") or []),
                },
                applied_targets=[
                    str((row.get("action") or {}).get("target") or "")
                    for row in (applied_updates_payload.get("applied_actions") or [])
                    if str((row.get("action") or {}).get("target") or "").strip()
                ],
                history_sync_count=history_count,
                history_sync_status="failed" if history_sync_error else ("ok" if refresh_history else "skipped"),
                history_sync_error=history_sync_error,
                latest_subject=None,
                analysis_summary=analysis_summary,
            ),
            source_key=bootstrap_note_source_key,
        )
        if write_internal_note
        else {"status": "skipped", "preview": "", "error": "internal_note_not_enabled", "source_key": bootstrap_note_source_key, "note_hash": ""}
    )

    store.append_audit(
        order_id,
        action="bootstrap_case",
        result="ok",
        files=[str(state_path), str(entities_path), str(tms_path), str(document_registry_path), str(pending_updates_json_path), str(pending_updates_md_path), str(applied_updates_path), str(case_report_json_path), str(case_report_md_path), str(summary_json_path), str(summary_txt_path), str(timeline_path)],
        extra={
            "history_sync_count": history_count,
            "initialized": not case_dir_existed,
            "mailbox": mailbox,
            "internal_note_status": str(internal_note.get("status") or ""),
            "internal_note_preview": str(internal_note.get("preview") or ""),
            "internal_note_error": internal_note.get("error"),
            "internal_note_source_key": str(internal_note.get("source_key") or bootstrap_note_source_key),
            "internal_note_hash": str(internal_note.get("note_hash") or ""),
        },
    )

    latest_history_path = _latest_history_message_path(history_rows)
    if latest_history_path is not None:
        analysis_input_result = ProcessingResult(
            status="processed",
            order_id=order_id,
            case_root=str(case_root),
            initialized=not case_dir_existed,
            timeline_entry=state.latest_summary,
            history_sync_count=history_count,
        )
        analysis_status, analysis_brief_path, analysis_priority, analysis_summary = run_postprocess_subagent_analysis(
            store=store,
            result=analysis_input_result,
            normalized_path=latest_history_path,
            draft_path=case_report_md_path,
            state_path=state_path,
            entities_path=entities_path,
            tms_path=tms_path,
            timeline_path=timeline_path,
            email_index_path=store.ensure_case(order_id) / "email_index.jsonl",
            task_log_path=store.ensure_case(order_id) / "tasks" / "task_log.jsonl",
        )
    else:
        analysis_status, analysis_brief_path, analysis_priority, analysis_summary = (
            "skipped",
            None,
            None,
            None,
        )

    if analysis_brief_path:
        try:
            analysis_brief_payload = json.loads(Path(analysis_brief_path).read_text(encoding="utf-8"))
            updated_pending_updates = _augment_pending_updates_with_analysis(pending_updates_payload, analysis_brief_payload)
            if updated_pending_updates != pending_updates_payload:
                pending_updates_payload = updated_pending_updates
                pending_updates_json_path, pending_updates_md_path = store.save_tms_pending_updates(
                    order_id,
                    pending_updates_payload,
                    _render_tms_pending_updates_markdown(pending_updates_payload),
                )
        except Exception:
            logger.debug("Could not augment bootstrap pending updates from analysis for %s", order_id, exc_info=True)

    _sync_orders_repo_immediately(order_id)

    return ProcessingResult(
        status="bootstrapped",
        order_id=order_id,
        case_root=str(case_root),
        initialized=not case_dir_existed,
        document_registry_path=str(document_registry_path),
        history_sync_count=history_count,
        history_sync_status="failed" if history_sync_error else ("ok" if refresh_history else "skipped"),
        history_sync_error=history_sync_error,
        timeline_entry=state.latest_summary,
        pending_updates_path=str(pending_updates_json_path),
        applied_updates_path=str(applied_updates_path),
        case_report_path=str(case_report_json_path),
        pending_action_summary={
            key: int(value)
            for key, value in (pending_updates_payload.get("action_summary") or {}).items()
            if isinstance(value, int)
        },
        applied_action_summary={
            "applied": len(applied_updates_payload.get("applied_actions") or []),
            "failed": len(applied_updates_payload.get("failed_actions") or []),
            "skipped": len(applied_updates_payload.get("skipped_actions") or []),
        },
        analysis_status=analysis_status,
        analysis_brief_path=analysis_brief_path,
        analysis_priority=analysis_priority,
        analysis_summary=analysis_summary,
        internal_note_status=str(internal_note.get("status") or ""),
        internal_note_preview=str(internal_note.get("preview") or ""),
        internal_note_error=internal_note.get("error"),
        message="Bootstrap baseline created successfully",
    )


def bootstrap_cases_from_tms(
    *,
    storage_root: Path | None = None,
    refresh_history: bool = True,
    mailbox: str = "asr@cargolo.com",
    write_internal_note: bool = False,
    limit: int | None = None,
    per_page: int = 100,
    status_filter: str = "",
    network_filter: str = "",
    search: str = "",
) -> dict[str, Any]:
    provider = build_tms_provider_from_env()
    if provider is None:
        raise RuntimeError("TMS provider is not configured")

    collected: list[str] = []
    page = 1
    while True:
        rows = provider.shipments_list(
            transport_category="asr",
            page=page,
            per_page=per_page,
            status_filter=status_filter,
            network_filter=network_filter,
            search=search,
        )
        if not rows:
            break
        for row in rows:
            shipment_number = str((row or {}).get("shipment_number") or "").strip().upper()
            if shipment_number:
                collected.append(shipment_number)
                if limit is not None and len(collected) >= limit:
                    break
        if limit is not None and len(collected) >= limit:
            break
        if len(rows) < per_page:
            break
        page += 1

    results: list[dict[str, Any]] = []
    for shipment_number in collected:
        try:
            result = bootstrap_case(
                shipment_number,
                storage_root=storage_root,
                refresh_history=refresh_history,
                mailbox=mailbox,
                write_internal_note=write_internal_note,
            )
            results.append(result.model_dump(mode="json"))
        except Exception as exc:
            results.append({
                "status": "error",
                "order_id": shipment_number,
                "message": str(exc),
            })

    success_count = sum(1 for row in results if row.get("status") == "bootstrapped")
    error_count = sum(1 for row in results if row.get("status") == "error")
    return {
        "status": "ok" if error_count == 0 else "partial",
        "total_selected": len(collected),
        "success_count": success_count,
        "error_count": error_count,
        "results": results,
    }


def process_email_event(
    payload: dict[str, Any],
    *,
    storage_root: Path | None = None,
    create_task: bool = False,
    refresh_history: bool = True,
    enable_subagent_analysis: bool = False,
    write_internal_note: bool = False,
    enforce_live_shipment_check: bool | None = None,
) -> ProcessingResult:
    event = IncomingEmailEvent.from_payload(payload)
    store = CaseStore(storage_root)
    message = event.primary_message

    if enforce_live_shipment_check is None:
        enforce_live_shipment_check = storage_root is None

    if not event.order_id:
        review_path = store.save_unassigned_event(event.raw_payload, "No AN/BU found in payload")
        return ProcessingResult(
            status="review_queue",
            review_required=True,
            suppress_delivery=True,
            message=f"No order id found. Saved for review at {review_path}",
        )

    order_id = event.order_id
    shipment_exists = _live_shipment_exists(order_id) if enforce_live_shipment_check else None
    if shipment_exists is False:
        review_path = store.save_unassigned_event(
            event.raw_payload,
            f"Order id {order_id} not found in ASR shipment list",
        )
        return ProcessingResult(
            status="skipped",
            order_id=order_id,
            suppress_delivery=True,
            message=f"Order id {order_id} not found in ASR shipment list. Skipped automatic processing; payload saved at {review_path}",
        )

    case_dir_existed = store.order_path(order_id).exists()
    case_root = store.ensure_case(order_id)

    # Robust initialization check: a case is "new" if either
    # (a) the directory didn't exist before, or
    # (b) the directory existed but no real ingest event was processed yet
    #     (only history_sync entries in index, or state still "new")
    if case_dir_existed:
        prior_state = store.load_case_state(order_id)
        prior_index = store.list_email_index(order_id)
        has_real_ingest = any(
            row.get("classification") not in (None, "history_sync")
            for row in prior_index
        )
        is_first_real_ingest = prior_state.current_status == "new" and not has_real_ingest
    else:
        is_first_real_ingest = True

    if store.has_message(order_id, event.message_id, message.dedupe_hash):
        duplicate_subject = " ".join(str(message.subject or "-").strip().split())[:120]
        duplicate_note_source_key = f"duplicate:{message.message_id or message.dedupe_hash}"
        duplicate_note = (
            _add_transport_internal_note(
                store,
                order_id,
                f"Für den Transport {order_id} wurde keine erneute Verarbeitung gestartet, weil diese Nachricht bereits vorhanden ist. Letzter Betreff: {duplicate_subject}.",
                source_key=duplicate_note_source_key,
            )
            if write_internal_note
            else {"status": "skipped", "preview": "", "error": "internal_note_not_enabled", "source_key": duplicate_note_source_key, "note_hash": ""}
        )
        store.append_audit(
            order_id,
            action="duplicate_email_event",
            result="ok",
            files=[],
            extra={
                "initialized": is_first_real_ingest,
                "duplicate": True,
                "message_id": message.message_id,
                "dedupe_hash": message.dedupe_hash,
                "internal_note_status": str(duplicate_note.get("status") or ""),
                "internal_note_preview": str(duplicate_note.get("preview") or ""),
                "internal_note_error": duplicate_note.get("error"),
                "internal_note_source_key": str(duplicate_note.get("source_key") or duplicate_note_source_key),
                "internal_note_hash": str(duplicate_note.get("note_hash") or ""),
            },
        )
        return ProcessingResult(
            status="duplicate",
            order_id=order_id,
            case_root=str(case_root),
            initialized=is_first_real_ingest,
            duplicate=True,
            internal_note_status=str(duplicate_note.get("status") or ""),
            internal_note_preview=str(duplicate_note.get("preview") or ""),
            internal_note_error=duplicate_note.get("error"),
            message="Event already processed for this case",
        )

    raw_path = store.store_raw_email(order_id, message, event.model_dump(by_alias=True), prefix="ingest")

    state = store.load_case_state(order_id)
    entities_before = store.load_entities(order_id)
    prior_document_registry = store.load_document_registry(order_id)

    # TMS snapshot: prefer MCP-backed provider, fall back to mock
    tms_snapshot_obj, tms_document_requirements, tms_billing_context = _fetch_tms_bundle(store, order_id, event.customer_hint)
    tms_snapshot = tms_snapshot_obj.model_dump(mode="json") if isinstance(tms_snapshot_obj, TMSSnapshot) else tms_snapshot_obj
    if tms_document_requirements:
        tms_snapshot["document_requirements"] = tms_document_requirements
    if tms_billing_context:
        tms_snapshot["billing_context"] = tms_billing_context
    tms = MockTMSAdapter(store.root)  # still needed for task creation

    history_count = 0
    history_sync_error: str | None = None
    if refresh_history:
        try:
            history_count = _sync_mail_history(
                store,
                order_id,
                state,
                event.mailbox,
                exclude_message_ids={message.message_id},
            )
        except Exception as exc:
            history_count = 0
            history_sync_error = f"mail_history_sync_failed: {exc}"

    history_rows = store.list_email_index(order_id)
    attachment_paths: list[str] = []
    attachment_records: list[dict[str, Any]] = []
    for attachment in message.attachments:
        content = _decode_attachment(attachment.content_base64)
        if not content:
            continue
        stored_path = store.store_attachment(order_id, attachment.filename or "attachment.bin", content)
        attachment_paths.append(str(stored_path))
        attachment_records.append({
            "source": "email_attachment",
            "message_id": message.message_id,
            "filename": attachment.filename or stored_path.name,
            "stored_path": str(stored_path),
            "mime_type": attachment.mime_type,
            "size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
            "received_at": message.received_at or event.received_at,
            "detected_types": _detect_document_types(attachment.filename, attachment.mime_type, message.subject),
        })

    entities_new = extract_entities(event)
    attachment_document_types = sorted({
        doc_type
        for row in attachment_records
        for doc_type in row.get("detected_types", [])
        if doc_type
    })
    if attachment_document_types:
        entities_new = EntitiesSnapshot(
            **{
                **entities_new.model_dump(),
                "document_types": sorted(set(entities_new.document_types) | set(attachment_document_types)),
            }
        )
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

    document_registry = _build_document_registry(
        prior_registry=prior_document_registry,
        message=message,
        attachment_records=attachment_records,
        tms_snapshot=tms_snapshot,
        tms_document_requirements=tms_document_requirements,
    )
    document_registry, document_open_questions = analyze_case_documents(
        order_id=order_id,
        case_root=case_root,
        registry=document_registry,
        tms_snapshot=tms_snapshot,
    )

    state.mode = entities_after.transport_mode_candidates[0] if entities_after.transport_mode_candidates else detect_mode(_body_blob(message))
    state.current_status = classification.value
    state.last_email_at = message.received_at or event.received_at
    if message.is_internal:
        state.last_internal_action_at = utc_now_iso()
    else:
        state.last_customer_message_at = message.received_at or event.received_at
    state.documents_received = sorted(set(state.documents_received) | set(document_registry.get("received_types", [])) | set(entities_after.document_types))
    state.documents_expected = sorted(set(document_registry.get("expected_types", [])))
    document_missing_flags = [f"document:{doc_type}" for doc_type in document_registry.get("missing_types", []) if doc_type]
    state.missing_information = sorted(set(delta.remaining_missing_information) | set(document_missing_flags))
    state.open_questions = sorted(
        set(state.open_questions)
        | set(document_open_questions)
        | set(document_registry.get("analysis_open_questions", []))
        | ({history_sync_error} if history_sync_error else set())
    )
    analysis_flags = [
        f"document_analysis:{flag}"
        for row in document_registry.get("analyzed_documents", [])
        if isinstance(row, dict)
        for flag in row.get("operational_flags", [])
        if str(flag).strip()
    ]
    state.risks = sorted(set(state.risks) | set(delta.contradictions) | set(analysis_flags))
    state.reply_recommended = delta.customer_reply_needed
    task_recommended, task_reason, next_best_action = _derive_action_guidance_from_signals(
        mode=state.mode,
        contradictions=delta.contradictions,
        missing_information=state.missing_information,
        open_questions=state.open_questions,
        analysis_flags=analysis_flags,
        escalation_needed=delta.escalation_needed,
    )
    state.task_recommended = task_recommended
    state.task_reason = task_reason
    state.next_best_action = next_best_action
    state.tms_order_id = tms_snapshot.get("order_id")
    state.tms_last_sync_at = utc_now_iso()

    # Enrich case state from live TMS data when available
    if tms_snapshot.get("source") == "live" and tms_snapshot.get("detail"):
        tms_detail = tms_snapshot["detail"]
        if not state.customer_name and tms_detail.get("company_name"):
            state.customer_name = tms_detail["company_name"]
        if not state.customer_reference and tms_detail.get("customer_reference"):
            state.customer_reference = tms_detail["customer_reference"]
        # Derive transport mode from the "network" field (air, sea, rail, road, asr)
        tms_network = tms_detail.get("network", "").lower()
        network_to_mode = {"air": "air", "sea": "ocean", "rail": "rail", "road": "road", "asr": "air"}
        if tms_network in network_to_mode and state.mode == "unknown":
            state.mode = network_to_mode[tms_network]
    # Collect TMS warnings as risks
    tms_warnings = tms_snapshot.get("warnings", [])
    if tms_warnings:
        state.risks = sorted(set(state.risks) | set(tms_warnings))
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
        "document_registry": {
            "received_types": document_registry.get("received_types", []),
            "expected_types": document_registry.get("expected_types", []),
            "missing_types": document_registry.get("missing_types", []),
            "analysis_open_questions": document_registry.get("analysis_open_questions", []),
            "document_analysis_summary_path": document_registry.get("document_analysis_summary_path"),
            "tms_match_summary": document_registry.get("tms_match_summary", []),
        },
        "history_sync_count": history_count,
        "history_email_count_before_current": len(history_rows),
        "case_initialized": is_first_real_ingest,
    }
    normalized_path = store.store_normalized_email(order_id, message, normalized)
    draft_path = store.store_draft(order_id, message, _draft_markdown(order_id, classification, delta, state, message))
    entities_path = store.save_entities(order_id, entities_after)
    state_path = store.save_case_state(order_id, state)
    tms_path = store.save_tms_snapshot(order_id, tms_snapshot)
    document_registry_path = store.save_document_registry(order_id, document_registry)
    pending_updates_payload = _build_tms_pending_updates(
        order_id=order_id,
        tms_snapshot=tms_snapshot,
        history_rows=history_rows,
        document_registry=document_registry,
    )
    pending_updates_json_path, pending_updates_md_path = store.save_tms_pending_updates(
        order_id,
        pending_updates_payload,
        _render_tms_pending_updates_markdown(pending_updates_payload),
    )
    state.task_recommended, state.task_reason, state.next_best_action = _derive_action_guidance_from_pending_actions(
        pending_updates_payload,
        fallback_next_step=state.next_best_action,
        fallback_reason=state.task_reason,
    )
    state_path = store.save_case_state(order_id, state)
    applied_updates_payload = _execute_write_now_actions(
        order_id=order_id,
        pending_plan=pending_updates_payload,
        pending_updates_path=pending_updates_json_path,
    )
    applied_updates_path = store.save_tms_applied_updates(order_id, applied_updates_payload)
    case_report_payload = _build_speditionsanalyse_payload(
        order_id=order_id,
        state=state,
        tms_snapshot=tms_snapshot,
        document_registry=document_registry,
        history_rows=history_rows,
        pending_updates=pending_updates_payload,
    )
    case_report_json_path, case_report_md_path = store.save_case_report(
        order_id,
        case_report_payload,
        _render_case_report_markdown(case_report_payload),
    )
    store.append_tms_sync_log(order_id, {
        "timestamp": utc_now_iso(),
        "phase": "planning_artifacts_created",
        "action": "process_email_event",
        "shipment_uuid": pending_updates_payload.get("shipment_uuid"),
        "shipment_number": pending_updates_payload.get("shipment_number"),
        "pending_updates_path": str(pending_updates_json_path),
        "applied_updates_path": str(applied_updates_path),
        "case_report_path": str(case_report_json_path),
    })
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

    task_proposal = _proposed_task(
        order_id,
        classification,
        delta,
        missing_document_types=document_registry.get("missing_types", []),
    )
    if task_proposal and create_task:
        shipment_uuid = tms_snapshot.get("shipment_uuid") or tms_snapshot.get("detail", {}).get("id")
        live_client = build_tms_client_from_env()
        if live_client and shipment_uuid:
            # Create task via real TMS API (POST /admin/todos/create)
            try:
                # Map task_type to Xano category
                type_to_category = {
                    "exception": "sonstiges",
                    "follow_up": "kommunikation",
                    "documents": "dokumente",
                    "customs": "zoll",
                    "pickup": "abholung",
                    "delivery": "zustellung",
                    "billing": "rechnung",
                }
                category = type_to_category.get(task_proposal.task_type, "sonstiges")
                result = live_client.create_todo(
                    title=task_proposal.title,
                    related_id=shipment_uuid,
                    description=task_proposal.description,
                    priority=task_proposal.priority,
                    category=category,
                    due_date=task_proposal.due_at,
                )
                task_proposal.created = True
                todo_data = result.get("todo", {})
                task_proposal.external_task_id = todo_data.get("id")
                logger.info("TMS todo created for %s: %s", order_id, task_proposal.external_task_id)
            except Exception:
                logger.exception("TMS todo creation failed for %s, saving as proposal only", order_id)
        else:
            # Fallback: create in local mock
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
    process_note_source_key = f"process_event:{message.message_id or message.dedupe_hash}"
    internal_note = (
        _add_transport_internal_note(
            store,
            order_id,
            _build_transport_internal_note(
                order_id=order_id,
                run_type="process_event",
                tms_snapshot=tms_snapshot,
                state=state,
                pending_summary=pending_updates_payload.get("action_summary"),
                applied_summary={
                    "applied": len(applied_updates_payload.get("applied_actions") or []),
                    "failed": len(applied_updates_payload.get("failed_actions") or []),
                    "skipped": len(applied_updates_payload.get("skipped_actions") or []),
                },
                applied_targets=[
                    str((row.get("action") or {}).get("target") or "")
                    for row in (applied_updates_payload.get("applied_actions") or [])
                    if str((row.get("action") or {}).get("target") or "").strip()
                ],
                history_sync_count=history_count,
                history_sync_status="failed" if history_sync_error else ("ok" if refresh_history else "skipped"),
                history_sync_error=history_sync_error,
                latest_subject=message.subject,
                analysis_summary=None,
            ),
            source_key=process_note_source_key,
        )
        if write_internal_note
        else {"status": "skipped", "preview": "", "error": "internal_note_not_enabled", "source_key": process_note_source_key, "note_hash": ""}
    )

    store.append_audit(
        order_id,
        action="process_email_event",
        result="ok",
        files=[str(raw_path), str(normalized_path), str(draft_path), str(state_path), str(entities_path), str(tms_path), str(document_registry_path), str(pending_updates_json_path), str(pending_updates_md_path), str(applied_updates_path), str(case_report_json_path), str(case_report_md_path), str(timeline_path), *attachment_paths],
        extra={
            "classification": classification.value,
            "history_sync_count": history_count,
            "history_email_count_before_current": len(history_rows),
            "initialized": is_first_real_ingest,
            "task_created": bool(task_proposal and task_proposal.created),
            "message_id": message.message_id,
            "dedupe_hash": message.dedupe_hash,
            "internal_note_status": str(internal_note.get("status") or ""),
            "internal_note_preview": str(internal_note.get("preview") or ""),
            "internal_note_error": internal_note.get("error"),
            "internal_note_source_key": str(internal_note.get("source_key") or process_note_source_key),
            "internal_note_hash": str(internal_note.get("note_hash") or ""),
        },
    )

    processing_result = ProcessingResult(
        status="processed",
        order_id=order_id,
        case_root=str(case_root),
        classification=classification,
        initialized=is_first_real_ingest,
        draft_path=str(draft_path),
        document_registry_path=str(document_registry_path),
        task=task_proposal,
        history_sync_count=history_count,
        history_sync_status="failed" if history_sync_error else ("ok" if refresh_history else "skipped"),
        history_sync_error=history_sync_error,
        latest_subject=message.subject,
        latest_sender=message.from_email,
        attachment_count=len(message.attachments),
        attachment_filenames=[str(item.filename or "") for item in message.attachments if str(item.filename or "").strip()],
        pending_updates_path=str(pending_updates_json_path),
        applied_updates_path=str(applied_updates_path),
        case_report_path=str(case_report_json_path),
        pending_action_summary={
            key: int(value)
            for key, value in (pending_updates_payload.get("action_summary") or {}).items()
            if isinstance(value, int)
        },
        applied_action_summary={
            "applied": len(applied_updates_payload.get("applied_actions") or []),
            "failed": len(applied_updates_payload.get("failed_actions") or []),
            "skipped": len(applied_updates_payload.get("skipped_actions") or []),
        },
        applied_action_targets=[
            str((row.get("action") or {}).get("target") or "")
            for row in (applied_updates_payload.get("applied_actions") or [])
            if str((row.get("action") or {}).get("target") or "").strip()
        ],
        failed_action_targets=[
            str((row.get("action") or {}).get("target") or "")
            for row in (applied_updates_payload.get("failed_actions") or [])
            if str((row.get("action") or {}).get("target") or "").strip()
        ],
        skipped_action_targets=[
            str((row.get("action") or {}).get("target") or "")
            for row in (applied_updates_payload.get("skipped_actions") or [])
            if str((row.get("action") or {}).get("target") or "").strip()
        ],
        internal_note_status=str(internal_note.get("status") or ""),
        internal_note_preview=str(internal_note.get("preview") or ""),
        internal_note_error=internal_note.get("error"),
        timeline_entry=state.latest_summary,
        message="Processed email event successfully",
    )

    if enable_subagent_analysis:
        analysis_status, analysis_brief_path, analysis_priority, analysis_summary = run_postprocess_subagent_analysis(
            store=store,
            result=processing_result,
            normalized_path=normalized_path,
            draft_path=draft_path,
            state_path=state_path,
            entities_path=entities_path,
            tms_path=tms_path,
            timeline_path=timeline_path,
            email_index_path=store.ensure_case(order_id) / "email_index.jsonl",
            task_log_path=store.ensure_case(order_id) / "tasks" / "task_log.jsonl",
        )
    else:
        analysis_status, analysis_brief_path, analysis_priority, analysis_summary = ("disabled", None, None, None)
    processing_result.analysis_status = analysis_status
    processing_result.analysis_brief_path = analysis_brief_path
    processing_result.analysis_priority = analysis_priority
    processing_result.analysis_summary = analysis_summary

    if analysis_brief_path:
        try:
            analysis_brief_payload = json.loads(Path(analysis_brief_path).read_text(encoding="utf-8"))
            updated_pending_updates = _augment_pending_updates_with_analysis(pending_updates_payload, analysis_brief_payload)
            if updated_pending_updates != pending_updates_payload:
                pending_updates_payload = updated_pending_updates
                pending_updates_json_path, pending_updates_md_path = store.save_tms_pending_updates(
                    order_id,
                    pending_updates_payload,
                    _render_tms_pending_updates_markdown(pending_updates_payload),
                )
                processing_result.pending_updates_path = str(pending_updates_json_path)
                processing_result.pending_action_summary = {
                    key: int(value)
                    for key, value in (pending_updates_payload.get("action_summary") or {}).items()
                    if isinstance(value, int)
                }
        except Exception:
            logger.debug("Could not augment process pending updates from analysis for %s", order_id, exc_info=True)

    _sync_orders_repo_immediately(order_id)

    return processing_result
