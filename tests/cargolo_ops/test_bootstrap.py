import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from plugins.cargolo_ops.models import TMSSnapshot
from plugins.cargolo_ops.processor import (
    _build_tms_pending_updates,
    _derive_precise_update_candidates,
    bootstrap_case,
    bootstrap_cases_from_tms,
)
from plugins.cargolo_ops.storage import CaseStore


def _mock_snapshot(order_id: str) -> TMSSnapshot:
    return TMSSnapshot(
        order_id=order_id,
        shipment_uuid=f"uuid-{order_id}",
        shipment_number=order_id,
        source="live",
        status="in_transit",
        detail={
            "network": "sea",
            "company_name": "Test Kunde GmbH",
            "customer_reference": f"REF-{order_id}",
            "route_origin_city": "Hamburg",
            "route_origin_country": "DE",
            "route_destination_city": "Shanghai",
            "route_destination_country": "CN",
            "incoterms": "FOB",
            "documents": [
                {"id": "doc-1", "document_type": "Commercial Invoice", "required": True},
            ],
        },
        billing_items=[],
        billing_sums={"total_vk": 0, "total_ek": 0, "margin": 0},
        fetched_at="2026-04-18T21:00:00Z",
        warnings=[],
        provider="mcp_bridge",
    )


def _mock_doc_requirements(order_id: str) -> dict:
    return {
        "status": "ok",
        "query": {"an": order_id},
        "shipment": {"shipment_uuid": f"uuid-{order_id}", "shipment_number": order_id},
        "documents": [
            {"tms_document_id": "doc-1", "label": "Commercial Invoice", "document_type": "commercial_invoice", "required": True},
            {"tms_document_id": "doc-2", "label": "Packing List", "document_type": "packing_list", "required": True},
        ],
        "expected_types": ["commercial_invoice", "packing_list"],
        "warnings": [],
    }


def _mock_billing_context(order_id: str) -> dict:
    return {
        "status": "ok",
        "query": {"an": order_id},
        "shipment": {"shipment_uuid": f"uuid-{order_id}", "shipment_number": order_id},
        "billing": {"items": [], "sums": {"total_vk": 0, "total_ek": 0, "margin": 0}},
        "warnings": [],
    }


def test_build_tms_pending_updates_classifies_action_statuses():
    tms_snapshot = {
        "shipment_uuid": "uuid-AN-CLS",
        "shipment_number": "AN-CLS",
        "status": "addresses_pending",
        "detail": {
            "status": "addresses_pending",
            "network": "rail",
            "dates": {
                "estimated_delivery_date": "2026-05-20",
                "latest_delivery_date": "2026-05-18",
                "pickup_date": "2026-04-17",
            },
        },
    }
    history_rows = []
    document_registry = {
        "received_types": ["packing_list"],
        "expected_types": ["commercial_invoice", "customs_document", "packing_list"],
        "missing_types": ["commercial_invoice", "customs_document", "proof_of_delivery"],
        "analysis_open_questions": [
            "Dokumentanalyse fehlgeschlagen für invoice.xlsx: Unsupported MIME type",
        ],
        "tms_match_summary": [],
    }

    with patch("plugins.cargolo_ops.processor._derive_precise_update_candidates", return_value=[
        {
            "field": "shipment.dates.latest_delivery_date",
            "suggested_value": "2026-05-20",
            "source": "tms.detail.dates.estimated_delivery_date",
            "reason": "latest_delivery_date is earlier than estimated_delivery_date while shipment is active.",
        },
    ]):
        payload = _build_tms_pending_updates(
            order_id="AN-CLS",
            tms_snapshot=tms_snapshot,
            history_rows=history_rows,
            document_registry=document_registry,
        )

    actions_by_target = {row["target"]: row for row in payload["pending_actions"]}
    assert actions_by_target["shipment.dates.latest_delivery_date"]["action_status"] == "write_now"
    assert actions_by_target["documents.customs_document"]["action_status"] == "not_yet_due"
    assert actions_by_target["documents.review.commercial_invoice_expected_but_mail_evidence_unclear"]["action_status"] == "review"
    assert actions_by_target["documents.proof_of_delivery"]["action_status"] == "review"
    assert payload["action_summary"] == {
        "write_now": 1,
        "review": 2,
        "not_yet_due": 1,
        "not_yet_knowable": 0,
    }


def test_build_tms_pending_updates_ignores_local_mirroring_gaps_when_tms_docs_are_uploaded():
    tms_snapshot = {
        "shipment_uuid": "uuid-BU-4638",
        "shipment_number": "BU-4638",
        "status": "delivered",
        "detail": {
            "status": "delivered",
            "network": "sea",
            "dates": {
                "estimated_delivery_date": "2025-12-08",
            },
        },
    }
    document_registry = {
        "received_types": [],
        "expected_types": ["bill_of_lading", "commercial_invoice", "customs_document", "packing_list"],
        "missing_types": ["bill_of_lading", "commercial_invoice", "customs_document", "packing_list"],
        "analysis_open_questions": [],
        "tms_match_summary": [],
        "tms_documents": [
            {"document_type": "bill_of_lading", "status": "uploaded", "required": True},
            {"document_type": "commercial_invoice", "status": "uploaded", "required": True},
            {"document_type": "customs_document", "status": "uploaded", "required": True},
            {"document_type": "packing_list", "status": "uploaded", "required": True},
        ],
    }

    payload = _build_tms_pending_updates(
        order_id="BU-4638",
        tms_snapshot=tms_snapshot,
        history_rows=[],
        document_registry=document_registry,
    )

    assert payload["pending_actions"] == []
    assert payload["action_summary"] == {
        "write_now": 0,
        "review": 0,
        "not_yet_due": 0,
        "not_yet_knowable": 0,
    }



def test_build_tms_pending_updates_creates_blank_customs_template_review_hint():
    tms_snapshot = {
        "shipment_uuid": "uuid-AN-BLANKO",
        "shipment_number": "AN-BLANKO",
        "status": "confirmed",
        "detail": {"status": "confirmed", "network": "sea", "dates": {}},
    }
    document_registry = {
        "received_types": ["customs_document"],
        "expected_types": ["customs_document"],
        "missing_types": [],
        "analysis_open_questions": ["Unterschrift des Ausstellers fehlt (Blanko-Formular)"],
        "tms_match_summary": [],
        "analyzed_documents": [
            {
                "filename": "CARGOLO_Zollvollmacht Export (4).pdf",
                "doc_type": "customs_document",
                "operational_flags": [
                    "Das Dokument muss vom Kunden (ANS answer elektronik) noch ausgefüllt und unterschrieben werden.",
                    "action_required_customer",
                    "unsigned_document",
                ],
                "missing_or_unreadable": [
                    "Unterschrift des Ausstellers fehlt (Blanko-Formular)",
                    "Adressdaten des Ausstellers nicht ausgefüllt",
                    "EORI-Nummer nicht eingetragen",
                ],
                "analysis_path": "/tmp/doc_customs_template.json",
            }
        ],
    }

    payload = _build_tms_pending_updates(
        order_id="AN-BLANKO",
        tms_snapshot=tms_snapshot,
        history_rows=[],
        document_registry=document_registry,
    )

    actions_by_target = {row["target"]: row for row in payload["pending_actions"]}
    assert "documents.review.customs_template_customer_completion" in actions_by_target
    action = actions_by_target["documents.review.customs_template_customer_completion"]
    assert action["review_topic"] == "customs_preparation"
    assert action["priority"] == "medium"
    assert action["action_status"] == "review"
    assert "CARGOLO_Zollvollmacht Export (4).pdf" in action["evidence"]
    assert action["proposed_decision"] == "Als von uns gesendete Kunden-Ausfüllvorlage behandeln und nur nach ausgefüllter Rücksendung als belastbares Zolldokument werten"
    assert "documents.review.unterschrift_des_ausstellers_fehlt_blanko_formular" not in actions_by_target



def test_build_tms_pending_updates_creates_review_hints_from_document_analysis():
    tms_snapshot = {
        "shipment_uuid": "uuid-AN-RVW",
        "shipment_number": "AN-RVW",
        "status": "confirmed",
        "detail": {
            "status": "confirmed",
            "network": "sea",
            "dates": {},
        },
    }
    document_registry = {
        "received_types": ["billing", "commercial_invoice"],
        "expected_types": ["commercial_invoice"],
        "missing_types": [],
        "analysis_open_questions": ["MRN (obwohl T1 erwähnt wird)", "Abfahrtshafen (POL)"],
        "tms_match_summary": [],
        "analyzed_documents": [
            {
                "filename": "TourNr_135192499.pdf",
                "doc_type": "billing",
                "operational_flags": ["T1-Versandverfahren", "Verplombung erforderlich"],
                "missing_or_unreadable": ["MRN (obwohl T1 erwähnt wird)"],
                "analysis_path": "/tmp/doc_billing.json",
            },
            {
                "filename": "Invoice.pdf",
                "doc_type": "commercial_invoice",
                "operational_flags": ["incoterm_mismatch", "high_value"],
                "missing_or_unreadable": ["Abfahrtshafen (POL)"],
                "analysis_path": "/tmp/doc_invoice.json",
            },
        ],
    }

    payload = _build_tms_pending_updates(
        order_id="AN-RVW",
        tms_snapshot=tms_snapshot,
        history_rows=[],
        document_registry=document_registry,
    )

    review_hint_targets = {row["target"] for row in payload["pending_actions"] if row["action_type"] == "review_hint"}
    assert "documents.review.mrn_missing_with_t1_reference" in review_hint_targets
    assert "documents.review.pol_missing" in review_hint_targets
    mrn_action = next(row for row in payload["pending_actions"] if row["target"] == "documents.review.mrn_missing_with_t1_reference")
    assert mrn_action["action_status"] == "review"
    assert mrn_action["review_topic"] == "customs"
    assert "TourNr_135192499.pdf" in mrn_action["evidence"]
    assert mrn_action["proposed_decision"] == "MRN im Dokumentensatz oder Mailverlauf prüfen und bei belastbarer Evidenz in den operativen Review übernehmen"



def test_build_tms_pending_updates_bundles_document_quality_noise_into_single_review_hint():
    tms_snapshot = {
        "shipment_uuid": "uuid-AN-BUNDLE",
        "shipment_number": "AN-BUNDLE",
        "status": "confirmed",
        "detail": {"status": "confirmed", "network": "sea", "dates": {}},
    }
    document_registry = {
        "received_types": ["customs_document"],
        "expected_types": ["customs_document"],
        "missing_types": [],
        "analysis_open_questions": [],
        "tms_match_summary": [],
        "analyzed_documents": [
            {
                "filename": "CARGOLO_Zollvollmacht Export (4).pdf",
                "doc_type": "customs_document",
                "operational_flags": ["Das Dokument muss vom Kunden noch ausgefüllt und unterschrieben werden."],
                "missing_or_unreadable": [
                    "Unterschrift des Ausstellers fehlt (Blanko-Formular)",
                    "Adressdaten des Ausstellers nicht ausgefüllt",
                    "EORI-Nummer nicht eingetragen",
                ],
                "analysis_path": "/tmp/doc_customs.json",
            }
        ],
    }

    payload = _build_tms_pending_updates(
        order_id="AN-BUNDLE",
        tms_snapshot=tms_snapshot,
        history_rows=[],
        document_registry=document_registry,
    )

    review_hints = [row for row in payload["pending_actions"] if row["action_type"] == "review_hint"]
    assert len(review_hints) == 1
    bundled = review_hints[0]
    assert bundled["target"] == "documents.review.customs_template_customer_completion"
    assert bundled["review_topic"] == "customs_preparation"
    assert "CARGOLO_Zollvollmacht Export (4).pdf" in bundled["evidence"]



def test_bootstrap_case_creates_baseline_without_fake_email(tmp_path):
    order_id = "AN-12001"
    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle") as mock_fetch, \
         patch("plugins.cargolo_ops.processor._sync_mail_history", return_value=3), \
         patch("plugins.cargolo_ops.processor._add_transport_internal_note", return_value={"status": "applied", "preview": "bootstrap kommentar", "error": None}), \
         patch("plugins.cargolo_ops.processor.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        mock_fetch.return_value = (
            _mock_snapshot(order_id),
            _mock_doc_requirements(order_id),
            _mock_billing_context(order_id),
        )

        result = bootstrap_case(order_id, storage_root=tmp_path, refresh_history=False, write_internal_note=True)

    assert result.status == "bootstrapped"
    assert result.internal_note_status == "applied"
    assert result.internal_note_preview == "bootstrap kommentar"
    case_root = tmp_path / "orders" / order_id
    assert (case_root / "case_state.json").exists()
    assert (case_root / "documents" / "registry.json").exists()
    assert (case_root / "tms_snapshot.json").exists()
    assert (case_root / "summary.txt").exists()
    assert (case_root / "bootstrap_summary.json").exists()
    state = json.loads((case_root / "case_state.json").read_text(encoding="utf-8"))
    assert state["customer_name"] == "Test Kunde GmbH"
    assert state["customer_reference"] == f"REF-{order_id}"
    assert state["mode"] == "ocean"
    assert state["documents_expected"] == ["commercial_invoice", "packing_list"]
    assert state["missing_information"] == ["document:commercial_invoice", "document:packing_list"]
    assert state["next_best_action"] == "Commercial Invoice / Packing List gegen Mail- und TMS-Stand prüfen"
    assert state["task_reason"] == "missing_documents:commercial_invoice,packing_list"
    registry = json.loads((case_root / "documents" / "registry.json").read_text(encoding="utf-8"))
    assert registry["expected_types"] == ["commercial_invoice", "packing_list"]
    assert registry["missing_types"] == ["commercial_invoice", "packing_list"]
    pending_updates = json.loads((case_root / "tms" / "pending_updates.json").read_text(encoding="utf-8"))
    assert pending_updates["order_id"] == order_id
    assert pending_updates["pending_actions"]
    assert {row["target"] for row in pending_updates["pending_actions"] if row["action_type"] == "document_gap"} == {
        "documents.commercial_invoice",
        "documents.packing_list",
    }
    writeback_queue = json.loads((tmp_path / "tms_writeback_queue.json").read_text(encoding="utf-8"))
    assert writeback_queue["summary"]["pending_orders"] == 1
    assert writeback_queue["summary"]["pending_actions"] == len(pending_updates["pending_actions"])
    assert writeback_queue["orders"][0]["order_id"] == order_id
    assert {row["target"] for row in writeback_queue["orders"][0]["pending_actions"]} == {row["target"] for row in pending_updates["pending_actions"]}
    applied_updates = json.loads((case_root / "tms" / "applied_updates.json").read_text(encoding="utf-8"))
    assert applied_updates["order_id"] == order_id
    assert applied_updates["status"] == "awaiting_write_access"
    assert applied_updates["applied_actions"] == []
    case_report = json.loads((case_root / "analysis" / "case_report_latest.json").read_text(encoding="utf-8"))
    assert case_report["order_id"] == order_id
    assert case_report["sections"]["tms_mcp"]["shipment"]["shipment_number"]["value"] == order_id
    assert case_report["sections"]["mail_history"]["email_count_total"]["value"] == 0
    assert case_report["sections"]["documents"]["missing_types"]["value"] == ["commercial_invoice", "packing_list"]
    summary_payload = json.loads((case_root / "bootstrap_summary.json").read_text(encoding="utf-8"))
    assert summary_payload["bootstrap"]["history_sync_count"] == 3
    assert summary_payload["documents"]["expected_types"] == ["commercial_invoice", "packing_list"]
    assert summary_payload["tms"]["origin"]["city"] == "Hamburg"
    assert summary_payload["comparison"]["history_email_count_total"] == 0
    assert summary_payload["comparison"]["history_matches_shipment_number"] is False
    assert summary_payload["comparison"]["tms_customer_available"] is True
    assert "customer_present_but_history_empty" in summary_payload["comparison"]["findings"]
    summary_txt = (case_root / "summary.txt").read_text(encoding="utf-8")
    assert "Erwartete Dokumente: commercial_invoice, packing_list" in summary_txt
    assert "Abgleich Mailhistorie vs. TMS:" in summary_txt
    assert "- customer_present_but_history_empty" in summary_txt
    email_index = (case_root / "email_index.jsonl").read_text(encoding="utf-8").strip()
    assert email_index == ""


def test_bootstrap_case_always_attempts_mail_history_sync(tmp_path):
    order_id = "AN-12002"
    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle") as mock_fetch, \
         patch("plugins.cargolo_ops.processor._sync_mail_history", return_value=0) as mock_history, \
         patch("plugins.cargolo_ops.processor.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        mock_fetch.return_value = (
            _mock_snapshot(order_id),
            _mock_doc_requirements(order_id),
            _mock_billing_context(order_id),
        )
        bootstrap_case(order_id, storage_root=tmp_path, refresh_history=False)

    mock_history.assert_called_once()


def test_bootstrap_case_surfaces_mail_history_sync_errors_in_open_questions(tmp_path):
    order_id = "AN-12003"
    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle") as mock_fetch, \
         patch("plugins.cargolo_ops.processor._sync_mail_history", side_effect=RuntimeError("n8n mail history workflow OOM")), \
         patch("plugins.cargolo_ops.processor.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        mock_fetch.return_value = (
            _mock_snapshot(order_id),
            _mock_doc_requirements(order_id),
            _mock_billing_context(order_id),
        )
        result = bootstrap_case(order_id, storage_root=tmp_path, refresh_history=False)

    assert result.history_sync_count == 0
    state = json.loads((tmp_path / "orders" / order_id / "case_state.json").read_text(encoding="utf-8"))
    assert any("mail_history_sync_failed" in item for item in state["open_questions"])
    assert any("OOM" in item for item in state["open_questions"])
    summary_payload = json.loads((tmp_path / "orders" / order_id / "bootstrap_summary.json").read_text(encoding="utf-8"))
    assert any("mail_history_sync_failed" in item for item in summary_payload["case_state"]["open_questions"])


def test_bootstrap_case_executes_write_now_actions_immediately(tmp_path):
    order_id = "AN-WRITE"
    pending_updates_payload = {
        "version": 1,
        "generated_at": "2026-04-19T00:00:00Z",
        "order_id": order_id,
        "shipment_uuid": f"uuid-{order_id}",
        "shipment_number": order_id,
        "status": "pending_write_access",
        "requires_write_access": True,
        "received_types": [],
        "expected_types": [],
        "missing_types": [],
        "document_matches": [],
        "field_update_candidates": [],
        "open_questions": [],
        "action_summary": {"write_now": 1, "review": 0, "not_yet_due": 0, "not_yet_knowable": 0},
        "pending_actions": [
            {
                "action_type": "field_update",
                "target": "shipment.dates.latest_delivery_date",
                "suggested_value": "2026-05-20",
                "source": "tms.detail.dates.estimated_delivery_date",
                "reason": "clear evidence",
                "requires_write_access": True,
                "action_status": "write_now",
            }
        ],
    }
    applied_calls = []

    def _fake_apply(action, context, *, admin_user_id=106):
        applied_calls.append({"action": dict(action), "context": dict(context), "admin_user_id": admin_user_id})
        return {"status": "applied", "executed_tool": "fake_field_update"}

    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle") as mock_fetch, \
         patch("plugins.cargolo_ops.processor._sync_mail_history", return_value=0), \
         patch("plugins.cargolo_ops.processor._build_tms_pending_updates", return_value=pending_updates_payload), \
         patch("plugins.cargolo_ops.processor.apply_pending_tms_action", side_effect=_fake_apply), \
         patch("plugins.cargolo_ops.processor._add_transport_internal_note", return_value={"status": "applied", "preview": "bootstrap kommentar", "error": None}), \
         patch("plugins.cargolo_ops.processor.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        mock_fetch.return_value = (
            _mock_snapshot(order_id),
            _mock_doc_requirements(order_id),
            _mock_billing_context(order_id),
        )
        result = bootstrap_case(order_id, storage_root=tmp_path, refresh_history=False, write_internal_note=True)

    assert result.applied_action_summary == {"applied": 1, "failed": 0, "skipped": 0}
    assert applied_calls and applied_calls[0]["action"]["target"] == "shipment.dates.latest_delivery_date"
    applied_updates = json.loads((tmp_path / "orders" / order_id / "tms" / "applied_updates.json").read_text(encoding="utf-8"))
    assert applied_updates["status"] == "applied"
    assert len(applied_updates["applied_actions"]) == 1


def test_save_tms_pending_updates_creates_global_review_queue(tmp_path):
    store = CaseStore(tmp_path)
    payload = {
        "generated_at": "2026-04-19T00:00:00Z",
        "order_id": "AN-REVIEW",
        "shipment_uuid": "uuid-AN-REVIEW",
        "shipment_number": "AN-REVIEW",
        "status": "pending_write_access",
        "requires_write_access": True,
        "pending_actions": [
            {
                "action_type": "review_hint",
                "target": "documents.review.document_quality_bundle",
                "suggested_value": "review_required",
                "source": "documents.analysis.latest_summary.json",
                "reason": "Dokumentqualität prüfen",
                "requires_write_access": False,
                "action_status": "review",
                "review_topic": "document_quality",
                "priority": "low",
                "evidence": ["doc.pdf"],
                "proposed_decision": "Prüfen",
            },
            {
                "action_type": "review_hint",
                "target": "documents.review.mrn_missing_with_t1_reference",
                "suggested_value": "review_required",
                "source": "documents.analysis.latest_summary.json",
                "reason": "T1 erwähnt, MRN aber nicht sichtbar.",
                "requires_write_access": False,
                "action_status": "review",
                "review_topic": "customs",
                "priority": "medium",
                "evidence": ["TourNr_135192499.pdf", "T1-Versandverfahren"],
                "proposed_decision": "MRN prüfen",
            },
            {
                "action_type": "document_gap",
                "target": "documents.bill_of_lading",
                "suggested_value": "missing_after_mail_tms_reconciliation",
                "source": "document_registry.missing_types",
                "reason": "BL fehlt",
                "requires_write_access": True,
                "action_status": "review",
            },
            {
                "action_type": "document_gap",
                "target": "documents.customs_document",
                "suggested_value": "missing_after_mail_tms_reconciliation",
                "source": "document_registry.missing_types",
                "reason": "Zolldokument fehlt",
                "requires_write_access": True,
                "action_status": "review",
            },
        ],
        "action_summary": {
            "write_now": 0,
            "review": 4,
            "not_yet_due": 0,
            "not_yet_knowable": 0,
        },
    }

    store.save_tms_pending_updates("AN-REVIEW", payload, "pending")

    review_queue = json.loads((tmp_path / "review_queue.json").read_text(encoding="utf-8"))
    assert review_queue["summary"]["review_orders"] == 1
    assert review_queue["summary"]["review_actions"] == 4
    assert review_queue["summary"]["high_priority"] == 1
    assert review_queue["summary"]["medium_priority"] == 2
    assert review_queue["summary"]["low_priority"] == 1
    assert review_queue["orders"][0]["order_id"] == "AN-REVIEW"
    assert [row["target"] for row in review_queue["orders"][0]["review_actions"]] == [
        "documents.bill_of_lading",
        "documents.customs_document",
        "documents.review.mrn_missing_with_t1_reference",
        "documents.review.document_quality_bundle",
    ]
    assert review_queue["orders"][0]["highest_priority"] == "high"
    assert review_queue["review_actions"][0]["target"] == "documents.bill_of_lading"
    assert review_queue["review_actions"][1]["target"] == "documents.customs_document"
    assert review_queue["review_actions"][2]["review_topic"] == "customs"



def test_global_writeback_queue_removes_order_when_pending_actions_are_empty(tmp_path):
    store = CaseStore(tmp_path)
    payload_with_actions = {
        "generated_at": "2026-04-19T00:00:00Z",
        "order_id": "AN-12002",
        "shipment_uuid": "uuid-AN-12002",
        "shipment_number": "AN-12002",
        "status": "pending_write_access",
        "requires_write_access": True,
        "pending_actions": [
            {
                "action_type": "field_update",
                "target": "shipment.dates.estimated_delivery_date",
                "suggested_value": "2026-04-20",
                "source": "mail+legs",
                "reason": "Date mismatch",
                "requires_write_access": True,
            }
        ],
    }
    payload_without_actions = {
        **payload_with_actions,
        "generated_at": "2026-04-19T01:00:00Z",
        "pending_actions": [],
    }

    store.save_tms_pending_updates("AN-12002", payload_with_actions, "with actions")
    queue_after_add = json.loads((tmp_path / "tms_writeback_queue.json").read_text(encoding="utf-8"))
    assert queue_after_add["summary"]["pending_orders"] == 1
    assert queue_after_add["orders"][0]["order_id"] == "AN-12002"

    store.save_tms_pending_updates("AN-12002", payload_without_actions, "without actions")
    queue_after_remove = json.loads((tmp_path / "tms_writeback_queue.json").read_text(encoding="utf-8"))
    assert queue_after_remove["summary"]["pending_orders"] == 0
    assert queue_after_remove["summary"]["pending_actions"] == 0
    assert queue_after_remove["orders"] == []


def test_bootstrap_case_stores_history_attachments(tmp_path):
    order_id = "AN-12003"
    message_row = {
        "message_id": "<hist-1@example.com>",
        "conversation_id": "thread-1",
        "subject": f"History for {order_id}",
        "from": "ops@example.com",
        "to": ["asr@cargolo.com"],
        "cc": [],
        "received_at": "2026-04-18T20:00:00Z",
        "body_text": "Attached docs",
        "attachments": [
            {
                "filename": "invoice.pdf",
                "mime_type": "application/pdf",
                "content_base64": "aGVsbG8=",
            }
        ],
        "attachment_count": 1,
        "has_attachments": True,
    }
    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle") as mock_fetch, \
         patch("plugins.cargolo_ops.processor.build_mail_history_client_from_env") as mock_history_client_factory, \
         patch("plugins.cargolo_ops.processor.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        mock_fetch.return_value = (
            _mock_snapshot(order_id),
            _mock_doc_requirements(order_id),
            _mock_billing_context(order_id),
        )
        mock_client = MagicMock()
        mock_client.fetch_history.return_value = {"messages": [message_row]}
        mock_history_client_factory.return_value = mock_client

        result = bootstrap_case(order_id, storage_root=tmp_path, refresh_history=True)

    assert result.status == "bootstrapped"
    case_root = tmp_path / "orders" / order_id
    assert (case_root / "documents" / "inbound" / "invoice.pdf").exists()
    email_index = (case_root / "email_index.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(email_index) == 1
    row = json.loads(email_index[0])
    assert any(path.endswith("invoice.pdf") for path in row["stored_paths"])
    registry = json.loads((case_root / "documents" / "registry.json").read_text(encoding="utf-8"))
    assert len(registry["received_documents"]) == 1
    assert registry["received_documents"][0]["filename"] == "invoice.pdf"


def test_bootstrap_case_deduplicates_identical_history_attachments(tmp_path):
    order_id = "AN-12003D"
    msg_a = {
        "message_id": "<hist-a@example.com>",
        "conversation_id": "thread-1",
        "subject": f"History for {order_id}",
        "from": "ops@example.com",
        "to": ["asr@cargolo.com"],
        "cc": [],
        "received_at": "2026-04-18T20:00:00Z",
        "body_text": "Attached docs",
        "attachments": [{"filename": "invoice.pdf", "mime_type": "application/pdf", "content_base64": "aGVsbG8="}],
        "attachment_count": 1,
        "has_attachments": True,
    }
    msg_b = {
        "message_id": "<hist-b@example.com>",
        "conversation_id": "thread-1",
        "subject": f"Re: History for {order_id}",
        "from": "ops@example.com",
        "to": ["asr@cargolo.com"],
        "cc": [],
        "received_at": "2026-04-18T21:00:00Z",
        "body_text": "Attached docs again",
        "attachments": [{"filename": "invoice-copy.pdf", "mime_type": "application/pdf", "content_base64": "aGVsbG8="}],
        "attachment_count": 1,
        "has_attachments": True,
    }
    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle") as mock_fetch, \
         patch("plugins.cargolo_ops.processor.build_mail_history_client_from_env") as mock_history_client_factory, \
         patch("plugins.cargolo_ops.processor.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        mock_fetch.return_value = (
            _mock_snapshot(order_id),
            _mock_doc_requirements(order_id),
            _mock_billing_context(order_id),
        )
        mock_client = MagicMock()
        mock_client.fetch_history.return_value = {"messages": [msg_a, msg_b]}
        mock_history_client_factory.return_value = mock_client

        bootstrap_case(order_id, storage_root=tmp_path, refresh_history=True)

    case_root = tmp_path / "orders" / order_id
    inbound_files = sorted((case_root / "documents" / "inbound").glob("*"))
    assert len(inbound_files) == 1
    registry = json.loads((case_root / "documents" / "registry.json").read_text(encoding="utf-8"))
    assert len(registry["received_documents"]) == 1


def test_bootstrap_case_records_subagent_analysis_metadata_and_syncs_repo(tmp_path):
    order_id = "AN-12003A"
    message_row = {
        "message_id": "<hist-1@example.com>",
        "conversation_id": "thread-1",
        "subject": f"History for {order_id}",
        "from": "ops@example.com",
        "to": ["asr@cargolo.com"],
        "cc": [],
        "received_at": "2026-04-18T20:00:00Z",
        "body_text": "Attached docs",
        "attachments": [],
        "attachment_count": 0,
        "has_attachments": False,
    }
    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle") as mock_fetch, \
         patch("plugins.cargolo_ops.processor.build_mail_history_client_from_env") as mock_history_client_factory, \
         patch("plugins.cargolo_ops.processor.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])), \
         patch("plugins.cargolo_ops.processor.run_postprocess_subagent_analysis", return_value=("completed", "/tmp/bootstrap/brief.json", "high", "Bootstrap-Analyse erfolgreich")) as mock_analysis, \
         patch("plugins.cargolo_ops.processor._sync_orders_repo_immediately") as mock_sync:
        mock_fetch.return_value = (
            _mock_snapshot(order_id),
            _mock_doc_requirements(order_id),
            _mock_billing_context(order_id),
        )
        mock_client = MagicMock()
        mock_client.fetch_history.return_value = {"messages": [message_row]}
        mock_history_client_factory.return_value = mock_client

        result = bootstrap_case(order_id, storage_root=tmp_path, refresh_history=True)

    assert result.status == "bootstrapped"
    assert result.analysis_status == "completed"
    assert result.analysis_brief_path == "/tmp/bootstrap/brief.json"
    assert result.analysis_priority == "high"
    assert result.analysis_summary == "Bootstrap-Analyse erfolgreich"
    assert result.history_sync_status == "ok"
    assert result.pending_updates_path.endswith("/tms/pending_updates.json")
    assert result.applied_updates_path.endswith("/tms/applied_updates.json")
    assert result.case_report_path.endswith("/analysis/case_report_latest.json")
    mock_analysis.assert_called_once()
    mock_sync.assert_called_once_with(order_id)


def test_derive_precise_update_candidates_returns_field_level_updates():
    order_id = "AN-12005"
    tms_snapshot = {
        "shipment_number": order_id,
        "detail": {
            "milestones": {
                "atd_main_carriage": None,
            },
            "dates": {
                "latest_delivery_date": "2026-04-16",
                "estimated_delivery_date": "2026-05-02",
            },
            "destination": {"city": "Duisburg"},
            "cargo": [
                {"quantity": 21, "weight_kg": 2709},
                {"quantity": 14, "weight_kg": 966.3},
            ],
        },
        "totals": {
            "total_weight_kg": 70417.2,
            "total_volume_m3": 0,
        },
        "billing_items": [
            {"id": 1206, "name": "Nachlauf ab Terminal Hamburg bis XXX, leer XXX"},
        ],
        "raw": {
            "transport_legs": [
                {"leg_type": "main_carriage", "atd": 1776038400000},
            ],
            "internal_notes": [
                {"content": "[Cargo-Daten] Gesamt: 3.675,3 kg, 30,58 cbm."},
            ],
        },
    }
    history_rows = [
        {"subject": f"{order_id} // Invoice sent", "sender": "ops@example.com"}
    ]

    candidates = _derive_precise_update_candidates(
        order_id=order_id,
        tms_snapshot=tms_snapshot,
        history_rows=history_rows,
        document_registry={"missing_types": ["commercial_invoice"]},
    )

    by_field = {row["field"]: row for row in candidates}
    assert by_field["shipment.milestones.atd_main_carriage"]["suggested_value"] == "2026-04-13T00:00:00Z"
    assert by_field["shipment.totals.total_weight_kg"]["suggested_value"] == 3675.3
    assert by_field["shipment.totals.total_volume_m3"]["suggested_value"] == 30.58
    assert by_field["billing_items[1206].name"]["suggested_value"] == "Nachlauf ab Terminal Hamburg bis Duisburg, leer XXX"
    assert by_field["shipment.dates.latest_delivery_date"]["suggested_value"] == "2026-05-02"
    assert by_field["documents.commercial_invoice"]["suggested_value"] == "import_from_mail_history_attachment"


def test_derive_precise_update_candidates_extracts_reliable_mail_references_for_write_now_fields():
    candidates = _derive_precise_update_candidates(
        order_id="AN-REFS",
        tms_snapshot={
            "detail": {
                "network": "sea",
                "customs": {},
                "freight_details": {},
                "carrier": {},
            },
        },
        history_rows=[
            {
                "subject": "AN-REFS container TGHU1234567 / HAWB 020-12345675 / MRN 25DE1234567890ABCDE ready",
                "body_text": "Container TGHU1234567 confirmed. HAWB: 020-12345675. MRN: 25DE1234567890ABCDE.",
                "sender": "ops@example.com",
            }
        ],
        document_registry={"missing_types": []},
    )

    by_field = {row["field"]: row for row in candidates if row.get("field")}
    assert by_field["shipment.freight_details.container_number"]["suggested_value"] == "TGHU1234567"
    assert by_field["shipment.freight_details.hawb_number"]["suggested_value"] == "020-12345675"
    assert by_field["shipment.customs.customs_reference"]["suggested_value"] == "25DE1234567890ABCDE"



def test_derive_precise_update_candidates_extracts_tracking_and_carrier_refs_from_mail():
    candidates = _derive_precise_update_candidates(
        order_id="AN-TRACK",
        tms_snapshot={
            "detail": {
                "network": "road",
                "carrier": {},
            },
        },
        history_rows=[
            {
                "subject": "AN-TRACK tracking 1Z999AA10123456784 / consignment 76543210 / carrier ref CR-9988",
                "body_text": "Tracking number: 1Z999AA10123456784. Consignment no 76543210. Carrier reference CR-9988.",
                "sender": "ops@example.com",
            }
        ],
        document_registry={"missing_types": []},
    )

    by_field = {row["field"]: row for row in candidates if row.get("field")}
    assert by_field["shipment.carrier.tracking_number"]["suggested_value"] == "1Z999AA10123456784"
    assert "shipment.carrier.consignment_number" not in by_field
    assert by_field["shipment.carrier.carrier_reference"]["suggested_value"] == "CR-9988"


def test_derive_precise_update_candidates_returns_transport_leg_update_for_placeholder_destination():
    candidates = _derive_precise_update_candidates(
        order_id="AN-12317",
        tms_snapshot={
            "detail": {
                "destination": {"city": "x", "country": "CH"},
                "transport_legs": [
                    {"leg_type": "pre_carriage", "sort_order": 1, "destination": "Hub"},
                    {"leg_type": "on_carriage", "leg_uuid": "leg-on-1", "sort_order": 3, "destination": "x"},
                ],
            },
        },
        history_rows=[],
        document_registry={
            "missing_types": [],
            "analysis_open_questions": [
                "Vollständiger Zielort-Name (nur 'x' im TMS, im Dokument als Bazenheid identifiziert)"
            ],
        },
    )

    leg_candidate = next(row for row in candidates if row.get("action_type") == "transport_leg_update")
    assert leg_candidate["target"] == "transport_leg.destination_name"
    assert leg_candidate["suggested_value"] == "Bazenheid"
    assert leg_candidate["tool_args"]["leg_uuid"] == "leg-on-1"
    assert leg_candidate["tool_args"]["destination_country_code"] == "CH"


def test_bootstrap_case_writes_detailed_reconciliation_summary(tmp_path):
    order_id = "AN-12004"
    snapshot = _mock_snapshot(order_id)
    snapshot.detail["transport_legs"] = [
        {
            "leg_type": "pre_carriage",
            "transport_mode": "truck",
            "origin": "Hamburg",
            "destination": "Zhengzhou",
            "etd": 0,
            "eta": 0,
            "status": "completed",
            "carrier": "Test Carrier",
        },
        {
            "leg_type": "main_carriage",
            "transport_mode": "rail",
            "origin": "Zhengzhou",
            "destination": "Shanghai",
            "etd": 1775952000000,
            "eta": 1777680000000,
            "status": "in_transit",
            "carrier": "Rail Carrier",
        },
    ]
    snapshot.detail["milestones"] = {
        "etd_main_carriage": 1775952000000,
        "eta_main_carriage": 1777680000000,
        "atd_main_carriage": None,
        "ata_main_carriage": None,
    }
    snapshot.billing_items = [
        {
            "name": "Nachlauf ab Terminal Hamburg bis XXX, leer XXX",
            "hint": "Placeholder pricing",
            "quantity": 1,
            "unit": "Sendung",
            "vk_price": 100,
            "ek_price": 90,
            "sort_order": 1,
            "source": "library",
            "invoiced": False,
        }
    ]
    message_rows = [
        {
            "message_id": "<hist-a@example.com>",
            "conversation_id": "thread-1",
            "subject": f"{order_id} update rail departure",
            "from": "ops@example.com",
            "to": ["asr@cargolo.com"],
            "cc": [],
            "received_at": "2026-04-18T20:00:00Z",
            "body_text": "Rail departure update",
            "attachments": [],
            "attachment_count": 0,
            "has_attachments": False,
        },
        {
            "message_id": "<hist-b@example.com>",
            "conversation_id": "thread-1",
            "subject": f"Re: {order_id} arrival planning",
            "from": "customer@example.com",
            "to": ["asr@cargolo.com"],
            "cc": [],
            "received_at": "2026-04-19T09:15:00Z",
            "body_text": "Arrival planning",
            "attachments": [],
            "attachment_count": 0,
            "has_attachments": False,
        },
    ]
    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle") as mock_fetch, \
         patch("plugins.cargolo_ops.processor.build_mail_history_client_from_env") as mock_history_client_factory, \
         patch("plugins.cargolo_ops.processor.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        mock_fetch.return_value = (
            snapshot,
            _mock_doc_requirements(order_id),
            _mock_billing_context(order_id),
        )
        mock_client = MagicMock()
        mock_client.fetch_history.return_value = {"messages": message_rows}
        mock_history_client_factory.return_value = mock_client

        bootstrap_case(order_id, storage_root=tmp_path, refresh_history=True)

    case_root = tmp_path / "orders" / order_id
    summary_payload = json.loads((case_root / "bootstrap_summary.json").read_text(encoding="utf-8"))
    assert summary_payload["reconciliation"]["mail_history"]["first_received_at"] == "2026-04-18T20:00:00Z"
    assert summary_payload["reconciliation"]["mail_history"]["last_received_at"] == "2026-04-19T09:15:00Z"
    assert summary_payload["reconciliation"]["mail_history"]["email_count_total"] == 2
    assert summary_payload["reconciliation"]["tms_transport"]["transport_leg_count"] == 2
    assert summary_payload["reconciliation"]["tms_transport"]["legs"][0]["leg_type"] == "pre_carriage"
    assert "billing_contains_placeholder_xxx" in summary_payload["reconciliation"]["integrity_findings"]
    assert "transport_legs_missing_schedule_data" in summary_payload["reconciliation"]["integrity_findings"]
    case_report = json.loads((case_root / "analysis" / "case_report_latest.json").read_text(encoding="utf-8"))
    assert case_report["sections"]["mail_history"]["first_received_at"]["value"] == "2026-04-18T20:00:00Z"
    assert case_report["sections"]["reconciliation"]["integrity_findings"]["value"]
    sync_log_rows = (case_root / "tms" / "sync_log.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert sync_log_rows
    latest_sync = json.loads(sync_log_rows[-1])
    assert latest_sync["phase"] == "planning_artifacts_created"
    assert latest_sync["pending_updates_path"].endswith("tms/pending_updates.json")
    assert latest_sync["applied_updates_path"].endswith("tms/applied_updates.json")
    summary_txt = (case_root / "summary.txt").read_text(encoding="utf-8")
    assert "Detaillierter Abgleich Mailverlauf vs. TMS:" in summary_txt
    assert "- Transportlegs laut TMS: 2" in summary_txt
    assert "- billing_contains_placeholder_xxx" in summary_txt


def test_bootstrap_cases_from_tms_loops_shipments_and_collects_results(tmp_path):
    provider = MagicMock()
    provider.shipments_list.side_effect = [
        [
            {"shipment_number": "AN-12001"},
            {"shipment_number": "AN-12002"},
        ],
        [],
    ]

    with patch("plugins.cargolo_ops.processor.build_tms_provider_from_env", return_value=provider), \
         patch("plugins.cargolo_ops.processor.bootstrap_case") as mock_bootstrap:
        mock_bootstrap.side_effect = [
            MagicMock(model_dump=lambda mode="json": {"status": "bootstrapped", "order_id": "AN-12001"}),
            MagicMock(model_dump=lambda mode="json": {"status": "bootstrapped", "order_id": "AN-12002"}),
        ]
        result = bootstrap_cases_from_tms(storage_root=tmp_path, refresh_history=False, limit=2)

    assert result["status"] == "ok"
    assert result["total_selected"] == 2
    assert result["success_count"] == 2
    assert result["error_count"] == 0
    assert [row["order_id"] for row in result["results"]] == ["AN-12001", "AN-12002"]
    assert mock_bootstrap.call_count == 2
