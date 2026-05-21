from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from plugins.cargolo_ops.document_monitoring import run_document_monitoring, _shipment_context
from plugins.cargolo_ops.document_reconciliation import reconcile_documents
from plugins.cargolo_ops.document_activity_monitor import (
    _processor_result_from_report,
    _queue_document_review_card_results,
    _queue_document_review_cards,
    _writeback_metadata_for_intent,
    run_document_activity_monitor,
)
from plugins.cargolo_ops.document_schema import normalize_mode


def _processor_result_for_uploaded_fields(
    tmp_path,
    *,
    filename="document.pdf",
    event_doc_type="master_bl",
    analysis_doc_type="bill_of_lading",
    extracted_fields=None,
    references=None,
    field_sources=None,
    findings=None,
    freight_details=None,
    transport_legs=None,
):
    analysis_path = tmp_path / f"{filename}.analysis.json"
    registry_path = tmp_path / f"{filename}.registry.json"
    snapshot_path = tmp_path / f"{filename}.snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": filename,
                "doc_type": analysis_doc_type,
                "summary": "Uploaded document under test",
                "references": references or [],
                "extracted_fields": extracted_fields or {},
                "field_sources": field_sources or {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "analyzed_documents": [
                    {"filename": filename, "analysis_path": str(analysis_path), "analysis_doc_type": analysis_doc_type}
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "freight_details": freight_details or {"pol_code": "CNNGB", "pod_code": "DEHAM", "mbl_number": "", "container_number": ""},
                    "transport_legs": transport_legs if transport_legs is not None else [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return _processor_result_from_report(
        {
            "order_id": "AN-11790",
            "case_root": str(tmp_path / "orders" / "AN-11790"),
            "tms_context": {"status": "customs_pending", "network": "sea"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": findings or []},
        },
        {"id": 9001, "changed_at": "2026-05-11T10:00:00Z", "metadata": {"file_name": filename, "document_type": event_doc_type}},
    )


def test_processor_resolves_archived_email_upload_to_latest_analyzed_attachment(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_CMD", raising=False)
    offer_analysis = tmp_path / "offer.analysis.json"
    booking_analysis = tmp_path / "booking.analysis.json"
    registry_path = tmp_path / "registry.json"
    snapshot_path = tmp_path / "snapshot.json"
    offer_analysis.write_text(
        json.dumps(
            {
                "filename": "Angebot-AN-13416-V1.pdf",
                "doc_type": "offer",
                "extracted_fields": {"document_type": "Angebot", "document_number": "AN-13416-V1", "amount": "2380.99", "currency": "EUR"},
                "operational_flags": ["Embargoprüfung erforderlich"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    booking_analysis.write_text(
        json.dumps(
            {
                "filename": "Transportauftrag-AN-13416.pdf",
                "doc_type": "offer",
                "extracted_fields": {
                    "document_type": "Transportauftrag",
                    "document_number": "AN-13416",
                    "shipment_number": "AN-13416",
                    "customer_reference": "CGL-20260313-130927",
                    "etd": "2026-05-31",
                    "eta": "2026-06-20",
                    "gross_weight": "1669,00",
                    "volume": "7,4052",
                },
                "field_sources": {"etd": {"value": "2026-05-31", "label": "ETD", "confidence": "high", "raw_context": "ETD 31.05.2026"}},
                "operational_flags": ["Bahnfracht (RAIL)", "Incoterm EXW bestätigt"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "received_documents": [
                    {"filename": "Angebot-AN-13416-V1.pdf", "received_at": "2026-05-20T07:12:58Z", "analysis_path": str(offer_analysis)},
                    {"filename": "Transportauftrag-AN-13416.pdf", "received_at": "2026-05-20T07:34:58Z", "analysis_path": str(booking_analysis)},
                ],
                "analyzed_documents": [
                    {"filename": "Angebot-AN-13416-V1.pdf", "analysis_path": str(offer_analysis), "analysis_doc_type": "offer"},
                    {"filename": "Transportauftrag-AN-13416.pdf", "analysis_path": str(booking_analysis), "analysis_doc_type": "offer"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps({"detail": {"transport_legs": [{"leg_type": "main_carriage", "etd": "2026-05-31"}]}}), encoding="utf-8")

    result = _processor_result_from_report(
        {
            "order_id": "AN-13416",
            "tms_context": {"status": "pickup_scheduled", "network": "rail"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {
                "risk": "medium",
                "needs_human_review": True,
                "findings": [
                    {"type": "document_flag", "severity": "medium", "filename": "Angebot-AN-13416-V1.pdf", "summary": "Embargoprüfung erforderlich"},
                    {"type": "document_flag", "severity": "medium", "filename": "Transportauftrag-AN-13416.pdf", "summary": "Bahnfracht (RAIL)"},
                ],
            },
            "trigger_event": {"metadata": {"file_name": "AW_ AN-13416 __ Neu Buchung Bahnfracht KW 21 .msg", "document_type": "email"}},
        },
        {"id": 1915, "changed_at": "2026-05-20T07:41:52Z", "metadata": {"file_name": "AW_ AN-13416 __ Neu Buchung Bahnfracht KW 21 .msg", "document_type": "email"}},
    )

    message = result["message"]
    assert result["latest_subject"] == "Transportauftrag-AN-13416.pdf"
    assert "nicht belastbar auslesbar" not in message
    assert "Embargoprüfung" not in message
    assert result["document_agent_evidence_packet"]["document"]["filename"] == "Transportauftrag-AN-13416.pdf"
    assert all(finding.get("filename") != "Angebot-AN-13416-V1.pdf" for finding in result["document_reconciliation"]["findings"])


def test_processor_resolves_tms_mirror_upload_by_sha_and_writes_world_class_invoice_message(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_CMD", raising=False)
    sha = "c3a89c1d34819aac9f1ad200ba59565185c0f2e36263bbb453e8d551d2eb7e59"
    analysis_path = tmp_path / "ci.analysis.json"
    registry_path = tmp_path / "registry.json"
    snapshot_path = tmp_path / "snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": "CI.PDF",
                "doc_type": "commercial_invoice",
                "summary": "Handelsrechnung über Displays im Gesamtwert von 6.810 USD auf Basis EXW.",
                "extracted_fields": {
                    "document_type": "Commercial Invoice",
                    "invoice_number": "20250402001",
                    "document_number": "20250402001",
                    "shipment_number": "AN-12258",
                    "customer": "Fitness Nation GmbH",
                    "incoterm_named_place": "EXW",
                    "pieces": "12",
                    "goods_description": "65-inch vertical advertising display",
                    "goods_value": "6810",
                    "currency": "USD",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    other_analysis_path = tmp_path / "packing.analysis.json"
    other_analysis_path.write_text(
        json.dumps({"filename": "PACKING LIST.PDF", "doc_type": "packing_list", "extracted_fields": {"pieces": "12"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "received_documents": [
                    {"filename": "CI.PDF", "sha256": sha, "analysis_path": str(analysis_path), "analysis_doc_type": "commercial_invoice"},
                    {"filename": "PACKING LIST.PDF", "sha256": "other-sha", "analysis_path": str(other_analysis_path), "analysis_doc_type": "packing_list"},
                ],
                "analyzed_documents": [
                    {"filename": "CI.PDF", "analysis_path": str(analysis_path), "analysis_doc_type": "commercial_invoice"},
                    {"filename": "PACKING LIST.PDF", "analysis_path": str(other_analysis_path), "analysis_doc_type": "packing_list"},
                ],
                "tms_documents": [
                    {
                        "tms_document_id": "f8d2221a-5598-4e63-9a06-b380ac2b6e42",
                        "document_id": 60359,
                        "filename": "CI(4).PDF",
                        "sha256": sha,
                        "analysis_path": str(analysis_path),
                        "analysis_doc_type": "commercial_invoice",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps({"detail": {"totals": {"total_pieces": 12}, "freight_details": {}, "transport_legs": []}}), encoding="utf-8")

    result = _processor_result_from_report(
        {
            "order_id": "AN-12258",
            "tms_context": {"status": "in_transit", "network": "rail", "destination_city": "Waltrop", "destination_country": "DE"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
            "trigger_event": {"metadata": {"file_name": "CI(4).PDF", "document_type": "commercial_invoice", "tms_document_id": "f8d2221a-5598-4e63-9a06-b380ac2b6e42"}},
        },
        {"id": 2020, "changed_at": "2026-05-21T09:47:00Z", "changed_by_name": "CARGOLO Admin", "metadata": {"file_name": "CI(4).PDF", "document_type": "commercial_invoice", "tms_document_id": "f8d2221a-5598-4e63-9a06-b380ac2b6e42"}},
    )

    message = result["message"]
    assert result["latest_subject"] == "CI(4).PDF"
    assert result["document_agent_evidence_packet"]["document"]["extracted_fields"]["invoice_number"] == "20250402001"
    assert result["evidence_summary"]["document_evidence_count"] >= 4
    assert "Rechnungsnr. 20250402001" in message
    assert "Warenwert 6810 USD" in message
    assert "Incoterm EXW" in message
    assert "Packstücke passt" in message
    assert "nicht auslesbar" not in message.lower()
    assert "keine auslesbaren" not in message.lower()
    assert result["analysis_priority"] == "low"
    assert result["agent_review_required"] is False
    assert result["pending_action_summary"]["review"] == 0


def test_shipment_context_includes_customer_reference_for_document_cards():
    context = _shipment_context({"detail": {"customer_reference": "AA2500432", "network": "rail", "status": "in_transit"}})

    assert context["customer_reference"] == "AA2500432"


def test_shipment_context_uses_cargo_rows_for_piece_weight_volume_totals():
    context = _shipment_context({
        "detail": {
            "network": "rail",
            "status": "in_transit",
            "cargo": [
                {"quantity": 2, "weight_kg": 910, "volume_m3": 4.4, "goods_description": "Displays"},
            ],
        }
    })

    assert context["pieces"] == 2
    assert context["weight_kg"] == 910
    assert context["volume_m3"] == 4.4
    assert context["cargo_description"] == "Displays"


def test_reconciliation_does_not_turn_missing_docs_alone_into_risk():
    report = reconcile_documents(
        order_id="AN-SEA",
        tms_snapshot={"detail": {"network": "sea", "status": "confirmed"}},
        registry={
            "expected_types": ["bill_of_lading", "commercial_invoice"],
            "received_types": [],
            "received_documents": [],
            "analyzed_documents": [],
        },
    )

    assert report["missing_types"] == ["bill_of_lading", "commercial_invoice"]
    assert report["risk"] == "low"
    assert report["needs_human_review"] is False
    assert report["missing_policy"] == "missing_documents_are_inventory_context_not_risk"


def test_reconciliation_normalizes_asr_landtransport_to_road():
    report = reconcile_documents(
        order_id="AN-LAND",
        tms_snapshot={"detail": {"network": "Landtransport", "status": "confirmed"}},
        registry={"expected_types": [], "received_types": [], "received_documents": [], "analyzed_documents": []},
    )

    assert report["mode"] == "road"


@pytest.mark.parametrize("mode", ["road", "Landtransport", "land_transport", "truck", "LTL", "FTL", "CargoLine", "OSL"])
def test_normalize_mode_accepts_asr_landtransport_aliases(mode):
    assert normalize_mode(mode) == "road"


def test_reconciliation_flags_present_document_weight_mismatch_against_tms(tmp_path):
    analysis_path = tmp_path / "invoice_analysis.json"
    analysis_path.write_text(
        json.dumps({"extracted_fields": {"gross_weight": "123 kg"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    report = reconcile_documents(
        order_id="AN-SEA",
        tms_snapshot={"detail": {"network": "sea", "totals": {"total_weight_kg": 500}}},
        registry={
            "expected_types": [],
            "received_types": ["commercial_invoice"],
            "received_documents": [{"filename": "invoice.pdf", "analysis_status": "ok"}],
            "analyzed_documents": [
                {
                    "filename": "invoice.pdf",
                    "analysis_doc_type": "commercial_invoice",
                    "analysis_path": str(analysis_path),
                }
            ],
        },
    )

    assert report["version"] == 2
    assert report["risk"] == "medium"
    assert report["needs_human_review"] is True
    assert report["findings"] == [
        {
            "type": "tms_document_weight_mismatch",
            "severity": "medium",
            "filename": "invoice.pdf",
            "summary": "Gewicht im Dokument 123 kg weicht vom TMS-Wert 500 kg ab.",
        }
    ]


def test_reconciliation_flags_mrn_mismatch_as_blocker(tmp_path):
    analysis_path = tmp_path / "customs_analysis.json"
    analysis_path.write_text(
        json.dumps({"extracted_fields": {"mrn": "MRN-DOC-222"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    report = reconcile_documents(
        order_id="AN-SEA",
        tms_snapshot={"detail": {"network": "sea", "customs_reference": "MRN-TMS-111"}},
        registry={
            "expected_types": [],
            "received_types": ["customs_document"],
            "received_documents": [{"filename": "customs.pdf", "analysis_status": "ok"}],
            "analyzed_documents": [
                {
                    "filename": "customs.pdf",
                    "analysis_doc_type": "customs_document",
                    "analysis_path": str(analysis_path),
                }
            ],
        },
    )

    assert report["risk"] == "high"
    assert report["needs_human_review"] is True
    assert report["findings"] == [
        {
            "type": "mrn_mismatch",
            "severity": "high",
            "filename": "customs.pdf",
            "summary": "MRN im Dokument MRN-DOC-222 passt nicht zur TMS-Zollreferenz MRN-TMS-111.",
        }
    ]


def test_processor_result_uses_human_document_message_without_risk_dump():
    result = _processor_result_from_report(
        {
            "order_id": "AN-12432",
            "report_json_path": "/tmp/report.json",
            "report_md_path": "/tmp/report.md",
            "tms_context": {"status": "in_transit", "network": "sea", "destination_city": "Hamburg", "destination_country": "DE"},
            "lifecycle": {"history_sync_count": 2, "last_email_at": "2026-05-11T09:00:00Z"},
            "registry_summary": {"received_documents": 2, "tms_documents": 1},
            "reconciliation": {
                "risk": "high",
                "needs_human_review": True,
                "findings": [
                    {"type": "mrn_mismatch", "severity": "high", "filename": "customs.pdf", "summary": "MRN im Dokument MRN-DOC-222 passt nicht zur TMS-Zollreferenz MRN-TMS-111."},
                    {"type": "document_open_question", "severity": "low", "filename": "invoice.pdf", "summary": "Steuernummer nicht lesbar."},
                    {"type": "tms_document_weight_mismatch", "severity": "medium", "filename": "invoice.pdf", "summary": "Gewicht im Dokument 123 kg weicht vom TMS-Wert 500 kg ab."},
                    {"type": "extra", "severity": "medium", "filename": "x.pdf", "summary": "Soll wegen Top-3 nicht erscheinen."},
                ],
            },
        },
        {"id": 77, "changed_at": "2026-05-11T10:00:00Z", "metadata": {"file_name": "customs.pdf", "document_type": "customs_document"}},
    )

    message = result["message"]
    assert "Lage:" in message
    assert "Auffällig:" in message
    assert "Empfehlung:" in message
    assert "Nächster Schritt:" in message
    assert "Reconciliation-Risiko" not in message
    assert "risk" not in message.lower()
    assert "MRN-DOC-222" in message
    assert "Soll wegen Top-3" not in message
    assert result["analysis_priority"] == "high"
    assert result["pending_action_summary"]["review"] == 1
    assert result["document_agent_review"]["mode"] == "guardrailed_fallback"
    assert result["document_agent_evidence_packet"]["contract"] == "agent_first_document_review_v1"


def test_document_agent_packet_carries_employee_quality_rubric():
    result = _processor_result_from_report(
        {
            "order_id": "AN-12140",
            "tms_context": {"status": "in_transit", "network": "rail"},
            "lifecycle": {},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
        },
        {"id": 8801, "changed_at": "2026-05-20T07:03:00Z", "metadata": {"file_name": "Invoice.png", "document_type": "commercial_invoice"}},
    )

    packet = result["document_agent_evidence_packet"]
    assert packet["contract"] == "agent_first_document_review_v1"
    assert packet["focus_rules"]["primary_focus"] == "new_upload"
    assert "stale_findings_must_not_dominate" in packet["focus_rules"]
    assert packet["operator_quality_rubric"]["style"] == "internal_forwarder_colleague"
    assert packet["operator_quality_rubric"]["avoid_generic_manual_review"] is True
    assert packet["guardrails"]["writes_allowed"] is False


def test_processor_result_uses_tms_cargo_items_and_soft_currency_uncertainty(tmp_path):
    filename = "GZ20260421 Packing List and Invoice_German.xlsx"
    analysis_path = tmp_path / "invoice_analysis.json"
    registry_path = tmp_path / "registry.json"
    snapshot_path = tmp_path / "tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": filename,
                "doc_type": "commercial_invoice",
                "suggested_registry_types": ["commercial_invoice", "packing_list"],
                "extracted_fields": {
                    "invoice_number": "GZ20260421",
                    "amount": "13915",
                    "currency": "USD",
                    "pieces": "52",
                    "packaging_type": "CTNS",
                    "gross_weight": "852",
                    "goods_value": "13915",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps({"analyzed_documents": [{"filename": filename, "analysis_path": str(analysis_path), "analysis_doc_type": "commercial_invoice"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "totals": {"total_weight_kg": None},
                    "cargo": [{"quantity": 52, "weight_kg": 851, "total_weight_kg": 44252}],
                    "freight_details": {},
                    "transport_legs": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with patch("plugins.cargolo_ops.document_activity_monitor._run_document_agent_review", return_value=None):
        result = _processor_result_from_report(
            {
                "order_id": "AN-12405",
                "tms_context": {"status": "in_transit", "network": "rail", "pieces": None, "weight_kg": None},
                "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
                "registry_summary": {},
                "reconciliation": {
                    "risk": "medium",
                    "needs_human_review": True,
                    "findings": [
                        {"type": "document_open_question", "severity": "low", "filename": filename, "summary": "Währung (Currency) nicht explizit genannt, aber im Kontext China-Export meist USD"}
                    ],
                },
            },
            {"id": 1893, "changed_at": 1779261294989, "changed_by_name": "Hendrik Lüdeking", "metadata": {"file_name": filename, "document_type": "commercial_invoice"}},
        )

    comparisons = {row["label"]: row for row in result["document_field_comparison"]}
    assert comparisons["Packstücke"]["status"] == "match"
    assert comparisons["Packstücke"]["source"] == "cargo_items.quantity"
    assert comparisons["Gewicht"]["status"] == "near_match"
    assert comparisons["Gewicht"]["source"] == "cargo_items.weight_kg"
    assert "total_weight_kg wirkt rechnerisch auffällig" in comparisons["Gewicht"]["note"]
    message = result["message"]
    assert "Packstücke passt" in message
    assert "Gewicht nahezu passend" in message
    assert "fehlt im TMS" not in message
    assert "USD extrahiert" in result["document_reconciliation"]["findings"][0]["summary"]


def test_processor_result_keeps_case_high_risk_out_of_current_packlist_priority(tmp_path):
    filename = "R4-4-10-G01-packing list-RAIL.xls"
    analysis_path = tmp_path / "packing_analysis.json"
    registry_path = tmp_path / "registry.json"
    snapshot_path = tmp_path / "tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": filename,
                "doc_type": "packing_list",
                "summary": "Packing list for rail shipment; 68 cartons and 2148 kg readable.",
                "extracted_fields": {"pieces": "68", "gross_weight": "2148", "customer_reference": "AA2500432"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps({"analyzed_documents": [{"filename": filename, "analysis_path": str(analysis_path), "analysis_doc_type": "packing_list"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "network": "rail",
                    "status": "in_transit",
                    "customer_reference": "AA2500432",
                    "totals": {"total_pieces": 68, "total_weight_kg": 2148},
                    "freight_details": {},
                    "transport_legs": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with patch("plugins.cargolo_ops.document_activity_monitor._run_document_agent_review", return_value=None):
        result = _processor_result_from_report(
            {
                "order_id": "AN-12374",
                "tms_context": {"status": "in_transit", "network": "rail", "customer_reference": "AA2500432"},
                "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
                "registry_summary": {},
                "reconciliation": {
                    "risk": "high",
                    "needs_human_review": True,
                    "findings": [
                        {"type": "document_flag", "severity": "medium", "filename": filename, "summary": "Multi-PO-Hinweis; mehrere POs auf Packliste"},
                        {"type": "document_open_question", "severity": "low", "filename": filename, "summary": "fehlende Dokumentnummer"},
                    ],
                },
            },
            {"id": 1998, "changed_at": "2026-05-21T08:00:00Z", "metadata": {"file_name": filename, "document_type": "packing_list"}},
        )

    assert result["analysis_priority"] == "low"
    assert result["agent_review_required"] is False
    assert result["pending_action_summary"]["review"] == 0
    assert result["evidence_summary"]["case_reconciliation_risk"] == "high"
    assert result["evidence_summary"]["current_upload_review"]["case_risk_not_used_for_document_priority"] is True
    assert "Packstücke passt" in result["message"]
    assert "Gewicht passt" in result["message"]
    assert "Kundenref. AA2500432" in result["message"]
    assert "fachlich/manuell prüfen" not in result["message"]
    assert "Agent Review erforderlich" not in result["document_decision"]


def test_processor_result_filters_stale_high_finding_from_other_document(tmp_path):
    filename = "current_packlist.pdf"
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename=filename,
        event_doc_type="packing_list",
        analysis_doc_type="packing_list",
        extracted_fields={},
        freight_details={"pol_code": "", "pod_code": ""},
        findings=[{"type": "mrn_mismatch", "severity": "high", "filename": "old_customs.pdf", "summary": "Alte MRN-Abweichung"}],
    )

    assert result["analysis_priority"] == "low"
    assert result["agent_review_required"] is False
    assert result["pending_action_summary"]["review"] == 0
    assert result["document_reconciliation"]["findings"] == []
    assert result["evidence_summary"]["current_upload_review"]["case_risk_not_used_for_document_priority"] is False


def test_current_upload_review_flag_is_false_for_low_case_without_action(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="clean_packlist.pdf",
        event_doc_type="packing_list",
        analysis_doc_type="packing_list",
        extracted_fields={},
        freight_details={"pol_code": "", "pod_code": ""},
    )

    assert result["analysis_priority"] == "low"
    assert result["evidence_summary"]["current_upload_review"]["case_risk_not_used_for_document_priority"] is False


def test_external_agent_cannot_escalate_case_only_risk_without_current_upload_action(tmp_path):
    agent_review = {
        "mode": "external_agent",
        "sections": {
            "lage": "Packliste gelesen, aber Case ist insgesamt kritisch.",
            "abgleich": "Packstücke und Gewicht passen; keine konkrete TMS-Korrektur.",
            "auffaellig": "Wegen Case-Risiko bitte manuell prüfen.",
            "empfehlung": "Fachlich/manuell prüfen.",
            "naechster_schritt": "Im TMS gegenprüfen.",
        },
        "decision": "manual_review",
        "priority": "high",
        "needs_review": True,
        "confidence": "medium",
    }
    with patch("plugins.cargolo_ops.document_activity_monitor._run_document_agent_review", return_value=agent_review):
        result = _processor_result_for_uploaded_fields(
            tmp_path,
            filename="clean_packlist_agent.pdf",
            event_doc_type="packing_list",
            analysis_doc_type="packing_list",
            extracted_fields={},
            freight_details={"pol_code": "", "pod_code": ""},
            findings=[{"type": "document_flag", "severity": "medium", "filename": "clean_packlist_agent.pdf", "summary": "Multi-PO-Hinweis"}],
        )

    assert result["document_agent_review"]["mode"] == "guardrailed_fallback"
    assert result["analysis_priority"] == "low"
    assert result["agent_review_required"] is False
    assert "Case ist insgesamt kritisch" not in result["message"]


def test_processor_result_prefers_populated_tms_totals_over_cargo_rows(tmp_path):
    filename = "invoice.pdf"
    analysis_path = tmp_path / "invoice_analysis.json"
    registry_path = tmp_path / "registry.json"
    snapshot_path = tmp_path / "tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": filename,
                "doc_type": "commercial_invoice",
                "extracted_fields": {"pieces": "52", "gross_weight": "852"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps({"analyzed_documents": [{"filename": filename, "analysis_path": str(analysis_path), "analysis_doc_type": "commercial_invoice"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "totals": {"total_pieces": 50, "total_weight_kg": 900},
                    "cargo": [{"quantity": 52, "weight_kg": 851, "total_weight_kg": 44252}],
                    "freight_details": {},
                    "transport_legs": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with patch("plugins.cargolo_ops.document_activity_monitor._run_document_agent_review", return_value=None):
        result = _processor_result_from_report(
            {
                "order_id": "AN-TOTALS",
                "tms_context": {"status": "in_transit", "network": "rail"},
                "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
                "registry_summary": {},
                "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
            },
            {"id": 1894, "changed_at": "2026-05-20T07:14:00Z", "metadata": {"file_name": filename, "document_type": "commercial_invoice"}},
        )

    comparisons = {row["label"]: row for row in result["document_field_comparison"]}
    assert comparisons["Packstücke"]["source"] == "totals"
    assert comparisons["Packstücke"]["status"] == "diff"
    assert comparisons["Gewicht"]["source"] == "totals"
    assert comparisons["Gewicht"]["status"] == "diff"
    message = result["message"]
    assert "Packstücke weicht ab: TMS (totals) 50, Dokument 52" in message
    assert "Gewicht weicht ab: TMS (totals) 900 kg, Dokument 852" in message


def test_weight_and_piece_discrepancies_create_direct_write_cards_when_cargo_row_is_addressable(tmp_path):
    filename = "angebot.pdf"
    analysis_path = tmp_path / "angebot_analysis.json"
    registry_path = tmp_path / "registry.json"
    snapshot_path = tmp_path / "tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": filename,
                "doc_type": "offer",
                "extracted_fields": {"pieces": "112", "gross_weight": "2.464 kg"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps({"analyzed_documents": [{"filename": filename, "analysis_path": str(analysis_path), "analysis_doc_type": "offer"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "totals": {"total_pieces": 111, "total_weight_kg": 22},
                    "cargo": [{"id": "cargo-12354", "quantity": 111, "weight_kg": 22, "total_weight_kg": 22}],
                    "freight_details": {},
                    "transport_legs": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with patch("plugins.cargolo_ops.document_activity_monitor._run_document_agent_review", return_value=None):
        result = _processor_result_from_report(
            {
                "order_id": "AN-12354",
                "tms_context": {"status": "in_transit", "network": "rail"},
                "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
                "registry_summary": {},
                "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
            },
            {"id": 1895, "changed_at": "2026-05-21T07:14:00Z", "metadata": {"file_name": filename, "document_type": "offer"}},
        )

    comparisons = {row["label"]: row for row in result["document_field_comparison"]}
    assert comparisons["Packstücke"]["target"] == "cargo_pieces"
    assert comparisons["Gewicht"]["target"] == "cargo_weight_kg"
    assert comparisons["Packstücke"]["status"] == "diff"
    assert comparisons["Gewicht"]["status"] == "diff"
    assert [(intent["target"], intent["value"], intent["guardrails"]["write_supported"], intent["guardrails"]["review_only"]) for intent in result["document_review_intents"]] == [
        ("cargo_pieces", "112", True, False),
        ("cargo_weight_kg", "2.464 kg", True, False),
    ]

    cards = _queue_document_review_cards(
        storage_root=tmp_path,
        order_id="AN-12354",
        intents=result["document_review_intents"],
        event={"id": 1895, "changed_at": "2026-05-21T07:14:00Z", "metadata": {"file_name": filename, "document_type": "offer"}},
    )

    assert [(card["target"], card["value"], card["write_supported"], card.get("action_type")) for card in cards] == [
        ("cargo_pieces", "112", True, "cargo_item_update"),
        ("cargo_weight_kg", "2.464 kg", True, "cargo_item_update"),
    ]
    pending_path = tmp_path / "orders" / "AN-12354" / "teams" / "pending_tms_actions.jsonl"
    queue = [json.loads(line) for line in pending_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [(row["target"], row["value"], row["status"], row["write_policy"], row["write_supported"], row.get("action_type")) for row in queue] == [
        ("cargo_pieces", "112", "pending_review", "no_auto_write_without_review", True, "cargo_item_update"),
        ("cargo_weight_kg", "2.464 kg", "pending_review", "no_auto_write_without_review", True, "cargo_item_update"),
    ]
    assert queue[0]["tool_args"] == {"cargo_item_id": "cargo-12354", "quantity": 112}
    assert queue[1]["tool_args"] == {"cargo_item_id": "cargo-12354", "weight_kg": 2464, "total_weight_kg": 2464}
    assert not (tmp_path / "orders" / "AN-12354" / "teams" / "applied_tms_actions.jsonl").exists()


@pytest.mark.parametrize(
    ("cargo_rows", "expected"),
    [
        ([{"quantity": 111, "weight_kg": 22}], False),
        ([{"id": "cargo-a", "quantity": 50, "weight_kg": 10}, {"id": "cargo-b", "quantity": 61, "weight_kg": 12}], False),
        ([{"id": "cargo-a", "quantity": 111, "weight_kg": 22}], True),
    ],
)
def test_cargo_writeback_metadata_requires_single_addressable_cargo_row(tmp_path, cargo_rows, expected):
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(
        json.dumps({"detail": {"cargo": cargo_rows, "totals": {"total_pieces": 111, "total_weight_kg": 22}}}),
        encoding="utf-8",
    )
    report = {"lifecycle": {"tms_snapshot_path": str(snapshot_path)}}

    write_supported, action_type, tool_args = _writeback_metadata_for_intent(report, "cargo_pieces", "112")

    assert write_supported is expected
    if expected:
        assert action_type == "cargo_item_update"
        assert tool_args == {"cargo_item_id": "cargo-a", "quantity": 112}
    else:
        assert action_type is None
        assert tool_args is None


def test_processor_result_rejects_generic_or_stale_external_review_for_unreadable_upload():
    generic_review = {
        "mode": "external_agent",
        "sections": {
            "lage": "Der neue Upload ist eine archivierte E-Mail, aus der keine lesbaren Sendungsdaten extrahiert wurden.",
            "abgleich": "POL, POD, ETD und ETA können nicht abgeglichen werden; zusätzlich liegen aus vorhandenen Dokumenten Abweichungen bei Gewicht 896 kg statt 508 kg vor.",
            "auffaellig": "Auffällig sind die erforderliche Sanktionsprüfung wegen Russland-Transit sowie Gewichts- und Packstückabweichungen.",
            "empfehlung": "Bitte fachlich/manuell prüfen, bevor Daten operativ übernommen werden.",
            "naechster_schritt": "Im TMS und in den vorliegenden Dokumenten Gewicht, Packstücke, Route/Russland-Transit und Terminangaben gegenprüfen.",
        },
        "decision": "manual_review",
        "priority": "medium",
        "needs_review": True,
    }
    with patch("plugins.cargolo_ops.document_activity_monitor._run_document_agent_review", return_value=generic_review):
        result = _processor_result_from_report(
            {
                "order_id": "AN-13329",
                "tms_context": {"status": "confirmed", "network": "rail"},
                "lifecycle": {},
                "registry_summary": {},
                "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
            },
            {"id": 8802, "changed_at": "2026-05-20T07:11:00Z", "metadata": {"file_name": "archived-email.eml", "document_type": "email"}},
        )

    assert result["document_agent_review"]["mode"] == "guardrailed_fallback"
    assert result["analysis_priority"] == "low"
    assert result["agent_review_required"] is False
    assert "Russland" not in result["message"]
    assert "896 kg" not in result["message"]
    assert "fachlich/manuell prüfen" not in result["message"]


def test_processor_result_uses_external_agent_review_when_available():
    agent_review = {
        "mode": "external_agent",
        "sections": {
            "lage": "Packing List gelesen; operativ nur Gewicht/Packstücke relevant.",
            "abgleich": "Packstücke passen zum TMS; keine sichere TMS-Korrektur ableitbar.",
            "auffaellig": "Kein harter Konflikt, aber Gewicht sollte bei Gelegenheit gegengeprüft werden.",
            "empfehlung": "Nicht eskalieren; Sendung weiter beobachten.",
            "naechster_schritt": "Keine Kachel nötig; erst bei weiterem Beleg erneut bewerten.",
        },
        "decision": "observe",
        "priority": "medium",
        "needs_review": False,
        "confidence": "high",
    }
    with patch("plugins.cargolo_ops.document_activity_monitor._run_document_agent_review", return_value=agent_review) as mock_review:
        result = _processor_result_from_report(
            {
                "order_id": "AN-AGENT",
                "tms_context": {"status": "in_transit", "network": "rail"},
                "lifecycle": {},
                "registry_summary": {},
                "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
            },
            {"id": 88, "changed_at": "2026-05-19T10:00:00Z", "metadata": {"file_name": "packing.pdf", "document_type": "packing_list"}},
        )

    packet = mock_review.call_args.args[0]
    assert packet["contract"] == "agent_first_document_review_v1"
    assert packet["guardrails"]["writes_allowed"] is False
    assert "Packing List gelesen" in result["message"]
    assert "Nicht eskalieren" in result["message"]
    assert result["analysis_priority"] == "medium"
    assert result["document_decision"] == "Agentische Bewertung: weiter beobachten"
    assert result["document_agent_review"]["mode"] == "external_agent"
    assert result["side_effects"] == {"tms_updates": 0, "queued_tms_actions": 0, "customer_notifications": 0}


def test_processor_result_does_not_let_agent_downgrade_deterministic_review_intent(tmp_path):
    agent_review = {
        "mode": "external_agent",
        "sections": {
            "lage": "Beleg gelesen.",
            "abgleich": "MBL fehlt im TMS, Dokument nennt NGP3497068.",
            "auffaellig": "Sicherer Feldkandidat vorhanden.",
            "empfehlung": "Freigabe-Kachel prüfen.",
            "naechster_schritt": "Bestätigen oder ablehnen.",
        },
        "decision": "no_action",
        "priority": "low",
        "needs_review": False,
    }
    with patch("plugins.cargolo_ops.document_activity_monitor._run_document_agent_review", return_value=agent_review):
        result = _processor_result_for_uploaded_fields(
            tmp_path,
            filename="mbl.pdf",
            event_doc_type="master_bl",
            analysis_doc_type="master_bl",
            field_sources={"mbl_number": "explicit_bl_number"},
            extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
        )

    assert result["document_review_intents"]
    assert result["pending_action_summary"]["review"] == 1
    assert result["agent_review_required"] is True
    assert result["analysis_priority"] == "medium"
    assert result["document_decision"] == "TMS-Korrektur nur nach Agent-/Operator-Freigabe prüfen"


def test_processor_result_escalates_blocker_finding_to_high_priority():
    result = _processor_result_from_report(
        {
            "order_id": "AN-BLOCKER",
            "tms_context": {"status": "customs_pending", "network": "sea"},
            "lifecycle": {},
            "registry_summary": {},
            "reconciliation": {
                "risk": "low",
                "needs_human_review": True,
                "findings": [
                    {
                        "type": "document_profile_blocker",
                        "code": "proforma_invoice",
                        "severity": "blocker",
                        "filename": "invoice.pdf",
                        "summary": "Commercial invoice is marked as proforma.",
                    }
                ],
            },
        },
        {"id": 78, "metadata": {"file_name": "invoice.pdf", "document_type": "commercial_invoice"}},
    )

    assert result["analysis_priority"] == "high"
    assert "Commercial invoice is marked as proforma" in result["message"]


def test_reconciliation_treats_commercial_invoice_zero_value_as_blocker_high_risk(tmp_path):
    analysis_path = tmp_path / "invoice_zero_analysis.json"
    analysis_path.write_text(
        json.dumps({"doc_type": "commercial_invoice", "extracted_fields": {"goods_value": "0", "currency": "EUR"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    report = reconcile_documents(
        order_id="AN-CI",
        tms_snapshot={"detail": {"network": "sea"}},
        registry={
            "expected_types": [],
            "received_types": ["commercial_invoice"],
            "received_documents": [{"filename": "invoice.pdf", "analysis_status": "ok"}],
            "analyzed_documents": [
                {"filename": "invoice.pdf", "analysis_doc_type": "commercial_invoice", "analysis_path": str(analysis_path)}
            ],
        },
    )

    assert report["risk"] == "high"
    assert any(row.get("severity") == "blocker" and row.get("code") == "zero_value" for row in report["findings"])


def test_reconciliation_treats_commercial_invoice_proforma_as_blocker_high_risk(tmp_path):
    analysis_path = tmp_path / "invoice_proforma_analysis.json"
    analysis_path.write_text(
        json.dumps({"doc_type": "commercial_invoice", "summary": "Proforma invoice for customs preview", "extracted_fields": {"goods_value": "100", "currency": "EUR"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    report = reconcile_documents(
        order_id="AN-CI",
        tms_snapshot={"detail": {"network": "sea"}},
        registry={
            "expected_types": [],
            "received_types": ["commercial_invoice"],
            "received_documents": [{"filename": "proforma.pdf", "analysis_status": "ok"}],
            "analyzed_documents": [
                {"filename": "proforma.pdf", "analysis_doc_type": "commercial_invoice", "analysis_path": str(analysis_path)}
            ],
        },
    )

    assert report["risk"] == "high"
    assert any(row.get("severity") == "blocker" and row.get("code") == "proforma_invoice" for row in report["findings"])


def test_processor_result_prioritizes_uploaded_document_and_labels_master_bl():
    result = _processor_result_from_report(
        {
            "order_id": "AN-11790",
            "tms_context": {"status": "customs_pending", "network": "sea"},
            "lifecycle": {},
            "registry_summary": {},
            "reconciliation": {
                "risk": "medium",
                "needs_human_review": True,
                "findings": [
                    {"type": "document_flag", "severity": "medium", "filename": "old.gif", "summary": "not_a_logistics_document"},
                    {"type": "tms_document_weight_mismatch", "severity": "medium", "filename": "BKGCONF_NGP3497068.pdf", "summary": "Gesamtgewicht im Dokument mit '5' angegeben (evtl. Tonnen), im TMS stehen 3100 kg."},
                    {"type": "document_open_question", "severity": "low", "filename": "NGP3497068.pdf", "summary": "ETA nicht explizit auf dem Dokument angegeben"},
                    {"type": "document_flag", "severity": "medium", "filename": "NGP3497068.pdf", "summary": "Entwurf (Draft) - Original BL prüfen"},
                ],
            },
        },
        {"id": 1263, "metadata": {"file_name": "NGP3497068.pdf", "document_type": "master_bl"}},
    )

    message = result["message"]
    assert "Master B/L" in message
    assert "Entwurf (Draft)" in message
    assert "ETA nicht explizit" in message
    assert "old.gif" not in message
    assert "not_a_logistics_document" not in message
    assert "BKGCONF_NGP3497068.pdf" not in message
    assert "3100 kg" not in message


def test_processor_result_ignores_untrusted_invoice_mbl_noise_and_skips_teams_card(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="invoice-with-mbl.pdf",
        event_doc_type="commercial_invoice",
        analysis_doc_type="commercial_invoice",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert "MBL / B/L-Nr. fehlt im TMS" not in result["message"]
    assert result["document_field_comparison"] == []
    assert result["teams_tms_review_cards"] == []


def test_processor_result_queues_trusted_master_bl_mbl_card(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="master-bl.pdf",
        event_doc_type="master_bl",
        analysis_doc_type="master_bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert [(intent["target"], intent["value"]) for intent in result["document_review_intents"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_queues_eta_ata_date_cards_from_shipment_advice(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="arrival-advice.pdf",
        event_doc_type="shipment_advice",
        analysis_doc_type="shipment_advice",
        extracted_fields={"eta": "2026-06-20", "ata": "21.06.2026", "shipment_number": "AN-11790"},
    )

    assert [(intent["target"], intent["value"], intent["guardrails"]["write_supported"]) for intent in result["document_review_intents"]] == [
        ("estimated_delivery_date", "2026-06-20", True),
        ("actual_delivery_date", "2026-06-21", True),
    ]


def test_processor_result_skips_etd_card_when_date_already_matches_tms(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="booking-confirmation.pdf",
        event_doc_type="booking_confirmation",
        analysis_doc_type="shipment_advice",
        extracted_fields={"etd": "10.05.2026", "shipment_number": "AN-11790"},
        transport_legs=[{"leg_type": "main_carriage", "etd": 1778371200000}],
    )

    etd_rows = [row for row in result["document_field_comparison"] if row["target"] == "etd_main_carriage"]
    assert [(row["status"], row["tms"], row["doc"]) for row in etd_rows] == [("match", "2026-05-10", "10.05.2026")]
    assert all(intent["target"] != "etd_main_carriage" for intent in result["document_review_intents"])
    assert result["teams_tms_review_cards"] == []


def test_processor_result_surfaces_etd_atd_as_review_only_cards(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="departure-advice.pdf",
        event_doc_type="shipment_advice",
        analysis_doc_type="shipment_advice",
        extracted_fields={"etd": "2026-06-18", "atd": "19.06.2026", "shipment_number": "AN-11790"},
    )

    assert [(intent["target"], intent["value"], intent["guardrails"]["write_supported"]) for intent in result["document_review_intents"]] == [
        ("etd_main_carriage", "2026-06-18", False),
        ("atd_main_carriage", "2026-06-19", False),
    ]


def test_processor_result_queues_pol_pod_eta_cards_from_transportauftrag_when_tms_missing(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="Transportauftrag-AN-12258.pdf",
        event_doc_type="commercial_invoice",
        analysis_doc_type="offer",
        extracted_fields={
            "document_type": "Transportauftrag",
            "shipment_number": "AN-12258",
            "pol": "Dongkeng",
            "pod": "Waltrop",
            "eta": "16.05.2026",
            "pieces": "2",
            "gross_weight": "910 kg",
        },
        freight_details={"mbl_number": "", "container_number": ""},
    )

    assert [(intent["target"], intent["value"], intent["guardrails"]["write_supported"], intent["guardrails"]["review_only"]) for intent in result["document_review_intents"]] == [
        ("pol", "Dongkeng", False, True),
        ("pod", "Waltrop", False, True),
        ("estimated_delivery_date", "2026-05-16", True, False),
    ]
    cards = _queue_document_review_cards(
        storage_root=tmp_path,
        order_id="AN-11790",
        intents=result["document_review_intents"],
        event={"id": 9001, "changed_at": "2026-05-11T10:00:00Z", "metadata": {"file_name": "Transportauftrag-AN-12258.pdf", "document_type": "commercial_invoice"}},
    )
    assert [(card["target"], card["value"], card["write_supported"]) for card in cards] == [
        ("pol", "Dongkeng", False),
        ("pod", "Waltrop", False),
        ("estimated_delivery_date", "2026-05-16", True),
    ]


def test_processor_result_offer_filename_overrides_false_billing_analysis(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="Angebot-AN-13322-V1.pdf",
        event_doc_type="email",
        analysis_doc_type="billing",
        extracted_fields={
            "document_type": "Angebot",
            "document_number": "AN-13322-V1",
            "amount": "3480.24",
            "currency": "EUR",
            "customer": "Mainhattan-Wheels GmbH",
            "incoterm_named_place": "EXW",
            "shipment_number": "AN-13322",
        },
    )

    assert result["document_activity_document_type"] == "offer"
    assert "Angebot geprüft" in result["message"]
    assert "Abrechnungsbeleg" not in result["message"]
    assert "Angebotsdaten:" in result["message"]


def test_processor_result_uses_analyzed_master_bl_type_when_event_has_legacy_bill_of_lading(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="legacy-event-bl.pdf",
        event_doc_type="bill_of_lading",
        analysis_doc_type="master_bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert [(intent["target"], intent["value"]) for intent in result["document_review_intents"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_allows_legacy_bill_of_lading_with_explicit_bl_field_evidence(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="legacy-explicit-bl.pdf",
        event_doc_type="bill_of_lading",
        analysis_doc_type="bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert [(intent["target"], intent["value"]) for intent in result["document_review_intents"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_uses_analyzed_master_bl_type_when_event_doc_type_missing(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="missing-event-type-bl.pdf",
        event_doc_type="",
        analysis_doc_type="master_bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert [(intent["target"], intent["value"]) for intent in result["document_review_intents"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_allows_missing_event_type_with_analyzed_legacy_bl_evidence(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="missing-event-legacy-bl.pdf",
        event_doc_type="",
        analysis_doc_type="bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert [(intent["target"], intent["value"]) for intent in result["document_review_intents"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_blocks_mbl_card_when_field_source_is_booking_number(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="booking-source-master-bl.pdf",
        event_doc_type="master_bl",
        analysis_doc_type="master_bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
        field_sources={
            "mbl_number": {
                "value": "NGP3497068",
                "label": "Booking No.",
                "source": "carrier booking confirmation",
                "raw_context": "Booking No. NGP3497068",
            }
        },
    )

    assert "MBL / B/L-Nr. fehlt im TMS" in result["message"]
    assert result["teams_tms_review_cards"] == []


def test_processor_result_blocks_mbl_card_when_field_source_is_invoice_number(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="invoice-source-master-bl.pdf",
        event_doc_type="master_bl",
        analysis_doc_type="master_bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
        field_sources={
            "mbl_number": {
                "value": "NGP3497068",
                "label": "Invoice No.",
                "source": "invoice header",
                "raw_context": "Invoice No. NGP3497068",
            }
        },
    )

    assert "MBL / B/L-Nr. fehlt im TMS" in result["message"]
    assert result["teams_tms_review_cards"] == []


def test_processor_result_queues_mbl_card_when_field_source_is_master_bl_number(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="master-bl-source.pdf",
        event_doc_type="master_bl",
        analysis_doc_type="master_bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
        field_sources={
            "mbl_number": {
                "value": "NGP3497068",
                "label": "Master B/L No.",
                "source": "ocean bill of lading header",
                "raw_context": "Master B/L No. NGP3497068",
            }
        },
    )

    assert [(intent["target"], intent["value"]) for intent in result["document_review_intents"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_queues_trusted_packing_list_valid_container_card(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="packing-list.pdf",
        event_doc_type="packing_list",
        analysis_doc_type="packing_list",
        extracted_fields={"pol": "Ningbo, China", "pod": "Hamburg, Germany"},
        references=["XHCU2996441"],
    )

    assert [(intent["target"], intent["value"]) for intent in result["document_review_intents"]] == [("container_number", "XHCU2996441")]


def test_telex_release_with_explicit_container_queues_mbl_and_container_review_cards(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_CMD", raising=False)

    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="telex-release.pdf",
        event_doc_type="telex_release",
        analysis_doc_type="telex_release",
        extracted_fields={
            "mbl_number": "ZIHWBK260510LH052-Q",
            "container_number": "CICU9982440",
        },
        field_sources={
            "mbl_number": {
                "value": "ZIHWBK260510LH052-Q",
                "label": "Master B/L No.",
                "source": "telex release",
                "raw_context": "Master B/L No. ZIHWBK260510LH052-Q",
            },
            "container_number": {
                "value": "CICU9982440",
                "label": "Container No.",
                "source": "telex release",
                "raw_context": "Container No. CICU9982440",
            },
        },
        freight_details={
            "pol_code": "Zhengzhou",
            "pod_code": "DEHAM",
            "mbl_number": "",
            "container_number": "",
        },
    )

    intents = result["document_review_intents"]
    assert [(intent["target"], intent["value"]) for intent in intents] == [
        ("mbl_number", "ZIHWBK260510LH052-Q"),
        ("container_number", "CICU9982440"),
    ]
    assert all(intent["guardrails"]["write_supported"] is True for intent in intents)
    assert all(intent["guardrails"]["side_effects_created"] is False for intent in intents)
    assert {intent["target"]: intent["guardrails"]["field_source_checked"] for intent in intents} == {
        "mbl_number": True,
        "container_number": True,
    }

    cards = _queue_document_review_cards(
        storage_root=tmp_path,
        order_id="AN-11790",
        intents=intents,
        event={
            "id": 9001,
            "changed_at": "2026-05-11T10:00:00Z",
            "metadata": {"file_name": "telex-release.pdf", "document_type": "telex_release"},
        },
    )

    assert [(card["target"], card["value"], card["write_supported"]) for card in cards] == [
        ("mbl_number", "ZIHWBK260510LH052-Q", True),
        ("container_number", "CICU9982440", True),
    ]

    pending_path = tmp_path / "orders" / "AN-11790" / "teams" / "pending_tms_actions.jsonl"
    queue = [json.loads(line) for line in pending_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [(row["target"], row["value"], row["status"], row["write_policy"], row["write_supported"]) for row in queue] == [
        ("mbl_number", "ZIHWBK260510LH052-Q", "pending_review", "no_auto_write_without_review", True),
        ("container_number", "CICU9982440", "pending_review", "no_auto_write_without_review", True),
    ]
    assert not (tmp_path / "orders" / "AN-11790" / "teams" / "applied_tms_actions.jsonl").exists()


def test_document_review_card_duplicate_is_reported_without_new_queue_entry(tmp_path):
    intents = [
        {
            "target": "container_number",
            "value": "XHCU2996441",
            "current_tms_value": "nicht gepflegt",
            "label": "Container",
            "confidence": "document_field_comparison",
            "context_id": "AN-11790:1994:document_monitor",
            "guardrails": {"effective_document_type": "house_bill_of_lading"},
        }
    ]
    event = {
        "id": 1994,
        "changed_at": "2026-05-21T08:45:02Z",
        "metadata": {"file_name": "DC5226040340 HBL OBD.PDF", "document_type": "house_bl"},
    }

    first = _queue_document_review_card_results(storage_root=tmp_path, order_id="AN-11790", intents=intents, event=event)
    second = _queue_document_review_card_results(storage_root=tmp_path, order_id="AN-11790", intents=intents, event=event)

    assert [(card["target"], card["value"]) for card in first["created"]] == [("container_number", "XHCU2996441")]
    assert first["duplicates"] == []
    assert second["created"] == []
    assert [(card["target"], card["value"], card["existing_action_id"]) for card in second["duplicates"]] == [
        ("container_number", "XHCU2996441", first["created"][0]["action_id"])
    ]
    assert "Review-Kachel existiert bereits" in second["duplicates"][0]["summary"]

    pending_path = tmp_path / "orders" / "AN-11790" / "teams" / "pending_tms_actions.jsonl"
    queue = [json.loads(line) for line in pending_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [(row["target"], row["value"], row["status"]) for row in queue] == [
        ("container_number", "XHCU2996441", "pending_review")
    ]


def test_document_review_card_duplicate_is_reported_after_new_card_cap(tmp_path):
    pending_dir = tmp_path / "orders" / "AN-11790" / "teams"
    pending_dir.mkdir(parents=True)
    existing_action_id = "existing-container-review"
    (pending_dir / "pending_tms_actions.jsonl").write_text(
        json.dumps(
            {
                "action_id": existing_action_id,
                "order_id": "AN-11790",
                "target": "container_number",
                "value": "XHCU2996441",
                "status": "pending_review",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    intents = [
        {"target": "mbl_number", "value": "MBL-1", "current_tms_value": "nicht gepflegt", "label": "MBL"},
        {"target": "hbl_number", "value": "HBL-1", "current_tms_value": "nicht gepflegt", "label": "HBL"},
        {"target": "hawb_number", "value": "HAWB-1", "current_tms_value": "nicht gepflegt", "label": "HAWB"},
        {"target": "container_number", "value": "XHCU2996441", "current_tms_value": "nicht gepflegt", "label": "Container"},
    ]
    event = {"id": 1995, "metadata": {"document_type": "house_bl"}}

    result = _queue_document_review_card_results(storage_root=tmp_path, order_id="AN-11790", intents=intents, event=event, max_cards=3)

    assert [(card["target"], card["value"]) for card in result["created"]] == [
        ("mbl_number", "MBL-1"),
        ("hbl_number", "HBL-1"),
        ("hawb_number", "HAWB-1"),
    ]
    assert [(card["target"], card["value"], card["existing_action_id"]) for card in result["duplicates"]] == [
        ("container_number", "XHCU2996441", existing_action_id)
    ]


def test_telex_release_container_requires_explicit_container_provenance(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="telex-release-reference.pdf",
        event_doc_type="telex_release",
        analysis_doc_type="telex_release",
        extracted_fields={
            "mbl_number": "ZIHWBK260510LH052-Q",
            "container_number": "CICU9982440",
        },
        field_sources={
            "mbl_number": {
                "value": "ZIHWBK260510LH052-Q",
                "label": "Master B/L No.",
                "source": "telex release",
                "raw_context": "Master B/L No. ZIHWBK260510LH052-Q",
            },
            "container_number": {
                "value": "CICU9982440",
                "label": "Customer Reference",
                "source": "reference block",
                "raw_context": "Customer Reference CICU9982440",
            },
        },
        freight_details={"mbl_number": "", "container_number": ""},
    )

    assert [(intent["target"], intent["value"]) for intent in result["document_review_intents"]] == [
        ("mbl_number", "ZIHWBK260510LH052-Q")
    ]


def test_telex_release_container_without_field_source_is_not_queued(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="telex-release-no-source.pdf",
        event_doc_type="telex_release",
        analysis_doc_type="telex_release",
        extracted_fields={"container_number": "CICU9982440"},
        field_sources={},
        freight_details={"mbl_number": "", "container_number": ""},
    )

    assert result["document_review_intents"] == []


def test_processor_result_blocks_master_bl_cards_when_uploaded_file_has_blocker_finding(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="blocked-master-bl.pdf",
        event_doc_type="master_bl",
        analysis_doc_type="master_bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
        references=["XHCU2996441"],
        findings=[
            {
                "type": "document_profile_blocker",
                "severity": "blocker",
                "filename": "blocked-master-bl.pdf",
                "summary": "Dokument ist als Entwurf/Blocker markiert.",
            }
        ],
    )

    assert "MBL / B/L-Nr. fehlt im TMS" in result["message"]
    assert "Container fehlt im TMS" in result["message"]
    assert "Dokument ist als Entwurf/Blocker markiert" in result["message"]
    assert result["teams_tms_review_cards"] == []


def test_processor_result_rejects_date_like_document_number_as_mbl_but_keeps_container_card(tmp_path):
    analysis_path = tmp_path / "date_like_analysis.json"
    registry_path = tmp_path / "date_like_registry.json"
    snapshot_path = tmp_path / "date_like_tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": "2026-05-0907MFD.pdf",
                "doc_type": "bill_of_lading",
                "summary": "B/L document with generic document code and container reference",
                "references": ["2026-05-0907MFD", "XHCU2996441"],
                "extracted_fields": {
                    "document_number": "2026-05-0907MFD",
                    "pol": "Ningbo, China",
                    "pod": "Hamburg, Germany",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "analyzed_documents": [
                    {"filename": "2026-05-0907MFD.pdf", "analysis_path": str(analysis_path), "analysis_doc_type": "bill_of_lading"}
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "freight_details": {"pol_code": "CNNGB", "pod_code": "DEHAM", "mbl_number": "", "container_number": ""},
                    "transport_legs": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = _processor_result_from_report(
        {
            "order_id": "AN-11790",
            "case_root": str(tmp_path / "orders" / "AN-11790"),
            "tms_context": {"status": "customs_pending", "network": "sea"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
        },
        {"id": 1264, "changed_at": "2026-05-11T10:00:00Z", "metadata": {"file_name": "2026-05-0907MFD.pdf", "document_type": "master_bl"}},
    )

    message = result["message"]
    assert "2026-05-0907MFD" not in "\n".join(
        f"{card['target']}={card['value']}" for card in result["teams_tms_review_cards"]
    )
    assert "MBL / B/L-Nr. fehlt im TMS" not in message
    assert [(intent["target"], intent["value"]) for intent in result["document_review_intents"]] == [("container_number", "XHCU2996441")]


@pytest.mark.parametrize(
    "mbl_number",
    [
        "09.05MFD",
        "09/05MFD",
        "09-05ABC",
        "31.12ABC",
        "SEA-WAYBILL",
        "MASTER-BL",
        "BILL-OF-LADING",
        "HAPAG-LLOYD",
        "OCEAN/BL",
    ],
)
def test_processor_result_rejects_false_positive_explicit_mbl_numbers(tmp_path, mbl_number):
    analysis_path = tmp_path / "false_positive_mbl_analysis.json"
    registry_path = tmp_path / "false_positive_mbl_registry.json"
    snapshot_path = tmp_path / "false_positive_mbl_tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": f"{mbl_number}.pdf",
                "doc_type": "bill_of_lading",
                "summary": "B/L document with non-reference label in the MBL field",
                "references": [],
                "extracted_fields": {
                    "mbl_number": mbl_number,
                    "pol": "Ningbo, China",
                    "pod": "Hamburg, Germany",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "analyzed_documents": [
                    {"filename": f"{mbl_number}.pdf", "analysis_path": str(analysis_path), "analysis_doc_type": "bill_of_lading"}
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "freight_details": {"pol_code": "CNNGB", "pod_code": "DEHAM", "mbl_number": "", "container_number": ""},
                    "transport_legs": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = _processor_result_from_report(
        {
            "order_id": "AN-11790",
            "case_root": str(tmp_path / "orders" / "AN-11790"),
            "tms_context": {"status": "customs_pending", "network": "sea"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
        },
        {"id": 1266, "changed_at": "2026-05-11T10:00:00Z", "metadata": {"file_name": f"{mbl_number}.pdf", "document_type": "master_bl"}},
    )

    message = result["message"]
    assert "MBL / B/L-Nr. fehlt im TMS" not in message
    assert [card for card in result["teams_tms_review_cards"] if card["target"] == "mbl_number"] == []


@pytest.mark.parametrize("mbl_field_name", ["mbl_number", "master_bl_number", "bill_of_lading_number", "bl_number"])
def test_processor_result_compares_uploaded_explicit_bl_fields_against_tms(tmp_path, mbl_field_name):
    analysis_path = tmp_path / "analysis.json"
    registry_path = tmp_path / "registry.json"
    snapshot_path = tmp_path / "tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": "NGP3497068.pdf",
                "doc_type": "bill_of_lading",
                "summary": "Draft B/L EVER GREET",
                "references": ["NGP3497068", "XHCU2996441"],
                "extracted_fields": {
                    mbl_field_name: "NGP3497068",
                    "pol": "Ningbo, China",
                    "pod": "Hamburg, Germany",
                    "etd": "2026-05-06",
                    "eta": None,
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "analyzed_documents": [
                    {"filename": "NGP3497068.pdf", "analysis_path": str(analysis_path), "analysis_doc_type": "bill_of_lading"}
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "dates": {"estimated_delivery_date": "2026-06-22"},
                    "freight_details": {"pol_code": "CNNGB", "pod_code": "DEHAM", "mbl_number": "", "container_number": ""},
                    "transport_legs": [
                        {"leg_type": "main_carriage", "etd": 1777852800000, "carrier": "Ever Greet"}
                    ],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = _processor_result_from_report(
        {
            "order_id": "AN-11790",
            "case_root": str(tmp_path / "orders" / "AN-11790"),
            "tms_context": {"status": "customs_pending", "network": "sea", "origin_city": "Jinhua", "origin_country": "CN", "destination_city": "Kißlegg", "destination_country": "DE"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
        },
        {"id": 1263, "changed_at": "2026-05-11T10:00:00Z", "changed_by_name": "Max Mustermann", "metadata": {"file_name": "NGP3497068.pdf", "document_type": "master_bl"}},
    )

    message = result["message"]
    assert "Abgleich:" in message
    assert "POL passt" in message
    assert "POD passt" in message
    assert "Schiff passt" in message
    assert "ETD weicht ab" in message
    assert "MBL / B/L-Nr. fehlt im TMS" in message
    assert "Container fehlt im TMS" in message
    assert "Max Mustermann" in message
    assert "ETA nicht explizit" not in message
    assert "Entwurf (Draft)" not in message
    assert result["pending_action_summary"]["review"] == 1
    intents = result["document_review_intents"]
    assert [(intent["target"], intent["value"]) for intent in intents] == [
        ("etd_main_carriage", "2026-05-06"),
        ("mbl_number", "NGP3497068"),
        ("container_number", "XHCU2996441"),
    ]
    etd_intent = next(intent for intent in intents if intent["target"] == "etd_main_carriage")
    assert etd_intent["guardrails"]["write_supported"] is False
    assert result["teams_tms_review_cards"] == []
    assert result["side_effects"]["queued_tms_actions"] == 0
    pending_path = tmp_path / "orders" / "AN-11790" / "teams" / "pending_tms_actions.jsonl"
    assert not pending_path.exists()


def test_processor_result_uses_profile_fields_for_billing_without_route_noise(tmp_path):
    analysis_path = tmp_path / "billing_analysis.json"
    registry_path = tmp_path / "billing_registry.json"
    snapshot_path = tmp_path / "billing_tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": "Rechnung-209390.pdf",
                "doc_type": "billing",
                "summary": "Frachtrechnung mit Abgaben und Verzollung.",
                "references": ["AN-11849", "26DE4851ECA01VTYR8"],
                "extracted_fields": {
                    "invoice_number": "209390",
                    "amount": "37197,51",
                    "currency": "EUR",
                    "mrn": "26DE4851ECA01VTYR8",
                    "container_number": "CICU9983004",
                    "pol": "Shenzhen",
                    "pod": "Hamburg",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "analyzed_documents": [
                    {"filename": "Rechnung-209390.pdf", "analysis_path": str(analysis_path), "analysis_doc_type": "billing"}
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "freight_details": {"pol_code": "Zhengzhou", "pod_code": "DEHAM", "container_number": "CICU9983004"},
                    "transport_legs": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = _processor_result_from_report(
        {
            "order_id": "AN-11849",
            "case_root": str(tmp_path / "orders" / "AN-11849"),
            "tms_context": {"status": "customs_clearance", "network": "rail", "origin_city": "Shenzhen", "origin_country": "CN", "destination_city": "Erfurt", "destination_country": "DE"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
        },
        {"id": 1814, "changed_at": "2026-05-19T11:10:00Z", "metadata": {"document_type": "email", "invoice_number": "209390", "email_subject": "Ihre Rechnung 209390 | Sendung AN-11849"}},
    )

    message = result["message"]
    assert "Abrechnungsbeleg" in message
    assert "Aus dem Beleg sicher gelesen" in message or "Gelesen:" in message
    assert "Rechnungsnr. 209390" in message
    assert "Betrag 37197,51 EUR" in message
    assert "POL" not in message
    assert "POD" not in message
    assert result["document_field_comparison"] == [
        {"label": "Container", "tms": "CICU9983004", "doc": "CICU9983004", "status": "match", "target": "container_number"}
    ]
    assert result["document_review_intents"] == []


def test_processor_result_uses_suggested_profile_for_generic_email_offer_document(tmp_path):
    analysis_path = tmp_path / "offer_analysis.json"
    registry_path = tmp_path / "offer_registry.json"
    snapshot_path = tmp_path / "offer_tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": "Angebot-AN-13380-V1.pdf",
                "doc_type": "unknown",
                "confidence": "high",
                "suggested_registry_types": ["offer"],
                "extracted_fields": {
                    "document_type": "Angebot",
                    "shipment_number": "AN-13380",
                    "loading_place": "Wiesbaden",
                    "unloading_place": "Duisburg",
                    "incoterm_named_place": "EXW",
                    "pieces": "1",
                    "volume": "1.435 m³",
                    "goods_description": "Warenautomat",
                    "amount": "380,00",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "analyzed_documents": [
                    {"filename": "Angebot-AN-13380-V1.pdf", "analysis_path": str(analysis_path), "doc_type": "unknown"}
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps({"detail": {"freight_details": {}, "transport_legs": []}}, ensure_ascii=False),
        encoding="utf-8",
    )

    result = _processor_result_from_report(
        {
            "order_id": "AN-13380",
            "case_root": str(tmp_path / "orders" / "AN-13380"),
            "tms_context": {"status": "pickup_scheduled", "network": "road", "origin_city": "Wiesbaden", "origin_country": "DE", "destination_city": "Duisburg", "destination_country": "DE"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
        },
        {"id": 1843, "changed_at": "2026-05-19T13:09:00Z", "metadata": {"document_type": "email", "email_type": "transportauftrag", "email_subject": "CARGOLO Transportauftrag: AN-13380", "attached_documents_count": 1}},
    )

    assert result["document_activity_document_type"] == "offer"
    assert "Angebot" in result["message"]
    assert "email" not in result["message"].lower()
    assert "ETD" not in result["message"]


def test_processor_result_offer_filename_overrides_wrong_billing_event_type(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="Angebot-AN-13322-V1.pdf",
        event_doc_type="billing",
        analysis_doc_type="billing",
        extracted_fields={
            "document_type": "Angebot",
            "document_number": "AN-13322-V1",
            "amount": "3480.24 EUR",
            "customer_name": "Mainhattan-Wheels GmbH",
            "incoterm_named_place": "EXW",
        },
    )

    assert result["document_activity_document_type"] == "offer"
    assert "Angebot" in result["message"]
    assert "Abrechnungsbeleg" not in result["message"]
    assert result["document_review_intents"] == []


def test_processor_result_rejects_impossible_date_review_intent(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="arrival.pdf",
        event_doc_type="shipment_advice",
        analysis_doc_type="shipment_advice",
        extracted_fields={"eta": "31.02.2026"},
        freight_details={"pol_code": "CNNGB", "pod_code": "DEHAM", "mbl_number": "", "container_number": ""},
    )

    assert all(intent.get("target") != "estimated_delivery_date" for intent in result["document_review_intents"])


@pytest.mark.parametrize("placeholder", ["unknown", "nicht lesbar", "not readable", "unreadable", "n/a"])
def test_processor_result_rejects_placeholder_explicit_mbl_fields(tmp_path, placeholder):
    analysis_path = tmp_path / "placeholder_analysis.json"
    registry_path = tmp_path / "placeholder_registry.json"
    snapshot_path = tmp_path / "placeholder_tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": "draft-bl.pdf",
                "doc_type": "bill_of_lading",
                "summary": "B/L document with unreadable MBL field",
                "references": [],
                "extracted_fields": {
                    "mbl_number": placeholder,
                    "pol": "Ningbo, China",
                    "pod": "Hamburg, Germany",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "analyzed_documents": [
                    {"filename": "draft-bl.pdf", "analysis_path": str(analysis_path), "analysis_doc_type": "bill_of_lading"}
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "freight_details": {"pol_code": "CNNGB", "pod_code": "DEHAM", "mbl_number": "", "container_number": ""},
                    "transport_legs": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = _processor_result_from_report(
        {
            "order_id": "AN-11790",
            "case_root": str(tmp_path / "orders" / "AN-11790"),
            "tms_context": {"status": "customs_pending", "network": "sea"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
        },
        {"id": 1265, "changed_at": "2026-05-11T10:00:00Z", "metadata": {"file_name": "draft-bl.pdf", "document_type": "master_bl"}},
    )

    message = result["message"]
    assert "MBL / B/L-Nr. fehlt im TMS" not in message
    assert result["teams_tms_review_cards"] == []


@pytest.mark.parametrize("date_like_mbl", ["09.05.2026", "09/05/2026", "09.05.26", "09.05.2026MFD", "09-05-26ABC"])
def test_processor_result_rejects_eu_date_like_explicit_mbl_fields(tmp_path, date_like_mbl):
    analysis_path = tmp_path / "eu_date_like_analysis.json"
    registry_path = tmp_path / "eu_date_like_registry.json"
    snapshot_path = tmp_path / "eu_date_like_tms_snapshot.json"
    analysis_path.write_text(
        json.dumps(
            {
                "filename": "draft-bl.pdf",
                "doc_type": "bill_of_lading",
                "summary": "B/L document with date-like explicit MBL field",
                "references": ["XHCU2996441"],
                "extracted_fields": {
                    "mbl_number": date_like_mbl,
                    "pol": "Ningbo, China",
                    "pod": "Hamburg, Germany",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "analyzed_documents": [
                    {"filename": "draft-bl.pdf", "analysis_path": str(analysis_path), "analysis_doc_type": "bill_of_lading"}
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "detail": {
                    "freight_details": {"pol_code": "CNNGB", "pod_code": "DEHAM", "mbl_number": "", "container_number": ""},
                    "transport_legs": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = _processor_result_from_report(
        {
            "order_id": "AN-11790",
            "case_root": str(tmp_path / "orders" / "AN-11790"),
            "tms_context": {"status": "customs_pending", "network": "sea"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
        },
        {"id": 1266, "changed_at": "2026-05-11T10:00:00Z", "metadata": {"file_name": "draft-bl.pdf", "document_type": "master_bl"}},
    )

    intents = result["document_review_intents"]
    assert all(intent["target"] != "mbl_number" for intent in intents)
    assert "MBL / B/L-Nr. fehlt im TMS" not in result["message"]



def test_document_monitoring_uses_lifecycle_and_writes_single_report_location(tmp_path):
    lifecycle = {
        "status": "ok",
        "order_id": "AN-12345",
        "case_root": str(tmp_path / "orders" / "AN-12345"),
        "initialized": True,
        "history_sync_count": 0,
        "history_sync_error": None,
        "tms_snapshot_path": str(tmp_path / "orders" / "AN-12345" / "tms_snapshot.json"),
        "document_registry_path": str(tmp_path / "orders" / "AN-12345" / "documents" / "registry.json"),
        "registry": {
            "expected_types": ["commercial_invoice"],
            "received_types": [],
            "received_documents": [],
            "analyzed_documents": [],
        },
        "tms_snapshot": {"status": "confirmed", "detail": {"network": "rail", "status": "confirmed"}},
        "state": {},
    }
    Path(lifecycle["case_root"]).mkdir(parents=True)

    with patch("plugins.cargolo_ops.document_monitoring.sync_case_lifecycle", return_value=lifecycle):
        report = run_document_monitoring("AN-12345", storage_root=tmp_path)

    assert report["report_json_path"].endswith("orders/AN-12345/document_monitoring/latest_report.json")
    assert report["report_md_path"].endswith("orders/AN-12345/document_monitoring/latest_report.md")
    saved = json.loads(Path(report["report_json_path"]).read_text(encoding="utf-8"))
    assert saved["reconciliation"]["risk"] == "low"


def test_document_activity_monitor_filters_new_document_uploads_and_updates_cursor(tmp_path):
    class FakeProvider:
        def list_asr_activity_log(self, **kwargs):
            assert kwargs["entity_type"] == "document"
            assert kwargs.get("action") is None
            return {
                "status": "ok",
                "items": [
                    {
                        "id": 8,
                        "entity_type": "billing_item",
                        "action": "update",
                        "asr_request": {"request_number": "AN-11111"},
                    },
                    {
                        "id": 9,
                        "entity_type": "document",
                        "action": "upload",
                        "changed_at": 123456,
                        "metadata": {"file_name": "invoice.pdf", "document_type": "commercial_invoice"},
                        "asr_request": {"request_number": "AN-12345"},
                    },
                ],
            }

    fake_report = {
        "order_id": "AN-12345",
        "report_json_path": str(tmp_path / "orders" / "AN-12345" / "document_monitoring" / "latest_report.json"),
        "report_md_path": str(tmp_path / "orders" / "AN-12345" / "document_monitoring" / "latest_report.md"),
        "lifecycle": {"history_sync_count": 2, "history_sync_error": None},
        "registry_summary": {"received_documents": 1, "tms_documents": 1},
        "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
    }

    with patch("plugins.cargolo_ops.document_activity_monitor.build_tms_provider_from_env", return_value=FakeProvider()), patch(
        "plugins.cargolo_ops.document_activity_monitor.run_document_monitoring", return_value=fake_report
    ) as mock_monitor, patch(
        "plugins.cargolo_ops.document_activity_monitor.send_manual_ops_notification", return_value={"enabled": True, "delivered": 1}
    ) as mock_notify:
        result = run_document_activity_monitor(storage_root=tmp_path, max_events=5)

    assert result["processed_count"] == 1
    assert result["processed"][0]["activity_id"] == 9
    mock_monitor.assert_called_once()
    assert mock_monitor.call_args.kwargs["trigger_event"]["id"] == 9
    mock_notify.assert_called_once()
    state = json.loads((tmp_path / "runtime" / "document_activity_monitor_state.json").read_text(encoding="utf-8"))
    assert state["last_seen_activity_id"] == 9


def test_document_activity_monitor_baseline_now_sets_cursor_without_processing_or_notify(tmp_path):
    class FakeProvider:
        def list_asr_activity_log(self, **kwargs):
            return {
                "status": "ok",
                "items": [
                    {"id": 100, "entity_type": "document", "action": "upload", "metadata": {"file_name": "old.pdf"}, "asr_request": {"request_number": "AN-10000"}},
                    {"id": 105, "entity_type": "document", "action": "create", "metadata": {"file_name": "latest.pdf"}, "asr_request": {"request_number": "AN-10001"}},
                    {"id": 110, "entity_type": "shipment", "action": "update", "asr_request": {"request_number": "AN-10002"}},
                ],
            }

    with patch("plugins.cargolo_ops.document_activity_monitor.build_tms_provider_from_env", return_value=FakeProvider()), patch(
        "plugins.cargolo_ops.document_activity_monitor.run_document_monitoring"
    ) as mock_monitor, patch("plugins.cargolo_ops.document_activity_monitor.send_manual_ops_notification") as mock_notify:
        result = run_document_activity_monitor(storage_root=tmp_path, baseline_now=True)

    assert result["status"] == "baselined"
    assert result["baselined_activity_id"] == 105
    assert result["processed_count"] == 0
    mock_monitor.assert_not_called()
    mock_notify.assert_not_called()
    state = json.loads((tmp_path / "runtime" / "document_activity_monitor_state.json").read_text(encoding="utf-8"))
    assert state["last_seen_activity_id"] == 105
    assert state["processed_activity_ids"] == []
    assert state["baseline"]["policy"] == "old_backlog_deleted_start_from_current_activity_log"


def test_document_activity_monitor_after_baseline_processes_only_newer_events(tmp_path):
    (tmp_path / "runtime").mkdir(parents=True)
    (tmp_path / "runtime" / "document_activity_monitor_state.json").write_text(
        json.dumps({"last_seen_activity_id": 105, "processed_activity_ids": []}),
        encoding="utf-8",
    )

    class FakeProvider:
        def list_asr_activity_log(self, **kwargs):
            return {
                "status": "ok",
                "items": [
                    {"id": 104, "entity_type": "document", "action": "upload", "metadata": {"file_name": "old.pdf"}, "asr_request": {"request_number": "AN-OLD"}},
                    {"id": 106, "entity_type": "document", "action": "upload", "metadata": {"file_name": "new.pdf"}, "asr_request": {"request_number": "AN-NEW"}},
                ],
            }

    fake_report = {
        "order_id": "AN-NEW",
        "lifecycle": {},
        "registry_summary": {},
        "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
    }
    with patch("plugins.cargolo_ops.document_activity_monitor.build_tms_provider_from_env", return_value=FakeProvider()), patch(
        "plugins.cargolo_ops.document_activity_monitor.run_document_monitoring", return_value=fake_report
    ) as mock_monitor, patch("plugins.cargolo_ops.document_activity_monitor.send_manual_ops_notification", return_value={"enabled": True, "delivered": 1}):
        result = run_document_activity_monitor(storage_root=tmp_path, max_events=5)

    assert result["processed_count"] == 1
    assert result["processed"][0]["activity_id"] == 106
    mock_monitor.assert_called_once()


def test_document_activity_monitor_keeps_failed_event_retryable(tmp_path):
    (tmp_path / "runtime").mkdir(parents=True)
    (tmp_path / "runtime" / "document_activity_monitor_state.json").write_text(
        json.dumps({"last_seen_activity_id": 8, "processed_activity_ids": []}),
        encoding="utf-8",
    )

    class FakeProvider:
        def list_asr_activity_log(self, **kwargs):
            return {
                "status": "ok",
                "items": [
                    {"id": 9, "entity_type": "document", "action": "upload", "metadata": {"file_name": "broken.pdf"}, "asr_request": {"request_number": "AN-FAIL"}},
                    {"id": 10, "entity_type": "document", "action": "upload", "metadata": {"file_name": "ok.pdf"}, "asr_request": {"request_number": "AN-OK"}},
                ],
            }

    def fake_monitor(order_id, **kwargs):
        if order_id == "AN-FAIL":
            raise RuntimeError("temporary processing failure")
        return {"order_id": order_id, "lifecycle": {}, "registry_summary": {}, "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []}}

    with patch("plugins.cargolo_ops.document_activity_monitor.build_tms_provider_from_env", return_value=FakeProvider()), patch(
        "plugins.cargolo_ops.document_activity_monitor.run_document_monitoring", side_effect=fake_monitor
    ), patch("plugins.cargolo_ops.document_activity_monitor.send_manual_ops_notification", return_value={"enabled": True, "delivered": 1}):
        result = run_document_activity_monitor(storage_root=tmp_path, max_events=5)

    assert result["status"] == "partial_error"
    assert result["error_count"] == 1
    state = json.loads((tmp_path / "runtime" / "document_activity_monitor_state.json").read_text(encoding="utf-8"))
    assert state["last_seen_activity_id"] == 8
    assert 10 in state["processed_activity_ids"]


def test_document_activity_monitor_dry_run_does_not_update_cursor_or_notify(tmp_path):
    class FakeProvider:
        def list_asr_activity_log(self, **kwargs):
            return {
                "status": "ok",
                "items": [
                    {
                        "id": 12,
                        "entity_type": "document",
                        "action": "upload",
                        "metadata": {"file_name": "hawb.pdf", "document_type": "hawb"},
                        "asr_request": {"request_number": "AN-12505"},
                    }
                ],
            }

    with patch("plugins.cargolo_ops.document_activity_monitor.build_tms_provider_from_env", return_value=FakeProvider()), patch(
        "plugins.cargolo_ops.document_activity_monitor.run_document_monitoring"
    ) as mock_monitor, patch("plugins.cargolo_ops.document_activity_monitor.send_manual_ops_notification") as mock_notify:
        result = run_document_activity_monitor(storage_root=tmp_path, max_events=1, dry_run=True)

    assert result["dry_run"] is True
    assert result["processed_count"] == 1
    assert not (tmp_path / "runtime" / "document_activity_monitor_state.json").exists()
    mock_monitor.assert_not_called()
    mock_notify.assert_not_called()


def test_document_agent_review_script_accepts_valid_json_from_nonzero_hermes_exit(tmp_path):
    fake_hermes = tmp_path / "fake_hermes"
    argv_path = tmp_path / "argv.json"
    fake_hermes.write_text(
        "#!/usr/bin/env bash\n"
        f"ARGV_PATH={str(argv_path)!r} {sys.executable} - \"$@\" <<'PY'\n"
        "import json, os, sys\n"
        "open(os.environ['ARGV_PATH'], 'w', encoding='utf-8').write(json.dumps(sys.argv[1:]))\n"
        "print(json.dumps({\"sections\":{\"lage\":\"ok\",\"abgleich\":\"ok\",\"auffaellig\":\"ok\",\"empfehlung\":\"ok\",\"naechster_schritt\":\"ok\"},\"decision\":\"observe\",\"priority\":\"low\",\"needs_review\":False,\"confidence\":\"high\"}))\n"
        "sys.exit(250)\n"
        "PY\n",
        encoding="utf-8",
    )
    fake_hermes.chmod(0o755)
    packet = {"contract": "agent_first_document_review_v1", "order_id": "AN-TEST"}

    completed = subprocess.run(
        [sys.executable, "scripts/cargolo_document_agent_review.py"],
        input=json.dumps(packet),
        text=True,
        capture_output=True,
        cwd=Path(__file__).resolve().parents[2],
        env={"HERMES_BIN": str(fake_hermes)},
        check=False,
    )

    assert completed.returncode == 0
    argv = json.loads(argv_path.read_text(encoding="utf-8"))
    assert "--toolsets" in argv
    assert argv[argv.index("--toolsets") + 1] == "no_tools"
    parsed = json.loads(completed.stdout)
    assert parsed["decision"] == "observe"


def test_document_activity_monitor_accepts_document_create_events(tmp_path):
    class FakeProvider:
        def list_asr_activity_log(self, **kwargs):
            assert kwargs["entity_type"] == "document"
            assert kwargs.get("action") is None
            return {
                "status": "ok",
                "items": [
                    {
                        "id": 15,
                        "entity_type": "document",
                        "action": "create",
                        "metadata": {"file_name": "mbl.pdf", "document_type": "master_bl"},
                        "asr_request": {"request_number": "AN-12506"},
                    }
                ],
            }

    fake_report = {
        "order_id": "AN-12506",
        "report_json_path": str(tmp_path / "orders" / "AN-12506" / "document_monitoring" / "latest_report.json"),
        "report_md_path": str(tmp_path / "orders" / "AN-12506" / "document_monitoring" / "latest_report.md"),
        "lifecycle": {},
        "registry_summary": {},
        "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
    }

    with patch("plugins.cargolo_ops.document_activity_monitor.build_tms_provider_from_env", return_value=FakeProvider()), patch(
        "plugins.cargolo_ops.document_activity_monitor.run_document_monitoring", return_value=fake_report
    ) as mock_monitor, patch(
        "plugins.cargolo_ops.document_activity_monitor.send_manual_ops_notification", return_value={"enabled": True, "delivered": 1}
    ):
        result = run_document_activity_monitor(storage_root=tmp_path, max_events=1)

    assert result["processed_count"] == 1
    assert result["processed"][0]["activity_id"] == 15
    mock_monitor.assert_called_once()



def test_cross_document_conflict_matching_current_tms_value_does_not_create_review_card(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_CMD", raising=False)
    current_filename = "packing-list.pdf"
    invoice_path = tmp_path / "invoice.analysis.json"
    packing_path = tmp_path / "packing.analysis.json"
    registry_path = tmp_path / "registry.json"
    snapshot_path = tmp_path / "snapshot.json"
    invoice_path.write_text(
        json.dumps({"filename": "invoice.pdf", "doc_type": "commercial_invoice", "extracted_fields": {"pieces": "52", "gross_weight": "852"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    packing_path.write_text(
        json.dumps({"filename": current_filename, "doc_type": "packing_list", "extracted_fields": {"pieces": "54", "gross_weight": "896"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    registry = {
        "expected_types": [],
        "received_types": ["commercial_invoice", "packing_list"],
        "received_documents": [
            {"filename": "invoice.pdf", "analysis_status": "analyzed"},
            {"filename": current_filename, "analysis_status": "analyzed"},
        ],
        "analyzed_documents": [
            {"filename": "invoice.pdf", "analysis_path": str(invoice_path), "analysis_doc_type": "commercial_invoice"},
            {"filename": current_filename, "analysis_path": str(packing_path), "analysis_doc_type": "packing_list"},
        ],
    }
    registry_path.write_text(json.dumps(registry, ensure_ascii=False), encoding="utf-8")
    tms_snapshot = {"detail": {"network": "rail", "totals": {"total_pieces": "54", "total_weight_kg": "896"}, "freight_details": {}, "transport_legs": []}}
    snapshot_path.write_text(json.dumps(tms_snapshot, ensure_ascii=False), encoding="utf-8")
    reconciliation = reconcile_documents(order_id="AN-55555", tms_snapshot=tms_snapshot, registry=registry)

    result = _processor_result_from_report(
        {
            "order_id": "AN-55555",
            "case_root": str(tmp_path / "orders" / "AN-55555"),
            "tms_context": {"status": "in_transit", "network": "rail"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": reconciliation,
        },
        {"id": 2555, "changed_at": "2026-05-21T12:00:00Z", "metadata": {"file_name": current_filename, "document_type": "packing_list"}},
    )

    assert result["analysis_priority"] == "medium"
    assert result["agent_review_required"] is True
    assert result["evidence_summary"]["cross_document_comparison_count"] == 2
    assert {row["type"] for row in result["document_cross_document_comparison"]} == {"cross_document_weight_mismatch", "cross_document_piece_mismatch"}
    assert result["document_review_intents"] == []

    cards = _queue_document_review_cards(
        storage_root=tmp_path,
        order_id="AN-55555",
        intents=result["document_review_intents"],
        event={"id": 2555, "changed_at": "2026-05-21T12:00:00Z", "metadata": {"file_name": current_filename, "document_type": "packing_list"}},
    )
    assert cards == []
    assert not (tmp_path / "orders" / "AN-55555" / "teams" / "pending_tms_actions.jsonl").exists()
    assert not (tmp_path / "orders" / "AN-55555" / "teams" / "applied_tms_actions.jsonl").exists()


def test_cross_document_current_value_matching_tms_stays_case_context_without_card(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_CMD", raising=False)
    current_filename = "angebot.pdf"
    offer_path = tmp_path / "offer.analysis.json"
    packing_path = tmp_path / "packing.analysis.json"
    registry_path = tmp_path / "registry.json"
    snapshot_path = tmp_path / "snapshot.json"
    offer_path.write_text(
        json.dumps({"filename": current_filename, "doc_type": "offer", "extracted_fields": {"pieces": "112", "gross_weight": "2464 kg"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    packing_path.write_text(
        json.dumps({"filename": "packlist.pdf", "doc_type": "packing_list", "extracted_fields": {"pieces": "110", "gross_weight": "2391.4 kg"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    registry = {
        "expected_types": [],
        "received_types": ["offer", "packing_list"],
        "received_documents": [
            {"filename": current_filename, "analysis_status": "analyzed"},
            {"filename": "packlist.pdf", "analysis_status": "analyzed"},
        ],
        "analyzed_documents": [
            {"filename": current_filename, "analysis_path": str(offer_path), "analysis_doc_type": "offer"},
            {"filename": "packlist.pdf", "analysis_path": str(packing_path), "analysis_doc_type": "packing_list"},
        ],
    }
    registry_path.write_text(json.dumps(registry, ensure_ascii=False), encoding="utf-8")
    tms_snapshot = {"detail": {"network": "rail", "totals": {"total_pieces": "112", "total_weight_kg": "22"}, "cargo": [{"id": "cargo-1", "quantity": 112, "weight_kg": 22}], "freight_details": {}, "transport_legs": []}}
    snapshot_path.write_text(json.dumps(tms_snapshot, ensure_ascii=False), encoding="utf-8")
    reconciliation = reconcile_documents(order_id="AN-12354", tms_snapshot=tms_snapshot, registry=registry)

    result = _processor_result_from_report(
        {
            "order_id": "AN-12354",
            "case_root": str(tmp_path / "orders" / "AN-12354"),
            "tms_context": {"status": "in_transit", "network": "rail"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": reconciliation,
        },
        {"id": 2141, "changed_at": "2026-05-21T13:59:00Z", "metadata": {"file_name": current_filename, "document_type": "offer"}},
    )

    assert any(row["type"] == "cross_document_piece_mismatch" for row in result["document_cross_document_comparison"])
    assert "Dokumentkonflikt Gewicht:" in result["message"]
    assert "2391.4 kg" in result["message"] and "2464 kg" in result["message"]
    assert "Dokumentkonflikt Packstücke:" in result["message"]
    assert "110" in result["message"] and "112" in result["message"]
    assert result["document_agent_evidence_packet"]["deterministic_evidence"]["cross_document_conflict_lines"]
    assert [(intent["target"], intent["value"], intent["source"]) for intent in result["document_review_intents"]] == [
        ("cargo_weight_kg", "2464 kg", "document_activity_monitor"),
    ]

    cards = _queue_document_review_cards(
        storage_root=tmp_path,
        order_id="AN-12354",
        intents=result["document_review_intents"],
        event={"id": 2141, "changed_at": "2026-05-21T13:59:00Z", "metadata": {"file_name": current_filename, "document_type": "offer"}},
    )
    assert [(card["target"], card["value"]) for card in cards] == [("cargo_weight_kg", "2464 kg")]


def test_cross_document_conflict_between_non_current_documents_stays_case_context(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_CMD", raising=False)
    reconciliation = {
        "risk": "medium",
        "needs_human_review": True,
        "findings": [
            {
                "type": "cross_document_weight_mismatch",
                "severity": "medium",
                "scope": "cross_document_comparison",
                "filenames": ["invoice.pdf", "packing-list.pdf"],
                "summary": "Dokumente widersprechen sich bei Gewicht: invoice.pdf: 852 kg; packing-list.pdf: 896 kg.",
                "documents": [
                    {"filename": "invoice.pdf", "doc_type": "commercial_invoice", "normalized": 852},
                    {"filename": "packing-list.pdf", "doc_type": "packing_list", "normalized": 896},
                ],
                "target": "cargo_weight_kg",
                "review_only": True,
                "write_supported": False,
            }
        ],
        "cross_document_comparisons": [
            {
                "type": "cross_document_weight_mismatch",
                "field": "gross_weight",
                "target": "cargo_weight_kg",
                "label": "Gewicht",
                "status": "conflict",
                "filenames": ["invoice.pdf", "packing-list.pdf"],
                "documents": [
                    {"filename": "invoice.pdf", "doc_type": "commercial_invoice", "normalized": 852},
                    {"filename": "packing-list.pdf", "doc_type": "packing_list", "normalized": 896},
                ],
            }
        ],
    }
    result = _processor_result_from_report(
        {
            "order_id": "AN-55556",
            "tms_context": {"status": "in_transit", "network": "rail"},
            "lifecycle": {},
            "registry_summary": {},
            "reconciliation": reconciliation,
        },
        {"id": 2556, "changed_at": "2026-05-21T12:00:00Z", "metadata": {"file_name": "delivery-note.pdf", "document_type": "delivery_note"}},
    )

    assert result["analysis_priority"] == "low"
    assert result["agent_review_required"] is False
    assert result["document_review_intents"] == []
    assert result["evidence_summary"]["case_reconciliation_risk"] == "medium"
    assert result["evidence_summary"]["current_upload_review"]["case_risk_not_used_for_document_priority"] is True



def test_archived_email_subject_version_selects_matching_offer_not_oldest(tmp_path):
    registry_path = tmp_path / "registry.json"
    snapshot_path = tmp_path / "snapshot.json"
    analysis_paths = []
    docs = []
    for version, amount in (("V1", "585,83"), ("V2", "1.088,20")):
        analysis_path = tmp_path / f"offer_{version}.json"
        filename = f"Angebot-AN-13363-{version}.pdf"
        analysis_path.write_text(
            json.dumps(
                {
                    "filename": filename,
                    "doc_type": "offer",
                    "confidence": "high",
                    "extracted_fields": {
                        "document_type": "Angebot",
                        "shipment_number": "AN-13363",
                        "amount": amount,
                        "currency": "EUR",
                        "pieces": "20",
                        "gross_weight": "160 kg",
                        "incoterm_named_place": "EXW",
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        analysis_paths.append(analysis_path)
        docs.append({"filename": filename, "analysis_path": str(analysis_path), "analysis_doc_type": "offer", "received_at": f"2026-05-19T09:5{1 if version == 'V1' else 9}:00Z"})
    registry_path.write_text(json.dumps({"analyzed_documents": docs, "received_documents": docs}, ensure_ascii=False), encoding="utf-8")
    snapshot_path.write_text(json.dumps({"detail": {"totals": {"total_pieces": 20, "total_weight_kg": 160}, "freight_details": {}, "transport_legs": []}}, ensure_ascii=False), encoding="utf-8")

    result = _processor_result_from_report(
        {
            "order_id": "AN-13363",
            "case_root": str(tmp_path / "orders" / "AN-13363"),
            "tms_context": {"status": "addresses_pending", "network": "air", "origin_city": "Dongguan", "origin_country": "CN", "destination_city": "Egling", "destination_country": "DE"},
            "lifecycle": {"document_registry_path": str(registry_path), "tms_snapshot_path": str(snapshot_path)},
            "registry_summary": {},
            "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
        },
        {"id": 2107, "changed_at": "2026-05-21T11:50:00Z", "metadata": {"file_name": "AW_ Ihr Luftfracht Transportangebot AN-13363-V2 ist bereit.msg", "document_type": "email", "email_subject": "AW: Ihr Luftfracht Transportangebot AN-13363-V2 ist bereit"}},
    )

    assert result.get("document_activity_filename") in (None, "Angebot-AN-13363-V2.pdf")
    assert "1.088,20" in result["message"]
    assert "585,83" not in result["message"]
