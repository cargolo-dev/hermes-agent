from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from plugins.cargolo_ops.document_monitoring import run_document_monitoring
from plugins.cargolo_ops.document_reconciliation import reconcile_documents
from plugins.cargolo_ops.document_activity_monitor import _processor_result_from_report, run_document_activity_monitor


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
                    "transport_legs": [],
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


def test_processor_result_keeps_untrusted_invoice_mbl_text_but_skips_teams_card(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="invoice-with-mbl.pdf",
        event_doc_type="commercial_invoice",
        analysis_doc_type="commercial_invoice",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert "MBL / B/L-Nr. fehlt im TMS" in result["message"]
    assert result["teams_tms_review_cards"] == []


def test_processor_result_queues_trusted_master_bl_mbl_card(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="master-bl.pdf",
        event_doc_type="master_bl",
        analysis_doc_type="master_bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert [(card["target"], card["value"]) for card in result["teams_tms_review_cards"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_uses_analyzed_master_bl_type_when_event_has_legacy_bill_of_lading(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="legacy-event-bl.pdf",
        event_doc_type="bill_of_lading",
        analysis_doc_type="master_bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert [(card["target"], card["value"]) for card in result["teams_tms_review_cards"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_allows_legacy_bill_of_lading_with_explicit_bl_field_evidence(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="legacy-explicit-bl.pdf",
        event_doc_type="bill_of_lading",
        analysis_doc_type="bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert [(card["target"], card["value"]) for card in result["teams_tms_review_cards"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_uses_analyzed_master_bl_type_when_event_doc_type_missing(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="missing-event-type-bl.pdf",
        event_doc_type="",
        analysis_doc_type="master_bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert [(card["target"], card["value"]) for card in result["teams_tms_review_cards"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_allows_missing_event_type_with_analyzed_legacy_bl_evidence(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="missing-event-legacy-bl.pdf",
        event_doc_type="",
        analysis_doc_type="bill_of_lading",
        extracted_fields={"mbl_number": "NGP3497068", "pol": "Ningbo, China", "pod": "Hamburg, Germany"},
    )

    assert [(card["target"], card["value"]) for card in result["teams_tms_review_cards"]] == [("mbl_number", "NGP3497068")]


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

    assert [(card["target"], card["value"]) for card in result["teams_tms_review_cards"]] == [("mbl_number", "NGP3497068")]


def test_processor_result_queues_trusted_packing_list_valid_container_card(tmp_path):
    result = _processor_result_for_uploaded_fields(
        tmp_path,
        filename="packing-list.pdf",
        event_doc_type="packing_list",
        analysis_doc_type="packing_list",
        extracted_fields={"pol": "Ningbo, China", "pod": "Hamburg, Germany"},
        references=["XHCU2996441"],
    )

    assert [(card["target"], card["value"]) for card in result["teams_tms_review_cards"]] == [("container_number", "XHCU2996441")]


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
    assert [(card["target"], card["value"]) for card in result["teams_tms_review_cards"]] == [("container_number", "XHCU2996441")]


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
    cards = result["teams_tms_review_cards"]
    assert [(card["target"], card["value"]) for card in cards] == [("mbl_number", "NGP3497068"), ("container_number", "XHCU2996441")]
    assert all(card["action_id"] for card in cards)
    pending_path = tmp_path / "orders" / "AN-11790" / "teams" / "pending_tms_actions.jsonl"
    assert pending_path.exists()


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

    cards = result["teams_tms_review_cards"]
    assert all(card["target"] != "mbl_number" for card in cards)
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
            assert kwargs["action"] == "upload"
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
