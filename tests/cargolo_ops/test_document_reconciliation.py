from __future__ import annotations

import json

from plugins.cargolo_ops.document_reconciliation import reconcile_documents


def test_reconciliation_treats_existing_blocker_finding_as_high_risk(tmp_path):
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
    assert report["needs_human_review"] is True
    assert any(row.get("severity") == "blocker" and row.get("code") == "zero_value" for row in report["findings"])


def test_reconciliation_treats_commercial_invoice_proforma_as_blocker(tmp_path):
    analysis_path = tmp_path / "invoice_proforma_analysis.json"
    analysis_path.write_text(
        json.dumps(
            {
                "doc_type": "commercial_invoice",
                "summary": "Proforma invoice for customs preview",
                "extracted_fields": {"goods_value": "100", "currency": "EUR"},
            },
            ensure_ascii=False,
        ),
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
    assert report["needs_human_review"] is True
    assert any(row.get("severity") == "blocker" and row.get("code") == "proforma_invoice" for row in report["findings"])


def test_reconciliation_filters_low_signal_excel_packlist_noise(tmp_path):
    analysis_path = tmp_path / "packlist_analysis.json"
    analysis_path.write_text(
        json.dumps(
            {
                "doc_type": "packing_list",
                "extracted_fields": {"document_number": "ESDE20251211", "gross_weight": "896"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    report = reconcile_documents(
        order_id="AN-13329",
        tms_snapshot={"detail": {"network": "rail", "totals": {"total_weight_kg": "896"}}},
        registry={
            "expected_types": [],
            "received_types": ["packing_list"],
            "received_documents": [{"filename": "packliste.xlsx", "analysis_status": "analyzed"}],
            "analyzed_documents": [
                {
                    "filename": "packliste.xlsx",
                    "analysis_doc_type": "packing_list",
                    "analysis_path": str(analysis_path),
                    "operational_flags": ["missing_net_weight", "net_weight (specified as 0)", "date"],
                    "missing_or_unreadable": ["date"],
                }
            ],
        },
    )

    assert report["risk"] == "low"
    assert report["needs_human_review"] is False
    assert report["findings"] == []


def test_reconciliation_keeps_concrete_weight_mismatch_after_noise_filter(tmp_path):
    analysis_path = tmp_path / "packlist_mismatch_analysis.json"
    analysis_path.write_text(
        json.dumps({"doc_type": "packing_list", "extracted_fields": {"gross_weight": "896"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    report = reconcile_documents(
        order_id="AN-13329",
        tms_snapshot={"detail": {"network": "rail", "totals": {"total_weight_kg": "950"}}},
        registry={
            "expected_types": [],
            "received_types": ["packing_list"],
            "received_documents": [{"filename": "packliste.xlsx", "analysis_status": "analyzed"}],
            "analyzed_documents": [
                {
                    "filename": "packliste.xlsx",
                    "analysis_doc_type": "packing_list",
                    "analysis_path": str(analysis_path),
                    "operational_flags": ["missing_net_weight", "date"],
                }
            ],
        },
    )

    assert report["risk"] == "medium"
    assert any(row.get("type") == "tms_document_weight_mismatch" for row in report["findings"])



def test_reconciliation_keeps_net_weight_zero_flag_on_non_packlist_document(tmp_path):
    analysis_path = tmp_path / "invoice_analysis.json"
    analysis_path.write_text(
        json.dumps({"doc_type": "commercial_invoice", "extracted_fields": {"invoice_number": "INV-1"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    report = reconcile_documents(
        order_id="AN-13329",
        tms_snapshot={"detail": {"network": "rail", "totals": {"total_weight_kg": "896"}}},
        registry={
            "expected_types": [],
            "received_types": ["commercial_invoice"],
            "received_documents": [{"filename": "invoice.pdf", "analysis_status": "analyzed"}],
            "analyzed_documents": [
                {
                    "filename": "invoice.pdf",
                    "analysis_doc_type": "commercial_invoice",
                    "analysis_path": str(analysis_path),
                    "operational_flags": ["net_weight specified as 0"],
                }
            ],
        },
    )

    assert report["risk"] == "medium"
    assert any(row.get("type") == "document_flag" and "nettogewicht" in row.get("summary", "").lower() for row in report["findings"])



def test_reconciliation_keeps_net_weight_zero_flag_on_non_packlist_spreadsheet(tmp_path):
    analysis_path = tmp_path / "invoice_xlsx_analysis.json"
    analysis_path.write_text(
        json.dumps({"doc_type": "commercial_invoice", "extracted_fields": {"invoice_number": "INV-2"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    report = reconcile_documents(
        order_id="AN-13329",
        tms_snapshot={"detail": {"network": "rail", "totals": {"total_weight_kg": "896"}}},
        registry={
            "expected_types": [],
            "received_types": ["commercial_invoice"],
            "received_documents": [{"filename": "invoice.xlsx", "analysis_status": "analyzed"}],
            "analyzed_documents": [
                {
                    "filename": "invoice.xlsx",
                    "analysis_doc_type": "commercial_invoice",
                    "analysis_path": str(analysis_path),
                    "operational_flags": ["net_weight specified as 0"],
                }
            ],
        },
    )

    assert report["risk"] == "medium"
    assert any(row.get("type") == "document_flag" and "nettogewicht" in row.get("summary", "").lower() for row in report["findings"])
