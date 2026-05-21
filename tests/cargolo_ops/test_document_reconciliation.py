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



def _analysis_file(tmp_path, filename: str, doc_type: str, fields: dict[str, str]):
    path = tmp_path / f"{filename}.analysis.json"
    path.write_text(json.dumps({"filename": filename, "doc_type": doc_type, "extracted_fields": fields}, ensure_ascii=False), encoding="utf-8")
    return path


def test_reconciliation_detects_cross_document_weight_and_piece_conflicts(tmp_path):
    invoice_path = _analysis_file(tmp_path, "invoice.pdf", "commercial_invoice", {"gross_weight": "852", "pieces": "52"})
    packing_path = _analysis_file(tmp_path, "packing-list.pdf", "packing_list", {"gross_weight": "896", "pieces": "54"})

    report = reconcile_documents(
        order_id="AN-XDOC",
        tms_snapshot={"detail": {"network": "rail", "totals": {"total_weight_kg": "852", "total_pieces": "52"}}},
        registry={
            "expected_types": [],
            "received_types": ["commercial_invoice", "packing_list"],
            "received_documents": [
                {"filename": "invoice.pdf", "analysis_status": "analyzed"},
                {"filename": "packing-list.pdf", "analysis_status": "analyzed"},
            ],
            "analyzed_documents": [
                {"filename": "invoice.pdf", "analysis_doc_type": "commercial_invoice", "analysis_path": str(invoice_path)},
                {"filename": "packing-list.pdf", "analysis_doc_type": "packing_list", "analysis_path": str(packing_path)},
            ],
        },
    )

    assert report["risk"] == "medium"
    assert report["needs_human_review"] is True
    finding_types = {row.get("type") for row in report["findings"]}
    assert "cross_document_weight_mismatch" in finding_types
    assert "cross_document_piece_mismatch" in finding_types
    cross = {row["type"]: row for row in report["cross_document_comparisons"]}
    assert cross["cross_document_weight_mismatch"]["write_supported"] is False
    assert cross["cross_document_weight_mismatch"]["review_only"] is True
    assert set(cross["cross_document_weight_mismatch"]["filenames"]) == {"invoice.pdf", "packing-list.pdf"}
    assert "852 kg" in cross["cross_document_weight_mismatch"]["summary"]
    assert "896 kg" in cross["cross_document_weight_mismatch"]["summary"]


def test_reconciliation_suppresses_cross_document_numeric_format_variants(tmp_path):
    invoice_path = _analysis_file(tmp_path, "invoice.pdf", "commercial_invoice", {"gross_weight": "2.464 kg", "pieces": "112.0"})
    packing_path = _analysis_file(tmp_path, "packing-list.pdf", "packing_list", {"gross_weight": "2464", "pieces": "112"})

    report = reconcile_documents(
        order_id="AN-XDOC",
        tms_snapshot={"detail": {"network": "rail"}},
        registry={
            "expected_types": [],
            "received_types": ["commercial_invoice", "packing_list"],
            "received_documents": [],
            "analyzed_documents": [
                {"filename": "invoice.pdf", "analysis_doc_type": "commercial_invoice", "analysis_path": str(invoice_path)},
                {"filename": "packing-list.pdf", "analysis_doc_type": "packing_list", "analysis_path": str(packing_path)},
            ],
        },
    )

    assert not any(str(row.get("type", "")).startswith("cross_document_") for row in report["findings"])
    assert report["cross_document_comparisons"] == []
