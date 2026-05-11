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
