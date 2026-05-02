from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from plugins.cargolo_ops.document_monitoring import run_document_monitoring
from plugins.cargolo_ops.document_reconciliation import reconcile_documents


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
