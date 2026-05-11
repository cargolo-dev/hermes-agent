from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from plugins.cargolo_ops.document_monitoring import run_document_monitoring
from plugins.cargolo_ops.document_reconciliation import reconcile_documents
from plugins.cargolo_ops.document_activity_monitor import _processor_result_from_report, run_document_activity_monitor


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
