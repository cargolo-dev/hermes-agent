import json
from unittest.mock import patch

from plugins.cargolo_ops.models import ProcessingResult
from plugins.cargolo_ops.ops_notifications import build_manual_ops_notification_body, send_manual_ops_notification
from tools.cargolo_asr_tool import (
    cargolo_asr_bootstrap_case_tool,
    cargolo_asr_bootstrap_cases_from_tms_tool,
    cargolo_asr_process_event_tool,
)


class _Response:
    def raise_for_status(self):
        return None


def test_build_manual_ops_notification_body_returns_html_payload(tmp_path):
    case_report_path = tmp_path / "case_report_latest.json"
    case_report_path.write_text(
        json.dumps(
            {
                "source_artifacts": {"case_report": "analysis/case_report_latest.json"},
                "sections": {
                    "mail_history": {
                        "email_count_total": {"value": 7, "source": "email_index.jsonl"},
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    body = build_manual_ops_notification_body(
        run_type="process_event",
        payload={
            "order_id": "AN-12001",
            "processor_result": {
                "order_id": "AN-12001",
                "status": "processed",
                "history_sync_count": 7,
                "pending_action_summary": {"write_now": 1},
                "case_report_path": str(case_report_path),
            },
        },
        route_name="cargolo-asr-ingest",
        delivery_id="delivery-1",
        delivered_at=123.0,
    )
    assert body["route"] == "cargolo-asr-ingest"
    assert body["delivery_id"] == "delivery-1"
    assert body["delivered_at"] == 123.0
    assert body["message_format"] == "html"
    assert "<html><body" in body["message"]
    assert body["payload"]["run_type"] == "process_event"
    assert body["payload"]["processor_result"]["order_id"] == "AN-12001"
    assert "TMS-Aktion" in body["message"]
    assert "Nächster Schritt" in body["message"]
    assert "Webhook-Kurzfazit" not in body["message"]
    assert len(body["message_text"].splitlines()) <= 4


def test_document_activity_notification_renders_operator_card(tmp_path):
    registry_path = tmp_path / "document_registry.json"
    analysis_path = tmp_path / "analysis.json"
    report_path = tmp_path / "document_monitoring_latest.json"
    analysis_path.write_text(
        json.dumps(
            {
                "doc_type": "commercial_invoice",
                "confidence": "high",
                "summary": "Handelsrechnung erkannt; Referenz und Betrag lesbar.",
                "extracted_fields": {"invoice_number": "CI-777", "amount": "1234.50", "currency": "EUR"},
                "consistency_notes": ["Gesamtgewicht laut Dokument 123kg weicht vom TMS-Wert 500kg ab."],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps(
            {
                "received_documents": [{"filename": "invoice.pdf"}],
                "mirrored_tms_documents": [{"document_type": "commercial_invoice"}],
                "analyzed_documents": [
                    {
                        "filename": "invoice.pdf",
                        "analysis_path": str(analysis_path),
                        "analysis_doc_type": "commercial_invoice",
                        "analysis_confidence": "high",
                        "tms_matches": [
                            {"document_type": "commercial_invoice", "filename": "invoice.pdf", "match_basis": ["filename"]}
                        ],
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    report_path.write_text(
        json.dumps(
            {
                "order_id": "AN-12505",
                "lifecycle": {
                    "history_sync_count": 3,
                    "last_email_at": "2026-05-08T08:22:00Z",
                    "document_registry_path": str(registry_path),
                },
                "tms_context": {
                    "customer": "Cargolo Testkunde",
                    "status": "in_transit",
                    "network": "ASR",
                    "origin_city": "Ningbo",
                    "origin_country": "CN",
                    "destination_city": "Hamburg",
                    "destination_country": "DE",
                    "incoterms": "FOB",
                    "pieces": 4,
                    "weight_kg": 500,
                    "cargo_description": "Bike parts",
                },
                "reconciliation": {"risk": "low", "needs_human_review": False, "findings": []},
                "trigger_event": {
                    "id": 12505,
                    "changed_at": "2026-05-08T08:30:00Z",
                    "changed_by_name": "Kundenportal",
                    "source": "customer_portal",
                    "metadata": {"file_name": "invoice.pdf", "document_type": "commercial_invoice"},
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    body = build_manual_ops_notification_body(
        run_type="document_activity_monitor",
        payload={
            "order_id": "AN-12505",
            "activity_event": {
                "changed_at": "2026-05-08T08:30:00Z",
                "changed_by_name": "Kundenportal",
                "source": "customer_portal",
                "metadata": {"file_name": "invoice.pdf", "document_type": "commercial_invoice"},
            },
            "processor_result": {
                "order_id": "AN-12505",
                "status": "document_uploaded_checked",
                "history_sync_count": 3,
                "last_email_at": "2026-05-08T08:22:00Z",
                "document_monitoring_report_path": str(report_path),
            },
        },
    )

    assert body["message_format"] == "html"
    assert "Dokumenten-Upload" in body["message"]
    assert "Dokument erkannt" in body["message"]
    assert "Abgleich" in body["message"]
    assert "Nächster Schritt" in body["message"]
    assert "Konkrete Abweichung" in body["message"]
    assert "Gesamtgewicht laut Dokument 123kg" in body["message"]
    assert "Handelsrechnung" in body["message"]
    assert "AN-12505 · Dokument-Check" in body["message_text"]
    assert "Ich sehe eine Gewichts-/Mengenabweichung" in body["message_text"]
    assert "Frage an euch:" in body["message_text"]
    assert "Vorschlag:" in body["message_text"]
    assert "Mail +3" in body["message_text"]
    assert "123kg vs 500kg" in body["message_text"]
    assert "Kontext:" in body["message_text"]


def test_send_manual_ops_notification_uses_native_teams_gateway_route(tmp_path, monkeypatch):
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir(parents=True, exist_ok=True)
    (hermes_home / "webhook_subscriptions.json").write_text(
        json.dumps(
            {
                "cargolo-asr-ops-teams": {
                    "events": ["cargolo_asr_manual_ops_notification"],
                    "secret": "native-teams-secret",
                    "deliver_only": True,
                    "deliver": "teams",
                    "prompt": "{message_text}",
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.delenv("HERMES_CARGOLO_ASR_OPS_DELIVERY", raising=False)

    captured = {}
    case_report_path = tmp_path / "case_report_latest.json"
    case_report_path.write_text(
        json.dumps(
            {
                "source_artifacts": {"case_report": "analysis/case_report_latest.json"},
                "sections": {
                    "mail_history": {
                        "email_count_total": {"value": 7, "source": "email_index.jsonl"},
                        "latest_subjects": {"value": ["AN-12001 // Test"], "source": "email_index.jsonl"},
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def _fake_request(method, url, data=None, headers=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["data"] = data
        captured["json"] = json.loads(data.decode("utf-8"))
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _Response()

    with patch("plugins.cargolo_ops.ops_notifications.requests.request", side_effect=_fake_request):
        result = send_manual_ops_notification(
            run_type="bootstrap_case",
            payload={
                "order_id": "AN-12001",
                "processor_result": {
                    "order_id": "AN-12001",
                    "status": "bootstrapped",
                    "history_sync_count": 7,
                    "pending_action_summary": {"write_now": 1},
                    "case_report_path": str(case_report_path),
                },
            },
            allow_route_fallback=True,
        )

    assert result["enabled"] is True
    assert result["delivered"] == 1
    assert result["targets"] == ["native_teams_route:cargolo-asr-ops-teams"]
    assert captured["url"] == "http://127.0.0.1:8644/webhooks/cargolo-asr-ops-teams"
    assert captured["headers"]["X-Hub-Signature-256"].startswith("sha256=")
    assert captured["json"]["event_type"] == "cargolo_asr_manual_ops_notification"
    assert captured["json"]["payload"]["run_type"] == "bootstrap_case"
    assert captured["json"]["payload"]["processor_result"]["order_id"] == "AN-12001"
    assert captured["json"]["message_format"] == "html"
    assert "<html><body" in captured["json"]["message"]
    assert "AN-12001 | bootstrapped" in captured["json"]["message_text"]
    assert "Nächster Schritt:" in captured["json"]["message_text"]
    assert "Mail +7" in captured["json"]["message_text"]
    assert len(captured["json"]["message_text"].splitlines()) <= 4


def test_process_event_tool_includes_ops_notification_by_default(tmp_path):
    result = ProcessingResult(status="processed", order_id="AN-10874", message="ok")

    with patch("tools.cargolo_asr_tool.process_email_event", return_value=result), patch(
        "tools.cargolo_asr_tool.send_manual_ops_notification",
        return_value={"enabled": True, "delivered": 1},
    ) as mock_notify:
        payload = json.loads(
            cargolo_asr_process_event_tool(
                {
                    "payload": {
                        "event_type": "asr_email_thread",
                        "an": "AN-10874",
                        "mailbox": "asr@cargolo.com",
                        "trigger_message_id": "<m1>",
                        "messages": [
                            {
                                "message_id": "<m1>",
                                "subject": "AN-10874",
                                "from": "ops@example.com",
                                "to": ["asr@cargolo.com"],
                                "received_at": "2026-04-20T10:00:00Z",
                                "body_text": "Body",
                            }
                        ],
                    },
                    "storage_root": str(tmp_path),
                }
            )
        )

    assert payload["order_id"] == "AN-10874"
    assert payload["ops_notification"]["delivered"] == 1
    mock_notify.assert_called_once()
    assert mock_notify.call_args.kwargs["run_type"] == "process_event"


def test_bootstrap_case_tool_can_disable_ops_notification(tmp_path):
    result = ProcessingResult(status="bootstrapped", order_id="AN-12001", message="done")

    with patch("tools.cargolo_asr_tool.bootstrap_case", return_value=result), patch(
        "tools.cargolo_asr_tool.send_manual_ops_notification"
    ) as mock_notify:
        payload = json.loads(
            cargolo_asr_bootstrap_case_tool(
                {
                    "an": "AN-12001",
                    "storage_root": str(tmp_path),
                    "notify_ops_webhook": False,
                }
            )
        )

    assert payload["order_id"] == "AN-12001"
    assert "ops_notification" not in payload
    mock_notify.assert_not_called()


def test_bootstrap_cases_from_tms_tool_includes_ops_notification(tmp_path):
    batch_result = {
        "status": "ok",
        "total_selected": 2,
        "success_count": 2,
        "error_count": 0,
        "results": [{"order_id": "AN-1"}, {"order_id": "AN-2"}],
    }

    with patch("tools.cargolo_asr_tool.bootstrap_cases_from_tms", return_value=batch_result), patch(
        "tools.cargolo_asr_tool.send_manual_ops_notification",
        return_value={"enabled": True, "delivered": 1},
    ) as mock_notify:
        payload = json.loads(
            cargolo_asr_bootstrap_cases_from_tms_tool(
                {
                    "storage_root": str(tmp_path),
                }
            )
        )

    assert payload["success_count"] == 2
    assert payload["ops_notification"]["delivered"] == 1
    assert mock_notify.call_args.kwargs["run_type"] == "bootstrap_cases_from_tms"
