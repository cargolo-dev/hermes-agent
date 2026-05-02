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


def test_send_manual_ops_notification_uses_route_webhook_forward(tmp_path, monkeypatch):
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir(parents=True, exist_ok=True)
    (hermes_home / "webhook_subscriptions.json").write_text(
        json.dumps(
            {
                "cargolo-asr-ingest": {
                    "deliver_additional": [
                        {
                            "deliver": "webhook_forward",
                            "deliver_extra": {
                                "url": "https://example.test/asr-ops",
                                "method": "POST",
                                "headers": {"X-Test": "1"},
                            },
                        }
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))

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

    def _fake_request(method, url, json=None, headers=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = json
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
    assert captured["url"] == "https://example.test/asr-ops"
    assert captured["json"]["payload"]["event_type"] == "cargolo_asr_manual_ops_notification"
    assert captured["json"]["payload"]["run_type"] == "bootstrap_case"
    assert captured["json"]["payload"]["processor_result"]["order_id"] == "AN-12001"
    assert captured["json"]["message_format"] == "html"
    assert "<html><body" in captured["json"]["message"]
    assert "CARGOLO ASR" in captured["json"]["message"]
    assert "TMS-Aktion" in captured["json"]["message"]
    assert "Nächster Schritt" in captured["json"]["message"]
    assert "Webhook-Kurzfazit" not in captured["json"]["message"]
    assert "Quellartefakte" not in captured["json"]["message"]
    assert "Mail History" not in captured["json"]["message"]
    assert "AN-12001" in captured["json"]["message"]
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
