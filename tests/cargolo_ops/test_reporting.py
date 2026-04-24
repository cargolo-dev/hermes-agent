from plugins.cargolo_ops.processor import process_email_event
from plugins.cargolo_ops.reporting import generate_daily_report


def payload(order_id: str, subject: str, body_text: str):
    return {
        "event_type": "asr_email_thread",
        "mailbox": "asr@cargolo.com",
        "an": order_id,
        "trigger_message_id": f"<{order_id}@example.com>",
        "trigger_conversation_id": f"thread-{order_id}",
        "message_count": 1,
        "messages": [
            {
                "message_id": f"<{order_id}@example.com>",
                "conversation_id": f"thread-{order_id}",
                "subject": subject,
                "from": "customer@example.com",
                "to": ["asr@cargolo.com"],
                "cc": [],
                "received_at": "2026-04-10T08:00:00Z",
                "body_text": body_text,
                "attachments": [],
                "attachment_count": 0,
                "has_attachments": False,
            }
        ],
    }


def test_generate_daily_report_aggregates_cases(tmp_path):
    process_email_event(payload("AN-10001", "Complaint AN-10001", "Complaint and delay issue for AN-10001"), storage_root=tmp_path, create_task=True, refresh_history=False)
    process_email_event(payload("AN-10002", "Quote AN-10002", "Please quote pickup and destination for AN-10002"), storage_root=tmp_path, refresh_history=False)
    report = generate_daily_report(tmp_path)
    assert "markdown" in report
    assert report["exceptions_by_mode"]["unknown"] >= 1
    assert len(report["open_cases_without_reply"]) >= 1
