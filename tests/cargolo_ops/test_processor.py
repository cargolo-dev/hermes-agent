import json
from pathlib import Path

from plugins.cargolo_ops.processor import process_email_event


def sample_payload():
    return {
        "event_type": "asr_email_thread",
        "mailbox": "asr@cargolo.com",
        "an": "AN-10874",
        "trigger_message_id": "<msg-1@example.com>",
        "trigger_conversation_id": "thread-1",
        "message_count": 1,
        "messages": [
            {
                "message_id": "<msg-1@example.com>",
                "conversation_id": "thread-1",
                "subject": "Status update AN-10874 delayed",
                "from": "customer@example.com",
                "to": ["asr@cargolo.com"],
                "cc": [],
                "received_at": "2026-04-10T08:00:00Z",
                "body_text": "AN-10874 has delay at port. Please confirm ETA and attach invoice.",
                "attachments": [
                    {
                        "filename": "invoice.pdf",
                        "mime_type": "application/pdf",
                        "content_base64": "aGVsbG8=",
                    }
                ],
                "attachment_count": 1,
                "has_attachments": True,
            }
        ],
    }


def test_process_event_creates_case_files(tmp_path):
    result = process_email_event(sample_payload(), storage_root=tmp_path, create_task=True, refresh_history=False)
    case_root = tmp_path / "orders" / "AN-10874"
    assert result.status == "processed"
    assert result.initialized is True
    assert case_root.exists()
    assert (case_root / "case_state.json").exists()
    assert (case_root / "entities.json").exists()
    assert (case_root / "emails/raw").exists()
    assert (case_root / "emails/normalized").exists()
    assert (case_root / "emails/drafts").exists()
    assert (case_root / "documents/inbound/invoice.pdf").exists()
    state = json.loads((case_root / "case_state.json").read_text(encoding="utf-8"))
    assert state["order_id"] == "AN-10874"
    assert state["task_recommended"] is True


def test_process_event_is_idempotent_for_duplicate_messages(tmp_path):
    first = process_email_event(sample_payload(), storage_root=tmp_path, refresh_history=False)
    second = process_email_event(sample_payload(), storage_root=tmp_path, refresh_history=False)
    assert first.status == "processed"
    assert second.duplicate is True
    rows = (tmp_path / "orders" / "AN-10874" / "email_index.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(rows) == 1


def test_missing_an_goes_to_review_queue(tmp_path):
    payload = sample_payload()
    payload.pop("an")
    payload["messages"][0]["subject"] = "No clear reference"
    payload["messages"][0]["body_text"] = "Need help with shipment"
    result = process_email_event(payload, storage_root=tmp_path, refresh_history=False)
    assert result.review_required is True
    assert list((tmp_path / "review_queue").glob("*.json"))


def test_attachment_name_collisions_do_not_overwrite(tmp_path):
    payload = sample_payload()
    process_email_event(payload, storage_root=tmp_path, refresh_history=False)
    payload2 = sample_payload()
    payload2["trigger_message_id"] = "<msg-2@example.com>"
    payload2["messages"][0]["message_id"] = "<msg-2@example.com>"
    payload2["messages"][0]["body_text"] = "AN-10874 delayed again"
    payload2["messages"][0]["attachments"][0]["content_base64"] = "d29ybGQ="
    process_email_event(payload2, storage_root=tmp_path, refresh_history=False)
    files = sorted((tmp_path / "orders" / "AN-10874" / "documents/inbound").glob("invoice*.pdf"))
    assert len(files) == 2
