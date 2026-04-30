from plugins.cargolo_ops.models import AttachmentPayload, IncomingEmailEvent, normalize_order_ids


SAMPLE_PAYLOAD = {
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
            "subject": "Status AN-10874 delayed",
            "from": "customer@example.com",
            "to": ["asr@cargolo.com"],
            "cc": [],
            "received_at": "2026-04-10T08:00:00Z",
            "body_text": "Please check AN-10874. Delay at port.",
            "attachments": [],
            "attachment_count": 0,
            "has_attachments": False,
        }
    ],
}


def test_normalize_order_ids_deduplicates_and_uppercases():
    assert normalize_order_ids("an-12345 and AN-12345", None) == ["AN-12345"]


def test_event_compatibility_with_current_webhook_shape():
    event = IncomingEmailEvent.from_payload(SAMPLE_PAYLOAD)
    assert event.order_id == "AN-10874"
    assert event.message_id == "<msg-1@example.com>"
    assert event.thread_id == "thread-1"
    assert event.primary_message.subject == "Status AN-10874 delayed"


def test_primary_message_follows_trigger_message_id():
    payload = {
        **SAMPLE_PAYLOAD,
        "trigger_message_id": "<msg-2@example.com>",
        "messages": [
            {**SAMPLE_PAYLOAD["messages"][0]},
            {
                **SAMPLE_PAYLOAD["messages"][0],
                "message_id": "<msg-2@example.com>",
                "subject": "Newest trigger message",
                "body_text": "Please review AN-10874 urgently",
            },
        ],
    }
    event = IncomingEmailEvent.from_payload(payload)
    assert event.primary_message.message_id == "<msg-2@example.com>"
    assert event.primary_message.subject == "Newest trigger message"


def test_order_id_prefers_trigger_message_reference_over_top_level_an():
    payload = {
        **SAMPLE_PAYLOAD,
        "an": "AN-99999",
        "trigger_message_id": "<msg-2@example.com>",
        "messages": [
            {
                **SAMPLE_PAYLOAD["messages"][0],
                "message_id": "<msg-1@example.com>",
                "subject": "Old thread note for AN-99999",
                "body_text": "AN-99999 historical context only.",
            },
            {
                **SAMPLE_PAYLOAD["messages"][0],
                "message_id": "<msg-2@example.com>",
                "subject": "Newest trigger message for BU-4638",
                "body_text": "Please review BU-4638 urgently.",
            },
        ],
    }
    event = IncomingEmailEvent.from_payload(payload)
    assert event.primary_message.message_id == "<msg-2@example.com>"
    assert event.order_id == "BU-4638"
    assert event.extracted_order_ids[0] == "BU-4638"


def test_attachment_size_parses_human_readable_strings():
    attachment = AttachmentPayload(filename="x.pdf", size="87.4 kB")
    assert attachment.size == int(87.4 * 1024)
