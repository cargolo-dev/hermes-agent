import base64
import io
import json
import zipfile
from types import SimpleNamespace
from unittest.mock import patch

from plugins.cargolo_ops.adapters import N8NMailHistoryClient


class _FakeResponse:
    def __init__(self, *, content: bytes, headers: dict[str, str], status_code: int = 200):
        self.content = content
        self.headers = headers
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return json.loads(self.content.decode("utf-8"))


def _build_zip_payload() -> bytes:
    manifest = {
        "an": "AN-11256",
        "mailbox": "asr@cargolo.com",
        "first_sync": True,
        "since": None,
        "count": 1,
        "messages": [
            {
                "message_id": "<hist-zip@example.com>",
                "conversation_id": "thread-zip",
                "subject": "AN-11256 // Test ZIP",
                "from": "ops@example.com",
                "to": ["asr@cargolo.com"],
                "cc": [],
                "received_at": "2026-04-19T09:30:00Z",
                "body_text": "See attached",
                "has_attachments": True,
                "attachment_count": 1,
                "attachments": [
                    {
                        "filename": "docs/invoice.pdf",
                        "mime_type": "application/pdf",
                        "size": 5,
                        "content_path": "docs/invoice.pdf"
                    }
                ]
            }
        ]
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest).encode("utf-8"))
        zf.writestr("docs/invoice.pdf", b"hello")
    return buf.getvalue()


def test_mail_history_client_parses_zip_manifest_and_embeds_attachment_content():
    client = N8NMailHistoryClient(url="https://example.test/history")
    response = _FakeResponse(
        content=_build_zip_payload(),
        headers={"Content-Type": "application/zip"},
    )

    with patch("plugins.cargolo_ops.adapters.requests.post", return_value=response):
        payload = client.fetch_history(
            "AN-11256",
            first_sync=True,
            since=None,
            mailbox="asr@cargolo.com",
            include_attachments=True,
            include_html=False,
        )

    assert payload["an"] == "AN-11256"
    assert payload["count"] == 1
    attachment = payload["messages"][0]["attachments"][0]
    assert attachment["filename"] == "docs/invoice.pdf"
    assert attachment["mime_type"] == "application/pdf"
    assert base64.b64decode(attachment["content_base64"].encode("utf-8")) == b"hello"


def test_mail_history_client_matches_zip_attachment_via_manifest_files_metadata():
    client = N8NMailHistoryClient(url="https://example.test/history")
    manifest = {
        "an": "AN-11256",
        "count": 1,
        "files": [
            {
                "binary_key": "file_4",
                "zip_name": "004_invoice.pdf",
                "content_path": "attachments/004_invoice.pdf",
                "original_name": "Invoice Final.pdf",
                "message_id": "<hist-zip-key@example.com>",
            }
        ],
        "messages": [
            {
                "message_id": "<hist-zip-key@example.com>",
                "subject": "AN-11256 // Test ZIP binary key",
                "from": "ops@example.com",
                "received_at": "2026-04-19T09:30:00Z",
                "attachments": [
                    {
                        "binary_key": "attachment_19",
                        "filename": "Invoice Final.pdf",
                        "mime_type": "application/pdf",
                    }
                ]
            }
        ]
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest).encode("utf-8"))
        zf.writestr("attachments/004_invoice.pdf", b"invoice-by-file-metadata")
    response = _FakeResponse(content=buf.getvalue(), headers={"Content-Type": "application/zip"})

    with patch("plugins.cargolo_ops.adapters.requests.post", return_value=response):
        payload = client.fetch_history(
            "AN-11256",
            first_sync=True,
            since=None,
            mailbox="asr@cargolo.com",
            include_attachments=True,
            include_html=False,
        )

    attachment = payload["messages"][0]["attachments"][0]
    assert base64.b64decode(attachment["content_base64"].encode("utf-8")) == b"invoice-by-file-metadata"


def test_mail_history_client_accepts_history_array_and_builds_attachments_from_files_index():
    client = N8NMailHistoryClient(url="https://example.test/history")
    manifest = {
        "an": "BU-4638",
        "count": 1,
        "history": [
            {
                "message_id": "<hist-history@example.com>",
                "conversation_id": "thread-history",
                "subject": "BU-4638 // Test history[] payload",
                "from": "ops@example.com",
                "to": ["asr@cargolo.com"],
                "cc": [],
                "received_at": "2026-04-19T09:30:00Z",
                "body_text": "Body stays in history[]",
                "has_attachments": True,
                "attachment_count": 1,
            }
        ],
        "files": [
            {
                "binary_key": "file_0",
                "zip_name": "000_invoice.pdf",
                "content_path": "attachments/000_invoice.pdf",
                "original_name": "Invoice Final.pdf",
                "mime_type": "application/pdf",
                "size": "12 kB",
                "message_id": "<hist-history@example.com>",
            }
        ],
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest).encode("utf-8"))
        zf.writestr("attachments/000_invoice.pdf", b"history-file-only")
    response = _FakeResponse(content=buf.getvalue(), headers={"Content-Type": "application/zip"})

    with patch("plugins.cargolo_ops.adapters.requests.post", return_value=response):
        payload = client.fetch_history(
            "BU-4638",
            first_sync=True,
            since=None,
            mailbox="asr@cargolo.com",
            include_attachments=True,
            include_html=False,
        )

    assert payload["messages"][0]["body_text"] == "Body stays in history[]"
    attachment = payload["messages"][0]["attachments"][0]
    assert attachment["filename"] == "Invoice Final.pdf"
    assert attachment["mime_type"] == "application/pdf"
    assert attachment["size"] == "12 kB"
    assert base64.b64decode(attachment["content_base64"].encode("utf-8")) == b"history-file-only"
