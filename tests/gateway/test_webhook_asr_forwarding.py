import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

from gateway.config import PlatformConfig
from gateway.platforms.webhook import WebhookAdapter


class _FakeResponse:
    def __init__(self, status=200, text='ok'):
        self.status = status
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._text


class _FakeSession:
    def __init__(self):
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append({
            'url': url,
            'json': json,
            'headers': headers,
            'timeout': timeout,
        })
        return _FakeResponse(status=200)


def test_webhook_forward_uses_asr_html_renderer_for_cargolo_ingest(tmp_path):
    cfg = PlatformConfig(enabled=True, extra={})
    adapter = WebhookAdapter(cfg)

    case_report_path = tmp_path / 'case_report_latest.json'
    case_report_path.write_text(
        json.dumps(
            {
                'source_artifacts': {'case_report': 'analysis/case_report_latest.json'},
                'sections': {'mail_history': {'email_count_total': {'value': 3, 'source': 'email_index.jsonl'}}},
            },
            ensure_ascii=False,
        ),
        encoding='utf-8',
    )

    delivery = {
        'route_name': 'cargolo-asr-ingest',
        'delivery_id': 'delivery-123',
        'deliver_extra': {
            'url': 'https://example.test/asr-teams',
            'method': 'POST',
            'headers': {'Content-Type': 'application/json'},
        },
        'payload': {
            'event_type': 'asr_email_thread',
            'processor_result': {
                'order_id': 'AN-12140',
                'status': 'processed',
                'history_sync_count': 1,
                'pending_action_summary': {'write_now': 1},
                'case_report_path': str(case_report_path),
            },
        },
    }

    fake_session = _FakeSession()

    with patch('gateway.platforms.webhook.ClientSession', return_value=fake_session):
        result = asyncio.run(adapter._deliver_webhook_forward('plain fallback content', delivery))

    assert result.success is True
    assert len(fake_session.calls) == 1
    body = fake_session.calls[0]['json']
    assert body['route'] == 'cargolo-asr-ingest'
    assert body['delivery_id'] == 'delivery-123'
    assert body['message_format'] == 'html'
    assert '<html><body' in body['message']
    assert body['payload']['run_type'] == 'process_event'
    assert body['payload']['processor_result']['order_id'] == 'AN-12140'
