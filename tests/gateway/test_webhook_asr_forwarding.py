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

    with patch('gateway.platforms.webhook.ClientSession', return_value=fake_session, create=True):
        result = asyncio.run(adapter._deliver_webhook_forward('plain fallback content', delivery))

    assert result.success is True
    assert len(fake_session.calls) == 1
    body = fake_session.calls[0]['json']
    assert body['route'] == 'cargolo-asr-ingest'
    assert body['delivery_id'] == 'delivery-123'
    assert body['message_format'] == 'html'
    assert '<html><body' in body['message']
    assert 'color:#ffffff' in body['message']
    assert 'background:#020617' in body['message']
    assert body['payload']['run_type'] == 'process_event'
    assert body['payload']['processor_result']['order_id'] == 'AN-12140'


def test_webhook_forward_uses_asr_html_renderer_without_processor_result_payload(tmp_path, monkeypatch):
    cfg = PlatformConfig(enabled=True, extra={})
    adapter = WebhookAdapter(cfg)

    hermes_home = tmp_path / '.hermes'
    monkeypatch.setenv('HERMES_HOME', str(hermes_home))
    case_root = hermes_home / 'cargolo_asr' / 'orders' / 'AN-12438' / 'analysis'
    case_root.mkdir(parents=True, exist_ok=True)
    (case_root / 'case_report_latest.json').write_text(json.dumps({'sections': {}}, ensure_ascii=False), encoding='utf-8')
    (case_root / 'latest_brief.json').write_text(json.dumps({'priority': 'medium', 'ops_summary': 'HTML bitte nutzen'}, ensure_ascii=False), encoding='utf-8')

    delivery = {
        'route_name': 'cargolo-asr-ingest',
        'delivery_id': 'delivery-456',
        'deliver_extra': {
            'url': 'https://example.test/asr-teams',
            'method': 'POST',
            'headers': {'Content-Type': 'application/json'},
        },
        'payload': {
            'event_type': 'asr_email_thread',
            'an': 'AN-12438',
        },
    }

    fake_session = _FakeSession()
    with patch('gateway.platforms.webhook.ClientSession', return_value=fake_session, create=True):
        result = asyncio.run(adapter._deliver_webhook_forward('plain fallback content', delivery))

    assert result.success is True
    assert len(fake_session.calls) == 1
    body = fake_session.calls[0]['json']
    assert body['message_format'] == 'html'
    assert '<html><body' in body['message']
    assert body['payload']['processor_result']['case_report_path'].endswith('case_report_latest.json')


def test_build_suppressed_notification_body_for_not_in_tms_skip():
    cfg = PlatformConfig(enabled=True, extra={})
    adapter = WebhookAdapter(cfg)

    body = adapter._build_suppressed_notification_body(
        route_name='cargolo-asr-ingest',
        delivery_id='delivery-skip-1',
        payload={
            'an': 'AN-12520',
            'processor_result': {
                'status': 'skipped',
                'order_id': 'AN-12520',
                'message': 'Order id AN-12520 not found in ASR shipment list. Skipped automatic processing; payload saved at /tmp/x',
                'suppress_delivery': True,
            },
        },
    )

    assert body is not None
    assert body['message_format'] == 'html'
    assert 'AN-12520' in body['message']
    assert 'nicht im ASR-TMS gefunden' in body['message']
    assert body['message_text'] == 'AN-12520 | übersprungen | nicht im ASR-TMS gefunden'
    assert body['payload']['run_type'] == 'skipped_not_in_tms'


def test_build_suppressed_notification_body_for_mail_history_failure_skip():
    cfg = PlatformConfig(enabled=True, extra={})
    adapter = WebhookAdapter(cfg)

    body = adapter._build_suppressed_notification_body(
        route_name='cargolo-asr-ingest',
        delivery_id='delivery-skip-history',
        payload={
            'bu': 'BU-4664',
            'processor_result': {
                'status': 'skipped',
                'order_id': 'BU-4664',
                'history_sync_status': 'failed',
                'history_sync_error': 'mail_history_sync_failed: n8n mail history timed out',
                'message': 'Mailhistory for BU-4664 could not be fetched during initial sync. Automatic processing skipped; current mail saved for later retry.',
                'suppress_delivery': True,
            },
        },
    )

    assert body is not None
    assert body['message_format'] == 'html'
    assert 'BU-4664' in body['message']
    assert 'Mailhistory konnte beim initialen Sync nicht gezogen werden' in body['message']
    assert body['message_text'] == 'BU-4664 | übersprungen | Mailhistory konnte nicht gezogen werden'
    assert body['payload']['run_type'] == 'skipped_mail_history_failed'
