"""Tests for CargoloTMSClient and TMS integration in the processor."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from plugins.cargolo_ops.adapters import CargoloTMSClient, build_tms_client_from_env
from plugins.cargolo_ops.models import TMSSnapshot
from plugins.cargolo_ops.processor import process_email_event
from plugins.cargolo_ops.tms_provider import MCPBridgeTMSProvider, build_tms_provider_from_env


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

def _mock_login_response():
    """Simulates the TMS login response — Xano returns an object with authToken."""
    return {"authToken": "test-token-abc123", "user": {"id": 1, "email": "test@cargolo.com"}}


def _mock_shipments_list():
    """Xano returns { items, total, page, pageSize, totalPages }."""
    return {
        "items": [
            {
                "id": "uuid-10874",
                "booking_id": 100,
                "shipment_number": "AN-10874",
                "status": "in_transit",
                "transport_mode": "sea",
                "network": "sea",
                "origin_city": "Hamburg",
                "origin_country": "DE",
                "destination_city": "Shanghai",
                "destination_country": "CN",
                "company_id": 1,
                "company_name": "Test Kunde GmbH",
                "customer_reference": "REF-K001",
                "eta_main_carriage": "2026-04-20T06:00:00Z",
                "etd_main_carriage": "2026-04-05T14:00:00Z",
                "created_at": "2026-04-01T10:00:00Z",
            },
            {
                "id": "uuid-10875",
                "booking_id": 101,
                "shipment_number": "AN-10875",
                "status": "pending",
                "transport_mode": "air",
                "network": "air",
            },
        ],
        "total": 2,
        "page": 1,
        "pageSize": 100,
        "totalPages": 1,
    }


def _mock_shipment_detail():
    """Xano returns a flat object with nested sender/recipient/cargo."""
    return {
        "id": "uuid-10874",
        "shipment_number": "AN-10874",
        "shipment_id_numeric": 500,
        "status": "in_transit",
        "network": "sea",
        "company_name": "Test Kunde GmbH",
        "customer_reference": "REF-K001",
        "route_origin_city": "Hamburg",
        "route_origin_country": "DE",
        "route_destination_city": "Shanghai",
        "route_destination_country": "CN",
        "eta_main_carriage": "2026-04-20T06:00:00Z",
        "etd_main_carriage": "2026-04-05T14:00:00Z",
        "incoterms": "FOB",
        "pol_code": "DEHAM",
        "pod_code": "CNSHA",
        "container_number": "CONT-123",
        "sender": {"company_name": "Shipper GmbH", "city": "Hamburg"},
        "recipient": {"company_name": "Receiver Co", "city": "Shanghai"},
        "cargo": [{"description": "Electronics", "quantity": 12, "weight": 4500.0}],
        "transport_legs": [{"leg_type": "main_carriage", "carrier_name": "Hapag-Lloyd"}],
        "created_at": "2026-04-01T10:00:00Z",
        "updated_at": "2026-04-10T08:00:00Z",
    }


def _mock_billing_items():
    """Xano returns { items: [...], sums: {...} }."""
    return {
        "items": [
            {
                "uuid": "bi-1",
                "sort_order": 1,
                "name": "Ocean freight",
                "hint": "Main carriage",
                "quantity": 1,
                "unit": "shipment",
                "vk_price": 2500.0,
                "ek_price": 2000.0,
                "source": "quote",
            }
        ],
        "sums": {
            "total_vk": 2500.0,
            "total_ek": 2000.0,
            "margin": 500.0,
            "margin_percent": 20.0,
        },
    }


def _mock_stats():
    """Xano returns { total, by_status: {...}, by_transport_mode: {...}, today, ... }."""
    return {
        "total": 42,
        "by_status": {"in_transit": 15, "delivered": 17, "pending": 10},
        "by_transport_mode": {"air": 20, "sea": 15, "rail": 5, "road": 2},
        "today": 3,
        "this_week": 12,
        "this_month": 42,
        "revenue_this_month": 125000.0,
    }


def _mock_todos(status: str | None = None):
    items = {
        "pending": [
            {"id": "todo-1", "title": "Task A", "status": "pending", "priority": "medium"},
        ],
        "in_progress": [
            {"id": "todo-2", "title": "Task B", "status": "in_progress", "priority": "high"},
        ],
        None: [
            {"id": "todo-1", "title": "Task A", "status": "pending", "priority": "medium"},
            {"id": "todo-2", "title": "Task B", "status": "in_progress", "priority": "high"},
        ],
    }
    selected = items.get(status, items[None])
    return {
        "items": selected,
        "itemsTotal": len(selected),
        "curPage": 1,
    }


class MockResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


# ---------------------------------------------------------------------------
# Tests: CargoloTMSClient
# ---------------------------------------------------------------------------


class TestCargoloTMSClient:

    def _make_client(self):
        return CargoloTMSClient(
            api_url="https://api.cargolo.de",
            email="test@cargolo.com",
            password="secret",
            timeout=10,
        )

    def _route_request(self, method, url, **kwargs):
        """Route mocked requests to appropriate test data based on URL."""
        if "/api:auth/auth/login" in url:
            return MockResponse(_mock_login_response())
        if "/api:XCxYMj7t/shipments_list" in url:
            return MockResponse(_mock_shipments_list())
        if "/api:XCxYMj7t/shipment_detail" in url:
            return MockResponse(_mock_shipment_detail())
        if "/api:XCxYMj7t/shipments_stats" in url:
            return MockResponse(_mock_stats())
        if "/api:XCxYMj7t/shipment_billing_items" in url:
            return MockResponse(_mock_billing_items())
        if "/api:XCxYMj7t/todos/list" in url:
            status = kwargs.get("params", {}).get("status") if isinstance(kwargs.get("params"), dict) else None
            return MockResponse(_mock_todos(status))
        return MockResponse({}, 404)

    @patch("plugins.cargolo_ops.adapters.requests")
    def test_login_extracts_token_from_array(self, mock_requests):
        mock_requests.post.return_value = MockResponse(_mock_login_response())
        client = self._make_client()
        token = client._login()
        assert token == "test-token-abc123"
        assert client._token == "test-token-abc123"

    @patch("plugins.cargolo_ops.adapters.requests")
    def test_token_is_cached(self, mock_requests):
        mock_requests.post.return_value = MockResponse(_mock_login_response())
        mock_requests.get.return_value = MockResponse(_mock_shipments_list())
        client = self._make_client()
        client.shipments_list()
        client.shipments_list()
        # Login should be called only once (cached)
        assert mock_requests.post.call_count == 1

    @patch("plugins.cargolo_ops.adapters.requests")
    def test_find_shipment_by_an(self, mock_requests):
        mock_requests.post.return_value = MockResponse(_mock_login_response())
        mock_requests.get.return_value = MockResponse(_mock_shipments_list())
        client = self._make_client()
        result = client.find_shipment_by_an("AN-10874")
        assert result is not None
        assert result["id"] == "uuid-10874"
        assert result["shipment_number"] == "AN-10874"

    @patch("plugins.cargolo_ops.adapters.requests")
    def test_find_shipment_by_an_not_found(self, mock_requests):
        mock_requests.post.return_value = MockResponse(_mock_login_response())
        mock_requests.get.return_value = MockResponse(_mock_shipments_list())
        client = self._make_client()
        result = client.find_shipment_by_an("AN-99999")
        assert result is None

    @patch("plugins.cargolo_ops.adapters.requests")
    def test_snapshot_bundle_returns_live_data(self, mock_requests):
        def side_effect_post(url, **kwargs):
            return self._route_request("POST", url, **kwargs)

        def side_effect_get(url, **kwargs):
            return self._route_request("GET", url, **kwargs)

        mock_requests.post.side_effect = side_effect_post
        mock_requests.get.side_effect = side_effect_get

        client = self._make_client()
        snapshot = client.snapshot_bundle("AN-10874")
        assert isinstance(snapshot, TMSSnapshot)
        assert snapshot.source == "live"
        assert snapshot.status == "in_transit"
        assert snapshot.shipment_uuid == "uuid-10874"
        assert snapshot.detail.get("company_name") == "Test Kunde GmbH"
        assert snapshot.detail.get("network") == "sea"
        assert len(snapshot.billing_items) == 1
        assert snapshot.billing_sums.get("total_vk") == 2500.0
        assert snapshot.open_tasks == []
        assert snapshot.fetched_at

    @patch("plugins.cargolo_ops.adapters.requests")
    def test_snapshot_bundle_billing_failure_keeps_other_working_data(self, mock_requests):
        def side_effect_post(url, **kwargs):
            return self._route_request("POST", url, **kwargs)

        def side_effect_get(url, **kwargs):
            if "/api:XCxYMj7t/shipment_billing_items" in url:
                return MockResponse({"error": "boom"}, 500)
            return self._route_request("GET", url, **kwargs)

        mock_requests.post.side_effect = side_effect_post
        mock_requests.get.side_effect = side_effect_get

        client = self._make_client()
        snapshot = client.snapshot_bundle("AN-10874")

        assert snapshot.status == "in_transit"
        assert snapshot.detail.get("company_name") == "Test Kunde GmbH"
        assert snapshot.billing_items == []
        assert snapshot.billing_sums == {}
        assert snapshot.open_tasks == []
        assert any("billing_items failed" in w for w in snapshot.warnings)

    @patch("plugins.cargolo_ops.adapters.requests")
    def test_snapshot_bundle_not_found(self, mock_requests):
        mock_requests.post.return_value = MockResponse(_mock_login_response())
        mock_requests.get.return_value = MockResponse({"items": [], "total": 0, "page": 1, "pageSize": 100, "totalPages": 0})
        client = self._make_client()
        snapshot = client.snapshot_bundle("AN-99999")
        assert snapshot.source == "live"
        assert snapshot.status == "not_found"
        assert any("No shipment found" in w for w in snapshot.warnings)


    @patch("plugins.cargolo_ops.adapters.requests")
    def test_create_todo(self, mock_requests):
        mock_requests.post.side_effect = lambda url, **kw: (
            MockResponse(_mock_login_response()) if "/auth/login" in url
            else MockResponse({
                "success": True,
                "message": "Aufgabe erfolgreich erstellt",
                "todo": {
                    "id": "todo-uuid-1",
                    "title": "Check missing docs",
                    "priority": "high",
                    "category": "dokumente",
                    "status": "pending",
                    "related_type": "shipment",
                    "related_id": "uuid-10874",
                },
            })
        )
        client = self._make_client()
        result = client.create_todo(
            title="Check missing docs",
            related_id="uuid-10874",
            priority="high",
            category="dokumente",
        )
        assert result["success"] is True
        assert result["todo"]["id"] == "todo-uuid-1"

    @patch("plugins.cargolo_ops.adapters.requests")
    def test_list_todos(self, mock_requests):
        mock_requests.post.return_value = MockResponse(_mock_login_response())
        mock_requests.get.return_value = MockResponse({
            "items": [
                {"id": "todo-1", "title": "Task A", "status": "pending", "priority": "medium"},
                {"id": "todo-2", "title": "Task B", "status": "in_progress", "priority": "high"},
            ],
            "itemsTotal": 2,
            "curPage": 1,
        })
        client = self._make_client()
        result = client.list_todos(related_id="uuid-10874")
        assert len(result["items"]) == 2
        assert result["itemsTotal"] == 2


# ---------------------------------------------------------------------------
# Tests: build_tms_client_from_env
# ---------------------------------------------------------------------------


def test_build_tms_client_returns_none_without_credentials():
    with patch.dict("os.environ", {}, clear=True):
        assert build_tms_client_from_env() is None


def test_build_tms_client_returns_client_with_credentials():
    with patch.dict("os.environ", {
        "CARGOLO_TMS_EMAIL": "test@cargolo.com",
        "CARGOLO_TMS_PASSWORD": "secret",
    }):
        client = build_tms_client_from_env()
        assert client is not None
        assert client.email == "test@cargolo.com"


def test_build_tms_provider_prefers_mcp_bridge_when_configured():
    with patch.dict("os.environ", {
        "CARGOLO_TMS_MCP_BACKEND": "xano_mcp",
        "CARGOLO_TMS_MCP_URL": "https://example.invalid/mcp",
        "CARGOLO_TMS_MCP_PYTHON": "/custom/python",
        "CARGOLO_TMS_MCP_PACKAGE_ROOT": "/custom/package",
        "CARGOLO_TMS_MCP_CALL_TIMEOUT": "77",
    }, clear=True), \
         patch("plugins.cargolo_ops.tms_provider.Path.exists", return_value=True):
        provider = build_tms_provider_from_env()
        assert isinstance(provider, MCPBridgeTMSProvider)
        assert provider.python_bin == "/custom/python"
        assert provider.package_root == "/custom/package"
        assert provider.timeout == 77


def test_build_tms_provider_prefers_mcp_bridge_by_default_when_available():
    with patch.dict("os.environ", {
        "CARGOLO_TMS_EMAIL": "test@cargolo.com",
        "CARGOLO_TMS_PASSWORD": "secret",
    }, clear=True), \
         patch("plugins.cargolo_ops.tms_provider.Path.exists", return_value=True):
        provider = build_tms_provider_from_env()
        assert isinstance(provider, MCPBridgeTMSProvider)


def test_build_tms_provider_falls_back_to_direct_client_when_requested():
    with patch.dict("os.environ", {
        "CARGOLO_TMS_MCP_BACKEND": "direct",
        "CARGOLO_TMS_EMAIL": "test@cargolo.com",
        "CARGOLO_TMS_PASSWORD": "secret",
    }, clear=True):
        provider = build_tms_provider_from_env()
        assert provider is not None
        assert provider.__class__.__name__ == "DirectTMSProvider"


# ---------------------------------------------------------------------------
# Tests: Processor with TMS integration
# ---------------------------------------------------------------------------

def _sample_payload():
    return {
        "event_type": "asr_email_thread",
        "mailbox": "asr@cargolo.com",
        "an": "AN-10874",
        "trigger_message_id": "<tms-test-1@example.com>",
        "trigger_conversation_id": "thread-tms-1",
        "message_count": 1,
        "messages": [
            {
                "message_id": "<tms-test-1@example.com>",
                "conversation_id": "thread-tms-1",
                "subject": "Delay update AN-10874",
                "from": "customer@example.com",
                "to": ["asr@cargolo.com"],
                "cc": [],
                "received_at": "2026-04-10T10:00:00Z",
                "body_text": "AN-10874 is delayed at port. Please check ETA.",
                "attachments": [],
                "attachment_count": 0,
                "has_attachments": False,
            }
        ],
    }


def test_processor_falls_back_to_mock_without_tms_env(tmp_path):
    """Without TMS env vars, processor should still work with mock adapter."""
    with patch.dict("os.environ", {}, clear=True):
        result = process_email_event(
            _sample_payload(),
            storage_root=tmp_path,
            refresh_history=False,
        )
    assert result.status == "processed"
    assert result.analysis_status == "disabled"
    snapshot = json.loads((tmp_path / "orders" / "AN-10874" / "tms_snapshot.json").read_text(encoding="utf-8"))
    # Mock adapter doesn't set "source" to "live"
    assert snapshot.get("source", "mock") != "live" or "order_id" in snapshot


def test_processor_records_subagent_analysis_metadata(tmp_path):
    with patch(
        "plugins.cargolo_ops.processor.run_postprocess_subagent_analysis",
        return_value=("completed", "/tmp/analysis/brief.json", "high", "Escalate internal verification before replying."),
    ):
        result = process_email_event(
            _sample_payload(),
            storage_root=tmp_path,
            refresh_history=False,
            enable_subagent_analysis=True,
        )
    assert result.status == "processed"
    assert result.analysis_status == "completed"
    assert result.analysis_brief_path == "/tmp/analysis/brief.json"
    assert result.analysis_priority == "high"
    assert result.analysis_summary == "Escalate internal verification before replying."


def test_processor_enriches_state_from_live_tms(tmp_path):
    """With a mocked live TMS, processor should enrich case_state with TMS data."""
    billing_data = _mock_billing_items()
    mock_snapshot = TMSSnapshot(
        order_id="AN-10874",
        shipment_uuid="uuid-10874",
        shipment_number="AN-10874",
        source="live",
        status="in_transit",
        detail=_mock_shipment_detail(),
        billing_items=billing_data["items"],
        billing_sums=billing_data["sums"],
        fetched_at="2026-04-10T10:00:00Z",
        warnings=[],
    )

    with patch("plugins.cargolo_ops.processor.build_tms_provider_from_env") as mock_build:
        mock_client = MagicMock()
        mock_client.snapshot_bundle.return_value = mock_snapshot
        mock_build.return_value = mock_client

        result = process_email_event(
            _sample_payload(),
            storage_root=tmp_path,
            refresh_history=False,
        )

    assert result.status == "processed"
    state = json.loads((tmp_path / "orders" / "AN-10874" / "case_state.json").read_text(encoding="utf-8"))
    assert state["customer_name"] == "Test Kunde GmbH"
    assert state["customer_reference"] == "REF-K001"
    # network "sea" stays as mode "sea"
    assert state["mode"] == "sea"


def test_processor_builds_document_registry_against_tms_documents(tmp_path):
    billing_data = _mock_billing_items()
    detail = _mock_shipment_detail()
    detail["documents"] = [
        {"id": "doc-1", "document_type": "Commercial Invoice", "required": True},
        {"id": "doc-2", "document_type": "Packing List", "required": True},
    ]
    mock_snapshot = TMSSnapshot(
        order_id="AN-10874",
        shipment_uuid="uuid-10874",
        shipment_number="AN-10874",
        source="live",
        status="in_transit",
        detail=detail,
        billing_items=billing_data["items"],
        billing_sums=billing_data["sums"],
        fetched_at="2026-04-10T10:00:00Z",
        warnings=[],
    )
    payload = _sample_payload()
    payload["messages"][0]["attachments"] = [{
        "filename": "invoice.pdf",
        "mime_type": "application/pdf",
        "content_base64": "aGVsbG8=",
    }]
    payload["messages"][0]["attachment_count"] = 1
    payload["messages"][0]["has_attachments"] = True

    with patch("plugins.cargolo_ops.processor.build_tms_provider_from_env") as mock_build:
        mock_client = MagicMock()
        mock_client.snapshot_bundle.return_value = mock_snapshot
        mock_client.document_requirements.return_value = {
            "status": "ok",
            "query": {"an": "AN-10874"},
            "shipment": {"shipment_uuid": "uuid-10874", "shipment_number": "AN-10874"},
            "documents": [
                {"tms_document_id": "doc-1", "label": "Commercial Invoice", "document_type": "commercial_invoice", "required": True},
                {"tms_document_id": "doc-2", "label": "Packing List", "document_type": "packing_list", "required": True},
            ],
            "expected_types": ["commercial_invoice", "packing_list", "customs_document"],
            "warnings": [],
        }
        mock_client.billing_context.return_value = {
            "status": "ok",
            "query": {"an": "AN-10874"},
            "shipment": {"shipment_uuid": "uuid-10874", "shipment_number": "AN-10874"},
            "billing": billing_data,
            "warnings": [],
        }
        mock_build.return_value = mock_client

        result = process_email_event(
            payload,
            storage_root=tmp_path,
            refresh_history=False,
        )

    assert result.status == "processed"
    assert result.document_registry_path
    registry = json.loads(Path(result.document_registry_path).read_text(encoding="utf-8"))
    assert registry["received_types"] == ["commercial_invoice"]
    assert registry["expected_types"] == ["commercial_invoice", "customs_document", "packing_list"]
    assert registry["missing_types"] == ["customs_document", "packing_list"]
    assert registry["tms_expected_types"] == ["commercial_invoice", "customs_document", "packing_list"]

    state = json.loads((tmp_path / "orders" / "AN-10874" / "case_state.json").read_text(encoding="utf-8"))
    assert state["documents_received"] == ["commercial_invoice"]
    assert state["documents_expected"] == ["commercial_invoice", "customs_document", "packing_list"]
    assert "document:packing_list" in state["missing_information"]
    assert "document:customs_document" in state["missing_information"]

    normalized_files = list((tmp_path / "orders" / "AN-10874" / "emails" / "normalized").glob("*.json"))
    normalized = json.loads(normalized_files[0].read_text(encoding="utf-8"))
    assert normalized["document_registry"]["missing_types"] == ["customs_document", "packing_list"]

    # TMS detail files should be stored
    tms_dir = tmp_path / "orders" / "AN-10874" / "tms"
    assert (tms_dir / "shipment_detail.json").exists()
    assert not (tms_dir / "shipment_diagnose.json").exists()
    assert (tms_dir / "shipment_billing_items.json").exists()
    assert (tms_dir / "document_requirements.json").exists()
    assert (tms_dir / "billing_context.json").exists()
    assert not (tms_dir / "damage_claims.json").exists()
    assert (tms_dir / "sync_log.jsonl").exists()


def test_erstinitialisierung_flag_robust_with_history_only(tmp_path):
    """Case created by history sync alone should still be 'initialized' on first real ingest."""
    from plugins.cargolo_ops.storage import CaseStore

    store = CaseStore(tmp_path)
    order_id = "AN-10900"
    store.ensure_case(order_id)
    # Simulate a history-sync entry (not a real ingest)
    store.append_email_index(order_id, {
        "message_id": "<hist-1@example.com>",
        "classification": "history_sync",
        "dedupe_hash": "abc123",
    })

    payload = {
        "event_type": "asr_email_thread",
        "mailbox": "asr@cargolo.com",
        "an": order_id,
        "trigger_message_id": "<real-1@example.com>",
        "trigger_conversation_id": "thread-real-1",
        "message_count": 1,
        "messages": [
            {
                "message_id": "<real-1@example.com>",
                "conversation_id": "thread-real-1",
                "subject": f"Booking {order_id}",
                "from": "customer@example.com",
                "to": ["asr@cargolo.com"],
                "cc": [],
                "received_at": "2026-04-10T12:00:00Z",
                "body_text": f"Please book {order_id}",
                "attachments": [],
                "attachment_count": 0,
                "has_attachments": False,
            }
        ],
    }

    result = process_email_event(payload, storage_root=tmp_path, refresh_history=False)
    assert result.status == "processed"
    assert result.initialized is True  # should be True even though directory existed
