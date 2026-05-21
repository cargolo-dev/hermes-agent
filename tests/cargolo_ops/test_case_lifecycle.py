from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from plugins.cargolo_ops.case_lifecycle import (
    mirror_tms_documents,
    sync_case_lifecycle,
    _enrich_tms_snapshot_with_cached_detail,
)
from plugins.cargolo_ops.models import TMSSnapshot
from plugins.cargolo_ops.storage import CaseStore


def test_case_store_creates_canonical_lifecycle_directories(tmp_path):
    case_root = CaseStore(tmp_path).ensure_case("AN-12345")

    for rel in [
        "emails/raw",
        "emails/normalized",
        "documents/inbound",
        "documents/tms",
        "documents/analysis",
        "document_monitoring",
        "tms",
        "analysis/briefs",
    ]:
        assert (case_root / rel).is_dir(), rel
    assert (case_root / "documents" / "registry.json").exists()


def test_enrich_tms_snapshot_uses_cached_cargo_after_partial_read(tmp_path):
    case_root = CaseStore(tmp_path).ensure_case("AN-12258")
    cached_detail = {
        "shipment_number": "AN-12258",
        "status": "in_transit",
        "cargo": [{"quantity": 2, "weight_kg": 910, "volume_m3": 4.4}],
    }
    (case_root / "tms" / "shipment_detail.json").write_text(json.dumps(cached_detail), encoding="utf-8")

    enriched = _enrich_tms_snapshot_with_cached_detail(
        case_root=case_root,
        tms_snapshot={"status": "error", "detail": {"documents": []}, "warnings": ["snapshot_failed"]},
    )

    assert enriched["detail"]["cargo"][0]["weight_kg"] == 910
    assert enriched["status"] == "in_transit"
    assert "used_cached_tms_shipment_detail_after_partial_snapshot" in enriched["warnings"]


def test_enrich_tms_snapshot_replaces_empty_current_cargo_with_cached_cargo(tmp_path):
    case_root = CaseStore(tmp_path).ensure_case("AN-12258")
    cached_detail = {
        "shipment_number": "AN-12258",
        "status": "in_transit",
        "cargo": [{"quantity": 2, "weight_kg": 910, "volume_m3": 4.4}],
    }
    (case_root / "tms" / "shipment_detail.json").write_text(json.dumps(cached_detail), encoding="utf-8")

    enriched = _enrich_tms_snapshot_with_cached_detail(
        case_root=case_root,
        tms_snapshot={"status": "error", "detail": {"cargo": [], "documents": []}},
    )

    assert enriched["detail"]["cargo"] == cached_detail["cargo"]


def test_enrich_tms_snapshot_does_not_treat_empty_cargo_row_as_operational(tmp_path):
    case_root = CaseStore(tmp_path).ensure_case("AN-12258")
    cached_detail = {"shipment_number": "AN-12258", "cargo": [{"quantity": 2, "weight_kg": 910}]}
    (case_root / "tms" / "shipment_detail.json").write_text(json.dumps(cached_detail), encoding="utf-8")

    enriched = _enrich_tms_snapshot_with_cached_detail(
        case_root=case_root,
        tms_snapshot={"status": "error", "detail": {"cargo": [{}]}},
    )

    assert enriched["detail"]["cargo"] == cached_detail["cargo"]


def test_sync_case_lifecycle_skips_unknown_tms_case_without_folder_or_mail_history(tmp_path):
    with patch("plugins.cargolo_ops.processor._live_shipment_exists", return_value=False), \
         patch("plugins.cargolo_ops.processor._fetch_tms_bundle") as fetch_bundle, \
         patch("plugins.cargolo_ops.processor._sync_mail_history") as sync_history:
        result = sync_case_lifecycle("AN-404404", storage_root=tmp_path, analyze_documents=True)

    assert result["status"] == "skipped"
    assert result["reason"] == "shipment_not_found_in_tms"
    assert not (tmp_path / "orders" / "AN-404404").exists()
    fetch_bundle.assert_not_called()
    sync_history.assert_not_called()


def test_sync_case_lifecycle_fetches_tms_before_creating_local_case(tmp_path):
    snapshot = TMSSnapshot(
        order_id="AN-22222",
        shipment_number="AN-22222",
        status="confirmed",
        source="live",
        detail={"network": "air"},
    )
    events = []

    def fake_fetch(store, order_id, customer_hint, *, persist_case_files=True):
        events.append(("fetch_tms", (tmp_path / "orders" / order_id).exists(), persist_case_files))
        return snapshot, {}, {}

    def fake_sync(store, order_id, state, mailbox, *, exclude_message_ids=None):
        events.append(("sync_mail", (tmp_path / "orders" / order_id).exists(), True))
        return 0

    with patch("plugins.cargolo_ops.processor._live_shipment_exists", return_value=True), \
         patch("plugins.cargolo_ops.processor._fetch_tms_bundle", side_effect=fake_fetch), \
         patch("plugins.cargolo_ops.processor._sync_mail_history", side_effect=fake_sync), \
         patch("plugins.cargolo_ops.case_lifecycle.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        result = sync_case_lifecycle("AN-22222", storage_root=tmp_path, analyze_documents=True)

    assert result["status"] == "ok"
    assert events[0] == ("fetch_tms", False, False)
    assert events[1][0] == "sync_mail"
    assert events[1][1] is True
    assert (tmp_path / "orders" / "AN-22222" / "tms" / "shipment_detail.json").exists()


def test_sync_case_lifecycle_new_tms_case_uses_full_first_mail_sync(tmp_path):
    snapshot = TMSSnapshot(
        order_id="AN-33333",
        shipment_number="AN-33333",
        status="confirmed",
        source="live",
        detail={"network": "sea"},
    )
    captured = {}

    def fake_sync(store, order_id, state, mailbox, *, exclude_message_ids=None):
        captured["prior_last_email_at"] = state.last_email_at
        store.append_email_index(order_id, {"message_id": "m-1", "received_at": "2026-05-19T08:00:00Z"})
        return 1

    with patch("plugins.cargolo_ops.processor._live_shipment_exists", return_value=True), \
         patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(snapshot, {}, {})), \
         patch("plugins.cargolo_ops.processor.build_mail_history_client_from_env", return_value=object()), \
         patch("plugins.cargolo_ops.processor._sync_mail_history", side_effect=fake_sync), \
         patch("plugins.cargolo_ops.case_lifecycle.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        result = sync_case_lifecycle("AN-33333", storage_root=tmp_path, analyze_documents=True)

    assert captured["prior_last_email_at"] is None
    assert result["initialized"] is True
    assert result["history_sync_mode"] == "full_first"
    assert result["history_sync_status"] == "ok"
    assert result["history_sync_count"] == 1
    assert result["last_email_at"] == "2026-05-19T08:00:00Z"


def test_sync_case_lifecycle_existing_case_uses_delta_mail_sync(tmp_path):
    snapshot = TMSSnapshot(
        order_id="AN-33334",
        shipment_number="AN-33334",
        status="confirmed",
        source="live",
        detail={"network": "air"},
    )
    store = CaseStore(tmp_path)
    state = store.load_case_state("AN-33334")
    state.last_email_at = "2026-05-18T08:00:00Z"
    store.save_case_state("AN-33334", state)
    captured = {}

    def fake_sync(store, order_id, state, mailbox, *, exclude_message_ids=None):
        captured["prior_last_email_at"] = state.last_email_at
        return 0

    with patch("plugins.cargolo_ops.processor._live_shipment_exists", return_value=True), \
         patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(snapshot, {}, {})), \
         patch("plugins.cargolo_ops.processor.build_mail_history_client_from_env", return_value=object()), \
         patch("plugins.cargolo_ops.processor._sync_mail_history", side_effect=fake_sync), \
         patch("plugins.cargolo_ops.case_lifecycle.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        result = sync_case_lifecycle("AN-33334", storage_root=tmp_path, analyze_documents=True)

    assert captured["prior_last_email_at"] == "2026-05-18T08:00:00Z"
    assert result["initialized"] is False
    assert result["history_sync_mode"] == "delta"
    assert result["history_sync_status"] == "no_messages"
    assert result["last_email_at"] == "2026-05-18T08:00:00Z"


def test_sync_case_lifecycle_skips_recent_successful_mail_history_sync(tmp_path):
    snapshot = TMSSnapshot(
        order_id="AN-33337",
        shipment_number="AN-33337",
        status="confirmed",
        source="live",
        detail={"network": "rail"},
    )
    store = CaseStore(tmp_path)
    state = store.load_case_state("AN-33337")
    state.last_email_at = "2026-05-18T08:00:00Z"
    store.save_case_state("AN-33337", state)
    store.append_audit(
        "AN-33337",
        action="sync_case_lifecycle",
        result="ok",
        files=[],
        extra={"history_sync_status": "no_messages", "history_sync_mode": "delta"},
    )

    with patch("plugins.cargolo_ops.processor._live_shipment_exists", return_value=True), \
         patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(snapshot, {}, {})), \
         patch("plugins.cargolo_ops.processor._sync_mail_history") as sync_mock, \
         patch("plugins.cargolo_ops.case_lifecycle.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        result = sync_case_lifecycle("AN-33337", storage_root=tmp_path, analyze_documents=True)

    sync_mock.assert_not_called()
    assert result["history_sync_mode"] == "freshness_skip"
    assert result["history_sync_status"] == "fresh_skipped"
    assert result["history_sync_count"] == 0


def test_sync_case_lifecycle_marks_initial_mail_history_no_client_as_unreliable(tmp_path):
    snapshot = TMSSnapshot(
        order_id="AN-33335",
        shipment_number="AN-33335",
        status="confirmed",
        source="live",
        detail={"network": "rail"},
    )

    with patch("plugins.cargolo_ops.processor._live_shipment_exists", return_value=True), \
         patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(snapshot, {}, {})), \
         patch("plugins.cargolo_ops.processor.build_mail_history_client_from_env", return_value=None), \
         patch("plugins.cargolo_ops.processor._sync_mail_history", return_value=0), \
         patch("plugins.cargolo_ops.case_lifecycle.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        result = sync_case_lifecycle("AN-33335", storage_root=tmp_path, analyze_documents=True)

    assert result["initialized"] is True
    assert result["history_sync_mode"] == "full_first"
    assert result["history_sync_status"] == "no_client"
    assert result["history_sync_error"] == "mail_history_sync_unavailable:no_client"
    state = json.loads((Path(result["case_root"]) / "case_state.json").read_text(encoding="utf-8"))
    assert "mail_history_sync_unavailable:no_client" in state["open_questions"]
    assert any("Initial-Mail-Historie nicht belastbar" in item for item in state["open_questions"])


def test_sync_case_lifecycle_marks_initial_mail_history_failure_as_unreliable(tmp_path):
    snapshot = TMSSnapshot(
        order_id="AN-33336",
        shipment_number="AN-33336",
        status="confirmed",
        source="live",
        detail={"network": "sea"},
    )

    with patch("plugins.cargolo_ops.processor._live_shipment_exists", return_value=True), \
         patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(snapshot, {}, {})), \
         patch("plugins.cargolo_ops.processor.build_mail_history_client_from_env", return_value=object()), \
         patch("plugins.cargolo_ops.processor._sync_mail_history", side_effect=RuntimeError("n8n timeout")), \
         patch("plugins.cargolo_ops.case_lifecycle.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        result = sync_case_lifecycle("AN-33336", storage_root=tmp_path, analyze_documents=True)

    assert result["initialized"] is True
    assert result["history_sync_mode"] == "full_first"
    assert result["history_sync_status"] == "failed"
    assert "n8n timeout" in result["history_sync_error"]
    state = json.loads((Path(result["case_root"]) / "case_state.json").read_text(encoding="utf-8"))
    assert any("Initial-Mail-Historie nicht belastbar" in item for item in state["open_questions"])


def test_sync_case_lifecycle_mirrors_tms_documents_and_updates_registry(tmp_path):
    source_doc = tmp_path / "source_invoice.txt"
    source_doc.write_text("Commercial invoice AN-12345", encoding="utf-8")
    snapshot = TMSSnapshot(
        order_id="AN-12345",
        shipment_number="AN-12345",
        status="confirmed",
        source="live",
        detail={"network": "air", "documents": []},
    )
    requirements = {
        "expected_types": ["commercial_invoice"],
        "documents": [
            {
                "id": "doc-1",
                "document_type": "Commercial Invoice",
                "required": True,
                "filename": "invoice.txt",
                "url": f"file://{source_doc}",
            }
        ],
    }

    store = CaseStore(tmp_path)
    store.append_email_index("AN-12345", {"message_id": "m-old", "received_at": "2026-05-08T08:00:00Z"})

    with patch("plugins.cargolo_ops.processor._live_shipment_exists", return_value=True), \
         patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(snapshot, requirements, {})), \
         patch("plugins.cargolo_ops.processor._sync_mail_history", return_value=0), \
         patch("plugins.cargolo_ops.case_lifecycle.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])):
        result = sync_case_lifecycle("AN-12345", storage_root=tmp_path, analyze_documents=True)

    case_root = Path(result["case_root"])
    mirrored = case_root / "documents" / "tms" / "invoice.txt"
    assert mirrored.exists()
    registry = json.loads((case_root / "documents" / "registry.json").read_text(encoding="utf-8"))
    assert registry["tms_documents"][0]["mirror_status"] == "mirrored"
    assert registry["tms_documents"][0]["local_path"] == str(mirrored)
    state = json.loads((case_root / "case_state.json").read_text(encoding="utf-8"))
    assert state["mode"] == "air"
    assert state["last_email_at"] == "2026-05-08T08:00:00Z"
    assert result["last_email_at"] == "2026-05-08T08:00:00Z"


def test_mirror_tms_documents_uses_tms_bearer_token_for_vault_downloads(tmp_path):
    class FakeTMSClient:
        api_url = "https://api.cargolo.de"

        def __init__(self):
            self.header_calls = 0

        def _headers(self):
            self.header_calls += 1
            return {"Authorization": "Bearer test-token", "Content-Type": "application/json"}

    class FakeResponse:
        status_code = 200
        url = "https://api.cargolo.de/vault/example/invoice.pdf"
        content = b"vault document"

        def raise_for_status(self):
            return None

    captured = {}

    def fake_get(url, headers=None, timeout=0, allow_redirects=True):
        captured["url"] = url
        captured["authorization"] = (headers or {}).get("Authorization")
        captured["content_type"] = (headers or {}).get("Content-Type")
        captured["allow_redirects"] = allow_redirects
        return FakeResponse()

    registry = {
        "tms_documents": [
            {
                "document_type": "commercial_invoice",
                "filename": "invoice.pdf",
                "url": "/vault/example/invoice.pdf",
            }
        ]
    }
    client = FakeTMSClient()

    with patch("plugins.cargolo_ops.case_lifecycle.requests.get", side_effect=fake_get):
        mirrored = mirror_tms_documents(case_root=tmp_path, registry=registry, tms_client=client)

    assert client.header_calls == 1
    assert captured["url"] == "https://api.cargolo.de/vault/example/invoice.pdf"
    assert captured["authorization"] == "Bearer test-token"
    assert captured["content_type"] is None
    assert captured["allow_redirects"] is True
    assert mirrored[0]["mirror_status"] == "mirrored"
    assert (tmp_path / "documents" / "tms" / "invoice.pdf").read_bytes() == b"vault document"


def test_mirror_tms_documents_prefers_mcp_signed_download_url(tmp_path):
    class FakeTMSClient:
        api_url = "https://api.cargolo.de"

        def __init__(self):
            self.calls = []

        def get_document_download_url(self, **kwargs):
            self.calls.append(kwargs)
            return {
                "status": "ok",
                "readonly": True,
                "document": {"download_url": "https://signed.example.test/invoice.pdf?token=secret"},
                "warnings": [],
            }

        def _headers(self):
            raise AssertionError("signed URL download must not request TMS bearer headers")

    class FakeResponse:
        status_code = 200
        url = "https://signed.example.test/invoice.pdf?token=secret"
        content = b"signed document"

        def raise_for_status(self):
            return None

    captured = {}

    def fake_get(url, headers=None, timeout=0, allow_redirects=True):
        captured["url"] = url
        captured["headers"] = headers
        return FakeResponse()

    registry = {
        "tms_documents": [
            {
                "tms_document_id": "doc-uuid-1",
                "document_type": "commercial_invoice",
                "filename": "invoice.pdf",
                "url": "/vault/private/invoice.pdf",
            }
        ]
    }
    client = FakeTMSClient()

    with patch("plugins.cargolo_ops.case_lifecycle.requests.get", side_effect=fake_get):
        mirrored = mirror_tms_documents(case_root=tmp_path / "AN-12345", registry=registry, tms_client=client, an="AN-12345")

    assert client.calls == [{"admin_user_id": 106, "an": "AN-12345", "tms_document_id": "doc-uuid-1", "document_id": None, "ttl_seconds": 3600}]
    assert captured["url"] == "https://signed.example.test/invoice.pdf?token=secret"
    assert captured["headers"] in (None, {})
    assert mirrored[0]["download_url_source"] == "tms_mcp_get_document_download_url"
    assert "https://signed.example.test" not in json.dumps(mirrored[0])
    assert mirrored[0]["mirror_status"] == "mirrored"
    assert (tmp_path / "AN-12345" / "documents" / "tms" / "invoice.pdf").read_bytes() == b"signed document"


def test_sync_case_lifecycle_hands_billing_context_to_pricing(tmp_path):
    snapshot = TMSSnapshot(
        order_id="AN-55555",
        shipment_number="AN-55555",
        status="confirmed",
        source="live",
        detail={"network": "sea", "documents": [], "customer_name": "Beispiel GmbH"},
    )
    billing_context = {
        "status": "ok",
        "billing": {
            "sums": {"total_vk": 3200, "total_ek": 2500, "margin": 700},
            "items": [{"name": "Sea freight", "vk_price": 3200, "ek_price": 2500}],
        },
    }
    captured = {}

    class FakePricingModule:
        @staticmethod
        def record_pricing_billing_context(**kwargs):
            captured.update(kwargs)
            return {"status": "ok", "event_type": "billing_context_synced", "record_hash": "bill-context"}

    with patch("plugins.cargolo_ops.processor._live_shipment_exists", return_value=True), \
         patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(snapshot, {}, billing_context)), \
         patch("plugins.cargolo_ops.processor._sync_mail_history", return_value=0), \
         patch("plugins.cargolo_ops.case_lifecycle.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])), \
         patch("plugins.cargolo_ops.case_lifecycle._load_pricing_ingest_adapter", return_value=FakePricingModule):
        result = sync_case_lifecycle("AN-55555", storage_root=tmp_path, analyze_documents=True)

    assert captured["order_id"] == "AN-55555"
    assert captured["billing_context"] == billing_context
    snapshot_json = json.loads((Path(result["case_root"]) / "tms_snapshot.json").read_text(encoding="utf-8"))
    assert snapshot_json["pricing_kb_billing_event"]["event_type"] == "billing_context_synced"
