from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from plugins.cargolo_ops.case_lifecycle import mirror_tms_documents, sync_case_lifecycle
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

    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(snapshot, requirements, {})), \
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

    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(snapshot, {}, billing_context)), \
         patch("plugins.cargolo_ops.processor._sync_mail_history", return_value=0), \
         patch("plugins.cargolo_ops.case_lifecycle.analyze_case_documents", side_effect=lambda **kwargs: (kwargs["registry"], [])), \
         patch("plugins.cargolo_ops.case_lifecycle._load_pricing_ingest_adapter", return_value=FakePricingModule):
        result = sync_case_lifecycle("AN-55555", storage_root=tmp_path, analyze_documents=True)

    assert captured["order_id"] == "AN-55555"
    assert captured["billing_context"] == billing_context
    snapshot_json = json.loads((Path(result["case_root"]) / "tms_snapshot.json").read_text(encoding="utf-8"))
    assert snapshot_json["pricing_kb_billing_event"]["event_type"] == "billing_context_synced"
