import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from plugins.cargolo_ops.document_analysis import analyze_case_documents


class _FakeResponse:
    def __init__(self, content: str):
        self.choices = [SimpleNamespace(message=SimpleNamespace(content=content, reasoning=None, reasoning_content=None, reasoning_details=None))]


def test_offer_document_analysis_hands_off_to_pricing_kb(tmp_path, monkeypatch):
    import plugins.cargolo_ops.document_analysis as document_analysis

    case_root = tmp_path / "orders" / "AN-77777"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    doc_path = inbound / "AN-77777-V1 Angebot.txt"
    doc_path.write_text("Angebot AN-77777 EUR 1490", encoding="utf-8")
    fake_pricing_root = tmp_path / "pricing"
    fake_pricing_root.mkdir()
    marker = tmp_path / "pricing_marker.json"
    (fake_pricing_root / "pricing_ingest_adapter_v1.py").write_text(
        "import json\n"
        "from pathlib import Path\n"
        f"MARKER = Path({str(marker)!r})\n"
        "def record_pricing_offer_document(**kwargs):\n"
        "    MARKER.write_text(json.dumps({k: str(v) for k, v in kwargs.items()}, sort_keys=True))\n"
        "    return {'status': 'ok', 'record_hash': 'hash-test'}\n"
        "def record_pricing_billing_document(**kwargs):\n"
        "    return {'status': 'skipped'}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(document_analysis, "PRICING_KB_ROOT", fake_pricing_root)

    registry = {
        "received_documents": [{
            "filename": doc_path.name,
            "stored_path": str(doc_path),
            "mime_type": "text/plain",
            "sha256": "offer-sha",
            "detected_types": [],
        }],
        "expected_types": [],
        "received_types": [],
        "missing_types": [],
    }
    llm_json = json.dumps({
        "doc_type": "offer",
        "confidence": "high",
        "summary": "Angebot erkannt.",
        "shipment_numbers": ["AN-77777"],
        "references": ["AN-77777-V1"],
        "suggested_registry_types": ["offer"],
        "extracted_fields": {"amount": "1490", "currency": "EUR"},
        "missing_or_unreadable": [],
        "consistency_notes": [],
        "operational_flags": [],
        "reply_relevance": "medium",
    }, ensure_ascii=False)

    def _fake_call_llm(**_kwargs):
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None, "openai")), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm):
        updated_registry, open_questions = analyze_case_documents(
            order_id="AN-77777",
            case_root=case_root,
            registry=registry,
            tms_snapshot={"detail": {"transport_mode": "sea"}},
        )

    assert open_questions == []
    assert updated_registry["received_documents"][0]["pricing_kb_event"]["status"] == "ok"
    assert marker.exists()
    marker_payload = json.loads(marker.read_text(encoding="utf-8"))
    assert marker_payload["source_skill"] == "cargolo-tms-document-monitoring"
    assert marker_payload["source_event"] == "document_analysis.analyzed"


def test_billing_document_analysis_hands_off_to_pricing_kb(tmp_path, monkeypatch):
    import plugins.cargolo_ops.document_analysis as document_analysis

    case_root = tmp_path / "orders" / "AN-88888"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    doc_path = inbound / "Hartmann Rechnung AN-88888.txt"
    doc_path.write_text("Eingangsrechnung AN-88888 EUR 6020", encoding="utf-8")
    fake_pricing_root = tmp_path / "pricing"
    fake_pricing_root.mkdir()
    marker = tmp_path / "billing_marker.json"
    (fake_pricing_root / "pricing_ingest_adapter_v1.py").write_text(
        "import json\n"
        "from pathlib import Path\n"
        f"MARKER = Path({str(marker)!r})\n"
        "def record_pricing_offer_document(**kwargs):\n"
        "    return {'status': 'skipped'}\n"
        "def record_pricing_billing_document(**kwargs):\n"
        "    MARKER.write_text(json.dumps({k: str(v) for k, v in kwargs.items()}, sort_keys=True))\n"
        "    return {'status': 'ok', 'event_type': 'billing_document_analyzed', 'record_hash': 'bill-hash'}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(document_analysis, "PRICING_KB_ROOT", fake_pricing_root)

    registry = {
        "received_documents": [{
            "filename": doc_path.name,
            "stored_path": str(doc_path),
            "mime_type": "text/plain",
            "sha256": "billing-sha",
            "detected_types": [],
        }],
        "expected_types": [],
        "received_types": [],
        "missing_types": [],
    }
    llm_json = json.dumps({
        "doc_type": "billing",
        "confidence": "high",
        "summary": "Eingangsrechnung erkannt.",
        "shipment_numbers": ["AN-88888"],
        "references": ["INV-1"],
        "suggested_registry_types": ["billing"],
        "extracted_fields": {"amount": "6020", "currency": "EUR"},
        "missing_or_unreadable": [],
        "consistency_notes": [],
        "operational_flags": [],
        "reply_relevance": "medium",
    }, ensure_ascii=False)

    def _fake_call_llm(**_kwargs):
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None, "openai")), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm):
        updated_registry, open_questions = analyze_case_documents(
            order_id="AN-88888",
            case_root=case_root,
            registry=registry,
            tms_snapshot={"detail": {"transport_mode": "road"}},
        )

    assert open_questions == []
    doc_row = updated_registry["received_documents"][0]
    assert doc_row["pricing_kb_event"]["event_type"] == "billing_document_analyzed"
    assert doc_row["pricing_kb_events"] == [doc_row["pricing_kb_event"]]
    assert marker.exists()
    marker_payload = json.loads(marker.read_text(encoding="utf-8"))
    assert marker_payload["source_skill"] == "cargolo-tms-document-monitoring"
    assert marker_payload["source_event"] == "document_analysis.analyzed"
