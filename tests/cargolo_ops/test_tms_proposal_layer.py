import json
from pathlib import Path


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_case(root: Path, order_id: str, *, analyzed_documents: list[dict], tms_snapshot: dict) -> None:
    case_root = root / "orders" / order_id
    (case_root / "documents" / "analysis").mkdir(parents=True, exist_ok=True)
    (case_root / "tms_snapshot.json").write_text(json.dumps(tms_snapshot, ensure_ascii=False), encoding="utf-8")
    registry_docs = []
    for index, doc in enumerate(analyzed_documents, start=1):
        filename = doc.get("filename") or f"doc-{index}.json"
        analysis_path = case_root / "documents" / "analysis" / f"{index}.json"
        analysis_path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        registry_docs.append({
            "filename": filename,
            "analysis_path": str(analysis_path),
            "analysis_doc_type": doc.get("doc_type"),
        })
    (case_root / "documents" / "registry.json").write_text(
        json.dumps({"analyzed_documents": registry_docs}, ensure_ascii=False),
        encoding="utf-8",
    )


def test_agentic_proposal_layer_queues_reliable_review_only_cards(tmp_path: Path) -> None:
    from plugins.cargolo_ops.tms_proposal_layer import queue_agentic_tms_review_cards

    _write_case(
        tmp_path,
        "AN-12218",
        tms_snapshot={
            "detail": {
                "totals": {"total_weight_kg": 0, "total_packages": 0},
                "freight_details": {"seal_number": ""},
                "dates": {"pickup_date": ""},
            }
        },
        analyzed_documents=[
            {
                "filename": "waybill.pdf",
                "doc_type": "waybill",
                "extracted_fields": {
                    "gross_weight_kg": "10100 kg",
                    "cartons": "714 cartons",
                    "seal_number": "ZH165701",
                    "loading_date": "30.04.2026",
                    "hs_code": "94036090",
                },
            }
        ],
    )

    cards = queue_agentic_tms_review_cards(root=tmp_path, order_id="AN-12218", max_cards=3)

    assert len(cards) == 3
    assert [(card["target"], card["value"]) for card in cards] == [
        ("cargo_weight_kg", "10100"),
        ("cargo_pieces", "714"),
        ("seal_number", "ZH165701"),
    ]
    assert all(card["write_supported"] is False for card in cards)
    queue_path = tmp_path / "orders" / "AN-12218" / "teams" / "pending_tms_actions.jsonl"
    rows = _read_jsonl(queue_path)
    assert [row["status"] for row in rows] == ["pending_review", "pending_review", "pending_review"]
    assert all(row["write_policy"] == "no_auto_write_without_review" for row in rows)
    assert all(row["evidence"]["summary"] for row in rows)


def test_agentic_proposal_layer_skips_duplicates_and_conflicts(tmp_path: Path) -> None:
    from plugins.cargolo_ops.tms_proposal_layer import queue_agentic_tms_review_cards

    _write_case(
        tmp_path,
        "AN-12218",
        tms_snapshot={"detail": {"totals": {"total_weight_kg": 0, "total_packages": 0}}},
        analyzed_documents=[
            {"filename": "waybill-a.pdf", "doc_type": "waybill", "extracted_fields": {"gross_weight_kg": "10100", "cartons": "714"}},
            {"filename": "waybill-b.pdf", "doc_type": "waybill", "extracted_fields": {"gross_weight_kg": "9999", "cartons": "714"}},
        ],
    )

    cards = queue_agentic_tms_review_cards(root=tmp_path, order_id="AN-12218", max_cards=3)
    assert [(card["target"], card["value"]) for card in cards] == [("cargo_pieces", "714")]

    second = queue_agentic_tms_review_cards(root=tmp_path, order_id="AN-12218", max_cards=3)
    assert second == []
    rows = _read_jsonl(tmp_path / "orders" / "AN-12218" / "teams" / "pending_tms_actions.jsonl")
    assert len(rows) == 1


def test_agentic_proposal_layer_queues_supported_pickup_date_but_no_write(tmp_path: Path) -> None:
    from plugins.cargolo_ops.tms_proposal_layer import queue_agentic_tms_review_cards

    _write_case(
        tmp_path,
        "AN-12218",
        tms_snapshot={"detail": {"dates": {"pickup_date": ""}, "totals": {"total_weight_kg": 10100, "total_packages": 714}}},
        analyzed_documents=[
            {"filename": "cmr.pdf", "doc_type": "waybill", "extracted_fields": {"loading_date": "2026-04-30"}},
        ],
    )

    cards = queue_agentic_tms_review_cards(root=tmp_path, order_id="AN-12218", max_cards=3)
    assert [(card["target"], card["value"], card["write_supported"]) for card in cards] == [("pickup_date", "2026-04-30", True)]
    row = _read_jsonl(tmp_path / "orders" / "AN-12218" / "teams" / "pending_tms_actions.jsonl")[0]
    assert row["source"] == "case_evidence_refresh"
    assert row["write_supported"] is True
