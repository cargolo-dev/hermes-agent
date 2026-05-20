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


def test_agentic_proposal_layer_prefers_tms_container_match_and_supersedes_stale_pending(tmp_path: Path) -> None:
    from plugins.cargolo_ops.tms_proposal_layer import queue_agentic_tms_review_cards

    _write_case(
        tmp_path,
        "AN-12218",
        tms_snapshot={"detail": {"totals": {"total_weight_kg": 0, "total_packages": 0}, "freight_details": {"container_number": "FORU8867533", "seal_number": ""}}},
        analyzed_documents=[
            {"filename": "CIMU1670214.pdf", "doc_type": "bill_of_lading", "extracted_fields": {"shipment_number": "AN-12218", "container_number": "CIMU1670214", "gross_weight": "6500", "pieces": "731", "seal_number": "ZH165700"}},
            {"filename": "FORU8867533.pdf", "doc_type": "bill_of_lading", "extracted_fields": {"shipment_number": "AN-12218", "container_number": "FORU8867533", "gross_weight": "10100", "pieces": "714", "seal_number": "ZH165701"}},
        ],
    )
    first = queue_agentic_tms_review_cards(root=tmp_path, order_id="AN-12218", max_cards=3)
    assert [(card["target"], card["value"]) for card in first] == [("cargo_weight_kg", "10100"), ("cargo_pieces", "714"), ("seal_number", "ZH165701")]

    # Simulate stale proposals from an earlier weaker pass and ensure stronger
    # evidence supersedes them instead of being silently suppressed.
    queue_path = tmp_path / "orders" / "AN-12219" / "teams" / "pending_tms_actions.jsonl"
    _write_case(
        tmp_path,
        "AN-12219",
        tms_snapshot={"detail": {"totals": {"total_weight_kg": 0}, "freight_details": {"container_number": "FORU8867533"}}},
        analyzed_documents=[
            {"filename": "FORU8867533.pdf", "doc_type": "bill_of_lading", "extracted_fields": {"shipment_number": "AN-12219", "container_number": "FORU8867533", "gross_weight": "10100"}},
        ],
    )
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    queue_path.write_text(json.dumps({"status": "pending_review", "order_id": "AN-12219", "target": "cargo_weight_kg", "value": "6500"}) + "\n", encoding="utf-8")
    second = queue_agentic_tms_review_cards(root=tmp_path, order_id="AN-12219", max_cards=1)
    assert [(card["target"], card["value"]) for card in second] == [("cargo_weight_kg", "10100")]
    rows = _read_jsonl(queue_path)
    assert rows[0]["status"] == "superseded"
    assert rows[0]["superseded_by_value"] == "10100"
    assert rows[1]["status"] == "pending_review"


def test_agentic_proposal_layer_extracts_hs_code_from_goods_description(tmp_path: Path) -> None:
    from plugins.cargolo_ops.tms_proposal_layer import queue_agentic_tms_review_cards

    _write_case(
        tmp_path,
        "AN-12218",
        tms_snapshot={"detail": {"customs": {"hs_code": ""}}},
        analyzed_documents=[
            {"filename": "invoice.pdf", "doc_type": "commercial_invoice", "extracted_fields": {"goods_description": "SHOES (HS CODE: 640212)"}},
        ],
    )

    cards = queue_agentic_tms_review_cards(root=tmp_path, order_id="AN-12218", max_cards=3)
    assert [(card["target"], card["value"]) for card in cards] == [("hs_code", "640212")]


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


def test_agentic_proposal_layer_queues_eta_and_ata_write_supported_dates(tmp_path: Path) -> None:
    from plugins.cargolo_ops.tms_proposal_layer import queue_agentic_tms_review_cards

    _write_case(
        tmp_path,
        "AN-12218",
        tms_snapshot={"detail": {"dates": {"estimated_delivery_date": "", "actual_delivery_date": ""}}},
        analyzed_documents=[
            {
                "filename": "arrival-advice.pdf",
                "doc_type": "shipment_advice",
                "extracted_fields": {"shipment_number": "AN-12218", "eta": "2026-06-20", "ata": "21.06.2026"},
            },
        ],
    )

    cards = queue_agentic_tms_review_cards(root=tmp_path, order_id="AN-12218", max_cards=3)

    assert [(card["target"], card["value"], card["write_supported"]) for card in cards] == [
        ("estimated_delivery_date", "2026-06-20", True),
        ("actual_delivery_date", "2026-06-21", True),
    ]


def test_agentic_proposal_layer_surfaces_etd_and_atd_as_review_only_until_write_target_exists(tmp_path: Path) -> None:
    from plugins.cargolo_ops.tms_proposal_layer import queue_agentic_tms_review_cards

    _write_case(
        tmp_path,
        "AN-12218",
        tms_snapshot={"detail": {"milestones": {"etd_main_carriage": "", "atd_main_carriage": ""}}},
        analyzed_documents=[
            {
                "filename": "departure-advice.pdf",
                "doc_type": "shipment_advice",
                "extracted_fields": {"shipment_number": "AN-12218", "etd": "2026-06-18", "atd": "19.06.2026"},
            },
        ],
    )

    cards = queue_agentic_tms_review_cards(root=tmp_path, order_id="AN-12218", max_cards=3)

    assert [(card["target"], card["value"], card["write_supported"]) for card in cards] == [
        ("etd_main_carriage", "2026-06-18", False),
        ("atd_main_carriage", "2026-06-19", False),
    ]
