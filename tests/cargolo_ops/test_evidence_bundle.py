from __future__ import annotations

import json
from pathlib import Path

from plugins.cargolo_ops.evidence_bundle import build_evidence_bundle
from plugins.cargolo_ops.evidence_freshness import EvidenceNeed, plan_evidence_refresh


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_missing_case_bundle_is_read_only(tmp_path: Path) -> None:
    bundle = build_evidence_bundle("AN-404", storage_root=tmp_path)

    assert bundle["status"] == "missing_case"
    assert bundle["order_id"] == "AN-404"
    assert not (tmp_path / "orders" / "AN-404").exists()
    assert "case_state" in bundle["missing_sources"]


def test_bundle_reads_canonical_sources_and_source_statuses(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-12345"
    _write_json(case_root / "case_state.json", {
        "order_id": "AN-12345",
        "mode": "sea",
        "current_status": "in_transit",
        "tms_last_sync_at": "2026-05-20T08:00:00Z",
        "last_email_at": "2026-05-20T07:55:00Z",
        "open_questions": ["Bitte ETA bestätigen"],
    })
    _write_json(case_root / "tms_snapshot.json", {
        "fetched_at": "2026-05-20T08:00:00Z",
        "status": "in_transit",
        "eta": "2026-05-25",
        "pod": "DEHAM",
        "carrier_reference": "HLCU123",
        "billing_context": {"total_vk": 1500, "total_ek": 1000, "margin": 500},
    })
    _write_jsonl(case_root / "email_index.jsonl", [
        {"received_at": "2026-05-20T07:00:00Z", "subject": "Alt", "from": "a@example.com"},
        {"received_at": "2026-05-20T07:55:00Z", "subject": "ETA Update", "from": "kunde@example.com"},
    ])
    _write_jsonl(case_root / "audit/actions.jsonl", [
        {"timestamp": "2026-05-20T08:01:00Z", "action": "sync_case_lifecycle", "history_sync_status": "ok", "history_sync_mode": "delta"}
    ])
    _write_json(case_root / "documents" / "registry.json", {
        "updated_at": "2026-05-20T07:50:00Z",
        "analysis_generated_at": "2026-05-20T07:59:00Z",
        "received_types": ["commercial_invoice"],
        "expected_types": ["commercial_invoice", "packing_list"],
        "missing_types": ["packing_list"],
        "received_documents": [{"document_type": "commercial_invoice"}],
        "document_analysis_summary_path": "documents/analysis/latest_summary.json",
    })
    _write_json(case_root / "documents" / "analysis" / "latest_summary.json", {
        "generated_at": "2026-05-20T07:59:00Z",
        "summary": "CI liegt vor, PL fehlt.",
        "open_questions": ["Packing List fehlt"],
        "documents": [{"document_type": "commercial_invoice"}],
    })
    _write_json(case_root / "tms" / "billing_context.json", {
        "generated_at": "2026-05-20T08:00:30Z",
        "total_vk": 1500,
        "total_ek": 1000,
        "margin": 500,
    })

    bundle = build_evidence_bundle("AN-12345", storage_root=tmp_path, question="Ist die Sendung sauber?")

    assert bundle["status"] == "ok"
    assert bundle["sources"]["case_state"]["available"] is True
    assert bundle["sources"]["tms_snapshot"]["summary"]["eta"] == "2026-05-25"
    assert bundle["sources"]["email_index"]["summary"]["latest_subject"] == "ETA Update"
    assert bundle["sources"]["email_index"]["freshness_at"] == "2026-05-20T08:01:00Z"
    assert bundle["sources"]["document_registry"]["summary"]["missing_types"] == ["packing_list"]
    assert bundle["sources"]["document_analysis"]["summary"]["open_questions"] == ["Packing List fehlt"]
    assert bundle["sources"]["billing_context"]["summary"]["margin"] == 500
    assert bundle["sources"]["teams_thread_context"]["available"] is False


def test_freshness_plan_is_intent_sensitive_and_requests_only_needed_sources(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-10000"
    case_root.mkdir(parents=True)
    _write_json(case_root / "case_state.json", {"order_id": "AN-10000", "tms_last_sync_at": "2026-05-20T08:00:00Z"})
    _write_json(case_root / "tms_snapshot.json", {"fetched_at": "2026-05-20T08:00:00Z", "eta": "2026-05-25"})
    _write_jsonl(case_root / "email_index.jsonl", [])

    eta_plan = plan_evidence_refresh(
        "AN-10000",
        storage_root=tmp_path,
        question="ETA AN-10000?",
        requested_needs=[EvidenceNeed.TMS_SNAPSHOT],
        now="2026-05-20T08:05:00Z",
    )
    assert eta_plan["requires_refresh"] is False
    assert "document_registry" not in eta_plan["required_sources"]
    assert "billing_context" not in eta_plan["required_sources"]

    mail_plan = plan_evidence_refresh(
        "AN-10000",
        storage_root=tmp_path,
        question="Hat der Kunde zu AN-10000 geantwortet?",
        requested_needs=[EvidenceNeed.MAIL_HISTORY],
        now="2026-05-20T08:45:00Z",
    )
    assert mail_plan["sources"]["email_index"]["refresh_needed"] is True
    assert "Mailhistorie" in " ".join(mail_plan["caveats"])


def test_document_analysis_stale_when_registry_newer_than_summary(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-20000"
    _write_json(case_root / "case_state.json", {"order_id": "AN-20000"})
    _write_json(case_root / "tms_snapshot.json", {"fetched_at": "2026-05-20T08:00:00Z"})
    _write_json(case_root / "documents" / "registry.json", {
        "updated_at": "2026-05-20T08:10:00Z",
        "analysis_generated_at": "2026-05-20T08:00:00Z",
        "document_analysis_summary_path": "documents/analysis/latest_summary.json",
        "received_documents": [{"document_type": "packing_list"}],
    })
    _write_json(case_root / "documents" / "analysis" / "latest_summary.json", {
        "generated_at": "2026-05-20T08:00:00Z",
        "summary": "Alt",
    })

    plan = plan_evidence_refresh(
        "AN-20000",
        storage_root=tmp_path,
        question="Fehlt noch was bei AN-20000?",
        requested_needs=[EvidenceNeed.DOCUMENTS],
        now="2026-05-20T08:12:00Z",
    )

    assert plan["sources"]["document_analysis"]["status"] == "stale"
    assert plan["sources"]["document_analysis"]["refresh_needed"] is True


def test_failed_mail_sync_audit_does_not_make_mail_history_fresh(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-FAILMAIL"
    _write_json(case_root / "case_state.json", {"order_id": "AN-FAILMAIL", "last_email_at": "2026-05-20T07:00:00Z"})
    _write_jsonl(case_root / "email_index.jsonl", [
        {"received_at": "2026-05-20T07:00:00Z", "subject": "Alte Mail", "from": "kunde@example.com"}
    ])
    _write_jsonl(case_root / "audit/actions.jsonl", [
        {"timestamp": "2026-05-20T08:59:00Z", "action": "sync_case_lifecycle", "history_sync_status": "failed"}
    ])

    plan = plan_evidence_refresh(
        "AN-FAILMAIL",
        storage_root=tmp_path,
        question="Hat Kunde bei AN-FAILMAIL geantwortet?",
        requested_needs=[EvidenceNeed.MAIL_HISTORY],
        now="2026-05-20T09:00:00Z",
    )
    bundle = build_evidence_bundle("AN-FAILMAIL", storage_root=tmp_path, question="Hat Kunde geantwortet?", now="2026-05-20T09:00:00Z")

    assert plan["sources"]["email_index"]["status"] == "stale"
    assert plan["sources"]["email_index"]["refresh_needed"] is True
    assert bundle["sources"]["email_index"]["freshness_at"] == "2026-05-20T07:00:00Z"
    assert "email_index" in bundle["stale_sources"]


def test_pricing_need_maps_to_missing_pricing_evidence_with_caveat(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-PRICE"
    _write_json(case_root / "case_state.json", {"order_id": "AN-PRICE"})

    plan = plan_evidence_refresh(
        "AN-PRICE",
        storage_root=tmp_path,
        question="Wie ist die Marge und Preisposition bei AN-PRICE?",
        requested_needs=[EvidenceNeed.PRICING_KB],
        now="2026-05-20T09:00:00Z",
    )

    assert "pricing_kb" in plan["required_sources"]
    assert plan["sources"]["pricing_kb"]["status"] == "missing"
    assert plan["sources"]["pricing_kb"]["refresh_needed"] is True
    assert "Pricing" in " ".join(plan["caveats"])


def test_bundle_includes_teams_thread_context_summary(tmp_path: Path) -> None:
    from plugins.cargolo_ops.evidence_bundle import build_evidence_bundle

    case_root = tmp_path / "orders" / "AN-11755"
    _write_json(case_root / "case_state.json", {"order_id": "AN-11755"})
    _write_json(case_root / "teams" / "thread_context.json", {
        "updated_at": "2026-05-20T08:00:00Z",
        "last_order_id": "AN-11755",
        "last_user_message": {"text": "Was ist mit AN-11755?"},
        "last_hermes_response": {"text": "Lage: sauber."},
        "recent_messages": [{"role": "user", "text": "Was ist mit AN-11755?"}, {"role": "assistant", "text": "Lage: sauber."}],
    })

    bundle = build_evidence_bundle("AN-11755", storage_root=tmp_path)

    source = bundle["sources"]["teams_thread_context"]
    assert source["available"] is True
    assert source["summary"]["last_order_id"] == "AN-11755"
    assert source["summary"]["last_user_message_text"] == "Was ist mit AN-11755?"
    assert source["summary"]["last_hermes_response_text"] == "Lage: sauber."
