import json
from pathlib import Path

from plugins.cargolo_ops.storage import CaseStore
from plugins.cargolo_ops.writeback_executor import run_writeback_executor


def _seed_order(tmp_path: Path, order_id: str, pending_actions: list[dict], action_summary: dict | None = None):
    store = CaseStore(tmp_path)
    case_root = store.ensure_case(order_id)
    pending_payload = {
        "version": 1,
        "generated_at": "2026-04-19T00:00:00Z",
        "order_id": order_id,
        "shipment_uuid": f"uuid-{order_id}",
        "shipment_number": order_id,
        "status": "pending_write_access",
        "requires_write_access": True,
        "received_types": [],
        "expected_types": [],
        "missing_types": [],
        "document_matches": [],
        "field_update_candidates": [],
        "open_questions": [],
        "action_summary": action_summary or {
            "write_now": sum(1 for row in pending_actions if row.get("action_status") == "write_now"),
            "review": sum(1 for row in pending_actions if row.get("action_status") == "review"),
            "not_yet_due": sum(1 for row in pending_actions if row.get("action_status") == "not_yet_due"),
            "not_yet_knowable": sum(1 for row in pending_actions if row.get("action_status") == "not_yet_knowable"),
        },
        "pending_actions": pending_actions,
    }
    store.save_tms_pending_updates(order_id, pending_payload, "pending")
    applied_payload = {
        "version": 1,
        "generated_at": "2026-04-19T00:00:00Z",
        "order_id": order_id,
        "shipment_uuid": f"uuid-{order_id}",
        "shipment_number": order_id,
        "status": "awaiting_write_access",
        "derived_from_pending_updates": "tms/pending_updates.json",
        "requires_write_access": True,
        "applied_actions": [],
        "failed_actions": [],
        "skipped_actions": [],
        "dry_run_actions": [],
        "last_attempted_at": None,
        "applied_at": None,
    }
    store.save_tms_applied_updates(order_id, applied_payload)
    return case_root


def test_run_writeback_executor_dry_run_only_plans_write_now(tmp_path):
    _seed_order(
        tmp_path,
        "AN-DRY",
        [
            {
                "action_type": "field_update",
                "target": "shipment.dates.latest_delivery_date",
                "suggested_value": "2026-05-20",
                "source": "mail+legs",
                "reason": "clear evidence",
                "requires_write_access": True,
                "action_status": "write_now",
            },
            {
                "action_type": "document_gap",
                "target": "documents.commercial_invoice",
                "suggested_value": "missing_after_mail_tms_reconciliation",
                "source": "document_registry.missing_types",
                "reason": "operator should review",
                "requires_write_access": True,
                "action_status": "review",
            },
        ],
    )

    result = run_writeback_executor(storage_root=tmp_path, dry_run=True)

    assert result["mode"] == "dry_run"
    assert result["summary"]["orders_seen"] == 1
    assert result["summary"]["write_now_candidates"] == 1
    assert result["summary"]["dry_run_actions"] == 1

    applied = json.loads((tmp_path / "orders" / "AN-DRY" / "tms" / "applied_updates.json").read_text(encoding="utf-8"))
    assert applied["status"] == "dry_run_ready"
    assert applied["applied_actions"] == []
    assert applied["failed_actions"] == []
    assert applied["skipped_actions"] == []
    assert len(applied["dry_run_actions"]) == 1
    assert applied["dry_run_actions"][0]["target"] == "shipment.dates.latest_delivery_date"

    latest_run = json.loads((tmp_path / "tms_writeback_runs" / "latest.json").read_text(encoding="utf-8"))
    assert latest_run["summary"]["dry_run_actions"] == 1
    assert latest_run["orders"][0]["order_id"] == "AN-DRY"


def test_run_writeback_executor_execute_mode_uses_injected_callback_for_write_now(tmp_path):
    _seed_order(
        tmp_path,
        "AN-LIVE",
        [
            {
                "action_type": "field_update",
                "target": "billing_items[?].name",
                "suggested_value": "Nachlauf bis Duisburg",
                "source": "tms.billing_items + shipment.destination.city",
                "reason": "clear evidence",
                "requires_write_access": True,
                "action_status": "write_now",
            },
            {
                "action_type": "document_gap",
                "target": "documents.customs_document",
                "suggested_value": "missing_after_mail_tms_reconciliation",
                "source": "document_registry.missing_types",
                "reason": "not due yet",
                "requires_write_access": True,
                "action_status": "not_yet_due",
            },
        ],
    )
    calls = []

    def _fake_apply(action, context):
        calls.append({"action": dict(action), "context": dict(context)})
        return {"status": "applied", "remote_action_id": "mcp-write-1"}

    result = run_writeback_executor(storage_root=tmp_path, dry_run=False, apply_action=_fake_apply)

    assert result["mode"] == "execute"
    assert result["summary"]["applied_actions"] == 1
    assert len(calls) == 1
    assert calls[0]["action"]["target"] == "billing_items[?].name"
    assert calls[0]["context"]["order_id"] == "AN-LIVE"

    applied = json.loads((tmp_path / "orders" / "AN-LIVE" / "tms" / "applied_updates.json").read_text(encoding="utf-8"))
    assert applied["status"] == "applied"
    assert len(applied["applied_actions"]) == 1
    assert applied["applied_actions"][0]["result"]["remote_action_id"] == "mcp-write-1"
    assert applied["dry_run_actions"] == []


def test_run_writeback_executor_records_skipped_results(tmp_path):
    _seed_order(
        tmp_path,
        "AN-SKIP",
        [
            {
                "action_type": "field_update",
                "target": "shipment.unsupported_field",
                "suggested_value": "foo",
                "source": "test",
                "reason": "unsupported",
                "requires_write_access": True,
                "action_status": "write_now",
            }
        ],
    )

    def _fake_apply(action, context):
        return {"status": "skipped", "reason": "unsupported_field_update_target"}

    result = run_writeback_executor(storage_root=tmp_path, dry_run=False, apply_action=_fake_apply)

    assert result["summary"]["skipped_actions"] == 1
    applied = json.loads((tmp_path / "orders" / "AN-SKIP" / "tms" / "applied_updates.json").read_text(encoding="utf-8"))
    assert applied["applied_actions"] == []
    assert applied["failed_actions"] == []
    assert len(applied["skipped_actions"]) == 1
    assert applied["skipped_actions"][0]["result"]["reason"] == "unsupported_field_update_target"
