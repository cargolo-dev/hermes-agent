from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _default_applied_updates(*, order_id: str, shipment_uuid: str | None, shipment_number: str | None) -> dict[str, Any]:
    return {
        "version": 1,
        "generated_at": _utc_now_iso(),
        "order_id": order_id,
        "shipment_uuid": shipment_uuid,
        "shipment_number": shipment_number or order_id,
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


def run_writeback_executor(
    *,
    storage_root: str | Path,
    dry_run: bool = True,
    apply_action: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = Path(storage_root)
    queue_path = root / "tms_writeback_queue.json"
    queue_payload = _read_json(queue_path)
    orders = [row for row in (queue_payload.get("orders") or []) if isinstance(row, dict)]
    now = _utc_now_iso()

    if not dry_run and apply_action is None:
        raise ValueError("apply_action callback is required when dry_run=False")

    run_payload: dict[str, Any] = {
        "version": 1,
        "generated_at": now,
        "mode": "dry_run" if dry_run else "execute",
        "orders": [],
        "summary": {
            "orders_seen": len(orders),
            "write_now_candidates": 0,
            "dry_run_actions": 0,
            "applied_actions": 0,
            "failed_actions": 0,
            "skipped_actions": 0,
        },
    }

    for order in orders:
        order_id = str(order.get("order_id") or "").strip()
        if not order_id:
            continue
        shipment_uuid = order.get("shipment_uuid")
        shipment_number = order.get("shipment_number") or order_id
        case_root = root / "orders" / order_id
        applied_path = case_root / "tms" / "applied_updates.json"
        applied_payload = _read_json(applied_path) or _default_applied_updates(
            order_id=order_id,
            shipment_uuid=shipment_uuid,
            shipment_number=shipment_number,
        )
        write_now_actions = [
            row for row in (order.get("pending_actions") or [])
            if isinstance(row, dict) and str(row.get("action_status") or "") == "write_now"
        ]
        run_payload["summary"]["write_now_candidates"] += len(write_now_actions)

        order_result = {
            "order_id": order_id,
            "shipment_uuid": shipment_uuid,
            "shipment_number": shipment_number,
            "mode": run_payload["mode"],
            "write_now_candidates": len(write_now_actions),
            "dry_run_actions": [],
            "applied_actions": [],
            "failed_actions": [],
            "skipped_actions": [],
        }

        if dry_run:
            planned_actions = [
                {
                    **action,
                    "planned_at": now,
                    "executor_mode": "dry_run",
                }
                for action in write_now_actions
            ]
            applied_payload["status"] = "dry_run_ready" if planned_actions else applied_payload.get("status") or "awaiting_write_access"
            applied_payload["last_attempted_at"] = now
            applied_payload["dry_run_actions"] = planned_actions
            order_result["dry_run_actions"] = planned_actions
            run_payload["summary"]["dry_run_actions"] += len(planned_actions)
        else:
            applied_payload["last_attempted_at"] = now
            applied_payload["dry_run_actions"] = []
            for action in write_now_actions:
                context = {
                    "order_id": order_id,
                    "shipment_uuid": shipment_uuid,
                    "shipment_number": shipment_number,
                    "pending_updates_path": order.get("pending_updates_path"),
                    "case_root": str(case_root),
                }
                try:
                    result = apply_action(action, context) if apply_action else {}
                    result_status = str((result or {}).get("status") or "applied").strip().lower()
                    entry = {
                        "attempted_at": now,
                        "action": action,
                        "result": result,
                    }
                    if result_status == "failed":
                        applied_payload.setdefault("failed_actions", []).append(entry)
                        order_result["failed_actions"].append(entry)
                        run_payload["summary"]["failed_actions"] += 1
                    elif result_status == "skipped":
                        applied_payload.setdefault("skipped_actions", []).append(entry)
                        order_result["skipped_actions"].append(entry)
                        run_payload["summary"]["skipped_actions"] += 1
                    else:
                        applied_payload.setdefault("applied_actions", []).append(entry)
                        order_result["applied_actions"].append(entry)
                        run_payload["summary"]["applied_actions"] += 1
                except Exception as exc:  # pragma: no cover - tested via result shape, not specific exception type
                    failed_entry = {
                        "attempted_at": now,
                        "action": action,
                        "error": str(exc),
                    }
                    applied_payload.setdefault("failed_actions", []).append(failed_entry)
                    order_result["failed_actions"].append(failed_entry)
                    run_payload["summary"]["failed_actions"] += 1
            if order_result["failed_actions"] and order_result["applied_actions"]:
                applied_payload["status"] = "partial"
            elif order_result["failed_actions"]:
                applied_payload["status"] = "failed"
            elif order_result["applied_actions"]:
                applied_payload["status"] = "applied"
                applied_payload["applied_at"] = now

        _write_json(applied_path, applied_payload)
        run_payload["orders"].append(order_result)

    runs_dir = root / "tms_writeback_runs"
    timestamp_name = now.replace(":", "").replace("-", "")
    _write_json(runs_dir / f"run_{timestamp_name}.json", run_payload)
    _write_json(runs_dir / "latest.json", run_payload)
    return run_payload
