from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from hermes_constants import get_hermes_home

from .models import CaseState, EntitiesSnapshot, IncomingMessagePayload, TaskProposal, utc_now_iso


class CaseStore:
    def __init__(self, root: Path | None = None):
        self.root = root or (get_hermes_home() / "cargolo_asr")
        self.orders_root = self.root / "orders"
        self.review_root = self.root / "review_queue"
        self.runtime_root = self.root / "runtime"
        for directory in (self.root, self.orders_root, self.review_root, self.runtime_root):
            directory.mkdir(parents=True, exist_ok=True)

    def order_path(self, order_id: str) -> Path:
        return self.orders_root / order_id

    def ensure_case(self, order_id: str) -> Path:
        case_root = self.order_path(order_id)
        for rel in [
            "emails/raw",
            "emails/normalized",
            "emails/drafts",
            "documents/inbound",
            "documents/generated",
            "documents/analysis",
            "tasks",
            "audit",
            "tms",
            "logs",
            "analysis/briefs",
            "analysis/raw",
        ]:
            (case_root / rel).mkdir(parents=True, exist_ok=True)
        self._ensure_json(case_root / "case_state.json", CaseState(order_id=order_id).model_dump())
        self._ensure_json(case_root / "entities.json", EntitiesSnapshot().model_dump())
        self._ensure_json(case_root / "tms_snapshot.json", {})
        self._ensure_text(case_root / "timeline.md", f"# Timeline {order_id}\n")
        self._ensure_text(case_root / "email_index.jsonl", "")
        self._ensure_text(case_root / "tasks/task_log.jsonl", "")
        self._ensure_text(case_root / "audit/actions.jsonl", "")
        self._ensure_json(case_root / "documents" / "registry.json", {
            "registry_version": 1,
            "updated_at": utc_now_iso(),
            "received_documents": [],
            "tms_documents": [],
            "received_types": [],
            "expected_types": [],
            "missing_types": [],
            "analyzed_documents": [],
            "analysis_open_questions": [],
            "document_analysis_summary_path": None,
        })
        return case_root

    def save_unassigned_event(self, payload: dict[str, Any], reason: str) -> Path:
        path = self.review_root / f"{utc_now_iso().replace(':', '').replace('-', '')}_{payload.get('event_id', 'event')}.json"
        self._write_json(path, {"reason": reason, "payload": payload, "saved_at": utc_now_iso()})
        return path

    def load_case_state(self, order_id: str) -> CaseState:
        path = self.ensure_case(order_id) / "case_state.json"
        return CaseState.model_validate(self._read_json(path))

    def save_case_state(self, order_id: str, state: CaseState) -> Path:
        path = self.ensure_case(order_id) / "case_state.json"
        self._write_json(path, state.model_dump())
        return path

    def load_entities(self, order_id: str) -> EntitiesSnapshot:
        path = self.ensure_case(order_id) / "entities.json"
        return EntitiesSnapshot.model_validate(self._read_json(path))

    def save_entities(self, order_id: str, entities: EntitiesSnapshot) -> Path:
        path = self.ensure_case(order_id) / "entities.json"
        self._write_json(path, entities.model_dump())
        return path

    def load_tms_snapshot(self, order_id: str) -> dict[str, Any]:
        path = self.ensure_case(order_id) / "tms_snapshot.json"
        return self._read_json(path)

    def save_tms_snapshot(self, order_id: str, snapshot: dict[str, Any]) -> Path:
        path = self.ensure_case(order_id) / "tms_snapshot.json"
        self._write_json(path, snapshot)
        return path

    def list_email_index(self, order_id: str) -> list[dict[str, Any]]:
        path = self.ensure_case(order_id) / "email_index.jsonl"
        rows: list[dict[str, Any]] = []
        if not path.exists():
            return rows
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
        return rows

    def has_message(self, order_id: str, message_id: str, dedupe_hash: str) -> bool:
        for row in self.list_email_index(order_id):
            if row.get("message_id") == message_id or row.get("dedupe_hash") == dedupe_hash:
                return True
        return False

    def append_email_index(self, order_id: str, row: dict[str, Any]) -> Path:
        path = self.ensure_case(order_id) / "email_index.jsonl"
        self._append_jsonl(path, row)
        return path

    def store_raw_email(self, order_id: str, message: IncomingMessagePayload, payload: dict[str, Any], prefix: str = "msg") -> Path:
        filename = self._message_file_name(prefix, message)
        path = self.ensure_case(order_id) / "emails/raw" / f"{filename}.json"
        self._write_json(path, payload)
        return path

    def store_normalized_email(self, order_id: str, message: IncomingMessagePayload, normalized: dict[str, Any]) -> Path:
        filename = self._message_file_name("norm", message)
        path = self.ensure_case(order_id) / "emails/normalized" / f"{filename}.json"
        self._write_json(path, normalized)
        return path

    def store_draft(self, order_id: str, message: IncomingMessagePayload, content: str) -> Path:
        filename = self._message_file_name("draft", message)
        path = self.ensure_case(order_id) / "emails/drafts" / f"{filename}.md"
        path.write_text(content, encoding="utf-8")
        return path

    def store_attachment(self, order_id: str, filename: str, content: bytes) -> Path:
        safe_name = filename.replace("/", "_").replace("\\", "_")
        inbound_dir = self.ensure_case(order_id) / "documents/inbound"
        inbound_dir.mkdir(parents=True, exist_ok=True)

        content_sha = hashlib.sha256(content).hexdigest()
        for existing in inbound_dir.iterdir():
            if not existing.is_file():
                continue
            try:
                if hashlib.sha256(existing.read_bytes()).hexdigest() == content_sha:
                    return existing
            except Exception:
                continue

        path = inbound_dir / safe_name
        if path.exists():
            stem = path.stem
            suffix = path.suffix
            counter = 2
            while path.exists():
                path = path.with_name(f"{stem}_{counter}{suffix}")
                counter += 1
        path.write_bytes(content)
        return path

    def append_timeline(self, order_id: str, heading: str, summary: str, delta: str, next_step: str) -> Path:
        path = self.ensure_case(order_id) / "timeline.md"
        chunk = (
            f"\n## {utc_now_iso()} — {heading}\n"
            f"- Summary: {summary}\n"
            f"- Delta: {delta}\n"
            f"- Next Step: {next_step}\n"
        )
        with path.open("a", encoding="utf-8") as handle:
            handle.write(chunk)
        return path

    def append_task_log(self, order_id: str, proposal: TaskProposal | dict[str, Any]) -> Path:
        path = self.ensure_case(order_id) / "tasks/task_log.jsonl"
        row = proposal.model_dump() if isinstance(proposal, TaskProposal) else proposal
        self._append_jsonl(path, row)
        return path

    def append_audit(self, order_id: str, action: str, result: str, files: list[str], extra: dict[str, Any] | None = None) -> Path:
        path = self.ensure_case(order_id) / "audit/actions.jsonl"
        row = {
            "timestamp": utc_now_iso(),
            "actor": "Hermes",
            "action": action,
            "result": result,
            "files": files,
        }
        if extra:
            row.update(extra)
        self._append_jsonl(path, row)
        return path

    def list_audit_events(self, order_id: str) -> list[dict[str, Any]]:
        path = self.order_path(order_id) / "audit/actions.jsonl"
        rows: list[dict[str, Any]] = []
        if not path.exists():
            return rows
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows

    def save_analysis_brief(self, order_id: str, payload: dict[str, Any], *, message_hint: str | None = None) -> Path:
        case_root = self.ensure_case(order_id)
        hint = (message_hint or utc_now_iso()).replace(":", "").replace("-", "")
        brief_dir = case_root / "analysis" / "briefs"
        path = brief_dir / f"brief_{hint[:80]}.json"
        self._write_json(path, payload)
        self._write_json(case_root / "analysis" / "latest_brief.json", payload)
        return path

    def save_analysis_raw(self, order_id: str, payload: dict[str, Any], *, name: str, message_hint: str | None = None) -> Path:
        case_root = self.ensure_case(order_id)
        hint = (message_hint or utc_now_iso()).replace(":", "").replace("-", "")
        raw_dir = case_root / "analysis" / "raw"
        path = raw_dir / f"{name}_{hint[:80]}.json"
        self._write_json(path, payload)
        return path

    def load_document_registry(self, order_id: str) -> dict[str, Any]:
        path = self.ensure_case(order_id) / "documents" / "registry.json"
        return self._read_json(path)

    def save_document_registry(self, order_id: str, registry: dict[str, Any]) -> Path:
        path = self.ensure_case(order_id) / "documents" / "registry.json"
        self._write_json(path, registry)
        return path

    def save_tms_pending_updates(self, order_id: str, payload: dict[str, Any], markdown: str) -> tuple[Path, Path]:
        case_root = self.ensure_case(order_id)
        json_path = case_root / "tms" / "pending_updates.json"
        md_path = case_root / "tms" / "pending_updates.md"
        self._write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        self._update_global_tms_writeback_queue(order_id=order_id, pending_plan=payload, pending_updates_path=json_path)
        self._update_global_review_queue(order_id=order_id, pending_plan=payload, pending_updates_path=json_path)
        return json_path, md_path

    def save_tms_applied_updates(self, order_id: str, payload: dict[str, Any]) -> Path:
        case_root = self.ensure_case(order_id)
        json_path = case_root / "tms" / "applied_updates.json"
        self._write_json(json_path, payload)
        return json_path

    def append_tms_sync_log(self, order_id: str, payload: dict[str, Any]) -> Path:
        path = self.ensure_case(order_id) / "tms" / "sync_log.jsonl"
        self._append_jsonl(path, payload)
        return path

    def save_case_report(self, order_id: str, payload: dict[str, Any], markdown: str) -> tuple[Path, Path]:
        case_root = self.ensure_case(order_id)
        legacy_json_path = case_root / "analysis" / "speditionsanalyse_latest.json"
        legacy_md_path = case_root / "analysis" / "speditionsanalyse_latest.md"
        json_path = case_root / "analysis" / "case_report_latest.json"
        md_path = case_root / "analysis" / "case_report_latest.md"
        self._write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        legacy_json_path.unlink(missing_ok=True)
        legacy_md_path.unlink(missing_ok=True)
        return json_path, md_path

    def list_orders(self) -> list[str]:
        if not self.orders_root.exists():
            return []
        return sorted(p.name for p in self.orders_root.iterdir() if p.is_dir())

    def _message_file_name(self, prefix: str, message: IncomingMessagePayload) -> str:
        stamp = (message.received_at or utc_now_iso()).replace(":", "").replace("-", "")
        ident = (message.message_id or "message").replace("<", "").replace(">", "").replace("/", "_")
        return f"{prefix}_{stamp}_{ident[:80]}"

    def _ensure_json(self, path: Path, default: dict[str, Any]) -> None:
        if not path.exists():
            self._write_json(path, default)

    def _ensure_text(self, path: Path, default: str) -> None:
        if not path.exists():
            path.write_text(default, encoding="utf-8")

    def _update_global_tms_writeback_queue(self, *, order_id: str, pending_plan: dict[str, Any], pending_updates_path: Path) -> Path:
        queue_path = self.root / "tms_writeback_queue.json"
        existing = self._read_json(queue_path)
        pending_actions = [row for row in (pending_plan.get("pending_actions") or []) if isinstance(row, dict)]
        existing_orders = [row for row in (existing.get("orders") or []) if isinstance(row, dict) and row.get("order_id") != order_id]

        if pending_actions:
            existing_orders.append({
                "order_id": order_id,
                "shipment_uuid": pending_plan.get("shipment_uuid"),
                "shipment_number": pending_plan.get("shipment_number") or order_id,
                "status": pending_plan.get("status") or "pending_write_access",
                "requires_write_access": bool(pending_plan.get("requires_write_access", True)),
                "generated_at": pending_plan.get("generated_at") or utc_now_iso(),
                "pending_updates_path": str(pending_updates_path),
                "action_summary": dict(pending_plan.get("action_summary") or {}),
                "pending_actions": pending_actions,
            })

        existing_orders = sorted(existing_orders, key=lambda row: str(row.get("order_id") or ""))
        flattened_actions = [
            {
                "order_id": row.get("order_id"),
                "shipment_uuid": row.get("shipment_uuid"),
                "shipment_number": row.get("shipment_number"),
                **action,
            }
            for row in existing_orders
            for action in (row.get("pending_actions") or [])
            if isinstance(action, dict)
        ]

        payload = {
            "version": 1,
            "generated_at": utc_now_iso(),
            "orders": existing_orders,
            "summary": {
                "pending_orders": len(existing_orders),
                "pending_actions": len(flattened_actions),
                "write_now": sum(1 for row in flattened_actions if row.get("action_status") == "write_now"),
                "review": sum(1 for row in flattened_actions if row.get("action_status") == "review"),
                "not_yet_due": sum(1 for row in flattened_actions if row.get("action_status") == "not_yet_due"),
                "not_yet_knowable": sum(1 for row in flattened_actions if row.get("action_status") == "not_yet_knowable"),
            },
            "pending_actions": flattened_actions,
        }
        self._write_json(queue_path, payload)
        return queue_path

    def _update_global_review_queue(self, *, order_id: str, pending_plan: dict[str, Any], pending_updates_path: Path) -> Path:
        queue_path = self.root / "review_queue.json"
        existing = self._read_json(queue_path)

        def _review_priority(action: dict[str, Any]) -> str:
            action_type = str(action.get("action_type") or "").strip().lower()
            target = str(action.get("target") or "").strip().lower()
            if action_type == "document_gap":
                if target.endswith(("bill_of_lading", "air_waybill", "proof_of_delivery")):
                    return "high"
                if target.endswith(("commercial_invoice", "packing_list", "customs_document")):
                    return "medium"
                return "medium"
            priority = str(action.get("priority") or "").strip().lower()
            if priority in {"high", "urgent"}:
                return "high"
            if priority == "medium":
                return "medium"
            return "low"

        def _review_sort_key(action: dict[str, Any]) -> tuple[int, str, str]:
            priority_rank = {"high": 0, "medium": 1, "low": 2}
            derived_priority = _review_priority(action)
            topic = str(action.get("review_topic") or "")
            target = str(action.get("target") or "")
            return (priority_rank.get(derived_priority, 9), topic, target)

        review_actions = sorted([
            row for row in (pending_plan.get("pending_actions") or [])
            if isinstance(row, dict) and str(row.get("action_status") or "") == "review"
        ], key=_review_sort_key)
        existing_orders = [row for row in (existing.get("orders") or []) if isinstance(row, dict) and row.get("order_id") != order_id]

        if review_actions:
            highest_priority = _review_priority(review_actions[0])
            existing_orders.append({
                "order_id": order_id,
                "shipment_uuid": pending_plan.get("shipment_uuid"),
                "shipment_number": pending_plan.get("shipment_number") or order_id,
                "generated_at": pending_plan.get("generated_at") or utc_now_iso(),
                "pending_updates_path": str(pending_updates_path),
                "highest_priority": highest_priority,
                "review_actions": review_actions,
            })

        existing_orders = sorted(existing_orders, key=lambda row: (str(row.get("highest_priority") or "z"), str(row.get("order_id") or "")))
        flattened_actions = sorted([
            {
                "order_id": row.get("order_id"),
                "shipment_uuid": row.get("shipment_uuid"),
                "shipment_number": row.get("shipment_number"),
                **action,
            }
            for row in existing_orders
            for action in (row.get("review_actions") or [])
            if isinstance(action, dict)
        ], key=_review_sort_key)
        payload = {
            "version": 1,
            "generated_at": utc_now_iso(),
            "orders": existing_orders,
            "summary": {
                "review_orders": len(existing_orders),
                "review_actions": len(flattened_actions),
                "high_priority": sum(1 for row in flattened_actions if _review_priority(row) == "high"),
                "medium_priority": sum(1 for row in flattened_actions if _review_priority(row) == "medium"),
                "low_priority": sum(1 for row in flattened_actions if _review_priority(row) == "low"),
            },
            "review_actions": flattened_actions,
        }
        self._write_json(queue_path, payload)
        return queue_path

    def _read_json(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _append_jsonl(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
