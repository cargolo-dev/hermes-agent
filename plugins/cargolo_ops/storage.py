from __future__ import annotations

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
            "tasks",
            "audit",
            "tms",
            "logs",
        ]:
            (case_root / rel).mkdir(parents=True, exist_ok=True)
        self._ensure_json(case_root / "case_state.json", CaseState(order_id=order_id).model_dump())
        self._ensure_json(case_root / "entities.json", EntitiesSnapshot().model_dump())
        self._ensure_json(case_root / "tms_snapshot.json", {})
        self._ensure_text(case_root / "timeline.md", f"# Timeline {order_id}\n")
        self._ensure_text(case_root / "email_index.jsonl", "")
        self._ensure_text(case_root / "tasks/task_log.jsonl", "")
        self._ensure_text(case_root / "audit/actions.jsonl", "")
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
        path = self.ensure_case(order_id) / "documents/inbound" / safe_name
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
