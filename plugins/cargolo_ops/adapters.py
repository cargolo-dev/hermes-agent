from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests

from .models import utc_now_iso


class MockTMSAdapter:
    """Safe mock adapter with a production-shaped interface.

    The first MVP stores mock orders and tasks locally. It never mutates or deletes
    external systems, which keeps development safe while preserving the contract
    expected by future real adapters.
    """

    def __init__(self, root: Path):
        self.root = root / "mock_tms"
        self.root.mkdir(parents=True, exist_ok=True)
        self.orders_path = self.root / "orders.json"
        self.tasks_path = self.root / "tasks.json"
        if not self.orders_path.exists():
            self.orders_path.write_text("{}", encoding="utf-8")
        if not self.tasks_path.exists():
            self.tasks_path.write_text("{}", encoding="utf-8")

    def _read(self, path: Path) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    def _write(self, path: Path, payload: dict[str, Any]) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def get_order(self, order_id: str) -> dict[str, Any] | None:
        return self._read(self.orders_path).get(order_id)

    def search_order_by_reference(self, reference: str) -> dict[str, Any] | None:
        reference = reference.upper()
        return self.get_order(reference)

    def create_task(self, order_id: str, title: str, description: str, priority: str, due_at: str | None, task_type: str) -> dict[str, Any]:
        tasks = self._read(self.tasks_path)
        order_tasks = tasks.setdefault(order_id, [])
        task_id = f"mock-task-{len(order_tasks) + 1}"
        task = {
            "task_id": task_id,
            "order_id": order_id,
            "title": title,
            "description": description,
            "priority": priority,
            "due_at": due_at,
            "task_type": task_type,
            "status": "open",
            "created_at": utc_now_iso(),
        }
        order_tasks.append(task)
        self._write(self.tasks_path, tasks)
        return task

    def add_internal_note(self, order_id: str, note: str) -> dict[str, Any]:
        orders = self._read(self.orders_path)
        order = orders.setdefault(order_id, {"order_id": order_id, "notes": [], "status": "new"})
        order.setdefault("notes", []).append({"timestamp": utc_now_iso(), "note": note})
        self._write(self.orders_path, orders)
        return order

    def list_open_tasks(self, order_id: str) -> list[dict[str, Any]]:
        tasks = self._read(self.tasks_path).get(order_id, [])
        return [task for task in tasks if task.get("status") != "done"]

    def get_customer_rules(self, customer_id_or_name: str | None) -> dict[str, Any]:
        return {
            "customer": customer_id_or_name or "unknown",
            "auto_send_allowed": False,
            "price_release_allowed": False,
            "requires_human_review_for": ["complaint", "delay_or_exception", "customs_or_compliance"],
        }

    def snapshot_bundle(self, order_id: str, customer_hint: str | None = None) -> dict[str, Any]:
        order = self.search_order_by_reference(order_id) or self.get_order(order_id) or {"order_id": order_id, "status": "unknown"}
        return {
            "order_id": order.get("order_id", order_id),
            "status": order.get("status", "unknown"),
            "order": order,
            "open_tasks": self.list_open_tasks(order_id),
            "customer_rules": self.get_customer_rules(customer_hint),
            "fetched_at": utc_now_iso(),
        }


class N8NMailHistoryClient:
    def __init__(self, url: str, timeout: int = 45, auth_token: str | None = None):
        self.url = url
        self.timeout = timeout
        self.auth_token = auth_token

    def fetch_history(
        self,
        an: str,
        *,
        first_sync: bool,
        since: str | None,
        mailbox: str = "asr@cargolo.com",
        include_attachments: bool = True,
        include_html: bool = False,
    ) -> dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        response = requests.post(
            self.url,
            headers=headers,
            json={
                "an": an,
                "first_sync": first_sync,
                "since": since,
                "mailbox": mailbox,
                "include_attachments": include_attachments,
                "include_html": include_html,
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()


def build_mail_history_client_from_env() -> N8NMailHistoryClient | None:
    url = os.getenv("HERMES_CARGOLO_ASR_MAIL_HISTORY_URL", "").strip()
    if not url:
        return None
    token = os.getenv("HERMES_CARGOLO_ASR_MAIL_HISTORY_TOKEN", "").strip() or None
    timeout_raw = os.getenv("HERMES_CARGOLO_ASR_MAIL_HISTORY_TIMEOUT", "45").strip()
    timeout = int(timeout_raw) if timeout_raw.isdigit() else 45
    return N8NMailHistoryClient(url=url, timeout=timeout, auth_token=token)
