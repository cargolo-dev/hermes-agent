from __future__ import annotations

import base64
import io
import json
import logging
import os
import time
import zipfile
from pathlib import Path
from typing import Any

import requests

from .models import (
    TMSSnapshot,
    utc_now_iso,
)

logger = logging.getLogger(__name__)


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


class CargoloTMSClient:
    """Read-only client for the CARGOLO TMS API (Xano backend).

    Authenticates via email/password, caches the bearer token (24h expiry,
    refreshed after ~23h), and provides read methods for all ASR-relevant
    endpoints.  Field names and response shapes match the real Xano API
    exactly (verified against the .xs source files).

    Credentials are read from environment variables:
      - CARGOLO_TMS_API_URL   (base URL, default https://api.cargolo.de)
      - CARGOLO_TMS_EMAIL     (login email — @cargolo.com bypasses rate limits)
      - CARGOLO_TMS_PASSWORD
      - CARGOLO_TMS_TIMEOUT   (seconds, default 30)
    """

    TOKEN_TTL = 82800  # ~23h — token expires after 86400s (24h)

    def __init__(
        self,
        api_url: str,
        email: str,
        password: str,
        timeout: int = 30,
    ):
        self.api_url = api_url.rstrip("/")
        self.email = email
        self.password = password
        self.timeout = timeout
        self._token: str | None = None
        self._token_acquired_at: float = 0.0

    # ------------------------------------------------------------------
    # Auth — POST /api:auth/auth/login
    # Input:  { email, password }
    # Output: { authToken, user: {...}, onboardingRequired, onboardingStep }
    # Note:   SKILL.md documents the response as an array; the actual Xano
    #         endpoint returns an object.  We handle both shapes defensively.
    # ------------------------------------------------------------------

    def _login(self) -> str:
        """Authenticate and return a bearer token."""
        resp = requests.post(
            f"{self.api_url}/api:auth/auth/login",
            json={"email": self.email, "password": self.password},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        # Handle both object and array response shapes
        if isinstance(data, list) and data:
            token = data[0].get("authToken")
        elif isinstance(data, dict):
            token = data.get("authToken")
        else:
            raise ValueError(f"Unexpected TMS login response shape: {type(data)}")
        if not token:
            raise ValueError("TMS login succeeded but no authToken found in response")
        self._token = token
        self._token_acquired_at = time.monotonic()
        logger.info("TMS login successful for %s", self.email)
        return token

    def _get_token(self) -> str:
        """Return a cached token or log in fresh."""
        if self._token and (time.monotonic() - self._token_acquired_at) < self.TOKEN_TTL:
            return self._token
        return self._login()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        """Issue an authenticated GET request."""
        resp = requests.get(
            f"{self.api_url}{path}",
            headers=self._headers(),
            params=params or {},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: dict[str, Any] | None = None) -> Any:
        """Issue an authenticated POST request."""
        resp = requests.post(
            f"{self.api_url}{path}",
            headers=self._headers(),
            json=body or {},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Read endpoints — shapes verified against Xano .xs sources
    # ------------------------------------------------------------------

    def shipments_list(
        self,
        transport_category: str = "asr",
        *,
        shipment_number: str | None = None,
        page: int = 1,
        limit: int = 100,
        **extra_params: str,
    ) -> list[dict[str, Any]]:
        """GET /admin/shipments_list → { items, total, page, pageSize, totalPages }.

        Returns the ``items`` array.  Supports direct ``shipment_number`` filter.
        """
        params: dict[str, str] = {
            "transport_category": transport_category,
            "page": str(page),
            "limit": str(limit),
            **extra_params,
        }
        if shipment_number:
            params["shipment_number"] = shipment_number
        data = self._get("/api:XCxYMj7t/shipments_list", params)
        if isinstance(data, dict):
            return data.get("items", [])
        return data if isinstance(data, list) else []

    def shipment_detail(
        self,
        *,
        shipment_uuid: str | None = None,
        shipment_id: int | None = None,
    ) -> dict[str, Any]:
        """GET /admin/shipment_detail → flat object with nested sender/recipient/cargo/…"""
        params: dict[str, str] = {}
        if shipment_uuid:
            params["shipment_uuid"] = shipment_uuid
        elif shipment_id is not None:
            params["shipment_id"] = str(shipment_id)
        else:
            raise ValueError("Provide shipment_uuid or shipment_id")
        return self._get("/api:XCxYMj7t/shipment_detail", params)

    def shipments_stats(self, transport_category: str = "asr") -> dict[str, Any]:
        """GET /admin/shipments_stats → { total, by_status, by_transport_mode, today, … }"""
        return self._get("/api:XCxYMj7t/shipments_stats", {"transport_category": transport_category})

    def shipment_billing_items(self, shipment_uuid: str) -> dict[str, Any]:
        """GET /admin/shipment_billing_items → { items: [...], sums: { total_vk, total_ek, margin, margin_percent } }"""
        return self._get("/api:XCxYMj7t/shipment_billing_items", {"shipment_uuid": shipment_uuid})

    # ------------------------------------------------------------------
    # Todo/Aufgaben endpoints — POST /admin/todos/create, GET /admin/todos/list
    # Verified against 2097_todos_create_POST.xs and 2096_todos_list_GET.xs
    # ------------------------------------------------------------------

    VALID_PRIORITIES = ("low", "medium", "high", "urgent")
    VALID_CATEGORIES = ("dokumente", "zoll", "abholung", "zustellung", "kommunikation", "rechnung", "sonstiges")

    def create_todo(
        self,
        *,
        title: str,
        related_id: str,
        description: str | None = None,
        priority: str = "medium",
        category: str = "sonstiges",
        due_date: str | None = None,
        assigned_to_user_id: int | None = None,
    ) -> dict[str, Any]:
        """POST /admin/todos/create — create a shipment-level task.

        Args:
            title: Task title (required).
            related_id: Shipment UUID (required) — the task is linked to this shipment.
            description: Optional description.
            priority: low | medium | high | urgent (default medium).
            category: dokumente | zoll | abholung | zustellung | kommunikation | rechnung | sonstiges.
            due_date: ISO timestamp for due date.
            assigned_to_user_id: Admin user ID to assign to (optional).

        Returns:
            { success: true, message: "...", todo: {...} }
        """
        body: dict[str, Any] = {
            "title": title,
            "related_id": related_id,
            "priority": priority,
            "category": category,
        }
        if description:
            body["description"] = description
        if due_date:
            body["due_date"] = due_date
        if assigned_to_user_id is not None:
            body["assigned_to_user_id"] = assigned_to_user_id
        return self._post("/api:XCxYMj7t/todos/create", body)

    def list_todos(
        self,
        *,
        related_id: str | None = None,
        status: str | None = None,
        priority: str | None = None,
        category: str | None = None,
        is_overdue: bool | None = None,
        page: int = 1,
        per_page: int = 50,
    ) -> dict[str, Any]:
        """GET /admin/todos/list → { items, itemsTotal, curPage, ... }"""
        params: dict[str, str] = {"page": str(page), "per_page": str(per_page)}
        if related_id:
            params["related_id"] = related_id
        if status:
            params["status"] = status
        if priority:
            params["priority"] = priority
        if category:
            params["category"] = category
        if is_overdue:
            params["is_overdue"] = "true"
        return self._get("/api:XCxYMj7t/todos/list", params)

    def list_open_todos_for_shipment(self, shipment_uuid: str) -> list[dict[str, Any]]:
        """Convenience: list non-completed todos for a specific shipment."""
        result = self.list_todos(related_id=shipment_uuid, status="pending", per_page=100)
        pending = result.get("items", []) if isinstance(result, dict) else []
        result2 = self.list_todos(related_id=shipment_uuid, status="in_progress", per_page=100)
        in_progress = result2.get("items", []) if isinstance(result2, dict) else []
        return pending + in_progress

    # ------------------------------------------------------------------
    # Convenience: find shipment by AN (shipment_number)
    # ------------------------------------------------------------------

    def find_shipment_by_an(self, an: str) -> dict[str, Any] | None:
        """Search via the ``shipment_number`` filter on shipments_list."""
        try:
            # Use the direct shipment_number filter — no need to fetch all
            rows = self.shipments_list(
                transport_category="asr",
                shipment_number=an.upper(),
            )
        except Exception:
            logger.exception("TMS shipments_list failed for AN=%s", an)
            return None
        an_upper = an.upper()
        for row in rows:
            if str(row.get("shipment_number", "")).upper() == an_upper:
                return row
        return None

    # ------------------------------------------------------------------
    # Full snapshot bundle
    # ------------------------------------------------------------------

    def snapshot_bundle(self, an: str, customer_hint: str | None = None) -> TMSSnapshot:
        """Fetch all read endpoints for a shipment and return a structured snapshot."""
        summary = self.find_shipment_by_an(an)
        if not summary:
            logger.warning("TMS: no shipment found for AN=%s", an)
            return TMSSnapshot(
                order_id=an,
                shipment_number=an,
                source="live",
                status="not_found",
                fetched_at=utc_now_iso(),
                warnings=[f"No shipment found in TMS for {an}"],
                customer_rules=_default_customer_rules(customer_hint),
            )

        # The UUID is returned as "id" in the list response
        shipment_uuid = summary.get("id", "")
        warnings: list[str] = []

        # Detail
        detail: dict[str, Any] = {}
        try:
            detail = self.shipment_detail(shipment_uuid=shipment_uuid)
        except Exception as exc:
            logger.warning("TMS shipment_detail failed: %s", exc)
            warnings.append(f"shipment_detail failed: {exc}")

        # Billing — returns { items, sums }
        billing_raw: dict[str, Any] = {}
        try:
            billing_raw = self.shipment_billing_items(shipment_uuid)
        except Exception as exc:
            logger.warning("TMS billing_items failed: %s", exc)
            warnings.append(f"billing_items failed: {exc}")

        # Stats remain useful as lightweight context.
        stats: dict[str, Any] = {}
        try:
            stats = self.shipments_stats("asr")
        except Exception as exc:
            logger.warning("TMS shipments_stats failed: %s", exc)
            warnings.append(f"shipments_stats failed: {exc}")

        return TMSSnapshot(
            order_id=an,
            shipment_uuid=shipment_uuid,
            shipment_number=summary.get("shipment_number", an),
            source="live",
            status=detail.get("status") or summary.get("status", "unknown"),
            detail=detail,
            billing_items=billing_raw.get("items", []) if isinstance(billing_raw, dict) else [],
            billing_sums=billing_raw.get("sums", {}) if isinstance(billing_raw, dict) else {},
            stats=stats,
            customer_rules=_default_customer_rules(customer_hint),
            open_tasks=[],
            fetched_at=utc_now_iso(),
            warnings=warnings,
        )



def _default_customer_rules(customer_hint: str | None) -> dict[str, Any]:
    return {
        "customer": customer_hint or "unknown",
        "auto_send_allowed": False,
        "price_release_allowed": False,
        "requires_human_review_for": ["complaint", "delay_or_exception", "customs_or_compliance"],
    }


def build_tms_client_from_env() -> CargoloTMSClient | None:
    """Create a TMS client from environment variables, or None if not configured.

    Required env vars:
      - CARGOLO_TMS_EMAIL    (or legacy CARGOLO_TMS_USERNAME)
      - CARGOLO_TMS_PASSWORD

    Optional:
      - CARGOLO_TMS_API_URL  (default https://api.cargolo.de)
      - CARGOLO_TMS_TIMEOUT  (default 30s)
    """
    api_url = os.getenv("CARGOLO_TMS_API_URL", "https://api.cargolo.de").strip()
    email = os.getenv("CARGOLO_TMS_EMAIL", "").strip() or os.getenv("CARGOLO_TMS_USERNAME", "").strip()
    password = os.getenv("CARGOLO_TMS_PASSWORD", "").strip()
    if not email or not password:
        return None
    timeout_raw = os.getenv("CARGOLO_TMS_TIMEOUT", "30").strip()
    timeout = int(timeout_raw) if timeout_raw.isdigit() else 30
    return CargoloTMSClient(api_url=api_url, email=email, password=password, timeout=timeout)


class N8NMailHistoryClient:
    def __init__(self, url: str, timeout: int = 45, auth_token: str | None = None):
        self.url = url
        self.timeout = timeout
        self.auth_token = auth_token

    @staticmethod
    def _looks_like_zip_response(response: requests.Response) -> bool:
        content_type = str(response.headers.get("Content-Type", "") or "").lower()
        if "application/zip" in content_type or "application/x-zip-compressed" in content_type:
            return True
        return response.content[:4] == b"PK\x03\x04"

    @staticmethod
    def _parse_zip_payload(raw_bytes: bytes) -> dict[str, Any]:
        def _build_attachment_from_file(entry: dict[str, Any]) -> dict[str, Any]:
            filename = str(entry.get("original_name") or entry.get("filename") or entry.get("zip_name") or entry.get("content_path") or "attachment.bin").strip()
            return {
                "filename": filename,
                "mime_type": str(entry.get("mime_type") or "application/octet-stream"),
                "size": entry.get("size"),
                "content_path": entry.get("content_path"),
                "binary_key": entry.get("binary_key"),
                "zip_name": entry.get("zip_name"),
            }

        def _find_zip_member(names: list[str], attachment: dict[str, Any], message: dict[str, Any], files_index: list[dict[str, Any]]) -> str | None:
            candidates = [
                attachment.get("content_path"),
                attachment.get("zip_path"),
                attachment.get("storage_path"),
                attachment.get("binary_key"),
                attachment.get("filename"),
            ]
            candidates = [str(value).strip() for value in candidates if str(value or "").strip()]
            message_id = str(message.get("message_id") or "").strip()
            filename = str(attachment.get("filename") or "").strip()
            if files_index:
                for entry in files_index:
                    if not isinstance(entry, dict):
                        continue
                    entry_message_id = str(entry.get("message_id") or "").strip()
                    entry_original_name = str(entry.get("original_name") or "").strip()
                    entry_content_path = str(entry.get("content_path") or "").strip()
                    if message_id and entry_message_id == message_id and filename and entry_original_name == filename and entry_content_path:
                        return entry_content_path
            for candidate in candidates:
                if candidate in names:
                    return candidate
            lower_name_map = {name.lower(): name for name in names}
            for candidate in candidates:
                direct = lower_name_map.get(candidate.lower())
                if direct:
                    return direct
            for candidate in candidates:
                candidate_base = Path(candidate).name.lower()
                for name in names:
                    name_base = Path(name).name.lower()
                    if name_base == candidate_base:
                        return name
                    if candidate_base and name_base.startswith(candidate_base):
                        return name
                    if candidate_base and name_base.endswith(candidate_base):
                        return name
            return None

        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            names = list(zf.namelist())
            manifest_name = "manifest.json" if "manifest.json" in names else next(
                (name for name in zf.namelist() if name.lower().endswith("manifest.json")),
                None,
            )
            if not manifest_name:
                raise ValueError("ZIP mail history payload is missing manifest.json")
            manifest = json.loads(zf.read(manifest_name).decode("utf-8"))
            messages = None
            if isinstance(manifest, dict):
                if isinstance(manifest.get("messages"), list):
                    messages = manifest.get("messages")
                elif isinstance(manifest.get("history"), list):
                    messages = manifest.get("history")
                    manifest["messages"] = messages
            if not isinstance(messages, list):
                raise ValueError("ZIP manifest.json must contain a messages/history array")
            files_index = manifest.get("files") if isinstance(manifest.get("files"), list) else []

            if files_index:
                files_by_message: dict[str, list[dict[str, Any]]] = {}
                for entry in files_index:
                    if not isinstance(entry, dict):
                        continue
                    message_id = str(entry.get("message_id") or "").strip()
                    if not message_id:
                        continue
                    files_by_message.setdefault(message_id, []).append(entry)
                for message in messages:
                    if not isinstance(message, dict):
                        continue
                    attachments = message.get("attachments")
                    if not isinstance(attachments, list):
                        attachments = []
                        message["attachments"] = attachments
                    if attachments:
                        continue
                    for entry in files_by_message.get(str(message.get("message_id") or "").strip(), []):
                        attachments.append(_build_attachment_from_file(entry))

            for message in messages:
                if not isinstance(message, dict):
                    continue
                attachments = message.get("attachments")
                if not isinstance(attachments, list):
                    continue
                for attachment in attachments:
                    if not isinstance(attachment, dict):
                        continue
                    if attachment.get("content_base64"):
                        continue
                    matched_name = _find_zip_member(names, attachment, message, files_index)
                    if not matched_name:
                        continue
                    attachment_bytes = zf.read(matched_name)
                    attachment["content_base64"] = base64.b64encode(attachment_bytes).decode("utf-8")
                    attachment.setdefault("size", len(attachment_bytes))
            return manifest

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
        if self._looks_like_zip_response(response):
            return self._parse_zip_payload(response.content)
        return response.json()


def build_mail_history_client_from_env() -> N8NMailHistoryClient | None:
    url = os.getenv("HERMES_CARGOLO_ASR_MAIL_HISTORY_URL", "").strip()
    if not url:
        return None
    token = os.getenv("HERMES_CARGOLO_ASR_MAIL_HISTORY_TOKEN", "").strip() or None
    timeout_raw = os.getenv("HERMES_CARGOLO_ASR_MAIL_HISTORY_TIMEOUT", "90").strip()
    timeout = int(timeout_raw) if timeout_raw.isdigit() else 90
    return N8NMailHistoryClient(url=url, timeout=timeout, auth_token=token)
