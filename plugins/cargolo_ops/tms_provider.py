from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any, Protocol

import yaml

from .adapters import CargoloTMSClient, build_tms_client_from_env
from .models import TMSSnapshot, utc_now_iso

DEFAULT_MCP_PYTHON = "/root/hermescargobot/.venv/bin/python"
DEFAULT_MCP_PACKAGE_ROOT = "/root/hermescargobot"
DEFAULT_MCP_CALL_TIMEOUT = 120
DEFAULT_HERMES_CONFIG_PATH = Path("/root/.hermes/config.yaml")


def _load_cargolo_tms_mcp_defaults() -> dict[str, str]:
    try:
        if not DEFAULT_HERMES_CONFIG_PATH.exists():
            return {}
        payload = yaml.safe_load(DEFAULT_HERMES_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}

    if not isinstance(payload, dict):
        return {}
    mcp_servers = payload.get("mcp_servers")
    if not isinstance(mcp_servers, dict):
        return {}
    cargolo_cfg = mcp_servers.get("cargolo_tms")
    if not isinstance(cargolo_cfg, dict):
        return {}

    defaults: dict[str, str] = {}
    command = str(cargolo_cfg.get("command") or "").strip()
    args = cargolo_cfg.get("args")
    env = cargolo_cfg.get("env") if isinstance(cargolo_cfg.get("env"), dict) else {}
    if command:
        defaults["CARGOLO_TMS_MCP_PYTHON"] = command
    if isinstance(args, list) and len(args) >= 2 and args[0] == "-m":
        module_name = str(args[1] or "").strip()
        if module_name == "cargolo_tms_mcp.server":
            pythonpath = str(env.get("PYTHONPATH") or "").strip()
            if pythonpath:
                defaults["CARGOLO_TMS_MCP_PACKAGE_ROOT"] = pythonpath.split(os.pathsep)[0]
    for key in (
        "CARGOLO_TMS_MCP_BACKEND",
        "CARGOLO_TMS_MCP_URL",
        "CARGOLO_TMS_API_URL",
        "CARGOLO_TMS_EMAIL",
        "CARGOLO_TMS_PASSWORD",
        "CARGOLO_TMS_TIMEOUT",
    ):
        value = str(env.get(key) or "").strip()
        if value:
            defaults[key] = value
    return defaults


class TMSReadProvider(Protocol):
    def snapshot_bundle(self, an: str, customer_hint: str | None = None) -> TMSSnapshot: ...
    def shipments_list(self, transport_category: str = "asr", **kwargs: Any) -> list[dict[str, Any]]: ...
    def document_requirements(self, an: str) -> dict[str, Any]: ...
    def billing_context(self, an: str) -> dict[str, Any]: ...


class TMSWriteProvider(Protocol):
    def update_shipment(self, **kwargs: Any) -> dict[str, Any]: ...
    def update_transport_leg(self, **kwargs: Any) -> dict[str, Any]: ...
    def update_shipment_address(self, **kwargs: Any) -> dict[str, Any]: ...
    def update_cargo_item(self, **kwargs: Any) -> dict[str, Any]: ...
    def upload_document(self, **kwargs: Any) -> dict[str, Any]: ...
    def create_todo(self, **kwargs: Any) -> dict[str, Any]: ...
    def add_internal_note(self, **kwargs: Any) -> dict[str, Any]: ...
    def set_shipment_status(self, **kwargs: Any) -> dict[str, Any]: ...


class DirectTMSProvider:
    def __init__(self, client: CargoloTMSClient):
        self.client = client

    def snapshot_bundle(self, an: str, customer_hint: str | None = None) -> TMSSnapshot:
        return self.client.snapshot_bundle(an, customer_hint)

    def shipments_list(self, transport_category: str = "asr", **kwargs: Any) -> list[dict[str, Any]]:
        return self.client.shipments_list(transport_category=transport_category, **kwargs)

    def document_requirements(self, an: str) -> dict[str, Any]:
        snapshot = self.client.snapshot_bundle(an)
        documents = snapshot.detail.get("documents", []) if isinstance(snapshot.detail, dict) else []
        expected_types = [
            str(row.get("document_type") or "")
            for row in documents
            if isinstance(row, dict) and row.get("required") and row.get("document_type")
        ]
        if not expected_types:
            expected_types = [
                str(row.get("document_type") or "")
                for row in documents
                if isinstance(row, dict) and row.get("document_type")
            ]
        return {
            "status": "ok",
            "query": {"an": an},
            "shipment": {
                "shipment_uuid": snapshot.shipment_uuid,
                "shipment_number": snapshot.shipment_number or an,
            },
            "documents": documents if isinstance(documents, list) else [],
            "expected_types": [value for value in expected_types if value],
            "warnings": list(snapshot.warnings or []),
            "source": "direct_tms",
        }

    def billing_context(self, an: str) -> dict[str, Any]:
        snapshot = self.client.snapshot_bundle(an)
        return {
            "status": "ok",
            "query": {"an": an},
            "shipment": {
                "shipment_uuid": snapshot.shipment_uuid,
                "shipment_number": snapshot.shipment_number or an,
            },
            "billing": {
                "items": list(snapshot.billing_items or []),
                "sums": dict(snapshot.billing_sums or {}),
            },
            "warnings": list(snapshot.warnings or []),
            "source": "direct_tms",
        }


class MCPBridgeTMSProvider:
    def __init__(
        self,
        *,
        python_bin: str = DEFAULT_MCP_PYTHON,
        package_root: str = DEFAULT_MCP_PACKAGE_ROOT,
        timeout: int = DEFAULT_MCP_CALL_TIMEOUT,
        env_defaults: dict[str, str] | None = None,
    ):
        self.python_bin = python_bin
        self.package_root = package_root
        self.timeout = timeout
        self.env_defaults = dict(env_defaults or {})

    def _env(self) -> dict[str, str]:
        env = os.environ.copy()
        for key, value in self.env_defaults.items():
            env.setdefault(key, value)
        existing_pythonpath = env.get("PYTHONPATH", "").strip()
        pythonpath_parts = [self.package_root]
        if existing_pythonpath:
            pythonpath_parts.append(existing_pythonpath)
        env["PYTHONPATH"] = os.pathsep.join(part for part in pythonpath_parts if part)
        env.setdefault("CARGOLO_TMS_MCP_BACKEND", "xano_mcp")
        return env

    def _call_backend(self, method_name: str, **kwargs: Any) -> dict[str, Any]:
        helper = (
            "import json\n"
            "from cargolo_tms_mcp.backend import build_backend\n"
            f"payload = build_backend().{method_name}(**json.loads({json.dumps(json.dumps(kwargs))}))\n"
            "print(json.dumps(payload, ensure_ascii=False))\n"
        )
        result = subprocess.run(
            [self.python_bin, "-c", helper],
            capture_output=True,
            text=True,
            timeout=self.timeout,
            check=False,
            env=self._env(),
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            raise RuntimeError(
                f"MCP TMS backend call failed for {method_name} (exit={result.returncode}): "
                f"stdout={stdout or '-'} stderr={stderr or '-'}"
            )
        raw = (result.stdout or "").strip()
        if not raw:
            raise RuntimeError(f"MCP TMS backend call returned no output for {method_name}")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise RuntimeError(f"MCP TMS backend returned unexpected payload type for {method_name}: {type(payload).__name__}")
        return payload

    def _call_remote_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        helper = (
            "import json\n"
            "from cargolo_tms_mcp.backend import build_backend\n"
            "backend = build_backend()\n"
            f"payload = backend._call_remote_tool({json.dumps('TOOL_NAME_PLACEHOLDER')}, json.loads({json.dumps(json.dumps({'ARGS_PLACEHOLDER': True}))}))\n"
            "print(json.dumps(payload, ensure_ascii=False))\n"
        )
        helper = helper.replace(json.dumps('TOOL_NAME_PLACEHOLDER'), json.dumps(tool_name))
        helper = helper.replace(json.dumps(json.dumps({'ARGS_PLACEHOLDER': True})), json.dumps(json.dumps(arguments)))
        result = subprocess.run(
            [self.python_bin, "-c", helper],
            capture_output=True,
            text=True,
            timeout=self.timeout,
            check=False,
            env=self._env(),
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            raise RuntimeError(
                f"MCP TMS remote tool call failed for {tool_name} (exit={result.returncode}): "
                f"stdout={stdout or '-'} stderr={stderr or '-'}"
            )
        raw = (result.stdout or "").strip()
        if not raw:
            raise RuntimeError(f"MCP TMS remote tool call returned no output for {tool_name}")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise RuntimeError(f"MCP TMS remote tool returned unexpected payload type for {tool_name}: {type(payload).__name__}")
        return payload

    def _customer_rules(self, customer_hint: str | None) -> dict[str, Any]:
        return {
            "customer": customer_hint or "unknown",
            "auto_send_allowed": False,
            "price_release_allowed": False,
            "requires_human_review_for": ["complaint", "delay_or_exception", "customs_or_compliance"],
        }

    def _normalize_snapshot(self, an: str, payload: dict[str, Any], customer_hint: str | None = None) -> TMSSnapshot:
        shipment = payload.get("shipment") if isinstance(payload.get("shipment"), dict) else {}
        billing = payload.get("billing") if isinstance(payload.get("billing"), dict) else {}
        stats = payload.get("stats") if isinstance(payload.get("stats"), dict) else {}
        warnings = payload.get("warnings") if isinstance(payload.get("warnings"), list) else []
        if payload.get("code") and payload.get("message"):
            warnings = [*warnings, f"{payload.get('code')}: {payload.get('message')}"]
        payload_status = str(payload.get("status") or "")
        effective_status = str(shipment.get("status") or payload_status or ("error" if warnings else "unknown"))
        detail = {
            **shipment,
            "documents": shipment.get("documents", []),
        }
        return TMSSnapshot(
            order_id=an,
            shipment_uuid=shipment.get("shipment_uuid"),
            shipment_number=str(shipment.get("shipment_number") or an),
            source="live",
            status=effective_status,
            detail=detail,
            billing_items=billing.get("items", []) if isinstance(billing.get("items"), list) else [],
            billing_sums=billing.get("sums", {}) if isinstance(billing.get("sums"), dict) else {},
            stats=stats,
            customer_rules=self._customer_rules(customer_hint),
            open_tasks=[],
            fetched_at=utc_now_iso(),
            warnings=[str(item) for item in warnings if item],
            provider="mcp_bridge",
            readonly=bool(payload.get("readonly", True)),
            source_endpoints=payload.get("source_endpoints", []),
            query=payload.get("query", {}),
            remote_code=payload.get("code"),
            remote_message=payload.get("message"),
            remote_payload=payload.get("payload") if isinstance(payload.get("payload"), dict) else {},
        )

    def snapshot_bundle(self, an: str, customer_hint: str | None = None) -> TMSSnapshot:
        payload = self._call_backend(
            "get_asr_shipment_snapshot",
            an=an,
            include_stats=True,
            include_billing=True,
            include_raw=False,
        )
        return self._normalize_snapshot(an, payload, customer_hint)

    def shipments_list(self, transport_category: str = "asr", **kwargs: Any) -> list[dict[str, Any]]:
        if transport_category != "asr":
            raise ValueError("MCPBridgeTMSProvider currently supports only transport_category='asr'")
        payload = self._call_backend(
            "list_asr_shipments",
            page=int(kwargs.get("page", 1) or 1),
            per_page=int(kwargs.get("limit", kwargs.get("per_page", 100)) or 100),
            status_filter=str(kwargs.get("status_filter", "") or ""),
            network_filter=str(kwargs.get("network_filter", "") or ""),
            search=str(kwargs.get("shipment_number", kwargs.get("search", "")) or ""),
        )
        shipments = payload.get("shipments")
        return shipments if isinstance(shipments, list) else []

    def document_requirements(self, an: str) -> dict[str, Any]:
        return self._call_backend("get_document_requirements", an=an)

    def billing_context(self, an: str) -> dict[str, Any]:
        return self._call_backend("get_billing_context", an=an)

    def update_shipment(self, **kwargs: Any) -> dict[str, Any]:
        return self._call_backend("update_shipment", **kwargs)

    def update_transport_leg(self, **kwargs: Any) -> dict[str, Any]:
        return self._call_remote_tool("cargolo_tms_update_transport_leg", kwargs)

    def update_shipment_address(self, **kwargs: Any) -> dict[str, Any]:
        return self._call_remote_tool("cargolo_tms_update_shipment_address", kwargs)

    def update_cargo_item(self, **kwargs: Any) -> dict[str, Any]:
        return self._call_remote_tool("cargolo_tms_update_cargo_item", kwargs)

    def upload_document(self, **kwargs: Any) -> dict[str, Any]:
        return self._call_backend("upload_document", **kwargs)

    def create_todo(self, **kwargs: Any) -> dict[str, Any]:
        return self._call_backend("create_todo", **kwargs)

    def add_internal_note(self, **kwargs: Any) -> dict[str, Any]:
        return self._call_backend("add_internal_note", **kwargs)

    def set_shipment_status(self, **kwargs: Any) -> dict[str, Any]:
        return self._call_remote_tool("cargolo_tms_set_shipment_status", kwargs)


def build_tms_provider_from_env() -> TMSReadProvider | None:
    config_defaults = _load_cargolo_tms_mcp_defaults()
    backend_mode = (os.getenv("CARGOLO_TMS_MCP_BACKEND", config_defaults.get("CARGOLO_TMS_MCP_BACKEND", "")) or "").strip().lower()
    mcp_url = (os.getenv("CARGOLO_TMS_MCP_URL", config_defaults.get("CARGOLO_TMS_MCP_URL", "")) or "").strip()
    python_bin = (os.getenv("CARGOLO_TMS_MCP_PYTHON", config_defaults.get("CARGOLO_TMS_MCP_PYTHON", DEFAULT_MCP_PYTHON)) or DEFAULT_MCP_PYTHON).strip()
    package_root = (os.getenv("CARGOLO_TMS_MCP_PACKAGE_ROOT", config_defaults.get("CARGOLO_TMS_MCP_PACKAGE_ROOT", DEFAULT_MCP_PACKAGE_ROOT)) or DEFAULT_MCP_PACKAGE_ROOT).strip()
    timeout_raw = (os.getenv("CARGOLO_TMS_MCP_CALL_TIMEOUT", str(DEFAULT_MCP_CALL_TIMEOUT)) or str(DEFAULT_MCP_CALL_TIMEOUT)).strip()
    timeout = int(timeout_raw) if timeout_raw.isdigit() else DEFAULT_MCP_CALL_TIMEOUT

    mcp_available = Path(python_bin).exists() and Path(package_root).exists()
    prefer_direct = backend_mode in {"direct", "legacy_direct", "xano_direct"}
    prefer_mcp = not prefer_direct and (mcp_available or bool(mcp_url) or backend_mode == "xano_mcp")
    if prefer_mcp and mcp_available:
        return MCPBridgeTMSProvider(
            python_bin=python_bin,
            package_root=package_root,
            timeout=timeout,
            env_defaults=config_defaults,
        )

    client = build_tms_client_from_env()
    if client is not None:
        return DirectTMSProvider(client)
    return None


def build_tms_write_provider_from_env() -> TMSWriteProvider | None:
    provider = build_tms_provider_from_env()
    if isinstance(provider, MCPBridgeTMSProvider):
        return provider
    return None
