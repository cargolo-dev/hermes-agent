"""CARGOLO ASR operations tools.

Deterministic helpers for the Hermes webhook/agent flow:
- process normalized n8n email events into case folders
- query the n8n mail-history endpoint as an agent-usable tool
- generate a daily ops report from local case files
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hermes_constants import display_hermes_home, get_hermes_home
from plugins.cargolo_ops.adapters import build_mail_history_client_from_env
from plugins.cargolo_ops.processor import process_email_event
from plugins.cargolo_ops.reporting import generate_daily_report
from tools.registry import registry, tool_error


DEFAULT_ROOT = get_hermes_home() / "cargolo_asr"


def _resolve_root(path_value: str | None) -> Path:
    if not path_value:
        return DEFAULT_ROOT
    return Path(path_value).expanduser()


def _parse_payload(args: dict[str, Any]) -> dict[str, Any]:
    payload = args.get("payload")
    if isinstance(payload, dict):
        return payload
    payload_json = args.get("payload_json", "")
    if payload_json:
        return json.loads(payload_json)
    raise ValueError("Provide either 'payload' or 'payload_json'")


PROCESS_EVENT_SCHEMA = {
    "name": "cargolo_asr_process_event",
    "description": (
        "Process a normalized CARGOLO ASR email event into the local order-folder system. "
        "Use this for webhook payloads from n8n after HMAC validation. "
        f"Default storage root: {display_hermes_home()}/cargolo_asr"
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "payload": {
                "type": "object",
                "description": "Normalized email event payload from n8n/Hermes webhook. Prefer passing the parsed object directly.",
            },
            "payload_json": {
                "type": "string",
                "description": "Alternative to payload: raw JSON string for the event.",
            },
            "storage_root": {
                "type": "string",
                "description": "Optional override for the ASR case root directory.",
            },
            "create_task": {
                "type": "boolean",
                "description": "If true, create a task through the safe mock TMS adapter when rules recommend it. Default false.",
            },
            "refresh_history": {
                "type": "boolean",
                "description": "If true, call the configured n8n mail-history endpoint for first-sync/delta sync. Default true.",
            },
        },
        "required": [],
    },
}


MAIL_HISTORY_SCHEMA = {
    "name": "cargolo_asr_mail_history",
    "description": "Call the configured n8n ASR mail-history endpoint by AN and return the normalized response.",
    "parameters": {
        "type": "object",
        "properties": {
            "an": {"type": "string", "description": "Order/shipment number like AN-12345."},
            "first_sync": {"type": "boolean", "description": "True for initial full sync, false for delta sync."},
            "since": {"type": "string", "description": "Optional ISO timestamp for delta sync lower bound."},
            "mailbox": {"type": "string", "description": "Mailbox to query. Default asr@cargolo.com."},
            "include_attachments": {"type": "boolean", "description": "Whether attachments/base64 should be included. Default true."},
            "include_html": {"type": "boolean", "description": "Whether HTML bodies should be included. Default false."},
        },
        "required": ["an"],
    },
}


DAILY_REPORT_SCHEMA = {
    "name": "cargolo_asr_daily_report",
    "description": "Generate a daily operational report from local CARGOLO ASR case files.",
    "parameters": {
        "type": "object",
        "properties": {
            "storage_root": {
                "type": "string",
                "description": "Optional override for the ASR case root directory.",
            }
        },
        "required": [],
    },
}


def cargolo_asr_process_event_tool(args: dict[str, Any], **_: Any) -> str:
    try:
        payload = _parse_payload(args)
        root = _resolve_root(args.get("storage_root"))
        result = process_email_event(
            payload,
            storage_root=root,
            create_task=bool(args.get("create_task", False)),
            refresh_history=bool(args.get("refresh_history", True)),
        )
        return json.dumps(result.model_dump(), ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_mail_history_tool(args: dict[str, Any], **_: Any) -> str:
    client = build_mail_history_client_from_env()
    if client is None:
        return tool_error("HERMES_CARGOLO_ASR_MAIL_HISTORY_URL is not configured")
    try:
        result = client.fetch_history(
            args["an"],
            first_sync=bool(args.get("first_sync", False)),
            since=args.get("since"),
            mailbox=args.get("mailbox") or "asr@cargolo.com",
            include_attachments=bool(args.get("include_attachments", True)),
            include_html=bool(args.get("include_html", False)),
        )
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_daily_report_tool(args: dict[str, Any], **_: Any) -> str:
    try:
        root = _resolve_root(args.get("storage_root"))
        report = generate_daily_report(root)
        return json.dumps(report, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


registry.register(
    name="cargolo_asr_process_event",
    toolset="business_ops",
    schema=PROCESS_EVENT_SCHEMA,
    handler=cargolo_asr_process_event_tool,
    description="Process ASR webhook events into deterministic order folders.",
    emoji="📦",
)

registry.register(
    name="cargolo_asr_mail_history",
    toolset="business_ops",
    schema=MAIL_HISTORY_SCHEMA,
    handler=cargolo_asr_mail_history_tool,
    description="Query the configured ASR mail-history endpoint.",
    emoji="📬",
)

registry.register(
    name="cargolo_asr_daily_report",
    toolset="business_ops",
    schema=DAILY_REPORT_SCHEMA,
    handler=cargolo_asr_daily_report_tool,
    description="Generate the ASR daily operations report.",
    emoji="📊",
)
