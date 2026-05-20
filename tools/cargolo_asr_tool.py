"""CARGOLO ASR operations tools.

Deterministic helpers for the Hermes webhook/agent flow:
- process normalized n8n email events into case folders
- query the n8n mail-history endpoint as an agent-usable tool
- generate a daily ops report from local case files
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from hermes_constants import display_hermes_home, get_hermes_home
from plugins.cargolo_ops.adapters import build_mail_history_client_from_env, build_tms_client_from_env
from plugins.cargolo_ops.processor import bootstrap_case, bootstrap_cases_from_tms, process_email_event
from plugins.cargolo_ops.reporting import generate_daily_report
from plugins.cargolo_ops.tms_provider import build_tms_provider_from_env
from plugins.cargolo_ops.ops_notifications import send_manual_ops_notification
from plugins.cargolo_ops.teams_reply_loop import record_agent_tms_update_intent
from plugins.cargolo_ops.writeback_actions import apply_pending_tms_action, DEFAULT_ADMIN_USER_ID
from plugins.cargolo_ops.writeback_executor import run_writeback_executor
from plugins.cargolo_ops.document_activity_monitor import run_document_activity_monitor
from tools.registry import registry, tool_error


DEFAULT_ROOT = get_hermes_home() / "cargolo_asr"


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"", "0", "false", "no", "off"}


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


def _live_shipment_exists_for_tool(order_id: str) -> bool | None:
    """TMS-first guard for direct agent tools.

    Returns False only when the live TMS shipment list authoritatively says the
    AN/BU does not exist. None keeps degraded/offline setups working.
    """
    provider = build_tms_provider_from_env()
    if provider is None or not hasattr(provider, "shipments_list"):
        return None
    normalized = str(order_id or "").strip().upper()
    if not normalized:
        return None
    try:
        rows = provider.shipments_list(
            transport_category="asr",
            shipment_number=normalized,
            limit=20,
        )
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, dict):
            return None
        if str(row.get("shipment_number") or "").strip().upper() == normalized:
            return True
    return False


def _shipment_not_found_payload(order_id: str, *, source: str) -> str:
    return json.dumps({
        "status": "skipped",
        "code": "shipment_not_found_in_tms",
        "an": order_id,
        "source": source,
        "message": f"{order_id} ist im ASR-TMS nicht zu finden. Keine n8n-/Mail-Historie-Suche ausgeführt.",
    }, ensure_ascii=False)


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
            "enable_subagent_analysis": {
                "type": "boolean",
                "description": "If true, run the post-processing ASR specialist subagent analysis layer after deterministic case writes. Default false.",
            },
            "notify_ops_webhook": {
                "type": "boolean",
                "description": "If true, forward the final manual/offline processing result to the configured ASR ops webhook (n8n/Teams). Default true.",
            },
        },
        "required": [],
    },
}


MAIL_HISTORY_SCHEMA = {
    "name": "cargolo_asr_mail_history",
    "description": (
        "Call the configured n8n ASR mail-history endpoint by AN and return the normalized response. "
        "TMS-first guard: if the AN/BU is not present in the ASR TMS shipment list, this returns shipment_not_found_in_tms and does not call n8n."
    ),
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


TMS_SYNC_SCHEMA = {
    "name": "cargolo_asr_tms_sync",
    "description": (
        "Sync a CARGOLO ASR shipment from the live TMS by AN (e.g. AN-12345). "
        "Fetches shipment_detail, billing_items, and aggregate stats. "
        "Returns a structured snapshot. Requires CARGOLO_TMS_EMAIL and CARGOLO_TMS_PASSWORD env vars."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "an": {
                "type": "string",
                "description": "Shipment number like AN-12345.",
            },
            "storage_root": {
                "type": "string",
                "description": "Optional override for the ASR case root directory. Results are stored in the order's tms/ folder.",
            },
        },
        "required": ["an"],
    },
}


TMS_SHIPMENTS_LIST_SCHEMA = {
    "name": "cargolo_asr_tms_shipments_list",
    "description": (
        "List ASR shipments from the CARGOLO TMS. "
        "Returns the shipments_list for transport_category=asr. "
        "Useful for searching or browsing current shipments."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "transport_category": {
                "type": "string",
                "description": "Transport category to filter. Default 'asr'.",
            },
        },
        "required": [],
    },
}


TMS_CREATE_TODO_SCHEMA = {
    "name": "cargolo_asr_tms_create_todo",
    "description": (
        "Create a task (Aufgabe) in the CARGOLO TMS for a shipment. "
        "Requires the shipment UUID (from shipment detail or TMS sync). "
        "Categories: dokumente, zoll, abholung, zustellung, kommunikation, rechnung, sonstiges. "
        "Priority: low, medium, high, urgent."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Task title (required)."},
            "related_id": {"type": "string", "description": "Shipment UUID (required). Get via tms_sync first."},
            "description": {"type": "string", "description": "Task description."},
            "priority": {"type": "string", "description": "low | medium | high | urgent. Default medium."},
            "category": {"type": "string", "description": "dokumente | zoll | abholung | zustellung | kommunikation | rechnung | sonstiges. Default sonstiges."},
            "due_date": {"type": "string", "description": "ISO timestamp for due date."},
        },
        "required": ["title", "related_id"],
    },
}


TMS_LIST_TODOS_SCHEMA = {
    "name": "cargolo_asr_tms_list_todos",
    "description": (
        "List tasks (Aufgaben) from the CARGOLO TMS. "
        "Filter by shipment UUID, status, priority, category, or overdue status. "
        "Note: this endpoint may currently return 404 in the live environment."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "related_id": {"type": "string", "description": "Shipment UUID to filter by."},
            "status": {"type": "string", "description": "pending | in_progress | completed | cancelled."},
            "priority": {"type": "string", "description": "low | medium | high | urgent."},
            "category": {"type": "string", "description": "dokumente | zoll | abholung | zustellung | kommunikation | rechnung | sonstiges."},
            "is_overdue": {"type": "boolean", "description": "Filter for overdue tasks only."},
        },
        "required": [],
    },
}


TMS_WRITEBACK_SCHEMA = {
    "name": "cargolo_asr_tms_writeback",
    "description": (
        "Execute or dry-run the local TMS writeback queue using the validated MCP write tools. "
        f"Defaults to admin_user_id {DEFAULT_ADMIN_USER_ID}."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "storage_root": {"type": "string", "description": "Optional override for the ASR case root directory."},
            "dry_run": {"type": "boolean", "description": "If true, only stage actions into applied_updates.json without executing writes. Default true."},
            "admin_user_id": {"type": "integer", "description": "Admin user ID for MCP write actions. Default 106."},
        },
        "required": [],
    },
}


TMS_AGENT_INTENT_SCHEMA = {
    "name": "cargolo_asr_record_teams_tms_intent",
    "description": (
        "Record an intelligent agent decision that a Teams reply is a CARGOLO ASR TMS change intent. "
        "This ONLY queues pending_review in the local case; it never writes to TMS. Use instead of direct TMS writes."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "order_id": {"type": "string", "description": "Shipment/order number like AN-11755 or BU-4664."},
            "target": {"type": "string", "description": "customs_reference | hbl_number | mbl_number | hawb_number | container_number | pickup_date | estimated_delivery_date | actual_delivery_date | etd_main_carriage (review-only) | atd_main_carriage (review-only)"},
            "value": {"type": "string", "description": "Requested field value exactly as the operator intended."},
            "text": {"type": "string", "description": "Short reason / relevant operator message."},
            "context_id": {"type": "string", "description": "ASRCTX/context id from the card, if known."},
            "source_message_id": {"type": "string", "description": "Teams reply message id, if known."},
            "operator": {"type": "string", "description": "Operator/user name, if known."},
            "confidence": {"type": "string", "description": "Agent confidence label. Default agent_decided."},
            "storage_root": {"type": "string", "description": "Optional ASR case root override."},
        },
        "required": ["order_id", "target", "value"],
    },
}


DOCUMENT_ACTIVITY_MONITOR_SCHEMA = {
    "name": "cargolo_asr_document_activity_monitor",
    "description": (
        "Poll the CARGOLO TMS ASR activity log for new document upload events, then run the shared "
        "mail-history + TMS + document monitoring lifecycle for each affected AN/BU and optionally notify Teams. "
        "Read-first except for the existing ops notification sink."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "storage_root": {"type": "string", "description": "Optional override for the ASR case root directory."},
            "admin_user_id": {"type": "integer", "description": "Admin user ID for reading the ASR activity log. Default 106."},
            "max_events": {"type": "integer", "description": "Maximum new document-upload events to process. Default 5."},
            "per_page": {"type": "integer", "description": "Activity-log page size. Default 50."},
            "date_from": {"type": "string", "description": "Optional lower bound for activity-log changed_at filtering."},
            "date_to": {"type": "string", "description": "Optional upper bound for activity-log changed_at filtering."},
            "force": {"type": "boolean", "description": "If true, ignore the local cursor/dedupe state. Default false."},
            "dry_run": {"type": "boolean", "description": "If true, only list selected events and do not run monitoring or update cursor. Default false."},
            "notify_ops_webhook": {"type": "boolean", "description": "If true, notify the configured ASR ops webhook/Teams sink. Default true."},
            "refresh_history": {"type": "boolean", "description": "If true, sync full/delta ASR mail history before document analysis. Default true."},
            "analyze_documents": {"type": "boolean", "description": "If true, run configured document analysis after mirroring TMS/mail documents. Default true."},
        },
        "required": [],
    },
}


BOOTSTRAP_CASE_SCHEMA = {
    "name": "cargolo_asr_bootstrap_case",
    "description": (
        "Create an initial ASR baseline folder for a single shipment number from current TMS data. "
        "This pulls the TMS snapshot, document requirements, billing context, and optional full mail history, "
        "without inventing a fake inbound customer email."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "an": {"type": "string", "description": "Shipment number like AN-12345."},
            "storage_root": {"type": "string", "description": "Optional override for the ASR case root directory."},
            "refresh_history": {"type": "boolean", "description": "If true, perform a first full mail-history sync. Default true."},
            "mailbox": {"type": "string", "description": "Mailbox to sync from. Default asr@cargolo.com."},
            "notify_ops_webhook": {
                "type": "boolean",
                "description": "If true, forward the final bootstrap result to the configured ASR ops webhook (n8n/Teams). Default true.",
            },
        },
        "required": ["an"],
    },
}


BOOTSTRAP_FROM_TMS_SCHEMA = {
    "name": "cargolo_asr_bootstrap_cases_from_tms",
    "description": (
        "Create initial ASR baseline folders for a batch of current TMS shipments. "
        "Use this for a controlled first-pass test run or full baseline build before live mail ingest is enabled."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "storage_root": {"type": "string", "description": "Optional override for the ASR case root directory."},
            "refresh_history": {"type": "boolean", "description": "If true, perform first full mail-history sync per case. Default true."},
            "mailbox": {"type": "string", "description": "Mailbox to sync from. Default asr@cargolo.com."},
            "limit": {"type": "integer", "description": "Optional max number of shipments to bootstrap."},
            "per_page": {"type": "integer", "description": "Page size for TMS listing. Default 100."},
            "status_filter": {"type": "string", "description": "Optional TMS status filter."},
            "network_filter": {"type": "string", "description": "Optional TMS network filter."},
            "search": {"type": "string", "description": "Optional TMS search string."},
            "notify_ops_webhook": {
                "type": "boolean",
                "description": "If true, forward the final batch bootstrap result to the configured ASR ops webhook (n8n/Teams). Default true.",
            },
        },
        "required": [],
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
            enable_subagent_analysis=bool(args.get("enable_subagent_analysis", False)),
            write_internal_note=bool(args.get("write_internal_note", _env_flag("HERMES_CARGOLO_ASR_ENABLE_TMS_INTERNAL_NOTES", False))),
        )
        result_payload = result.model_dump(mode="json")
        response_payload: dict[str, Any] = dict(result_payload)
        if bool(args.get("notify_ops_webhook", True)):
            response_payload["ops_notification"] = send_manual_ops_notification(
                run_type="process_event",
                payload={
                    "order_id": result.order_id,
                    "storage_root": str(root),
                    "processor_result": result_payload,
                },
                allow_route_fallback=True,
            )
        return json.dumps(response_payload, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_mail_history_tool(args: dict[str, Any], **_: Any) -> str:
    an = str(args.get("an") or "").strip().upper()
    if not an:
        return tool_error("Missing required parameter 'an'")
    if _live_shipment_exists_for_tool(an) is False:
        return _shipment_not_found_payload(an, source="cargolo_asr_mail_history")
    client = build_mail_history_client_from_env()
    if client is None:
        return tool_error("HERMES_CARGOLO_ASR_MAIL_HISTORY_URL is not configured")
    try:
        result = client.fetch_history(
            an,
            first_sync=bool(args.get("first_sync", False)),
            since=args.get("since"),
            mailbox=args.get("mailbox") or "asr@cargolo.com",
            include_attachments=bool(args.get("include_attachments", True)),
            include_html=bool(args.get("include_html", False)),
        )
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_tms_sync_tool(args: dict[str, Any], **_: Any) -> str:
    provider = build_tms_provider_from_env()
    if provider is None:
        return tool_error("TMS not configured. Set MCP bridge env or legacy CARGOLO_TMS credentials.")
    an = args.get("an", "").strip().upper()
    if not an:
        return tool_error("Missing required parameter 'an'")
    try:
        snapshot = provider.snapshot_bundle(an)
        # Optionally store in case folder
        root = _resolve_root(args.get("storage_root"))
        from plugins.cargolo_ops.storage import CaseStore
        store = CaseStore(root)
        if store.order_path(an).exists():
            store.save_tms_snapshot(an, snapshot.model_dump(mode="json"))
            tms_dir = store.order_path(an) / "tms"
            tms_dir.mkdir(parents=True, exist_ok=True)
            if snapshot.detail:
                (tms_dir / "shipment_detail.json").write_text(
                    json.dumps(snapshot.detail, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            if snapshot.billing_items:
                (tms_dir / "shipment_billing_items.json").write_text(
                    json.dumps(snapshot.billing_items, ensure_ascii=False, indent=2), encoding="utf-8"
                )
        return json.dumps(snapshot.model_dump(mode="json"), ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_tms_shipments_list_tool(args: dict[str, Any], **_: Any) -> str:
    provider = build_tms_provider_from_env()
    if provider is None:
        return tool_error("TMS not configured. Set MCP bridge env or legacy CARGOLO_TMS credentials.")
    try:
        category = args.get("transport_category", "asr") or "asr"
        rows = provider.shipments_list(transport_category=category)
        return json.dumps({"transport_category": category, "count": len(rows), "shipments": rows}, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_tms_create_todo_tool(args: dict[str, Any], **_: Any) -> str:
    client = build_tms_client_from_env()
    if client is None:
        return tool_error("TMS not configured. Set CARGOLO_TMS_EMAIL and CARGOLO_TMS_PASSWORD env vars.")
    title = args.get("title", "").strip()
    related_id = args.get("related_id", "").strip()
    if not title or not related_id:
        return tool_error("Missing required parameters 'title' and 'related_id'")
    try:
        result = client.create_todo(
            title=title,
            related_id=related_id,
            description=args.get("description"),
            priority=args.get("priority", "medium"),
            category=args.get("category", "sonstiges"),
            due_date=args.get("due_date"),
        )
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_tms_list_todos_tool(args: dict[str, Any], **_: Any) -> str:
    client = build_tms_client_from_env()
    if client is None:
        return tool_error("TMS not configured. Set CARGOLO_TMS_EMAIL and CARGOLO_TMS_PASSWORD env vars.")
    try:
        result = client.list_todos(
            related_id=args.get("related_id"),
            status=args.get("status"),
            priority=args.get("priority"),
            category=args.get("category"),
            is_overdue=args.get("is_overdue"),
        )
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_tms_writeback_tool(args: dict[str, Any], **_: Any) -> str:
    try:
        root = _resolve_root(args.get("storage_root"))
        dry_run = bool(args.get("dry_run", True))
        admin_user_id = int(args.get("admin_user_id") or DEFAULT_ADMIN_USER_ID)
        result = run_writeback_executor(
            storage_root=root,
            dry_run=dry_run,
            apply_action=None if dry_run else (lambda action, context: apply_pending_tms_action(action, context, admin_user_id=admin_user_id)),
        )
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_record_teams_tms_intent_tool(args: dict[str, Any], **_: Any) -> str:
    try:
        root = _resolve_root(args.get("storage_root"))
        result = record_agent_tms_update_intent(
            root=root,
            order_id=str(args.get("order_id") or ""),
            target=str(args.get("target") or ""),
            value=str(args.get("value") or ""),
            text=str(args.get("text") or ""),
            operator=args.get("operator"),
            source_message_id=args.get("source_message_id"),
            context_id=args.get("context_id"),
            confidence=str(args.get("confidence") or "agent_decided"),
        )
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_document_activity_monitor_tool(args: dict[str, Any], **_: Any) -> str:
    try:
        root = _resolve_root(args.get("storage_root"))
        result = run_document_activity_monitor(
            storage_root=root,
            admin_user_id=int(args.get("admin_user_id") or DEFAULT_ADMIN_USER_ID),
            max_events=int(args.get("max_events", 5) or 5),
            per_page=int(args.get("per_page", 50) or 50),
            date_from=args.get("date_from"),
            date_to=args.get("date_to"),
            force=bool(args.get("force", False)),
            dry_run=bool(args.get("dry_run", False)),
            notify_ops_webhook=bool(args.get("notify_ops_webhook", True)),
            refresh_history=bool(args.get("refresh_history", True)),
            analyze_documents=bool(args.get("analyze_documents", True)),
        )
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))


def cargolo_asr_bootstrap_case_tool(args: dict[str, Any], **_: Any) -> str:
    an = args.get("an", "").strip().upper()
    if not an:
        return tool_error("Missing required parameter 'an'")
    try:
        root = _resolve_root(args.get("storage_root"))
        result = bootstrap_case(
            an,
            storage_root=root,
            refresh_history=bool(args.get("refresh_history", True)),
            mailbox=args.get("mailbox") or "asr@cargolo.com",
            write_internal_note=bool(args.get("write_internal_note", _env_flag("HERMES_CARGOLO_ASR_ENABLE_TMS_INTERNAL_NOTES", False))),
        )
        result_payload = result.model_dump(mode="json")
        response_payload: dict[str, Any] = dict(result_payload)
        if bool(args.get("notify_ops_webhook", True)):
            response_payload["ops_notification"] = send_manual_ops_notification(
                run_type="bootstrap_case",
                payload={
                    "order_id": an,
                    "storage_root": str(root),
                    "processor_result": result_payload,
                },
                allow_route_fallback=True,
            )
        return json.dumps(response_payload, ensure_ascii=False)
    except Exception as exc:
        return tool_error(str(exc))



def cargolo_asr_bootstrap_cases_from_tms_tool(args: dict[str, Any], **_: Any) -> str:
    try:
        root = _resolve_root(args.get("storage_root"))
        result = bootstrap_cases_from_tms(
            storage_root=root,
            refresh_history=bool(args.get("refresh_history", True)),
            mailbox=args.get("mailbox") or "asr@cargolo.com",
            write_internal_note=bool(args.get("write_internal_note", _env_flag("HERMES_CARGOLO_ASR_ENABLE_TMS_INTERNAL_NOTES", False))),
            limit=args.get("limit"),
            per_page=int(args.get("per_page", 100) or 100),
            status_filter=args.get("status_filter") or "",
            network_filter=args.get("network_filter") or "",
            search=args.get("search") or "",
        )
        response_payload: dict[str, Any] = dict(result)
        if bool(args.get("notify_ops_webhook", True)):
            response_payload["ops_notification"] = send_manual_ops_notification(
                run_type="bootstrap_cases_from_tms",
                payload={
                    "storage_root": str(root),
                    "processor_result": result,
                },
                allow_route_fallback=True,
            )
        return json.dumps(response_payload, ensure_ascii=False)
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

registry.register(
    name="cargolo_asr_tms_sync",
    toolset="business_ops",
    schema=TMS_SYNC_SCHEMA,
    handler=cargolo_asr_tms_sync_tool,
    description="Sync a shipment from the live CARGOLO TMS by AN.",
    emoji="🔄",
)

registry.register(
    name="cargolo_asr_tms_shipments_list",
    toolset="business_ops",
    schema=TMS_SHIPMENTS_LIST_SCHEMA,
    handler=cargolo_asr_tms_shipments_list_tool,
    description="List ASR shipments from the CARGOLO TMS.",
    emoji="📋",
)

registry.register(
    name="cargolo_asr_tms_create_todo",
    toolset="business_ops",
    schema=TMS_CREATE_TODO_SCHEMA,
    handler=cargolo_asr_tms_create_todo_tool,
    description="Create a task (Aufgabe) in the CARGOLO TMS for a shipment.",
    emoji="✅",
)

registry.register(
    name="cargolo_asr_tms_list_todos",
    toolset="business_ops",
    schema=TMS_LIST_TODOS_SCHEMA,
    handler=cargolo_asr_tms_list_todos_tool,
    description="List tasks (Aufgaben) from the CARGOLO TMS.",
    emoji="📝",
)

registry.register(
    name="cargolo_asr_tms_writeback",
    toolset="business_ops",
    schema=TMS_WRITEBACK_SCHEMA,
    handler=cargolo_asr_tms_writeback_tool,
    description="Execute or dry-run pending ASR TMS writeback actions through the MCP write tools.",
    emoji="🛠️",
)

registry.register(
    name="cargolo_asr_record_teams_tms_intent",
    toolset="business_ops",
    schema=TMS_AGENT_INTENT_SCHEMA,
    handler=cargolo_asr_record_teams_tms_intent_tool,
    description="Queue an agent-decided Teams TMS update intent as pending review without writing to TMS.",
    emoji="🧠",
)

registry.register(
    name="cargolo_asr_document_activity_monitor",
    toolset="business_ops",
    schema=DOCUMENT_ACTIVITY_MONITOR_SCHEMA,
    handler=cargolo_asr_document_activity_monitor_tool,
    description="Monitor TMS ASR document uploads and trigger mail/TMS/document analysis plus Teams notification.",
    emoji="📄",
)

registry.register(
    name="cargolo_asr_bootstrap_case",
    toolset="business_ops",
    schema=BOOTSTRAP_CASE_SCHEMA,
    handler=cargolo_asr_bootstrap_case_tool,
    description="Create an initial ASR baseline case folder for one shipment.",
    emoji="🧱",
)

registry.register(
    name="cargolo_asr_bootstrap_cases_from_tms",
    toolset="business_ops",
    schema=BOOTSTRAP_FROM_TMS_SCHEMA,
    handler=cargolo_asr_bootstrap_cases_from_tms_tool,
    description="Create initial ASR baseline case folders from the current TMS shipment list.",
    emoji="🏗️",
)
