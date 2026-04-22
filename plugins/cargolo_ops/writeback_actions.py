from __future__ import annotations

import base64
from pathlib import Path
from typing import Any

from .tms_provider import build_tms_write_provider_from_env

DEFAULT_ADMIN_USER_ID = 106

SUPPORTED_FIELD_TARGETS: dict[str, str] = {
    "shipment.customs.customs_reference": "customs_reference",
    "shipment.customs.customs_status": "customs_status",
    "shipment.freight_details.hbl_number": "hbl_number",
    "shipment.freight_details.mbl_number": "mbl_number",
    "shipment.freight_details.hawb_number": "hawb_number",
    "shipment.freight_details.container_number": "container_number",
    "shipment.carrier.carrier_reference": "carrier_reference",
    "shipment.carrier.tracking_number": "tracking_number",
    "shipment.carrier.tracking_link": "tracking_link",
    "shipment.carrier.mware_number": "mware_number",
    "shipment.carrier.consignment_number": "consignment_number",
    "shipment.dates.pickup_date": "pickup_date",
    "shipment.dates.estimated_delivery_date": "estimated_delivery_date",
    "shipment.dates.latest_delivery_date": "latest_delivery_date",
    "shipment.dates.actual_delivery_date": "actual_delivery_date",
    "shipment.dates.cargo_ready_date": "cargo_ready_date",
}


def supports_field_update_target(target: str) -> bool:
    return str(target or "").strip() in SUPPORTED_FIELD_TARGETS


def apply_pending_tms_action(
    action: dict[str, Any],
    context: dict[str, Any],
    *,
    admin_user_id: int = DEFAULT_ADMIN_USER_ID,
) -> dict[str, Any]:
    provider = build_tms_write_provider_from_env()
    if provider is None:
        raise RuntimeError("TMS write provider is not configured")

    order_id = str(context.get("order_id") or action.get("order_id") or "").strip().upper()
    if not order_id:
        raise ValueError("order_id missing from writeback context")

    action_type = str(action.get("action_type") or "").strip().lower()
    target = str(action.get("target") or "").strip()
    suggested_value = action.get("suggested_value")

    def _tool_args() -> dict[str, Any]:
        payload = action.get("tool_args") if isinstance(action.get("tool_args"), dict) else {}
        return dict(payload)

    if action_type == "field_update":
        field_name = SUPPORTED_FIELD_TARGETS.get(target)
        if not field_name:
            return {
                "status": "skipped",
                "reason": "unsupported_field_update_target",
                "target": target,
            }
        response = provider.update_shipment(
            an=order_id,
            admin_user_id=admin_user_id,
            **{field_name: suggested_value},
        )
        return {
            "status": "applied" if str(response.get("status") or "").lower() == "ok" else "failed",
            "executed_tool": "cargolo_tms_update_shipment",
            "target": target,
            "response": response,
        }

    if action_type == "status_update":
        response = provider.set_shipment_status(
            an=order_id,
            admin_user_id=admin_user_id,
            new_status=str(suggested_value or "").strip(),
            milestone_note=str(action.get("reason") or action.get("source") or "Hermes writeback") or "Hermes writeback",
        )
        return {
            "status": "applied" if str(response.get("status") or "").lower() == "ok" else "failed",
            "executed_tool": "cargolo_tms_set_shipment_status",
            "target": target,
            "response": response,
        }

    if action_type == "transport_leg_update":
        response = provider.update_transport_leg(
            an=order_id,
            admin_user_id=admin_user_id,
            **_tool_args(),
        )
        return {
            "status": "applied" if str(response.get("status") or "").lower() == "ok" else "failed",
            "executed_tool": "cargolo_tms_update_transport_leg",
            "target": target,
            "response": response,
        }

    if action_type == "shipment_address_update":
        response = provider.update_shipment_address(
            an=order_id,
            admin_user_id=admin_user_id,
            **_tool_args(),
        )
        return {
            "status": "applied" if str(response.get("status") or "").lower() == "ok" else "failed",
            "executed_tool": "cargolo_tms_update_shipment_address",
            "target": target,
            "response": response,
        }

    if action_type == "cargo_item_update":
        response = provider.update_cargo_item(
            an=order_id,
            admin_user_id=admin_user_id,
            **_tool_args(),
        )
        return {
            "status": "applied" if str(response.get("status") or "").lower() == "ok" else "failed",
            "executed_tool": "cargolo_tms_update_cargo_item",
            "target": target,
            "response": response,
        }

    if action_type == "document_upload":
        source_path = Path(str(action.get("source_path") or ""))
        if not source_path.exists() or not source_path.is_file():
            return {
                "status": "skipped",
                "reason": "missing_source_file",
                "target": target,
                "source_path": str(source_path),
            }
        response = provider.upload_document(
            an=order_id,
            admin_user_id=admin_user_id,
            document_type=str(action.get("document_type") or "other"),
            file_name=str(action.get("file_name") or source_path.name),
            file_base64=base64.b64encode(source_path.read_bytes()).decode("ascii"),
            description=str(action.get("reason") or action.get("description") or "").strip() or None,
            mime_type=str(action.get("mime_type") or "").strip() or None,
        )
        return {
            "status": "applied" if str(response.get("status") or "").lower() == "ok" else "failed",
            "executed_tool": "cargolo_tms_upload_document",
            "target": target,
            "response": response,
        }

    return {
        "status": "skipped",
        "reason": "unsupported_action_type",
        "target": target,
        "action_type": action_type,
    }
