from __future__ import annotations

from typing import Any

ASR_MODES = {"air", "sea", "rail", "unknown"}
GOODS_DOCUMENT_TYPES = {"commercial_invoice", "packing_list", "customs_document", "bill_of_lading", "air_waybill", "proof_of_delivery"}
BILLING_DOCUMENT_TYPES = {"billing", "supplier_invoice", "agent_invoice"}


def normalize_document_type(value: Any) -> str:
    raw = str(value or "").strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "invoice": "commercial_invoice",
        "ci": "commercial_invoice",
        "commercial_invoice": "commercial_invoice",
        "packing": "packing_list",
        "pl": "packing_list",
        "packing_list": "packing_list",
        "awb": "air_waybill",
        "hawb": "air_waybill",
        "mawb": "air_waybill",
        "air_waybill": "air_waybill",
        "bl": "bill_of_lading",
        "b_l": "bill_of_lading",
        "bill_of_lading": "bill_of_lading",
        "pod": "proof_of_delivery",
        "proof_of_delivery": "proof_of_delivery",
        "customs": "customs_document",
        "customs_document": "customs_document",
        "billing": "billing",
    }
    return aliases.get(raw, raw or "unknown")


def normalize_mode(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"air", "airfreight", "air_freight"}:
        return "air"
    if raw in {"sea", "ocean", "oceanfreight", "sea_freight"}:
        return "sea"
    if raw in {"rail", "train"}:
        return "rail"
    return "unknown"
