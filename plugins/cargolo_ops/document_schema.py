from __future__ import annotations

from typing import Any

ASR_MODES = {"air", "sea", "rail", "unknown"}
GOODS_DOCUMENT_TYPES = {"commercial_invoice", "packing_list", "customs_document", "bill_of_lading", "air_waybill", "proof_of_delivery"}
BILLING_DOCUMENT_TYPES = {"billing", "supplier_invoice", "agent_invoice"}


def _document_type_key(value: Any) -> str:
    raw = str(value or "").strip().lower()
    for old, new in (("/", "_"), (" ", "_"), ("-", "_"), (".", ""), ("&", "and")):
        raw = raw.replace(old, new)
    while "__" in raw:
        raw = raw.replace("__", "_")
    return raw.strip("_")


def normalize_document_type(value: Any) -> str:
    raw = _document_type_key(value)
    aliases = {
        "invoice": "commercial_invoice",
        "ci": "commercial_invoice",
        "commercial_invoice": "commercial_invoice",
        "handelsrechnung": "commercial_invoice",
        "packing": "packing_list",
        "pl": "packing_list",
        "packing_list": "packing_list",
        "packliste": "packing_list",
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
        "supplier_invoice": "supplier_invoice",
        "agent_invoice": "agent_invoice",
    }

    try:
        from .document_profiles import DOCUMENT_PROFILES

        for profile_key, profile in DOCUMENT_PROFILES.items():
            aliases[profile_key] = profile_key
            aliases[_document_type_key(profile_key)] = profile_key
            for alias in profile.aliases:
                aliases[_document_type_key(alias)] = profile_key
    except Exception:
        # Keep the legacy normalizer usable even if optional profile loading fails.
        pass

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
