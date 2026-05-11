from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

COMMON_FIELDS: tuple[str, ...] = (
    "document_type",
    "document_number",
    "date",
    "issuer",
    "recipient",
    "customer",
    "tms_reference",
    "shipment_number",
    "customer_reference",
    "booking_number",
    "hbl_number",
    "mbl_number",
    "invoice_number",
    "mrn",
    "container_number",
    "seal_number",
    "vessel",
    "voyage",
    "loading_place",
    "unloading_place",
    "pol",
    "pod",
    "incoterm_named_place",
    "pieces",
    "packaging_type",
    "gross_weight",
    "net_weight",
    "volume",
    "goods_description",
    "goods_value",
    "currency",
    "eta",
    "etd",
)


@dataclass(frozen=True)
class DocumentProfile:
    document_type: str
    aliases: tuple[str, ...] = ()
    relevant_fields: tuple[str, ...] = COMMON_FIELDS
    checks: tuple[dict[str, Any], ...] = ()
    trusted_tms_update_fields: tuple[str, ...] = ()
    trusted_sources: dict[str, tuple[str, ...]] = field(default_factory=dict)


def _check(code: str, severity: str, summary: str) -> dict[str, str]:
    return {"code": code, "severity": severity, "summary": summary}


DOCUMENT_PROFILES: dict[str, DocumentProfile] = {
    "offer": DocumentProfile(
        "offer",
        aliases=("angebot", "quotation", "quote"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "customer", "tms_reference", "customer_reference", "goods_description", "goods_value", "currency", "eta", "etd"),
    ),
    "customs_power_of_attorney": DocumentProfile(
        "customs_power_of_attorney",
        aliases=("zollvollmacht", "customs_poa", "power_of_attorney", "vollmacht"),
        relevant_fields=("date", "issuer", "recipient", "customer", "shipment_number", "customer_reference", "mrn"),
    ),
    "commercial_invoice": DocumentProfile(
        "commercial_invoice",
        aliases=("invoice", "ci", "commercial invoice", "handelsrechnung"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "customer", "customer_reference", "invoice_number", "incoterm_named_place", "pieces", "gross_weight", "net_weight", "goods_description", "goods_value", "currency"),
        checks=(
            _check("proforma_invoice", "blocker", "Commercial invoice must not be only a proforma invoice."),
            _check("zero_value", "blocker", "Goods value must be greater than zero for customs use."),
            _check("missing_currency", "blocker", "Currency is required."),
            _check("missing_incoterm", "blocker", "Incoterm and named place are required."),
            _check("missing_invoice_number", "blocker", "Invoice number is required."),
        ),
        trusted_tms_update_fields=("invoice_number", "incoterm_named_place", "goods_value", "currency", "goods_description"),
    ),
    "packing_list": DocumentProfile(
        "packing_list",
        aliases=("packing", "pl", "packliste", "packing list"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "customer_reference", "container_number", "seal_number", "pieces", "packaging_type", "gross_weight", "net_weight", "volume", "goods_description"),
        checks=(_check("missing_net_weight", "warning", "Net weight should be present on the packing list."),),
        trusted_tms_update_fields=("container_number", "pieces", "packaging_type", "gross_weight", "net_weight", "volume"),
    ),
    "customer_misc": DocumentProfile("customer_misc", aliases=("customer document", "kundendokument", "customer_misc")),
    "certificate": DocumentProfile(
        "certificate",
        aliases=("certificate", "cert", "zeugnis", "zertifikat", "certificate_of_origin"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "customer", "goods_description", "goods_value", "currency"),
    ),
    "master_bill_of_lading": DocumentProfile(
        "master_bill_of_lading",
        aliases=("m/b", "mbl", "master b/l", "master_bl", "master bill of lading"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "booking_number", "mbl_number", "container_number", "seal_number", "vessel", "voyage", "loading_place", "unloading_place", "pol", "pod", "pieces", "gross_weight", "volume", "goods_description", "eta", "etd"),
        trusted_tms_update_fields=("mbl_number", "container_number", "seal_number", "vessel", "voyage", "pol", "pod", "eta", "etd"),
    ),
    "house_bill_of_lading": DocumentProfile(
        "house_bill_of_lading",
        aliases=("hbl", "h/b/l", "house_bl", "house b/l", "house bill of lading"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "booking_number", "hbl_number", "mbl_number", "container_number", "seal_number", "vessel", "voyage", "pol", "pod", "pieces", "gross_weight", "volume", "goods_description", "eta", "etd"),
        trusted_tms_update_fields=("hbl_number", "container_number", "seal_number", "pol", "pod"),
    ),
    "telex_release": DocumentProfile(
        "telex_release",
        aliases=("telex", "telex release", "surrender_notice"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "mbl_number", "hbl_number", "container_number", "vessel", "voyage"),
        trusted_tms_update_fields=("mbl_number", "hbl_number"),
    ),
    "release_order": DocumentProfile(
        "release_order",
        aliases=("freistellung", "delivery order", "delivery_order", "release order"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "mbl_number", "hbl_number", "container_number", "seal_number", "vessel", "voyage", "unloading_place", "pod"),
        trusted_tms_update_fields=("container_number", "release_reference"),
    ),
    "customs_instruction": DocumentProfile(
        "customs_instruction",
        aliases=("customs instruction", "zollanweisung", "verzollungsanweisung"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "customer", "shipment_number", "customer_reference", "mrn", "incoterm_named_place", "goods_description", "goods_value", "currency"),
    ),
    "export_accompanying_document": DocumentProfile(
        "export_accompanying_document",
        aliases=("abd", "ausfuhrbegleitdokument", "export accompanying document", "ead"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "shipment_number", "mrn", "loading_place", "unloading_place", "pieces", "gross_weight", "goods_description", "goods_value", "currency"),
        trusted_tms_update_fields=("mrn",),
    ),
    "shipment_advice": DocumentProfile(
        "shipment_advice",
        aliases=("shipment advice", "versandavis", "shipping advice", "avis"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "customer", "shipment_number", "customer_reference", "booking_number", "eta", "etd", "loading_place", "unloading_place", "pieces", "gross_weight", "volume"),
        trusted_tms_update_fields=("eta", "etd"),
    ),
    "terminal_receipt": DocumentProfile(
        "terminal_receipt",
        aliases=("kaischein", "terminal receipt", "terminal_receipt", "dock receipt"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "booking_number", "container_number", "seal_number", "vessel", "voyage", "loading_place", "pol", "pieces", "gross_weight"),
        trusted_tms_update_fields=("container_number", "seal_number", "pol"),
    ),
    "outgoing_invoice": DocumentProfile(
        "outgoing_invoice",
        aliases=("ausgangsrechnung", "outgoing invoice", "ar_invoice", "customer_invoice"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "customer", "tms_reference", "shipment_number", "customer_reference", "invoice_number", "goods_value", "currency"),
        trusted_tms_update_fields=("invoice_number",),
    ),
    "freight_cost_invoice_cfr_cpt": DocumentProfile(
        "freight_cost_invoice_cfr_cpt",
        aliases=("frachtkostenrechnung cfr/cpt", "freight cost invoice cfr/cpt", "freight_cost_invoice", "cfr_cpt_invoice"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "customer", "shipment_number", "customer_reference", "invoice_number", "incoterm_named_place", "goods_value", "currency"),
    ),
    "tax_assessment": DocumentProfile(
        "tax_assessment",
        aliases=("steuerbescheid", "tax assessment", "einfuhrabgabenbescheid", "duties_tax_notice"),
        relevant_fields=("document_number", "date", "issuer", "recipient", "customer", "shipment_number", "mrn", "invoice_number", "goods_value", "currency"),
        trusted_tms_update_fields=("mrn",),
    ),
    "internal_misc": DocumentProfile("internal_misc", aliases=("internal", "internal_misc", "intern", "sonstiges_intern")),
}


def get_document_profile(doc_type: Any) -> DocumentProfile:
    from .document_schema import normalize_document_type

    normalized = normalize_document_type(doc_type)
    return DOCUMENT_PROFILES.get(normalized, DOCUMENT_PROFILES["internal_misc"])


def trusted_update_fields(doc_type: Any) -> tuple[str, ...]:
    return get_document_profile(doc_type).trusted_tms_update_fields


def is_trusted_source_for_field(doc_type: Any, field: str) -> bool:
    profile = get_document_profile(doc_type)
    if field in profile.trusted_tms_update_fields:
        return True
    sources = profile.trusted_sources.get(field, ())
    return profile.document_type in sources
