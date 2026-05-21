from __future__ import annotations

import pytest

from plugins.cargolo_ops.document_profiles import (
    COMMON_FIELDS,
    DOCUMENT_PROFILES,
    get_document_profile,
    is_trusted_source_for_field,
    trusted_update_fields,
)
from plugins.cargolo_ops.document_schema import normalize_document_type


EXPECTED_PROFILE_KEYS = {
    "offer",
    "customs_power_of_attorney",
    "commercial_invoice",
    "packing_list",
    "customer_misc",
    "certificate",
    "master_bill_of_lading",
    "house_bill_of_lading",
    "telex_release",
    "release_order",
    "customs_instruction",
    "export_accompanying_document",
    "booking_confirmation",
    "shipment_advice",
    "terminal_receipt",
    "outgoing_invoice",
    "billing",
    "freight_cost_invoice_cfr_cpt",
    "tax_assessment",
    "internal_misc",
}


def test_all_document_profiles_exist():
    assert set(DOCUMENT_PROFILES) == EXPECTED_PROFILE_KEYS
    for key in EXPECTED_PROFILE_KEYS:
        assert get_document_profile(key).document_type == key


@pytest.mark.parametrize(
    ("alias", "expected"),
    [
        ("M/B", "master_bill_of_lading"),
        ("MBL", "master_bill_of_lading"),
        ("Master B/L", "master_bill_of_lading"),
        ("master_bl", "master_bill_of_lading"),
        ("HBL", "house_bill_of_lading"),
        ("H/B/L", "house_bill_of_lading"),
        ("house_bl", "house_bill_of_lading"),
        ("ABD", "export_accompanying_document"),
        ("Ausfuhrbegleitdokument", "export_accompanying_document"),
        ("Zollvollmacht", "customs_power_of_attorney"),
        ("Freistellung", "release_order"),
        ("Delivery Order", "release_order"),
        ("Kaischein", "terminal_receipt"),
        ("Steuerbescheid", "tax_assessment"),
        ("Ausgangsrechnung", "outgoing_invoice"),
        ("Frachtkostenrechnung CFR/CPT", "freight_cost_invoice_cfr_cpt"),
    ],
)
def test_key_aliases_normalize_to_distinct_profile_types(alias, expected):
    assert normalize_document_type(alias) == expected


def test_common_fields_include_required_reconciliation_fields():
    for field in ["mbl_number", "container_number", "incoterm_named_place", "gross_weight", "eta", "etd"]:
        assert field in COMMON_FIELDS


def test_master_bl_trusts_transport_fields_but_commercial_invoice_does_not_trust_mbl():
    expected_trusted = {"mbl_number", "container_number", "seal_number", "vessel", "voyage", "pol", "pod"}
    assert expected_trusted.issubset(set(trusted_update_fields("master_bill_of_lading")))
    for field in expected_trusted:
        assert is_trusted_source_for_field("master_bill_of_lading", field) is True

    assert "mbl_number" not in trusted_update_fields("commercial_invoice")
    assert is_trusted_source_for_field("commercial_invoice", "mbl_number") is False


def test_telex_release_trusts_b_l_and_single_container_review_fields():
    expected_trusted = {"mbl_number", "hbl_number", "container_number"}
    assert expected_trusted.issubset(set(trusted_update_fields("telex_release")))
    for field in expected_trusted:
        assert is_trusted_source_for_field("telex_release", field) is True


def test_commercial_invoice_has_blocker_checks_for_business_critical_invoice_fields():
    checks = get_document_profile("commercial_invoice").checks
    blocker_codes = {check["code"] for check in checks if check.get("severity") == "blocker"}
    assert {
        "proforma_invoice",
        "zero_value",
        "missing_currency",
        "missing_incoterm",
        "missing_invoice_number",
    }.issubset(blocker_codes)


def test_packing_list_warns_for_missing_net_weight():
    checks = get_document_profile("packing_list").checks
    assert {check["code"] for check in checks if check.get("severity") == "warning"} >= {"missing_net_weight"}
