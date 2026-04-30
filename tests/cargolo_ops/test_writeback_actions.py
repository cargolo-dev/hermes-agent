import json
from pathlib import Path

from plugins.cargolo_ops.processor import _augment_pending_updates_with_analysis, _build_tms_pending_updates
from plugins.cargolo_ops.writeback_actions import apply_pending_tms_action


class _FakeWriteProvider:
    def __init__(self):
        self.calls = []

    def update_shipment(self, **kwargs):
        self.calls.append(("update_shipment", kwargs))
        return {"status": "ok", "echo": kwargs}

    def upload_document(self, **kwargs):
        self.calls.append(("upload_document", kwargs))
        return {"status": "ok", "echo": kwargs}

    def set_shipment_status(self, **kwargs):
        self.calls.append(("set_shipment_status", kwargs))
        return {"status": "ok", "echo": kwargs}

    def add_internal_note(self, **kwargs):
        self.calls.append(("add_internal_note", kwargs))
        return {"status": "ok", "echo": kwargs}

    def update_transport_leg(self, **kwargs):
        self.calls.append(("update_transport_leg", kwargs))
        return {"status": "ok", "echo": kwargs}

    def update_shipment_address(self, **kwargs):
        self.calls.append(("update_shipment_address", kwargs))
        return {"status": "ok", "echo": kwargs}

    def update_cargo_item(self, **kwargs):
        self.calls.append(("update_cargo_item", kwargs))
        return {"status": "ok", "echo": kwargs}


def test_apply_pending_tms_action_maps_field_update(monkeypatch):
    provider = _FakeWriteProvider()
    monkeypatch.setattr(
        "plugins.cargolo_ops.writeback_actions.build_tms_write_provider_from_env",
        lambda: provider,
    )

    result = apply_pending_tms_action(
        {
            "action_type": "field_update",
            "target": "shipment.dates.latest_delivery_date",
            "suggested_value": "2026-05-20",
        },
        {"order_id": "BU-4639"},
        admin_user_id=106,
    )

    assert result["status"] == "applied"
    assert provider.calls == [
        (
            "update_shipment",
            {
                "an": "BU-4639",
                "admin_user_id": 106,
                "latest_delivery_date": "2026-05-20",
            },
        )
    ]


def test_apply_pending_tms_action_maps_status_update(monkeypatch):
    provider = _FakeWriteProvider()
    monkeypatch.setattr(
        "plugins.cargolo_ops.writeback_actions.build_tms_write_provider_from_env",
        lambda: provider,
    )

    result = apply_pending_tms_action(
        {
            "action_type": "status_update",
            "target": "shipment.status",
            "suggested_value": "delivered",
            "reason": "actual_delivery_date vorhanden",
        },
        {"order_id": "AN-11362"},
        admin_user_id=106,
    )

    assert result["status"] == "applied"
    assert provider.calls == [
        (
            "set_shipment_status",
            {
                "an": "AN-11362",
                "admin_user_id": 106,
                "new_status": "delivered",
                "milestone_note": "actual_delivery_date vorhanden",
            },
        )
    ]


def test_apply_pending_tms_action_maps_document_upload(monkeypatch, tmp_path):
    provider = _FakeWriteProvider()
    monkeypatch.setattr(
        "plugins.cargolo_ops.writeback_actions.build_tms_write_provider_from_env",
        lambda: provider,
    )
    source = tmp_path / "invoice.txt"
    source.write_text("hello", encoding="utf-8")

    result = apply_pending_tms_action(
        {
            "action_type": "document_upload",
            "target": "documents.commercial_invoice",
            "document_type": "commercial_invoice",
            "file_name": "invoice.txt",
            "source_path": str(source),
            "mime_type": "text/plain",
            "reason": "lokal vorhanden",
        },
        {"order_id": "BU-4639"},
        admin_user_id=106,
    )

    assert result["status"] == "applied"
    tool_name, kwargs = provider.calls[0]
    assert tool_name == "upload_document"
    assert kwargs["an"] == "BU-4639"
    assert kwargs["document_type"] == "commercial_invoice"
    assert kwargs["file_name"] == "invoice.txt"
    assert kwargs["mime_type"] == "text/plain"
    assert kwargs["description"] == "lokal vorhanden"
    assert kwargs["file_base64"] == "aGVsbG8="


def test_apply_pending_tms_action_maps_transport_leg_update(monkeypatch):
    provider = _FakeWriteProvider()
    monkeypatch.setattr(
        "plugins.cargolo_ops.writeback_actions.build_tms_write_provider_from_env",
        lambda: provider,
    )

    result = apply_pending_tms_action(
        {
            "action_type": "transport_leg_update",
            "target": "transport_leg.destination_name",
            "tool_args": {
                "leg_uuid": "leg-123",
                "destination_name": "Bazenheid",
                "destination_country_code": "CH",
            },
        },
        {"order_id": "AN-12317"},
        admin_user_id=106,
    )

    assert result["status"] == "applied"
    assert provider.calls == [
        (
            "update_transport_leg",
            {
                "an": "AN-12317",
                "admin_user_id": 106,
                "leg_uuid": "leg-123",
                "destination_name": "Bazenheid",
                "destination_country_code": "CH",
            },
        )
    ]


def test_build_tms_pending_updates_adds_status_and_document_upload_actions(tmp_path):
    source = tmp_path / "invoice.txt"
    source.write_text("invoice", encoding="utf-8")
    tms_snapshot = {
        "shipment_uuid": "uuid-BU-4639",
        "shipment_number": "BU-4639",
        "status": "pickup_scheduled",
        "detail": {
            "status": "pickup_scheduled",
            "dates": {
                "actual_delivery_date": "2026-04-20",
                "estimated_delivery_date": "2026-04-10",
            },
        },
    }
    document_registry = {
        "missing_types": ["commercial_invoice"],
        "analysis_open_questions": [],
        "tms_documents": [],
        "tms_match_summary": [],
        "analyzed_documents": [],
        "received_documents": [
            {
                "filename": "invoice.txt",
                "stored_path": str(source),
                "mime_type": "text/plain",
                "detected_types": ["commercial_invoice"],
            }
        ],
    }

    payload = _build_tms_pending_updates(
        order_id="BU-4639",
        tms_snapshot=tms_snapshot,
        history_rows=[],
        document_registry=document_registry,
    )

    by_type = {}
    for action in payload["pending_actions"]:
        by_type.setdefault(action["action_type"], []).append(action)

    assert by_type["status_update"][0]["suggested_value"] == "delivered"
    assert by_type["status_update"][0]["action_status"] == "write_now"
    assert by_type["document_upload"][0]["document_type"] == "commercial_invoice"
    assert by_type["document_upload"][0]["source_path"] == str(source)
    assert by_type["document_upload"][0]["action_status"] == "write_now"
    assert "document_gap" not in by_type


def test_build_tms_pending_updates_does_not_auto_deliver_when_actual_delivery_date_looks_like_default_placeholder():
    tms_snapshot = {
        "shipment_uuid": "uuid-AN-11707",
        "shipment_number": "AN-11707",
        "status": "pending_confirmation",
        "detail": {
            "status": "pending_confirmation",
            "dates": {
                "pickup_date": "2026-03-30",
                "estimated_delivery_date": "2026-05-02",
                "latest_delivery_date": "2026-03-30",
                "actual_delivery_date": "2026-03-30",
                "wish_date": "2026-03-30",
            },
            "milestones": {
                "ata_main_carriage": None,
            },
        },
    }
    document_registry = {
        "missing_types": [],
        "analysis_open_questions": [],
        "tms_documents": [],
        "tms_match_summary": [],
        "analyzed_documents": [],
        "received_documents": [],
    }

    payload = _build_tms_pending_updates(
        order_id="AN-11707",
        tms_snapshot=tms_snapshot,
        history_rows=[],
        document_registry=document_registry,
    )

    targets = {action["target"]: action for action in payload["pending_actions"]}
    assert "shipment.status" not in targets
    assert targets["shipment.review.actual_delivery_date_placeholder"]["action_status"] == "review"
    assert targets["shipment.review.actual_delivery_date_placeholder"]["priority"] == "high"


def test_build_tms_pending_updates_prioritizes_status_review_over_generic_document_noise_for_delivered_mail_conflict():
    tms_snapshot = {
        "shipment_uuid": "uuid-AN-11796",
        "shipment_number": "AN-11796",
        "status": "delivered",
        "detail": {
            "status": "delivered",
            "network": "air",
            "dates": {
                "actual_delivery_date": None,
                "estimated_delivery_date": "2026-04-25",
            },
        },
    }
    history_rows = [
        {
            "subject": "AN-11796 Bosch UN38.3 Report / Verpackung für Abholung",
            "body_text": "Bitte DGR-Unterlagen und Verpackungsbilder vor Abholung prüfen. Status delivered passt noch nicht.",
            "sender": "ops@example.com",
        }
    ]
    document_registry = {
        "missing_types": ["air_waybill", "packing_list"],
        "analysis_open_questions": [],
        "tms_documents": [],
        "tms_match_summary": [],
        "analyzed_documents": [],
        "received_documents": [],
    }

    payload = _build_tms_pending_updates(
        order_id="AN-11796",
        tms_snapshot=tms_snapshot,
        history_rows=history_rows,
        document_registry=document_registry,
    )

    targets = {action["target"]: action for action in payload["pending_actions"]}
    status_review = targets["shipment.review.status_inconsistent_with_recent_mail_activity"]
    assert status_review["action_status"] == "review"
    assert status_review["priority"] == "high"
    assert targets["documents.air_waybill"]["action_status"] == "not_yet_knowable"
    assert targets["documents.review.packing_list_expected_but_mail_evidence_unclear"]["action_status"] == "review"
    assert payload["action_summary"] == {
        "write_now": 0,
        "review": 2,
        "not_yet_due": 0,
        "not_yet_knowable": 1,
    }


def test_build_tms_pending_updates_adds_precise_air_review_hints_instead_of_generic_document_noise():
    tms_snapshot = {
        "shipment_uuid": "uuid-AN-AIR",
        "shipment_number": "AN-AIR",
        "status": "confirmed",
        "detail": {
            "status": "confirmed",
            "network": "air",
            "dates": {},
        },
    }
    history_rows = [
        {
            "subject": "AN-AIR flight booking LH123 / AWB pending",
            "body_text": "Flight confirmed, please share AWB soon. Packing list has not been clearly confirmed yet.",
            "sender": "air@example.com",
        }
    ]
    document_registry = {
        "missing_types": ["air_waybill", "packing_list", "commercial_invoice"],
        "analysis_open_questions": [],
        "tms_documents": [],
        "tms_match_summary": [],
        "analyzed_documents": [],
        "received_documents": [],
    }

    payload = _build_tms_pending_updates(
        order_id="AN-AIR",
        tms_snapshot=tms_snapshot,
        history_rows=history_rows,
        document_registry=document_registry,
    )

    targets = {action["target"]: action for action in payload["pending_actions"]}
    assert "documents.review.air_waybill_missing_but_flight_context_present" in targets
    assert "documents.review.packing_list_expected_but_mail_evidence_unclear" in targets
    assert "documents.review.commercial_invoice_expected_but_mail_evidence_unclear" in targets
    assert "documents.air_waybill" not in targets
    assert "documents.packing_list" not in targets
    assert "documents.commercial_invoice" not in targets


def test_build_tms_pending_updates_adds_precise_ocean_review_hint_for_missing_bl():
    payload = _build_tms_pending_updates(
        order_id="AN-OCEAN",
        tms_snapshot={
            "shipment_uuid": "uuid-AN-OCEAN",
            "shipment_number": "AN-OCEAN",
            "status": "confirmed",
            "detail": {
                "status": "confirmed",
                "network": "sea",
                "dates": {},
            },
        },
        history_rows=[
            {
                "subject": "AN-OCEAN vessel booking / BL pending",
                "body_text": "Ocean booking confirmed, vessel ETD updated, B/L still pending.",
                "sender": "sea@example.com",
            }
        ],
        document_registry={
            "missing_types": ["bill_of_lading"],
            "analysis_open_questions": [],
            "tms_documents": [],
            "tms_match_summary": [],
            "analyzed_documents": [],
            "received_documents": [],
        },
    )

    targets = {action["target"]: action for action in payload["pending_actions"]}
    assert "documents.review.bill_of_lading_missing_but_ocean_context_present" in targets
    assert "documents.bill_of_lading" not in targets


def test_augment_pending_updates_with_analysis_adds_status_review_hint_from_analysis_brief():
    pending_plan = {
        "action_summary": {
            "write_now": 0,
            "review": 2,
            "not_yet_due": 0,
            "not_yet_knowable": 0,
        },
        "pending_actions": [
            {
                "action_type": "document_gap",
                "target": "documents.air_waybill",
                "action_status": "review",
                "reason": "fehlend",
            },
            {
                "action_type": "document_gap",
                "target": "documents.packing_list",
                "action_status": "review",
                "reason": "fehlend",
            },
        ],
    }
    analysis_brief = {
        "internal_actions": [
            {"action": "TMS Status-Korrektur", "urgency": "high", "reason": "Status delivered ist falsch."},
        ],
        "risk_flags": [
            {"code": "DATA_INCONSISTENCY_STATUS", "severity": "high", "reason": "System zeigt Zustellung an, obwohl die Sendung noch in Klärung ist."},
        ],
        "ops_summary": "TMS delivered ist fachlich falsch.",
    }

    updated = _augment_pending_updates_with_analysis(pending_plan, analysis_brief)

    targets = {action["target"]: action for action in updated["pending_actions"]}
    assert "shipment.review.status_inconsistent_with_analysis" in targets
    assert targets["shipment.review.status_inconsistent_with_analysis"]["action_status"] == "review"
    assert targets["documents.air_waybill"]["action_status"] == "not_yet_knowable"
    assert targets["documents.packing_list"]["action_status"] == "not_yet_knowable"
    assert updated["action_summary"] == {
        "write_now": 0,
        "review": 1,
        "not_yet_due": 0,
        "not_yet_knowable": 2,
    }
