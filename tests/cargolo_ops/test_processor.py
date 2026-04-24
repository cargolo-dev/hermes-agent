import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from plugins.cargolo_ops.models import CaseState, TMSSnapshot
from plugins.cargolo_ops.processor import _add_transport_internal_note, _build_transport_internal_note, process_email_event
from plugins.cargolo_ops.storage import CaseStore


def sample_payload():
    return {
        "event_type": "asr_email_thread",
        "mailbox": "asr@cargolo.com",
        "an": "AN-10874",
        "trigger_message_id": "<msg-1@example.com>",
        "trigger_conversation_id": "thread-1",
        "message_count": 1,
        "messages": [
            {
                "message_id": "<msg-1@example.com>",
                "conversation_id": "thread-1",
                "subject": "Status update AN-10874 delayed",
                "from": "customer@example.com",
                "to": ["asr@cargolo.com"],
                "cc": [],
                "received_at": "2026-04-10T08:00:00Z",
                "body_text": "AN-10874 has delay at port. Please confirm ETA and attach invoice.",
                "attachments": [
                    {
                        "filename": "invoice.pdf",
                        "mime_type": "application/pdf",
                        "content_base64": "aGVsbG8=",
                    }
                ],
                "attachment_count": 1,
                "has_attachments": True,
            }
        ],
    }


def test_process_event_creates_case_files(tmp_path):
    result = process_email_event(sample_payload(), storage_root=tmp_path, create_task=True, refresh_history=False)
    case_root = tmp_path / "orders" / "AN-10874"
    assert result.status == "processed"
    assert result.initialized is True
    assert case_root.exists()
    assert (case_root / "case_state.json").exists()
    assert (case_root / "entities.json").exists()
    assert (case_root / "emails/raw").exists()
    assert (case_root / "emails/normalized").exists()
    assert (case_root / "emails/drafts").exists()
    assert (case_root / "documents/inbound/invoice.pdf").exists()
    state = json.loads((case_root / "case_state.json").read_text(encoding="utf-8"))
    assert state["order_id"] == "AN-10874"
    assert state["task_recommended"] is True
    assert state["next_best_action"] != "Create follow-up task"
    assert state["task_reason"] != ""


def test_tms_transport_mode_overrides_signature_air_sea_text(tmp_path):
    payload = sample_payload()
    payload["an"] = "BU-4664"
    payload["messages"][0]["message_id"] = "<msg-bu-4664@example.com>"
    payload["trigger_message_id"] = "<msg-bu-4664@example.com>"
    payload["messages"][0]["subject"] = "WG: CARGOLO Transportauftrag: BU-4664 // Seefracht Standard // Nordvant"
    payload["messages"][0]["body_text"] = (
        "Können Sie uns bitte den Preis geben?\n\n"
        "Freundliche Grüße\n"
        "i. A. Sandra Henneken\n"
        "Import Air & Sea\n"
        "BU-4664 Seefracht Standard"
    )
    tms_snapshot = TMSSnapshot(
        order_id="BU-4664",
        shipment_uuid="uuid-bu-4664",
        shipment_number="BU-4664",
        source="live",
        status="confirmed",
        detail={"id": "uuid-bu-4664", "network": "sea", "transport_mode": "sea", "documents": []},
        billing_items=[],
        warnings=[],
    )

    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(tms_snapshot, {}, {})):
        result = process_email_event(payload, storage_root=tmp_path, refresh_history=False)

    assert result.status == "processed"
    state = json.loads((tmp_path / "orders" / "BU-4664" / "case_state.json").read_text(encoding="utf-8"))
    entities = json.loads((tmp_path / "orders" / "BU-4664" / "entities.json").read_text(encoding="utf-8"))
    assert "air" in entities["transport_mode_candidates"]  # signature still mentions Import Air & Sea
    assert state["mode"] == "sea"


def test_initial_mail_history_failure_skips_remaining_pipeline(tmp_path):
    payload = sample_payload()
    tms_snapshot = TMSSnapshot(
        order_id="AN-10874",
        shipment_uuid="uuid-10874",
        shipment_number="AN-10874",
        source="live",
        status="confirmed",
        detail={"id": "uuid-10874", "network": "sea", "transport_mode": "sea", "documents": []},
        billing_items=[],
        warnings=[],
    )

    with patch("plugins.cargolo_ops.processor._fetch_tms_bundle", return_value=(tms_snapshot, {}, {})), \
         patch("plugins.cargolo_ops.processor._sync_mail_history", side_effect=TimeoutError("n8n mail history timed out")), \
         patch("plugins.cargolo_ops.processor.analyze_case_documents") as mock_analyze:
        result = process_email_event(payload, storage_root=tmp_path, refresh_history=True)

    assert result.status == "skipped"
    assert result.suppress_delivery is True
    assert result.history_sync_status == "failed"
    assert "mail_history_sync_failed" in (result.history_sync_error or "")
    assert "Mailhistory" in result.message
    mock_analyze.assert_not_called()
    case_root = tmp_path / "orders" / "AN-10874"
    assert (case_root / "tms_snapshot.json").exists()
    assert not (case_root / "analysis" / "case_report_latest.json").exists()
    audit = (case_root / "audit" / "actions.jsonl").read_text(encoding="utf-8")
    assert "initial_mail_history_sync_failed" in audit


def test_process_event_is_idempotent_for_duplicate_messages(tmp_path):
    first = process_email_event(sample_payload(), storage_root=tmp_path, refresh_history=False)
    second = process_email_event(sample_payload(), storage_root=tmp_path, refresh_history=False)
    assert first.status == "processed"
    assert second.duplicate is True
    rows = (tmp_path / "orders" / "AN-10874" / "email_index.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(rows) == 1


def test_missing_an_goes_to_review_queue(tmp_path):
    payload = sample_payload()
    payload.pop("an")
    payload["messages"][0]["subject"] = "No clear reference"
    payload["messages"][0]["body_text"] = "Need help with shipment"
    result = process_email_event(payload, storage_root=tmp_path, refresh_history=False)
    assert result.review_required is True
    assert result.suppress_delivery is True
    assert list((tmp_path / "review_queue").glob("*.json"))


def test_bu_reference_creates_independent_bu_case(tmp_path):
    payload = sample_payload()
    payload.pop("an")
    payload["bu"] = "BU-4638"
    payload["messages"][0]["message_id"] = "<msg-bu@example.com>"
    payload["trigger_message_id"] = "<msg-bu@example.com>"
    payload["messages"][0]["subject"] = "Status update BU-4638 delayed"
    payload["messages"][0]["body_text"] = "BU-4638 has delay at port."

    result = process_email_event(payload, storage_root=tmp_path, refresh_history=False)

    assert result.status == "processed"
    assert result.order_id == "BU-4638"
    assert (tmp_path / "orders" / "BU-4638").exists()
    assert not (tmp_path / "orders" / "AN-10874").exists()


def test_trigger_message_reference_wins_over_top_level_an_in_processing(tmp_path):
    payload = sample_payload()
    payload["an"] = "AN-10874"
    payload["trigger_message_id"] = "<msg-bu@example.com>"
    payload["messages"] = [
        {
            **payload["messages"][0],
            "message_id": "<msg-old@example.com>",
            "subject": "Old thread item for AN-10874",
            "body_text": "AN-10874 historical context only.",
        },
        {
            **payload["messages"][0],
            "message_id": "<msg-bu@example.com>",
            "subject": "Status update BU-4638 delayed",
            "body_text": "BU-4638 has delay at port.",
        },
    ]

    with patch("plugins.cargolo_ops.processor.build_tms_provider_from_env", return_value=None):
        result = process_email_event(payload, storage_root=tmp_path, refresh_history=False)

    assert result.status == "processed"
    assert result.order_id == "BU-4638"
    assert (tmp_path / "orders" / "BU-4638").exists()
    assert not (tmp_path / "orders" / "AN-10874").exists()


def test_order_id_not_found_in_live_shipment_list_is_skipped(tmp_path):
    payload = sample_payload()
    live_provider = MagicMock()
    live_provider.shipments_list.return_value = []

    with patch("plugins.cargolo_ops.processor.build_tms_provider_from_env", return_value=live_provider):
        result = process_email_event(
            payload,
            storage_root=tmp_path,
            refresh_history=False,
            enforce_live_shipment_check=True,
        )

    assert result.status == "skipped"
    assert result.order_id == "AN-10874"
    assert result.suppress_delivery is True
    assert not (tmp_path / "orders" / "AN-10874").exists()
    queued = list((tmp_path / "review_queue").glob("*.json"))
    assert queued
    saved_payload = json.loads(queued[0].read_text(encoding="utf-8"))
    assert "not found in ASR shipment list" in saved_payload["reason"]


def test_add_transport_internal_note_requires_explicit_env_opt_in(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_CARGOLO_ASR_ENABLE_TMS_INTERNAL_NOTES", raising=False)
    store = CaseStore(tmp_path)
    provider = MagicMock()

    with patch("plugins.cargolo_ops.processor.build_tms_write_provider_from_env", return_value=provider):
        result = _add_transport_internal_note(store, "AN-10874", "Test note", source_key="process_event:test")

    assert result["status"] == "skipped"
    assert result["error"] == "tms_internal_notes_disabled"
    provider.add_internal_note.assert_not_called()


def test_attachment_name_collisions_do_not_overwrite(tmp_path):
    payload = sample_payload()
    process_email_event(payload, storage_root=tmp_path, refresh_history=False)
    payload2 = sample_payload()
    payload2["trigger_message_id"] = "<msg-2@example.com>"
    payload2["messages"][0]["message_id"] = "<msg-2@example.com>"
    payload2["messages"][0]["body_text"] = "AN-10874 delayed again"
    payload2["messages"][0]["attachments"][0]["content_base64"] = "d29ybGQ="
    process_email_event(payload2, storage_root=tmp_path, refresh_history=False)
    files = sorted((tmp_path / "orders" / "AN-10874" / "documents/inbound").glob("invoice*.pdf"))
    assert len(files) == 2


def test_process_event_auto_applies_write_now_actions_for_current_case(tmp_path):
    payload = sample_payload()
    tms_snapshot = TMSSnapshot(
        order_id="AN-10874",
        shipment_uuid="uuid-10874",
        shipment_number="AN-10874",
        source="live",
        status="in_transit",
        detail={"id": "uuid-10874", "status": "in_transit", "documents": [], "dates": {}},
        billing_items=[],
        warnings=[],
    )
    pending_updates_payload = {
        "version": 1,
        "generated_at": "2026-04-20T08:00:00Z",
        "order_id": "AN-10874",
        "shipment_uuid": "uuid-10874",
        "shipment_number": "AN-10874",
        "status": "pending_write_access",
        "requires_write_access": True,
        "received_types": ["commercial_invoice"],
        "expected_types": ["commercial_invoice"],
        "missing_types": [],
        "document_matches": [],
        "field_update_candidates": [],
        "open_questions": [],
        "action_summary": {"write_now": 2, "review": 0, "not_yet_due": 0, "not_yet_knowable": 0},
        "pending_actions": [
            {
                "action_type": "status_update",
                "target": "shipment.status",
                "suggested_value": "delivered",
                "source": "tms.detail.dates.actual_delivery_date",
                "reason": "actual_delivery_date gesetzt",
                "requires_write_access": True,
                "action_status": "write_now",
            },
            {
                "action_type": "document_upload",
                "target": "documents.commercial_invoice",
                "document_type": "commercial_invoice",
                "file_name": "invoice.pdf",
                "source_path": str(tmp_path / "orders" / "AN-10874" / "documents" / "inbound" / "invoice.pdf"),
                "mime_type": "application/pdf",
                "suggested_value": "upload_local_case_document_to_tms",
                "source": "documents/inbound/invoice.pdf",
                "reason": "liegt lokal vor",
                "requires_write_access": True,
                "action_status": "write_now",
            },
        ],
    }
    applied_calls: list[dict] = []

    def _fake_apply(action, context, *, admin_user_id=106):
        applied_calls.append({
            "action": dict(action),
            "context": dict(context),
            "admin_user_id": admin_user_id,
        })
        return {"status": "applied", "executed_tool": f"fake_{action['action_type']}"}

    with patch(
        "plugins.cargolo_ops.processor._fetch_tms_bundle",
        return_value=(tms_snapshot, {}, {}),
    ), patch(
        "plugins.cargolo_ops.processor._build_tms_pending_updates",
        return_value=pending_updates_payload,
    ), patch(
        "plugins.cargolo_ops.processor.apply_pending_tms_action",
        side_effect=_fake_apply,
    ), patch(
        "plugins.cargolo_ops.processor._add_transport_internal_note",
        return_value={"status": "applied", "preview": "kurzer transportkommentar", "error": None},
    ):
        result = process_email_event(payload, storage_root=tmp_path, refresh_history=False, write_internal_note=True)

    assert result.status == "processed"
    assert result.latest_subject == "Status update AN-10874 delayed"
    assert result.latest_sender == "customer@example.com"
    assert result.attachment_count == 1
    assert result.pending_action_summary == {"write_now": 2, "review": 0, "not_yet_due": 0, "not_yet_knowable": 0}
    assert result.applied_action_summary == {"applied": 2, "failed": 0, "skipped": 0}
    assert result.applied_action_targets == ["shipment.status", "documents.commercial_invoice"]
    assert result.applied_action_details[0] == "shipment.status; → delivered; Grund: actual_delivery_date gesetzt; Quelle: tms.detail.dates.actual_delivery_date"
    assert "documents.commercial_invoice" in result.applied_action_details[1]
    assert "→ upload_local_case_document_to_tms" in result.applied_action_details[1]
    assert "Grund: liegt lokal vor" in result.applied_action_details[1]
    assert result.internal_note_status == "applied"
    assert result.internal_note_preview == "kurzer transportkommentar"
    assert [call["action"]["action_type"] for call in applied_calls] == ["status_update", "document_upload"]
    assert all(call["context"]["order_id"] == "AN-10874" for call in applied_calls)

    applied_updates = json.loads(
        (tmp_path / "orders" / "AN-10874" / "tms" / "applied_updates.json").read_text(encoding="utf-8")
    )
    assert applied_updates["status"] == "applied"
    assert len(applied_updates["applied_actions"]) == 2
    assert applied_updates["failed_actions"] == []
    assert applied_updates["skipped_actions"] == []


def test_process_event_uploads_documents_found_only_in_mail_history(tmp_path):
    payload = sample_payload()
    payload["messages"][0]["message_id"] = "<acceptance@example.com>"
    payload["trigger_message_id"] = "<acceptance@example.com>"
    payload["messages"][0]["subject"] = "Angebot AN-10874 angenommen – CARGOLO"
    payload["messages"][0]["body_text"] = "Angebot angenommen."
    payload["messages"][0]["attachments"] = []
    payload["messages"][0]["attachment_count"] = 0
    payload["messages"][0]["has_attachments"] = False

    case_root = tmp_path / "orders" / "AN-10874"
    inbound_dir = case_root / "documents" / "inbound"
    inbound_dir.mkdir(parents=True)
    history_doc = inbound_dir / "packing list and invoice-06IWM260203.pdf"
    history_doc.write_bytes(b"pdf bytes")
    store = CaseStore(tmp_path)
    store.append_email_index("AN-10874", {
        "message_id": "<history@example.com>",
        "subject": "AN-10874 // Rail LCL CN-DE // Cnee: Duschkraft",
        "sender": "shipper@example.com",
        "received_at": "2026-04-23T08:06:34Z",
        "stored_paths": [str(history_doc)],
        "classification": "history_sync",
        "linked_order_id": "AN-10874",
        "dedupe_hash": "history-hash",
    })

    tms_snapshot = TMSSnapshot(
        order_id="AN-10874",
        shipment_uuid="uuid-10874",
        shipment_number="AN-10874",
        source="live",
        status="addresses_pending",
        detail={"id": "uuid-10874", "status": "addresses_pending", "network": "rail", "documents": [], "dates": {}},
        billing_items=[],
        warnings=[],
    )
    requirements = {
        "status": "ok",
        "expected_types": ["commercial_invoice", "packing_list", "customs_document"],
        "documents": [],
    }
    applied_calls: list[dict] = []

    def _fake_apply(action, context, *, admin_user_id=106):
        applied_calls.append({"action": dict(action), "context": dict(context)})
        return {"status": "applied", "executed_tool": "fake_upload"}

    with patch(
        "plugins.cargolo_ops.processor._fetch_tms_bundle",
        return_value=(tms_snapshot, requirements, {}),
    ), patch(
        "plugins.cargolo_ops.processor.apply_pending_tms_action",
        side_effect=_fake_apply,
    ), patch(
        "plugins.cargolo_ops.processor._add_transport_internal_note",
        return_value={"status": "applied", "preview": "kurzer transportkommentar", "error": None},
    ):
        result = process_email_event(payload, storage_root=tmp_path, refresh_history=False, write_internal_note=True)

    assert result.status == "processed"
    registry = json.loads((case_root / "documents" / "registry.json").read_text(encoding="utf-8"))
    assert registry["received_types"] == ["commercial_invoice", "packing_list"]
    assert registry["missing_types"] == ["customs_document"]
    assert {call["action"]["document_type"] for call in applied_calls} == {"commercial_invoice", "packing_list"}
    assert all(call["action"]["source_path"] == str(history_doc) for call in applied_calls)

    pending = json.loads((case_root / "tms" / "pending_updates.json").read_text(encoding="utf-8"))
    write_now_targets = {row["target"] for row in pending["pending_actions"] if row.get("action_status") == "write_now"}
    assert write_now_targets >= {"documents.commercial_invoice", "documents.packing_list"}
    assert result.pending_action_summary["write_now"] == 2
    assert result.applied_action_summary == {"applied": 2, "failed": 0, "skipped": 0}


def test_build_transport_internal_note_is_human_readable():
    note = _build_transport_internal_note(
        order_id="AN-TEST",
        run_type="bootstrap_case",
        tms_snapshot={
            "status": "confirmed",
            "detail": {
                "network": "rail",
                "origin": {"city": "Shenzhen"},
                "destination": {"city": "Delbrück"},
            },
        },
        state=CaseState(order_id="AN-TEST", next_best_action="Commercial Invoice nachfordern"),
        pending_summary={"write_now": 1, "review": 2, "not_yet_due": 0, "not_yet_knowable": 0},
        applied_summary={"applied": 1, "failed": 0, "skipped": 0},
        applied_targets=["shipment.dates.latest_delivery_date; → 2026-05-09; Grund: latest_delivery_date war älter als ETA"],
        history_sync_count=3,
        history_sync_status="ok",
        history_sync_error=None,
        latest_subject="AN-TEST booking confirmed",
        analysis_summary="Zollunterlagen sind noch unvollständig",
    )

    assert "Initialer Stand zu AN-TEST:" in note
    assert "Offen:" in note
    assert "TMS-Rückmeldung:" in note
    assert "Übernommen:" in note
    assert "→ 2026-05-09" in note
    assert "Grund: latest_delivery_date war älter als ETA" in note
    assert "Nächster Schritt:" in note

    assert "Einschätzung:" not in note
    assert "W:1" not in note


def test_build_transport_internal_note_uses_analysis_and_avoids_generic_document_step():
    analysis_summary = (
        "FTL-Transport (17,3t Stahl-Schalung) von Paderborn (DE) nach Bazenheid (CH). "
        "Die Sendung wurde am 23.04. planmäßig verladen (Carrier: Hartmann International, LKW WGM5880H). "
        "Aktuell besteht eine Diskrepanz zwischen der operativen Realität (In-Transit) und dem TMS-Status ('addresses_pending'). "
        "CMR, Rechnung und Lieferschein liegen vor."
    )
    note = _build_transport_internal_note(
        order_id="AN-12317",
        run_type="process_event",
        tms_snapshot={
            "status": "addresses_pending",
            "detail": {
                "network": "road",
                "origin": {"city": "Paderborn"},
                "destination": {"city": "x"},
            },
        },
        state=CaseState(
            order_id="AN-12317",
            next_best_action="Gebündelte Dokumentqualitäts-Hinweise prüfen und entscheiden, ob Nachforderung, Klarstellung oder Ignorieren angemessen ist",
            open_questions=["Genaue Empfängeradresse in CH (nur 'x' im TMS, 'Bazenheid' im Dokument)"],
        ),
        pending_summary={"write_now": 0, "review": 1, "not_yet_due": 0, "not_yet_knowable": 0},
        applied_summary={"applied": 0, "failed": 0, "skipped": 0},
        applied_targets=[],
        history_sync_count=5,
        history_sync_status="ok",
        history_sync_error=None,
        latest_subject="AW: AN-12317 // FTL DE-CH",
        analysis_summary=analysis_summary,
        pending_actions=[
            {
                "action_type": "review_hint",
                "target": "shipment.review.status_inconsistent_with_analysis",
                "action_status": "review",
            }
        ],
    )

    assert "FTL-Transport (17,3t Stahl-Schalung)" in note
    assert "Route Paderborn → Bazenheid" in note
    assert "TMS-Ziel war Platzhalter 'x'" in note
    assert "Mail +5" in note
    assert "Offen: 1 Review" in note
    assert "TMS-Status gegen Mailverlauf prüfen" in note
    assert "Gebündelte Dokumentqualitäts-Hinweise" not in note
