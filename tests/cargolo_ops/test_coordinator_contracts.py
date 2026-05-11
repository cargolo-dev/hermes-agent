from __future__ import annotations

import json
from pathlib import Path

from plugins.cargolo_ops.coordinator import CoordinatorDecision, CoordinatorIntent, handle_event
from plugins.cargolo_ops.coordinator_events import (
    CargoloOpsEvent,
    EventSource,
    EventType,
    normalize_cron_document_upload_event,
    normalize_teams_message_event,
)
from plugins.cargolo_ops.specialist_results import SpecialistResult, SpecialistStatus


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_normalize_teams_message_event_extracts_order_and_context() -> None:
    event = normalize_teams_message_event(
        text="@Hermes bitte prüfe AN-11755 komplett",
        conversation_id="conv-1",
        message_id="msg-1",
        from_user="Dominik",
        reply_to_message_id="quoted-1",
    )

    assert event.event_type == EventType.TEAMS_MESSAGE
    assert event.source == EventSource.TEAMS
    assert event.order_id == "AN-11755"
    assert event.text == "@Hermes bitte prüfe AN-11755 komplett"
    assert event.teams == {
        "conversation_id": "conv-1",
        "message_id": "msg-1",
        "reply_to_message_id": "quoted-1",
        "from_user": "Dominik",
    }
    assert event.requires_teams_send is False
    assert event.to_audit_row()["event_id"] == event.event_id


def test_normalize_cron_document_upload_event_is_internal_no_teams_send() -> None:
    event = normalize_cron_document_upload_event(
        order_id="an-11755",
        activity_event={"activity_id": 42, "file_name": "awb.pdf"},
        text="Neues Dokument awb.pdf",
    )

    assert event.event_type == EventType.CRON_DOCUMENT_UPLOAD
    assert event.source == EventSource.CRON
    assert event.order_id == "AN-11755"
    assert event.payload["activity_event"]["activity_id"] == 42
    assert event.requires_teams_send is False


def test_specialist_result_contract_serializes_write_intents_and_human_gate() -> None:
    result = SpecialistResult(
        agent="document_analyst",
        status=SpecialistStatus.NEEDS_REVIEW,
        confidence=0.87,
        summary="MRN passt nicht eindeutig zum TMS.",
        findings=[{"field": "mrn", "value": "26DE99999"}],
        risks=[{"severity": "medium", "text": "TMS-Abgleich nötig"}],
        recommended_actions=[{"action": "ask_operator", "text": "MRN übernehmen?"}],
        evidence_refs=["orders/AN-11755/docs/awb.pdf"],
        requires_human=True,
        write_intents=[{"system": "tms", "target": "customs_reference", "value": "26DE99999"}],
    )

    row = result.to_dict()
    assert row["agent"] == "document_analyst"
    assert row["status"] == "needs_review"
    assert row["requires_human"] is True
    assert row["write_intents"][0]["system"] == "tms"


def test_malformed_order_id_is_not_used_as_a_case_path(tmp_path: Path) -> None:
    event = CargoloOpsEvent(
        event_type=EventType.TEAMS_MESSAGE,
        source=EventSource.TEAMS,
        order_id="AN-../../x",
        text="malformed id should not become a path",
    )

    decision = handle_event(event, root=tmp_path / "cargolo_asr")

    assert event.order_id is None
    assert decision.order_id is None
    assert decision.audit_path == str(tmp_path / "cargolo_asr" / "runtime" / "coordinator" / "events.jsonl")
    assert not (tmp_path / "x").exists()


def test_event_audit_supports_context_refs_and_raw_ref_without_team_delivery() -> None:
    event = CargoloOpsEvent(
        event_type=EventType.WEBHOOK_INGEST,
        source=EventSource.WEBHOOK,
        order_id="AN-11755",
        text="webhook payload stored externally",
        context_refs=["orders/AN-11755/mail/history.json"],
        raw_ref="runtime/raw/webhook-1.json",
    )

    row = event.to_audit_row()

    assert row["context_refs"] == ["orders/AN-11755/mail/history.json"]
    assert row["raw_ref"] == "runtime/raw/webhook-1.json"
    assert row["requires_teams_send"] is False


def test_coordinator_skeleton_audits_event_and_never_sends_to_teams(tmp_path: Path) -> None:
    event = CargoloOpsEvent(
        event_type=EventType.TEAMS_MESSAGE,
        source=EventSource.TEAMS,
        order_id="AN-11755",
        text="prüfe AN-11755 komplett",
        teams={"conversation_id": "conv-1", "message_id": "msg-1"},
    )

    decision = handle_event(event, root=tmp_path / "cargolo_asr")

    assert decision.decision == CoordinatorDecision.ROUTE_TO_SPECIALISTS
    assert decision.order_id == "AN-11755"
    assert decision.should_send_to_teams is False
    assert decision.requires_human is False
    assert decision.specialist_tasks
    audit_rows = _read_jsonl(tmp_path / "cargolo_asr" / "orders" / "AN-11755" / "coordinator" / "events.jsonl")
    assert audit_rows[0]["event_type"] == "teams_message"
    assert audit_rows[0]["source"] == "teams"
    assert audit_rows[0]["order_id"] == "AN-11755"


def test_coordinator_routes_status_request_without_queue_or_team_delivery(tmp_path: Path) -> None:
    event = normalize_teams_message_event(
        text="@Hermes status AN-11755",
        conversation_id="conv-1",
        message_id="msg-status",
    )

    decision = handle_event(event, root=tmp_path / "cargolo_asr")

    assert decision.intent == CoordinatorIntent.STATUS_REQUEST
    assert decision.decision == CoordinatorDecision.ROUTE_TO_SPECIALISTS
    assert decision.should_send_to_teams is False
    assert [task["agent"] for task in decision.specialist_tasks] == ["case_context", "tms_snapshot"]
    assert not (tmp_path / "cargolo_asr" / "orders" / "AN-11755" / "coordinator" / "pending_tasks.jsonl").exists()


def test_coordinator_routes_case_deep_dive_to_pending_task_queue(tmp_path: Path) -> None:
    event = normalize_teams_message_event(
        text="@Hermes prüfe AN-11755 komplett mit Mail-Historie und Dokumenten",
        conversation_id="conv-1",
        message_id="msg-deep",
        from_user="Dominik",
    )

    decision = handle_event(event, root=tmp_path / "cargolo_asr", enqueue_tasks=True)

    assert decision.intent == CoordinatorIntent.CASE_DEEP_DIVE
    assert decision.decision == CoordinatorDecision.ROUTE_TO_SPECIALISTS
    assert decision.should_send_to_teams is False
    assert [task["agent"] for task in decision.specialist_tasks] == ["case_context", "document_analyst", "mail_history", "tms_snapshot"]
    queue_rows = _read_jsonl(tmp_path / "cargolo_asr" / "orders" / "AN-11755" / "coordinator" / "pending_tasks.jsonl")
    assert len(queue_rows) == 4
    assert {row["event_id"] for row in queue_rows} == {event.event_id}
    assert all(row["status"] == "pending" for row in queue_rows)
    assert all(row["mode"] == "read_only" for row in queue_rows)
    assert all(row["order_id"] == "AN-11755" for row in queue_rows)
    assert all(row["should_send_to_teams"] is False for row in queue_rows)


def test_coordinator_tms_status_request_remains_read_only_status_routing(tmp_path: Path) -> None:
    event = normalize_teams_message_event(
        text="Bitte TMS Status AN-11755 prüfen",
        conversation_id="conv-1",
        message_id="msg-tms-status",
    )

    decision = handle_event(event, root=tmp_path / "cargolo_asr", enqueue_tasks=True)

    assert decision.intent == CoordinatorIntent.STATUS_REQUEST
    assert decision.decision == CoordinatorDecision.ROUTE_TO_SPECIALISTS
    assert decision.requires_human is False
    assert decision.should_send_to_teams is False
    assert [task["agent"] for task in decision.specialist_tasks] == ["case_context", "tms_snapshot"]


def test_coordinator_guards_tms_write_like_free_text_as_human_approval(tmp_path: Path) -> None:
    event = normalize_teams_message_event(
        text="AN-11755 bitte MRN 26DE99999 ins TMS eintragen",
        conversation_id="conv-1",
        message_id="msg-tms",
    )

    decision = handle_event(event, root=tmp_path / "cargolo_asr", enqueue_tasks=True)

    assert decision.intent == CoordinatorIntent.TMS_WRITE_INTENT
    assert decision.decision == CoordinatorDecision.ASK_HUMAN
    assert decision.requires_human is True
    assert decision.should_send_to_teams is False
    assert decision.specialist_tasks == []
    queue_path = tmp_path / "cargolo_asr" / "orders" / "AN-11755" / "coordinator" / "pending_tasks.jsonl"
    assert not queue_path.exists()


def test_coordinator_unknown_teams_message_asks_one_question_without_live_send(tmp_path: Path) -> None:
    event = normalize_teams_message_event(
        text="Kannst du das bitte anschauen?",
        conversation_id="conv-1",
        message_id="msg-unknown",
    )

    decision = handle_event(event, root=tmp_path / "cargolo_asr")

    assert decision.intent == CoordinatorIntent.UNKNOWN
    assert decision.decision == CoordinatorDecision.ASK_HUMAN
    assert decision.requires_human is True
    assert decision.should_send_to_teams is False
    assert decision.response_text
    assert decision.response_text.count("?") == 1


def test_coordinator_cron_document_upload_can_enqueue_internal_document_task(tmp_path: Path) -> None:
    event = normalize_cron_document_upload_event(
        order_id="AN-11755",
        activity_event={"activity_id": 100, "file_name": "awb.pdf"},
    )

    decision = handle_event(event, root=tmp_path / "cargolo_asr", enqueue_tasks=True)

    assert decision.intent == CoordinatorIntent.DOCUMENT_UPLOAD
    assert decision.decision == CoordinatorDecision.RECORD_INTERNAL_EVENT
    assert decision.should_send_to_teams is False
    assert [task["agent"] for task in decision.specialist_tasks] == ["document_analyst", "tms_snapshot"]
    rows = _read_jsonl(tmp_path / "cargolo_asr" / "orders" / "AN-11755" / "coordinator" / "pending_tasks.jsonl")
    assert [row["agent"] for row in rows] == ["document_analyst", "tms_snapshot"]


def test_coordinator_cron_event_records_internal_decision_without_team_delivery(tmp_path: Path) -> None:
    event = normalize_cron_document_upload_event(
        order_id="AN-11755",
        activity_event={"activity_id": 99, "file_name": "ci.pdf"},
    )

    decision = handle_event(event, root=tmp_path / "cargolo_asr")

    assert decision.decision == CoordinatorDecision.RECORD_INTERNAL_EVENT
    assert decision.should_send_to_teams is False
    assert decision.response_text is None
    rows = _read_jsonl(tmp_path / "cargolo_asr" / "orders" / "AN-11755" / "coordinator" / "events.jsonl")
    assert rows[0]["payload"]["activity_event"]["file_name"] == "ci.pdf"
