"""CARGOLO Teams-first coordinator skeleton.

This module is deliberately conservative: it records normalized events and
returns decisions for callers.  It does **not** send Teams messages, perform TMS
writes, or start customer-facing actions.
"""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .coordinator_events import CargoloOpsEvent, EventType
from .models import utc_now_iso


class CoordinatorDecision(str, Enum):
    ROUTE_TO_SPECIALISTS = "route_to_specialists"
    RECORD_INTERNAL_EVENT = "record_internal_event"
    ASK_HUMAN = "ask_human"
    IGNORE = "ignore"


class CoordinatorIntent(str, Enum):
    STATUS_REQUEST = "status_request"
    CASE_DEEP_DIVE = "case_deep_dive"
    DOCUMENT_UPLOAD = "document_upload"
    TMS_WRITE_INTENT = "tms_write_intent"
    UNKNOWN = "unknown"


class CoordinatorResult(BaseModel):
    model_config = ConfigDict(extra="allow", use_enum_values=False)

    decision: CoordinatorDecision
    intent: CoordinatorIntent = CoordinatorIntent.UNKNOWN
    order_id: str | None = None
    response_text: str | None = None
    should_send_to_teams: bool = False
    requires_human: bool = False
    specialist_tasks: list[dict[str, Any]] = Field(default_factory=list)
    audit_path: str | None = None
    queue_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision.value,
            "intent": self.intent.value,
            "order_id": self.order_id,
            "response_text": self.response_text,
            "should_send_to_teams": self.should_send_to_teams,
            "requires_human": self.requires_human,
            "specialist_tasks": self.specialist_tasks,
            "audit_path": self.audit_path,
            "queue_path": self.queue_path,
        }


def _default_root() -> Path:
    return Path.home() / ".hermes" / "cargolo_asr"


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _audit_path(root: Path, order_id: str | None) -> Path:
    if order_id:
        return root / "orders" / order_id / "coordinator" / "events.jsonl"
    return root / "runtime" / "coordinator" / "events.jsonl"


def _queue_path(root: Path, order_id: str | None) -> Path:
    if order_id:
        return root / "orders" / order_id / "coordinator" / "pending_tasks.jsonl"
    return root / "runtime" / "coordinator" / "pending_tasks.jsonl"


def _append_pending_tasks(path: Path, *, event: CargoloOpsEvent, intent: CoordinatorIntent, tasks: list[dict[str, Any]]) -> None:
    for index, task in enumerate(tasks, start=1):
        row = {
            "timestamp": utc_now_iso(),
            "status": "pending",
            "event_id": event.event_id,
            "event_type": event.event_type.value,
            "intent": intent.value,
            "order_id": event.order_id,
            "task_id": f"{event.event_id}:{index}:{task.get('agent', 'specialist')}",
            "agent": task.get("agent"),
            "mode": "read_only",
            "task": {**task, "mode": "read_only"},
            "should_send_to_teams": False,
        }
        _append_jsonl(path, row)


def _has_any(text: str, tokens: tuple[str, ...]) -> bool:
    return any(token in text for token in tokens)


def _classify_intent(event: CargoloOpsEvent) -> CoordinatorIntent:
    text = (event.text or "").lower()
    if event.event_type == EventType.CRON_DOCUMENT_UPLOAD:
        return CoordinatorIntent.DOCUMENT_UPLOAD
    if event.event_type in {EventType.TEAMS_MESSAGE, EventType.TEAMS_REPLY, EventType.TEAMS_BUTTON}:
        write_semantics = _has_any(text, ("eintragen", "schreib", "write", "ändern", "aendern", "übernehmen", "uebernehmen"))
        tms_context = "tms" in text
        if tms_context and write_semantics:
            return CoordinatorIntent.TMS_WRITE_INTENT
        if _has_any(text, ("status", "lage", "stand", "wo stehen", "update")):
            return CoordinatorIntent.STATUS_REQUEST
        if write_semantics:
            return CoordinatorIntent.TMS_WRITE_INTENT
        if _has_any(text, ("prüfe", "pruefe", "komplett", "fall", "case", "dokument", "doc", "mail", "historie", "awb", "ci", "pl", "tms")):
            return CoordinatorIntent.CASE_DEEP_DIVE
    return CoordinatorIntent.UNKNOWN


def _task(agent: str, event: CargoloOpsEvent, *, purpose: str, priority: int = 50) -> dict[str, Any]:
    return {
        "agent": agent,
        "mode": "read_only",
        "order_id": event.order_id,
        "purpose": purpose,
        "priority": priority,
        "event_id": event.event_id,
        "context_refs": event.context_refs,
        "raw_ref": event.raw_ref,
    }


def _specialist_tasks_for(event: CargoloOpsEvent, intent: CoordinatorIntent) -> list[dict[str, Any]]:
    text = (event.text or "").lower()
    if intent == CoordinatorIntent.STATUS_REQUEST:
        return [
            _task("case_context", event, purpose="summarize current case state", priority=10),
            _task("tms_snapshot", event, purpose="read current shipment/TMS status", priority=20),
        ]
    if intent == CoordinatorIntent.CASE_DEEP_DIVE:
        tasks = [_task("case_context", event, purpose="collect case folder context", priority=10)]
        if _has_any(text, ("dokument", "doc", "awb", "ci", "pl", "komplett", "prüfe", "pruefe")):
            tasks.append(_task("document_analyst", event, purpose="analyze document state and discrepancies", priority=20))
        if _has_any(text, ("mail", "historie", "kunde", "antwort", "komplett")):
            tasks.append(_task("mail_history", event, purpose="summarize latest ASR mail history", priority=30))
        tasks.append(_task("tms_snapshot", event, purpose="compare read-only TMS state", priority=40))
        return tasks
    if intent == CoordinatorIntent.DOCUMENT_UPLOAD:
        return [
            _task("document_analyst", event, purpose="analyze newly detected document upload", priority=10),
            _task("tms_snapshot", event, purpose="read TMS state for document comparison", priority=20),
        ]
    return []


def handle_event(
    event: CargoloOpsEvent,
    *,
    root: Path | None = None,
    enqueue_tasks: bool = False,
) -> CoordinatorResult:
    """Record an event and return the next coordinator decision.

    Safety invariant for this foundation stage: ``should_send_to_teams`` is
    always false.  Callers may later render a response/card only after an
    explicit integration step and approval policy.
    """

    case_root = root or _default_root()
    audit_path = _audit_path(case_root, event.order_id)
    _append_jsonl(audit_path, event.to_audit_row())

    intent = _classify_intent(event)
    tasks = _specialist_tasks_for(event, intent)
    queue_path = _queue_path(case_root, event.order_id) if tasks and enqueue_tasks else None
    if queue_path is not None:
        _append_pending_tasks(queue_path, event=event, intent=intent, tasks=tasks)

    if intent == CoordinatorIntent.DOCUMENT_UPLOAD:
        return CoordinatorResult(
            decision=CoordinatorDecision.RECORD_INTERNAL_EVENT,
            intent=intent,
            order_id=event.order_id,
            should_send_to_teams=False,
            requires_human=False,
            specialist_tasks=tasks,
            audit_path=str(audit_path),
            queue_path=str(queue_path) if queue_path else None,
        )

    if intent == CoordinatorIntent.TMS_WRITE_INTENT:
        return CoordinatorResult(
            decision=CoordinatorDecision.ASK_HUMAN,
            intent=intent,
            order_id=event.order_id,
            response_text="Ich brauche die konkrete Review-/Approval-Karte, bevor ich TMS-Änderungen vorbereite?",
            should_send_to_teams=False,
            requires_human=True,
            audit_path=str(audit_path),
        )

    if tasks:
        return CoordinatorResult(
            decision=CoordinatorDecision.ROUTE_TO_SPECIALISTS,
            intent=intent,
            order_id=event.order_id,
            should_send_to_teams=False,
            requires_human=False,
            specialist_tasks=tasks,
            audit_path=str(audit_path),
            queue_path=str(queue_path) if queue_path else None,
        )

    if event.event_type in {EventType.TEAMS_MESSAGE, EventType.TEAMS_REPLY, EventType.TEAMS_BUTTON}:
        return CoordinatorResult(
            decision=CoordinatorDecision.ASK_HUMAN,
            intent=CoordinatorIntent.UNKNOWN,
            order_id=event.order_id,
            response_text="Welche AN/BU soll ich prüfen?",
            should_send_to_teams=False,
            requires_human=True,
            audit_path=str(audit_path),
        )

    return CoordinatorResult(
        decision=CoordinatorDecision.IGNORE,
        intent=intent,
        order_id=event.order_id,
        should_send_to_teams=False,
        requires_human=False,
        audit_path=str(audit_path),
    )
