"""Normalized CARGOLO coordinator event contracts.

These contracts are intentionally side-effect free.  They only normalize incoming
Teams/cron/webhook/manual signals into one envelope; delivery back to Teams is a
separate coordinator decision.
"""

from __future__ import annotations

import hashlib
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .models import ORDER_ID_RE, normalize_order_ids, utc_now_iso


class EventType(str, Enum):
    TEAMS_MESSAGE = "teams_message"
    TEAMS_REPLY = "teams_reply"
    TEAMS_BUTTON = "teams_button"
    CRON_DOCUMENT_UPLOAD = "cron_document_upload"
    CRON_HEALTH = "cron_health"
    WEBHOOK_INGEST = "webhook_ingest"
    MANUAL_CHECK = "manual_check"


class EventSource(str, Enum):
    TEAMS = "teams"
    CRON = "cron"
    WEBHOOK = "webhook"
    TELEGRAM_ADMIN = "telegram_admin"
    INTERNAL = "internal"


def _stable_event_id(*parts: Any) -> str:
    raw = "\n".join(str(part or "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _normalize_order_id(value: str | None, *fallbacks: str | None) -> str | None:
    candidates = normalize_order_ids(value, *fallbacks)
    if candidates:
        return candidates[0]
    if value:
        raw = str(value).strip().upper()
        if ORDER_ID_RE.fullmatch(raw):
            return raw
    return None


class CargoloOpsEvent(BaseModel):
    """One normalized input envelope for Teams, cron, webhook, and manual events."""

    model_config = ConfigDict(extra="allow", use_enum_values=False)

    event_type: EventType
    source: EventSource
    event_id: str | None = None
    order_id: str | None = None
    text: str = ""
    received_at: str = Field(default_factory=utc_now_iso)
    teams: dict[str, Any] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)
    context_refs: list[str] = Field(default_factory=list)
    raw_ref: str | None = None
    requires_teams_send: bool = False

    @field_validator("order_id", mode="before")
    @classmethod
    def normalize_order(cls, value: Any) -> str | None:
        return _normalize_order_id(str(value) if value is not None else None)

    def model_post_init(self, __context: Any) -> None:
        if not self.order_id:
            object.__setattr__(self, "order_id", _normalize_order_id(None, self.text, repr(self.payload)))
        if not self.event_id:
            object.__setattr__(
                self,
                "event_id",
                _stable_event_id(
                    self.source.value,
                    self.event_type.value,
                    self.order_id,
                    self.text,
                    self.teams.get("message_id"),
                    self.teams.get("reply_to_message_id"),
                    self.payload,
                ),
            )

    def to_audit_row(self) -> dict[str, Any]:
        return {
            "timestamp": utc_now_iso(),
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "source": self.source.value,
            "order_id": self.order_id,
            "text": self.text,
            "received_at": self.received_at,
            "teams": self.teams,
            "payload": self.payload,
            "context_refs": self.context_refs,
            "raw_ref": self.raw_ref,
            "requires_teams_send": self.requires_teams_send,
        }


def normalize_teams_message_event(
    *,
    text: str,
    conversation_id: str | None = None,
    message_id: str | None = None,
    from_user: str | None = None,
    reply_to_message_id: str | None = None,
    payload: dict[str, Any] | None = None,
    event_type: EventType = EventType.TEAMS_MESSAGE,
) -> CargoloOpsEvent:
    teams = {
        "conversation_id": conversation_id,
        "message_id": message_id,
        "reply_to_message_id": reply_to_message_id,
        "from_user": from_user,
    }
    teams = {key: value for key, value in teams.items() if value is not None}
    return CargoloOpsEvent(
        event_type=event_type,
        source=EventSource.TEAMS,
        order_id=_normalize_order_id(None, text, repr(payload or {})),
        text=text or "",
        teams=teams,
        payload=payload or {},
        requires_teams_send=False,
    )


def normalize_cron_document_upload_event(
    *,
    order_id: str | None,
    activity_event: dict[str, Any],
    text: str | None = None,
    payload: dict[str, Any] | None = None,
) -> CargoloOpsEvent:
    merged = dict(payload or {})
    merged["activity_event"] = activity_event or {}
    return CargoloOpsEvent(
        event_type=EventType.CRON_DOCUMENT_UPLOAD,
        source=EventSource.CRON,
        order_id=_normalize_order_id(order_id, repr(activity_event)),
        text=text or "",
        payload=merged,
        requires_teams_send=False,
    )
