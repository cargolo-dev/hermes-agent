"""Honcho memory bridge contracts for CARGOLO employee runtime.

The runtime treats Honcho as contextual memory, never as an action authority.
Honcho availability is optional: if the session/tool is unavailable, the agent
continues safely with local context and records the degraded state.
"""

from __future__ import annotations

from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field


class HonchoMemorySource(Protocol):
    """Small adapter protocol for controller-side Honcho tool access.

    Production/controller code can wrap `honcho_profile`, `honcho_search`, and
    `honcho_context` tools behind this shape.  The plugin runtime receives only
    the resulting `HonchoMemorySnapshot`, so Honcho stays optional and testable.
    """

    def profile(self, *, peer: str = "user") -> dict[str, Any]: ...

    def search(self, query: str, *, peer: str = "user", max_tokens: int = 800) -> dict[str, Any]: ...

    def context(self, query: str | None = None, *, peer: str = "user") -> dict[str, Any]: ...


class HonchoMemorySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    available: bool = False
    peer: str = "user"
    facts: list[str] = Field(default_factory=list)
    excerpts: list[str] = Field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "peer": self.peer,
            "facts": self.facts,
            "excerpts": self.excerpts,
            "error": self.error,
        }


def unavailable_honcho_snapshot(error: str | None = None) -> HonchoMemorySnapshot:
    return HonchoMemorySnapshot(available=False, error=error)


def _request_text(request: Any) -> str:
    return str(getattr(request, "text", "") or "")


def _build_query(request: Any) -> str:
    text = _request_text(request)
    return " ".join(
        part
        for part in (
            "CARGOLO Hermes employee agent memory",
            text,
            "agent-first normal Hermes functions guardrails Teams TMS customer draft",
        )
        if part
    )


def _unwrap_result_payload(payload: Any) -> Any:
    if isinstance(payload, dict) and set(payload) == {"result"}:
        return payload["result"]
    if isinstance(payload, dict) and "result" in payload and not any(
        key in payload for key in ("card", "facts", "results", "summary", "peer_card", "data", "matches")
    ):
        return payload["result"]
    return payload


def _payload_error(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("error"):
        return _clean_item(payload.get("error"), max_chars=500)
    if payload.get("success") is False:
        return _clean_item(payload.get("message") or payload.get("reason") or payload, max_chars=500)
    return None


def _clean_item(value: Any, *, max_chars: int) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple, set)):
        value = str(value)
    text = " ".join(str(value).split())
    if not text:
        return None
    return text[:max_chars]


def _add_limited(target: list[str], value: Any, *, max_items: int, max_chars: int) -> None:
    if len(target) >= max_items:
        return
    text = _clean_item(value, max_chars=max_chars)
    if text and text not in target:
        target.append(text)


def _collect_profile_facts(profile_payload: Any, *, max_facts: int, max_chars: int) -> list[str]:
    profile_payload = _unwrap_result_payload(profile_payload)
    facts: list[str] = []
    if isinstance(profile_payload, list):
        for item in profile_payload:
            _add_limited(facts, item, max_items=max_facts, max_chars=max_chars)
        return facts
    if not isinstance(profile_payload, dict):
        _add_limited(facts, profile_payload, max_items=max_facts, max_chars=max_chars)
        return facts
    for key in ("card", "facts", "conclusions", "profile", "peer_card"):
        value = profile_payload.get(key)
        if isinstance(value, list):
            for item in value:
                _add_limited(facts, item, max_items=max_facts, max_chars=max_chars)
        else:
            _add_limited(facts, value, max_items=max_facts, max_chars=max_chars)
    return facts


def _collect_search_excerpts(search_payload: Any, *, max_excerpts: int, max_chars: int) -> list[str]:
    search_payload = _unwrap_result_payload(search_payload)
    excerpts: list[str] = []
    if isinstance(search_payload, list):
        values = search_payload
    elif isinstance(search_payload, dict):
        values = search_payload.get("results") or search_payload.get("data") or search_payload.get("matches") or []
    else:
        values = [search_payload]
    if isinstance(values, dict):
        values = values.values()
    if not isinstance(values, list):
        values = [values]
    for item in values:
        if isinstance(item, dict):
            for key in ("text", "content", "excerpt", "summary"):
                if key in item:
                    _add_limited(excerpts, item[key], max_items=max_excerpts, max_chars=max_chars)
                    break
        else:
            _add_limited(excerpts, item, max_items=max_excerpts, max_chars=max_chars)
    return excerpts


def _collect_context_items(context_payload: Any, *, max_facts: int, max_excerpts: int, max_chars: int) -> tuple[list[str], list[str]]:
    context_payload = _unwrap_result_payload(context_payload)
    facts: list[str] = []
    excerpts: list[str] = []
    if not isinstance(context_payload, dict):
        _add_limited(excerpts, context_payload, max_items=max_excerpts, max_chars=max_chars)
        return facts, excerpts
    for key in ("peer_card", "card", "facts"):
        value = context_payload.get(key)
        if isinstance(value, list):
            for item in value:
                _add_limited(facts, item, max_items=max_facts, max_chars=max_chars)
        else:
            _add_limited(facts, value, max_items=max_facts, max_chars=max_chars)
    for key in ("summary", "recent_messages", "context", "session_summary"):
        value = context_payload.get(key)
        if isinstance(value, list):
            for item in value:
                _add_limited(excerpts, item, max_items=max_excerpts, max_chars=max_chars)
        else:
            _add_limited(excerpts, value, max_items=max_excerpts, max_chars=max_chars)
    return facts, excerpts


def build_honcho_memory_snapshot(
    request: Any,
    *,
    source: HonchoMemorySource,
    peer: str = "user",
    max_facts: int = 8,
    max_excerpts: int = 6,
    max_chars_per_item: int = 500,
) -> HonchoMemorySnapshot:
    """Build a bounded contextual Honcho snapshot for employee runtime.

    The snapshot intentionally contains only memory content.  It never carries
    action flags, approvals, send permissions, or write permissions.
    """

    try:
        query = _build_query(request)
        profile_payload = source.profile(peer=peer) or {}
        error = _payload_error(profile_payload)
        if error:
            return unavailable_honcho_snapshot(error)

        search_payload = source.search(query, peer=peer, max_tokens=800) or {}
        error = _payload_error(search_payload)
        if error:
            return unavailable_honcho_snapshot(error)

        context_payload = source.context(query, peer=peer) or {}
        error = _payload_error(context_payload)
        if error:
            return unavailable_honcho_snapshot(error)

        facts: list[str] = []
        excerpts: list[str] = []
        for item in _collect_profile_facts(profile_payload, max_facts=max_facts, max_chars=max_chars_per_item):
            _add_limited(facts, item, max_items=max_facts, max_chars=max_chars_per_item)
        context_facts, context_excerpts = _collect_context_items(
            context_payload,
            max_facts=max_facts,
            max_excerpts=max_excerpts,
            max_chars=max_chars_per_item,
        )
        for item in context_facts:
            _add_limited(facts, item, max_items=max_facts, max_chars=max_chars_per_item)
        for item in _collect_search_excerpts(search_payload, max_excerpts=max_excerpts, max_chars=max_chars_per_item):
            _add_limited(excerpts, item, max_items=max_excerpts, max_chars=max_chars_per_item)
        for item in context_excerpts:
            _add_limited(excerpts, item, max_items=max_excerpts, max_chars=max_chars_per_item)

        return HonchoMemorySnapshot(available=True, peer=peer, facts=facts, excerpts=excerpts)
    except Exception as exc:  # noqa: BLE001 - Honcho must never block CARGOLO runtime.
        return unavailable_honcho_snapshot(str(exc))
