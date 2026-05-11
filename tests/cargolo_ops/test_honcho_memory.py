from __future__ import annotations

import pytest
from pydantic import ValidationError

from plugins.cargolo_ops.employee_agent import EmployeeRequest
from plugins.cargolo_ops.honcho_memory import HonchoMemorySnapshot, build_honcho_memory_snapshot


class FakeHonchoSource:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []

    def profile(self, *, peer: str = "user") -> dict:
        self.calls.append(("profile", peer))
        return {"card": ["User wants CARGOLO to stay agent-first.", "User has no Teams access right now."]}

    def search(self, query: str, *, peer: str = "user", max_tokens: int = 800) -> dict:
        self.calls.append(("search", query))
        return {"results": [{"text": "normal Hermes functions must keep working"}, {"content": "guardrails only at boundaries"}]}

    def context(self, query: str | None = None, *, peer: str = "user") -> dict:
        self.calls.append(("context", query))
        return {"summary": "CARGOLO employee runtime with optional Honcho memory.", "peer_card": ["German default."]}


class FailingHonchoSource:
    def profile(self, *, peer: str = "user") -> dict:
        raise RuntimeError("Honcho session could not be initialized.")


class ErrorPayloadHonchoSource:
    def profile(self, *, peer: str = "user") -> dict:
        return {"error": "Honcho session could not be initialized."}

    def search(self, query: str, *, peer: str = "user", max_tokens: int = 800) -> dict:
        raise AssertionError("search should not be called after error payload")

    def context(self, query: str | None = None, *, peer: str = "user") -> dict:
        raise AssertionError("context should not be called after error payload")


class ResultWrappedHonchoSource:
    def profile(self, *, peer: str = "user") -> dict:
        return {"result": {"card": ["Agent-first CARGOLO employee, not rigid bot."]}}

    def search(self, query: str, *, peer: str = "user", max_tokens: int = 800) -> dict:
        return {"result": [{"text": "Teams sends require explicit guard."}]}

    def context(self, query: str | None = None, *, peer: str = "user") -> dict:
        return {"result": {"summary": "Normal Hermes chat remains available.", "peer_card": ["German default."]}}


class NoisyHonchoSource:
    def profile(self, *, peer: str = "user") -> dict:
        return {"card": ["x" * 1000 for _ in range(10)]}

    def search(self, query: str, *, peer: str = "user", max_tokens: int = 800) -> dict:
        return {"results": [{"text": "y" * 1000} for _ in range(10)]}

    def context(self, query: str | None = None, *, peer: str = "user") -> dict:
        return {"summary": "z" * 1000}


def test_build_honcho_memory_snapshot_collects_profile_search_and_context() -> None:
    source = FakeHonchoSource()
    request = EmployeeRequest(text="Was ist mit AN-11755 los?", channel="telegram")

    snapshot = build_honcho_memory_snapshot(request, source=source)

    assert snapshot.available is True
    assert snapshot.peer == "user"
    assert "User wants CARGOLO to stay agent-first." in snapshot.facts
    assert "German default." in snapshot.facts
    assert "normal Hermes functions must keep working" in snapshot.excerpts
    assert any(call[0] == "profile" for call in source.calls)
    assert any(call[0] == "search" and "AN-11755" in (call[1] or "") for call in source.calls)
    assert any(call[0] == "context" for call in source.calls)
    assert snapshot.error is None


def test_build_honcho_memory_snapshot_is_safe_when_honcho_fails() -> None:
    snapshot = build_honcho_memory_snapshot(
        EmployeeRequest(text="Was ist mit AN-11755 los?", channel="telegram"),
        source=FailingHonchoSource(),
    )

    assert snapshot.available is False
    assert snapshot.facts == []
    assert snapshot.excerpts == []
    assert "Honcho session could not be initialized" in (snapshot.error or "")


def test_build_honcho_memory_snapshot_unwraps_native_result_payloads() -> None:
    snapshot = build_honcho_memory_snapshot(
        EmployeeRequest(text="Was ist mit AN-11755 los?", channel="telegram"),
        source=ResultWrappedHonchoSource(),
    )

    assert snapshot.available is True
    assert "Agent-first CARGOLO employee, not rigid bot." in snapshot.facts
    assert "German default." in snapshot.facts
    assert "Teams sends require explicit guard." in snapshot.excerpts
    assert "Normal Hermes chat remains available." in snapshot.excerpts


def test_build_honcho_memory_snapshot_treats_error_payload_as_unavailable() -> None:
    snapshot = build_honcho_memory_snapshot(
        EmployeeRequest(text="Was ist mit AN-11755 los?", channel="telegram"),
        source=ErrorPayloadHonchoSource(),
    )

    assert snapshot.available is False
    assert snapshot.facts == []
    assert snapshot.excerpts == []
    assert "Honcho session could not be initialized" in (snapshot.error or "")


def test_honcho_memory_snapshot_rejects_action_authority_extras() -> None:
    with pytest.raises(ValidationError):
        HonchoMemorySnapshot(available=True, should_send_to_teams=True)


def test_build_honcho_memory_snapshot_is_context_only_not_action_authority() -> None:
    snapshot = build_honcho_memory_snapshot(
        EmployeeRequest(text="Poste das Update zu AN-11755 in Teams", channel="telegram"),
        source=FakeHonchoSource(),
    )

    row = snapshot.to_dict()
    assert set(row) == {"available", "peer", "facts", "excerpts", "error"}
    assert "should_send_to_teams" not in row
    assert "should_write_tms" not in row
    assert "approval" not in row


def test_build_honcho_memory_snapshot_limits_context_size() -> None:
    snapshot = build_honcho_memory_snapshot(
        EmployeeRequest(text="Was ist mit AN-11755 los?", channel="telegram"),
        source=NoisyHonchoSource(),
        max_facts=3,
        max_excerpts=2,
        max_chars_per_item=80,
    )

    assert snapshot.available is True
    assert len(snapshot.facts) <= 3
    assert len(snapshot.excerpts) <= 2
    assert all(len(item) <= 80 for item in snapshot.facts + snapshot.excerpts)
