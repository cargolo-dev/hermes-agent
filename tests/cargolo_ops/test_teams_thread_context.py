from __future__ import annotations

from pathlib import Path

from plugins.cargolo_ops.teams_thread_context import (
    load_thread_context,
    record_inbound_message,
    record_outbound_response,
    resolve_followup_reference,
)


def test_records_inbound_explicit_order_as_last_case(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"

    ctx = record_inbound_message(root=root, chat_id="19:ops", message_id="m1", user_id="u1", user_name="Dominik", text="Was ist mit AN-11755?")

    assert ctx["last_order_id"] == "AN-11755"
    assert (root / "orders" / "AN-11755" / "teams" / "thread_context.json").exists()
    loaded = load_thread_context(root, "19:ops")
    assert loaded["last_user_message"]["text"] == "Was ist mit AN-11755?"


def test_records_outbound_hermes_answer(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    record_inbound_message(root=root, chat_id="19:ops", message_id="m1", text="Was ist mit AN-11755?")

    ctx = record_outbound_response(root=root, chat_id="19:ops", message_id="h1", reply_to_message_id="m1", text="Lage: sauber.")

    assert ctx["last_order_id"] == "AN-11755"
    assert ctx["last_hermes_response"]["text"] == "Lage: sauber."
    assert ctx["recent_messages"][-1]["role"] == "assistant"


def test_resolves_docs_followup_to_last_order(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    ctx = record_inbound_message(root=root, chat_id="19:ops", message_id="m1", text="Was ist mit AN-11755?")

    resolved = resolve_followup_reference("und was ist mit den Docs?", ctx)

    assert resolved["resolved"] is True
    assert resolved["order_id"] == "AN-11755"
    assert "documents" in resolved["needs"]


def test_does_not_resolve_without_context() -> None:
    resolved = resolve_followup_reference("und was ist mit den Docs?", {})

    assert resolved["resolved"] is False
