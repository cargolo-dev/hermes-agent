from __future__ import annotations

import json
from pathlib import Path

from plugins.cargolo_ops.teams_employee_handoff import TeamsHandoffConfig, handle_teams_employee_message


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_dedicated_channel_routes_case_assist_to_agent_after_evidence_refresh(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cargolo_asr"
    calls = []

    def fake_handoff(**kwargs):
        calls.append(kwargs)
        return {
            "handled": False,
            "allow_generic_chat": True,
            "classification": "case_evidence_refreshed_agent_handoff",
            "order_id": kwargs["order_id"],
            "agent_prompt": "frisch synchronisiert AN-11755",
            "case_path": str(root / "orders" / kwargs["order_id"]),
            "lifecycle": {"status": "ok"},
            "should_send_to_teams": False,
            "should_write_tms": False,
            "should_send_customer_message": False,
        }

    monkeypatch.setattr("plugins.cargolo_ops.teams_employee_handoff.build_case_evidence_agent_handoff", fake_handoff)

    result = handle_teams_employee_message(
        root=root,
        text="Was ist mit AN-11755 los? Schau in Mails, TMS und Docs.",
        channel_id="cargolo-hermes",
        message_id="teams-msg-1",
        user_id="aad-1",
        user_name="Ops",
        config=TeamsHandoffConfig(dedicated_channel_ids={"cargolo-hermes"}),
    )

    assert result["handled"] is False
    assert result["handoff_mode"] == "dedicated_channel"
    assert result["requires_mention"] is False
    assert result["order_id"] == "AN-11755"
    assert result["allow_generic_chat"] is True
    assert result["classification"] == "case_evidence_refreshed_agent_handoff"
    assert result["agent_prompt"] == "frisch synchronisiert AN-11755"
    assert result["response_text"] is None
    assert result["should_send_to_teams"] is False
    assert result["should_write_tms"] is False
    assert result["should_send_customer_message"] is False
    assert calls[0]["order_id"] == "AN-11755"

    audit_rows = _read_jsonl(root / "runtime" / "teams_employee_handoff.jsonl")
    assert audit_rows[-1]["message_id"] == "teams-msg-1"
    assert audit_rows[-1]["handoff_mode"] == "dedicated_channel"
    assert audit_rows[-1]["handled"] is False


def test_dedicated_channel_free_chat_falls_through_to_generic_hermes(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"

    result = handle_teams_employee_message(
        root=root,
        text="erzähl mal einen witz",
        channel_id="cargolo-hermes",
        message_id="teams-msg-free-chat",
        user_id="aad-1",
        user_name="Ops",
        config=TeamsHandoffConfig(dedicated_channel_ids={"cargolo-hermes"}),
    )

    assert result["handled"] is False
    assert result["reason"] == "generic_hermes_chat"
    assert result["classification"] == "free_chat"
    assert result["passthrough_text"] == "erzähl mal einen witz"
    assert result["allow_generic_chat"] is True
    assert "Rolle: Du bist Hermes CARGOLO" in result["agent_prompt"]
    assert "Teams-Nachricht: erzähl mal einen witz" in result["agent_prompt"]
    assert result["response_text"] is None
    assert result["should_write_tms"] is False
    assert result["should_send_customer_message"] is False

    audit_rows = _read_jsonl(root / "runtime" / "teams_employee_handoff.jsonl")
    assert audit_rows[-1]["handled"] is False
    assert audit_rows[-1]["reason"] == "generic_hermes_chat"
    assert audit_rows[-1]["allow_generic_chat"] is True


def test_shared_channel_ignores_unmentioned_message(tmp_path: Path) -> None:
    result = handle_teams_employee_message(
        root=tmp_path / "cargolo_asr",
        text="Was ist mit AN-11755 los?",
        channel_id="shared-ops",
        message_id="teams-msg-2",
        config=TeamsHandoffConfig(dedicated_channel_ids={"cargolo-hermes"}),
    )

    assert result == {"handled": False, "reason": "mention_required", "requires_mention": True}


def test_shared_channel_does_not_treat_mention_prefix_embedded_in_word_as_mention(tmp_path: Path) -> None:
    result = handle_teams_employee_message(
        root=tmp_path / "cargolo_asr",
        text="@HermesFoo Was ist mit AN-11755 los?",
        channel_id="shared-ops",
        message_id="teams-msg-embedded",
        config=TeamsHandoffConfig(dedicated_channel_ids={"cargolo-hermes"}),
    )

    assert result == {"handled": False, "reason": "mention_required", "requires_mention": True}


def test_shared_channel_routes_when_mentioned_and_strips_mention(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755" / "case_summary.json").parent.mkdir(parents=True)
    (root / "orders" / "AN-11755" / "case_summary.json").write_text(
        json.dumps({"shipment_number": "AN-11755", "mode": "Air", "lane": "FRA -> JFK"}),
        encoding="utf-8",
    )

    result = handle_teams_employee_message(
        root=root,
        text="@Hermes CARGOLO Was ist mit AN-11755 los?",
        channel_id="shared-ops",
        message_id="teams-msg-3",
        config=TeamsHandoffConfig(dedicated_channel_ids={"cargolo-hermes"}),
    )

    assert result["handled"] is False
    assert result["handoff_mode"] == "mention"
    assert result["request_text"] == "Was ist mit AN-11755 los?"
    assert result["allow_generic_chat"] is True
    assert "AN-11755" in result["agent_prompt"]
    assert result["response_text"] is None
    assert result["should_send_to_teams"] is False


def test_dedicated_channel_tms_write_request_remains_guarded_without_external_action(tmp_path: Path) -> None:
    result = handle_teams_employee_message(
        root=tmp_path / "cargolo_asr",
        text="Setze MRN 26DE99999 in AN-11755 im TMS",
        channel_id="cargolo-hermes",
        message_id="teams-msg-4",
        config=TeamsHandoffConfig(dedicated_channel_ids={"cargolo-hermes"}),
    )

    assert result["handled"] is True
    assert result["classification"] == "guarded_action_required"
    assert "Nicht geschrieben" in result["response_text"]
    assert "kein TMS-Write" in result["response_text"]
    assert result["should_send_to_teams"] is False
    assert result["should_write_tms"] is False
    assert result["should_send_customer_message"] is False
    assert not (tmp_path / "cargolo_asr" / "orders" / "AN-11755" / "employee" / "review_required.json").exists()
