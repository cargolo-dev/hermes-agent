from __future__ import annotations

import json
from pathlib import Path

from plugins.cargolo_ops.teams_employee_handoff import TeamsHandoffConfig, handle_teams_employee_message


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_dedicated_channel_routes_without_mention_to_employee_runtime(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755").mkdir(parents=True)

    result = handle_teams_employee_message(
        root=root,
        text="Was ist mit AN-11755 los? Schau in Mails, TMS und Docs.",
        channel_id="cargolo-hermes",
        message_id="teams-msg-1",
        user_id="aad-1",
        user_name="Ops",
        config=TeamsHandoffConfig(dedicated_channel_ids={"cargolo-hermes"}),
    )

    assert result["handled"] is True
    assert result["handoff_mode"] == "dedicated_channel"
    assert result["requires_mention"] is False
    assert result["order_id"] == "AN-11755"
    assert result["response_text"].startswith("<div><h2>🔎 Fallprüfung AN-11755")
    assert result["should_send_to_teams"] is False
    assert result["should_write_tms"] is False
    assert result["should_send_customer_message"] is False
    assert (root / "orders" / "AN-11755" / "employee" / "review_required.json").exists()

    audit_rows = _read_jsonl(root / "runtime" / "teams_employee_handoff.jsonl")
    assert audit_rows[-1]["message_id"] == "teams-msg-1"
    assert audit_rows[-1]["handoff_mode"] == "dedicated_channel"


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


def test_paperclip_bridge_case_assist_creates_chef_handoff(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755").mkdir(parents=True)
    calls = []

    def fake_bridge(**kwargs):
        calls.append(kwargs)
        return {
            "handled": True,
            "classification": "paperclip_case_assist",
            "handoff_target": "paperclip_chef",
            "response_text": "Paperclip Chef hat übernommen.",
            "paperclip_issue_id": "issue-1",
            "paperclip_issue_identifier": "PC-1",
            "should_send_to_teams": False,
            "should_write_tms": False,
            "should_send_customer_message": False,
        }

    monkeypatch.setattr(
        "plugins.cargolo_ops.teams_employee_handoff.handle_paperclip_teams_case_assist",
        fake_bridge,
    )

    result = handle_teams_employee_message(
        root=root,
        text="Was ist mit AN-11755 los? Schau in Mails, TMS und Docs.",
        channel_id="cargolo-hermes",
        message_id="teams-msg-paperclip",
        user_id="aad-1",
        user_name="Ops",
        config=TeamsHandoffConfig(
            dedicated_channel_ids={"cargolo-hermes"},
            paperclip_bridge_enabled=True,
            paperclip_wait_timeout_seconds=0,
        ),
    )

    assert result["handled"] is True
    assert result["classification"] == "paperclip_case_assist"
    assert result["handoff_target"] == "paperclip_chef"
    assert result["response_text"] == "Paperclip Chef hat übernommen."
    assert result["paperclip_issue_id"] == "issue-1"
    assert result["should_write_tms"] is False
    assert result["should_send_customer_message"] is False
    assert calls[0]["request"].text == "Was ist mit AN-11755 los? Schau in Mails, TMS und Docs."
    assert calls[0]["response"].mode.value == "case_assist"
    assert calls[0]["config"].enabled is True
    assert calls[0]["config"].chef_agent_id == "23685acf-9da7-4504-b496-66260c51293b"

    audit_rows = _read_jsonl(root / "runtime" / "teams_employee_handoff.jsonl")
    assert audit_rows[-1]["classification"] == "paperclip_case_assist"
    assert audit_rows[-1]["paperclip_bridge_enabled"] is True


def test_paperclip_bridge_free_chat_stays_plain_generic_hermes(tmp_path: Path) -> None:
    result = handle_teams_employee_message(
        root=tmp_path / "cargolo_asr",
        text="erzähl mal einen witz",
        channel_id="cargolo-hermes",
        message_id="teams-msg-free-chat-bridge",
        config=TeamsHandoffConfig(
            dedicated_channel_ids={"cargolo-hermes"},
            paperclip_bridge_enabled=True,
        ),
    )

    assert result["handled"] is False
    assert result["classification"] == "free_chat"
    assert result["allow_generic_chat"] is True
    assert result["passthrough_text"] == "erzähl mal einen witz"
    assert result["agent_prompt"] is None


def test_paperclip_bridge_does_not_receive_guarded_tms_write(tmp_path: Path, monkeypatch) -> None:
    def fail_bridge(**kwargs):
        raise AssertionError("guarded write must not create a Paperclip issue")

    monkeypatch.setattr(
        "plugins.cargolo_ops.teams_employee_handoff.handle_paperclip_teams_case_assist",
        fail_bridge,
    )

    result = handle_teams_employee_message(
        root=tmp_path / "cargolo_asr",
        text="Setze MRN 26DE99999 in AN-11755 im TMS",
        channel_id="cargolo-hermes",
        message_id="teams-msg-guarded-bridge",
        config=TeamsHandoffConfig(
            dedicated_channel_ids={"cargolo-hermes"},
            paperclip_bridge_enabled=True,
        ),
    )

    assert result["handled"] is True
    assert result["classification"] == "guarded_action_required"
    assert "TMS Guard erforderlich" in result["response_text"]
    assert result["should_write_tms"] is False
    assert result["should_send_customer_message"] is False


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

    assert result["handled"] is True
    assert result["handoff_mode"] == "mention"
    assert result["request_text"] == "Was ist mit AN-11755 los?"
    assert "Air / FRA -&gt; JFK" in result["response_text"]
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
    assert "TMS Guard erforderlich" in result["response_text"]
    assert result["should_send_to_teams"] is False
    assert result["should_write_tms"] is False
    assert result["should_send_customer_message"] is False
    assert not (tmp_path / "cargolo_asr" / "orders" / "AN-11755" / "employee" / "review_required.json").exists()
