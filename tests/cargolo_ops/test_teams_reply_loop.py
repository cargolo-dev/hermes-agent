from __future__ import annotations

import json
from pathlib import Path

from plugins.cargolo_ops.teams_reply_loop import (
    _default_tms_verify,
    build_card_context,
    handle_teams_message,
    process_teams_tms_card_action,
    record_sent_card,
)


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_default_tms_verify_reads_snapshot_bundle_detail(monkeypatch) -> None:
    class FakeSnapshot:
        detail = {"freight_details": {"container_number": "CIMU1670214"}}

    class FakeProvider:
        def snapshot_bundle(self, an: str, customer_hint: str | None = None) -> FakeSnapshot:
            assert an == "AN-12218"
            return FakeSnapshot()

    from plugins.cargolo_ops import tms_provider

    monkeypatch.setattr(tms_provider, "build_tms_provider_from_env", lambda: FakeProvider())

    assert _default_tms_verify("AN-12218", "container_number") == "CIMU1670214"


def test_record_sent_card_persists_context_and_message_index(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    context = build_card_context(
        route_name="cargolo-asr-ops-teams",
        delivery_id="delivery-1",
        payload={
            "processor_result": {"order_id": "AN-11755"},
            "activity_event": {"id": 1200, "metadata": {"document_type": "tax_assessment"}},
        },
        message_id="teams-msg-1",
        chat_id="teams-chat-1",
    )

    record_sent_card(root=root, context=context)

    cards = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "cards.jsonl")
    assert cards[-1]["context_id"] == "AN-11755:1200:delivery-1"
    assert cards[-1]["message_id"] == "teams-msg-1"
    assert cards[-1]["activity_id"] == 1200
    assert cards[-1]["document_type"] == "tax_assessment"

    index = json.loads((root / "runtime" / "teams_card_index.json").read_text(encoding="utf-8"))
    assert index["by_message_id"]["teams-msg-1"]["order_id"] == "AN-11755"


def test_build_card_context_unwraps_manual_ops_notification_body(tmp_path: Path) -> None:
    context = build_card_context(
        route_name="cargolo-asr-ops-teams",
        delivery_id="delivery-outer",
        payload={
            "event_type": "cargolo_asr_manual_ops_notification",
            "payload": {
                "processor_result": {"order_id": "AN-11755"},
                "activity_event": {
                    "id": "live-1",
                    "metadata": {"file_name": "test.pdf", "document_type": "tax_assessment"},
                },
            },
        },
        message_id="teams-msg-live",
        chat_id="teams-chat-live",
    )

    assert context["order_id"] == "AN-11755"
    assert context["activity_id"] == "live-1"
    assert context["file_name"] == "test.pdf"
    assert context["document_type"] == "tax_assessment"


def test_handle_reply_to_known_card_writes_learning_and_response(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    context = build_card_context(
        route_name="cargolo-asr-ops-teams",
        delivery_id="delivery-1",
        payload={"processor_result": {"order_id": "AN-11755"}, "activity_event": {"id": 1200}},
        message_id="teams-msg-1",
        chat_id="teams-chat-1",
    )
    record_sent_card(root=root, context=context)

    result = handle_teams_message(
        root=root,
        text="Ja, bitte TMS mit MRN 26DE123 aktualisieren und interne Notiz setzen.",
        chat_id="teams-chat-1",
        user_id="aad-1",
        user_name="Julian Hainer",
        message_id="reply-1",
        reply_to_message_id="teams-msg-1",
    )

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["order_id"] == "AN-11755"
    assert result["classification"] == "agent_decision_required"
    assert "cargolo_asr_record_teams_tms_intent" in result["agent_prompt"]
    assert "ASR Ops Coordinator" in result["agent_prompt"]
    assert "proaktiv" in result["agent_prompt"].lower()
    assert "Unsicherheit" in result["agent_prompt"]
    assert "Case-Learning" in result["agent_prompt"]

    replies = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "replies.jsonl")
    assert replies[-1]["message_id"] == "reply-1"
    assert replies[-1]["derived_action"]["type"] == "agent_tms_intent_candidate"
    assert replies[-1]["classification"] == "agent_decision_required"

    learnings = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "case_learning.jsonl")
    assert learnings[-1]["source"] == "teams_reply"
    assert learnings[-1]["operator"] == "Julian Hainer"


def test_handle_reply_with_context_marker_matches_exact_card_without_reply_id(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    context = build_card_context(
        route_name="cargolo-asr-ops-teams",
        delivery_id="delivery-ctx",
        payload={"processor_result": {"order_id": "AN-11755"}, "activity_event": {"id": 1201}},
        message_id="teams-msg-ctx",
        chat_id="teams-chat-1",
    )
    record_sent_card(root=root, context=context)

    result = handle_teams_message(
        root=root,
        text=f"ASRCTX:{context['context_id']} passt so",
        chat_id="teams-chat-1",
        user_id="aad-2",
        user_name="Ops",
        message_id="reply-ctx",
    )

    assert result["handled"] is True
    assert result["context_id"] == "AN-11755:1201:delivery-ctx"
    replies = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "replies.jsonl")
    assert replies[-1]["context_match"] == "context_marker"


def test_tms_update_reply_creates_pending_action_queue_entry(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    context = build_card_context(
        route_name="cargolo-asr-ops-teams",
        delivery_id="delivery-queue",
        payload={"processor_result": {"order_id": "AN-11755"}, "activity_event": {"id": 1202}},
        message_id="teams-msg-queue",
        chat_id="teams-chat-1",
    )
    record_sent_card(root=root, context=context)

    result = handle_teams_message(
        root=root,
        text="Bitte TMS MRN 26DE12345 eintragen.",
        chat_id="teams-chat-1",
        user_id="aad-3",
        user_name="Ops",
        message_id="reply-queue",
        reply_to_message_id="teams-msg-queue",
    )

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["derived_action"]["type"] == "agent_tms_intent_candidate"
    assert result["derived_action"]["target_candidate"] == "customs_reference"
    assert result["derived_action"]["value_candidate"] == "26DE12345"
    assert "Bitte TMS MRN 26DE12345 eintragen" in result["agent_prompt"]

    assert not (root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl").exists()


def test_handle_message_with_order_reference_without_reply_context(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755").mkdir(parents=True)

    result = handle_teams_message(
        root=root,
        text="AN-11755 passt so, Dokument ist korrekt.",
        chat_id="teams-chat-1",
        user_id="aad-1",
        user_name="Ops",
        message_id="reply-2",
    )

    assert result["handled"] is True
    assert result["order_id"] == "AN-11755"
    assert result["classification"] == "confirmation"
    assert "bestätigt" in result["response_text"].lower()


def test_unrelated_teams_message_is_not_intercepted(tmp_path: Path) -> None:
    result = handle_teams_message(
        root=tmp_path / "cargolo_asr",
        text="Guten Morgen zusammen",
        chat_id="teams-chat-1",
        user_id="aad-1",
        user_name="Ops",
        message_id="reply-3",
    )

    assert result["handled"] is False


def test_quoted_teams_mention_with_display_name_glued_to_order_id_is_handled(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    context = build_card_context(
        route_name="cargolo-asr-ops-teams",
        delivery_id="delivery-quoted",
        payload={"processor_result": {"order_id": "AN-11755"}, "activity_event": {"id": 1205}},
        message_id="teams-msg-quoted",
        chat_id="teams-chat-1",
    )
    record_sent_card(root=root, context=context)

    # Live Teams quote+mention payloads can concatenate the displayed sender name
    # directly with the quoted card text, e.g. "Display NameAN-11755 ...".
    result = handle_teams_message(
        root=root,
        text="Display NameAN-11755 | teams_reply_loop_livetest\nBitte TMS MRN 26DE12345 eintragen",
        chat_id="teams-chat-1",
        user_id="aad-4",
        user_name="Ops",
        message_id="reply-quoted",
    )

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["order_id"] == "AN-11755"
    assert result["classification"] == "agent_decision_required"
    assert result["derived_action"]["target_candidate"] == "customs_reference"
    assert result["derived_action"]["value_candidate"] == "26DE12345"
    assert not (root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl").exists()


def test_quoted_card_old_mrn_does_not_override_operator_requested_mrn(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    context = build_card_context(
        route_name="cargolo-asr-ops-teams",
        delivery_id="delivery-mrn-tail",
        payload={"processor_result": {"order_id": "AN-11755"}, "activity_event": {"id": 1301}},
        message_id="card-mrn-tail",
        chat_id="teams-chat",
    )
    record_sent_card(context=context, root=root)

    result = handle_teams_message(
        root=root,
        text=(
            "Display NameAN-11755 | teams_reply_safety_guard_livetest | Lage: "
            "Aktuelle MRN im TMS: 26DE12345. Teste Reply-Loop mit neuer MRN 26DE99999\n"
            "Bitte TMS MRN 26DE99999 eintragen"
        ),
        chat_id="teams-chat",
        user_name="Operator",
        message_id="reply-mrn-tail",
    )

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["derived_action"]["value_candidate"] == "26DE99999"
    assert "26DE99999" in result["agent_prompt"]
    assert "26DE12345" not in result["agent_prompt"].split("Operator-Nachricht:", 1)[-1]
    assert not (root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl").exists()



def test_generic_confirmation_does_not_apply_pending_tms_action(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755" / "teams").mkdir(parents=True)
    pending_path = root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl"
    pending_path.write_text(
        json.dumps({
            "timestamp": "2026-05-08T11:58:25Z",
            "status": "pending_review",
            "order_id": "AN-11755",
            "target": "customs_reference",
            "value": "26DE99999",
            "write_policy": "no_auto_write_without_review",
        }, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    result = handle_teams_message(
        root=root,
        text="AN-11755 ja passt so",
        chat_id="teams-chat",
        user_name="Operator",
        message_id="reply-generic-ok",
        enable_tms_writeback=True,
        apply_tms_update=lambda action, context: (_ for _ in ()).throw(AssertionError("must not apply")),
    )

    assert result["handled"] is True
    assert result["classification"] == "confirmation"
    queue = _read_jsonl(pending_path)
    assert queue[-1]["status"] == "pending_review"


def test_explicit_approval_applies_pending_tms_action_and_verifies(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755" / "teams").mkdir(parents=True)
    pending_path = root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl"
    pending_path.write_text(
        json.dumps({
            "timestamp": "2026-05-08T11:58:25Z",
            "status": "pending_review",
            "order_id": "AN-11755",
            "context_id": "AN-11755:manual",
            "target": "customs_reference",
            "value": "26DE99999",
            "write_policy": "no_auto_write_without_review",
        }, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    calls: list[tuple[dict, dict]] = []

    def fake_apply(action: dict, context: dict) -> dict:
        calls.append((action, context))
        return {"status": "applied", "executed_tool": "cargolo_tms_update_shipment"}

    def fake_verify(order_id: str, target: str) -> str:
        assert order_id == "AN-11755"
        assert target == "customs_reference"
        return "26DE99999"

    result = handle_teams_message(
        root=root,
        text="AN-11755 freigegeben, bitte 26DE99999 jetzt ins TMS schreiben",
        chat_id="teams-chat",
        user_name="Operator",
        message_id="reply-approve",
        enable_tms_writeback=True,
        apply_tms_update=fake_apply,
        verify_tms_value=fake_verify,
    )

    assert result["handled"] is True
    assert result["classification"] == "tms_update_approved"
    assert result["derived_action"]["type"] == "tms_update_applied"
    assert calls[0][0]["action_type"] == "field_update"
    assert calls[0][0]["target"] == "shipment.customs.customs_reference"
    assert calls[0][0]["suggested_value"] == "26DE99999"
    assert calls[0][1]["order_id"] == "AN-11755"

    queue = _read_jsonl(pending_path)
    assert queue[-1]["status"] == "applied"
    assert queue[-1]["approved_by"] == "Operator"
    assert queue[-1]["applied_by"] == "Hermes Teams Reply Loop"
    assert queue[-1]["verified_value"] == "26DE99999"

    applied = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "applied_tms_actions.jsonl")
    assert applied[-1]["target"] == "customs_reference"
    assert applied[-1]["value"] == "26DE99999"

    audit = _read_jsonl(root / "orders" / "AN-11755" / "audit" / "actions.jsonl")
    assert any(row["action"] == "teams_tms_update_applied" for row in audit)


def test_explicit_approval_requires_fresh_verification_match(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755" / "teams").mkdir(parents=True)
    pending_path = root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl"
    pending_path.write_text(
        json.dumps({
            "timestamp": "2026-05-08T11:58:25Z",
            "status": "pending_review",
            "order_id": "AN-11755",
            "target": "customs_reference",
            "value": "26DE99999",
            "write_policy": "no_auto_write_without_review",
        }, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    result = handle_teams_message(
        root=root,
        text="AN-11755 freigegeben, bitte 26DE99999 ins TMS schreiben",
        chat_id="teams-chat",
        user_name="Operator",
        message_id="reply-approve-mismatch",
        enable_tms_writeback=True,
        apply_tms_update=lambda action, context: {"status": "applied"},
        verify_tms_value=lambda order_id, target: "26DE12345",
    )

    assert result["handled"] is True
    assert result["classification"] == "tms_update_approved"
    assert result["derived_action"]["type"] == "tms_update_verification_failed"
    queue = _read_jsonl(pending_path)
    assert queue[-1]["status"] == "verification_failed"
    assert queue[-1]["verified_value"] == "26DE12345"


def test_review_only_confirmation_closes_pending_without_tms_write(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-12218" / "teams").mkdir(parents=True)
    pending_path = root / "orders" / "AN-12218" / "teams" / "pending_tms_actions.jsonl"
    pending_path.write_text(
        json.dumps({
            "timestamp": "2026-05-19T08:00:00Z",
            "action_id": "act-review-only",
            "status": "pending_review",
            "order_id": "AN-12218",
            "target": "cargo_weight_kg",
            "value": "10100",
            "write_supported": "false",
            "write_policy": "no_auto_write_without_review",
        }, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    calls: list[tuple[dict, dict]] = []

    result = process_teams_tms_card_action(
        root=root,
        data={
            "hermes_action": "cargolo_asr_tms_approve",
            "order_id": "AN-12218",
            "action_id": "act-review-only",
            "target": "cargo_weight_kg",
            "value": "10100",
        },
        user_name="Operator",
        enable_tms_writeback=True,
        apply_tms_update=lambda action, context: calls.append((action, context)) or {"status": "applied"},
    )

    assert result["handled"] is True
    assert result["status"] == "review_confirmed"
    assert result["derived_action"]["type"] == "tms_review_only_confirmed"
    assert calls == []
    queue = _read_jsonl(pending_path)
    assert queue[-1]["status"] == "review_confirmed"
    confirmed = _read_jsonl(root / "orders" / "AN-12218" / "teams" / "confirmed_tms_review_actions.jsonl")
    assert confirmed[-1]["target"] == "cargo_weight_kg"


def test_quoted_card_with_tms_words_but_unrelated_operator_reply_is_not_tms_update(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    context = build_card_context(
        route_name="cargolo-asr-ops-teams",
        delivery_id="delivery-bu",
        payload={"processor_result": {"order_id": "BU-4664"}, "activity_event": {"id": 1304}},
        message_id="teams-msg-bu",
        chat_id="teams-chat-1",
    )
    record_sent_card(root=root, context=context)

    result = handle_teams_message(
        root=root,
        text=(
            "Display NameBU-4664 | Dokument-Check | TMS-Aktion: TMS unverändert | "
            "Review 0 | Offen 0 | MRN 26DE12345\n"
            "ich bin kurz im termin, melde mich später"
        ),
        chat_id="teams-chat-1",
        user_id="aad-7",
        user_name="Ops",
        message_id="reply-unrelated",
        reply_to_message_id="teams-msg-bu",
    )

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["order_id"] == "BU-4664"
    assert result["classification"] == "note"
    assert result["derived_action"]["type"] == "case_learning"
    assert "behaupte keinen TMS-Wunsch" in result["agent_prompt"]
    assert "ich bin kurz im termin" in result["agent_prompt"]
    assert not (root / "orders" / "BU-4664" / "teams" / "pending_tms_actions.jsonl").exists()


def test_quoted_card_with_tms_words_and_generic_ok_is_confirmation_not_tms_update(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    context = build_card_context(
        route_name="cargolo-asr-ops-teams",
        delivery_id="delivery-bu-ok",
        payload={"processor_result": {"order_id": "BU-4664"}, "activity_event": {"id": 1305}},
        message_id="teams-msg-bu-ok",
        chat_id="teams-chat-1",
    )
    record_sent_card(root=root, context=context)

    result = handle_teams_message(
        root=root,
        text="Display NameBU-4664 | TMS-Aktion: Review | MRN 26DE12345\nok passt",
        chat_id="teams-chat-1",
        user_id="aad-8",
        user_name="Ops",
        message_id="reply-ok",
        reply_to_message_id="teams-msg-bu-ok",
    )

    assert result["classification"] == "confirmation"
    assert result["derived_action"] == {"type": "review_decision", "decision": "confirmed"}
    assert not (root / "orders" / "BU-4664" / "teams" / "pending_tms_actions.jsonl").exists()


def test_tms_update_language_is_routed_to_agent_decision_not_regex_pending(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    context = build_card_context(
        route_name="cargolo-asr-ops-teams",
        delivery_id="delivery-7",
        payload={"order_id": "AN-11755", "activity_event": {"id": 1303}},
        message_id="card-7",
        chat_id="chat-1",
    )
    record_sent_card(root=root, context=context)

    result = handle_teams_message(
        root=root,
        text="Bitte TMS MRN 26DE12345 eintragen",
        chat_id="chat-1",
        user_id="u1",
        user_name="Dominik",
        message_id="reply-7",
        reply_to_message_id="card-7",
    )

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["classification"] == "agent_decision_required"
    assert "cargolo_asr_record_teams_tms_intent" in result["agent_prompt"]
    assert "nicht per starrem Regex" in result["agent_prompt"]
    assert not (root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl").exists()


def test_agent_can_record_decided_tms_intent_as_pending_review(tmp_path: Path) -> None:
    from plugins.cargolo_ops.teams_reply_loop import record_agent_tms_update_intent

    root = tmp_path / "cargolo_asr"

    result = record_agent_tms_update_intent(
        root=root,
        order_id="AN-11755",
        target="customs_reference",
        value="26DE12345",
        text="Operator meint: MRN ins TMS eintragen",
        operator="Hermes Agent",
        source_message_id="reply-7",
        context_id="AN-11755:1303:delivery-7",
        confidence="agent_decided",
    )

    assert result["status"] == "ok"
    assert result["queued"] is True
    queue = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl")
    assert queue[-1]["status"] == "pending_review"
    assert queue[-1]["source"] == "teams_agent_decision"
    assert queue[-1]["target"] == "customs_reference"
    assert queue[-1]["value"] == "26DE12345"
    assert queue[-1]["write_policy"] == "no_auto_write_without_review"


def test_teams_button_reject_marks_pending_without_tms_write(tmp_path: Path) -> None:
    from plugins.cargolo_ops.teams_reply_loop import record_agent_tms_update_intent, process_teams_tms_card_action

    root = tmp_path / "cargolo_asr"
    queued = record_agent_tms_update_intent(
        root=root,
        order_id="AN-11755",
        target="customs_reference",
        value="26DE99999",
        text="MRN ändern",
        operator="Hermes Agent",
        context_id="AN-11755:card",
    )

    result = process_teams_tms_card_action(
        root=root,
        data={
            "hermes_action": "cargolo_asr_tms_reject",
            "order_id": "AN-11755",
            "action_id": queued["action_id"],
            "target": "customs_reference",
            "value": "26DE99999",
        },
        user_id="u-1",
        user_name="Dominik",
    )

    assert result["status"] == "rejected"
    assert "Abgelehnt" in result["response_text"]
    queue = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl")
    assert queue[-1]["status"] == "rejected"
    assert queue[-1]["rejected_by"] == "Dominik"
    assert queue[-1]["value"] == "26DE99999"


def test_teams_button_correct_marks_pending_as_correction_requested_without_tms_write(tmp_path: Path) -> None:
    from plugins.cargolo_ops.teams_reply_loop import record_agent_tms_update_intent, process_teams_tms_card_action

    root = tmp_path / "cargolo_asr"
    queued = record_agent_tms_update_intent(
        root=root,
        order_id="AN-11755",
        target="customs_reference",
        value="26DE99999",
        text="MRN ändern",
        operator="Hermes Agent",
        context_id="AN-11755:card",
    )

    result = process_teams_tms_card_action(
        root=root,
        data={
            "hermes_action": "cargolo_asr_tms_correct",
            "order_id": "AN-11755",
            "action_id": queued["action_id"],
            "target": "customs_reference",
            "value": "26DE99999",
        },
        user_id="u-1",
        user_name="Dominik",
        apply_tms_update=lambda *_: (_ for _ in ()).throw(AssertionError("must not apply")),
    )

    assert result["status"] == "correction_requested"
    assert "Korrektur" in result["response_text"]
    assert "nicht ins TMS geschrieben" in result["response_text"]
    queue = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl")
    assert queue[-1]["status"] == "correction_requested"
    assert queue[-1]["correction_requested_by"] == "Dominik"
    assert not (root / "orders" / "AN-11755" / "teams" / "applied_tms_actions.jsonl").exists()
    audit = _read_jsonl(root / "orders" / "AN-11755" / "audit" / "actions.jsonl")
    assert any(row["action"] == "teams_tms_update_correction_requested" for row in audit)


def test_teams_button_case_check_runs_read_only_case_summary_without_changing_pending_status(tmp_path: Path) -> None:
    from plugins.cargolo_ops.teams_reply_loop import record_agent_tms_update_intent, process_teams_tms_card_action

    root = tmp_path / "cargolo_asr"
    case_root = root / "orders" / "AN-11755"
    (case_root / "documents" / "analysis").mkdir(parents=True)
    (case_root / "tms_snapshot.json").write_text(
        json.dumps({"status": "in_transit", "pickup_date": "2026-05-07"}),
        encoding="utf-8",
    )
    (case_root / "email_index.jsonl").write_text(
        json.dumps({"subject": "AW: AN-11755 // Zolldokumente", "sender": "asr@cargolo.com", "received_at": "2026-05-08T07:24:10Z"}) + "\n",
        encoding="utf-8",
    )
    (case_root / "documents" / "analysis" / "latest_summary.json").write_text(
        json.dumps({
            "documents": [{
                "filename": "CommercialInvoice.pdf",
                "operational_flags": ["MRN prüfen"],
                "missing_or_unreadable": ["HS-Code"],
            }]
        }),
        encoding="utf-8",
    )
    queued = record_agent_tms_update_intent(
        root=root,
        order_id="AN-11755",
        target="customs_reference",
        value="26DE99999",
        text="MRN ändern",
        operator="Hermes Agent",
        context_id="AN-11755:card",
    )

    result = process_teams_tms_card_action(
        root=root,
        data={
            "hermes_action": "cargolo_asr_case_check",
            "order_id": "AN-11755",
            "action_id": queued["action_id"],
            "target": "customs_reference",
            "value": "26DE99999",
        },
        user_id="u-1",
        user_name="Dominik",
        apply_tms_update=lambda *_: (_ for _ in ()).throw(AssertionError("must not apply")),
    )

    assert result["status"] == "case_check_completed"
    assert "<h2>🔎 Fallprüfung AN-11755" in result["response_text"]
    assert "Read-only ausgeführt" not in result["response_text"]
    assert "<strong>Lage:</strong> Ich sehe die Sendung im TMS als unterwegs / im Hauptlauf" in result["response_text"]
    assert "Zur Akte liegen 1 Mails" in result["response_text"]
    assert "<h3>Auffällig</h3>" in result["response_text"]
    assert "<h3>Empfehlung</h3>" in result["response_text"]
    assert "Offene Freigabe" in result["response_text"]
    assert "Zollreferenz / MRN" in result["response_text"]
    assert "26DE99999" in result["response_text"]
    assert "document_analyst" not in result["response_text"]
    assert " | " not in result["response_text"]
    queue = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl")
    assert queue[-1]["status"] == "pending_review"
    case_checks = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "case_check_requests.jsonl")
    assert case_checks[-1]["action_id"] == queued["action_id"]
    assert case_checks[-1]["status"] == "completed_read_only"
    assert case_checks[-1]["case_check"]["should_write_tms"] is False
    audit = _read_jsonl(root / "orders" / "AN-11755" / "audit" / "actions.jsonl")
    assert any(row["action"] == "teams_case_check_completed" for row in audit)
    assert not (root / "orders" / "AN-11755" / "teams" / "applied_tms_actions.jsonl").exists()


def test_teams_button_correct_missing_pending_action_does_not_create_new_action(tmp_path: Path) -> None:
    from plugins.cargolo_ops.teams_reply_loop import process_teams_tms_card_action

    root = tmp_path / "cargolo_asr"
    result = process_teams_tms_card_action(
        root=root,
        data={
            "hermes_action": "cargolo_asr_tms_correct",
            "order_id": "AN-11755",
            "action_id": "missing",
            "target": "customs_reference",
            "value": "26DE99999",
        },
        user_id="u-1",
        user_name="Dominik",
        apply_tms_update=lambda *_: (_ for _ in ()).throw(AssertionError("must not apply")),
    )

    assert result["status"] == "not_found"
    assert not (root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl").exists()
    assert not (root / "orders" / "AN-11755" / "teams" / "applied_tms_actions.jsonl").exists()


def test_teams_button_approve_applies_and_verifies_pending_action(tmp_path: Path) -> None:
    from plugins.cargolo_ops.teams_reply_loop import record_agent_tms_update_intent, process_teams_tms_card_action

    root = tmp_path / "cargolo_asr"
    queued = record_agent_tms_update_intent(
        root=root,
        order_id="AN-11755",
        target="customs_reference",
        value="26DE99999",
        text="MRN ändern",
        operator="Hermes Agent",
        context_id="AN-11755:card",
    )
    applied = []

    def fake_apply(action, context):
        applied.append((action, context))
        return {"status": "applied"}

    result = process_teams_tms_card_action(
        root=root,
        data={
            "hermes_action": "cargolo_asr_tms_approve",
            "order_id": "AN-11755",
            "action_id": queued["action_id"],
            "target": "customs_reference",
            "value": "26DE99999",
        },
        user_id="u-1",
        user_name="Dominik",
        enable_tms_writeback=True,
        apply_tms_update=fake_apply,
        verify_tms_value=lambda order_id, target: "26DE99999",
    )

    assert result["status"] == "applied"
    assert "geschrieben und frisch verifiziert" in result["response_text"]
    assert applied[0][0]["target"] == "shipment.customs.customs_reference"
    queue = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl")
    assert queue[-1]["status"] == "applied"
    assert queue[-1]["approved_by"] == "Dominik"
    assert queue[-1]["verified_value"] == "26DE99999"


def test_teams_button_approve_blocks_when_writeback_disabled(tmp_path: Path) -> None:
    from plugins.cargolo_ops.teams_reply_loop import record_agent_tms_update_intent, process_teams_tms_card_action

    root = tmp_path / "cargolo_asr"
    queued = record_agent_tms_update_intent(
        root=root,
        order_id="AN-11755",
        target="customs_reference",
        value="26DE99999",
        text="MRN ändern",
        operator="Hermes Agent",
    )

    result = process_teams_tms_card_action(
        root=root,
        data={
            "hermes_action": "cargolo_asr_tms_approve",
            "order_id": "AN-11755",
            "action_id": queued["action_id"],
            "target": "customs_reference",
            "value": "26DE99999",
        },
        user_id="u-1",
        user_name="Dominik",
        enable_tms_writeback=False,
        apply_tms_update=lambda *_: (_ for _ in ()).throw(AssertionError("must not apply")),
    )

    assert result["status"] == "approval_blocked"
    assert "Writeback ist deaktiviert" in result["response_text"]
    queue = _read_jsonl(root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl")
    assert queue[-1]["status"] == "pending_review"


def test_teams_button_click_on_already_resolved_action_is_not_reapplied(tmp_path: Path) -> None:
    from plugins.cargolo_ops.teams_reply_loop import record_agent_tms_update_intent, process_teams_tms_card_action

    root = tmp_path / "cargolo_asr"
    queued = record_agent_tms_update_intent(
        root=root,
        order_id="AN-11755",
        target="customs_reference",
        value="26DE99999",
        text="MRN ändern",
        operator="Hermes Agent",
    )
    queue_path = root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl"
    rows = _read_jsonl(queue_path)
    rows[-1]["status"] = "applied"
    queue_path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    result = process_teams_tms_card_action(
        root=root,
        data={
            "hermes_action": "cargolo_asr_tms_approve",
            "order_id": "AN-11755",
            "action_id": queued["action_id"],
            "target": "customs_reference",
            "value": "26DE99999",
        },
        user_id="u-1",
        user_name="Dominik",
        enable_tms_writeback=True,
        apply_tms_update=lambda *_: (_ for _ in ()).throw(AssertionError("must not reapply")),
    )

    assert result["status"] == "already_resolved"
    assert "bereits" in result["response_text"]



def test_explicit_approval_without_value_does_not_apply_ambiguous_pending_action(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755" / "teams").mkdir(parents=True)
    pending_path = root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl"
    rows = [
        {
            "timestamp": "2026-05-08T11:58:25Z",
            "status": "pending_review",
            "order_id": "AN-11755",
            "action_id": "act-customs",
            "context_id": "AN-11755:manual",
            "target": "customs_reference",
            "value": "26DE99999",
            "write_policy": "no_auto_write_without_review",
        },
        {
            "timestamp": "2026-05-08T11:59:25Z",
            "status": "pending_review",
            "order_id": "AN-11755",
            "action_id": "act-container",
            "context_id": "AN-11755:manual",
            "target": "container_number",
            "value": "MSCU1234567",
            "write_policy": "no_auto_write_without_review",
        },
    ]
    pending_path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")

    result = handle_teams_message(
        root=root,
        text="AN-11755 freigegeben, bitte jetzt ins TMS übernehmen",
        chat_id="teams-chat",
        user_name="Operator",
        message_id="reply-ambiguous-approve",
        enable_tms_writeback=True,
        apply_tms_update=lambda action, context: (_ for _ in ()).throw(AssertionError("must not apply ambiguous approval")),
    )

    assert result["handled"] is False
    assert result["classification"] == "agent_decision_required"
    assert [row["status"] for row in _read_jsonl(pending_path)] == ["pending_review", "pending_review"]
