from __future__ import annotations

import json
from pathlib import Path

from plugins.cargolo_ops.teams_ops_router import route_teams_ops_message, should_use_case_assist_speed_layer


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_status_command_returns_compact_ops_status(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    (tmp_path / "cron").mkdir(parents=True)
    (tmp_path / "cron" / "jobs.json").write_text(json.dumps({
        "jobs": [{
            "job_id": "d8d3772a1f77",
            "name": "cargolo-asr-document-upload-monitor",
            "state": "scheduled",
            "last_status": "ok",
        }]
    }), encoding="utf-8")
    root = tmp_path / "cargolo_asr"
    (root / "runtime").mkdir(parents=True)
    (root / "runtime" / "document_activity_monitor_state.json").write_text(
        json.dumps({"last_seen_activity_id": 1200}), encoding="utf-8"
    )

    result = route_teams_ops_message(text="status", root=root)

    assert result["handled"] is True
    assert result["classification"] == "ops_status"
    assert "CARGOLO Teams Ops · Status" in result["response_text"]
    assert "cargolo-asr-document-upload-monitor" in result["response_text"]
    assert "Teams ist verbunden" in result["response_text"]
    assert "Activity-Watermark" not in result["response_text"]


def test_pending_review_command_lists_open_tms_actions(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    _write_jsonl(root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl", [{
        "timestamp": "2026-05-08T12:00:00Z",
        "status": "pending_review",
        "order_id": "AN-11755",
        "target": "customs_reference",
        "value": "26DE99999",
        "operator": "Dominik",
    }])

    result = route_teams_ops_message(text="offene Freigaben", root=root)

    assert result["handled"] is True
    assert result["classification"] == "pending_tms_reviews"
    assert "AN-11755" in result["response_text"]
    assert "customs_reference = 26DE99999" in result["response_text"]
    assert "freigeben" in result["response_text"]
    assert result["teams_tms_review_cards"]
    assert result["teams_tms_review_cards"][0]["order_id"] == "AN-11755"


def test_correction_followup_value_records_new_pending_review(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    queue = root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl"
    _write_jsonl(queue, [{
        "timestamp": "2026-05-11T07:56:27Z",
        "action_id": "old-action",
        "status": "correction_requested",
        "order_id": "AN-11755",
        "context_id": "AN-11755:manual",
        "target": "customs_reference",
        "value": "26DE99999",
        "operator": "Dominik",
        "correction_requested_at": "2026-05-11T07:56:41Z",
    }])

    result = route_teams_ops_message(
        text="Hermes CARGOLO 26DE888888",
        root=root,
        user_id="user-1",
        user_name="Dominik",
        message_id="msg-1",
    )

    assert result["handled"] is True
    assert result["classification"] == "correction_followup_recorded"
    assert result["order_id"] == "AN-11755"
    assert "26DE888888" in result["response_text"]
    assert result["teams_tms_review_cards"][0]["value"] == "26DE888888"
    assert result["teams_tms_review_cards"][0]["status"] == "pending_review"
    rows = [json.loads(line) for line in queue.read_text(encoding="utf-8").splitlines()]
    assert rows[-1]["source"] == "teams_correction_followup"
    assert rows[-1]["status"] == "pending_review"
    assert rows[-1]["previous_value"] == "26DE99999"
    audit_rows = [
        json.loads(line)
        for line in (root / "orders" / "AN-11755" / "audit" / "actions.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert audit_rows[-1]["action"] == "teams_tms_correction_followup_recorded"
    assert audit_rows[-1]["value"] == "26DE888888"


def test_correction_followup_with_multiple_open_corrections_does_not_guess(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    _write_jsonl(root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl", [{
        "timestamp": "2026-05-11T07:56:27Z",
        "status": "correction_requested",
        "order_id": "AN-11755",
        "target": "customs_reference",
        "value": "26DE99999",
    }])
    _write_jsonl(root / "orders" / "AN-11756" / "teams" / "pending_tms_actions.jsonl", [{
        "timestamp": "2026-05-11T07:57:27Z",
        "status": "correction_requested",
        "order_id": "AN-11756",
        "target": "customs_reference",
        "value": "26DE777777",
    }])

    result = route_teams_ops_message(text="Hermes CARGOLO 26DE888888", root=root)

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["classification"] == "general_cargolo_ops"


def test_correction_followup_with_explicit_order_disambiguates(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    _write_jsonl(root / "orders" / "AN-11755" / "teams" / "pending_tms_actions.jsonl", [{
        "timestamp": "2026-05-11T07:56:27Z",
        "status": "correction_requested",
        "order_id": "AN-11755",
        "target": "customs_reference",
        "value": "26DE99999",
    }])
    _write_jsonl(root / "orders" / "AN-11756" / "teams" / "pending_tms_actions.jsonl", [{
        "timestamp": "2026-05-11T07:57:27Z",
        "status": "correction_requested",
        "order_id": "AN-11756",
        "target": "customs_reference",
        "value": "26DE777777",
    }])

    result = route_teams_ops_message(text="AN-11755 26DE888888", root=root)

    assert result["handled"] is True
    assert result["classification"] == "correction_followup_recorded"
    assert result["order_id"] == "AN-11755"
    assert result["teams_tms_review_cards"][0]["value"] == "26DE888888"


def test_case_deep_dive_refreshes_local_case_before_answer(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: None)
    calls = []

    def fake_sync(order_id, **kwargs):
        calls.append({"order_id": order_id, **kwargs})
        return {"status": "ok", "order_id": order_id}

    monkeypatch.setattr("plugins.cargolo_ops.case_lifecycle.sync_case_lifecycle", fake_sync)
    result = route_teams_ops_message(text="prüfe AN-12345 komplett", root=tmp_path / "cargolo_asr")

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["classification"] == "case_evidence_refreshed_agent_handoff"
    assert result["order_id"] == "AN-12345"
    assert calls[0]["order_id"] == "AN-12345"
    assert calls[0]["storage_root"] == tmp_path / "cargolo_asr"
    assert calls[0]["refresh_history"] is True
    assert calls[0]["analyze_documents"] is True
    assert "AN-12345" in result["agent_prompt"]
    assert "frisch synchronisiert" in result["agent_prompt"]
    assert "Originalfrage: prüfe AN-12345 komplett" in result["agent_prompt"]


def test_gib_mir_alles_refreshes_local_case_before_answer(tmp_path: Path, monkeypatch) -> None:
    provider = _FakeTMSProvider([{"shipment_number": "AN-12345"}])
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: provider)
    calls = []

    def fake_sync(order_id, **kwargs):
        calls.append({"order_id": order_id, **kwargs})
        return {"status": "ok", "order_id": order_id}

    monkeypatch.setattr("plugins.cargolo_ops.case_lifecycle.sync_case_lifecycle", fake_sync)

    result = route_teams_ops_message(text="Hermes CARGOLO Sag mir alles zu AN-12345", root=tmp_path / "cargolo_asr")

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["classification"] == "case_evidence_refreshed_agent_handoff"
    assert result["order_id"] == "AN-12345"
    assert calls[0]["refresh_history"] is True
    assert provider.calls[0]["shipment_number"] == "AN-12345"


def test_deep_dive_prompt_remains_read_only_case_assist_not_tms_guard(tmp_path: Path, monkeypatch) -> None:
    provider = _FakeTMSProvider([{"shipment_number": "AN-12345"}])
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: provider)

    def fake_sync_case_lifecycle(order_id, **kwargs):
        root = Path(kwargs["storage_root"])
        (root / "orders" / order_id).mkdir(parents=True, exist_ok=True)
        return {"status": "ok", "order_id": order_id}

    monkeypatch.setattr("plugins.cargolo_ops.case_lifecycle.sync_case_lifecycle", fake_sync_case_lifecycle)

    result = route_teams_ops_message(text="Sag mir alles zu AN-12345", root=tmp_path / "cargolo_asr")

    assert result["handled"] is False
    assert result["classification"] == "case_evidence_refreshed_agent_handoff"
    assert "TMS Guard erforderlich" not in result["agent_prompt"]
    assert "case_assist_agentic" in result["agent_prompt"]
    assert result["teams_tms_review_cards"] == []
    assert result["side_effects"]["queued_tms_actions"] == 0
    assert not (tmp_path / "cargolo_asr" / "orders" / "AN-12345" / "teams" / "pending_tms_actions.jsonl").exists()
    assert provider.calls[0]["shipment_number"] == "AN-12345"


def test_tms_like_free_text_without_card_context_is_guarded(tmp_path: Path) -> None:
    result = route_teams_ops_message(
        text="AN-11755 bitte MRN 26DE99999 ins TMS eintragen",
        root=tmp_path / "cargolo_asr",
    )

    assert result["handled"] is True
    assert result["classification"] == "tms_review_card_prepared"
    assert result["order_id"] == "AN-11755"
    assert result["teams_tms_review_cards"][0]["target"] == "customs_reference"
    assert result["teams_tms_review_cards"][0]["value"] == "26DE99999"
    assert "Noch kein TMS-Write" in result["response_text"]


def test_deep_dive_runs_local_case_assist_directly(tmp_path: Path, monkeypatch) -> None:
    calls = []

    def fake_sync(order_id, **kwargs):
        calls.append({"order_id": order_id, **kwargs})
        return {"status": "ok", "order_id": order_id}

    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: None)
    monkeypatch.setattr("plugins.cargolo_ops.case_lifecycle.sync_case_lifecycle", fake_sync)

    result = route_teams_ops_message(
        text="prüfe AN-12345 komplett",
        root=tmp_path / "cargolo_asr",
    )

    assert result["handled"] is False
    assert result["classification"] == "case_evidence_refreshed_agent_handoff"
    assert calls[0]["order_id"] == "AN-12345"


def test_natural_an_question_refreshes_evidence_and_falls_through_to_agent(tmp_path: Path, monkeypatch) -> None:
    provider = _FakeTMSProvider([{"shipment_number": "AN-12345"}])
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: provider)
    calls = []

    def fake_sync(order_id, **kwargs):
        calls.append({"order_id": order_id, **kwargs})
        return {"status": "ok", "order_id": order_id}

    monkeypatch.setattr("plugins.cargolo_ops.case_lifecycle.sync_case_lifecycle", fake_sync)

    result = route_teams_ops_message(text="was ist mit AN-12345?", root=tmp_path / "cargolo_asr")

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["classification"] == "case_evidence_refreshed_agent_handoff"
    assert result["order_id"] == "AN-12345"
    assert calls[0]["refresh_history"] is True
    assert provider.calls[0]["shipment_number"] == "AN-12345"


def test_tms_write_still_guarded_before_case_handoff(tmp_path: Path, monkeypatch) -> None:
    def fake_deep_dive(**kwargs):
        raise AssertionError("write-like TMS text must not enter deep-dive route")

    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_case_evidence_agent_handoff", fake_deep_dive)

    result = route_teams_ops_message(
        text="aktualisiere MRN 26DE99999 in AN-11755 im TMS",
        root=tmp_path / "cargolo_asr",
    )

    assert result["handled"] is True
    assert result["classification"] == "tms_review_card_prepared"
    assert result["order_id"] == "AN-11755"
    assert result["teams_tms_review_cards"][0]["value"] == "26DE99999"


def test_unrelated_message_is_not_intercepted(tmp_path: Path) -> None:
    result = route_teams_ops_message(text="was gibt es zum Mittag?", root=tmp_path / "cargolo_asr")

    assert result == {"handled": False}


def test_case_assist_speed_layer_classifier_is_cheap_and_read_only() -> None:
    use_speed_layer, order_id = should_use_case_assist_speed_layer("Sag mir alles zu AN-12345")

    assert use_speed_layer is True
    assert order_id == "AN-12345"
    assert should_use_case_assist_speed_layer("Gib mir alle Infos zu AN-12218") == (True, "AN-12218")
    assert should_use_case_assist_speed_layer("Details zu AN-12218") == (True, "AN-12218")


def test_case_assist_speed_layer_does_not_swallow_free_chat_or_tms_writes() -> None:
    assert should_use_case_assist_speed_layer("erzähl mal einen witz") == (False, None)
    assert should_use_case_assist_speed_layer("AN-11755 bitte MRN 26DE99999 ins TMS eintragen") == (False, "AN-11755")


class _FakeTMSProvider:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def shipments_list(self, **kwargs):
        self.calls.append(kwargs)
        return self.rows


def test_unknown_tms_case_is_answered_without_generic_agent_or_n8n(tmp_path: Path, monkeypatch) -> None:
    provider = _FakeTMSProvider([])
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: provider)

    result = route_teams_ops_message(
        text="Hermes CARGOLO Sag mir alles zu AN-914458534581",
        root=tmp_path / "cargolo_asr",
    )

    assert result["handled"] is True
    assert result["classification"] == "shipment_not_found_in_tms"
    assert result["order_id"] == "AN-914458534581"
    assert "nicht im ASR-TMS" in result["response_text"]
    assert "keine Mail-/n8n-Suche" in result["response_text"]
    assert "Nächster Schritt" in result["response_text"]
    assert provider.calls[0]["shipment_number"] == "AN-914458534581"


def test_existing_tms_case_refreshes_local_case(tmp_path: Path, monkeypatch) -> None:
    provider = _FakeTMSProvider([{"shipment_number": "AN-12345"}])
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: provider)
    calls = []

    def fake_sync(order_id, **kwargs):
        calls.append({"order_id": order_id, **kwargs})
        return {"status": "ok", "order_id": order_id}

    monkeypatch.setattr("plugins.cargolo_ops.case_lifecycle.sync_case_lifecycle", fake_sync)

    result = route_teams_ops_message(text="prüfe AN-12345 komplett", root=tmp_path / "cargolo_asr")

    assert result["handled"] is False
    assert result["classification"] == "case_evidence_refreshed_agent_handoff"
    assert result["order_id"] == "AN-12345"
    assert calls[0]["order_id"] == "AN-12345"
    assert provider.calls[0]["shipment_number"] == "AN-12345"


def test_followup_docs_without_an_uses_last_thread_case(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755" / "teams").mkdir(parents=True, exist_ok=True)
    (root / "orders" / "AN-11755" / "teams" / "thread_context.json").write_text(json.dumps({
        "updated_at": "2026-05-20T08:00:00Z",
        "last_order_id": "AN-11755",
        "last_user_message": {"text": "Was ist mit AN-11755?"},
        "recent_messages": [{"role": "user", "order_id": "AN-11755", "text": "Was ist mit AN-11755?"}],
    }), encoding="utf-8")
    from plugins.cargolo_ops.teams_thread_context import record_inbound_message
    record_inbound_message(root=root, chat_id="19:ops", text="Was ist mit AN-11755?")
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: _FakeTMSProvider([{"shipment_number": "AN-11755"}]))
    calls = []
    monkeypatch.setattr("plugins.cargolo_ops.case_lifecycle.sync_case_lifecycle", lambda order_id, **kwargs: calls.append({"order_id": order_id, **kwargs}) or {"status": "ok"})

    result = route_teams_ops_message(text="und was ist mit den Docs?", root=root, chat_id="19:ops")

    assert result["classification"] == "case_evidence_refreshed_agent_handoff"
    assert result["order_id"] == "AN-11755"
    assert calls[0]["order_id"] == "AN-11755"
    assert "Teams-Thread-Kontext" in result["agent_prompt"]


def test_document_upload_and_internal_done_prepare_review_only(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"

    upload = route_teams_ops_message(text="Lade die CI für AN-11755 ins TMS hoch", root=root)
    done = route_teams_ops_message(text="Markier erledigt bei AN-11755: Verzollung geprüft", root=root)

    assert upload["classification"] == "document_upload_review_prepared"
    assert upload["should_write_tms"] is False
    assert done["classification"] == "internal_action_review_prepared"
    rows_path = root / "orders" / "AN-11755" / "teams" / "pending_internal_actions.jsonl"
    assert rows_path.exists()
    rows = [json.loads(line) for line in rows_path.read_text(encoding="utf-8").splitlines()]
    assert {row["action_kind"] for row in rows} == {"document_upload_review", "internal_note_or_todo_review"}


def test_tms_review_extraction_does_not_queue_status_update_noise(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: None)
    root = tmp_path / "cargolo_asr"

    hbl = route_teams_ops_message(text="Update: HBL fehlt noch für AN-12345", root=root)
    customs = route_teams_ops_message(text="Update: customs status nicht erledigt für AN-12345", root=root)

    assert hbl["handled"] is True
    assert hbl["classification"] == "tms_control_without_card_context"
    assert not hbl.get("teams_tms_review_cards")
    assert customs["handled"] is True
    assert customs["classification"] == "tms_control_without_card_context"
    assert not customs.get("teams_tms_review_cards")
    assert not (root / "orders" / "AN-12345" / "teams" / "pending_tms_actions.jsonl").exists()


def test_tms_review_extraction_accepts_explicit_assignment_only_review(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: None)
    root = tmp_path / "cargolo_asr"

    hbl = route_teams_ops_message(text="Setz HBL ABC12345 für AN-12345", root=root)
    customs = route_teams_ops_message(text="Setz customs status erledigt für AN-12345", root=root)

    assert hbl["classification"] == "tms_review_card_prepared"
    assert hbl["teams_tms_review_cards"][0]["target"] == "hbl_number"
    assert hbl["teams_tms_review_cards"][0]["value"] == "ABC12345"
    assert hbl["teams_tms_review_cards"][0]["write_supported"] is False
    assert customs["classification"] == "tms_review_card_prepared"
    assert customs["teams_tms_review_cards"][0]["target"] == "customs_status"
    rows = [json.loads(line) for line in (root / "orders" / "AN-12345" / "teams" / "pending_tms_actions.jsonl").read_text(encoding="utf-8").splitlines()]
    assert rows[-1]["write_supported"] is False


def test_internal_review_actions_are_deduped(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: None)
    root = tmp_path / "cargolo_asr"
    text = "Lade die CI für AN-11755 ins TMS hoch"

    first = route_teams_ops_message(text=text, root=root, user_name="Dominik")
    second = route_teams_ops_message(text=text, root=root, user_name="Dominik")

    assert first["classification"] == "document_upload_review_prepared"
    assert second["classification"] == "document_upload_review_prepared"
    rows_path = root / "orders" / "AN-11755" / "teams" / "pending_internal_actions.jsonl"
    rows = [json.loads(line) for line in rows_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert second["teams_upload_review_cards"][0]["duplicate"] is True


def test_upload_review_respects_tms_first_unknown_shipment(tmp_path: Path, monkeypatch) -> None:
    class FakeProvider:
        def shipments_list(self, **kwargs):
            return []

    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.build_tms_provider_from_env", lambda: FakeProvider())
    root = tmp_path / "cargolo_asr"

    result = route_teams_ops_message(text="Lade die CI für AN-999999 ins TMS hoch", root=root, user_name="Ops")

    assert result["handled"] is True
    assert result["classification"] == "shipment_not_found_in_tms"
    assert not (root / "orders" / "AN-999999" / "teams" / "pending_internal_actions.jsonl").exists()
