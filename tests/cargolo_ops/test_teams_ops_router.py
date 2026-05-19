from __future__ import annotations

import json
from pathlib import Path

from plugins.cargolo_ops.teams_ops_router import route_teams_ops_message


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
    assert provider.calls[0]["shipment_number"] == "AN-12345"


def test_tms_like_free_text_without_card_context_is_guarded(tmp_path: Path) -> None:
    result = route_teams_ops_message(
        text="AN-11755 bitte MRN 26DE99999 ins TMS eintragen",
        root=tmp_path / "cargolo_asr",
    )

    assert result["handled"] is True
    assert result["classification"] == "tms_control_without_card_context"
    assert result["order_id"] == "AN-11755"
    assert "nicht eindeutig einer Operator-Karte" in result["response_text"]
    assert "Review-Vorschlag" in result["response_text"]


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
    assert result["classification"] == "tms_control_without_card_context"
    assert result["order_id"] == "AN-11755"
    assert "Review-Vorschlag" in result["response_text"]


def test_unrelated_message_is_not_intercepted(tmp_path: Path) -> None:
    result = route_teams_ops_message(text="was gibt es zum Mittag?", root=tmp_path / "cargolo_asr")

    assert result == {"handled": False}


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
