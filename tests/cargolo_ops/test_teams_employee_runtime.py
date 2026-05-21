from __future__ import annotations

import json
from pathlib import Path

from plugins.cargolo_ops.teams_employee_runtime import run_teams_employee_runtime


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_runtime_free_chat_stays_generic_without_case_refresh(tmp_path: Path) -> None:
    calls = []

    result = run_teams_employee_runtime(
        text="Kannst du mir ETA und ETD kurz erklären?",
        root=tmp_path,
        channel_id="cargolo-hermes",
        message_id="m1",
        is_dedicated_channel=True,
        refresh_func=lambda **kwargs: calls.append(kwargs),
    )

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["classification"] == "free_chat"
    assert calls == []


def test_runtime_guarded_tms_write_does_not_refresh_or_write(tmp_path: Path) -> None:
    calls = []

    result = run_teams_employee_runtime(
        text="Setz MRN 26DE99999 in AN-11755 im TMS",
        root=tmp_path,
        channel_id="cargolo-hermes",
        message_id="m2",
        is_dedicated_channel=True,
        refresh_func=lambda **kwargs: calls.append(kwargs),
    )

    assert result["handled"] is True
    assert result["classification"] == "guarded_action_required"
    assert result["boundary_action"] == "tms_write"
    assert "nicht direkt" in result["response_text"].lower()
    assert result["should_write_tms"] is False
    assert result["should_send_customer_message"] is False
    assert calls == []


def test_runtime_case_question_refreshes_stale_sources_and_builds_evidence_prompt(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-11755"
    _write_json(case_root / "case_state.json", {"order_id": "AN-11755", "tms_last_sync_at": "2026-05-20T07:00:00Z"})
    _write_json(case_root / "tms_snapshot.json", {"fetched_at": "2026-05-20T07:00:00Z", "eta": "2026-05-25", "status": "in_transit"})
    refresh_calls = []

    def fake_refresh(**kwargs):
        refresh_calls.append(kwargs)
        return {"status": "ok", "history_sync_status": "ok", "history_sync_mode": "delta", "history_sync_count": 1}

    result = run_teams_employee_runtime(
        text="Ist die Sendung AN-11755 sauber und hat der Kunde geantwortet?",
        root=tmp_path,
        channel_id="cargolo-hermes",
        message_id="m3",
        is_dedicated_channel=True,
        now="2026-05-20T08:00:00Z",
        refresh_func=fake_refresh,
    )

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["classification"] == "case_evidence_runtime_handoff"
    assert result["order_id"] == "AN-11755"
    assert result["progress_message"]
    assert refresh_calls == [{"order_id": "AN-11755", "storage_root": tmp_path, "refresh_history": True, "analyze_documents": True}]
    assert "EVIDENCE_BUNDLE" in result["agent_prompt"]
    assert "Wenn eine Quelle fehlt" in result["agent_prompt"]
    assert result["should_write_tms"] is False


def test_runtime_unknown_an_stops_tms_first_without_mail_sync(tmp_path: Path) -> None:
    calls = []

    result = run_teams_employee_runtime(
        text="Was ist mit AN-99999?",
        root=tmp_path,
        channel_id="cargolo-hermes",
        message_id="m4",
        is_dedicated_channel=True,
        tms_exists_func=lambda order_id: False,
        refresh_func=lambda **kwargs: calls.append(kwargs),
    )

    assert result["handled"] is True
    assert result["classification"] == "shipment_not_found_in_tms"
    assert "nicht im ASR-TMS" in result["response_text"]
    assert calls == []


def test_runtime_answer_composer_mentions_missing_source_caveats(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-30000"
    _write_json(case_root / "case_state.json", {"order_id": "AN-30000", "current_status": "in_transit"})
    _write_json(case_root / "tms_snapshot.json", {"fetched_at": "2026-05-20T08:00:00Z", "eta": "2026-05-25"})

    result = run_teams_employee_runtime(
        text="Hat der Kunde zu AN-30000 geantwortet?",
        root=tmp_path,
        channel_id="cargolo-hermes",
        message_id="m5",
        is_dedicated_channel=True,
        now="2026-05-20T08:05:00Z",
        refresh_func=lambda **kwargs: {"status": "error", "history_sync_status": "failed", "history_sync_error": "mail_down"},
    )

    assert "Mailhistorie" in result["agent_prompt"]
    assert "Vorbehalt" in result["agent_prompt"]
    assert "nicht raten" in result["agent_prompt"]


def test_runtime_recomputes_freshness_after_successful_refresh(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-12346"
    _write_json(case_root / "case_state.json", {"order_id": "AN-12346", "last_email_at": "2026-05-20T07:00:00Z"})

    def fake_refresh(**kwargs):
        _write_json(case_root / "case_state.json", {"order_id": "AN-12346", "last_email_at": "2026-05-20T08:59:00Z"})
        (case_root / "email_index.jsonl").write_text(json.dumps({"received_at": "2026-05-20T08:59:00Z", "subject": "Neue Antwort", "from": "kunde@example.com"}) + "\n", encoding="utf-8")
        (case_root / "audit").mkdir(parents=True, exist_ok=True)
        (case_root / "audit" / "actions.jsonl").write_text(json.dumps({"timestamp": "2026-05-20T08:59:30Z", "action": "sync_case_lifecycle", "history_sync_status": "ok"}) + "\n", encoding="utf-8")
        return {"status": "ok", "history_sync_status": "ok"}

    result = run_teams_employee_runtime(
        text="Hat der Kunde zu AN-12346 geantwortet?",
        root=tmp_path,
        channel_id="cargolo-hermes",
        message_id="m6",
        is_dedicated_channel=True,
        now="2026-05-20T09:00:00Z",
        refresh_func=fake_refresh,
    )

    assert result["lifecycle"]["status"] == "ok"
    assert result["freshness_plan"]["requires_refresh"] is False
    assert result["freshness_plan"]["refresh_sources"] == []
    assert "Mailhistorie ist nicht frisch" not in result["agent_prompt"]
    assert result["evidence_bundle"]["sources"]["email_index"]["summary"]["latest_subject"] == "Neue Antwort"


def test_runtime_refresh_exception_degrades_to_evidence_prompt(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-12347"
    _write_json(case_root / "case_state.json", {"order_id": "AN-12347", "last_email_at": "2026-05-20T07:00:00Z"})

    def broken_refresh(**kwargs):
        raise RuntimeError("mail backend down")

    result = run_teams_employee_runtime(
        text="Hat der Kunde zu AN-12347 geantwortet?",
        root=tmp_path,
        channel_id="cargolo-hermes",
        message_id="m7",
        is_dedicated_channel=True,
        now="2026-05-20T09:00:00Z",
        refresh_func=broken_refresh,
    )

    assert result["handled"] is False
    assert result["lifecycle"]["status"] == "error"
    assert "mail backend down" in result["lifecycle"]["error"]
    assert "Vorbehalt" in result["agent_prompt"]
    assert result["should_write_tms"] is False


def test_runtime_prompt_uses_structured_intent_and_thread_context(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-11755"
    _write_json(case_root / "case_state.json", {"order_id": "AN-11755", "tms_last_sync_at": "2026-05-20T08:00:00Z"})
    _write_json(case_root / "teams" / "thread_context.json", {
        "updated_at": "2026-05-20T08:01:00Z",
        "last_order_id": "AN-11755",
        "last_user_message": {"text": "Was ist mit AN-11755?"},
        "last_hermes_response": {"text": "Lage: Dokumente waren offen."},
        "recent_messages": [{"role": "user", "text": "Was ist mit AN-11755?"}, {"role": "assistant", "text": "Lage: Dokumente waren offen."}],
    })

    result = run_teams_employee_runtime(
        text="Blockt da was bei AN-11755?",
        root=tmp_path,
        now="2026-05-20T08:05:00Z",
        refresh_func=lambda **kwargs: {"status": "ok"},
    )

    assert result["structured_intent"]["intent"] == "blocker_check"
    assert "Strukturierter Intent" in result["agent_prompt"]
    assert "Lage: Dokumente waren offen" in result["agent_prompt"]
    assert "teams_thread_context" in result["structured_intent"]["requested_sources"]


def test_runtime_release_readiness_requests_billing_documents_mail_tms(tmp_path: Path) -> None:
    case_root = tmp_path / "orders" / "AN-11755"
    _write_json(case_root / "case_state.json", {"order_id": "AN-11755"})

    result = run_teams_employee_runtime(
        text="Kann ich AN-11755 ziehen lassen?",
        root=tmp_path,
        now="2026-05-20T08:05:00Z",
        refresh_func=lambda **kwargs: {"status": "ok"},
    )

    assert result["structured_intent"]["intent"] == "release_readiness_check"
    required = set(result["freshness_plan"]["required_sources"])
    assert {"tms_snapshot", "email_index", "document_registry", "document_analysis", "billing_context"}.issubset(required)
    assert result["should_write_tms"] is False
