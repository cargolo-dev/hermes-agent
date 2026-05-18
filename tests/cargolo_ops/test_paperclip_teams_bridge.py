from __future__ import annotations

from plugins.cargolo_ops.employee_agent import EmployeeRequest, EmployeeResponse, ResponseMode
from plugins.cargolo_ops.paperclip_teams_bridge import (
    PaperclipTeamsBridgeConfig,
    _extract_teams_answer,
    _latest_issue_comment_answer,
    _latest_run_answer,
    _looks_probably_like_truncated_json,
    handle_paperclip_teams_case_assist,
    poll_paperclip_issue_answer,
)


def test_extract_teams_answer_strips_internal_tail() -> None:
    body = """TEAMS_ANTWORT:

---

**Lage:** Bridge ist bereit.

**Auffälligkeit:** Keine.

---

**Interne Notiz (nicht für Teams):**
- Debug
- Audit
"""

    answer = _extract_teams_answer(body)

    assert answer is not None
    assert "Bridge ist bereit" in answer
    assert "Interne Notiz" not in answer
    assert "Debug" not in answer


def test_extract_teams_answer_ignores_diff_artifacts() -> None:
    assert _extract_teams_answer("┊ review diff\na//tmp/comment.json → b//tmp/comment.json") is None


def test_latest_issue_comment_answer_requires_explicit_teams_marker(monkeypatch) -> None:
    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, url, payload, timeout
        return [
            {"createdAt": "2026-05-18T10:00:01Z", "body": "Interne Notiz: bitte nicht nach Teams senden."},
            {"createdAt": "2026-05-18T10:00:02Z", "body": "Chef heartbeat summary without Teams marker"},
        ]

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._request_json", fake_request_json)

    answer = _latest_issue_comment_answer(
        PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local"),
        "issue-1",
    )

    assert answer is None


def test_latest_issue_comment_answer_ignores_user_local_board_marker(monkeypatch) -> None:
    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, url, payload, timeout
        return [
            {
                "createdAt": "2026-05-18T10:00:02Z",
                "authorType": "user",
                "authorUserId": "local-board",
                "body": "TEAMS_ANTWORT:\n\nSoll nicht nach Teams, weil es ein User-/Bridge-Kommentar ist.",
            },
            {
                "createdAt": "2026-05-18T10:00:01Z",
                "authorType": "agent",
                "createdByRunId": "run-1",
                "body": "TEAMS_ANTWORT:\n\nAgentenantwort",
            },
        ]

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._request_json", fake_request_json)

    answer = _latest_issue_comment_answer(
        PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local"),
        "issue-1",
    )

    assert answer == "Agentenantwort"


def test_latest_issue_comment_answer_ignores_unverified_local_board_marker(monkeypatch) -> None:
    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, url, payload, timeout
        return [
            {
                "createdAt": "2026-05-18T10:00:02Z",
                "authorUserId": "local-board",
                "body": "TEAMS_ANTWORT:\n\nNicht beweisbar agent-authored, daher intern lassen.",
            }
        ]

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._request_json", fake_request_json)

    answer = _latest_issue_comment_answer(
        PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local"),
        "issue-1",
    )

    assert answer is None


def test_latest_issue_comment_answer_requires_explicit_agent_author_type(monkeypatch) -> None:
    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, url, payload, timeout
        return [
            {
                "createdAt": "2026-05-18T10:00:02Z",
                "createdByRunId": "run-1",
                "authorAgentId": "agent-1",
                "body": "TEAMS_ANTWORT:\n\nOhne authorType nicht an Teams weiterleiten.",
            }
        ]

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._request_json", fake_request_json)

    answer = _latest_issue_comment_answer(
        PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local"),
        "issue-1",
    )

    assert answer is None


def test_latest_run_answer_extracts_marker_from_matching_issue_result(monkeypatch) -> None:
    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, url, payload, timeout
        return [
            {
                "id": "run-other",
                "createdAt": "2026-05-18T10:00:03Z",
                "contextSnapshot": {"issueId": "issue-other"},
                "resultJson": {"result": "TEAMS_ANTWORT:\n\nFalsches Issue"},
            },
            {
                "id": "run-1",
                "createdAt": "2026-05-18T10:00:02Z",
                "contextSnapshot": {"issueId": "issue-1"},
                "resultJson": {"result": "TEAMS_ANTWORT:\n\nDirekt aus Run"},
            },
        ]

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._request_json", fake_request_json)

    answer = _latest_run_answer(
        PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local"),
        "issue-1",
    )

    assert answer == "Direkt aus Run"


def test_latest_run_answer_accepts_paperclip_wrapped_heartbeat_runs(monkeypatch) -> None:
    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, url, payload, timeout
        return {
            "heartbeatRuns": [
                {
                    "id": "run-1",
                    "createdAt": "2026-05-18T10:00:02Z",
                    "contextSnapshot": {"issueId": "issue-1"},
                    "resultJson": {"summary": "TEAMS_ANTWORT:\n\nAus gewrapptem Heartbeat-Result"},
                }
            ]
        }

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._request_json", fake_request_json)

    answer = _latest_run_answer(
        PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local"),
        "issue-1",
    )

    assert answer == "Aus gewrapptem Heartbeat-Result"


def test_latest_run_answer_skips_truncated_result_excerpt(monkeypatch) -> None:
    truncated = "TEAMS_ANTWORT:\n\n" + ("Sehr kompakter Satz. " * 25) + "Nächster Schri"

    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, url, payload, timeout
        return {
            "heartbeatRuns": [
                {
                    "id": "run-1",
                    "createdAt": "2026-05-18T10:00:02Z",
                    "contextSnapshot": {"issueId": "issue-1"},
                    "resultJson": {"summary": truncated},
                }
            ]
        }

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._request_json", fake_request_json)

    answer = _latest_run_answer(
        PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local"),
        "issue-1",
    )

    assert answer is None


def test_latest_run_answer_extracts_marker_from_stringified_result_json(monkeypatch) -> None:
    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, url, payload, timeout
        return {
            "heartbeatRuns": [
                {
                    "id": "run-1",
                    "createdAt": "2026-05-18T10:00:02Z",
                    "contextSnapshot": {"issueId": "issue-1"},
                    "resultJson": '{"summary":"TEAMS_ANTWORT:\\n\\nAus stringifiziertem Result."}',
                }
            ]
        }

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._request_json", fake_request_json)

    answer = _latest_run_answer(
        PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local"),
        "issue-1",
    )

    assert answer == "Aus stringifiziertem Result."


def test_stringified_run_result_truncation_falls_back_to_agent_comment(monkeypatch) -> None:
    truncated_json = '{"summary":"TEAMS_ANTWORT:\\n\\n' + ("Sehr kompakter Satz. " * 25) + "Nächster Schri"

    assert _looks_probably_like_truncated_json(truncated_json) is True

    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, payload, timeout
        if "heartbeat-runs" in url:
            return {
                "heartbeatRuns": [
                    {
                        "id": "run-1",
                        "createdAt": "2026-05-18T10:00:02Z",
                        "contextSnapshot": {"issueId": "issue-1"},
                        "resultJson": truncated_json,
                    }
                ]
            }
        if url.endswith("/comments"):
            return [
                {
                    "authorType": "agent",
                    "createdAt": "2026-05-18T10:00:03Z",
                    "body": "TEAMS_ANTWORT:\n\nVollständige Antwort aus Agent-Kommentar.",
                }
            ]
        raise AssertionError(url)

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._request_json", fake_request_json)

    result = poll_paperclip_issue_answer(
        issue_id="issue-1",
        config=PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local", poll_interval_seconds=0.01),
        timeout_seconds=1,
    )

    assert result["answer"] == "Vollständige Antwort aus Agent-Kommentar."
    assert result["source"] == "issue_comment"


def test_poll_paperclip_issue_answer_prefers_matching_run_result_over_comment(monkeypatch) -> None:
    monkeypatch.setattr(
        "plugins.cargolo_ops.paperclip_teams_bridge._latest_run_answer",
        lambda config, issue_id: "Antwort aus resultJson",
    )
    monkeypatch.setattr(
        "plugins.cargolo_ops.paperclip_teams_bridge._latest_issue_comment_answer",
        lambda config, issue_id: "Antwort aus Kommentar",
    )

    result = poll_paperclip_issue_answer(
        issue_id="issue-1",
        config=PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local", poll_interval_seconds=0.01),
        timeout_seconds=1,
    )

    assert result["answer"] == "Antwort aus resultJson"
    assert result["source"] == "run_result"


def test_poll_paperclip_issue_answer_falls_back_to_comment_when_run_lookup_fails(monkeypatch) -> None:
    def fail_run_lookup(config, issue_id):
        del config, issue_id
        raise RuntimeError("heartbeat endpoint temporarily unavailable")

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._latest_run_answer", fail_run_lookup)
    monkeypatch.setattr(
        "plugins.cargolo_ops.paperclip_teams_bridge._latest_issue_comment_answer",
        lambda config, issue_id: "Antwort aus Agent-Kommentar",
    )

    result = poll_paperclip_issue_answer(
        issue_id="issue-1",
        config=PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local", poll_interval_seconds=0.01),
        timeout_seconds=1,
    )

    assert result["answer"] == "Antwort aus Agent-Kommentar"
    assert result["source"] == "issue_comment"


def test_poll_paperclip_issue_answer_sorts_comments_and_uses_issue_status(monkeypatch) -> None:
    calls: list[str] = []

    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, payload, timeout
        calls.append(url)
        if "heartbeat-runs" in url:
            return []
        if url.endswith("/comments"):
            return [
                {"authorType": "agent", "createdAt": "2026-05-18T10:00:03Z", "body": "Chef heartbeat summary without Teams marker"},
                {"authorType": "agent", "createdAt": "2026-05-18T10:00:02Z", "body": "TEAMS_ANTWORT:\n\nLage: aktuell"},
                {"authorType": "agent", "createdAt": "2026-05-18T10:00:01Z", "body": "TEAMS_ANTWORT:\n\nLage: alt"},
            ]
        if url.endswith("/api/issues/issue-1"):
            return {"id": "issue-1", "status": "in_progress"}
        raise AssertionError(url)

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._request_json", fake_request_json)

    result = poll_paperclip_issue_answer(
        issue_id="issue-1",
        config=PaperclipTeamsBridgeConfig(enabled=True, api_base="http://paperclip.local", poll_interval_seconds=0.01),
        timeout_seconds=1,
    )

    assert result["answer"] == "Lage: aktuell"
    assert any(url.endswith("/api/issues/issue-1/comments") for url in calls)


def test_poll_paperclip_issue_answer_uses_terminal_grace_for_late_run_result(monkeypatch) -> None:
    run_answers = iter([None, "Lage: spät, aber innerhalb Grace"])

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._latest_issue_comment_answer", lambda config, issue_id: None)
    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._latest_run_answer", lambda config, issue_id: next(run_answers))
    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._issue_status", lambda config, issue_id: "done")
    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge.time.sleep", lambda seconds: None)

    result = poll_paperclip_issue_answer(
        issue_id="issue-1",
        config=PaperclipTeamsBridgeConfig(
            enabled=True,
            api_base="http://paperclip.local",
            poll_interval_seconds=0.01,
            terminal_grace_seconds=5.0,
        ),
        timeout_seconds=1,
    )

    assert result["answer"] == "Lage: spät, aber innerhalb Grace"
    assert result["source"] == "run_result"


def test_bridge_runs_lifecycle_preflight_when_local_case_is_incomplete(monkeypatch, tmp_path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-12807" / "employee").mkdir(parents=True)
    calls: list[str] = []

    def fake_sync_case_lifecycle(order_id: str, **kwargs):
        calls.append(f"sync:{order_id}")
        assert kwargs["storage_root"] == root
        assert kwargs["refresh_history"] is True
        assert kwargs["analyze_documents"] is True
        return {
            "status": "ok",
            "order_id": order_id,
            "case_root": str(root / "orders" / order_id),
            "history_sync_count": 4,
            "tms_snapshot_path": str(root / "orders" / order_id / "tms_snapshot.json"),
            "document_registry_path": str(root / "orders" / order_id / "documents" / "registry.json"),
        }

    def fake_create_issue(**kwargs):
        calls.append("create_issue")
        return {"id": "issue-1", "identifier": "CAR-12"}

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._sync_case_lifecycle", fake_sync_case_lifecycle)
    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._create_chef_issue", fake_create_issue)
    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._wait_for_issue_answer", lambda config, issue: (None, None))

    result = handle_paperclip_teams_case_assist(
        root=root,
        request=EmployeeRequest(text="Hermes CARGOLO gib mir alle infos zu AN-12807", channel="teams", order_id="AN-12807"),
        response=EmployeeResponse(mode=ResponseMode.CASE_ASSIST, order_id="AN-12807"),
        channel_id="teams-channel",
        message_id="teams-message",
        config=PaperclipTeamsBridgeConfig(enabled=True, wait_timeout_seconds=0),
    )

    assert calls == ["sync:AN-12807", "create_issue"]
    assert result["local_case_preflight"]["status"] == "synced"
    assert result["local_case_preflight"]["answerable_from_local"] is False
    assert result["paperclip_result_pending"] is True
    assert result["suppress_initial_response"] is False
    assert "Bin dran" in result["response_text"]
    assert "CARGOLO Operations Board" in result["response_text"]


def test_bridge_skips_lifecycle_preflight_when_local_case_has_required_evidence(monkeypatch, tmp_path) -> None:
    root = tmp_path / "cargolo_asr"
    case_root = root / "orders" / "AN-12807"
    (case_root / "tms").mkdir(parents=True)
    (case_root / "documents" / "analysis").mkdir(parents=True)
    (case_root / "tms" / "shipment_detail.json").write_text('{"shipment_number":"AN-12807"}', encoding="utf-8")
    (case_root / "email_index.jsonl").write_text('{"subject":"Update AN-12807"}\n', encoding="utf-8")
    (case_root / "documents" / "registry.json").write_text('{"received_types":["commercial_invoice"]}', encoding="utf-8")
    (case_root / "documents" / "analysis" / "latest_summary.json").write_text('{"documents":[]}', encoding="utf-8")

    def fail_sync(*args, **kwargs):
        raise AssertionError("complete local evidence should not trigger a fresh sync")

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._sync_case_lifecycle", fail_sync)
    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._create_chef_issue", lambda **kwargs: {"id": "issue-1", "identifier": "CAR-12"})
    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._wait_for_issue_answer", lambda config, issue: (None, None))

    result = handle_paperclip_teams_case_assist(
        root=root,
        request=EmployeeRequest(text="Gib mir alle Infos zu AN-12807", channel="teams", order_id="AN-12807"),
        response=EmployeeResponse(mode=ResponseMode.CASE_ASSIST, order_id="AN-12807"),
        channel_id="teams-channel",
        message_id="teams-message",
        config=PaperclipTeamsBridgeConfig(enabled=True, wait_timeout_seconds=0),
    )

    assert result["local_case_preflight"]["status"] == "local_ready"
    assert result["local_case_preflight"]["answerable_from_local"] is True
    assert result["local_case_preflight"]["case_root"] == str(case_root)


def test_bridge_stops_unknown_tms_case_without_paperclip_issue(monkeypatch, tmp_path) -> None:
    root = tmp_path / "cargolo_asr"

    monkeypatch.setattr(
        "plugins.cargolo_ops.paperclip_teams_bridge._sync_case_lifecycle",
        lambda order_id, **kwargs: {"status": "skipped", "reason": "shipment_not_found_in_tms", "order_id": order_id},
    )

    def fail_create_issue(**kwargs):
        raise AssertionError("unknown TMS cases must not create Paperclip work")

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._create_chef_issue", fail_create_issue)

    result = handle_paperclip_teams_case_assist(
        root=root,
        request=EmployeeRequest(text="Gib mir alles zu AN-404404", channel="teams", order_id="AN-404404"),
        response=EmployeeResponse(mode=ResponseMode.CASE_ASSIST, order_id="AN-404404"),
        channel_id="teams-channel",
        message_id="teams-message",
        config=PaperclipTeamsBridgeConfig(enabled=True, wait_timeout_seconds=0),
    )

    assert result["classification"] == "shipment_not_found_in_tms"
    assert result["paperclip_result_pending"] is False
    assert "nicht im ASR-TMS" in result["response_text"]
    assert not (root / "orders" / "AN-404404").exists()


def test_bridge_error_response_keeps_raw_api_error_internal(monkeypatch, tmp_path) -> None:
    def fail_create_issue(**kwargs):
        del kwargs
        raise RuntimeError("Paperclip API POST http://127.0.0.1:3100 failed with HTTP 500: sensitive body")

    monkeypatch.setattr("plugins.cargolo_ops.paperclip_teams_bridge._create_chef_issue", fail_create_issue)

    result = handle_paperclip_teams_case_assist(
        root=tmp_path,
        request=EmployeeRequest(text="Bitte Fall prüfen", channel="teams"),
        response=EmployeeResponse(mode=ResponseMode.CASE_ASSIST),
        channel_id="teams-channel",
        message_id="teams-message",
        config=PaperclipTeamsBridgeConfig(enabled=True),
    )

    assert result["handled"] is True
    assert result["should_send_to_teams"] is False
    assert "sensitive body" not in result["response_text"]
    assert "http://127.0.0.1:3100" not in result["response_text"]
    assert "sensitive body" in result["paperclip_error"]
