from __future__ import annotations

from plugins.cargolo_ops.employee_agent import EmployeeRequest, EmployeeResponse, ResponseMode
from plugins.cargolo_ops.paperclip_teams_bridge import (
    PaperclipTeamsBridgeConfig,
    _extract_teams_answer,
    _latest_issue_comment_answer,
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


def test_poll_paperclip_issue_answer_sorts_comments_and_uses_issue_status(monkeypatch) -> None:
    calls: list[str] = []

    def fake_request_json(method: str, url: str, *, payload=None, timeout=8.0):
        del method, payload, timeout
        calls.append(url)
        if url.endswith("/comments"):
            return [
                {"createdAt": "2026-05-18T10:00:03Z", "body": "Chef heartbeat summary without Teams marker"},
                {"createdAt": "2026-05-18T10:00:02Z", "body": "TEAMS_ANTWORT:\n\nLage: aktuell"},
                {"createdAt": "2026-05-18T10:00:01Z", "body": "TEAMS_ANTWORT:\n\nLage: alt"},
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
