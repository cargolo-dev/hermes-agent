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
    assert "Activity-Watermark: 1200" in result["response_text"]


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


def test_case_deep_dive_routes_to_employee_agent_prompt_without_direct_write(tmp_path: Path) -> None:
    result = route_teams_ops_message(text="prüfe AN-12345 komplett", root=tmp_path / "cargolo_asr")

    assert result["handled"] is False
    assert result["allow_generic_chat"] is True
    assert result["classification"] == "case_deep_dive_request"
    assert result["order_id"] == "AN-12345"
    assert "ASR Ops Coordinator" in result["agent_prompt"]
    assert "Mail-Historie" in result["agent_prompt"]
    assert "read-only" in result["agent_prompt"]
    assert "Keine Audit-Dumps" in result["agent_prompt"]


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


def test_unrelated_message_is_not_intercepted(tmp_path: Path) -> None:
    result = route_teams_ops_message(text="was gibt es zum Mittag?", root=tmp_path / "cargolo_asr")

    assert result == {"handled": False}
