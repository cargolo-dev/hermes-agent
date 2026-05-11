from __future__ import annotations

import json
from pathlib import Path

from plugins.cargolo_ops.employee_agent import BoundaryAction, EmployeeRequest, ResponseMode
from plugins.cargolo_ops.employee_runtime import run_employee_runtime
from plugins.cargolo_ops.honcho_memory import HonchoMemorySnapshot


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_free_chat_runtime_does_not_execute_specialists_or_write_case_files(tmp_path: Path) -> None:
    result = run_employee_runtime(
        EmployeeRequest(text="Erklär mir ETA vs ETD", channel="telegram"),
        root=tmp_path / "cargolo_asr",
    )

    assert result.employee_response.mode == ResponseMode.FREE_CHAT
    assert result.specialist_results == []
    assert result.draft_response
    assert result.should_send_to_teams is False
    assert result.should_write_tms is False
    assert result.should_send_customer_message is False
    assert not (tmp_path / "cargolo_asr" / "orders").exists()


def test_case_assist_runtime_executes_read_only_specialists_and_writes_results(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    case_dir = root / "orders" / "AN-11755"
    (case_dir / "mail").mkdir(parents=True)
    (case_dir / "mail" / "history.json").write_text('{"messages": [{"subject": "CI fehlt"}]}', encoding="utf-8")
    (case_dir / "tms_snapshot.json").write_text('{"shipment_number": "AN-11755", "status": "docs pending"}', encoding="utf-8")

    result = run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 los? Schau in Mails und TMS.", channel="telegram"),
        root=root,
    )

    assert result.employee_response.mode == ResponseMode.CASE_ASSIST
    assert [r.agent for r in result.specialist_results] == ["case_context", "mail_history", "tms_snapshot"]
    assert all(r.status.value == "ok" for r in result.specialist_results)
    assert result.draft_response.startswith("<div><h2>🔎 Fallprüfung AN-11755")
    assert "Read-only ausgeführt: kein TMS-Write" in result.draft_response
    result_rows = _read_jsonl(case_dir / "employee" / "specialist_results.jsonl")
    assert [row["agent"] for row in result_rows] == ["case_context", "mail_history", "tms_snapshot"]
    assert all(row["write_intents"] == [] for row in result_rows)


def test_guarded_tms_write_runtime_executes_no_specialists_and_no_writes(tmp_path: Path) -> None:
    result = run_employee_runtime(
        EmployeeRequest(text="Setze MRN 26DE99999 in AN-11755", channel="telegram"),
        root=tmp_path / "cargolo_asr",
    )

    assert result.employee_response.mode == ResponseMode.GUARDED_ACTION_REQUIRED
    assert result.employee_response.boundary_action == BoundaryAction.TMS_WRITE
    assert result.specialist_results == []
    assert result.should_write_tms is False
    assert not (tmp_path / "cargolo_asr" / "orders" / "AN-11755" / "employee" / "specialist_results.jsonl").exists()


def test_customer_draft_runtime_returns_draft_only_without_customer_send(tmp_path: Path) -> None:
    result = run_employee_runtime(
        EmployeeRequest(text="Schreib dem Kunden die MRN 26DE99999 für AN-11755", channel="telegram"),
        root=tmp_path / "cargolo_asr",
    )

    assert result.employee_response.mode == ResponseMode.DRAFT_ONLY
    assert result.should_send_customer_message is False
    assert result.draft_response
    assert "Entwurf" in result.draft_response


def test_business_draft_uses_read_only_case_context_without_sending(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    case_dir = root / "orders" / "AN-11755"
    (case_dir / "mail").mkdir(parents=True)
    (case_dir / "docs").mkdir(parents=True)
    (case_dir / "case_summary.json").write_text(json.dumps({"shipment_number": "AN-11755", "mode": "Sea"}), encoding="utf-8")
    (case_dir / "mail" / "history.json").write_text(json.dumps({"messages": [{"from": "kunde@example.com", "subject": "Bitte Status"}]}), encoding="utf-8")
    (case_dir / "tms_snapshot.json").write_text(json.dumps({"shipment_number": "AN-11755", "status": "docs pending", "pickup_date": "2026-05-12"}), encoding="utf-8")
    (case_dir / "docs" / "analysis.json").write_text(json.dumps({"documents": [], "missing": ["commercial_invoice"]}), encoding="utf-8")

    result = run_employee_runtime(
        EmployeeRequest(text="Schreib dem Kunden ein kurzes Update zu AN-11755 mit Mails, TMS und Docs", channel="teams"),
        root=root,
    )

    assert result.employee_response.mode == ResponseMode.DRAFT_ONLY
    assert [row.agent for row in result.specialist_results] == ["case_context", "document_analyst", "mail_history", "tms_snapshot"]
    assert "Entwurf für Kunde" in (result.draft_response or "")
    assert "Nicht gesendet" in (result.draft_response or "")
    assert "docs pending" in (result.draft_response or "")
    assert "commercial_invoice" in (result.draft_response or "")
    assert result.should_send_customer_message is False
    assert result.should_write_tms is False
    assert not (case_dir / "employee" / "specialist_results.jsonl").exists()


def test_teams_send_runtime_is_guarded_and_does_not_send(tmp_path: Path) -> None:
    result = run_employee_runtime(
        EmployeeRequest(text="Poste das Update zu AN-11755 in Teams: Dokumente sind in Prüfung", channel="telegram"),
        root=tmp_path / "cargolo_asr",
    )

    assert result.employee_response.mode == ResponseMode.GUARDED_ACTION_REQUIRED
    assert result.employee_response.boundary_action == BoundaryAction.TEAMS_SEND
    assert result.should_send_to_teams is False
    assert "Teams" in result.draft_response


def test_honcho_memory_snapshot_is_included_but_optional(tmp_path: Path) -> None:
    snapshot = HonchoMemorySnapshot(
        available=True,
        peer="user",
        facts=["User wants CARGOLO to be agent-first, not a rigid workflow bot."],
        excerpts=["normal Hermes functions must continue to work"],
    )

    result = run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 los?", channel="telegram"),
        root=tmp_path / "cargolo_asr",
        memory_snapshot=snapshot,
    )

    assert result.memory_snapshot.available is True
    assert "agent-first" in result.memory_snapshot.facts[0]
    assert result.to_audit_row()["memory_snapshot"]["available"] is True
    assert result.to_audit_row()["should_send_to_teams"] is False


def test_honcho_unavailable_snapshot_does_not_block_runtime(tmp_path: Path) -> None:
    snapshot = HonchoMemorySnapshot(available=False, error="Honcho session could not be initialized.")

    result = run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 los?", channel="telegram"),
        root=tmp_path / "cargolo_asr",
        memory_snapshot=snapshot,
    )

    assert result.memory_snapshot.available is False
    assert result.employee_response.mode == ResponseMode.CASE_ASSIST
    assert result.should_send_to_teams is False


def test_case_assist_reads_structured_local_sources_and_synthesizes_compact_ops_answer(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    case_dir = root / "orders" / "AN-11755"
    (case_dir / "mail").mkdir(parents=True)
    (case_dir / "docs").mkdir(parents=True)
    (case_dir / "case_summary.json").write_text(
        json.dumps({"shipment_number": "AN-11755", "mode": "Sea", "lane": "Hamburg -> Shanghai"}),
        encoding="utf-8",
    )
    (case_dir / "mail" / "history.json").write_text(
        json.dumps(
            {
                "messages": [
                    {"from": "kunde@example.com", "subject": "CI fehlt", "body": "Bitte Status zur Commercial Invoice."},
                    {"from": "asr@cargolo.com", "subject": "Reminder", "body": "Packing List liegt vor."},
                ]
            }
        ),
        encoding="utf-8",
    )
    (case_dir / "tms_snapshot.json").write_text(
        json.dumps({"shipment_number": "AN-11755", "status": "docs pending", "pickup_date": "2026-05-12"}),
        encoding="utf-8",
    )
    (case_dir / "docs" / "analysis.json").write_text(
        json.dumps({"documents": [{"type": "packing_list", "status": "available"}], "missing": ["commercial_invoice"]}),
        encoding="utf-8",
    )

    result = run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 los? Schau in Mails, TMS und Docs.", channel="telegram"),
        root=root,
    )

    by_agent = {row.agent: row for row in result.specialist_results}
    assert by_agent["case_context"].findings[0]["summary"]["lane"] == "Hamburg -> Shanghai"
    assert by_agent["mail_history"].findings[0]["message_count"] == 2
    assert by_agent["mail_history"].findings[0]["latest_subject"] == "Reminder"
    assert by_agent["tms_snapshot"].findings[0]["snapshot"]["status"] == "docs pending"
    assert by_agent["document_analyst"].findings[0]["missing"] == ["commercial_invoice"]
    assert by_agent["document_analyst"].requires_human is True
    assert "Fallprüfung" in (result.draft_response or "")
    assert "AN-11755" in (result.draft_response or "")
    assert "docs pending" in (result.draft_response or "")
    assert "commercial_invoice" in (result.draft_response or "")
    assert "Read-only ausgeführt: kein TMS-Write" in (result.draft_response or "")
    assert result.should_send_to_teams is False
    assert result.should_write_tms is False
    assert result.should_send_customer_message is False


def test_case_assist_marks_missing_local_sources_as_needs_review_not_failed(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755").mkdir(parents=True)

    result = run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 los? Schau in Mails, TMS und Docs.", channel="telegram"),
        root=root,
    )

    statuses = {row.agent: row.status.value for row in result.specialist_results}
    assert statuses["mail_history"] == "needs_review"
    assert statuses["tms_snapshot"] == "needs_review"
    assert statuses["document_analyst"] == "needs_review"
    assert all(row.write_intents == [] for row in result.specialist_results)
    assert "fehlend/nicht lokal verfügbar" in (result.draft_response or "")


def test_case_assist_writes_local_review_marker_when_specialists_need_review(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    case_dir = root / "orders" / "AN-11755"
    case_dir.mkdir(parents=True)

    result = run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 los? Schau in Mails, TMS und Docs.", channel="teams"),
        root=root,
    )

    marker_path = case_dir / "employee" / "review_required.json"
    assert marker_path.exists()
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    assert marker["order_id"] == "AN-11755"
    assert marker["requires_human"] is True
    assert marker["source"] == "employee_runtime"
    assert marker["specialists"] == ["document_analyst", "mail_history", "tms_snapshot"]
    assert marker["summary"] == result.draft_response
    assert marker["should_send_to_teams"] is False
    assert marker["should_write_tms"] is False
    assert marker["should_send_customer_message"] is False


def test_case_assist_removes_stale_review_marker_when_sources_are_clean(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    case_dir = root / "orders" / "AN-11755"
    (case_dir / "mail").mkdir(parents=True)
    (case_dir / "docs").mkdir(parents=True)
    employee_dir = case_dir / "employee"
    employee_dir.mkdir(parents=True)
    marker_path = employee_dir / "review_required.json"
    marker_path.write_text(json.dumps({"stale": True, "specialists": ["document_analyst", "mail_history", "tms_snapshot"]}), encoding="utf-8")
    (case_dir / "mail" / "history.json").write_text(json.dumps({"messages": [{"subject": "OK"}]}), encoding="utf-8")
    (case_dir / "tms_snapshot.json").write_text(json.dumps({"status": "clean"}), encoding="utf-8")
    (case_dir / "docs" / "analysis.json").write_text(json.dumps({"documents": [], "missing": []}), encoding="utf-8")

    run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 los? Schau in Mails, TMS und Docs.", channel="teams"),
        root=root,
    )

    assert not marker_path.exists()


def test_case_assist_keeps_review_marker_when_narrow_clean_run_did_not_recheck_marked_scope(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    case_dir = root / "orders" / "AN-11755"
    employee_dir = case_dir / "employee"
    employee_dir.mkdir(parents=True)
    marker_path = employee_dir / "review_required.json"
    marker = {"order_id": "AN-11755", "specialists": ["document_analyst", "mail_history", "tms_snapshot"]}
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    (case_dir / "case_summary.json").write_text(json.dumps({"shipment_number": "AN-11755", "mode": "Sea"}), encoding="utf-8")

    run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 los?", channel="teams"),
        root=root,
    )

    assert json.loads(marker_path.read_text(encoding="utf-8"))["specialists"] == marker["specialists"]


def test_case_assist_preserves_out_of_scope_marker_specialists_when_narrow_run_still_needs_review(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    case_dir = root / "orders" / "AN-11755"
    employee_dir = case_dir / "employee"
    employee_dir.mkdir(parents=True)
    marker_path = employee_dir / "review_required.json"
    marker = {"order_id": "AN-11755", "specialists": ["document_analyst", "mail_history", "tms_snapshot"]}
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    (case_dir / "case_summary.json").write_text(json.dumps({"shipment_number": "AN-11755", "mode": "Sea"}), encoding="utf-8")

    run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 los? Schau in Mails.", channel="teams"),
        root=root,
    )

    updated = json.loads(marker_path.read_text(encoding="utf-8"))
    assert updated["specialists"] == ["document_analyst", "mail_history", "tms_snapshot"]
    assert "mail_history" in updated["summary"]


def test_unimplemented_context_specialist_is_needs_review_not_false_ok(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    (root / "orders" / "AN-11755").mkdir(parents=True)

    result = run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 pricing und billing?", channel="telegram"),
        root=root,
    )

    by_agent = {row.agent: row for row in result.specialist_results}
    assert by_agent["pricing_context"].status.value == "needs_review"
    assert by_agent["billing_context"].status.value == "needs_review"
    assert by_agent["pricing_context"].requires_human is True
    assert by_agent["billing_context"].requires_human is True
    assert all(row.write_intents == [] for row in result.specialist_results)


def test_document_reader_requires_review_when_only_raw_files_exist_without_analysis(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    docs_dir = root / "orders" / "AN-11755" / "docs"
    docs_dir.mkdir(parents=True)
    (docs_dir / "packing_list.pdf").write_text("fake pdf text", encoding="utf-8")

    result = run_employee_runtime(
        EmployeeRequest(text="Was ist mit AN-11755 los? Schau in Docs.", channel="telegram"),
        root=root,
    )

    docs = {row.agent: row for row in result.specialist_results}["document_analyst"]
    assert docs.status.value == "needs_review"
    assert docs.requires_human is True
    assert docs.findings[0]["files"] == ["packing_list.pdf"]
    assert docs.write_intents == []
