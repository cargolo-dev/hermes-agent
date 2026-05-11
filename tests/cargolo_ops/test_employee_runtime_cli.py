from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _write_case(root: Path, order_id: str = "AN-11755", *, status: str = "docs pending") -> None:
    case_dir = root / "orders" / order_id
    (case_dir / "mail").mkdir(parents=True)
    (case_dir / "docs").mkdir(parents=True)
    (case_dir / "case_summary.json").write_text(
        json.dumps({"shipment_number": order_id, "mode": "Sea", "lane": "Hamburg -> Shanghai"}),
        encoding="utf-8",
    )
    (case_dir / "mail" / "history.json").write_text(
        json.dumps({"messages": [{"subject": "CI fehlt", "body": "Bitte Status."}]}),
        encoding="utf-8",
    )
    (case_dir / "tms_snapshot.json").write_text(
        json.dumps({"shipment_number": order_id, "status": status}),
        encoding="utf-8",
    )
    (case_dir / "docs" / "analysis.json").write_text(
        json.dumps({"missing": ["commercial_invoice"], "documents": []}),
        encoding="utf-8",
    )


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "plugins.cargolo_ops.employee_runtime_cli", *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_employee_runtime_cli_outputs_compact_text_and_writes_audit(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    _write_case(root)

    result = _run_cli(
        "--root",
        str(root),
        "--order",
        "AN-11755",
        "--text",
        "Was ist der Stand? Schau in Mails, TMS und Docs.",
    )

    assert result.returncode == 0, result.stderr
    assert "Fallprüfung AN-11755" in result.stdout
    assert "Dokumente offen" in result.stdout
    assert "Handelsrechnung" in result.stdout
    assert "<h3>Nächster Schritt</h3>" in result.stdout
    assert "Read-only ausgeführt" not in result.stdout
    assert "should_send_to_teams" not in result.stdout

    audit_path = root / "runtime" / "employee_runtime_cli.jsonl"
    rows = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["request"]["order_id"] == "AN-11755"
    assert rows[0]["result"]["should_send_to_teams"] is False
    assert rows[0]["result"]["should_write_tms"] is False
    assert rows[0]["result"]["should_send_customer_message"] is False


def test_employee_runtime_cli_json_mode_is_machine_readable_and_default_deny(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"

    result = _run_cli(
        "--root",
        str(root),
        "--order",
        "AN-11755",
        "--text",
        "Setze MRN 26DE99999 in TMS",
        "--json",
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["employee_response"]["mode"] == "guarded_action_required"
    assert payload["employee_response"]["boundary_action"] == "tms_write"
    assert payload["should_write_tms"] is False
    assert payload["should_send_to_teams"] is False
    assert payload["should_send_customer_message"] is False
    assert payload["specialist_results"] == []


def test_employee_runtime_cli_requires_text(tmp_path: Path) -> None:
    result = _run_cli("--root", str(tmp_path / "cargolo_asr"), "--order", "AN-11755")

    assert result.returncode == 2
    assert "--text" in result.stderr


def test_employee_runtime_cli_latest_picks_most_recent_case(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    _write_case(root, "AN-10000", status="old case")
    _write_case(root, "AN-20000", status="latest case")
    latest_marker = root / "orders" / "AN-20000" / "tms_snapshot.json"
    latest_marker.touch()

    result = _run_cli(
        "--root",
        str(root),
        "--latest",
        "--text",
        "Was ist der Stand?",
    )

    assert result.returncode == 0, result.stderr
    assert "Fallprüfung AN-20000" in result.stdout
    assert "latest case" in result.stdout
    assert "AN-10000" not in result.stdout


def test_employee_runtime_cli_next_review_prefers_case_with_review_markers(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    _write_case(root, "AN-10000", status="clean")
    _write_case(root, "AN-20000", status="needs attention")
    review_file = root / "orders" / "AN-20000" / "review_required.json"
    review_file.write_text(json.dumps({"reason": "docs missing"}), encoding="utf-8")

    result = _run_cli(
        "--root",
        str(root),
        "--next-review",
        "--text",
        "Was braucht Review im TMS?",
    )

    assert result.returncode == 0, result.stderr
    assert "Fallprüfung AN-20000" in result.stdout
    assert "needs attention" in result.stdout


def test_employee_runtime_cli_rejects_ambiguous_order_selection(tmp_path: Path) -> None:
    result = _run_cli(
        "--root",
        str(tmp_path / "cargolo_asr"),
        "--order",
        "AN-11755",
        "--latest",
        "--text",
        "Was ist der Stand?",
    )

    assert result.returncode == 2
    assert "not allowed with argument" in result.stderr.lower()


def test_employee_runtime_cli_latest_ignores_non_order_directories(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    _write_case(root, "AN-11755", status="real case")
    archive = root / "orders" / "archive"
    archive.mkdir(parents=True)
    (archive / "note.txt").write_text("not a case", encoding="utf-8")

    result = _run_cli("--root", str(root), "--latest", "--text", "Was ist der Stand im TMS?")

    assert result.returncode == 0, result.stderr
    assert "Fallprüfung AN-11755" in result.stdout
    assert "real case" in result.stdout
    assert "archive" not in result.stdout


def test_employee_runtime_cli_next_review_errors_when_no_review_marker_exists(tmp_path: Path) -> None:
    root = tmp_path / "cargolo_asr"
    _write_case(root, "AN-11755", status="no marker")

    result = _run_cli("--root", str(root), "--next-review", "--text", "Was braucht Review?")

    assert result.returncode == 2
    assert "no local review-marked case folders" in result.stderr.lower()
