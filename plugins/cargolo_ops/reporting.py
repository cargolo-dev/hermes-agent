from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .storage import CaseStore


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def generate_daily_report(root: Path | None = None) -> dict[str, Any]:
    store = CaseStore(root)
    summary = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "open_cases_without_reply": [],
        "cases_with_missing_documents": [],
        "cases_with_contradictions": [],
        "overdue_internal_tasks": [],
        "exceptions_by_mode": {"air": 0, "ocean": 0, "rail": 0, "unknown": 0},
        "unassigned_cases": [],
    }

    for order_id in store.list_orders():
        case_root = store.order_path(order_id)
        state = _read_json(case_root / "case_state.json")
        tasks = _read_jsonl(case_root / "tasks/task_log.jsonl")
        risks = state.get("risks", []) or []
        missing = state.get("missing_information", []) or []
        if state.get("reply_recommended"):
            summary["open_cases_without_reply"].append(order_id)
        if missing:
            summary["cases_with_missing_documents"].append({"order_id": order_id, "missing": missing})
        if risks:
            summary["cases_with_contradictions"].append({"order_id": order_id, "risks": risks})
        mode = state.get("mode", "unknown") or "unknown"
        if state.get("current_status") in {"delay_or_exception", "complaint", "customs_or_compliance"}:
            summary["exceptions_by_mode"][mode if mode in summary["exceptions_by_mode"] else "unknown"] += 1
        now = datetime.now(timezone.utc)
        for task in tasks:
            due_at = task.get("due_at")
            if not due_at:
                continue
            try:
                due = datetime.fromisoformat(due_at.replace("Z", "+00:00"))
            except ValueError:
                continue
            if due < now and task.get("created"):
                summary["overdue_internal_tasks"].append({"order_id": order_id, "task": task})

    if store.review_root.exists():
        for path in sorted(store.review_root.glob("*.json")):
            summary["unassigned_cases"].append(path.name)

    markdown = [
        "# CARGOLO ASR Daily Ops Briefing",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        f"- Open cases without reply: {len(summary['open_cases_without_reply'])}",
        f"- Cases with missing documents: {len(summary['cases_with_missing_documents'])}",
        f"- Cases with contradictions: {len(summary['cases_with_contradictions'])}",
        f"- Overdue internal tasks: {len(summary['overdue_internal_tasks'])}",
        f"- Unassigned cases: {len(summary['unassigned_cases'])}",
        "",
        "## Exceptions by mode",
    ]
    for mode, count in summary["exceptions_by_mode"].items():
        markdown.append(f"- {mode}: {count}")
    markdown.append("")
    markdown.append("## Cases needing reply")
    reply_lines = [f"- {order_id}" for order_id in summary["open_cases_without_reply"]] or ["- none"]
    markdown.extend(reply_lines)
    markdown.append("")
    markdown.append("## Unassigned cases")
    unassigned_lines = [f"- {name}" for name in summary["unassigned_cases"]] or ["- none"]
    markdown.extend(unassigned_lines)
    summary["markdown"] = "\n".join(markdown)
    return summary
