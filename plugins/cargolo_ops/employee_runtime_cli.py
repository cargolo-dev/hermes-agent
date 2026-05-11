"""CLI smoke runner for the local CARGOLO employee runtime.

Read-only by design: runs the runtime, prints either compact text or JSON, and writes
an audit row under the selected CARGOLO root. It does not send Teams messages,
write TMS, or contact customers.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from .employee_agent import EmployeeRequest
from .employee_runtime import run_employee_runtime
from .models import normalize_order_ids


REVIEW_MARKER_NAMES = (
    "review_required.json",
    "needs_review.json",
    "pending_review.json",
    "employee/review_required.json",
)


def _default_root() -> Path:
    return Path.home() / ".hermes" / "cargolo_asr"


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _case_dirs(root: Path) -> list[Path]:
    orders_dir = root / "orders"
    if not orders_dir.exists():
        return []
    return sorted(
        [
            path
            for path in orders_dir.iterdir()
            if path.is_dir() and not path.name.startswith(".") and normalize_order_ids(path.name)
        ],
        key=lambda path: path.name,
    )


def _latest_mtime(path: Path) -> float:
    mtimes = [path.stat().st_mtime]
    for child in path.rglob("*"):
        try:
            mtimes.append(child.stat().st_mtime)
        except OSError:
            continue
    return max(mtimes)


def _select_latest_order(root: Path) -> str | None:
    cases = _case_dirs(root)
    if not cases:
        return None
    return max(cases, key=lambda path: (_latest_mtime(path), path.name)).name


def _has_review_marker(case_dir: Path) -> bool:
    return any((case_dir / marker).exists() for marker in REVIEW_MARKER_NAMES)


def _select_next_review_order(root: Path) -> str | None:
    cases = [case_dir for case_dir in _case_dirs(root) if _has_review_marker(case_dir)]
    if not cases:
        return None
    return max(cases, key=lambda path: (_latest_mtime(path), path.name)).name


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m plugins.cargolo_ops.employee_runtime_cli",
        description="Run the local CARGOLO employee runtime in read-only smoke mode.",
    )
    parser.add_argument("--text", required=True, help="Employee request text to evaluate.")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--order", dest="order_id", help="Optional AN/BU order id context.")
    selection.add_argument("--latest", action="store_true", help="Use the most recently changed local case folder.")
    selection.add_argument("--next-review", action="store_true", help="Use the most recent case folder with a review marker.")
    parser.add_argument("--channel", default="cli", help="Source channel label for audit context.")
    parser.add_argument("--actor", help="Optional actor/user label for audit context.")
    parser.add_argument("--root", type=Path, default=_default_root(), help="CARGOLO ASR root directory.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable runtime result JSON.")
    parser.add_argument(
        "--no-audit",
        action="store_true",
        help="Do not append CLI audit row. Specialist result JSONL writes still follow runtime behavior.",
    )
    return parser


def _resolve_order_id(args: argparse.Namespace, parser: argparse.ArgumentParser) -> str | None:
    if args.order_id:
        return args.order_id
    if args.latest:
        order_id = _select_latest_order(args.root)
        if not order_id:
            parser.error(f"no local case folders found under {args.root / 'orders'}")
        return order_id
    if args.next_review:
        order_id = _select_next_review_order(args.root)
        if not order_id:
            parser.error(f"no local review-marked case folders found under {args.root / 'orders'}")
        return order_id
    return None


def _audit_row(request: EmployeeRequest, result: Any) -> dict[str, Any]:
    return {
        "request": {
            "text": request.text,
            "channel": request.channel,
            "order_id": request.order_id,
            "actor": request.actor,
            "context_refs": request.context_refs,
        },
        "result": result.to_audit_row(),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    order_id = _resolve_order_id(args, parser)

    request = EmployeeRequest(text=args.text, channel=args.channel, order_id=order_id, actor=args.actor)
    result = run_employee_runtime(request, root=args.root)

    if not args.no_audit:
        _append_jsonl(args.root / "runtime" / "employee_runtime_cli.jsonl", _audit_row(request, result))

    if args.json:
        print(json.dumps(result.to_audit_row(), ensure_ascii=False, sort_keys=True))
    else:
        print(result.draft_response or "")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
