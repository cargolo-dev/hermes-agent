from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .case_lifecycle import sync_case_lifecycle
from .document_reconciliation import reconcile_documents
from .models import utc_now_iso
from .storage import CaseStore
from .tms_provider import build_tms_provider_from_env


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# Dokumentenmonitoring {report['order_id']}",
        "",
        f"- Erstellt: {report['generated_at']}",
        f"- Modus: {report.get('mode') or 'unknown'}",
        f"- Status: {report.get('tms_status') or '-'}",
        f"- Risiko: {report['reconciliation']['risk']}",
        f"- Human Review: {'ja' if report['reconciliation']['needs_human_review'] else 'nein'}",
        "",
        "## Dokumente",
        f"- Erwartet: {', '.join(report['reconciliation']['expected_types']) or '-'}",
        f"- Erhalten/erkannt: {', '.join(report['reconciliation']['received_types']) or '-'}",
        f"- Fehlend: {', '.join(report['reconciliation']['missing_types']) or '-'}",
        f"- Missing-Policy: {report['reconciliation']['missing_policy']}",
        "",
        "## Auffälligkeiten",
    ]
    findings = report["reconciliation"].get("findings") or []
    if findings:
        for row in findings:
            lines.append(f"- {row.get('severity', 'low')}: {row.get('type')} — {row.get('filename') or '-'} — {row.get('summary') or ''}")
    else:
        lines.append("- Keine present-document/TMS-Mirroring-Auffälligkeiten erkannt.")
    lines.extend([
        "",
        "## Nicht gemacht",
        "- Keine Kundenmail gesendet.",
        "- Keine TMS-Statusänderung vorgenommen.",
        "- Keine Dokumente hochgeladen.",
    ])
    return "\n".join(lines) + "\n"


def run_document_monitoring(
    order_id: str,
    *,
    storage_root: Path | None = None,
    refresh_history: bool = True,
    analyze_documents: bool = True,
) -> dict[str, Any]:
    lifecycle = sync_case_lifecycle(
        order_id,
        storage_root=storage_root,
        refresh_history=refresh_history,
        analyze_documents=analyze_documents,
    )
    store = CaseStore(storage_root)
    case_root = Path(lifecycle["case_root"])
    registry = lifecycle["registry"]
    tms_snapshot = lifecycle["tms_snapshot"]
    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    reconciliation = reconcile_documents(order_id=lifecycle["order_id"], tms_snapshot=tms_snapshot, registry=registry)
    report = {
        "version": 1,
        "generated_at": utc_now_iso(),
        "order_id": lifecycle["order_id"],
        "case_root": str(case_root),
        "mode": reconciliation.get("mode"),
        "tms_status": tms_snapshot.get("status") or (detail or {}).get("status"),
        "lifecycle": {k: lifecycle.get(k) for k in ["initialized", "history_sync_count", "history_sync_error", "tms_snapshot_path", "document_registry_path"]},
        "registry_summary": {
            "received_documents": len(registry.get("received_documents", []) or []),
            "tms_documents": len(registry.get("tms_documents", []) or []),
            "mirrored_tms_documents": len(registry.get("mirrored_tms_documents", []) or []),
            "tms_mirroring_gaps": len(registry.get("tms_mirroring_gaps", []) or []),
        },
        "reconciliation": reconciliation,
    }
    json_path, md_path = store.save_document_monitoring_report(lifecycle["order_id"], report, _render_markdown(report))
    report["report_json_path"] = str(json_path)
    report["report_md_path"] = str(md_path)
    store.append_audit(
        lifecycle["order_id"],
        action="document_monitoring",
        result="ok",
        files=[str(json_path), str(md_path)],
        extra={"risk": reconciliation.get("risk"), "needs_human_review": reconciliation.get("needs_human_review")},
    )
    return report


def discover_asr_shipments(*, limit: int = 5) -> list[str]:
    provider = build_tms_provider_from_env()
    if provider is None:
        return []
    rows = provider.shipments_list(transport_category="asr", page=1, per_page=limit)
    result: list[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        shipment_number = str(row.get("shipment_number") or "").strip().upper()
        if shipment_number:
            result.append(shipment_number)
    return result[:limit]


def main() -> None:
    parser = argparse.ArgumentParser(description="CARGOLO ASR document monitoring")
    parser.add_argument("order_id", nargs="?", help="AN/BU shipment number. If omitted, discover live ASR shipments.")
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--skip-analysis", action="store_true", help="Plumbing only; do not use for production ops reports.")
    parser.add_argument("--no-history", action="store_true")
    args = parser.parse_args()

    order_ids = [args.order_id.strip().upper()] if args.order_id else discover_asr_shipments(limit=args.limit)
    reports = [
        run_document_monitoring(order_id, refresh_history=not args.no_history, analyze_documents=not args.skip_analysis)
        for order_id in order_ids
    ]
    runtime_path = CaseStore().runtime_root / "document_monitoring_phase1_latest.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(json.dumps({"generated_at": utc_now_iso(), "reports": reports}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "ok", "count": len(reports), "runtime_path": str(runtime_path), "reports": reports}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
