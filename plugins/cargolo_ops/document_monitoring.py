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
    cross_document = report["reconciliation"].get("cross_document_comparisons") or []
    if cross_document:
        lines.extend(["", "## Dokument-zu-Dokument-Abgleich"])
        for row in cross_document[:5]:
            if isinstance(row, dict):
                lines.append(f"- {row.get('severity', 'medium')}: {row.get('label') or row.get('field')} — {row.get('summary') or ''}")
    raw_review_intents = report.get("document_review_intents")
    review_intents: list[Any] = raw_review_intents if isinstance(raw_review_intents, list) else []
    lines.extend([
        "",
        "## Agent Review",
        f"- Review-Modus: {report.get('review_mode') or 'agent_first'}",
        f"- Side-Effect-Policy: {report.get('side_effect_policy') or 'keine automatische TMS-/Kundenaktion'}",
        f"- Review-Intents: {len(review_intents)}",
    ])
    for intent in review_intents[:5]:
        if isinstance(intent, dict):
            label = intent.get("label") or intent.get("target") or "Feld"
            lines.append(f"- {label}: TMS {intent.get('current_tms_value') or '-'} ↔ Dokument {intent.get('document_value') or intent.get('value') or '-'}")
    lines.extend([
        "",
        "## Nicht gemacht",
        "- Keine Kundenmail gesendet.",
        "- Keine TMS-Statusänderung vorgenommen.",
        "- Keine Dokumente hochgeladen.",
        "- Keine TMS-Review-Karte ohne Agent-/Operator-Entscheidung erzeugt.",
    ])
    return "\n".join(lines) + "\n"


def _shipment_context(tms_snapshot: dict[str, Any]) -> dict[str, Any]:
    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    if not isinstance(detail, dict):
        detail = {}
    origin = detail.get("origin") if isinstance(detail.get("origin"), dict) else {}
    destination = detail.get("destination") if isinstance(detail.get("destination"), dict) else {}
    totals = tms_snapshot.get("totals") if isinstance(tms_snapshot.get("totals"), dict) else {}
    cargo_rows = detail.get("cargo") if isinstance(detail.get("cargo"), list) else []
    cargo_description = ""
    if cargo_rows:
        first_cargo = next((row for row in cargo_rows if isinstance(row, dict)), {})
        cargo_description = str(first_cargo.get("description") or first_cargo.get("goods_description") or "").strip()
    customer = detail.get("customer") if isinstance(detail.get("customer"), dict) else {}
    customer_name = (
        customer.get("company_name")
        or customer.get("name")
        or detail.get("customer_name")
        or detail.get("customer_company_name")
    )
    customer_reference = (
        detail.get("customer_reference")
        or detail.get("customer_ref")
        or detail.get("customer_order_number")
        or detail.get("reference")
        or detail.get("shipment_reference")
    )
    return {
        "customer": customer_name,
        "customer_reference": customer_reference,
        "status": tms_snapshot.get("status") or detail.get("status"),
        "network": detail.get("network") or detail.get("transport_mode") or tms_snapshot.get("network") or tms_snapshot.get("mode"),
        "origin_city": origin.get("city") or detail.get("origin_city"),
        "origin_country": origin.get("country") or origin.get("country_code") or detail.get("origin_country"),
        "destination_city": destination.get("city") or detail.get("destination_city"),
        "destination_country": destination.get("country") or destination.get("country_code") or detail.get("destination_country"),
        "incoterms": detail.get("incoterms") or detail.get("incoterm"),
        "pieces": totals.get("total_pieces") or detail.get("pieces") or detail.get("total_pieces"),
        "weight_kg": totals.get("total_weight_kg") or detail.get("weight_kg") or detail.get("total_weight_kg"),
        "volume_m3": totals.get("total_volume_m3") or detail.get("volume_m3") or detail.get("total_volume_m3"),
        "cargo_description": cargo_description or detail.get("cargo_description"),
    }


def run_document_monitoring(
    order_id: str,
    *,
    storage_root: Path | None = None,
    refresh_history: bool = True,
    analyze_documents: bool = True,
    trigger_event: dict[str, Any] | None = None,
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
        "review_mode": "agent_first",
        "side_effect_policy": "deterministic_evidence_only_no_tms_write_or_customer_contact_without_explicit_review",
        "tms_context": _shipment_context(tms_snapshot),
        "evidence": {
            "trigger_event_present": bool(trigger_event),
            "tms_snapshot_path": lifecycle.get("tms_snapshot_path"),
            "document_registry_path": lifecycle.get("document_registry_path"),
            "mail_history": {
                "history_sync_count": lifecycle.get("history_sync_count", 0),
                "history_sync_error": lifecycle.get("history_sync_error"),
                "last_email_at": lifecycle.get("last_email_at"),
            },
            "source_contract": "TMS/Mail/Dokumente werden synchronisiert; Hermes/Operator entscheidet über Folgeschritte.",
        },
        "lifecycle": {k: lifecycle.get(k) for k in ["initialized", "history_sync_count", "history_sync_error", "last_email_at", "tms_snapshot_path", "document_registry_path"]},
        "registry_summary": {
            "received_documents": len(registry.get("received_documents", []) or []),
            "tms_documents": len(registry.get("tms_documents", []) or []),
            "mirrored_tms_documents": len(registry.get("mirrored_tms_documents", []) or []),
            "tms_mirroring_gaps": len(registry.get("tms_mirroring_gaps", []) or []),
        },
        "reconciliation": reconciliation,
    }
    if trigger_event:
        report["trigger_event"] = {
            "id": trigger_event.get("id"),
            "entity_type": trigger_event.get("entity_type"),
            "action": trigger_event.get("action"),
            "changed_at": trigger_event.get("changed_at"),
            "changed_by_name": trigger_event.get("changed_by_name"),
            "source": trigger_event.get("source"),
            "metadata": trigger_event.get("metadata") if isinstance(trigger_event.get("metadata"), dict) else {},
        }
    json_path, md_path = store.save_document_monitoring_report(lifecycle["order_id"], report, _render_markdown(report))
    report["report_json_path"] = str(json_path)
    report["report_md_path"] = str(md_path)
    store.append_audit(
        lifecycle["order_id"],
        action="document_monitoring",
        result="ok",
        files=[str(json_path), str(md_path)],
        extra={
            "risk": reconciliation.get("risk"),
            "needs_human_review": reconciliation.get("needs_human_review"),
            "trigger_activity_id": (trigger_event or {}).get("id"),
        },
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
