from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .document_monitoring import run_document_monitoring
from .models import utc_now_iso
from .ops_notifications import send_manual_ops_notification
from .storage import CaseStore
from .tms_provider import build_tms_provider_from_env

DEFAULT_ADMIN_USER_ID = 106
STATE_FILE_NAME = "document_activity_monitor_state.json"
LATEST_RUN_FILE_NAME = "document_activity_monitor_latest.json"


def _activity_state_path(storage_root: Path | None = None) -> Path:
    return CaseStore(storage_root).runtime_root / STATE_FILE_NAME


def _latest_run_path(storage_root: Path | None = None) -> Path:
    return CaseStore(storage_root).runtime_root / LATEST_RUN_FILE_NAME


def _load_state(storage_root: Path | None = None) -> dict[str, Any]:
    path = _activity_state_path(storage_root)
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {"last_seen_activity_id": 0, "processed_activity_ids": []}


def _save_state(state: dict[str, Any], storage_root: Path | None = None) -> Path:
    path = _activity_state_path(storage_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _save_latest_run(payload: dict[str, Any], storage_root: Path | None = None) -> Path:
    path = _latest_run_path(storage_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _activity_id(row: dict[str, Any]) -> int:
    try:
        return int(row.get("id") or 0)
    except Exception:
        return 0


def _activity_order_id(row: dict[str, Any]) -> str:
    request = row.get("asr_request") if isinstance(row.get("asr_request"), dict) else {}
    candidates = [
        request.get("request_number"),
        request.get("booking_number"),
        row.get("an"),
        row.get("shipment_number"),
    ]
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    candidates.extend([metadata.get("shipment_number"), metadata.get("an")])
    for value in candidates:
        text = str(value or "").strip().upper()
        if text.startswith(("AN-", "BU-")):
            return text
    return ""


def _is_document_upload(row: dict[str, Any]) -> bool:
    return (
        str(row.get("entity_type") or "").strip().lower() == "document"
        and str(row.get("action") or "").strip().lower() in {"upload", "create"}
    )


def _doc_type_label(value: Any) -> str:
    raw = str(value or "unknown").strip().lower()
    return {
        "commercial_invoice": "Handelsrechnung",
        "packing_list": "Packliste",
        "air_waybill": "AWB/HAWB",
        "bill_of_lading": "B/L",
        "master_bl": "Master B/L",
        "house_bl": "House B/L",
        "hbl": "House B/L",
        "mbl": "Master B/L",
        "proof_of_delivery": "POD",
        "mrn": "MRN/Zollreferenz",
        "customs_document": "Zolldokument",
        "billing": "Abrechnungsbeleg",
        "offer": "Angebot",
        "unknown": "Dokument",
        "unbekannt": "Dokument",
    }.get(raw, raw.replace("_", " ") or "Dokument")


def _finding_rank(row: Any) -> tuple[int, str]:
    if not isinstance(row, dict):
        return (9, str(row or ""))
    severity = str(row.get("severity") or "").lower()
    finding_type = str(row.get("type") or "").lower()
    text = str(row.get("summary") or "").lower()
    if severity in {"critical", "high"} or "mrn" in finding_type or "customs" in finding_type:
        return (0, text)
    if any(token in finding_type + " " + text for token in ("weight", "gewicht", "piece", "packstück", "package")):
        return (1, text)
    if finding_type in {"document_open_question", "implausible_goods_value"}:
        return (2, text)
    if severity == "medium":
        return (3, text)
    return (4, text)


def _finding_text(row: Any) -> str:
    if not isinstance(row, dict):
        return str(row or "").strip()
    filename = str(row.get("filename") or "").strip()
    summary = str(row.get("summary") or row.get("type") or "Dokument fachlich prüfen.").strip()
    return f"{filename}: {summary}" if filename else summary


def _route_hint(context: dict[str, Any]) -> str:
    parts = []
    for city_key, country_key in (("origin_city", "origin_country"), ("destination_city", "destination_country")):
        value = " ".join(str(context.get(key) or "").strip() for key in (city_key, country_key) if str(context.get(key) or "").strip())
        if value:
            parts.append(value)
    return " → ".join(parts)


def _human_document_message(*, order_id: Any, filename: Any, doc_type: Any, context: dict[str, Any], findings: list[Any], needs_review: bool) -> str:
    label = _doc_type_label(doc_type)
    route = _route_hint(context)
    status = str(context.get("status") or "").strip()
    network = str(context.get("network") or context.get("mode") or "").strip()
    context_bits = [bit for bit in (network, status, route) if bit]
    lage = f"{label} '{filename}' wurde geprüft."
    if context_bits:
        lage += " Kontext: " + " · ".join(context_bits) + "."
    uploaded_norm = str(filename or "").strip().lower()
    usable_findings = [
        row for row in findings
        if not isinstance(row, dict)
        or str(row.get("filename") or "").strip().lower() == uploaded_norm
        or _finding_rank(row)[0] <= 0
    ]
    top_findings = sorted([row for row in usable_findings if row], key=_finding_rank)[:3]
    if top_findings:
        auffaellig = " | ".join(_finding_text(row) for row in top_findings)
        empfehlung = "Vor Übernahme oder Folgeaktion bitte die auffälligen Werte gegen TMS/Mailverlauf prüfen."
        naechster_schritt = "Führenden Wert festlegen; bei TMS-Korrektur anschließend bewusst freigeben."
    else:
        auffaellig = "Keine fachlichen Dokumenten-Widersprüche erkannt."
        empfehlung = "Keine direkte Aktion nötig; Case-Kontext und Dokumentenstand sind aktualisiert."
        naechster_schritt = "Nur weiter beobachten, bis ein fachlicher Trigger entsteht."
    if needs_review and not top_findings:
        auffaellig = "Dokument ist nicht vollständig automatisch belastbar; manuelle Sichtprüfung sinnvoll."
    return "\n".join([
        f"Lage: {order_id} · {lage}",
        f"Auffällig: {auffaellig}",
        f"Empfehlung: {empfehlung}",
        f"Nächster Schritt: {naechster_schritt}",
    ])


def list_recent_document_uploads(
    *,
    admin_user_id: int = DEFAULT_ADMIN_USER_ID,
    page: int = 1,
    per_page: int = 50,
    an: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any]:
    provider = build_tms_provider_from_env()
    if provider is None or not hasattr(provider, "list_asr_activity_log"):
        raise RuntimeError("TMS MCP activity-log provider is not configured")
    payload = provider.list_asr_activity_log(
        admin_user_id=admin_user_id,
        an=an or None,
        entity_type="document",
        action="upload",
        date_from=date_from,
        date_to=date_to,
        page=page,
        per_page=per_page,
    )
    items = payload.get("items") if isinstance(payload, dict) else []
    uploads = [row for row in items or [] if isinstance(row, dict) and _is_document_upload(row)]
    return {**(payload if isinstance(payload, dict) else {}), "document_uploads": uploads}


def _processor_result_from_report(report: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    reconciliation = report.get("reconciliation") if isinstance(report.get("reconciliation"), dict) else {}
    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
    registry = report.get("registry_summary") if isinstance(report.get("registry_summary"), dict) else {}
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    filename = metadata.get("file_name") or metadata.get("filename") or event.get("field_name") or "Dokument"
    doc_type = metadata.get("document_type") or "unbekannt"
    risk = str(reconciliation.get("risk") or "low").strip().lower()
    needs_review = bool(reconciliation.get("needs_human_review"))
    findings = reconciliation.get("findings") if isinstance(reconciliation.get("findings"), list) else []
    priority = "high" if risk in {"high", "critical"} else ("medium" if needs_review or findings else "low")
    context = report.get("tms_context") if isinstance(report.get("tms_context"), dict) else {}
    message = _human_document_message(
        order_id=report.get("order_id"),
        filename=filename,
        doc_type=doc_type,
        context=context,
        findings=findings,
        needs_review=needs_review,
    )
    pending_review = 1 if needs_review else 0
    return {
        "status": "document_uploaded_checked",
        "order_id": report.get("order_id"),
        "message": message,
        "analysis_summary": message,
        "analysis_priority": priority,
        "history_sync_count": lifecycle.get("history_sync_count", 0),
        "history_sync_status": "error" if lifecycle.get("history_sync_error") else "ok",
        "history_sync_error": lifecycle.get("history_sync_error"),
        "last_email_at": lifecycle.get("last_email_at"),
        "pending_action_summary": {"review": pending_review, "write_now": 0, "not_yet_due": 0, "not_yet_knowable": 0},
        "applied_action_summary": {"applied": 0, "failed": 0, "skipped": 0},
        "latest_subject": str(filename),
        "case_report_path": report.get("report_json_path"),
        "document_monitoring_report_path": report.get("report_json_path"),
        "document_monitoring_report_md_path": report.get("report_md_path"),
        "document_activity_event_id": _activity_id(event),
        "document_activity_changed_at": event.get("changed_at"),
        "document_activity_changed_by": event.get("changed_by_name") or event.get("changed_by"),
        "document_activity_source": event.get("source"),
        "document_activity_file_name": str(filename),
        "document_activity_document_type": str(doc_type),
        "document_registry_summary": registry,
        "document_reconciliation": reconciliation,
        "tms_context": context,
    }


def run_document_activity_monitor(
    *,
    storage_root: Path | None = None,
    admin_user_id: int = DEFAULT_ADMIN_USER_ID,
    max_events: int = 5,
    per_page: int = 50,
    date_from: str | None = None,
    date_to: str | None = None,
    force: bool = False,
    dry_run: bool = False,
    notify_ops_webhook: bool = True,
    refresh_history: bool = True,
    analyze_documents: bool = True,
) -> dict[str, Any]:
    state = _load_state(storage_root)
    last_seen = int(state.get("last_seen_activity_id") or 0)
    processed_ids = {int(x) for x in state.get("processed_activity_ids", []) if str(x).isdigit()}
    activity_payload = list_recent_document_uploads(
        admin_user_id=admin_user_id,
        per_page=per_page,
        date_from=date_from,
        date_to=date_to,
    )
    uploads = activity_payload.get("document_uploads") or []
    candidates: list[dict[str, Any]] = []
    for row in uploads:
        event_id = _activity_id(row)
        if not event_id:
            continue
        if not force and (event_id <= last_seen or event_id in processed_ids):
            continue
        if not _activity_order_id(row):
            continue
        candidates.append(row)
    candidates.sort(key=_activity_id)
    selected = candidates[: max(0, int(max_events or 0))]

    processed: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    notifications: list[dict[str, Any]] = []
    highest_seen = last_seen

    for event in selected:
        event_id = _activity_id(event)
        highest_seen = max(highest_seen, event_id)
        order_id = _activity_order_id(event)
        try:
            if dry_run:
                processed.append({"activity_id": event_id, "order_id": order_id, "dry_run": True, "event": event})
                continue
            report = run_document_monitoring(
                order_id,
                storage_root=storage_root,
                refresh_history=refresh_history,
                analyze_documents=analyze_documents,
                trigger_event=event,
            )
            processor_result = _processor_result_from_report(report, event)
            notification_result: dict[str, Any] | None = None
            if notify_ops_webhook:
                notification_result = send_manual_ops_notification(
                    run_type="document_activity_monitor",
                    payload={
                        "order_id": order_id,
                        "storage_root": str(CaseStore(storage_root).runtime_root),
                        "processor_result": processor_result,
                        "activity_event": event,
                    },
                    allow_route_fallback=True,
                )
                notifications.append({"activity_id": event_id, "order_id": order_id, "result": notification_result})
            processed.append(
                {
                    "activity_id": event_id,
                    "order_id": order_id,
                    "report_json_path": report.get("report_json_path"),
                    "report_md_path": report.get("report_md_path"),
                    "processor_result": processor_result,
                    "document_reconciliation": processor_result.get("document_reconciliation") if isinstance(processor_result, dict) else {},
                    "notification": notification_result,
                }
            )
        except Exception as exc:
            errors.append({"activity_id": event_id, "order_id": order_id, "error": str(exc)})

    if not dry_run:
        successful_ids = {_activity_id(row) for row in selected} - {int(err.get("activity_id") or 0) for err in errors}
        processed_ids.update(successful_ids)
        state["last_seen_activity_id"] = max(highest_seen, last_seen)
        state["processed_activity_ids"] = sorted(processed_ids)[-500:]
        state["updated_at"] = utc_now_iso()
        state_path = _save_state(state, storage_root)
    else:
        state_path = _activity_state_path(storage_root)

    run_payload = {
        "status": "ok" if not errors else "partial_error",
        "generated_at": utc_now_iso(),
        "dry_run": dry_run,
        "source": "tms_activity_log",
        "filter": {"entity_type": "document", "action": "upload", "date_from": date_from, "date_to": date_to},
        "last_seen_activity_id_before": last_seen,
        "last_seen_activity_id_after": state.get("last_seen_activity_id", last_seen),
        "candidates": len(candidates),
        "selected": len(selected),
        "processed_count": len(processed),
        "error_count": len(errors),
        "processed": processed,
        "errors": errors,
        "notifications": notifications,
        "state_path": str(state_path),
    }
    latest_path = _save_latest_run(run_payload, storage_root)
    run_payload["latest_run_path"] = str(latest_path)
    return run_payload


def main() -> None:
    parser = argparse.ArgumentParser(description="CARGOLO ASR TMS document-upload activity monitor")
    parser.add_argument("--max-events", type=int, default=5)
    parser.add_argument("--per-page", type=int, default=50)
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-notify", action="store_true")
    parser.add_argument("--no-history", action="store_true")
    parser.add_argument("--skip-analysis", action="store_true")
    args = parser.parse_args()
    result = run_document_activity_monitor(
        max_events=args.max_events,
        per_page=args.per_page,
        date_from=args.date_from,
        date_to=args.date_to,
        force=args.force,
        dry_run=args.dry_run,
        notify_ops_webhook=not args.no_notify,
        refresh_history=not args.no_history,
        analyze_documents=not args.skip_analysis,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
