from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
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


def _load_json(path_value: Any) -> dict[str, Any]:
    try:
        path = Path(str(path_value or ""))
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _select_uploaded_analysis(report: dict[str, Any], filename: str) -> dict[str, Any]:
    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
    registry = _load_json(lifecycle.get("document_registry_path"))
    uploaded_norm = str(filename or "").strip().lower()
    for row in registry.get("analyzed_documents") or []:
        if isinstance(row, dict) and str(row.get("filename") or "").strip().lower() == uploaded_norm:
            analysis = _load_json(row.get("analysis_path"))
            return {**row, "analysis": analysis}
    return {}


def _clean(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return str(value).strip()


def _date_from_ms(value: Any) -> str:
    try:
        number = int(value or 0)
    except Exception:
        return ""
    if number <= 0:
        return ""
    return datetime.fromtimestamp(number / 1000, tz=timezone.utc).date().isoformat()


def _display_timestamp(value: Any) -> str:
    if value in (None, ""):
        return ""
    raw = str(value).strip()
    if raw.isdigit():
        try:
            number = int(raw)
            if number > 10_000_000_000:
                return datetime.fromtimestamp(number / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            if number > 0:
                return datetime.fromtimestamp(number, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            return raw
    return raw


def _norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _same_location(tms_value: Any, doc_value: Any) -> bool:
    tms = _norm(tms_value)
    doc = _norm(doc_value)
    if not tms or not doc:
        return False
    aliases = {
        "cnngb": "ningbo",
        "deham": "hamburg",
    }
    tms_alias = aliases.get(tms, tms)
    return tms_alias in doc or doc in tms_alias or tms in doc


def _contains_reference(haystack: dict[str, Any], value: Any) -> bool:
    needle = _norm(value)
    if not needle:
        return False
    blob = json.dumps(haystack, ensure_ascii=False)
    return needle in _norm(blob)


def _build_document_field_comparison(report: dict[str, Any], filename: str) -> list[dict[str, str]]:
    uploaded = _select_uploaded_analysis(report, filename)
    analysis = uploaded.get("analysis") if isinstance(uploaded.get("analysis"), dict) else {}
    fields = analysis.get("extracted_fields") if isinstance(analysis.get("extracted_fields"), dict) else {}
    detail = report.get("tms_snapshot", {}).get("detail") if isinstance(report.get("tms_snapshot"), dict) else {}
    if not detail:
        lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
        detail = _load_json(lifecycle.get("tms_snapshot_path")).get("detail") or {}
    if not isinstance(detail, dict):
        detail = {}
    freight = detail.get("freight_details") if isinstance(detail.get("freight_details"), dict) else {}
    dates = detail.get("dates") if isinstance(detail.get("dates"), dict) else {}
    totals = detail.get("totals") if isinstance(detail.get("totals"), dict) else {}
    legs = detail.get("transport_legs") if isinstance(detail.get("transport_legs"), list) else []
    main_leg = next((leg for leg in legs if isinstance(leg, dict) and str(leg.get("leg_type") or "") == "main_carriage"), {})

    refs = analysis.get("references") if isinstance(analysis.get("references"), list) else []
    doc_number = _clean(fields.get("document_number"))
    container_doc = next((str(ref).strip() for ref in refs if re.fullmatch(r"[A-Z]{4}\d{7}", str(ref).strip())), "")
    vessel_doc = "EVER GREET" if _contains_reference(analysis, "EVER GREET") else ""

    comparisons: list[dict[str, str]] = []

    def add(label: str, tms: Any, doc: Any, *, match: bool | None = None, target: str = "") -> None:
        tms_s = _clean(tms)
        doc_s = _clean(doc)
        if not tms_s and not doc_s:
            return
        if match is None:
            if tms_s and doc_s:
                status = "match" if _norm(tms_s) == _norm(doc_s) else "diff"
            elif doc_s and not tms_s:
                status = "missing_tms"
            elif tms_s and not doc_s:
                status = "missing_doc"
            else:
                return
        else:
            if match:
                status = "match"
            elif doc_s and not tms_s:
                status = "missing_tms"
            elif tms_s and not doc_s:
                status = "missing_doc"
            else:
                status = "diff"
        comparisons.append({"label": label, "tms": tms_s or "nicht gepflegt", "doc": doc_s or "nicht lesbar", "status": status, "target": target})

    add("POL", freight.get("pol_code") or main_leg.get("origin"), fields.get("pol"), match=_same_location(freight.get("pol_code") or main_leg.get("origin"), fields.get("pol")))
    add("POD", freight.get("pod_code") or main_leg.get("destination"), fields.get("pod"), match=_same_location(freight.get("pod_code") or main_leg.get("destination"), fields.get("pod")))
    add("ETD", _date_from_ms(main_leg.get("etd")) or _date_from_ms((detail.get("milestones") or {}).get("etd_main_carriage")), fields.get("etd"), target="Hauptlauf-ETD")
    add("ETA", dates.get("estimated_delivery_date") or _date_from_ms((detail.get("milestones") or {}).get("eta_main_carriage")), fields.get("eta"), target="ETA")
    add("MBL / B/L-Nr.", freight.get("mbl_number") or freight.get("bl_number"), doc_number, target="mbl_number")
    add("Container", freight.get("container_number"), container_doc, target="container_number")
    if vessel_doc or main_leg.get("carrier") or main_leg.get("vessel_name"):
        add("Schiff", main_leg.get("vessel_name") or main_leg.get("carrier"), vessel_doc, target="Vessel/Hauptlauf")
    weight_doc = _clean(fields.get("weight_kg") or fields.get("total_weight_kg") or fields.get("gross_weight_kg"))
    if weight_doc:
        add("Gewicht", totals.get("total_weight_kg"), weight_doc, target="Gewicht")
    return comparisons


def _comparison_lines(comparisons: list[dict[str, str]], statuses: set[str], limit: int = 3) -> list[str]:
    labels = {"match": "passt", "diff": "abweicht", "missing_tms": "fehlt im TMS", "missing_doc": "nicht auf dem Dokument"}
    lines = []
    for row in comparisons:
        if row.get("status") not in statuses:
            continue
        status = row.get("status", "")
        label = row.get("label") or "Feld"
        tms = row.get("tms") or "nicht gepflegt"
        doc = row.get("doc") or "nicht lesbar"
        if status == "match":
            lines.append(f"{label} passt: TMS {tms} = Dokument {doc}")
        elif status == "missing_tms":
            target = f" ({row.get('target')})" if row.get("target") else ""
            lines.append(f"{label} fehlt im TMS{target}: Dokument {doc}")
        elif status == "missing_doc":
            lines.append(f"{label} nicht beurteilbar: TMS {tms}, im Dokument nicht lesbar/angegeben")
        else:
            lines.append(f"{label} weicht ab: TMS {tms}, Dokument {doc}")
        if len(lines) >= limit:
            break
    return lines


def _human_document_message(
    *,
    order_id: Any,
    filename: Any,
    doc_type: Any,
    context: dict[str, Any],
    findings: list[Any],
    needs_review: bool,
    comparisons: list[dict[str, str]] | None = None,
    uploaded_by: Any = None,
    uploaded_at: Any = None,
) -> str:
    label = _doc_type_label(doc_type)
    route = _route_hint(context)
    status = str(context.get("status") or "").strip()
    network = str(context.get("network") or context.get("mode") or "").strip()
    context_bits = [bit for bit in (network, status, route) if bit]
    lage = f"{label} '{filename}' wurde gegen die im TMS gepflegten Sendungsdaten geprüft."
    uploader = str(uploaded_by or "").strip()
    upload_time = _display_timestamp(uploaded_at)
    if uploader and uploader != "-":
        lage += f" Upload laut TMS-Activity-Log: {uploader}"
        if upload_time and upload_time != "-":
            lage += f" am {upload_time}"
        lage += "."
    if context_bits:
        lage += " Kontext: " + " · ".join(context_bits) + "."

    comparisons = comparisons or []
    matching = _comparison_lines(comparisons, {"match"}, limit=3)
    problems = _comparison_lines(comparisons, {"diff", "missing_tms"}, limit=3)
    unknown = _comparison_lines(comparisons, {"missing_doc"}, limit=2)

    if matching:
        abgleich = " | ".join(matching)
    else:
        abgleich = "Für dieses Dokument konnten noch keine belastbaren TMS-Feldtreffer bestätigt werden."

    uploaded_norm = str(filename or "").strip().lower()
    blocker_findings = [
        row for row in findings
        if isinstance(row, dict)
        and (str(row.get("filename") or "").strip().lower() == uploaded_norm or _finding_rank(row)[0] <= 0)
        and _finding_rank(row)[0] <= 0
    ]
    blocker_lines = [_finding_text(row) for row in sorted(blocker_findings, key=_finding_rank)[:2]]
    if not comparisons:
        uploaded_findings = [
            row for row in findings
            if isinstance(row, dict) and str(row.get("filename") or "").strip().lower() == uploaded_norm
        ]
        blocker_lines.extend(_finding_text(row) for row in sorted(uploaded_findings, key=_finding_rank)[:3])
    auffaellig_items = problems + blocker_lines
    if auffaellig_items:
        auffaellig = " | ".join(auffaellig_items[:3])
        empfehlung = "Die abweichenden bzw. im TMS fehlenden Werte bitte fachlich bestätigen; erst danach TMS-Felder korrigieren oder Dokument als Vorversion markieren."
        targets = [row.get("target") or row.get("label") for row in comparisons if row.get("status") in {"diff", "missing_tms"}]
        target_text = ", ".join(str(x) for x in targets[:3] if x)
        naechster_schritt = f"Zu klären/korrigieren: {target_text}." if target_text else "Führenden Wert festlegen; bei TMS-Korrektur anschließend bewusst freigeben."
    else:
        suffix = (" Nicht beurteilbar: " + " | ".join(unknown)) if unknown else ""
        auffaellig = "Keine TMS-/Dokument-Abweichung aus den lesbaren Feldern erkannt." + suffix
        empfehlung = "Keine TMS-Korrektur aus diesem Dokument ableiten; nur die nicht lesbaren/nicht angegebenen Felder bei Bedarf manuell nachsehen."
        naechster_schritt = "Dokumentstand weiter beobachten; erst bei echtem Feldkonflikt oder fehlendem TMS-Wert freigeben."
    if needs_review and not comparisons and not blocker_lines:
        auffaellig = "Dokument ist nicht vollständig automatisch belastbar; manueller Feldabgleich sinnvoll."

    return "\n".join([
        f"Lage: {order_id} · {lage}",
        f"Abgleich: {abgleich}",
        f"Auffällig: {auffaellig}",
        f"Empfehlung: {empfehlung}",
        f"Nächster Schritt: {naechster_schritt}",
    ])


def _case_root_from_report(report: dict[str, Any], order_id: str) -> Path | None:
    del order_id
    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
    for value in (lifecycle.get("case_root"), report.get("case_root")):
        if value:
            return Path(str(value))
    return None


def _queue_tms_review_cards_from_comparisons(
    *,
    report: dict[str, Any],
    event: dict[str, Any],
    comparisons: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Persist Teams approval items for document-derived TMS field updates.

    The document monitor may propose concrete TMS updates, but never writes them
    directly. For fields supported by the existing Teams review/writeback flow,
    queue one pending item and let the Teams adapter render a Yes/No review card.
    Unsupported operational fields, such as main-leg ETD, stay as textual review
    findings until a dedicated writeback tool exists for them.
    """
    order_id = str(report.get("order_id") or "").strip().upper()
    if not order_id:
        return []
    try:
        from .teams_reply_loop import record_agent_tms_update_intent
    except Exception:
        return []

    case_root = _case_root_from_report(report, order_id)
    if not case_root:
        return []
    root = case_root.parent.parent if case_root.name == order_id and case_root.parent.name == "orders" else case_root
    operator = "Hermes Document Monitor"
    activity_id = _activity_id(event)
    context_id = f"{order_id}:{activity_id}:document_monitor" if activity_id else f"{order_id}:document_monitor"
    supported_targets = {"mbl_number", "container_number", "hbl_number", "hawb_number", "customs_reference"}
    cards: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in comparisons:
        if row.get("status") not in {"diff", "missing_tms"}:
            continue
        target = str(row.get("target") or "").strip()
        value = str(row.get("doc") or "").strip()
        if target not in supported_targets or not value or value == "nicht lesbar":
            continue
        key = (target, value)
        if key in seen:
            continue
        seen.add(key)
        queued = record_agent_tms_update_intent(
            root=root,
            order_id=order_id,
            target=target,
            value=value,
            text=f"Dokumentenabgleich: {row.get('label') or target} aus Dokument übernehmen? TMS {row.get('tms')} ↔ Dokument {value}.",
            operator=operator,
            source_message_id=str(activity_id or "") or None,
            context_id=context_id,
            confidence="document_field_comparison",
        )
        if queued.get("queued"):
            cards.append({
                "order_id": order_id,
                "action_id": queued.get("action_id"),
                "target": target,
                "value": value,
                "operator": operator,
                "context_id": context_id,
                "source": "document_activity_monitor",
                "question": f"{row.get('label') or target}: TMS auf Dokumentwert setzen?",
            })
    return cards


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
    comparisons = _build_document_field_comparison(report, str(filename))
    if any(row.get("status") in {"diff", "missing_tms"} for row in comparisons):
        needs_review = True
        if priority == "low":
            priority = "medium"
    tms_review_cards = _queue_tms_review_cards_from_comparisons(report=report, event=event, comparisons=comparisons)
    message = _human_document_message(
        order_id=report.get("order_id"),
        filename=filename,
        doc_type=doc_type,
        context=context,
        findings=findings,
        needs_review=needs_review,
        comparisons=comparisons,
        uploaded_by=event.get("changed_by_name") or event.get("changed_by"),
        uploaded_at=event.get("changed_at"),
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
        "document_field_comparison": comparisons,
        "teams_tms_review_cards": tms_review_cards,
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
