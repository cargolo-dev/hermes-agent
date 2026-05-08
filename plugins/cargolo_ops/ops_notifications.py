from __future__ import annotations

import html as _html
import hashlib
import hmac
import json
import logging
import os
import re
import time
from html import escape as _html_escape
from pathlib import Path
from typing import Any

import requests

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)

_DEFAULT_ROUTE_NAME = "cargolo-asr-ingest"
_DEFAULT_NATIVE_TEAMS_ROUTE_NAME = "cargolo-asr-ops-teams"
_DEFAULT_GATEWAY_WEBHOOK_BASE = "http://127.0.0.1:8644"


def _load_targets_from_route_config(route_name: str) -> list[dict[str, Any]]:
    subscriptions_path = get_hermes_home() / "webhook_subscriptions.json"
    if not subscriptions_path.exists():
        return []
    try:
        data = json.loads(subscriptions_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Could not read webhook subscriptions for ASR ops notifications: %s", exc)
        return []
    route = data.get(route_name)
    if not isinstance(route, dict):
        return []
    targets: list[dict[str, Any]] = []
    for item in route.get("deliver_additional") or []:
        if not isinstance(item, dict) or item.get("deliver") != "webhook_forward":
            continue
        extra = item.get("deliver_extra") or {}
        url = str(extra.get("url") or "").strip()
        if not url:
            continue
        targets.append(
            {
                "url": url,
                "method": str(extra.get("method") or "POST").strip().upper() or "POST",
                "headers": dict(extra.get("headers") or {}),
                "source": f"route:{route_name}",
            }
        )
    return targets


def _load_gateway_route_config(route_name: str) -> dict[str, Any]:
    subscriptions_path = get_hermes_home() / "webhook_subscriptions.json"
    if not subscriptions_path.exists():
        return {}
    try:
        data = json.loads(subscriptions_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Could not read webhook subscriptions for native Teams ops notifications: %s", exc)
        return {}
    route = data.get(route_name)
    return route if isinstance(route, dict) else {}


def _load_native_teams_targets() -> list[dict[str, Any]]:
    """Return the internal gateway deliver_only route used for native Teams.

    CARGOLO ops notifications no longer post to n8n by default. Instead they
    post to a local, HMAC-protected Hermes webhook route whose delivery target
    is ``teams``; the running gateway then sends via the native Teams adapter
    and its configured Teams home channel.
    """
    route_name = str(os.getenv("HERMES_CARGOLO_ASR_OPS_TEAMS_ROUTE", _DEFAULT_NATIVE_TEAMS_ROUTE_NAME)).strip()
    route = _load_gateway_route_config(route_name)
    if not route:
        return []
    base = str(os.getenv("HERMES_CARGOLO_ASR_OPS_GATEWAY_BASE_URL", _DEFAULT_GATEWAY_WEBHOOK_BASE)).strip().rstrip("/")
    url = str(os.getenv("HERMES_CARGOLO_ASR_OPS_GATEWAY_URL", f"{base}/webhooks/{route_name}")).strip()
    secret = str(route.get("secret") or "").strip()
    if not url or not secret:
        return []
    return [
        {
            "url": url,
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "secret": secret,
            "source": f"native_teams_route:{route_name}",
            "kind": "native_teams_gateway",
        }
    ]


def _load_webhook_targets(*, route_name: str, allow_route_fallback: bool) -> list[dict[str, Any]]:
    url = str(os.getenv("HERMES_CARGOLO_ASR_OPS_WEBHOOK_URL", "")).strip()
    if url:
        headers = {"Content-Type": "application/json"}
        auth_token = str(os.getenv("HERMES_CARGOLO_ASR_OPS_WEBHOOK_TOKEN", "")).strip()
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        return [
            {
                "url": url,
                "method": str(os.getenv("HERMES_CARGOLO_ASR_OPS_WEBHOOK_METHOD", "POST") or "POST").strip().upper() or "POST",
                "headers": headers,
                "source": "env:HERMES_CARGOLO_ASR_OPS_WEBHOOK_URL",
                "kind": "webhook",
            }
        ]
    if allow_route_fallback:
        return _load_targets_from_route_config(route_name)
    return []


def _load_targets(*, route_name: str, allow_route_fallback: bool) -> list[dict[str, Any]]:
    delivery_mode = str(os.getenv("HERMES_CARGOLO_ASR_OPS_DELIVERY", "native_teams")).strip().lower()
    if delivery_mode in {"", "native", "native_teams", "teams"}:
        return _load_native_teams_targets()
    if delivery_mode in {"both", "native_teams_and_webhook", "teams_and_webhook"}:
        return _load_native_teams_targets() + _load_webhook_targets(
            route_name=route_name,
            allow_route_fallback=allow_route_fallback,
        )
    return _load_webhook_targets(route_name=route_name, allow_route_fallback=allow_route_fallback)


def _priority_emoji(priority: Any) -> str:
    raw = str(priority or "").strip().lower()
    return {
        "low": "🟢",
        "medium": "🟡",
        "high": "🟠",
        "urgent": "🔴",
    }.get(raw, "⚪")


def _priority_label_de(priority: Any) -> str:
    raw = str(priority or "").strip().lower()
    return {
        "low": "niedrig",
        "medium": "mittel",
        "high": "hoch",
        "urgent": "urgent",
    }.get(raw, "")


def _priority_label(value: Any) -> str:
    label = _priority_label_de(value)
    return label or "mittel"


def _join_list(value: Any, *, empty: str, limit: int = 4) -> str:
    if isinstance(value, str):
        return value.strip() or empty
    if not isinstance(value, list):
        return empty
    items = [str(item).strip() for item in value if str(item).strip()]
    if not items:
        return empty
    shown = items[:limit]
    if len(items) > limit:
        shown.append(f"+{len(items) - limit} weitere")
    return ", ".join(shown)


def _format_summary_map(summary: Any) -> str:
    if not isinstance(summary, dict):
        return "Keine"
    ordered = []
    for key, label in (
        ("write_now", "sofort umsetzbar"),
        ("review", "Review"),
        ("not_yet_due", "noch nicht faellig"),
        ("not_yet_knowable", "noch nicht belastbar"),
        ("applied", "umgesetzt"),
        ("failed", "fehlgeschlagen"),
        ("skipped", "uebersprungen"),
    ):
        value = summary.get(key)
        if isinstance(value, int) and value > 0:
            ordered.append(f"{label}={value}")
    return ", ".join(ordered) if ordered else "Keine"


def _truncate(text: Any, limit: int = 220) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def _truncate_sentence(text: Any, limit: int = 320) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    cut = value[:limit].rsplit(" ", 1)[0].rstrip(" ,;:.-")
    return cut + " …"


def _first_present(*values: Any, empty: str = "-") -> str:
    for value in values:
        if value not in (None, "", [], {}):
            text = str(value).strip()
            if text:
                return text
    return empty


def _load_json_file(path_value: Any) -> dict[str, Any]:
    path = str(path_value or "").strip()
    if not path:
        return {}
    try:
        candidate = Path(path)
        if not candidate.exists() or not candidate.is_file():
            return {}
        data = json.loads(candidate.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.debug("Could not load JSON artifact from %s", path, exc_info=True)
        return {}


def _doc_type_label(value: Any) -> str:
    raw = str(value or "unknown").strip().lower()
    return {
        "commercial_invoice": "Handelsrechnung",
        "packing_list": "Packliste",
        "air_waybill": "AWB/HAWB",
        "bill_of_lading": "B/L",
        "proof_of_delivery": "POD",
        "mrn": "MRN/Zollreferenz",
        "customs_document": "Zolldokument",
        "billing": "Abrechnungsbeleg",
        "offer": "Angebot",
        "unknown": "unbekannt",
    }.get(raw, raw.replace("_", " ") or "unbekannt")


def _format_route_from_context(context: dict[str, Any]) -> str:
    origin = " ".join(str(x).strip() for x in [context.get("origin_city"), context.get("origin_country")] if str(x or "").strip())
    dest = " ".join(str(x).strip() for x in [context.get("destination_city"), context.get("destination_country")] if str(x or "").strip())
    if origin and dest:
        return f"{origin} → {dest}"
    return origin or dest or "-"


def _format_cargo_from_context(context: dict[str, Any]) -> str:
    parts = []
    if context.get("pieces") not in (None, ""):
        parts.append(f"{context.get('pieces')} Packst.")
    if context.get("weight_kg") not in (None, ""):
        parts.append(f"{context.get('weight_kg')} kg")
    if context.get("volume_m3") not in (None, ""):
        parts.append(f"{context.get('volume_m3')} cbm")
    if context.get("cargo_description"):
        parts.append(str(context.get("cargo_description")))
    return ", ".join(parts) if parts else "-"


def _load_document_analysis(path_value: Any) -> dict[str, Any]:
    return _load_json_file(path_value)


def _select_uploaded_document(registry: dict[str, Any], filename: str) -> dict[str, Any]:
    documents = registry.get("analyzed_documents") if isinstance(registry.get("analyzed_documents"), list) else []
    filename_norm = str(filename or "").strip().lower()
    if filename_norm:
        for row in documents:
            if isinstance(row, dict) and str(row.get("filename") or "").strip().lower() == filename_norm:
                return row
        for row in documents:
            if isinstance(row, dict) and filename_norm in str(row.get("filename") or "").strip().lower():
                return row
    return next((row for row in documents if isinstance(row, dict)), {})


def _finding_summary_de(finding: dict[str, Any]) -> str:
    raw = str(finding.get("summary") or finding.get("type") or "").strip()
    mapping = {
        "Incoterm_EXW_detected": "Incoterm EXW erkannt; Zuständigkeiten/Kosten gegen Angebot und TMS prüfen.",
        "Incoterm EXW": "Incoterm EXW erkannt; Zuständigkeiten/Kosten gegen Angebot und TMS prüfen.",
        "Incoterm EXW bestätigt": "Incoterm EXW bestätigt; Zuständigkeiten/Kosten gegen Angebot und TMS prüfen.",
        "Incoterm EXW bestätigt.": "Incoterm EXW bestätigt; Zuständigkeiten/Kosten gegen Angebot und TMS prüfen.",
        "Discrepancy_Weight_Detected": "Abweichung Gewicht zwischen Dokument/Angebot und TMS prüfen.",
        "weight_discrepancy": "Gewichtsabweichung zwischen HAWB/Dokument und TMS prüfen.",
        "Discrepancy_Packages_Detected": "Abweichung Packstückanzahl zwischen Dokument/Angebot und TMS prüfen.",
        "DOCUMENT_ANALYSIS_ERROR": "Dokument konnte nicht sicher analysiert werden; Datei manuell öffnen.",
    }
    if raw in mapping:
        return mapping[raw]
    cleaned = raw.replace("_", " ").strip()
    if cleaned:
        return cleaned[:1].upper() + cleaned[1:]
    return "Dokument/TMS-Abgleich fachlich prüfen."


def _finding_text(finding: Any) -> str:
    if isinstance(finding, dict):
        filename = str(finding.get("filename") or "").strip()
        summary = _finding_summary_de(finding)
        severity = str(finding.get("severity") or "").strip()
        prefix = f"{filename}: " if filename else ""
        suffix = f" ({severity})" if severity else ""
        return f"{prefix}{summary}{suffix}"
    return str(finding or "").strip()


def _is_weight_or_quantity_finding(finding: Any) -> bool:
    if not isinstance(finding, dict):
        text = str(finding or "").lower()
    else:
        text = " ".join(
            str(finding.get(key) or "")
            for key in ("type", "summary", "filename", "reason", "evidence")
        ).lower()
    return any(token in text for token in ("weight", "gewicht", "menge", "quantity", "packstück", "pieces", "packages"))


def _pick_document_next_step(findings: list[Any]) -> str:
    if not findings:
        return "Dokument/TMS-Abgleich fachlich prüfen."
    if any(_is_weight_or_quantity_finding(row) for row in findings):
        return "Abweichung Gewicht/Menge zwischen Angebot, Dokumenten und TMS-Auszug prüfen."
    preferred_tokens = (
        "Incoterm_EXW_detected",
        "Incoterm EXW",
    )
    dict_findings = [row for row in findings if isinstance(row, dict)]
    for token in preferred_tokens:
        for row in dict_findings:
            if str(row.get("summary") or "") == token:
                return _finding_summary_de(row)
    return _finding_summary_de(dict_findings[0]) if dict_findings else "Dokument/TMS-Abgleich fachlich prüfen."


def _read_email_index_count(case_root: Any) -> int | None:
    try:
        root = Path(str(case_root or ""))
        index = root / "email_index.jsonl"
        if not index.exists():
            return None
        return sum(1 for line in index.read_text(encoding="utf-8").splitlines() if line.strip())
    except Exception:
        return None


def _display_doc_type_for_row(row: dict[str, Any]) -> str:
    filename = str(row.get("filename") or "").lower()
    summary = str(row.get("summary") or row.get("analysis_summary") or "").lower()
    if "angebot" in filename or "transportangebot" in summary:
        return "Angebot"
    return _doc_type_label(row.get("doc_type") or row.get("analysis_doc_type"))


def _deviation_notes_from_doc(row: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    analysis = _load_json_file(row.get("analysis_path")) if row.get("analysis_path") else {}
    candidates: list[Any] = []
    for source in (analysis, row):
        if not isinstance(source, dict):
            continue
        candidates.extend(source.get("consistency_notes") or [])
        candidates.extend(source.get("operational_flags") or [])
    for item in candidates:
        text = str(item or "").strip()
        if not text:
            continue
        lower = text.lower()
        is_concrete = any(token in lower for token in ("abweich", " vs ", "differ", "discrep")) or ("tms" in lower and any(token in lower for token in ("kg", "gewicht", "packstück", "menge", "quantity", "pieces")))
        is_generic = lower in {"weight_discrepancy", "freight_collect"}
        if is_concrete and not is_generic and text not in notes:
            notes.append(text)
    return notes[:3]


def _document_line_from_analysis(row: dict[str, Any]) -> str:
    filename = _truncate(str(row.get("filename") or "Dokument"), 72)
    doc_type = _display_doc_type_for_row(row)
    summary = _truncate_sentence(row.get("summary") or row.get("analysis_summary") or "kein Kurzinhalt extrahiert", 150)
    deviation_notes = _deviation_notes_from_doc(row)
    flags = row.get("operational_flags") if isinstance(row.get("operational_flags"), list) else []
    visible_flags = [str(x) for x in flags if str(x).strip().lower() not in {"weight_discrepancy", "freight_collect"}]
    hint_items = deviation_notes[:2] or visible_flags[:2]
    flag_text = ""
    if hint_items:
        flag_text = " | Hinweis: " + _truncate_sentence("; ".join(str(x) for x in hint_items if x), 170)
    return f"- {filename}: {doc_type}; {summary}{flag_text}"


def _build_document_activity_text(payload: dict[str, Any], report: dict[str, Any]) -> str:
    """Return the compact Teams-facing document-upload card.

    The native Teams webhook route currently delivers ``message_text``.  For
    Teams this is intentionally HTML, not a long plaintext audit block: the
    adapter passes known HTML through unchanged, giving us stable line breaks,
    emphasis, and compact visual grouping without requiring a separate Adaptive
    Card delivery path for this operational alert.
    """
    model = _build_document_activity_model(payload, report)
    context = model.get("context") if isinstance(model.get("context"), dict) else {}
    route = _format_route_from_context(context)
    history_delta = model.get("history_sync_count") or 0
    total_mails = _read_email_index_count(report.get("case_root"))
    history_bits = [f"Mail +{history_delta}"]
    if total_mails is not None:
        history_bits.append(f"gesamt {total_mails}")
    if model.get("last_email_at"):
        history_bits.append(f"Stand {model.get('last_email_at')}")
    history_status = " / ".join(history_bits)

    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
    registry = _load_json_file(lifecycle.get("document_registry_path"))
    latest_summary = {}
    try:
        case_root = Path(str(report.get("case_root") or ""))
        candidate = case_root / "documents" / "analysis" / "latest_summary.json"
        if candidate.exists():
            latest_summary = _load_json_file(candidate)
    except Exception:
        latest_summary = {}
    analyzed_docs = latest_summary.get("documents") if isinstance(latest_summary.get("documents"), list) else []
    if not analyzed_docs and isinstance(registry.get("analyzed_documents"), list):
        analyzed_docs = registry.get("analyzed_documents") or []

    uploaded_name = str(model.get("filename") or "")
    selected_docs: list[dict[str, Any]] = []
    for row in analyzed_docs:
        if isinstance(row, dict) and uploaded_name and str(row.get("filename") or "").strip().lower() == uploaded_name.strip().lower():
            selected_docs.append(row)
            break
    for row in analyzed_docs:
        if isinstance(row, dict) and row not in selected_docs and _deviation_notes_from_doc(row):
            selected_docs.append(row)
        if len(selected_docs) >= 3:
            break
    for row in analyzed_docs:
        if isinstance(row, dict) and row not in selected_docs:
            selected_docs.append(row)
        if len(selected_docs) >= 3:
            break

    findings = model.get("findings") if isinstance(model.get("findings"), list) else []

    def _finding_rank(row: Any) -> tuple[int, str]:
        text = _finding_text(row).lower()
        if "weight" in text or "gewicht" in text:
            return (0, text)
        if any(token in text for token in ("menge", "quantity", "packstück", "pieces", "packages")):
            return (1, text)
        return (2, text)

    major_findings = [row for row in findings if _is_weight_or_quantity_finding(row)]
    if not major_findings:
        major_findings = [row for row in findings if isinstance(row, dict) and str(row.get("severity") or "").lower() in {"medium", "high", "critical"}]
    major_findings = sorted(major_findings, key=_finding_rank)[:4]

    concrete_deviations: list[str] = []
    for row in selected_docs:
        if isinstance(row, dict):
            filename = _truncate(str(row.get("filename") or "Dokument"), 38)
            for note in _deviation_notes_from_doc(row):
                line = f"{filename}: {note}"
                if line not in concrete_deviations:
                    concrete_deviations.append(line)
    if not concrete_deviations:
        for row in major_findings:
            text = _finding_text(row)
            lower = text.lower()
            if any(token in lower for token in ("abweich", " vs ", "kg", "gewicht", "discrep")) and text not in concrete_deviations:
                concrete_deviations.append(text)

    def _compact_deviation_item(item: str) -> str:
        text = str(item or "").strip()
        filename, _, note = text.partition(": ")
        prefix = filename if note else ""
        body = note or text
        weights = re.findall(r"\d+(?:[,.]\d+)?\s*kg", body, flags=re.IGNORECASE)
        packages = re.findall(r"\d+\s*(?:Cartons?|Packages?|Packstücke|Packstuecke|Paletten)", body, flags=re.IGNORECASE)
        if len(weights) >= 2:
            compact = f"{weights[0]} vs {weights[-1]}"
        elif len(packages) >= 2:
            compact = f"{packages[0]} vs {packages[-1]}"
        else:
            compact = _truncate_sentence(body, 105)
        return f"{prefix}: {compact}" if prefix else compact

    compact_details: list[str] = []
    seen_compact: set[str] = set()
    for item in concrete_deviations or [_finding_text(x) for x in major_findings]:
        compact = _compact_deviation_item(item)
        compact_key = compact.lower()
        value_key = compact.split(": ", 1)[-1].lower()
        if compact and compact_key not in seen_compact and value_key not in seen_compact:
            compact_details.append(compact)
            seen_compact.add(compact_key)
            seen_compact.add(value_key)
        if len(compact_details) >= 3:
            break

    risk = str(model.get("risk") or "low").strip().lower() or "low"
    status_is_issue = bool(compact_details or major_findings or risk in {"medium", "high", "critical"})
    tone = "danger" if risk in {"high", "critical"} else "warn" if status_is_issue else "good"
    border = {"danger": "#f87171", "warn": "#fbbf24", "good": "#34d399"}.get(tone, "#60a5fa")
    bg = {"danger": "#3f1d1d", "warn": "#3a2b0a", "good": "#123524"}.get(tone, "#172554")
    issue_title = "Auffälligkeit" if status_is_issue else "Keine Auffälligkeit"
    issue_text = " · ".join(compact_details) if compact_details else "Keine fachlichen Dokumenten-Widersprüche erkannt."
    question = "Ist der TMS-Wert korrekt oder sind die Dokumente Teil-/Vorversionen?"
    if any("pack" in x.lower() or "carton" in x.lower() or "palette" in x.lower() for x in compact_details):
        question = "Welche Werte sollen führend sein: TMS-Stammdaten oder Dokumentwerte?"
    suggestion = "Wenn TMS korrekt ist, markiere ich die Dokumentwerte als Vorversion/Schreibweise; sonst bereite ich die TMS-Korrektur vor."
    doc_type = _doc_type_label(model.get("analysis_doc_type") or model.get("event_doc_type"))

    def _context_value(*keys: str) -> str:
        for key in keys:
            if context.get(key) not in (None, "", [], {}):
                return str(context.get(key))
        return "-"

    def _short_doc(row: dict[str, Any]) -> str:
        filename = _truncate(str(row.get("filename") or "Dokument"), 34)
        analysis = _load_json_file(row.get("analysis_path")) if row.get("analysis_path") else {}
        fields = analysis.get("extracted_fields") if isinstance(analysis.get("extracted_fields"), dict) else {}
        label = _display_doc_type_for_row(row)
        pieces = [filename, label]
        amount = _first_present(fields.get("amount"), fields.get("total_amount"), empty="")
        currency = _first_present(fields.get("currency"), empty="")
        if amount:
            pieces.append(f"{amount} {currency}".strip())
        doc_number = _first_present(fields.get("document_number"), fields.get("invoice_number"), fields.get("awb"), fields.get("hawb"), empty="")
        if doc_number:
            pieces.append(f"Nr. {doc_number}")
        notes = _deviation_notes_from_doc(row)
        if notes:
            pieces.append("Hinweis: " + _compact_deviation_item(notes[0]))
        return " · ".join(_html_escape(str(part)) for part in pieces if part)

    doc_lines = [_short_doc(row) for row in selected_docs[:3] if isinstance(row, dict)]
    if not doc_lines:
        doc_lines = ["Keine analysierten Dokumentdetails gefunden."]
    if len(analyzed_docs) > len(selected_docs):
        doc_lines.append(f"+{len(analyzed_docs) - len(selected_docs)} weitere Dokumente nur lokal im Order-Artefakt")

    tms_line = (
        f"{_context_value('network', 'mode')} · {_context_value('status')} · {route}"
        f" · Incoterms {_context_value('incoterms')}"
    )
    cargo_bits = []
    if _context_value('cargo_description') != "-":
        cargo_bits.append(_truncate(str(_context_value('cargo_description')), 80))
    if _context_value('weight_kg') != "-":
        cargo_bits.append(f"TMS-Gewicht {_context_value('weight_kg')} kg")
    if _context_value('pieces') != "-":
        cargo_bits.append(f"Packstücke {_context_value('pieces')}")
    cargo_line = " · ".join(cargo_bits) if cargo_bits else "TMS-Cargo: keine Gewichts-/Packstückwerte im Snapshot"

    footer = (
        f"{model.get('received_documents')} lokale Dok. / {model.get('mirrored_tms_documents')} TMS-Dok. · "
        f"{history_status} · Findings {len(findings)}"
    )
    uploaded = _truncate(uploaded_name, 58)

    return (
        f"<div style='border:1px solid #475569;border-left:6px solid {border};border-radius:14px;"
        "padding:14px 16px;background:#111827;color:#f8fafc;font-family:Segoe UI,Arial,sans-serif;line-height:1.35;'>"
        f"<div style='font-size:18px;font-weight:800;margin-bottom:6px;color:#ffffff;'>📄 {_html_escape(str(model.get('order_id')))} · Dokumenten-Upload geprüft</div>"
        f"<div style='font-size:13px;margin-bottom:10px;color:#cbd5e1;'>Upload: <b>{_html_escape(uploaded)}</b> · Typ: <b>{_html_escape(doc_type)}</b> · Risiko: <b>{_html_escape(risk)}</b></div>"
        f"<div style='background:{bg};border:1px solid {border};border-radius:10px;padding:10px 12px;margin-bottom:10px;color:#f8fafc;'>"
        f"<b>⚠️ {issue_title}:</b> {_html_escape(_truncate_sentence(issue_text, 230))}</div>"
        f"<div style='margin-bottom:8px;color:#f8fafc;'><b>TMS:</b> {_html_escape(_truncate(tms_line, 180))}<br>"
        f"<span style='color:#cbd5e1;'>{_html_escape(cargo_line)}</span></div>"
        "<div style='margin-bottom:8px;color:#f8fafc;'><b>Dokumente geprüft:</b><br>"
        + "<br>".join(f"• {line}" for line in doc_lines[:4])
        + "</div>"
        f"<div style='background:#1f2937;border:1px solid #374151;border-radius:10px;padding:10px 12px;margin-bottom:8px;color:#f8fafc;'>"
        f"<b>Frage:</b> {_html_escape(question)}<br>"
        f"<b>Vorschlag:</b> {_html_escape(suggestion)}</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>{_html_escape(footer)}</div>"
        "</div>"
    )

def _build_document_activity_model(payload: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    result = payload.get("processor_result") if isinstance(payload.get("processor_result"), dict) else {}
    event = payload.get("activity_event") if isinstance(payload.get("activity_event"), dict) else {}
    trigger = report.get("trigger_event") if isinstance(report.get("trigger_event"), dict) else {}
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else trigger.get("metadata") if isinstance(trigger.get("metadata"), dict) else {}
    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
    registry = _load_json_file(lifecycle.get("document_registry_path"))
    if not registry and isinstance(report.get("registry"), dict):
        registry = report.get("registry")
    reconciliation = report.get("reconciliation") if isinstance(report.get("reconciliation"), dict) else result.get("document_reconciliation") if isinstance(result.get("document_reconciliation"), dict) else {}
    context = report.get("tms_context") if isinstance(report.get("tms_context"), dict) else result.get("tms_context") if isinstance(result.get("tms_context"), dict) else {}
    filename = _first_present(metadata.get("file_name"), metadata.get("filename"), result.get("document_activity_file_name"), result.get("latest_subject"), empty="Dokument")
    uploaded_doc = _select_uploaded_document(registry, filename)
    analysis = _load_document_analysis(uploaded_doc.get("analysis_path"))
    fields = analysis.get("extracted_fields") if isinstance(analysis.get("extracted_fields"), dict) else {}
    findings = reconciliation.get("findings") if isinstance(reconciliation.get("findings"), list) else []
    unreadable = list(analysis.get("missing_or_unreadable") or uploaded_doc.get("missing_or_unreadable") or [])
    operational_flags = list(analysis.get("operational_flags") or uploaded_doc.get("operational_flags") or [])
    consistency_notes = list(analysis.get("consistency_notes") or [])
    if not consistency_notes and not findings and uploaded_doc.get("tms_matches"):
        consistency_notes = ["Dokumenttyp/Referenzen passen zu einem TMS-Dokumenteintrag."]
    risk = str(reconciliation.get("risk") or "low").strip().lower()
    if unreadable or "DOCUMENT_ANALYSIS_ERROR" in operational_flags:
        result_label = "nicht belastbar / manuell prüfen"
        next_step = "Dokument manuell öffnen bzw. Analysefehler prüfen."
        tone = "warn"
    elif findings or risk in {"medium", "high", "critical"} or reconciliation.get("needs_human_review"):
        result_label = "Auffälligkeit erkannt"
        next_step = _pick_document_next_step(findings)
        tone = "danger" if risk in {"high", "critical"} else "warn"
    else:
        result_label = "plausibel / keine Auffälligkeit"
        next_step = "Keine direkte Aktion nötig; Ablage und Monitoring sind aktualisiert."
        tone = "good"
    return {
        "order_id": result.get("order_id") or report.get("order_id") or payload.get("order_id") or "-",
        "status": result.get("status") or report.get("tms_status") or "document_uploaded_checked",
        "priority": result.get("analysis_priority") or ("medium" if tone == "warn" else "high" if tone == "danger" else "low"),
        "filename": filename,
        "event_doc_type": _first_present(metadata.get("document_type"), result.get("document_activity_document_type"), empty="unbekannt"),
        "analysis_doc_type": analysis.get("doc_type") or uploaded_doc.get("doc_type") or uploaded_doc.get("analysis_doc_type"),
        "confidence": analysis.get("confidence") or uploaded_doc.get("confidence") or uploaded_doc.get("analysis_confidence"),
        "summary": analysis.get("summary") or uploaded_doc.get("summary") or uploaded_doc.get("analysis_summary"),
        "fields": fields,
        "changed_at": _first_present(event.get("changed_at"), trigger.get("changed_at"), result.get("document_activity_changed_at"), empty="-"),
        "changed_by": _first_present(event.get("changed_by_name"), result.get("document_activity_changed_by"), empty="-"),
        "source": _first_present(event.get("source"), result.get("document_activity_source"), empty="-"),
        "context": context,
        "history_sync_count": result.get("history_sync_count"),
        "history_sync_error": result.get("history_sync_error") or lifecycle.get("history_sync_error"),
        "last_email_at": result.get("last_email_at") or lifecycle.get("last_email_at"),
        "mirrored_tms_documents": len(registry.get("mirrored_tms_documents", []) or []),
        "received_documents": len(registry.get("received_documents", []) or []),
        "findings": findings,
        "unreadable": unreadable,
        "operational_flags": operational_flags,
        "consistency_notes": consistency_notes,
        "tms_matches": uploaded_doc.get("tms_matches") or analysis.get("tms_matches") or [],
        "result_label": result_label,
        "next_step": _truncate_sentence(next_step, 180),
        "tone": tone,
        "report_path": result.get("document_monitoring_report_path") or report.get("report_json_path"),
    }


def _run_label(run_type: str) -> str:
    return {
        "bootstrap_case": "Bootstrap",
        "bootstrap_cases_from_tms": "Bulk-Bootstrap",
        "process_event": "Mail-Ingest",
        "analysis_update": "Analyse-Update",
        "document_activity_monitor": "Dokumenten-Upload-Monitor",
    }.get(run_type, run_type or "Run")


def _status_label(status: str, initialized: bool) -> str:
    mapping = {
        "bootstrapped": "initialisiert",
        "processed": "aktualisiert",
        "duplicate": "Dublette",
        "review_queue": "zur Prüfung",
    }
    base = mapping.get((status or "").strip(), status or "—")
    if initialized and status != "bootstrapped":
        base = f"{base} (neu)"
    return base


def _load_analysis_brief(path_value: Any) -> dict[str, Any]:
    path = str(path_value or "").strip()
    if not path:
        return {}
    try:
        candidate = Path(path)
        if not candidate.exists() or not candidate.is_file():
            return {}
        data = json.loads(candidate.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.debug("Could not load ASR analysis brief from %s", path, exc_info=True)
        return {}


def _load_tms_route(case_root: Path) -> dict[str, str]:
    try:
        data = json.loads((case_root / "tms_snapshot.json").read_text(encoding="utf-8"))
    except Exception:
        return {}
    detail = data.get("detail") if isinstance(data.get("detail"), dict) else {}
    origin_block = detail.get("origin") if isinstance(detail.get("origin"), dict) else {}
    dest_block = detail.get("destination") if isinstance(detail.get("destination"), dict) else {}
    return {
        "origin": str(origin_block.get("city") or "").strip(),
        "destination": str(dest_block.get("city") or "").strip(),
        "network": str(detail.get("network") or "").strip(),
        "shipment_uuid": str(data.get("shipment_uuid") or detail.get("shipment_uuid") or "").strip(),
    }


def _build_tms_url(shipment_uuid: str) -> str | None:
    if not shipment_uuid:
        return None
    base = os.getenv("HERMES_CARGOLO_TMS_BASE_URL", "https://api.cargolo.de").strip().rstrip("/")
    if not base:
        return None
    return f"{base}/admin/shipment_detail?shipment_uuid={shipment_uuid}"


def _build_review_links(order_id: str, suggestion_key: str) -> list[str]:
    """Return HTML anchor snippets for accept/reject review clicks, or []."""
    base = os.getenv("HERMES_CARGOLO_ASR_REVIEW_BASE_URL", "").strip().rstrip("/")
    if not base or not order_id:
        return []
    try:
        from .review import review_signing_available, sign_review_tokens
    except Exception:
        return []
    if not review_signing_available():
        return []
    try:
        tokens = sign_review_tokens(order_id=order_id, suggestion_key=suggestion_key)
    except Exception:
        logger.debug("review token signing failed", exc_info=True)
        return []
    accept_url = f"{base}?t={tokens['accepted']}"
    reject_url = f"{base}?t={tokens['rejected']}"
    return [
        f'<a href="{_html.escape(accept_url)}"><b>✅ Vorschlag richtig</b></a>',
        f'<a href="{_html.escape(reject_url)}"><b>✏️ War so nicht richtig</b></a>',
    ]


def _pick_top_risks(risk_flags: list, limit: int = 3) -> list[dict]:
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    ranked = sorted(
        (r for r in risk_flags if isinstance(r, dict)),
        key=lambda r: order.get(str(r.get("severity", "")).lower(), 9),
    )
    return ranked[:limit]


def _risk_short_text(risk: dict) -> str:
    reason = str(risk.get("reason") or "").strip()
    if not reason:
        return str(risk.get("code") or "Risiko").strip()
    return _truncate_sentence(reason, 140)


def _pick_next_step(brief: dict, fallback: str = "") -> str:
    actions = brief.get("internal_actions") if isinstance(brief.get("internal_actions"), list) else []
    blocking_first = [a for a in actions if isinstance(a, dict) and bool(a.get("blocking")) and a.get("action")]
    pool = blocking_first or [a for a in actions if isinstance(a, dict) and a.get("action")]
    if pool:
        return str(pool[0].get("action") or "").strip()
    return (fallback or "").strip()


def _build_bulk_message(run_type: str, payload: dict[str, Any]) -> str:
    total = payload.get("total_selected")
    success = payload.get("success_count")
    errors = payload.get("error_count")
    lines = [
        "🗂 <b>CARGOLO ASR · Bulk-Lauf</b>",
        f"<i>{_html.escape(_run_label(run_type))}</i>",
    ]
    stats = []
    if isinstance(total, int):
        stats.append(f"{total} ausgewählt")
    if isinstance(success, int):
        stats.append(f"{success} erfolgreich")
    if isinstance(errors, int) and errors > 0:
        stats.append(f"<b>{errors} Fehler</b>")
    if stats:
        lines.append(" · ".join(stats))
    return "<br>".join(lines)


def _build_summary_message(run_type: str, payload: dict[str, Any]) -> str:
    if run_type == "document_activity_monitor":
        result = payload.get("processor_result") if isinstance(payload.get("processor_result"), dict) else {}
        report = _load_json_file(result.get("document_monitoring_report_path") or result.get("case_report_path"))
        return _build_document_activity_text(payload, report)
    result = payload.get("processor_result") if isinstance(payload.get("processor_result"), dict) else {}
    order_id = result.get("order_id") or payload.get("order_id") or "-"
    status = str(result.get("status") or payload.get("status") or run_type).strip()
    history_sync_count = result.get("history_sync_count")
    history_sync_status = str(result.get("history_sync_status") or "").strip().lower()
    history_sync_error = str(result.get("history_sync_error") or "").strip()
    pending_summary = result.get("pending_action_summary") or {}
    applied_summary = result.get("applied_action_summary") or {}
    message_text = str(result.get("message") or payload.get("message") or "").strip()
    latest_subject = str(result.get("latest_subject") or payload.get("latest_subject") or "").strip()
    analysis_brief = _load_analysis_brief(result.get("analysis_brief_path"))
    analysis_priority = analysis_brief.get("priority") or result.get("analysis_priority")
    ops_summary = str(analysis_brief.get("ops_summary") or result.get("analysis_summary") or "").strip()
    risk_flags = analysis_brief.get("risk_flags") if isinstance(analysis_brief.get("risk_flags"), list) else []
    internal_actions = analysis_brief.get("internal_actions") if isinstance(analysis_brief.get("internal_actions"), list) else []

    recommendation = _pick_next_step(analysis_brief, fallback=ops_summary or message_text or latest_subject or "Weiter beobachten")
    recommendation = _truncate(recommendation or "Weiter beobachten", 140)

    attention = ""
    for risk in risk_flags:
        if not isinstance(risk, dict):
            continue
        severity = str(risk.get("severity") or "").strip().lower()
        reason = str(risk.get("reason") or risk.get("code") or "").strip()
        if severity in {"high", "critical"} and reason:
            attention = _truncate(reason, 120)
            break
    if not attention and str(analysis_priority or "").strip().lower() == "urgent":
        attention = "operative Dringlichkeit hoch"
    if not attention and history_sync_error:
        attention = "Mailhistorie nicht belastbar"

    if history_sync_error:
        history_token = "Mailhistorie Fehler"
    elif history_sync_status == "ok" or (history_sync_status == "" and isinstance(history_sync_count, int)):
        history_token = f"Mail +{history_sync_count or 0}"
    elif history_sync_status == "skipped":
        history_token = "Mail unverändert"
    else:
        history_token = f"Mail {history_sync_status or 'unbekannt'}"

    applied_targets = [str(item).strip() for item in (result.get("applied_action_targets") or []) if str(item).strip()]
    failed_targets = [str(item).strip() for item in (result.get("failed_action_targets") or []) if str(item).strip()]
    pending_total = sum(int(value or 0) for value in pending_summary.values()) if isinstance(pending_summary, dict) else 0
    review_count = int(pending_summary.get("review", 0) or 0) if isinstance(pending_summary, dict) else 0
    write_now_count = int(pending_summary.get("write_now", 0) or 0) if isinstance(pending_summary, dict) else 0

    if applied_targets:
        tms_token = f"TMS geändert: {_truncate(', '.join(applied_targets[:2]), 90)}"
    elif failed_targets:
        tms_token = f"TMS Fehler: {_truncate(', '.join(failed_targets[:2]), 90)}"
    elif isinstance(applied_summary, dict) and int(applied_summary.get("applied", 0) or 0) > 0:
        tms_token = f"TMS geändert: {int(applied_summary.get('applied', 0) or 0)}"
    elif write_now_count > 0:
        tms_token = f"TMS offen: {write_now_count} direkt umsetzbar"
    else:
        tms_token = "TMS unverändert"

    situation = _truncate(ops_summary or message_text or latest_subject or recommendation, 160)
    tms_line = f"TMS-Aktion: {tms_token} | Review {review_count} | Offen {pending_total} | {history_token}"
    next_step_line = f"Nächster Schritt: {recommendation}"
    if attention:
        next_step_line += f" | Achtung: {attention}"

    return "\n".join([
        f"{order_id} | {status} | Priorität {_priority_label(analysis_priority)}",
        f"Lage: {situation}",
        tms_line,
        next_step_line,
    ])


def _format_html_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "ja" if value else "nein"
    if isinstance(value, (int, float)):
        return _html_escape(str(value))
    if isinstance(value, str):
        stripped = value.strip()
        return _html_escape(stripped) if stripped else "-"
    if isinstance(value, list):
        items = [item for item in value if item not in (None, "", [], {})]
        if not items:
            return "-"
        inner = "".join(f"<li style='color:#f8fafc;margin-bottom:4px;'>{_format_html_value(item)}</li>" for item in items)
        return f"<ul style='color:#f8fafc;margin:8px 0 0 18px;padding:0;'>{inner}</ul>"
    if isinstance(value, dict):
        if not value:
            return "-"
        rows = []
        for key, item in value.items():
            rows.append(
                f"<tr><th align='left' style='color:#ffffff;border-color:#334155;'>{_html_escape(str(key))}</th><td style='color:#f8fafc;border-color:#334155;'>{_format_html_value(item)}</td></tr>"
            )
        return f"<table border='1' cellspacing='0' cellpadding='6' style='color:#f8fafc;border-color:#334155;border-collapse:collapse;'>{''.join(rows)}</table>"
    return _html_escape(str(value))


def _html_badge(text: Any, tone: str = "neutral") -> str:
    palette = {
        "neutral": ("#1e3a8a", "#ffffff"),
        "good": ("#065f46", "#ffffff"),
        "warn": ("#92400e", "#ffffff"),
        "danger": ("#991b1b", "#ffffff"),
        "dark": ("#334155", "#ffffff"),
    }
    bg, fg = palette.get(tone, palette["neutral"])
    return (
        f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;"
        f"background:{bg};color:{fg};font-size:12px;font-weight:700;margin-right:6px;'>"
        f"{_html_escape(str(text or '-'))}</span>"
    )


def _html_section(title: str, body: str, subtitle: str | None = None) -> str:
    subtitle_html = (
        f"<div style='color:#cbd5e1;font-size:13px;margin-top:4px;'>{_html_escape(subtitle)}</div>"
        if subtitle
        else ""
    )
    return (
        "<section style='background:#111827;border:1px solid #334155;border-radius:16px;"
        "padding:20px;margin:18px 0;box-shadow:0 1px 2px rgba(15,23,42,0.20);'>"
        f"<div style='font-size:20px;font-weight:800;color:#ffffff;margin-bottom:14px;'>{_html_escape(title)}</div>"
        f"{subtitle_html}{body}</section>"
    )


def _html_fact_grid(items: list[tuple[str, Any]]) -> str:
    cards = []
    for label, value in items:
        cards.append(
            "<div style='background:#0f172a;border:1px solid #334155;border-radius:14px;padding:14px;'>"
            f"<div style='font-size:12px;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;color:#93c5fd;margin-bottom:6px;'>{_html_escape(label)}</div>"
            f"<div style='font-size:16px;font-weight:700;color:#ffffff;line-height:1.35;'>{_format_html_value(value)}</div>"
            "</div>"
        )
    return (
        "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;'>"
        + "".join(cards)
        + "</div>"
    )


def _html_list_block(title: str, items: list[Any], *, tone: str = "neutral", empty: str = "Keine") -> str:
    clean_items = [item for item in items if item not in (None, "", [], {})]
    if not clean_items:
        content = f"<div style='color:#cbd5e1;'>{_html_escape(empty)}</div>"
    else:
        lis = []
        for item in clean_items:
            lis.append(
                "<li style='margin:0 0 10px 0;padding-left:2px;color:#ffffff;line-height:1.5;'>"
                f"{_format_html_value(item)}</li>"
            )
        content = f"<ul style='margin:10px 0 0 18px;padding:0;'>{''.join(lis)}</ul>"
    accent = {"neutral": "#60a5fa", "good": "#86efac", "warn": "#fcd34d", "danger": "#fca5a5"}.get(tone, "#60a5fa")
    return (
        f"<div style='background:#0f172a;border:1px solid #334155;border-left:5px solid {accent};border-radius:12px;padding:14px 16px;'>"
        f"<div style='font-size:14px;font-weight:800;color:#ffffff;'>{_html_escape(title)}</div>{content}</div>"
    )


def _document_field_lines(fields: dict[str, Any]) -> list[str]:
    labels = [
        ("invoice_number", "Rechnung"),
        ("document_number", "Dok.-Nr."),
        ("amount", "Betrag"),
        ("currency", "Währung"),
        ("carrier", "Carrier"),
        ("pol", "POL"),
        ("pod", "POD"),
        ("etd", "ETD"),
        ("eta", "ETA"),
        ("mrn", "MRN"),
    ]
    lines = []
    for key, label in labels:
        value = fields.get(key) if isinstance(fields, dict) else None
        if value not in (None, "", [], {}):
            lines.append(f"{label}: {value}")
    return lines[:8]


def _build_document_activity_html(payload: dict[str, Any], report: dict[str, Any]) -> str:
    """Use the same compact card for the full HTML payload and Teams prompt text."""
    return _build_document_activity_text(payload, report)


def _extract_section_value(case_report: dict[str, Any], *path: str) -> Any:
    node: Any = case_report.get("sections") if isinstance(case_report, dict) else None
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    if isinstance(node, dict) and "value" in node:
        return node.get("value")
    return node


def _priority_tone(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"urgent", "critical"}:
        return "danger"
    if raw == "high":
        return "warn"
    if raw == "medium":
        return "neutral"
    return "good"


def _build_overview_html(run_type: str, payload: dict[str, Any], case_report: dict[str, Any], analysis_brief: dict[str, Any]) -> str:
    result = payload.get("processor_result") if isinstance(payload.get("processor_result"), dict) else {}
    order_id = result.get("order_id") or payload.get("order_id") or "-"
    status = str(result.get("status") or payload.get("status") or run_type).strip() or "-"
    priority = analysis_brief.get("priority") or result.get("analysis_priority") or "mittel"
    history = result.get("history_sync_count")
    pending = result.get("pending_action_summary") or {}
    applied = result.get("applied_action_summary") or {}
    pending_total = sum(int(v or 0) for v in pending.values()) if isinstance(pending, dict) else 0
    write_now = int(pending.get("write_now", 0) or 0) if isinstance(pending, dict) else 0
    review_count = int(pending.get("review", 0) or 0) if isinstance(pending, dict) else 0
    applied_count = int(applied.get("applied", 0) or 0) if isinstance(applied, dict) else 0
    action_list = analysis_brief.get("internal_actions") if isinstance(analysis_brief.get("internal_actions"), list) else []
    risk_flags = analysis_brief.get("risk_flags") if isinstance(analysis_brief.get("risk_flags"), list) else []
    top_action = _pick_next_step(analysis_brief, fallback=result.get("message") or "Weiter beobachten")

    shipment = _extract_section_value(case_report, "tms_mcp", "shipment") or {}
    route = None
    if isinstance(shipment, dict):
        origin = (shipment.get("origin_city") or {}).get("value") if isinstance(shipment.get("origin_city"), dict) else None
        destination = (shipment.get("destination_city") or {}).get("value") if isinstance(shipment.get("destination_city"), dict) else None
        if origin or destination:
            route = f"{origin or '-'} → {destination or '-'}"

    latest_subjects = _extract_section_value(case_report, "mail_history", "latest_subjects") or []
    latest_subject = str(latest_subjects[-1] if latest_subjects else result.get("latest_subject") or "").strip()
    ops_summary = _truncate(
        analysis_brief.get("ops_summary") or result.get("analysis_summary") or result.get("message") or latest_subject or "-",
        220,
    )
    top_risks = [_risk_short_text(risk) for risk in _pick_top_risks(risk_flags, limit=2)]
    applied_targets = [str(item).strip() for item in (result.get("applied_action_targets") or []) if str(item).strip()]
    failed_targets = [str(item).strip() for item in (result.get("failed_action_targets") or []) if str(item).strip()]

    if applied_targets:
        tms_status = f"Geändert: {_truncate(', '.join(applied_targets[:2]), 90)}"
    elif failed_targets:
        tms_status = f"Fehler: {_truncate(', '.join(failed_targets[:2]), 90)}"
    elif applied_count > 0:
        tms_status = f"Geändert: {applied_count}"
    elif write_now > 0:
        tms_status = f"Offen: {write_now} direkt umsetzbar"
    else:
        tms_status = "Keine direkte Änderung"

    header = (
        "<div style='background:#0f172a;color:#ffffff;border:1px solid #2563eb;border-left:8px solid #60a5fa;"
        "border-radius:20px;padding:24px 24px 18px 24px;box-shadow:0 6px 18px rgba(15,23,42,0.25);'>"
        f"<div style='font-size:28px;font-weight:900;line-height:1.2;margin-bottom:10px;color:#ffffff;'>CARGOLO ASR – {_html_escape(str(order_id))}</div>"
        f"<div style='margin-bottom:12px;'>{_html_badge('Status ' + status, 'dark')}{_html_badge('Priorität ' + _priority_label(priority), _priority_tone(priority))}{_html_badge('Run ' + str(run_type), 'neutral')}</div>"
        f"<div style='font-size:16px;line-height:1.6;max-width:980px;color:#ffffff;'>{_format_html_value(ops_summary)}</div>"
        "</div>"
    )

    facts = _html_fact_grid([
        ("Route", route or "-"),
        ("Mailhistorie", f"+{history or 0}"),
        ("TMS", tms_status),
        ("Offen", pending_total),
    ])

    lage = _html_list_block(
        "Lage",
        [
            ops_summary,
            f"Neueste Mail: {latest_subject}" if latest_subject else None,
        ],
        tone="neutral",
        empty="Keine Kurzlage verfügbar",
    )
    tms_block = _html_list_block(
        "TMS-Aktion",
        [
            tms_status,
            f"Review-Punkte: {review_count}" if review_count else None,
            f"Direkt umsetzbar: {write_now}" if write_now else None,
        ],
        tone="warn" if (review_count or write_now) else "good",
        empty="Keine TMS-Aktion offen",
    )
    next_step_block = _html_list_block(
        "Nächster Schritt",
        [
            _truncate(top_action, 160),
            *top_risks,
        ],
        tone="danger" if top_risks else "neutral",
        empty="Weiter beobachten",
    )

    columns = (
        "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px;margin-top:16px;'>"
        + lage
        + tms_block
        + next_step_block
        + "</div>"
    )
    return header + "<div style='margin-top:18px;'>" + facts + columns + "</div>"


def _render_case_report_sections(sections: dict[str, Any]) -> str:
    parts: list[str] = []
    for section_name, section_body in sections.items():
        title = section_name.replace("_", " ").strip().title()
        body_parts: list[str] = []
        if isinstance(section_body, dict):
            for block_name, block_value in section_body.items():
                subtitle = block_name.replace("_", " ").strip().title()
                body_parts.append(
                    "<div style='margin:14px 0 0 0;padding-top:14px;border-top:1px solid #334155;color:#f8fafc;'>"
                    f"<div style='font-size:15px;font-weight:800;color:#ffffff;margin-bottom:8px;'>{_html_escape(subtitle)}</div>"
                    f"{_format_html_value(block_value)}</div>"
                )
        else:
            body_parts.append(_format_html_value(section_body))
        parts.append(_html_section(title, "".join(body_parts)))
    return "".join(parts)


def _load_case_report(path_value: Any) -> dict[str, Any]:
    path = str(path_value or "").strip()
    if not path:
        return {}
    try:
        candidate = Path(path)
        if not candidate.exists() or not candidate.is_file():
            return {}
        data = json.loads(candidate.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.debug("Could not load ASR case report from %s", path, exc_info=True)
        return {}


def _build_message(run_type: str, payload: dict[str, Any]) -> str:
    result = payload.get("processor_result") if isinstance(payload.get("processor_result"), dict) else {}
    case_report = _load_case_report(result.get("document_monitoring_report_path") or result.get("case_report_path"))
    analysis_brief = _load_analysis_brief(result.get("analysis_brief_path"))

    html_parts = [
        "<html><body style='margin:0;padding:0;background:#020617;font-family:Segoe UI,Arial,sans-serif;color:#ffffff;'>",
        "<div style='max-width:1180px;margin:0 auto;padding:24px 18px 40px 18px;'>",
        _build_document_activity_html(payload, case_report) if run_type == "document_activity_monitor" else _build_overview_html(run_type, payload, case_report, analysis_brief),
        "</div></body></html>",
    ]
    return "".join(html_parts)


def build_manual_ops_notification_body(
    *,
    run_type: str,
    payload: dict[str, Any],
    route_name: str = _DEFAULT_ROUTE_NAME,
    delivery_id: str | None = None,
    delivered_at: float | None = None,
) -> dict[str, Any]:
    message = _build_message(run_type, payload)
    message_text = _build_summary_message(run_type, payload)
    return {
        "event_type": "cargolo_asr_manual_ops_notification",
        "route": route_name,
        "delivery_id": delivery_id or f"manual-{run_type}-{int(time.time() * 1000)}",
        "delivered_at": delivered_at if delivered_at is not None else time.time(),
        "message": message,
        "message_text": message_text,
        "message_format": "html",
        "payload": {
            "event_type": "cargolo_asr_manual_ops_notification",
            "run_type": run_type,
            **payload,
        },
    }


def send_manual_ops_notification(
    *,
    run_type: str,
    payload: dict[str, Any],
    route_name: str = _DEFAULT_ROUTE_NAME,
    allow_route_fallback: bool = False,
) -> dict[str, Any]:
    targets = _load_targets(route_name=route_name, allow_route_fallback=allow_route_fallback)
    if not targets:
        return {
            "enabled": False,
            "attempted": 0,
            "delivered": 0,
            "errors": [],
            "targets": [],
        }

    timeout_raw = str(os.getenv("HERMES_CARGOLO_ASR_OPS_WEBHOOK_TIMEOUT", "30")).strip()
    timeout = int(timeout_raw) if timeout_raw.isdigit() else 30
    body = build_manual_ops_notification_body(run_type=run_type, payload=payload, route_name=route_name)

    delivered = 0
    errors: list[str] = []
    target_urls: list[str] = []
    delivery_targets: list[str] = []
    for target in targets:
        url = str(target.get("url") or "").strip()
        if not url:
            continue
        target_urls.append(url)
        delivery_targets.append(str(target.get("source") or url))
        method = str(target.get("method") or "POST").strip().upper() or "POST"
        headers = dict(target.get("headers") or {})
        headers.setdefault("Content-Type", "application/json")
        try:
            request_body = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            secret = str(target.get("secret") or "").strip()
            if secret:
                signature = hmac.new(secret.encode("utf-8"), request_body, hashlib.sha256).hexdigest()
                headers.setdefault("X-Hub-Signature-256", f"sha256={signature}")
            response = requests.request(method, url, data=request_body, headers=headers, timeout=timeout)
            response.raise_for_status()
            delivered += 1
        except Exception as exc:
            safe_error = str(exc).replace(url, "<redacted_url>")
            logger.warning("ASR manual ops notification failed for configured target: %s", safe_error)
            errors.append(f"<redacted_url>: {safe_error}")

    return {
        "enabled": True,
        "attempted": len(target_urls),
        "delivered": delivered,
        "errors": errors,
        "targets": delivery_targets or target_urls,
    }
