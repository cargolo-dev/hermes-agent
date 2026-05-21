from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .document_monitoring import run_document_monitoring
from .document_profiles import get_document_profile, is_trusted_source_for_field
from .document_schema import normalize_document_type
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
        "master_bill_of_lading": "Master B/L",
        "house_bl": "House B/L",
        "house_bill_of_lading": "House B/L",
        "hbl": "House B/L",
        "mbl": "Master B/L",
        "proof_of_delivery": "POD",
        "mrn": "MRN/Zollreferenz",
        "customs_document": "Zolldokument",
        "billing": "Abrechnungsbeleg",
        "offer": "Angebot",
        "telex_release": "Telex Release",
        "transport_order": "Transportauftrag",
        "unknown": "Dokument",
        "unbekannt": "Dokument",
    }.get(raw, raw.replace("_", " ") or "Dokument")


def _finding_rank(row: Any) -> tuple[int, str]:
    if not isinstance(row, dict):
        return (9, str(row or ""))
    severity = str(row.get("severity") or "").lower()
    finding_type = str(row.get("type") or "").lower()
    text = str(row.get("summary") or "").lower()
    if severity in {"critical", "high", "blocker"} or "mrn" in finding_type or "customs" in finding_type:
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


def _is_archived_email_upload(filename: Any, metadata: dict[str, Any] | None = None) -> bool:
    text = str(filename or "").strip().lower()
    metadata = metadata or {}
    doc_type = normalize_document_type(metadata.get("document_type"))
    return doc_type == "email" or text.endswith(".msg") or text.endswith(".eml")


def _parse_registry_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _registry_identity_values(row: dict[str, Any]) -> list[Any]:
    values = [
        row.get("filename"),
        row.get("sha256"),
        row.get("tms_document_id"),
        row.get("document_uuid"),
        row.get("document_id"),
        row.get("local_path"),
        row.get("stored_path"),
    ]
    for path_key in ("local_path", "stored_path"):
        path_value = str(row.get(path_key) or "").strip()
        if path_value:
            values.append(Path(path_value).name)
    return values


def _received_document_lookup(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in registry.get("received_documents") or []:
        if not isinstance(row, dict):
            continue
        for key_value in _registry_identity_values(row):
            key = _norm(key_value)
            if key:
                lookup[key] = row
    return lookup


def _registry_document_lookup(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for collection_name in ("received_documents", "tms_documents", "mirrored_tms_documents"):
        for row in registry.get(collection_name) or []:
            if not isinstance(row, dict):
                continue
            for key_value in _registry_identity_values(row):
                key = _norm(key_value)
                if key:
                    lookup[key] = row
    return lookup


def _enrich_registry_document_row(row: dict[str, Any], received_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    received = received_lookup.get(_norm(row.get("filename"))) or received_lookup.get(_norm(row.get("sha256"))) or {}
    analysis = _load_json(row.get("analysis_path") or received.get("analysis_path"))
    enriched = {**received, **row, "analysis": analysis}
    if not enriched.get("received_at") and received.get("received_at"):
        enriched["received_at"] = received.get("received_at")
    return enriched


def _enrich_analyzed_row(row: dict[str, Any], received_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return _enrich_registry_document_row(row, received_lookup)


def _latest_analyzed_mail_attachment(registry: dict[str, Any]) -> dict[str, Any]:
    received_lookup = _received_document_lookup(registry)
    analyzed = [_enrich_analyzed_row(row, received_lookup) for row in registry.get("analyzed_documents") or [] if isinstance(row, dict)]
    if not analyzed:
        return {}

    def sort_key(row: dict[str, Any]) -> tuple[datetime, str]:
        received_at = _parse_registry_datetime(row.get("received_at")) or datetime.min.replace(tzinfo=timezone.utc)
        return (received_at, str(row.get("filename") or ""))

    return sorted(analyzed, key=sort_key)[-1]


def _select_uploaded_analysis(report: dict[str, Any], filename: str) -> dict[str, Any]:
    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
    registry = _load_json(lifecycle.get("document_registry_path"))
    metadata = (report.get("trigger_event") or {}).get("metadata") if isinstance(report.get("trigger_event"), dict) else {}
    metadata = metadata if isinstance(metadata, dict) else {}
    uploaded_norm = str(filename or "").strip().lower()
    received_lookup = _received_document_lookup(registry)
    analyzed = [row for row in registry.get("analyzed_documents") or [] if isinstance(row, dict)]
    for row in analyzed:
        if str(row.get("filename") or "").strip().lower() == uploaded_norm:
            return _enrich_analyzed_row(row, received_lookup)

    # TMS mirror names often differ from the original mail-history attachment
    # name even when the file content is identical (e.g. CI(4).PDF in TMS vs
    # CI.PDF in mail history).  Resolve the current upload by stable identity
    # before declaring its analysis/evidence empty.
    document_lookup = _registry_document_lookup(registry)
    target_values: list[Any] = [filename, Path(str(filename)).name if str(filename or "").strip() else ""]
    target_values.extend([metadata.get(key) for key in ("file_name", "filename", "sha256", "tms_document_id", "document_uuid", "document_id")])
    target_keys = [_norm(value) for value in target_values if _norm(value)]
    for key in target_keys:
        matched_document = document_lookup.get(key)
        if not matched_document:
            continue
        enriched_document = _enrich_registry_document_row(matched_document, received_lookup)
        if enriched_document.get("analysis"):
            return enriched_document
        matched_sha = _norm(matched_document.get("sha256"))
        matched_filename = _norm(matched_document.get("filename"))
        for row in analyzed:
            enriched = _enrich_analyzed_row(row, received_lookup)
            if matched_sha and _norm(enriched.get("sha256")) == matched_sha:
                return enriched
            if matched_filename and _norm(enriched.get("filename")) == matched_filename:
                return enriched

    if _is_archived_email_upload(filename, metadata):
        return _latest_analyzed_mail_attachment(registry)
    if len(analyzed) == 1:
        return _enrich_analyzed_row(analyzed[0], received_lookup)
    return {}


def _infer_uploaded_filename(report: dict[str, Any], event: dict[str, Any]) -> str:
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    explicit = metadata.get("file_name") or metadata.get("filename") or event.get("field_name")
    explicit_is_email_archive = _is_archived_email_upload(explicit, metadata)
    if _clean(explicit) and not explicit_is_email_archive:
        return str(explicit)
    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
    registry = _load_json(lifecycle.get("document_registry_path"))
    invoice_number = _norm(metadata.get("invoice_number"))
    subject = _norm(metadata.get("email_subject"))
    received_lookup = _received_document_lookup(registry)
    analyzed = [_enrich_analyzed_row(row, received_lookup) for row in registry.get("analyzed_documents") or [] if isinstance(row, dict)]
    for row in analyzed:
        filename = str(row.get("filename") or "")
        if invoice_number and invoice_number in _norm(filename):
            return filename
        raw_analysis = row.get("analysis")
        analysis: dict[str, Any] = raw_analysis if isinstance(raw_analysis, dict) else {}
        raw_fields = analysis.get("extracted_fields")
        fields: dict[str, Any] = raw_fields if isinstance(raw_fields, dict) else {}
        if invoice_number and invoice_number in {_norm(fields.get("invoice_number")), _norm(fields.get("document_number"))}:
            return filename
        field_refs = {
            _norm(fields.get("shipment_number")),
            _norm(fields.get("tms_reference")),
            _norm(fields.get("document_number")),
            _norm(fields.get("customer_reference")),
        }
        if subject and any(ref and ref in subject for ref in field_refs):
            return filename
        if subject and filename and _norm(filename) in subject:
            return filename
    if explicit_is_email_archive:
        latest = _latest_analyzed_mail_attachment(registry)
        latest_filename = str(latest.get("filename") or "").strip()
        if latest_filename:
            return latest_filename
    if len(analyzed) == 1 and str(analyzed[0].get("filename") or "").strip():
        return str(analyzed[0].get("filename") or "")
    received = [row for row in registry.get("received_documents") or [] if isinstance(row, dict)]
    for row in received:
        filename = str(row.get("filename") or "")
        if invoice_number and invoice_number in _norm(filename):
            return filename
    return str(explicit) if _clean(explicit) else "Dokument"


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


def _normalize_date_value(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    match = re.search(r"\b(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})\b", text)
    if match:
        normalized = f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        try:
            datetime.strptime(normalized, "%Y-%m-%d")
        except ValueError:
            return ""
        return normalized
    match = re.search(r"\b(\d{1,2})[./-](\d{1,2})[./-](20\d{2})\b", text)
    if match:
        normalized = f"{match.group(3)}-{int(match.group(2)):02d}-{int(match.group(1)):02d}"
        try:
            datetime.strptime(normalized, "%Y-%m-%d")
        except ValueError:
            return ""
        return normalized
    return ""


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


def _number_from_value(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"[-+]?\d[\d .,'’]*", text)
    if not match:
        return None
    token = match.group(0).replace("'", "").replace("’", "").replace(" ", "")
    if "," in token and "." in token:
        if token.rfind(",") > token.rfind("."):
            token = token.replace(".", "").replace(",", ".")
        else:
            token = token.replace(",", "")
    elif "," in token:
        token = token.replace(",", ".")
    try:
        return float(token)
    except Exception:
        return None


def _format_number(value: float | int | None) -> str:
    if value is None:
        return ""
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.3f}".rstrip("0").rstrip(".")


def _cargo_rows_from_detail(detail: dict[str, Any]) -> list[dict[str, Any]]:
    rows = detail.get("cargo") if isinstance(detail.get("cargo"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _cargo_sum_value(rows: list[dict[str, Any]], key: str) -> float | None:
    values: list[float] = []
    for row in rows:
        number = _number_from_value(row.get(key))
        if number is not None:
            values.append(number)
    if not values:
        return None
    return sum(values)


def _cargo_unit_value(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [_number_from_value(row.get(key)) for row in rows]
    values = [value for value in values if value is not None]
    if len(values) == 1:
        return values[0]
    return None


def _tms_cargo_reference(detail: dict[str, Any], totals: dict[str, Any], target: str) -> tuple[str, str]:
    """Return a TMS cargo value plus source label for document comparison.

    Top-level TMS totals are the authoritative comparison source when they are
    populated.  Cargo item rows are a fallback for snapshots where totals are
    empty, so the monitor does not incorrectly say "missing in TMS" although
    operational cargo rows are filled.  Avoid cargo-row total_weight_kg for the
    displayed gross weight because some rows store quantity*weight there (as in
    AN-12405); use it only as an inconsistency note elsewhere.
    """
    rows = _cargo_rows_from_detail(detail)
    if target == "pieces":
        total_value = _number_from_value(totals.get("pieces") or totals.get("total_pieces"))
        if total_value is not None:
            return _format_number(total_value), "totals"
        cargo_value = _cargo_sum_value(rows, "quantity")
        if cargo_value is not None:
            return _format_number(cargo_value), "cargo_items.quantity"
        return "", ""
    if target == "weight":
        total_weight = _number_from_value(totals.get("total_weight_kg") or totals.get("weight_kg"))
        if total_weight is not None:
            return f"{_format_number(total_weight)} kg", "totals"
        cargo_weight = None
        source = "cargo_items.weight_kg"
        if len(rows) == 1:
            row = rows[0]
            unit_weight = _number_from_value(row.get("weight_kg"))
            quantity = _number_from_value(row.get("quantity"))
            row_total_weight = _number_from_value(row.get("total_weight_kg"))
            if row_total_weight is not None and (unit_weight is None or row_total_weight <= unit_weight * 5):
                cargo_weight = row_total_weight
                source = "cargo_items.total_weight_kg"
            elif unit_weight is not None:
                cargo_weight = unit_weight
                source = "cargo_items.weight_kg"
            elif quantity is not None and row_total_weight is not None:
                cargo_weight = row_total_weight
                source = "cargo_items.total_weight_kg"
        else:
            cargo_weight = _cargo_sum_value(rows, "weight_kg")
            source = "cargo_items.weight_kg"
        if cargo_weight is not None:
            return f"{_format_number(cargo_weight)} kg", source
        return "", ""
    return "", ""


def _numeric_comparison_status(tms: Any, doc: Any, *, tolerance: float = 0.0) -> str | None:
    tms_number = _number_from_value(tms)
    doc_number = _number_from_value(doc)
    if tms_number is None or doc_number is None:
        return None
    diff = abs(tms_number - doc_number)
    if diff == 0:
        return "match"
    if tolerance and diff <= tolerance:
        return "near_match"
    return "diff"


def _is_date_like_mbl_candidate(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    year_first = r"^(?:19|20)\d{2}[-./]?\d{2}[-./]?\d{2}"
    eu_day_first = r"^(?:0?[1-9]|[12]\d|3[01])[-./](?:0?[1-9]|1[0-2])[-./](?:(?:19|20)\d{2}|\d{2})"
    eu_day_month_without_year = r"^(?:0?[1-9]|[12]\d|3[01])[-./](?:0?[1-9]|1[0-2])(?:[A-Za-z]+|$)"
    return bool(
        re.match(year_first, text)
        or re.match(eu_day_first, text)
        or re.match(eu_day_month_without_year, text)
    )


def _is_valid_mbl_candidate(value: Any) -> bool:
    text = _clean(value)
    if not text or _is_date_like_mbl_candidate(text):
        return False
    normalized = _norm(text)
    if len(normalized) < 6:
        return False
    placeholder_values = {
        "unknown",
        "unbekannt",
        "notreadable",
        "unreadable",
        "nichtlesbar",
        "notavailable",
        "notapplicable",
        "missing",
        "pending",
        "tobeadvised",
        "tobeconfirmed",
    }
    if normalized in placeholder_values:
        return False
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ./_-]*[A-Za-z0-9]", text):
        return False
    return bool(re.search(r"\d", text))


def _select_explicit_mbl_candidate(fields: dict[str, Any]) -> str:
    for key in ("mbl_number", "master_bl_number", "bill_of_lading_number", "bl_number"):
        candidate = _clean(fields.get(key))
        if _is_valid_mbl_candidate(candidate):
            return candidate
    return ""


def _is_valid_container_candidate(value: Any) -> bool:
    return bool(re.fullmatch(r"[A-Z]{4}\d{7}", str(value or "").strip()))


def _is_valid_document_field_evidence(target: str, value: Any) -> bool:
    if target == "mbl_number":
        return _is_valid_mbl_candidate(value)
    if target == "container_number":
        return _is_valid_container_candidate(value)
    return bool(_clean(value))


def _has_uploaded_document_blocker_finding(findings: list[Any], filename: Any) -> bool:
    uploaded_norm = str(filename or "").strip().lower()
    for row in findings:
        if not isinstance(row, dict):
            continue
        if uploaded_norm and str(row.get("filename") or "").strip().lower() != uploaded_norm:
            continue
        severity = str(row.get("severity") or "").strip().lower()
        finding_type = str(row.get("type") or "").strip().lower()
        if severity in {"high", "critical", "blocker"} or "blocker" in finding_type:
            return True
    return False


def _has_blocker_finding(findings: list[Any]) -> bool:
    for row in findings:
        if not isinstance(row, dict):
            continue
        severity = str(row.get("severity") or "").strip().lower()
        finding_type = str(row.get("type") or "").strip().lower()
        if severity == "blocker" or "blocker" in finding_type:
            return True
    return False


def _focus_findings_for_upload(findings: list[Any], filename: str) -> list[Any]:
    uploaded_norm = str(filename or "").strip().lower()
    if not uploaded_norm:
        return findings
    focused: list[Any] = []
    for row in findings:
        if not isinstance(row, dict):
            focused.append(row)
            continue
        row_filename = str(row.get("filename") or "").strip().lower()
        severity = str(row.get("severity") or "").strip().lower()
        finding_type = str(row.get("type") or "").strip().lower()
        if row_filename == uploaded_norm or (not row_filename and (severity in {"high", "critical", "blocker"} or "blocker" in finding_type)):
            focused.append(row)
    return focused


def _normalize_uploaded_findings(findings: list[Any], uploaded: dict[str, Any], filename: str) -> list[Any]:
    analysis = uploaded.get("analysis") if isinstance(uploaded.get("analysis"), dict) else {}
    fields = analysis.get("extracted_fields") if isinstance(analysis.get("extracted_fields"), dict) else {}
    currency = _clean(fields.get("currency"))
    if not currency:
        return findings
    uploaded_norm = str(filename or "").strip().lower()
    normalized: list[Any] = []
    for row in findings:
        if not isinstance(row, dict):
            normalized.append(row)
            continue
        row_filename = str(row.get("filename") or "").strip().lower()
        summary = str(row.get("summary") or "")
        if row_filename == uploaded_norm and re.search(r"währung|currency", summary, re.IGNORECASE):
            row = {**row, "summary": f"Währung {currency} extrahiert, aber Quelle/Explizitheit unsicher – für Zollwert prüfen."}
        normalized.append(row)
    return normalized


def _effective_trusted_doc_type(event_doc_type: Any, uploaded: dict[str, Any], target: str, value: Any) -> str:
    analysis = uploaded.get("analysis") if isinstance(uploaded.get("analysis"), dict) else {}
    candidates = [
        analysis.get("doc_type"),
        uploaded.get("analysis_doc_type"),
        uploaded.get("doc_type"),
    ]
    suggested = analysis.get("suggested_registry_types")
    if isinstance(suggested, list):
        candidates.extend(suggested)
    fields = analysis.get("extracted_fields") if isinstance(analysis.get("extracted_fields"), dict) else {}
    candidates.extend([
        fields.get("document_type"),
        uploaded.get("filename"),
        analysis.get("filename"),
        event_doc_type,
    ])
    legacy_bl_seen = False
    fallback_normalized = ""
    for candidate in candidates:
        normalized = normalize_document_type(candidate)
        if normalized == "bill_of_lading":
            legacy_bl_seen = True
            fallback_normalized = normalized
            continue
        if normalized and normalized != "unknown" and get_document_profile(normalized).document_type != "internal_misc":
            return normalized
        if normalized and not fallback_normalized:
            fallback_normalized = normalized

    if legacy_bl_seen:
        has_bl_evidence = bool(
            _select_explicit_mbl_candidate(fields)
            or _clean(fields.get("hbl_number") or fields.get("house_bl_number"))
            or _clean(fields.get("bill_of_lading_number") or fields.get("bl_number"))
            or (target in {"mbl_number", "hbl_number", "container_number"} and _is_valid_document_field_evidence(target, value))
        )
        if has_bl_evidence:
            return "house_bill_of_lading" if target == "hbl_number" else "master_bill_of_lading"
    return fallback_normalized or normalize_document_type(event_doc_type)


def _mbl_candidate_field_name(fields: dict[str, Any], value: Any) -> str:
    value_norm = _norm(value)
    for key in ("mbl_number", "master_bl_number", "bill_of_lading_number", "bl_number"):
        candidate = _clean(fields.get(key))
        if _is_valid_mbl_candidate(candidate) and _norm(candidate) == value_norm:
            return key
    return ""


def _field_source_allows_mbl_candidate(analysis: dict[str, Any], value: Any) -> bool:
    field_sources = analysis.get("field_sources") if isinstance(analysis.get("field_sources"), dict) else {}
    if not field_sources:
        return True
    fields = analysis.get("extracted_fields") if isinstance(analysis.get("extracted_fields"), dict) else {}
    candidate_key = _mbl_candidate_field_name(fields, value) or "mbl_number"
    source_meta = field_sources.get(candidate_key)
    if not isinstance(source_meta, dict):
        return True
    provenance = " ".join(
        _clean(source_meta.get(part))
        for part in ("label", "source", "raw_context")
        if _clean(source_meta.get(part))
    )
    if not provenance:
        return False
    has_bl_context = bool(re.search(r"\b(?:b\s*/\s*l|bl|bill\s+of\s+lading|master|ocean)\b", provenance, re.IGNORECASE))
    has_wrong_context = bool(re.search(r"\b(?:booking|invoice|date)\b", provenance, re.IGNORECASE))
    return has_bl_context and not has_wrong_context


def _container_candidate_field_name(fields: dict[str, Any], value: Any) -> str:
    value_norm = _norm(value)
    for key in ("container_number", "container_no", "cntr_number", "container"):
        candidate = _clean(fields.get(key))
        if _is_valid_container_candidate(candidate) and _norm(candidate) == value_norm:
            return key
    return ""


def _field_source_allows_container_candidate(analysis: dict[str, Any], value: Any) -> bool:
    field_sources = analysis.get("field_sources") if isinstance(analysis.get("field_sources"), dict) else {}
    if not field_sources:
        return False
    raw_fields = analysis.get("extracted_fields")
    fields: dict[str, Any] = raw_fields if isinstance(raw_fields, dict) else {}
    candidate_key = _container_candidate_field_name(fields, value) or "container_number"
    source_meta = field_sources.get(candidate_key)
    if not isinstance(source_meta, dict):
        return False
    provenance = " ".join(
        _clean(source_meta.get(part))
        for part in ("label", "source", "raw_context")
        if _clean(source_meta.get(part))
    )
    if not provenance:
        return False
    has_container_context = bool(re.search(r"\b(?:container|cntr|cont\.?\s*no\.?|container\s*no\.?)\b", provenance, re.IGNORECASE))
    has_wrong_context = bool(re.search(r"\b(?:booking|invoice|customer|reference|date|vessel|voyage)\b", provenance, re.IGNORECASE))
    return has_container_context and not has_wrong_context


def _contains_reference(haystack: dict[str, Any], value: Any) -> bool:
    needle = _norm(value)
    if not needle:
        return False
    blob = json.dumps(haystack, ensure_ascii=False)
    return needle in _norm(blob)


def _uploaded_analysis_doc_type(event_doc_type: Any, uploaded: dict[str, Any]) -> str:
    raw_analysis = uploaded.get("analysis")
    analysis: dict[str, Any] = raw_analysis if isinstance(raw_analysis, dict) else {}
    raw_fields = analysis.get("extracted_fields")
    fields: dict[str, Any] = raw_fields if isinstance(raw_fields, dict) else {}
    transport_order_signals = [
        fields.get("document_type"),
        uploaded.get("filename"),
        analysis.get("filename"),
    ]
    if any(re.search(r"transportauftrag|transport\s+order", str(value or ""), re.IGNORECASE) for value in transport_order_signals):
        return "booking_confirmation"
    strong_offer_signals = [
        fields.get("document_type"),
        uploaded.get("filename"),
        analysis.get("filename"),
    ]
    if any(normalize_document_type(value) == "offer" for value in strong_offer_signals):
        return "offer"
    candidates: list[Any] = [
        analysis.get("doc_type"),
        uploaded.get("analysis_doc_type"),
        uploaded.get("doc_type"),
    ]
    suggested = analysis.get("suggested_registry_types")
    if isinstance(suggested, list):
        candidates.extend(suggested)
    candidates.extend([
        fields.get("document_type"),
        uploaded.get("filename"),
        analysis.get("filename"),
        event_doc_type,
    ])
    for candidate in candidates:
        normalized = normalize_document_type(candidate)
        if normalized and normalized != "unknown" and get_document_profile(normalized).document_type != "internal_misc":
            return normalized
    return normalize_document_type(event_doc_type)


def _relevant_fields_for_uploaded_document(event_doc_type: Any, uploaded: dict[str, Any]) -> set[str]:
    doc_type = _uploaded_analysis_doc_type(event_doc_type, uploaded)
    profile = get_document_profile(doc_type)
    return {str(field) for field in profile.relevant_fields}


def _build_document_evidence_lines(report: dict[str, Any], filename: str, limit: int = 6) -> list[str]:
    uploaded = _select_uploaded_analysis(report, filename)
    analysis = uploaded.get("analysis") if isinstance(uploaded.get("analysis"), dict) else {}
    fields = analysis.get("extracted_fields") if isinstance(analysis.get("extracted_fields"), dict) else {}
    if not fields:
        return []
    metadata = (report.get("trigger_event") or {}).get("metadata") if isinstance(report.get("trigger_event"), dict) else {}
    event_doc_type = metadata.get("document_type") if isinstance(metadata, dict) else None
    relevant = _relevant_fields_for_uploaded_document(event_doc_type, uploaded)
    doc_type = _uploaded_analysis_doc_type(event_doc_type, uploaded)
    preferred = (
        ("document_number", "amount", "currency", "customer", "incoterm_named_place", "shipment_number", "pieces", "goods_description", "gross_weight", "volume")
        if doc_type == "offer"
        else ("invoice_number", "document_number", "goods_value", "amount", "currency", "mrn", "container_number", "shipment_number", "customer_reference", "etd", "eta", "atd", "ata", "gross_weight", "volume", "incoterm_named_place", "pieces", "goods_description")
    )
    labels = {
        "invoice_number": "Rechnungsnr.",
        "document_number": "Dokumentnr.",
        "amount": "Betrag",
        "goods_value": "Warenwert",
        "currency": "Währung",
        "mrn": "MRN",
        "container_number": "Container",
        "shipment_number": "Sendung",
        "customer_reference": "Kundenref.",
        "etd": "ETD",
        "eta": "ETA",
        "atd": "ATD",
        "ata": "ATA",
        "gross_weight": "Gewicht",
        "volume": "Volumen",
        "customer": "Kunde",
        "incoterm_named_place": "Incoterm",
        "pieces": "Packstücke",
        "goods_description": "Ware",
    }
    lines: list[str] = []
    for key in preferred:
        value = _clean(fields.get(key))
        if not value or key not in relevant:
            continue
        if key == "currency" and (_clean(fields.get("amount")) or _clean(fields.get("goods_value"))):
            continue
        if key in {"amount", "goods_value"} and _clean(fields.get("currency")):
            value = f"{value} {fields.get('currency')}"
        lines.append(f"{labels.get(key, key)} {value}")
        if len(lines) >= limit:
            break
    return lines


def _build_document_field_comparison(report: dict[str, Any], filename: str) -> list[dict[str, str]]:
    uploaded = _select_uploaded_analysis(report, filename)
    analysis = uploaded.get("analysis") if isinstance(uploaded.get("analysis"), dict) else {}
    fields = analysis.get("extracted_fields") if isinstance(analysis.get("extracted_fields"), dict) else {}
    metadata = (report.get("trigger_event") or {}).get("metadata") if isinstance(report.get("trigger_event"), dict) else {}
    event_doc_type = metadata.get("document_type") if isinstance(metadata, dict) else None
    relevant_fields = _relevant_fields_for_uploaded_document(event_doc_type, uploaded)
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
    mbl_doc = _select_explicit_mbl_candidate(fields)
    container_doc = _clean(fields.get("container_number"))
    if not container_doc:
        container_doc = next((str(ref).strip() for ref in refs if _is_valid_container_candidate(ref)), "")
    vessel_doc = "EVER GREET" if _contains_reference(analysis, "EVER GREET") else ""

    comparisons: list[dict[str, str]] = []

    def add(label: str, tms: Any, doc: Any, *, match: bool | None = None, target: str = "", field: str = "") -> None:
        if field and field not in relevant_fields:
            return
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

    add("POL", freight.get("pol_code") or main_leg.get("origin"), fields.get("pol"), match=_same_location(freight.get("pol_code") or main_leg.get("origin"), fields.get("pol")), field="pol")
    add("POD", freight.get("pod_code") or main_leg.get("destination"), fields.get("pod"), match=_same_location(freight.get("pod_code") or main_leg.get("destination"), fields.get("pod")), field="pod")
    add("ETD", _date_from_ms(main_leg.get("etd")) or _date_from_ms((detail.get("milestones") or {}).get("etd_main_carriage")), fields.get("etd"), target="etd_main_carriage", field="etd")
    add("ETA", dates.get("estimated_delivery_date") or _date_from_ms((detail.get("milestones") or {}).get("eta_main_carriage")), fields.get("eta"), target="estimated_delivery_date", field="eta")
    add("ATD", _date_from_ms(main_leg.get("atd")) or _date_from_ms((detail.get("milestones") or {}).get("atd_main_carriage")), fields.get("atd"), target="atd_main_carriage", field="atd")
    add("ATA", dates.get("actual_delivery_date") or _date_from_ms((detail.get("milestones") or {}).get("ata_main_carriage")), fields.get("ata"), target="actual_delivery_date", field="ata")
    add("MBL / B/L-Nr.", freight.get("mbl_number") or freight.get("bl_number"), mbl_doc, target="mbl_number", field="mbl_number")
    add("Container", freight.get("container_number"), container_doc, target="container_number", field="container_number")
    if vessel_doc or main_leg.get("carrier") or main_leg.get("vessel_name"):
        add("Schiff", main_leg.get("vessel_name") or main_leg.get("carrier"), vessel_doc, target="Vessel/Hauptlauf", field="vessel")
    pieces_doc = _clean(fields.get("pieces") or fields.get("quantity") or fields.get("total_pieces"))
    if pieces_doc:
        pieces_tms, pieces_source = _tms_cargo_reference(detail, totals, "pieces")
        status = _numeric_comparison_status(pieces_tms, pieces_doc)
        add("Packstücke", pieces_tms, pieces_doc, match=(status == "match") if status else None, target="cargo_pieces", field="pieces")
        if comparisons and comparisons[-1].get("label") == "Packstücke" and pieces_source:
            comparisons[-1]["source"] = pieces_source
    weight_doc = _clean(fields.get("weight_kg") or fields.get("total_weight_kg") or fields.get("gross_weight_kg") or fields.get("gross_weight"))
    if weight_doc:
        weight_tms, weight_source = _tms_cargo_reference(detail, totals, "weight")
        status = _numeric_comparison_status(weight_tms, weight_doc, tolerance=1.0)
        add("Gewicht", weight_tms, weight_doc, match=(status == "match") if status in {"match", "diff"} else None, target="cargo_weight_kg", field="gross_weight")
        if comparisons and comparisons[-1].get("label") == "Gewicht":
            if status == "near_match":
                comparisons[-1]["status"] = "near_match"
            if weight_source:
                comparisons[-1]["source"] = weight_source
            cargo_rows = _cargo_rows_from_detail(detail)
            total_weight_values = [
                _number_from_value(row.get("total_weight_kg"))
                for row in cargo_rows
                if _number_from_value(row.get("total_weight_kg")) is not None
            ]
            unit_weight = _number_from_value(weight_tms)
            if len(cargo_rows) == 1 and total_weight_values and unit_weight is not None and total_weight_values[0] > unit_weight * 5:
                comparisons[-1]["note"] = f"TMS-Cargo total_weight_kg wirkt rechnerisch auffällig ({_format_number(total_weight_values[0])} kg)"
    return comparisons


def _comparison_lines(comparisons: list[dict[str, str]], statuses: set[str], limit: int = 3) -> list[str]:
    labels = {"match": "passt", "near_match": "nahezu passend", "diff": "abweicht", "missing_tms": "fehlt im TMS", "missing_doc": "nicht auf dem Dokument"}
    lines = []
    for row in comparisons:
        if row.get("status") not in statuses:
            continue
        status = row.get("status", "")
        label = row.get("label") or "Feld"
        tms = row.get("tms") or "nicht gepflegt"
        doc = row.get("doc") or "nicht lesbar"
        note = f"; {row.get('note')}" if row.get("note") else ""
        source = f" ({row.get('source')})" if row.get("source") and status in {"match", "near_match", "diff"} else ""
        if status == "match":
            lines.append(f"{label} passt: TMS{source} {tms} = Dokument {doc}{note}")
        elif status == "near_match":
            lines.append(f"{label} nahezu passend: TMS{source} {tms}, Dokument {doc}; bitte Rundung/Quelle prüfen{note}")
        elif status == "missing_tms":
            target = f" ({row.get('target')})" if row.get("target") else ""
            lines.append(f"{label} fehlt im TMS{target}: Dokument {doc}")
        elif status == "missing_doc":
            lines.append(f"{label} nicht beurteilbar: TMS {tms}, im Dokument nicht lesbar/angegeben")
        else:
            lines.append(f"{label} weicht ab: TMS{source} {tms}, Dokument {doc}{note}")
        if len(lines) >= limit:
            break
    return lines


def _priority_rank(value: Any) -> int:
    return {"low": 0, "medium": 1, "high": 2, "critical": 3}.get(str(value or "low").strip().lower(), 0)


def _priority_max(*values: Any) -> str:
    labels = ["low", "medium", "high", "critical"]
    rank = max((_priority_rank(value) for value in values), default=0)
    return labels[min(rank, len(labels) - 1)]


def _document_finding_requires_operator_review(row: Any) -> bool:
    """Return True only for findings that are actionable for the current upload.

    The reconciliation report can carry case-level risk from older/other
    documents.  Document activity notifications must not convert those stale or
    inventory-style notes into a high-priority manual-review instruction for a
    newly uploaded document whose concrete fields match TMS.
    """
    if not isinstance(row, dict):
        return False
    severity = str(row.get("severity") or "").strip().lower()
    finding_type = str(row.get("type") or "").strip().lower()
    summary = str(row.get("summary") or "").strip().lower()
    code = str(row.get("code") or "").strip().lower()
    if severity in {"blocker", "high", "critical"} or "blocker" in finding_type:
        return True
    if finding_type in {"mrn_mismatch", "tms_document_weight_mismatch", "tms_document_piece_mismatch", "document_unreadable", "local_file_missing"}:
        return True
    if code in {"proforma_invoice", "zero_value"}:
        return True
    # Observations that are useful to show, but should not by themselves create a
    # generic manual-review task when the new upload otherwise matches TMS.
    if any(token in summary for token in ("multi-po", "multi po", "mehrere po", "fehlende dokumentnummer", "missing document number", "dokumentnummer fehlt")):
        return False
    if finding_type in {"document_flag", "document_open_question"} and severity in {"low", "medium", ""}:
        return False
    return bool(row)


def _current_upload_review_floor(
    *,
    case_risk: Any,
    case_needs_review: bool,
    findings: list[Any],
    comparisons: list[dict[str, str]],
    document_review_intents: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    actionable_findings = [row for row in findings if _document_finding_requires_operator_review(row)]
    mismatch_rows = [row for row in comparisons if row.get("status") in {"diff", "missing_tms"}]
    intents = document_review_intents or []
    if _has_blocker_finding(actionable_findings) or any(isinstance(row, dict) and str(row.get("severity") or "").lower() in {"high", "critical"} for row in actionable_findings):
        priority = "high"
    elif actionable_findings or mismatch_rows or intents:
        priority = "medium"
    else:
        priority = "low"
    return {
        "priority": priority,
        "needs_review": bool(actionable_findings or mismatch_rows or intents),
        "actionable_finding_count": len(actionable_findings),
        "mismatch_count": len(mismatch_rows),
        "review_intent_count": len(intents),
        "case_risk": str(case_risk or "low").strip().lower() or "low",
        "case_needs_review": bool(case_needs_review),
        "case_risk_not_used_for_document_priority": bool((str(case_risk or "low").strip().lower() in {"medium", "high", "critical"} or case_needs_review) and not (actionable_findings or mismatch_rows or intents)),
    }


def _build_document_agent_evidence_packet(
    *,
    report: dict[str, Any],
    event: dict[str, Any],
    filename: str,
    doc_type: Any,
    context: dict[str, Any],
    findings: list[Any],
    needs_review: bool,
    priority: str,
    comparisons: list[dict[str, str]],
    evidence_lines: list[str],
    document_review_intents: list[dict[str, Any]],
    case_risk: str = "low",
    case_needs_review: bool = False,
    current_upload_review: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
    uploaded = _select_uploaded_analysis(report, filename)
    analysis = uploaded.get("analysis") if isinstance(uploaded.get("analysis"), dict) else {}
    effective_doc_type = _uploaded_analysis_doc_type(doc_type, uploaded)
    profile = get_document_profile(effective_doc_type)
    return {
        "contract": "agent_first_document_review_v1",
        "language": "de",
        "operator_surface": "microsoft_teams_internal_cargolo",
        "task": "Bewerte den neuen TMS-Dokument-Upload wie ein interner ASR-Mitarbeiter. Nutze nur diese Evidenz; rate nicht. Keine TMS-Änderung und keine Kundenkommunikation auslösen.",
        "focus_rules": {
            "primary_focus": "new_upload",
            "stale_findings_must_not_dominate": "Erwähne ältere/andere Dokument-Findings nur, wenn sie laut Evidenz ein aktueller Blocker für genau diesen Upload sind.",
            "if_uploaded_document_unreadable": "Nicht aus anderen Belegen operative Abweichungen hochziehen; kurz sagen, dass aus diesem Upload keine belastbare neue Aktion folgt.",
            "separate_actionable_from_observe": "Nur konkrete Feldkonflikte, Blocker oder sichere Review-Intents als Handlung darstellen; reine Inventar-/Kontextinfos als Beobachten einstufen.",
        },
        "operator_quality_rubric": {
            "style": "internal_forwarder_colleague",
            "max_top_items": 3,
            "avoid_generic_manual_review": True,
            "must_include_operational_recommendation": True,
            "must_state_no_side_effects": True,
        },
        "order_id": report.get("order_id"),
        "document": {
            "filename": filename,
            "event_document_type": metadata.get("document_type"),
            "effective_document_type": effective_doc_type,
            "label": _doc_type_label(effective_doc_type),
            "profile_relevant_fields": sorted(str(field) for field in profile.relevant_fields),
            "extracted_fields": analysis.get("extracted_fields") if isinstance(analysis.get("extracted_fields"), dict) else {},
            "field_sources": analysis.get("field_sources") if isinstance(analysis.get("field_sources"), dict) else {},
            "summary": analysis.get("summary"),
        },
        "upload": {
            "activity_id": _activity_id(event),
            "changed_at": event.get("changed_at"),
            "changed_by": event.get("changed_by_name") or event.get("changed_by"),
            "metadata": metadata,
        },
        "shipment_context": context,
        "lifecycle": {
            "tms_snapshot_path": lifecycle.get("tms_snapshot_path"),
            "document_registry_path": lifecycle.get("document_registry_path"),
            "history_sync_count": lifecycle.get("history_sync_count", 0),
            "history_sync_error": lifecycle.get("history_sync_error"),
            "last_email_at": lifecycle.get("last_email_at"),
        },
        "deterministic_evidence": {
            "priority_floor": priority,
            "needs_review_floor": needs_review,
            "case_reconciliation_risk": case_risk,
            "case_needs_human_review": case_needs_review,
            "current_upload_review": current_upload_review or {},
            "field_comparisons": comparisons,
            "readable_evidence_lines": evidence_lines,
            "findings": findings,
            "safe_tms_review_intents": document_review_intents,
        },
        "guardrails": {
            "writes_allowed": False,
            "customer_messages_allowed": False,
            "safe_writeback_targets": ["mbl_number", "hbl_number", "hawb_number", "container_number", "customs_reference", "estimated_delivery_date", "actual_delivery_date"],
            "review_only_targets": ["etd_main_carriage", "atd_main_carriage"],
            "agent_may_raise_priority": True,
            "agent_may_not_hide_safe_review_intents": True,
            "agent_may_not_downgrade_blockers": True,
        },
        "expected_agent_json": {
            "sections": {"lage": "...", "abgleich": "...", "auffaellig": "...", "empfehlung": "...", "naechster_schritt": "..."},
            "decision": "no_action|observe|manual_review|queue_review_card",
            "priority": "low|medium|high|critical",
            "needs_review": "boolean",
            "confidence": "low|medium|high",
        },
    }


def _sanitize_agent_section(value: Any, fallback: str = "-") -> str:
    text = " ".join(str(value or "").replace("\r", " ").split())
    if not text:
        return fallback
    blocked = ("ASRCTX", "report_json_path", "document_registry_path", "tms_snapshot_path", "/root/.hermes")
    for token in blocked:
        text = text.replace(token, "")
    return text[:420].strip() or fallback


def _parse_document_agent_review(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return None
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    if not isinstance(raw, dict):
        return None
    sections = raw.get("sections") if isinstance(raw.get("sections"), dict) else raw
    required = ("lage", "abgleich", "auffaellig", "empfehlung", "naechster_schritt")
    if not all(_clean(sections.get(key)) for key in required):
        return None
    priority = str(raw.get("priority") or "").strip().lower()
    if priority not in {"low", "medium", "high", "critical"}:
        priority = "medium"
    decision = str(raw.get("decision") or "manual_review").strip().lower()
    if decision not in {"no_action", "observe", "manual_review", "queue_review_card"}:
        decision = "manual_review"
    needs_review_raw = raw.get("needs_review")
    if isinstance(needs_review_raw, bool):
        needs_review = needs_review_raw
    elif isinstance(needs_review_raw, str) and needs_review_raw.strip().lower() in {"true", "false"}:
        needs_review = needs_review_raw.strip().lower() == "true"
    else:
        needs_review = decision in {"manual_review", "queue_review_card"}
    return {
        "mode": "external_agent",
        "sections": {key: _sanitize_agent_section(sections.get(key)) for key in required},
        "decision": decision,
        "priority": priority,
        "needs_review": needs_review,
        "confidence": str(raw.get("confidence") or "medium").strip().lower(),
        "raw": raw,
    }


def _run_document_agent_review(packet: dict[str, Any]) -> dict[str, Any] | None:
    """Optional agent-review hook.

    The monitor itself stays safe and deterministic for evidence gathering.  If
    `HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_CMD` is configured, the command gets
    the evidence packet on stdin and must return a small JSON review.  This lets
    production wire the packet to a Hermes/LLM worker without letting it perform
    writes; invalid/slow responses fall back to the local guardrailed message.
    """
    command = os.environ.get("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_CMD", "").strip()
    if not command:
        return None
    try:
        timeout = float(os.environ.get("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_TIMEOUT", "90") or 90)
    except Exception:
        timeout = 90.0
    try:
        completed = subprocess.run(
            shlex.split(command),
            input=json.dumps(packet, ensure_ascii=False),
            text=True,
            capture_output=True,
            timeout=max(1.0, timeout),
            check=False,
        )
    except Exception:
        return None
    if completed.returncode != 0:
        return None
    return _parse_document_agent_review(completed.stdout)


def _document_agent_review_text(review: dict[str, Any]) -> str:
    sections = review.get("sections") if isinstance(review.get("sections"), dict) else {}
    parts = [str(review.get("decision") or ""), str(review.get("priority") or "")]
    parts.extend(str(sections.get(key) or "") for key in ("lage", "abgleich", "auffaellig", "empfehlung", "naechster_schritt"))
    return " ".join(parts).lower()


def _document_agent_review_is_quality_safe(packet: dict[str, Any], review: dict[str, Any]) -> bool:
    """Keep the LLM as decision layer, but reject ungrounded stale/generic reviews.

    This is a guardrail, not routing logic: the agent may decide, prioritize and
    word the employee answer, but it may not invent stale issues or escalate an
    unreadable/empty upload with generic "manual review" language.
    """
    if not isinstance(review, dict):
        return False
    text = _document_agent_review_text(review)
    packet_text = json.dumps(packet, ensure_ascii=False).lower()
    for token in ("russland", "sanktion", "896 kg", "508 kg", "896", "508"):
        if token in text and token not in packet_text:
            return False
    raw_evidence = packet.get("deterministic_evidence")
    evidence: dict[str, Any] = raw_evidence if isinstance(raw_evidence, dict) else {}
    current_upload_review = evidence.get("current_upload_review") if isinstance(evidence.get("current_upload_review"), dict) else {}
    no_current_upload_action = not any(
        int(current_upload_review.get(key) or 0) > 0
        for key in ("actionable_finding_count", "mismatch_count", "review_intent_count")
    )
    agent_escalates = (
        str(review.get("decision") or "").strip().lower() in {"manual_review", "queue_review_card"}
        or str(review.get("priority") or "").strip().lower() in {"high", "critical"}
        or bool(review.get("needs_review"))
    )
    if no_current_upload_action and agent_escalates:
        return False
    no_actionable_evidence = not any(
        evidence.get(key)
        for key in ("field_comparisons", "readable_evidence_lines", "findings", "safe_tms_review_intents")
    )
    generic_manual_review = (
        str(review.get("decision") or "").strip().lower() == "manual_review"
        or bool(review.get("needs_review"))
    ) and any(token in text for token in ("manuell prüfen", "manuell pruefen", "fachlich/manuell", "gegenprüfen", "gegenpruefen"))
    if no_actionable_evidence and generic_manual_review:
        return False
    return True


def _message_from_agent_review(order_id: Any, review: dict[str, Any]) -> str:
    sections = review.get("sections") if isinstance(review.get("sections"), dict) else {}
    return "\n".join([
        f"Lage: {order_id} · {_sanitize_agent_section(sections.get('lage'))}",
        f"Abgleich: {_sanitize_agent_section(sections.get('abgleich'))}",
        f"Auffällig: {_sanitize_agent_section(sections.get('auffaellig'))}",
        f"Empfehlung: {_sanitize_agent_section(sections.get('empfehlung'))}",
        f"Nächster Schritt: {_sanitize_agent_section(sections.get('naechster_schritt'))}",
    ])


def _human_document_message(
    *,
    order_id: Any,
    filename: Any,
    doc_type: Any,
    context: dict[str, Any],
    findings: list[Any],
    needs_review: bool,
    comparisons: list[dict[str, str]] | None = None,
    evidence_lines: list[str] | None = None,
    uploaded_by: Any = None,
    uploaded_at: Any = None,
) -> str:
    normalized_doc_type = normalize_document_type(doc_type)
    label = _doc_type_label(doc_type)
    route = _route_hint(context)
    status = str(context.get("status") or "").strip()
    network = str(context.get("network") or context.get("mode") or "").strip()
    customer_ref = str(context.get("customer_reference") or "").strip()
    context_bits = [bit for bit in (network, status, route, f"Kundenref. {customer_ref}" if customer_ref else "") if bit]
    lage = f"Neuer Beleg geprüft: {label}. Ich habe ihn mit dem aktuellen TMS-Stand abgeglichen."
    if normalized_doc_type == "offer":
        lage = "Angebot geprüft. Ich habe Angebotsdaten und TMS-Kontext abgeglichen; daraus wurde keine automatische TMS-Korrektur abgeleitet."
    uploader = str(uploaded_by or "").strip()
    upload_time = _display_timestamp(uploaded_at)
    if uploader and uploader != "-":
        lage += f" Upload laut TMS: {uploader}"
        if upload_time and upload_time != "-":
            lage += f" · {upload_time}"
        lage += "."
    if context_bits:
        lage += " Kontext: " + " · ".join(context_bits[:3]) + "."

    comparisons = comparisons or []
    matching = _comparison_lines(comparisons, {"match", "near_match"}, limit=4)
    problems = _comparison_lines(comparisons, {"diff", "missing_tms"}, limit=4)
    unknown = _comparison_lines(comparisons, {"missing_doc"}, limit=2)

    evidence_lines = evidence_lines or []
    if matching:
        abgleich = " | ".join(matching)
        if evidence_lines:
            abgleich += " | Gelesen: " + " | ".join(evidence_lines[:5])
    elif evidence_lines:
        abgleich = "Aus dem Beleg sicher gelesen: " + " | ".join(evidence_lines[:6]) + ". Keine direkte TMS-Korrektur daraus abgeleitet."
        if normalized_doc_type == "offer":
            abgleich = "Angebotsdaten: " + " | ".join(evidence_lines[:7]) + ". Kein direkt schreibbares TMS-Feld daraus abgeleitet."
    else:
        abgleich = "Noch kein sicherer Feldtreffer aus dem lesbaren Dokumenttext. Ich werte das deshalb nicht als TMS-Bestätigung."

    uploaded_norm = str(filename or "").strip().lower()
    blocker_findings = [
        row for row in findings
        if isinstance(row, dict)
        and (str(row.get("filename") or "").strip().lower() == uploaded_norm or _finding_rank(row)[0] <= 0)
        and _finding_rank(row)[0] <= 0
    ]
    blocker_lines = [_finding_text(row) for row in sorted(blocker_findings, key=_finding_rank)[:2]]
    uploaded_findings = [
        row for row in findings
        if isinstance(row, dict) and str(row.get("filename") or "").strip().lower() == uploaded_norm
    ]
    review_topic_lines: list[str] = []
    if not comparisons:
        blocker_lines.extend(_finding_text(row) for row in sorted(uploaded_findings, key=_finding_rank)[:3])
    elif not problems:
        # Keep the card focused on the new upload, but do not suppress concrete
        # non-write review topics such as uncertain customs value currency just
        # because pieces/weight matched.
        review_topics = [
            row for row in uploaded_findings
            if _finding_rank(row)[0] <= 3
            and re.search(r"währung|currency|warenwert|goods value|zollwert|customs value", str(row.get("summary") or ""), re.IGNORECASE)
        ]
        seen_topic_lines: set[str] = set()
        for row in sorted(review_topics, key=_finding_rank):
            line = _finding_text(row)
            key = _norm(line)
            if key and key not in seen_topic_lines:
                seen_topic_lines.add(key)
                review_topic_lines.append(line)
            if len(review_topic_lines) >= 2:
                break
        blocker_lines.extend(review_topic_lines)
    auffaellig_items = problems + blocker_lines
    if auffaellig_items:
        auffaellig = " | ".join(auffaellig_items[:4])
        supported_card_targets = {"mbl_number", "container_number", "hbl_number", "hawb_number", "customs_reference", "estimated_delivery_date", "actual_delivery_date", "etd_main_carriage", "atd_main_carriage"}
        card_targets = [
            row.get("target") or row.get("label")
            for row in comparisons
            if row.get("status") in {"diff", "missing_tms"} and str(row.get("target") or "") in supported_card_targets
        ]
        if card_targets:
            empfehlung = "Ich sehe einen konkreten TMS-Feldwert aus belastbarer Dokument-Evidenz. Bitte die Freigabe-Kachel bestätigen oder ablehnen; vorher wird im TMS nichts geändert."
            targets = card_targets
        else:
            if review_topic_lines and not problems:
                empfehlung = "Währung/Warenwert fachlich für Zollwert prüfen; Hermes hat keine TMS-Änderung und keinen Kundenkontakt ausgelöst."
                targets = []
            else:
                empfehlung = "Ich sehe eine fachliche Abweichung, aber keinen sicheren direkt schreibbaren TMS-Feldwert. Bitte operativ prüfen; Hermes hat nichts geändert."
                if normalized_doc_type == "offer":
                    empfehlung = "Angebot fachlich gegen TMS/ASR-Angebot prüfen; keine automatische TMS-Korrektur oder Kundenkommunikation ableiten."
                targets = [row.get("target") or row.get("label") for row in comparisons if row.get("status") in {"diff", "missing_tms"}]
        target_text = ", ".join(str(x) for x in targets[:3] if x)
        if review_topic_lines and not problems and not target_text:
            naechster_schritt = "Zollwert/Währung bei der Zollprüfung bestätigen; keine automatische TMS-Korrektur nötig."
        else:
            naechster_schritt = f"Zu klären/korrigieren: {target_text}." if target_text else "Führenden Wert festlegen; bei TMS-Korrektur anschließend bewusst freigeben."
    else:
        suffix = (" Nicht beurteilbar: " + " | ".join(unknown)) if unknown else ""
        auffaellig = "Keine belastbare Abweichung aus den lesbaren Feldern." + suffix
        empfehlung = "Keine TMS-Korrektur aus diesem Beleg ableiten. Nur bei Bedarf die nicht lesbaren Felder manuell nachsehen."
        if normalized_doc_type == "offer":
            empfehlung = "Keine TMS-Korrektur aus dem Angebot ableiten. Angebotswert/Scope nur fachlich gegen ASR-Angebot und Pricing-Kontext plausibilisieren."
        naechster_schritt = "Weiter beobachten; erst bei echtem Feldkonflikt oder fehlendem TMS-Wert freigeben."
    if needs_review and not comparisons and not blocker_lines:
        auffaellig = "Dokument ist nicht vollständig automatisch belastbar; manueller Feldabgleich sinnvoll."

    return "\n".join([
        f"Lage: {order_id} · {lage}",
        f"Abgleich: {abgleich}",
        f"Auffällig: {auffaellig}",
        f"Empfehlung: {empfehlung}",
        f"Nächster Schritt: {naechster_schritt}",
    ])


def _existing_pending_review_card(root: Path, order_id: str, target: str, value: str) -> dict[str, Any] | None:
    normalized_order_id = str(order_id or "").strip().upper()
    if not re.fullmatch(r"(?:AN|BU)-[A-Z0-9_-]+", normalized_order_id):
        return None
    queue_path = root / "orders" / normalized_order_id / "teams" / "pending_tms_actions.jsonl"
    if not queue_path.exists():
        return None
    try:
        lines = queue_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    normalized_value = str(value or "").strip().upper()
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "") != "pending_review":
            continue
        if str(row.get("order_id") or "").strip().upper() != normalized_order_id:
            continue
        if str(row.get("target") or "").strip() == target and str(row.get("value") or "").strip().upper() == normalized_value:
            return row
    return None


def _duplicate_review_card_summary(card: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
    target = str(card.get("target") or "").strip()
    value = str(card.get("value") or "").strip()
    return {
        **card,
        "action_id": None,
        "existing_action_id": existing.get("action_id"),
        "duplicate": True,
        "summary": f"Review-Kachel existiert bereits: {target} {value}; keine zweite Kachel erstellt.",
    }


def _queue_document_review_card_results(
    *,
    storage_root: Path | None,
    order_id: str,
    intents: list[dict[str, Any]],
    event: dict[str, Any],
    max_cards: int = 3,
) -> dict[str, list[dict[str, Any]]]:
    """Queue safe document-derived TMS proposals and report created vs duplicate cards.

    Duplicates are not errors and must be visible to the operator-facing result:
    an already-open pending_review card means the value was recognised again, but
    no second Teams/TMS approval card should be created.
    """
    result: dict[str, list[dict[str, Any]]] = {"created": [], "duplicates": []}
    if not intents:
        return result
    try:
        from .teams_reply_loop import record_agent_tms_update_intent
    except Exception:
        return result
    root = CaseStore(storage_root).root
    normalized_order = str(order_id or "").strip().upper()
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    fallback_doc_label = _doc_type_label(metadata.get("document_type") or "Dokument")
    write_supported_targets = {"mbl_number", "container_number", "hbl_number", "hawb_number", "customs_reference", "estimated_delivery_date", "actual_delivery_date"}
    activity_id = _activity_id(event)
    for intent in intents:
        if not isinstance(intent, dict):
            continue
        target = str(intent.get("target") or "").strip()
        value = str(intent.get("value") or intent.get("document_value") or "").strip()
        previous = str(intent.get("current_tms_value") or intent.get("previous_value") or "nicht gepflegt").strip()
        label = str(intent.get("label") or target).strip()
        guardrails = intent.get("guardrails") if isinstance(intent.get("guardrails"), dict) else {}
        doc_label = _doc_type_label(guardrails.get("effective_document_type") or fallback_doc_label)
        context_id = str(intent.get("context_id") or f"{normalized_order}:{activity_id}:document_monitor").strip()
        text = (
            f"{label} bei {normalized_order}: Dokumentwert {value}; "
            f"TMS aktuell {previous}. Quelle: {doc_label}, Activity {activity_id or '-'}; "
            "keine automatische TMS-Änderung ohne Teams-Freigabe."
        )
        card = {
            "order_id": normalized_order,
            "action_id": None,
            "target": target,
            "value": value,
            "previous_value": previous,
            "operator": "Hermes Dokumentmonitor",
            "context_id": context_id,
            "source": "document_activity_monitor",
            "evidence": {"source": doc_label, "previous_value": previous, "document_value": value, "summary": text},
            "write_supported": target in write_supported_targets,
            "question": f"{label}: Dokumentwert {value} ins TMS übernehmen?" if target in write_supported_targets else f"{label}: Dokumentwert {value} fachlich bestätigen?",
        }
        existing_duplicate = _existing_pending_review_card(root, normalized_order, target, value)
        if existing_duplicate:
            result["duplicates"].append(_duplicate_review_card_summary(card, existing_duplicate))
            continue
        if len(result["created"]) >= max_cards:
            continue
        queued = record_agent_tms_update_intent(
            root=root,
            order_id=normalized_order,
            target=target,
            value=value,
            text=text,
            operator="Hermes Dokumentmonitor",
            source_message_id=str(activity_id) if activity_id else None,
            context_id=context_id,
            confidence=str(intent.get("confidence") or "document_field_comparison"),
            source="document_activity_monitor",
            evidence={
                "source": doc_label,
                "summary": text,
                "previous_value": previous,
                "document_value": value,
                "activity_id": activity_id,
                "guardrails": guardrails,
            },
            previous_value=previous,
            write_supported=target in write_supported_targets,
        )
        if queued.get("queued"):
            card["action_id"] = queued.get("action_id")
            result["created"].append(card)
        elif queued.get("duplicate"):
            duplicate_existing = {"action_id": queued.get("action_id")}
            result["duplicates"].append(_duplicate_review_card_summary(card, duplicate_existing))
    return result


def _queue_document_review_cards(
    *,
    storage_root: Path | None,
    order_id: str,
    intents: list[dict[str, Any]],
    event: dict[str, Any],
    max_cards: int = 3,
) -> list[dict[str, Any]]:
    """Queue safe document-derived TMS proposals as Teams approval cards."""
    return _queue_document_review_card_results(
        storage_root=storage_root,
        order_id=order_id,
        intents=intents,
        event=event,
        max_cards=max_cards,
    )["created"]


def _section_from_message(message: str, label: str) -> str:
    pattern = rf"(?:^|\n){re.escape(label)}:\s*(.*?)(?=\n(?:Lage|Abgleich|Auffällig|Empfehlung|Nächster Schritt):|\Z)"
    match = re.search(pattern, message or "", flags=re.S)
    return match.group(1).strip() if match else ""


def _case_root_from_report(report: dict[str, Any], order_id: str) -> Path | None:
    del order_id
    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
    for value in (lifecycle.get("case_root"), report.get("case_root")):
        if value:
            return Path(str(value))
    return None


def _build_tms_update_review_intents_from_comparisons(
    *,
    report: dict[str, Any],
    event: dict[str, Any],
    comparisons: list[dict[str, str]],
    findings: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Build evidence-only TMS update review intents from document comparisons.

    The monitor is deliberately agent-first: deterministic code prepares guarded
    evidence and never writes TMS fields. If the monitor later turns an intent into
    a Teams card, the card still requires explicit Hermes/operator approval before
    any write can happen.
    """
    order_id = str(report.get("order_id") or "").strip().upper()
    if not order_id:
        return []
    activity_id = _activity_id(event)
    context_id = f"{order_id}:{activity_id}:document_monitor" if activity_id else f"{order_id}:document_monitor"
    supported_targets = {
        "mbl_number",
        "container_number",
        "hbl_number",
        "hawb_number",
        "customs_reference",
        "estimated_delivery_date",
        "actual_delivery_date",
        "etd_main_carriage",
        "atd_main_carriage",
        "cargo_weight_kg",
        "cargo_pieces",
    }
    write_supported_targets = {"mbl_number", "container_number", "hbl_number", "hawb_number", "customs_reference", "estimated_delivery_date", "actual_delivery_date"}
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    filename = metadata.get("file_name") or metadata.get("filename") or event.get("field_name")
    doc_type = metadata.get("document_type") or ""
    uploaded = _select_uploaded_analysis(report, str(filename))
    if _has_uploaded_document_blocker_finding(findings or [], filename):
        return []
    intents: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in comparisons:
        if row.get("status") not in {"diff", "missing_tms"}:
            continue
        target = str(row.get("target") or "").strip()
        value = str(row.get("doc") or "").strip()
        if target in {"estimated_delivery_date", "actual_delivery_date", "etd_main_carriage", "atd_main_carriage"}:
            value = _normalize_date_value(value)
        if target not in supported_targets or not value or value == "nicht lesbar":
            continue
        effective_doc_type = _effective_trusted_doc_type(doc_type, uploaded, target, value)
        if not is_trusted_source_for_field(effective_doc_type, target):
            continue
        raw_analysis = uploaded.get("analysis")
        analysis: dict[str, Any] = raw_analysis if isinstance(raw_analysis, dict) else {}
        field_source_checked = target == "mbl_number"
        if target == "mbl_number" and not _field_source_allows_mbl_candidate(analysis, value):
            continue
        if target == "container_number" and effective_doc_type == "telex_release":
            field_source_checked = True
            if not _field_source_allows_container_candidate(analysis, value):
                continue
        if not _is_valid_document_field_evidence(target, value):
            continue
        key = (target, value)
        if key in seen:
            continue
        seen.add(key)
        label = row.get("label") or target
        intents.append({
            "order_id": order_id,
            "target": target,
            "value": value,
            "current_tms_value": row.get("tms"),
            "document_value": value,
            "label": label,
            "source": "document_activity_monitor",
            "requires_review": True,
            "review_status": "agent_review_required",
            "confidence": "document_field_comparison",
            "context_id": context_id,
            "question": f"{label}: TMS nach Agent-/Operator-Prüfung auf Dokumentwert setzen?",
            "guardrails": {
                "trusted_source": True,
                "effective_document_type": effective_doc_type,
                "valid_field_evidence": True,
                "write_supported": target in write_supported_targets,
                "review_only": target not in write_supported_targets,
                "blocker_finding_present": False,
                "field_source_checked": field_source_checked,
                "side_effects_created": False,
            },
        })
    return intents


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
        action=None,
        date_from=date_from,
        date_to=date_to,
        page=page,
        per_page=per_page,
    )
    items = payload.get("items") if isinstance(payload, dict) else []
    uploads = [row for row in items or [] if isinstance(row, dict) and _is_document_upload(row)]
    return {**(payload if isinstance(payload, dict) else {}), "document_uploads": uploads}


def baseline_document_activity_monitor_state(
    *,
    storage_root: Path | None = None,
    admin_user_id: int = DEFAULT_ADMIN_USER_ID,
    per_page: int = 100,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any]:
    """Set the document monitor cursor to the latest current upload without processing backlog."""
    activity_payload = list_recent_document_uploads(
        admin_user_id=admin_user_id,
        per_page=per_page,
        date_from=date_from,
        date_to=date_to,
    )
    uploads = activity_payload.get("document_uploads") or []
    upload_ids = [_activity_id(row) for row in uploads if isinstance(row, dict) and _activity_id(row)]
    highest_id = max(upload_ids) if upload_ids else 0
    now = utc_now_iso()
    state = {
        "last_seen_activity_id": highest_id,
        "processed_activity_ids": [],
        "updated_at": now,
        "baseline": {
            "created_at": now,
            "source": "tms_activity_log",
            "document_upload_count_seen": len(uploads),
            "highest_activity_id": highest_id,
            "date_from": date_from,
            "date_to": date_to,
            "policy": "old_backlog_deleted_start_from_current_activity_log",
        },
    }
    state_path = _save_state(state, storage_root)
    run_payload = {
        "status": "baselined",
        "generated_at": now,
        "dry_run": False,
        "source": "tms_activity_log",
        "filter": {"entity_type": "document", "action": "upload", "date_from": date_from, "date_to": date_to},
        "baselined_activity_id": highest_id,
        "last_seen_activity_id_before": None,
        "last_seen_activity_id_after": highest_id,
        "candidates": len(uploads),
        "selected": 0,
        "processed_count": 0,
        "error_count": 0,
        "processed": [],
        "errors": [],
        "notifications": [],
        "state_path": str(state_path),
    }
    latest_path = _save_latest_run(run_payload, storage_root)
    run_payload["latest_run_path"] = str(latest_path)
    return run_payload


def _processor_result_from_report(report: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    raw_reconciliation = report.get("reconciliation")
    reconciliation: dict[str, Any] = raw_reconciliation if isinstance(raw_reconciliation, dict) else {}
    raw_lifecycle = report.get("lifecycle")
    lifecycle: dict[str, Any] = raw_lifecycle if isinstance(raw_lifecycle, dict) else {}
    raw_registry = report.get("registry_summary")
    registry: dict[str, Any] = raw_registry if isinstance(raw_registry, dict) else {}
    raw_metadata = event.get("metadata")
    metadata: dict[str, Any] = raw_metadata if isinstance(raw_metadata, dict) else {}
    filename = _infer_uploaded_filename(report, event)
    doc_type = metadata.get("document_type") or "unbekannt"
    uploaded_for_label = _select_uploaded_analysis(report, str(filename))
    effective_doc_type_for_label = _uploaded_analysis_doc_type(doc_type, uploaded_for_label)
    if effective_doc_type_for_label and effective_doc_type_for_label != "unknown":
        doc_type = effective_doc_type_for_label
    case_risk = str(reconciliation.get("risk") or "low").strip().lower()
    case_needs_review = bool(reconciliation.get("needs_human_review"))
    raw_findings = reconciliation.get("findings")
    findings: list[Any] = raw_findings if isinstance(raw_findings, list) else []
    findings = _focus_findings_for_upload(findings, str(filename))
    findings = _normalize_uploaded_findings(findings, uploaded_for_label, str(filename))
    if findings is not raw_findings:
        reconciliation = {**reconciliation, "findings": findings}
    context = report.get("tms_context") if isinstance(report.get("tms_context"), dict) else {}
    comparisons = _build_document_field_comparison(report, str(filename))
    evidence_lines = _build_document_evidence_lines(report, str(filename))
    document_review_intents = _build_tms_update_review_intents_from_comparisons(report=report, event=event, comparisons=comparisons, findings=findings)
    current_upload_review = _current_upload_review_floor(
        case_risk=case_risk,
        case_needs_review=case_needs_review,
        findings=findings,
        comparisons=comparisons,
        document_review_intents=document_review_intents,
    )
    priority = str(current_upload_review.get("priority") or "low")
    needs_review = bool(current_upload_review.get("needs_review"))
    agent_evidence_packet = _build_document_agent_evidence_packet(
        report=report,
        event=event,
        filename=str(filename),
        doc_type=doc_type,
        context=context,
        findings=findings,
        needs_review=needs_review,
        priority=priority,
        comparisons=comparisons,
        evidence_lines=evidence_lines,
        document_review_intents=document_review_intents,
        case_risk=case_risk,
        case_needs_review=case_needs_review,
        current_upload_review=current_upload_review,
    )
    agent_review = _run_document_agent_review(agent_evidence_packet)
    rejected_agent_review: dict[str, Any] | None = None
    if agent_review and not _document_agent_review_is_quality_safe(agent_evidence_packet, agent_review):
        rejected_agent_review = {"mode": "external_agent_rejected", "reason": "ungrounded_stale_or_generic_review", "raw": agent_review}
        agent_review = None
    fallback_message = _human_document_message(
        order_id=report.get("order_id"),
        filename=filename,
        doc_type=doc_type,
        context=context,
        findings=findings,
        needs_review=needs_review,
        comparisons=comparisons,
        evidence_lines=evidence_lines,
        uploaded_by=event.get("changed_by_name") or event.get("changed_by"),
        uploaded_at=event.get("changed_at"),
    )
    if agent_review:
        message = _message_from_agent_review(report.get("order_id"), agent_review)
        priority = _priority_max(priority, agent_review.get("priority"))
        needs_review = bool(needs_review or document_review_intents or agent_review.get("needs_review"))
    else:
        message = fallback_message
    pending_review = 1 if (needs_review or document_review_intents) else 0
    review_count = len(document_review_intents)
    side_effects = {"tms_updates": 0, "queued_tms_actions": 0, "customer_notifications": 0}
    document_message_sections = {
        "lage": _section_from_message(message, "Lage"),
        "abgleich": _section_from_message(message, "Abgleich"),
        "auffaellig": _section_from_message(message, "Auffällig"),
        "empfehlung": _section_from_message(message, "Empfehlung"),
        "naechster_schritt": _section_from_message(message, "Nächster Schritt"),
    }
    document_decision = "Agent Review erforderlich" if pending_review else "Keine Aktion nötig"
    if agent_review and agent_review.get("decision"):
        decision_labels = {
            "no_action": "Agentische Bewertung: keine Aktion nötig",
            "observe": "Agentische Bewertung: weiter beobachten",
            "manual_review": "Agentische Bewertung: fachlich prüfen",
            "queue_review_card": "Agentische Bewertung: TMS-Freigabe prüfen",
        }
        document_decision = decision_labels.get(str(agent_review.get("decision")), "Agentische Bewertung übernommen")
    if document_review_intents:
        document_decision = "TMS-Korrektur nur nach Agent-/Operator-Freigabe prüfen"
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
        "document_message_sections": document_message_sections,
        "document_agent_review": agent_review or {"mode": "guardrailed_fallback", "reason": "external_agent_review_not_configured_or_invalid", "rejected_agent_review": rejected_agent_review},
        "document_agent_evidence_packet": agent_evidence_packet,
        "document_agent_fallback_message": fallback_message,
        "document_decision": document_decision,
        "document_review_intents": document_review_intents,
        "agent_review_required": bool(pending_review),
        "review_contract": "deterministic_evidence_only_agent_or_operator_must_decide",
        "evidence_summary": {
            "tms_snapshot_path": lifecycle.get("tms_snapshot_path"),
            "document_registry_path": lifecycle.get("document_registry_path"),
            "history_sync_count": lifecycle.get("history_sync_count", 0),
            "history_sync_error": lifecycle.get("history_sync_error"),
            "last_email_at": lifecycle.get("last_email_at"),
            "field_comparison_count": len(comparisons),
            "document_evidence_count": len(evidence_lines),
            "review_intent_count": review_count,
            "case_reconciliation_risk": case_risk,
            "current_upload_priority": priority,
            "current_upload_review": current_upload_review,
        },
        "side_effects": side_effects,
        "teams_tms_review_cards": [],
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
    baseline_now: bool = False,
) -> dict[str, Any]:
    if baseline_now:
        return baseline_document_activity_monitor_state(
            storage_root=storage_root,
            admin_user_id=admin_user_id,
            per_page=per_page,
            date_from=date_from,
            date_to=date_to,
        )
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
            raw_review_intents = processor_result.get("document_review_intents")
            document_review_intents = raw_review_intents if isinstance(raw_review_intents, list) else []
            review_card_result = _queue_document_review_card_results(
                storage_root=storage_root,
                order_id=order_id,
                intents=document_review_intents,
                event=event,
            )
            review_cards = review_card_result.get("created") or []
            duplicate_review_cards = review_card_result.get("duplicates") or []
            if review_cards:
                processor_result["teams_tms_review_cards"] = review_cards
                side_effects = processor_result.get("side_effects") if isinstance(processor_result.get("side_effects"), dict) else {}
                side_effects["queued_tms_actions"] = len(review_cards)
                processor_result["side_effects"] = side_effects
                pending_raw = processor_result.get("pending_action_summary")
                pending = pending_raw if isinstance(pending_raw, dict) else {}
                pending["review"] = max(int(pending.get("review", 0) or 0), len(review_cards))
                pending["review_intents_detected"] = len(document_review_intents)
                pending["review_cards_created"] = len(review_cards)
                pending["review_cards_duplicate"] = len(duplicate_review_cards)
                processor_result["pending_action_summary"] = pending
                processor_result["document_decision"] = "TMS-Freigabe-Kachel erstellt; wartet auf Bestätigung/Ablehnung"
            if duplicate_review_cards:
                processor_result["duplicate_tms_review_cards"] = duplicate_review_cards
                pending_raw = processor_result.get("pending_action_summary")
                pending = pending_raw if isinstance(pending_raw, dict) else {}
                pending["review"] = max(int(pending.get("review", 0) or 0), len(duplicate_review_cards))
                pending["review_intents_detected"] = len(document_review_intents)
                pending["review_cards_created"] = len(review_cards)
                pending["review_cards_duplicate"] = len(duplicate_review_cards)
                processor_result["pending_action_summary"] = pending
                side_effects = processor_result.get("side_effects") if isinstance(processor_result.get("side_effects"), dict) else {}
                processor_result["side_effects"] = side_effects
                processor_result["open_review_cards_referenced"] = len(duplicate_review_cards)
                duplicate_summary = "; ".join(str(card.get("summary") or "").strip() for card in duplicate_review_cards if isinstance(card, dict) and card.get("summary"))
                if duplicate_summary:
                    processor_result["message"] = f"{str(processor_result.get('message') or '').rstrip()}\nHinweis: {duplicate_summary}"
                    processor_result["analysis_summary"] = processor_result["message"]
                if not review_cards:
                    processor_result["document_decision"] = "TMS-Freigabe-Kachel existiert bereits; keine zweite Kachel erstellt"
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
        failed_ids = {int(err.get("activity_id") or 0) for err in errors}
        successful_ids = {_activity_id(row) for row in selected} - failed_ids
        processed_ids.update(successful_ids)
        cursor = last_seen
        for row in selected:
            event_id = _activity_id(row)
            if event_id in failed_ids:
                break
            if event_id in successful_ids:
                cursor = max(cursor, event_id)
        state["last_seen_activity_id"] = cursor
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
    parser.add_argument("--baseline-now", action="store_true", help="Set cursor to current latest document upload and do not process old backlog.")
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
        baseline_now=args.baseline_now,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
