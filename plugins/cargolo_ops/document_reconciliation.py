from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .document_schema import normalize_document_type, normalize_mode


MISSING_ONLY_NOTE = "missing_documents_are_inventory_context_not_risk"


def _load_analysis(path_value: Any) -> dict[str, Any]:
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
        return {}


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def _number(value: Any) -> float | None:
    if value in (None, "", [], {}):
        return None
    text = str(value).strip().replace("\u202f", " ").replace("'", "")
    match = re.search(r"[-+]?\d[\d .,_]*", text)
    if not match:
        return None
    token = match.group(0).strip().replace(" ", "").replace("_", "")
    if not token:
        return None
    comma = token.rfind(",")
    dot = token.rfind(".")
    if comma >= 0 and dot >= 0:
        decimal_sep = "," if comma > dot else "."
        thousand_sep = "." if decimal_sep == "," else ","
        token = token.replace(thousand_sep, "").replace(decimal_sep, ".")
    elif comma >= 0:
        parts = token.split(",")
        if len(parts[-1]) == 3 and all(part.isdigit() for part in parts):
            token = "".join(parts)
        else:
            token = token.replace(",", ".")
    elif dot >= 0:
        parts = token.split(".")
        if len(parts[-1]) == 3 and all(part.isdigit() for part in parts):
            token = "".join(parts)
    try:
        return float(token)
    except ValueError:
        return None


def _fmt_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _tms_detail(tms_snapshot: dict[str, Any]) -> dict[str, Any]:
    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    return detail if isinstance(detail, dict) else {}


def _tms_totals(tms_snapshot: dict[str, Any]) -> dict[str, Any]:
    detail = _tms_detail(tms_snapshot)
    totals = detail.get("totals") if isinstance(detail.get("totals"), dict) else tms_snapshot.get("totals") if isinstance(tms_snapshot.get("totals"), dict) else {}
    return totals if isinstance(totals, dict) else {}


def _norm_reference(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _tms_customs_reference(tms_snapshot: dict[str, Any]) -> str:
    detail = _tms_detail(tms_snapshot)
    candidates = [
        detail.get("customs_reference"),
        detail.get("mrn"),
        detail.get("customs_mrn"),
        tms_snapshot.get("customs_reference") if isinstance(tms_snapshot, dict) else None,
    ]
    customs = detail.get("customs") if isinstance(detail.get("customs"), dict) else {}
    candidates.extend([customs.get("mrn"), customs.get("customs_reference")])
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _field_dict(row: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    analysis = _load_analysis(row.get("analysis_path"))
    if isinstance(analysis.get("extracted_fields"), dict):
        fields.update(analysis.get("extracted_fields") or {})
    if isinstance(row.get("extracted_fields"), dict):
        fields.update(row.get("extracted_fields") or {})
    return fields


def _has_blocker_finding(findings: list[dict[str, Any]]) -> bool:
    for row in findings:
        if not isinstance(row, dict):
            continue
        severity = str(row.get("severity") or "").strip().lower()
        finding_type = str(row.get("type") or "").strip().lower()
        if severity == "blocker" or "blocker" in finding_type:
            return True
    return False


def _append_profile_blocker(findings: list[dict[str, Any]], *, filename: Any, code: str, summary: str) -> None:
    findings.append({
        "type": "document_profile_blocker",
        "code": code,
        "severity": "blocker",
        "filename": filename,
        "summary": summary,
    })


def _normalize_note(value: Any) -> str:
    return " ".join(str(value or "").replace("_", " ").strip().split())


def _row_doc_type(row: dict[str, Any]) -> str:
    return str(row.get("analysis_doc_type") or row.get("doc_type") or "").strip().lower()


def _is_spreadsheet_like(row: dict[str, Any]) -> bool:
    filename = str(row.get("filename") or "").strip().lower()
    return filename.endswith((".xls", ".xlsx"))


def _is_packlist_like(row: dict[str, Any]) -> bool:
    doc_type = _row_doc_type(row)
    return doc_type in {"packing_list", "packlist", "packing list"}


def _is_low_signal_document_note(value: Any, *, row: dict[str, Any] | None = None) -> bool:
    """Suppress model/tooling noise that should not create an ops exception.

    Keep this deliberately narrow: concrete mismatch phrases must still surface.
    Bare date/status labels are global noise; net-weight-zero suppression is only
    for the observed spreadsheet/packing-list artefact and never for invoices or
    customs documents.
    """
    text = _normalize_note(value).lower()
    if not text:
        return True
    if text in {"date", "datum", "document ok", "ok"}:
        return True
    if re.fullmatch(r"(?:field|feld)?\s*(?:date|datum)", text):
        return True
    spreadsheet_packlist_artifact = bool(row) and _is_packlist_like(row or {})
    if spreadsheet_packlist_artifact and text in {"net weight", "missing net weight"}:
        return True
    if spreadsheet_packlist_artifact and "net weight" in text and re.search(r"\b(?:specified as|value is|wert|=|:)\s*0(?:[.,]0+)?\b|\(\s*0(?:[.,]0+)?\s*\)", text):
        return True
    return False


def _humanize_document_note(value: Any) -> str:
    text = _normalize_note(value)
    replacements = {
        "missing net weight": "Nettogewicht im Beleg nicht belastbar lesbar",
        "net weight specified as 0": "Nettogewicht im Beleg mit 0 angegeben; nur fachlich relevant, wenn Netto-Gewicht benötigt wird",
    }
    return replacements.get(text.lower(), text)




def _cross_doc_value(row: dict[str, Any], field: str) -> Any:
    fields = _field_dict(row)
    if field == "gross_weight":
        return _first_present(fields, "total_weight_kg", "weight_kg", "gross_weight_kg", "gross_weight", "weight")
    if field == "pieces":
        return _first_present(fields, "total_packages", "packages", "pieces", "total_pieces", "cartons")
    if field == "volume":
        return _first_present(fields, "total_volume_m3", "volume_m3", "volume", "cbm")
    if field == "incoterm":
        return _first_present(fields, "incoterm", "incoterms", "incoterm_named_place")
    return None


def _normalize_incoterm(value: Any) -> str:
    text = str(value or "").upper()
    match = re.search(r"\b(EXW|FCA|FAS|FOB|CFR|CIF|CPT|CIP|DAP|DPU|DAT|DDP)\b", text)
    return match.group(1) if match else ""


def _cross_doc_doc_type(row: dict[str, Any]) -> str:
    analysis = _load_analysis(row.get("analysis_path"))
    return str(row.get("analysis_doc_type") or row.get("doc_type") or analysis.get("doc_type") or "unknown").strip().lower()


def _cross_doc_entry(row: dict[str, Any], field: str) -> dict[str, Any] | None:
    raw = _cross_doc_value(row, field)
    filename = str(row.get("filename") or "").strip()
    if not filename or raw in (None, "", [], {}):
        return None
    if field in {"gross_weight", "pieces", "volume"}:
        normalized = _number(raw)
        if normalized is None:
            return None
        if field in {"pieces", "gross_weight", "volume"} and normalized <= 0:
            return None
        return {"filename": filename, "doc_type": _cross_doc_doc_type(row), "raw": str(raw), "normalized": normalized}
    if field == "incoterm":
        normalized = _normalize_incoterm(raw)
        if not normalized:
            return None
        return {"filename": filename, "doc_type": _cross_doc_doc_type(row), "raw": str(raw), "normalized": normalized}
    return None


def _cross_doc_groups_conflict(field: str, entries: list[dict[str, Any]]) -> bool:
    if len(entries) < 2:
        return False
    values = [entry.get("normalized") for entry in entries]
    if field == "gross_weight":
        low = min(float(value) for value in values)
        high = max(float(value) for value in values)
        return abs(high - low) > max(2.0, high * 0.02)
    if field == "volume":
        low = min(float(value) for value in values)
        high = max(float(value) for value in values)
        return abs(high - low) > max(0.1, high * 0.05)
    return len({str(value) for value in values}) > 1


def _append_cross_document_reconciliation(findings: list[dict[str, Any]], *, registry: dict[str, Any]) -> list[dict[str, Any]]:
    analyzed = [row for row in registry.get("analyzed_documents", []) or [] if isinstance(row, dict)]
    comparisons: list[dict[str, Any]] = []
    rules = {
        "gross_weight": {"type": "cross_document_weight_mismatch", "label": "Gewicht", "unit": "kg", "target": "cargo_weight_kg"},
        "pieces": {"type": "cross_document_piece_mismatch", "label": "Packstücke", "unit": "", "target": "cargo_pieces"},
        "incoterm": {"type": "cross_document_incoterm_mismatch", "label": "Incoterm", "unit": "", "target": "incoterms"},
    }
    for field, rule in rules.items():
        entries = [entry for row in analyzed if (entry := _cross_doc_entry(row, field))]
        # Same filename/doc can appear twice through mail and TMS mirror; keep one value per filename.
        deduped: dict[str, dict[str, Any]] = {}
        for entry in entries:
            deduped.setdefault(str(entry.get("filename") or ""), entry)
        entries = list(deduped.values())
        if not _cross_doc_groups_conflict(field, entries):
            continue
        values_text = "; ".join(
            f"{entry['filename']} ({entry.get('doc_type') or 'Dokument'}): {_fmt_number(float(entry['normalized'])) if isinstance(entry.get('normalized'), (int, float)) else entry.get('normalized')}{(' ' + rule['unit']) if rule.get('unit') and isinstance(entry.get('normalized'), (int, float)) else ''}"
            for entry in entries[:5]
        )
        filenames = [str(entry.get("filename") or "") for entry in entries if entry.get("filename")]
        comparison = {
            "field": field,
            "target": rule["target"],
            "type": rule["type"],
            "label": rule["label"],
            "severity": "medium",
            "status": "conflict",
            "scope": "cross_document_comparison",
            "review_only": field in {"gross_weight", "pieces"},
            "write_supported": False,
            "filenames": filenames,
            "documents": entries,
            "summary": f"Dokumente widersprechen sich bei {rule['label']}: {values_text}.",
        }
        comparisons.append(comparison)
        findings.append({
            "type": rule["type"],
            "severity": "medium",
            "scope": "cross_document_comparison",
            "category": "case_context",
            "filename": filenames[0] if len(filenames) == 1 else None,
            "filenames": filenames,
            "field": field,
            "target": rule["target"],
            "review_only": field in {"gross_weight", "pieces"},
            "write_supported": False,
            "summary": comparison["summary"],
            "documents": entries,
        })
    return comparisons

def _append_content_reconciliation(findings: list[dict[str, Any]], *, tms_snapshot: dict[str, Any], registry: dict[str, Any]) -> None:
    totals = _tms_totals(tms_snapshot)
    tms_reference = _tms_customs_reference(tms_snapshot)
    tms_weight = _number(_first_present(totals, "total_weight_kg", "weight_kg", "gross_weight_kg", "gross_weight"))
    tms_pieces = _number(_first_present(totals, "total_packages", "total_pieces", "packages", "pieces"))
    for row in registry.get("analyzed_documents", []) or []:
        if not isinstance(row, dict):
            continue
        analysis = _load_analysis(row.get("analysis_path"))
        fields: dict[str, Any] = {}
        if isinstance(analysis.get("extracted_fields"), dict):
            fields.update(analysis.get("extracted_fields") or {})
        if isinstance(row.get("extracted_fields"), dict):
            fields.update(row.get("extracted_fields") or {})
        filename = row.get("filename")
        doc_reference = str(_first_present(fields, "mrn", "customs_reference", "customs_mrn") or "").strip()
        doc_type = str(row.get("analysis_doc_type") or row.get("doc_type") or analysis.get("doc_type") or "").lower()
        if doc_type == "commercial_invoice":
            blob = "\n".join(
                str(value or "")
                for value in (
                    filename,
                    row.get("analysis_summary"),
                    analysis.get("summary"),
                    analysis.get("doc_type"),
                    fields,
                )
            ).lower()
            if "proforma" in blob or "pro forma" in blob:
                _append_profile_blocker(
                    findings,
                    filename=filename,
                    code="proforma_invoice",
                    summary="Commercial invoice is marked as proforma; customs-ready commercial invoice required.",
                )
        if tms_reference and doc_reference and _norm_reference(tms_reference) != _norm_reference(doc_reference):
            findings.append({
                "type": "mrn_mismatch",
                "severity": "high",
                "filename": filename,
                "summary": f"MRN im Dokument {doc_reference} passt nicht zur TMS-Zollreferenz {tms_reference}.",
            })
        doc_weight = _number(_first_present(fields, "total_weight_kg", "weight_kg", "gross_weight_kg", "gross_weight", "weight"))
        if tms_weight is not None and doc_weight is not None and abs(tms_weight - doc_weight) > max(2.0, tms_weight * 0.02):
            findings.append({
                "type": "tms_document_weight_mismatch",
                "severity": "medium",
                "filename": filename,
                "summary": f"Gewicht im Dokument {_fmt_number(doc_weight)} kg weicht vom TMS-Wert {_fmt_number(tms_weight)} kg ab.",
            })
        doc_pieces = _number(_first_present(fields, "total_packages", "packages", "pieces", "total_pieces", "cartons", "quantity"))
        if tms_pieces is not None and doc_pieces is not None and abs(tms_pieces - doc_pieces) >= 1:
            findings.append({
                "type": "tms_document_piece_mismatch",
                "severity": "medium",
                "filename": filename,
                "summary": f"Packstückzahl im Dokument {_fmt_number(doc_pieces)} weicht vom TMS-Wert {_fmt_number(tms_pieces)} ab.",
            })
        amount = _number(_first_present(fields, "amount", "total_amount", "goods_value", "cargo_value"))
        doc_type = str(row.get("analysis_doc_type") or row.get("doc_type") or analysis.get("doc_type") or "").lower()
        if amount == 0 and doc_type in {"commercial_invoice", "customs_document"}:
            if doc_type == "commercial_invoice":
                _append_profile_blocker(
                    findings,
                    filename=filename,
                    code="zero_value",
                    summary="Commercial invoice goods value is 0; value must be greater than zero for customs use.",
                )
            else:
                findings.append({
                    "type": "implausible_goods_value",
                    "severity": "medium",
                    "filename": filename,
                    "summary": "Warenwert im Dokument ist 0; für Zoll/Versicherung nicht plausibel.",
                })


def reconcile_documents(*, order_id: str, tms_snapshot: dict[str, Any], registry: dict[str, Any]) -> dict[str, Any]:
    """Deterministic reconciliation layer for document monitoring.

    Missing expected documents are inventory context, not risk by themselves.
    Risk/review is driven by present-document issues: unreadable files, concrete
    operational flags, mirroring gaps, or mismatches between extracted document
    content and TMS values such as weight, package count, or implausible goods
    value.
    """
    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    mode = normalize_mode((detail or {}).get("network") or (detail or {}).get("transport_mode") or tms_snapshot.get("mode"))
    expected = [normalize_document_type(x) for x in registry.get("expected_types", []) if str(x or "").strip()]
    received = [normalize_document_type(x) for x in registry.get("received_types", []) if str(x or "").strip()]
    missing = sorted(set(expected) - set(received))

    findings: list[dict[str, Any]] = []
    for row in registry.get("received_documents", []) or []:
        if not isinstance(row, dict):
            continue
        status = str(row.get("analysis_status") or "").lower()
        if status in {"error", "missing_file"}:
            findings.append({
                "type": "document_unreadable" if status == "error" else "local_file_missing",
                "severity": "medium",
                "filename": row.get("filename"),
                "summary": row.get("analysis_summary") or status,
            })
    for row in registry.get("analyzed_documents", []) or []:
        if not isinstance(row, dict):
            continue
        for flag in row.get("operational_flags") or []:
            if _is_low_signal_document_note(flag, row=row):
                continue
            findings.append({
                "type": "document_flag",
                "severity": "medium",
                "filename": row.get("filename"),
                "summary": _humanize_document_note(flag),
            })
        for item in row.get("missing_or_unreadable") or []:
            if _is_low_signal_document_note(item, row=row):
                continue
            findings.append({
                "type": "document_open_question",
                "severity": "low",
                "filename": row.get("filename"),
                "summary": _humanize_document_note(item),
            })
    for gap in registry.get("tms_mirroring_gaps", []) or []:
        if isinstance(gap, dict):
            findings.append({
                "type": "tms_mirroring_gap",
                "severity": "medium" if gap.get("mirror_status") == "download_failed" else "low",
                "filename": gap.get("filename") or gap.get("label"),
                "summary": f"TMS upload not mirrored locally: {gap.get('mirror_status')}",
            })

    _append_content_reconciliation(findings, tms_snapshot=tms_snapshot, registry=registry)
    cross_document_comparisons = _append_cross_document_reconciliation(findings, registry=registry)

    max_severity = "low"
    if _has_blocker_finding(findings) or any(row.get("severity") in {"high", "critical"} for row in findings):
        max_severity = "high"
    elif any(row.get("severity") == "medium" for row in findings):
        max_severity = "medium"

    return {
        "version": 2,
        "order_id": order_id,
        "mode": mode,
        "expected_types": sorted(set(expected)),
        "received_types": sorted(set(received)),
        "missing_types": missing,
        "missing_policy": MISSING_ONLY_NOTE,
        "findings": findings,
        "cross_document_comparisons": cross_document_comparisons,
        "risk": max_severity,
        "needs_human_review": bool(findings),
    }
