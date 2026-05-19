"""Agentic, read-only TMS proposal layer for CARGOLO ASR cases.

This module turns reliable evidence deltas between the local TMS snapshot and
case/document/mail artefacts into Teams ``pending_review`` cards. It never writes
TMS data; it only appends review intents to the existing Teams queue.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable


_ORDER_RE = re.compile(r"\b(?:AN|BU)-\d{3,}\b", re.IGNORECASE)
_PLACEHOLDERS = {"", "-", "n/a", "na", "none", "null", "unknown", "unbekannt", "nicht lesbar", "not readable", "unreadable"}
_WRITE_SUPPORTED_TARGETS = {"customs_reference", "hbl_number", "mbl_number", "hawb_number", "container_number", "pickup_date", "estimated_delivery_date"}
_REVIEW_ONLY_TARGETS = {"cargo_weight_kg", "cargo_pieces", "seal_number", "hs_code"}
_SUPPORTED_TARGETS = _WRITE_SUPPORTED_TARGETS | _REVIEW_ONLY_TARGETS


_FIELD_SPECS: dict[str, dict[str, Any]] = {
    "cargo_weight_kg": {
        "label": "Gewicht",
        "doc_keys": ("total_weight_kg", "weight_kg", "gross_weight_kg", "gross_weight", "weight"),
        "tms_paths": (("detail", "totals", "total_weight_kg"), ("detail", "totals", "weight_kg"), ("totals", "total_weight_kg"), ("weight_kg",)),
        "kind": "number",
        "trusted_doc_types": {"waybill", "bill_of_lading", "master_bl", "house_bl", "packing_list", "commercial_invoice", "delivery_note"},
    },
    "cargo_pieces": {
        "label": "Packstücke",
        "doc_keys": ("total_packages", "total_pieces", "packages", "pieces", "cartons", "quantity", "package_count"),
        "tms_paths": (("detail", "totals", "total_packages"), ("detail", "totals", "total_pieces"), ("totals", "total_packages"), ("pieces",)),
        "kind": "integer",
        "trusted_doc_types": {"waybill", "bill_of_lading", "master_bl", "house_bl", "packing_list", "commercial_invoice", "delivery_note"},
    },
    "seal_number": {
        "label": "Seal",
        "doc_keys": ("seal_number", "seal", "seal_no", "seal_no_", "container_seal"),
        "tms_paths": (("detail", "freight_details", "seal_number"), ("detail", "freight_details", "seal"), ("freight_details", "seal_number")),
        "kind": "code",
        "trusted_doc_types": {"waybill", "bill_of_lading", "master_bl", "house_bl", "packing_list"},
    },
    "pickup_date": {
        "label": "Pickup/Loading Date",
        "doc_keys": ("pickup_date", "loading_date", "load_date", "delivery_date", "shipment_date"),
        "tms_paths": (("detail", "dates", "pickup_date"), ("dates", "pickup_date"), ("pickup_date",)),
        "kind": "date",
        "trusted_doc_types": {"waybill", "cmr", "delivery_note", "packing_list", "bill_of_lading", "master_bl", "house_bl"},
    },
    "estimated_delivery_date": {
        "label": "ETA/Delivery Date",
        "doc_keys": ("estimated_delivery_date", "delivery_date", "eta"),
        "tms_paths": (("detail", "dates", "estimated_delivery_date"), ("dates", "estimated_delivery_date"), ("estimated_delivery_date",)),
        "kind": "date",
        "trusted_doc_types": {"waybill", "delivery_note", "bill_of_lading", "master_bl", "house_bl"},
    },
    "container_number": {
        "label": "Container",
        "doc_keys": ("container_number", "container_no", "container"),
        "tms_paths": (("detail", "freight_details", "container_number"), ("freight_details", "container_number"), ("container_number",)),
        "kind": "container",
        "trusted_doc_types": {"waybill", "bill_of_lading", "master_bl", "house_bl", "packing_list"},
    },
    "customs_reference": {
        "label": "Zollreferenz/MRN",
        "doc_keys": ("mrn", "customs_reference", "customs_mrn"),
        "tms_paths": (("detail", "customs", "customs_reference"), ("customs", "customs_reference"), ("customs_reference",)),
        "kind": "mrn",
        "trusted_doc_types": {"customs_document", "customs", "export_declaration", "commercial_invoice"},
    },
    "hs_code": {
        "label": "HS-Code",
        "doc_keys": ("hs_code", "hs_codes", "tariff_code", "customs_tariff_number"),
        "tms_paths": (("detail", "customs", "hs_code"), ("customs", "hs_code"), ("hs_code",)),
        "kind": "hs_code",
        "trusted_doc_types": {"commercial_invoice", "customs_document", "packing_list"},
        "caution": True,
    },
}


def _load_json(path: Path | str | None) -> dict[str, Any]:
    if not path:
        return {}
    try:
        p = Path(path)
        if not p.exists():
            return {}
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in _PLACEHOLDERS:
        return ""
    return text


def _norm(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", "", _clean(value).upper())


def _path_get(data: dict[str, Any], path: Iterable[str]) -> Any:
    current: Any = data
    for part in path:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _first_path(data: dict[str, Any], paths: Iterable[Iterable[str]]) -> str:
    for path in paths:
        value = _clean(_path_get(data, path))
        if value:
            return value
    return ""


def _number(value: Any) -> float | None:
    text = _clean(value)
    if not text:
        return None
    text = text.replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _format_number(value: float, *, integer: bool = False) -> str:
    if integer or abs(value - round(value)) < 0.0001:
        return str(int(round(value)))
    return (f"{value:.3f}".rstrip("0").rstrip("."))


def _normalize_value(value: Any, kind: str) -> str:
    if kind in {"number", "integer"}:
        num = _number(value)
        if num is None or num <= 0:
            return ""
        return _format_number(num, integer=kind == "integer")
    text = _clean(value)
    if not text:
        return ""
    if kind == "date":
        match = re.search(r"\b(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})\b", text)
        if match:
            return f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        match = re.search(r"\b(\d{1,2})[./-](\d{1,2})[./-](20\d{2})\b", text)
        if match:
            return f"{match.group(3)}-{int(match.group(2)):02d}-{int(match.group(1)):02d}"
        return ""
    if kind == "container":
        match = re.search(r"\b([A-Z]{4}\d{7})\b", text.upper())
        return match.group(1) if match else ""
    if kind == "mrn":
        match = re.search(r"\b([0-9]{2}[A-Z]{2}[A-Z0-9]{3,})\b", text.upper())
        return match.group(1) if match else ""
    if kind == "hs_code":
        matches = re.findall(r"\b(\d{6,10})\b", text.replace(" ", ""))
        unique = sorted(set(matches))
        return ", ".join(unique[:3]) if len(unique) == 1 else ""
    if kind == "code":
        compact = re.sub(r"\s+", "", text.upper())
        return compact if re.search(r"[A-Z0-9]", compact) and len(compact) >= 3 else ""
    return text


def _same_value(left: str, right: str, kind: str) -> bool:
    if not left or not right:
        return False
    if kind in {"number", "integer"}:
        a = _number(left)
        b = _number(right)
        if a is None or b is None:
            return False
        tolerance = max(0.01, abs(a) * 0.02) if kind == "number" else 0.01
        return abs(a - b) <= tolerance
    return _norm(left) == _norm(right)


def _doc_type(row: dict[str, Any], analysis: dict[str, Any]) -> str:
    return str(row.get("analysis_doc_type") or row.get("doc_type") or analysis.get("doc_type") or "").strip().lower()


def _iter_evidence_documents(case_root: Path, registry: dict[str, Any], summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in registry.get("analyzed_documents", []) or []:
        if not isinstance(row, dict):
            continue
        analysis = _load_json(row.get("analysis_path"))
        if not analysis and isinstance(row.get("analysis"), dict):
            analysis = row.get("analysis") or {}
        fields: dict[str, Any] = {}
        if isinstance(analysis.get("extracted_fields"), dict):
            fields.update(analysis.get("extracted_fields") or {})
        if isinstance(row.get("extracted_fields"), dict):
            fields.update(row.get("extracted_fields") or {})
        rows.append({"row": row, "analysis": analysis, "fields": fields, "source": row.get("filename") or analysis.get("filename") or row.get("analysis_path")})
    # Some tests/legacy runs only have latest_summary.json.
    for key in ("analyzed_documents", "documents", "analyses"):
        for row in summary.get(key, []) if isinstance(summary.get(key), list) else []:
            if not isinstance(row, dict):
                continue
            fields: dict[str, Any] = {}
            if isinstance(row.get("extracted_fields"), dict):
                fields.update(row.get("extracted_fields") or {})
            if fields:
                rows.append({"row": row, "analysis": row, "fields": fields, "source": row.get("filename") or key})
    return rows


def _pending_duplicate_or_conflict(root: Path, order_id: str, target: str, value: str) -> tuple[bool, bool]:
    duplicate = False
    conflict = False
    queue = root / "orders" / order_id / "teams" / "pending_tms_actions.jsonl"
    for row in _read_jsonl(queue):
        if str(row.get("status") or "") != "pending_review":
            continue
        if str(row.get("order_id") or "").strip().upper() != order_id:
            continue
        if str(row.get("target") or "").strip() != target:
            continue
        existing = _clean(row.get("value"))
        if _norm(existing) == _norm(value):
            duplicate = True
        elif existing:
            conflict = True
    return duplicate, conflict


def _load_case_artifacts(root: Path, order_id: str) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    case_root = root / "orders" / order_id
    case_state = _load_json(case_root / "case_state.json")
    tms_snapshot = _load_json(case_root / "tms_snapshot.json") or _load_json(case_root / "tms" / "snapshot.json")
    registry = _load_json(case_root / "documents" / "registry.json")
    summary = _load_json(case_root / "documents" / "analysis" / "latest_summary.json")
    return case_root, case_state, tms_snapshot, registry, summary


def queue_agentic_tms_review_cards(
    *,
    root: Path,
    order_id: str,
    case_state: dict[str, Any] | None = None,
    tms_snapshot: dict[str, Any] | None = None,
    registry: dict[str, Any] | None = None,
    document_summary: dict[str, Any] | None = None,
    source: str = "case_evidence_refresh",
    source_message_id: str | None = None,
    operator: str | None = "Hermes Agentic Proposal Layer",
    context_id: str | None = None,
    max_cards: int = 3,
) -> list[dict[str, Any]]:
    """Queue up to ``max_cards`` safe TMS review proposals from local evidence.

    Guardrails: requires AN/BU order id, concrete target/value/evidence, skips
    duplicate and conflicting proposed values, queues only pending_review rows,
    and never calls any TMS writeback function.
    """
    normalized_order = str(order_id or "").strip().upper()
    if not _ORDER_RE.fullmatch(normalized_order):
        return []
    case_root, loaded_state, loaded_tms, loaded_registry, loaded_summary = _load_case_artifacts(root, normalized_order)
    del case_state  # reserved for future mail/case extraction; loaded for side-effect compatibility below
    tms_snapshot = tms_snapshot if isinstance(tms_snapshot, dict) else loaded_tms
    registry = registry if isinstance(registry, dict) else loaded_registry
    document_summary = document_summary if isinstance(document_summary, dict) else loaded_summary
    if not isinstance(tms_snapshot, dict) or not tms_snapshot:
        return []
    context_id = context_id or f"{normalized_order}:agentic_tms_proposal"

    candidates: list[dict[str, Any]] = []
    per_target_values: dict[str, set[str]] = {}
    documents = _iter_evidence_documents(case_root, registry or {}, document_summary or {})
    for item in documents:
        row = item["row"]
        analysis = item["analysis"]
        fields = item["fields"]
        source_name = _clean(item.get("source")) or "Dokumentanalyse"
        doc_type = _doc_type(row, analysis)
        for target, spec in _FIELD_SPECS.items():
            if target not in _SUPPORTED_TARGETS:
                continue
            trusted_types = spec.get("trusted_doc_types") or set()
            if doc_type and trusted_types and doc_type not in trusted_types:
                continue
            raw_doc = next((_clean(fields.get(key)) for key in spec["doc_keys"] if _clean(fields.get(key))), "")
            value = _normalize_value(raw_doc, str(spec.get("kind") or "text"))
            if not value:
                continue
            tms_value = _normalize_value(_first_path(tms_snapshot, spec["tms_paths"]), str(spec.get("kind") or "text"))
            if tms_value and _same_value(tms_value, value, str(spec.get("kind") or "text")):
                continue
            per_target_values.setdefault(target, set()).add(_norm(value))
            candidates.append({
                "target": target,
                "value": value,
                "previous_value": tms_value or _first_path(tms_snapshot, spec["tms_paths"]) or "nicht gepflegt",
                "label": spec.get("label") or target,
                "evidence": f"{source_name} ({doc_type or 'Dokument'}): {spec.get('label') or target} = {value}",
                "source": source_name,
                "doc_type": doc_type,
                "caution": bool(spec.get("caution")),
                "write_supported": target in _WRITE_SUPPORTED_TARGETS,
            })

    # Drop fields where different documents yielded competing values.
    conflict_targets = {target for target, values in per_target_values.items() if len(values) > 1}
    seen: set[tuple[str, str]] = set()
    queued_cards: list[dict[str, Any]] = []
    try:
        from .teams_reply_loop import record_agent_tms_update_intent
    except Exception:
        return []

    for candidate in candidates:
        if len(queued_cards) >= max_cards:
            break
        target = str(candidate["target"])
        value = str(candidate["value"])
        if target in conflict_targets:
            continue
        key = (target, _norm(value))
        if key in seen:
            continue
        seen.add(key)
        duplicate, pending_conflict = _pending_duplicate_or_conflict(root, normalized_order, target, value)
        if duplicate or pending_conflict:
            continue
        caution = " Vorsicht: HS-Code nur aus Dokument übernehmen, wenn fachlich plausibel." if candidate.get("caution") else ""
        write_note = "Review-only; kein direkter TMS-Write-Zielpfad vorhanden." if not candidate.get("write_supported") else "Bestehender TMS-Writeback-Zielpfad vorhanden; trotzdem nur nach Freigabe."
        text = (
            f"Agentic Evidence Delta: {candidate['label']} für {normalized_order} prüfen. "
            f"TMS={candidate['previous_value']} ↔ Evidenz={value}. Quelle: {candidate['evidence']}.{caution} {write_note}"
        )
        queued = record_agent_tms_update_intent(
            root=root,
            order_id=normalized_order,
            target=target,
            value=value,
            text=text,
            operator=operator,
            source_message_id=source_message_id,
            context_id=context_id,
            confidence="agentic_evidence_delta_caution" if candidate.get("caution") else "agentic_evidence_delta",
            source=source,
            evidence={
                "summary": candidate["evidence"],
                "previous_value": candidate["previous_value"],
                "doc_type": candidate.get("doc_type"),
                "write_supported": candidate.get("write_supported"),
            },
            previous_value=str(candidate["previous_value"]),
            write_supported=bool(candidate.get("write_supported")),
        )
        if queued.get("queued"):
            queued_cards.append({
                "order_id": normalized_order,
                "action_id": queued.get("action_id"),
                "target": target,
                "value": value,
                "previous_value": candidate["previous_value"],
                "operator": operator,
                "context_id": context_id,
                "source": source,
                "evidence": candidate["evidence"],
                "write_supported": candidate.get("write_supported"),
                "question": f"{candidate['label']}: TMS-Wert aus Evidenz als pending_review bestätigen?",
            })
    return queued_cards
