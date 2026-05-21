"""Agentic, read-only TMS proposal layer for CARGOLO ASR cases.

This module turns reliable evidence deltas between the local TMS snapshot and
case/document/mail artefacts into Teams ``pending_review`` cards. It never writes
TMS data; it only appends review intents to the existing Teams queue.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


_ORDER_RE = re.compile(r"\b(?:AN|BU)-\d{3,}\b", re.IGNORECASE)
_PLACEHOLDERS = {"", "-", "n/a", "na", "none", "null", "unknown", "unbekannt", "nicht lesbar", "not readable", "unreadable"}
_WRITE_SUPPORTED_TARGETS = {
    "customs_reference",
    "hbl_number",
    "mbl_number",
    "hawb_number",
    "container_number",
    "pickup_date",
    "estimated_delivery_date",
    "actual_delivery_date",
}
_REVIEW_ONLY_TARGETS = {"cargo_weight_kg", "cargo_pieces", "seal_number", "hs_code", "etd_main_carriage", "atd_main_carriage", "pol", "pod"}
_SUPPORTED_TARGETS = _WRITE_SUPPORTED_TARGETS | _REVIEW_ONLY_TARGETS
_TARGET_PRIORITY = {
    "cargo_weight_kg": 10,
    "cargo_pieces": 20,
    "seal_number": 30,
    "hs_code": 40,
    "pickup_date": 50,
    "estimated_delivery_date": 60,
    "actual_delivery_date": 65,
    "etd_main_carriage": 66,
    "atd_main_carriage": 67,
    "pol": 68,
    "pod": 69,
    "container_number": 70,
    "customs_reference": 80,
}


_FIELD_SPECS: dict[str, dict[str, Any]] = {
    "cargo_weight_kg": {
        "label": "Gewicht",
        "doc_keys": ("total_weight_kg", "weight_kg", "gross_weight_kg", "gross_weight", "weight"),
        "tms_paths": (("detail", "totals", "total_weight_kg"), ("detail", "totals", "weight_kg"), ("totals", "total_weight_kg"), ("weight_kg",)),
        "kind": "number",
        "trusted_doc_types": {"waybill", "bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading", "packing_list", "commercial_invoice", "delivery_note"},
    },
    "cargo_pieces": {
        "label": "Packstücke",
        "doc_keys": ("total_packages", "total_pieces", "packages", "pieces", "cartons", "quantity", "package_count"),
        "tms_paths": (("detail", "totals", "total_packages"), ("detail", "totals", "total_pieces"), ("totals", "total_packages"), ("pieces",)),
        "kind": "integer",
        "trusted_doc_types": {"waybill", "bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading", "packing_list", "commercial_invoice", "delivery_note"},
    },
    "seal_number": {
        "label": "Seal",
        "doc_keys": ("seal_number", "seal", "seal_no", "seal_no_", "container_seal"),
        "tms_paths": (("detail", "freight_details", "seal_number"), ("detail", "freight_details", "seal"), ("freight_details", "seal_number")),
        "kind": "code",
        "trusted_doc_types": {"waybill", "bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading", "packing_list"},
    },
    "pickup_date": {
        "label": "Pickup/Loading Date",
        "doc_keys": ("pickup_date", "loading_date", "load_date", "delivery_date", "shipment_date"),
        "tms_paths": (("detail", "dates", "pickup_date"), ("dates", "pickup_date"), ("pickup_date",)),
        "kind": "date",
        "trusted_doc_types": {"waybill", "cmr", "delivery_note", "packing_list", "bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading"},
    },
    "estimated_delivery_date": {
        "label": "ETA/Delivery Date",
        "doc_keys": ("estimated_delivery_date", "delivery_date", "eta", "estimated_arrival_date"),
        "tms_paths": (("detail", "dates", "estimated_delivery_date"), ("dates", "estimated_delivery_date"), ("estimated_delivery_date",)),
        "kind": "date",
        "trusted_doc_types": {"waybill", "delivery_note", "bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading", "booking_confirmation", "shipment_advice"},
    },
    "actual_delivery_date": {
        "label": "ATA/Actual Delivery Date",
        "doc_keys": ("actual_delivery_date", "ata", "actual_arrival_date", "arrival_date"),
        "tms_paths": (("detail", "dates", "actual_delivery_date"), ("dates", "actual_delivery_date"), ("actual_delivery_date",)),
        "kind": "date",
        "trusted_doc_types": {"waybill", "proof_of_delivery", "delivery_note", "bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading", "shipment_advice"},
    },
    "etd_main_carriage": {
        "label": "ETD Hauptlauf",
        "doc_keys": ("etd", "estimated_departure_date", "estimated_departure", "etd_main_carriage"),
        "tms_paths": (("detail", "milestones", "etd_main_carriage"), ("milestones", "etd_main_carriage"), ("detail", "transport_legs", "main_carriage", "etd")),
        "kind": "date",
        "trusted_doc_types": {"waybill", "bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading", "booking_confirmation", "shipment_advice"},
    },
    "atd_main_carriage": {
        "label": "ATD Hauptlauf",
        "doc_keys": ("atd", "actual_departure_date", "actual_departure", "atd_main_carriage"),
        "tms_paths": (("detail", "milestones", "atd_main_carriage"), ("milestones", "atd_main_carriage"), ("detail", "transport_legs", "main_carriage", "atd")),
        "kind": "date",
        "trusted_doc_types": {"waybill", "bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading", "booking_confirmation", "shipment_advice"},
    },
    "pol": {
        "label": "POL",
        "doc_keys": ("pol", "port_of_loading", "loading_port", "origin_port"),
        "tms_paths": (("detail", "freight_details", "pol_code"), ("freight_details", "pol_code"), ("detail", "transport_legs", "main_carriage", "origin")),
        "kind": "text",
        "trusted_doc_types": {"bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading", "booking_confirmation", "shipment_advice", "terminal_receipt"},
    },
    "pod": {
        "label": "POD",
        "doc_keys": ("pod", "port_of_discharge", "discharge_port", "destination_port"),
        "tms_paths": (("detail", "freight_details", "pod_code"), ("freight_details", "pod_code"), ("detail", "transport_legs", "main_carriage", "destination")),
        "kind": "text",
        "trusted_doc_types": {"bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading", "booking_confirmation", "shipment_advice"},
    },
    "container_number": {
        "label": "Container",
        "doc_keys": ("container_number", "container_no", "container"),
        "tms_paths": (("detail", "freight_details", "container_number"), ("freight_details", "container_number"), ("container_number",)),
        "kind": "container",
        "trusted_doc_types": {"waybill", "bill_of_lading", "master_bl", "house_bl", "master_bill_of_lading", "house_bill_of_lading", "packing_list"},
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


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


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
        if isinstance(current, list):
            if part == "main_carriage":
                current = next((row for row in current if isinstance(row, dict) and str(row.get("leg_type") or "") == "main_carriage"), None)
                if current is None:
                    return None
                continue
            return None
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


def _extract_hs_code_from_text(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    matches = re.findall(r"\bHS(?:\s*[-/]?\s*CODE)?\s*[:#-]?\s*(\d{6,10})\b", text, re.IGNORECASE)
    unique = sorted(set(matches))
    return ", ".join(unique[:3]) if len(unique) == 1 else ""


def _raw_doc_value(fields: dict[str, Any], spec: dict[str, Any]) -> str:
    for key in spec["doc_keys"]:
        value = _clean(fields.get(key))
        if value:
            return value
    if str(spec.get("kind") or "") == "hs_code":
        for key in ("goods_description", "description", "commodity_description", "notes"):
            value = _extract_hs_code_from_text(fields.get(key))
            if value:
                return value
    return ""


def _candidate_score(*, fields: dict[str, Any], analysis: dict[str, Any], source_name: str, tms_snapshot: dict[str, Any], order_id: str) -> int:
    score = 0
    order_blob = " ".join(_clean(fields.get(key)) for key in ("shipment_number", "order_id", "reference", "booking_reference", "customer_reference"))
    if _norm(order_id) and (_norm(order_id) in _norm(order_blob) or _norm(order_id) in _norm(source_name)):
        score += 60
    tms_container = _normalize_value(_first_path(tms_snapshot, (("detail", "freight_details", "container_number"), ("freight_details", "container_number"), ("container_number",))), "container")
    doc_container = _normalize_value(" ".join(_clean(fields.get(key)) for key in ("container_number", "container_no", "container")), "container")
    if tms_container and doc_container:
        score += 80 if _norm(tms_container) == _norm(doc_container) else -60
    elif tms_container and _norm(tms_container) in _norm(source_name):
        score += 40
    doc_type = str(analysis.get("doc_type") or "").lower()
    if doc_type in {"waybill", "bill_of_lading", "master_bl", "house_bl"}:
        score += 20
    elif doc_type in {"packing_list", "commercial_invoice", "customs_document"}:
        score += 10
    return score


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
    try:
        from .document_schema import normalize_document_type
    except Exception:
        normalize_document_type = None  # type: ignore[assignment]
    for candidate in (row.get("analysis_doc_type"), row.get("doc_type"), analysis.get("doc_type")):
        value = str(candidate or "").strip().lower()
        if value and value not in {"unknown", "email", "unbekannt"}:
            return str(normalize_document_type(value) if normalize_document_type else value).lower()
    suggested = analysis.get("suggested_registry_types")
    if isinstance(suggested, list):
        for candidate in suggested:
            value = str(candidate or "").strip().lower()
            if value and value not in {"unknown", "email", "unbekannt"}:
                return str(normalize_document_type(value) if normalize_document_type else value).lower()
    raw_fields = analysis.get("extracted_fields")
    fields: dict[str, Any] = raw_fields if isinstance(raw_fields, dict) else {}
    value = str(fields.get("document_type") or analysis.get("doc_type") or row.get("doc_type") or "").strip().lower()
    return str(normalize_document_type(value) if normalize_document_type else value).lower()


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


def _supersede_pending_conflicts(root: Path, order_id: str, target: str, value: str, *, operator: str | None, source: str) -> int:
    queue = root / "orders" / order_id / "teams" / "pending_tms_actions.jsonl"
    rows = _read_jsonl(queue)
    changed = 0
    if not rows:
        return 0
    now = None
    for row in rows:
        if str(row.get("status") or "") != "pending_review":
            continue
        if str(row.get("order_id") or "").strip().upper() != order_id:
            continue
        if str(row.get("target") or "").strip() != target:
            continue
        existing = _clean(row.get("value"))
        if existing and _norm(existing) != _norm(value):
            now = now or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            row["status"] = "superseded"
            row["superseded_at"] = now
            row["superseded_by_value"] = value
            row["superseded_reason"] = "stronger_agentic_evidence_delta"
            changed += 1
    if changed:
        _write_jsonl(queue, rows)
        _append_jsonl(root / "orders" / order_id / "audit" / "actions.jsonl", {
            "timestamp": now,
            "actor": operator or "Hermes Agentic Proposal Layer",
            "action": "teams_agent_tms_update_intent_superseded",
            "result": "superseded",
            "target": target,
            "value": value,
            "count": changed,
            "source": source,
            "files": [str(queue)],
        })
    return changed


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

    raw_candidates: list[dict[str, Any]] = []
    documents = _iter_evidence_documents(case_root, registry or {}, document_summary or {})
    for item in documents:
        row = item["row"]
        analysis = item["analysis"]
        fields = item["fields"]
        source_name = _clean(item.get("source")) or "Dokumentanalyse"
        doc_type = _doc_type(row, analysis)
        score = _candidate_score(fields=fields, analysis=analysis, source_name=source_name, tms_snapshot=tms_snapshot, order_id=normalized_order)
        for target, spec in _FIELD_SPECS.items():
            if target not in _SUPPORTED_TARGETS:
                continue
            trusted_types = spec.get("trusted_doc_types") or set()
            if doc_type and trusted_types and doc_type not in trusted_types:
                continue
            raw_doc = _raw_doc_value(fields, spec)
            value = _normalize_value(raw_doc, str(spec.get("kind") or "text"))
            if not value:
                continue
            raw_tms = _first_path(tms_snapshot, spec["tms_paths"])
            tms_value = _normalize_value(raw_tms, str(spec.get("kind") or "text"))
            if target in {"pol", "pod"} and tms_value:
                # POL/POD from documents are safe as missing-TMS review hints; do not
                # raise code-vs-city formatting deltas as actionable cards here.
                continue
            if tms_value and _same_value(tms_value, value, str(spec.get("kind") or "text")):
                continue
            raw_candidates.append({
                "target": target,
                "value": value,
                "previous_value": tms_value or raw_tms or "nicht gepflegt",
                "label": spec.get("label") or target,
                "evidence": f"{source_name} ({doc_type or 'Dokument'}): {spec.get('label') or target} = {value}",
                "source": source_name,
                "doc_type": doc_type,
                "caution": bool(spec.get("caution")),
                "write_supported": target in _WRITE_SUPPORTED_TARGETS,
                "score": score,
            })

    # Pick the strongest evidence per target instead of dropping a useful target
    # just because an older/other document carries a competing value.  If two
    # competing values are equally strong, skip that target as ambiguous.
    candidates: list[dict[str, Any]] = []
    for target in {str(item.get("target")) for item in raw_candidates}:
        grouped = [item for item in raw_candidates if str(item.get("target")) == target]
        grouped.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        if not grouped:
            continue
        top = grouped[0]
        runner_up = next((item for item in grouped[1:] if _norm(item.get("value")) != _norm(top.get("value"))), None)
        if runner_up and int(runner_up.get("score") or 0) >= int(top.get("score") or 0):
            continue
        candidates.append(top)
    candidates.sort(key=lambda item: (-int(item.get("score") or 0), _TARGET_PRIORITY.get(str(item.get("target")), 999)))
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
        key = (target, _norm(value))
        if key in seen:
            continue
        seen.add(key)
        duplicate, pending_conflict = _pending_duplicate_or_conflict(root, normalized_order, target, value)
        if duplicate:
            continue
        if pending_conflict:
            _supersede_pending_conflicts(root, normalized_order, target, value, operator=operator, source=source)
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
