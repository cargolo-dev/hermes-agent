from __future__ import annotations

from pathlib import Path
from typing import Any

from .evidence_freshness import (
    case_root_for,
    iso_from_mtime,
    latest_audit_history_sync,
    parse_iso,
    read_json,
    read_jsonl,
    source_is_stale,
    utc_now_iso,
)


def _source_entry(path: Path | None, *, available: bool, freshness_at: str | None, summary: dict[str, Any] | None = None, reason: str | None = None) -> dict[str, Any]:
    return {
        "available": available,
        "path": str(path) if path is not None else None,
        "updated_at": iso_from_mtime(path) if path is not None and path.exists() else None,
        "freshness_at": freshness_at,
        "summary": summary or {},
        "reason": reason,
    }


def _first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _document_analysis_path(case_root: Path, registry: dict[str, Any]) -> Path | None:
    pointer = registry.get("document_analysis_summary_path")
    if pointer:
        pointed = Path(str(pointer))
        if not pointed.is_absolute():
            pointed = case_root / pointed
        if pointed.exists():
            return pointed
    return _first_existing([
        case_root / "documents" / "analysis" / "latest_summary.json",
        case_root / "documents" / "analysis.json",
        case_root / "document_analysis.json",
    ])


def _case_state_entry(case_root: Path) -> dict[str, Any]:
    path = case_root / "case_state.json"
    data = read_json(path)
    if not data:
        return _source_entry(path, available=False, freshness_at=None, reason="case_state_missing")
    summary = {
        "mode": data.get("mode"),
        "current_status": data.get("current_status") or data.get("status"),
        "last_email_at": data.get("last_email_at"),
        "tms_last_sync_at": data.get("tms_last_sync_at"),
        "open_questions": data.get("open_questions") or [],
    }
    return _source_entry(path, available=True, freshness_at=data.get("tms_last_sync_at") or iso_from_mtime(path), summary=summary)


def _tms_entry(case_root: Path, case_state: dict[str, Any]) -> dict[str, Any]:
    path = _first_existing([case_root / "tms_snapshot.json", case_root / "tms" / "snapshot.json", case_root / "tms.json"])
    if not path:
        return _source_entry(case_root / "tms_snapshot.json", available=False, freshness_at=None, reason="tms_snapshot_missing")
    data = read_json(path)
    if not data:
        return _source_entry(path, available=False, freshness_at=None, reason="tms_snapshot_empty")
    summary = {
        "source": data.get("source"),
        "status": data.get("status") or data.get("shipment_status") or data.get("current_status"),
        "eta": data.get("eta") or data.get("estimated_delivery_date"),
        "etd": data.get("etd") or data.get("estimated_departure_date"),
        "pod": data.get("pod") or data.get("destination_port"),
        "pol": data.get("pol") or data.get("origin_port"),
        "carrier_reference": data.get("carrier_reference") or data.get("tracking_number"),
        "warnings": data.get("warnings") or [],
    }
    freshness = data.get("fetched_at") or case_state.get("tms_last_sync_at") or iso_from_mtime(path)
    return _source_entry(path, available=True, freshness_at=freshness, summary=summary)


def _email_entry(case_root: Path, case_state: dict[str, Any]) -> dict[str, Any]:
    path = case_root / "email_index.jsonl"
    if not path.exists():
        return _source_entry(path, available=False, freshness_at=None, reason="email_index_missing")
    rows = read_jsonl(path)
    rows.sort(key=lambda row: str(row.get("received_at") or ""), reverse=True)
    latest = rows[0] if rows else {}
    audit = latest_audit_history_sync(case_root)
    if not rows and not audit:
        return _source_entry(path, available=False, freshness_at=None, reason="email_index_empty")
    freshness = (audit or {}).get("timestamp") or case_state.get("last_email_at") or latest.get("received_at") or iso_from_mtime(path)
    summary = {
        "message_count": len(rows),
        "latest_received_at": latest.get("received_at") or case_state.get("last_email_at"),
        "latest_subject": latest.get("subject"),
        "latest_from": latest.get("from") or latest.get("sender"),
        "history_sync_status": (audit or {}).get("history_sync_status"),
        "history_sync_mode": (audit or {}).get("history_sync_mode"),
    }
    return _source_entry(path, available=True, freshness_at=freshness, summary=summary)


def _registry_entry(case_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    path = case_root / "documents" / "registry.json"
    registry = read_json(path)
    if not registry:
        return _source_entry(path, available=False, freshness_at=None, reason="document_registry_missing"), {}
    summary = {
        "received_types": registry.get("received_types") or [],
        "expected_types": registry.get("expected_types") or [],
        "missing_types": registry.get("missing_types") or [],
        "received_documents": len(registry.get("received_documents") or []),
        "tms_documents": len(registry.get("tms_documents") or []),
        "tms_mirroring_gaps": len(registry.get("tms_mirroring_gaps") or []),
        "analysis_generated_at": registry.get("analysis_generated_at"),
    }
    freshness = registry.get("updated_at") or iso_from_mtime(path)
    return _source_entry(path, available=True, freshness_at=freshness, summary=summary), registry


def _analysis_entry(case_root: Path, registry: dict[str, Any]) -> dict[str, Any]:
    path = _document_analysis_path(case_root, registry)
    if not path:
        return _source_entry(case_root / "documents" / "analysis" / "latest_summary.json", available=False, freshness_at=None, reason="document_analysis_missing")
    data = read_json(path)
    if not data:
        return _source_entry(path, available=False, freshness_at=None, reason="document_analysis_empty")
    docs = data.get("documents") or data.get("analyzed_documents") or []
    summary = {
        "summary": data.get("summary") or data.get("brief"),
        "document_count": len(docs) if isinstance(docs, list) else 0,
        "open_questions": data.get("open_questions") or data.get("analysis_open_questions") or [],
        "missing_types": data.get("missing_types") or [],
    }
    freshness = data.get("generated_at") or registry.get("analysis_generated_at") or iso_from_mtime(path)
    return _source_entry(path, available=True, freshness_at=freshness, summary=summary)


def _billing_entry(case_root: Path, tms_entry: dict[str, Any]) -> dict[str, Any]:
    path = case_root / "tms" / "billing_context.json"
    data = read_json(path)
    source_path: Path | None = path if data else None
    if not data:
        tms_path_value = tms_entry.get("path")
        if tms_path_value:
            tms_path = Path(str(tms_path_value))
            tms_data = read_json(tms_path)
        else:
            tms_path = None
            tms_data = {}
        embedded = tms_data.get("billing_context") if isinstance(tms_data.get("billing_context"), dict) else {}
        if embedded:
            data = embedded
            source_path = tms_path
    if not data:
        return _source_entry(path, available=False, freshness_at=None, reason="billing_context_missing")
    summary = {
        "status": data.get("status"),
        "total_vk": data.get("total_vk") or data.get("vk_total"),
        "total_ek": data.get("total_ek") or data.get("ek_total"),
        "margin": data.get("margin") or data.get("gross_margin"),
        "currency": data.get("currency") or "EUR",
    }
    return _source_entry(source_path, available=True, freshness_at=data.get("generated_at") or data.get("fetched_at") or tms_entry.get("freshness_at"), summary=summary)


def _compact_text(value: Any, limit: int = 500) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit] + ("…" if len(text) > limit else "")


def _teams_entry(case_root: Path) -> dict[str, Any]:
    path = _first_existing([case_root / "teams" / "thread_context.json", case_root / "teams_thread_context.json"])
    if not path:
        return _source_entry(None, available=False, freshness_at=None, reason="no canonical local Teams thread artifact")
    data = read_json(path)
    if not data:
        return _source_entry(path, available=False, freshness_at=None, reason="teams_thread_context_empty")
    recent_raw = data.get("recent_messages") or data.get("messages") or []
    recent = []
    for item in recent_raw[-4:] if isinstance(recent_raw, list) else []:
        if not isinstance(item, dict):
            continue
        recent.append({
            "role": item.get("role"),
            "order_id": item.get("order_id"),
            "text": _compact_text(item.get("text"), 350),
            "timestamp": item.get("timestamp"),
        })
    last_user = data.get("last_user_message") if isinstance(data.get("last_user_message"), dict) else {}
    last_response = data.get("last_hermes_response") if isinstance(data.get("last_hermes_response"), dict) else {}
    summary = {
        "message_count": len(recent_raw) if isinstance(recent_raw, list) else 0,
        "last_order_id": data.get("last_order_id"),
        "last_user_message_text": _compact_text(last_user.get("text"), 500),
        "last_hermes_response_text": _compact_text(last_response.get("text"), 700),
        "recent_messages": recent,
        "open_references": data.get("open_references") or {},
    }
    return _source_entry(path, available=True, freshness_at=data.get("updated_at") or data.get("generated_at") or iso_from_mtime(path), summary=summary)


def _pricing_entry(case_root: Path) -> dict[str, Any]:
    path = _first_existing([
        case_root / "pricing" / "knowledge_context.json",
        case_root / "pricing_kb.json",
        case_root / "pricing" / "pricing_kb.json",
    ])
    if not path:
        return _source_entry(case_root / "pricing" / "knowledge_context.json", available=False, freshness_at=None, reason="pricing_kb_missing")
    data = read_json(path)
    if not data:
        return _source_entry(path, available=False, freshness_at=None, reason="pricing_kb_empty")
    summary = {
        "offer_count": data.get("offer_count") or len(data.get("offers") or []),
        "lane_history_hits": data.get("lane_history_hits"),
        "price_position": data.get("price_position"),
        "margin": data.get("margin") or data.get("gross_margin"),
        "currency": data.get("currency") or "EUR",
    }
    return _source_entry(path, available=True, freshness_at=data.get("generated_at") or data.get("updated_at") or iso_from_mtime(path), summary=summary)


def build_source_entries(case_root: Path) -> dict[str, dict[str, Any]]:
    case_state_data = read_json(case_root / "case_state.json")
    case_entry = _case_state_entry(case_root)
    tms_entry = _tms_entry(case_root, case_state_data)
    registry_entry, registry = _registry_entry(case_root)
    return {
        "case_state": case_entry,
        "tms_snapshot": tms_entry,
        "email_index": _email_entry(case_root, case_state_data),
        "document_registry": registry_entry,
        "document_analysis": _analysis_entry(case_root, registry),
        "pricing_kb": _pricing_entry(case_root),
        "billing_context": _billing_entry(case_root, tms_entry),
        "teams_thread_context": _teams_entry(case_root),
    }


def _stale_sources_for(sources: dict[str, dict[str, Any]], *, now: str | None = None) -> list[str]:
    stale: list[str] = []
    registry_ts = sources.get("document_registry", {}).get("freshness_at")
    analysis_ts = sources.get("document_analysis", {}).get("freshness_at")
    for name, entry in sources.items():
        if not entry.get("available"):
            continue
        is_stale = source_is_stale(name, entry.get("freshness_at"), now=now)
        if name == "document_analysis":
            reg_dt = parse_iso(registry_ts)
            ana_dt = parse_iso(analysis_ts)
            if reg_dt and ana_dt and ana_dt < reg_dt:
                is_stale = True
        if is_stale:
            stale.append(name)
    return stale


def _source_limitation(source: str, *, stale: bool = False) -> str | None:
    suffix = "ist lokal veraltet und nur mit Vorbehalt belastbar." if stale else "ist lokal nicht belastbar verfügbar."
    labels = {
        "case_state": "Case-State",
        "tms_snapshot": "TMS-Stand",
        "email_index": "Mailhistorie",
        "document_registry": "Dokumentregistry",
        "document_analysis": "Dokumentanalyse",
        "pricing_kb": "Pricing-KB/Preisposition",
        "billing_context": "Billing-Kontext",
        "teams_thread_context": "Teams-Kontext",
    }
    label = labels.get(source)
    return f"{label} {suffix}" if label else None


def build_evidence_bundle(
    order_id: str,
    *,
    storage_root: Path | None = None,
    question: str | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    normalized = str(order_id).upper()
    case_root = case_root_for(normalized, storage_root)
    if not case_root.exists():
        return {
            "version": 1,
            "status": "missing_case",
            "order_id": normalized,
            "question": question,
            "generated_at": now or utc_now_iso(),
            "case_root": str(case_root),
            "sources": {},
            "missing_sources": ["case_state", "tms_snapshot", "email_index", "document_registry", "document_analysis", "pricing_kb", "billing_context"],
            "stale_sources": [],
            "source_limitations": ["Lokaler Case fehlt; keine operative Aussage aus lokalen Quellen möglich."],
            "evidence_refs": [],
        }
    sources = build_source_entries(case_root)
    missing = [name for name, entry in sources.items() if not entry.get("available")]
    stale = _stale_sources_for(sources, now=now)
    limitations = []
    for source in missing:
        limitation = _source_limitation(source)
        if limitation:
            limitations.append(limitation)
    for source in stale:
        limitation = _source_limitation(source, stale=True)
        if limitation:
            limitations.append(limitation)
    refs = [
        {"source": name, "path": entry.get("path"), "freshness_at": entry.get("freshness_at")}
        for name, entry in sources.items()
        if entry.get("available") and entry.get("path")
    ]
    return {
        "version": 1,
        "status": "ok",
        "order_id": normalized,
        "question": question,
        "generated_at": now or utc_now_iso(),
        "case_root": str(case_root),
        "sources": sources,
        "missing_sources": missing,
        "stale_sources": stale,
        "source_limitations": limitations,
        "evidence_refs": refs,
    }
