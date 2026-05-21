from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from hermes_constants import get_hermes_home


class EvidenceNeed(str, Enum):
    CASE_FOLDER = "case_folder"
    MAIL_HISTORY = "mail_history"
    TMS_SNAPSHOT = "tms_snapshot"
    DOCUMENTS = "documents"
    PRICING_KB = "pricing_kb"
    BILLING_CONTEXT = "billing_context"
    TEAMS_THREAD = "teams_thread"


SOURCE_FOR_NEED = {
    EvidenceNeed.CASE_FOLDER: ["case_state"],
    EvidenceNeed.TMS_SNAPSHOT: ["tms_snapshot"],
    EvidenceNeed.MAIL_HISTORY: ["email_index"],
    EvidenceNeed.DOCUMENTS: ["document_registry", "document_analysis"],
    EvidenceNeed.PRICING_KB: ["pricing_kb"],
    EvidenceNeed.BILLING_CONTEXT: ["billing_context"],
    EvidenceNeed.TEAMS_THREAD: ["teams_thread_context"],
}

DEFAULT_TTL_SECONDS = {
    "case_state": None,
    "tms_snapshot": 15 * 60,
    "email_index": 30 * 60,
    "document_registry": None,
    "document_analysis": None,
    "pricing_kb": 24 * 60 * 60,
    "billing_context": 60 * 60,
    "teams_thread_context": None,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def iso_from_mtime(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def case_root_for(order_id: str, storage_root: Path | None = None) -> Path:
    root = Path(storage_root) if storage_root is not None else get_hermes_home() / "cargolo_asr"
    return root / "orders" / str(order_id).upper()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        if not path.exists():
            return rows
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                rows.append(value)
    except Exception:
        return rows
    return rows


def latest_audit_history_sync(case_root: Path) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for row in read_jsonl(case_root / "audit" / "actions.jsonl"):
        action = str(row.get("action") or "")
        if "sync_case_lifecycle" not in action:
            continue
        if row.get("history_sync_status") in {"ok", "no_messages", "fresh_skipped"}:
            candidates.append(row)
    candidates.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
    return candidates[0] if candidates else None


def required_sources_for(needs: list[EvidenceNeed] | list[str] | None, question: str | None = None) -> list[str]:
    result: list[str] = []
    normalized: list[EvidenceNeed] = []
    for need in needs or []:
        try:
            normalized.append(need if isinstance(need, EvidenceNeed) else EvidenceNeed(str(need)))
        except ValueError:
            continue
    lowered = (question or "").lower()
    if not normalized and any(token in lowered for token in ("an-", "bu-")):
        normalized.append(EvidenceNeed.CASE_FOLDER)
    if any(token in lowered for token in ("eta", "etd", "status", "stand", "lage")):
        normalized.append(EvidenceNeed.TMS_SNAPSHOT)
    if any(token in lowered for token in ("kunde", "geantwortet", "antwort", "mail", "email")):
        normalized.append(EvidenceNeed.MAIL_HISTORY)
    if any(token in lowered for token in ("fehlt", "dokument", "doc", "sauber", "freigabe", "release")):
        normalized.append(EvidenceNeed.DOCUMENTS)
    if any(token in lowered for token in ("rechnung", "billing", "kosten", "marge", "sauber")):
        normalized.append(EvidenceNeed.BILLING_CONTEXT)
    for need in normalized:
        for source in SOURCE_FOR_NEED.get(need, []):
            if source not in result:
                result.append(source)
    if "case_state" not in result:
        result.insert(0, "case_state")
    return result


def source_is_stale(source: str, freshness_at: str | None, *, now: str | None = None) -> bool:
    ttl = DEFAULT_TTL_SECONDS.get(source)
    if ttl is None:
        return False
    stamp = parse_iso(freshness_at)
    current = parse_iso(now) or datetime.now(timezone.utc)
    if not stamp:
        return True
    return (current - stamp).total_seconds() > ttl


def collect_source_statuses(case_root: Path, *, now: str | None = None) -> dict[str, dict[str, Any]]:
    # Local import avoids a circular import at module load time.
    from .evidence_bundle import build_source_entries

    bundle_sources = build_source_entries(case_root)
    statuses: dict[str, dict[str, Any]] = {}
    registry_ts = bundle_sources.get("document_registry", {}).get("freshness_at")
    analysis_ts = bundle_sources.get("document_analysis", {}).get("freshness_at")
    for source, entry in bundle_sources.items():
        available = bool(entry.get("available"))
        status = "fresh" if available else "missing"
        if available and source_is_stale(source, entry.get("freshness_at"), now=now):
            status = "stale"
        if source == "document_analysis" and available:
            reg_dt = parse_iso(registry_ts)
            ana_dt = parse_iso(analysis_ts)
            if reg_dt and ana_dt and ana_dt < reg_dt:
                status = "stale"
        statuses[source] = {
            "available": available,
            "status": status,
            "freshness_at": entry.get("freshness_at"),
            "path": entry.get("path"),
            "refresh_needed": False,
            "reason": entry.get("reason"),
        }
    return statuses


def plan_evidence_refresh(
    order_id: str,
    *,
    storage_root: Path | None = None,
    question: str | None = None,
    requested_needs: list[EvidenceNeed] | list[str] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    case_root = case_root_for(order_id, storage_root)
    required = required_sources_for(requested_needs, question)
    statuses = collect_source_statuses(case_root, now=now) if case_root.exists() else {}
    caveats: list[str] = []
    for source in required:
        status = statuses.get(source, {"available": False, "status": "missing", "freshness_at": None, "path": None})
        if status["status"] in {"missing", "stale"} and source != "teams_thread_context":
            status["refresh_needed"] = True
        if source == "email_index" and status["status"] in {"missing", "stale"}:
            caveats.append("Mailhistorie ist nicht frisch belastbar und muss aktualisiert oder mit Vorbehalt genutzt werden.")
        elif source == "document_registry" and status["status"] in {"missing", "stale"}:
            caveats.append("Dokumentstatus ist lokal nicht vollständig belastbar.")
        elif source == "document_analysis" and status["status"] in {"missing", "stale"}:
            caveats.append("Dokumentanalyse fehlt oder ist älter als die Registry.")
        elif source == "billing_context" and status["status"] in {"missing", "stale"}:
            caveats.append("Billing-Kontext ist nicht frisch belastbar.")
        elif source == "pricing_kb" and status["status"] in {"missing", "stale"}:
            caveats.append("Pricing-KB/Preisposition ist lokal nicht frisch belastbar.")
        elif source == "tms_snapshot" and status["status"] in {"missing", "stale"}:
            caveats.append("TMS-Stand ist nicht frisch belastbar.")
        statuses[source] = status
    refresh_sources = [source for source, status in statuses.items() if source in required and status.get("refresh_needed")]
    return {
        "order_id": str(order_id).upper(),
        "generated_at": now or utc_now_iso(),
        "case_root": str(case_root),
        "required_sources": required,
        "sources": statuses,
        "refresh_sources": refresh_sources,
        "requires_refresh": bool(refresh_sources),
        "refresh_history": "email_index" in refresh_sources,
        "analyze_documents": any(source in refresh_sources for source in ("document_registry", "document_analysis")),
        "caveats": caveats,
    }
