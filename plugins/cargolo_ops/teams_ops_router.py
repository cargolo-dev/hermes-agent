"""CARGOLO Teams Ops Router.

Small deterministic front-door for native Teams messages that are not already
card replies. It turns Teams into a safe CARGOLO ops surface without making
free-text TMS writes.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .models import utc_now_iso

try:
    from .tms_provider import build_tms_provider_from_env
except Exception:  # pragma: no cover - optional live integration fallback
    build_tms_provider_from_env = None  # type: ignore[assignment]

try:
    from hermes_constants import get_hermes_home
except Exception:  # pragma: no cover - test/import fallback
    def get_hermes_home() -> Path:  # type: ignore[no-redef]
        return Path.home() / ".hermes"


_ORDER_RE = re.compile(r"\b(?:AN|BU)-\d{3,}\b", re.IGNORECASE)
_STATUS_RE = re.compile(r"\b(status|health|readiness|zustand|system|läuft|laeuft|cron)\b", re.IGNORECASE)
_PENDING_RE = re.compile(r"\b(offene?|pending|freigaben?|review|tms[-\s]?freigaben?)\b", re.IGNORECASE)
_CASE_CHECK_RE = re.compile(r"\b(prüf(?:e|en)?|pruef(?:e|en)?|(?<!-)check|aktualisier(?:e|en)?|sync|zieh(?:e|en)?)\b", re.IGNORECASE)
_FULL_CASE_RE = re.compile(r"\b(?:gib|geb|zeig|sag|hol|hole)\b.*\b(?:alles|lage|stand|komplett|übersicht|uebersicht)\b|\b(?:alles|lage|stand|komplett|übersicht|uebersicht)\b.*\b(?:zu|für|fuer)\b", re.IGNORECASE)
_WRITE_RE = re.compile(r"\b(schreib(?:e|en)?|setz(?:e|en)?|eintragen|ändern|aendern|update|aktualisier(?:e|en)?)\b", re.IGNORECASE)
_TMS_FIELD_RE = re.compile(r"\b(TMS|MRN|HBL|MBL|HAWB|Container|Container[-\s]?Nr|Zollreferenz|customs)\b", re.IGNORECASE)
_MRN_VALUE_RE = re.compile(r"\b([0-9]{2}[A-Z]{2}[A-Z0-9]{3,})\b", re.IGNORECASE)


def default_case_root() -> Path:
    return get_hermes_home() / "cargolo_asr"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
    return rows


def _load_cron_jobs() -> list[dict[str, Any]]:
    path = get_hermes_home() / "cron" / "jobs.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        jobs = data.get("jobs")
        if isinstance(jobs, list):
            return [item for item in jobs if isinstance(item, dict)]
        return [item for item in data.values() if isinstance(item, dict)]
    return []


def _pending_action_id_for_row(item: dict[str, Any]) -> str:
    raw = "|".join([
        str(item.get("order_id") or "").strip().upper(),
        str(item.get("target") or "").strip(),
        str(item.get("value") or "").strip(),
        str(item.get("context_id") or "").strip(),
        str(item.get("timestamp") or item.get("created_at") or "").strip(),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


_SUPPORTED_TMS_REVIEW_TARGETS = {"customs_reference", "hbl_number", "mbl_number", "hawb_number", "container_number", "pickup_date", "estimated_delivery_date"}


def _collect_pending_tms_actions(root: Path, limit: int = 8) -> list[dict[str, Any]]:
    orders_root = root / "orders"
    if not orders_root.exists():
        return []
    pending: list[dict[str, Any]] = []
    for path in sorted(orders_root.glob("*/teams/pending_tms_actions.jsonl")):
        for item in _read_jsonl(path):
            if str(item.get("status") or "").lower() != "pending_review":
                continue
            target = str(item.get("target") or "").strip()
            value = str(item.get("value") or "").strip()
            order_id = str(item.get("order_id") or "").strip().upper()
            if target not in _SUPPORTED_TMS_REVIEW_TARGETS or not value or not _ORDER_RE.fullmatch(order_id):
                continue
            enriched = dict(item)
            if not str(enriched.get("action_id") or "").strip():
                enriched["action_id"] = _pending_action_id_for_row(enriched)
                enriched["action_id_derived"] = True
            pending.append(enriched)
    pending.sort(key=lambda x: str(x.get("timestamp") or ""), reverse=True)
    return pending[:limit]


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _collect_correction_requested_tms_actions(root: Path, limit: int = 8) -> list[dict[str, Any]]:
    orders_root = root / "orders"
    if not orders_root.exists():
        return []
    corrections: list[dict[str, Any]] = []
    for path in sorted(orders_root.glob("*/teams/pending_tms_actions.jsonl")):
        for item in _read_jsonl(path):
            if str(item.get("status") or "").lower() != "correction_requested":
                continue
            target = str(item.get("target") or "").strip()
            order_id = str(item.get("order_id") or "").strip().upper()
            if target not in _SUPPORTED_TMS_REVIEW_TARGETS or not _ORDER_RE.fullmatch(order_id):
                continue
            corrections.append(dict(item))
    corrections.sort(key=lambda x: str(x.get("correction_requested_at") or x.get("timestamp") or ""), reverse=True)
    return corrections[:limit]


def _extract_correction_value(raw: str, *, target: str) -> str | None:
    text = str(raw or "")
    if target == "customs_reference":
        match = _MRN_VALUE_RE.search(text)
        return match.group(1).upper() if match else None
    field_patterns = {
        "hbl_number": r"\bHBL\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{2,})\b",
        "mbl_number": r"\bMBL\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{2,})\b",
        "hawb_number": r"\bHAWB\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{2,})\b",
        "container_number": r"\b([A-Z]{4}\d{7})\b",
        "pickup_date": r"\b(\d{4}-\d{2}-\d{2})\b",
        "estimated_delivery_date": r"\b(\d{4}-\d{2}-\d{2})\b",
    }
    pattern = field_patterns.get(target)
    if not pattern:
        return None
    match = re.search(pattern, text, flags=re.IGNORECASE)
    return match.group(1).upper() if match else None


def _has_pending_review_value(root: Path, *, order_id: str, target: str, value: str) -> bool:
    queue_path = root / "orders" / order_id / "teams" / "pending_tms_actions.jsonl"
    for item in _read_jsonl(queue_path):
        if str(item.get("status") or "") != "pending_review":
            continue
        if str(item.get("order_id") or "").strip().upper() != order_id:
            continue
        if str(item.get("target") or "").strip() == target and str(item.get("value") or "").strip().upper() == value.upper():
            return True
    return False


def _record_correction_followup(
    *,
    root: Path,
    correction: dict[str, Any],
    value: str,
    text: str,
    user_id: str | None,
    user_name: str | None,
    message_id: str | None,
) -> dict[str, Any]:
    order_id = str(correction.get("order_id") or "").strip().upper()
    target = str(correction.get("target") or "").strip()
    now = utc_now_iso()
    context_id = str(correction.get("context_id") or f"{order_id}:correction").strip()
    row = {
        "timestamp": now,
        "action_id": _pending_action_id_for_row({
            "order_id": order_id,
            "target": target,
            "value": value,
            "context_id": context_id,
            "timestamp": now,
        }),
        "status": "pending_review",
        "order_id": order_id,
        "context_id": context_id,
        "activity_id": correction.get("activity_id"),
        "target": target,
        "value": value,
        "previous_value": correction.get("value"),
        "correction_of_action_id": correction.get("action_id"),
        "confidence": "operator_correction_followup",
        "source": "teams_correction_followup",
        "source_message_id": message_id,
        "reply_to_message_id": None,
        "operator": user_name,
        "operator_user_id": user_id,
        "text": text,
        "write_policy": "no_auto_write_without_review",
    }
    teams_dir = root / "orders" / order_id / "teams"
    _append_jsonl(teams_dir / "pending_tms_actions.jsonl", row)
    _append_jsonl(teams_dir / "case_learning.jsonl", {
        "timestamp": now,
        "source": "teams_correction_followup",
        "order_id": order_id,
        "operator": user_name,
        "classification": "tms_correction_followup",
        "learning": text,
        "context_id": context_id,
        "derived_action": {"type": "pending_tms_update", "target": target, "value": value, "previous_value": correction.get("value")},
    })
    _append_jsonl(root / "orders" / order_id / "audit" / "actions.jsonl", {
        "timestamp": now,
        "actor": user_name or "Teams Operator",
        "action": "teams_tms_correction_followup_recorded",
        "result": "pending_review",
        "target": target,
        "value": value,
        "previous_value": correction.get("value"),
        "order_id": order_id,
        "files": [str(teams_dir / "pending_tms_actions.jsonl")],
    })
    return row


def _route_correction_followup(
    *,
    raw: str,
    root: Path,
    order_id: str | None,
    user_id: str | None,
    user_name: str | None,
    message_id: str | None,
) -> dict[str, Any] | None:
    corrections = _collect_correction_requested_tms_actions(root, limit=8)
    if order_id:
        corrections = [item for item in corrections if str(item.get("order_id") or "").strip().upper() == order_id]
    if len(corrections) != 1:
        return None
    correction = corrections[0]
    target = str(correction.get("target") or "").strip()
    value = _extract_correction_value(raw, target=target)
    if not value:
        return None
    old_value = str(correction.get("value") or "").strip().upper()
    normalized_order = str(correction.get("order_id") or "").strip().upper()
    if value == old_value:
        return {
            "handled": True,
            "classification": "correction_followup_unchanged",
            "order_id": normalized_order,
            "response_text": (
                f"Ich sehe denselben Wert wie vorher ({target} = {value}). "
                "Bitte sende den korrigierten neuen Wert oder lehne die Freigabe ab. Kein TMS-Write."
            ),
        }
    if _has_pending_review_value(root, order_id=normalized_order, target=target, value=value):
        return {
            "handled": True,
            "classification": "correction_followup_duplicate",
            "order_id": normalized_order,
            "response_text": f"ℹ️ Für {normalized_order} ist {target} = {value} bereits als offene TMS-Freigabe vorgemerkt.",
        }
    row = _record_correction_followup(
        root=root,
        correction=correction,
        value=value,
        text=raw,
        user_id=user_id,
        user_name=user_name,
        message_id=message_id,
    )
    return {
        "handled": True,
        "classification": "correction_followup_recorded",
        "order_id": normalized_order,
        "response_text": (
            f"✏️ Korrektur übernommen für {normalized_order}: {target} = {value} ist wieder als pending_review vorgemerkt. "
            "Kein TMS-Write; bitte die neue Karte bestätigen oder ablehnen."
        ),
        "teams_tms_review_cards": [row],
    }


def _status_response(root: Path) -> str:
    jobs = _load_cron_jobs()
    doc_jobs = [j for j in jobs if "cargolo" in str(j.get("name") or j.get("job_id") or "").lower()]
    pending = _collect_pending_tms_actions(root, limit=99)
    if doc_jobs:
        first = doc_jobs[0]
        cron_line = f"{first.get('name') or first.get('job_id')} · {first.get('state') or first.get('status') or 'unbekannt'} · last={first.get('last_status') or 'n/a'}"
    elif jobs:
        cron_line = f"{len(jobs)} Cronjobs gefunden"
    else:
        cron_line = "keine Cronjobs im lokalen Jobs-File gefunden"

    return (
        "CARGOLO Teams Ops · Status\n"
        "Lage: Teams ist verbunden, Router aktiv.\n"
        f"Dokumenten-Monitor: {cron_line}\n"
        f"Offene TMS-Freigaben: {len(pending)}\n"
        "Nächster Schritt: `offene Freigaben` oder `prüfe AN-12345 komplett`."
    )


def _pending_response(root: Path) -> str:
    pending = _collect_pending_tms_actions(root)
    if not pending:
        return "CARGOLO Teams Ops · Offene TMS-Freigaben\n- Keine pending_review TMS-Aktionen gefunden."
    lines = ["CARGOLO Teams Ops · Offene TMS-Freigaben"]
    for item in pending:
        order = item.get("order_id") or "AN/BU?"
        target = item.get("target") or "Feld?"
        value = item.get("value") or "Wert?"
        operator = item.get("operator") or "unbekannt"
        lines.append(f"- {order}: {target} = {value} · von {operator}")
    lines.append("Nächster Schritt: Auf die konkrete Karte antworten mit `freigeben` oder `ablehnen: Grund`.")
    return "\n".join(lines)


def _live_shipment_exists(order_id: str) -> bool | None:
    """Return True/False only when the live TMS lookup is authoritative.

    CARGOLO Teams requests are TMS-first: AN/BU numbers that are not present in
    the ASR TMS must not fall through to the generic agent, because the generic
    agent may otherwise try repeated n8n mail-history searches for a non-case.
    None means the live provider itself is unavailable/uncertain, so the router
    keeps the existing degraded behaviour instead of blocking a real case.
    """
    if build_tms_provider_from_env is None:
        return None
    provider = build_tms_provider_from_env()
    if provider is None or not hasattr(provider, "shipments_list"):
        return None
    normalized = str(order_id or "").strip().upper()
    if not normalized:
        return None
    try:
        rows = provider.shipments_list(
            transport_category="asr",
            shipment_number=normalized,
            limit=20,
        )
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, dict):
            return None
        if str(row.get("shipment_number") or "").strip().upper() == normalized:
            return True
    return False


def _unknown_shipment_response(order_id: str) -> dict[str, Any]:
    return {
        "handled": True,
        "classification": "shipment_not_found_in_tms",
        "order_id": order_id,
        "response_text": (
            f"{order_id} finde ich nicht im ASR-TMS.\n"
            "Lage: Ich stoppe hier TMS-first und starte keine Mail-/n8n-Suche.\n"
            "Nächster Schritt: AN/BU prüfen oder die Sendung zuerst im TMS anlegen/finden."
        ),
    }


def _coordinator_prompt(text: str, *, order_id: str | None = None, intent: str = "general_ops") -> str:
    order_line = f"Case: {order_id}. " if order_id else "Case: nicht eindeutig. "
    return (
        "Rolle: Du bist Hermes CARGOLO als ASR Ops Coordinator in Microsoft Teams — ein interner, proaktiver Mitarbeiter.\n"
        f"{order_line}Intent: {intent}.\n"
        "Arbeitsweise: TMS-first. Jede AN/BU muss zuerst live im ASR-TMS existieren; wenn nicht, sofort sagen `nicht im TMS zu finden` und keine n8n-/Mail-Historie suchen. "
        "Erst nach positivem TMS-Fund bei Bedarf CARGOLO Skills, Case-Folder, TMS/MCP-Kontext, Mail-Historie, Dokumentregistry, Cron-/Plugin-Status heranziehen. "
        "Bleibe read-only, außer ein freigegebener zweistufiger Write-Pfad ist eindeutig aktiv.\n"
        "Sicherheit: Freitext in Teams darf keine direkten TMS-, Angebots- oder Kundenmail-Writes auslösen. "
        "TMS-Änderungswünsche nur über `cargolo_asr_record_teams_tms_intent` als pending_review vormerken; bei Unklarheit genau eine Rückfrage stellen.\n"
        "Antwortstil: Deutsch, kurz, operativ: Gemacht/Lage/Auffälligkeit/Nächster Schritt. Keine Audit-Dumps, keine KI-Floskeln.\n"
        f"Teams-Nachricht: {text.strip()}"
    )


def _run_local_case_deep_dive(*, root: Path, order_id: str, text: str, user_name: str | None = None) -> dict[str, Any]:
    """Refresh the canonical local case first, then synthesize a read-only Teams answer."""
    try:
        from .case_lifecycle import sync_case_lifecycle
        from .employee_agent import EmployeeRequest
        from .employee_runtime import run_employee_runtime
    except Exception as exc:  # pragma: no cover - import degradation only
        return {
            "handled": True,
            "classification": "case_deep_dive_unavailable",
            "order_id": order_id,
            "response_text": f"⚠️ Fallprüfung für {order_id} konnte nicht gestartet werden: {exc}",
        }

    lifecycle: dict[str, Any] | None = None
    lifecycle_error: str | None = None
    try:
        lifecycle = sync_case_lifecycle(
            order_id,
            storage_root=root,
            refresh_history=True,
            analyze_documents=True,
        )
    except Exception as exc:
        lifecycle_error = str(exc)

    request_text = (
        f"Gib mir alles zu {order_id}: komplette lesende Lage aus dem soeben synchronisierten lokalen Case, "
        "aktueller TMS-Status/Stand, Mail-Historie, Dokumente, Billing/Pricing-Kontext falls vorhanden, "
        "Auffälligkeiten und konkreter nächster Schritt. Rein interne Antwort; keine externe Aktion auslösen. "
        f"Originalfrage: {text}"
    )
    runtime_result = run_employee_runtime(
        EmployeeRequest(text=request_text, channel="teams", order_id=order_id, actor=user_name),
        root=root,
    )
    response_text = runtime_result.draft_response or f"Lage: {order_id} | Case lokal aktualisiert, keine externe Aktion ausgeführt."
    if lifecycle_error:
        response_text = f"⚠️ Vorab-Sync für {order_id} nicht vollständig: {lifecycle_error}\n\n{response_text}"
    return {
        "handled": True,
        "classification": "case_deep_dive_local_refresh",
        "order_id": order_id,
        "response_text": response_text,
        "case_path": str(root / "orders" / order_id),
        "lifecycle": lifecycle or {"status": "error", "error": lifecycle_error},
        "result_path": runtime_result.result_path,
    }


def route_teams_ops_message(
    *,
    text: str,
    root: Path | None = None,
    chat_id: str | None = None,
    user_id: str | None = None,
    user_name: str | None = None,
    message_id: str | None = None,
    paperclip_bridge_enabled: bool = False,
) -> dict[str, Any]:
    """Route non-card CARGOLO Teams messages.

    Returns one of:
    - {handled: True, response_text: ...} for deterministic safe status/list commands
    - {handled: False, allow_generic_chat: True, agent_prompt: ...} for agent work
    - {handled: False} for unrelated Teams chat
    """
    del chat_id  # reserved for audit extension
    raw = str(text or "").strip()
    if not raw:
        return {"handled": False}
    case_root = root or default_case_root()
    lowered = raw.lower()
    order_match = _ORDER_RE.search(raw)
    order_id = order_match.group(0).upper() if order_match else None

    if order_id:
        live_exists = _live_shipment_exists(order_id)
        if live_exists is False:
            return _unknown_shipment_response(order_id)

    correction_followup = _route_correction_followup(
        raw=raw,
        root=case_root,
        order_id=order_id,
        user_id=user_id,
        user_name=user_name,
        message_id=message_id,
    )
    if correction_followup:
        return correction_followup

    # Deterministic, read-only commands for the operations surface.
    if _STATUS_RE.search(raw) and ("cargolo" in lowered or "asr" in lowered or raw.strip().lower() in {"status", "health", "cron status"}):
        return {"handled": True, "classification": "ops_status", "response_text": _status_response(case_root)}

    if _PENDING_RE.search(raw) and ("freig" in lowered or "pending" in lowered or "review" in lowered):
        pending = _collect_pending_tms_actions(case_root)
        return {
            "handled": True,
            "classification": "pending_tms_reviews",
            "response_text": _pending_response(case_root),
            "teams_tms_review_cards": pending[:5],
        }

    # TMS-looking free text with AN but no card context: guard in-channel before
    # local deep-dive/Paperclip handoff. `_CASE_CHECK_RE` also contains
    # "aktualisier", so write-like text must be protected first.
    if order_id and _WRITE_RE.search(raw) and _TMS_FIELD_RE.search(raw):
        return {
            "handled": True,
            "classification": "tms_control_without_card_context",
            "order_id": order_id,
            "response_text": (
                "Ich erkenne eine CARGOLO-ASR/TMS-Anweisung, kann sie aber nicht eindeutig einer Operator-Karte zuordnen. "
                "Bitte auf die konkrete ASR-Karte antworten/quote-reply und @Hermes CARGOLO erwähnen; "
                "ich lege TMS-Änderungen dann nur als Review-Vorschlag ab."
            ),
        }

    # Case deep-dive: for "gib mir alles" / complete case questions, refresh the
    # canonical local case first (create if missing, update if present), then
    # synthesize from the refreshed local TMS/mail/document evidence. When the
    # Paperclip bridge is enabled, let the employee handoff create a Chef issue
    # instead of swallowing the Fallfrage in this older local deep-dive path.
    if order_id and (_CASE_CHECK_RE.search(raw) or _FULL_CASE_RE.search(raw) or "komplett" in lowered or "case" in lowered):
        if paperclip_bridge_enabled:
            return {
                "handled": False,
                "reason": "paperclip_bridge_case_assist",
                "classification": "paperclip_case_assist_candidate",
                "order_id": order_id,
                "allow_employee_handoff": True,
            }
        return _run_local_case_deep_dive(root=case_root, order_id=order_id, text=raw, user_name=user_name)

    # General CARGOLO/ASR mentions should get the employee prompt rather than generic chatbot tone.
    if "cargolo" in lowered or " asr" in f" {lowered}":
        return {
            "handled": False,
            "allow_generic_chat": True,
            "classification": "general_cargolo_ops",
            "agent_prompt": _coordinator_prompt(raw, order_id=order_id, intent="general_ops"),
        }

    return {"handled": False}
