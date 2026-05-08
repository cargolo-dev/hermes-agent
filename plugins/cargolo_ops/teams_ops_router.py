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

try:
    from hermes_constants import get_hermes_home
except Exception:  # pragma: no cover - test/import fallback
    def get_hermes_home() -> Path:  # type: ignore[no-redef]
        return Path.home() / ".hermes"


_ORDER_RE = re.compile(r"\b(?:AN|BU)-\d{3,}\b", re.IGNORECASE)
_STATUS_RE = re.compile(r"\b(status|health|readiness|zustand|system|läuft|laeuft|cron)\b", re.IGNORECASE)
_PENDING_RE = re.compile(r"\b(offene?|pending|freigaben?|review|tms[-\s]?freigaben?)\b", re.IGNORECASE)
_CASE_CHECK_RE = re.compile(r"\b(prüf(?:e|en)?|pruef(?:e|en)?|check|aktualisier(?:e|en)?|sync|zieh(?:e|en)?)\b", re.IGNORECASE)
_WRITE_RE = re.compile(r"\b(schreib(?:e|en)?|setz(?:e|en)?|eintragen|ändern|aendern|update|aktualisier(?:e|en)?)\b", re.IGNORECASE)
_TMS_FIELD_RE = re.compile(r"\b(TMS|MRN|HBL|MBL|HAWB|Zollreferenz|customs)\b", re.IGNORECASE)


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


_SUPPORTED_TMS_REVIEW_TARGETS = {"customs_reference", "hbl_number", "mbl_number", "hawb_number"}


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


def _status_response(root: Path) -> str:
    jobs = _load_cron_jobs()
    doc_jobs = [j for j in jobs if "cargolo" in str(j.get("name") or j.get("job_id") or "").lower()]
    pending = _collect_pending_tms_actions(root, limit=99)
    runtime_state = root / "runtime" / "document_activity_monitor_state.json"
    watermark = "unbekannt"
    if runtime_state.exists():
        try:
            state = json.loads(runtime_state.read_text(encoding="utf-8"))
            watermark = str(state.get("last_seen_activity_id") or state.get("last_activity_id") or "unbekannt")
        except Exception:
            watermark = "nicht lesbar"

    if doc_jobs:
        first = doc_jobs[0]
        cron_line = f"{first.get('name') or first.get('job_id')} · {first.get('state') or first.get('status') or 'unbekannt'} · last={first.get('last_status') or 'n/a'}"
    elif jobs:
        cron_line = f"{len(jobs)} Cronjobs gefunden"
    else:
        cron_line = "keine Cronjobs im lokalen Jobs-File gefunden"

    return (
        "CARGOLO Teams Ops · Status\n"
        f"- Gateway/Teams: Nachricht empfangen, Router aktiv\n"
        f"- Dokumenten-Monitor: {cron_line}\n"
        f"- Activity-Watermark: {watermark}\n"
        f"- Offene TMS-Freigaben: {len(pending)}\n"
        "Nächster Schritt: Sag z.B. `offene Freigaben` oder `prüfe AN-12345 komplett`."
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


def _coordinator_prompt(text: str, *, order_id: str | None = None, intent: str = "general_ops") -> str:
    order_line = f"Case: {order_id}. " if order_id else "Case: nicht eindeutig. "
    return (
        "Rolle: Du bist Hermes CARGOLO als ASR Ops Coordinator in Microsoft Teams — ein interner, proaktiver Mitarbeiter.\n"
        f"{order_line}Intent: {intent}.\n"
        "Arbeitsweise: Ziehe bei Bedarf CARGOLO Skills, Case-Folder, TMS/MCP-Kontext, Mail-Historie, Dokumentregistry, Cron-/Plugin-Status heran. "
        "Bleibe read-only, außer ein freigegebener zweistufiger Write-Pfad ist eindeutig aktiv.\n"
        "Sicherheit: Freitext in Teams darf keine direkten TMS-, Angebots- oder Kundenmail-Writes auslösen. "
        "TMS-Änderungswünsche nur über `cargolo_asr_record_teams_tms_intent` als pending_review vormerken; bei Unklarheit genau eine Rückfrage stellen.\n"
        "Antwortstil: Deutsch, kurz, operativ: Gemacht/Lage/Auffälligkeit/Nächster Schritt. Keine Audit-Dumps, keine KI-Floskeln.\n"
        f"Teams-Nachricht: {text.strip()}"
    )


def route_teams_ops_message(
    *,
    text: str,
    root: Path | None = None,
    chat_id: str | None = None,
    user_id: str | None = None,
    user_name: str | None = None,
    message_id: str | None = None,
) -> dict[str, Any]:
    """Route non-card CARGOLO Teams messages.

    Returns one of:
    - {handled: True, response_text: ...} for deterministic safe status/list commands
    - {handled: False, allow_generic_chat: True, agent_prompt: ...} for agent work
    - {handled: False} for unrelated Teams chat
    """
    del chat_id, user_id, user_name, message_id  # reserved for audit extension
    raw = str(text or "").strip()
    if not raw:
        return {"handled": False}
    case_root = root or default_case_root()
    lowered = raw.lower()
    order_match = _ORDER_RE.search(raw)
    order_id = order_match.group(0).upper() if order_match else None

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

    # Case deep-dive: let the normal Hermes agent use tools/context, but wrap it in a strict ops prompt.
    if order_id and (_CASE_CHECK_RE.search(raw) or "komplett" in lowered or "case" in lowered):
        return {
            "handled": False,
            "allow_generic_chat": True,
            "classification": "case_deep_dive_request",
            "order_id": order_id,
            "agent_prompt": _coordinator_prompt(raw, order_id=order_id, intent="read_only_case_deep_dive"),
        }

    # TMS-looking free text with AN but no card context: guard in-channel.
    # The safe pending tool is only offered from matched card/context prompts.
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

    # General CARGOLO/ASR mentions should get the employee prompt rather than generic chatbot tone.
    if "cargolo" in lowered or " asr" in f" {lowered}":
        return {
            "handled": False,
            "allow_generic_chat": True,
            "classification": "general_cargolo_ops",
            "agent_prompt": _coordinator_prompt(raw, order_id=order_id, intent="general_ops"),
        }

    return {"handled": False}
