from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Callable

from hermes_constants import get_hermes_home

from .models import utc_now_iso

_ORDER_RE = re.compile(r"(?:AN|BU)-\d{3,}", re.IGNORECASE)
_CONTEXT_RE = re.compile(r"ASRCTX:([^\s<]+)", re.IGNORECASE)


def _default_root() -> Path:
    return get_hermes_home() / "cargolo_asr"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _order_from_payload(payload: dict[str, Any]) -> str:
    result = payload.get("processor_result") if isinstance(payload.get("processor_result"), dict) else {}
    event = payload.get("activity_event") if isinstance(payload.get("activity_event"), dict) else {}
    for source in (result, event, payload):
        for key in ("order_id", "shipment_number", "an", "bu"):
            value = str(source.get(key) or "").strip()
            if value:
                match = _ORDER_RE.search(value)
                return match.group(0).upper() if match else value.upper()
    return ""


def _activity_id_from_payload(payload: dict[str, Any]) -> Any:
    event = payload.get("activity_event") if isinstance(payload.get("activity_event"), dict) else {}
    trigger = payload.get("trigger_event") if isinstance(payload.get("trigger_event"), dict) else {}
    for source in (event, trigger, payload):
        value = source.get("id") or source.get("activity_id")
        if value not in (None, ""):
            return value
    return None


def _metadata_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    event = payload.get("activity_event") if isinstance(payload.get("activity_event"), dict) else {}
    trigger = payload.get("trigger_event") if isinstance(payload.get("trigger_event"), dict) else {}
    for source in (event, trigger, payload):
        metadata = source.get("metadata") if isinstance(source, dict) else None
        if isinstance(metadata, dict):
            return metadata
    return {}


def build_card_context(
    *,
    route_name: str,
    delivery_id: str,
    payload: dict[str, Any],
    message_id: str | None = None,
    chat_id: str | None = None,
) -> dict[str, Any]:
    """Build the durable context stored for one sent CARGOLO Teams card.

    The webhook delivery layer may pass either the original ASR payload or the
    outer manual-ops notification body.  The latter nests the ASR payload under
    ``payload``; unwrap it so live Teams deliveries are indexable for replies.
    """
    effective_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
    order_id = _order_from_payload(effective_payload)
    activity_id = _activity_id_from_payload(effective_payload)
    metadata = _metadata_from_payload(effective_payload)
    context_parts = [order_id or "unknown", str(activity_id or "noactivity"), str(delivery_id or "manual")]
    context_id = ":".join(context_parts)
    return {
        "context_id": context_id,
        "route_name": route_name,
        "delivery_id": delivery_id,
        "order_id": order_id,
        "activity_id": activity_id,
        "message_id": message_id,
        "chat_id": chat_id,
        "document_type": metadata.get("document_type") or metadata.get("type"),
        "file_name": metadata.get("file_name") or metadata.get("filename") or metadata.get("name"),
        "created_at": utc_now_iso(),
    }


def record_sent_card(*, root: Path | None = None, context: dict[str, Any]) -> dict[str, Any]:
    """Persist Teams card context under the case and in a runtime lookup index."""
    root = root or _default_root()
    order_id = str(context.get("order_id") or "").strip().upper()
    if not order_id:
        return {"recorded": False, "reason": "missing_order_id"}
    case_root = root / "orders" / order_id
    teams_dir = case_root / "teams"
    row = {**context, "recorded_at": utc_now_iso()}
    _append_jsonl(teams_dir / "cards.jsonl", row)

    index_path = root / "runtime" / "teams_card_index.json"
    index = _read_json(index_path)
    by_message = index.setdefault("by_message_id", {})
    by_context = index.setdefault("by_context_id", {})
    entry = {
        "order_id": order_id,
        "context_id": row.get("context_id"),
        "message_id": row.get("message_id"),
        "chat_id": row.get("chat_id"),
        "activity_id": row.get("activity_id"),
        "document_type": row.get("document_type"),
        "file_name": row.get("file_name"),
        "created_at": row.get("created_at"),
    }
    if row.get("message_id"):
        by_message[str(row["message_id"])] = entry
    if row.get("context_id"):
        by_context[str(row["context_id"])] = entry
    index["updated_at"] = utc_now_iso()
    _write_json(index_path, index)
    return {"recorded": True, "order_id": order_id, "context_id": row.get("context_id")}


def _operator_instruction_text(text: str) -> str:
    """Return the likely operator-authored tail after quoted Teams card text.

    Teams quote-replies often concatenate the quoted card before the actual
    operator instruction.  For extraction we prefer the last non-empty line so
    values shown in the quoted card (old MRN, current TMS fields, etc.) do not
    win over the operator's requested update.
    """
    raw = str(text or "")
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    return lines[-1] if lines else raw.strip()


def _classify(text: str) -> tuple[str, dict[str, Any]]:
    raw = str(text or "").strip()
    instruction = _operator_instruction_text(raw)
    lowered = raw.lower()
    instruction_lowered = instruction.lower()
    extraction_text = instruction if any(word in instruction_lowered for word in ("tms", "ändern", "aendern", "aktualis", "eintragen", "setzen", "hbl", "mbl", "hawb", "mrn")) else raw
    mrn = re.search(r"\b(?:MRN\s*)?([0-9]{2}[A-Z]{2}[A-Z0-9]{3,})\b", extraction_text, re.IGNORECASE)
    hbl = re.search(r"\bHBL\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{2,})\b", extraction_text, re.IGNORECASE)
    mbl = re.search(r"\bMBL\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{2,})\b", extraction_text, re.IGNORECASE)
    hawb = re.search(r"\bHAWB\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{2,})\b", extraction_text, re.IGNORECASE)
    if any(word in instruction_lowered for word in ("tms", "ändern", "aendern", "aktualis", "eintragen", "setzen", "hbl", "mbl", "hawb", "mrn")):
        target = None
        value = None
        if mrn:
            target = "customs_reference"
            value = mrn.group(1).upper()
        elif hbl:
            target = "hbl_number"
            value = hbl.group(1).upper()
        elif mbl:
            target = "mbl_number"
            value = mbl.group(1).upper()
        elif hawb:
            target = "hawb_number"
            value = hawb.group(1).upper()
        return "agent_decision_required", {
            "type": "agent_tms_intent_candidate",
            "target_candidate": target,
            "value_candidate": value,
            "extracted_reference": value,
            "status": "agent_decision_required",
            "confidence": "candidate_only",
        }
    if any(word in instruction_lowered for word in ("nein", "falsch", "stimmt nicht", "korrektur", "ablehnen", "nicht korrekt")):
        return "correction", {"type": "review_decision", "decision": "correction_needed"}
    if "?" in instruction or any(word in instruction_lowered for word in ("warum", "was meinst", "welche", "bitte prüfen", "prüfen?", "unklar")):
        return "question", {"type": "followup_question"}
    if re.search(r"\b(ja|passt|korrekt|freigabe|ok|okay|bestätigt|bestaetigt|stimmt)\b", instruction_lowered):
        return "confirmation", {"type": "review_decision", "decision": "confirmed"}
    return "note", {"type": "case_learning"}


def _with_match_type(context: dict[str, Any], match_type: str) -> dict[str, Any]:
    return {**context, "_match_type": match_type}


def _find_context(root: Path, *, text: str, reply_to_message_id: str | None, chat_id: str | None) -> dict[str, Any] | None:
    index = _read_json(root / "runtime" / "teams_card_index.json")
    if reply_to_message_id:
        hit = (index.get("by_message_id") or {}).get(str(reply_to_message_id))
        if isinstance(hit, dict):
            return _with_match_type(hit, "reply_to_message_id")
    context_match = _CONTEXT_RE.search(text or "")
    if context_match:
        context_id = context_match.group(1).strip()
        hit = (index.get("by_context_id") or {}).get(context_id)
        if isinstance(hit, dict):
            return _with_match_type(hit, "context_marker")
    match = _ORDER_RE.search(text or "")
    if match:
        order_id = match.group(0).upper()
        if (root / "orders" / order_id).exists():
            return _with_match_type({"order_id": order_id, "context_id": f"{order_id}:manual", "chat_id": chat_id}, "order_reference")
    return None


def _persist_pending_tms_action(
    *,
    root: Path,
    order_id: str,
    context: dict[str, Any],
    event: dict[str, Any],
    derived_action: dict[str, Any],
) -> None:
    if derived_action.get("type") != "pending_tms_update":
        return
    teams_dir = root / "orders" / order_id / "teams"
    timestamp = event.get("timestamp")
    target = derived_action.get("target")
    value = derived_action.get("value")
    context_id = context.get("context_id")
    row = {
        "timestamp": timestamp,
        "action_id": _pending_action_id(order_id=order_id, target=str(target or ""), value=str(value or ""), context_id=str(context_id or ""), created_at=str(timestamp or "")),
        "status": "pending_review",
        "order_id": order_id,
        "context_id": context_id,
        "activity_id": context.get("activity_id"),
        "target": target,
        "value": value,
        "confidence": derived_action.get("confidence"),
        "source": "teams_reply",
        "source_message_id": event.get("message_id"),
        "reply_to_message_id": event.get("reply_to_message_id"),
        "operator": event.get("operator"),
        "text": event.get("text"),
        "write_policy": "no_auto_write_without_review",
    }
    _append_jsonl(teams_dir / "pending_tms_actions.jsonl", row)


def _pending_action_id(*, order_id: str, target: str, value: str, context_id: str | None, created_at: str) -> str:
    raw = "|".join([
        str(order_id or "").strip().upper(),
        str(target or "").strip(),
        str(value or "").strip(),
        str(context_id or "").strip(),
        str(created_at or "").strip(),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def record_agent_tms_update_intent(
    *,
    root: Path | None = None,
    order_id: str,
    target: str,
    value: str,
    text: str = "",
    operator: str | None = None,
    source_message_id: str | None = None,
    reply_to_message_id: str | None = None,
    context_id: str | None = None,
    confidence: str = "agent_decided",
) -> dict[str, Any]:
    """Persist an LLM/agent-decided Teams TMS update intent as pending review.

    This is the safe bridge between natural-language agent judgment and TMS
    writeback: the agent may decide intent, but this function only queues a
    review item.  Actual TMS writes still require the separate explicit
    approval/apply/verify workflow.
    """
    root = root or _default_root()
    normalized_order = str(order_id or "").strip().upper()
    if not normalized_order or not _ORDER_RE.fullmatch(normalized_order):
        return {"status": "validation_error", "queued": False, "error": "invalid_order_id"}
    normalized_target = str(target or "").strip()
    if normalized_target not in _SHORT_TO_FULL_TARGET:
        return {"status": "validation_error", "queued": False, "error": "unsupported_target", "supported_targets": sorted(_SHORT_TO_FULL_TARGET)}
    normalized_value = str(value or "").strip()
    if not normalized_value:
        return {"status": "validation_error", "queued": False, "error": "missing_value"}

    now = utc_now_iso()
    teams_dir = root / "orders" / normalized_order / "teams"
    action_id = _pending_action_id(
        order_id=normalized_order,
        target=normalized_target,
        value=normalized_value,
        context_id=context_id,
        created_at=now,
    )
    row = {
        "timestamp": now,
        "action_id": action_id,
        "status": "pending_review",
        "order_id": normalized_order,
        "context_id": context_id,
        "activity_id": None,
        "target": normalized_target,
        "value": normalized_value,
        "confidence": confidence,
        "source": "teams_agent_decision",
        "source_message_id": source_message_id,
        "reply_to_message_id": reply_to_message_id,
        "operator": operator,
        "text": text,
        "write_policy": "no_auto_write_without_review",
    }
    _append_jsonl(teams_dir / "pending_tms_actions.jsonl", row)
    _append_jsonl(teams_dir / "case_learning.jsonl", {
        "timestamp": now,
        "source": "teams_agent_decision",
        "order_id": normalized_order,
        "operator": operator,
        "classification": "agent_tms_update_intent",
        "learning": text,
        "context_id": context_id,
        "derived_action": {"type": "pending_tms_update", "target": normalized_target, "value": normalized_value},
    })
    _append_jsonl(root / "orders" / normalized_order / "audit" / "actions.jsonl", {
        "timestamp": now,
        "actor": operator or "Hermes Agent",
        "action": "teams_agent_tms_update_intent_recorded",
        "result": "pending_review",
        "target": normalized_target,
        "value": normalized_value,
        "files": [str(teams_dir / "pending_tms_actions.jsonl")],
    })
    return {
        "status": "ok",
        "queued": True,
        "action_id": action_id,
        "order_id": normalized_order,
        "target": normalized_target,
        "value": normalized_value,
        "queue_path": str(teams_dir / "pending_tms_actions.jsonl"),
    }


_SHORT_TO_FULL_TARGET: dict[str, str] = {
    "customs_reference": "shipment.customs.customs_reference",
    "hbl_number": "shipment.freight_details.hbl_number",
    "mbl_number": "shipment.freight_details.mbl_number",
    "hawb_number": "shipment.freight_details.hawb_number",
}


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


def _is_explicit_tms_write_approval(text: str) -> bool:
    raw = str(text or "")
    lowered = raw.lower()
    has_approval = bool(re.search(r"\b(freigeben|freigegeben|freigabe|ausführen|ausfuehren|übernehmen|uebernehmen)\b", lowered))
    has_write_intent = bool(re.search(r"\b(tms|schreib(?:en)?|eintragen|setzen|anwenden|übernehmen|uebernehmen)\b", lowered))
    return has_approval and has_write_intent


def _find_pending_tms_action(root: Path, order_id: str, text: str) -> tuple[Path, list[dict[str, Any]], int, dict[str, Any]] | None:
    queue_path = root / "orders" / order_id / "teams" / "pending_tms_actions.jsonl"
    rows = _read_jsonl(queue_path)
    pending = [
        (idx, row) for idx, row in enumerate(rows)
        if str(row.get("status") or "") in {"pending_review", "approved_pending_apply"}
        and str(row.get("order_id") or "").strip().upper() == order_id
    ]
    if not pending:
        return None
    raw = str(text or "")
    for idx, row in reversed(pending):
        value = str(row.get("value") or "").strip()
        if value and value in raw:
            return queue_path, rows, idx, row
    idx, row = pending[-1]
    return queue_path, rows, idx, row


def _find_pending_tms_action_by_button_data(
    root: Path,
    *,
    order_id: str,
    data: dict[str, Any],
) -> tuple[Path, list[dict[str, Any]], int, dict[str, Any]] | None:
    normalized_order = str(order_id or "").strip().upper()
    queue_path = root / "orders" / normalized_order / "teams" / "pending_tms_actions.jsonl"
    rows = _read_jsonl(queue_path)
    action_id = str(data.get("action_id") or "").strip()
    target = str(data.get("target") or "").strip()
    value = str(data.get("value") or "").strip()
    candidates = [
        (idx, row) for idx, row in enumerate(rows)
        if str(row.get("status") or "") in {"pending_review", "approved_pending_apply"}
        and str(row.get("order_id") or "").strip().upper() == normalized_order
    ]
    for idx, row in reversed(candidates):
        row_action_id = str(row.get("action_id") or "").strip()
        if not row_action_id:
            row_action_id = _pending_action_id(
                order_id=str(row.get("order_id") or ""),
                target=str(row.get("target") or ""),
                value=str(row.get("value") or ""),
                context_id=str(row.get("context_id") or ""),
                created_at=str(row.get("timestamp") or row.get("created_at") or ""),
            )
        if action_id and row_action_id == action_id:
            return queue_path, rows, idx, row
    for idx, row in reversed(candidates):
        if target and str(row.get("target") or "").strip() != target:
            continue
        if value and str(row.get("value") or "").strip() != value:
            continue
        return queue_path, rows, idx, row
    return None


def _find_any_tms_action_by_button_data(
    root: Path,
    *,
    order_id: str,
    data: dict[str, Any],
) -> dict[str, Any] | None:
    normalized_order = str(order_id or "").strip().upper()
    queue_path = root / "orders" / normalized_order / "teams" / "pending_tms_actions.jsonl"
    rows = _read_jsonl(queue_path)
    action_id = str(data.get("action_id") or "").strip()
    target = str(data.get("target") or "").strip()
    value = str(data.get("value") or "").strip()
    for row in reversed(rows):
        if str(row.get("order_id") or "").strip().upper() != normalized_order:
            continue
        row_action_id = str(row.get("action_id") or "").strip()
        if not row_action_id:
            row_action_id = _pending_action_id(
                order_id=str(row.get("order_id") or ""),
                target=str(row.get("target") or ""),
                value=str(row.get("value") or ""),
                context_id=str(row.get("context_id") or ""),
                created_at=str(row.get("timestamp") or row.get("created_at") or ""),
            )
        if action_id and row_action_id == action_id:
            return row
        if target and str(row.get("target") or "").strip() != target:
            continue
        if value and str(row.get("value") or "").strip() != value:
            continue
        if target or value:
            return row
    return None


def _button_event(*, user_id: str | None, user_name: str | None) -> dict[str, Any]:
    return {
        "timestamp": utc_now_iso(),
        "operator_user_id": user_id,
        "operator": user_name,
        "message_id": None,
    }


def _button_response_text(order_id: str, action: dict[str, Any]) -> str:
    if action.get("type") == "tms_update_applied":
        return f"✅ Freigabe umgesetzt für {order_id}: {action.get('target')} = {action.get('value')} wurde ins TMS geschrieben und frisch verifiziert."
    if action.get("type") == "tms_update_verification_failed":
        return f"⚠️ TMS-Write für {order_id} nicht sauber verifiziert: erwartet {action.get('value')}, gesehen {action.get('verified_value')}. Ich lasse das als Review-Fehler offen."
    if action.get("type") == "tms_update_rejected":
        return f"❌ Abgelehnt für {order_id}: {action.get('target')} = {action.get('value')} wurde nicht ins TMS geschrieben."
    if action.get("reason") == "writeback_disabled":
        return f"⚠️ Freigabe für {order_id} erkannt, aber Live-TMS-Writeback ist deaktiviert. Ich schreibe nichts ins TMS."
    return f"⚠️ Aktion für {order_id} konnte nicht umgesetzt werden; nichts wurde als erfolgreich markiert."


def process_teams_tms_card_action(
    *,
    root: Path | None = None,
    data: dict[str, Any],
    user_id: str | None = None,
    user_name: str | None = None,
    enable_tms_writeback: bool | None = None,
    apply_tms_update: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None,
    verify_tms_value: Callable[[str, str], Any] | None = None,
) -> dict[str, Any]:
    """Handle CARGOLO ASR Teams Adaptive Card approve/reject button payloads.

    Buttons are deterministic and auditable. Reject never writes to TMS.
    Approve applies only when writeback is explicitly enabled and the exact
    pending_review action still exists; successful writes are freshly verified.
    """
    root = root or _default_root()
    hermes_action = str(data.get("hermes_action") or "").strip()
    order_id = str(data.get("order_id") or "").strip().upper()
    if hermes_action not in {"cargolo_asr_tms_approve", "cargolo_asr_tms_reject"}:
        return {"handled": False, "status": "unknown_action", "response_text": "Unknown action."}
    if not order_id or not _ORDER_RE.fullmatch(order_id):
        return {"handled": True, "status": "validation_error", "response_text": "⛔ Ungültige oder fehlende AN/BU in der Teams-Aktion."}
    pending_match = _find_pending_tms_action_by_button_data(root, order_id=order_id, data=data)
    if not pending_match:
        resolved = _find_any_tms_action_by_button_data(root, order_id=order_id, data=data)
        if resolved:
            status = str(resolved.get("status") or "unbekannt")
            target = str(resolved.get("target") or data.get("target") or "Feld")
            value = str(resolved.get("value") or data.get("value") or "Wert")
            if status == "rejected":
                text = f"ℹ️ Diese TMS-Freigabe für {order_id} ist bereits abgelehnt: {target} = {value}. Kein TMS-Write."
            elif status in {"applied", "verification_failed"}:
                text = f"ℹ️ Diese TMS-Freigabe für {order_id} ist bereits erledigt mit Status {status}: {target} = {value}. Ich führe nichts erneut aus."
            else:
                text = f"ℹ️ Diese TMS-Freigabe für {order_id} ist nicht mehr offen (Status {status}): {target} = {value}. Ich führe nichts erneut aus."
            return {"handled": True, "status": "already_resolved", "order_id": order_id, "response_text": text}
        return {"handled": True, "status": "not_found", "response_text": f"⚠️ Keine offene passende TMS-Freigabe für {order_id} gefunden. Vermutlich bereits erledigt, abgelehnt oder abgelaufen."}

    pending_path, rows, pending_index, pending_action = pending_match
    event = _button_event(user_id=user_id, user_name=user_name)
    teams_dir = root / "orders" / order_id / "teams"

    if hermes_action == "cargolo_asr_tms_reject":
        updated = {
            **pending_action,
            "status": "rejected",
            "rejected_at": event["timestamp"],
            "rejected_by": user_name,
            "rejected_by_user_id": user_id,
        }
        rows[pending_index] = updated
        _write_jsonl(pending_path, rows)
        action = {"type": "tms_update_rejected", "target": pending_action.get("target"), "value": pending_action.get("value"), "status": "rejected"}
        _append_jsonl(teams_dir / "rejected_tms_actions.jsonl", {**updated, "source": "teams_adaptive_card"})
        _append_jsonl(root / "orders" / order_id / "audit" / "actions.jsonl", {
            "timestamp": event["timestamp"],
            "actor": user_name or "Teams Operator",
            "action": "teams_tms_update_rejected",
            "result": "rejected",
            "target": action.get("target"),
            "value": action.get("value"),
            "order_id": order_id,
            "files": [str(pending_path), str(teams_dir / "rejected_tms_actions.jsonl")],
        })
        return {"handled": True, "status": "rejected", "order_id": order_id, "derived_action": action, "response_text": _button_response_text(order_id, action)}

    writeback_enabled = bool(enable_tms_writeback) or str(os.getenv("CARGOLO_ASR_TEAMS_TMS_WRITEBACK") or "").strip().lower() in {"1", "true", "yes", "on"}
    if not writeback_enabled:
        action = {
            "type": "tms_update_approval_blocked",
            "reason": "writeback_disabled",
            "target": pending_action.get("target"),
            "value": pending_action.get("value"),
            "status": "pending_review",
        }
        _append_jsonl(root / "orders" / order_id / "audit" / "actions.jsonl", {
            "timestamp": event["timestamp"],
            "actor": user_name or "Teams Operator",
            "action": "teams_tms_update_approval_blocked",
            "result": "writeback_disabled",
            "target": action.get("target"),
            "value": action.get("value"),
            "order_id": order_id,
            "files": [str(pending_path)],
        })
        return {"handled": True, "status": "approval_blocked", "order_id": order_id, "derived_action": action, "response_text": _button_response_text(order_id, action)}

    action = _apply_approved_pending_tms_action(
        root=root,
        order_id=order_id,
        pending_path=pending_path,
        rows=rows,
        pending_index=pending_index,
        pending_action=pending_action,
        event=event,
        apply_tms_update=apply_tms_update,
        verify_tms_value=verify_tms_value,
    )
    return {"handled": True, "status": action.get("status"), "order_id": order_id, "derived_action": action, "response_text": _button_response_text(order_id, action)}


def _default_tms_apply(action: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    from .writeback_actions import apply_pending_tms_action

    return apply_pending_tms_action(action, context)


def _extract_snapshot_value(snapshot: dict[str, Any], target: str) -> Any:
    shipment = snapshot.get("shipment") if isinstance(snapshot.get("shipment"), dict) else snapshot
    if target == "customs_reference":
        customs = shipment.get("customs") if isinstance(shipment.get("customs"), dict) else {}
        return customs.get("customs_reference") or shipment.get("customs_reference")
    if target in {"hbl_number", "mbl_number", "hawb_number"}:
        freight = shipment.get("freight_details") if isinstance(shipment.get("freight_details"), dict) else {}
        return freight.get(target) or shipment.get(target)
    return None


def _default_tms_verify(order_id: str, target: str) -> Any:
    from .tms_provider import build_tms_provider_from_env

    provider = build_tms_provider_from_env()
    if provider is None:
        raise RuntimeError("TMS read provider is not configured")
    snapshot = provider.snapshot(order_id)
    return _extract_snapshot_value(snapshot, target)


def _apply_approved_pending_tms_action(
    *,
    root: Path,
    order_id: str,
    pending_path: Path,
    rows: list[dict[str, Any]],
    pending_index: int,
    pending_action: dict[str, Any],
    event: dict[str, Any],
    apply_tms_update: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None,
    verify_tms_value: Callable[[str, str], Any] | None,
) -> dict[str, Any]:
    now = event.get("timestamp") or utc_now_iso()
    short_target = str(pending_action.get("target") or "").strip()
    full_target = _SHORT_TO_FULL_TARGET.get(short_target)
    value = pending_action.get("value")
    base_update = {
        "approved_at": now,
        "approved_by": event.get("operator"),
        "approval_message_id": event.get("message_id"),
    }
    if not full_target or value in (None, ""):
        rows[pending_index] = {**pending_action, **base_update, "status": "approval_blocked", "block_reason": "unsupported_or_missing_target_value"}
        _write_jsonl(pending_path, rows)
        return {"type": "tms_update_approval_blocked", "target": short_target, "value": value, "status": "approval_blocked"}

    writeback_action = {
        "action_type": "field_update",
        "target": full_target,
        "suggested_value": value,
        "source": "teams_reply_explicit_approval",
    }
    context = {"order_id": order_id, "source_message_id": event.get("message_id")}
    apply_fn = apply_tms_update or _default_tms_apply
    verify_fn = verify_tms_value or _default_tms_verify

    try:
        apply_result = apply_fn(writeback_action, context)
    except Exception as exc:
        rows[pending_index] = {**pending_action, **base_update, "status": "apply_failed", "apply_error": str(exc)}
        _write_jsonl(pending_path, rows)
        return {"type": "tms_update_apply_failed", "target": short_target, "value": value, "status": "apply_failed", "error": str(exc)}

    applied_ok = str(apply_result.get("status") or "").lower() in {"ok", "applied"}
    if not applied_ok:
        rows[pending_index] = {**pending_action, **base_update, "status": "apply_failed", "apply_result": apply_result}
        _write_jsonl(pending_path, rows)
        return {"type": "tms_update_apply_failed", "target": short_target, "value": value, "status": "apply_failed", "apply_result": apply_result}

    try:
        verified_value = verify_fn(order_id, short_target)
    except Exception as exc:
        rows[pending_index] = {**pending_action, **base_update, "status": "verification_failed", "apply_result": apply_result, "verification_error": str(exc)}
        _write_jsonl(pending_path, rows)
        return {"type": "tms_update_verification_failed", "target": short_target, "value": value, "status": "verification_failed", "error": str(exc)}

    verified_match = str(verified_value or "").strip() == str(value or "").strip()
    final_status = "applied" if verified_match else "verification_failed"
    updated = {
        **pending_action,
        **base_update,
        "status": final_status,
        "applied_at": now,
        "applied_by": "Hermes Teams Reply Loop",
        "apply_result": apply_result,
        "verified_value": verified_value,
        "verification": "fresh_tms_snapshot_matched" if verified_match else "fresh_tms_snapshot_mismatch",
    }
    rows[pending_index] = updated
    _write_jsonl(pending_path, rows)
    derived = {
        "type": "tms_update_applied" if verified_match else "tms_update_verification_failed",
        "target": short_target,
        "value": value,
        "status": final_status,
        "apply_result": apply_result,
        "verified_value": verified_value,
    }
    if verified_match:
        teams_dir = root / "orders" / order_id / "teams"
        _append_jsonl(teams_dir / "applied_tms_actions.jsonl", {**updated, "source": "teams_reply_explicit_approval"})
        _append_jsonl(root / "orders" / order_id / "audit" / "actions.jsonl", {
            "timestamp": now,
            "actor": "Hermes Teams Reply Loop",
            "action": "teams_tms_update_applied",
            "result": "applied",
            "target": short_target,
            "value": value,
            "order_id": order_id,
            "verification": "fresh_tms_snapshot_matched",
            "files": [str(pending_path), str(teams_dir / "applied_tms_actions.jsonl")],
        })
    return derived


def _agent_prompt_for_contextual_reply(order_id: str, context: dict[str, Any], text: str, classification: str) -> str:
    instruction = _operator_instruction_text(text)
    return (
        "Rolle: Du bist Hermes CARGOLO als ASR Ops Coordinator im Teams-Channel — ein interner, proaktiver Kollege, "
        "nicht ein Formular-Parser.\n"
        f"Kontext: Antwort auf CARGOLO ASR Operator-Karte zu {order_id}. "
        f"Context-ID: {context.get('context_id') or 'unbekannt'}. "
        f"Vorfilter: {classification} — das ist nur Kontext, keine finale Entscheidung.\n"
        "Arbeitsweise: Entscheide intelligent nach der Operator-Nachricht, nicht per starrem Regex und nicht nach zitiertem Kartentext. "
        "Trenne sicher zwischen Hinweis, Rückfrage, Freigabe, Korrektur, Case-Learning und echtem TMS-Änderungswunsch. "
        "Wenn etwas unklar ist, benenne die Unsicherheit und stelle genau eine kurze Rückfrage. "
        "Wenn du aus dem Kontext einen sinnvollen nächsten Schritt erkennst, schlage ihn proaktiv vor.\n"
        "TMS-Sicherheit: Wenn die Nachricht fachlich ein TMS-Änderungswunsch ist, schreibe NICHT direkt ins TMS. "
        "Nutze ausschließlich das Tool `cargolo_asr_record_teams_tms_intent` mit order_id, target, value, text, context_id, source_message_id, operator. "
        "Unterstützte targets: customs_reference, hbl_number, mbl_number, hawb_number. "
        "Wenn target oder value unklar sind, frage nach statt zu raten.\n"
        "Antwortstil: kurz, deutsch, operativ, mit klarer Aussage: geprüft/gespeichert/nicht geschrieben/nächster Schritt. "
        "Wenn es kein TMS-Änderungswunsch ist, antworte natürlich als Kollege und behaupte keinen TMS-Wunsch.\n"
        f"Operator-Nachricht: {instruction or text}"
    )


def _response_text(order_id: str, classification: str, action: dict[str, Any]) -> str:
    if classification in {"tms_update_approval_blocked", "tms_update_approved"}:
        if action.get("type") == "tms_update_applied":
            return f"Freigabe umgesetzt für {order_id}: {action.get('target')} = {action.get('value')} wurde ins TMS geschrieben und frisch verifiziert."
        if action.get("type") == "tms_update_verification_failed":
            return f"Achtung für {order_id}: TMS-Schreibaktion wurde versucht, aber die frische Verifikation passt nicht. Erwartet {action.get('value')}, gesehen {action.get('verified_value')}. Ich halte das als Review-Fehler offen."
        if action.get("reason") == "writeback_disabled":
            return f"Freigabe für {order_id} erkannt, aber Live-TMS-Writeback ist deaktiviert. Ich lasse den Vorschlag auf pending_review und schreibe nichts ins TMS."
        return f"Freigabe für {order_id} erkannt, aber die TMS-Schreibaktion wurde blockiert/ist fehlgeschlagen. Ich habe nichts als erfolgreich markiert."
    if classification == "tms_update_request":
        ref = action.get("extracted_reference")
        ref_txt = f" Referenz/MRN erkannt: {ref}." if ref else ""
        return f"Verstanden für {order_id}: ich habe das als TMS-Änderungswunsch erfasst.{ref_txt} Ich lege es im Case ab; Umsetzung bleibt bis zur verifizierten TMS-Schreibaktion/Review nachvollziehbar."
    if classification == "confirmation":
        return f"Danke, für {order_id} als bestätigt gespeichert. Ich nutze das als Case-Learning für diese Dokument-/Review-Situation."
    if classification == "correction":
        return f"Danke, für {order_id} als Korrektur/Review gespeichert. Ich halte den Punkt offen und behandle die ursprüngliche Bewertung nicht als final."
    if classification == "question":
        return f"Danke, Rückfrage zu {order_id} gespeichert. Ich prüfe den Case-Kontext und antworte mit der knappsten belastbaren Einschätzung."
    return f"Danke, Hinweis zu {order_id} im Case gespeichert."


def handle_teams_message(
    *,
    root: Path | None = None,
    text: str,
    chat_id: str | None = None,
    user_id: str | None = None,
    user_name: str | None = None,
    message_id: str | None = None,
    reply_to_message_id: str | None = None,
    enable_tms_writeback: bool | None = None,
    apply_tms_update: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None,
    verify_tms_value: Callable[[str, str], Any] | None = None,
) -> dict[str, Any]:
    """Intercept CARGOLO Teams replies and store structured learning/action events.

    Returns {handled: False} for unrelated Teams traffic so normal Hermes chat still works.
    """
    root = root or _default_root()
    context = _find_context(root, text=text, reply_to_message_id=reply_to_message_id, chat_id=chat_id)
    if not context:
        return {"handled": False}
    order_id = str(context.get("order_id") or "").strip().upper()
    if not order_id:
        return {"handled": False, "reason": "missing_order_id"}
    classification, derived_action = _classify(text)
    now = utc_now_iso()
    event = {
        "timestamp": now,
        "source": "teams_reply",
        "order_id": order_id,
        "context_id": context.get("context_id"),
        "reply_to_message_id": reply_to_message_id,
        "message_id": message_id,
        "chat_id": chat_id,
        "operator_user_id": user_id,
        "operator": user_name,
        "text": text,
        "classification": classification,
        "derived_action": derived_action,
        "context_match": context.get("_match_type"),
    }

    writeback_enabled = bool(enable_tms_writeback) or str(os.getenv("CARGOLO_ASR_TEAMS_TMS_WRITEBACK") or "").strip().lower() in {"1", "true", "yes", "on"}
    pending_match = _find_pending_tms_action(root, order_id, text) if _is_explicit_tms_write_approval(text) else None
    if pending_match and writeback_enabled:
        pending_path, rows, pending_index, pending_action = pending_match
        classification = "tms_update_approved"
        derived_action = _apply_approved_pending_tms_action(
            root=root,
            order_id=order_id,
            pending_path=pending_path,
            rows=rows,
            pending_index=pending_index,
            pending_action=pending_action,
            event=event,
            apply_tms_update=apply_tms_update,
            verify_tms_value=verify_tms_value,
        )
        event["classification"] = classification
        event["derived_action"] = derived_action
    elif pending_match and not writeback_enabled:
        classification = "tms_update_approval_blocked"
        derived_action = {
            "type": "tms_update_approval_blocked",
            "reason": "writeback_disabled",
            "target": pending_match[3].get("target"),
            "value": pending_match[3].get("value"),
            "status": "pending_review",
        }
        event["classification"] = classification
        event["derived_action"] = derived_action

    teams_dir = root / "orders" / order_id / "teams"
    _append_jsonl(teams_dir / "replies.jsonl", event)
    _persist_pending_tms_action(
        root=root,
        order_id=order_id,
        context=context,
        event=event,
        derived_action=derived_action,
    )
    _append_jsonl(teams_dir / "case_learning.jsonl", {
        "timestamp": now,
        "source": "teams_reply",
        "order_id": order_id,
        "operator": user_name,
        "classification": classification,
        "learning": text,
        "context_id": context.get("context_id"),
        "derived_action": derived_action,
    })
    _append_jsonl(root / "orders" / order_id / "audit" / "actions.jsonl", {
        "timestamp": now,
        "actor": "Hermes Teams Reply Loop",
        "action": "teams_reply_processed",
        "result": classification,
        "files": [
            str(teams_dir / "replies.jsonl"),
            str(teams_dir / "case_learning.jsonl"),
        ],
        "extra": {"message_id": message_id, "context_id": context.get("context_id"), "derived_action": derived_action},
    })
    response = _response_text(order_id, classification, derived_action)
    if classification in {"note", "question", "agent_decision_required"}:
        return {
            "handled": False,
            "asr_context_saved": True,
            "allow_generic_chat": True,
            "order_id": order_id,
            "context_id": context.get("context_id"),
            "classification": classification,
            "derived_action": derived_action,
            "agent_prompt": _agent_prompt_for_contextual_reply(order_id, context, text, classification),
        }
    return {
        "handled": True,
        "order_id": order_id,
        "context_id": context.get("context_id"),
        "classification": classification,
        "derived_action": derived_action,
        "response_text": response,
    }
