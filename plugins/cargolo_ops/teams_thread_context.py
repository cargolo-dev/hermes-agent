from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .models import normalize_order_ids, utc_now_iso

_ORDER_RE = re.compile(r"\b(?:AN|BU)-\d{3,}\b", re.IGNORECASE)
_FOLLOWUP_MARKERS = (
    "und", "auch", "dazu", "da", "die sendung", "der kunde", "die docs", "docs", "dokument", "dokumente",
    "ci", "pl", "packing", "commercial", "kundenseite", "geantwortet", "antwort", "eta", "status", "sauber",
)
_WRITE_MARKERS = (
    "setz", "setze", "tragen", "trage", "trag ", "eintragen", "schreib ins tms", "update ", "aktualisier",
    "lade", "upload", "hochladen", "sende", "mail dem", "antworte dem",
)


def _hash_chat_id(chat_id: str | None) -> str:
    raw = str(chat_id or "unknown").strip() or "unknown"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
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


def _compact(text: str | None, limit: int = 700) -> str:
    value = " ".join(str(text or "").split())
    return value[:limit] + ("…" if len(value) > limit else "")


def _thread_path(root: Path, chat_id: str | None) -> Path:
    return root / "runtime" / "teams_threads" / f"{_hash_chat_id(chat_id)}.json"


def load_thread_context(root: Path, chat_id: str | None) -> dict[str, Any]:
    if not chat_id:
        return {}
    return _read_json(_thread_path(root, chat_id))


def _case_thread_path(root: Path, order_id: str) -> Path:
    return root / "orders" / str(order_id).upper() / "teams" / "thread_context.json"


def _merge_recent(existing: list[Any], row: dict[str, Any], limit: int = 12) -> list[dict[str, Any]]:
    rows = [item for item in existing if isinstance(item, dict)]
    if row.get("message_id") and any(str(item.get("message_id")) == str(row.get("message_id")) for item in rows):
        return rows[-limit:]
    rows.append(row)
    return rows[-limit:]


def _persist_context(root: Path, chat_id: str | None, payload: dict[str, Any], order_id: str | None = None) -> dict[str, Any]:
    if chat_id:
        _write_json(_thread_path(root, chat_id), payload)
    if order_id:
        _write_json(_case_thread_path(root, order_id), payload)
    return payload


def record_inbound_message(
    *,
    root: Path,
    chat_id: str | None,
    message_id: str | None = None,
    user_id: str | None = None,
    user_name: str | None = None,
    text: str,
    explicit_order_id: str | None = None,
) -> dict[str, Any]:
    order_id = explicit_order_id or (normalize_order_ids(text)[0] if normalize_order_ids(text) else None)
    existing = load_thread_context(root, chat_id)
    row = {
        "role": "user",
        "message_id": message_id,
        "user_id": user_id,
        "user_name": user_name,
        "order_id": order_id,
        "text": _compact(text),
        "timestamp": utc_now_iso(),
    }
    payload = {
        **existing,
        "version": 1,
        "chat_id_hash": _hash_chat_id(chat_id),
        "chat_id_hint": str(chat_id or "")[:48],
        "updated_at": row["timestamp"],
        "last_order_id": order_id or existing.get("last_order_id"),
        "last_user_message": row,
        "last_hermes_response": existing.get("last_hermes_response"),
        "recent_messages": _merge_recent(existing.get("recent_messages") or [], row),
    }
    if payload.get("last_order_id"):
        payload["open_references"] = {
            "die_sendung": payload.get("last_order_id"),
            "der_kunde": {"order_id": payload.get("last_order_id"), "source": "last_case"},
            "docs": {"order_id": payload.get("last_order_id"), "need": "documents"},
        }
    _append_jsonl(root / "runtime" / "teams_threads" / f"{_hash_chat_id(chat_id)}.jsonl", row)
    if payload.get("last_order_id"):
        _append_jsonl(root / "orders" / str(payload["last_order_id"]).upper() / "teams" / "thread_context.jsonl", row)
    return _persist_context(root, chat_id, payload, str(payload.get("last_order_id") or "") or None)


def record_outbound_response(
    *,
    root: Path,
    chat_id: str | None,
    message_id: str | None = None,
    reply_to_message_id: str | None = None,
    text: str,
    order_id: str | None = None,
) -> dict[str, Any]:
    existing = load_thread_context(root, chat_id)
    resolved_order = order_id or existing.get("last_order_id")
    row = {
        "role": "assistant",
        "message_id": message_id,
        "reply_to_message_id": reply_to_message_id,
        "order_id": resolved_order,
        "text": _compact(text),
        "timestamp": utc_now_iso(),
    }
    payload = {
        **existing,
        "version": 1,
        "chat_id_hash": _hash_chat_id(chat_id),
        "chat_id_hint": str(chat_id or "")[:48],
        "updated_at": row["timestamp"],
        "last_order_id": resolved_order,
        "last_user_message": existing.get("last_user_message"),
        "last_hermes_response": row,
        "recent_messages": _merge_recent(existing.get("recent_messages") or [], row),
    }
    _append_jsonl(root / "runtime" / "teams_threads" / f"{_hash_chat_id(chat_id)}.jsonl", row)
    if resolved_order:
        _append_jsonl(root / "orders" / str(resolved_order).upper() / "teams" / "thread_context.jsonl", row)
    return _persist_context(root, chat_id, payload, str(resolved_order or "") or None)


def resolve_followup_reference(text: str, thread_context: dict[str, Any] | None) -> dict[str, Any]:
    raw = str(text or "")
    explicit = normalize_order_ids(raw)
    if explicit:
        return {"resolved": True, "order_id": explicit[0], "source": "explicit", "is_followup": False, "needs": []}
    ctx = thread_context if isinstance(thread_context, dict) else {}
    last_order = str(ctx.get("last_order_id") or "").strip().upper()
    if not last_order or not _ORDER_RE.fullmatch(last_order):
        return {"resolved": False, "reason": "no_last_order_id"}
    lowered = raw.lower()
    is_followup = any(marker in lowered for marker in _FOLLOWUP_MARKERS)
    if not is_followup:
        return {"resolved": False, "reason": "no_followup_marker", "last_order_id": last_order}
    wants_write = any(marker in lowered for marker in _WRITE_MARKERS)
    needs: list[str] = []
    if any(token in lowered for token in ("doc", "dokument", "ci", "pl", "packing", "commercial", "verzollung")):
        needs.append("documents")
    if any(token in lowered for token in ("kunde", "kundenseite", "antwort", "geantwortet", "mail")):
        needs.append("mail_history")
    if any(token in lowered for token in ("eta", "status", "sendung", "hängt", "haengt", "sauber", "block")):
        needs.append("tms_snapshot")
    return {
        "resolved": True,
        "order_id": last_order,
        "source": "teams_thread_context",
        "is_followup": True,
        "wants_write": wants_write,
        "needs": needs,
    }
