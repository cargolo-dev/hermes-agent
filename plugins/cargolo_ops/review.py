"""CARGOLO ASR human-review signal ingestion.

Signs review tokens for Teams notification buttons, verifies them on callback,
and appends review events to the case's audit log. Designed to run behind the
gateway webhook route 'cargolo-asr-review' as a direct_processor.

Security model:
- Tokens are HMAC-SHA256-signed (secret: HERMES_CARGOLO_ASR_REVIEW_HMAC_SECRET).
- Each proposal issues ONE nonce shared by the accept/reject tokens.
- First click per nonce wins; subsequent clicks are still logged but flagged
  `duplicate_click=True`. Dedup is driven by the audit log itself, so restarts
  do not reset the state.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from pathlib import Path
from typing import Any

from .storage import CaseStore

logger = logging.getLogger(__name__)

_TOKEN_VERSION = "v1"
_DEFAULT_TTL_SECONDS = 60 * 60 * 24 * 7  # 7 days


class ReviewTokenError(Exception):
    """Raised when the review token is missing, invalid, or expired."""


def _review_secret() -> bytes:
    raw = os.getenv("HERMES_CARGOLO_ASR_REVIEW_HMAC_SECRET", "").strip()
    if not raw:
        raise ReviewTokenError("HERMES_CARGOLO_ASR_REVIEW_HMAC_SECRET is not configured")
    return raw.encode("utf-8")


def review_signing_available() -> bool:
    return bool(os.getenv("HERMES_CARGOLO_ASR_REVIEW_HMAC_SECRET", "").strip())


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _sign_body(body: dict[str, Any]) -> str:
    body_bytes = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    signature = hmac.new(_review_secret(), body_bytes, hashlib.sha256).digest()
    return f"{_b64url_encode(body_bytes)}.{_b64url_encode(signature)}"


def sign_review_tokens(
    *,
    order_id: str,
    suggestion_key: str,
    ttl_seconds: int = _DEFAULT_TTL_SECONDS,
) -> dict[str, str]:
    """Issue a nonce plus accept/reject tokens for a single proposal.

    Returns {"nonce": str, "accepted": token, "rejected": token}.
    Caller is responsible for building the click URLs (query param ?t=<token>).
    """
    nonce = secrets.token_urlsafe(9)
    expires_at = int(time.time()) + int(ttl_seconds)
    accept_body = {
        "v": _TOKEN_VERSION,
        "o": order_id,
        "s": suggestion_key,
        "d": "accepted",
        "n": nonce,
        "e": expires_at,
    }
    reject_body = dict(accept_body, d="rejected")
    return {
        "nonce": nonce,
        "accepted": _sign_body(accept_body),
        "rejected": _sign_body(reject_body),
    }


def verify_review_token(token: str) -> dict[str, Any]:
    """Verify signature and expiry; return the decoded body or raise."""
    if not token or "." not in token:
        raise ReviewTokenError("token missing or malformed")
    body_b64, sig_b64 = token.split(".", 1)
    try:
        body_bytes = _b64url_decode(body_b64)
        actual_sig = _b64url_decode(sig_b64)
    except Exception as exc:
        raise ReviewTokenError(f"token not base64url: {exc}") from exc
    expected_sig = hmac.new(_review_secret(), body_bytes, hashlib.sha256).digest()
    if not hmac.compare_digest(expected_sig, actual_sig):
        raise ReviewTokenError("signature mismatch")
    try:
        body = json.loads(body_bytes)
    except json.JSONDecodeError as exc:
        raise ReviewTokenError(f"body not json: {exc}") from exc
    if body.get("v") != _TOKEN_VERSION:
        raise ReviewTokenError(f"unknown token version {body.get('v')!r}")
    if int(body.get("e", 0)) < int(time.time()):
        raise ReviewTokenError("token expired")
    for field in ("o", "s", "d", "n"):
        if not body.get(field):
            raise ReviewTokenError(f"token payload missing {field}")
    return body


def _nonce_already_decided(store: CaseStore, order_id: str, nonce: str) -> bool:
    for event in store.list_audit_events(order_id):
        if event.get("action") != "review":
            continue
        if event.get("token_nonce") == nonce and not event.get("duplicate_click"):
            return True
    return False


def process_review(payload: dict[str, Any], *, storage_root: Path | None = None) -> dict[str, Any]:
    """Direct processor entry: consume one click event and update the audit log.

    Expected payload (produced by the n8n review-callback workflow):
      {
        "event_type": "asr_review",
        "token": "<signed_token>",
        "meta": {
          "clicker_ip": "...",
          "user_agent": "...",
          "teams_user": "<optional>"
        }
      }
    """
    token = str(payload.get("token") or "").strip()
    try:
        body = verify_review_token(token)
    except ReviewTokenError as exc:
        logger.warning("[cargolo-asr-review] token rejected: %s", exc)
        return {"status": "error", "error": str(exc)}

    order_id = str(body["o"])
    decision = str(body["d"])
    suggestion_key = str(body["s"])
    nonce = str(body["n"])

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    actor = str(meta.get("teams_user") or "").strip() or "channel_member"

    store = CaseStore(storage_root)
    duplicate = _nonce_already_decided(store, order_id, nonce)

    extra: dict[str, Any] = {
        "actor": actor,
        "suggestion_key": suggestion_key,
        "token_nonce": nonce,
        "token_expires": body.get("e"),
        "duplicate_click": duplicate,
    }
    if meta.get("clicker_ip"):
        extra["clicker_ip"] = meta["clicker_ip"]
    if meta.get("user_agent"):
        extra["user_agent"] = meta["user_agent"]

    store.append_audit(
        order_id,
        action="review",
        result=decision,
        files=[],
        extra=extra,
    )
    return {
        "status": "duplicate" if duplicate else "ok",
        "order_id": order_id,
        "decision": decision,
        "suggestion_key": suggestion_key,
        "duplicate_click": duplicate,
    }
