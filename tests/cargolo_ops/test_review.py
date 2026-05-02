import json
import time

import pytest

from plugins.cargolo_ops.review import (
    ReviewTokenError,
    process_review,
    sign_review_tokens,
    verify_review_token,
)
from plugins.cargolo_ops.storage import CaseStore


@pytest.fixture(autouse=True)
def _secret(monkeypatch):
    monkeypatch.setenv("HERMES_CARGOLO_ASR_REVIEW_HMAC_SECRET", "test-secret-value")


def test_tokens_round_trip():
    tokens = sign_review_tokens(order_id="AN-12001", suggestion_key="next_step")
    accepted = verify_review_token(tokens["accepted"])
    rejected = verify_review_token(tokens["rejected"])
    assert accepted["o"] == "AN-12001"
    assert accepted["d"] == "accepted"
    assert rejected["d"] == "rejected"
    # Same nonce for the pair — enables "first click wins" dedup
    assert accepted["n"] == rejected["n"] == tokens["nonce"]


def test_verify_rejects_tampered_signature():
    tokens = sign_review_tokens(order_id="AN-12001", suggestion_key="next_step")
    body_b64, sig_b64 = tokens["accepted"].split(".", 1)
    bad = f"{body_b64}.{'A' * len(sig_b64)}"
    with pytest.raises(ReviewTokenError):
        verify_review_token(bad)


def test_verify_rejects_expired_token(monkeypatch):
    real_time = time.time
    monkeypatch.setattr("plugins.cargolo_ops.review.time.time", real_time)
    tokens = sign_review_tokens(order_id="AN-12001", suggestion_key="next_step", ttl_seconds=1)
    # jump forward
    monkeypatch.setattr("plugins.cargolo_ops.review.time.time", lambda: real_time() + 3600)
    with pytest.raises(ReviewTokenError):
        verify_review_token(tokens["accepted"])


def test_process_review_writes_audit_event(tmp_path):
    tokens = sign_review_tokens(order_id="AN-12001", suggestion_key="next_step")
    result = process_review(
        {
            "event_type": "asr_review",
            "token": tokens["accepted"],
            "meta": {"clicker_ip": "203.0.113.7", "user_agent": "curl/8"},
        },
        storage_root=tmp_path,
    )
    assert result["status"] == "ok"
    assert result["decision"] == "accepted"
    assert result["duplicate_click"] is False

    audit_path = tmp_path / "orders" / "AN-12001" / "audit" / "actions.jsonl"
    entries = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    review_events = [e for e in entries if e.get("action") == "review"]
    assert len(review_events) == 1
    event = review_events[0]
    assert event["actor"] == "channel_member"
    assert event["result"] == "accepted"
    assert event["suggestion_key"] == "next_step"
    assert event["token_nonce"] == tokens["nonce"]
    assert event["duplicate_click"] is False
    assert event["clicker_ip"] == "203.0.113.7"


def test_process_review_marks_second_click_as_duplicate(tmp_path):
    tokens = sign_review_tokens(order_id="AN-12001", suggestion_key="next_step")
    first = process_review({"token": tokens["accepted"]}, storage_root=tmp_path)
    second = process_review({"token": tokens["rejected"]}, storage_root=tmp_path)
    assert first["status"] == "ok"
    assert second["status"] == "duplicate"
    assert second["duplicate_click"] is True

    store = CaseStore(tmp_path)
    review_events = [e for e in store.list_audit_events("AN-12001") if e.get("action") == "review"]
    assert len(review_events) == 2
    assert review_events[0]["duplicate_click"] is False
    assert review_events[1]["duplicate_click"] is True
    # Both share the nonce, so the aggregator can correlate them
    assert {e["token_nonce"] for e in review_events} == {tokens["nonce"]}


def test_process_review_rejects_invalid_token(tmp_path):
    result = process_review({"token": "not-a-real-token"}, storage_root=tmp_path)
    assert result["status"] == "error"
    assert "error" in result


def test_process_review_rejects_missing_token(tmp_path):
    result = process_review({}, storage_root=tmp_path)
    assert result["status"] == "error"


def test_signing_requires_secret(monkeypatch):
    monkeypatch.delenv("HERMES_CARGOLO_ASR_REVIEW_HMAC_SECRET", raising=False)
    with pytest.raises(ReviewTokenError):
        sign_review_tokens(order_id="AN-12001", suggestion_key="next_step")
