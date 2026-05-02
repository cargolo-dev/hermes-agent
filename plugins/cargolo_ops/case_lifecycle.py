from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import requests

from .document_analysis import analyze_case_documents, _load_pricing_ingest_adapter
from .models import CaseState, IncomingMessagePayload, TMSSnapshot, utc_now_iso
from .storage import CaseStore


ASR_MODES = {"air", "sea", "rail", "unknown"}


def normalize_asr_mode(value: Any) -> str:
    """Normalize CARGOLO ASR transport mode vocabulary.

    ASR means Air, Sea, Rail. Land/Road values are deliberately not normalized
    into ASR modes; they remain unknown in the ASR case lifecycle.
    """
    mode = str(value or "").strip().lower()
    if mode in {"air", "airfreight", "air_freight"}:
        return "air"
    if mode in {"sea", "ocean", "oceanfreight", "sea_freight", "seafreight"}:
        return "sea"
    if mode in {"rail", "train"}:
        return "rail"
    return "unknown"


def _snapshot_to_dict(snapshot: TMSSnapshot | dict[str, Any]) -> dict[str, Any]:
    if isinstance(snapshot, TMSSnapshot):
        return snapshot.model_dump(mode="json")
    return dict(snapshot or {})


def _tms_mode_from_snapshot(tms_snapshot: dict[str, Any]) -> str:
    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    candidates = []
    if isinstance(detail, dict):
        candidates.extend([
            detail.get("network"),
            detail.get("transport_mode"),
            detail.get("mode"),
            detail.get("shipment_mode"),
        ])
    candidates.extend([tms_snapshot.get("network"), tms_snapshot.get("mode")])
    for value in candidates:
        mode = normalize_asr_mode(value)
        if mode != "unknown":
            return mode
    return "unknown"


def _download_tms_document(url: str, target: Path, *, headers: dict[str, str] | None = None) -> tuple[bool, str | None]:
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        if url.startswith("file://"):
            shutil.copyfile(Path(url[7:]), target)
        else:
            response = requests.get(url, headers=headers or {}, timeout=45, allow_redirects=True)
            if response.status_code == 403 and response.url != url and urlsplit(response.url).netloc == "storage.googleapis.com":
                return False, "private_vault_redirect_requires_signed_url_or_public_storage"
            response.raise_for_status()
            target.write_bytes(response.content)
        return True, None
    except Exception as exc:  # keep monitoring read-first; do not crash lifecycle on one failed file
        return False, str(exc)


def mirror_tms_documents(*, case_root: Path, registry: dict[str, Any], tms_client: Any | None = None) -> list[dict[str, Any]]:
    """Mirror downloadable TMS documents into the canonical case folder.

    Output lives under `<case>/documents/tms/`. Existing files with the same
    SHA-256 are reused. Missing download URLs are recorded as not mirrored so a
    TMS upload without local file remains visible as a mirroring gap.
    """
    tms_dir = case_root / "documents" / "tms"
    tms_dir.mkdir(parents=True, exist_ok=True)
    mirrored: list[dict[str, Any]] = []
    existing_by_sha: dict[str, Path] = {}
    for existing in tms_dir.iterdir():
        if existing.is_file():
            try:
                existing_by_sha[hashlib.sha256(existing.read_bytes()).hexdigest()] = existing
            except Exception:
                continue

    for idx, row in enumerate([r for r in registry.get("tms_documents", []) if isinstance(r, dict)], start=1):
        record = dict(row)
        url = str(record.get("url") or record.get("download_url") or "").strip()
        if url.startswith("/vault/"):
            url = f"https://api.cargolo.de{url}"
        if not url:
            record["mirror_status"] = "no_download_url"
            mirrored.append(record)
            continue
        filename = str(record.get("filename") or record.get("label") or f"tms_document_{idx}").strip() or f"tms_document_{idx}"
        safe_name = filename.replace("/", "_").replace("\\", "_")
        target = tms_dir / safe_name
        headers = None
        if tms_client is not None and str(url).startswith(str(getattr(tms_client, "api_url", "")).rstrip("/")):
            headers = {
                key: value
                for key, value in tms_client._headers().items()
                if key.lower() != "content-type"
            }
        ok, error = _download_tms_document(url, target, headers=headers)
        if not ok:
            record["mirror_status"] = "download_failed"
            record["mirror_error"] = error
            mirrored.append(record)
            continue
        sha = hashlib.sha256(target.read_bytes()).hexdigest()
        if sha in existing_by_sha and existing_by_sha[sha] != target:
            target.unlink(missing_ok=True)
            target = existing_by_sha[sha]
        else:
            existing_by_sha[sha] = target
        record.update({
            "mirror_status": "mirrored",
            "local_path": str(target),
            "sha256": sha,
            "size": target.stat().st_size,
        })
        mirrored.append(record)
    return mirrored


def sync_case_lifecycle(
    order_id: str,
    *,
    storage_root: Path | None = None,
    refresh_history: bool = True,
    mailbox: str = "asr@cargolo.com",
    analyze_documents: bool = True,
) -> dict[str, Any]:
    """Shared read-first lifecycle used by ingest and document monitoring.

    It owns the canonical per-transport folder shape and evidence refresh:
    folder -> TMS snapshot/requirements -> full-first/delta mail history -> TMS
    document mirror -> merged registry -> optional document analysis.
    """
    from . import processor  # late import avoids circular dependency
    from .adapters import build_tms_client_from_env

    order_id = str(order_id or "").strip().upper()
    if not order_id:
        raise ValueError("sync_case_lifecycle requires a non-empty AN/BU")

    store = CaseStore(storage_root)
    case_existed = store.order_path(order_id).exists()
    case_root = store.ensure_case(order_id)
    state = store.load_case_state(order_id)
    prior_registry = store.load_document_registry(order_id)

    snapshot_obj, tms_document_requirements, billing_context = processor._fetch_tms_bundle(store, order_id, None)
    tms_snapshot = _snapshot_to_dict(snapshot_obj)
    if tms_document_requirements:
        tms_snapshot["document_requirements"] = tms_document_requirements
    if billing_context:
        tms_snapshot["billing_context"] = billing_context
        try:
            pricing_module = _load_pricing_ingest_adapter()
            if hasattr(pricing_module, "record_pricing_billing_context"):
                billing_event = pricing_module.record_pricing_billing_context(
                    order_id=order_id,
                    billing_context=billing_context,
                    tms_snapshot=tms_snapshot,
                    source_skill="cargolo-tms-document-monitoring",
                    source_event="case_lifecycle.billing_context",
                )
                if billing_event and billing_event.get("status") != "skipped":
                    tms_snapshot["pricing_kb_billing_event"] = billing_event
        except Exception as exc:  # never let pricing indexing break lifecycle sync
            tms_snapshot["pricing_kb_billing_event"] = {"status": "error", "error": str(exc)}

    history_count = 0
    history_error: str | None = None
    if refresh_history:
        try:
            history_count = processor._sync_mail_history(store, order_id, state, mailbox, exclude_message_ids=set())
        except Exception as exc:
            history_error = f"mail_history_sync_failed: {exc}"

    history_rows = store.list_email_index(order_id)
    attachment_records = processor._collect_attachment_records_from_email_index(history_rows)
    lifecycle_message = IncomingMessagePayload(
        message_id=f"lifecycle:{order_id}:{utc_now_iso()}",
        subject=f"Lifecycle sync {order_id}",
        **{"from": "asr-lifecycle@cargolo.internal"},
        received_at=utc_now_iso(),
        body_text="",
        attachments=[],
    )
    registry = processor._build_document_registry(
        prior_registry=prior_registry,
        message=lifecycle_message,
        attachment_records=attachment_records,
        tms_snapshot=tms_snapshot,
        tms_document_requirements=tms_document_requirements,
    )
    tms_client = build_tms_client_from_env()
    mirrored_tms_documents = mirror_tms_documents(case_root=case_root, registry=registry, tms_client=tms_client)
    registry["tms_documents"] = mirrored_tms_documents
    registry["mirrored_tms_documents"] = [row for row in mirrored_tms_documents if row.get("mirror_status") == "mirrored"]
    registry["tms_mirroring_gaps"] = [row for row in mirrored_tms_documents if row.get("mirror_status") != "mirrored"]

    analysis_open_questions: list[str] = []
    if analyze_documents:
        registry, analysis_open_questions = analyze_case_documents(
            order_id=order_id,
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    state.mode = _tms_mode_from_snapshot(tms_snapshot) or normalize_asr_mode(state.mode)
    state.current_status = str(tms_snapshot.get("status") or state.current_status or "synced")
    state.documents_received = sorted(set(registry.get("received_types", [])))
    state.documents_expected = sorted(set(registry.get("expected_types", [])))
    state.missing_information = sorted({f"document:{doc}" for doc in registry.get("missing_types", []) if doc})
    # Recompute lifecycle-derived questions from the current evidence instead of
    # accumulating stale analyzer errors/open items across reruns.
    open_questions = set(analysis_open_questions)
    if history_error:
        open_questions.add(history_error)
    for gap in registry.get("tms_mirroring_gaps", []):
        label = gap.get("label") or gap.get("filename") or gap.get("document_type")
        open_questions.add(f"TMS-Dokument nicht lokal gespiegelt: {label}")
    state.open_questions = sorted(open_questions)
    state.tms_last_sync_at = utc_now_iso()

    state_path = store.save_case_state(order_id, state)
    tms_path = store.save_tms_snapshot(order_id, tms_snapshot)
    registry_path = store.save_document_registry(order_id, registry)
    timeline_path = store.append_timeline(
        order_id,
        heading="case lifecycle sync",
        summary=f"Lifecycle sync: TMS fresh, history +{history_count}, TMS docs mirrored {len(registry.get('mirrored_tms_documents', []))}",
        delta=f"expected_docs={','.join(registry.get('expected_types', [])) or '-'}; missing_docs={','.join(registry.get('missing_types', [])) or '-'}",
        next_step="Dokumentenmonitoring/Reconciliation auf dieser aktualisierten Evidenz ausführen.",
    )
    store.append_audit(
        order_id,
        action="sync_case_lifecycle",
        result="ok",
        files=[str(state_path), str(tms_path), str(registry_path), str(timeline_path)],
        extra={
            "initialized": not case_existed,
            "history_sync_count": history_count,
            "history_sync_error": history_error,
            "tms_documents_mirrored": len(registry.get("mirrored_tms_documents", [])),
            "tms_mirroring_gaps": len(registry.get("tms_mirroring_gaps", [])),
            "analysis_enabled": analyze_documents,
        },
    )

    return {
        "status": "ok",
        "order_id": order_id,
        "case_root": str(case_root),
        "initialized": not case_existed,
        "history_sync_count": history_count,
        "history_sync_error": history_error,
        "tms_snapshot_path": str(tms_path),
        "document_registry_path": str(registry_path),
        "registry": registry,
        "tms_snapshot": tms_snapshot,
        "state": state.model_dump(mode="json"),
    }
