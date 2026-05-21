from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import requests

from .document_analysis import analyze_case_documents, _load_pricing_ingest_adapter
from .models import CaseState, IncomingMessagePayload, TMSSnapshot, utc_now_iso
from .storage import CaseStore


ASR_MODES = {"air", "sea", "rail", "unknown"}
DEFAULT_TMS_ADMIN_USER_ID = 106
MAIL_HISTORY_FRESHNESS_TTL_SECONDS = 10 * 60


def _parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _last_successful_history_sync_at(store: CaseStore, order_id: str) -> datetime | None:
    path = store.order_path(order_id) / "audit" / "actions.jsonl"
    if not path.exists():
        return None
    latest: datetime | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if row.get("action") != "sync_case_lifecycle" or row.get("result") != "ok":
            continue
        extra = row.get("extra") if isinstance(row.get("extra"), dict) else row
        if str(extra.get("history_sync_status") or "") not in {"ok", "no_messages"}:
            continue
        seen = _parse_utc(row.get("timestamp"))
        if seen and (latest is None or seen > latest):
            latest = seen
    return latest


def _should_skip_fresh_mail_history(store: CaseStore, order_id: str, state: CaseState) -> bool:
    if not state.last_email_at:
        return False
    last_sync = _last_successful_history_sync_at(store, order_id)
    if not last_sync:
        return False
    return (datetime.now(timezone.utc) - last_sync).total_seconds() < MAIL_HISTORY_FRESHNESS_TTL_SECONDS


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


def _persist_tms_evidence_sidecars(
    *,
    store: CaseStore,
    order_id: str,
    snapshot_obj: TMSSnapshot | dict[str, Any],
    tms_snapshot: dict[str, Any],
    document_requirements: dict[str, Any],
    billing_context: dict[str, Any],
) -> None:
    """Persist already-fetched TMS evidence after the local case is allowed to exist."""
    case_root = store.ensure_case(order_id)
    tms_dir = case_root / "tms"
    tms_dir.mkdir(parents=True, exist_ok=True)
    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot.get("detail"), dict) else {}
    billing_items = tms_snapshot.get("billing_items") if isinstance(tms_snapshot.get("billing_items"), list) else []
    if detail:
        (tms_dir / "shipment_detail.json").write_text(json.dumps(detail, ensure_ascii=False, indent=2), encoding="utf-8")
    if billing_items:
        (tms_dir / "shipment_billing_items.json").write_text(json.dumps(billing_items, ensure_ascii=False, indent=2), encoding="utf-8")
    if document_requirements:
        (tms_dir / "document_requirements.json").write_text(
            json.dumps(document_requirements, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    if billing_context:
        (tms_dir / "billing_context.json").write_text(json.dumps(billing_context, ensure_ascii=False, indent=2), encoding="utf-8")

    if isinstance(snapshot_obj, TMSSnapshot):
        sync_log = {
            "timestamp": utc_now_iso(),
            "phase": "read_sync",
            "action": "fetch_tms_bundle",
            "source": snapshot_obj.source,
            "provider": getattr(snapshot_obj, "provider", None),
            "status": snapshot_obj.status,
            "shipment_uuid": snapshot_obj.shipment_uuid,
            "shipment_number": snapshot_obj.shipment_number,
            "warnings": snapshot_obj.warnings,
            "document_requirements_synced": bool(document_requirements),
            "billing_context_synced": bool(billing_context),
        }
    else:
        sync_log = {
            "timestamp": utc_now_iso(),
            "phase": "read_sync",
            "action": "fetch_tms_bundle",
            "source": tms_snapshot.get("source"),
            "provider": tms_snapshot.get("provider"),
            "status": tms_snapshot.get("status"),
            "shipment_uuid": tms_snapshot.get("shipment_uuid"),
            "shipment_number": tms_snapshot.get("shipment_number") or order_id,
            "warnings": tms_snapshot.get("warnings", []),
            "document_requirements_synced": bool(document_requirements),
            "billing_context_synced": bool(billing_context),
        }
    store.append_tms_sync_log(order_id, sync_log)


def _detail_has_operational_cargo(detail: Any) -> bool:
    if not isinstance(detail, dict):
        return False
    cargo = detail.get("cargo")
    cargo_keys = ("quantity", "pieces", "weight_kg", "total_weight_kg", "volume_m3", "total_volume_m3", "description", "goods_description")
    if isinstance(cargo, list):
        for row in cargo:
            if isinstance(row, dict) and any(row.get(key) not in (None, "", [], {}) for key in cargo_keys):
                return True
    return any(detail.get(key) not in (None, "", [], {}) for key in ("pieces", "total_pieces", "weight_kg", "total_weight_kg", "volume_m3", "total_volume_m3"))


def _enrich_tms_snapshot_with_cached_detail(*, case_root: Path, tms_snapshot: dict[str, Any]) -> dict[str, Any]:
    """Keep document monitoring from treating transient TMS read errors as empty TMS data.

    The live MCP snapshot can occasionally return an error/partial shell while the
    sidecar `tms/shipment_detail.json` from the last successful read still contains
    the operational cargo rows. For read-only reconciliation it is safer to use
    that cached detail with an explicit warning than to say fields are "not
    gepflegt" and create misleading review cards.
    """
    if not isinstance(tms_snapshot, dict):
        return tms_snapshot
    raw_detail = tms_snapshot.get("detail")
    detail: dict[str, Any] = raw_detail if isinstance(raw_detail, dict) else {}
    if _detail_has_operational_cargo(detail):
        return tms_snapshot
    cached_path = case_root / "tms" / "shipment_detail.json"
    if not cached_path.exists():
        return tms_snapshot
    try:
        cached_detail = json.loads(cached_path.read_text(encoding="utf-8"))
    except Exception:
        return tms_snapshot
    if not _detail_has_operational_cargo(cached_detail):
        return tms_snapshot
    merged_detail = {
        **(cached_detail if isinstance(cached_detail, dict) else {}),
        **(detail if isinstance(detail, dict) else {}),
    }
    # Preserve fresh document rows from the current read, but fill operational
    # cargo/route fields from the previous successful detail if the current read
    # was only an error shell.
    for key in ("cargo", "origin", "destination", "parties", "transport_legs", "freight_details"):
        current_value = detail.get(key)
        if key == "cargo":
            current_is_empty = not _detail_has_operational_cargo({"cargo": current_value})
        else:
            current_is_empty = current_value in (None, "", [], {})
        if current_is_empty and isinstance(cached_detail, dict) and key in cached_detail:
            merged_detail[key] = cached_detail[key]
    enriched = {**tms_snapshot, "detail": merged_detail}
    if str(enriched.get("status") or "").strip().lower() in {"", "error", "unknown"} and isinstance(cached_detail, dict):
        enriched["status"] = cached_detail.get("status") or enriched.get("status")
    warnings = [str(item) for item in enriched.get("warnings", []) if item] if isinstance(enriched.get("warnings"), list) else []
    warning = "used_cached_tms_shipment_detail_after_partial_snapshot"
    if warning not in warnings:
        warnings.append(warning)
    enriched["warnings"] = warnings
    return enriched


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


def _resolve_tms_download_url(
    *,
    an: str | None,
    record: dict[str, Any],
    tms_client: Any | None,
    admin_user_id: int = DEFAULT_TMS_ADMIN_USER_ID,
) -> tuple[str | None, list[str]]:
    """Ask the TMS MCP for an ephemeral/signed document URL before falling back to raw Vault URLs.

    The signed URL is used only for the immediate download and is not persisted
    into the registry, because it can contain short-lived credentials.
    """
    if not an or tms_client is None or not hasattr(tms_client, "get_document_download_url"):
        return None, []
    tms_document_id = record.get("tms_document_id") or record.get("document_uuid") or record.get("uuid")
    document_id = record.get("document_id") or record.get("id")
    if not tms_document_id and document_id in (None, ""):
        return None, []
    try:
        payload = tms_client.get_document_download_url(
            admin_user_id=admin_user_id,
            an=an,
            tms_document_id=str(tms_document_id) if tms_document_id else None,
            document_id=int(document_id) if document_id not in (None, "") else None,
            ttl_seconds=3600,
        )
    except Exception as exc:
        return None, [f"download_url_tool_failed:{type(exc).__name__}"]
    document = payload.get("document") if isinstance(payload.get("document"), dict) else {}
    candidate = str(
        payload.get("download_url")
        or payload.get("signed_url")
        or payload.get("url")
        or document.get("download_url")
        or document.get("signed_url")
        or document.get("url")
        or ""
    ).strip()
    warnings = [str(item) for item in payload.get("warnings", []) if item] if isinstance(payload, dict) else []
    return (candidate or None), warnings


def mirror_tms_documents(*, case_root: Path, registry: dict[str, Any], tms_client: Any | None = None, an: str | None = None) -> list[dict[str, Any]]:
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
        resolved_url, resolve_warnings = _resolve_tms_download_url(an=an or case_root.name, record=record, tms_client=tms_client)
        if resolved_url:
            record["download_url_source"] = "tms_mcp_get_document_download_url"
        if resolve_warnings:
            record["download_url_warnings"] = resolve_warnings
        url = resolved_url or str(record.get("url") or record.get("download_url") or "").strip()
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
        if (
            tms_client is not None
            and hasattr(tms_client, "_headers")
            and str(url).startswith(str(getattr(tms_client, "api_url", "")).rstrip("/"))
        ):
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

    TMS-first invariant: for live ASR cases the TMS existence/read gate happens
    before the local case folder is created. Unknown AN/BU returns `skipped`
    without n8n/mail-history lookup and without creating `orders/<AN-or-BU>/`.
    Once TMS evidence is positive/usable, the local case becomes the derived
    working folder for mail history, TMS documents, registry and analysis.
    """
    from . import processor  # late import avoids circular dependency
    from .adapters import build_tms_client_from_env

    order_id = str(order_id or "").strip().upper()
    if not order_id:
        raise ValueError("sync_case_lifecycle requires a non-empty AN/BU")

    store = CaseStore(storage_root)
    shipment_exists = processor._live_shipment_exists(order_id)
    if shipment_exists is False:
        return {
            "status": "skipped",
            "reason": "shipment_not_found_in_tms",
            "order_id": order_id,
            "message": f"{order_id} not found in ASR TMS; skipped local case creation and mail-history sync.",
        }

    snapshot_obj, tms_document_requirements, billing_context = processor._fetch_tms_bundle(
        store,
        order_id,
        None,
        persist_case_files=False,
    )
    tms_snapshot = _snapshot_to_dict(snapshot_obj)
    if tms_document_requirements:
        tms_snapshot["document_requirements"] = tms_document_requirements

    case_existed = store.order_path(order_id).exists()
    case_root = store.ensure_case(order_id)
    tms_snapshot = _enrich_tms_snapshot_with_cached_detail(case_root=case_root, tms_snapshot=tms_snapshot)
    state = store.load_case_state(order_id)
    prior_registry = store.load_document_registry(order_id)
    _persist_tms_evidence_sidecars(
        store=store,
        order_id=order_id,
        snapshot_obj=snapshot_obj,
        tms_snapshot=tms_snapshot,
        document_requirements=tms_document_requirements,
        billing_context=billing_context,
    )
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
    history_sync_status = "disabled"
    history_sync_mode = "skipped"
    history_client_available: bool | None = None
    prior_last_email_at = state.last_email_at
    if refresh_history:
        history_sync_mode = "delta" if prior_last_email_at else "full_first"
        if _should_skip_fresh_mail_history(store, order_id, state):
            history_sync_mode = "freshness_skip"
            history_sync_status = "fresh_skipped"
        else:
            try:
                history_client_available = processor.build_mail_history_client_from_env() is not None
            except Exception:
                history_client_available = None
            try:
                history_count = processor._sync_mail_history(store, order_id, state, mailbox, exclude_message_ids=set())
                if history_client_available is False:
                    history_sync_status = "no_client"
                    history_error = "mail_history_sync_unavailable:no_client"
                elif history_count > 0:
                    history_sync_status = "ok"
                else:
                    # A successful call with zero new messages is not a failure, but it
                    # is distinct from an unavailable client or crashed n8n workflow.
                    history_sync_status = "no_messages"
            except Exception as exc:
                history_sync_status = "failed"
                history_error = f"mail_history_sync_failed: {exc}"

    history_rows = store.list_email_index(order_id)
    latest_history_at = max(
        (str(row.get("received_at") or "").strip() for row in history_rows if isinstance(row, dict) and str(row.get("received_at") or "").strip()),
        default=None,
    )
    if latest_history_at and (not state.last_email_at or latest_history_at > state.last_email_at):
        state.last_email_at = latest_history_at
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
    from .tms_provider import build_tms_provider_from_env

    tms_client = build_tms_provider_from_env() or build_tms_client_from_env()
    mirrored_tms_documents = mirror_tms_documents(case_root=case_root, registry=registry, tms_client=tms_client, an=order_id)
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
    if not case_existed and refresh_history and history_sync_status in {"no_client", "failed"}:
        open_questions.add("Initial-Mail-Historie nicht belastbar synchronisiert; operative Mail-/Kundenaussagen nur mit Vorbehalt ableiten.")
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
        summary=(
            f"Lifecycle sync: TMS fresh, mail_history={history_sync_status}/{history_sync_mode} +{history_count}, "
            f"TMS docs mirrored {len(registry.get('mirrored_tms_documents', []))}"
        ),
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
            "history_sync_status": history_sync_status,
            "history_sync_mode": history_sync_mode,
            "history_client_available": history_client_available,
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
        "history_sync_status": history_sync_status,
        "history_sync_mode": history_sync_mode,
        "history_client_available": history_client_available,
        "history_sync_error": history_error,
        "last_email_at": state.last_email_at,
        "tms_snapshot_path": str(tms_path),
        "document_registry_path": str(registry_path),
        "registry": registry,
        "tms_snapshot": tms_snapshot,
        "state": state.model_dump(mode="json"),
    }
