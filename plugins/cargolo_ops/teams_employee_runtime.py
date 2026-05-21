from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from .employee_agent import (
    BoundaryAction,
    ContextNeed,
    EmployeeRequest,
    ResponseMode,
    handle_employee_request,
)
from .evidence_bundle import build_evidence_bundle
from .evidence_freshness import EvidenceNeed, plan_evidence_refresh
RefreshFunc = Callable[..., dict[str, Any]]
TmsExistsFunc = Callable[[str], bool | None]


def _unknown_shipment_response(order_id: str) -> dict[str, Any]:
    return {
        "handled": True,
        "allow_generic_chat": False,
        "classification": "shipment_not_found_in_tms",
        "order_id": order_id,
        "response_text": (
            f"{order_id} finde ich nicht im ASR-TMS.\n"
            "Lage: Ich stoppe hier TMS-first und starte keine Mail-/n8n-Suche.\n"
            "Nächster Schritt: AN/BU prüfen oder die Sendung zuerst im TMS anlegen/finden."
        ),
    }


def _needs_to_evidence(needs: list[ContextNeed] | list[str]) -> list[EvidenceNeed]:
    mapped: list[EvidenceNeed] = []
    for need in needs:
        value = need.value if isinstance(need, ContextNeed) else str(need)
        try:
            mapped.append(EvidenceNeed(value))
        except ValueError:
            continue
    return mapped


def _guarded_response(action: BoundaryAction, order_id: str | None) -> str:
    prefix = f"Für {order_id}: " if order_id else ""
    if action is BoundaryAction.TMS_WRITE:
        return prefix + "ich schreibe das nicht direkt ins TMS. Ich kann daraus einen Review-/Freigabevorschlag machen; ohne Freigabe bleibt alles read-only."
    if action is BoundaryAction.CUSTOMER_MESSAGE_SEND:
        return prefix + "ich sende keine Kundenmail direkt aus Teams. Ich bereite höchstens einen Entwurf vor, Versand nur nach Freigabe."
    if action is BoundaryAction.DOCUMENT_UPLOAD:
        return prefix + "ich lade kein Dokument direkt hoch. Uploads laufen nur über den freigegebenen Review-/Upload-Pfad."
    if action is BoundaryAction.TEAMS_SEND:
        return prefix + "ich poste nichts proaktiv in Teams ohne Freigabe. Als Antwort auf diese Anfrage kann ich aber eine Einschätzung liefern."
    return prefix + "diese Aktion bleibt bis zur Freigabe read-only."


def _intent_from_text(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ("eta", "ankunft", "wann kommt", "liefertermin")):
        return "eta_status"
    if any(token in lowered for token in ("geantwortet", "antwort", "kunde", "mail")):
        return "customer_reply_check"
    if any(token in lowered for token in ("fehlt", "dokument", "doc", "ci", "pl", "packing", "commercial")):
        return "document_gap_check"
    if any(token in lowered for token in ("sauber", "alles ok", "risiko", "freigabe")):
        return "cleanliness_check"
    return "case_overview"


def _case_prompt(*, text: str, order_id: str, bundle: dict[str, Any], freshness_plan: dict[str, Any], lifecycle: dict[str, Any] | None = None, structured_intent: Any | None = None) -> str:
    caveats = list(freshness_plan.get("caveats") or []) + list(bundle.get("source_limitations") or [])
    intent_payload = structured_intent.to_dict() if hasattr(structured_intent, "to_dict") else (structured_intent or {})
    intent_name = intent_payload.get("intent") or _intent_from_text(text)
    thread_summary = ((bundle.get("sources") or {}).get("teams_thread_context") or {}).get("summary") or {}
    return (
        "Rolle: Du bist Hermes CARGOLO in Microsoft Teams — ein natürlicher, sehr guter interner Speditionsmitarbeiter.\n"
        f"Case: {order_id}. Intent: case_assist_agentic / {intent_name}. Strukturierter Intent: {json.dumps(intent_payload, ensure_ascii=False, sort_keys=True)[:2000]}.\n"
        "Vorarbeit: Der lokale CARGOLO Case wurde für diese Frage frisch synchronisiert bzw. per zentraler Freshness-Policy geprüft.\n"
        "Antworte ausschließlich aus dem folgenden lokalen EVIDENCE_BUNDLE und der Freshness-Policy. "
        "Keine externen Fakten, keine Annahmen, keine Tool-/Debugpfade. Wenn eine Quelle fehlt oder alt ist: klarer Vorbehalt, nicht raten.\n"
        "Sicherheitsgrenze: kein TMS-Write, keine Kundenmail, kein Dokumentupload, keine proaktive Teams-Sendung.\n"
        "Antwortstil: Deutsch, knapp, menschlich-operativ. Struktur: Lage, Auffällig, Belastbar/Vorbehalt, Empfehlung, Nächster Schritt. Maximal Top-3 Auffälligkeiten.\n"
        f"Freshness Refresh: required={freshness_plan.get('required_sources')}; refreshed={bool(lifecycle)}; refresh_sources={freshness_plan.get('refresh_sources')}; caveats={caveats}.\n"
        "Wenn Mailhistorie nicht belastbar ist, keine Aussage treffen, ob der Kunde geantwortet hat. Wenn Dokumentanalyse fehlt, Dokumentlage nur mit Vorbehalt.\n"
        f"Teams-Thread-Kontext (nur Referenzauflösung, keine operative Wahrheit): {json.dumps(thread_summary, ensure_ascii=False, sort_keys=True)[:2500]}.\n"
        "EVIDENCE_BUNDLE:\n"
        f"{json.dumps(bundle, ensure_ascii=False, sort_keys=True)[:12000]}\n"
        f"Originalfrage: {text.strip()}"
    )


def run_teams_employee_runtime(
    *,
    text: str,
    root: Path | None = None,
    channel_id: str | None = None,
    message_id: str | None = None,
    user_id: str | None = None,
    user_name: str | None = None,
    is_dedicated_channel: bool = False,
    now: str | None = None,
    refresh_func: RefreshFunc | None = None,
    tms_exists_func: TmsExistsFunc | None = None,
    order_id_override: str | None = None,
    force_full_case_refresh: bool = False,
) -> dict[str, Any]:
    del channel_id, message_id, user_id, user_name, is_dedicated_channel  # reserved for audit metadata
    storage_root = Path(root) if root is not None else None
    request = EmployeeRequest(text=text, channel="teams", order_id=order_id_override)
    response = handle_employee_request(request)
    if order_id_override and response.mode is ResponseMode.FREE_CHAT:
        response.mode = ResponseMode.CASE_ASSIST
        response.order_id = str(order_id_override).upper()
        response.context_needs = [ContextNeed.CASE_FOLDER]

    if response.mode is ResponseMode.FREE_CHAT:
        return {
            "handled": False,
            "allow_generic_chat": True,
            "classification": "free_chat",
            "agent_prompt": text,
            "should_send_to_teams": False,
            "should_write_tms": False,
            "should_send_customer_message": False,
        }

    if response.mode is ResponseMode.GUARDED_ACTION_REQUIRED:
        return {
            "handled": True,
            "allow_generic_chat": False,
            "classification": "guarded_action_required",
            "order_id": response.order_id,
            "boundary_action": response.boundary_action.value,
            "response_text": _guarded_response(response.boundary_action, response.order_id),
            "should_send_to_teams": False,
            "should_write_tms": False,
            "should_send_customer_message": False,
        }

    if response.mode is ResponseMode.DRAFT_ONLY:
        needs = response.context_needs or [ContextNeed.CASE_FOLDER, ContextNeed.MAIL_HISTORY, ContextNeed.TMS_SNAPSHOT]
    else:
        needs = response.context_needs or [ContextNeed.CASE_FOLDER]

    order_id = response.order_id
    if not order_id:
        return {
            "handled": False,
            "allow_generic_chat": True,
            "classification": "free_chat",
            "agent_prompt": text,
            "should_send_to_teams": False,
            "should_write_tms": False,
            "should_send_customer_message": False,
        }

    if tms_exists_func is not None:
        exists = tms_exists_func(order_id)
        if exists is False:
            result = _unknown_shipment_response(order_id)
            result.update({"should_write_tms": False, "should_send_customer_message": False, "should_send_to_teams": False})
            return result

    evidence_needs = _needs_to_evidence(needs)
    freshness = plan_evidence_refresh(order_id, storage_root=storage_root, question=text, requested_needs=evidence_needs, now=now)
    if force_full_case_refresh:
        forced_sources = ["case_state", "tms_snapshot", "email_index", "document_registry", "document_analysis", "billing_context"]
        freshness["required_sources"] = forced_sources
        existing_refresh = set(freshness.get("refresh_sources") or [])
        if not existing_refresh:
            existing_refresh.add("case_state")
        freshness["refresh_sources"] = sorted(existing_refresh)
        freshness["requires_refresh"] = True
        freshness["refresh_history"] = True
        freshness["analyze_documents"] = True
    lifecycle: dict[str, Any] | None = None
    pre_refresh_plan = freshness
    if freshness.get("requires_refresh"):
        if refresh_func is None:
            from .case_lifecycle import sync_case_lifecycle

            refresh_func = lambda **kwargs: sync_case_lifecycle(**kwargs)  # noqa: E731
        try:
            lifecycle = refresh_func(
                order_id=order_id,
                storage_root=storage_root,
                refresh_history=bool(freshness.get("refresh_history")),
                analyze_documents=bool(freshness.get("analyze_documents")),
            )
        except Exception as exc:
            lifecycle = {"status": "error", "error": str(exc)}
        if lifecycle and lifecycle.get("status") == "skipped" and lifecycle.get("reason") == "shipment_not_found_in_tms":
            result = _unknown_shipment_response(order_id)
            result.update({"should_write_tms": False, "should_send_customer_message": False, "should_send_to_teams": False})
            return result
        if not lifecycle or lifecycle.get("status") != "error":
            freshness = plan_evidence_refresh(order_id, storage_root=storage_root, question=text, requested_needs=evidence_needs, now=now)
            if force_full_case_refresh:
                freshness["required_sources"] = ["case_state", "tms_snapshot", "email_index", "document_registry", "document_analysis", "billing_context"]

    bundle = build_evidence_bundle(order_id, storage_root=storage_root, question=text, now=now)
    if lifecycle and lifecycle.get("status") == "skipped" and lifecycle.get("reason") == "shipment_not_found_in_tms":
        result = _unknown_shipment_response(order_id)
        result.update({"should_write_tms": False, "should_send_customer_message": False, "should_send_to_teams": False})
        return result

    classification = "case_evidence_runtime_handoff" if response.mode is ResponseMode.CASE_ASSIST else "draft_runtime_handoff"
    prompt = _case_prompt(text=text, order_id=order_id, bundle=bundle, freshness_plan=freshness, lifecycle=lifecycle, structured_intent=response.structured_intent)
    return {
        "handled": False,
        "allow_generic_chat": True,
        "classification": classification,
        "order_id": order_id,
        "agent_prompt": prompt,
        "evidence_bundle": bundle,
        "freshness_plan": freshness,
        "structured_intent": response.structured_intent.to_dict() if response.structured_intent else None,
        "pre_refresh_plan": pre_refresh_plan,
        "lifecycle": lifecycle,
        "progress_message": "Bin dran, aktualisiere TMS/Mail/Dokumente…" if freshness.get("requires_refresh") else None,
        "should_send_to_teams": False,
        "should_write_tms": False,
        "should_send_customer_message": False,
    }
