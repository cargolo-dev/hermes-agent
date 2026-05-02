from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import yaml
from hermes_constants import get_hermes_home
from tools.delegate_tool import delegate_task

from .models import (
    ASRAnalysisAction,
    ASRAnalysisBrief,
    ASRAnalysisConfidence,
    ASRAnalysisReplyGuidance,
    ASRAnalysisRisk,
    ASRSpecialistOutput,
    ProcessingResult,
    utc_now_iso,
)
from .storage import CaseStore
from .ops_notifications import send_manual_ops_notification

logger = logging.getLogger(__name__)

ANALYSIS_VERSION = "asr-subagent-v1"


COORDINATOR_SYSTEM_PROMPT = """You are the CARGOLO ASR Analysis Coordinator for an online freight forwarder.

Domain context:
- ASR means Air / Sea / Rail forwarding operations.
- You operate in freight-forwarding semantics, not generic customer support semantics.
- Think in terms of shipment status, milestone deviations, pre-carriage/main-carriage/on-carriage, ETA/ETD/ATD/ATA, handovers, customs relevance, document completeness, carrier dependencies, billing relevance, and operational urgency.
- Your outputs are for internal dispatch / operations colleagues in Germany.

Your job starts only after deterministic case processing has already completed.
Deterministic artifacts on disk are the canonical operational record.
You may synthesize judgment and recommendations, but you must not invent facts.

Rules:
- Internal ops only. Never write customer-facing mail.
- Do not create tasks in the TMS.
- Do not mutate canonical case files.
- Base every important conclusion on evidence from the provided specialist outputs and file paths.
- Prefer conservative, operationally useful advice over speculative certainty.
- Use freight-forwarding language, not generic helpdesk language.
- All summaries, findings, actions, and explanations must be in German.
- Return strict JSON only. No markdown fences.
"""


RECONCILIATION_PROMPT = """You are the Reconciliation Analyst for CARGOLO ASR.

Domain context:
- This is freight forwarding, especially Air / Sea / Rail operations.
- Evaluate the case like an experienced Speditionsmitarbeiter in international forwarding.
- Pay attention to shipment milestones, route legs, Incoterm implications if visible, customs relevance, document flow, carrier / terminal dependencies, billing indicators, time-critical escalations, and missing operational handoff information.

Mission:
- Reconstruct the operational truth of the case from the current artifacts.
- Compare newest inbound mail, current case state, and TMS snapshot.
- Detect contradictions, stale assumptions, missing evidence, and shipment-management risks.
- Focus on chronology, operational truth, and what must be verified before action.

Do not:
- Write customer wording.
- Suggest task creation.
- Invent facts not supported by files.
- Use generic support phrasing; stay in freight-forwarding semantics.

Read these files:
- {case_state_path}
- {entities_path}
- {tms_snapshot_path}
- {normalized_email_path}
- {timeline_path}
- {email_index_path}

Output language rules:
- All string values intended for humans must be in German.
- Keep evidence references and file paths unchanged.

Return strict JSON with exactly this shape:
{{
  "role": "reconciliation",
  "summary": "kurze interne Zusammenfassung auf Deutsch",
  "facts_confirmed": ["..."],
  "contradictions": [
    {{"issue": "...", "severity": "low|medium|high", "evidence": ["path:field", "..."]}}
  ],
  "open_questions": ["..."],
  "suggested_verifications": ["..."],
  "confidence": "low|medium|high",
  "files_used": ["absolute/path", "..."]
}}
"""


DRAFT_REVIEW_PROMPT = """You are the Draft Review Analyst for CARGOLO ASR.

Domain context:
- This is freight forwarding communication support for Air / Sea / Rail cases.
- Review drafts like an experienced Speditionsmitarbeiter who knows that imprecise wording about status, milestones, documents, customs, free time, delays, handover, or responsibility can create operational and commercial risk.

Mission:
- Review the deterministic internal draft and determine whether it is operationally safe and directionally correct.
- Produce internal reply guidance only.
- Flag unsupported claims, overpromises, missing asks, weak operational wording, and statements that could misrepresent shipment reality.

Do not:
- Send or finalize customer communication.
- Invent shipment facts.
- Use generic helpdesk phrasing; use forwarding-aware semantics.

Read these files:
- {draft_path}
- {case_state_path}
- {tms_snapshot_path}
- {normalized_email_path}
- {timeline_path}

Output language rules:
- All string values intended for humans must be in German.
- Keep evidence references and file paths unchanged.

Return strict JSON with exactly this shape:
{{
  "role": "draft_review",
  "summary": "kurze interne Zusammenfassung auf Deutsch",
  "draft_status": "ok|needs_revision|unsafe",
  "must_include": ["..."],
  "must_avoid": ["..."],
  "missing_for_reply": ["..."],
  "tone_guidance": "...",
  "revised_internal_reply_brief": "...",
  "confidence": "low|medium|high",
  "files_used": ["absolute/path", "..."]
}}
"""


OPS_ACTION_PROMPT = """You are the Ops Action Analyst for CARGOLO ASR.

Domain context:
- This is international freight forwarding for Air / Sea / Rail.
- Think like an experienced online-spedition operations lead.
- Prioritize actions based on shipment progress, service failure risk, customs/document exposure, missing milestones, unresolved ownership, carrier dependency, customer expectation management, and commercial/billing implications.

Mission:
- Convert the current case situation into a ranked internal action plan.
- Think like an experienced online-freight-forwarding operations lead.
- Optimize for the next best internal move, blockers, handoffs, and escalation readiness.

Do not:
- Create tasks.
- Suggest customer send actions as if already approved.
- Speculate beyond the evidence.
- Use generic support phrasing; stay in forwarding operations language.

Read these files:
- {case_state_path}
- {tms_snapshot_path}
- {normalized_email_path}
- {timeline_path}
- {task_log_path}

Output language rules:
- All string values intended for humans must be in German.
- Keep evidence references and file paths unchanged.

Return strict JSON with exactly this shape:
{{
  "role": "ops_action",
  "summary": "kurze interne Zusammenfassung auf Deutsch",
  "priority": "low|medium|high|urgent",
  "recommended_actions": [
    {{"action": "...", "urgency": "low|medium|high|urgent", "owner_role": "ops|customs|billing|management|warehouse", "reason": "...", "blocking": true}}
  ],
  "sla_risk": "none|low|medium|high",
  "handoff_needed": true,
  "watch_items": ["..."],
  "confidence": "low|medium|high",
  "files_used": ["absolute/path", "..."]
}}
"""


COORDINATOR_SYNTHESIS_PROMPT = """Synthesize the specialist outputs into one final CARGOLO ASR ops brief.

Domain context:
- This is an internal analysis for an online freight forwarder in Air / Sea / Rail.
- The final brief must read like a high-quality internal Speditionsbewertung, not generic ticket triage.
- Prioritize shipment truth, milestone deviations, operational risk, document/customs exposure, escalation need, and next ownership.
- All human-readable output must be in German.

Deterministic case facts:
- order_id: {order_id}
- deterministic_status: {status}
- case_initialized: {initialized}
- classification: {classification}
- case_root: {case_root}
- timeline_entry: {timeline_entry}
- history_sync_count: {history_sync_count}

Specialist outputs:
{specialist_outputs_json}

Return strict JSON only with exactly this shape:
{{
  "analysis_version": "asr-subagent-v1",
  "order_id": "{order_id}",
  "deterministic_status": "{status}",
  "case_initialized": true,
  "message_classification": "{classification}",
  "priority": "low|medium|high|urgent",
  "ops_summary": "interne Zusammenfassung auf Deutsch",
  "customer_reply_guidance": {{
    "reply_recommended": true,
    "draft_status": "ok|needs_revision|unsafe",
    "must_include": ["..."],
    "must_avoid": ["..."],
    "missing_for_reply": ["..."],
    "tone_guidance": "...",
    "revised_internal_reply_brief": "..."
  }},
  "internal_actions": [
    {{"action": "...", "urgency": "low|medium|high|urgent", "owner_role": "ops|customs|billing|management|warehouse", "reason": "...", "blocking": true}}
  ],
  "risk_flags": [
    {{"code": "...", "severity": "low|medium|high", "reason": "...", "evidence": ["..."]}}
  ],
  "open_questions": ["..."],
  "confidence": {{"overall": "low|medium|high", "why": ["..."]}},
  "provenance": {{
    "case_root": "{case_root}",
    "files_used": ["absolute/path", "..."],
    "specialists_used": ["reconciliation", "draft_review", "ops_action"]
  }}
}}

Rules:
- Deterministic artifacts override specialist speculation.
- If specialists disagree, choose the more conservative operational interpretation.
- Separate facts, risks, actions, and reply guidance.
- Use evidence-driven risk codes in SHOUTY_SNAKE_CASE where possible.
- All human-readable strings must be in German.
- Use forwarding-aware language: shipment, Sendung, Laufweg, Status, ETA/ETD, Dokumente, Verzögerung, Verzollung, Übergabe, Billing, Eskalation.
"""


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _load_runtime_config() -> dict[str, Any]:
    cfg_path = get_hermes_home() / "config.yaml"
    if not cfg_path.exists():
        return {}
    try:
        data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.debug("Could not read Hermes config for ASR subagent analysis", exc_info=True)
        return {}


def _analysis_runtime_supported() -> bool:
    cfg = _load_runtime_config()
    delegation = cfg.get("delegation", {}) if isinstance(cfg.get("delegation"), dict) else {}
    delegation_provider = str(delegation.get("provider", "") or "").strip().lower()
    if delegation_provider:
        try:
            from hermes_cli.auth import get_external_process_provider_status
            from hermes_cli.runtime_provider import resolve_runtime_provider

            runtime = resolve_runtime_provider(requested=delegation_provider)
            if delegation_provider in {"copilot-acp", "codex-cli"}:
                status = get_external_process_provider_status(delegation_provider)
                return bool(status.get("configured") and status.get("logged_in"))
            return bool(runtime.get("base_url") and runtime.get("api_mode"))
        except Exception:
            logger.debug("Delegation runtime is not currently resolvable", exc_info=True)
            return False
    if any(str(delegation.get(k, "")).strip() for k in ("model", "base_url", "api_key")):
        return True

    model_cfg = cfg.get("model", {}) if isinstance(cfg.get("model"), dict) else {}
    provider = str(model_cfg.get("provider", "") or "").strip().lower()
    base_url = str(model_cfg.get("base_url", "") or "").strip().lower()

    # The ChatGPT Codex backend is interactive/browser-oriented and is not a
    # reliable headless provider for background subagent orchestration.
    if provider == "openai-codex" and "chatgpt.com/backend-api/codex" in base_url:
        return False

    return True


def _analysis_enabled() -> bool:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False
    if not _env_flag("HERMES_CARGOLO_ASR_SUBAGENT_ANALYSIS", True):
        return False
    return _analysis_runtime_supported()


def _extract_json_blob(text: str) -> dict[str, Any]:
    payload = (text or "").strip()
    if not payload:
        raise ValueError("empty analysis response")
    try:
        return json.loads(payload)
    except Exception:
        pass
    start = payload.find("{")
    end = payload.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("no JSON object found in analysis response")
    return json.loads(payload[start:end + 1])


def _build_parent_agent():
    from run_agent import AIAgent

    agent_kwargs = _analysis_agent_runtime_kwargs()
    return AIAgent(
        enabled_toolsets=["file", "delegation"],
        quiet_mode=True,
        skip_context_files=True,
        skip_memory=True,
        platform="telegram",
        ephemeral_system_prompt=COORDINATOR_SYSTEM_PROMPT,
        persist_session=False,
        **agent_kwargs,
    )


def _build_synthesis_agent():
    from run_agent import AIAgent

    agent_kwargs = _analysis_agent_runtime_kwargs()
    return AIAgent(
        enabled_toolsets=[],
        quiet_mode=True,
        skip_context_files=True,
        skip_memory=True,
        platform="telegram",
        ephemeral_system_prompt=COORDINATOR_SYSTEM_PROMPT,
        persist_session=False,
        **agent_kwargs,
    )


def _analysis_agent_runtime_kwargs() -> dict[str, Any]:
    cfg = _load_runtime_config()
    delegation = cfg.get("delegation", {}) if isinstance(cfg.get("delegation"), dict) else {}
    requested_provider = str(delegation.get("provider", "") or "").strip() or None
    requested_model = str(delegation.get("model", "") or "").strip()
    explicit_api_key = str(delegation.get("api_key", "") or "").strip() or None
    explicit_base_url = str(delegation.get("base_url", "") or "").strip() or None

    kwargs: dict[str, Any] = {}
    if requested_provider:
        try:
            from hermes_cli.runtime_provider import resolve_runtime_provider

            runtime = resolve_runtime_provider(
                requested=requested_provider,
                explicit_api_key=explicit_api_key,
                explicit_base_url=explicit_base_url,
            )
            if runtime.get("provider"):
                kwargs["provider"] = runtime["provider"]
            if runtime.get("base_url"):
                kwargs["base_url"] = runtime["base_url"]
            if runtime.get("api_key"):
                kwargs["api_key"] = runtime["api_key"]
            if runtime.get("api_mode"):
                kwargs["api_mode"] = runtime["api_mode"]
            if runtime.get("command"):
                kwargs["command"] = runtime["command"]
            if runtime.get("args"):
                kwargs["args"] = list(runtime.get("args") or [])
        except Exception:
            logger.debug("Could not resolve delegation runtime for ASR analysis agent", exc_info=True)

    if requested_model:
        kwargs["model"] = requested_model
    return kwargs


def _specialist_tasks(*, case_root: Path, case_state_path: Path, entities_path: Path, tms_snapshot_path: Path, normalized_email_path: Path, draft_path: Path, timeline_path: Path, email_index_path: Path, task_log_path: Path) -> list[dict[str, Any]]:
    return [
        {
            "goal": "Produce the reconciliation analysis JSON for this ASR case.",
            "context": RECONCILIATION_PROMPT.format(
                case_state_path=str(case_state_path),
                entities_path=str(entities_path),
                tms_snapshot_path=str(tms_snapshot_path),
                normalized_email_path=str(normalized_email_path),
                timeline_path=str(timeline_path),
                email_index_path=str(email_index_path),
            ),
            "toolsets": ["file"],
        },
        {
            "goal": "Produce the draft review analysis JSON for this ASR case.",
            "context": DRAFT_REVIEW_PROMPT.format(
                draft_path=str(draft_path),
                case_state_path=str(case_state_path),
                tms_snapshot_path=str(tms_snapshot_path),
                normalized_email_path=str(normalized_email_path),
                timeline_path=str(timeline_path),
            ),
            "toolsets": ["file"],
        },
        {
            "goal": "Produce the ops action analysis JSON for this ASR case.",
            "context": OPS_ACTION_PROMPT.format(
                case_state_path=str(case_state_path),
                tms_snapshot_path=str(tms_snapshot_path),
                normalized_email_path=str(normalized_email_path),
                timeline_path=str(timeline_path),
                task_log_path=str(task_log_path),
            ),
            "toolsets": ["file"],
        },
    ]


def _known_specialist_fallback_paths(case_root: Path | None) -> list[Path]:
    if case_root is None:
        return []
    analysis_root = case_root / "analysis"
    return [
        case_root / "reconciliation.json",
        analysis_root / "reconciliation.json",
        case_root / "draft_review.json",
        analysis_root / "draft_review.json",
        case_root / "ops_action_analysis.json",
        analysis_root / "ops_action_analysis.json",
        case_root / "ops_action.json",
        analysis_root / "ops_action.json",
    ]


def _load_specialist_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.debug("Could not parse specialist fallback file %s", path, exc_info=True)
        return None
    if not isinstance(payload, dict):
        logger.debug("Ignoring specialist fallback file %s because it is not a JSON object", path)
        return None
    return payload


def _append_specialist_payload(parsed: list[ASRSpecialistOutput], payload: dict[str, Any], *, source: str) -> None:
    try:
        specialist = ASRSpecialistOutput.model_validate(payload)
    except Exception:
        logger.debug("Ignoring invalid specialist output from %s", source, exc_info=True)
        return
    existing_roles = {str(item.role or "").strip() for item in parsed}
    if specialist.role and specialist.role in existing_roles:
        return
    parsed.append(specialist)


def _parse_specialist_outputs(raw_results: list[dict[str, Any]], *, case_root: Path | None = None) -> list[ASRSpecialistOutput]:
    parsed: list[ASRSpecialistOutput] = []
    parse_failures = 0
    for idx, entry in enumerate(raw_results):
        summary = entry.get("summary") or "" if isinstance(entry, dict) else ""
        try:
            payload = _extract_json_blob(summary)
        except Exception:
            parse_failures += 1
            logger.debug("Specialist result %s did not contain inline JSON; will try case-local fallback files", idx, exc_info=True)
            continue
        _append_specialist_payload(parsed, payload, source=f"delegate_result[{idx}]")

    for candidate in _known_specialist_fallback_paths(case_root):
        payload = _load_specialist_payload(candidate)
        if payload is not None:
            _append_specialist_payload(parsed, payload, source=str(candidate))

    if parse_failures and parsed:
        logger.warning(
            "Recovered %s ASR specialist output(s) from fallback files/other results after %s non-JSON delegate result(s)",
            len(parsed),
            parse_failures,
        )
    elif parse_failures and not parsed:
        logger.warning("No parseable ASR specialist outputs found after %s non-JSON delegate result(s); using deterministic fallback brief", parse_failures)
    return parsed


def _synthesize_brief(result: ProcessingResult, specialists: list[ASRSpecialistOutput]) -> ASRAnalysisBrief:
    if not specialists:
        return _fallback_synthesize_brief(result, specialists)
    agent = _build_synthesis_agent()
    prompt = COORDINATOR_SYNTHESIS_PROMPT.format(
        order_id=result.order_id or "",
        status=result.status,
        initialized=str(bool(result.initialized)).lower(),
        classification=result.classification.value if result.classification else "unknown",
        case_root=result.case_root or "",
        timeline_entry=result.timeline_entry or "",
        history_sync_count=result.history_sync_count,
        specialist_outputs_json=json.dumps([s.model_dump(mode="json") for s in specialists], ensure_ascii=False, indent=2),
    )
    response = agent.run_conversation(prompt)
    final_response = response.get("final_response") if isinstance(response, dict) else response
    try:
        payload = _extract_json_blob(final_response or "")
        return ASRAnalysisBrief.model_validate(payload)
    except Exception:
        logger.warning("ASR synthesis returned non-JSON output for %s; using deterministic fallback synthesis", result.order_id)
        return _fallback_synthesize_brief(result, specialists)



def _fallback_synthesize_brief(result: ProcessingResult, specialists: list[ASRSpecialistOutput]) -> ASRAnalysisBrief:
    specialist_by_role = {str(s.role or "").strip(): s.model_dump(mode="json") for s in specialists}
    reconciliation = specialist_by_role.get("reconciliation", {})
    draft_review = specialist_by_role.get("draft_review", {})
    ops_action = specialist_by_role.get("ops_action", {})

    priority = str(ops_action.get("priority") or "medium").strip().lower() or "medium"
    ops_summary = str(ops_action.get("summary") or reconciliation.get("summary") or draft_review.get("summary") or result.timeline_entry or "").strip()

    internal_actions = []
    for row in ops_action.get("recommended_actions") or []:
        if not isinstance(row, dict):
            continue
        action = str(row.get("action") or "").strip()
        if not action:
            continue
        internal_actions.append(
            ASRAnalysisAction(
                action=action,
                urgency=str(row.get("urgency") or priority or "medium").strip() or "medium",
                owner_role=str(row.get("owner_role") or "ops").strip() or "ops",
                reason=str(row.get("reason") or reconciliation.get("summary") or ops_summary or "Interne operative Bewertung erforderlich").strip(),
                blocking=bool(row.get("blocking")),
            )
        )

    risk_flags: list[ASRAnalysisRisk] = []
    for row in reconciliation.get("contradictions") or []:
        if not isinstance(row, dict):
            continue
        issue = str(row.get("issue") or "").strip()
        if not issue:
            continue
        severity = str(row.get("severity") or "medium").strip().lower() or "medium"
        risk_flags.append(
            ASRAnalysisRisk(
                code=f"RECON_{severity.upper()}",
                severity=severity,
                reason=issue,
                evidence=[str(item) for item in (row.get("evidence") or []) if str(item).strip()],
            )
        )

    if not risk_flags and str(ops_action.get("sla_risk") or "").strip().lower() in {"medium", "high"}:
        sla_risk = str(ops_action.get("sla_risk") or "").strip().lower()
        risk_flags.append(
            ASRAnalysisRisk(
                code=f"SLA_RISK_{sla_risk.upper()}",
                severity=sla_risk,
                reason=str(ops_action.get("summary") or ops_summary or "Operatives SLA-/Terminrisiko sichtbar").strip(),
                evidence=[str(item) for item in (ops_action.get("watch_items") or [])[:4] if str(item).strip()],
            )
        )

    reply_guidance = ASRAnalysisReplyGuidance(
        reply_recommended=bool(draft_review.get("draft_status") == "ok"),
        draft_status=str(draft_review.get("draft_status") or "needs_revision").strip() or "needs_revision",
        must_include=[str(item) for item in (draft_review.get("must_include") or []) if str(item).strip()],
        must_avoid=[str(item) for item in (draft_review.get("must_avoid") or []) if str(item).strip()],
        missing_for_reply=[str(item) for item in (draft_review.get("missing_for_reply") or []) if str(item).strip()],
        tone_guidance=str(draft_review.get("tone_guidance") or "").strip(),
        revised_internal_reply_brief=str(draft_review.get("revised_internal_reply_brief") or "").strip(),
    )

    open_questions = []
    for seq in (
        reconciliation.get("open_questions") or [],
        draft_review.get("missing_for_reply") or [],
        ops_action.get("watch_items") or [],
    ):
        for item in seq:
            text = str(item or "").strip()
            if text and text not in open_questions:
                open_questions.append(text)

    files_used = []
    for spec in specialists:
        for path in spec.files_used:
            text = str(path or "").strip()
            if text and text not in files_used:
                files_used.append(text)

    confidence_items = []
    for spec in specialists:
        role = str(spec.role or "").strip() or "specialist"
        conf = str(spec.confidence or "medium").strip()
        confidence_items.append(f"{role}: {conf}")

    return ASRAnalysisBrief(
        analysis_version=ANALYSIS_VERSION,
        order_id=result.order_id or "",
        deterministic_status=result.status,
        case_initialized=bool(result.initialized),
        message_classification=result.classification.value if result.classification else "unknown",
        priority=priority,
        ops_summary=ops_summary,
        customer_reply_guidance=reply_guidance,
        internal_actions=internal_actions,
        risk_flags=risk_flags,
        open_questions=open_questions,
        confidence=ASRAnalysisConfidence(
            overall="medium" if specialists else "low",
            why=confidence_items or ["Fallback-Synthese aus Spezialisten-Ausgaben verwendet"],
        ),
        provenance={
            "case_root": result.case_root or "",
            "files_used": files_used,
            "specialists_used": [str(s.role or "").strip() for s in specialists if str(s.role or "").strip()],
            "synthesis_mode": "fallback_from_specialists",
        },
    )


def run_postprocess_subagent_analysis(
    *,
    store: CaseStore,
    result: ProcessingResult,
    normalized_path: Path,
    draft_path: Path,
    state_path: Path,
    entities_path: Path,
    tms_path: Path,
    timeline_path: Path,
    email_index_path: Path,
    task_log_path: Path,
) -> tuple[str, str | None, str | None, str | None]:
    """Run the Hermes-native specialist subagent analysis layer.

    Returns (analysis_status, analysis_brief_path, analysis_priority, analysis_summary).
    """
    if not _analysis_enabled():
        return ("disabled", None, None, None)
    if result.status != "processed" or result.duplicate or result.review_required or not result.order_id or not result.case_root:
        return ("skipped", None, None, None)

    case_root = Path(result.case_root)
    try:
        parent = _build_parent_agent()
        raw = json.loads(delegate_task(
            tasks=_specialist_tasks(
                case_root=case_root,
                case_state_path=state_path,
                entities_path=entities_path,
                tms_snapshot_path=tms_path,
                normalized_email_path=normalized_path,
                draft_path=draft_path,
                timeline_path=timeline_path,
                email_index_path=email_index_path,
                task_log_path=task_log_path,
            ),
            parent_agent=parent,
            max_iterations=35,
        ))
        raw_results = raw.get("results", []) if isinstance(raw, dict) else []
        specialists = _parse_specialist_outputs(raw_results, case_root=case_root)
        brief = _synthesize_brief(result, specialists)
        brief_path = store.save_analysis_brief(
            result.order_id,
            brief.model_dump(mode="json"),
            message_hint=Path(normalized_path).stem,
        )
        store.save_analysis_raw(
            result.order_id,
            {
                "analysis_version": ANALYSIS_VERSION,
                "generated_at": utc_now_iso(),
                "specialist_results": [s.model_dump(mode="json") for s in specialists],
                "delegate_task_raw": raw,
            },
            name="subagent_delegate_results",
            message_hint=Path(normalized_path).stem,
        )
        store.append_audit(
            result.order_id,
            action="subagent_analysis",
            result="ok",
            files=[str(brief_path)],
            extra={
                "analysis_version": ANALYSIS_VERSION,
                "specialists_used": [s.role for s in specialists],
                "priority": brief.priority,
            },
        )
        return ("completed", str(brief_path), brief.priority, brief.ops_summary)
    except Exception as exc:
        logger.exception("ASR subagent analysis failed for %s", result.order_id)
        try:
            raw_path = store.save_analysis_raw(
                result.order_id,
                {
                    "analysis_version": ANALYSIS_VERSION,
                    "generated_at": utc_now_iso(),
                    "status": "error",
                    "error": str(exc),
                },
                name="subagent_analysis_error",
                message_hint=Path(normalized_path).stem,
            )
            store.append_audit(
                result.order_id,
                action="subagent_analysis",
                result="error",
                files=[str(raw_path)],
                extra={"error": str(exc), "analysis_version": ANALYSIS_VERSION},
            )
        except Exception:
            logger.exception("Failed to persist ASR subagent analysis error for %s", result.order_id)
        return ("error", None, None, None)
