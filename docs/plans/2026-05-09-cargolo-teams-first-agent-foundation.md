# CARGOLO Teams-First Agent Foundation Plan

> **For Hermes:** Use `subagent-driven-development` and Codex CLI where possible to implement this plan task-by-task after user approval. Until then this document is a planning/foundation contract, not an implementation mandate.

**Goal:** Establish the architectural prerequisites so CARGOLO Hermes becomes the first-line Microsoft Teams ops colleague that orchestrates specialist agents/subagents/crons/tools and returns one verified operational answer to Teams.

**Architecture:** Teams and cron/webhook events are normalized into a CARGOLO Coordinator event model. The Coordinator is the only final operational speaker to Teams; specialist workers produce structured findings, evidence, risks, and recommended actions. TMS writes and customer-facing messages remain approval-gated.

**Tech Stack:** Hermes Agent gateway, bundled Teams platform plugin, CARGOLO ops plugin, webhook/cron, Codex-first Hermes `delegate_task` via inherited `openai-codex` runtime for normal subagents, Codex CLI subprocess workers via `codex exec` for coding/refactor/test tasks, local CARGOLO case folders under `/root/.hermes/cargolo_asr`.

---

## Non-negotiable operating principles

1. **Teams is the CARGOLO ops frontdoor.** Operators should ask Hermes in Teams first, not choose tools/agents manually.
2. **One Hermes voice.** Subagents/crons do not send final operational Teams messages directly. They return structured results to the Coordinator; Coordinator writes Teams.
3. **Event-driven core.** Teams messages, replies, buttons, cron detections, and webhooks are normalized into one event schema before handling.
4. **Agentic internal work.** Coordinator selects and runs specialist workers for TMS, documents, mail history, pricing, billing, risk/priority, and response formatting.
5. **Evidence before assertion.** Teams responses must distinguish verified facts, likely findings, missing data, and next action.
6. **Human approval for writes.** TMS writes, customer mails, and commercial commitments require explicit approved flows/cards.
7. **ASR separation.** Air/Sea/Rail stays separate from Land/Road unless explicitly requested.
8. **Telegram is temporary admin/control surface.** While user only has Telegram access, planning, readiness, and controlled technical checks happen via Telegram; Teams UX is prepared and later tested in Teams.

---

## Current readiness facts captured on 2026-05-09

- Hermes version: `v0.13.0 (2026.5.7)`, up to date.
- Current repo: `/root/.hermes/hermes-agent`, branch `main`, HEAD `f74c6c8c7`.
- Teams platform is enabled in config with local port `3978`.
- Webhook platform is enabled on port `8644`.
- Codex CLI is installed at `/root/.hermes/node/bin/codex`, version `0.121.0`.
- 2026-05-09 readiness re-check after re-login: `codex login status` reports ChatGPT login and a real `codex exec --skip-git-repo-check --sandbox read-only --output-last-message ... --ephemeral -` smoke test returned `CODEX_SMOKE_OK`. Treat Codex CLI as **execution-ready for subprocess-style coding workers**.
- Current Codex CLI does not expose ACP mode in this environment, so the practical pattern is subprocess workers via `codex exec`, not ACP-backed Hermes delegation.
- 2026-05-09 foundation config fix: compression summary routing is now explicit (`compression.summary_provider=openrouter`, `compression.summary_model=google/gemini-3-flash-preview`, `auxiliary.compression.provider=openrouter`) to avoid the ChatGPT Codex unsupported-model warning.
- 2026-05-09 Codex-first delegation update: Hermes `delegate_task` routing is now intentionally blank (`delegation.provider=""`, `delegation.model=""`, `delegation.reasoning_effort=""`) so child agents inherit the main `openai-codex` / `gpt-5.5` runtime. Fresh-process smoke test returned `FRESH_CODEX_INHERIT_OK` with reported child model `gpt-5.5`.
- OpenRouter/Sonnet is no longer the default delegation route; keep it as an explicit fallback/review option if a task needs second-model review or Codex load isolation.

---

## Target runtime flow

```text
Teams / Cron / Webhook Event
  -> CARGOLO Event Normalizer
  -> CARGOLO Coordinator
  -> Specialist workers/subagents/tools
      - TMS Controller
      - Document Analyst
      - Mail History Analyst
      - Pricing Analyst
      - Billing/Invoice Analyst
      - Risk/Priority Analyst
      - Response Formatter
  -> Case Folder Evidence + Audit
  -> Coordinator Safety/Quality Gate
  -> Teams answer/card
```

---

## Acceptance boundary for this foundation tranche

We stop this tranche when these prerequisites are true, even if specific business features are not built yet:

1. **Architecture contract exists** — this file plus a concise runtime design doc are committed.
2. **Codex readiness is real** — `codex login status` and `codex exec` smoke test pass.
3. **Delegation strategy is explicit** — Hermes-native `delegate_task` inherits the main Codex runtime by default; Codex CLI is used as subprocess worker for code tasks; OpenRouter/Sonnet remains optional fallback/review route.
4. **Teams-first event model is specified** — event schema for Teams messages/replies/buttons and cron events.
5. **Specialist result schema is specified** — common JSON result shape for all workers.
6. **Coordinator ownership is specified** — no direct final Teams sends by workers for operational content.
7. **Safety gates are specified** — TMS/customer/commercial writes require approval.
8. **Test plan is specified** — Telegram-admin readiness checks now; Teams live tests later.

---

## Foundation Tasks for later implementation

### Task 1: Codex CLI readiness and provider decision

**Objective:** Make native Codex worker execution reliable before using it for CARGOLO implementation.

**Checks:**
- Run `codex login status`.
- Run real smoke test:
  ```bash
  TMP=$(mktemp)
  printf 'Reply with exactly: CODEX_SMOKE_OK\n' | codex exec --skip-git-repo-check --sandbox read-only --output-last-message "$TMP" --ephemeral -
  cat "$TMP"
  ```
- Expected: `CODEX_SMOKE_OK`.

**If failed:** user must re-authenticate Codex CLI from an environment with browser/login access:
```bash
codex logout
codex login
```
Then rerun smoke test.

**Decision:**
- Hermes-native subagents: default to inherited main Codex runtime by leaving `delegation.provider`, `delegation.model`, and `delegation.reasoning_effort` blank.
- Coding/refactor/test workers: use Codex CLI subprocesses via `codex exec`.
- Fallback/review: use OpenRouter/Sonnet explicitly only when second-model review or load isolation is desired.

---

### Task 2: CARGOLO Coordinator event schema

**Objective:** Define one normalized input envelope for Teams, cron, webhook, and manual events.

**Create:** `plugins/cargolo_ops/coordinator_events.py` or design doc first.

**Minimum schema:**
```json
{
  "event_id": "string",
  "event_type": "teams_message | teams_reply | teams_button | cron_document_upload | cron_health | webhook_ingest | manual_check",
  "source": "teams | cron | webhook | telegram_admin | internal",
  "order_id": "AN-... | BU-... | null",
  "text": "string",
  "received_at": "ISO-8601",
  "teams": {
    "conversation_id": "string|null",
    "message_id": "string|null",
    "reply_to_id": "string|null",
    "card_id": "string|null",
    "from_user_id_present": true
  },
  "context_refs": [],
  "raw_ref": "path or id, no secrets"
}
```

---

### Task 3: Specialist result schema

**Objective:** Make all workers/subagents return mergeable structured results.

**Minimum schema:**
```json
{
  "agent": "tms_controller | document_analyst | mail_history | pricing | billing | risk_priority | response_formatter",
  "status": "ok | needs_review | failed | skipped",
  "confidence": 0.0,
  "summary": "short German ops summary",
  "findings": [],
  "risks": [],
  "recommended_actions": [],
  "evidence_refs": [],
  "requires_human": false,
  "write_intents": []
}
```

---

### Task 4: Coordinator policy and routing matrix

**Objective:** Specify which workers run for each operator intent or event type.

**Examples:**
- `status` -> health/runtime + pending queue; no specialist deep dive.
- `offene Freigaben` -> pending TMS review worker only.
- `prüfe AN komplett` -> TMS + documents + mail + pending actions + risk; pricing/billing only if evidence indicates commercial context.
- `neuer Dokumenten-Upload` -> documents + TMS compare + mail context + risk.
- `Kunde fragt nach Status` -> TMS + mail history + docs + customer response draft, no auto-send.
- TMS-looking free text -> create pending review proposal, never direct write.

---

### Task 5: Teams response quality gate

**Objective:** Before Coordinator posts to Teams, enforce a compact, safe response contract.

**Gate checklist:**
- AN/BU clear or exactly one question asked.
- Verified facts separated from assumptions.
- Missing data separated from not-yet-due.
- No raw audit dumps.
- No secrets/IDs exposed.
- No direct TMS/customer mail write from free text.
- German ops style: `Gemacht`, `Lage`, `Auffällig`, `Nächster Schritt` where useful.

---

### Task 6: Cron-to-Coordinator contract

**Objective:** Convert future crons into event producers instead of direct final Teams speakers.

**Rules:**
- Cron may detect and package event.
- Cron may write raw detection evidence.
- Cron should call Coordinator or enqueue event.
- Coordinator decides whether/how to notify Teams.

---

### Task 7: Teams maximum-potential backlog

**Objective:** Prepare features that use Teams well instead of treating it as plain chat.

**Backlog:**
- Adaptive Cards with visible status update after click.
- Context marker per card for robust replies.
- Buttons: approve/reject/correct/reanalyse/fall prüfen.
- Thread-aware reply mapping.
- Compact HTML/multiline cards dark-mode safe.
- Channel vs DM behavior: channel only on real mention, DM direct.
- `/sethome`/route-specific test surface handling.
- Delivery deduplication/idempotency for cron notifications.

---

## Telegram-only working mode until Teams testing

Since the user currently only has Telegram access:

1. Use Telegram for decisions, readiness reports, and approving plans.
2. Do local checks from Hermes tools.
3. Do not require interactive Teams validation in this tranche.
4. Do not send live Teams test messages while Teams access is unavailable.
5. Prepare Teams test scripts and expected messages for later.
6. If Codex re-login is needed, report exact command and wait until user can perform it.

---

## Implementation progress

### 2026-05-09 — Foundation Tranche 1 started

Implemented conservative coordinator contracts with no Teams/TMS/customer side effects:

- Created `plugins/cargolo_ops/coordinator_events.py` with normalized `CargoloOpsEvent`, event/source enums, Teams message normalizer, and cron document-upload normalizer.
- Created `plugins/cargolo_ops/specialist_results.py` with common `SpecialistResult` schema for future Hermes/Codex/Python specialists.
- Created `plugins/cargolo_ops/coordinator.py` with `handle_event(event, root=...)`, `CoordinatorResult`, deterministic initial routing, and JSONL audit append.
- Added `tests/cargolo_ops/test_coordinator_contracts.py` covering event normalization, specialist result serialization, coordinator audit, cron internal recording, malformed order-id path safety, and no Teams delivery behavior.
- Extended coordinator routing matrix with `CoordinatorIntent`: `status_request`, `case_deep_dive`, `document_upload`, `tms_write_intent`, `unknown`.
- Added optional local pending-task queue (`pending_tasks.jsonl`) behind `enqueue_tasks=True`; rows are forced read-only, pending, linked to `event_id`/`order_id`/`intent`, and `should_send_to_teams=False`.
- Guardrail: TMS write-like free text returns `ASK_HUMAN`, queues nothing, and still does not send live Teams messages. Read-only TMS status requests stay `STATUS_REQUEST` and route to `case_context` + `tms_snapshot`.
- Unknown Teams messages prepare exactly one clarifying question but keep `should_send_to_teams=False` until later Teams integration.
- Verification: `./venv-py312/bin/python -m pytest tests/cargolo_ops/test_coordinator_contracts.py tests/cargolo_ops/test_teams_ops_router.py tests/cargolo_ops/test_teams_reply_loop.py -q -o 'addopts='` returned `38 passed`.
- Safety check: AST scan of the new foundation files found no Teams/network send calls, TMS/writeback imports, or ops notification imports.
- Codex-first `delegate_task` review returned PASS after two fix cycles: malformed order-id path safety, TMS read-only routing, and forced read-only queue serialization.

### 2026-05-09 — Agent-first Employee Runtime added

Course correction: CARGOLO foundation must not turn Hermes into a rigid workflow bot. Normal Hermes chat/drafting remains available; CARGOLO adds employee context, dynamic specialist planning, and guarded operational boundaries.

- Created `plugins/cargolo_ops/employee_agent.py` with `EmployeeRequest`, `EmployeeResponse`, `ResponseMode`, `BoundaryAction`, `ContextNeed`, `SpecialistPlan`, and `handle_employee_request(...)`.
- Added `tests/cargolo_ops/test_employee_agent.py` covering normal free chat, customer draft-only behavior, case assist context planning, Teams read vs send distinction, TMS write guardrails, and audit serialization.
- Agent-first invariant: responses keep `can_answer_normally=True`; the runtime marks boundaries instead of suppressing normal Hermes replies.
- Free-chat invariant: non-CARGOLO/no-AN questions stay `ResponseMode.FREE_CHAT` with no context plan and no guard.
- Case-assist invariant: AN/BU-like prompts become `ResponseMode.CASE_ASSIST` with dynamic read-only context needs (`case_folder`, `mail_history`, `tms_snapshot`, `documents`, `pricing_kb`, `billing_context`, `teams_thread`) and read-only specialist plans.
- Draft invariant: customer-facing wording (`schreib`, `sende`, `antworte`, `mail` + customer context) becomes `DRAFT_ONLY`; no customer message is sent.
- Guard invariant: Teams send/post requests and TMS field writes become `GUARDED_ACTION_REQUIRED`; no Teams send, no TMS write, no customer send.
- Review hardening fixed three important edge classes: customer send verbs falling through to free chat; Teams-thread read requests over-guarding as sends; TMS field writes without literal `TMS`; plus two later review catches around customer drafts mentioning MRN and informational `Update zur MRN` remaining read-only case assist.
- Verification: `./venv-py312/bin/python -m pytest tests/cargolo_ops/test_employee_agent.py tests/cargolo_ops/test_coordinator_contracts.py tests/cargolo_ops/test_teams_ops_router.py tests/cargolo_ops/test_teams_reply_loop.py -q -o 'addopts='` returned `50 passed`.
- Safety check: AST scan of foundation files found no Teams/network send calls, TMS/writeback imports, customer-send calls, or ops notification imports.
- Codex-first `delegate_task` review returned PASS after three fix cycles.

### 2026-05-09 — Local Employee Runtime + Honcho memory contract added

Implemented the first complete local loop from employee brain to read-only specialist results. This is still local/dry-run only: no Teams, no TMS, no customer sends.

- Created `plugins/cargolo_ops/employee_runtime.py` with `run_employee_runtime(...)` and `EmployeeRuntimeResult`.
- Created `plugins/cargolo_ops/honcho_memory.py` with `HonchoMemorySnapshot` and `unavailable_honcho_snapshot(...)`.
- Added `tests/cargolo_ops/test_employee_runtime.py` covering free chat, case assist execution, guarded TMS writes, guarded Teams sends, customer draft-only behavior, optional Honcho memory, and Honcho-degraded operation.
- Runtime loop: `EmployeeRequest -> handle_employee_request -> read-only SpecialistPlan execution -> SpecialistResult list -> orders/<AN>/employee/specialist_results.jsonl -> draft_response`.
- Free-chat invariant: normal non-case Hermes chat executes no specialists and creates no case files.
- Case-assist invariant: read-only local specialist stubs run only for `CASE_ASSIST`; initial stubs cover `case_context`, `mail_history`, `tms_snapshot`, `document_analyst`, plus safe generic stubs for future agents.
- Guard invariant: `GUARDED_ACTION_REQUIRED` for TMS/Teams executes no specialists, writes no result file, and keeps all side-effect flags false.
- Honcho strategy: Honcho is contextual memory only, never an authority for actions. It is optional/non-blocking; if unavailable (`Honcho session could not be initialized` observed in this session), runtime continues safely and records degraded memory state in audit payloads.
- Current Honcho stage now includes a controller-side builder contract: `build_honcho_memory_snapshot(request, source=...)` calls injected `profile/search/context`, includes request text in search query, bounds facts/excerpts, and returns a safe `HonchoMemorySnapshot`.
- Honcho builder hardening: supports native Honcho `{"result": ...}` payloads, treats `{"error": ...}` / `success=False` payloads as unavailable, and forbids extra fields on `HonchoMemorySnapshot` so action-authority flags cannot leak into memory.
- Honcho readiness checked live: Hermes memory provider is `honcho`, `hermes honcho status` is OK, workspace is `cargolo-asr`, peers are `cargolo-ops`/`cargolo-asr`, local Honcho Docker services are up (`honcho-api`, `honcho-deriver`, `honcho-redis`, `honcho-database`), and `http://localhost:8000/health` returns `{"status":"ok"}`.
- Important nuance: direct Honcho tool calls from this running Telegram/API session still returned `Honcho session could not be initialized` even while CLI status is healthy. Treat this as a session/tool initialization gap, not a server outage. Do not make CARGOLO runtime depend on direct Honcho tools until a fresh gateway/session smoke confirms them.
- Decision: keep Honcho as useful implicit Hermes memory plus optional explicit snapshot input. Do not build it into the critical path. Next stage should either (a) rely on normal Hermes/Honcho auto-context, or (b) add a thin controller adapter around actual Hermes `honcho_profile`, `honcho_search`, and `honcho_context` tools only behind degraded fallback and after tool-session readiness is verified.
- Verification: `./venv-py312/bin/python -m pytest tests/cargolo_ops/test_honcho_memory.py tests/cargolo_ops/test_employee_runtime.py tests/cargolo_ops/test_employee_agent.py tests/cargolo_ops/test_coordinator_contracts.py tests/cargolo_ops/test_teams_ops_router.py tests/cargolo_ops/test_teams_reply_loop.py -q -o 'addopts='` returned `64 passed`.
- Safety check: AST scan of foundation/runtime files found no Teams/network send calls, TMS/writeback imports, customer-send calls, or ops notification imports.
- Codex-first `delegate_task` review returned PASS after fixing native Honcho payload support, error-payload handling, and extra-field forbidding on snapshots. Non-blocking notes: unknown future specialists currently return safe generic stubs; Honcho is recorded/audited but not yet used for planning beyond injected context.

### 2026-05-09 — Honcho held outside; local readers/synthesizer promoted

User decision: keep Honcho outside for now. Runtime still accepts optional snapshots for future use/audit, but default user-facing drafts no longer mention unavailable Honcho and no CARGOLO control path depends on Honcho tool availability.

- Promoted `plugins/cargolo_ops/employee_runtime.py` from preview-only stubs to structured local read-only readers:
  - `case_context`: reads `case_summary.json` / `summary.json` / `case.json` when present.
  - `mail_history`: reads `mail/history.json`, `mail_history.json`, `mail/messages.json` or markdown fallback; summarizes count/latest subject/from/preview.
  - `tms_snapshot`: reads `tms_snapshot.json`, `tms/snapshot.json`, `tms.json` or markdown fallback.
  - `document_analyst`: reads `docs/analysis.json`, `documents/analysis.json`, `document_analysis.json`; missing docs/discrepancies produce `NEEDS_REVIEW` and `requires_human=True`.
- Missing local mail/TMS/doc sources now return `NEEDS_REVIEW` with `missing_local_source` risk, not `FAILED`; the employee response stays operational but clearly marks source gaps as missing/not locally available.
- Raw documents without an analysis file now return `NEEDS_REVIEW` rather than `OK`, because file inventory alone is not a document discrepancy analysis.
- Added compact German ops synthesizer for `CASE_ASSIST`: `Lage: <AN> | ... | Keine externe Aktion ausgeführt.` It surfaces TMS status, pickup date, latest mail subject, document gaps, unavailable sources, and human-review needs.
- Added TDD coverage for structured local source reads, compact synthesis, missing local source handling, and raw-docs-without-analysis review behavior.
- Verification: `./venv-py312/bin/python -m pytest tests/cargolo_ops/test_honcho_memory.py tests/cargolo_ops/test_employee_runtime.py tests/cargolo_ops/test_employee_agent.py tests/cargolo_ops/test_coordinator_contracts.py tests/cargolo_ops/test_teams_ops_router.py tests/cargolo_ops/test_teams_reply_loop.py -q -o 'addopts='` returned `66 passed`; after doc-analysis fix `tests/cargolo_ops/test_employee_runtime.py` returned `10 passed`.
- Safety check: AST call/import scan found no Teams/network send calls, TMS/writeback imports, customer-send calls, or ops notification imports in runtime/foundation files.
- Codex-first `delegate_task` review returned PASS; minor raw-docs-without-analysis concern was fixed and re-review returned PASS.

### 2026-05-09 — Employee Runtime CLI smoke runner added

Added the first local operator/smoke entrypoint so CARGOLO cases can be tested without Teams access and without external side effects.

- Created `plugins/cargolo_ops/employee_runtime_cli.py`, invokable with `python -m plugins.cargolo_ops.employee_runtime_cli`.
- CLI arguments:
  - `--text` required employee request text.
  - `--order` optional AN/BU context.
  - `--channel`, `--actor`, `--root` optional audit/context knobs.
  - `--json` prints machine-readable `EmployeeRuntimeResult.to_audit_row()`.
  - `--no-audit` skips only the CLI audit JSONL; runtime case-assist may still append local specialist result JSONL.
- Default output is the compact German ops draft (`Lage: <AN> ... Keine externe Aktion ausgeführt.`), suitable for quick terminal/Telegram smoke checks.
- CLI appends audit rows to `<root>/runtime/employee_runtime_cli.jsonl` unless `--no-audit`.
- Added `tests/cargolo_ops/test_employee_runtime_cli.py` covering compact text mode, JSON guarded-action mode, default-deny flags, audit writing, and required `--text` validation.
- Smoke result with local fixture: `Lage: AN-11755 | Sea / Hamburg -> Shanghai | TMS: docs pending | Mails: 1 / zuletzt: CI fehlt | Docs offen: commercial_invoice | Prüfung nötig: document_analyst | Keine externe Aktion ausgeführt.`
- Verification: `./venv-py312/bin/python -m pytest tests/cargolo_ops/test_honcho_memory.py tests/cargolo_ops/test_employee_runtime.py tests/cargolo_ops/test_employee_runtime_cli.py tests/cargolo_ops/test_employee_agent.py tests/cargolo_ops/test_coordinator_contracts.py tests/cargolo_ops/test_teams_ops_router.py tests/cargolo_ops/test_teams_reply_loop.py -q -o 'addopts='` returned `70 passed`; final verification later returned `70 passed` before docs-only update and `67 passed` prior to CLI tests.
- Safety check: AST call/import scan found no Teams/network send calls, TMS/writeback imports, customer-send calls, subprocess calls, or ops notification imports in the CLI/runtime path.
- Codex-first review returned PASS. Minor notes: `--no-audit` only suppresses CLI audit (not local runtime specialist JSONL), and separately generated timestamps may differ between audit row and JSON stdout.

### 2026-05-09 — CLI case picker added (`--latest` / `--next-review`)

Added local case selection so an operator can run quick case smokes without manually copying AN numbers.

- `employee_runtime_cli.py` now has mutually exclusive selectors:
  - `--order AN-...` explicit case.
  - `--latest` selects the most recently changed valid local case folder under `<root>/orders`.
  - `--next-review` selects the most recently changed valid local case folder with a review marker.
- Review markers currently supported: `review_required.json`, `needs_review.json`, `pending_review.json`, `employee/review_required.json`.
- Case picker filters candidates through `normalize_order_ids(path.name)` and ignores hidden/non-order folders such as `archive`, `template`, or `tmp`.
- Empty/no-match states fail via argparse with clear operator errors instead of silently falling back to free chat.
- Added TDD coverage for latest selection, next-review selection, mutual exclusion, ignoring non-order folders, and no-review-marker errors.
- Smoke outputs:
  - `--latest`: `Lage: AN-20000 | Air / FRA -> JFK | TMS: needs attention | Keine externe Aktion ausgeführt.`
  - `--next-review`: `Lage: AN-20000 | Air / FRA -> JFK | TMS: needs attention | Docs offen: commercial_invoice | Prüfung nötig: document_analyst | Keine externe Aktion ausgeführt.`
- Verification: CARGOLO runtime slice returned `75 passed`.
- Safety check: AST call/import scan remained clean: no Teams/network send calls, TMS/writeback imports, customer-send calls, subprocess calls, or ops notification imports in the CLI/runtime path.
- Codex-first review returned PASS after hardening non-order-folder filtering and adding no-marker error coverage. Remaining non-blocking note: equal-mtime tie-break uses lexicographically largest folder name.

### 2026-05-09 — Runtime review marker writer added

Added the first automatic local review-queue feed so `--next-review` can be driven by runtime findings instead of only manually placed marker files.

- `run_employee_runtime()` now synchronizes `<root>/orders/<AN>/employee/review_required.json` after CASE_ASSIST runs.
- Marker is written when any specialist returns `NEEDS_REVIEW` or `requires_human=True`.
- Marker payload includes `source=employee_runtime`, `order_id`, `specialists`, compact `summary`, risks, recommended actions, evidence refs, and default-deny flags:
  - `should_send_to_teams=false`
  - `should_write_tms=false`
  - `should_send_customer_message=false`
- Full clean runs remove stale markers only when the current checked scope covers the marker scope.
- Narrow clean runs preserve markers if prior marker-listed specialists were not rechecked.
- Narrow review runs preserve out-of-scope marker specialists while updating currently reviewed specialists, preventing pending Docs/TMS/Mail review from being hidden by a smaller follow-up question.
- Added TDD coverage for marker creation, full clean marker removal, narrow-clean marker preservation, and narrow-review scope preservation.
- Verification: CARGOLO runtime slice returned `79 passed`.
- CLI smoke: a missing-source `--order AN-11755` run writes `employee/review_required.json`; a later `--next-review` run selects that case successfully.
- Safety check: AST call/import scan remained clean: no Teams/network send calls, TMS/writeback imports, customer-send calls, subprocess calls, or ops notification imports in the CLI/runtime path.
- Codex-first review initially requested two scope-loss fixes; both were implemented. Final re-review returned PASS.

### 2026-05-09 — Teams dedicated-channel assumption accepted

User clarified the intended future Teams deployment will use a dedicated CARGOLO/Hermes channel where all messages are meant for Hermes. Therefore the Teams adapter should support a no-mention mode for that channel.

Design implication for next adapter tranche:

- Dedicated Teams channel: every inbound message is eligible for the CARGOLO employee runtime without requiring `@Hermes CARGOLO`.
- Non-dedicated/shared channels: keep mention/reply gating to avoid accidental interception.
- First production mode should remain internal and read-only/draft-only:
  - Teams inbound allowed.
  - Internal Teams reply/draft only in the configured channel/thread.
  - No TMS writes.
  - No customer sends.
  - Guard cards/default-deny remain required for boundary actions.
- Adapter should make channel mode explicit in config/tests, e.g. `dedicated_channel=True` or allowlist entry with `requires_mention=False`, so later Teams rollout is auditable.

### 2026-05-09 — Teams employee handoff safe-mode contract added

Added the pre-adapter contract for the next Teams tranche. This is still read-only/draft-only and does not send anything to Teams; it only converts eligible inbound Teams messages into local employee-runtime work plus local audit.

- Created `plugins/cargolo_ops/teams_employee_handoff.py`.
- Created `tests/cargolo_ops/test_teams_employee_handoff.py`.
- `TeamsHandoffConfig` supports:
  - `dedicated_channel_ids`: channel allowlist where every inbound message is intended for Hermes and no mention is required.
  - `mention_patterns`: shared-channel prefixes such as `@Hermes CARGOLO`, `@Hermes`, `Hermes CARGOLO`.
  - `audit_enabled`: local JSONL audit toggle.
- `handle_teams_employee_message(...)` behavior:
  - Dedicated channel: routes without mention, `handoff_mode=dedicated_channel`, `requires_mention=false`.
  - Shared channel with mention prefix: strips mention and routes, `handoff_mode=mention`.
  - Shared channel without mention: returns `handled=false`, `reason=mention_required`.
- Eligible messages run `run_employee_runtime(EmployeeRequest(channel="teams", actor=...))` and return a local row with `response_text`, `classification`, `order_id`, handoff mode, and default-deny flags.
- Audit is local only: `<root>/runtime/teams_employee_handoff.jsonl`.
- Safety contract remains explicit:
  - No Teams sends.
  - No TMS writes.
  - No customer sends.
  - Boundary intents return guarded runtime drafts only.
- Added regression coverage for:
  - Dedicated channel without mention.
  - Shared channel ignored unless mentioned.
  - Shared channel mention stripping.
  - Mention prefix boundary (`@HermesFoo` must not match `@Hermes`).
  - TMS write request in dedicated channel remains guarded and side-effect-free.
- Verification: CARGOLO runtime/Teams slice returned `84 passed`.
- Safety check: AST call/import scan remained clean: no Teams/network send calls, TMS/writeback imports, customer-send calls, subprocess calls, or ops notification imports in the new handoff path.
- Codex-first review returned PASS after mention-boundary hardening.

#### Next-session Teams implementation target

Start from `teams_employee_handoff.py` and connect it to the real Teams gateway/platform adapter in controlled safe mode:

1. Locate the Teams inbound message handler and extract `channel_id`, `message_id`, `user_id`, `user_name`, text, and reply/thread identifiers.
2. Add explicit config/allowlist for the dedicated CARGOLO/Hermes channel with `requires_mention=false` semantics.
3. Call `handle_teams_employee_message(...)` for that channel.
4. Initially return/draft the `response_text` only to the internal channel/thread; keep TMS/customer actions guarded.
5. Add gateway-level tests proving shared channels still require mention/reply gating.
6. Run the same CARGOLO regression and safety scans before any live gateway restart.

### 2026-05-11 — Real Teams inbound connected to employee handoff safe mode

Implemented the next-session target up to the safe internal Teams reply boundary. The CARGOLO employee runtime is now reachable from the real Teams adapter when explicitly enabled, while TMS/customer side effects remain guarded by the existing runtime contract.

- Updated `plugins/platforms/teams/adapter.py`:
  - Added `cargolo_employee_handoff_enabled` / `CARGOLO_TEAMS_EMPLOYEE_HANDOFF_ENABLED` feature gate.
  - Added `cargolo_employee_dedicated_channel_ids` / `CARGOLO_TEAMS_EMPLOYEE_DEDICATED_CHANNEL_IDS` allowlist.
  - Extracts the best channel id from `channelData.channel.id` / channel id fields, falling back to conversation id.
  - Preserves Teams `<at>...</at>` mention intent for shared-channel handoff after stripping Teams HTML mention markup.
  - Routes eligible inbound Teams messages through `handle_teams_employee_message(...)` before the legacy ops router.
  - Sends only the safe `response_text` back as an internal Teams reply; no TMS/customer action is introduced by this adapter path.
- Added gateway tests in `tests/gateway/test_teams.py` proving:
  - Dedicated CARGOLO/Hermes channel routes without mention and replies with the handoff draft.
  - Shared channel routes only when Teams mention metadata is present and passes a synthetic `@Hermes` prefix into the handoff gate.
  - Existing ops-router/status/pending-card behavior remains intact when the new gate is disabled or not matched.
- Verification: targeted handoff/router slice returned `10 passed`; broader CARGOLO+Teams gateway slice returned `141 passed`.
- Safety check: the new handoff path still imports no writeback/notification modules and adds no TMS/customer write calls. Existing Teams adapter outbound POST helpers pre-existed and are not part of the new CARGOLO handoff side-effect surface.
- Live rollout note: do not restart/enable blindly. For live Teams test set the feature flag and dedicated channel id, restart gateway, then first test with a read-only message such as `Was ist mit AN-11755 los?` in the dedicated channel.

---

## Later implementation method

When user says to implement:

1. Load `cargolo-asr-agent`, `hermes-agent`, `codex`, `writing-plans`, and `subagent-driven-development`.
2. Run readiness checks.
3. Use Codex CLI workers for code tasks if smoke test passes; use Hermes-native `delegate_task` with inherited Codex runtime for normal subagent implementation/review tasks.
4. Use fresh worker per task, then spec review, then quality review.
5. Run CARGOLO Teams regression slice.
6. Commit using CARGOLO git identity.
7. Report in German with Teams relevance and next test step.
