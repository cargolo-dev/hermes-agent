# CARGOLO ASR Ops MVP Implementation Plan

> For Hermes: use subagent-driven-development for review and focused follow-up work.

Goal: Build a production-oriented Hermes-native MVP that turns normalized n8n email events into durable per-order case folders, delta-aware case updates, safe drafts/tasks, and daily ops reports.

Architecture: Reuse Hermes webhook ingress and add deterministic domain logic under plugins/cargolo_ops plus dedicated Hermes tools for event processing, mail-history sync, and reporting. Keep customer communication draft-only and TMS writes mock-safe.

Tech stack: Python 3.11, pydantic v2, pytest, Hermes webhook + tools runtime, JSON/JSONL local case store.

---

## Target tree

- .hermes.md
- docs/cargolo-asr/README.md
- docs/cargolo-asr/architecture.md
- docs/cargolo-asr/workflows/n8n-asr-ingest.json
- docs/cargolo-asr/workflows/n8n-asr-mail-history-v3.json
- plugins/cargolo_ops/__init__.py
- plugins/cargolo_ops/models.py
- plugins/cargolo_ops/storage.py
- plugins/cargolo_ops/adapters.py
- plugins/cargolo_ops/processor.py
- plugins/cargolo_ops/reporting.py
- tools/cargolo_asr_tool.py
- tests/cargolo_ops/test_models.py
- tests/cargolo_ops/test_processor.py
- tests/cargolo_ops/test_reporting.py

## Implemented slice

1. Schema layer
   - Canonical IncomingEmailEvent model with compatibility for current asr_email_thread webhook payloads.
2. Storage layer
   - Per-order file structure under ~/.hermes/cargolo_asr/orders/AN-xxxxx.
3. Processing layer
   - Idempotent event processing, folder creation, delta analysis, draft generation, task proposal, audit logging.
4. Adapter layer
   - Safe mock TMS/task adapter and n8n mail-history HTTP client.
5. Tool layer
   - cargolo_asr_process_event
   - cargolo_asr_mail_history
   - cargolo_asr_daily_report
6. Reporting layer
   - Daily operational summary over local cases.
7. Tests and docs
   - Focused regression suite and workflow docs.

## Remaining iterative expansion points

- Replace mock TMS with live read adapter once credentials and field contracts are finalized.
- Wire webhook route/skill/runtime config to call cargolo_asr_process_event automatically.
- Add richer entity extraction and stronger contradiction logic.
- Add scheduled cron wiring and delivery presets.
- Add explicit review-queue tooling and dashboards.
