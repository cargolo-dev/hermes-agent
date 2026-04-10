# CARGOLO ASR Architecture

## Principles
- n8n owns mailbox ingestion and normalization.
- Hermes owns deterministic case-folder processing, comparison, recommendation, and reporting.
- TMS remains source of truth for official shipment data.
- The order folder is the local operational case file.
- No destructive actions. No customer auto-send. No implicit TMS mutations.

## Runtime flow
1. Outlook/M365 -> n8n ingest workflow
2. n8n normalizes payload, computes HMAC, calls Hermes webhook
3. Hermes webhook validates request and accepts the event
4. The ASR webhook route runs a deterministic direct processor before the LLM summary step
5. Processor:
   - validates payload shape
   - resolves AN/order id
   - creates or loads case folder
   - checks idempotency via message_id/dedupe_hash
   - stores raw email and attachments
   - for a brand-new case: first initializes the folder by pulling mail history and writing the first TMS snapshot before recommendations are made
   - loads case_state/entities/tms snapshot
   - optionally pulls mail history via n8n endpoint
   - classifies and extracts entities
   - computes delta vs prior state and TMS snapshot
   - updates case_state/entities/timeline/audit
   - creates a draft
   - creates or proposes a safe task
6. Ops users consume Telegram summaries, files, and periodic reports

## Code map
- plugins/cargolo_ops/models.py
  - pydantic schemas for webhook payloads, case state, delta analysis, results
- plugins/cargolo_ops/storage.py
  - order-folder persistence and audit/index helpers
- plugins/cargolo_ops/adapters.py
  - mock TMS/task adapter and n8n mail-history client
- plugins/cargolo_ops/processor.py
  - deterministic orchestration and update logic
- plugins/cargolo_ops/reporting.py
  - daily briefing aggregation
- tools/cargolo_asr_tool.py
  - Hermes tool surface

## Environment variables
- HERMES_CARGOLO_ASR_MAIL_HISTORY_URL
- HERMES_CARGOLO_ASR_MAIL_HISTORY_TOKEN (optional)
- HERMES_CARGOLO_ASR_MAIL_HISTORY_TIMEOUT (optional)

## Future live adapter slots
- replace MockTMSAdapter with live read adapter for:
  - get_order
  - search_order_by_reference
  - list_open_tasks
  - create_task
  - add_internal_note
  - get_customer_rules
- keep the same public method contract to avoid rewriting processor logic
