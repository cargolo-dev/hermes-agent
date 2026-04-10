# CARGOLO ASR Ops MVP

This directory documents the Hermes-native MVP for CARGOLO ASR (Air/Sea/Rail).

What is implemented:
- Deterministic processing of normalized n8n email events
- Per-order case folders under ~/.hermes/cargolo_asr/orders/AN-xxxxx
- Idempotent email indexing and raw/normalized storage
- Delta comparison against the current case state and TMS snapshot
- Draft generation without customer auto-send
- Safe task proposal / mock task creation
- Daily ops report generation
- n8n mail-history endpoint client for full-sync/delta-sync

Main code:
- plugins/cargolo_ops/
- tools/cargolo_asr_tool.py

Recommended webhook usage:
1. Keep Hermes webhook HMAC validation at the gateway layer.
2. Let n8n send normalized payloads with top-level mail_context and messages[].
3. Use the cargolo_asr_process_event tool to persist and reconcile the event deterministically.
4. Use cargolo_asr_mail_history for first-sync / delta-sync by AN.
5. Use cargolo_asr_daily_report from cron or manually for ops briefings.

Default runtime root:
- ~/.hermes/cargolo_asr/

Case file layout per order:
- case_state.json
- timeline.md
- entities.json
- tms_snapshot.json
- email_index.jsonl
- emails/raw/
- emails/normalized/
- emails/drafts/
- documents/inbound/
- documents/generated/
- tasks/task_log.jsonl
- audit/actions.jsonl
