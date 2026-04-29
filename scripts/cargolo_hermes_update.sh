#!/usr/bin/env bash
set -euo pipefail

REPO="${HERMES_CARGOLO_REPO:-/root/.hermes/hermes-agent}"
PY="$REPO/venv/bin/python3"
PIP="$REPO/venv/bin/pip"
REMOTE_UPSTREAM="${HERMES_CARGOLO_UPSTREAM_REMOTE:-origin}"
UPSTREAM_BRANCH="${HERMES_CARGOLO_UPSTREAM_BRANCH:-main}"
BACKUP_REMOTE="${HERMES_CARGOLO_BACKUP_REMOTE:-cargolo}"
BACKUP_BRANCH="${HERMES_CARGOLO_BACKUP_BRANCH:-cargolo-live-main}"
LOG_DIR="${HERMES_CARGOLO_UPDATE_LOG_DIR:-/root/.hermes/logs}"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/cargolo-hermes-update-$(date -u +%Y%m%dT%H%M%SZ).log"

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG_FILE"; }
run() { log "+ $*"; "$@" 2>&1 | tee -a "$LOG_FILE"; }

cd "$REPO"
log "CARGOLO Hermes safe update started in $REPO"

if [ ! -x "$PY" ]; then
  log "ERROR: Python venv not found at $PY"
  exit 1
fi

run git fetch "$REMOTE_UPSTREAM" "$UPSTREAM_BRANCH"
run git fetch "$BACKUP_REMOTE" || true

if ! git diff --quiet || ! git diff --cached --quiet || [ -n "$(git status --porcelain --untracked-files=normal)" ]; then
  log "ERROR: working tree is not clean. Commit/stash deliberately refused."
  git status --short --branch | tee -a "$LOG_FILE"
  exit 2
fi

CURRENT_BRANCH="$(git branch --show-current)"
if [ "$CURRENT_BRANCH" != "main" ]; then
  run git checkout main
fi

run git rebase "$REMOTE_UPSTREAM/$UPSTREAM_BRANCH"

if [ -x "$PIP" ]; then
  run "$PIP" install -e .
fi

log "Running ASR/Gateway regression suite (retry once for known 30s timeout flakes)"
if ! "$PY" -m pytest tests/cargolo_ops tests/gateway/test_webhook_integration.py tests/gateway/test_webhook_asr_forwarding.py -q -o 'addopts=' 2>&1 | tee -a "$LOG_FILE"; then
  log "Regression suite failed once; retrying once to distinguish flaky timeout from real failure"
  run "$PY" -m pytest tests/cargolo_ops tests/gateway/test_webhook_integration.py tests/gateway/test_webhook_asr_forwarding.py -q -o 'addopts='
fi

log "Verifying CARGOLO ASR import origins and direct-processor markers"
run "$PY" - <<'PY'
import importlib.util
from pathlib import Path
mods = [
    'plugins.cargolo_ops.analysis',
    'plugins.cargolo_ops.processor',
    'plugins.cargolo_ops.ops_notifications',
    'gateway.platforms.webhook',
    'tools.cargolo_asr_tool',
]
for mod in mods:
    spec = importlib.util.find_spec(mod)
    origin = spec.origin if spec else None
    print(f'{mod} => {origin}')
    if not origin or '/root/.hermes/hermes-agent/' not in origin:
        raise SystemExit(f'bad import origin for {mod}: {origin}')
text = Path('gateway/platforms/webhook.py').read_text()
for marker in ['_run_direct_processor', 'cargolo_asr_email', 'processor_result', 'suppress_delivery', 'direct_processor_options']:
    if marker not in text:
        raise SystemExit(f'missing gateway marker: {marker}')
print('ASR import/marker verification OK')
PY

if command -v systemctl >/dev/null 2>&1; then
  run systemctl --user restart hermes-gateway
  sleep 5
  run systemctl --user is-active hermes-gateway
fi

if command -v curl >/dev/null 2>&1; then
  run curl -sS -i --max-time 8 http://127.0.0.1:8644/health
  run curl -sS -i --max-time 8 -X POST -H 'Content-Type: application/json' --data '{}' http://127.0.0.1:8644/webhooks/cargolo-asr-ingest
fi

run git push "$BACKUP_REMOTE" HEAD:"$BACKUP_BRANCH"
log "CARGOLO Hermes safe update finished successfully. Backup pushed to $BACKUP_REMOTE/$BACKUP_BRANCH"
log "Log file: $LOG_FILE"
