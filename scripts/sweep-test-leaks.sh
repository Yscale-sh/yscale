#!/usr/bin/env bash
# sweep-test-leaks.sh — destroy backend resources left over from prior
# yscaletest runs. Safe-by-design per the cleanup_safety_directive
# memory: ONLY touches resources whose names match `ys-burst-*`
# (yscale-created naming convention). NEVER touches anything else,
# even if it shares the same Fly app or Linode account.
#
# Why this is needed:
#   - When a test is killed (ctrl-c, terminal closes, runner crashes),
#     the runner's deferred cleanup() doesn't run.
#   - The agent's CR-deletion → cancel path (added 2026-05-24) handles
#     the case where the CR is deleted gracefully, but a killed runner
#     may leave the CR around too.
#   - Even with both, central's reap is best-effort — if backend
#     CreateNode succeeded but TS auth/bootstrap failed, the burst
#     never enters central's state.Store and is unknown to reap.
#
# This script catches all three failure modes by going DIRECTLY to the
# backend APIs and destroying anything matching the yscale prefix.
#
# Reads FLYIO_TOKEN, FLY_ORG, LINODE_TOKEN from env (or .env file).
# Run from the repo root.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ -f "${REPO_ROOT}/.env" ] && set -a && . "${REPO_ROOT}/.env" && set +a || true

# Tunable: leave most-recent N seconds alone in case a test is mid-flight.
# Default 0 = sweep everything; set MIN_AGE_SEC=300 if you want a 5-min
# grace period to avoid clobbering a concurrent test run.
MIN_AGE_SEC="${MIN_AGE_SEC:-0}"
DRY_RUN="${DRY_RUN:-0}"

log() { printf '[sweep] %s\n' "$*" >&2; }
maybe_run() { if [ "$DRY_RUN" = "1" ]; then echo "DRY: $*"; else "$@"; fi; }

# ---------- Fly machines ----------
sweep_fly() {
  if [ -z "${FLYIO_TOKEN:-}" ]; then
    log "Fly: skipping (no FLYIO_TOKEN)"
    return 0
  fi
  local app="hs-${FLY_ORG:-personal}-burst"
  # Fall back to the literal app name if FLY_ORG is unset and the
  # personal-prefixed app doesn't exist. The user's setup uses
  # hs-personal-burst per .env defaults.

  local now resp
  now="$(date +%s)"
  resp="$(curl -fsS -H "Authorization: Bearer ${FLYIO_TOKEN}" \
    "https://api.machines.dev/v1/apps/${app}/machines" 2>/dev/null || echo '[]')"

  echo "$resp" | python3 -c "
import sys, json, time
now = int(time.time())
min_age = int('$MIN_AGE_SEC')
m = json.load(sys.stdin)
victims = []
for x in m:
    if x.get('state') == 'destroyed':
        continue
    if not x.get('name','').startswith('ys-burst-'):
        continue
    # parse RFC3339 created_at into epoch seconds
    from datetime import datetime
    try:
        created = datetime.strptime(x['created_at'].split('.')[0], '%Y-%m-%dT%H:%M:%S').timestamp()
    except Exception:
        created = 0
    age = now - created
    if age < min_age:
        continue
    victims.append(x['id'])
    print(f'{x[\"id\"]} {x.get(\"name\",\"\")} age={int(age)}s', file=sys.stderr)
print(' '.join(victims))
" > /tmp/sweep-fly-victims.$$ 2> /tmp/sweep-fly-list.$$ || true
  cat /tmp/sweep-fly-list.$$ >&2
  local ids
  ids="$(cat /tmp/sweep-fly-victims.$$)"
  rm -f /tmp/sweep-fly-list.$$ /tmp/sweep-fly-victims.$$
  if [ -z "$ids" ]; then
    log "Fly: no leaked ys-burst-* machines (app=$app)"
    return 0
  fi
  for mid in $ids; do
    log "Fly: destroying $mid"
    maybe_run curl -fsS -X POST -H "Authorization: Bearer ${FLYIO_TOKEN}" \
      "https://api.machines.dev/v1/apps/${app}/machines/${mid}/stop" >/dev/null 2>&1 || true
    sleep 2
    maybe_run curl -fsS -X DELETE -H "Authorization: Bearer ${FLYIO_TOKEN}" \
      "https://api.machines.dev/v1/apps/${app}/machines/${mid}?force=true" >/dev/null 2>&1 || true
  done
}

# ---------- Linode instances ----------
sweep_linode() {
  if [ -z "${LINODE_TOKEN:-}" ]; then
    log "Linode: skipping (no LINODE_TOKEN)"
    return 0
  fi
  local resp
  resp="$(curl -fsS -H "Authorization: Bearer ${LINODE_TOKEN}" \
    "https://api.linode.com/v4/linode/instances" 2>/dev/null || echo '{}')"

  echo "$resp" | python3 -c "
import sys, json, time
from datetime import datetime, timezone
now = datetime.now(timezone.utc).timestamp()
min_age = int('$MIN_AGE_SEC')
d = json.load(sys.stdin)
victims = []
for x in d.get('data', []):
    label = x.get('label','')
    if not label.startswith('ys-burst-'):
        continue
    try:
        created = datetime.fromisoformat(x['created'].replace('Z', '+00:00')).timestamp()
    except Exception:
        created = 0
    age = now - created
    if age < min_age:
        continue
    victims.append(str(x['id']))
    print(f'{x[\"id\"]} {label} age={int(age)}s', file=sys.stderr)
print(' '.join(victims))
" > /tmp/sweep-linode-victims.$$ 2> /tmp/sweep-linode-list.$$ || true
  cat /tmp/sweep-linode-list.$$ >&2
  local ids
  ids="$(cat /tmp/sweep-linode-victims.$$)"
  rm -f /tmp/sweep-linode-list.$$ /tmp/sweep-linode-victims.$$
  if [ -z "$ids" ]; then
    log "Linode: no leaked ys-burst-* instances"
    return 0
  fi
  for lid in $ids; do
    log "Linode: destroying instance $lid"
    maybe_run curl -fsS -X DELETE -H "Authorization: Bearer ${LINODE_TOKEN}" \
      "https://api.linode.com/v4/linode/instances/${lid}" >/dev/null 2>&1 || true
  done
}

# ---------- main ----------
log "starting sweep (min_age=${MIN_AGE_SEC}s, dry_run=${DRY_RUN})"
sweep_fly
sweep_linode
log "done"
