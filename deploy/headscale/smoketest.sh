#!/usr/bin/env bash
# yscale:proprietary
# smoketest.sh — provision a Headscale factory box, wait for health, then
# ALWAYS tear it down. This is the safe way to exercise a real box on the
# shared Linode account: teardown is guaranteed by a shell trap that fires
# on success, failure, error, OR interrupt (Ctrl-C / SIGTERM / SIGHUP).
#
# Two independent auto-teardown safety nets, so a box can never leak:
#   1. THIS wrapper's trap — deletes by tag on ANY exit (host side).
#   2. A box-side dead-man self-poweroff baked into the test box's
#      cloud-init (see HS_SELFDESTRUCT_MIN below) — if this wrapper's host
#      dies / loses network mid-run and the trap never fires, the box
#      powers ITSELF off after the TTL, stopping compute billing without
#      anyone touching it. A later `deprovision.sh --all` reaps the husk.
#
# Usage:
#   LINODE_TOKEN=... ./smoketest.sh [user] [region] [type]
#   KEEP=1 LINODE_TOKEN=... ./smoketest.sh   # provision + verify, DON'T tear down
#
# Env:
#   HS_SELFDESTRUCT_MIN  box-side self-poweroff TTL in minutes (default 30).
#                        Belt-and-braces against an orphaned test box.
#   HEALTH_TIMEOUT_S     how long to wait for :443 health (default 480).
set -euo pipefail

USER_SLUG="${1:-smoketest}"
REGION="${2:-us-ord}"
TYPE="${3:-g6-standard-1}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
: "${LINODE_TOKEN:?set LINODE_TOKEN}"
export LINODE_TOKEN
HS_SELFDESTRUCT_MIN="${HS_SELFDESTRUCT_MIN:-30}"
HEALTH_TIMEOUT_S="${HEALTH_TIMEOUT_S:-480}"
export HS_SELFDESTRUCT_MIN   # consumed by cloud-init via provision.sh

# Optional diagnostics: if SMOKE_SSHKEY points at a private key, inject its
# .pub into the box so we can pull /var/log on failure. Test-only; provision
# passes SMOKE_PUBKEY through to the create call's authorized_keys.
if [ -n "${SMOKE_SSHKEY:-}" ] && [ -f "${SMOKE_SSHKEY}.pub" ]; then
  SMOKE_PUBKEY="$(cat "${SMOKE_SSHKEY}.pub")"
  export SMOKE_PUBKEY
fi

log() { echo "[smoketest] $*" >&2; }

LID=""
HOST=""

# teardown — the guaranteed cleanup. Deletes by the EXACT yscale-headscale
# tag via deprovision.sh (never by name), so even if $LID was never
# captured (e.g. create response lost), the tag sweep still gets it.
teardown() {
  local rc=$?
  if [ "${KEEP:-0}" = "1" ]; then
    log "KEEP=1 — leaving box up. Tear down later with: ./deprovision.sh --all"
    return 0
  fi
  log "tearing down (exit code $rc) ..."
  if [ -n "$LID" ]; then
    bash "$SCRIPT_DIR/deprovision.sh" "$LID" 2>&1 | sed 's/^/[smoketest]   /' || true
  fi
  # Backstop: sweep ANY box for this user by tag, in case the id teardown
  # missed (lost id, retry, second box). Tag-verified inside deprovision.sh.
  bash "$SCRIPT_DIR/deprovision.sh" --user "$USER_SLUG" 2>&1 | sed 's/^/[smoketest]   /' || true
  log "teardown done"
}
trap teardown EXIT INT TERM HUP

# Provision. provision.sh prints "<LID> <IP> <HOSTNAME>" on its last line.
log "provisioning test box (user=$USER_SLUG region=$REGION type=$TYPE, self-destruct ${HS_SELFDESTRUCT_MIN}m) ..."
OUT="$(bash "$SCRIPT_DIR/provision.sh" "$USER_SLUG" "$REGION" "$TYPE")"
read -r LID _IP HOST <<<"$(echo "$OUT" | tail -1)"
if [ -z "$LID" ] || [ "$LID" = "null" ]; then
  log "FATAL: provision did not return a Linode id; trap will sweep by tag"
  exit 1
fi
log "provisioned LID=$LID host=$HOST — waiting up to ${HEALTH_TIMEOUT_S}s for https://$HOST/health"

# Poll health.
deadline=$(( $(date +%s) + HEALTH_TIMEOUT_S ))
healthy=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  if curl -fsS --max-time 8 "https://$HOST/health" >/dev/null 2>&1; then
    healthy=1; break
  fi
  sleep 15
done

if [ "$healthy" = "1" ]; then
  log "HEALTHY ✓  https://$HOST"
  log "verifying TLS issuer + key endpoint ..."
  echo | openssl s_client -connect "$HOST:443" -servername "$HOST" 2>/dev/null \
    | openssl x509 -noout -issuer 2>/dev/null | sed 's/^/[smoketest]   /' || true
  # /health 200 + a valid LE cert IS the readiness signal. The /key endpoint
  # needs a Tailscale protocol-version query param, so a bare GET returns
  # 400 — that's expected and NOT a failure. Report it without alarm.
  kc="$(curl -s --max-time 8 -o /dev/null -w '%{http_code}' "https://$HOST/key?v=106" || echo 000)"
  log "  key endpoint http=$kc (400 on a bare GET is normal)"
  log "SMOKE TEST PASSED"
else
  log "SMOKE TEST FAILED — :443 never healthy within ${HEALTH_TIMEOUT_S}s"
  # If a smoke SSH key was injected, pull the real bootstrap log + headscale
  # journal so we diagnose the ACTUAL cause instead of guessing. Best-effort.
  if [ -n "${SMOKE_SSHKEY:-}" ] && [ -n "${_IP:-}" ]; then
    log "fetching box logs via ssh ($_IP) ..."
    SSHOPT="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -i ${SMOKE_SSHKEY}"
    # shellcheck disable=SC2086
    ssh $SSHOPT "root@${_IP}" \
      'echo "=== bootstrap.log (tail) ==="; tail -40 /var/log/headscale-bootstrap.log 2>&1; echo "=== headscale journal (tail) ==="; journalctl -u headscale --no-pager -n 40 2>&1; echo "=== :443 listen ==="; ss -tlnp 2>/dev/null | grep -E ":443|:80" || echo none' \
      2>&1 | sed 's/^/[box]   /' || log "ssh log-fetch failed (box may not have booted ssh yet)"
  fi
  log "(box-side self-destruct will poweroff in <= ${HS_SELFDESTRUCT_MIN}m even if teardown below is skipped)"
  exit 1
fi
# trap teardown fires here on normal exit too.
