#!/usr/bin/env bash
# Preflight for `make e2e`: verifies (a) the agent's WS has reconnected
# to central after a deploy-rollout, and (b) the agent's pod can resolve
# + reach yscale-cloud over HTTP. Either failure should be fatal —
# running tests when the agent can't talk to central wastes wall-clock
# (workloads time out at Provisioning).
#
# Usage:
#   scripts/wait-agent-connected.sh                # just check WS reconnect
#   scripts/wait-agent-connected.sh --verify-http  # also probe HTTP
#
# Exit 0 on success, non-zero (with stderr message) on failure. The
# Makefile wraps this so a failure bubbles up as `make e2e` failing.

set -euo pipefail

NS="${YSCALE_NAMESPACE:-yscale}"
MODE="${1:-ws}"

agent_pod() {
  kubectl -n "$NS" get pods -l app.kubernetes.io/name=yscale-agent -o json 2>/dev/null \
    | jq -r '.items[]
        | select(any(.spec.containers[]?; .name == "agent"))
        | select(any(.status.containerStatuses[]?; .name == "agent" and .ready == true))
        | .metadata.name' \
    | head -1
}

cloud_log_tail() {
  kubectl -n "$NS" logs deploy/yscale-cloud --tail=20 2>/dev/null
}

# Wait up to 90s for the AGENT to log "connected to central" — checked
# in the agent's own pod log, not central's, because the agent's most-
# recent line is authoritative about its current state. Central may
# have logged "agent connected" minutes ago from a since-dead WS.
#
# We also reject "agent connection ended" lines newer than any
# "connected to central" line — that means the WS came up briefly
# then died (the DNS-flaky scenario that bit us today).
if [ "$MODE" = "ws" ]; then
  echo "[preflight] waiting up to 90s for agent WS to be ACTIVELY connected..." >&2
  POD="$(agent_pod)"
  if [ -z "$POD" ]; then
    echo "[preflight] ERROR: no yscale-agent pod found" >&2
    exit 1
  fi
  for i in $(seq 1 45); do
    LOG="$(kubectl -n "$NS" logs "$POD" -c agent --tail=20 2>/dev/null)"
    # Most-recent "connected to central" must come AFTER any most-
    # recent "agent connection ended". Use line numbers.
    UP_LINE=$(printf '%s\n' "$LOG" | grep -n '"connected to central"' | tail -1 | cut -d: -f1 || true)
    DOWN_LINE=$(printf '%s\n' "$LOG" | grep -nE '"agent connection ended"' | tail -1 | cut -d: -f1 || true)
    if [ -n "$UP_LINE" ] && { [ -z "$DOWN_LINE" ] || [ "$UP_LINE" -gt "$DOWN_LINE" ]; }; then
      echo "[preflight] agent WS actively connected ✓" >&2
      exit 0
    fi
    sleep 2
  done
  echo "[preflight] ERROR: agent never reached a stable 'connected to central' state in 90s" >&2
  echo "[preflight] agent log tail:" >&2
  kubectl -n "$NS" logs "$POD" -c agent --tail=15 2>&1 | tail -15 >&2
  exit 1
fi

# HTTP probe: from inside the agent's pod, curl yscale-cloud's /healthz.
# This catches the case where the agent + cloud are both "Running" but
# CoreDNS is broken or the cluster network is fragmented — the exact
# failure mode that made `make e2e` time out at 20m / 16s today.
if [ "$MODE" = "--verify-http" ] || [ "$MODE" = "http" ]; then
  POD="$(agent_pod)"
  if [ -z "$POD" ]; then
    echo "[preflight] ERROR: no yscale-agent pod found in namespace $NS" >&2
    exit 1
  fi
  # Probe from inside the tailscale sidecar — same network namespace
  # as the agent container, so the result reflects exactly what the
  # agent would see. `tailscale` sidecar is alpine-based and has wget.
  # The `agent` container is distroless so we can't exec into it.
  echo "[preflight] probing yscale-cloud /healthz from $POD (tailscale sidecar)..." >&2
  RES="$(kubectl -n "$NS" exec "$POD" -c tailscale -- \
    wget -qO- --timeout=5 http://yscale-cloud.yscale:8443/healthz 2>&1 \
    || echo "[exec-failed]")"
  if echo "$RES" | grep -q '^ok'; then
    echo "[preflight] yscale-cloud reachable from agent pod ✓" >&2
    exit 0
  fi
  echo "[preflight] ERROR: yscale-cloud not reachable from agent pod: $RES" >&2
  exit 1
fi

echo "[preflight] usage: $0 [--verify-http]" >&2
exit 2
