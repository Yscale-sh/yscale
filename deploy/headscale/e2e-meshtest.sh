#!/usr/bin/env bash
# yscale:proprietary
# e2e-meshtest.sh — REAL mesh data-plane e2e + speed test.
#
# Stands up a per-customer Headscale box, then TWO client nodes that
# actually join its tailnet via --login-server + preauth key, then
# measures the mesh: tailscale ping (RTT + direct-vs-DERP path) and
# iperf3 throughput between the two clients across the Headscale mesh.
#
# Proves the join path (currently unproven) AND gives the throughput
# number that decides the tailnet-data-plane thesis.
#
# ALL three boxes are tagged `yscale-headscale`, so the trap below tears
# down everything with one tag-scoped sweep on ANY exit. Each box also
# self-powers-off (dead-man) as a backstop.
#
# Usage: LINODE_TOKEN=... SMOKE_SSHKEY=/path/to/key ./e2e-meshtest.sh
#        [region-a] [region-b]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
: "${LINODE_TOKEN:?set LINODE_TOKEN}"
: "${SMOKE_SSHKEY:?set SMOKE_SSHKEY (path to an ssh private key; .pub injected into nodes)}"
export LINODE_TOKEN
REGION_A="${1:-us-ord}"
REGION_B="${2:-us-east}"     # different region → more realistic relay/path
TYPE="g6-standard-1"
SELFDESTRUCT_MIN="${SELFDESTRUCT_MIN:-30}"
export HS_SELFDESTRUCT_MIN="$SELFDESTRUCT_MIN"   # box dead-man via provision.sh
export SMOKE_SSHKEY
PUBKEY="$(cat "${SMOKE_SSHKEY}.pub")"
SSHOPT="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -i ${SMOKE_SSHKEY}"

log() { echo "[e2e] $*" >&2; }
api() { curl -sS -H "Authorization: Bearer $LINODE_TOKEN" "$@"; }

teardown() {
  local rc=$?
  log "tearing down ALL yscale-headscale nodes (exit $rc) ..."
  bash "$SCRIPT_DIR/deprovision.sh" --all 2>&1 | sed 's/^/[e2e]   /' || true
  log "teardown done"
}
trap teardown EXIT INT TERM HUP

# create_client <name> <region> <login-server> <authkey> -> prints "LID IP"
create_client() {
  local name="$1" region="$2" login="$3" authkey="$4"
  local body userdata pass
  userdata="$(printf '#!/bin/bash\nexport HS_LOGIN_SERVER=%q\nexport HS_AUTHKEY=%q\nexport HS_HOSTNAME=%q\nexport SELFDESTRUCT_MIN=%q\n' \
    "$login" "$authkey" "$name" "$SELFDESTRUCT_MIN"; tail -n +2 "$SCRIPT_DIR/client-cloud-init.sh")"
  userdata="$(printf '%s' "$userdata" | base64 | tr -d '\n')"
  pass="$(LC_ALL=C tr -dc 'A-Za-z0-9' < <(head -c 256 /dev/urandom) | cut -c1-32)"
  local resp lid ip
  resp="$(api -X POST -H "Content-Type: application/json" -d "{
    \"label\": \"headscale-${name}-$(date +%s)\",
    \"region\": \"$region\", \"type\": \"$TYPE\", \"image\": \"linode/debian12\",
    \"root_pass\": \"$pass\",
    \"authorized_keys\": [$(printf '%s' "$PUBKEY" | jq -R .)],
    \"tags\": [\"yscale-headscale\"],
    \"metadata\": {\"user_data\": \"$userdata\"}
  }" https://api.linode.com/v4/linode/instances)"
  lid="$(echo "$resp" | jq -r '.id')"
  [ "$lid" != "null" ] && [ -n "$lid" ] || { log "FATAL: client $name create failed: $resp"; return 1; }
  for _ in $(seq 1 30); do
    sleep 8
    ip="$(api "https://api.linode.com/v4/linode/instances/$lid" | jq -r '.ipv4[0]')"
    [ "$ip" != "null" ] && [ -n "$ip" ] && break
  done
  echo "$lid $ip"
}

# ---- 1. Headscale box ----
log "provisioning Headscale box in $REGION_A ..."
OUT="$(bash "$SCRIPT_DIR/provision.sh" smoketest "$REGION_A" "$TYPE")"
read -r BOX_LID BOX_IP BOX_HOST <<<"$(echo "$OUT" | tail -1)"
log "box LID=$BOX_LID host=$BOX_HOST — waiting for health ..."
for i in $(seq 1 40); do
  curl -fsS --max-time 8 "https://$BOX_HOST/health" >/dev/null 2>&1 && { log "box healthy (~$((i*15))s)"; break; }
  sleep 15
done
curl -fsS --max-time 8 "https://$BOX_HOST/health" >/dev/null 2>&1 || { log "FATAL: box never healthy"; exit 1; }

# ---- 2. grab the reusable preauth key off the box (it was minted at boot) ----
log "fetching preauth key from box via ssh ..."
for _ in $(seq 1 12); do
  AUTHKEY="$(ssh $SSHOPT "root@${BOX_IP}" 'cat /var/lib/headscale/bootstrap-preauthkey 2>/dev/null' 2>/dev/null || true)"
  [ -n "$AUTHKEY" ] && break
  sleep 5
done
[ -n "$AUTHKEY" ] || { log "FATAL: could not read preauth key from box"; exit 1; }
log "got preauth key (len ${#AUTHKEY})"
LOGIN="https://${BOX_HOST}"

# ---- 3. two client nodes that join the mesh ----
log "provisioning client-a ($REGION_A) + client-b ($REGION_B) ..."
read -r A_LID A_IP <<<"$(create_client client-a "$REGION_A" "$LOGIN" "$AUTHKEY")"
read -r B_LID B_IP <<<"$(create_client client-b "$REGION_B" "$LOGIN" "$AUTHKEY")"
log "client-a LID=$A_LID IP=$A_IP ; client-b LID=$B_LID IP=$B_IP"

# ---- 4. wait for both to register on the box ----
log "waiting for both clients to register in headscale ..."
A_TS=""; B_TS=""
for _ in $(seq 1 40); do
  NODES_JSON="$(ssh $SSHOPT "root@${BOX_IP}" 'headscale nodes list -o json 2>/dev/null' 2>/dev/null || echo '[]')"
  A_TS="$(echo "$NODES_JSON" | jq -r '.[] | select(.given_name=="client-a" or .name=="client-a") | .ip_addresses[]?' | grep -E '^100\.' | head -1)"
  B_TS="$(echo "$NODES_JSON" | jq -r '.[] | select(.given_name=="client-b" or .name=="client-b") | .ip_addresses[]?' | grep -E '^100\.' | head -1)"
  [ -n "$A_TS" ] && [ -n "$B_TS" ] && break
  sleep 10
done
[ -n "$A_TS" ] && [ -n "$B_TS" ] || { log "FATAL: clients did not both register (a='$A_TS' b='$B_TS')"; exit 1; }
log "registered: client-a=$A_TS  client-b=$B_TS"

# ---- 5. measure: RTT + path, then throughput ----
log "waiting for tailscale path to come up (a->b) ..."
for _ in $(seq 1 18); do
  ssh $SSHOPT "root@${A_IP}" "tailscale ping --timeout=3s --c=1 ${B_TS}" >/dev/null 2>&1 && break
  sleep 5
done

echo "================ MESH SPEED TEST RESULTS ================"
echo "--- tailscale ping client-a -> client-b (RTT + path: direct vs DERP) ---"
ssh $SSHOPT "root@${A_IP}" "tailscale ping --c=5 ${B_TS}" 2>&1 | sed 's/^/  /' || echo "  ping failed"
echo "--- iperf3 client-a -> client-b over the Headscale mesh (10s) ---"
IPERF_JSON="$(ssh $SSHOPT "root@${A_IP}" "iperf3 -c ${B_TS} -t 10 -J" 2>/dev/null || echo '{}')"
BPS="$(echo "$IPERF_JSON" | jq -r '.end.sum_received.bits_per_second // empty')"
if [ -n "$BPS" ]; then
  MBPS="$(awk "BEGIN{printf \"%.1f\", ${BPS}/1000000}")"
  echo "  throughput: ${MBPS} Mbps  (received)"
else
  echo "  iperf3 failed; raw: $(echo "$IPERF_JSON" | head -c 200)"
fi
echo "--- tailscale netcheck (client-a: which DERP, latencies) ---"
ssh $SSHOPT "root@${A_IP}" "tailscale netcheck" 2>&1 | sed 's/^/  /' | head -25 || true
echo "========================================================"
log "test complete — trap will tear everything down"
