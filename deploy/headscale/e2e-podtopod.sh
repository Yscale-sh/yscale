#!/usr/bin/env bash
# yscale:proprietary
# e2e-podtopod.sh — REAL pod-to-pod data-plane test over a per-customer
# Headscale mesh, WITHOUT a burst (two single-node k3s clusters stand in).
#
# Headscale box + node-a (pod CIDR 10.42/16) + node-b (pod CIDR 10.44/16),
# both joined to the box's tailnet advertising their CIDRs. We approve the
# routes on the box, deploy nginx on A + a probe on B, and curl/iperf
# pod-on-B -> pod-on-A ACROSS THE HEADSCALE MESH.
#
# All boxes tagged yscale-headscale -> one trap-driven teardown sweep on
# ANY exit; each box also self-powers-off as a backstop.
#
# Usage: LINODE_TOKEN=... SMOKE_SSHKEY=/path/key ./e2e-podtopod.sh [regionA] [regionB]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
: "${LINODE_TOKEN:?set LINODE_TOKEN}"
: "${SMOKE_SSHKEY:?set SMOKE_SSHKEY (ssh private key path; .pub injected)}"
export LINODE_TOKEN
REGION_A="${1:-us-ord}"
REGION_B="${2:-us-east}"
TYPE="g6-standard-2"          # 4GB: k3s + flannel + pods need headroom
SELFDESTRUCT_MIN="${SELFDESTRUCT_MIN:-40}"
export HS_SELFDESTRUCT_MIN="$SELFDESTRUCT_MIN"
export SMOKE_SSHKEY
PUBKEY="$(cat "${SMOKE_SSHKEY}.pub")"
# provision.sh injects this into the Headscale box's authorized_keys so we
# can read the preauth/api key back. MUST be exported before provision.sh.
export SMOKE_PUBKEY="$PUBKEY"
SSHOPT="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -i ${SMOKE_SSHKEY}"

A_CIDR="10.42.0.0/16"; A_SVC="10.43.0.0/16"
B_CIDR="10.44.0.0/16"; B_SVC="10.45.0.0/16"

log() { echo "[p2p] $*" >&2; }
api() { curl -sS -H "Authorization: Bearer $LINODE_TOKEN" "$@"; }

teardown() {
  local rc=$?
  log "tearing down ALL yscale-headscale nodes (exit $rc) ..."
  bash "$SCRIPT_DIR/deprovision.sh" --all 2>&1 | sed 's/^/[p2p]   /' || true
  log "teardown done"
}
trap teardown EXIT INT TERM HUP

# create_node <name> <region> <login> <authkey> <cluster_cidr> <svc_cidr> -> "LID IP"
create_node() {
  local name="$1" region="$2" login="$3" authkey="$4" ccidr="$5" scidr="$6"
  local userdata pass resp lid ip
  userdata="$(printf '#!/bin/bash\nexport HS_LOGIN_SERVER=%q\nexport HS_AUTHKEY=%q\nexport HS_HOSTNAME=%q\nexport CLUSTER_CIDR=%q\nexport SVC_CIDR=%q\nexport SELFDESTRUCT_MIN=%q\n' \
    "$login" "$authkey" "$name" "$ccidr" "$scidr" "$SELFDESTRUCT_MIN"; tail -n +2 "$SCRIPT_DIR/node-k3s-cloud-init.sh")"
  userdata="$(printf '%s' "$userdata" | base64 | tr -d '\n')"
  pass="$(LC_ALL=C tr -dc 'A-Za-z0-9' < <(head -c 256 /dev/urandom) | cut -c1-32)"
  resp="$(api -X POST -H "Content-Type: application/json" -d "{
    \"label\": \"headscale-${name}-$(date +%s)\",
    \"region\": \"$region\", \"type\": \"$TYPE\", \"image\": \"linode/debian12\",
    \"root_pass\": \"$pass\",
    \"authorized_keys\": [$(printf '%s' "$PUBKEY" | jq -R .)],
    \"tags\": [\"yscale-headscale\"],
    \"metadata\": {\"user_data\": \"$userdata\"}
  }" https://api.linode.com/v4/linode/instances)"
  lid="$(echo "$resp" | jq -r '.id')"
  [ "$lid" != "null" ] && [ -n "$lid" ] || { log "FATAL: $name create failed: $resp"; return 1; }
  for _ in $(seq 1 30); do
    sleep 8; ip="$(api "https://api.linode.com/v4/linode/instances/$lid" | jq -r '.ipv4[0]')"
    [ "$ip" != "null" ] && [ -n "$ip" ] && break
  done
  echo "$lid $ip"
}

# ---- 1. Headscale box ----
log "provisioning Headscale box ($REGION_A) ..."
OUT="$(bash "$SCRIPT_DIR/provision.sh" smoketest "$REGION_A" g6-standard-1)"
read -r BOX_LID BOX_IP BOX_HOST <<<"$(echo "$OUT" | tail -1)"
log "box LID=$BOX_LID host=$BOX_HOST — waiting health ..."
for i in $(seq 1 40); do curl -fsS --max-time 8 "https://$BOX_HOST/health" >/dev/null 2>&1 && { log "box healthy (~$((i*15))s)"; break; }; sleep 15; done
curl -fsS --max-time 8 "https://$BOX_HOST/health" >/dev/null 2>&1 || { log "FATAL: box never healthy"; exit 1; }

# ---- 2. preauth key off the box ----
for _ in $(seq 1 12); do AUTHKEY="$(ssh $SSHOPT "root@${BOX_IP}" 'cat /var/lib/headscale/bootstrap-preauthkey 2>/dev/null' 2>/dev/null || true)"; [ -n "$AUTHKEY" ] && break; sleep 5; done
[ -n "$AUTHKEY" ] || { log "FATAL: no preauth key"; exit 1; }
LOGIN="https://${BOX_HOST}"; log "got preauth key (len ${#AUTHKEY})"

# ---- 3. two k3s nodes ----
log "provisioning node-a ($REGION_A, $A_CIDR) + node-b ($REGION_B, $B_CIDR) — k3s install takes a few min ..."
read -r A_LID A_IP <<<"$(create_node node-a "$REGION_A" "$LOGIN" "$AUTHKEY" "$A_CIDR" "$A_SVC")"
read -r B_LID B_IP <<<"$(create_node node-b "$REGION_B" "$LOGIN" "$AUTHKEY" "$B_CIDR" "$B_SVC")"
log "node-a LID=$A_LID IP=$A_IP ; node-b LID=$B_LID IP=$B_IP"

# ---- 4. wait for both to register, then APPROVE their advertised routes ----
log "waiting for both nodes to register + advertise routes ..."
for _ in $(seq 1 48); do
  NJSON="$(ssh $SSHOPT "root@${BOX_IP}" 'headscale nodes list -o json 2>/dev/null' 2>/dev/null || echo '[]')"
  na="$(echo "$NJSON" | jq -r '.[]|select(.given_name=="node-a" or .name=="node-a")|.id' | head -1)"
  nb="$(echo "$NJSON" | jq -r '.[]|select(.given_name=="node-b" or .name=="node-b")|.id' | head -1)"
  [ -n "$na" ] && [ -n "$nb" ] && break
  sleep 10
done
[ -n "${na:-}" ] && [ -n "${nb:-}" ] || { log "FATAL: nodes did not register (a=${na:-} b=${nb:-})"; exit 1; }
log "registered node-a id=$na, node-b id=$nb — approving routes ..."
# Headscale 0.26 route approval is per-node. Try the modern CLI; capture errors.
ssh $SSHOPT "root@${BOX_IP}" "headscale nodes approve-routes -i $na -r ${A_CIDR},${A_SVC} 2>&1; headscale nodes approve-routes -i $nb -r ${B_CIDR},${B_SVC} 2>&1" 2>&1 | sed 's/^/[box]   /' || true
sleep 5
echo "--- approved routes on box ---"
ssh $SSHOPT "root@${BOX_IP}" 'headscale nodes list 2>&1 | head -20; echo "--- routes ---"; headscale routes list 2>&1 | head -20 || true' 2>&1 | sed 's/^/[box]   /' || true

# tailnet IPs
A_TS="$(echo "$NJSON" | jq -r '.[]|select(.given_name=="node-a" or .name=="node-a")|.ip_addresses[]?' | grep -E '^100\.' | head -1)"
B_TS="$(echo "$NJSON" | jq -r '.[]|select(.given_name=="node-b" or .name=="node-b")|.ip_addresses[]?' | grep -E '^100\.' | head -1)"
log "tailnet IPs: node-a=$A_TS node-b=$B_TS"

# ---- 5. wait k3s ready on both, deploy workloads ----
log "waiting for k3s Ready on both nodes ..."
for _ in $(seq 1 60); do
  ra="$(ssh $SSHOPT "root@${A_IP}" 'k3s kubectl get nodes --no-headers 2>/dev/null | grep -c " Ready "' 2>/dev/null || echo 0)"
  rb="$(ssh $SSHOPT "root@${B_IP}" 'k3s kubectl get nodes --no-headers 2>/dev/null | grep -c " Ready "' 2>/dev/null || echo 0)"
  [ "$ra" = "1" ] && [ "$rb" = "1" ] && break
  sleep 10
done
log "k3s ready: a=$ra b=$rb"

log "deploying nginx + iperf3 server on node-a ..."
ssh $SSHOPT "root@${A_IP}" 'k3s kubectl run nginx --image=nginx:alpine --port=80 2>&1; k3s kubectl run iperf3 --image=networkstatic/iperf3 --port=5201 -- -s 2>&1; for i in $(seq 1 30); do k3s kubectl get pod nginx iperf3 -o jsonpath="{.items[*].status.phase}" 2>/dev/null | grep -q "Running Running" && break; sleep 5; done; k3s kubectl get pods -o wide' 2>&1 | sed 's/^/[node-a]   /' || true
A_POD_IP="$(ssh $SSHOPT "root@${A_IP}" "k3s kubectl get pod nginx -o jsonpath='{.status.podIP}' 2>/dev/null" 2>/dev/null || true)"
A_IPERF_IP="$(ssh $SSHOPT "root@${A_IP}" "k3s kubectl get pod iperf3 -o jsonpath='{.status.podIP}' 2>/dev/null" 2>/dev/null || true)"
log "node-a pod IPs: nginx=$A_POD_IP iperf3=$A_IPERF_IP"
[ -n "$A_POD_IP" ] || { log "FATAL: no nginx pod IP on node-a"; exit 1; }

# ---- 6. THE TEST: pod on node-b -> pod on node-a, across the Headscale mesh ----
echo "================ POD-TO-POD OVER HEADSCALE ================"
echo "--- node-b host -> node-a pod ${A_POD_IP} (route via tailscale0) ---"
ssh $SSHOPT "root@${B_IP}" "ip route get ${A_POD_IP%/*} 2>&1 | head -1; curl -s --max-time 12 -o /dev/null -w 'host->pod HTTP=%{http_code} time=%{time_total}s\n' http://${A_POD_IP}/ 2>&1" 2>&1 | sed 's/^/  /' || echo "  host->pod failed"
echo "--- node-b POD -> node-a POD ${A_POD_IP} (true pod-to-pod across mesh) ---"
ssh $SSHOPT "root@${B_IP}" "k3s kubectl run probe --image=curlimages/curl --restart=Never --rm -i --timeout=90s -- curl -s --max-time 15 -o /dev/null -w 'pod->pod HTTP=%{http_code} time=%{time_total}s\n' http://${A_POD_IP}/ 2>&1" 2>&1 | sed 's/^/  /' || echo "  pod->pod probe failed"
echo "--- throughput: node-b pod -> node-a iperf3 pod ${A_IPERF_IP} (10s) ---"
if [ -n "$A_IPERF_IP" ]; then
  ssh $SSHOPT "root@${B_IP}" "k3s kubectl run iperfc --image=networkstatic/iperf3 --restart=Never --rm -i --timeout=120s -- -c ${A_IPERF_IP} -t 10 2>&1 | tail -8" 2>&1 | sed 's/^/  /' || echo "  iperf3 failed"
fi
echo "=========================================================="
log "pod-to-pod test complete — trap will tear everything down"
