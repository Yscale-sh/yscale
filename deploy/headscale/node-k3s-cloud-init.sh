#!/bin/bash
# yscale:proprietary
# node-k3s-cloud-init.sh — a single-node k3s cluster that joins a
# per-customer Headscale mesh and advertises its pod+svc CIDR, for the
# pod-to-pod data-plane test. This is the "without a burst" stand-in:
# two of these advertise different pod CIDRs into the same Headscale
# tailnet, and we test pod-on-A -> pod-on-B across the mesh — the real
# subnet-router + ip_forward joined-CNI mechanism, minus the kubelet
# cross-join ceremony.
#
# Per-node exports prepended by the orchestrator:
#   HS_LOGIN_SERVER   https://<box>
#   HS_AUTHKEY        reusable preauth key minted on the box
#   HS_HOSTNAME       tailnet hostname (node-a / node-b)
#   CLUSTER_CIDR      k3s pod CIDR (e.g. 10.42.0.0/16)
#   SVC_CIDR          k3s service CIDR (e.g. 10.43.0.0/16)
#   SELFDESTRUCT_MIN  dead-man self-poweroff TTL (default 40)
#
# xtrace OFF so HS_AUTHKEY never lands in the log.
set -eo pipefail
exec > /var/log/yscale-node-bootstrap.log 2>&1
echo "[node] bootstrap starting $(date -u)"

: "${HS_LOGIN_SERVER:?HS_LOGIN_SERVER required}"
: "${HS_AUTHKEY:?HS_AUTHKEY required}"
: "${HS_HOSTNAME:=node}"
: "${CLUSTER_CIDR:=10.42.0.0/16}"
: "${SVC_CIDR:=10.43.0.0/16}"
: "${SELFDESTRUCT_MIN:=40}"

# Dead-man self-poweroff backstop.
if [ "${SELFDESTRUCT_MIN}" -gt 0 ] 2>/dev/null; then
  echo "[node] self-poweroff scheduled in ${SELFDESTRUCT_MIN} min"
  ( sleep $(( SELFDESTRUCT_MIN * 60 )); /sbin/shutdown -h now "node self-destruct" ) &
fi

sysctl -w net.ipv4.ip_forward=1 >/dev/null 2>&1 || true

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends curl ca-certificates iproute2 iperf3

# --- Tailscale: join the customer's Headscale mesh, advertise THIS
# cluster's pod+svc CIDR, accept the other node's routes. ip_forward is
# on, so packets arriving for our CIDR get forwarded onto flannel. ---
curl -fsSL https://tailscale.com/install.sh | sh
systemctl enable --now tailscaled
sleep 2
tailscale up \
  --login-server="${HS_LOGIN_SERVER}" \
  --authkey="${HS_AUTHKEY}" \
  --hostname="${HS_HOSTNAME}" \
  --advertise-routes="${CLUSTER_CIDR},${SVC_CIDR}" \
  --accept-routes \
  --reset
unset HS_AUTHKEY
TS_IP="$(tailscale ip -4 2>/dev/null || echo '?')"
echo "[node] tailscale up: ${TS_IP} advertising ${CLUSTER_CIDR},${SVC_CIDR}"

# --- k3s single-node with the assigned pod/svc CIDR. Default flannel
# CNI. --node-ip stays the box's private/public IP; pod traffic to the
# OTHER cluster's CIDR routes out via tailscale0 (accept-routes installed
# it) and the remote node forwards onto its flannel. ---
curl -fsSL https://get.k3s.io | sh -s - \
  --cluster-cidr="${CLUSTER_CIDR}" \
  --service-cidr="${SVC_CIDR}" \
  --disable=traefik,servicelb \
  --write-kubeconfig-mode=644
echo "[node] k3s installed; waiting for node Ready..."
for _ in $(seq 1 60); do
  k3s kubectl get nodes 2>/dev/null | grep -q ' Ready ' && break
  sleep 5
done
k3s kubectl get nodes -o wide 2>&1 | sed 's/^/[node]   /' || true
echo "[node] DONE $(date -u)"
