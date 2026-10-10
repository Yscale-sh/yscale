#!/bin/bash
# yscale:proprietary
# client-cloud-init.sh — a TEST node that joins a per-customer Headscale
# mesh as a tailscale client, for the e2e mesh speed test. Installs
# tailscale + iperf3, joins via --login-server + preauth key, runs an
# iperf3 server daemon so the orchestrator can measure throughput.
#
# NOT a product component — this stands in for a burst/gateway node to
# exercise the real Headscale data plane. Per-box exports are prepended
# by the orchestrator:
#   HS_LOGIN_SERVER  https://<box>          (the Headscale box URL)
#   HS_AUTHKEY       the reusable preauth key minted on the box
#   HS_HOSTNAME      tailnet hostname (e.g. client-a)
#   SELFDESTRUCT_MIN dead-man self-poweroff TTL (default 30)
#
# NOTE: xtrace is intentionally OFF so HS_AUTHKEY never lands in the log.
set -eo pipefail
exec > /var/log/yscale-client-bootstrap.log 2>&1
echo "[client] bootstrap starting $(date -u)"

: "${HS_LOGIN_SERVER:?HS_LOGIN_SERVER required}"
: "${HS_AUTHKEY:?HS_AUTHKEY required}"
: "${HS_HOSTNAME:=client}"
: "${SELFDESTRUCT_MIN:=30}"

# Box-side dead-man switch: poweroff after the TTL so an orphaned test
# node can't bill forever even if the orchestrator's teardown never runs.
if [ "${SELFDESTRUCT_MIN}" -gt 0 ] 2>/dev/null; then
  echo "[client] self-poweroff scheduled in ${SELFDESTRUCT_MIN} min"
  ( sleep $(( SELFDESTRUCT_MIN * 60 )); /sbin/shutdown -h now "client self-destruct" ) &
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends curl ca-certificates iperf3

curl -fsSL https://tailscale.com/install.sh | sh
systemctl enable --now tailscaled
sleep 2

# Join the customer's Headscale mesh. --accept-routes so advertised pod/
# svc routes install (full-mode parity). Key fed via env, not echoed.
tailscale up \
  --login-server="${HS_LOGIN_SERVER}" \
  --authkey="${HS_AUTHKEY}" \
  --hostname="${HS_HOSTNAME}" \
  --accept-routes \
  --reset
unset HS_AUTHKEY

# iperf3 server daemon so the orchestrator can pull throughput numbers.
iperf3 -s -D

TS_IP="$(tailscale ip -4 2>/dev/null || echo '?')"
echo "[client] joined as ${HS_HOSTNAME}, tailnet IP=${TS_IP}"
echo "[client] DONE $(date -u)"
