#!/usr/bin/env bash
# yscale:proprietary
# provision.sh — stand up one Headscale + embedded-DERP node on Linode.
# Phase 0: one shared instance to de-risk the integration. Later this
# becomes per-customer (one VM per customer, HS_USER = customer id).
#
# Reads LINODE_TOKEN from env (or the yscale-cloud pod). Tags the VM
# `yscale-headscale` (NOT yscale-burst) so the burst TTL reaper +
# leak-sweep never touch it.
#
# Usage: LINODE_TOKEN=... ./provision.sh [user] [region] [type]
set -euo pipefail

HS_USER="${1:-default}"
REGION="${2:-us-ord}"
# Smallest Linode (1GB shared, ~$0.0075/hr). Headscale + embedded DERP is
# light (sqlite + a relay), so the nanode is plenty and keeps the per-customer
# DERP as small + cheap as possible. Override via $3 for a bigger box.
TYPE="${3:-g6-nanode-1}"
LABEL="headscale-${HS_USER}-$(date +%s)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

: "${LINODE_TOKEN:?set LINODE_TOKEN}"

log() { echo "[provision-headscale] $*" >&2; }

# ── Render the per-customer FULL-MODE ACL (acls.hujson) NOW so it can ride
# into the box via user_data and be applied on-boot by cloud-init (the box
# has no inbound SSH in prod, so the policy must be self-applied). The render
# MUST leave no placeholder behind: <CUSTOMER> scopes the admin group to this
# tenant; <POD_CIDR>/<SVC_CIDR> bound the routes the gateway may auto-advertise
# (full mode needs BOTH — pod CIDR for cross-cluster pod-to-pod, service CIDR
# for ClusterIP). Defaults match k3s (10.42/16 pod, 10.43/16 svc); override via
# env for other clusters. The tag NAMES in the template (tag:yscale,
# tag:yscale-gateway) MUST match what the clients advertise — see acls.hujson.
POD_CIDR="${POD_CIDR:-10.42.0.0/16}"
SVC_CIDR="${SVC_CIDR:-10.43.0.0/16}"
# Node LAN CIDR: the gateway advertises this too so bursts can reach the
# apiserver by node IP to complete kubelet bootstrap. MUST match the third
# entry in gateway.advertiseRoutes (deploy/helm/yscale-agent/values.yaml,
# 10.0.0.0/24 on the homelab). If the box doesn't auto-approve it, the burst
# never reaches the apiserver and its Node never joins.
NODE_CIDR="${NODE_CIDR:-10.0.0.0/24}"
ACL_TEMPLATE="$SCRIPT_DIR/acls.hujson"
ACL_B64=""
if [ -f "$ACL_TEMPLATE" ]; then
  ACL_RENDERED="/tmp/acls-${HS_USER}.hujson"
  sed -e "s|<CUSTOMER>|${HS_USER}|g" \
      -e "s|<POD_CIDR>|${POD_CIDR}|g" \
      -e "s|<SVC_CIDR>|${SVC_CIDR}|g" \
      -e "s|<NODE_CIDR>|${NODE_CIDR}|g" \
    "$ACL_TEMPLATE" > "$ACL_RENDERED"
  if grep -q -e '<CUSTOMER>' -e '<POD_CIDR>' -e '<SVC_CIDR>' -e '<NODE_CIDR>' "$ACL_RENDERED"; then
    log "FATAL: rendered ACL $ACL_RENDERED still has <CUSTOMER>/<POD_CIDR>/<SVC_CIDR>/<NODE_CIDR> placeholders"
    exit 1
  fi
  # Minify before embedding: strip // comment lines + blank lines. Linode caps
  # decoded user_data at 16384 bytes and cloud-init.sh is already ~9KB, so the
  # verbose (mostly-comments) ACL would tip it over. headscale accepts the
  # comment-free HuJSON (== plain JSON) identically. ~4.1KB -> ~0.9KB.
  ACL_MIN="/tmp/acls-${HS_USER}.min.hujson"
  sed -e 's|^[[:space:]]*//.*$||' -e '/^[[:space:]]*$/d' "$ACL_RENDERED" > "$ACL_MIN"
  ACL_B64="$(base64 < "$ACL_MIN" | tr -d '\n')"
  log "rendered+minified FULL-MODE ACL (pod=$POD_CIDR svc=$SVC_CIDR, $(wc -c < "$ACL_MIN") bytes) -> applied on-box by cloud-init"
else
  log "WARN: $ACL_TEMPLATE not found — box will boot with NO policy (tag:yscale joins will fail)"
fi

# Prepend per-box exports to the cloud-init body. HS_SELFDESTRUCT_MIN is
# OPTIONAL and only set for smoke-test boxes (via smoketest.sh): when
# present, cloud-init schedules a self-poweroff after that many minutes —
# a box-side dead-man switch so an orphaned TEST box can't bill forever.
# A real production box is provisioned WITHOUT it and never self-destructs.
USERDATA="$(printf '#!/bin/bash\nexport HS_USER=%q\nexport HS_SELFDESTRUCT_MIN=%q\nexport HS_ACL_B64=%q\n' \
  "$HS_USER" "${HS_SELFDESTRUCT_MIN:-}" "${ACL_B64}"; tail -n +2 "$SCRIPT_DIR/cloud-init.sh")"
USERDATA_B64="$(printf '%s' "$USERDATA" | base64 | tr -d '\n')"
# SIGPIPE-safe random password: `tr -dc ... | head -c 32` makes head close
# the pipe early, sending tr SIGPIPE → exit 141 → `set -o pipefail` aborts
# the whole script. Read a fixed slice of /dev/urandom instead (no pipe
# that closes early), then map to the alphabet.
PASS="$(LC_ALL=C tr -dc 'A-Za-z0-9' < <(head -c 256 /dev/urandom) | cut -c1-32)"

# SMOKE_PUBKEY (optional, smoke tests only) injects an SSH key so the test
# wrapper can pull /var/log on failure. Production boxes are provisioned
# WITHOUT it and have no SSH key — access is over the tailnet only.
AUTH_KEYS_JSON=""
if [ -n "${SMOKE_PUBKEY:-}" ]; then
  AUTH_KEYS_JSON="$(printf '"authorized_keys": [%s],' "$(printf '%s' "$SMOKE_PUBKEY" | jq -R .)")"
fi

# --- Cloud Firewall: protect the box from first boot (network-edge, independent
# of the guest). Default-DROP inbound except 443 (headscale control + DERP-over-
# HTTPS), 80 (Let's Encrypt HTTP-01), 3478+41641/udp (DERP STUN + tailscale), and
# 22 (key-only SSH — smoke boxes carry SMOKE_PUBKEY; prod boxes get no key).
# Idempotent by label. Best-effort: a firewall hiccup must not block provisioning.
HS_FW_LABEL="yscale-headscale-fw"
HS_FW_ID="$(curl -sS -H "Authorization: Bearer $LINODE_TOKEN" "https://api.linode.com/v4/networking/firewalls" 2>/dev/null \
  | jq -r --arg L "$HS_FW_LABEL" '.data[]? | select(.label==$L) | .id' | head -1)"
if [ -z "$HS_FW_ID" ] || [ "$HS_FW_ID" = "null" ]; then
  HS_FW_ID="$(curl -sS -X POST -H "Authorization: Bearer $LINODE_TOKEN" -H "Content-Type: application/json" \
    -d '{"label":"yscale-headscale-fw","tags":["yscale-headscale"],"rules":{"inbound_policy":"DROP","outbound_policy":"ACCEPT","inbound":[
      {"label":"https","action":"ACCEPT","protocol":"TCP","ports":"443","addresses":{"ipv4":["0.0.0.0/0"],"ipv6":["::/0"]}},
      {"label":"http-le","action":"ACCEPT","protocol":"TCP","ports":"80","addresses":{"ipv4":["0.0.0.0/0"],"ipv6":["::/0"]}},
      {"label":"derp-udp","action":"ACCEPT","protocol":"UDP","ports":"3478,41641","addresses":{"ipv4":["0.0.0.0/0"],"ipv6":["::/0"]}},
      {"label":"ssh","action":"ACCEPT","protocol":"TCP","ports":"22","addresses":{"ipv4":["0.0.0.0/0"],"ipv6":["::/0"]}}]}}' \
    "https://api.linode.com/v4/networking/firewalls" 2>/dev/null | jq -r '.id')"
  log "created Cloud Firewall $HS_FW_LABEL (id=$HS_FW_ID)"
else
  log "reusing Cloud Firewall $HS_FW_LABEL (id=$HS_FW_ID)"
fi
FW_JSON=""
[ -n "$HS_FW_ID" ] && [ "$HS_FW_ID" != "null" ] && FW_JSON="\"firewall_id\": $HS_FW_ID,"

log "creating Linode $LABEL ($TYPE, $REGION) for Headscale user '$HS_USER'..."
RESP="$(curl -sS -X POST -H "Authorization: Bearer $LINODE_TOKEN" -H "Content-Type: application/json" \
  -d "{
    \"label\": \"$LABEL\",
    \"region\": \"$REGION\",
    \"type\": \"$TYPE\",
    \"image\": \"linode/debian12\",
    \"root_pass\": \"$PASS\",
    ${AUTH_KEYS_JSON}
    ${FW_JSON}
    \"tags\": [\"yscale-headscale\"],
    \"metadata\": {\"user_data\": \"$USERDATA_B64\"}
  }" \
  https://api.linode.com/v4/linode/instances)"
LID="$(echo "$RESP" | jq -r '.id')"
if [ "$LID" = "null" ] || [ -z "$LID" ]; then
  log "FATAL: create failed: $RESP"; exit 1
fi
log "Linode id=$LID — waiting for running + cloud-init..."

for i in $(seq 1 30); do
  sleep 10
  STATE="$(curl -sS -H "Authorization: Bearer $LINODE_TOKEN" \
    "https://api.linode.com/v4/linode/instances/$LID" | jq -r '.status')"
  [ "$STATE" = "running" ] && break
  log "  t=${i}0s status=$STATE"
done
IP="$(curl -sS -H "Authorization: Bearer $LINODE_TOKEN" \
  "https://api.linode.com/v4/linode/instances/$LID" | jq -r '.ipv4[0]')"
HOSTNAME_FQDN="$(echo "$IP" | tr '.' '-').ip.linodeusercontent.com"
log "Linode running: id=$LID ip=$IP"
log "Headscale login-server (once cloud-init + LE finish, ~2-3 min): https://$HOSTNAME_FQDN"
log "Poll readiness:  curl -fsS https://$HOSTNAME_FQDN/health"
# The FULL-MODE ACL was rendered + embedded into user_data above; cloud-init
# applies it on-box with `headscale policy set` once the user exists. No
# operator step and no inbound SSH required.

echo "$LID $IP $HOSTNAME_FQDN"
