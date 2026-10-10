#!/usr/bin/env bash
# yscale:proprietary
# localtest.sh — validate the Headscale config + permissions + daemon
# startup + user/preauthkey creation LOCALLY in Docker, with ZERO cloud
# spend. Catches the exact failure class we hit on real boxes (config not
# found, permission/ownership bugs, schema-version mismatch, crash-loop).
#
# What it CAN'T cover: Let's Encrypt HTTP-01 (needs a real public hostname),
# so this runs with TLS disabled and probes plain :8080/health. The live
# smoketest.sh remains the final check for the TLS/DERP path.
#
# Mirrors cloud-init.sh: same Debian 12 base, same .deb install, same
# config heredoc shape, same `headscale` daemon user, same dir perms.
set -euo pipefail

HEADSCALE_VERSION="${HEADSCALE_VERSION:-0.26.1}"
HS_USER="${1:-localtest}"
CTR="hs-localtest-$$"
log() { echo "[localtest] $*" >&2; }

cleanup() { docker rm -f "$CTR" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

# The in-container script: install headscale, write a TLS-OFF config using
# the SAME permission model as cloud-init.sh (dir 755, config 644, state
# dir owned by headscale), start the daemon, wait for /health, then create
# a user + reusable preauthkey exactly like the real bootstrap.
read -r -d '' INNER <<'INNER_EOF' || true
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
# apt-utils + util-linux(setpriv) up front so dpkg's debconf/triggers don't
# warn-then-fail; minimal base image lacks them.
apt-get update -qq
apt-get install -y -qq --no-install-recommends apt-utils >/dev/null 2>&1 || true
apt-get install -y -qq --no-install-recommends ca-certificates curl jq util-linux >/dev/null
curl -fsSL -o /tmp/headscale.deb \
  "https://github.com/juanfont/headscale/releases/download/v${HEADSCALE_VERSION}/headscale_${HEADSCALE_VERSION}_linux_amd64.deb"
# apt-get install of a local .deb can emit triggers warnings on a slim
# image; fall back to dpkg + fix-broken so a benign trigger code != fatal.
apt-get install -y -qq /tmp/headscale.deb >/dev/null 2>&1 || {
  dpkg -i /tmp/headscale.deb >/dev/null 2>&1 || true
  apt-get install -y -qq -f >/dev/null 2>&1 || true
}
command -v headscale >/dev/null || { echo "[ctr] headscale install FAILED"; exit 1; }
echo "[ctr] installed headscale ${HEADSCALE_VERSION}"

# SAME permission model as the fixed cloud-init.sh.
install -d -m 755 /etc/headscale
cat > /etc/headscale/config.yaml <<CFG
server_url: http://127.0.0.1:8080
listen_addr: 0.0.0.0:8080
metrics_listen_addr: 127.0.0.1:9090
grpc_listen_addr: 127.0.0.1:50443
grpc_allow_insecure: false
noise:
  private_key_path: /var/lib/headscale/noise_private.key
prefixes:
  v4: 100.64.0.0/10
  v6: fd7a:115c:a1e0::/48
derp:
  urls: []
  paths: []
  auto_update_enabled: false
  update_frequency: 24h
  server:
    enabled: true
    region_id: 900
    region_code: "yscale"
    region_name: "yscale embedded DERP"
    stun_listen_addr: "0.0.0.0:3478"
    private_key_path: /var/lib/headscale/derp_server_private.key
    automatically_add_embedded_derp_region: true
    ipv4: 127.0.0.1
disable_check_updates: true
ephemeral_node_inactivity_timeout: 30m
database:
  type: sqlite
  sqlite:
    path: /var/lib/headscale/db.sqlite
log:
  level: info
policy:
  mode: database
dns:
  magic_dns: true
  base_domain: yscale.internal
  nameservers:
    global:
      - 1.1.1.1
      - 9.9.9.9
CFG
chmod 644 /etc/headscale/config.yaml
install -d -o headscale -g headscale -m 750 /var/lib/headscale

# Run the daemon as the headscale user (no systemd in container) — this is
# the exact identity that failed to read the config on the real box.
echo "[ctr] starting headscale as user 'headscale' ..."
setpriv --reuid headscale --regid headscale --init-groups headscale serve \
  >/var/log/hs.log 2>&1 &
HSPID=$!

ok=0
for i in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then ok=1; break; fi
  if ! kill -0 "$HSPID" 2>/dev/null; then echo "[ctr] DAEMON DIED early"; break; fi
  sleep 1
done

echo "=== /health ==="
curl -fsS http://127.0.0.1:8080/health 2>&1 || echo "(health failed)"
echo
if [ "$ok" != "1" ]; then
  echo "=== HEADSCALE FAILED TO COME UP — log tail ==="
  tail -30 /var/log/hs.log
  exit 1
fi
echo "[ctr] /health OK"

# Exercise the real bootstrap actions EXACTLY as cloud-init.sh does:
# create user, resolve numeric id (0.26 is ID-based), mint key by id.
headscale users create "${HS_USER}" 2>&1 | sed 's/^/[ctr] /' || echo "[ctr] user exists"
HS_UID="$(headscale users list -o json 2>/dev/null | jq -r --arg n "${HS_USER}" '.[] | select(.name==$n) | .id' | head -1)"
echo "[ctr] resolved user id=${HS_UID}"
[ -n "${HS_UID}" ] || { echo "[ctr] FAIL: could not resolve user id"; exit 1; }
PK="$(headscale preauthkeys create --user "${HS_UID}" --reusable --expiration 720h 2>&1 | tail -1)"
echo "[ctr] preauthkey created: ${PK:0:12}... (len ${#PK})"
[ "${#PK}" -ge 24 ] || { echo "[ctr] FAIL: preauthkey looks wrong (len ${#PK})"; exit 1; }

# Validate the rendered ACL policy loads (policy v2 / tests block).
echo "[ctr] ACL policy v2 + tests would apply via: headscale policy set (skipped: needs rendered file mount)"
echo "[ctr] ALL LOCAL CHECKS PASSED"
INNER_EOF

# Force amd64: the headscale .deb is amd64 and so is the Linode burst image.
# On Apple Silicon (arm64) this runs under emulation — slower but it mirrors
# the real target arch (without it, dpkg rejects the .deb as wrong-arch).
log "starting Debian 12 (linux/amd64, matches Linode image) ..."
docker run -d --platform linux/amd64 --name "$CTR" --cap-add=NET_ADMIN debian:12 sleep 600 >/dev/null

# setpriv is in util-linux; ensure present.
docker exec "$CTR" bash -c "command -v setpriv >/dev/null || (apt-get update -qq && apt-get install -y -qq util-linux >/dev/null)"

log "running in-container bring-up (install + config + daemon + preauthkey) ..."
docker exec -e HEADSCALE_VERSION="$HEADSCALE_VERSION" -e HS_USER="$HS_USER" "$CTR" bash -c "$INNER"
rc=$?

if [ $rc -eq 0 ]; then
  log "LOCAL TEST PASSED ✓  (config parses, daemon runs as headscale user, preauthkey mints)"
else
  log "LOCAL TEST FAILED (rc=$rc) — see container log above"
fi
exit $rc
