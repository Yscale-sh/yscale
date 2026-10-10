#!/bin/bash
# yscale:proprietary
# Headscale + embedded-DERP node bootstrap (Phase 0 of the per-customer
# control-plane plan). Runs as Linode cloud-init user-data on a stock
# Debian 12 VM. Self-configures TLS via the Linode-provided hostname
# (<ip-dashed>.ip.linodeusercontent.com), which has a valid forward A
# record so Let's Encrypt HTTP-01 works with no external DNS.
#
# One of these VMs per customer = that customer's own Headscale control
# plane + their own DERP relay + their own DERPMap. Network-level tenant
# isolation by construction.
#
# Per-instance values are prepended as shell exports by the provisioner:
#   HS_USER  — the Headscale user (namespace) to pre-create, e.g. "acme"
set -eo pipefail

# Keep the operator ops mesh separate from the tenant coordinator installed
# below. Never let a missing setting select Tailscale's hosted control plane.
yscale_ops_join() {
  case "${HS_OPS_LOGIN_SERVER:-}" in
    https://?*) ;;
    *) echo "[headscale] ops handoff requires an explicit HTTPS HS_OPS_LOGIN_SERVER" >&2; return 1 ;;
  esac
  [ -n "${HS_OPS_AUTHKEY:-}" ] || return 1
  # Ephemeral membership is chosen when Headscale mints the auth key, not via
  # a tailscale up flag. Leave the coordination box's resolver unchanged.
  tailscale up --login-server="${HS_OPS_LOGIN_SERVER}" \
    --auth-key="${HS_OPS_AUTHKEY}" --accept-dns=false --timeout=60s
}

exec > /var/log/headscale-bootstrap.log 2>&1
echo "[headscale] bootstrap starting $(date -u)"

: "${HS_USER:=default}"

# Box-side dead-man switch (smoke tests only). When HS_SELFDESTRUCT_MIN is
# a positive integer, schedule a self-poweroff after that many minutes so
# an ORPHANED test box (host died mid-run, trap never fired) stops compute
# billing on its own — no external action needed. Production boxes are
# provisioned WITHOUT this var and never self-destruct. `shutdown -h` stops
# compute billing immediately; the (cheap) disk husk is reaped later by
# deprovision.sh --all. We do NOT put a Linode token on the box, so the box
# cannot delete itself — poweroff is the safe, tokenless dead-man action.
if [ -n "${HS_SELFDESTRUCT_MIN:-}" ] && [ "${HS_SELFDESTRUCT_MIN}" -gt 0 ] 2>/dev/null; then
  echo "[headscale] SMOKE-TEST box: scheduling self-poweroff in ${HS_SELFDESTRUCT_MIN} min"
  ( sleep $(( HS_SELFDESTRUCT_MIN * 60 )); /sbin/shutdown -h now "yscale headscale smoke-test self-destruct" ) &
fi
# 0.26+ is REQUIRED for ACL policy v2 — the `tests` block in acls.hujson
# (load-time, fail-closed assertions that cap yscale's reach) only exists
# in policy v2. 0.23.0 would silently ignore `tests`. Pinned to the 0.26
# line; bump deliberately after re-verifying the v1 API JSON shapes the
# central mesh client (central/internal/mesh/headscale.go) depends on.
# Policy v2 also tightened user references (usernames need an '@' suffix).
HEADSCALE_VERSION="0.26.1"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl jq

# SSH hardening (key-only). Smoke boxes carry SMOKE_PUBKEY for debug; prod boxes
# get no key and are tailnet-only. Either way: never accept passwords, no root
# password login. Pairs with the Cloud Firewall attached at create (provision.sh).
mkdir -p /etc/ssh/sshd_config.d
cat > /etc/ssh/sshd_config.d/10-yscale-hardening.conf <<'SSHEOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin prohibit-password
SSHEOF
systemctl reload ssh 2>/dev/null || systemctl reload sshd 2>/dev/null || true

# Public IP + Linode-provided hostname for TLS (forward A record exists,
# so Let's Encrypt HTTP-01 validates without us owning DNS).
PUBIP="$(curl -fsS4 https://api.ipify.org || curl -fsS4 ifconfig.me)"
HOSTNAME_FQDN="$(echo "$PUBIP" | tr '.' '-').ip.linodeusercontent.com"
echo "[headscale] public IP=$PUBIP hostname=$HOSTNAME_FQDN"

# Install Headscale from the official .deb.
ARCH=amd64
curl -fsSL -o /tmp/headscale.deb \
  "https://github.com/juanfont/headscale/releases/download/v${HEADSCALE_VERSION}/headscale_${HEADSCALE_VERSION}_linux_${ARCH}.deb"
apt-get install -y /tmp/headscale.deb

# Config: serve control on :443 with Let's Encrypt; run the EMBEDDED
# DERP server so this box is also the customer's relay. No upstream
# Tailscale DERP map — clients only ever use our relay. Schema = 0.26.x.
#
# PERMISSIONS ARE LOAD-BEARING: the headscale .deb runs the daemon as the
# `headscale` system user (NOT root). So /etc/headscale must be traversable
# + the config world-readable, and /var/lib/headscale must be OWNED by the
# headscale user (it writes db.sqlite + the noise/derp keys there). A
# root:root mode-750 /etc/headscale made the daemon report
# "Config File not found" and crash-loop (diagnosed via smoketest SSH log
# capture, 2026-05-30). 755 dir + 644 file + chown'd state dir fixes it.
install -d -m 755 /etc/headscale
cat > /etc/headscale/config.yaml <<CFG
server_url: https://${HOSTNAME_FQDN}:443
listen_addr: 0.0.0.0:443
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
    ipv4: ${PUBIP}

disable_check_updates: true
ephemeral_node_inactivity_timeout: 30m

database:
  type: sqlite
  sqlite:
    path: /var/lib/headscale/db.sqlite

tls_letsencrypt_hostname: ${HOSTNAME_FQDN}
tls_letsencrypt_challenge_type: HTTP-01
tls_letsencrypt_cache_dir: /var/lib/headscale/cache

log:
  level: info

# Policy in DB mode for v0 bring-up. File-mode (tamper-resistant ACL, see
# acls.hujson) is the next hardening step.
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

# State dir MUST be owned by the headscale daemon user. The package created
# it as headscale:headscale; re-assert (don't clobber to root:root).
install -d -o headscale -g headscale -m 750 /var/lib/headscale

systemctl enable --now headscale
echo "[headscale] waiting for service + LE cert..."
for i in $(seq 1 60); do
  if curl -fsS "https://${HOSTNAME_FQDN}/health" >/dev/null 2>&1; then
    echo "[headscale] healthy at iter=$i (~$((i*5))s)"; break
  fi
  sleep 5
done

# Pre-create the customer user + a reusable pre-auth key central can hand
# to agents/gateways/bursts. Written to a file the provisioner reads via
# `linode-cli` / SSH, or central reads via the gRPC API later.
#
# Headscale 0.26 CHANGED preauthkey to be ID-based: `--user` now wants the
# numeric user id, NOT the name (`--user acme` fails "user not found").
# So: create the user, then resolve its id from `users list -o json`, then
# mint the key against that id. (Caught by deploy/headscale/localtest.sh.)
headscale users create "${HS_USER}" 2>/dev/null || echo "[headscale] user ${HS_USER} exists"
HS_UID="$(headscale users list -o json 2>/dev/null | jq -r --arg n "${HS_USER}" '.[] | select(.name==$n) | .id' | head -1)"
if [ -z "${HS_UID}" ]; then
  echo "[headscale] FATAL: could not resolve user id for ${HS_USER}"; exit 1
fi

# Apply the FULL-MODE ACL policy so the tags the clients advertise
# (tag:yscale / tag:yscale-gateway) are owned and node registration is
# authorized. Without a policy, headscale 0.26 leaves those tags unowned and
# every `tailscale up --advertise-tags=tag:yscale` join is rejected (the box
# just never gains nodes). The policy's group member is "<user>@", so it must
# be applied AFTER the user exists. Do NOT abort the box if this fails — the
# api-key/preauthkey readback files below must still be written so the
# provisioner can surface the box; a bad policy is logged loudly instead.
if [ -n "${HS_ACL_B64}" ]; then
  printf '%s' "${HS_ACL_B64}" | base64 -d > /etc/headscale/acl.hujson 2>/dev/null || \
    echo "[headscale] WARN: failed to decode HS_ACL_B64"
  if headscale policy check -f /etc/headscale/acl.hujson >/dev/null 2>&1; then
    if headscale policy set -f /etc/headscale/acl.hujson >/dev/null 2>&1; then
      echo "[headscale] ACL policy applied (tag:yscale + tag:yscale-gateway owned by ${HS_USER}@)"
    else
      echo "[headscale] FATAL-NONABORT: 'headscale policy set' failed; node joins will be UNAUTHORIZED"
    fi
  else
    echo "[headscale] FATAL-NONABORT: rendered ACL failed 'headscale policy check'; not applying"
  fi
else
  echo "[headscale] WARN: no HS_ACL_B64 provided; box has NO policy — tag:yscale joins will fail"
fi
PREAUTH="$(headscale preauthkeys create --user "${HS_UID}" --reusable --expiration 720h 2>/dev/null | tail -1)"
if [ -z "${PREAUTH}" ]; then
  echo "[headscale] FATAL: preauthkey creation returned empty for user id ${HS_UID}"; exit 1
fi
# NEVER echo the reusable 720h pre-auth key into this persistent bootstrap
# log — it is a long-lived join secret. The file below is the single source
# of truth; it is written 0600 so only root can read it.
echo "[headscale] user=${HS_USER} preauthkey written to /var/lib/headscale/bootstrap-preauthkey"
( umask 077; printf '%s' "${PREAUTH}" > /var/lib/headscale/bootstrap-preauthkey )
( umask 077; printf '%s' "${HOSTNAME_FQDN}" > /var/lib/headscale/server-hostname )
chmod 600 /var/lib/headscale/bootstrap-preauthkey /var/lib/headscale/server-hostname

# --- API key for central's HTTPS control path (the "bridge") ---
# central/internal/mesh.Headscale talks to this box's v1 HTTP API with a
# Headscale API key as a bearer token, so it can mint per-burst preauth
# keys + list/delete nodes WITHOUT SSH. This is the credential that goes
# into state.MeshEndpoint.APIKey. 0600 file, never logged. Registration
# (provision.sh / central) reads it back once, then it lives in state.
HS_APIKEY="$(headscale apikeys create --expiration 8760h 2>/dev/null | tail -1)"
if [ -z "${HS_APIKEY}" ]; then
  echo "[headscale] WARN: apikey creation returned empty; central HTTPS path unavailable"
else
  echo "[headscale] API key written to /var/lib/headscale/api-key (for central mesh.Headscale)"
  ( umask 077; printf '%s' "${HS_APIKEY}" > /var/lib/headscale/api-key )
  chmod 600 /var/lib/headscale/api-key

  # Optional ops-tailnet handoff. Operator-provided user-data enables this
  # private path; without every value below the box keeps the local key only.
  if [ -n "${HS_OPS_TOKEN:-}" ] && [ -n "${HS_OPS_AUTHKEY:-}" ] && [ -n "${HS_OPS_FACTORY_URL:-}" ]; then
    if [ -z "${HS_OPS_LOGIN_SERVER:-}" ]; then
      echo "[headscale] missing HS_OPS_LOGIN_SERVER; refusing hosted coordination fallback"
      exit 1
    fi
    if ! command -v tailscale >/dev/null 2>&1; then
      curl -fsSL https://tailscale.com/install.sh | sh >/dev/null 2>&1 || true
    fi
    if command -v tailscale >/dev/null 2>&1 && yscale_ops_join >/dev/null 2>&1; then
      HS_OPS_PAYLOAD="$(jq -cn \
        --arg token "${HS_OPS_TOKEN}" \
        --arg tenant_id "${HS_USER}" \
        --arg login_server "https://${HOSTNAME_FQDN}" \
        --arg api_key "${HS_APIKEY}" \
        '{token:$token,tenant_id:$tenant_id,login_server:$login_server,api_key:$api_key}')"
      # Retry generously across the provisioning window: the factory's AwaitKey
      # blocks for the whole provision, so a transient failure here (ops listener
      # not ready yet, network blip) must not strand the handoff. ~5 min of
      # attempts; the local key file is already written as the fallback.
      HS_OPS_OK=0
      for HS_OPS_ATTEMPT in $(seq 1 30); do
        if curl -fsS --connect-timeout 10 --max-time 20 -X POST \
          -H 'Content-Type: application/json' \
          --data-binary "${HS_OPS_PAYLOAD}" \
          "${HS_OPS_FACTORY_URL%/}/ops/register" >/dev/null 2>&1; then
          echo "[headscale] ops-tailnet key handoff completed"
          HS_OPS_OK=1
          break
        fi
        sleep 10
      done
      [ "${HS_OPS_OK}" = 1 ] || echo "[headscale] WARN: ops-tailnet key handoff did not complete after ${HS_OPS_ATTEMPT} attempts; local key retained"
      unset HS_OPS_PAYLOAD HS_OPS_ATTEMPT HS_OPS_OK
    else
      echo "[headscale] WARN: ops-tailnet key handoff unavailable; local key retained"
    fi
  fi
fi
unset HS_APIKEY

echo "[headscale] DONE $(date -u)"
echo "[headscale] login-server: https://${HOSTNAME_FQDN}"
echo "[headscale] embedded DERP region: 900 (yscale)"
