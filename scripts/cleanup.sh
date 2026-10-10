#!/usr/bin/env bash
# Hard reset for yscale smoke-test environments.
#
# Destroys ALL resources matching yscale's naming conventions across:
#   - Fly machines + volumes (in $FLY_BURST_APP, default hs-personal-burst)
#   - Linode instances (yscale-burst tagged)
#   - Tailscale devices (hostname yscale-*)
#   - k8s Node objects (name ys-burst-*)
#
# Idempotent. Safe to run before/after every smoke session. Uses
# credentials from ../.env (same dir layout as the rest of the repo).
#
# Usage: scripts/cleanup.sh
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO}/.env}"
FLY_BURST_APP="${FLY_BURST_APP:-hs-personal-burst}"

if [ ! -f "$ENV_FILE" ]; then
  echo "error: $ENV_FILE not found" >&2
  exit 1
fi

env_get() {
  grep "^${1}=" "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d '"'
}

FLY_TOKEN=$(env_get FLYIO_TOKEN)
LINODE_TOKEN=$(env_get LINODE_TOKEN)
TS_OAUTH_CLIENT_ID=$(env_get TS_OAUTH_CLIENT_ID)
TS_OAUTH_CLIENT_SECRET=$(env_get TS_OAUTH_CLIENT_SECRET)

destroyed=0

echo "=== Fly: $FLY_BURST_APP ==="
if [ -n "$FLY_TOKEN" ] && command -v flyctl >/dev/null; then
  export FLY_API_TOKEN="$FLY_TOKEN"
  # Parse --json (stable) instead of the human table: flyctl prepends a
  # "N machines have been retrieved" banner that a positional grep mistakes
  # for a machine ID. Filter to ys-burst-* / ys_burst_* names only.
  for id in $(flyctl machine list -a "$FLY_BURST_APP" --json 2>/dev/null \
      | jq -r '.[] | select(.name | startswith("ys-burst-")) | .id'); do
    echo "  destroy machine $id"
    flyctl machine destroy "$id" --force -a "$FLY_BURST_APP" >/dev/null 2>&1 && destroyed=$((destroyed+1))
  done
  for v in $(flyctl volumes list -a "$FLY_BURST_APP" --json 2>/dev/null \
      | jq -r '.[] | select(.name | startswith("ys_burst_")) | .id'); do
    echo "  destroy volume $v"
    flyctl volumes destroy "$v" -y -a "$FLY_BURST_APP" >/dev/null 2>&1 && destroyed=$((destroyed+1))
  done
else
  echo "  skipped (no FLYIO_TOKEN or flyctl)"
fi

# Linode cleanup deletes ONLY instances yscale created — identified by
# the exact "yscale-burst" tag CreateNode applies, never a name pattern.
# The customer's own instances (LKE nodes, etc.) lack that tag, so this
# can never touch them.
echo "=== Linode: yscale-burst tagged instances ==="
if [ -n "$LINODE_TOKEN" ]; then
  ids=$(curl -fsS "https://api.linode.com/v4/linode/instances" \
    -H "Authorization: Bearer ${LINODE_TOKEN}" 2>/dev/null \
    | jq -r '.data[]? | select((.tags // []) | index("yscale-burst")) | .id' 2>/dev/null)
  if [ -z "$ids" ]; then
    echo "  none"
  else
    for id in $ids; do
      echo "  delete instance $id"
      curl -fsS -X DELETE "https://api.linode.com/v4/linode/instances/${id}" \
        -H "Authorization: Bearer ${LINODE_TOKEN}" >/dev/null 2>&1 \
        && destroyed=$((destroyed+1))
    done
  fi
else
  echo "  skipped (no LINODE_TOKEN)"
fi

echo "=== Tailscale: yscale-* devices ==="
if [ -n "$TS_OAUTH_CLIENT_ID" ] && [ -n "$TS_OAUTH_CLIENT_SECRET" ]; then
  TS_TOKEN=$(curl -fsS -X POST "https://api.tailscale.com/api/v2/oauth/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=client_credentials&client_id=${TS_OAUTH_CLIENT_ID}&client_secret=${TS_OAUTH_CLIENT_SECRET}" \
    | jq -r .access_token)
  if [ -n "$TS_TOKEN" ] && [ "$TS_TOKEN" != "null" ]; then
    ids=$(curl -fsS "https://api.tailscale.com/api/v2/tailnet/-/devices" \
      -H "Authorization: Bearer ${TS_TOKEN}" \
      | jq -r '.devices[]? | select(.hostname | startswith("yscale-burst-")) | .id')
    if [ -z "$ids" ]; then
      echo "  none"
    else
      for id in $ids; do
        echo "  delete device $id"
        curl -fsS -X DELETE "https://api.tailscale.com/api/v2/device/${id}" \
          -H "Authorization: Bearer ${TS_TOKEN}" >/dev/null 2>&1 \
          && destroyed=$((destroyed+1))
      done
    fi
  else
    echo "  skipped (TS oauth failed)"
  fi
else
  echo "  skipped (no TS_OAUTH_CLIENT_ID/SECRET)"
fi

echo "=== k8s: ys-burst-* nodes ==="
if command -v kubectl >/dev/null; then
  for n in $(kubectl get nodes 2>/dev/null | grep "^ys-burst-" | awk '{print $1}'); do
    echo "  delete node $n"
    kubectl delete node "$n" --ignore-not-found >/dev/null 2>&1 && destroyed=$((destroyed+1))
  done
else
  echo "  skipped (no kubectl)"
fi

echo
echo "destroyed: $destroyed resources"
