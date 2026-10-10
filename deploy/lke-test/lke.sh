#!/usr/bin/env bash
# Lifecycle for a throwaway LKE (Linode Kubernetes Engine) cluster, used to
# verify that yscale burst nodes SURVIVE LKE's CCM (the providerID fix). LKE's
# control plane is free; a 1-node pool is pennies. Talks to the Linode API
# directly with $LINODE_TOKEN (same token the backend uses) — no linode-cli dep.
#
#   ./lke.sh up | kubeconfig | delete | status | nuke | id
set -euo pipefail
cd "$(dirname "$0")"

: "${LINODE_TOKEN:?set LINODE_TOKEN (the same token the Linode backend uses)}"
command -v jq      >/dev/null || { echo "jq not installed: brew install jq"; exit 1; }
command -v openssl >/dev/null || { echo "openssl not found (needed to decode kubeconfig)"; exit 1; }

LABEL="${CLUSTER:-yscale-lke-test}"
REGION="${REGION:-us-ord}"
NODE_TYPE="${NODE_TYPE:-g6-standard-1}"
API="https://api.linode.com/v4"
KCFG="$(pwd)/kubeconfig.yaml"

# shellcheck source=deploy/lke-test/inventory.sh
source ./inventory.sh

api() { curl -sf -H "Authorization: Bearer $LINODE_TOKEN" -H "Content-Type: application/json" "$@"; }

cluster_id() {
  local ids count
  ids="$(linode_cluster_ids_by_label "$LABEL")"
  count="$(printf '%s\n' "$ids" | sed '/^$/d' | wc -l | tr -d ' ')"
  [ "$count" -le 1 ] || { echo "multiple clusters have exact label $LABEL; refusing ambiguous operation" >&2; return 1; }
  printf '%s\n' "$ids"
}

cmd_id() { cluster_id; }

cmd_create() {
  local id; id="$(cluster_id)"
  if [ -n "$id" ]; then echo "$id"; return; fi
  local ver; ver="$(api "$API/lke/versions" | jq -r '.data[].id' | sort -V | tail -1)"
  echo "creating LKE $LABEL in $REGION (k8s $ver, 1x $NODE_TYPE)..." >&2
  local body; body="$(jq -n --arg l "$LABEL" --arg r "$REGION" --arg v "$ver" --arg t "$NODE_TYPE" \
    '{label:$l, region:$r, k8s_version:$v, tags:["yscale-ephemeral-test"], node_pools:[{type:$t, count:1}]}')"
  api -X POST "$API/lke/clusters" -d "$body" | jq -r '.id'
}

cmd_kubeconfig() {
  local id; id="$(cluster_id)"; [ -n "$id" ] || { echo "no cluster $LABEL" >&2; exit 1; }
  echo "waiting for kubeconfig (control plane provisioning, ~1-2m)..." >&2
  local b64
  for _ in $(seq 1 60); do
    b64="$(api "$API/lke/clusters/$id/kubeconfig" 2>/dev/null | jq -r '.kubeconfig // empty')" || true
    if [ -n "${b64:-}" ]; then
      printf '%s' "$b64" | openssl base64 -d -A > "$KCFG"
      echo "wrote $KCFG" >&2
      return
    fi
    sleep 5
  done
  echo "timed out waiting for kubeconfig" >&2; exit 1
}

cmd_up() {
  cmd_create >/dev/null
  cmd_kubeconfig
  echo "waiting for the node pool to be Ready (~2-3m)..." >&2
  kubectl --kubeconfig="$KCFG" wait --for=condition=Ready nodes --all --timeout=360s || true
  kubectl --kubeconfig="$KCFG" get nodes >&2
  echo ""
  echo ">>> LKE up. export KUBECONFIG=$KCFG   (REMEMBER: ./lke.sh delete when finished) <<<" >&2
}

cmd_delete() {
  local id; id="$(cluster_id)"
  [ -n "$id" ] || { echo "no cluster $LABEL to delete" >&2; return; }
  echo "deleting LKE $LABEL (id $id)..." >&2
  api -X DELETE "$API/lke/clusters/$id" >/dev/null && echo "deleted" >&2
  rm -f "$KCFG"
}

cmd_status() {
  local id; id="$(cluster_id)"
  [ -n "$id" ] || { echo "no cluster $LABEL"; return; }
  echo "cluster $LABEL id=$id"
  if [ -f "$KCFG" ]; then
    kubectl --kubeconfig="$KCFG" get nodes 2>/dev/null || true
  fi
}

# Orphan safety net: delete every cluster whose label begins yscale-lke-test.
cmd_nuke() {
  for id in $(api "$API/lke/clusters" | jq -r '.data[] | select(.label|startswith("yscale-lke-test")) | .id'); do
    echo "deleting lingering LKE cluster id=$id" >&2
    api -X DELETE "$API/lke/clusters/$id" >/dev/null || true
  done
  echo "nuke sweep done" >&2
}

case "${1:-}" in
  up) cmd_up ;;
  create) cmd_create ;;
  kubeconfig) cmd_kubeconfig ;;
  delete|down) cmd_delete ;;
  status) cmd_status ;;
  nuke) cmd_nuke ;;
  id) cmd_id ;;
  *) echo "usage: $0 {up|kubeconfig|delete|status|nuke|id}" >&2; exit 2 ;;
esac
