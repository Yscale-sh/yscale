#!/usr/bin/env bash
# Paid, manual LKE release gate: prove a Linode burst survives LKE's CCM,
# schedules a Pod, is authoritatively reaped, and leaves no provider/mesh leak.
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${CONFIRM_SPEND:-}" != "yes" ]]; then
  echo "This test creates paid Linode resources. Set CONFIRM_SPEND=yes to proceed."
  exit 1
fi

: "${LINODE_TOKEN:?set LINODE_TOKEN}"
: "${YSCALE_TOKEN:?set YSCALE_TOKEN}"
: "${FABRIC_URL:?set FABRIC_URL for the final mesh audit}"
: "${FABRIC_API_KEY:?set FABRIC_API_KEY for the final mesh audit}"
: "${FABRIC_USER:?set FABRIC_USER for the final mesh audit}"

for command_name in curl jq openssl helm kubectl; do
  command -v "${command_name}" >/dev/null || { echo "${command_name} not installed"; exit 1; }
done

# shellcheck source=deploy/lke-test/inventory.sh
source ./inventory.sh

ENDPOINT="${ENDPOINT:-wss://api.yscale.sh}"
RUN_ID="${RUN_ID:-$(date +%s)-$$}"
CLUSTER="${CLUSTER:-yscale-lke-test-${RUN_ID}}"
MAX_TEST_SECONDS="${MAX_TEST_SECONDS:-2100}"
JOIN_TIMEOUT="${JOIN_TIMEOUT:-600}"
REAP_WAIT_SECONDS="${REAP_WAIT_SECONDS:-600}"
RECOVERY_WAIT_SECONDS="${RECOVERY_WAIT_SECONDS:-180}"
KUBECONFIG="$(pwd)/kubeconfig.yaml"
export CLUSTER KUBECONFIG

[[ "${CLUSTER}" =~ ^yscale-lke-test-[a-zA-Z0-9-]+$ ]] || {
  echo "CLUSTER must be a unique yscale-lke-test-* label"
  exit 1
}

echo "==> preflight: authenticating to central before any paid mutation"
central_auth_check "${ENDPOINT}" "${YSCALE_TOKEN}"

existing_clusters="$(linode_cluster_ids_by_label "${CLUSTER}")" || exit 1
[[ -z "${existing_clusters}" ]] || {
  echo "Refusing to reuse existing LKE cluster ${CLUSTER}"
  exit 1
}

BURST_ID_FILE="$(mktemp)"
NODE_NAME_FILE="$(mktemp)"
PROVIDER_ID_FILE="$(mktemp)"
TEST_RC=0
_CLEANUP_RAN=false
CLUSTER_CREATE_STARTED=false
KUBECONFIG_OK=false

cleanup() {
  local entry_rc=$?
  "${_CLEANUP_RAN}" && return 0
  _CLEANUP_RAN=true
  trap - EXIT INT TERM
  set +e
  if (( TEST_RC == 0 && entry_rc != 0 )); then
    TEST_RC="${entry_rc}"
  fi
  [[ -n "${WATCHDOG_PID:-}" ]] && kill "${WATCHDOG_PID}" 2>/dev/null

  local burst_id node_name provider_uri provider_id="" tagged="" query_rc=0
  burst_id="$(tr -d '\r\n' <"${BURST_ID_FILE}" 2>/dev/null)"
  node_name="$(tr -d '\r\n' <"${NODE_NAME_FILE}" 2>/dev/null)"
  provider_uri="$(tr -d '\r\n' <"${PROVIDER_ID_FILE}" 2>/dev/null)"
  if [[ "${provider_uri}" =~ ^linode://([1-9][0-9]*)$ ]]; then
    provider_id="${BASH_REMATCH[1]}"
  elif [[ -n "${provider_uri}" ]]; then
    echo "FAIL: burst Node exposed invalid providerID ${provider_uri}"
    TEST_RC=1
  fi

  # The Workload deletion is the authoritative lifecycle request. Keep the
  # customer cluster alive while central and the provider complete teardown.
  if "${KUBECONFIG_OK}"; then
    echo "==> teardown: deleting Workload survive-probe"
    if ! kubectl delete workload survive-probe -n default --ignore-not-found --wait=true --timeout=60s; then
      echo "FAIL: Workload deletion request failed"
      TEST_RC=1
    fi
  fi

  if [[ -n "${burst_id}" ]]; then
    echo "==> teardown: waiting up to ${REAP_WAIT_SECONDS}s for authoritative provider deletion"
    local deadline=$(( $(date +%s) + REAP_WAIT_SECONDS ))
    while (( $(date +%s) < deadline )); do
      query_rc=0
      tagged="$(linode_burst_instance_ids "${burst_id}")" || query_rc=$?
      (( query_rc == 0 )) || { TEST_RC=1; break; }
      [[ -z "${tagged}" ]] && break
      sleep 10
    done

    if (( query_rc == 0 )); then
      tagged="$(linode_burst_instance_ids "${burst_id}")" || query_rc=$?
    fi
    if (( query_rc != 0 )); then
      echo "FAIL: provider inventory was unavailable during teardown"
      TEST_RC=1
    elif [[ -n "${tagged}" ]]; then
      echo "FAIL: authoritative deletion timed out; recovery-deleting exact burst-tagged instances"
      TEST_RC=1
      while IFS= read -r id; do
        [[ -n "${id}" ]] || continue
        linode_delete_burst_instance "${id}" || TEST_RC=1
      done <<<"${tagged}"

      deadline=$(( $(date +%s) + RECOVERY_WAIT_SECONDS ))
      while (( $(date +%s) < deadline )); do
        query_rc=0
        tagged="$(linode_burst_instance_ids "${burst_id}")" || query_rc=$?
        (( query_rc == 0 )) || { TEST_RC=1; break; }
        [[ -z "${tagged}" ]] && break
        sleep 10
      done
    fi
  fi

  if "${KUBECONFIG_OK}"; then
    kubectl delete pod burst-pod-proof -n default --ignore-not-found --wait=false >/dev/null 2>&1
  fi

  if "${CLUSTER_CREATE_STARTED}"; then
    echo "==> teardown: deleting exact LKE cluster ${CLUSTER}"
    if ! ./lke.sh delete; then
      echo "FAIL: LKE cluster deletion failed"
      TEST_RC=1
    fi
  fi

  echo "==> teardown: final fail-closed residue audit"
  if [[ -z "${burst_id}" || -z "${provider_id}" || -z "${node_name}" ]]; then
    echo "FAIL: burst ID, provider ID, and node hostname are all required for residue proof"
    TEST_RC=1
  else
    local exact_count volumes firewalls firewall_devices=0 fabric_count cluster_ids
    tagged="$(linode_burst_instance_ids "${burst_id}")" || { tagged="inventory-error"; TEST_RC=1; }
    exact_count="$(linode_exact_instance_count "${provider_id}")" || { exact_count="inventory-error"; TEST_RC=1; }
    volumes="$(linode_attached_volume_ids "${provider_id}")" || { volumes="inventory-error"; TEST_RC=1; }
    firewalls="$(linode_firewall_ids_by_label yscale-burst-fw)" || { firewalls="inventory-error"; TEST_RC=1; }
    if [[ "${firewalls}" != "inventory-error" ]]; then
      while IFS= read -r firewall_id; do
        [[ -n "${firewall_id}" ]] || continue
        local devices
        devices="$(linode_firewall_device_ids "${firewall_id}" "${provider_id}")" || {
          firewall_devices=-1
          TEST_RC=1
          break
        }
        while IFS= read -r device_id; do
          [[ -n "${device_id}" ]] && firewall_devices=$(( firewall_devices + 1 ))
        done <<<"${devices}"
      done <<<"${firewalls}"
    else
      firewall_devices=-1
    fi
    fabric_count="$(fabric_node_count "${node_name}")" || { fabric_count="inventory-error"; TEST_RC=1; }

    [[ -z "${tagged}" ]] || { echo "LEAK: tagged Linode instances remain: ${tagged}"; TEST_RC=1; }
    [[ "${exact_count}" == "0" ]] || { echo "LEAK: exact provider instance remains or audit failed"; TEST_RC=1; }
    [[ -z "${volumes}" ]] || { echo "LEAK: volumes remain attached to provider instance: ${volumes}"; TEST_RC=1; }
    (( firewall_devices == 0 )) || { echo "LEAK: firewall device attachments remain or audit failed"; TEST_RC=1; }
    [[ "${fabric_count}" == "0" ]] || { echo "LEAK: exact Fabric device remains or audit failed"; TEST_RC=1; }
  fi

  local cluster_ids cluster_query_rc=0 cluster_deadline=$(( $(date +%s) + 180 ))
  while (( $(date +%s) < cluster_deadline )); do
    cluster_query_rc=0
    cluster_ids="$(linode_cluster_ids_by_label "${CLUSTER}")" || cluster_query_rc=$?
    (( cluster_query_rc == 0 )) || break
    [[ -z "${cluster_ids}" ]] && break
    sleep 10
  done
  if (( cluster_query_rc != 0 )); then
    cluster_ids="inventory-error"
    TEST_RC=1
  fi
  [[ -z "${cluster_ids}" ]] || { echo "LEAK: LKE cluster remains or audit failed: ${cluster_ids}"; TEST_RC=1; }

  rm -f "${BURST_ID_FILE}" "${NODE_NAME_FILE}" "${PROVIDER_ID_FILE}" "${KUBECONFIG}"
  echo "==> teardown complete (rc=${TEST_RC})"
  exit "${TEST_RC}"
}
trap cleanup EXIT
trap 'TEST_RC=130; exit 130' INT
trap 'TEST_RC=124; exit 124' TERM

echo "==> run=${RUN_ID} cluster=${CLUSTER} max_test=${MAX_TEST_SECONDS}s"
( sleep "${MAX_TEST_SECONDS}" && echo "TIMEOUT: ${MAX_TEST_SECONDS}s exceeded — forcing teardown" && kill -TERM $$ 2>/dev/null ) &
WATCHDOG_PID=$!

echo "==> creating throwaway LKE cluster"
CLUSTER_CREATE_STARTED=true
./lke.sh up
KUBECONFIG_OK=true

echo "==> installing yscale-agent"
kubectl create namespace yscale --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n yscale create secret generic yscale-agent-auth \
  --from-literal=YSCALE_TOKEN="${YSCALE_TOKEN}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
helm upgrade --install yscale-agent ../helm/yscale-agent \
  --namespace yscale --create-namespace \
  --set existingSecret=yscale-agent-auth \
  --set endpoint="${ENDPOINT}" \
  --set clusterID="${RUN_ID}" \
  --set cloudProvider=linode \
  --set persistence.enabled=false \
  --set rbac.scope=cluster
kubectl -n yscale rollout status deploy/yscale-agent-yscale-agent --timeout=180s

echo "==> running LKE burst-survival and Pod proof"
SURVIVE_CLEANUP=false \
BURST_ID_FILE="${BURST_ID_FILE}" \
NODE_NAME_FILE="${NODE_NAME_FILE}" \
PROVIDER_ID_FILE="${PROVIDER_ID_FILE}" \
BACKEND=linode JOIN_TIMEOUT="${JOIN_TIMEOUT}" \
  ../eks-test/survive-test.sh || TEST_RC=$?

if (( TEST_RC == 0 )); then
  burst_id="$(tr -d '\r\n' <"${BURST_ID_FILE}")"
  provider_uri="$(tr -d '\r\n' <"${PROVIDER_ID_FILE}")"
  if [[ ! "${provider_uri}" =~ ^linode://([1-9][0-9]*)$ ]]; then
    echo "FAIL: expected real linode://<id> providerID, got ${provider_uri:-empty}"
    TEST_RC=1
  else
    provider_id="${BASH_REMATCH[1]}"
    tagged="$(linode_burst_instance_ids "${burst_id}")" || TEST_RC=1
    if ! grep -Fxq "${provider_id}" <<<"${tagged:-}"; then
      echo "FAIL: Node providerID is not the exact burst-tagged Linode instance"
      TEST_RC=1
    fi
  fi
fi

echo "==> test phase complete (rc=${TEST_RC}); teardown runs now"
