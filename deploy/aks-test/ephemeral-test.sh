#!/usr/bin/env bash
# Spin up an ephemeral AKS cluster, run yscale validation, and ALWAYS tear it
# down — even on failure or Ctrl-C — so a test run can never leak a paid cluster.
#
#   CONFIRM_SPEND=yes ./ephemeral-test.sh                          # bare cluster smoke test (no burst validation)
#   CONFIRM_SPEND=yes YSCALE_TOKEN=yscale_... ./ephemeral-test.sh  # real burst compatibility test
#   CONFIRM_SPEND=yes YSCALE_TOKEN=... HOLD=900 ./ephemeral-test.sh # hold 15 min for manual test
set -euo pipefail
cd "$(dirname "$0")"

CLUSTER="${CLUSTER:-yscale-aks-test}"
LOCATION="${LOCATION:-eastus}"
ENDPOINT="${ENDPOINT:-wss://api.yscale.sh}"
RUN_ID="${RUN_ID:-aks-$(date +%s)-$$}"
MAX_TEST_SECONDS="${MAX_TEST_SECONDS:-2700}"

SURVIVE_NS="yscale-survive-test"
BURST_ID_FILE=""

if [ "${CONFIRM_SPEND:-}" != "yes" ]; then
  echo "This test creates paid AKS and Azure burst resources. Set CONFIRM_SPEND=yes to proceed."
  exit 1
fi
if [ -n "${YSCALE_TOKEN:-}" ] && [ "${CENTRAL_AZURE_SCOPE_VERIFIED:-}" != "yes" ]; then
  echo "Refusing Azure burst: the remote central's subscription/resource-group/location scope is not verified."
  echo "Use a dedicated central bound to this isolated test group, then set CENTRAL_AZURE_SCOPE_VERIFIED=yes."
  exit 1
fi

BURST_ID_FILE="$(mktemp)"

export CLUSTER LOCATION RUN_ID

TEST_RC=0
_CLEANUP_RAN=false
PREFLIGHT_OK=false
KUBECONFIG_OK=false

cleanup() {
  local entry_rc=$?
  "${_CLEANUP_RAN}" && return 0
  _CLEANUP_RAN=true
  if [ "${TEST_RC}" -eq 0 ] && [ "${entry_rc}" -ne 0 ]; then
    TEST_RC="${entry_rc}"
  fi

  [ -n "${WATCHDOG_PID:-}" ] && kill "${WATCHDOG_PID}" 2>/dev/null || true

  local burst_id=""
  if [ -f "${BURST_ID_FILE}" ]; then
    burst_id="$(cat "${BURST_ID_FILE}" 2>/dev/null || true)"
  fi

  if [ "${PREFLIGHT_OK}" != "true" ]; then
    rm -f "${BURST_ID_FILE}"
    echo "==> teardown skipped: preflight never approved this resource group"
    exit "${TEST_RC}"
  fi

  if [ "${KUBECONFIG_OK}" = "true" ]; then
    echo "==> teardown: deleting Workload survive-probe"
    kubectl delete workload survive-probe -n "${SURVIVE_NS}" --ignore-not-found 2>/dev/null || true
  fi

  if [ -n "${burst_id}" ]; then
    echo "==> teardown: waiting up to 60s for central to clean burst ${burst_id}"
    local deadline=$(( $(date +%s) + 60 ))
    local remaining=""
    local query_rc=1
    while [ "$(date +%s)" -lt "${deadline}" ]; do
      query_rc=0
      remaining="$(BURST_ID="${burst_id}" ./cluster.sh burst-check 2>&1)" || query_rc=$?
      [ "${query_rc}" -eq 0 ] && break
      if [ "${query_rc}" -ne 1 ]; then
        echo "FAIL: Azure cleanup query failed: ${remaining}"
        TEST_RC=1
        break
      fi
      sleep 10
    done

    if [ "${query_rc}" -eq 1 ]; then
      echo "WARN: burst VMs remain after central cleanup window — recovery delete"
      BURST_ID="${burst_id}" ./cluster.sh burst-cleanup || TEST_RC=1
    fi
  fi

  if [ "${KUBECONFIG_OK}" = "true" ]; then
    kubectl delete ns "${SURVIVE_NS}" --ignore-not-found --wait=false 2>/dev/null || true
  fi

  echo "==> teardown: deleting cluster ${CLUSTER} (guaranteed)"
  if ! ./cluster.sh delete; then
    echo "FAIL: cluster deletion failed — run 'make nuke'"
    TEST_RC=1
  fi

  if [ -n "${burst_id}" ]; then
    echo "==> teardown: final leak check (burst_id=${burst_id})"
    local leaked=""
    local final_query_rc=0
    leaked="$(BURST_ID="${burst_id}" ./cluster.sh burst-check 2>&1)" || final_query_rc=$?
    if [ "${final_query_rc}" -eq 1 ]; then
      echo "LEAK: Azure VMs still running after teardown (burst_id=${burst_id}):"
      echo "${leaked}"
      echo "  ACTION REQUIRED: manually delete these VMs."
      TEST_RC=1
    elif [ "${final_query_rc}" -ne 0 ]; then
      echo "FAIL: final Azure leak check could not be completed: ${leaked}"
      TEST_RC=1
    fi
  fi

  # Network scaffolding can be created before the VM or burst ID is persisted,
  # so audit it even when the run failed before BURST_ID_FILE was populated.
  echo "==> teardown: removing backend-owned Azure burst network scaffolding"
  local network_rc=0
  ./cluster.sh burst-network-check >/dev/null 2>&1 || network_rc=$?
  if [ "${network_rc}" -eq 1 ]; then
    ./cluster.sh burst-network-cleanup || TEST_RC=1
  elif [ "${network_rc}" -ne 0 ]; then
    echo "FAIL: final Azure network inventory could not be completed"
    TEST_RC=1
  fi

  network_rc=0
  local network_leaks=""
  network_leaks="$(./cluster.sh burst-network-check 2>&1)" || network_rc=$?
  if [ "${network_rc}" -eq 1 ]; then
    echo "LEAK: Azure burst network resources still exist after teardown:"
    echo "${network_leaks}"
    TEST_RC=1
  elif [ "${network_rc}" -ne 0 ]; then
    echo "FAIL: final Azure network leak check could not be completed: ${network_leaks}"
    TEST_RC=1
  fi

  rm -f "${BURST_ID_FILE}"
  echo "==> teardown complete (rc=${TEST_RC})"
  exit "${TEST_RC}"
}
trap cleanup EXIT

./cluster.sh preflight
PREFLIGHT_OK=true

echo "==> run=$RUN_ID max_test=${MAX_TEST_SECONDS}s"
echo "==> creating ${CLUSTER} in ${LOCATION}"

( sleep "${MAX_TEST_SECONDS}" && echo "TIMEOUT: ${MAX_TEST_SECONDS}s exceeded — forcing teardown" && kill -TERM $$ 2>/dev/null ) &
WATCHDOG_PID=$!

./cluster.sh up
KUBECONFIG_OK=true

echo "==> cluster ready:"
kubectl get nodes

if [ -n "${YSCALE_TOKEN:-}" ]; then
  echo "==> installing yscale-agent (endpoint ${ENDPOINT})"
  helm upgrade --install yscale-agent ../helm/yscale-agent \
    --namespace yscale --create-namespace \
    --set token="${YSCALE_TOKEN}" \
    --set endpoint="${ENDPOINT}" \
    --set cloudProvider=azure
  kubectl -n yscale rollout status deploy/yscale-agent --timeout=180s
  echo "==> running burst compatibility test (backend=azure)"
  SURVIVE_CLEANUP=false BURST_ID_FILE="${BURST_ID_FILE}" BACKEND=azure \
    ../eks-test/survive-test.sh || TEST_RC=$?
  if [ -n "${HOLD:-}" ]; then
    echo "==> holding ${HOLD}s for manual testing; cluster tears down automatically after."
    sleep "${HOLD}"
  fi
else
  echo "==> no YSCALE_TOKEN set — bare-cluster smoke test only (no burst validation)."
fi

echo "==> test phase complete (rc=${TEST_RC}); teardown runs now via trap."
