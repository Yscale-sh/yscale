#!/usr/bin/env bash
# Lifecycle for a throwaway AKS Free-tier cluster, used to validate that yscale
# burst nodes survive AKS's cloud-controller-manager. Cost-minimized: Free SKU,
# two Standard_D4as_v5 system nodes, smallest disk. Current AKS system-pool
# rules require at least two nodes and at least 4 vCPU / 4 GB per node.
#
#   ./cluster.sh up | kubeconfig | delete | status | nuke | preflight
#   ./cluster.sh burst-check BURST_ID=... | burst-cleanup BURST_ID=...
#   ./cluster.sh burst-network-check | burst-network-cleanup
set -euo pipefail
cd "$(dirname "$0")"

CLUSTER="${CLUSTER:-yscale-aks-test}"
LOCATION="${LOCATION:-eastus}"
RESOURCE_GROUP="${RESOURCE_GROUP:-}"
RUN_ID="${RUN_ID:-aks-$(date +%s)-$$}"
NODE_TYPE="${NODE_TYPE:-Standard_D4as_v5}"
NODE_COUNT="${NODE_COUNT:-2}"
DISK_SIZE="${DISK_SIZE:-30}"
BURST_ID="${BURST_ID:-}"

# --- credential bootstrap from SSM ---
SSM_AZURE_TENANT="${SSM_AZURE_TENANT:-/shared/azure/tenant-id}"
SSM_AZURE_CLIENT_ID="${SSM_AZURE_CLIENT_ID:-/shared/azure/client-id}"
SSM_AZURE_CLIENT_SECRET="${SSM_AZURE_CLIENT_SECRET:-/shared/azure/client-secret}"
SSM_AZURE_SUBSCRIPTION="${SSM_AZURE_SUBSCRIPTION:-/shared/azure/subscription-id}"
SSM_AZURE_RESOURCE_GROUP="${SSM_AZURE_RESOURCE_GROUP:-/shared/azure/resource-group}"

activate_azure_creds() {
  if az account show >/dev/null 2>&1; then
    return 0
  fi
  if ! command -v aws >/dev/null 2>&1; then
    echo "FAIL: az not logged in and aws CLI unavailable to read SSM" >&2
    exit 1
  fi

  local tenant client_id client_secret subscription rg
  tenant="$(aws ssm get-parameter --name "${SSM_AZURE_TENANT}" --with-decryption --query 'Parameter.Value' --output text 2>/dev/null || true)"
  client_id="$(aws ssm get-parameter --name "${SSM_AZURE_CLIENT_ID}" --with-decryption --query 'Parameter.Value' --output text 2>/dev/null || true)"
  client_secret="$(aws ssm get-parameter --name "${SSM_AZURE_CLIENT_SECRET}" --with-decryption --query 'Parameter.Value' --output text 2>/dev/null || true)"
  subscription="$(aws ssm get-parameter --name "${SSM_AZURE_SUBSCRIPTION}" --with-decryption --query 'Parameter.Value' --output text 2>/dev/null || true)"
  rg="$(aws ssm get-parameter --name "${SSM_AZURE_RESOURCE_GROUP}" --with-decryption --query 'Parameter.Value' --output text 2>/dev/null || true)"

  if [ -z "${tenant}" ] || [ -z "${client_id}" ] || [ -z "${client_secret}" ] || [ -z "${subscription}" ] || [ -z "${rg}" ]; then
    echo "FAIL: Azure credentials incomplete. Required SSM parameters:" >&2
    echo "  ${SSM_AZURE_TENANT}" >&2
    echo "  ${SSM_AZURE_CLIENT_ID}" >&2
    echo "  ${SSM_AZURE_CLIENT_SECRET}" >&2
    echo "  ${SSM_AZURE_SUBSCRIPTION}" >&2
    echo "  ${SSM_AZURE_RESOURCE_GROUP}" >&2
    exit 1
  fi

  az login --service-principal \
    --tenant "${tenant}" \
    --username "${client_id}" \
    --password "${client_secret}" \
    --output none 2>/dev/null

  az account set --subscription "${subscription}" 2>/dev/null
  RESOURCE_GROUP="${rg}"
  export RESOURCE_GROUP
  echo "activated Azure creds from SSM" >&2
}

resolve_rg() {
  if [ -z "${RESOURCE_GROUP}" ]; then
    echo "FAIL: RESOURCE_GROUP not set. Set RESOURCE_GROUP= or populate SSM." >&2
    exit 1
  fi
}

require_test_rg() {
  resolve_rg
  local test_only
  if ! test_only="$(az group show --name "${RESOURCE_GROUP}" \
    --query 'tags."yscale-ephemeral-test"' --output tsv 2>/dev/null)"; then
    echo "FAIL: cannot inspect resource group ${RESOURCE_GROUP}" >&2
    return 1
  fi
  if [ "${test_only}" != "true" ]; then
    echo "FAIL: resource group ${RESOURCE_GROUP} is not marked yscale-ephemeral-test=true" >&2
    echo "Azure burst qualification requires an isolated test-only resource group." >&2
    return 1
  fi
}

require_test_cluster() {
  local cluster_name="$1" cluster_json
  if ! cluster_json="$(az aks show --resource-group "${RESOURCE_GROUP}" \
    --name "${cluster_name}" --output json 2>/dev/null)"; then
    echo "FAIL: cannot inspect AKS cluster ${cluster_name}" >&2
    return 1
  fi
  if ! jq -e '.tags["yscale-ephemeral-test"] == "true" and
    .tags.purpose == "yscale-managed-cluster-validation"' \
    >/dev/null <<<"${cluster_json}"; then
    echo "FAIL: refusing cluster action without exact Yscale test tags: ${cluster_name}" >&2
    return 1
  fi
}

validate_system_pool() {
  if ! [[ "${NODE_COUNT}" =~ ^[0-9]+$ ]] || [ "${NODE_COUNT}" -lt 2 ]; then
    echo "FAIL: AKS system pool requires NODE_COUNT>=2" >&2
    return 1
  fi
  case "${NODE_TYPE}" in
    Standard_D4as_v5|Standard_D4s_v5) ;;
    *)
      echo "FAIL: NODE_TYPE=${NODE_TYPE} is not an approved 4-vCPU AKS qualification size" >&2
      return 1
      ;;
  esac
}

list_burst_network_resources() {
  local resources
  resources="$(az resource list --resource-group "${RESOURCE_GROUP}" --output json)" || return
  jq '[.[] | select(
    (.name | startswith("ys-vnet-")) or
    (.name | startswith("ys-nsg-")) or
    (.name | startswith("ys-natgw-")) or
    (.name | startswith("ys-natgw-pip-"))
  )]' <<<"${resources}"
}

cmd_preflight() {
  local ok=true
  command -v az      >/dev/null || { echo "FAIL: az CLI not installed"; ok=false; }
  command -v kubectl >/dev/null || { echo "FAIL: kubectl not installed"; ok=false; }
  command -v helm    >/dev/null || { echo "FAIL: helm not installed"; ok=false; }
  command -v jq      >/dev/null || { echo "FAIL: jq not installed"; ok=false; }
  if [ "${ok}" = false ]; then
    exit 1
  fi
  activate_azure_creds
  require_test_rg || ok=false
  validate_system_pool || ok=false
  if [ "${ok}" = true ]; then
    echo "OK: all tools and credentials present (rg=${RESOURCE_GROUP}, location=${LOCATION})"
  else
    exit 1
  fi
}

cmd_up() {
  require_test_rg
  validate_system_pool
  local existing
  if ! existing="$(az aks list --resource-group "${RESOURCE_GROUP}" \
    --query "[?name=='${CLUSTER}'].name" --output tsv 2>/dev/null)"; then
    echo "FAIL: unable to inventory AKS clusters before create" >&2
    return 1
  fi
  if [ -n "${existing}" ]; then
    require_test_cluster "${CLUSTER}"
    echo "cluster ${CLUSTER} already exists in ${RESOURCE_GROUP}" >&2
    cmd_kubeconfig
    return
  fi

  echo "creating AKS ${CLUSTER} in ${LOCATION} (Free tier, ${NODE_COUNT}x ${NODE_TYPE}, disk=${DISK_SIZE}GB)..." >&2
  az aks create \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${CLUSTER}" \
    --location "${LOCATION}" \
    --tier free \
    --node-count "${NODE_COUNT}" \
    --node-vm-size "${NODE_TYPE}" \
    --os-disk-size-gb "${DISK_SIZE}" \
    --generate-ssh-keys \
    --tags "yscale-ephemeral-test=true" "purpose=yscale-managed-cluster-validation"

  cmd_kubeconfig
  echo "" >&2
  echo ">>> AKS cluster UP and billing. Run './cluster.sh delete' when finished. <<<" >&2
}

cmd_kubeconfig() {
  resolve_rg
  az aks get-credentials \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${CLUSTER}" \
    --overwrite-existing
}

cmd_delete() {
  require_test_rg
  local existing
  if ! existing="$(az aks list --resource-group "${RESOURCE_GROUP}" \
    --query "[?name=='${CLUSTER}'].name" --output tsv 2>/dev/null)"; then
    echo "FAIL: unable to inventory AKS clusters before delete" >&2
    return 1
  fi
  if [ -z "${existing}" ]; then
    echo "no cluster ${CLUSTER} in ${RESOURCE_GROUP} to delete" >&2
    return
  fi
  require_test_cluster "${CLUSTER}"
  echo "deleting AKS ${CLUSTER} in ${RESOURCE_GROUP}..." >&2
  az aks delete \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${CLUSTER}" \
    --yes
  echo "deleted" >&2
}

cmd_status() {
  resolve_rg
  az aks list --resource-group "${RESOURCE_GROUP}" \
    --query "[?name=='${CLUSTER}']" --output table 2>/dev/null || echo "no cluster ${CLUSTER}"
  kubectl get nodes 2>/dev/null || true
}

cmd_nuke() {
  require_test_rg
  local clusters
  if ! clusters="$(az aks list --resource-group "${RESOURCE_GROUP}" \
    --query "[?starts_with(name,'yscale-aks-test')].name" --output tsv 2>/dev/null)"; then
    echo "FAIL: unable to inventory AKS clusters for nuke" >&2
    return 1
  fi
  for c in ${clusters}; do
    require_test_cluster "${c}"
    echo "deleting lingering AKS cluster ${c}" >&2
    az aks delete \
      --resource-group "${RESOURCE_GROUP}" \
      --name "${c}" \
      --yes
  done
  echo "nuke sweep done" >&2
}

cmd_burst_check() {
  if [ -z "${BURST_ID}" ]; then
    echo "FAIL: BURST_ID not set" >&2
    return 2
  fi
  resolve_rg
  local found
  if ! found="$(az vm list -d --resource-group "${RESOURCE_GROUP}" \
    --query "[?tags.\"ys-owner\"=='yscale-burst' && tags.\"ys-burst-id\"=='${BURST_ID}'].{name:name,state:powerState}" \
    --output tsv 2>/dev/null)"; then
    echo "FAIL: unable to query Azure burst VMs" >&2
    return 2
  fi
  if [ -n "${found}" ]; then
    echo "${found}"
    return 1
  fi
  return 0
}

cmd_burst_cleanup() {
  if [ -z "${BURST_ID}" ]; then
    echo "FAIL: BURST_ID not set" >&2
    return 2
  fi
  require_test_rg
  local vms
  if ! vms="$(az vm list --resource-group "${RESOURCE_GROUP}" \
    --query "[?tags.\"ys-owner\"=='yscale-burst' && tags.\"ys-burst-id\"=='${BURST_ID}'].name" \
    --output tsv 2>/dev/null)"; then
    echo "FAIL: unable to query Azure burst VMs for cleanup" >&2
    return 2
  fi
  if [ -z "${vms}" ]; then
    echo "no burst VMs to clean up" >&2
    return 0
  fi
  for vm in ${vms}; do
    echo "recovery-deleting Azure VM ${vm}" >&2
    az vm delete --resource-group "${RESOURCE_GROUP}" --name "${vm}" --yes
  done
}

cmd_burst_network_check() {
  resolve_rg
  local resources
  if ! resources="$(list_burst_network_resources 2>/dev/null)"; then
    echo "FAIL: unable to inventory Azure burst network resources" >&2
    return 2
  fi
  if ! jq -e 'all(.[]; .tags["ys-owner"] == "yscale-burst")' \
    >/dev/null <<<"${resources}"; then
    echo "FAIL: a reserved Azure burst network name is not Yscale-owned" >&2
    return 2
  fi
  if [ "$(jq 'length' <<<"${resources}")" -eq 0 ]; then
    return 0
  fi
  jq -r '.[] | [.name, .type] | @tsv' <<<"${resources}"
  return 1
}

cmd_burst_network_cleanup() {
  require_test_rg

  local owned_vms resources
  if ! owned_vms="$(az vm list --resource-group "${RESOURCE_GROUP}" \
    --query '[?tags."ys-owner"==`yscale-burst`].name' --output tsv 2>/dev/null)"; then
    echo "FAIL: unable to prove the Azure burst network is idle" >&2
    return 2
  fi
  if [ -n "${owned_vms}" ]; then
    echo "FAIL: refusing shared-network cleanup while Yscale-owned VMs remain:" >&2
    echo "${owned_vms}" >&2
    return 2
  fi

  if ! resources="$(list_burst_network_resources 2>/dev/null)"; then
    echo "FAIL: unable to inventory Azure burst network resources" >&2
    return 2
  fi
  if ! jq -e 'all(.[]; .tags["ys-owner"] == "yscale-burst")' \
    >/dev/null <<<"${resources}"; then
    echo "FAIL: refusing to delete a reserved Azure network name without exact Yscale ownership" >&2
    return 2
  fi

  local name
  while IFS= read -r name; do
    [ -n "${name}" ] && az network vnet delete --resource-group "${RESOURCE_GROUP}" --name "${name}"
  done < <(jq -r '.[] | select(.name | startswith("ys-vnet-")) | .name' <<<"${resources}")
  while IFS= read -r name; do
    [ -n "${name}" ] && az network nat gateway delete --resource-group "${RESOURCE_GROUP}" --name "${name}"
  done < <(jq -r '.[] | select((.name | startswith("ys-natgw-")) and ((.name | startswith("ys-natgw-pip-")) | not)) | .name' <<<"${resources}")
  while IFS= read -r name; do
    [ -n "${name}" ] && az network public-ip delete --resource-group "${RESOURCE_GROUP}" --name "${name}"
  done < <(jq -r '.[] | select(.name | startswith("ys-natgw-pip-")) | .name' <<<"${resources}")
  while IFS= read -r name; do
    [ -n "${name}" ] && az network nsg delete --resource-group "${RESOURCE_GROUP}" --name "${name}"
  done < <(jq -r '.[] | select(.name | startswith("ys-nsg-")) | .name' <<<"${resources}")

  if cmd_burst_network_check; then
    echo "Azure burst network cleanup complete" >&2
    return 0
  fi
  echo "FAIL: Azure burst network resources remain after cleanup" >&2
  return 2
}

case "${1:-}" in
  preflight)      cmd_preflight ;;
  up)             activate_azure_creds; cmd_up ;;
  kubeconfig)     activate_azure_creds; cmd_kubeconfig ;;
  delete|down)    activate_azure_creds; cmd_delete ;;
  status)         activate_azure_creds; cmd_status ;;
  nuke)           activate_azure_creds; cmd_nuke ;;
  burst-check)    activate_azure_creds; cmd_burst_check ;;
  burst-cleanup)  activate_azure_creds; cmd_burst_cleanup ;;
  burst-network-check)   activate_azure_creds; cmd_burst_network_check ;;
  burst-network-cleanup) activate_azure_creds; cmd_burst_network_cleanup ;;
  *) echo "usage: $0 {preflight|up|kubeconfig|delete|status|nuke|burst-check|burst-cleanup|burst-network-check|burst-network-cleanup}" >&2; exit 2 ;;
esac
