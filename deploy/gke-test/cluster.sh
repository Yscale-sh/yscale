#!/usr/bin/env bash
# Lifecycle for a throwaway GKE Standard zonal cluster, used to validate that
# yscale burst nodes survive GKE's cloud-controller-manager. Cost-minimized:
# one e2-medium, smallest disk, no extra logging/monitoring/NAT.
#
#   ./cluster.sh up | kubeconfig | delete | status | nuke | preflight
#   ./cluster.sh burst-check BURST_ID=... | burst-cleanup BURST_ID=...
set -euo pipefail
cd "$(dirname "$0")"

CLUSTER="${CLUSTER:-yscale-gke-test}"
ZONE="${ZONE:-us-central1-a}"
REGION="${REGION:-us-central1}"
PROJECT="${PROJECT:-}"
RUN_ID="${RUN_ID:-gke-$(date +%s)-$$}"
NODE_TYPE="${NODE_TYPE:-e2-medium}"
DISK_SIZE="${DISK_SIZE:-20}"
BURST_ID="${BURST_ID:-}"

SSM_GCP_SA_KEY="${SSM_GCP_SA_KEY:-/shared/gcp/service-account-json}"

_GCP_TMPDIR=""

_gcp_cleanup() {
  if [ -n "${_GCP_TMPDIR}" ] && [ -d "${_GCP_TMPDIR}" ]; then
    rm -rf "${_GCP_TMPDIR}"
  fi
}
trap _gcp_cleanup EXIT

activate_gcp_creds() {
  if [ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ] && [ -f "${GOOGLE_APPLICATION_CREDENTIALS}" ]; then
    if [ -z "${PROJECT}" ] && command -v jq >/dev/null 2>&1; then
      PROJECT="$(jq -r '.project_id // empty' "${GOOGLE_APPLICATION_CREDENTIALS}" 2>/dev/null || true)"
    fi
    return 0
  fi
  if ! command -v aws >/dev/null 2>&1; then
    return 1
  fi
  _GCP_TMPDIR="$(mktemp -d)"
  local sa_file="${_GCP_TMPDIR}/sa.json"
  if aws ssm get-parameter --name "${SSM_GCP_SA_KEY}" --with-decryption --query 'Parameter.Value' --output text > "${sa_file}" 2>/dev/null; then
    export GOOGLE_APPLICATION_CREDENTIALS="${sa_file}"
    export CLOUDSDK_CONFIG="${_GCP_TMPDIR}/gcloud-config"
    mkdir -p "${CLOUDSDK_CONFIG}"
    gcloud auth activate-service-account --key-file="${sa_file}" --quiet 2>/dev/null
    if [ -z "${PROJECT}" ]; then
      PROJECT="$(jq -r '.project_id // empty' "${sa_file}" 2>/dev/null || true)"
    fi
    echo "activated GCP creds from SSM (${SSM_GCP_SA_KEY})" >&2
  else
    rm -rf "${_GCP_TMPDIR}"
    _GCP_TMPDIR=""
  fi
}

resolve_project() {
  if [ -z "${PROJECT}" ]; then
    PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
  fi
  if [ -z "${PROJECT}" ]; then
    echo "FAIL: GCP project not set. Set PROJECT= or run gcloud config set project <id>" >&2
    exit 1
  fi
}

_sanitize_label() {
  echo "$1" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/-/g'
}

cmd_preflight() {
  local ok=true
  command -v gcloud  >/dev/null || { echo "FAIL: gcloud not installed"; ok=false; }
  command -v kubectl >/dev/null || { echo "FAIL: kubectl not installed"; ok=false; }
  command -v helm    >/dev/null || { echo "FAIL: helm not installed"; ok=false; }
  command -v jq      >/dev/null || { echo "FAIL: jq not installed"; ok=false; }
  if [ "${ok}" = false ]; then
    exit 1
  fi
  activate_gcp_creds
  resolve_project
  gcloud projects describe "${PROJECT}" --format='value(projectId)' >/dev/null 2>&1 \
    || { echo "FAIL: cannot access project ${PROJECT}"; ok=false; }
  if [ "${ok}" = true ]; then
    echo "OK: all tools and credentials present (project=${PROJECT})"
  else
    exit 1
  fi
}

cmd_up() {
  resolve_project
  local existing
  existing="$(gcloud container clusters list --project="${PROJECT}" --zone="${ZONE}" \
    --filter="name=${CLUSTER}" --format='value(name)' 2>/dev/null || true)"
  if [ -n "${existing}" ]; then
    echo "cluster ${CLUSTER} already exists in ${ZONE}" >&2
    cmd_kubeconfig
    return
  fi

  echo "creating GKE ${CLUSTER} in ${ZONE} (1x ${NODE_TYPE}, disk=${DISK_SIZE}GB)..." >&2
  gcloud container clusters create "${CLUSTER}" \
    --project="${PROJECT}" \
    --zone="${ZONE}" \
    --num-nodes=1 \
    --machine-type="${NODE_TYPE}" \
    --disk-size="${DISK_SIZE}" \
    --disk-type=pd-standard \
    --logging=NONE \
    --monitoring=NONE \
    --labels="yscale-ephemeral-test=true" \
    --quiet

  cmd_kubeconfig
  echo "" >&2
  echo ">>> GKE cluster UP and billing. Run './cluster.sh delete' when finished. <<<" >&2
}

cmd_kubeconfig() {
  resolve_project
  gcloud container clusters get-credentials "${CLUSTER}" \
    --project="${PROJECT}" \
    --zone="${ZONE}"
}

cmd_delete() {
  resolve_project
  local existing
  existing="$(gcloud container clusters list --project="${PROJECT}" --zone="${ZONE}" \
    --filter="name=${CLUSTER}" --format='value(name)' 2>/dev/null || true)"
  if [ -z "${existing}" ]; then
    echo "no cluster ${CLUSTER} in ${ZONE} to delete" >&2
    return
  fi
  echo "deleting GKE ${CLUSTER} in ${ZONE}..." >&2
  gcloud container clusters delete "${CLUSTER}" \
    --project="${PROJECT}" \
    --zone="${ZONE}" \
    --quiet
  echo "deleted" >&2
}

cmd_status() {
  resolve_project
  gcloud container clusters list --project="${PROJECT}" --zone="${ZONE}" \
    --filter="name=${CLUSTER}" 2>/dev/null || echo "no cluster ${CLUSTER}"
  kubectl get nodes 2>/dev/null || true
}

cmd_nuke() {
  resolve_project
  local clusters
  clusters="$(gcloud container clusters list --project="${PROJECT}" --zone="${ZONE}" \
    --filter="name~^yscale-gke-test" --format='value(name)' 2>/dev/null || true)"
  for c in ${clusters}; do
    echo "deleting lingering GKE cluster ${c}" >&2
    gcloud container clusters delete "${c}" \
      --project="${PROJECT}" --zone="${ZONE}" --quiet || true
  done
  echo "nuke sweep done" >&2
}

cmd_burst_check() {
  if [ -z "${BURST_ID}" ]; then
    echo "FAIL: BURST_ID not set" >&2
    return 2
  fi
  resolve_project
  local sanitized
  sanitized="$(_sanitize_label "${BURST_ID}")"
  local found
  if ! found="$(gcloud compute instances list --project="${PROJECT}" \
    --filter="labels.ys-owner=yscale-burst AND labels.ys-burst-id=${sanitized} AND status!=TERMINATED" \
    --format='value(name,zone)' 2>/dev/null)"; then
    echo "FAIL: unable to query GCE burst instances" >&2
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
  resolve_project
  local sanitized
  sanitized="$(_sanitize_label "${BURST_ID}")"
  local instances
  if ! instances="$(gcloud compute instances list --project="${PROJECT}" \
    --filter="labels.ys-owner=yscale-burst AND labels.ys-burst-id=${sanitized} AND status!=TERMINATED" \
    --format='csv[no-heading](name,zone)' 2>/dev/null)"; then
    echo "FAIL: unable to query GCE burst instances for cleanup" >&2
    return 2
  fi
  if [ -z "${instances}" ]; then
    echo "no burst VMs to clean up" >&2
    return 0
  fi
  echo "${instances}" | while IFS=, read -r name zone; do
    echo "recovery-deleting GCE instance ${name} in ${zone}" >&2
    gcloud compute instances delete "${name}" --project="${PROJECT}" --zone="${zone}" --quiet
  done
}

case "${1:-}" in
  preflight)      cmd_preflight ;;
  up)             activate_gcp_creds; cmd_up ;;
  kubeconfig)     activate_gcp_creds; cmd_kubeconfig ;;
  delete|down)    activate_gcp_creds; cmd_delete ;;
  status)         activate_gcp_creds; cmd_status ;;
  nuke)           activate_gcp_creds; cmd_nuke ;;
  burst-check)    activate_gcp_creds; cmd_burst_check ;;
  burst-cleanup)  activate_gcp_creds; cmd_burst_cleanup ;;
  *) echo "usage: $0 {preflight|up|kubeconfig|delete|status|nuke|burst-check|burst-cleanup}" >&2; exit 2 ;;
esac
