#!/usr/bin/env bash
# Spin up an ephemeral GKE cluster, run yscale validation, and ALWAYS tear it
# down — even on failure or Ctrl-C — so a test run can never leak a paid
# cluster. This is the short-TTL guarantee: teardown is in a trap, not a step.
#
#   CONFIRM_SPEND=yes ./ephemeral-test.sh                          # bare cluster smoke test (no burst validation)
#   CONFIRM_SPEND=yes YSCALE_TOKEN=yscale_... BACKEND=linode ./ephemeral-test.sh
#   CONFIRM_SPEND=yes YSCALE_TOKEN=yscale_... BACKEND=aws ./ephemeral-test.sh
#   CONFIRM_SPEND=yes YSCALE_TOKEN=... HOLD=900 ./ephemeral-test.sh # hold 15 min for manual test
set -euo pipefail
cd "$(dirname "$0")"

CLUSTER="${CLUSTER:-yscale-gke-test}"
ZONE="${ZONE:-us-central1-a}"
REGION="${REGION:-us-central1}"
ENDPOINT="${ENDPOINT:-wss://api.yscale.sh}"
RUN_ID="${RUN_ID:-gke-$(date +%s)-$$}"
MAX_TEST_SECONDS="${MAX_TEST_SECONDS:-2700}"
JOIN_TIMEOUT="${JOIN_TIMEOUT:-600}"
BACKEND="${BACKEND:-linode}"
AWS_BURST_REGION="${AWS_BURST_REGION:-us-east-1}"
CLUSTER_ID="${CLUSTER_ID:-${RUN_ID}}"
SOURCE_KUBE_CONTEXT="${SOURCE_KUBE_CONTEXT:-}"
AGENT_PULL_SECRET="${AGENT_PULL_SECRET:-}"
AGENT_IMAGE_REPOSITORY="${AGENT_IMAGE_REPOSITORY:-}"
AGENT_IMAGE_TAG="${AGENT_IMAGE_TAG:-}"
AGENT_TS_AUTHKEY="${AGENT_TS_AUTHKEY:-}"
GATEWAY_TS_AUTHKEY="${GATEWAY_TS_AUTHKEY:-}"
TS_LOGIN_SERVER="${TS_LOGIN_SERVER:-}"

SURVIVE_NS="default"
BURST_ID_FILE=""

if [ "${CONFIRM_SPEND:-}" != "yes" ]; then
  echo "This test creates a PAID GKE cluster (~\$0.10/hr). Set CONFIRM_SPEND=yes to proceed."
  exit 1
fi

command -v gcloud  >/dev/null || { echo "gcloud not installed"; exit 1; }
command -v kubectl >/dev/null || { echo "kubectl not installed"; exit 1; }
command -v helm    >/dev/null || { echo "helm not installed"; exit 1; }
command -v jq      >/dev/null || { echo "jq not installed"; exit 1; }

# GCP credential activation — needed for inline gcloud calls (existence check,
# CIDR queries). cluster.sh activates per-subprocess; this makes creds available
# to ephemeral-test.sh itself and to child processes (cluster.sh burst-check
# during cleanup).
_GKE_CRED_TMPDIR=""
cleanup_gke_credentials() {
  if [ -n "${_GKE_CRED_TMPDIR}" ] && [ -d "${_GKE_CRED_TMPDIR}" ]; then
    rm -rf -- "${_GKE_CRED_TMPDIR}"
    _GKE_CRED_TMPDIR=""
  fi
}
trap cleanup_gke_credentials EXIT

PROJECT="${PROJECT:-}"
if [ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ] && [ -f "${GOOGLE_APPLICATION_CREDENTIALS}" ]; then
  PROJECT="${PROJECT:-$(jq -r '.project_id // empty' "${GOOGLE_APPLICATION_CREDENTIALS}" 2>/dev/null || true)}"
else
  if command -v aws >/dev/null 2>&1; then
    _GKE_CRED_TMPDIR="$(mktemp -d)"
    if aws ssm get-parameter --name "/shared/gcp/service-account-json" --with-decryption \
         --query 'Parameter.Value' --output text > "${_GKE_CRED_TMPDIR}/sa.json" 2>/dev/null; then
      export GOOGLE_APPLICATION_CREDENTIALS="${_GKE_CRED_TMPDIR}/sa.json"
      export CLOUDSDK_CONFIG="${_GKE_CRED_TMPDIR}/gcloud-config"
      mkdir -p "${CLOUDSDK_CONFIG}"
      gcloud auth activate-service-account --key-file="${_GKE_CRED_TMPDIR}/sa.json" --quiet 2>/dev/null
      PROJECT="${PROJECT:-$(jq -r '.project_id // empty' "${_GKE_CRED_TMPDIR}/sa.json" 2>/dev/null || true)}"
    else
      rm -rf "${_GKE_CRED_TMPDIR}"
      _GKE_CRED_TMPDIR=""
    fi
  fi
fi
if [ -z "${PROJECT}" ]; then
  PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
fi
[ -n "${PROJECT}" ] || { echo "GCP PROJECT not resolved; set PROJECT= explicitly"; exit 1; }

_burst_vms() {
  local bid="$1"
  if [ "${BACKEND}" = "aws" ]; then
    local instances
    if ! instances="$(aws ec2 describe-instances \
      --region "${AWS_BURST_REGION}" \
      --filters "Name=tag:yscale-owner,Values=burst" \
                "Name=tag:yscale-burst-id,Values=${bid}" \
                "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)"; then
      echo "FAIL: unable to query EC2 burst instances" >&2
      return 2
    fi
    printf '%s' "${instances}"
  elif [ "${BACKEND}" = "gcp" ]; then
    local found rc=0
    found="$(BURST_ID="${bid}" ./cluster.sh burst-check)" || rc=$?
    case "${rc}" in
      0) return 0 ;;
      1) printf '%s' "${found}"; return 0 ;;
      *) echo "FAIL: unable to query GCE burst instances" >&2; return 2 ;;
    esac
  else
    local response
    if ! response="$(curl -fsS -H "Authorization: Bearer ${LINODE_TOKEN}" \
      'https://api.linode.com/v4/linode/instances?page_size=500')"; then
      echo "FAIL: unable to query Linode burst instances" >&2
      return 2
    fi
    local instances
    if ! instances="$(jq -r --arg bid "${bid}" \
      '.data[] | select((.tags // [] | index("yscale-burst")) and (.tags // [] | index($bid))) | .id' \
      <<<"${response}")"; then
      echo "FAIL: unable to decode Linode burst inventory" >&2
      return 2
    fi
    printf '%s' "${instances}"
  fi
}

_burst_cleanup() {
  local bid="$1"
  if [ "${BACKEND}" = "aws" ]; then
    local instances
    instances="$(_burst_vms "${bid}")"
    if [ -n "${instances}" ]; then
      echo "recovery-terminating EC2 instances: ${instances}" >&2
      local -a instance_ids
      read -r -a instance_ids <<< "${instances}"
      aws ec2 terminate-instances --region "${AWS_BURST_REGION}" \
        --instance-ids "${instance_ids[@]}" >/dev/null
    fi
  elif [ "${BACKEND}" = "gcp" ]; then
    BURST_ID="${bid}" ./cluster.sh burst-cleanup
  else
    local instances
    instances="$(_burst_vms "${bid}")"
    if [ -n "${instances}" ]; then
      echo "recovery-deleting Linode instances: ${instances}" >&2
      local -a instance_ids
      read -r -a instance_ids <<< "${instances}"
      local id
      for id in "${instance_ids[@]}"; do
        curl -fsS -X DELETE -H "Authorization: Bearer ${LINODE_TOKEN}" \
          "https://api.linode.com/v4/linode/instances/${id}" >/dev/null
      done
    fi
  fi
}

TEST_RC=0
_CLEANUP_RAN=false
CLUSTER_CREATE_STARTED=false
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

  # 1. Delete the Workload CR (tells central to stop the burst)
  if "${KUBECONFIG_OK}"; then
    echo "==> teardown: deleting Workload survive-probe"
    kubectl delete workload survive-probe -n "${SURVIVE_NS}" --ignore-not-found 2>/dev/null || true
  fi

  # 2. Wait bounded interval for central/provider to clean up burst VMs
  if [ -n "${burst_id}" ]; then
    echo "==> teardown: waiting up to 60s for central to clean burst ${burst_id}"
    local deadline=$(( $(date +%s) + 60 ))
    local remaining=""
    local query_rc=0
    while [ "$(date +%s)" -lt "${deadline}" ]; do
      query_rc=0
      remaining="$(_burst_vms "${burst_id}")" || query_rc=$?
      if [ "${query_rc}" -ne 0 ]; then
        TEST_RC=1
        break
      fi
      [ -z "${remaining}" ] && break
      sleep 10
    done

    # 3. Recovery deletion if VMs remain
    if [ "${query_rc}" -eq 0 ]; then
      remaining="$(_burst_vms "${burst_id}")" || query_rc=$?
    fi
    if [ "${query_rc}" -eq 0 ] && [ -n "${remaining}" ]; then
      echo "WARN: ${BACKEND} burst VMs remain after central cleanup window — recovery delete"
      _burst_cleanup "${burst_id}" || TEST_RC=1
    fi
  fi

  # 4. Kubernetes scratch resources
  if "${KUBECONFIG_OK}"; then
    kubectl delete pod burst-pod-proof -n "${SURVIVE_NS}" --ignore-not-found --wait=false 2>/dev/null || true
    if [ "${SURVIVE_NS}" != "default" ]; then
      kubectl delete ns "${SURVIVE_NS}" --ignore-not-found --wait=false 2>/dev/null || true
    fi
  fi

  # 5. Delete managed cluster
  if "${CLUSTER_CREATE_STARTED}"; then
    echo "==> teardown: deleting cluster ${CLUSTER} (guaranteed)"
    if ! ./cluster.sh delete; then
      echo "FAIL: cluster deletion failed — run 'make nuke'"
      TEST_RC=1
    fi
  fi

  # 6. Final burst-ID-scoped leak check
  if [ -n "${burst_id}" ]; then
    echo "==> teardown: final leak check (burst_id=${burst_id})"
    local leaked=""
    local final_query_rc=0
    leaked="$(_burst_vms "${burst_id}")" || final_query_rc=$?
    if [ "${final_query_rc}" -ne 0 ]; then
      echo "FAIL: final ${BACKEND} leak check could not be completed"
      TEST_RC=1
    elif [ -n "${leaked}" ]; then
      echo "LEAK: ${BACKEND} instances still running after teardown (burst_id=${burst_id}): ${leaked}"
      echo "  ACTION REQUIRED: manually terminate these instances."
      TEST_RC=1
    fi
  fi

  rm -f "${BURST_ID_FILE}"
  cleanup_gke_credentials
  echo "==> teardown complete (rc=${TEST_RC})"
  exit "${TEST_RC}"
}
trap cleanup EXIT

export CLUSTER ZONE REGION PROJECT RUN_ID

./cluster.sh preflight

existing_cluster="$(gcloud container clusters list --project="${PROJECT}" --zone="${ZONE}" \
  --filter="name=${CLUSTER}" --format='value(name)' 2>/dev/null)" || {
  echo "unable to inventory GKE clusters"; exit 1;
}
[ -z "${existing_cluster}" ] || {
  echo "refusing to reuse existing GKE cluster ${CLUSTER}"; exit 1;
}
case "${BACKEND}" in
  aws)
    if [ -n "${YSCALE_TOKEN:-}" ]; then
      command -v aws >/dev/null || { echo "aws not installed"; exit 1; }
      aws ec2 describe-regions --region "${AWS_BURST_REGION}" \
        --region-names "${AWS_BURST_REGION}" >/dev/null || {
        echo "AWS credentials cannot query EC2 in ${AWS_BURST_REGION}"; exit 1;
      }
    fi
    ;;
  gcp) ;;
  linode)
    if [ -n "${YSCALE_TOKEN:-}" ]; then
      [ -n "${LINODE_TOKEN:-}" ] || { echo "LINODE_TOKEN is required for exact Linode leak recovery"; exit 1; }
      command -v curl >/dev/null || { echo "curl not installed"; exit 1; }
    fi
    ;;
  *) echo "unsupported BACKEND=${BACKEND} (expected aws, gcp, or linode)"; exit 1 ;;
esac
if [ -n "${AGENT_PULL_SECRET}" ]; then
  [ -n "${SOURCE_KUBE_CONTEXT}" ] || { echo "SOURCE_KUBE_CONTEXT is required with AGENT_PULL_SECRET"; exit 1; }
fi
if [ -n "${AGENT_TS_AUTHKEY}${GATEWAY_TS_AUTHKEY}${TS_LOGIN_SERVER}" ]; then
  [ -n "${AGENT_TS_AUTHKEY}" ] && [ -n "${GATEWAY_TS_AUTHKEY}" ] && [ -n "${TS_LOGIN_SERVER}" ] || {
    echo "AGENT_TS_AUTHKEY, GATEWAY_TS_AUTHKEY, and TS_LOGIN_SERVER must be set together"
    exit 1
  }
fi

BURST_ID_FILE="$(mktemp)"

echo "==> run=$RUN_ID backend=$BACKEND max_test=${MAX_TEST_SECONDS}s"
echo "==> creating ${CLUSTER} in ${ZONE}"

( sleep "${MAX_TEST_SECONDS}" && echo "TIMEOUT: ${MAX_TEST_SECONDS}s exceeded — forcing teardown" && kill -TERM $$ 2>/dev/null ) &
WATCHDOG_PID=$!

CLUSTER_CREATE_STARTED=true
./cluster.sh up
KUBECONFIG_OK=true

echo "==> cluster ready:"
kubectl get nodes

if [ -n "${YSCALE_TOKEN:-}" ]; then
  kubectl create namespace yscale --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n yscale create secret generic yscale-agent-auth \
    --from-literal=YSCALE_TOKEN="${YSCALE_TOKEN}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  if [ -n "${AGENT_TS_AUTHKEY}" ]; then
    kubectl -n yscale create secret generic yscale-agent-tailnet \
      --from-literal=TS_AUTHKEY="${AGENT_TS_AUTHKEY}" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    kubectl -n yscale create secret generic yscale-gateway-tailnet \
      --from-literal=TS_AUTHKEY="${GATEWAY_TS_AUTHKEY}" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  fi
  if [ -n "${AGENT_PULL_SECRET}" ]; then
    kubectl --context "${SOURCE_KUBE_CONTEXT}" -n yscale get secret "${AGENT_PULL_SECRET}" -o json | \
      jq 'del(.metadata.creationTimestamp,.metadata.resourceVersion,.metadata.uid,.metadata.managedFields) | .metadata.namespace="yscale"' | \
      kubectl apply -f - >/dev/null
  fi
  api_server="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
  api_host="${api_server#*://}"
  api_host="${api_host%%:*}"
  dns_ip="$(kubectl -n kube-system get service kube-dns -o jsonpath='{.spec.clusterIP}')"
  pod_cidr="$(gcloud container clusters describe "${CLUSTER}" --zone="${ZONE}" --project="${PROJECT}" \
    --format='value(clusterIpv4Cidr)')"
  service_cidr="$(gcloud container clusters describe "${CLUSTER}" --zone="${ZONE}" --project="${PROJECT}" \
    --format='value(servicesIpv4Cidr)')"
  echo "==> installing yscale-agent (endpoint ${ENDPOINT})"
  helm_args=(upgrade --install yscale-agent ../helm/yscale-agent
    --namespace yscale --create-namespace \
    --set existingSecret=yscale-agent-auth \
    --set endpoint="${ENDPOINT}" \
    --set clusterID="${CLUSTER_ID}" \
    --set cloudProvider=gcp \
    --set bootstrap.authMode=gke \
    --set bootstrap.apiserverURL="${api_server}" \
    --set cluster.dnsIP="${dns_ip}" \
    --set persistence.enabled=false \
    --set gateway.enabled=true \
    --set gateway.enableForwardingViaInitContainer=true \
    --set "gateway.advertiseRoutes[0]=${pod_cidr}" \
    --set "gateway.advertiseRoutes[1]=${service_cidr}" \
    --set priorityClassName=yscale-burst-test \
    --set gateway.priorityClassName=yscale-burst-test \
    --set rbac.scope=cluster)
  if [ -n "${AGENT_PULL_SECRET}" ]; then
    helm_args+=(--set "imagePullSecrets[0].name=${AGENT_PULL_SECRET}")
  fi
  if [ -n "${AGENT_IMAGE_REPOSITORY}" ]; then
    helm_args+=(--set "agent.image.repository=${AGENT_IMAGE_REPOSITORY}")
  fi
  if [ -n "${AGENT_IMAGE_TAG}" ]; then
    helm_args+=(--set "agent.image.tag=${AGENT_IMAGE_TAG}")
  fi
  if [ -n "${AGENT_TS_AUTHKEY}" ]; then
    private_no_proxy="127.0.0.1,localhost,.svc,.cluster.local,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,.amazonaws.com,${api_host},169.254.169.254,metadata.google.internal,.googleapis.com"
    private_no_proxy_json="$(jq -Rn --arg value "${private_no_proxy}" '$value')"
    helm_args+=(
      --set tailscale.authKeySecret.name=yscale-agent-tailnet
      --set tailscale.tsLoginServer="${TS_LOGIN_SERVER}"
      --set tailscale.acceptRoutes=true
      --set tailscale.outboundHTTPProxy.enabled=true
      --set-json "tailscale.outboundHTTPProxy.noProxy=${private_no_proxy_json}"
      --set gateway.authKeySecret.name=yscale-gateway-tailnet
      --set gateway.tsLoginServer="${TS_LOGIN_SERVER}"
    )
  fi
  kubectl create priorityclass yscale-burst-test \
    --value=0 \
    --global-default=false \
    --description="Disposable yscale managed-cloud compatibility test" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  helm "${helm_args[@]}"
  kubectl -n yscale rollout status deploy/yscale-agent-yscale-agent --timeout=180s
  echo "==> waiting for connector stream to reach central"
  connect_deadline=$(( $(date +%s) + 180 ))
  until kubectl -n yscale logs deploy/yscale-agent-yscale-agent -c agent --tail=120 2>/dev/null | \
      grep -q 'connected to central'; do
    if [ "$(date +%s)" -gt "${connect_deadline}" ]; then
      echo "FAIL: connector did not reach central within 180s"
      exit 1
    fi
    sleep 5
  done
  echo "==> running burst compatibility test (backend=${BACKEND})"
  SURVIVE_CLEANUP=false BURST_ID_FILE="${BURST_ID_FILE}" BACKEND="${BACKEND}" JOIN_TIMEOUT="${JOIN_TIMEOUT}" \
    ../eks-test/survive-test.sh || TEST_RC=$?
  if [ -n "${HOLD:-}" ]; then
    echo "==> holding ${HOLD}s for manual testing; cluster tears down automatically after."
    sleep "${HOLD}"
  fi
else
  echo "==> no YSCALE_TOKEN set — bare-cluster smoke test only (no burst validation)."
fi

echo "==> test phase complete (rc=${TEST_RC}); teardown runs now via trap."
