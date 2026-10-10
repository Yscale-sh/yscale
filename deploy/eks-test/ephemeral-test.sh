#!/usr/bin/env bash
# Spin up an ephemeral EKS cluster, run yscale validation, and ALWAYS tear it
# down — even on failure or Ctrl-C — so a test run can never leak a paid
# cluster. This is the short-TTL guarantee: teardown is in a trap, not a step.
#
#   CONFIRM_SPEND=yes ./ephemeral-test.sh                          # bare cluster smoke test (no burst validation)
#   CONFIRM_SPEND=yes YSCALE_TOKEN=yscale_... BACKEND=linode ./ephemeral-test.sh
#   CONFIRM_SPEND=yes YSCALE_TOKEN=... HOLD=900 ./ephemeral-test.sh # hold 15 min for manual test
set -euo pipefail
cd "$(dirname "$0")"

CLUSTER="${CLUSTER:-yscale-eks-test}"
REGION="${REGION:-us-east-1}"
ENDPOINT="${ENDPOINT:-wss://api.yscale.sh}"
RUN_ID="${RUN_ID:-eks-$(date +%s)-$$}"
MAX_TEST_SECONDS="${MAX_TEST_SECONDS:-2700}"
BACKEND="${BACKEND:-linode}"
CLUSTER_ID="${CLUSTER_ID:-${RUN_ID}}"
SOURCE_KUBE_CONTEXT="${SOURCE_KUBE_CONTEXT:-}"
AGENT_PULL_SECRET="${AGENT_PULL_SECRET:-}"
AGENT_IMAGE_REPOSITORY="${AGENT_IMAGE_REPOSITORY:-}"
AGENT_IMAGE_TAG="${AGENT_IMAGE_TAG:-}"
AGENT_TS_AUTHKEY="${AGENT_TS_AUTHKEY:-}"
GATEWAY_TS_AUTHKEY="${GATEWAY_TS_AUTHKEY:-}"
TS_LOGIN_SERVER="${TS_LOGIN_SERVER:-}"
IAM_AGENT_ROLE_NAME="${IAM_AGENT_ROLE_NAME:-${CLUSTER}-yscale-agent}"
IAM_BOOTSTRAP_ROLE_NAME="${IAM_BOOTSTRAP_ROLE_NAME:-${CLUSTER}-yscale-bootstrap}"
IAM_AGENT_POLICY_NAME="yscale-assume-bootstrap"

SURVIVE_NS="default"
BURST_ID_FILE=""
IAM_AGENT_ROLE_ARN=""
IAM_BOOTSTRAP_ROLE_ARN=""
IAM_OIDC_PROVIDER_ARN=""
IAM_AGENT_ROLE_CREATED=false
IAM_BOOTSTRAP_ROLE_CREATED=false
IAM_AGENT_POLICY_CREATED=false
EKS_ACCESS_ENTRY_CREATED=false

if [ "${CONFIRM_SPEND:-}" != "yes" ]; then
  echo "This test creates a PAID EKS cluster (~\$0.15/hr). Set CONFIRM_SPEND=yes to proceed."
  exit 1
fi

command -v eksctl  >/dev/null || { echo "eksctl not installed: brew install eksctl"; exit 1; }
command -v aws     >/dev/null || { echo "aws cli not installed"; exit 1; }
command -v kubectl >/dev/null || { echo "kubectl not installed"; exit 1; }
command -v helm    >/dev/null || { echo "helm not installed"; exit 1; }
aws sts get-caller-identity >/dev/null || { echo "AWS creds not configured"; exit 1; }
existing_cluster="$(aws eks list-clusters --region "${REGION}" \
  --query "clusters[?@ == '${CLUSTER}']" --output text)" || {
  echo "unable to inventory EKS clusters"; exit 1;
}
[ -z "${existing_cluster}" ] || {
  echo "refusing to reuse existing EKS cluster ${CLUSTER}"; exit 1;
}
case "${BACKEND}" in
  aws) ;;
  linode)
    if [ -n "${YSCALE_TOKEN:-}" ]; then
      [ -n "${LINODE_TOKEN:-}" ] || { echo "LINODE_TOKEN is required for exact Linode leak recovery"; exit 1; }
      command -v curl >/dev/null || { echo "curl not installed"; exit 1; }
      command -v jq >/dev/null || { echo "jq not installed"; exit 1; }
    fi
    ;;
  *) echo "unsupported BACKEND=${BACKEND} (expected aws or linode)"; exit 1 ;;
esac
if [ -n "${AGENT_PULL_SECRET}" ]; then
  [ -n "${SOURCE_KUBE_CONTEXT}" ] || { echo "SOURCE_KUBE_CONTEXT is required with AGENT_PULL_SECRET"; exit 1; }
  command -v jq >/dev/null || { echo "jq not installed"; exit 1; }
fi
if [ -n "${AGENT_TS_AUTHKEY}${GATEWAY_TS_AUTHKEY}${TS_LOGIN_SERVER}" ]; then
  [ -n "${AGENT_TS_AUTHKEY}" ] && [ -n "${GATEWAY_TS_AUTHKEY}" ] && [ -n "${TS_LOGIN_SERVER}" ] || {
    echo "AGENT_TS_AUTHKEY, GATEWAY_TS_AUTHKEY, and TS_LOGIN_SERVER must be set together"
    exit 1
  }
fi
if [ -n "${YSCALE_TOKEN:-}" ]; then
  command -v jq >/dev/null || { echo "jq not installed"; exit 1; }
  if aws iam get-role --role-name "${IAM_AGENT_ROLE_NAME}" >/dev/null 2>&1 ||
     aws iam get-role --role-name "${IAM_BOOTSTRAP_ROLE_NAME}" >/dev/null 2>&1; then
    echo "refusing to reuse existing EKS bootstrap IAM roles; run 'make nuke' after verifying ownership"
    exit 1
  fi
fi

BURST_ID_FILE="$(mktemp)"

_burst_vms() {
  local bid="$1"
  local instances
  if [ "${BACKEND}" = "aws" ]; then
    if ! instances="$(aws ec2 describe-instances \
      --region "${REGION}" \
      --filters "Name=tag:yscale-owner,Values=burst" \
                "Name=tag:yscale-burst-id,Values=${bid}" \
                "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)"; then
      echo "FAIL: unable to query EC2 burst instances" >&2
      return 2
    fi
  else
    local response
    if ! response="$(curl -fsS -H "Authorization: Bearer ${LINODE_TOKEN}" \
      'https://api.linode.com/v4/linode/instances?page_size=500')"; then
      echo "FAIL: unable to query Linode burst instances" >&2
      return 2
    fi
    if ! instances="$(jq -r --arg bid "${bid}" \
      '.data[] | select((.tags // [] | index("yscale-burst")) and (.tags // [] | index($bid))) | .id' \
      <<<"${response}")"; then
      echo "FAIL: unable to decode Linode burst inventory" >&2
      return 2
    fi
  fi
  printf '%s' "${instances}"
}

_burst_cleanup() {
  local bid="$1"
  local instances
  instances="$(_burst_vms "${bid}")"
  if [ -n "${instances}" ]; then
    echo "recovery-deleting ${BACKEND} instances: ${instances}" >&2
    local -a instance_ids
    read -r -a instance_ids <<< "${instances}"
    if [ "${BACKEND}" = "aws" ]; then
      aws ec2 terminate-instances --instance-ids "${instance_ids[@]}" --region "${REGION}" >/dev/null
    else
      local id
      for id in "${instance_ids[@]}"; do
        curl -fsS -X DELETE -H "Authorization: Bearer ${LINODE_TOKEN}" \
          "https://api.linode.com/v4/linode/instances/${id}" >/dev/null
      done
    fi
  fi
}

_configure_eks_bootstrap_iam() {
  local account_id oidc_issuer oidc_provider trust policy bootstrap_trust
  account_id="$(aws sts get-caller-identity --query Account --output text)"
  oidc_issuer="$(aws eks describe-cluster --name "${CLUSTER}" --region "${REGION}" \
    --query 'cluster.identity.oidc.issuer' --output text)"
  oidc_provider="${oidc_issuer#https://}"
  IAM_OIDC_PROVIDER_ARN="arn:aws:iam::${account_id}:oidc-provider/${oidc_provider}"

  trust="$(jq -nc \
    --arg provider "${IAM_OIDC_PROVIDER_ARN}" \
    --arg aud "${oidc_provider}:aud" \
    --arg sub "${oidc_provider}:sub" \
    '{Version:"2012-10-17",Statement:[{Effect:"Allow",Principal:{Federated:$provider},Action:"sts:AssumeRoleWithWebIdentity",Condition:{StringEquals:{($aud):"sts.amazonaws.com",($sub):"system:serviceaccount:yscale:yscale-agent-yscale-agent"}}}]}')"
  IAM_AGENT_ROLE_ARN="$(aws iam create-role \
    --role-name "${IAM_AGENT_ROLE_NAME}" \
    --assume-role-policy-document "${trust}" \
    --query 'Role.Arn' --output text)"
  IAM_AGENT_ROLE_CREATED=true

  bootstrap_trust="$(jq -nc --arg role "${IAM_AGENT_ROLE_ARN}" \
    '{Version:"2012-10-17",Statement:[{Effect:"Allow",Principal:{AWS:$role},Action:"sts:AssumeRole"}]}')"
  local attempt
  for attempt in 1 2 3 4 5 6; do
    if IAM_BOOTSTRAP_ROLE_ARN="$(aws iam create-role \
      --role-name "${IAM_BOOTSTRAP_ROLE_NAME}" \
      --assume-role-policy-document "${bootstrap_trust}" \
      --query 'Role.Arn' --output text 2>/dev/null)"; then
      IAM_BOOTSTRAP_ROLE_CREATED=true
      break
    fi
    sleep 10
  done
  if ! "${IAM_BOOTSTRAP_ROLE_CREATED}"; then
    echo "failed to create EKS bootstrap role after IAM propagation window" >&2
    return 1
  fi

  policy="$(jq -nc --arg role "${IAM_BOOTSTRAP_ROLE_ARN}" \
    '{Version:"2012-10-17",Statement:[{Effect:"Allow",Action:"sts:AssumeRole",Resource:$role}]}')"
  aws iam put-role-policy \
    --role-name "${IAM_AGENT_ROLE_NAME}" \
    --policy-name "${IAM_AGENT_POLICY_NAME}" \
    --policy-document "${policy}"
  IAM_AGENT_POLICY_CREATED=true

  for attempt in 1 2 3 4 5 6; do
    if aws eks create-access-entry \
      --cluster-name "${CLUSTER}" \
      --region "${REGION}" \
      --principal-arn "${IAM_BOOTSTRAP_ROLE_ARN}" \
      --type HYBRID_LINUX >/dev/null 2>&1; then
      EKS_ACCESS_ENTRY_CREATED=true
      return 0
    fi
    sleep 10
  done
  echo "failed to create EKS bootstrap access entry after IAM propagation window" >&2
  return 1
}

_cleanup_eks_bootstrap_iam() {
  local failed=false
  if "${EKS_ACCESS_ENTRY_CREATED}"; then
    aws eks delete-access-entry --cluster-name "${CLUSTER}" --region "${REGION}" \
      --principal-arn "${IAM_BOOTSTRAP_ROLE_ARN}" >/dev/null 2>&1 || true
    EKS_ACCESS_ENTRY_CREATED=false
  fi
  if "${IAM_AGENT_POLICY_CREATED}"; then
    aws iam delete-role-policy --role-name "${IAM_AGENT_ROLE_NAME}" \
      --policy-name "${IAM_AGENT_POLICY_NAME}" >/dev/null 2>&1 || failed=true
    IAM_AGENT_POLICY_CREATED=false
  fi
  if "${IAM_BOOTSTRAP_ROLE_CREATED}"; then
    local attempt
    for attempt in 1 2 3 4 5 6; do
      if aws iam delete-role --role-name "${IAM_BOOTSTRAP_ROLE_NAME}" >/dev/null 2>&1; then
        IAM_BOOTSTRAP_ROLE_CREATED=false
        break
      fi
      sleep 5
    done
    "${IAM_BOOTSTRAP_ROLE_CREATED}" && failed=true
  fi
  if "${IAM_AGENT_ROLE_CREATED}"; then
    local attempt
    for attempt in 1 2 3 4 5 6; do
      if aws iam delete-role --role-name "${IAM_AGENT_ROLE_NAME}" >/dev/null 2>&1; then
        IAM_AGENT_ROLE_CREATED=false
        break
      fi
      sleep 5
    done
    "${IAM_AGENT_ROLE_CREATED}" && failed=true
  fi
  if [ -n "${IAM_OIDC_PROVIDER_ARN}" ]; then
    aws iam delete-open-id-connect-provider --open-id-connect-provider-arn "${IAM_OIDC_PROVIDER_ARN}" >/dev/null 2>&1 || true
    IAM_OIDC_PROVIDER_ARN=""
  fi
  if "${failed}"; then
    echo "FAIL: EKS bootstrap IAM cleanup incomplete" >&2
    return 1
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

  if "${KUBECONFIG_OK}"; then
    kubectl delete pod burst-pod-proof -n "${SURVIVE_NS}" --ignore-not-found --wait=false 2>/dev/null || true
    if [ "${SURVIVE_NS}" != "default" ]; then
      kubectl delete ns "${SURVIVE_NS}" --ignore-not-found --wait=false 2>/dev/null || true
    fi
  fi

  # 4. Delete managed cluster
  if "${CLUSTER_CREATE_STARTED}"; then
    echo "==> teardown: deleting cluster ${CLUSTER} (guaranteed)"
    if ! eksctl delete cluster --name "${CLUSTER}" --region "${REGION}" --wait; then
      echo "FAIL: cluster deletion failed — run 'make nuke'"
      TEST_RC=1
    fi
  fi

  # 5. Remove the two short-lived IAM roles used only for EKS bootstrap.
  # The access entry is cluster-owned, but delete it explicitly when the API
  # still exists and always remove role policies/roles after cluster teardown.
  _cleanup_eks_bootstrap_iam || TEST_RC=1

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
  echo "==> teardown complete (rc=${TEST_RC})"
  exit "${TEST_RC}"
}
trap cleanup EXIT

echo "==> run=$RUN_ID backend=$BACKEND max_test=${MAX_TEST_SECONDS}s"
echo "==> creating ${CLUSTER} (control plane ~9m, full cluster ~15m)"

( sleep "${MAX_TEST_SECONDS}" && echo "TIMEOUT: ${MAX_TEST_SECONDS}s exceeded — forcing teardown" && kill -TERM $$ 2>/dev/null ) &
WATCHDOG_PID=$!

CLUSTER_CREATE_STARTED=true
eksctl create cluster -f cluster.yaml
KUBECONFIG_OK=true

echo "==> cluster ready:"
kubectl get nodes

if [ -n "${YSCALE_TOKEN:-}" ]; then
  echo "==> configuring EKS IAM bootstrap identity"
  _configure_eks_bootstrap_iam
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
  dns_ip="$(kubectl -n kube-system get service kube-dns -o jsonpath='{.spec.clusterIP}')"
  service_cidr="$(aws eks describe-cluster --name "${CLUSTER}" --region "${REGION}" \
    --query 'cluster.kubernetesNetworkConfig.serviceIpv4Cidr' --output text)"
  vpc_id="$(aws eks describe-cluster --name "${CLUSTER}" --region "${REGION}" \
    --query 'cluster.resourcesVpcConfig.vpcId' --output text)"
  vpc_cidr="$(aws ec2 describe-vpcs --vpc-ids "${vpc_id}" --region "${REGION}" \
    --query 'Vpcs[0].CidrBlock' --output text)"
  echo "==> installing yscale-agent (endpoint ${ENDPOINT})"
  helm_args=(upgrade --install yscale-agent ../helm/yscale-agent
    --namespace yscale --create-namespace \
    --set existingSecret=yscale-agent-auth \
    --set endpoint="${ENDPOINT}" \
    --set clusterID="${CLUSTER_ID}" \
    --set cloudProvider=aws \
    --set bootstrap.authMode=eks \
    --set bootstrap.eks.clusterName="${CLUSTER}" \
    --set bootstrap.eks.region="${REGION}" \
    --set bootstrap.eks.bootstrapRoleARN="${IAM_BOOTSTRAP_ROLE_ARN}" \
    --set bootstrap.apiserverURL="${api_server}" \
    --set cluster.dnsIP="${dns_ip}" \
    --set persistence.enabled=false \
    --set gateway.enabled=true \
    --set gateway.enableForwardingViaInitContainer=true \
    --set "gateway.advertiseRoutes[0]=${vpc_cidr}" \
    --set "gateway.advertiseRoutes[1]=${service_cidr}" \
    --set rbac.scope=cluster \
    --set-string "serviceAccount.annotations.eks\.amazonaws\.com/role-arn=${IAM_AGENT_ROLE_ARN}")
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
    helm_args+=(
      --set tailscale.authKeySecret.name=yscale-agent-tailnet
      --set tailscale.tsLoginServer="${TS_LOGIN_SERVER}"
      --set tailscale.outboundHTTPProxy.enabled=true
      --set gateway.authKeySecret.name=yscale-gateway-tailnet
      --set gateway.tsLoginServer="${TS_LOGIN_SERVER}"
    )
  fi
  helm "${helm_args[@]}"
  kubectl -n yscale rollout status deploy/yscale-agent-yscale-agent --timeout=180s
  echo "==> running burst compatibility test (backend=${BACKEND})"
  SURVIVE_CLEANUP=false BURST_ID_FILE="${BURST_ID_FILE}" BACKEND="${BACKEND}" \
    ./survive-test.sh || TEST_RC=$?
  if [ -n "${HOLD:-}" ]; then
    echo "==> holding ${HOLD}s for manual testing; cluster tears down automatically after."
    sleep "${HOLD}"
  fi
else
  echo "==> no YSCALE_TOKEN set — bare-cluster smoke test only (no burst validation)."
fi

echo "==> test phase complete (rc=${TEST_RC}); teardown runs now via trap."
