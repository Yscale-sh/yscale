#!/usr/bin/env bash
# Deterministic local tests for the managed CPU burst harness.
# No cloud commands run; no credentials required; no spend.
set -euo pipefail
cd "$(dirname "$0")/.."

PASS=0
FAIL=0

pass() { echo "  PASS: $1"; PASS=$(( PASS + 1 )); }
fail() { echo "  FAIL: $1"; FAIL=$(( FAIL + 1 )); }

echo "=== Shell syntax (bash -n) ==="
for f in eks-test/ephemeral-test.sh eks-test/survive-test.sh eks-test/watch-burst-nodes.sh \
         gke-test/cluster.sh gke-test/ephemeral-test.sh \
         aks-test/cluster.sh aks-test/ephemeral-test.sh \
         lke-test/lke.sh lke-test/inventory.sh lke-test/verify.sh; do
  if bash -n "$f" 2>/dev/null; then
    pass "syntax: $f"
  else
    fail "syntax: $f"
  fi
done

echo ""
echo "=== Executable modes ==="
for f in eks-test/ephemeral-test.sh eks-test/survive-test.sh eks-test/watch-burst-nodes.sh \
         gke-test/cluster.sh gke-test/ephemeral-test.sh \
         aks-test/cluster.sh aks-test/ephemeral-test.sh \
         lke-test/lke.sh lke-test/verify.sh; do
  if [ -x "$f" ]; then
    pass "executable: $f"
  else
    fail "executable: $f (mode=$(stat -c %a "$f" 2>/dev/null || stat -f %Lp "$f" 2>/dev/null))"
  fi
done

echo ""
echo "=== No-spend rejection ==="
for f in eks-test/ephemeral-test.sh gke-test/ephemeral-test.sh aks-test/ephemeral-test.sh lke-test/verify.sh; do
  out="$(CONFIRM_SPEND="" bash "$f" 2>&1 || true)"
  if echo "${out}" | grep -q "CONFIRM_SPEND=yes"; then
    pass "no-spend gate: $f"
  else
    fail "no-spend gate: $f (did not reject without CONFIRM_SPEND)"
  fi
done

echo ""
echo "=== LKE paginated inventory ==="
if (
  # shellcheck source=deploy/lke-test/inventory.sh
  source lke-test/inventory.sh
  fake_central_curl() { printf '204\n'; }
  CENTRAL_CURL_BIN=fake_central_curl central_auth_check wss://central.example token-redacted
); then
  pass "LKE central preflight: accepts the dedicated HTTP 204 auth proof"
else
  fail "LKE central preflight: rejected a valid auth proof"
fi
if (
  # shellcheck source=deploy/lke-test/inventory.sh
  source lke-test/inventory.sh
  denied_central_curl() { printf '401\n'; }
  CENTRAL_CURL_BIN=denied_central_curl central_auth_check wss://central.example token-redacted >/dev/null 2>&1
); then
  fail "LKE central preflight: accepted a denied credential"
else
  pass "LKE central preflight: fails closed on denied credentials"
fi

lke_inventory_ids="$({
  # shellcheck source=deploy/lke-test/inventory.sh
  source lke-test/inventory.sh
  fake_linode_curl() {
    local url="${!#}"
    case "${url}" in
      *'/linode/instances?'*'page=1&'*)
        printf '%s\n' '{"data":[{"id":11,"tags":["yscale-burst","other"]}],"page":1,"pages":2}' ;;
      *'/linode/instances?'*'page=2&'*)
        printf '%s\n' '{"data":[{"id":22,"tags":["yscale-burst","burst-exact"]},{"id":23,"tags":["yscale-burst","burst-exact-extra"]}],"page":2,"pages":2}' ;;
      *) return 1 ;;
    esac
  }
  LINODE_TOKEN=test LINODE_CURL_BIN=fake_linode_curl linode_burst_instance_ids burst-exact
})" || lke_inventory_ids="inventory-error"
if [ "${lke_inventory_ids}" = "22" ]; then
  pass "LKE inventory: walks every page and matches exact burst tags"
else
  fail "LKE inventory: pagination/exact match returned ${lke_inventory_ids}"
fi

if (
  # shellcheck source=deploy/lke-test/inventory.sh
  source lke-test/inventory.sh
  invalid_linode_curl() {
    printf '%s\n' '{"data":[],"page":9,"pages":9}'
  }
  LINODE_TOKEN=test LINODE_CURL_BIN=invalid_linode_curl linode_burst_instance_ids burst-exact >/dev/null 2>&1
); then
  fail "LKE inventory: accepted invalid pagination metadata"
else
  pass "LKE inventory: fails closed on invalid pagination metadata"
fi

echo ""
echo "=== LKE release workflow ==="
if grep -q 'workflow_dispatch:' ../.github/workflows/lke-release-gate.yaml && \
   ! grep -q 'schedule:' ../.github/workflows/lke-release-gate.yaml && \
   grep -q "github.ref == 'refs/heads/staging'" ../.github/workflows/lke-release-gate.yaml && \
   grep -q '/shared/linode/admin-api-token' ../.github/workflows/lke-release-gate.yaml && \
   grep -q 'runs-on: \[self-hosted, yscale-repo\]' ../.github/workflows/lke-release-gate.yaml; then
  pass "LKE workflow: staging-only manual dispatch on the self-hosted runner"
else
  fail "LKE workflow: dispatch or self-hosted constraints missing"
fi
if grep -q 'REAP_WAIT_SECONDS: "600"' ../.github/workflows/lke-release-gate.yaml && \
   grep -q 'central_auth_check' lke-test/verify.sh && \
   grep -q 'authoritative deletion timed out' lke-test/verify.sh && \
   grep -q 'linode_exact_instance_count' lke-test/verify.sh && \
   grep -q 'linode_attached_volume_ids' lke-test/verify.sh && \
   grep -q 'linode_firewall_device_ids' lke-test/verify.sh && \
   grep -q 'fabric_node_count' lke-test/verify.sh; then
  pass "LKE workflow: bounded authoritative reap and exact residue audit"
else
  fail "LKE workflow: teardown audit contract incomplete"
fi
if grep -q 'NODE_NAME_FILE' eks-test/survive-test.sh && \
   grep -q 'PROVIDER_ID_FILE' eks-test/survive-test.sh && \
   grep -q "jsonpath='{.spec.providerID}'" eks-test/survive-test.sh; then
  pass "survive-test.sh: exports exact node and provider identity for teardown"
else
  fail "survive-test.sh: exact teardown identity outputs missing"
fi
out="$(CONFIRM_SPEND=yes YSCALE_TOKEN=redacted bash aks-test/ephemeral-test.sh 2>&1 || true)"
if echo "${out}" | grep -q 'CENTRAL_AZURE_SCOPE_VERIFIED=yes'; then
  pass "AKS scope gate: real burst rejects unverified remote central scope"
else
  fail "AKS scope gate: real burst can reach cloud preflight without scope verification"
fi

echo ""
echo "=== GCP SSM parameter path ==="
if grep -q '/shared/gcp/service-account-json' gke-test/cluster.sh; then
  pass "GCP SSM path: /shared/gcp/service-account-json"
else
  fail "GCP SSM path: wrong SSM parameter path"
fi
if grep -q '/shared/gcp/sa-key-json' gke-test/cluster.sh; then
  fail "GCP SSM path: old /shared/gcp/sa-key-json still present"
else
  pass "GCP SSM path: old path removed"
fi

echo ""
echo "=== GCP credential isolation ==="
if grep -q 'CLOUDSDK_CONFIG' gke-test/cluster.sh; then
  pass "GCP cred isolation: CLOUDSDK_CONFIG set"
else
  fail "GCP cred isolation: CLOUDSDK_CONFIG not set"
fi
if grep -q 'mktemp -d' gke-test/cluster.sh; then
  pass "GCP cred isolation: temp dir used"
else
  fail "GCP cred isolation: no temp dir"
fi
if grep -q '_gcp_cleanup' gke-test/cluster.sh; then
  pass "GCP cred isolation: cleanup trap present"
else
  fail "GCP cred isolation: no cleanup trap"
fi
if grep -q 'project_id' gke-test/cluster.sh; then
  pass "GCP cred isolation: project_id derived from SA JSON"
else
  fail "GCP cred isolation: project_id not derived"
fi

echo ""
echo "=== Burst ID tags (not RUN_ID) ==="
# EKS must use yscale-owner=burst + yscale-burst-id
if grep -q 'yscale-owner,Values=burst' eks-test/ephemeral-test.sh && \
   grep -q 'yscale-burst-id' eks-test/ephemeral-test.sh; then
  pass "EKS burst tags: yscale-owner=burst + yscale-burst-id"
else
  fail "EKS burst tags: wrong tag names"
fi
if grep -q 'index("yscale-burst")' eks-test/ephemeral-test.sh && \
   grep -q 'index($bid)' eks-test/ephemeral-test.sh && \
   grep -q '/v4/linode/instances/${id}' eks-test/ephemeral-test.sh; then
  pass "EKS Linode cleanup: exact owner + burst ID tags"
else
  fail "EKS Linode cleanup: exact-tag recovery delete missing"
fi
if grep -q 'BACKEND="${BACKEND}"' eks-test/ephemeral-test.sh && \
   grep -q 'BACKEND="${BACKEND:-linode}"' eks-test/ephemeral-test.sh && \
   ! grep -q 'BACKEND=aws.*survive-test' eks-test/ephemeral-test.sh; then
  pass "EKS backend selection: defaults to Linode and reaches survive test"
else
  fail "EKS backend selection: Linode is not the default real-burst path"
fi
if grep -q 'persistence.enabled=false' eks-test/ephemeral-test.sh && \
   grep -q 'gateway.enabled=true' eks-test/ephemeral-test.sh && \
   grep -q 'gateway.enableForwardingViaInitContainer=true' eks-test/ephemeral-test.sh && \
   grep -q 'gateway.advertiseRoutes\[0\].*vpc_cidr' eks-test/ephemeral-test.sh && \
   grep -q 'gateway.advertiseRoutes\[1\].*service_cidr' eks-test/ephemeral-test.sh && \
   grep -q 'SURVIVE_NS="default"' eks-test/ephemeral-test.sh && \
   grep -q 'deploy/yscale-agent-yscale-agent' eks-test/ephemeral-test.sh; then
  pass "EKS agent install: authorized namespace and managed full-tier routes"
else
  fail "EKS agent install: managed-cluster overrides are incomplete"
fi
if grep -q 'JOIN_TIMEOUT="${JOIN_TIMEOUT:-600}"' eks-test/survive-test.sh; then
  pass "EKS burst join: allows 600 seconds for baked-image startup"
else
  fail "EKS burst join: timeout regressed below 600 seconds"
fi
if grep -q 'authenticationMode: API_AND_CONFIG_MAP' eks-test/cluster.yaml && \
   grep -q 'bootstrap.authMode=eks' eks-test/ephemeral-test.sh && \
   grep -q -- '--type HYBRID_LINUX' eks-test/ephemeral-test.sh && \
   ! grep -q -- '--type STANDARD' eks-test/ephemeral-test.sh && \
   ! grep -q -- '--kubernetes-groups yscale:bootstrappers' eks-test/ephemeral-test.sh; then
  pass "EKS bootstrap auth: native hybrid-node access entry"
else
  fail "EKS bootstrap auth: access-entry contract is incomplete"
fi
if grep -q '_cleanup_eks_bootstrap_iam || TEST_RC=1' eks-test/ephemeral-test.sh && \
   grep -q 'delete-role-policy' eks-test/ephemeral-test.sh && \
   grep -q 'delete-role --role-name "${IAM_BOOTSTRAP_ROLE_NAME}"' eks-test/ephemeral-test.sh && \
   grep -q 'delete-role --role-name "${IAM_AGENT_ROLE_NAME}"' eks-test/ephemeral-test.sh; then
  pass "EKS bootstrap auth: temporary IAM roles are trap-cleaned"
else
  fail "EKS bootstrap auth: temporary IAM role cleanup is incomplete"
fi

echo ""
echo "=== GKE bootstrap Helm RBAC ==="
# GKE mode RBAC: conditional SA/CRB grants must appear only when authMode=gke
if grep -q 'eq .Values.bootstrap.authMode "gke"' helm/yscale-agent/templates/rbac.yaml && \
   grep -q 'serviceaccounts\]' helm/yscale-agent/templates/rbac.yaml && \
   grep -q 'serviceaccounts/token' helm/yscale-agent/templates/rbac.yaml && \
   grep -q 'clusterrolebindings' helm/yscale-agent/templates/rbac.yaml; then
  pass "GKE Helm RBAC: conditional SA + CRB permissions gated on authMode=gke"
else
  fail "GKE Helm RBAC: missing conditional GKE bootstrap permissions"
fi
# The GKE ClusterRole may bind only the built-in node bootstrapper role.
if grep -q 'resources: \[clusterrolebindings\]' helm/yscale-agent/templates/rbac.yaml && \
   grep -q 'resources: \[clusterroles\]' helm/yscale-agent/templates/rbac.yaml && \
   grep -q 'resourceNames: \[system:node-bootstrapper\]' helm/yscale-agent/templates/rbac.yaml && \
   grep -q 'verbs: \[bind\]' helm/yscale-agent/templates/rbac.yaml; then
  pass "GKE Helm RBAC: CRB lifecycle and bind restricted to node-bootstrapper"
else
  fail "GKE Helm RBAC: CRB or restricted node-bootstrapper bind grant missing"
fi
# GKE mode must be validated in the deployment template
if grep -q 'bootstrap.authMode.*gke' helm/yscale-agent/templates/deployment.yaml; then
  pass "GKE Helm deployment: authMode=gke is a valid bootstrap mode"
else
  fail "GKE Helm deployment: authMode=gke not validated"
fi

echo ""
echo "=== Baked burst networking contracts ==="
if grep -q 'mountpoint -q /sys/fs/bpf || mount -t bpf' ../pkg/backends/linode/bootstrap-baked.sh && \
   grep -q 'mountpoint -q /sys/fs/bpf || mount -t bpf' ../burst/image/entrypoint-kubelet.sh; then
  pass "Cilium BPF mount: already-mounted filesystems are not stacked"
else
  fail "Cilium BPF mount: duplicate mount guard missing"
fi
if grep -q '/usr/local/bin/yscale-eks-credential' ../pkg/backends/linode/bootstrap-baked.sh && \
   grep -q 'client.authentication.k8s.io/v1beta1' ../pkg/backends/linode/bootstrap-baked.sh && \
   grep -q 'interactiveMode: Never' ../pkg/backends/linode/bootstrap-baked.sh; then
  pass "EKS baked bootstrap: renewable IAM exec credential configured"
else
  fail "EKS baked bootstrap: renewable IAM credential contract missing"
fi
for crd in ciliumloadbalancerippools ciliumcidrgroups; do
  if [ -s "helm/yscale-agent/files/cilium-crds/${crd}.yaml" ] && \
     grep -q -- "- ${crd}" helm/yscale-agent/templates/cilium-scaffolding.yaml; then
    pass "Cilium scaffold: ${crd} CRD and RBAC present"
  else
    fail "Cilium scaffold: ${crd} CRD or RBAC missing"
  fi
done
eks_kube_guard=$(grep -n 'KUBECONFIG_OK.*=.*false' eks-test/ephemeral-test.sh | head -1 | cut -d: -f1)
eks_first_kube_delete=$(grep -n 'kubectl delete' eks-test/ephemeral-test.sh | head -1 | cut -d: -f1)
if [ -n "${eks_kube_guard}" ] && [ -n "${eks_first_kube_delete}" ] && \
   [ "${eks_kube_guard}" -lt "${eks_first_kube_delete}" ]; then
  pass "EKS failed create: Kubernetes cleanup requires acquired kubeconfig"
else
  fail "EKS failed create: cleanup can mutate the previous kubecontext"
fi

# GKE must use ys-owner=yscale-burst + ys-burst-id
if grep -q 'ys-owner=yscale-burst' gke-test/cluster.sh && \
   grep -q 'ys-burst-id' gke-test/cluster.sh; then
  pass "GKE burst tags: ys-owner=yscale-burst + ys-burst-id"
else
  fail "GKE burst tags: wrong label names"
fi

echo ""
echo "=== GKE Linode burst parity ==="
if grep -q 'BACKEND="${BACKEND:-linode}"' gke-test/ephemeral-test.sh && \
   grep -q 'BACKEND="${BACKEND}"' gke-test/ephemeral-test.sh && \
   ! grep -q 'BACKEND=gcp.*survive-test' gke-test/ephemeral-test.sh; then
  pass "GKE backend selection: defaults to Linode and reaches survive test"
else
  fail "GKE backend selection: Linode is not the default real-burst path"
fi
if grep -q 'JOIN_TIMEOUT="${JOIN_TIMEOUT:-600}"' gke-test/ephemeral-test.sh && \
   grep -q 'JOIN_TIMEOUT="${JOIN_TIMEOUT}"' gke-test/ephemeral-test.sh; then
  pass "GKE burst join: pins and propagates the 600 second node timeout"
else
  fail "GKE burst join: 600 second node timeout is not explicit"
fi
if grep -q 'index("yscale-burst")' gke-test/ephemeral-test.sh && \
   grep -q 'index($bid)' gke-test/ephemeral-test.sh && \
   grep -q '/v4/linode/instances/${id}' gke-test/ephemeral-test.sh; then
  pass "GKE Linode cleanup: exact owner + burst ID tags"
else
  fail "GKE Linode cleanup: exact-tag recovery delete missing"
fi
if grep -q 'AWS_BURST_REGION="${AWS_BURST_REGION:-us-east-1}"' gke-test/ephemeral-test.sh && \
   grep -q 'Name=tag:yscale-owner,Values=burst' gke-test/ephemeral-test.sh && \
   grep -q 'Name=tag:yscale-burst-id,Values=${bid}' gke-test/ephemeral-test.sh && \
   grep -q 'aws ec2 terminate-instances' gke-test/ephemeral-test.sh; then
  pass "GKE EC2 cleanup: exact owner + burst ID tags and recovery termination"
else
  fail "GKE EC2 cleanup: exact-tag recovery termination missing"
fi
if grep -q 'existingSecret=yscale-agent-auth' gke-test/ephemeral-test.sh && \
   ! grep -q -- '--set token=' gke-test/ephemeral-test.sh; then
  pass "GKE agent install: YSCALE_TOKEN stored in existingSecret"
else
  fail "GKE agent install: YSCALE_TOKEN exposed in Helm values"
fi
if grep -q 'persistence.enabled=false' gke-test/ephemeral-test.sh && \
   grep -q 'gateway.enabled=true' gke-test/ephemeral-test.sh && \
   grep -q 'gateway.enableForwardingViaInitContainer=true' gke-test/ephemeral-test.sh && \
   grep -q 'gateway.advertiseRoutes\[0\].*pod_cidr' gke-test/ephemeral-test.sh && \
   grep -q 'gateway.advertiseRoutes\[1\].*service_cidr' gke-test/ephemeral-test.sh && \
   grep -q 'SURVIVE_NS="default"' gke-test/ephemeral-test.sh && \
   grep -q 'deploy/yscale-agent-yscale-agent' gke-test/ephemeral-test.sh; then
  pass "GKE agent install: authorized namespace and managed full-tier routes"
else
  fail "GKE agent install: managed-cluster overrides are incomplete"
fi
gke_kube_guard=$(grep -n 'KUBECONFIG_OK.*=.*false' gke-test/ephemeral-test.sh | head -1 | cut -d: -f1)
gke_first_kube_delete=$(grep -n 'kubectl delete' gke-test/ephemeral-test.sh | head -1 | cut -d: -f1)
if [ -n "${gke_kube_guard}" ] && [ -n "${gke_first_kube_delete}" ] && \
   [ "${gke_kube_guard}" -lt "${gke_first_kube_delete}" ]; then
  pass "GKE failed create: Kubernetes cleanup requires acquired kubeconfig"
else
  fail "GKE failed create: cleanup can mutate the previous kubecontext"
fi
if grep -q 'cloudProvider=gcp' gke-test/ephemeral-test.sh && \
   grep -q 'bootstrap.authMode=gke' gke-test/ephemeral-test.sh && \
   grep -q 'rbac.scope=cluster' gke-test/ephemeral-test.sh; then
  pass "GKE agent install: GKE-specific bootstrap and RBAC"
else
  fail "GKE agent install: GKE-specific bootstrap or RBAC missing"
fi
if grep -q 'create priorityclass yscale-burst-test' gke-test/ephemeral-test.sh && \
   grep -q 'priorityClassName=yscale-burst-test' gke-test/ephemeral-test.sh && \
   grep -q 'gateway.priorityClassName=yscale-burst-test' gke-test/ephemeral-test.sh; then
  pass "GKE scheduling: connector and gateway use an allowed disposable priority class"
else
  fail "GKE scheduling: reserved system priority classes are not overridden"
fi
if grep -q 'tailscale.acceptRoutes=true' gke-test/ephemeral-test.sh && \
   grep -q 'tailscale.outboundHTTPProxy.noProxy' gke-test/ephemeral-test.sh && \
   grep -q 'metadata.google.internal' gke-test/ephemeral-test.sh && \
   grep -q 'api_host' gke-test/ephemeral-test.sh; then
  pass "GKE private central: accepts coordination routes without proxying GKE control traffic"
else
  fail "GKE private central: route acceptance or GKE NO_PROXY coverage missing"
fi
if grep -q 'waiting for connector stream to reach central' gke-test/ephemeral-test.sh && \
   grep -q "grep -q 'connected to central'" gke-test/ephemeral-test.sh; then
  pass "GKE startup ordering: workload waits for the connector stream"
else
  fail "GKE startup ordering: workload can race connector registration"
fi

# GKE ValidatingAdmissionPolicy rejects providerIDs that don't end with
# /<node-name>. Bursts must register as linode://<NODE_NAME> and must NOT
# carry the cloud-provider uninitialized taint (lifecycle ignores taints;
# the taint only blocks workload scheduling for no benefit).
echo ""
echo "=== GKE providerID identity ==="
for f in ../pkg/backends/linode/bootstrap-baked.sh \
         ../pkg/backends/linode/bootstrap.sh \
         ../burst/image/entrypoint-kubelet.sh; do
  if grep -q 'PROVIDER_ID="linode://${NODE_NAME}"' "$f"; then
    pass "GKE providerID: $f uses linode://\${NODE_NAME}"
  else
    fail "GKE providerID: $f missing linode://\${NODE_NAME} for GKE"
  fi
  if grep -q 'node.cloudprovider.kubernetes.io/uninitialized' "$f"; then
    fail "GKE stale taint: $f still references cloud-provider uninitialized taint"
  else
    pass "GKE stale taint: $f correctly removed uninitialized taint"
  fi
done

# AKS must use ys-owner=yscale-burst + ys-burst-id
if grep -q 'ys-owner.*yscale-burst' aks-test/cluster.sh && \
   grep -q 'ys-burst-id' aks-test/cluster.sh; then
  pass "AKS burst tags: ys-owner=yscale-burst + ys-burst-id"
else
  fail "AKS burst tags: wrong tag names"
fi

# No orphan sweep should reference yscale-run-id for burst VM scoping
for f in eks-test/ephemeral-test.sh eks-test/Makefile \
         gke-test/cluster.sh gke-test/Makefile \
         aks-test/cluster.sh aks-test/Makefile; do
  if grep -q 'yscale-run-id' "$f" 2>/dev/null; then
    fail "stale RUN_ID ref: $f still references yscale-run-id"
  else
    pass "no stale RUN_ID ref: $f"
  fi
done

echo ""
echo "=== Azure --no-wait semantics ==="
if grep -q '\-\-no-wait=false' aks-test/cluster.sh; then
  fail "Azure --no-wait: --no-wait=false still present in cluster.sh"
else
  pass "Azure --no-wait: no --no-wait=false in cluster.sh"
fi

echo ""
echo "=== Azure VM list uses -d for powerState ==="
if grep -q 'az vm list -d' aks-test/cluster.sh; then
  pass "Azure VM list: uses -d for powerState"
else
  if grep -q 'powerState' aks-test/cluster.sh; then
    fail "Azure VM list: queries powerState without -d"
  else
    pass "Azure VM list: no powerState query (OK if not needed)"
  fi
fi

echo ""
echo "=== AKS supported system pool ==="
if grep -q 'NODE_TYPE="${NODE_TYPE:-Standard_D4as_v5}"' aks-test/cluster.sh && \
   grep -q 'NODE_COUNT="${NODE_COUNT:-2}"' aks-test/cluster.sh && \
   grep -q -- '--node-count "${NODE_COUNT}"' aks-test/cluster.sh; then
  pass "AKS system pool: defaults to 2x Standard_D4as_v5"
else
  fail "AKS system pool: current 2-node/4-vCPU minimum is not enforced"
fi
if grep -q 'yscale-ephemeral-test=true' aks-test/cluster.sh && \
   grep -q 'require_test_rg' aks-test/cluster.sh; then
  pass "AKS resource group: isolated test-only guard present"
else
  fail "AKS resource group: missing isolated test-only guard"
fi
cleanup_guard_line=$(grep -n 'PREFLIGHT_OK.*!=.*true' aks-test/ephemeral-test.sh | head -1 | cut -d: -f1)
first_cleanup_write=$(grep -n 'kubectl delete\|cluster.sh delete\|burst-network-cleanup' aks-test/ephemeral-test.sh | head -1 | cut -d: -f1)
if [ -n "${cleanup_guard_line}" ] && [ -n "${first_cleanup_write}" ] && \
   [ "${cleanup_guard_line}" -lt "${first_cleanup_write}" ]; then
  pass "AKS failed preflight: cleanup exits before mutation"
else
  fail "AKS failed preflight: cleanup can mutate before the approval guard"
fi
kube_guard_line=$(grep -n 'KUBECONFIG_OK.*=.*true' aks-test/ephemeral-test.sh | head -1 | cut -d: -f1)
first_kube_delete=$(grep -n 'kubectl delete' aks-test/ephemeral-test.sh | head -1 | cut -d: -f1)
if [ -n "${kube_guard_line}" ] && [ -n "${first_kube_delete}" ] && \
   [ "${kube_guard_line}" -lt "${first_kube_delete}" ]; then
  pass "AKS failed create: Kubernetes cleanup requires acquired kubeconfig"
else
  fail "AKS failed create: cleanup can mutate the previous kubecontext"
fi
for fn in cmd_delete cmd_nuke cmd_burst_cleanup; do
  if sed -n "/^${fn}()/,/^}/p" aks-test/cluster.sh | grep -q 'require_test_rg'; then
    pass "AKS destructive guard: ${fn} requires test-only RG"
  else
    fail "AKS destructive guard: ${fn} bypasses test-only RG"
  fi
done
if sed -n '/^cmd_delete()/,/^}/p' aks-test/cluster.sh | grep -q 'require_test_cluster' && \
   sed -n '/^cmd_nuke()/,/^}/p' aks-test/cluster.sh | grep -q 'require_test_cluster'; then
  pass "AKS cluster deletion: exact test tags required"
else
  fail "AKS cluster deletion: exact test tags not enforced"
fi
for fn in cmd_up cmd_delete cmd_nuke; do
  if sed -n "/^${fn}()/,/^}/p" aks-test/cluster.sh | grep -q 'az aks list.*|| true'; then
    fail "AKS inventory: ${fn} suppresses provider failure"
  else
    pass "AKS inventory: ${fn} fails closed"
  fi
done

echo ""
echo "=== Azure billable network teardown ==="
for name in 'ys-vnet-' 'ys-nsg-' 'ys-natgw-' 'ys-natgw-pip-'; do
  if grep -q "${name}" aks-test/cluster.sh; then
    pass "Azure network inventory: ${name}"
  else
    fail "Azure network inventory: missing ${name}"
  fi
done
if grep -q 'burst-network-cleanup' aks-test/ephemeral-test.sh && \
   grep -q 'burst-network-check' aks-test/ephemeral-test.sh; then
  pass "Azure teardown: shared NAT/PIP cleanup and final check wired"
else
  fail "Azure teardown: shared NAT/PIP cleanup/check missing"
fi
if grep -q 'NAT gateway.*not provisioned' aks-test/README.md aks-test/Makefile; then
  fail "Azure cost: still claims backend NAT is not provisioned"
else
  pass "Azure cost: backend NAT standing charge is disclosed"
fi

echo ""
echo "=== GKE deprecated flags removed ==="
if grep -q '\-\-no-enable-cloud-logging' gke-test/cluster.sh; then
  fail "GKE flags: deprecated --no-enable-cloud-logging still present"
else
  pass "GKE flags: --no-enable-cloud-logging removed"
fi
if grep -q '\-\-no-enable-cloud-monitoring' gke-test/cluster.sh; then
  fail "GKE flags: deprecated --no-enable-cloud-monitoring still present"
else
  pass "GKE flags: --no-enable-cloud-monitoring removed"
fi
if grep -q '\-\-logging=NONE' gke-test/cluster.sh; then
  pass "GKE flags: --logging=NONE present"
else
  fail "GKE flags: --logging=NONE missing"
fi

echo ""
echo "=== Preflight checks commands before cloud calls ==="
for f in gke-test/cluster.sh aks-test/cluster.sh; do
  cmd_line=$(grep -n 'command -v' "$f" | head -1 | cut -d: -f1)
  cloud_line=$(grep -n 'activate_.*_creds\|az account\|gcloud' "$f" | grep -v '^#' | head -1 | cut -d: -f1)
  # In preflight function, command checks should come before cloud auth calls
  preflight_start=$(grep -n 'cmd_preflight()' "$f" | head -1 | cut -d: -f1)
  if [ -n "${preflight_start}" ]; then
    first_cmd=$(sed -n "${preflight_start},\$p" "$f" | grep -n 'command -v' | head -1 | cut -d: -f1)
    first_cloud=$(sed -n "${preflight_start},\$p" "$f" | grep -n 'activate_.*_creds' | head -1 | cut -d: -f1)
    if [ -n "${first_cmd}" ] && [ -n "${first_cloud}" ] && [ "${first_cmd}" -lt "${first_cloud}" ]; then
      pass "preflight order: $f checks commands before cloud auth"
    else
      fail "preflight order: $f calls cloud auth before checking commands"
    fi
  fi
  if grep -Eq 'preflight\)[[:space:]]+activate_' "$f"; then
    fail "preflight dispatch: $f bypasses command checks"
  else
    pass "preflight dispatch: $f enters cmd_preflight directly"
  fi
done

echo ""
echo "=== BURST_ID_FILE in survive-test.sh ==="
if grep -q 'BURST_ID_FILE' eks-test/survive-test.sh; then
  pass "survive-test.sh: writes BURST_ID_FILE"
else
  fail "survive-test.sh: no BURST_ID_FILE support"
fi
if grep -q 'status.burstID' eks-test/survive-test.sh; then
  pass "survive-test.sh: captures .status.burstID"
else
  fail "survive-test.sh: does not capture .status.burstID"
fi

echo ""
echo "=== Pod scheduling proof ==="
if grep -q 'burst-pod-proof' eks-test/survive-test.sh; then
  pass "survive-test.sh: pod scheduling proof present"
else
  fail "survive-test.sh: no pod scheduling proof"
fi
if grep -A8 'pod_phase=' eks-test/survive-test.sh | grep -q 'exit 1'; then
  pass "survive-test.sh: failed Pod proof fails the run"
else
  fail "survive-test.sh: failed Pod proof is non-fatal"
fi

echo ""
echo "=== Teardown deletes Workload first ==="
for f in eks-test/ephemeral-test.sh gke-test/ephemeral-test.sh aks-test/ephemeral-test.sh; do
  workload_line=$(grep -n 'delete workload survive-probe' "$f" | head -1 | cut -d: -f1 || true)
  cluster_line=$(grep -n 'deleting cluster\|eksctl delete cluster' "$f" | head -1 | cut -d: -f1 || true)
  if [ -n "${workload_line}" ] && [ -n "${cluster_line}" ] && [ "${workload_line}" -lt "${cluster_line}" ]; then
    pass "teardown order: $f deletes Workload before cluster"
  else
    fail "teardown order: $f does not delete Workload before cluster"
  fi
done

echo ""
echo "=== No recursive cleanup traps ==="
for f in eks-test/ephemeral-test.sh gke-test/ephemeral-test.sh aks-test/ephemeral-test.sh; do
  if grep -q '_CLEANUP_RAN' "$f"; then
    pass "no-recurse guard: $f"
  else
    fail "no-recurse guard: $f missing _CLEANUP_RAN"
  fi
done

echo ""
echo "=== Credential redaction ==="
# GKE must never print the SA JSON
if grep -q 'echo.*sa.*json\|cat.*sa.*json\|printf.*sa.*json' gke-test/cluster.sh; then
  fail "GKE cred redaction: SA JSON may be printed"
else
  pass "GKE cred redaction: SA JSON not printed"
fi

echo ""
echo "=== Summary ==="
echo "  ${PASS} passed, ${FAIL} failed"
if [ "${FAIL}" -gt 0 ]; then
  exit 1
fi
