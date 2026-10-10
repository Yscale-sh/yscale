#!/usr/bin/env bash
# seed-cilium-scaffolding.sh
#
# One-time prerequisite for running yscale tier=full bursts against a
# customer cluster that does NOT already run Cilium as its CNI
# (LKE, EKS+VPC-CNI, GKE+Dataplane-V1, AKS+Azure-CNI, plain kubeadm, etc.).
#
# What this installs:
#   • Cilium v2 + v2alpha1 CRDs                 (CiliumEndpoint, CiliumIdentity, CiliumNode, ...)
#   • cilium ServiceAccount in kube-system      (the agent on bursts mints tokens for it)
#   • cilium ClusterRole + ClusterRoleBinding   (read nodes/pods/services, write Cilium CRDs)
#   • cilium-config ConfigMap in kube-system    (host-mode-Cilium config for bursts)
#
# What this does NOT install:
#   • Cilium CNI on the customer cluster — the customer keeps using
#     whatever CNI they already have (Calico, Flannel, AWS VPC CNI, ...)
#   • Cilium agent/operator pods — those run only on yscale burst nodes,
#     as a host-mode process baked into the burst image
#
# This is the "joined-CNI" scaffolding pattern: bursts join the customer
# apiserver, run Cilium host-mode against shared CiliumEndpoint /
# CiliumIdentity CRDs in the customer cluster, and the gateway sidecar
# routes cross-cluster traffic. Customer's existing CNI is untouched.
#
# Usage:
#   kubectl config use-context <customer-cluster>
#   ./seed-cilium-scaffolding.sh [--cilium-version v1.19.4]
#
# Idempotent: safe to re-run; uses kubectl apply throughout.

set -euo pipefail

CILIUM_VERSION="${CILIUM_VERSION:-v1.19.4}"
while [ $# -gt 0 ]; do
  case "$1" in
    --cilium-version) CILIUM_VERSION="$2"; shift 2 ;;
    -h|--help) sed -n '1,40p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

echo "[seed] target cluster: $(kubectl config current-context)"
echo "[seed] cilium version: $CILIUM_VERSION"
echo ""

# 1. CRDs from upstream Cilium repo. Failures on individual CRDs (e.g.
#    renamed in newer versions) are non-fatal — log and continue.
echo "[seed] installing Cilium CRDs"
for crd in \
    v2/ciliumnetworkpolicies \
    v2/ciliumclusterwidenetworkpolicies \
    v2/ciliumendpoints \
    v2/ciliumidentities \
    v2/ciliumnodes \
    v2/ciliumlocalredirectpolicies \
    v2/ciliumenvoyconfigs \
    v2/ciliumclusterwideenvoyconfigs \
    v2alpha1/ciliuml2announcementpolicies \
    v2alpha1/ciliumpodippools \
  ; do
  url="https://raw.githubusercontent.com/cilium/cilium/${CILIUM_VERSION}/pkg/k8s/apis/cilium.io/client/crds/${crd}.yaml"
  http=$(curl -sf -o /tmp/.yscale-crd.yaml -w "%{http_code}" "$url" || echo "000")
  if [ "$http" = "200" ]; then
    kubectl apply -f /tmp/.yscale-crd.yaml 2>&1 | sed 's/^/  /'
  else
    echo "  skip $crd (HTTP $http)"
  fi
done
rm -f /tmp/.yscale-crd.yaml

# 2. ServiceAccount + RBAC. ClusterRole grants what host-mode
#    cilium-agent on bursts needs to talk to the customer apiserver.
echo ""
echo "[seed] applying cilium SA + RBAC"
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: cilium
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: cilium
rules:
  - apiGroups: [""]
    resources: ["nodes", "endpoints", "services", "pods", "namespaces", "componentstatuses"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["nodes/status"]
    verbs: ["patch"]
  - apiGroups: [""]
    resources: ["pods", "pods/finalizers"]
    verbs: ["get", "list", "watch", "update", "delete"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["cilium.io"]
    resources:
      - ciliumnetworkpolicies
      - ciliumclusterwidenetworkpolicies
      - ciliumendpoints
      - ciliumidentities
      - ciliumnodes
      - ciliumlocalredirectpolicies
      - ciliumenvoyconfigs
      - ciliumclusterwideenvoyconfigs
      - ciliuml2announcementpolicies
      - ciliumpodippools
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["cilium.io"]
    resources:
      - ciliumendpoints/status
      - ciliumnodes/status
      - ciliumidentities/status
    verbs: ["patch", "update"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["create", "get", "list", "update"]
  - apiGroups: ["apiextensions.k8s.io"]
    resources: ["customresourcedefinitions"]
    verbs: ["get", "list", "watch", "create", "update"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: cilium
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cilium
subjects:
  - kind: ServiceAccount
    name: cilium
    namespace: kube-system
YAML

# 3. cilium-config ConfigMap. Minimal defaults for host-mode bursts.
#    Customer can tune individual keys after install if they want
#    NetworkPolicy enforcement, Hubble, etc.
echo ""
echo "[seed] applying cilium-config ConfigMap"
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: ConfigMap
metadata:
  name: cilium-config
  namespace: kube-system
data:
  identity-allocation-mode: crd
  enable-ipv4: "true"
  enable-ipv6: "false"
  enable-ipv4-masquerade: "true"
  routing-mode: native
  tunnel-protocol: vxlan
  ipam: kubernetes
  enable-host-firewall: "false"
  enable-hubble: "false"
  enable-bandwidth-manager: "false"
  enable-bgp-control-plane: "false"
  enable-bpf-clock-probe: "false"
  enable-endpoint-health-checking: "true"
  enable-l7-proxy: "false"
  enable-policy: default
  kube-proxy-replacement: "false"
  cluster-name: default
  cluster-id: "0"
  install-iptables-rules: "true"
  enable-runtime-device-detection: "true"
  monitor-aggregation: medium
  preallocate-bpf-maps: "false"
  sidecar-istio-proxy-image: cilium/istio_proxy
  socket-lb-tracing: "false"
YAML

echo ""
echo "[seed] verifying"
kubectl -n kube-system get sa cilium 2>&1 | tail -2
kubectl -n kube-system get cm cilium-config 2>&1 | tail -2
echo "  crds: $(kubectl get crd 2>&1 | grep -c cilium.io)"
echo ""
echo "[seed] done. tier=full bursts can now run host-mode Cilium against this cluster."
echo "[seed] next: helm install yscale-agent (see deploy/helm/yscale-agent/)"
