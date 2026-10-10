#!/usr/bin/env bash
# Does a yscale burst SURVIVE on the CURRENT cluster, or does its CCM reap it?
# This is THE managed-cluster compatibility check (the LKE problem; see README
# "Two kinds of deletion"). Cluster-agnostic: point your kubeconfig at any
# cluster (EKS / LKE / GKE / AKS / homelab) and run it.
#
# Assumes: kubectl points at the target cluster, the yscale-agent is installed
# and connected (`make agent`), and the agent's Helm chart installed the Workload
# CRD. Submits a tiny nodeOnly burst, waits for the Node to register, then watches
# it. PASS = node persists; FAIL = the cluster's CCM deleted it (LKE-style).
#
#   BACKEND=linode ./survive-test.sh       # test a linode:// burst (lenient case on EKS)
#   BACKEND=aws    ./survive-test.sh       # test an aws:// burst (the risky case on EKS)
#
# When output files are set, the Workload burst ID, exact Node hostname, and
# provider ID are written for the caller's teardown and residue audit.
set -euo pipefail
cd "$(dirname "$0")"

NS="${NS:-default}"
BACKEND="${BACKEND:-linode}"
JOIN_TIMEOUT="${JOIN_TIMEOUT:-600}"
SURVIVE_WINDOW="${SURVIVE_WINDOW:-180}"
BURST_ID_FILE="${BURST_ID_FILE:-}"
NODE_NAME_FILE="${NODE_NAME_FILE:-}"
PROVIDER_ID_FILE="${PROVIDER_ID_FILE:-}"

cleanup() {
  if [ "${SURVIVE_CLEANUP:-true}" = "true" ]; then
    echo "==> cleaning up probe workload (namespace $NS)"
    kubectl delete workload survive-probe -n "$NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kubectl delete pod burst-pod-proof -n "$NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    if [ "$NS" != "default" ]; then
      kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT

before="$(kubectl get nodes -l yscale.sh/burst-node -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
is_new() { case " $before " in *" $1 "*) return 1;; *) return 0;; esac; }
newest_burst_node() {
  for n in $(kubectl get nodes -l yscale.sh/burst-node -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    if is_new "$n"; then echo "$n"; return; fi
  done
}

kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "==> submitting nodeOnly burst (backend=$BACKEND)"
sed -e "s/__NS__/$NS/" -e "s/__BACKEND__/$BACKEND/" workload-nodeonly.yaml | kubectl apply -f -

echo "==> waiting for burstID on Workload survive-probe..."
burst_id=""
bid_deadline=$(( $(date +%s) + JOIN_TIMEOUT ))
while [ -z "${burst_id}" ]; do
  burst_id="$(kubectl get workload survive-probe -n "$NS" -o jsonpath='{.status.burstID}' 2>/dev/null || true)"
  [ -n "${burst_id}" ] && break
  if [ "$(date +%s)" -gt "${bid_deadline}" ]; then
    echo "WARN: burstID never appeared on Workload status within ${JOIN_TIMEOUT}s"
    break
  fi
  sleep 5
done
if [ -n "${burst_id}" ]; then
  echo "==> burstID: ${burst_id}"
  if [ -n "${BURST_ID_FILE}" ]; then
    echo "${burst_id}" > "${BURST_ID_FILE}"
  fi
else
  echo "INCONCLUSIVE: Workload status never exposed a burstID; exact provider cleanup cannot be proved."
  exit 2
fi

echo "==> waiting up to ${JOIN_TIMEOUT}s for the burst Node to register..."
deadline=$(( $(date +%s) + JOIN_TIMEOUT ))
node=""
while [ -z "$node" ]; do
  node="$(newest_burst_node)"
  [ -n "$node" ] && break
  if [ "$(date +%s)" -gt "$deadline" ]; then
    echo "INCONCLUSIVE: no burst Node joined within ${JOIN_TIMEOUT}s."
    echo "  Check: central reachable from this cluster? agent connected? backend ($BACKEND) creds set on central?"
    kubectl -n "$NS" get workload survive-probe -o yaml 2>/dev/null | sed -n '/status:/,$p' || true
    exit 2
  fi
  sleep 10
done
echo "==> burst Node joined: $node"
if [ -n "${NODE_NAME_FILE}" ]; then
  printf '%s\n' "${node}" > "${NODE_NAME_FILE}"
fi
provider_id="$(kubectl get node "${node}" -o jsonpath='{.spec.providerID}' 2>/dev/null || true)"
if [ -z "${provider_id}" ]; then
  echo "INCONCLUSIVE: burst Node ${node} has no providerID; exact cleanup cannot be proved."
  exit 2
fi
if [ -n "${PROVIDER_ID_FILE}" ]; then
  printf '%s\n' "${provider_id}" > "${PROVIDER_ID_FILE}"
fi
echo "==> providerID: ${provider_id}"

echo "==> watching $node for ${SURVIVE_WINDOW}s for CCM reaping..."
end=$(( $(date +%s) + SURVIVE_WINDOW ))
while [ "$(date +%s)" -lt "$end" ]; do
  if ! kubectl get node "$node" >/dev/null 2>&1; then
    echo "FAIL: $node was DELETED during the window — this cluster's CCM reaps yscale bursts (LKE-style)."
    kubectl get events -A 2>/dev/null | grep -iE 'RemovingNode|DeletingNode|removing node' | tail -5 || true
    exit 1
  fi
  sleep 15
done
echo "PASS: $node survived ${SURVIVE_WINDOW}s with backend=$BACKEND — this managed cluster does NOT reap yscale bursts."

echo "==> pod scheduling proof: scheduling a verification Pod on $node"
kubectl run burst-pod-proof \
  --namespace="$NS" \
  --image=busybox:1.36 \
  --restart=Never \
  --overrides="{\"spec\":{\"nodeSelector\":{\"kubernetes.io/hostname\":\"$node\"},\"tolerations\":[{\"operator\":\"Exists\"}]}}" \
  -- sh -c "echo burst-pod-ok && sleep 1"

kubectl wait pod/burst-pod-proof -n "$NS" --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s 2>/dev/null || true

pod_phase="$(kubectl get pod burst-pod-proof -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
if [ "${pod_phase}" = "Succeeded" ]; then
  echo "PASS: verification Pod completed on burst node $node"
else
  echo "FAIL: verification Pod phase=${pod_phase:-unknown} (expected Succeeded)"
  kubectl describe pod burst-pod-proof -n "$NS" 2>/dev/null || true
  exit 1
fi
