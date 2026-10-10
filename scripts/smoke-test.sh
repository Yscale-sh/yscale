#!/usr/bin/env bash
# scripts/smoke-test.sh — yscale end-to-end smoke test for one backend.
#
# Runs the full joined-CNI lifecycle on the cheapest instance of the
# chosen backend, in <10 minutes, leaves zero leaked resources behind.
# Designed to be the canonical "did I break anything?" check before
# shipping changes that touch central / agent / burst image / chart.
#
# Usage:
#   scripts/smoke-test.sh BACKEND [SIZE] [TIMEOUT_SECONDS]
#     BACKEND          = fly | linode   (required)
#     SIZE             = nano | small | medium   (default: nano)
#     TIMEOUT_SECONDS  = total wall-clock budget (default: 600)
#
# Prereqs:
#   KUBECONFIG must point at the *customer* K8s cluster (the one
#   running yscale-agent). For homelab: ~/.kube/config (k3s).
#   For LKE smoke: ~/.kube/<your-cluster>-kubeconfig.yaml.
#
#   The customer cluster must already have:
#     - yscale-agent installed + connected to a yscale-cloud instance
#     - yscale-cloud configured with credentials for $BACKEND
#
#   For backend-side cleanup verification:
#     - FLYIO_TOKEN + FLY_ORG in env or .env (for Fly)
#     - LINODE_TOKEN in env or .env (for Linode)
#   The script reads .env automatically if present in repo root.
#
# Cost ceilings (worst case if backend doesn't auto-reap):
#     - fly      <$0.01 per run (nano @ ~$1.94/mo)
#     - linode   <$0.02 per run (g6-nanode-1 @ $5/mo)
#
# Safety: cleanup only deletes resources labeled with this run's own
# TEST_ID (yscale.sh/smoke-test-id=$TEST_ID). No cluster-wide sweeps —
# a shared cluster may have other jobs/pods running concurrently.

set -euo pipefail

# ---------- args ----------
BACKEND="${1:-}"
SIZE="${2:-nano}"
TIMEOUT="${3:-600}"

if [ -z "$BACKEND" ]; then
  echo "FATAL: backend required" >&2
  echo "  usage: $0 BACKEND [SIZE] [TIMEOUT_SECONDS]" >&2
  echo "  e.g.   $0 fly nano 600" >&2
  exit 2
fi

case "$BACKEND" in
  fly|flyio) BACKEND=flyio ;;   # accept either; canonical is flyio
  linode) ;;
  aws)
    echo "FATAL: aws smoke is disabled because its provider-side residue audit is not implemented" >&2
    exit 2
    ;;
  *) echo "FATAL: backend must be one of: fly | flyio | linode" >&2; exit 2 ;;
esac

# ---------- config ----------
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TPL="${REPO_ROOT}/test/smoke/workload-cpu.yaml.tpl"
# Fail here, before anything is created. The template lives under test/, which
# is not part of every distribution of this repo; without this check the script
# sed's into a missing file and dies partway through, after it has already
# provisioned cluster resources that then need manual cleanup.
if [ ! -f "$TPL" ]; then
  echo "smoke-test: workload template not found: ${TPL#"$REPO_ROOT"/}" >&2
  echo "            test/smoke/ is not included in this distribution." >&2
  echo "            Apply an example from examples/workloads/ instead." >&2
  exit 2
fi
NS="${SMOKE_NAMESPACE:-default}"     # customer-side namespace
TEST_ID="smoke-$(date +%s)-$$"        # unique per run, no special chars
START_SECS="$(date +%s)"

# Background port-forward bookkeeping — populated during Stage 3 and read by
# the EXIT trap so a failure between kubectl port-forward and its own teardown
# still reaps the child process and its scratch log.
PROBE_PF_PID=""
PROBE_PF_LOG=""

# Load .env for backend credentials (FLYIO_TOKEN etc.). Don't error if
# missing — operator may have env vars set externally.
# shellcheck disable=SC1091
[ -f "${REPO_ROOT}/.env" ] && set -a && . "${REPO_ROOT}/.env" && set +a || true

# ---------- helpers ----------
log() { printf '[smoke %s] %s\n' "$(date +%H:%M:%S)" "$*"; }
elapsed() { echo "+$(($(date +%s) - START_SECS))s"; }

# Poll a condition until it returns 0 or TIMEOUT expires. The script's
# overall TIMEOUT bounds the SUM of waits, not each individual one.
wait_for() {
  local desc="$1" cmd="$2" interval="${3:-3}"
  log "waiting $(elapsed): $desc"
  while ! eval "$cmd" >/dev/null 2>&1; do
    local now=$(($(date +%s) - START_SECS))
    if [ "$now" -gt "$TIMEOUT" ]; then
      log "FATAL $(elapsed): timed out waiting for: $desc"
      return 1
    fi
    sleep "$interval"
  done
  log "OK $(elapsed): $desc"
}

cleanup() {
  local rc=$?
  # Kill any still-running port-forward first so its log stops churning
  # and its socket is freed before we tear down the pod behind it.
  if [ -n "$PROBE_PF_PID" ]; then
    if kill -0 "$PROBE_PF_PID" 2>/dev/null; then
      kill "$PROBE_PF_PID" 2>/dev/null || true
    fi
    # Reap an already-exited child too; kill -0 alone cannot distinguish that
    # case from a PID that was never ours.
    wait "$PROBE_PF_PID" 2>/dev/null || true
  fi
  if [ -n "$PROBE_PF_LOG" ]; then
    rm -f "$PROBE_PF_LOG"
  fi
  log "cleanup $(elapsed): removing test resources (test_id=$TEST_ID)"
  # Delete the Job and any leftover pods tagged with our test id.
  # --wait=false so cleanup doesn't extend wall-clock past TIMEOUT.
  kubectl -n "$NS" delete job   -l "yscale.sh/smoke-test-id=$TEST_ID" --wait=false 2>/dev/null || true
  kubectl -n "$NS" delete pod   -l "yscale.sh/smoke-test-id=$TEST_ID" --wait=false 2>/dev/null || true
  # Note: burst node + Fly/Linode VM cleanup happens via yscale-cloud's
  # natural reap path on Job completion. The verify_no_leaks step
  # checks that path worked; this cleanup only handles K8s-side
  # objects we directly created.
  exit "$rc"
}
trap cleanup EXIT

# ---------- preflight ----------
log "starting backend=$BACKEND size=$SIZE timeout=${TIMEOUT}s test_id=$TEST_ID"

# kubectl must reach a customer cluster with yscale-agent running.
if ! kubectl version --request-timeout=5s >/dev/null 2>&1; then
  log "FATAL: kubectl can't reach the cluster (\$KUBECONFIG=${KUBECONFIG:-default})"
  exit 1
fi

# Select the real, Ready agent container. The gateway and kubelet-route pods
# deliberately share the chart's app label, so selecting the first matching
# Pod can produce a false-positive while the agent Deployment is scaled to 0.
AGENT_ROW="$(kubectl get pods -A -l 'app.kubernetes.io/name=yscale-agent' -o json 2>/dev/null \
  | jq -r '.items[]
      | select(any(.spec.containers[]?; .name == "agent"))
      | select(any(.status.containerStatuses[]?; .name == "agent" and .ready == true))
      | [.metadata.namespace, .metadata.name] | @tsv' \
  | head -1)"
AGENT_NS="${AGENT_ROW%%$'\t'*}"
AGENT_POD="${AGENT_ROW#*$'\t'}"
if [ -z "$AGENT_ROW" ] || [ "$AGENT_NS" = "$AGENT_POD" ]; then
  log "FATAL: no Ready yscale agent container found in the cluster"
  exit 1
fi
if ! YSCALE_NAMESPACE="$AGENT_NS" "${REPO_ROOT}/scripts/wait-agent-connected.sh"; then
  log "FATAL: agent pod $AGENT_NS/$AGENT_POD is not actively connected to central"
  exit 1
fi
log "preflight OK: connected agent=$AGENT_NS/$AGENT_POD"

# ---------- render + apply ----------
RENDERED="$(mktemp -t yscale-smoke.XXXXXX.yaml)"
trap 'rm -f "$RENDERED"' RETURN  # tmpfile cleanup is layered on top of EXIT trap
sed -e "s|__TEST_ID__|${TEST_ID}|g" \
    -e "s|__SIZE__|${SIZE}|g" \
    -e "s|__BACKEND__|${BACKEND}|g" \
    -e "s|__NAMESPACE__|${NS}|g" \
    "$TPL" > "$RENDERED"

log "applying workload (Job=yscale-smoke-${TEST_ID})"
kubectl apply -f "$RENDERED" >/dev/null

# ---------- lifecycle waits ----------
# Stage 1: prove the Pending-Pod bridge recorded the exact central
# workload and burst on this Job. CompletionWatcher selects the workload
# label to report terminal status and trigger the natural provider reap;
# the burst label identifies the capacity created for this exact run.
JOB="yscale-smoke-${TEST_ID}"
wait_for "Job receives workload and burst IDs" \
  "test -n \"\$(kubectl -n '$NS' get job '$JOB' -o jsonpath='{.metadata.labels.yscale\\.sh/workload-id}' 2>/dev/null)\" && test -n \"\$(kubectl -n '$NS' get job '$JOB' -o jsonpath='{.metadata.labels.yscale\\.sh/burst-id}' 2>/dev/null)\""
WORKLOAD_ID="$(kubectl -n "$NS" get job "$JOB" -o jsonpath='{.metadata.labels.yscale\.sh/workload-id}')"
BURST_ID="$(kubectl -n "$NS" get job "$JOB" -o jsonpath='{.metadata.labels.yscale\.sh/burst-id}')"
BURST_NODE="ys-burst-${BURST_ID#burst_}"
log "workload: $WORKLOAD_ID"
log "burst: $BURST_ID node: $BURST_NODE"

# Bind every later assertion to this run's own pod and provisioned node.
# Selecting the pod's scheduled node is also unsafe: shared spare burst
# capacity may run the Job while this run's newly-provisioned node boots.
wait_for "smoke pod created" \
  "kubectl -n '$NS' get pod -l 'yscale.sh/smoke-test-id=$TEST_ID' --no-headers 2>/dev/null | grep -q ."
POD="$(kubectl -n "$NS" get pod -l "yscale.sh/smoke-test-id=$TEST_ID" --no-headers -o custom-columns=NAME:.metadata.name | head -1)"

# Stage 2: this exact node joins and goes Ready. CNI setup + kubelet-server
# cert rotation typically adds another ~30-60s.
wait_for "exact burst node joins cluster" \
  "kubectl get node '$BURST_NODE' >/dev/null 2>&1"
wait_for "burst node Ready" \
  "kubectl get node '$BURST_NODE' -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}' 2>/dev/null | grep -q True"

# Stage 3: pin a diagnostic pod to the exact new node and exercise the full
# apiserver→kubelet streaming surface — logs, exec, AND port-forward. This
# remains exact even when the Job itself used existing shared burst capacity.
# The probe runs busybox httpd on an unprivileged container port serving a
# deterministic marker file so a real TCP tunnel can be curl-verified.
PROBE="yscale-probe-${TEST_ID}"
PROBE_PORT=8080
PROBE_HTTP_MARKER="yscale-smoke: PROBE-HTTP-OK"
kubectl -n "$NS" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${PROBE}
  labels:
    yscale.sh/smoke-test-id: "${TEST_ID}"
spec:
  nodeName: ${BURST_NODE}
  restartPolicy: Never
  tolerations:
    - operator: Exists
  containers:
    - name: probe
      image: busybox:1.36
      command: ["sh", "-c"]
      args:
        - |
          mkdir -p /www
          printf '%s\n' '${PROBE_HTTP_MARKER}' > /www/probe
          echo 'yscale-smoke: PROBE-RUNNING'
          exec httpd -f -p ${PROBE_PORT} -h /www
      ports:
        - name: probe-http
          containerPort: ${PROBE_PORT}
          protocol: TCP
EOF
wait_for "exact-node probe Running" \
  "kubectl -n '$NS' get pod '$PROBE' -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Running"
wait_for "live probe logs available through kubelet" \
  "kubectl -n '$NS' logs '$PROBE' 2>/dev/null | grep -q 'PROBE-RUNNING'"
if ! kubectl -n "$NS" exec "$PROBE" -- sh -c 'printf "yscale-smoke: EXEC-OK\\n"' 2>/dev/null | grep -q 'EXEC-OK'; then
  log "FATAL: kubectl exec to burst pod failed"
  exit 1
fi

# Port-forward exercises the same apiserver→kubelet SPDY/streaming seam as
# logs+exec, but carries arbitrary TCP end-to-end. Use a kernel-assigned
# local port (":REMOTE") so parallel smoke runs on one workstation never
# collide on a fixed listener. Bind to 127.0.0.1 explicitly — the transient
# listener must never be reachable off-host.
PROBE_PF_LOG="$(mktemp -t yscale-smoke-pf.XXXXXX.log)"
kubectl -n "$NS" port-forward --address=127.0.0.1 "pod/${PROBE}" ":${PROBE_PORT}" \
  >"$PROBE_PF_LOG" 2>&1 &
PROBE_PF_PID=$!
LOCAL_PORT=""
# Ready detection: kubectl prints "Forwarding from 127.0.0.1:<port> -> 8080"
# once the listener is bound. Bounded to ~30s, well inside the global budget.
for _ in $(seq 1 30); do
  if ! kill -0 "$PROBE_PF_PID" 2>/dev/null; then
    break
  fi
  LOCAL_PORT="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9][0-9]*\) -> .*/\1/p' "$PROBE_PF_LOG" | head -1)"
  [ -n "$LOCAL_PORT" ] && break
  sleep 1
done
if [ -z "$LOCAL_PORT" ] || ! kill -0 "$PROBE_PF_PID" 2>/dev/null; then
  log "FATAL: kubectl port-forward to burst probe never became ready"
  sed -n '1,20p' "$PROBE_PF_LOG" >&2 || true
  exit 1
fi
if ! curl -sS --max-time 10 "http://127.0.0.1:${LOCAL_PORT}/probe" | grep -q 'PROBE-HTTP-OK'; then
  log "FATAL: port-forward marker mismatch on http://127.0.0.1:${LOCAL_PORT}/probe"
  sed -n '1,20p' "$PROBE_PF_LOG" >&2 || true
  exit 1
fi
log "OK $(elapsed): live kubectl logs + exec + port-forward verified (local :${LOCAL_PORT} → probe :${PROBE_PORT})"
kill "$PROBE_PF_PID" 2>/dev/null || true
wait "$PROBE_PF_PID" 2>/dev/null || true
PROBE_PF_PID=""
rm -f "$PROBE_PF_LOG"
PROBE_PF_LOG=""
kubectl -n "$NS" delete pod "$PROBE" --wait=false >/dev/null

# Stage 4: release the Job fixture and let it complete successfully. It may
# be running on shared capacity, so this second exec is intentionally bound
# to the Job pod while Stage 3 remains bound to the newly-created node.
wait_for "Job pod Running" \
  "kubectl -n '$NS' get pod '$POD' -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Running"
kubectl -n "$NS" exec "$POD" -- touch /tmp/yscale-smoke-release

# Stage 5: read the marker while the fixture still holds the node alive, then
# acknowledge it and independently require successful container completion.
# Fetching logs after Succeeded races central's natural provider teardown.
wait_for "pod output contains MARKER-OK" \
  "kubectl -n '$NS' logs '$POD' --request-timeout=10s 2>/dev/null | grep -q 'yscale-smoke: MARKER-OK'"
log "OK $(elapsed): pod output verified (MARKER-OK)"
kubectl -n "$NS" exec "$POD" -- touch /tmp/yscale-smoke-marker-read
wait_for "pod Succeeded" \
  "kubectl -n '$NS' get pod '$POD' -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Succeeded"

# Stage 6: yscale-cloud's natural reap. After Job completion the agent
# POSTs /v1/workloads/{id}/complete to central, central calls
# backend.DeleteNode, the cloud VM disappears, then K8s node-lifecycle-
# controller removes the Node object once kubelet stops heartbeating.
wait_for "burst node removed (yscale reap)" \
  "! kubectl get node '$BURST_NODE' >/dev/null 2>&1" 5

# ---------- backend-side leak check ----------
verify_fly_clean() {
  : "${FLYIO_TOKEN:?FLYIO_TOKEN required for fly leak check}"
  : "${FLY_ORG:?FLY_ORG required for fly leak check}"
  local app="hs-${FLY_ORG}-burst"
  local active
  active="$(curl -sS -H "Authorization: Bearer ${FLYIO_TOKEN}" \
    "https://api.machines.dev/v1/apps/${app}/machines" \
    | jq -r --arg node "$BURST_NODE" '[.[] | select(.config.metadata["yscale-node"] == $node and .state != "destroyed")] | length')"
  if [ "$active" != "0" ]; then
    log "FAIL: Fly machine for $BURST_NODE is still active in $app — leak"
    return 1
  fi
  log "OK $(elapsed): no leaked Fly machines"
}

verify_linode_clean() {
  : "${LINODE_TOKEN:?LINODE_TOKEN required for linode leak check}"
  local active=0 page=1 pages=1 response page_active
  while [ "$page" -le "$pages" ]; do
    if [ "$page" -gt 100 ]; then
      log "FAIL: Linode instance inventory exceeds 100 pages — refusing truncated absence proof"
      return 1
    fi
    response="$(curl -fsS -H "Authorization: Bearer ${LINODE_TOKEN}" \
      "https://api.linode.com/v4/linode/instances?page=${page}&page_size=100")"
    if ! jq -e '.data | type == "array"' >/dev/null <<<"$response"; then
      log "FAIL: Linode instance inventory returned an invalid response"
      return 1
    fi
    page_active="$(jq -r --arg node "$BURST_NODE" '[.data[] | select(.label == $node)] | length' <<<"$response")"
    active=$((active + page_active))
    pages="$(jq -r '.pages // 0' <<<"$response")"
    if ! [[ "$pages" =~ ^[1-9][0-9]*$ ]] || [ "$pages" -lt "$page" ]; then
      log "FAIL: Linode instance inventory returned invalid pagination metadata"
      return 1
    fi
    page=$((page + 1))
  done
  if [ "$active" != "0" ]; then
    log "FAIL: Linode instance for $BURST_NODE is still active — leak"
    return 1
  fi
  log "OK $(elapsed): no leaked Linode instances"
}

case "$BACKEND" in
  flyio)  verify_fly_clean ;;
  linode) verify_linode_clean ;;
esac

# ---------- done ----------
log "PASS $(elapsed) backend=$BACKEND size=$SIZE test_id=$TEST_ID"
