#!/usr/bin/env bash
# burst-timing.sh — measure the burst lifecycle phase by phase, per backend.
#
# Emits one TSV row per run so results across backends are directly
# comparable. Phases are observed from the cluster's own objects rather than
# central's logs: the Workload CR, the Node, and the Job pod are what a
# customer actually experiences, and they carry timestamps central does not.
#
#   submit      -> the Workload CR is accepted
#   planned     -> central assigned a burst id (status.burstID)
#   node_seen   -> the burst Node object first appears (VM booted + kubelet
#                  registered + CSR approved)
#   node_ready  -> that Node reports Ready
#   pod_running -> the workload pod is actually executing
#   reaped      -> the Node is gone again
#
# node_seen is the honest measure of provisioning: everything before it is
# cloud API + boot + install + mesh join, and it is where backends differ most.
#
# Usage:
#   ./deploy/smoketest/burst-timing.sh --backend gcp [--tier full]
#                                      [--size small] [--timeout 900]
#                                      [--namespace default] [--keep]
#
# --tier accepts only "full" (the default and the only supported networking
# tier). Passing "lite" or any other value exits before any Workload is
# submitted, so a legacy caller can't silently run — or measure — a tier
# that no longer exists.
#
# Exit 0 only when the pod ran AND the burst was reaped. --keep skips the
# teardown wait (leaves the burst for inspection; it still reaps on deadline).
set -uo pipefail

BACKEND=""
TIER="full"
SIZE="small"
TIMEOUT=900
NS="default"
KEEP=0

while [ $# -gt 0 ]; do
  case "$1" in
    --backend)   BACKEND="$2"; shift 2 ;;
    --tier)      TIER="$2"; shift 2 ;;
    --size)      SIZE="$2"; shift 2 ;;
    --timeout)   TIMEOUT="$2"; shift 2 ;;
    --namespace) NS="$2"; shift 2 ;;
    --keep)      KEEP=1; shift ;;
    -h|--help)   sed -n '1,30p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done
[ -n "$BACKEND" ] || { echo "--backend is required" >&2; exit 2; }
# --tier is validated here rather than translated: the removed lite tier
# would otherwise be silently rewritten to full, which is exactly the
# "silently ran the wrong tier" failure this smoketest is supposed to
# catch. Central refuses lite at admission (pkg/workload.Validate), and
# the burst bootstraps refuse it before touching kubelet, so surfacing
# it here keeps the failure mode consistent all the way down.
case "$TIER" in
  full) ;;
  lite)
    echo "--tier=lite is not supported: the lite tier has been removed; use --tier=full (the default)" >&2
    exit 2
    ;;
  *)
    echo "--tier=$TIER is not supported: only --tier=full is accepted" >&2
    exit 2
    ;;
esac

NAME="timing-${BACKEND}-$(date -u +%H%M%S)"
START=$(date +%s)

# Phase clocks, all seconds since submit. -1 means never observed.
t_planned=-1; t_node_seen=-1; t_node_ready=-1; t_pod_running=-1; t_reaped=-1
burst=""; node=""

now()  { echo $(( $(date +%s) - START )); }
log()  { printf '[%4ss] %s\n' "$(now)" "$*" >&2; }

cleanup() {
  kubectl delete workload "$NAME" -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

cat <<YAML | kubectl apply -n "$NS" -f - >/dev/null
apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: ${NAME}
spec:
  image: docker.io/library/busybox:1.36
  backend: ${BACKEND}
  size: ${SIZE}
  command: ["sh","-c"]
  # NOTE: this heredoc is unquoted so it can expand ${NAME}/${BACKEND}, which
  # means backticks and $( ) inside these comments would be EXECUTED by the
  # outer shell. Keep prose here backtick-free.
  #
  # Assert via EXIT CODE, never via kubectl logs: gateway.kubeletProxyRouting
  # defaults to false, so the apiserver cannot reach a burst kubelet on :10250
  # and "kubectl logs" against a burst returns nothing. A log-based check would
  # report every run as failed.
  #
  # The image pull itself is the large-packet test — several MB through the
  # burst's egress path. A tiny echo would pass over a broken MTU (GCP's
  # default VPC MTU is 1460 against the CNI bridge's 1500 default, and a
  # bridge wider than its underlay blackholes large packets silently), but a
  # multi-MB pull cannot. Pod Succeeded therefore already implies the path
  # carried real traffic; the byte check below just makes the exit code
  # meaningful rather than trivially zero.
  args: ["test \$(dd if=/dev/zero bs=1M count=64 2>/dev/null | wc -c) -eq 67108864"]
  retries: 0
  budget:
    maxUSD: 0.50
    deadline: 20m
  networking:
    tier: ${TIER}
YAML
log "submitted ${NAME} backend=${BACKEND} tier=${TIER}"

while [ "$(now)" -lt "$TIMEOUT" ]; do
  if [ "$t_planned" -lt 0 ]; then
    burst=$(kubectl get workload "$NAME" -n "$NS" -o jsonpath='{.status.burstID}' 2>/dev/null || true)
    if [ -n "$burst" ]; then t_planned=$(now); node="ys-burst-${burst#burst_}"; log "planned burst=$burst node=$node"; fi
  fi
  if [ -n "$node" ] && [ "$t_node_seen" -lt 0 ]; then
    if kubectl get node "$node" >/dev/null 2>&1; then t_node_seen=$(now); log "node object appeared"; fi
  fi
  if [ "$t_node_seen" -ge 0 ] && [ "$t_node_ready" -lt 0 ]; then
    st=$(kubectl get node "$node" -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}' 2>/dev/null || true)
    if [ "$st" = "True" ]; then t_node_ready=$(now); log "node Ready"; fi
  fi
  if [ "$t_pod_running" -lt 0 ]; then
    ph=$(kubectl get pods -n "$NS" -l job-name="$NAME" -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)
    case "$ph" in
      Running|Succeeded)
        t_pod_running=$(now); log "pod $ph"
        ;;
    esac
  fi
  # Reaped: the Node is gone again after we saw it.
  if [ "$t_node_seen" -ge 0 ] && [ "$t_reaped" -lt 0 ]; then
    if ! kubectl get node "$node" >/dev/null 2>&1; then t_reaped=$(now); log "node gone (reaped)"; break; fi
  fi
  [ "$KEEP" = 1 ] && [ "$t_pod_running" -ge 0 ] && break
  sleep 2
done

phase=$(kubectl get workload "$NAME" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null || echo "?")
# Succeeded means the container exited 0, which means the byte check passed
# AND the multi-MB image pull completed over the burst's network path.
case "$phase" in Succeeded) payload="ok" ;; *) payload="unverified" ;; esac

# TSV: one row per run, header printed by the caller.
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  "$BACKEND" "$TIER" "$SIZE" \
  "$t_planned" "$t_node_seen" "$t_node_ready" "$t_pod_running" "$t_reaped" \
  "$phase" "$payload"

[ "$t_pod_running" -ge 0 ] && [ "$payload" = "ok" ]
