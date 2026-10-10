#!/usr/bin/env bash
# scripts/rollback-preflight.sh — READ-ONLY preflight for a Flux pin rollback.
#
# What this is: the check you run BEFORE reverting the pin commit in the
# infrastructure repository (onprem/yscale Flux manifests). It reports what the
# cluster currently runs, whether the proposed rollback target is digest-pinned,
# and whether the workloads are healthy enough to roll back from.
#
# What this is NOT: a rollback. This script performs no mutation of any kind.
# It runs `kubectl get`/`version` (and, if present, `flux get`) only. There is
# no apply, patch, rollout, reconcile, suspend, delete, edit, scale or annotate
# in this file, and adding one would be a defect, not a feature: the actual
# rollback is a git revert in the infrastructure repository reconciled by Flux,
# never an imperative change against the cluster.
#
# Usage:
#   scripts/rollback-preflight.sh \
#     --context CONTEXT \
#     --target-cloud-digest sha256:<64 hex> \
#     --target-hosted-controller-digest sha256:<64 hex> \
#     --target-agent-digest sha256:<64 hex> \
#     --target-connector-source <40 hex git sha> \
#     [--namespace yscale] \
#     [--cloud-deployment yscale-cloud] \
#     [--hosted-controller-deployment NAME] \
#     [--flux-namespace flux-system] \
#     [--flux-kustomization onprem-yscale]
#
# Defaults: --namespace yscale and --cloud-deployment yscale-cloud match the
# in-repo manifest deploy/cloud/cloud.yaml. The DEPLOYED names live in the
# infrastructure repository and must be confirmed there; pass them explicitly
# if they differ. Anything this script cannot read, it FAILS on. It never
# assumes a value it did not observe.
#
# Exit: 0 only when every check PASSes. Any FAIL, any missing tool, any missing
# context, any unreadable field exits non-zero. Fail closed is the point.

set -euo pipefail

FAILURES=0
CHECKS=0
REQUEST_TIMEOUT="10s"

pass() { CHECKS=$((CHECKS + 1)); printf 'PASS  %s\n' "$*"; }
fail() { CHECKS=$((CHECKS + 1)); FAILURES=$((FAILURES + 1)); printf 'FAIL  %s\n' "$*"; }
info() { printf 'INFO  %s\n' "$*"; }
die()  { printf 'ABORT %s\n' "$*" >&2; exit 2; }

# ---------- mutation-flag refusal ----------
# A caller who passed a mutating flag meant to change something. This script
# cannot change anything, so running an ordinary read and reporting success
# would be the silent-ignore that makes an operator believe a rollback ran.
# Refuse before reading anything.
MUTATION_FLAGS="--apply --force --reconcile --patch --rollout --rollback --write --delete --prune --suspend --resume --scale --edit --annotate --label --set --confirm --yes -y"

refuse_mutation_flags() {
  local arg denied
  for arg in "$@"; do
    for denied in $MUTATION_FLAGS; do
      if [ "$arg" = "$denied" ]; then
        die "refusing mutating flag '$arg': this preflight is read-only and performs no rollback"
      fi
    done
    case "$arg" in
      --force-*|--set-*|--dry-run=none)
        die "refusing mutating flag '$arg': this preflight is read-only and performs no rollback"
        ;;
    esac
  done
}
refuse_mutation_flags "$@"

# ---------- args ----------
CONTEXT=""
NAMESPACE="yscale"
CLOUD_DEPLOY="yscale-cloud"
HOSTED_DEPLOY=""
FLUX_NS="flux-system"
FLUX_KUSTOMIZATION="onprem-yscale"
TARGET_CLOUD_DIGEST=""
TARGET_HOSTED_DIGEST=""
TARGET_AGENT_DIGEST=""
TARGET_CONNECTOR_SOURCE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --context) CONTEXT="${2:-}"; shift 2 ;;
    --namespace) NAMESPACE="${2:-}"; shift 2 ;;
    --cloud-deployment) CLOUD_DEPLOY="${2:-}"; shift 2 ;;
    --hosted-controller-deployment) HOSTED_DEPLOY="${2:-}"; shift 2 ;;
    --flux-namespace) FLUX_NS="${2:-}"; shift 2 ;;
    --flux-kustomization) FLUX_KUSTOMIZATION="${2:-}"; shift 2 ;;
    --target-cloud-digest) TARGET_CLOUD_DIGEST="${2:-}"; shift 2 ;;
    --target-hosted-controller-digest) TARGET_HOSTED_DIGEST="${2:-}"; shift 2 ;;
    --target-agent-digest) TARGET_AGENT_DIGEST="${2:-}"; shift 2 ;;
    --target-connector-source) TARGET_CONNECTOR_SOURCE="${2:-}"; shift 2 ;;
    -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
    *) die "unknown argument '$1'" ;;
  esac
done

# ---------- fail closed on missing inputs ----------
[ -n "$CONTEXT" ] || die "--context is required: this preflight refuses to guess a kubeconfig context"
[ -n "$NAMESPACE" ] || die "--namespace must not be empty"
[ -n "$CLOUD_DEPLOY" ] || die "--cloud-deployment must not be empty"
[ -n "$TARGET_CLOUD_DIGEST" ] || die "--target-cloud-digest is required"
[ -n "$TARGET_HOSTED_DIGEST" ] || die "--target-hosted-controller-digest is required"
[ -n "$TARGET_AGENT_DIGEST" ] || die "--target-agent-digest is required"
[ -n "$TARGET_CONNECTOR_SOURCE" ] || die "--target-connector-source is required"

command -v kubectl >/dev/null 2>&1 || die "kubectl not found: cannot read cluster state, refusing to report a partial preflight"

# kube() is the ONLY cluster call site, and it hardcodes the verb. There is no
# code path in this script that can reach kubectl with any other verb.
kube() {
  kubectl --context "$CONTEXT" --request-timeout="$REQUEST_TIMEOUT" get "$@"
}

if ! kubectl --context "$CONTEXT" --request-timeout="$REQUEST_TIMEOUT" version >/dev/null 2>&1; then
  die "kubeconfig context '$CONTEXT' is not reachable"
fi

echo "== rollback preflight (read-only) =="
info "context=$CONTEXT namespace=$NAMESPACE"

# ---------- 1. target pins are digest-pinned ----------
# A rollback target that is not a digest is not a rollback target: a tag can be
# repointed, so reverting to one proves nothing about which bytes run.
check_digest() {
  local label="$1" value="$2"
  if [[ "$value" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    pass "$label is digest-pinned (sha256, 64 hex)"
  else
    fail "$label is not a 64-hex sha256 digest: '$value'"
  fi
}
check_digest "target cloud image" "$TARGET_CLOUD_DIGEST"
check_digest "target hosted-controller image" "$TARGET_HOSTED_DIGEST"
check_digest "target cluster-agent image" "$TARGET_AGENT_DIGEST"
info "target cluster-agent digest and connector source are syntax checks only; this script does not verify their live HelmRelease, workload or GitRepository"

if [[ "$TARGET_CONNECTOR_SOURCE" =~ ^[0-9a-f]{40}$ ]]; then
  pass "target connector source pin is a full 40-hex commit sha"
else
  fail "target connector source pin is not a full 40-hex commit sha: '$TARGET_CONNECTOR_SOURCE'"
fi

# ---------- 2. current Flux revision ----------
FLUX_REV=""
if FLUX_REV="$(kube kustomization "$FLUX_KUSTOMIZATION" -n "$FLUX_NS" \
    -o jsonpath='{.status.lastAppliedRevision}' 2>/dev/null)" && [ -n "$FLUX_REV" ]; then
  pass "Flux Kustomization $FLUX_NS/$FLUX_KUSTOMIZATION lastAppliedRevision=$FLUX_REV"
else
  fail "could not read lastAppliedRevision for Kustomization $FLUX_NS/$FLUX_KUSTOMIZATION"
fi

FLUX_READY=""
if FLUX_READY="$(kube kustomization "$FLUX_KUSTOMIZATION" -n "$FLUX_NS" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" && [ -n "$FLUX_READY" ]; then
  if [ "$FLUX_READY" = "True" ]; then
    pass "Flux Kustomization $FLUX_KUSTOMIZATION is Ready"
  else
    fail "Flux Kustomization $FLUX_KUSTOMIZATION Ready=$FLUX_READY — resolve reconciliation before planning a revert"
  fi
else
  fail "could not read Ready condition for Kustomization $FLUX_NS/$FLUX_KUSTOMIZATION"
fi

# `flux get` is read-only and used only as a second opinion when the CLI exists.
if command -v flux >/dev/null 2>&1; then
  info "flux CLI present; read-only cross-check:"
  flux --context "$CONTEXT" --timeout="$REQUEST_TIMEOUT" get kustomization "$FLUX_KUSTOMIZATION" -n "$FLUX_NS" 2>/dev/null || \
    info "flux get returned non-zero; the kubectl reads above are authoritative"
else
  info "flux CLI absent; kubectl reads above are the only source"
fi

# ---------- 3. running images ----------
running_images() {
  local deploy="$1"
  kube deploy "$deploy" -n "$NAMESPACE" \
    -o jsonpath='{range .spec.template.spec.containers[*]}{.image}{"\n"}{end}' 2>/dev/null
}

compare_running() {
  local label="$1" deploy="$2" target="$3" images=""
  if ! images="$(running_images "$deploy")" || [ -z "$images" ]; then
    fail "$label: could not read images for deployment $NAMESPACE/$deploy"
    return
  fi
  info "$label running images:"
  local image
  while IFS= read -r image; do
    if [ -n "$image" ]; then
      printf '        %s\n' "$image"
      if [[ ! "$image" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]]; then
        fail "$label has a container image without a complete sha256 digest"
      fi
    fi
  done <<<"$images"
  if printf '%s\n' "$images" | grep -Fq "@$target"; then
    pass "$label already runs the rollback target digest — rolling back would be a no-op"
  else
    pass "$label does not run the rollback target digest — a revert would change it"
  fi
}

compare_running "cloud" "$CLOUD_DEPLOY" "$TARGET_CLOUD_DIGEST"
if [ -n "$HOSTED_DEPLOY" ]; then
  compare_running "hosted-controller" "$HOSTED_DEPLOY" "$TARGET_HOSTED_DIGEST"
else
  fail "hosted-controller deployment name not supplied (--hosted-controller-deployment); its digest was not verified"
fi

# ---------- 4. workload health ----------
check_deployment_health() {
  local label="$1" deploy="$2" fields="" generation observed desired updated ready
  if ! fields="$(kube deploy "$deploy" -n "$NAMESPACE" \
      -o jsonpath='{.metadata.generation}{"|"}{.status.observedGeneration}{"|"}{.spec.replicas}{"|"}{.status.updatedReplicas}{"|"}{.status.readyReplicas}' 2>/dev/null)"; then
    fail "$label deployment health is unreadable"
    return
  fi
  IFS='|' read -r generation observed desired updated ready <<<"$fields"
  if [[ ! "$generation" =~ ^[0-9]+$ || ! "$observed" =~ ^[0-9]+$ || \
        ! "$desired" =~ ^[0-9]+$ || ! "$updated" =~ ^[0-9]+$ || ! "$ready" =~ ^[0-9]+$ ]]; then
    fail "$label deployment health fields are missing or invalid"
  elif (( desired < 1 || observed < generation || updated < desired || ready < desired )); then
    fail "$label deployment is not current and fully ready (generation=$generation observed=$observed desired=$desired updated=$updated ready=$ready)"
  else
    pass "$label deployment is current and fully ready ($ready/$desired replicas)"
  fi
}
check_deployment_health "cloud" "$CLOUD_DEPLOY"
if [ -n "$HOSTED_DEPLOY" ]; then
  check_deployment_health "hosted-controller" "$HOSTED_DEPLOY"
fi

STRATEGY=""
REPLICAS=""
if REPLICAS="$(kube deploy "$CLOUD_DEPLOY" -n "$NAMESPACE" \
    -o jsonpath='{.spec.replicas}' 2>/dev/null)"; then
  :
fi
if STRATEGY="$(kube deploy "$CLOUD_DEPLOY" -n "$NAMESPACE" \
    -o jsonpath='{.spec.strategy.type}' 2>/dev/null)" && [ -n "$STRATEGY" ]; then
  info "cloud deployment strategy=$STRATEGY replicas=${REPLICAS:-unreadable}"
  if [ "$STRATEGY" = "Recreate" ]; then
    info "Recreate: the rollback itself is an outage window, not a rolling swap"
  fi
else
  fail "could not read the cloud deployment update strategy"
fi

# ---------- verdict ----------
echo
if [ "$FAILURES" -eq 0 ]; then
  echo "PREFLIGHT OK — $CHECKS checks, 0 failures."
  echo "This is a read-only report. It authorises nothing: the rollback is a git"
  echo "revert of the pin commit in the infrastructure repository, reconciled by Flux."
  exit 0
fi
echo "PREFLIGHT FAILED — $FAILURES of $CHECKS checks failed. Do not revert on this report."
exit 1
