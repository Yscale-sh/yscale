#!/usr/bin/env bash
# scripts/test-immutable-images.sh — proves the immutable-image reference
# contract for both first-party Helm charts (REF #19).
#
# For EACH chart it asserts, by rendering `templates/deployment.yaml`:
#   1. tag fallback  — no digest + requireDigest=false renders the SAME
#                      `repository:tag` reference as before (back-compat).
#   2. precedence    — a valid digest renders `repository@sha256:<64 hex>`
#                      and takes precedence over the tag.
#   3. sensitivity   — changing ONLY the digest changes the Pod-template image.
#   4. reject bad    — a malformed non-empty digest fails rendering, even when
#                      requireDigest is false (tags / short / uppercase /
#                      other algorithms / embedded repository all rejected).
#   5. enforcement   — requireDigest=true with no digest fails rendering.
#
# The first-party image is the CONTROLLER for deploy/helm/yscale and the
# Cluster Connector (agent container) for deploy/helm/yscale-agent. Third-party
# sidecars (tailscale, curl init) are intentionally out of scope.
#
# Usage: scripts/test-immutable-images.sh
# Exit 0 iff every assertion passes.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Prefer a helm on PATH; fall back to the pinned mise toolchain used elsewhere.
if command -v helm >/dev/null 2>&1; then
  HELM=(helm)
elif command -v mise >/dev/null 2>&1; then
  HELM=(mise x helm@3 -- helm)
else
  echo "FATAL: neither 'helm' nor 'mise' found on PATH" >&2
  exit 1
fi

# Two distinct, well-formed digests (64 lowercase hex after sha256:).
DIGEST_A="sha256:$(printf 'a%.0s' {1..64})"
DIGEST_B="sha256:$(printf 'b%.0s' {1..64})"

FAILURES=0
pass() { printf '  \033[32m✓\033[0m %s\n' "$1"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$1"; [ -n "${2:-}" ] && printf '%s\n' "$2" | sed 's/^/      /'; FAILURES=$((FAILURES + 1)); }

# render <chart> [helm args...] — full manifest on success (stdout);
# on failure prints helm's error and returns non-zero.
render() {
  local chart="$1"; shift
  "${HELM[@]}" template test "$chart" --show-only templates/deployment.yaml "$@" 2>&1
}

# image_of <chart> <repo> [helm args...] — the resolved first-party image
# reference (the `image:` line matching <repo>); returns helm's exit code.
image_of() {
  local chart="$1" repo="$2"; shift 2
  local out
  if ! out=$(render "$chart" "$@"); then
    printf '%s' "$out"
    return 1
  fi
  printf '%s' "$out" | grep 'image:' | grep -F "$repo" | head -n1 \
    | sed -E 's/.*image:[[:space:]]*//; s/^"//; s/"$//'
}

# check_chart <name> <chart-path> <values-prefix> <repo> <expected-tag-image>
check_chart() {
  local name="$1" chart="$2" prefix="$3" repo="$4" want_tag="$5"

  echo "== $name ($chart) =="

  # 1. tag fallback unchanged (additive: new fields absent → identical image).
  local got_tag
  got_tag=$(image_of "$chart" "$repo")
  if [ "$got_tag" = "$want_tag" ]; then
    pass "tag fallback renders unchanged: $got_tag"
  else
    fail "tag fallback changed" "expected: $want_tag"$'\n'"actual:   $got_tag"
  fi

  # 2. valid digest takes precedence → repository@sha256:<hex>.
  local got_a want_a="${repo}@${DIGEST_A}"
  got_a=$(image_of "$chart" "$repo" --set-string "$prefix.digest=$DIGEST_A")
  if [ "$got_a" = "$want_a" ]; then
    pass "digest takes precedence: $got_a"
  else
    fail "digest precedence wrong" "expected: $want_a"$'\n'"actual:   $got_a"
  fi

  # 2b. digest wins even when a tag is also set.
  local got_both
  got_both=$(image_of "$chart" "$repo" --set-string "$prefix.digest=$DIGEST_A" --set-string "$prefix.tag=someothertag")
  if [ "$got_both" = "$want_a" ]; then
    pass "digest overrides an explicit tag"
  else
    fail "digest did not override tag" "expected: $want_a"$'\n'"actual:   $got_both"
  fi

  # 3. changing ONLY the digest changes the Pod-template image.
  local got_b
  got_b=$(image_of "$chart" "$repo" --set-string "$prefix.digest=$DIGEST_B")
  if [ "$got_b" != "$got_a" ] && [ "$got_b" = "${repo}@${DIGEST_B}" ]; then
    pass "changing the digest changes the image"
  else
    fail "digest change not reflected" "A: $got_a"$'\n'"B: $got_b"
  fi

  # 4. malformed non-empty digests are rejected (enforcement OFF).
  local bad desc
  local -a bads=(
    "v1.2.3|a tag"
    "sha256:abc123|a short hash"
    "sha256:$(printf 'A%.0s' {1..64})|uppercase hex"
    "sha512:$(printf 'a%.0s' {1..64})|a non-sha256 algorithm"
    "${repo}@${DIGEST_A}|an embedded repository"
  )
  for entry in "${bads[@]}"; do
    bad="${entry%%|*}"; desc="${entry#*|}"
    if render "$chart" --set-string "$prefix.digest=$bad" >/dev/null 2>&1; then
      fail "malformed digest accepted ($desc): $bad"
    else
      pass "rejects malformed digest ($desc)"
    fi
  done

  # 5. production enforcement: requireDigest=true with no digest fails.
  if render "$chart" --set "$prefix.requireDigest=true" >/dev/null 2>&1; then
    fail "requireDigest=true rendered without a digest"
  else
    pass "requireDigest=true fails when no digest supplied"
  fi

  # 5b. enforcement satisfied by a valid digest still renders.
  if image_of "$chart" "$repo" --set "$prefix.requireDigest=true" --set-string "$prefix.digest=$DIGEST_A" >/dev/null; then
    pass "requireDigest=true renders with a valid digest"
  else
    fail "requireDigest=true rejected a valid digest"
  fi

  echo
}

echo "helm: ${HELM[*]}"
echo

check_chart "yscale controller" \
  "deploy/helm/yscale" "image" \
  "ghcr.io/jakenesler/yscale-controller" \
  "ghcr.io/jakenesler/yscale-controller:latest"

# The connector's tag fallback is the chart's appVersion: the release it ships in.
AGENT_APP_VERSION=$(sed -nE 's/^appVersion:[[:space:]]*"?([^"[:space:]]+)"?[[:space:]]*$/\1/p' \
  deploy/helm/yscale-agent/Chart.yaml)
check_chart "yscale-agent (Cluster Connector)" \
  "deploy/helm/yscale-agent" "agent.image" \
  "ghcr.io/yscale-sh/yscale-cluster-agent" \
  "ghcr.io/yscale-sh/yscale-cluster-agent:${AGENT_APP_VERSION}"

if [ "$FAILURES" -eq 0 ]; then
  echo "ALL IMMUTABLE-IMAGE CHECKS PASSED"
  exit 0
fi
echo "IMMUTABLE-IMAGE CHECKS FAILED: $FAILURES assertion(s)"
exit 1
