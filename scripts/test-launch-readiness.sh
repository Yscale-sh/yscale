#!/usr/bin/env bash
# scripts/test-launch-readiness.sh — deterministic launch-readiness gate.
#
# Proves the launch-evidence validator works (positive and adversarial
# regression tests), that the checked-in release manifest is schema-valid,
# that the supported-configuration matrix doc matches the manifest, and —
# critically — that the actual readiness command still FAILS CLOSED while
# live launch gates are unproven. A readiness pass against the real
# manifest is treated as a gate FAILURE here: nothing in this repository
# may certify the launch until operators supply real evidence.
#
# Runs locally and in CI with no network, no cluster, and no credentials.
# The validator binary is built into a temporary directory and cleaned up;
# nothing is written into the repository tree.
#
# Usage:
#   scripts/test-launch-readiness.sh
#
# Exit 0 = validator proven and real manifest correctly not-ready.
# Any other exit = the gate failed; do not ship.

set -euo pipefail

cd "$(dirname "$0")/.."

MANIFEST="docs/release/manifest.json"
MATRIX_DOC="docs/release/supported-configurations.md"

# The exact diagnostic the fail-closed path prints. Any other failure —
# a panic, a missing manifest, a bad invocation — also exits non-zero and
# MUST NOT be accepted as "expected not-ready".
NOT_READY_MARKER="launch readiness: NOT READY (fail closed)"

fail() { echo "FAIL: $*" >&2; exit 1; }

[ -f "$MANIFEST" ] || fail "release manifest missing: $MANIFEST"
[ -f "$MATRIX_DOC" ] || fail "supported-configuration matrix missing: $MATRIX_DOC"

echo "==> validator regression tests (positive + adversarial)"
go test -count=1 ./internal/launch || fail "internal/launch tests failed"

echo "==> workflow service-port guard"
go test -count=1 ./test/workflows || fail "workflow guard tests failed"

echo "==> validator builds (into a temporary directory, not the repo tree)"
BIN_DIR="$(mktemp -d)"
trap 'rm -rf "$BIN_DIR"' EXIT
BIN="$BIN_DIR/yscale-launch-readiness"
go build -o "$BIN" ./cmd/yscale-launch-readiness || fail "validator does not build"

echo "==> checked-in manifest is schema-valid (schema validity != readiness)"
"$BIN" schema -manifest "$MANIFEST" \
  || fail "checked-in manifest failed schema validation"

echo "==> matrix doc matches manifest"
"$BIN" matrix -manifest "$MANIFEST" -verify -doc "$MATRIX_DOC" \
  || fail "matrix doc does not match manifest"

echo "==> readiness MUST fail closed while live gates are unproven"
READINESS_CODE=0
READINESS_OUT="$("$BIN" readiness -manifest "$MANIFEST" 2>&1)" || READINESS_CODE=$?
printf '%s\n' "$READINESS_OUT"
if [ "$READINESS_CODE" -eq 0 ]; then
  fail "readiness unexpectedly PASSED against the real manifest —" \
    "refusing to certify; a launch pass requires operator-supplied evidence"
fi
if [ "$READINESS_CODE" -ne 1 ]; then
  fail "readiness exited $READINESS_CODE, not the normal not-ready exit 1 —" \
    "this is a validator/invocation error, not an evaluated not-ready verdict"
fi
case "$READINESS_OUT" in
  *"$NOT_READY_MARKER"*) ;;
  *) fail "readiness exit 1 without the expected diagnostic '$NOT_READY_MARKER' —" \
       "refusing to treat an unexplained failure as an evaluated verdict" ;;
esac
echo "OK: readiness correctly reports not-ready with actionable gates"

echo "PASS: launch-readiness gate (validator proven; real manifest not-ready)"
