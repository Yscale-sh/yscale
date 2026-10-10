#!/usr/bin/env bash
# timed.sh — wraps a command and reports its duration in MM:SS to
# stderr after it runs. Exit code preserved.
#
# Used by `make e2e` to time each step so the user sees where time
# goes:
#   ⏱  cloud-image-push: 01:34
#   ⏱  agent-image-push: 00:47
#   ⏱  rollout:          00:42
#   ⏱  preflight:        00:18
#   ⏱  yscaletest:       05:11
#   ────────────────────────────
#   ⏱  TOTAL:            08:32
#
# Usage:
#   scripts/timed.sh <label> -- <command...>

set -euo pipefail

LABEL="${1:?label required}"
shift
if [ "${1:-}" = "--" ]; then shift; fi

START="$(date +%s)"
# Use a temp file so we can dump the duration to its own row in
# stderr without interleaving with the command's own output.
"$@"
RC=$?
END="$(date +%s)"

DUR=$((END - START))
MIN=$((DUR / 60))
SEC=$((DUR % 60))
printf '⏱  %-22s %02d:%02d\n' "$LABEL:" "$MIN" "$SEC" >&2

exit $RC
