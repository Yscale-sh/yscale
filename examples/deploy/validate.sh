#!/usr/bin/env bash
# Validate Helm templates for all values overlay examples.
# Fails if helm template fails for any overlay.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHART_DIR="${REPO_DIR}/deploy/helm/yscale-agent"

command -v helm >/dev/null 2>&1 || {
  echo "helm is required" >&2
  exit 127
}

shopt -s nullglob
OVERLAYS=("${SCRIPT_DIR}"/values-*.yaml)
if ((${#OVERLAYS[@]} == 0)); then
  echo "no deployment overlays found" >&2
  exit 1
fi

errors=0

for overlay_path in "${OVERLAYS[@]}"; do
  overlay="$(basename "${overlay_path}")"
  printf 'Validating %s... ' "${overlay}"
  if ! helm template yscale-agent "${CHART_DIR}" -f "${overlay_path}" >/dev/null; then
    echo "FAILED"
    errors=$((errors + 1))
  else
    echo "OK"
  fi
done

if [ "$errors" -ne 0 ]; then
  echo "Validation failed with ${errors} errors."
  exit 1
fi

echo "All overlays validated successfully."
