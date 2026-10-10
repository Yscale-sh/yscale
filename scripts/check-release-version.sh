#!/usr/bin/env bash
# scripts/check-release-version.sh <tag> — fails unless every version the shipped
# tree pins names <tag>: the connector chart's appVersion (its default image
# tag), each ghcr.io/yscale-sh image reference under deploy/, and the
# quickstart's download VERSION. A release must never publish a chart or
# manifest whose default image tag it did not build.
set -euo pipefail

tag="${1:?usage: scripts/check-release-version.sh <tag>}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail=0
mismatch() { echo "release version mismatch: $1 (want $tag)" >&2; fail=1; }

app=$(sed -nE 's/^appVersion:[[:space:]]*"?([^"[:space:]]+)"?[[:space:]]*$/\1/p' \
  "$root/deploy/helm/yscale-agent/Chart.yaml")
[ "$app" = "$tag" ] || mismatch "deploy/helm/yscale-agent/Chart.yaml appVersion=$app"

refs=$(grep -rhoE 'ghcr\.io/yscale-sh/yscale-(cloud|factory|cluster-agent):[A-Za-z0-9._-]+' \
  "$root/deploy" | sort -u)
[ -n "$refs" ] || mismatch "no ghcr.io/yscale-sh image reference found under deploy/"
while IFS= read -r ref; do
  [ -z "$ref" ] || [ "${ref##*:}" = "$tag" ] || mismatch "$ref"
done <<<"$refs"

for doc in docs/quickstart.md website/content/release/docs/quickstart.md; do
  version=$(sed -nE 's/^VERSION=([^[:space:]]+).*/\1/p' "$root/$doc" | head -n1)
  [ "$version" = "$tag" ] || mismatch "$doc VERSION=$version"
done

[ "$fail" -eq 0 ] && echo "release version pins all name $tag"
exit "$fail"
