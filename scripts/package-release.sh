#!/usr/bin/env bash
# Full-source release only: never run the historical split-edition exporter.
set -euo pipefail
version="${1:?usage: package-release.sh VERSION [OUTPUT_DIRECTORY]}"
output="${2:-dist}"
: "${GOOS:?set GOOS for the release target}"
: "${GOARCH:?set GOARCH for the release target}"
for component in "$version" "$GOOS" "$GOARCH"; do
  if [[ ! "$component" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
    echo "invalid release version or target" >&2
    exit 1
  fi
done
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$output"
output="$(cd "$output" && pwd)"
stage="$(mktemp -d "$output/.yscale-release.XXXXXX")"
trap 'rm -rf -- "$stage"' EXIT
name="yscale-${version}-${GOOS}-${GOARCH}"
bundle="$stage/$name"
mkdir -p "$bundle"
export CGO_ENABLED=0
# The factory reads deploy/headscale templates relative to its working
# directory. Review the exact inventory before building, and never recursively
# copy private docs or operator receipts. Run this helper on the BUILD host,
# not the requested GOOS/GOARCH target (including macOS cross-builds).
(cd "$root" && GOOS='' GOARCH='' go run ./scripts/release-export \
  -mode assets -output "$stage/assets")
cp -R "$stage/assets/." "$bundle/"
make -C "$root" build BIN_DIR="$bundle/bin"
# Compress and checksum inside the staging directory, then rename into place:
# a failed or interrupted run must never publish or replace a release archive.
tar -czf "$stage/$name.tar.gz" -C "$stage" "$name"
(cd "$stage" && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")
mv -f "$stage/$name.tar.gz" "$output/$name.tar.gz"
mv -f "$stage/$name.tar.gz.sha256" "$output/$name.tar.gz.sha256"
printf 'Packaged full managed-mesh stack: %s\n' "$output/$name.tar.gz"
