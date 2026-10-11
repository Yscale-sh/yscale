#!/usr/bin/env bash
# Creates a docker-registry Secret in the `yscale` namespace so k3s
# can pull yscale-cloud + yscale-cluster-agent from registry.fly.io.
#
# Prereqs:
#   flyctl auth docker     # writes Fly creds into ~/.docker/config.json
#   kubectl get ns yscale  # namespace must exist
#
# Re-runnable: deletes the existing Secret first.
set -euo pipefail

NS="${NAMESPACE:-yscale}"
NAME="${SECRET_NAME:-fly-registry}"
CONFIG="${DOCKER_CONFIG:-$HOME/.docker/config.json}"

if [ ! -f "$CONFIG" ]; then
  echo "error: $CONFIG not found. Run 'flyctl auth docker' first." >&2
  exit 1
fi

if ! grep -q "registry.fly.io" "$CONFIG"; then
  echo "error: no registry.fly.io creds in $CONFIG. Run 'flyctl auth docker' first." >&2
  exit 1
fi

# Copy ONLY the registry.fly.io entry. A Docker config usually holds
# credentials for other registries too, and an image pull Secret is readable
# by anyone who can read Secrets in the namespace.
FILTERED="$(mktemp)"
trap 'rm -f "$FILTERED"' EXIT
jq '{auths: {"registry.fly.io": .auths["registry.fly.io"]}}' "$CONFIG" > "$FILTERED"

kubectl -n "$NS" delete secret "$NAME" --ignore-not-found
kubectl -n "$NS" create secret generic "$NAME" \
  --type=kubernetes.io/dockerconfigjson \
  --from-file=.dockerconfigjson="$FILTERED"

echo "created Secret $NS/$NAME from $CONFIG"
