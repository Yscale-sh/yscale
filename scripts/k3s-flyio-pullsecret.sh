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

kubectl -n "$NS" delete secret "$NAME" --ignore-not-found
kubectl -n "$NS" create secret generic "$NAME" \
  --type=kubernetes.io/dockerconfigjson \
  --from-file=.dockerconfigjson="$CONFIG"

echo "created Secret $NS/$NAME from $CONFIG"
