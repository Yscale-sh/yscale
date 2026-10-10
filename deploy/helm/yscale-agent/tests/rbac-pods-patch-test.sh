#!/usr/bin/env bash
set -euo pipefail

# Verify that the rendered RBAC for both cluster-scope and namespace-scope
# workload roles includes the pods "patch" verb that the pending-pod watcher
# requires to label triggering pods.

echo "=== Running helm lint ==="
helm lint deploy/helm/yscale-agent

echo "=== Cluster-scope: rendering rbac.yaml ==="
CLUSTER=$(helm template deploy/helm/yscale-agent \
  --set rbac.create=true \
  --set rbac.scope=cluster \
  -s templates/rbac.yaml)

if ! echo "$CLUSTER" | grep -A5 'resources: \[pods\]' | grep -q 'patch'; then
  echo "FAIL: cluster-scope workload role missing pods patch verb"
  exit 1
fi
echo "PASS: cluster-scope has pods patch"

echo "=== Namespace-scope: rendering rbac.yaml ==="
NS=$(helm template deploy/helm/yscale-agent \
  --set rbac.create=true \
  --set "rbac.allowedNamespaces[0]=jobs" \
  -s templates/rbac.yaml)

if ! echo "$NS" | grep -A5 'resources: \[pods\]' | grep -q 'patch'; then
  echo "FAIL: namespace-scope workload role missing pods patch verb"
  exit 1
fi
echo "PASS: namespace-scope has pods patch"

# Verify the artifact role does NOT gain pods patch.
ARTIFACT=$(echo "$NS" | sed -n '/yscale-agent-artifacts/,/---/p')
if echo "$ARTIFACT" | grep -A5 'resources: \[pods\]' | grep -q 'patch'; then
  echo "FAIL: artifact role gained pods patch — should stay narrow"
  exit 1
fi
echo "PASS: artifact role does not have pods patch"

echo "=== All RBAC assertions passed ==="
