#!/usr/bin/env bash
set -euo pipefail

# Regression: a gateway release installed OUTSIDE central's namespace must
# still get the hostAliases /etc/hosts pin for the in-cluster central
# Service (live shape: release in yscale-system, central Service
# yscale/yscale-cloud, endpoint ws://yscale-cloud.yscale:8443). The
# gateway pod runs dnsPolicy None, so a lookup that misses renders no pin
# and the ts-auth-fetch init container can never resolve central.
#
# `helm template` runs `lookup` against no cluster and gets nil back, so
# what is testable offline is the namespace SELECTION feeding the lookup.
# That lives in the yscale-agent.gatewayCentralServiceNamespace helper: a
# throwaway probe template rendered from a COPY of the chart observes it,
# and a source assertion ties the real lookup line to that same helper.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$REPO_ROOT"
CHART=deploy/helm/yscale-agent

fail() { echo "FAIL: $1" >&2; exit 1; }

echo "=== Running helm lint ==="
helm lint "$CHART"

echo "=== Default render: single-release behaviour unchanged ==="
RENDERED=$(helm template gw "$CHART" \
  --namespace yscale-system \
  --set gateway.enabled=true \
  -s templates/gateway-deployment.yaml)
grep -q 'dnsPolicy: None' <<<"$RENDERED" || fail "dnsPolicy None missing from default render"
grep -q '1.1.1.1' <<<"$RENDERED" || fail "public resolvers missing from default render"
# Client-side lookup returns nil — the hostAliases guard must hold.
if grep -q '^[[:space:]]*hostAliases:' <<<"$RENDERED"; then
  fail "hostAliases rendered with no Service visible to lookup"
fi

echo "=== Render with gateway.centralServiceNamespace=yscale ==="
RENDERED=$(helm template gw "$CHART" \
  --namespace yscale-system \
  --set gateway.enabled=true \
  --set gateway.centralServiceNamespace=yscale \
  -s templates/gateway-deployment.yaml)
grep -q 'dnsPolicy: None' <<<"$RENDERED" || fail "dnsPolicy None lost when centralServiceNamespace is set"
if grep -q '^[[:space:]]*hostAliases:' <<<"$RENDERED"; then
  fail "hostAliases rendered from a nil lookup when centralServiceNamespace is set"
fi

echo "=== Namespace selection (helper probe) ==="
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT
cp -R "$CHART" "$TMP_DIR/chart"
cat > "$TMP_DIR/chart/templates/zz-central-ns-probe.yaml" <<'EOF'
{{- /* Test-only probe: renders the SAME helper the gateway lookup consumes. */}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: zz-central-ns-probe
data:
  centralLookupNamespace: {{ include "yscale-agent.gatewayCentralServiceNamespace" . | quote }}
EOF

probe() {
  helm template gw "$TMP_DIR/chart" -s templates/zz-central-ns-probe.yaml "$@" \
    | awk '/centralLookupNamespace:/ {print $2}'
}

GOT=$(probe --namespace yscale-system)
[ "$GOT" = '"yscale-system"' ] || fail "default lookup namespace: expected \"yscale-system\" (the release namespace), got $GOT"

GOT=$(probe --namespace yscale-system --set gateway.centralServiceNamespace=yscale)
[ "$GOT" = '"yscale"' ] || fail "overridden lookup namespace: expected \"yscale\", got $GOT"

GOT=$(probe --namespace yscale-system --set gateway.centralServiceNamespace=)
[ "$GOT" = '"yscale-system"' ] || fail "explicit empty override must fall back to the release namespace, got $GOT"

echo "=== Gateway lookup consumes the helper ==="
if ! grep -qF 'lookup "v1" "Service" (include "yscale-agent.gatewayCentralServiceNamespace" .) "yscale-cloud"' \
    "$CHART/templates/gateway-deployment.yaml"; then
  fail "gateway-deployment.yaml lookup does not take its namespace from yscale-agent.gatewayCentralServiceNamespace"
fi

echo "=== All gateway central-namespace regression checks passed! ==="
