#!/usr/bin/env bash
#
# yscale source-fastpath REVOKE — the AGENT-side teardown (central drives on workload
# teardown). Removes the per-grant source gateway: deletes the Deployment/Service (releases
# the LoadBalancer IP), ConfigMap (token+allowlist), and TLS secret. Idempotent.
#
# Usage: source-revoke.sh --ns NS --name NAME [--delete-ns]
set -euo pipefail
NS="" NAME="" DELNS=0
while [ $# -gt 0 ]; do case "$1" in
  --ns) NS="$2"; shift 2;;
  --name) NAME="$2"; shift 2;;
  --delete-ns) DELNS=1; shift;;
  *) echo "unknown arg: $1" >&2; exit 2;;
esac; done
: "${NS:?--ns required}" "${NAME:?--name required}"

kubectl -n "$NS" delete deploy "$NAME" svc "$NAME" configmap "$NAME-caddy" secret "$NAME-tls" \
  --ignore-not-found --wait=false 2>&1 | sed 's/^/revoked: /' || true
[ "$DELNS" = 1 ] && kubectl delete ns "$NS" --ignore-not-found --wait=false 2>&1 | sed 's/^/revoked: /'
echo "revoke complete: $NS/$NAME"
