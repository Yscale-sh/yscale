#!/usr/bin/env bash
#
# yscale source-fastpath GRANT — the AGENT-side reconcile logic (the action central drives
# per opt-in workload). Deploys a jailed, read-only, TLS + bearer-token + IP-allowlisted
# source gateway in the CUSTOMER's cluster and exposes it via a Service type=LoadBalancer:
#   - managed EKS/GKE/AKS  → cloud hands back a PUBLIC IP (no router/edge API needed)
#   - on-prem / self-managed → MetalLB LAN IP (+ the customer's own edge port-forward;
#     automating that port-forward is router/vendor-specific and out of scope here)
#
# Generalizable: the source is whatever volume the customer designates (NFS or PVC) — NOT
# hardcoded to media. Pass one or more --mount <bucket>=<subPath> to expose only those
# subtrees, read-only (the jail). Central never touches customer data/edges; the agent does.
#
# Usage:
#   source-grant.sh --ns NS --name NAME --burst-ip IP \
#     ( --nfs-server IP --nfs-path PATH | --pvc PVCNAME ) \
#     --mount bucket=subPath [--mount bucket=subPath ...] \
#     [--node NODE] [--lb-ip IP] [--lb-pool POOL] [--token TOK] [--dry-run]
#
# --dry-run renders the Caddyfile + manifests to stdout and applies NOTHING (used by tests).
#
# Emits to stdout (the grant result central returns to the burst):
#   ENDPOINT=https://<lb-ip>:8443
#   TOKEN=<token>
set -euo pipefail

NS="" NAME="" BURST_IP="" NFS_SERVER="" NFS_PATH="" PVC="" NODE="" LB_IP="" LB_POOL="" TOKEN="" DRY_RUN=0
MOUNTS=()
while [ $# -gt 0 ]; do case "$1" in
  --ns) NS="$2"; shift 2;;
  --name) NAME="$2"; shift 2;;
  --burst-ip) BURST_IP="$2"; shift 2;;
  --nfs-server) NFS_SERVER="$2"; shift 2;;
  --nfs-path) NFS_PATH="$2"; shift 2;;
  --pvc) PVC="$2"; shift 2;;
  --mount) MOUNTS+=("$2"); shift 2;;
  --node) NODE="$2"; shift 2;;
  --lb-ip) LB_IP="$2"; shift 2;;
  --lb-pool) LB_POOL="$2"; shift 2;;
  --token) TOKEN="$2"; shift 2;;
  --dry-run) DRY_RUN=1; shift;;
  *) echo "unknown arg: $1" >&2; exit 2;;
esac; done

: "${NS:?--ns required}" "${NAME:?--name required}" "${BURST_IP:?--burst-ip required}"
[ ${#MOUNTS[@]} -gt 0 ] || { echo "at least one --mount bucket=subPath required" >&2; exit 2; }
[ -n "$NFS_SERVER$PVC" ] || { echo "--nfs-server/--nfs-path or --pvc required" >&2; exit 2; }
TOKEN="${TOKEN:-brst_$(head -c16 /dev/urandom | od -An -tx1 | tr -d ' \n')}"

# --- Caddyfile: deny-by-default; require allowlisted source IP AND bearer token AND GET/HEAD;
# serve the jailed mounts read-only. Built as a string so --dry-run can print it verbatim. ---
read -r -d '' CADDYFILE <<CF || true
{
	admin off
}
https://:8443 {
	tls /etc/caddy/tls/tls.crt /etc/caddy/tls/tls.key
	log {
		output stdout
		format json
	}
	@authed {
		remote_ip ${BURST_IP}/32
		header Authorization "Bearer ${TOKEN}"
		method GET HEAD
	}
	handle @authed {
		root * /srv
		file_server
	}
	handle {
		respond "403 denied" 403
	}
}
CF

# --- volumeMounts (one RO subPath per --mount) + the source volume (nfs or pvc) ---
VM=""; for m in "${MOUNTS[@]}"; do b="${m%%=*}"; sp="${m#*=}"
  VM+=$'\n''        - {name: src, mountPath: /srv/'"$b"', subPath: "'"$sp"'", readOnly: true}'
done
if [ -n "$PVC" ]; then
  VOL="      - {name: src, persistentVolumeClaim: {claimName: $PVC, readOnly: true}}"
else
  VOL="      - name: src"$'\n'"        nfs: {server: \"$NFS_SERVER\", path: \"$NFS_PATH\", readOnly: true}"
fi
NODESEL=""; [ -n "$NODE" ] && NODESEL=$'\n'"      nodeSelector: {kubernetes.io/hostname: $NODE}"
SVC_ANN=""; [ -n "$LB_IP" ] && SVC_ANN=$'\n'"    metallb.universe.tf/loadBalancerIPs: \"$LB_IP\""
[ -n "$LB_POOL" ] && SVC_ANN="$SVC_ANN"$'\n'"    metallb.universe.tf/address-pool: \"$LB_POOL\""

read -r -d '' MANIFEST <<YAML || true
apiVersion: apps/v1
kind: Deployment
metadata: {name: $NAME, namespace: $NS, labels: {app: $NAME, yscale.sh/source-grant: "true"}}
spec:
  replicas: 1
  selector: {matchLabels: {app: $NAME}}
  template:
    metadata: {labels: {app: $NAME}}
    spec:$NODESEL
      containers:
      - name: caddy
        image: caddy:2
        command: ["caddy","run","--config","/etc/caddy/Caddyfile","--adapter","caddyfile"]
        ports: [{containerPort: 8443}]
        volumeMounts:
        - {name: cfg, mountPath: /etc/caddy}
        - {name: tls, mountPath: /etc/caddy/tls, readOnly: true}$VM
        resources: {requests: {cpu: "500m", memory: "256Mi"}}
      volumes:
      - {name: cfg, configMap: {name: $NAME-caddy}}
      - {name: tls, secret: {secretName: $NAME-tls}}
$VOL
---
apiVersion: v1
kind: Service
metadata:
  name: $NAME
  namespace: $NS
  annotations:$SVC_ANN
spec:
  type: LoadBalancer
  externalTrafficPolicy: Local   # preserve burst source IP for the allowlist
  selector: {app: $NAME}
  ports: [{port: 8443, targetPort: 8443, protocol: TCP}]
YAML

if [ "$DRY_RUN" = 1 ]; then
  echo "### Caddyfile"
  printf '%s\n' "$CADDYFILE"
  echo "### Manifests"
  printf '%s\n' "$MANIFEST"
  echo "ENDPOINT=https://${LB_IP:-<lb-pending>}:8443"
  echo "TOKEN=$TOKEN"
  exit 0
fi

# --- apply path: per-grant self-signed cert (managed k8s should prefer cert-manager/LE) ---
CERTDIR="$(mktemp -d)"; trap 'rm -rf "$CERTDIR"' EXIT
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$CERTDIR/tls.key" -out "$CERTDIR/tls.crt" \
  -days 7 -subj "/CN=yscale-source-$NAME" -addext "subjectAltName=IP:${LB_IP:-127.0.0.1}" >/dev/null 2>&1
printf '%s\n' "$CADDYFILE" > "$CERTDIR/Caddyfile"

kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null 2>&1 || true
kubectl -n "$NS" create configmap "$NAME-caddy" --from-file=Caddyfile="$CERTDIR/Caddyfile" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" create secret generic "$NAME-tls" --from-file=tls.crt="$CERTDIR/tls.crt" --from-file=tls.key="$CERTDIR/tls.key" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
printf '%s\n' "$MANIFEST" | kubectl apply -f - >/dev/null

kubectl -n "$NS" rollout status deploy/"$NAME" --timeout=150s >/dev/null 2>&1 || true
EP=""
for _ in $(seq 1 30); do
  EP=$(kubectl -n "$NS" get svc "$NAME" -o jsonpath='{.status.loadBalancer.ingress[0].ip}{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
  [ -n "$EP" ] && break; sleep 2
done
echo "ENDPOINT=https://${EP:-<pending>}:8443"
echo "TOKEN=$TOKEN"
