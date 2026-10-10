#!/bin/bash
#
# yscale AWS burst bootstrap — JOIN-ONLY variant for baked AMIs.
#
# Used when the EC2 burst boots from an AMI produced by
# pkg/backends/aws/ami/build-ami.sh — that AMI already carries
# tailscale, kubelet, containerd, runc, cilium binaries + BPF
# precompile, and the CNI plugins. This script does ONLY the per-
# burst join; the cold-boot variant (full install) is bootstrap.sh.
#
# Per-burst values arrive as shell exports CreateNode prepends ahead
# of this body.

set -eo pipefail
exec > /var/log/yscale-bootstrap.log 2>&1
trap 'EC=$?; echo "[yscale] BOOTSTRAP FAILED at line $LINENO (exit=$EC); see /var/log/yscale-bootstrap.log on the host" > /dev/console 2>/dev/null; echo "FAILED line=$LINENO exit=$EC" > /var/log/yscale-bootstrap.FAILED; exit $EC' ERR

__T0_NS=$(date +%s%N)
log_t() {
  local now_ns elapsed_ms
  now_ns=$(date +%s%N)
  elapsed_ms=$(( (now_ns - __T0_NS) / 1000000 ))
  printf '[%4d.%03ds] %s\n' $((elapsed_ms / 1000)) $((elapsed_ms % 1000)) "$*"
}

log_t "join-only bootstrap starting $(date -u)"

: "${TS_AUTHKEY:?TS_AUTHKEY required}"
: "${TS_HOSTNAME:?TS_HOSTNAME required}"
: "${NODE_NAME:?NODE_NAME required}"
: "${BURST_ID:?BURST_ID required}"
: "${POD_CIDR:?POD_CIDR required (e.g. 10.42.7.0/24)}"
: "${BOOTSTRAP_ENDPOINT:?BOOTSTRAP_ENDPOINT required}"
: "${CLUSTER_DNS:=10.43.0.10}"
: "${CLUSTER_DOMAIN:=cluster.local}"
: "${TS_TAGS:=}"
# TS_LOGIN_SERVER points 'tailscale up' at a self-hosted coordination server;
# empty = Tailscale SaaS (no flag).
: "${TS_LOGIN_SERVER:=}"
: "${EXTRA_NODE_LABELS:=}"
: "${BURST_TIER:=full}"
: "${PROVIDER_ID:=aws://yscale-burst-${BURST_ID#burst_}}"

# full is the only supported tier here — the removed lite tier is
# refused before any provider-adjacent work runs. Central always emits
# BURST_TIER=full (pkg/backends/internal/agentenv), so any non-empty
# non-"full" value here is an out-of-date central or a tampered
# user-data and must exit clean.
case "${BURST_TIER}" in
  full) ;;
  lite)
    echo "[yscale] FATAL: BURST_TIER=lite: lite tier removed; only full/empty supported" >&2
    exit 2
    ;;
  *)
    echo "[yscale] FATAL: BURST_TIER=${BURST_TIER}: unsupported value; only full/empty supported" >&2
    exit 2
    ;;
esac

# tun device + IP forward sysctls.
mkdir -p /dev/net
[ -c /dev/net/tun ] || { mknod /dev/net/tun c 10 200; chmod 600 /dev/net/tun; }
sysctl -w net.ipv4.ip_forward=1 >/dev/null 2>&1 || true
swapoff -a 2>/dev/null || true

# TCP tuning for the high-BDP cross-WAN mesh path (see linode/bootstrap-baked.sh
# for the rationale): burst↔cluster TCP rides a ~32ms WireGuard tunnel; stock
# cubic + small buffers collapse on path loss and cap single-flow throughput.
# BBR (rate-based, loss-tolerant) + fq + larger windows lift it. Best-effort.
modprobe tcp_bbr 2>/dev/null || true
cat > /etc/sysctl.d/99-yscale-bbr.conf <<SYSCTLEOF
net.core.default_qdisc=fq
net.ipv4.tcp_congestion_control=bbr
net.core.rmem_max=67108864
net.core.wmem_max=67108864
net.ipv4.tcp_rmem=4096 131072 33554432
net.ipv4.tcp_wmem=4096 65536 33554432
SYSCTLEOF
sysctl -p /etc/sysctl.d/99-yscale-bbr.conf >/dev/null 2>&1 || true

# Bridge CNI fallback (cilium-agent overwrites this once ready). Needs
# only POD_CIDR, so it's written before containerd starts — containerd
# sees a CNI config from its very first scan.
mkdir -p /etc/cni/net.d /opt/cni/bin
cat > /etc/cni/net.d/10-yscale-bootstrap.conflist <<CNIEOF
{
  "cniVersion": "1.0.0",
  "name": "yscale-bootstrap",
  "plugins": [
    { "type": "bridge", "bridge": "cni0", "isGateway": true,
      "ipMasq": true, "hairpinMode": true,
      "ipam": { "type": "host-local",
                "ranges": [[{"subnet": "${POD_CIDR}"}]],
                "routes": [{"dst": "0.0.0.0/0"}] } },
    {"type": "portmap", "capabilities": {"portMappings": true}}
  ]
}
CNIEOF

# BPF + cgroup2 mounts the host-mode cilium agent needs.
mount bpffs -t bpf /sys/fs/bpf 2>/dev/null || true
mount cgroup2 -t cgroup2 /sys/fs/cgroup 2>/dev/null || true

# containerd config — minimal, kubelet-CRI on default socket. Started
# HERE, before the tailscale handshake, so its boot rides under the
# multi-second `tailscale up` instead of adding serial time after it.
# The socket readiness gate sits just before the cilium/kubelet section.
mkdir -p /etc/containerd
containerd config default > /etc/containerd/config.toml
sed -i 's|SystemdCgroup = false|SystemdCgroup = true|' /etc/containerd/config.toml
mkdir -p /run/containerd /var/lib/containerd
log_t "starting containerd (background)"
containerd >/var/log/containerd.log 2>&1 &

log_t "starting tailscaled"
env -u TS_AUTHKEY tailscaled \
    --state=/var/lib/tailscale/tailscaled.state \
    --socket=/var/run/tailscale/tailscaled.sock &
# Wait for the LocalAPI socket instead of a fixed sleep — it appears in
# well under a second; `tailscale up` fails hard without it.
for _ in $(seq 1 50); do
    [ -S /var/run/tailscale/tailscaled.sock ] && break
    sleep 0.1
done
install -m 600 /dev/null /run/ts-authkey
printf '%s' "${TS_AUTHKEY}" > /run/ts-authkey
TS_UP_ARGS="--auth-key=file:/run/ts-authkey --hostname=${TS_HOSTNAME} --accept-routes --reset"
[ -n "${TS_TAGS}" ] && TS_UP_ARGS="${TS_UP_ARGS} --advertise-tags=${TS_TAGS}"
[ -n "${POD_CIDR}" ] && TS_UP_ARGS="${TS_UP_ARGS} --advertise-routes=${POD_CIDR}"
[ -n "${TS_LOGIN_SERVER:-}" ] && TS_UP_ARGS="${TS_UP_ARGS} --login-server=${TS_LOGIN_SERVER}"
# shellcheck disable=SC2086
tailscale up ${TS_UP_ARGS}
shred -u /run/ts-authkey 2>/dev/null || rm -f /run/ts-authkey
unset TS_AUTHKEY

log_t "waiting for tailscale"
until tailscale status --peers=false >/dev/null 2>&1; do sleep 0.2; done
TS_IP=$(tailscale ip -4)
log_t "tailscale up: ${TS_IP}"

# Fetch bootstrap kubeconfig from the agent. retry-delay 1 (not 3):
# the announce normally lands at the agent long before the VM boots,
# so a refused first attempt is a fast-retry case, not a back-off one.
mkdir -p /etc/kubernetes
log_t "fetching bootstrap kubeconfig from ${BOOTSTRAP_ENDPOINT}"
BOOT_REQ=$(printf '{"burst_id":"%s","node_name":"%s"}' "${BURST_ID}" "${NODE_NAME}")
if ! BOOT_RESP=$(curl -fsS -X POST -H 'Content-Type: application/json' \
    --retry 30 --retry-delay 1 --retry-connrefused --retry-all-errors \
    --max-time 90 \
    -d "${BOOT_REQ}" \
    "${BOOTSTRAP_ENDPOINT}/bootstrap-kubeconfig"); then
  log_t "bootstrap fetch failed"
  exit 1
fi
echo "${BOOT_RESP}" | jq -er '.kubeconfig' | base64 -d > /etc/kubernetes/bootstrap-kubeconfig.yaml

# GKE's validate-node-providerid policy requires the provider ID to end in
# /<node-name>. The normal EC2 synthetic ID uses the longer
# yscale-burst-<id> spelling, while the authoritative Kubernetes Node name is
# ys-burst-<id>, so GKE rejects registration after approving the client CSR.
# Preserve the aws:// identity, but make its suffix match the real Node name.
CLUSTER_CLOUD="$(echo "${BOOT_RESP}" | jq -r '.cloud_provider // empty' 2>/dev/null || true)"
if [ "${CLUSTER_CLOUD}" = "gcp" ]; then
  PROVIDER_ID="aws://${NODE_NAME}"
  log_t "GKE cluster: providerID=${PROVIDER_ID} (admission requires /<node-name> suffix)"
fi

# Cilium kubeconfig for tier=full.
HOST_CILIUM=0
if [ "${BURST_TIER}" = "full" ] && echo "${BOOT_RESP}" | jq -er '.cilium_kubeconfig' >/dev/null 2>&1; then
    mkdir -p /etc/cilium
    echo "${BOOT_RESP}" | jq -er '.cilium_kubeconfig' | base64 -d > /etc/cilium/kubeconfig
    chmod 0600 /etc/cilium/kubeconfig
    HOST_CILIUM=1
    log_t "tier=full + cilium-kubeconfig received"
fi

# containerd readiness gate: it was started before the tailscale
# handshake and has had that whole window to come up, so this is
# normally an instant pass-through.
for _ in $(seq 1 50); do
    [ -S /run/containerd/containerd.sock ] && break
    sleep 0.2
done

# Host-mode cilium-agent — only if tier=full + we got the kubeconfig.
if [ "${HOST_CILIUM:-0}" = "1" ] && [ -x /usr/local/bin/cilium-agent ]; then
    log_t "starting host-mode cilium-agent"
    mkdir -p /var/run/cilium /var/lib/cilium /etc/cilium-config
    APISERVER="$(awk '/server:/ {print $2}' /etc/cilium/kubeconfig | head -1)"
    TOKEN="$(awk '/token:/ {print $2}' /etc/cilium/kubeconfig | head -1)"
    CM_JSON="$(curl -sS --insecure -H "Authorization: Bearer $TOKEN" \
        "$APISERVER/api/v1/namespaces/kube-system/configmaps/cilium-config" 2>/dev/null || echo '{}')"
    if echo "$CM_JSON" | jq -e '.data' >/dev/null 2>&1; then
        echo "$CM_JSON" | jq -r '.data | keys[]' | while IFS= read -r k; do
            echo "$CM_JSON" | jq -r --arg key "$k" '.data[$key]' > "/etc/cilium-config/$k"
        done
    fi
    echo false > /etc/cilium-config/enable-hubble
    echo false > /etc/cilium-config/enable-bandwidth-manager
    echo false > /etc/cilium-config/enable-bgp-control-plane

    # Pre-warm the BPF template cache from the install-time precompile.
    if [ -d /var/lib/cilium/bpf/.precompiled ]; then
        cp -a /var/lib/cilium/bpf/.precompiled/*.o /var/lib/cilium/bpf/ 2>/dev/null || true
        touch /var/lib/cilium/bpf/*.o 2>/dev/null || true
    fi

    nohup /usr/local/bin/cilium-agent \
        --k8s-kubeconfig-path=/etc/cilium/kubeconfig \
        --k8s-namespace=kube-system \
        --config-dir=/etc/cilium-config \
        --state-dir=/var/run/cilium \
        --write-cni-conf-when-ready=/etc/cni/net.d/05-cilium.conflist \
        > /var/log/cilium-agent.log 2>&1 &
    for i in $(seq 1 60); do
        [ -f /etc/cni/net.d/05-cilium.conflist ] && { log_t "cilium ready ($i s)"; break; }
        sleep 1
    done
fi

# Taints/labels — full is the only supported tier here (guarded above);
# HOST_CILIUM selects the host-mode vs DaemonSet Cilium path.
if [ "${HOST_CILIUM:-0}" = "1" ]; then
    REGISTER_TAINTS="yscale.sh/burst-node=true:NoSchedule"
    CILIUM_MODE_LABEL=",yscale.sh/cilium-mode=host"
else
    REGISTER_TAINTS="yscale.sh/burst-node=true:NoSchedule,node.cilium.io/agent-not-ready=true:NoSchedule"
    CILIUM_MODE_LABEL=""
fi

cat > /etc/systemd/system/yscale-kubelet.service <<UNITEOF
[Unit]
Description=yscale burst kubelet
After=containerd.service tailscaled.service

[Service]
ExecStart=/usr/local/bin/kubelet \\
  --bootstrap-kubeconfig=/etc/kubernetes/bootstrap-kubeconfig.yaml \\
  --kubeconfig=/etc/kubernetes/kubelet.kubeconfig \\
  --node-ip=${TS_IP} \\
  --provider-id=${PROVIDER_ID} \\
  --rotate-server-certificates \\
  --hostname-override=${NODE_NAME} \\
  --container-runtime-endpoint=unix:///run/containerd/containerd.sock \\
  --cgroup-driver=systemd \\
  --fail-swap-on=false \\
  --cluster-dns=${CLUSTER_DNS} \\
  --cluster-domain=${CLUSTER_DOMAIN} \\
  --register-with-taints=${REGISTER_TAINTS} \\
  --node-labels=yscale.sh/burst-node=true${CILIUM_MODE_LABEL}${EXTRA_NODE_LABELS:+,${EXTRA_NODE_LABELS}}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNITEOF

# Block the customer workload pod from reaching the cloud metadata service
# (169.254.169.254): it serves this burst's cloud-init user-data — which
# carries TS_AUTHKEY (a reusable tailnet key) and the kubelet bootstrap
# token — to anything on the VM that can reach it, including a pod curl'ing
# it over its default route (IMDSv2 raises the bar but a pod can still PUT
# for a token). Host traffic (OUTPUT chain) is untouched; only FORWARDed pod
# traffic from POD_CIDR is dropped. Installed before the kubelet starts so
# no pod can run ahead of the rule. Best-effort + idempotent — a firewall
# hiccup must not brick the burst. Full-tier Cilium (eBPF) may bypass
# iptables; a CiliumClusterwideNetworkPolicy is the belt-and-suspenders
# equivalent (TODO). Mirrors entrypoint-kubelet.sh.
if ! iptables -C FORWARD -s "${POD_CIDR}" -d 169.254.169.254/32 -j DROP 2>/dev/null; then
  iptables -A FORWARD -s "${POD_CIDR}" -d 169.254.169.254/32 -j DROP \
    || log_t "WARNING: could not install pod->metadata (169.254.169.254) egress block"
fi

# --- GPU telemetry reporter (systemd-managed, best-effort). Reports
# max utilization across all GPUs to the agent every 30s. Only when
# nvidia-smi is present. Does not block kubelet startup. ---
if command -v nvidia-smi >/dev/null 2>&1; then
  cat > /usr/local/bin/yscale-gpu-telemetry.sh <<'GPUSCRIPT'
#!/bin/bash
set -euo pipefail
ENDPOINT="$1"; BURST_ID="$2"; NODE_NAME="$3"
while true; do
  sleep 30
  max_util=0; found=0; product=""; product_mixed=0; product_json=""
  while IFS= read -r line; do
    v=$(echo "$line" | tr -d '[:space:]')
    case "${v}" in ''|*[!0-9]*) continue ;; esac
    [ "$v" -lt 0 ] || [ "$v" -gt 100 ] && continue
    found=1
    [ "$v" -gt "$max_util" ] && max_util=$v
  done <<EOF
$(nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits 2>/dev/null)
EOF
  [ "$found" = 0 ] && continue
  while IFS= read -r line; do
    line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    [ -z "$line" ] && continue
    if [ -z "$product" ]; then product="$line"
    elif [ "$product" != "$line" ]; then product_mixed=1; product=""; break
    fi
  done <<EOF
$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null)
EOF
  if [ "$product_mixed" = 0 ] && [ -n "$product" ]; then
    product_escaped=$(printf '%s' "$product" | sed 's/\\/\\\\/g;s/"/\\"/g')
    product_json=",\"product\":\"${product_escaped}\""
  fi
  curl -fsS -X POST -H 'Content-Type: application/json' \
    --max-time 5 \
    -d "{\"burst_id\":\"${BURST_ID}\",\"node_name\":\"${NODE_NAME}\",\"utilization\":${max_util}${product_json}}" \
    "${ENDPOINT}/gpu-telemetry" >/dev/null 2>&1 || true
done
GPUSCRIPT
  chmod +x /usr/local/bin/yscale-gpu-telemetry.sh
  cat > /etc/systemd/system/yscale-gpu-telemetry.service <<UNITEOF
[Unit]
Description=yscale GPU telemetry reporter
After=yscale-kubelet.service

[Service]
ExecStart=/usr/local/bin/yscale-gpu-telemetry.sh ${BOOTSTRAP_ENDPOINT} ${BURST_ID} ${NODE_NAME}
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
UNITEOF
  log_t "GPU telemetry reporter service installed"
fi

log_t "starting kubelet ${NODE_NAME}"
systemctl daemon-reload
systemctl enable --now yscale-kubelet.service
if [ -f /etc/systemd/system/yscale-gpu-telemetry.service ]; then
  systemctl enable --now yscale-gpu-telemetry.service
fi
log_t "join-only bootstrap done $(date -u)"
