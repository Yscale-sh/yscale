#!/bin/bash
#
# yscale AWS burst bootstrap. Run by cloud-init (EC2 user-data) on first
# boot from a stock Debian 12 AMI: installs containerd + kubelet +
# tailscale, then joins the customer's cluster over the yscale tailnet
# as a burst node.
#
# Per-burst values arrive as shell exports CreateNode prepends ahead of
# this body. v0 installs everything at boot (~3-5 min cold); the fast
# path bakes a per-region AMI so this only does the join.
set -euo pipefail
# 'x' (xtrace) intentionally OMITTED: with xtrace on, the
# `tailscale up --authkey=...` line below would be written verbatim
# into bootstrap.log — a reusable tailnet credential persisted on
# disk for the burst's lifetime. Use `bash -x` interactively to debug.
exec > /var/log/yscale-bootstrap.log 2>&1
echo "[yscale] bootstrap starting $(date -u)"

# Required exports (stamped by linode.CreateNode ahead of this body).
# Fail loud + early — an empty POD_CIDR would later expand into an
# invalid bridge CNI conflist (`"subnet": ""`), kubelet would stall at
# NetworkNotReady forever, and the only signal would be a "node never
# registered" mystery in central. Same for the others.
: "${TS_AUTHKEY:?TS_AUTHKEY required}"
: "${TS_HOSTNAME:?TS_HOSTNAME required}"
: "${NODE_NAME:?NODE_NAME required}"
: "${BURST_ID:?BURST_ID required}"
: "${POD_CIDR:?POD_CIDR required (e.g. 10.42.7.0/24)}"
: "${BOOTSTRAP_ENDPOINT:?BOOTSTRAP_ENDPOINT required}"
# Customer-cluster service DNS. Hardcoded default matches k3s; central
# can override per-customer via CLUSTER_DNS / CLUSTER_DOMAIN env stamps
# once linode.CreateNode plumbs them through (currently TODO — see
# docs/JOINED-CNI.md). EKS uses 172.20.0.10, GKE uses 10.0.0.10,
# kubeadm default is 10.96.0.10.
: "${CLUSTER_DNS:=10.43.0.10}"
: "${CLUSTER_DOMAIN:=cluster.local}"

# KUBE_VERSION is not in the agent env; default it. TS_TAGS and
# EXTRA_NODE_LABELS are exported (maybe empty) by CreateNode.
# TS_LOGIN_SERVER points 'tailscale up' at a self-hosted coordination server;
# empty = Tailscale SaaS (no flag).
: "${TS_TAGS:=}"
: "${TS_LOGIN_SERVER:=}"
: "${KUBE_VERSION:=v1.31.5}"
: "${EXTRA_NODE_LABELS:=}"
# GPU bursts boot the AWS Deep Learning Base AMI (driver + CUDA + nvidia
# container toolkit pre-installed) and set BURST_GPU=1 so this bootstrap
# points containerd at the nvidia runtime. CPU bursts leave it 0.
: "${BURST_GPU:=0}"
: "${GPU_KIND:=}"

# Mirrors bootstrap-baked.sh — see the comment there for the tier
# contract. full is the only supported value (and the default when
# BURST_TIER is unset); the removed lite tier is refused before any
# provider-adjacent work (kubelet unit, taints, iptables) runs. Central
# always emits BURST_TIER=full (pkg/backends/internal/agentenv), so an
# explicit "lite" or any other non-empty non-"full" here means either
# an out-of-date central or a tampered user-data and must exit clean.
: "${BURST_TIER:=full}"
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
echo "[yscale] BURST_TIER=${BURST_TIER}"

# EC2 Linux AMIs don't provision swap by default, but be defensive —
# kubelet refuses to run with swap on, and a future custom AMI could
# add it.
swapoff -a || true

ARCH=amd64
CNI_VERSION=v1.5.1
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends \
  curl jq ca-certificates conntrack socat ethtool iptables
# containerd: stock Debian (CPU bursts) has none, so install it. The GPU
# DLAMI may already ship containerd.io (pulled in by Docker); apt-installing
# the distro `containerd` package on top of it conflicts and aborts the
# bootstrap, so only install when containerd is genuinely absent.
if ! command -v containerd >/dev/null 2>&1; then
  apt-get install -y --no-install-recommends containerd
fi

# --- Tailscale (kernel mode - the Linode VM is dedicated to this burst,
# so there is no host-tailscaled conflict like in the homelab agent) ---
curl -fsSL https://tailscale.com/install.sh | sh
systemctl enable --now tailscaled
# Write TS_AUTHKEY to a 600 file and feed via --auth-key=file: so the
# key never appears in `ps aux` (visible via /proc/<pid>/cmdline) and,
# combined with `set +x` above, never appears in bootstrap.log either.
# Shred and unset after `tailscale up` consumes it.
install -m 600 /dev/null /run/ts-authkey
printf '%s' "${TS_AUTHKEY}" > /run/ts-authkey
TS_UP_ARGS="--auth-key=file:/run/ts-authkey --hostname=${TS_HOSTNAME} --accept-routes"
if [ -n "${TS_TAGS}" ]; then
  TS_UP_ARGS="${TS_UP_ARGS} --advertise-tags=${TS_TAGS}"
fi
# Advertise this burst's pod CIDR so the customer's cluster nodes
# (which also run tailscale + accept-routes via the agent's subnet
# router DaemonSet) can route pod-to-pod into this burst. Joined-CNI
# primitive — matches burst/image/entrypoint-kubelet.sh on Fly. The
# Tailscale ACL must autoApprove these advertisements (autoApprovers
# .routes scoped to the burst tag); otherwise the route stays Pending.
if [ -n "${POD_CIDR:-}" ]; then
  TS_UP_ARGS="${TS_UP_ARGS} --advertise-routes=${POD_CIDR}"
fi
# Point at a self-hosted coordination server when set; empty
# (the default) means Tailscale SaaS and no --login-server flag.
if [ -n "${TS_LOGIN_SERVER:-}" ]; then
  TS_UP_ARGS="${TS_UP_ARGS} --login-server=${TS_LOGIN_SERVER}"
fi
# shellcheck disable=SC2086 # word-splitting is intentional: TS_UP_ARGS is built as a space-separated arg list
tailscale up ${TS_UP_ARGS}
shred -u /run/ts-authkey 2>/dev/null || rm -f /run/ts-authkey
unset TS_AUTHKEY
TS_IP="$(tailscale ip -4)"
echo "[yscale] tailscale up: ${TS_IP}"

# --- kube binaries + CNI plugins ---
for bin in kubelet kubectl; do
  curl -fsSLo "/usr/local/bin/${bin}" \
    "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/${bin}"
  chmod +x "/usr/local/bin/${bin}"
done
mkdir -p /opt/cni/bin
curl -fsSL "https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz" \
  | tar -C /opt/cni/bin -xz

# --- fetch the kubelet bootstrap kubeconfig from the agent over the
# tailnet (MagicDNS resolves the agent hostname; --retry rides out the
# agent not being instantly reachable) ---
mkdir -p /etc/kubernetes
BOOT_REQ="$(printf '{"burst_id":"%s","node_name":"%s"}' "${BURST_ID}" "${NODE_NAME}")"
BOOT_RESP="$(curl -fsS -X POST -H 'Content-Type: application/json' \
  --retry 30 --retry-delay 3 --retry-connrefused --retry-all-errors --max-time 120 \
  -d "${BOOT_REQ}" "${BOOTSTRAP_ENDPOINT}/bootstrap-kubeconfig")"
[ -n "${BOOT_RESP}" ] || { echo "[yscale] bootstrap response empty"; exit 1; }
echo "${BOOT_RESP}" | jq -er '.kubeconfig' | base64 -d > /etc/kubernetes/bootstrap-kubeconfig.yaml

# GKE's validate-node-providerid policy requires the provider ID to end in
# /<node-name>. Keep an AWS identity while matching the authoritative Node
# name; other customer clusters retain the existing synthetic EC2 ID.
CLUSTER_CLOUD="$(echo "${BOOT_RESP}" | jq -r '.cloud_provider // empty' 2>/dev/null || true)"
if [ "${CLUSTER_CLOUD}" = "gcp" ]; then
  PROVIDER_ID="aws://${NODE_NAME}"
  echo "[bootstrap] GKE cluster: providerID=${PROVIDER_ID} (admission requires /<node-name> suffix)"
else
  : "${PROVIDER_ID:=aws://yscale-burst-${BURST_ID#burst_}}"
fi

# Gateway routing: see burst/image/entrypoint-kubelet.sh for full
# rationale. Gateway advertises customer pod CIDRs via Tailscale; our
# --accept-routes installs them automatically.
:

# --- Bridge CNI conflist as a BOOTSTRAP FALLBACK. Named with the `10-`
# prefix so when Cilium's DaemonSet pod (cluster-wide, pinned to burst
# nodes via nodeSelector yscale.sh/burst-node=true) writes its own
# `05-cilium.conflist`, the lex-smaller filename takes precedence in
# kubelet's CNI scanner. The bridge conf only ever serves as the
# initial floor so kubelet has a CNI plugin from boot and can register
# the Node — it shouldn't carry real pod traffic once Cilium is up.
# Matches the burst/image/entrypoint-kubelet.sh pattern on Fly. ---
mkdir -p /etc/cni/net.d
cat > /etc/cni/net.d/10-yscale-bootstrap.conflist <<CNIEOF
{
  "cniVersion": "1.0.0",
  "name": "yscale-bootstrap",
  "plugins": [
    {
      "type": "bridge", "bridge": "cni0", "isGateway": true,
      "ipMasq": true, "hairpinMode": true,
      "ipam": {
        "type": "host-local",
        "ranges": [[{"subnet": "${POD_CIDR}"}]],
        "routes": [{"dst": "0.0.0.0/0"}]
      }
    },
    {"type": "portmap", "capabilities": {"portMappings": true}}
  ]
}
CNIEOF

# --- Cilium prerequisites: BPF + cgroup2 + shared root mount.
# Linode's stock Debian usually has both /sys/fs/bpf and the unified
# cgroup hierarchy mounted by systemd, so these are best-effort —
# they no-op on the second mount attempt. Matches Fly's pattern. ---
mount -t bpf bpf /sys/fs/bpf 2>/dev/null || true
mkdir -p /run/cilium/cgroupv2
mount -t cgroup2 none /run/cilium/cgroupv2 2>/dev/null || true
mount --make-rshared / 2>/dev/null || true

# --- GPU runtime prep (BURST_GPU=1). The DLAMI ships the NVIDIA driver +
# container toolkit, but containerd won't expose the GPU to pods until it's
# pointed at /usr/bin/nvidia-container-runtime and that runtime is made the
# default. Verify the driver is actually usable first — a GPU burst on a
# driverless AMI must fail loudly here, not register a node whose GPU pods
# stall in init. The runtime stanzas are templated into the containerd
# config below; they stay empty (plain runc) for CPU bursts. ---
GPU_DEFAULT_RUNTIME=""
GPU_RUNTIME_BLOCK=""
if [ "${BURST_GPU:-0}" = "1" ]; then
  echo "[yscale] GPU burst (kind=${GPU_KIND:-unknown}): configuring nvidia containerd runtime"
  if ! command -v nvidia-smi >/dev/null 2>&1 || ! nvidia-smi >/dev/null 2>&1; then
    echo "[yscale] FATAL: BURST_GPU=1 but the NVIDIA driver is unusable (nvidia-smi failed) — wrong AMI?" >&2
    exit 1
  fi
  nvidia-smi --query-gpu=name,driver_version --format=csv,noheader || true
  # Resolve the runtime binary from PATH rather than hard-coding /usr/bin —
  # a future DLAMI (or the GPU Operator layout) may place it elsewhere, and
  # a wrong path would silently fall back to runc (no GPU) with the node
  # still Ready. Fail loudly if the toolkit isn't present at all.
  NV_RUNTIME_BIN="$(command -v nvidia-container-runtime || true)"
  if [ -z "${NV_RUNTIME_BIN}" ]; then
    echo "[yscale] FATAL: BURST_GPU=1 but nvidia-container-runtime is not on PATH — NVIDIA container toolkit missing from the AMI" >&2
    exit 1
  fi
  GPU_DEFAULT_RUNTIME='  default_runtime_name = "nvidia"'
  GPU_RUNTIME_BLOCK=$(cat <<NVEOF
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.nvidia]
  runtime_type = "io.containerd.runc.v2"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.nvidia.options]
  BinaryName = "${NV_RUNTIME_BIN}"
  SystemdCgroup = true
NVEOF
)
fi

# --- containerd CRI config. GPU_DEFAULT_RUNTIME / GPU_RUNTIME_BLOCK expand
# to the nvidia runtime wiring on GPU bursts and to nothing (empty lines,
# harmless in TOML) on CPU bursts. ---
mkdir -p /etc/containerd
cat > /etc/containerd/config.toml <<CTDEOF
version = 2
[plugins."io.containerd.grpc.v1.cri"]
  sandbox_image = "registry.k8s.io/pause:3.10"
[plugins."io.containerd.grpc.v1.cri".containerd]
  snapshotter = "overlayfs"
${GPU_DEFAULT_RUNTIME}
[plugins."io.containerd.grpc.v1.cri".cni]
  bin_dir = "/opt/cni/bin"
  conf_dir = "/etc/cni/net.d"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
  SystemdCgroup = true
${GPU_RUNTIME_BLOCK}
CTDEOF
systemctl restart containerd

# --- Burst-local CoreDNS (see burst/image/entrypoint-kubelet.sh "DNS
# routing on the burst" comment). Stock-Debian path doesn't have the
# binary baked in, so curl it from the GitHub release. Pin matches the
# Fly Dockerfile + Linode gpu-setup.sh. ---
COREDNS_VERSION=1.11.3
if [ ! -x /usr/local/bin/coredns ]; then
  curl -fsSL "https://github.com/coredns/coredns/releases/download/v${COREDNS_VERSION}/coredns_${COREDNS_VERSION}_linux_amd64.tgz" \
    | tar xz -C /usr/local/bin coredns
  chmod +x /usr/local/bin/coredns
fi
POD_CIDR_GW=$(echo "${POD_CIDR}" | awk -F'[./]' '{print $1"."$2"."$3".1"}')
mkdir -p /etc/coredns
cat > /etc/coredns/Corefile <<COREDNSEOF
. {
    bind ${POD_CIDR_GW} 127.0.0.1
    forward . 1.1.1.1 100.100.100.100 8.8.8.8 {
        policy sequential
        health_check 5s
    }
    cache 30
    loop
    reload
    errors
}
COREDNSEOF
cat > /etc/systemd/system/yscale-coredns.service <<UNITEOF
[Unit]
Description=yscale burst-local CoreDNS
After=tailscaled.service network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/coredns -conf /etc/coredns/Corefile
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
UNITEOF
systemctl daemon-reload
systemctl enable --now yscale-coredns.service
echo "[yscale] burst-local CoreDNS up (bind ${POD_CIDR_GW}:53, 127.0.0.1:53)"
CLUSTER_DNS="${POD_CIDR_GW}"

# Kubelet taints/label — full is the only supported tier here (guarded
# above), so unconditionally register with the cilium-not-ready taint;
# the in-cluster Cilium DaemonSet is expected to schedule onto this
# burst and lift the taint. See bootstrap-baked.sh for full rationale.
REGISTER_TAINTS="yscale.sh/burst-node=true:NoSchedule,node.cilium.io/agent-not-ready=true:NoSchedule"
CILIUM_MODE_LABEL=""

# --- GPU device-plugin readiness gate (issue #39). The NVIDIA device plugin
# advertises nvidia.com/gpu to the kubelet AFTER the node is already Ready, so
# a GPU Job scheduled the instant the node appears hits FailedScheduling
# (Insufficient nvidia.com/gpu) and exhausts its backoffLimit before the plugin
# registers (~90s race). Mirror the Cilium agent-not-ready gate above: register
# a NoSchedule taint here via --register-with-taints and let the yscale
# controller clear it once nvidia.com/gpu is allocatable. The taint is removed
# CONTROLLER-side (own API credentials) — never by the kubelet: NodeRestriction
# admission forbids a kubelet from mutating its own Node's taints, so a
# kubelet-side removal would retry forever and leave GPU pods Pending. GPU Jobs
# deliberately omit this taint from their tolerations, so they stay Pending (no
# FailedScheduling, no backoffLimit burn) until the plugin is up.
if [ "${BURST_GPU:-0}" = "1" ]; then
  REGISTER_TAINTS="${REGISTER_TAINTS},nvidia.com/gpu-not-ready=true:NoSchedule"
fi

# --- NVIDIA device plugin (static pod), GPU bursts only. The nvidia
# containerd runtime configured above only grants a container GPU *access*;
# Kubernetes does not learn the node has a GPU — and so never schedules a
# pod that requests nvidia.com/gpu — until the device plugin advertises that
# resource to the kubelet. It's a pod, not a host package, so the kubelet
# runs it as a static pod from --pod-manifest-path (set on the kubelet unit
# below). `operator: Exists` tolerates the burst NoSchedule taint. Mirrors
# Linode's bootstrap-baked.sh. ---
mkdir -p /etc/kubernetes/manifests
if [ "${BURST_GPU:-0}" = "1" ]; then
  cat > /etc/kubernetes/manifests/nvidia-device-plugin.yaml <<DPEOF
apiVersion: v1
kind: Pod
metadata:
  name: nvidia-device-plugin
  namespace: kube-system
spec:
  priorityClassName: system-node-critical
  tolerations:
  - operator: Exists
  containers:
  - name: nvidia-device-plugin
    image: nvcr.io/nvidia/k8s-device-plugin:v0.17.1
    securityContext:
      privileged: true
    volumeMounts:
    - name: device-plugin
      mountPath: /var/lib/kubelet/device-plugins
  volumes:
  - name: device-plugin
    hostPath:
      path: /var/lib/kubelet/device-plugins
DPEOF
  echo "[yscale] GPU device-plugin static pod written"
fi

# --- kubelet systemd unit. --rotate-server-certificates so the kubelet
# files a serving CSR with the tailnet IP in its SAN (the agent's CSR
# approver auto-approves it) - without it, apiserver can't reach :10250.
# --pod-manifest-path runs the GPU device-plugin static pod above (empty
# dir = no-op for CPU bursts). ---
cat > /etc/systemd/system/yscale-kubelet.service <<UNITEOF
[Unit]
Description=yscale burst kubelet
After=containerd.service tailscaled.service
Requires=containerd.service

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
  --pod-manifest-path=/etc/kubernetes/manifests \\
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
    || echo "[yscale] WARNING: could not install pod->metadata (169.254.169.254) egress block"
fi

# --- GPU telemetry reporter (systemd-managed, best-effort). Reports
# max utilization across all GPUs to the agent every 30s. Only when
# nvidia-smi is present (BURST_GPU=1 path). Does not block kubelet startup. ---
if [ "${BURST_GPU:-0}" = "1" ] && command -v nvidia-smi >/dev/null 2>&1; then
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
  echo "[yscale] GPU telemetry reporter service installed"
fi

systemctl daemon-reload
systemctl enable --now yscale-kubelet.service
if [ -f /etc/systemd/system/yscale-gpu-telemetry.service ]; then
  systemctl enable --now yscale-gpu-telemetry.service
fi
echo "[yscale] bootstrap done $(date -u)"
