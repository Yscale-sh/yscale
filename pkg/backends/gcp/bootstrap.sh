#!/bin/bash
#
# yscale GCP burst bootstrap. Run by the Compute Engine guest agent (the
# `startup-script` instance metadata key) on first boot from a stock
# Debian 12 image: installs containerd + kubelet + tailscale, then
# joins the customer's cluster over the yscale tailnet as a burst node.
#
# Per-burst values arrive as shell exports CreateNode prepends ahead of
# this body. v0 installs everything at boot (~3-5 min cold). Unlike a
# Linode private Image, a future baked GCP image needs no per-zone
# replication (images are global) — a join-only variant of this script
# can land later with no extra machinery.
set -euo pipefail
# 'x' (xtrace) intentionally OMITTED: with xtrace on, the
# `tailscale up --authkey=...` line below would be written verbatim
# into bootstrap.log — a reusable tailnet credential persisted on
# disk for the burst's lifetime. Use `bash -x` interactively to debug.
exec > /var/log/yscale-bootstrap.log 2>&1
echo "[yscale] bootstrap starting $(date -u)"

# Required exports (stamped by gcp.CreateNode ahead of this body).
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
# once gcp.CreateNode plumbs them through (currently TODO — see
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

# Mirrors linode/bootstrap-baked.sh — see the comment there for the tier
# contract. full is the only supported value (and the default when
# BURST_TIER is unset); the removed lite tier is refused before any
# provider-adjacent work (kubelet unit, taints, iptables) runs. Central
# always emits BURST_TIER=full (pkg/backends/internal/agentenv), so an
# explicit "lite" or any other non-empty non-"full" here means either
# an out-of-date central or a tampered startup-script and must exit clean.
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

# GCE does not disable swap by default, but stock images generally ship
# without a swap partition already — swapoff defensively anyway, since
# kubelet refuses to run with swap on (gcp-plan.md §5).
swapoff -a || true

ARCH=amd64
CNI_VERSION=v1.5.1
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends \
  curl jq ca-certificates conntrack socat ethtool iptables containerd

# --- Tailscale (kernel mode - the GCE VM is dedicated to this burst,
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

# Gateway routing: see burst/image/entrypoint-kubelet.sh for full
# rationale. Gateway advertises customer pod CIDRs via Tailscale; our
# --accept-routes installs them automatically.
:

# --- CNI MTU: derive from the primary NIC instead of assuming 1500.
# GCP's default VPC MTU is 1460 (Linode's and AWS's underlays are both
# 1500, which is why no other backend has needed this) — a cni0 bridge
# created at the bridge plugin's implicit 1500 default is then WIDER
# than the NIC (ens4) it rides on, and packets that land in the gap
# blackhole silently instead of getting fragmented or rejected (verified
# live 2026-08-01; a small test payload happened to fit under 1460
# anyway, so it went unnoticed). Reading the actual NIC MTU rather than
# hardcoding 1460 also covers a custom VPC with jumbo frames enabled.
PRIMARY_IFACE=$(ip route show default 2>/dev/null | awk '/^default/ {for (i=1;i<=NF;i++) if ($i=="dev") {print $(i+1); exit}}')
CNI_MTU=$(cat "/sys/class/net/${PRIMARY_IFACE}/mtu" 2>/dev/null || true)
: "${CNI_MTU:=1500}"
echo "[yscale] primary iface=${PRIMARY_IFACE:-unknown} mtu=${CNI_MTU}"

# --- Bridge CNI conflist as a BOOTSTRAP FALLBACK. Named with the `10-`
# prefix so that when cilium-agent writes its own `05-cilium.conflist`,
# the lex-smaller filename takes precedence in kubelet's CNI scanner.
# The bridge conf only ever serves as the initial floor so kubelet has a
# CNI plugin from boot and can register the Node — it shouldn't carry
# real pod traffic once Cilium is up.
#
# On THIS path it is the only CNI there will ever be: cilium-agent runs
# host-mode from the baked rootfs (see bootstrap-baked.sh), and this
# script is the stock-debian-12 path with nothing baked. There is no
# in-cluster Cilium DaemonSet to arrive later — full tier requires
# YSCALE_GCP_IMAGE. A full-tier burst booted from here therefore keeps
# the node.cilium.io/agent-not-ready taint forever and never runs work.
# Matches the burst/image/entrypoint-kubelet.sh pattern on Fly. ---
mkdir -p /etc/cni/net.d
cat > /etc/cni/net.d/10-yscale-bootstrap.conflist <<CNIEOF
{
  "cniVersion": "1.0.0",
  "name": "yscale-bootstrap",
  "plugins": [
    {
      "type": "bridge", "bridge": "cni0", "isGateway": true, "mtu": ${CNI_MTU},
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
# GCP's stock Debian usually has both /sys/fs/bpf and the unified
# cgroup hierarchy mounted by systemd, so these are best-effort —
# they no-op on the second mount attempt. Matches Fly's pattern. ---
mount -t bpf bpf /sys/fs/bpf 2>/dev/null || true
mkdir -p /run/cilium/cgroupv2
mount -t cgroup2 none /run/cilium/cgroupv2 2>/dev/null || true
mount --make-rshared / 2>/dev/null || true

# --- containerd CRI config ---
mkdir -p /etc/containerd
cat > /etc/containerd/config.toml <<CTDEOF
version = 2
[plugins."io.containerd.grpc.v1.cri"]
  sandbox_image = "registry.k8s.io/pause:3.10"
[plugins."io.containerd.grpc.v1.cri".containerd]
  snapshotter = "overlayfs"
[plugins."io.containerd.grpc.v1.cri".cni]
  bin_dir = "/opt/cni/bin"
  conf_dir = "/etc/cni/net.d"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
  SystemdCgroup = true
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
# burst and lift the taint. See linode/bootstrap-baked.sh for full rationale.
REGISTER_TAINTS="yscale.sh/burst-node=true:NoSchedule,node.cilium.io/agent-not-ready=true:NoSchedule"
CILIUM_MODE_LABEL=""

# --- Resolve the kubelet providerID --------------------------------------
# A yscale burst is a REAL GCE instance, just not part of any GKE
# nodepool. On a GKE *customer* cluster the GCE CCM reaps any Node whose
# providerID doesn't resolve to a real instance it can query, so register
# as our ACTUAL project/zone/instance-name (gce://<project>/<zone>/<name>):
# its instanceExistsByProviderID then returns true and the Node is left
# alone. On non-GKE clusters (EKS/LKE/AKS/self-managed) the gce:// prefix
# is ignored by their CCMs, so this is harmless there. Unlike Linode's
# Metadata service (token PUT/GET dance) or AWS's IMDSv2, GCE's metadata
# server only needs the Metadata-Flavor header — no token to mint. Falls
# back to a synthetic id if the lookup fails (Node may then be reaped on
# GKE). An explicit PROVIDER_ID env always wins.
if [ -z "${PROVIDER_ID:-}" ]; then
  md_hdr="Metadata-Flavor: Google"
  gce_name="$(curl -sf -m 5 --retry 3 --retry-connrefused --retry-delay 2 -H "${md_hdr}" http://metadata.google.internal/computeMetadata/v1/instance/name 2>/dev/null || true)"
  gce_zone_path="$(curl -sf -m 5 --retry 3 --retry-connrefused --retry-delay 2 -H "${md_hdr}" http://metadata.google.internal/computeMetadata/v1/instance/zone 2>/dev/null || true)"
  gce_project="$(curl -sf -m 5 --retry 3 --retry-connrefused --retry-delay 2 -H "${md_hdr}" http://metadata.google.internal/computeMetadata/v1/project/project-id 2>/dev/null || true)"
  gce_zone="${gce_zone_path##*/}"
  if [ -n "${gce_name}" ] && [ -n "${gce_zone}" ] && [ -n "${gce_project}" ]; then
    PROVIDER_ID="gce://${gce_project}/${gce_zone}/${gce_name}"
    echo "[bootstrap] providerID=${PROVIDER_ID} (real GCE instance — survives GKE CCM)"
  else
    PROVIDER_ID="gce://yscale-burst-${BURST_ID#burst_}"
    echo "[bootstrap] WARN: GCE metadata lookup failed; using synthetic providerID ${PROVIDER_ID} (Node may be reaped on GKE customer clusters)"
  fi
fi

# --- kubelet systemd unit. --rotate-server-certificates so the kubelet
# files a serving CSR with the tailnet IP in its SAN (the agent's CSR
# approver auto-approves it) - without it, apiserver can't reach :10250. ---
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
# (169.254.169.254): it serves this burst's startup-script metadata — which
# carries TS_AUTHKEY (a reusable tailnet key) and the kubelet bootstrap
# token — to anything on the VM that can reach it, including a pod curl'ing
# it over its default route. Host traffic (OUTPUT chain) is untouched; only
# FORWARDed pod traffic from POD_CIDR is dropped. Installed before the
# kubelet starts so no pod can run ahead of the rule. Best-effort +
# idempotent — a firewall hiccup must not brick the burst. Full-tier Cilium
# (eBPF) may bypass iptables; a CiliumClusterwideNetworkPolicy is the
# belt-and-suspenders equivalent (TODO). Mirrors entrypoint-kubelet.sh.
if ! iptables -C FORWARD -s "${POD_CIDR}" -d 169.254.169.254/32 -j DROP 2>/dev/null; then
  iptables -A FORWARD -s "${POD_CIDR}" -d 169.254.169.254/32 -j DROP \
    || echo "[yscale] WARNING: could not install pod->metadata (169.254.169.254) egress block"
fi

systemctl daemon-reload
systemctl enable --now yscale-kubelet.service
echo "[yscale] bootstrap done $(date -u)"
