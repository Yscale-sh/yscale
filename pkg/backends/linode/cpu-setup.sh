#!/usr/bin/env bash
#
# yscale Linode CPU image-prep script. Run ONCE on a stock Debian 12
# instance to install the slow, static layer; the instance is then
# powered off and snapshotted into a Linode private Image that future
# CPU bursts boot from. Baking this turns a ~3-5 min cold install (and,
# for tier=full, a flaky in-cluster-DaemonSet Cilium dependency) into a
# sub-minute join with host-mode Cilium that lifts its own taint.
#
# This is gpu-setup.sh WITHOUT the NVIDIA layer, and with containerd's
# default runtime = runc (not nvidia). Everything else — kubelet/CNI,
# tailscale, host-mode Cilium binaries + eBPF assets, CoreDNS — is
# identical, so a CPU burst on this image takes the SAME join-only
# bootstrap-baked.sh path the GPU image already uses.
#
# What it bakes:
#   - base packages + containerd (CRI-configured, runc default runtime)
#   - kubelet / kubectl / CNI plugins
#   - tailscale (installed, NOT `up` — that is per-burst)
#   - cilium binaries + cilium-cni + eBPF source assets (host-mode Cilium,
#     no DS image pull at boot — see burst/image/Dockerfile.kubelet.base)
#   - CoreDNS (burst-local resolver)
#
# What it does NOT do: anything per-burst (tailscale up, kubeconfig fetch,
# CNI conflist, kubelet start, host-mode cilium-agent launch). That stays
# in bootstrap-baked.sh.
#
# After running, snapshotting, and updating cpuBurstImage in linode.go to
# the new image ID, BURST_TIER=full on Linode runs host-mode Cilium (taint
# lifted locally) — same shape as Fly, no DaemonSet dependency.
#
# Usage: copy to the instance and `sudo bash cpu-setup.sh`, then verify
# `cilium-agent --version` + `coredns --version` + `kubelet --version`,
# power off, and create the Image from the disk.
set -euxo pipefail
exec > /var/log/yscale-cpu-setup.log 2>&1
echo "[yscale-cpu] image prep starting $(date -u)"

ARCH=amd64
CNI_VERSION=v1.5.1
KUBE_VERSION=v1.31.5
# Pin matches deploy/manifests/cilium-values.yaml AND the
# burst/image/Dockerfile.kubelet.base extraction stage on Fly AND
# gpu-setup.sh. Bump all together; otherwise the burst-side cilium-agent
# identity protocol won't match the cluster-side Cilium operator.
CILIUM_VERSION=v1.19.4
# Burst-local CoreDNS — same pin as Fly's Dockerfile.kubelet.base.
COREDNS_VERSION=1.11.3
export DEBIAN_FRONTEND=noninteractive

# --- base packages + containerd (no dkms/build-essential/headers: those
# were only for the NVIDIA DKMS module on the GPU image). ---
apt-get update
apt-get install -y --no-install-recommends \
  curl jq ca-certificates conntrack socat ethtool iptables containerd \
  nfs-common  # mount.nfs for pods mounting an NFS source volume (sharded transcode)

# --- kube binaries + CNI plugins (static — safe to bake) ---
for bin in kubelet kubectl; do
  curl -fsSLo "/usr/local/bin/${bin}" \
    "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/${bin}"
  chmod +x "/usr/local/bin/${bin}"
done
mkdir -p /opt/cni/bin
curl -fsSL "https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz" \
  | tar -C /opt/cni/bin -xz

# --- tailscale: install the package only; `tailscale up` is per-burst ---
curl -fsSL https://tailscale.com/install.sh | sh
systemctl disable tailscaled || true

# --- containerd CRI config with runc as the default runtime (CPU image:
# no NVIDIA runtime). Static — no per-burst values — so it is baked in. ---
mkdir -p /etc/containerd
cat > /etc/containerd/config.toml <<'CTDEOF'
version = 2
[plugins."io.containerd.grpc.v1.cri"]
  sandbox_image = "registry.k8s.io/pause:3.10"
[plugins."io.containerd.grpc.v1.cri".containerd]
  snapshotter = "overlayfs"
  default_runtime_name = "runc"
[plugins."io.containerd.grpc.v1.cri".cni]
  bin_dir = "/opt/cni/bin"
  conf_dir = "/etc/cni/net.d"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
  SystemdCgroup = true
CTDEOF
systemctl restart containerd

# --- Cilium binaries + eBPF assets (host-mode Cilium, no DS image pull at
# boot). Identical to gpu-setup.sh: extract from quay.io/cilium/cilium via
# `ctr` and cp the binaries + bpf/ sources onto the rootfs. Required apt
# deps for the eBPF runtime: clang llvm libelf libcap2-bin. ---
apt-get install -y --no-install-recommends clang llvm libelf1 libcap2-bin

# Wait for containerd's socket before `ctr` calls it (async after restart).
for _ in $(seq 1 10); do
  [ -S /run/containerd/containerd.sock ] && break
  sleep 1
done

ctr -n k8s.io images pull "quay.io/cilium/cilium:${CILIUM_VERSION}"
CILIUM_MNT="$(mktemp -d)"
ctr -n k8s.io images mount "quay.io/cilium/cilium:${CILIUM_VERSION}" "${CILIUM_MNT}"
trap 'ctr -n k8s.io images unmount "${CILIUM_MNT}" 2>/dev/null || true; rm -rf "${CILIUM_MNT}"' EXIT

# Binary set must match the COPY block in burst/image/Dockerfile.kubelet.base
# and gpu-setup.sh.
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium"            /usr/local/bin/cilium
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium-agent"      /usr/local/bin/cilium-agent
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium-mount"      /usr/local/bin/cilium-mount
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium-sysctlfix"  /usr/local/bin/cilium-sysctlfix
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium-dbg"        /usr/local/bin/cilium-dbg
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium-health"     /usr/local/bin/cilium-health
mkdir -p /opt/cni/bin
install -m 0755 "${CILIUM_MNT}/opt/cni/bin/cilium-cni"    /opt/cni/bin/cilium-cni
# eBPF source assets — cilium-agent compiles these at startup.
mkdir -p /var/lib/cilium/bpf
cp -a "${CILIUM_MNT}/var/lib/cilium/bpf/." /var/lib/cilium/bpf/

ctr -n k8s.io images unmount "${CILIUM_MNT}"
rm -rf "${CILIUM_MNT}"
trap - EXIT
# Drop the cached image — the binaries are on the rootfs now (~250MB saved).
ctr -n k8s.io images rm "quay.io/cilium/cilium:${CILIUM_VERSION}" || true

# --- CoreDNS binary (burst-local resolver). Same pin as the Fly bake. ---
curl -fsSL "https://github.com/coredns/coredns/releases/download/v${COREDNS_VERSION}/coredns_${COREDNS_VERSION}_linux_amd64.tgz" \
  | tar xz -C /usr/local/bin coredns
chmod +x /usr/local/bin/coredns

# --- SSH hardening (key-only). Same posture as gpu-setup.sh: no password auth,
# no root password login. Pairs with the Cloud Firewall (default-DROP inbound). ---
mkdir -p /etc/ssh/sshd_config.d
cat > /etc/ssh/sshd_config.d/10-yscale-hardening.conf <<'SSHEOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin prohibit-password
SSHEOF

echo "[yscale-cpu] image prep done $(date -u) — verify 'cilium-agent --version', 'coredns --version', 'kubelet --version', power off, snapshot into a private Image, set cpuBurstImage in linode.go"
