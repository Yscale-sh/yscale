#!/usr/bin/env bash
#
# yscale Linode GPU image-prep script. Run ONCE on a stock Debian 12
# GPU instance to install the slow, static layer; the instance is then
# powered off and snapshotted into a Linode private Image that future
# GPU bursts boot from. Baking this layer turns a ~10 min cold install
# into a sub-minute join.
#
# What it bakes:
#   - base packages + containerd (CRI-configured, with the NVIDIA runtime)
#   - kubelet / kubectl / CNI plugins
#   - tailscale (installed, NOT `up` — that is per-burst)
#   - the NVIDIA driver (DKMS) + nvidia-container-toolkit
#   - cilium binaries + cilium-cni + eBPF source assets (host-mode Cilium,
#     no DS image pull at boot — see Fly burst/image/Dockerfile.kubelet.base)
#
# What it does NOT do: anything per-burst (tailscale up, kubeconfig
# fetch, CNI conflist, kubelet start). That stays in bootstrap.sh.
#
# After running, snapshotting, and updating gpuBurstImage in linode.go
# to the new image ID, BURST_TIER=full on Linode joins the customer's
# Cilium via the host-mode binaries — same shape as Fly, no DS pull.
#
# Usage: copy to the instance and `sudo bash gpu-setup.sh`, then verify
# `nvidia-smi`, power off, and create the Image from the disk.
set -euxo pipefail
exec > /var/log/yscale-gpu-setup.log 2>&1
echo "[yscale-gpu] image prep starting $(date -u)"

ARCH=amd64
CNI_VERSION=v1.5.1
KUBE_VERSION=v1.31.5
# Pin matches deploy/manifests/cilium-values.yaml AND the
# burst/image/Dockerfile.kubelet.base extraction stage on Fly. Bump all
# three together; otherwise burst-side cilium-agent identity protocol
# won't match the cluster-side Cilium operator.
CILIUM_VERSION=v1.19.4
# Burst-local CoreDNS — same pin as Fly's Dockerfile.kubelet.base.
COREDNS_VERSION=1.11.3
export DEBIAN_FRONTEND=noninteractive

# --- base packages + containerd ---
apt-get update
apt-get install -y --no-install-recommends \
  curl jq ca-certificates conntrack socat ethtool iptables containerd \
  gnupg dkms build-essential "linux-headers-$(uname -r)" \
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

# --- NVIDIA driver via NVIDIA's CUDA repo (Debian's own driver lags too
# far behind for RTX 4000 Ada / RTX 6000). cuda-drivers is the driver
# metapackage only — workloads bring their own CUDA in the container. ---
curl -fsSLo /tmp/cuda-keyring.deb \
  https://developer.download.nvidia.com/compute/cuda/repos/debian12/x86_64/cuda-keyring_1.1-1_all.deb
dpkg -i /tmp/cuda-keyring.deb
apt-get update
apt-get install -y cuda-drivers

# --- nvidia-container-toolkit (lets containerd run GPU containers) ---
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
  | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
  | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
  > /etc/apt/sources.list.d/nvidia-container-toolkit.list
apt-get update
apt-get install -y nvidia-container-toolkit

# --- containerd CRI config WITH the NVIDIA runtime as default. Static —
# no per-burst values — so it is baked into the image. bootstrap.sh on a
# baked image can leave this as-is. ---
mkdir -p /etc/containerd
cat > /etc/containerd/config.toml <<'CTDEOF'
version = 2
[plugins."io.containerd.grpc.v1.cri"]
  sandbox_image = "registry.k8s.io/pause:3.10"
[plugins."io.containerd.grpc.v1.cri".containerd]
  snapshotter = "overlayfs"
  default_runtime_name = "nvidia"
[plugins."io.containerd.grpc.v1.cri".cni]
  bin_dir = "/opt/cni/bin"
  conf_dir = "/etc/cni/net.d"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
  SystemdCgroup = true
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.nvidia]
  runtime_type = "io.containerd.runc.v2"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.nvidia.options]
  SystemdCgroup = true
  BinaryName = "/usr/bin/nvidia-container-runtime"
CTDEOF
systemctl restart containerd

# --- Cilium binaries + eBPF assets (host-mode Cilium, no DS image pull
# at boot). Mirrors burst/image/Dockerfile.kubelet.base on Fly: extract
# from quay.io/cilium/cilium:${CILIUM_VERSION}. Uses `ctr` (the
# containerd CLI shipped alongside containerd) to pull the image and
# mount its rootfs so we can cp out the binaries — no docker needed.
# Eats ~250MB of image disk but saves ~3-5 min on every BURST_TIER=full
# boot (no in-burst image pull, no eBPF compile-from-scratch on first
# pod). Required apt deps: clang llvm libelf libcap2-bin (eBPF runtime). ---
apt-get install -y --no-install-recommends clang llvm libelf1 libcap2-bin

# Wait for containerd's socket to be available before `ctr` calls it.
# This is normally instant after `systemctl restart` returns, but the
# socket lifecycle is async — and `ctr` returns a confusing "no such
# file" error if we race it. Short bounded wait.
for _ in $(seq 1 10); do
  [ -S /run/containerd/containerd.sock ] && break
  sleep 1
done

ctr -n k8s.io images pull "quay.io/cilium/cilium:${CILIUM_VERSION}"
CILIUM_MNT="$(mktemp -d)"
ctr -n k8s.io images mount "quay.io/cilium/cilium:${CILIUM_VERSION}" "${CILIUM_MNT}"
# Trap cleanup so a failure here doesn't leave a stale ctr mount.
trap 'ctr -n k8s.io images unmount "${CILIUM_MNT}" 2>/dev/null || true; rm -rf "${CILIUM_MNT}"' EXIT

# Binary set must match the COPY block in burst/image/Dockerfile.kubelet.base.
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium"            /usr/local/bin/cilium
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium-agent"      /usr/local/bin/cilium-agent
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium-mount"      /usr/local/bin/cilium-mount
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium-sysctlfix"  /usr/local/bin/cilium-sysctlfix
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium-dbg"        /usr/local/bin/cilium-dbg
install -m 0755 "${CILIUM_MNT}/usr/bin/cilium-health"     /usr/local/bin/cilium-health
mkdir -p /opt/cni/bin
install -m 0755 "${CILIUM_MNT}/opt/cni/bin/cilium-cni"    /opt/cni/bin/cilium-cni
# eBPF source assets — cilium-agent compiles these at startup. The host
# needs them on-disk; they aren't in the binary.
mkdir -p /var/lib/cilium/bpf
cp -a "${CILIUM_MNT}/var/lib/cilium/bpf/." /var/lib/cilium/bpf/

ctr -n k8s.io images unmount "${CILIUM_MNT}"
rm -rf "${CILIUM_MNT}"
trap - EXIT
# Drop the cached image — the binaries are on the rootfs now, the image
# itself is dead weight in the snapshot. ~250MB saved.
ctr -n k8s.io images rm "quay.io/cilium/cilium:${CILIUM_VERSION}" || true

# --- CoreDNS binary (burst-local resolver — see entrypoint-kubelet.sh
# DNS routing comment for the architectural rationale). Same release
# pin as the Fly bake. ---
curl -fsSL "https://github.com/coredns/coredns/releases/download/v${COREDNS_VERSION}/coredns_${COREDNS_VERSION}_linux_amd64.tgz" \
  | tar xz -C /usr/local/bin coredns
chmod +x /usr/local/bin/coredns

# --- SSH hardening (key-only). The burst gets an authorized_keys entry
# (LINODE_SSH_KEY) for debug; passwords are never used. With the Cloud Firewall
# (default-DROP inbound, see linode.go ensureBurstFirewall) this is the hardened
# posture: no password auth, no root password login. ---
mkdir -p /etc/ssh/sshd_config.d
cat > /etc/ssh/sshd_config.d/10-yscale-hardening.conf <<'SSHEOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin prohibit-password
SSHEOF

echo "[yscale-gpu] image prep done $(date -u) — reboot, verify 'nvidia-smi', 'cilium-agent --version', 'coredns --version', power off, snapshot"
