#!/bin/bash
# install.sh — bake-time script that runs on a Debian 12 GCE builder VM
# (launched by build-image.sh) and pre-installs everything a yscale
# burst needs at boot.
#
# gcp/bootstrap.sh runs ~250 LoC of apt-get + curl on every cold boot;
# this script does it ONCE at image-build time and ships the result as
# a GCE image. A burst booting from this image then runs only the thin
# pkg/backends/gcp/bootstrap-baked.sh — join-only, ~30-60 s to
# Node-Ready instead of ~230 s.
#
# Ported from pkg/backends/aws/ami/install.sh — see that file for the
# canonical version of this recipe; keep the two in lockstep.
#
# Sized for a small image: the debian-cloud project's Debian 12 base
# (~300-400 MB) plus our binaries + BPF objects, with docs/locales/
# apt-cache stripped. Target final image size: < 800 MB.

set -euo pipefail
exec > /var/log/yscale-image-install.log 2>&1
echo "[image-install] starting $(date -u)"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends \
    ca-certificates curl jq iptables iproute2 conntrack socat \
    fuse3 fuse-overlayfs \
    clang llvm libelf-dev libcap2-bin

# Pinned versions match burst/image/Dockerfile.kubelet.base and
# pkg/backends/aws/ami/install.sh. Keep tailscale + kubelet + containerd
# + runc + cilium + cni-plugins in lockstep with the Fly burst image and
# the AWS baked image so we have one mental model across providers.
ARCH=amd64
TAILSCALE_VERSION=1.80.3
KUBE_VERSION=v1.31.5
CNI_VERSION=v1.6.2
CONTAINERD_VERSION=1.7.27
RUNC_VERSION=v1.2.6
CILIUM_VERSION=v1.19.4

# Tailscale.
curl -fsSL "https://pkgs.tailscale.com/stable/tailscale_${TAILSCALE_VERSION}_${ARCH}.tgz" \
    | tar xzf - --strip-components=1 -C /usr/local/bin \
        "tailscale_${TAILSCALE_VERSION}_${ARCH}/tailscale" \
        "tailscale_${TAILSCALE_VERSION}_${ARCH}/tailscaled"

# kubelet.
curl -fsSL "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kubelet" \
    -o /usr/local/bin/kubelet
chmod +x /usr/local/bin/kubelet

# CNI plugins.
mkdir -p /opt/cni/bin
curl -fsSL "https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz" \
    | tar xzf - -C /opt/cni/bin

# containerd.
curl -fsSL "https://github.com/containerd/containerd/releases/download/v${CONTAINERD_VERSION}/containerd-${CONTAINERD_VERSION}-linux-${ARCH}.tar.gz" \
    | tar xzf - -C /usr/local

# runc.
curl -fsSL "https://github.com/opencontainers/runc/releases/download/${RUNC_VERSION}/runc.${ARCH}" \
    -o /usr/local/bin/runc
chmod +x /usr/local/bin/runc

# Cilium binaries + bpf source tree extracted from the official
# image. We don't run the DS — host-mode cilium-agent on the burst
# uses these directly.
mkdir -p /var/lib/cilium
docker_image="quay.io/cilium/cilium:${CILIUM_VERSION}"
tmpdir=$(mktemp -d)
# Pull and extract via the same skopeo+tar trick we use in the Fly
# burst-base build. Skopeo isn't in apt-get's minimal install list so
# fetch it explicitly.
apt-get install -y --no-install-recommends skopeo
skopeo copy --override-arch=$ARCH --override-os=linux \
    docker://${docker_image} dir:${tmpdir}
# Extract each blob (some are gzip-compressed tar layers).
for blob in ${tmpdir}/*; do
    case $(file -b --mime-type "$blob") in
        application/gzip|application/x-gzip)
            tar -tzf "$blob" 2>/dev/null | grep -qE "^usr/bin/cilium|^var/lib/cilium/bpf" && \
                tar -xzf "$blob" -C /tmp_cilium_extract --keep-directory-symlink 2>/dev/null || true
            mkdir -p /tmp_cilium_extract
            tar -xzf "$blob" -C /tmp_cilium_extract --keep-directory-symlink \
                usr/bin/cilium usr/bin/cilium-agent usr/bin/cilium-mount \
                usr/bin/cilium-sysctlfix usr/bin/cilium-dbg usr/bin/cilium-health \
                opt/cni/bin/cilium-cni var/lib/cilium/bpf 2>/dev/null || true
            ;;
    esac
done
[ -d /tmp_cilium_extract ] && {
    cp -a /tmp_cilium_extract/usr/bin/cilium*       /usr/local/bin/   2>/dev/null || true
    cp -a /tmp_cilium_extract/opt/cni/bin/cilium-cni /opt/cni/bin/    2>/dev/null || true
    cp -a /tmp_cilium_extract/var/lib/cilium/bpf    /var/lib/cilium/   2>/dev/null || true
    rm -rf /tmp_cilium_extract
}
rm -rf "$tmpdir"
chmod +x /usr/local/bin/cilium* 2>/dev/null || true

# BPF precompile (matches the burst image Dockerfile). Saves ~30-50 s
# of clang invocations on every burst boot.
if [ -d /var/lib/cilium/bpf ]; then
    cd /var/lib/cilium/bpf
    mkdir -p /var/lib/cilium/bpf/.precompiled include/lib
    cat > include/lib/node_config.h.precompile <<'NODECFG'
#ifndef __NODE_CONFIG_H_
#define __NODE_CONFIG_H_
#define IPV4_GATEWAY 0
#define IPV4_LOOPBACK 0
#define IPV4_MASK 0
#define ROUTER_IP {}
#define HOST_IP {}
#define HOST_ID 1
#define WORLD_ID 2
#define LXC_ID 0
#define SECCTX_FROM_IPCACHE 1
#define HAVE_LARGE_INSN_LIMIT 1
#endif
NODECFG
    for src in bpf_alignchecker.c bpf_lxc.c bpf_host.c bpf_overlay.c bpf_sock.c bpf_xdp.c bpf_network.c; do
        [ -f "$src" ] || continue
        out="/var/lib/cilium/bpf/.precompiled/${src%.c}.o"
        clang -O2 -emit-llvm -g -target bpf -std=gnu99 -nostdinc \
            -Wno-address-of-packed-member -Wno-unknown-warning-option \
            -Wno-gnu-variable-sized-type-not-at-end \
            -I/var/lib/cilium/bpf \
            -I/var/lib/cilium/bpf/include \
            -I/var/lib/cilium/bpf/include/bpf \
            -isystem /usr/include/x86_64-linux-gnu \
            -include include/lib/node_config.h.precompile \
            -DSKIP_DEBUG -DENABLE_IPV4 -DENABLE_IPV6=0 \
            -c "$src" -o "$out" 2>/dev/null || echo "  skip $src"
    done
    cd /
fi

# Persistent dirs the bootstrap-baked.sh expects.
mkdir -p /var/lib/tailscale /var/run/tailscale \
         /var/lib/kubelet /var/lib/containerd \
         /etc/kubernetes/manifests /etc/cni/net.d

# Strip.
echo "[image-install] stripping for slimmer image"
apt-get autoremove -y --purge skopeo
apt-get clean
rm -rf /var/lib/apt/lists/*
rm -rf /var/cache/apt/archives/*
# Docs + man pages — saved ~50 MB.
rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/* /usr/share/lintian/* /usr/share/groff/*
# Locales except en_US — saved ~100 MB. Keep C/POSIX too.
find /usr/share/locale -mindepth 1 -maxdepth 1 -type d \
    ! -name 'en' ! -name 'en_US' ! -name 'C' ! -name 'POSIX' \
    -exec rm -rf {} +
# initramfs hooks for hardware we won't have. The debian-cloud kernel
# carries virtio-only modules; trim everything else.
update-initramfs -u 2>/dev/null || true
# cloud-init/guest-agent logs from the bake itself.
rm -rf /var/log/cloud-init*.log /var/log/yscale-image-install.log.[12]*.gz

# Final sanity.
echo "[image-install] final disk usage:"
du -sh /usr/local/bin /opt/cni /var/lib/cilium 2>/dev/null || true
df -h / 2>&1 | head -2

# Log the instance name for a human tailing the serial console — GCE's
# metadata server needs only the Metadata-Flavor header (no IMDSv2-style
# token dance like AWS), per gcp/bootstrap.sh's providerID comment.
INSTANCE_NAME=$(curl -sf -m 3 -H "Metadata-Flavor: Google" \
    http://metadata.google.internal/computeMetadata/v1/instance/name 2>/dev/null || true)
echo "[image-install] done $(date -u); instance=$INSTANCE_NAME"
touch /var/lib/yscale-image-install-done

# Power off so build-image.sh can image a clean filesystem. A
# guest-initiated shutdown leaves the GCE instance TERMINATED rather
# than triggering a host-level restart.
shutdown -h now
