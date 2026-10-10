#!/bin/bash
set -euo pipefail

# Networking tier is a first-boundary invariant. Validate it before checking
# credentials, starting Tailscale, or writing any bootstrap material so a
# stale image invocation cannot partially initialize a removed tier.
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

# Required exports (stamped by the backend's CreateNode env injection).
# Fail loud + early — an empty POD_CIDR would later expand into an
# invalid bridge CNI conflist (`"subnet": ""`), kubelet would stall at
# NetworkNotReady forever. Same for the others.
: "${TS_AUTHKEY:?TS_AUTHKEY required}"
: "${TS_HOSTNAME:?TS_HOSTNAME required}"
: "${NODE_NAME:?NODE_NAME required}"
# Two join contracts share this image (see pkg/backends/internal/agentenv):
#   agent mode (central+agent): BOOTSTRAP_ENDPOINT serves the bootstrap
#     kubeconfig, BURST_ID authenticates the fetch, POD_CIDR is allocated
#     centrally.
#   standalone mode (cmd/yscale controller): there is NO agent. The
#     controller stamps API_SERVER + CLUSTER_CA + BOOTSTRAP_TOKEN; the
#     bootstrap kubeconfig is self-minted below, and the pod CIDR comes
#     from the cluster's node-CIDR allocator AFTER the kubelet registers
#     (watched by standalone_podcidr_watcher).
STANDALONE=0
if [ -n "${BOOTSTRAP_ENDPOINT:-}" ]; then
  : "${BURST_ID:?BURST_ID required}"
  : "${POD_CIDR:?POD_CIDR required (e.g. 10.42.7.0/24)}"
else
  : "${API_SERVER:?BOOTSTRAP_ENDPOINT (agent mode) or API_SERVER (standalone mode) required}"
  : "${CLUSTER_CA:?CLUSTER_CA required in standalone (no-agent) mode}"
  : "${BOOTSTRAP_TOKEN:?BOOTSTRAP_TOKEN required in standalone (no-agent) mode}"
  : "${BURST_ID:=${NODE_NAME}}"
  STANDALONE=1
fi
# Customer-cluster service DNS. Default matches k3s; central can
# override per-customer via CLUSTER_DNS / CLUSTER_DOMAIN env stamps
# once agentenv.Build plumbs them through (currently TODO — see
# docs/JOINED-CNI.md). EKS uses 172.20.0.10, GKE uses 10.0.0.10,
# kubeadm default is 10.96.0.10.
: "${CLUSTER_DNS:=10.43.0.10}"
: "${CLUSTER_DOMAIN:=cluster.local}"
# Fly's init sometimes strips PATH down to /usr/bin:/bin — make sure
# the binaries we curl'd into /usr/local/bin are findable.
export PATH="/usr/local/bin:/usr/local/sbin:${PATH}"

# Boot timing — every [yscale-agent] log line is prefixed with seconds
# elapsed since this script started. Lets us read a burst's stdout
# (`fly machine logs` / `linode-cli linodes view-instance-stats`) and
# compute per-phase durations without external instrumentation.
#
# Phase breakdown we care about (target: total → Node-Ready < 60 s):
#   tailscale up                 ~5-10 s
#   bootstrap kubeconfig fetch   ~2-5 s
#   cilium-config fetch          ~1-2 s
#   cilium-agent → CNI ready     ~10-180 s  ← biggest variable
#   kubelet bootstrap + Ready    ~10-20 s
#
# The cilium-agent line is the dominant uncertain phase; this script's
# wait-loop already reports `host-mode cilium ready ($i s)` which gives
# the precise duration. The added timer captures everything around it.
__T0_NS=$(date +%s%N)
log_t() {
  local now_ns elapsed_ms
  now_ns=$(date +%s%N)
  elapsed_ms=$(( (now_ns - __T0_NS) / 1000000 ))
  printf '[%4d.%03ds] %s\n' $((elapsed_ms / 1000)) $((elapsed_ms % 1000)) "$*"
}

# Ensure tun device exists for kernel-mode Tailscale.
mkdir -p /dev/net
if [ ! -c /dev/net/tun ]; then
  mknod /dev/net/tun c 10 200
  chmod 600 /dev/net/tun
fi

log_t "starting tailscale..."
# `env -u TS_AUTHKEY` scrubs the key from tailscaled's inherited env so
# it's not readable via /proc/<pid>/environ for the burst's lifetime.
# `tailscale up` below still gets it (via --auth-key=file:...).
env -u TS_AUTHKEY tailscaled --state=/var/lib/tailscale/tailscaled.state --socket=/var/run/tailscale/tailscaled.sock &
sleep 2

# Write TS_AUTHKEY to a 600 file and feed via `--auth-key=file:<path>`
# so the key never appears in `ps aux` (visible via /proc/<pid>/cmdline)
# and is not inherited into kubelet's process env (which runs for the
# full burst lifetime). `--auth-key-file=...` is NOT a valid tailscale
# flag in 1.80+ — it dumps `tailscale up --help` and exits 2 (the
# silent boot failure mode we hit on every Fly burst 2026-05-23 till
# this fix). The `file:` prefix on --auth-key is the documented form
# (see `tailscale up --help` output for `--auth-key string`).
# Shred + unset after `tailscale up` consumes it.
install -m 600 /dev/null /run/ts-authkey
printf '%s' "${TS_AUTHKEY}" > /run/ts-authkey
TS_UP_ARGS="--auth-key=file:/run/ts-authkey --hostname=${TS_HOSTNAME} --accept-routes --reset"
if [ -n "${TS_TAGS:-}" ]; then
  TS_UP_ARGS="${TS_UP_ARGS} --advertise-tags=${TS_TAGS}"
fi
# Advertise this burst's pod CIDR so homelab nodes (which also run
# tailscale + accept-routes) can route to pods scheduled on this burst.
# Joined-CNI primitive — see docs/JOINED-CNI.md. The Tailscale ACL must
# autoApprove these advertisements (autoApprovers.routes scoped to the
# burst tag), otherwise the route stays Pending until manual approval.
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

log_t "waiting for tailscale..."
until tailscale status --peers=false > /dev/null 2>&1; do
  sleep 1
done

TS_IP=$(tailscale ip -4)
log_t "tailscale is up: ${TS_IP}"

# Fetch bootstrap kubeconfig from the agent over the tailnet. The
# agent mints a fresh kubelet bootstrap token (15-min TTL) and signs
# the kubeconfig with the customer cluster's CA. BurstID is the auth
# secret — central tells the agent which BurstID to honor via
# BurstAnnounce; tailnet ACL is the network-layer trust boundary.
mkdir -p /etc/kubernetes
if [ "${STANDALONE}" = "1" ]; then
  log_t "standalone mode: self-minting bootstrap kubeconfig for ${API_SERVER}"
  printf '%s' "${CLUSTER_CA}" | base64 -d > /etc/kubernetes/ca.crt
  if [ ! -s /etc/kubernetes/ca.crt ]; then
    log_t "CLUSTER_CA did not decode to a non-empty ca.crt"
    exit 1
  fi
  cat > /etc/kubernetes/bootstrap-kubeconfig.yaml <<KUBECONFIGEOF
apiVersion: v1
kind: Config
clusters:
- name: default
  cluster:
    server: ${API_SERVER}
    certificate-authority: /etc/kubernetes/ca.crt
users:
- name: kubelet-bootstrap
  user:
    token: ${BOOTSTRAP_TOKEN}
contexts:
- name: default
  context:
    cluster: default
    user: kubelet-bootstrap
current-context: default
KUBECONFIGEOF
  chmod 0600 /etc/kubernetes/bootstrap-kubeconfig.yaml
  BOOT_RESP='{}'
else
log_t "fetching bootstrap kubeconfig from ${BOOTSTRAP_ENDPOINT}..."
BOOT_REQ=$(printf '{"burst_id":"%s","node_name":"%s"}' "${BURST_ID}" "${NODE_NAME}")
if ! BOOT_RESP=$(curl -fsS -X POST -H 'Content-Type: application/json' \
  --retry 10 --retry-delay 3 --retry-connrefused --retry-all-errors \
  --max-time 90 \
  -d "${BOOT_REQ}" \
  "${BOOTSTRAP_ENDPOINT}/bootstrap-kubeconfig"); then
  log_t "bootstrap fetch failed — agent unreachable over tailnet, or BurstID not authorized"
  exit 1
fi
if [ -z "${BOOT_RESP}" ]; then
  log_t "bootstrap response empty"
  exit 1
fi
echo "${BOOT_RESP}" | jq -er '.kubeconfig' | base64 -d > /etc/kubernetes/bootstrap-kubeconfig.yaml
if [ ! -s /etc/kubernetes/bootstrap-kubeconfig.yaml ]; then
  log_t "bootstrap-kubeconfig.yaml empty after decode"
  exit 1
fi

# Customer cluster's cloud provider. GKE ValidatingAdmissionPolicy
# rejects providerIDs that don't end with /<node-name>.
CLUSTER_CLOUD="$(echo "${BOOT_RESP}" | jq -r '.cloud_provider // empty' 2>/dev/null || true)"
case "${CLUSTER_CLOUD}" in
  gcp)
    # GKE ValidatingAdmissionPolicy validate-node-providerid requires providerID
    # to end with /<node-name>. linode://<NODE_NAME> satisfies this; a numeric
    # Linode instance ID does not. Unconditional: a preseeded value would fail.
    PROVIDER_ID="linode://${NODE_NAME}"
    log_t "GKE cluster: providerID=${PROVIDER_ID} (admission requires /<node-name> suffix)"
    ;;
esac
fi

# Gateway routing — historical context for future readers:
#
# An earlier design (commit 5373296) tried to install per-CIDR static
# kernel routes here (`ip route ... via <gateway-IP> dev tailscale0
# onlink`) to avoid Tailscale subnet-route broadcast. That approach
# put packets on tailscale0 correctly but tailscaled DROPPED them
# because no peer had the CIDR in its AllowedIPs (no advertisement,
# no peer-routing entry). Subnet routing IS the right primitive;
# tailscaled needs the per-peer AllowedIPs mapping to forward.
#
# Current design: gateway pod advertises customer pod CIDRs via
# --advertise-routes. Bursts (with --accept-routes, set by tailscale
# up earlier in this script) install the route automatically and
# tailscaled knows to forward via the gateway peer. Nothing for the
# burst entrypoint to do here — kernel routing is already wired by
# tailscaled.
#
# Field GatewayHostname is preserved in BootstrapResponse for future
# debugging / observability; not currently consumed by the burst.
:

# Tier — full runs the binary-baked Cilium agent as a host process, and
# is now the only supported value; the removed lite tier used to skip
# Cilium and ride the bridge CNI fallback. Tier comes from central via
# BURST_TIER env; central always emits "full" (see
# pkg/backends/internal/agentenv), so an explicit "lite" or any other
# non-empty non-"full" value here means either an out-of-date central or
# a tampered env. The first-boundary check above has already refused it
# before any Tailscale or bootstrap activity.
# Standalone mode has no agent to serve a cilium_kubeconfig, so
# HOST_CILIUM stays 0 below and the kubelet registers with the
# cilium-not-ready taint — the customer cluster's own Cilium DaemonSet
# is expected to schedule onto this burst and lift it. Earlier versions
# silently forced BURST_TIER=lite here to bypass the wait; the removed
# lite tier is no longer a valid escape hatch.
log_t "networking tier: ${BURST_TIER}"

# Host-mode cilium kubeconfig is consumed when the bootstrap response
# carries one; the K8s Cilium DS is expected to skip host-mode bursts
# via the yscale.sh/cilium-mode=host label (excluded by nodeAffinity
# NotIn). full is now the only tier we ever reach here — the guard
# above exits on lite/unknown — but we keep the branch explicit so a
# future tier addition slots in cleanly.
mkdir -p /etc/cilium
HOST_CILIUM=0
if [ "${BURST_TIER}" = "full" ]; then
  if echo "${BOOT_RESP}" | jq -er '.cilium_kubeconfig' >/dev/null 2>&1; then
    echo "${BOOT_RESP}" | jq -er '.cilium_kubeconfig' | base64 -d > /etc/cilium/kubeconfig
    chmod 0600 /etc/cilium/kubeconfig
    HOST_CILIUM=1
    log_t "tier=full: cilium-kubeconfig received; host-mode Cilium enabled"
  else
    log_t "tier=full but no cilium-kubeconfig in bootstrap; falling back to K8s DaemonSet Cilium path"
  fi
fi

# Write a bridge CNI conflist as a FALLBACK so kubelet has a network
# plugin from boot and can register the Node. When the Cilium DaemonSet
# pod schedules onto this burst (after the node is Ready), Cilium writes
# /etc/cni/net.d/05-cilium.conflist which takes precedence (lower
# lexicographic filename = higher priority in kubelet's CNI scanner).
# From that point pods get Cilium networking. The bridge conf is just
# the bootstrap floor.
mkdir -p /etc/cni/net.d /opt/cni/bin
write_bridge_cni() {
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
        "ranges": [[{"subnet": "$1"}]],
        "routes": [{"dst": "0.0.0.0/0"}]
      }
    },
    {"type": "portmap", "capabilities": {"portMappings": true}}
  ]
}
CNIEOF
}
if [ -n "${POD_CIDR:-}" ]; then
  write_bridge_cni "${POD_CIDR}"
fi

# Block the customer workload pod from reaching the cloud metadata service
# (169.254.169.254). The burst's provisioning secrets — TS_AUTHKEY (a reusable
# tailnet key) and the kubelet bootstrap token — ride in cloud-init user-data,
# which the metadata endpoint serves to ANYTHING on the VM that can reach it,
# including a pod curl'ing it over its default route. The host finished reading
# metadata during cloud-init (before this entrypoint), so host access (OUTPUT
# chain) is untouched; only FORWARDed pod traffic from POD_CIDR is dropped.
# Best-effort + idempotent — a firewall hiccup must not brick the burst. On Fly
# there is no such metadata service (env is injected directly), so this is a
# harmless no-op there. Full-tier Cilium (eBPF) may bypass iptables; a
# CiliumClusterwideNetworkPolicy is the belt-and-suspenders equivalent (TODO).
block_pod_metadata() {
  if ! iptables -C FORWARD -s "$1" -d 169.254.169.254/32 -j DROP 2>/dev/null; then
    iptables -A FORWARD -s "$1" -d 169.254.169.254/32 -j DROP \
      || log_t "WARNING: could not install pod->metadata (169.254.169.254) egress block"
  fi
}
if [ -n "${POD_CIDR:-}" ]; then
  block_pod_metadata "${POD_CIDR}"
fi

# Mount BPF and cgroup2 filesystems Cilium needs. Both are best-effort:
# on a fresh Firecracker microVM /sys/fs/bpf is typically already
# mounted by systemd, and cgroup2 may already be the unified hierarchy.
# Failures here are NOT fatal — Cilium's mount-bpf-fs init container
# does the same work if these no-op.
mountpoint -q /sys/fs/bpf || mount -t bpf bpf /sys/fs/bpf 2>/dev/null || true
mkdir -p /run/cilium/cgroupv2
mount -t cgroup2 none /run/cilium/cgroupv2 2>/dev/null || true

# Write stargz-snapshotter config. cri_keychain points stargz at
# containerd's CRI image service so it can reuse the kubelet-supplied
# imagePullSecrets when fetching streamed layers from private registries
# (Docker Hub with stackmaster-dockerhub, registry.fly.io, etc).
mkdir -p /etc/containerd-stargz-grpc
cat > /etc/containerd-stargz-grpc/config.toml <<STARGZEOF
[cri_keychain]
enable_keychain = true
image_service_path = "/run/containerd/containerd.sock"
STARGZEOF

# Ensure /dev/fuse exists for the stargz daemon (Fly microVMs don't
# create the device node by default; same pattern as /dev/net/tun above).
if [ ! -c /dev/fuse ]; then
  mknod /dev/fuse c 10 229 2>/dev/null || true
  chmod 666 /dev/fuse 2>/dev/null || true
fi

# Start the stargz remote snapshotter. Non-fatal: if it fails (its
# multi-lowerdir probe hits Fly's nested-overlay restriction and bails)
# we fall back to fuse-overlayfs / native below. Diagnose via
# /var/log/containerd-stargz-grpc.log.
mkdir -p /run/containerd-stargz-grpc /var/lib/containerd-stargz-grpc
STARGZ_SOCK=/run/containerd-stargz-grpc/containerd-stargz-grpc.sock
log_t "starting containerd-stargz-grpc (non-fatal)..."
/usr/local/bin/containerd-stargz-grpc \
  --address="${STARGZ_SOCK}" \
  --config=/etc/containerd-stargz-grpc/config.toml \
  --root=/var/lib/containerd-stargz-grpc \
  >/var/log/containerd-stargz-grpc.log 2>&1 &

# Start the fuse-overlayfs proxy snapshotter. This is the real fix for
# Fly: it gives containerd a working overlay-mount snapshotter that
# uses FUSE underneath so it doesn't trip the kernel's no-overlay-on-
# overlay check. ~5-10s container init instead of native's 1-2min.
#
# --root is the CONVENTIONAL path that cadvisor (kubelet's stats
# collector) probes for image-fs stats. If we put it anywhere else,
# kubelet's eviction_manager spams "Failed to get HasDedicatedImageFs"
# every 10s forever. Naming convention is
# /var/lib/containerd/io.containerd.snapshotter.v1.<name>.
FUSE_OVERLAYFS_ROOT=/var/lib/containerd/io.containerd.snapshotter.v1.fuse-overlayfs
mkdir -p "${FUSE_OVERLAYFS_ROOT}" /run/containerd-fuse-overlayfs
FUSE_OVERLAYFS_SOCK=/run/containerd-fuse-overlayfs/snapshotter.sock
log_t "starting containerd-fuse-overlayfs-grpc..."
/usr/local/bin/containerd-fuse-overlayfs-grpc \
  "${FUSE_OVERLAYFS_SOCK}" \
  "${FUSE_OVERLAYFS_ROOT}" \
  >/var/log/containerd-fuse-overlayfs.log 2>&1 &

# CRI snapshotter selection.
#  1. stargz   — lazy-pull (rarely works on Fly per nested-overlay)
#  2. fuse-overlayfs — real overlay via FUSE (the path that works on Fly)
#  3. native   — slow fallback if both daemons failed to socket up
SNAPSHOTTER=native
for _ in 1 2 3 4 5; do
  if [ -S "${STARGZ_SOCK}" ]; then SNAPSHOTTER=stargz; break; fi
  if [ -S "${FUSE_OVERLAYFS_SOCK}" ]; then SNAPSHOTTER=fuse-overlayfs; break; fi
  sleep 1
done
log_t "CRI snapshotter selected: ${SNAPSHOTTER}"

# Write containerd config. Default root (/var/lib/containerd) so
# cadvisor's image-fs heuristics line up. The stargz proxy_plugin
# block is included ONLY when SNAPSHOTTER=stargz (daemon socket
# confirmed up) — leaving it registered but the socket unreachable
# was stalling every pull while containerd tried to look up snapshot
# annotations via the dead remote snapshotter.
mkdir -p /etc/containerd
cat > /etc/containerd/config.toml <<CTDEOF
version = 2
state = "/run/containerd"
[plugins."io.containerd.grpc.v1.cri"]
  sandbox_image = "registry.k8s.io/pause:3.10"
[plugins."io.containerd.grpc.v1.cri".containerd]
  snapshotter = "${SNAPSHOTTER}"
  disable_snapshot_annotations = true
[plugins."io.containerd.grpc.v1.cri".cni]
  bin_dir = "/opt/cni/bin"
  conf_dir = "/etc/cni/net.d"
CTDEOF
if [ "${SNAPSHOTTER}" = "stargz" ]; then
  cat >> /etc/containerd/config.toml <<STARGZPP
[proxy_plugins.stargz]
  type = "snapshot"
  address = "${STARGZ_SOCK}"
STARGZPP
  # Stargz needs snapshot annotations on for lazy-pull layer fetching.
  sed -i 's/disable_snapshot_annotations = true/disable_snapshot_annotations = false/' /etc/containerd/config.toml
elif [ "${SNAPSHOTTER}" = "fuse-overlayfs" ]; then
  cat >> /etc/containerd/config.toml <<FUSEPP
[proxy_plugins.fuse-overlayfs]
  type = "snapshot"
  address = "${FUSE_OVERLAYFS_SOCK}"
FUSEPP
fi

# Make root mount shared (rshared) so containerd can create container
# bind mounts under it. Without this, CreateContainer fails with
# "path / is mounted on / but it is not a shared or slave mount".
# Best-effort: on Firecracker/systemd setups where root is already
# rshared, no-op; only fail if propagation can't be set at all.
mount --make-rshared / 2>/dev/null || true

# Start containerd. Absolute paths because Fly's machine init runs
# the entrypoint with a PATH that may exclude /usr/local/bin.
log_t "starting containerd..."
/usr/local/bin/containerd --config /etc/containerd/config.toml >/var/log/containerd.log 2>&1 &
sleep 3

# Import any pre-baked OCI archives into containerd's k8s.io namespace
# (where kubelet looks). The Cilium agent image is preloaded into
# /var/lib/yscale/preload/ by Dockerfile.kubelet.base via skopeo, so
# when the Cilium DaemonSet schedules a pod onto this burst the image
# is already present — no quay.io pull, no minutes-long cold-boot.
#
# `timeout 60` is REQUIRED on Fly. The fuse-overlayfs snapshotter
# daemon comes up fine and its socket is reachable, but its first
# real mount operation (during ctr import's commit-to-snapshotter
# step) hangs forever on Fly's Firecracker overlay-on-overlay rootfs
# instead of failing fast. Without the timeout, the entrypoint blocks
# here forever and kubelet never starts — bursts never join the
# cluster. With the timeout, the import gets killed at 60s, the ||
# branch fires, and kubelet starts (it'll just pull Cilium fresh from
# quay.io at runtime — slower but functional). Linode/AWS/etc. don't
# need the timeout (real disks → fuse-overlayfs mounts work), but
# capping at 60s is cheap insurance everywhere.
if [ -d /var/lib/yscale/preload ]; then
  for tar in /var/lib/yscale/preload/*.tar; do
    [ -f "$tar" ] || continue
    log_t "importing preloaded OCI archive: $tar"
    timeout 60 /usr/local/bin/ctr --namespace=k8s.io images import --no-unpack=false "$tar" \
      >>/var/log/yscale-preload.log 2>&1 || \
      log_t "warn: import of $tar failed/timed out (non-fatal; kubelet will pull at runtime)"
  done
fi

# DNS routing on the burst.
#
# A burst-local CoreDNS runs as a HOST process, listening on the bridge
# gateway IP (the cni0 .1 address). Pod default-route already points at
# that IP via the bridge CNI conflist, so a pod's DNS lookup trivially
# reaches it. CoreDNS upstream forwards to 1.1.1.1 (always works) and
# 100.100.100.100 (Tailscale MagicDNS — reachable because tailscale is
# up on the host before CoreDNS starts).
#
# Full-tier bursts that ALSO have host-mode cilium can rely on Cilium's
# kube-proxy-replacement to translate the cluster's CoreDNS ServiceIP,
# but we run the burst-local resolver anyway — it's strictly additive
# (different listen IP) and avoids a hard dep on the customer's CoreDNS
# being healthy. Earlier iterations of this entrypoint shipped a CoreDNS
# static pod that crashlooped on kubeconfig permissions; running it as a
# host process side-steps that entirely.
# Standalone mode learns its pod CIDR only after registration, so the
# resolver binds a fixed dummy-interface address (the node-local-dns
# convention, 169.254.20.10) that pods reach via their default route,
# instead of the not-yet-known cni0 gateway IP.
if [ "${STANDALONE}" = "1" ]; then
  ip link add ysdns0 type dummy 2>/dev/null || true
  ip addr add 169.254.20.10/32 dev ysdns0 2>/dev/null || true
  ip link set ysdns0 up 2>/dev/null || true
  DNS_BIND="169.254.20.10"
else
  DNS_BIND=$(echo "${POD_CIDR}" | awk -F'[./]' '{print $1"."$2"."$3".1"}')
fi
POD_CIDR_GW="${DNS_BIND}"

mkdir -p /etc/coredns /var/log
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

# Start CoreDNS in the background. It binds on POD_CIDR_GW:53 (pod
# default-route target) AND 127.0.0.1:53 (handy for host-process
# lookups). Logs to /var/log/coredns.log for post-mortem.
if [ -x /usr/local/bin/coredns ]; then
  nohup /usr/local/bin/coredns -conf /etc/coredns/Corefile >/var/log/coredns.log 2>&1 &
  log_t "burst-local CoreDNS started pid=$! (bind ${POD_CIDR_GW}:53, 127.0.0.1:53)"
else
  log_t "WARN: /usr/local/bin/coredns missing; falling back to CLUSTER_DNS=${CLUSTER_DNS}"
fi

# Point kubelet (and therefore every pod) at the burst-local CoreDNS.
# Override CLUSTER_DNS regardless of caller-provided value — a
# customer-cluster ClusterIP isn't reachable from a burst without
# kube-proxy or its Cilium replacement translating it.
CLUSTER_DNS="${POD_CIDR_GW}"
log_t "cni0 gateway: ${POD_CIDR_GW}; cluster-dns -> ${CLUSTER_DNS} (burst-local CoreDNS)"

# Build kubelet args.
#
# --rotate-server-certificates sets serverTLSBootstrap: instead of
# self-signing a serving cert (which lacks IP SANs, so the apiserver
# can't reach :10250 for logs/exec/attach over the tailnet), the
# kubelet files a kubernetes.io/kubelet-serving CSR. The cert it gets
# back carries --node-ip (the tailnet IP) in its SAN list. The agent's
# CSR approver auto-approves these for system:node:ys-burst-* nodes.
# --provider-id shapes how the CUSTOMER cluster's CCM sees this Node:
#   - EKS/AKS CCMs act only on their own providerID prefix and ignore a
#     foreign linode:// node, so they do NOT reap it — fine.
#   - GKE's ValidatingAdmissionPolicy (validate-node-providerid) rejects
#     providerIDs that don't end with /<node-name>. Bursts register as
#     linode://<NODE_NAME> to pass admission. The upstream GCE cloud-node-
#     lifecycle controller retains nodes whose provider lookup errors out.
#   - k3s/homelab: no CCM, nothing reaps it — fine.
#   - LKE (Linode-managed customer cluster) is the hostile case: its CCM reaps
#     any Node whose providerID doesn't resolve to a REAL Linode instance. The
#     synthetic `linode://yscale-burst-<id>` below does NOT survive LKE — LKE
#     treats the parse failure as "not mine → delete" (re-confirmed 2026-06-13;
#     the earlier "retries forever" assumption was wrong). Linode-BACKED bursts
#     dodge this by registering as their real instance id (see
#     pkg/backends/linode/bootstrap*.sh, which self-fetch it from the Linode
#     Metadata Service). A FLY burst can't self-fetch a Linode id, so Fly-on-LKE
#     is unprotected — documented limitation.
# Override via PROVIDER_ID env.
: "${PROVIDER_ID:=linode://yscale-burst-${BURST_ID}}"

# If host-mode Cilium is enabled, launch cilium-agent as a backgrounded
# host process BEFORE kubelet. Binaries (cilium-agent, cilium-cni) and
# the bpf/ source tree are baked into the burst image at
# Dockerfile.kubelet.base build time (no containerd image, no
# snapshotter unpack). cilium-agent compiles its BPF programs at boot
# via the clang we install in the base, then writes the CNI conflist.
# When kubelet starts, CNI is already in place → no cilium-not-ready
# taint needed → pods can schedule the moment the node is Ready.
if [ "${HOST_CILIUM:-0}" = "1" ] && [ -x /usr/local/bin/cilium-agent ]; then
  log_t "starting host-mode cilium-agent (binary baked)"
  mkdir -p /var/run/cilium /var/lib/cilium /etc/cilium-config

  # Pre-warm cilium-agent's BPF template cache from build-time precompiles.
  # Dockerfile.kubelet.base ran `clang -target bpf` against the invariant
  # templates at image build, producing .o files in
  # /var/lib/cilium/bpf/.precompiled/. We copy them into the runtime
  # state dir BEFORE cilium-agent starts, so its hash-cache lookup hits
  # for the invariant pieces. Per-node templates (whose hash includes
  # POD_CIDR, MAC, etc.) still recompile at runtime — that's correct.
  #
  # Note: cilium-agent's actual cache layout is
  # /var/run/cilium/state/templates/<sha>/. We can't predict the <sha>
  # at build time because it includes runtime kernel/feature inputs.
  # Instead, we place precompiled .o's next to the .c sources where
  # cilium-agent's "src older than .o" heuristic skips the compile pass
  # for invariant templates. Measured impact lands in the
  # `host-mode cilium ready (Xs)` boot-log line below.
  # Pre-warm is best-effort — failing here must NEVER kill the burst.
  # `set +e` for the whole block; `set -e` restored after.
  if [ -d /var/lib/cilium/bpf/.precompiled ]; then
    set +e
    log_t "warming BPF template cache from precompiled .o files"
    PRECOMP_COUNT=0
    for src in /var/lib/cilium/bpf/.precompiled/*.o; do
      [ -e "$src" ] || continue
      cp -a "$src" /var/lib/cilium/bpf/ 2>/dev/null
      PRECOMP_COUNT=$((PRECOMP_COUNT + 1))
    done
    # Touch each .o so its mtime > the corresponding .c → cilium-agent's
    # "src older than .o" heuristic skips the compile.
    for o in /var/lib/cilium/bpf/*.o; do
      [ -e "$o" ] || continue
      touch "$o" 2>/dev/null
    done
    log_t "warmed $PRECOMP_COUNT BPF objects (cilium-agent reuses whichever match its runtime config hash)"
    set -e
  fi
  CILIUM_LOG=/var/log/cilium-agent.log
  # cilium-agent is HEAVILY configured via the cilium-config ConfigMap
  # in kube-system (the DS path mounts it at /tmp/cilium/config-map).
  # Per-key files, one config value each. We mirror that pattern by
  # fetching the ConfigMap once at boot and writing each key as a
  # file in /etc/cilium-config. cilium-agent reads it via --config-dir.
  log_t "fetching cilium-config ConfigMap via apiserver"
  APISERVER="$(awk '/server:/ {print $2}' /etc/cilium/kubeconfig | head -1)"
  TOKEN="$(awk '/token:/ {print $2}' /etc/cilium/kubeconfig | head -1)"
  CM_JSON="$(curl -sS --insecure \
    -H "Authorization: Bearer $TOKEN" \
    "$APISERVER/api/v1/namespaces/kube-system/configmaps/cilium-config" 2>/dev/null || echo '{}')"
  # Parse + write each ConfigMap data key as a file in /etc/cilium-config/.
  # jq is in the burst-base image (apt: jq); python3 isn't, so this is
  # the shell-portable path.
  if echo "$CM_JSON" | jq -e '.data' >/dev/null 2>&1; then
    NUM_KEYS=$(echo "$CM_JSON" | jq -r '.data | keys[]' 2>/dev/null | wc -l)
    echo "$CM_JSON" | jq -r '.data | keys[]' | while IFS= read -r k; do
      echo "$CM_JSON" | jq -r --arg key "$k" '.data[$key]' > "/etc/cilium-config/$k"
    done
    log_t "wrote $NUM_KEYS cilium-config keys"
  else
    log_t "WARN: cilium-config ConfigMap not found / unparseable; cilium-agent will use defaults"
  fi

  # Overrides specific to host-mode:
  #   - kube-proxy-replacement=false  → customer cluster's kube-proxy
  #     handles service IPs; we don't need cilium to replace it.
  #   - enable-bgp-control-plane=false → no BGP on bursts.
  #   - enable-hubble=false → no metrics/observability stack on burst.
  # These are written AFTER the CM fetch so they win.
  echo false > /etc/cilium-config/enable-hubble
  echo false > /etc/cilium-config/enable-bandwidth-manager
  echo false > /etc/cilium-config/enable-bgp-control-plane

  # Minimal CLI flag set. EVERYTHING else lives in the ConfigMap-
  # mirrored /etc/cilium-config/. Cilium reads CNI bin/conf paths
  # from defaults (/opt/cni/bin, /etc/cni/net.d) — those aren't CLI
  # flags. The CNI conflist is written via --write-cni-conf-when-ready
  # only after the agent is fully up and BPF programs are loaded;
  # this is the signal our wait loop watches for.
  nohup /usr/local/bin/cilium-agent \
    --k8s-kubeconfig-path=/etc/cilium/kubeconfig \
    --k8s-namespace=kube-system \
    --config-dir=/etc/cilium-config \
    --state-dir=/var/run/cilium \
    --write-cni-conf-when-ready=/etc/cni/net.d/05-cilium.conflist \
    >"$CILIUM_LOG" 2>&1 &
  CILIUM_PID=$!
  log_t "cilium-agent pid=$CILIUM_PID (logs: $CILIUM_LOG)"
  # Wait up to 60s for cilium-agent to write its CNI conflist — that's
  # the signal it's ready to handle pod-creation requests.
  for i in $(seq 1 60); do
    if [ -f /etc/cni/net.d/05-cilium.conflist ]; then
      log_t "host-mode cilium ready ($i s)"
      break
    fi
    if ! kill -0 "$CILIUM_PID" 2>/dev/null; then
      log_t "FATAL: cilium-agent died before writing CNI conflist; tail of log:"
      tail -30 "$CILIUM_LOG" 2>&1 || true
      exit 1
    fi
    sleep 1
  done
fi

# Taints + cilium-mode label by HOST_CILIUM (tier=full is the only value
# we ever reach here — see the tier gate above):
#
#   HOST_CILIUM=1 → host-mode Cilium is already up; CNI conflist is
#                   written before kubelet registers. No cilium-not-ready
#                   taint. Label cilium-mode=host so the in-cluster DS
#                   skips (avoids double-Cilium contention).
#
#   HOST_CILIUM=0 → fallback: cilium-kubeconfig wasn't provided. Keep the
#                   cilium-not-ready taint so the K8s DS gates pod
#                   scheduling until its agent pod is healthy. cilium-mode
#                   label omitted so the DS WILL schedule on this node.
if [ "${HOST_CILIUM:-0}" = "1" ]; then
  REGISTER_TAINTS="yscale.sh/burst-node=true:NoSchedule"
  HOST_CILIUM_LABEL=",yscale.sh/cilium-mode=host"
else
  REGISTER_TAINTS="yscale.sh/burst-node=true:NoSchedule,node.cilium.io/agent-not-ready=true:NoSchedule"
  HOST_CILIUM_LABEL=""
fi

KUBELET_ARGS="--bootstrap-kubeconfig=/etc/kubernetes/bootstrap-kubeconfig.yaml \
  --kubeconfig=/etc/kubernetes/kubelet.kubeconfig \
  --node-ip=${TS_IP} \
  ${PROVIDER_ID:+--provider-id=${PROVIDER_ID}} \
  --rotate-server-certificates \
  --hostname-override=${NODE_NAME} \
  --container-runtime-endpoint=unix:///run/containerd/containerd.sock \
  --pod-manifest-path=/etc/kubernetes/manifests \
  --cgroup-driver=cgroupfs \
  --cluster-dns=${CLUSTER_DNS} \
  --cluster-domain=${CLUSTER_DOMAIN} \
  --image-gc-high-threshold=100 \
  --image-gc-low-threshold=99 \
  --eviction-hard=imagefs.available<0%,memory.available<100Mi,nodefs.available<5% \
  --register-with-taints=${REGISTER_TAINTS} \
  --node-labels=yscale.sh/burst-node=true${HOST_CILIUM_LABEL}"

# Add node labels if set.
if [ -n "${EXTRA_NODE_LABELS:-}" ]; then
  KUBELET_ARGS="${KUBELET_ARGS},${EXTRA_NODE_LABELS}"
fi

# In standalone mode kubelet starts with NO CNI conflist: the node
# registers, stays NotReady on NetworkNotReady, the cluster's node-CIDR
# allocator assigns spec.podCIDR, and this watcher then writes the
# bridge conflist for exactly that CIDR (collision-free by construction,
# unlike any statically stamped guess). It authenticates with the
# kubelet's own bootstrapped client cert, which the Node authorizer
# scopes to reading this node only.
standalone_podcidr_watcher() {
  local kc=/var/lib/kubelet/pki/kubelet-client-current.pem
  local cidr=""
  for _ in $(seq 1 300); do
    if [ -f "${kc}" ]; then
      cidr=$(curl -sS --cacert /etc/kubernetes/ca.crt --cert "${kc}" --key "${kc}" \
        "${API_SERVER}/api/v1/nodes/${NODE_NAME}" 2>/dev/null \
        | jq -er '.spec.podCIDR // empty' 2>/dev/null) || cidr=""
      if [ -n "${cidr}" ]; then break; fi
    fi
    sleep 2
  done
  if [ -z "${cidr}" ]; then
    log_t "standalone: node podCIDR never assigned; CNI left unconfigured"
    return 0
  fi
  log_t "standalone: cluster assigned podCIDR ${cidr}; writing bridge CNI conflist"
  write_bridge_cni "${cidr}"
  block_pod_metadata "${cidr}"
}
if [ "${STANDALONE}" = "1" ]; then
  standalone_podcidr_watcher &
fi

# GPU telemetry reporter — best-effort, never blocks kubelet startup.
# Posts a bounded nvidia-smi utilisation sample to the agent every 30s.
# Stops when the host/service exits (it runs in the background of this
# script's process group, killed by SIGTERM or the kubelet exec below
# replacing the shell). Only runs in agent mode (BOOTSTRAP_ENDPOINT set)
# and only when nvidia-smi is available.
gpu_telemetry_reporter() {
  local endpoint="$1" burst_id="$2" node_name="$3"
  while true; do
    sleep 30
    local max_util=0 found=0 product="" product_mixed=0 product_json="" product_escaped=""
    while IFS= read -r line; do
      local v
      v=$(echo "$line" | tr -d '[:space:]')
      case "${v}" in ''|*[!0-9]*) continue ;; esac
      if [ "$v" -lt 0 ] || [ "$v" -gt 100 ]; then continue; fi
      found=1
      if [ "$v" -gt "$max_util" ]; then max_util=$v; fi
    done <<EOF
$(nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits 2>/dev/null)
EOF
    if [ "$found" = 0 ]; then continue; fi
    while IFS= read -r line; do
      line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
      [ -z "$line" ] && continue
      if [ -z "$product" ]; then
        product="$line"
      elif [ "$product" != "$line" ]; then
        product_mixed=1
        product=""
        break
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
      -d "{\"burst_id\":\"${burst_id}\",\"node_name\":\"${node_name}\",\"utilization\":${max_util}${product_json}}" \
      "${endpoint}/gpu-telemetry" >/dev/null 2>&1 || true
  done
}
if [ "${STANDALONE}" = "0" ] && command -v nvidia-smi >/dev/null 2>&1; then
  gpu_telemetry_reporter "${BOOTSTRAP_ENDPOINT}" "${BURST_ID}" "${NODE_NAME}" &
  log_t "gpu telemetry reporter started pid=$!"
fi

log_t "starting kubelet: ${NODE_NAME}"
# shellcheck disable=SC2086 # word-splitting is intentional: KUBELET_ARGS is built as a space-separated arg list
exec /usr/local/bin/kubelet ${KUBELET_ARGS}
