#!/bin/bash
#
# yscale GCP burst bootstrap — JOIN-ONLY variant for baked images.
#
# Run by the Compute Engine guest agent (the `startup-script` instance
# metadata key) on a burst booting from an image produced by
# pkg/backends/gcp/image/build-image.sh — that image already carries
# tailscale, kubelet, containerd, runc, cilium binaries + BPF
# precompile, and the CNI plugins (pkg/backends/gcp/image/install.sh).
# This script does ONLY the per-burst join; the cold-boot variant (full
# install) is bootstrap.sh, used for stock Debian 12.
#
# Per-burst values arrive as shell exports gcp.CreateNode prepends ahead
# of this body. Ported from pkg/backends/aws/bootstrap-baked.sh — see
# that file for the canonical shape of this recipe.
set -euo pipefail
# 'x' (xtrace) intentionally OMITTED: with xtrace on, the
# `tailscale up --authkey=...` line below would be written verbatim
# into bootstrap.log — a reusable tailnet credential persisted on
# disk for the burst's lifetime. Use `bash -x` interactively to debug.

# ERR trap: if the script bombs, write a one-line failure summary to
# the serial console (and a sentinel file for the guest agent to
# surface) BEFORE redirecting stdout/stderr to the log file. Without
# this, a startup-script failure shows up in `gcloud compute instances
# get-serial-port-output` as an opaque non-zero exit — useless for
# triage. With this, the console line tells you WHICH step died.
trap 'EC=$?; echo "[yscale] BOOTSTRAP FAILED at line $LINENO (exit=$EC); see /var/log/yscale-bootstrap.log on the host" > /dev/console 2>/dev/null; echo "FAILED line=$LINENO exit=$EC" > /var/log/yscale-bootstrap.FAILED; exit $EC' ERR

exec > /var/log/yscale-bootstrap.log 2>&1
echo "[yscale] join-only bootstrap starting $(date -u)"

# Required exports (stamped by gcp.CreateNode ahead of this body). Fail
# loud + early — an empty POD_CIDR would later expand into an invalid
# bridge CNI conflist (`"subnet": ""`), kubelet would stall at
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

# TS_TAGS and EXTRA_NODE_LABELS are exported (maybe empty) by CreateNode.
: "${TS_TAGS:=}"
: "${EXTRA_NODE_LABELS:=}"
# TS_LOGIN_SERVER points 'tailscale up' at a self-hosted coordination server;
# empty = Tailscale SaaS (no flag).
: "${TS_LOGIN_SERVER:=}"

# BURST_TIER selects the in-burst networking shape — see
# pkg/workload/types.go NetworkingSpec for the contract. full is the
# only supported value (and the default when BURST_TIER is unset):
# host-mode cilium-agent runs from the baked binaries and lifts the
# cilium-not-ready taint locally, or (fallback) the kubelet registers
# with the taint for the in-cluster DS to lift. The removed lite tier
# is refused before any provider-adjacent work runs. Central always
# emits BURST_TIER=full (pkg/backends/internal/agentenv), so any
# non-empty non-"full" here is either an out-of-date central or a
# tampered startup-script and must exit clean.
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
# kubelet refuses to run with swap on.
swapoff -a || true

# --- CNI MTU: derive from the primary NIC instead of assuming 1500. See
# gcp/bootstrap.sh for the full rationale (GCP's default VPC MTU is 1460,
# unlike Linode/AWS's 1500 underlays). Cilium's own datapath cares about
# this even more than the bridge floor below does: cilium-agent writes
# its own CNI conflist and does not use cni0 at all once it's up, but a
# wrong MTU baked into its config would silently blackhole the same way. ---
PRIMARY_IFACE=$(ip route show default 2>/dev/null | awk '/^default/ {for (i=1;i<=NF;i++) if ($i=="dev") {print $(i+1); exit}}')
CNI_MTU=$(cat "/sys/class/net/${PRIMARY_IFACE}/mtu" 2>/dev/null || true)
: "${CNI_MTU:=1500}"
echo "[yscale] primary iface=${PRIMARY_IFACE:-unknown} mtu=${CNI_MTU}"

# --- Bridge CNI conflist as a BOOTSTRAP FALLBACK. Named with the `10-`
# prefix so when host-mode cilium-agent (below) writes its own
# `05-cilium.conflist`, the lex-smaller filename takes precedence in
# kubelet's CNI scanner. The bridge conf only ever serves as the
# initial floor so kubelet has a CNI plugin from boot and can register
# the Node — it shouldn't carry real pod traffic once Cilium is up.
# Written before containerd starts below so containerd's very first
# CNI-dir scan already sees it. ---
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

# --- Cilium prerequisites: BPF + cgroup2 + shared root mount. GCP's
# stock Debian usually has both /sys/fs/bpf and the unified cgroup
# hierarchy mounted by systemd, so these are best-effort — they no-op
# on the second mount attempt. Matches gcp/bootstrap.sh. ---
mount -t bpf bpf /sys/fs/bpf 2>/dev/null || true
mkdir -p /run/cilium/cgroupv2
mount -t cgroup2 none /run/cilium/cgroupv2 2>/dev/null || true
mount --make-rshared / 2>/dev/null || true

# --- containerd. The binary is baked (no systemd unit — image/install.sh
# extracts the raw upstream release, same as the AWS baked image), so it
# is started directly here rather than via `systemctl restart containerd`.
# Started BEFORE the tailscale join so its startup rides under the
# multi-second `tailscale up` handshake instead of adding serial time
# after it; the readiness gate sits just before the cilium/kubelet
# section below. ---
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
mkdir -p /run/containerd /var/lib/containerd
echo "[yscale] starting containerd (background)"
containerd >/var/log/containerd.log 2>&1 &

# --- Tailscale: the binary is baked in but left un-configured (no
# systemd unit — same reasoning as containerd above), so tailscaled is
# launched directly against the state/socket paths image/install.sh
# pre-created. ---
echo "[yscale] starting tailscaled"
env -u TS_AUTHKEY tailscaled \
    --state=/var/lib/tailscale/tailscaled.state \
    --socket=/var/run/tailscale/tailscaled.sock &
# Wait for the LocalAPI socket instead of a fixed sleep — it appears in
# well under a second; `tailscale up` fails hard without it.
for _ in $(seq 1 50); do
    [ -S /var/run/tailscale/tailscaled.sock ] && break
    sleep 0.1
done
# Write TS_AUTHKEY to a 600 file and feed via `--auth-key=file:<path>`
# so the key never appears in `ps aux` (visible via /proc/<pid>/cmdline)
# and, combined with `set +x` above, never appears in bootstrap.log
# either. `--reset` clears any state the baked rootfs might otherwise
# carry forward from a prior boot of the same disk image. Shred +
# unset after `tailscale up` consumes it.
install -m 600 /dev/null /run/ts-authkey
printf '%s' "${TS_AUTHKEY}" > /run/ts-authkey
TS_UP_ARGS="--auth-key=file:/run/ts-authkey --hostname=${TS_HOSTNAME} --accept-routes --reset"
if [ -n "${TS_TAGS}" ]; then
  TS_UP_ARGS="${TS_UP_ARGS} --advertise-tags=${TS_TAGS}"
fi
# Advertise this burst's pod CIDR so the customer's cluster nodes
# (which also run tailscale + accept-routes via the agent's subnet
# router DaemonSet) can route pod-to-pod into this burst. The Tailscale
# ACL must autoApprove these advertisements (autoApprovers .routes
# scoped to the burst tag); otherwise the route stays Pending.
if [ -n "${POD_CIDR:-}" ]; then
  TS_UP_ARGS="${TS_UP_ARGS} --advertise-routes=${POD_CIDR}"
fi
if [ -n "${TS_LOGIN_SERVER:-}" ]; then
  TS_UP_ARGS="${TS_UP_ARGS} --login-server=${TS_LOGIN_SERVER}"
fi
# shellcheck disable=SC2086 # word-splitting is intentional: TS_UP_ARGS is built as a space-separated arg list
tailscale up ${TS_UP_ARGS}
shred -u /run/ts-authkey 2>/dev/null || rm -f /run/ts-authkey
unset TS_AUTHKEY
until tailscale status --peers=false >/dev/null 2>&1; do sleep 0.2; done
TS_IP="$(tailscale ip -4)"
echo "[yscale] tailscale up: ${TS_IP}"

# --- fetch the kubelet bootstrap kubeconfig from the agent over the
# tailnet (MagicDNS resolves the agent hostname; --retry rides out the
# agent not being instantly reachable). ---
mkdir -p /etc/kubernetes
BOOT_REQ="$(printf '{"burst_id":"%s","node_name":"%s"}' "${BURST_ID}" "${NODE_NAME}")"
BOOT_RESP="$(curl -fsS -X POST -H 'Content-Type: application/json' \
  --retry 30 --retry-delay 3 --retry-connrefused --retry-all-errors --max-time 120 \
  -d "${BOOT_REQ}" "${BOOTSTRAP_ENDPOINT}/bootstrap-kubeconfig")"
[ -n "${BOOT_RESP}" ] || { echo "[yscale] bootstrap response empty"; exit 1; }
echo "${BOOT_RESP}" | jq -er '.kubeconfig' | base64 -d > /etc/kubernetes/bootstrap-kubeconfig.yaml

# --- HOST-MODE Cilium gate (tier=full only). The agent's bootstrap response
# carries a cilium_kubeconfig when the burst should run cilium-agent ITSELF
# (host process) rather than wait for the in-cluster Cilium DaemonSet — which
# never schedules onto a burst node at all (no cilium-agent in a stock
# debian-12 rootfs to satisfy its nodeAffinity, the bug this image fixes).
# The binaries + eBPF assets are baked (image/install.sh), so host-mode is
# the correct path here: it lifts the cilium-not-ready taint locally. ---
mkdir -p /etc/cilium
HOST_CILIUM=0
if [ "${BURST_TIER}" = "full" ]; then
  if echo "${BOOT_RESP}" | jq -er '.cilium_kubeconfig' >/dev/null 2>&1; then
    echo "${BOOT_RESP}" | jq -er '.cilium_kubeconfig' | base64 -d > /etc/cilium/kubeconfig
    chmod 0600 /etc/cilium/kubeconfig
    HOST_CILIUM=1
    echo "[yscale] tier=full: cilium-kubeconfig received; host-mode Cilium enabled"
  else
    echo "[yscale] tier=full but no cilium-kubeconfig in bootstrap; falling back to DaemonSet Cilium path"
  fi
fi

# containerd readiness gate: it was started before the tailscale
# handshake and has had that whole window to come up, so this is
# normally an instant pass-through.
for _ in $(seq 1 50); do
    [ -S /run/containerd/containerd.sock ] && break
    sleep 0.2
done

# --- HOST-MODE cilium-agent (tier=full + cilium_kubeconfig present).
# Launched as a plain background process (no systemd unit — matching the
# baked binary's lack of one, same as containerd/tailscaled above) BEFORE
# kubelet so it writes 05-cilium.conflist (lex-precedes the bridge 10-*
# floor) and is ready before the node registers — then no
# cilium-not-ready taint is needed and pods schedule the moment the node
# is Ready. ---
if [ "${HOST_CILIUM}" = "1" ] && [ -x /usr/local/bin/cilium-agent ]; then
  echo "[yscale] starting host-mode cilium-agent (baked binary)"
  mkdir -p /var/run/cilium /var/lib/cilium /etc/cilium-config
  # Mirror the kube-system cilium-config ConfigMap into per-key files so the
  # burst's cilium-agent matches the cluster operator's settings (identity
  # allocation, etc.). Best-effort: defaults are sane if the fetch fails.
  CIL_API="$(awk '/server:/ {print $2}' /etc/cilium/kubeconfig | head -1)"
  CIL_TOK="$(awk '/token:/ {print $2}' /etc/cilium/kubeconfig | head -1)"
  CM_JSON="$(curl -sS --insecure -H "Authorization: Bearer ${CIL_TOK}" \
    "${CIL_API}/api/v1/namespaces/kube-system/configmaps/cilium-config" 2>/dev/null || echo '{}')"
  if echo "${CM_JSON}" | jq -e '.data' >/dev/null 2>&1; then
    echo "${CM_JSON}" | jq -r '.data | keys[]' | while IFS= read -r k; do
      echo "${CM_JSON}" | jq -r --arg key "$k" '.data[$key]' > "/etc/cilium-config/$k"
    done
    echo "[yscale] wrote cilium-config keys from ConfigMap"
  else
    echo "[yscale] WARN: cilium-config ConfigMap unavailable; cilium-agent uses defaults"
  fi
  # Host-mode overrides (written after the CM so they win): the customer
  # cluster's kube-proxy handles Service IPs; no BGP/Hubble on a burst.
  # mtu pins cilium's own datapath to the primary NIC's real MTU rather
  # than whatever it autodetects — the same 1460-vs-1500 mismatch that
  # bites the bridge floor above bites cilium's tunnel/veth sizing too.
  echo false > /etc/cilium-config/enable-hubble
  echo false > /etc/cilium-config/enable-bandwidth-manager
  echo false > /etc/cilium-config/enable-bgp-control-plane
  echo "${CNI_MTU}" > /etc/cilium-config/mtu
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
    [ -f /etc/cni/net.d/05-cilium.conflist ] && { echo "[yscale] host-mode cilium ready (${i}s)"; break; }
    sleep 1
  done
  [ -f /etc/cni/net.d/05-cilium.conflist ] || echo "[yscale] WARN: cilium-agent did not write CNI conflist in 60s; check /var/log/cilium-agent.log"
fi

# Kubelet taints/label by HOST_CILIUM — full is the only supported tier
# here (guarded above); mirrors the matrix in linode/bootstrap-baked.sh
# and aws/bootstrap-baked.sh:
#   HOST_CILIUM=1 → host-mode cilium-agent already wrote the CNI conflist
#                   above; register WITHOUT the cilium taint (pods schedule
#                   once Ready) and label cilium-mode=host so the DS skips
#                   this node.
#   HOST_CILIUM=0 → fallback (no cilium_kubeconfig): keep the cilium-not-ready
#                   taint so the DS gates pod scheduling; no cilium-mode label
#                   so the DS WILL schedule here (and never ready — the
#                   original bug — until a cilium_kubeconfig is actually
#                   issued).
if [ "${HOST_CILIUM}" = "1" ]; then
  REGISTER_TAINTS="yscale.sh/burst-node=true:NoSchedule"
  CILIUM_MODE_LABEL=",yscale.sh/cilium-mode=host"
else
  REGISTER_TAINTS="yscale.sh/burst-node=true:NoSchedule,node.cilium.io/agent-not-ready=true:NoSchedule"
  CILIUM_MODE_LABEL=""
fi

# --- Resolve the kubelet providerID --------------------------------------
# A yscale burst is a REAL GCE instance, just not part of any GKE nodepool.
# On a GKE *customer* cluster the GCE CCM reaps any Node whose providerID
# doesn't resolve to a real instance it can query, so register as our
# ACTUAL project/zone/instance-name (gce://<project>/<zone>/<name>): its
# instanceExistsByProviderID then returns true and the Node is left alone.
# On non-GKE clusters (EKS/LKE/AKS/self-managed) the gce:// prefix is
# ignored by their CCMs, so this is harmless there. GCE's metadata server
# only needs the Metadata-Flavor header — no token to mint, unlike Linode's
# or AWS's metadata services. Falls back to a synthetic id if the lookup
# fails (Node may then be reaped on GKE). An explicit PROVIDER_ID env
# always wins.
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
# approver auto-approves it) - without it, apiserver can't reach :10250.
# No Requires=containerd.service: containerd has no systemd unit on this
# baked image (started directly above), so declaring it would only make
# this unit fail to start. ---
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
echo "[yscale] join-only bootstrap done $(date -u)"
