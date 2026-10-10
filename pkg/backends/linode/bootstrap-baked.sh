#!/bin/bash
#
# yscale Linode burst bootstrap — JOIN-ONLY variant for baked images.
#
# Run by cloud-init on a burst booting from a yscale baked Image — one
# that already has containerd (CRI-configured), kubelet/kubectl, CNI
# plugins, and tailscale installed (e.g. the GPU image, which also
# carries the NVIDIA driver + container-toolkit). This script does ONLY
# the per-burst join; the full-install variant is bootstrap.sh, used
# for stock Debian 12.
#
# Per-burst values arrive as shell exports CreateNode prepends ahead of
# this body.
set -euo pipefail
# 'x' (xtrace) intentionally OMITTED: with xtrace on, the
# `tailscale up --authkey=...` line below would be written verbatim
# into bootstrap.log — a reusable tailnet credential persisted on
# disk for the burst's lifetime. Use `bash -x` interactively to debug.

# ERR trap: if the script bombs, write a one-line failure summary to
# the serial console (and a sentinel file for cloud-init to surface)
# BEFORE redirecting stdout/stderr to the log file. Without this, a
# cloud-final failure shows up on the Linode lish console as opaque
# "[FAILED] Execute cloud user/final scripts" — useless for triage.
# With this, the console line tells you WHICH step died.
trap 'EC=$?; echo "[yscale] BOOTSTRAP FAILED at line $LINENO (exit=$EC); see /var/log/yscale-bootstrap.log on the host" > /dev/console 2>/dev/null; echo "FAILED line=$LINENO exit=$EC" > /var/log/yscale-bootstrap.FAILED; exit $EC' ERR

exec > /var/log/yscale-bootstrap.log 2>&1
echo "[yscale] join-only bootstrap starting $(date -u)"

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

# TS_TAGS and EXTRA_NODE_LABELS are exported (maybe empty) by CreateNode.
: "${TS_TAGS:=}"
: "${EXTRA_NODE_LABELS:=}"
# TS_LOGIN_SERVER points tailscale at a per-customer self-hosted coordination
# server; empty = default Tailscale SaaS. CreateNode stamps it from NodeSpec.LoginServer.
: "${TS_LOGIN_SERVER:=}"

# BURST_TIER selects the in-burst networking shape — see
# pkg/workload/types.go NetworkingSpec for the contract. full is the
# only supported value (and the default when BURST_TIER is unset):
# either host-mode cilium-agent runs from the baked binaries and lifts
# the cilium-not-ready taint locally, or the kubelet registers with the
# taint and the in-cluster Cilium DaemonSet is expected to lift it.
# The removed lite tier (no in-burst Cilium at all, cilium-mode=lite
# label) is refused before any provider-adjacent work runs. Central
# always emits BURST_TIER=full (pkg/backends/internal/agentenv), so any
# non-empty non-"full" here is either an out-of-date central or a
# tampered cloud-init and must exit clean.
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

# Linode provisions a swap disk by default; kubelet refuses to run with
# swap on. Disable it (the burst is ephemeral, no fstab edit).
swapoff -a || true

# --- TCP tuning for the high-BDP cross-WAN mesh path. Burst↔cluster data
# is pod-to-pod TCP carried over a WireGuard tunnel at ~32ms RTT; the
# stock cubic + 208KB buffers collapse on the path's intermittent loss,
# capping single-flow throughput (~330 Mbps measured, 143 retransmits in a
# 10s run). BBR is rate-based (ignores loss) and fills long-fat-network
# paths; fq is its companion qdisc. Bigger rmem/wmem raise the window
# ceiling for the BDP. The burst is the SENDER for segment-write (upload)
# and the RECEIVER for source-fetch, so it wants both. Best-effort: no-op
# if the bbr module is absent (older kernels). ---
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
echo "[yscale] TCP cc=$(sysctl -n net.ipv4.tcp_congestion_control 2>/dev/null) qdisc=$(sysctl -n net.core.default_qdisc 2>/dev/null)"

# NFS client helper: pods that mount an NFS source volume (e.g. the sharded
# transcode worker reading media over the mesh) need mount.nfs on the host or
# the kubelet volume mount fails with exit 32. It's baked into the image
# (gpu/cpu-setup.sh); install at boot as a fallback for images baked before
# that change so existing baked images still work.
command -v mount.nfs >/dev/null 2>&1 || { apt-get update -qq && apt-get install -y -qq nfs-common; } || true

# --- Pre-seeded model cache volume. When the workload requested a model
# volume (Spec.ModelVolume), CreateNode stamps MODEL_VOLUME and attaches a
# Block Storage volume to this burst; it shows up as a SCSI block device
# /dev/disk/by-id/scsi-0Linode_Volume_*. Mount it read-only at
# /mnt/model-cache so the workload pod (which gets a hostPath volume there
# from ToJob) sees pre-downloaded model weights and skips a multi-GB
# HuggingFace download. Gated on MODEL_VOLUME so non-model bursts skip this
# entirely and pay no device-wait. `|| true` on the glob keeps `set -e`
# from tripping when no device matches yet.
: "${MODEL_VOLUME:=}"
if [ -n "${MODEL_VOLUME}" ]; then
  MODEL_DEV=""
  for _i in $(seq 1 30); do
    MODEL_DEV="$(ls /dev/disk/by-id/scsi-0Linode_Volume_* 2>/dev/null | head -1 || true)"
    [ -n "${MODEL_DEV}" ] && break
    sleep 2
  done
  if [ -n "${MODEL_DEV}" ]; then
    mkdir -p /mnt/model-cache
    if mount -o ro "${MODEL_DEV}" /mnt/model-cache 2>/dev/null; then
      echo "[yscale] mounted model cache (${MODEL_VOLUME}) ${MODEL_DEV} -> /mnt/model-cache (ro)"
    else
      echo "[yscale] WARN: model-cache device ${MODEL_DEV} present but mount failed"
    fi
  else
    echo "[yscale] WARN: MODEL_VOLUME=${MODEL_VOLUME} requested but no volume device appeared"
  fi
fi

# --- Bridge CNI conflist as a BOOTSTRAP FALLBACK. Named with the `10-`
# prefix so when Cilium's DaemonSet pod (cluster-wide, pinned to burst
# nodes via nodeSelector yscale.sh/burst-node=true) writes its own
# `05-cilium.conflist`, the lex-smaller filename takes precedence in
# kubelet's CNI scanner. The bridge conf only ever serves as the
# initial floor so kubelet has a CNI plugin from boot and can register
# the Node — it shouldn't carry real pod traffic once Cilium is up.
# POD_CIDR is per-burst, so the conflist is written at boot even though
# the CNI plugin binaries are baked in. Matches burst/image/entrypoint-
# kubelet.sh on Fly. Written HERE — before the tailscale join — because
# it needs only POD_CIDR, so the containerd restart below can overlap
# the multi-second tailscale handshake instead of running after it. ---
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
# Linode's baked image already mounts /sys/fs/bpf. Mounting it again creates
# stacked mount points that Cilium rejects, so only mount when absent.
mountpoint -q /sys/fs/bpf || mount -t bpf bpf /sys/fs/bpf 2>/dev/null || true
mkdir -p /run/cilium/cgroupv2
mount -t cgroup2 none /run/cilium/cgroupv2 2>/dev/null || true
mount --make-rshared / 2>/dev/null || true

# containerd + its CRI config are baked into the image; restart so it
# re-scans the CNI conf dir now that the conflist exists. --no-block:
# the restart proceeds while tailscale joins below. Everything that
# needs containerd (yscale-cilium, yscale-kubelet) declares
# Requires/After=containerd.service, so systemd still orders their
# starts after the restart completes.
systemctl --no-block restart containerd

# --- Tailscale: the binary is baked in (and left disabled by the bake);
# bring it up per-burst (kernel mode — the Linode VM is dedicated to
# this burst, so no host-tailscaled conflict). ---
systemctl enable --now tailscaled
# Write TS_AUTHKEY to a 600 file and feed via `--auth-key=file:<path>`
# so the key never appears in `ps aux` (visible via /proc/<pid>/cmdline)
# and, combined with `set +x` above, never appears in bootstrap.log
# either. `--auth-key-file=...` is NOT a valid tailscale flag — it
# crashes `tailscale up` with "flag provided but not defined" (the
# baked Linode image's tailscale binary confirmed this failure mode
# 2026-05-25). The `file:` prefix on --auth-key IS the documented form
# (per `tailscale up --help`).
# Shred + unset after `tailscale up` consumes it.
install -m 600 /dev/null /run/ts-authkey
printf '%s' "${TS_AUTHKEY}" > /run/ts-authkey
TS_UP_ARGS="--auth-key=file:/run/ts-authkey --hostname=${TS_HOSTNAME} --accept-routes"
# Per-customer coordination server: aim tailscale at that box. WITHOUT
# this, a burst minted a preauth key for that server joins default
# Tailscale SaaS instead and fails with "invalid key: unable to validate API
# key" — the self-hosted join silently broke on Linode bursts (Fly's entrypoint
# already passes this). Empty TS_LOGIN_SERVER = SaaS, unchanged.
if [ -n "${TS_LOGIN_SERVER}" ]; then
  TS_UP_ARGS="${TS_UP_ARGS} --login-server=${TS_LOGIN_SERVER}"
fi
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
# shellcheck disable=SC2086 # word-splitting is intentional: TS_UP_ARGS is built as a space-separated arg list
tailscale up ${TS_UP_ARGS}
shred -u /run/ts-authkey 2>/dev/null || rm -f /run/ts-authkey
unset TS_AUTHKEY
TS_IP="$(tailscale ip -4)"
echo "[yscale] tailscale up: ${TS_IP}"

# --- fetch the kubelet bootstrap kubeconfig from the agent over the
# tailnet (MagicDNS resolves the agent hostname; --retry rides out the
# agent not being instantly reachable). retry-delay 1 (not 3): the
# announce normally lands at the agent long before the VM boots, so a
# refused first attempt is a fast-retry case; 90 retries keeps the
# same ~90s worst-case coverage the old 30x3 had. ---
mkdir -p /etc/kubernetes
BOOT_REQ="$(printf '{"burst_id":"%s","node_name":"%s"}' "${BURST_ID}" "${NODE_NAME}")"
BOOT_RESP="$(curl -fsS -X POST -H 'Content-Type: application/json' \
  --retry 90 --retry-delay 1 --retry-connrefused --retry-all-errors --max-time 120 \
  -d "${BOOT_REQ}" "${BOOTSTRAP_ENDPOINT}/bootstrap-kubeconfig")"
[ -n "${BOOT_RESP}" ] || { echo "[yscale] bootstrap response empty"; exit 1; }
echo "${BOOT_RESP}" | jq -er '.kubeconfig' | base64 -d > /etc/kubernetes/bootstrap-kubeconfig.yaml

# Customer cluster's cloud provider (if the agent plumbed it). Used to
# adjust providerID: GKE ValidatingAdmissionPolicy rejects providerIDs
# that don't end with /<node-name>, so GKE bursts use linode://<NODE_NAME>.
CLUSTER_CLOUD="$(echo "${BOOT_RESP}" | jq -r '.cloud_provider // empty' 2>/dev/null || true)"

# EKS does not use the client certificate issued by the Kubernetes kubelet
# signer for ongoing node authentication. Its HYBRID_LINUX access entry maps
# the assumed-role session directly to system:node:<name>, so keep using fresh
# IAM authenticator credentials without putting AWS credentials on this VM.
# The exec helper calls the already-authenticated tailnet bootstrap endpoint;
# client-go caches each returned token until the advertised expiry.
BOOTSTRAP_TOKEN="$(awk '/^[[:space:]]*token:/ {print $2; exit}' /etc/kubernetes/bootstrap-kubeconfig.yaml)"
if [[ "${BOOTSTRAP_TOKEN}" == k8s-aws-v1.* ]]; then
  install -m 0700 /dev/null /usr/local/bin/yscale-eks-credential
  cat > /usr/local/bin/yscale-eks-credential <<'EXECEOF'
#!/bin/bash
set -euo pipefail
endpoint="$1"; burst_id="$2"; node_name="$3"
body="$(printf '{"burst_id":"%s","node_name":"%s"}' "$burst_id" "$node_name")"
response="$(curl -fsS -X POST -H 'Content-Type: application/json' --max-time 30 \
  -d "$body" "$endpoint/bootstrap-kubeconfig")"
token="$(printf '%s' "$response" | jq -er '.kubeconfig' | base64 -d | \
  awk '/^[[:space:]]*token:/ {print $2; exit}')"
expires="$(date -u -d '+10 minutes' '+%Y-%m-%dT%H:%M:%SZ')"
printf '{"apiVersion":"client.authentication.k8s.io/v1beta1","kind":"ExecCredential","status":{"expirationTimestamp":"%s","token":"%s"}}\n' \
  "$expires" "$token"
EXECEOF
  chmod 0700 /usr/local/bin/yscale-eks-credential

  ca_data="$(awk '/certificate-authority-data:/ {print $2; exit}' /etc/kubernetes/bootstrap-kubeconfig.yaml)"
  api_server="$(awk '/server:/ {print $2; exit}' /etc/kubernetes/bootstrap-kubeconfig.yaml)"
  cat > /etc/kubernetes/kubelet.kubeconfig <<KUBECONFIGEOF
apiVersion: v1
kind: Config
clusters:
- name: yscale
  cluster:
    server: ${api_server}
    certificate-authority-data: ${ca_data}
contexts:
- name: default-context
  context:
    cluster: yscale
    user: yscale-eks-node
current-context: default-context
users:
- name: yscale-eks-node
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      command: /usr/local/bin/yscale-eks-credential
      args:
      - ${BOOTSTRAP_ENDPOINT}
      - ${BURST_ID}
      - ${NODE_NAME}
      interactiveMode: Never
KUBECONFIGEOF
  chmod 0600 /etc/kubernetes/kubelet.kubeconfig
  echo "[yscale] EKS hybrid IAM credential renewal enabled"
fi

# --- HOST-MODE Cilium gate (tier=full only). The agent's bootstrap response
# carries a cilium_kubeconfig when the burst should run cilium-agent ITSELF
# (host process) rather than wait for the in-cluster Cilium DaemonSet. On a
# baked image the cilium binaries + eBPF assets are already on the rootfs
# (cpu-setup.sh / gpu-setup.sh), so host-mode is the correct path: it lifts the
# cilium-not-ready taint locally instead of depending on a DS pod that, on a
# Linode burst, crashloops in init and never readies (the bug this fixes).
# Mirrors burst/image/entrypoint-kubelet.sh on Fly. ---
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

# Gateway routing: see burst/image/entrypoint-kubelet.sh for the full
# rationale. tl;dr: gateway advertises customer pod CIDRs via
# Tailscale subnet routing, this burst's --accept-routes picks them
# up. Nothing to do here. (CNI conflist + containerd restart moved
# above the tailscale join so the restart overlaps the handshake.)
:

# --- HOST-MODE cilium-agent (tier=full + cilium_kubeconfig present). Launch
# cilium-agent as a host process BEFORE kubelet so it writes the CNI conflist
# (05-cilium.conflist, lex-precedes the bridge 10-* floor) and is ready before
# the node registers — then NO cilium-not-ready taint is needed and pods
# schedule the moment the node is Ready. Binaries + bpf/ sources are baked
# (cpu-setup.sh). This is the Linode port of the Fly entrypoint host-cilium
# block; without it a Linode full-tier burst depends on the in-cluster Cilium
# DaemonSet, which crashloops in init on the burst and never lifts the taint. ---
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
  echo false > /etc/cilium-config/enable-hubble
  echo false > /etc/cilium-config/enable-bandwidth-manager
  echo false > /etc/cilium-config/enable-bgp-control-plane
  cat > /etc/systemd/system/yscale-cilium.service <<CILUNIT
[Unit]
Description=yscale host-mode cilium-agent
After=containerd.service tailscaled.service
Requires=containerd.service

[Service]
ExecStart=/usr/local/bin/cilium-agent \\
  --k8s-kubeconfig-path=/etc/cilium/kubeconfig \\
  --k8s-namespace=kube-system \\
  --config-dir=/etc/cilium-config \\
  --state-dir=/var/run/cilium \\
  --write-cni-conf-when-ready=/etc/cni/net.d/05-cilium.conflist
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
CILUNIT
  systemctl daemon-reload
  systemctl enable --now yscale-cilium.service
  # Wait up to 120s for cilium-agent to write its CNI conflist (the ready
  # signal). Linode full-tier boot is slower than Fly; give it room.
  for i in $(seq 1 120); do
    [ -f /etc/cni/net.d/05-cilium.conflist ] && { echo "[yscale] host-mode cilium ready (${i}s)"; break; }
    sleep 1
  done
  [ -f /etc/cni/net.d/05-cilium.conflist ] || echo "[yscale] WARN: cilium-agent did not write CNI conflist in 120s; check journalctl -u yscale-cilium"
fi

# --- Burst-local CoreDNS resolver. See burst/image/entrypoint-kubelet.sh
# "DNS routing on the burst" comment for the architectural rationale: a
# burst without kube-proxy or its Cilium replacement translating the
# customer-cluster CoreDNS ClusterIP cannot reach it. A CoreDNS bound to
# the cni0 bridge gateway makes pod DNS work regardless of cluster state. ---
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

if [ -x /usr/local/bin/coredns ]; then
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
  # Point kubelet at burst-local CoreDNS — override caller-provided value.
  CLUSTER_DNS="${POD_CIDR_GW}"
else
  echo "[yscale] WARN: /usr/local/bin/coredns missing on baked image; keeping CLUSTER_DNS=${CLUSTER_DNS}"
fi

# --- NVIDIA device plugin (static pod). The driver + container-toolkit
# are baked in, but Kubernetes only learns the node has a GPU once the
# device plugin advertises `nvidia.com/gpu` to the kubelet. It is a pod,
# not a host package, so it cannot be baked into the image — the kubelet
# runs it as a static pod from --pod-manifest-path. Gated on the GPU
# actually being present, so a non-GPU baked image skips it. ---
mkdir -p /etc/kubernetes/manifests
if command -v nvidia-smi >/dev/null 2>&1; then
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
fi

# Kubelet taints/label by HOST_CILIUM (tier=full is the only value that
# reaches here — see the tier gate above; mirrors the matrix in
# burst/image/entrypoint-kubelet.sh on Fly):
#   HOST_CILIUM=1 → host-mode cilium-agent already wrote the CNI conflist
#                   above; register WITHOUT the cilium taint (pods schedule
#                   once Ready) and label cilium-mode=host so the in-cluster
#                   DS skips this node.
#   HOST_CILIUM=0 → fallback (no cilium_kubeconfig): keep the cilium-not-ready
#                   taint so the DS gates pod scheduling; no cilium-mode label
#                   so the DS WILL schedule here.
if [ "${HOST_CILIUM}" = "1" ]; then
  REGISTER_TAINTS="yscale.sh/burst-node=true:NoSchedule"
  CILIUM_MODE_LABEL=",yscale.sh/cilium-mode=host"
else
  REGISTER_TAINTS="yscale.sh/burst-node=true:NoSchedule,node.cilium.io/agent-not-ready=true:NoSchedule"
  CILIUM_MODE_LABEL=""
fi

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

# --- Resolve the kubelet providerID --------------------------------------
# Strategy depends on the CUSTOMER cluster type (CLUSTER_CLOUD, plumbed by
# the agent in the bootstrap response):
#
#   gcp (GKE)   — GKE ValidatingAdmissionPolicy (validate-node-providerid)
#                 rejects providerIDs that don't end with /<node-name>.
#                 linode://<NODE_NAME> passes admission; the upstream GCE
#                 cloud-node-lifecycle controller retains nodes whose
#                 provider lookup errors out. Unconditional override: a
#                 preseeded numeric Linode ID would be rejected by admission.
#
#   linode (LKE)— LKE CCM reaps any Node whose providerID doesn't resolve
#                 to a REAL Linode instance. Self-fetch the burst's actual
#                 instance id from the Linode Metadata Service.
#
#   others      — EKS/AKS CCMs act only on their own prefix and ignore
#                 a linode:// node. Self-fetch from metadata (safe).
#
# An explicit PROVIDER_ID env always wins EXCEPT on GKE, where admission
# rejects providerIDs that don't end with /<node-name>.
if [ "${CLUSTER_CLOUD}" = "gcp" ]; then
  PROVIDER_ID="linode://${NODE_NAME}"
  echo "[bootstrap] GKE cluster: providerID=${PROVIDER_ID} (admission requires /<node-name> suffix)"
elif [ -z "${PROVIDER_ID:-}" ]; then
  # --retry: the Metadata Service can be slow to answer right after boot; a
  # missed lookup silently drops to the synthetic id below (reap risk on LKE).
  md_token="$(curl -sf -m 5 --retry 3 --retry-connrefused --retry-delay 2 -X PUT -H 'Metadata-Token-Expiry-Seconds: 3600' http://169.254.169.254/v1/token 2>/dev/null || true)"
  linode_id=""
  if [ -n "$md_token" ]; then
    linode_id="$(curl -sf -m 5 --retry 3 --retry-connrefused --retry-delay 2 -H "Metadata-Token: $md_token" http://169.254.169.254/v1/instance 2>/dev/null | grep -E '^id:' | head -1 | grep -oE '[0-9]+' || true)"
  fi
  if [ -n "$linode_id" ]; then
    PROVIDER_ID="linode://${linode_id}"
    echo "[bootstrap] providerID=${PROVIDER_ID} (real Linode instance id — survives LKE CCM)"
  else
    PROVIDER_ID="linode://yscale-burst-${BURST_ID#burst_}"
    echo "[bootstrap] WARN: Linode Metadata id lookup failed; using synthetic providerID ${PROVIDER_ID} (Node may be reaped on LKE customer clusters)"
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
  ${PROVIDER_ID:+--provider-id=${PROVIDER_ID}} \\
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

# --- GPU telemetry reporter (systemd-managed, best-effort). Reports
# max utilization across all GPUs to the agent every 30s. Only when
# nvidia-smi is present and the agent bootstrap endpoint is reachable.
# Does not block kubelet startup. ---
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
  echo "[yscale] GPU telemetry reporter service installed"
fi

systemctl daemon-reload
systemctl enable --now yscale-kubelet.service
if [ -f /etc/systemd/system/yscale-gpu-telemetry.service ]; then
  systemctl enable --now yscale-gpu-telemetry.service
fi
echo "[yscale] join-only bootstrap done $(date -u)"
