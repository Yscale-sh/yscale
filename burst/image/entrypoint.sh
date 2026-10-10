#!/bin/sh
set -e

# Ensure tun device exists for kernel-mode Tailscale networking.
mkdir -p /dev/net
if [ ! -c /dev/net/tun ]; then
  mknod /dev/net/tun c 10 200
  chmod 600 /dev/net/tun
fi

echo "[yscale-agent] starting tailscale (tun mode)..."
# `env -u TS_AUTHKEY` scrubs the key from tailscaled's inherited env so
# it's not readable via /proc/<pid>/environ for the burst's lifetime.
# `tailscale up` below still gets it (via --auth-key-file).
env -u TS_AUTHKEY tailscaled --state=/var/lib/tailscale/tailscaled.state --socket=/var/run/tailscale/tailscaled.sock &
sleep 2

# Write TS_AUTHKEY to a 600 file and feed via --auth-key-file so the
# key never appears in `ps aux` (visible via /proc/<pid>/cmdline) and
# is not inherited into the k3s-agent process env. Shred + unset after
# `tailscale up` consumes it.
install -m 600 /dev/null /run/ts-authkey
printf '%s' "${TS_AUTHKEY}" > /run/ts-authkey
tailscale up \
  --auth-key-file=/run/ts-authkey \
  --hostname="${TS_HOSTNAME}" \
  --accept-routes \
  --reset
shred -u /run/ts-authkey 2>/dev/null || rm -f /run/ts-authkey
unset TS_AUTHKEY

echo "[yscale-agent] tailscale connected, waiting for network..."
until tailscale status --peers=false > /dev/null 2>&1; do
  sleep 1
done

TS_IP=$(tailscale ip -4)
TS_IFACE=$(ip -o link show | grep tailscale | awk -F: '{print $2}' | tr -d ' ')
echo "[yscale-agent] tailscale is up: ${TS_IP} (iface: ${TS_IFACE})"

# Build K3s agent args.
# --node-ip: use Tailscale IP so cluster can reach this node
# --flannel-iface: route flannel traffic through Tailscale
# --snapshotter: use fuse-overlayfs (overlayfs not supported on Fly)
# --node-taint: opt-in pattern — only pods that explicitly tolerate this
#               taint land on burst nodes. Matches kubelet-mode behavior so
#               accidental scheduling on paid burst capacity can't happen.
K3S_ARGS="agent \
  --server=${K3S_URL} \
  --token=${K3S_TOKEN} \
  --node-name=${K3S_NODE_NAME} \
  --node-ip=${TS_IP} \
  --node-external-ip=${TS_IP} \
  --flannel-iface=${TS_IFACE:-tailscale0} \
  --snapshotter=fuse-overlayfs \
  --prefer-bundled-bin \
  --lb-server-port=0 \
  --node-taint=yscale.sh/burst-node=true:NoSchedule"

# Apply node labels if set.
if [ -n "${K3S_NODE_LABELS}" ]; then
  IFS=','
  for label in ${K3S_NODE_LABELS}; do
    K3S_ARGS="${K3S_ARGS} --node-label=${label}"
  done
  unset IFS
fi

echo "[yscale-agent] starting k3s agent: ${K3S_NODE_NAME} -> ${K3S_URL}"
# shellcheck disable=SC2086 # word-splitting is intentional: K3S_ARGS is built as a space-separated arg list
exec k3s ${K3S_ARGS}
