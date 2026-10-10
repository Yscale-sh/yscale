#!/usr/bin/env bash
#
# bake-cpu-image.sh — build the yscale CPU burst private Image end-to-end.
#
# Provisions a throwaway Debian-12 Linode, runs cpu-setup.sh on it via
# cloud-init user-data (installs containerd/kubelet/CNI/tailscale + host-mode
# Cilium binaries + CoreDNS), waits for the prep to finish, powers it off,
# snapshots the disk into a private Image, and prints the "private/<id>" to set
# as cpuBurstImage in linode.go. Then deletes the builder instance.
#
# Idempotent-ish: tags the builder `yscale-image-builder` so a failed run can be
# swept. Reads LINODE_TOKEN from env. SMOKE_PUBKEY (optional) adds an SSH key.
#
# Usage: LINODE_TOKEN=... [REGION=us-ord] [TYPE=g6-standard-2] ./bake-cpu-image.sh
set -euo pipefail
: "${LINODE_TOKEN:?set LINODE_TOKEN}"
apiBase="https://api.linode.com/v4"
REGION="${REGION:-us-ord}"
# A 2-core box bakes fast (cilium extract + apt). Disk size matters: the Image
# captures the whole disk, so a standard plan with ~50GB disk is fine.
TYPE="${TYPE:-g6-standard-2}"
LABEL="yscale-cpu-image-builder-$(date +%s)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
api() { curl -sS -H "Authorization: Bearer $LINODE_TOKEN" "$@"; }
log() { echo "[bake-cpu] $*" >&2; }

# cloud-init: run cpu-setup.sh, then drop a sentinel file. We DETECT COMPLETION
# by SSH-polling for that sentinel — NOT by instance power status, and we do NOT
# poweroff from inside the VM.
#
# WHY (learned the hard way 2026-06-02/03): Linode's "Lassie" shutdown watchdog
# REBOOTS an instance that powers off from inside (unless it was shut down via
# the API). So an in-VM `/sbin/poweroff` makes the box flicker offline then come
# right back "running" — which (a) defeats any "wait for offline = done" poll
# (it catches the boot-time transient or the Lassie-rebooted box) and (b) leaves
# the builder running. Result seen: false-positive completion at ~120s, a
# "Linode busy" resize rejection, and a builder left up. The robust signal is
# the sentinel over SSH (a connect timeout just means "not ready, keep
# polling"); once seen, we shut the box down via the API (Lassie does not fight
# an API shutdown) and only then resize + imagize.
#
# SSH is therefore REQUIRED: set SMOKE_PUBKEY to a public key whose private half
# is at SSH_PRIVKEY (default ~/.ssh/id_rsa).
SMOKE_PUBKEY="${SMOKE_PUBKEY:?set SMOKE_PUBKEY (public key) — needed to SSH-poll the build sentinel}"
SSH_PRIVKEY="${SSH_PRIVKEY:-$HOME/.ssh/id_rsa}"
[ -f "$SSH_PRIVKEY" ] || { log "FATAL: SSH_PRIVKEY not found at $SSH_PRIVKEY (set SSH_PRIVKEY to the private key matching SMOKE_PUBKEY)"; exit 1; }
bssh() { ssh -i "$SSH_PRIVKEY" -o StrictHostKeyChecking=no -o ConnectTimeout=8 \
         -o BatchMode=yes -o UserKnownHostsFile=/dev/null root@"$IP" "$@"; }

USERDATA="$(cat "$SCRIPT_DIR/cpu-setup.sh"; printf '\ntouch /var/log/yscale-cpu-setup.done\nsync\n')"
USERDATA_B64="$(printf '%s' "$USERDATA" | base64 | tr -d '\n')"

AUTH_KEYS_JSON="$(printf '"authorized_keys": [%s],' "$(printf '%s' "$SMOKE_PUBKEY" | jq -R .)")"
PASS="$(LC_ALL=C tr -dc 'A-Za-z0-9' < <(head -c 256 /dev/urandom) | cut -c1-32)"

log "creating builder $LABEL ($TYPE, $REGION)..."
RESP="$(api -X POST -H 'Content-Type: application/json' -d "{
  \"label\": \"$LABEL\",
  \"region\": \"$REGION\",
  \"type\": \"$TYPE\",
  \"image\": \"linode/debian12\",
  \"root_pass\": \"$PASS\",
  ${AUTH_KEYS_JSON}
  \"tags\": [\"yscale-image-builder\"],
  \"metadata\": {\"user_data\": \"$USERDATA_B64\"}
}" "$apiBase/linode/instances")"
LID="$(echo "$RESP" | jq -r '.id')"
[ "$LID" != "null" ] && [ -n "$LID" ] || { log "FATAL: create failed: $RESP"; exit 1; }
log "builder id=$LID — waiting for running + cpu-setup (~5-8 min)..."

# Wait for the box to reach "running" — cloud-init (and thus cpu-setup) only
# starts after that. CRITICAL: a fresh instance briefly reports "offline" during
# the provisioning→boot transition BEFORE cloud-init runs; if we polled for
# "offline" directly we'd catch that false-positive and imagize an empty box
# (hit 2026-06-01). So we require "running" FIRST, then wait for the box to go
# back to "offline" (the final `poweroff` cpu-setup runs on success).
for i in $(seq 1 40); do
  sleep 10
  st="$(api "$apiBase/linode/instances/$LID" | jq -r '.status')"
  [ "$st" = "running" ] && break
  log "  t=${i}0s status=$st (awaiting running)"
done
IP="$(api "$apiBase/linode/instances/$LID" | jq -r '.ipv4[0]')"
log "builder running: id=$LID ip=$IP — SSH-polling for cpu-setup sentinel..."

# Wait for cpu-setup to finish by SSH-polling for the sentinel file. A failed
# SSH (connect timeout / refused while apt+ctr load the box) is NOT a failure —
# just "not ready yet", keep polling. cpu-setup is heavy (apt + ~250MB cilium
# extract); allow ~25 min. If cpu-setup itself errored (set -euxo pipefail) the
# sentinel never appears → we time out and leave the box up for inspection.
DONE=0
for i in $(seq 1 100); do
  sleep 15
  if bssh 'test -f /var/log/yscale-cpu-setup.done' 2>/dev/null; then
    DONE=1; log "cpu-setup sentinel present (~$((i*15))s)"; break
  fi
  [ $((i % 8)) -eq 0 ] && log "  t=$((i*15))s (sentinel not yet present; build in progress)"
done
[ "$DONE" = "1" ] || { log "FATAL: cpu-setup did not finish in ~25min; builder $LID left up for inspection (ssh root@$IP 'tail /var/log/yscale-cpu-setup.log')"; exit 1; }

# Shut down via the API (NOT in-VM poweroff — Lassie would reboot it). Then poll
# for genuine offline before any disk op, else the resize gets "Linode busy."
log "cpu-setup done — API shutdown $LID..."
api -X POST "$apiBase/linode/instances/$LID/shutdown" >/dev/null
DOWN=0
for i in $(seq 1 30); do
  sleep 6
  st="$(api "$apiBase/linode/instances/$LID" | jq -r '.status')"
  if [ "$st" = "offline" ]; then DOWN=1; log "builder offline (~$((i*6))s)"; break; fi
done
[ "$DOWN" = "1" ] || { log "FATAL: builder $LID did not go offline after API shutdown"; exit 1; }

# Find the root disk id.
DISK_ID="$(api "$apiBase/linode/instances/$LID/disks" | jq -r '.data[] | select(.filesystem=="ext4") | .id' | head -1)"
[ -n "$DISK_ID" ] || { log "FATAL: no ext4 disk found on $LID"; exit 1; }

# CRITICAL: shrink the disk before imagizing. A stock Linode instance auto-fills
# its plan (~80GB on g6-standard-2), and `POST /images` from a disk_id captures
# the WHOLE disk size — producing an 81GB image (expensive to store, slow to
# deploy per burst) even though the actual install is only ~3-4GB. Resizing the
# offline disk down to IMG_DISK_MB first makes the image that size instead. The
# GPU image (5.5GB) was built this way. Linode allows shrinking only if used
# space fits; 6GB comfortably holds Debian + containerd + kubelet + cilium.
IMG_DISK_MB="${IMG_DISK_MB:-6144}"
USED_MB="$(api "$apiBase/linode/instances/$LID/disks/$DISK_ID" | jq -r '.size')"
log "root disk $DISK_ID is ${USED_MB}MB; resizing down to ${IMG_DISK_MB}MB before imagize..."
RZ="$(api -X POST -H 'Content-Type: application/json' \
  -d "{\"size\": ${IMG_DISK_MB}}" \
  "$apiBase/linode/instances/$LID/disks/$DISK_ID/resize")"
if echo "$RZ" | jq -e '.errors' >/dev/null 2>&1; then
  log "FATAL: disk resize rejected: $RZ (used space may exceed ${IMG_DISK_MB}MB; raise IMG_DISK_MB)"
  exit 1
fi
# Resize is async (the instance must be offline, which it is). Poll until the
# disk reports the new size + status=ready.
for i in $(seq 1 60); do
  sleep 5
  DS="$(api "$apiBase/linode/instances/$LID/disks/$DISK_ID")"
  cur="$(echo "$DS" | jq -r '.size')"; dst="$(echo "$DS" | jq -r '.status')"
  [ "$cur" = "$IMG_DISK_MB" ] && [ "$dst" = "ready" ] && { log "disk resized to ${IMG_DISK_MB}MB (${i}x5s)"; break; }
done
cur="$(api "$apiBase/linode/instances/$LID/disks/$DISK_ID" | jq -r '.size')"
[ "$cur" = "$IMG_DISK_MB" ] || { log "FATAL: disk did not resize to ${IMG_DISK_MB}MB (now ${cur}MB)"; exit 1; }

log "snapshotting disk $DISK_ID into a private Image (~${IMG_DISK_MB}MB)..."
IMG_RESP="$(api -X POST -H 'Content-Type: application/json' \
  -d "{\"disk_id\": $DISK_ID, \"label\": \"yscale-cpu-burst-$(date +%Y%m%d)\", \"description\": \"yscale CPU burst: containerd+kubelet+host-mode cilium\"}" \
  "$apiBase/images")"
IMG_ID="$(echo "$IMG_RESP" | jq -r '.id')"
[ "$IMG_ID" != "null" ] && [ -n "$IMG_ID" ] || { log "FATAL: image create failed: $IMG_RESP"; exit 1; }
log "Image creating: $IMG_ID (Linode processes the snapshot async, ~a few min)"

# CRITICAL: wait for the image to reach status=available BEFORE deleting the
# builder. The imagize is async and CAN hang in "creating" indefinitely
# (observed 2026-06-02: an image stuck "creating" for 25h). If we deleted the
# builder first (as the old script did), the source disk would be gone and a
# hung imagize is unrecoverable — you'd have to re-bake from scratch. By keeping
# the builder until the image is confirmed available, a hung imagize can be
# retried from the same disk. ~6GB image typically goes available in a few min.
IMG_OK=0
for i in $(seq 1 60); do
  sleep 15
  ist="$(api "$apiBase/images/$IMG_ID" | jq -r '.status')"
  if [ "$ist" = "available" ]; then IMG_OK=1; log "image available (~$((i*15))s)"; break; fi
  [ $((i % 4)) -eq 0 ] && log "  image t=$((i*15))s status=$ist"
done
if [ "$IMG_OK" != "1" ]; then
  log "FATAL: image $IMG_ID did not reach 'available' in ~15min (status=$ist)."
  log "  Builder $LID (disk $DISK_ID) LEFT UP for retry. Re-imagize with:"
  log "  curl -X POST -H \"Authorization: Bearer \$LINODE_TOKEN\" -H 'Content-Type: application/json' \\"
  log "    -d '{\"disk_id\": $DISK_ID, \"label\": \"yscale-cpu-burst-retry\"}' $apiBase/images"
  log "  Then delete builder $LID when done. (Old stuck image: delete via DELETE $apiBase/images/$IMG_ID)"
  exit 1
fi

# Image confirmed available — now safe to delete the builder.
log "deleting builder instance $LID..."
api -X DELETE "$apiBase/linode/instances/$LID" >/dev/null

echo "$IMG_ID"
log "DONE. Set cpuBurstImage = \"$IMG_ID\" in pkg/backends/linode/linode.go, rebuild cloud image."
