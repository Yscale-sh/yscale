#!/usr/bin/env bash
# build-image.sh — build a pre-baked yscale burst GCE image.
#
# Flow:
#   1. Resolve the newest debian-12 family image (debian-cloud project).
#   2. Launch a builder instance with install.sh as the `startup-script`
#      metadata key.
#   3. install.sh installs everything, strips, then `shutdown -h now`.
#   4. A guest-initiated shutdown leaves the instance TERMINATED; wait
#      for it.
#   5. images.insert (`gcloud compute images create`) from the builder's
#      boot disk. GCE images are GLOBAL — unlike an AWS AMI (per-region)
#      or a Linode private Image, there is no replication step here at
#      all: this one call is the entire story.
#   6. Delete the builder instance.
#   7. Write the image reference to pkg/backends/gcp/image/image-name.
#
# Run locally from a machine with `gcloud auth login` (or Application
# Default Credentials) set up for the target project. Total wall-clock:
# ~10-15 min (install.sh dominates).
#
# Usage:
#   ./pkg/backends/gcp/image/build-image.sh --project my-gcp-project
#                                           [--zone us-central1-a]
#                                           [--machine-type e2-small]
#                                           [--disk-size 10]

set -euo pipefail

PROJECT=""
ZONE="us-central1-a"
MACHINE_TYPE="e2-small"
# GCE rejects a boot disk under 10GB outright ("Value must be greater than
# or equal to 10 GB"). AWS accepts 8, which is where this default came from.
DISK_SIZE=10

while [ $# -gt 0 ]; do
  case "$1" in
    --project)       PROJECT="$2"; shift 2 ;;
    --zone)          ZONE="$2"; shift 2 ;;
    --machine-type)  MACHINE_TYPE="$2"; shift 2 ;;
    --disk-size)     DISK_SIZE="$2"; shift 2 ;;
    -h|--help)       sed -n '1,25p' "$0"; exit 0 ;;
    *) echo "unknown: $1" >&2; exit 2 ;;
  esac
done
[ -n "$PROJECT" ] || { echo "usage: $0 --project <gcp-project> [--zone ...]" >&2; exit 2; }

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
INSTALL_SH="$SCRIPT_DIR/install.sh"
[ -f "$INSTALL_SH" ] || { echo "missing $INSTALL_SH" >&2; exit 1; }
IMAGE_NAME_FILE="$SCRIPT_DIR/image-name"

log() { echo "[build-image $PROJECT/$ZONE] $*" >&2; }

log "resolving newest debian-12 family image..."
BASE_IMAGE=$(gcloud compute images describe-from-family debian-12 \
  --project=debian-cloud --format='value(name)')
log "base image: debian-cloud/$BASE_IMAGE"

# --no-service-account: gcloud otherwise attaches the project's DEFAULT compute
# service account, which requires iam.serviceAccountUser on it and hands the
# builder credentials it has no use for — install.sh only apt-installs and
# shuts down. The burst backend itself never attaches an SA either, so this
# keeps the bake's permission requirements identical to the backend's.
BUILDER_NAME="yscale-image-builder-$(date -u +%Y%m%d-%H%M%S)"
log "launching builder instance $BUILDER_NAME ($MACHINE_TYPE, ${DISK_SIZE}GB)..."
gcloud compute instances create "$BUILDER_NAME" \
  --project="$PROJECT" --zone="$ZONE" \
  --machine-type="$MACHINE_TYPE" \
  --image="$BASE_IMAGE" --image-project=debian-cloud \
  --boot-disk-size="${DISK_SIZE}GB" --boot-disk-type=pd-balanced \
  --no-restart-on-failure \
  --metadata-from-file startup-script="$INSTALL_SH" \
  --labels=ys-owner=image-build \
  --no-service-account --no-scopes \
  >/dev/null
log "builder up — waiting for install.sh + shutdown (15-min timeout)..."

# install.sh ends with `shutdown -h now`. GCE treats a guest-initiated
# shutdown as a deliberate stop (TERMINATED), not a failure needing
# automatic restart — --no-restart-on-failure above is belt-and-suspenders.
for i in $(seq 1 90); do
  STATE=$(gcloud compute instances describe "$BUILDER_NAME" \
    --project="$PROJECT" --zone="$ZONE" --format='value(status)' 2>/dev/null || echo "UNKNOWN")
  case "$STATE" in
    TERMINATED) log "builder stopped at iter=$i (~$((i*10)) s)"; break ;;
    PROVISIONING|STAGING|RUNNING|STOPPING) ;;
    *) log "unexpected state $STATE" ;;
  esac
  if [ "$i" = "90" ]; then
    log "FATAL: builder did not stop within 15 min"
    log "leaving the builder running for debug — delete manually: gcloud compute instances delete $BUILDER_NAME --project=$PROJECT --zone=$ZONE"
    exit 1
  fi
  sleep 10
done

TIMESTAMP=$(date -u +%Y%m%d-%H%M%S)
IMAGE_NAME="yscale-burst-$TIMESTAMP"
log "creating image $IMAGE_NAME from builder's boot disk..."
# Blocks until the image is READY — unlike AWS's create-image (async,
# needs its own `wait image-available`), there is no separate wait step.
gcloud compute images create "$IMAGE_NAME" \
  --project="$PROJECT" \
  --source-disk="$BUILDER_NAME" --source-disk-zone="$ZONE" \
  --labels=ys-owner=image-build
log "image $IMAGE_NAME ready"

log "deleting builder $BUILDER_NAME"
gcloud compute instances delete "$BUILDER_NAME" --project="$PROJECT" --zone="$ZONE" --quiet >/dev/null

# Store the fully-qualified image reference locally so an operator can
# feed it straight into YSCALE_GCP_IMAGE without reconstructing the
# projects/.../global/images/... path by hand (gcp.go's SourceImage
# takes that same form for the stock debian-12 image today).
FQ_IMAGE="projects/${PROJECT}/global/images/${IMAGE_NAME}"
echo "$FQ_IMAGE" > "$IMAGE_NAME_FILE"
log "wrote image reference to $IMAGE_NAME_FILE"

# Report final size.
SIZE=$(gcloud compute images describe "$IMAGE_NAME" --project="$PROJECT" \
  --format='value(diskSizeGb)')
log "image disk size: ${SIZE} GB (provisioned)"
log "done. Set YSCALE_GCP_IMAGE=${FQ_IMAGE} to use this baked image."
