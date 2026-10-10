#!/usr/bin/env bash
# build-ami.sh — build a pre-baked yscale burst AMI in us-east-1 (or
# any region passed via --region).
#
# Flow:
#   1. Find latest Debian 12 genericcloud AMI for the region.
#   2. Launch a t3.small builder with install.sh as user-data.
#   3. install.sh installs everything, strips, then `shutdown -h now`.
#   4. EC2 stops the instance; we wait for `stopped` state.
#   5. CreateImage → snapshot the EBS volume as a new AMI.
#   6. Wait for AMI to be `available`.
#   7. TerminateInstances on the builder.
#   8. Write the AMI ID to pkg/backends/aws/ami/ami-id.<region>.
#
# Run locally from a machine with ~/.aws/credentials configured. Total
# wall-clock: ~10-15 min (install.sh dominates).
#
# Usage:
#   ./pkg/backends/aws/ami/build-ami.sh [--region us-east-1]
#                                      [--instance-type t3.small]
#                                      [--volume-size 8]

set -euo pipefail

REGION="us-east-1"
INSTANCE_TYPE="t3.small"
VOLUME_SIZE=8

while [ $# -gt 0 ]; do
  case "$1" in
    --region)        REGION="$2"; shift 2 ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    --volume-size)   VOLUME_SIZE="$2"; shift 2 ;;
    -h|--help)       sed -n '1,28p' "$0"; exit 0 ;;
    *) echo "unknown: $1" >&2; exit 2 ;;
  esac
done

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
INSTALL_SH="$SCRIPT_DIR/install.sh"
[ -f "$INSTALL_SH" ] || { echo "missing $INSTALL_SH" >&2; exit 1; }
AMI_ID_FILE="$SCRIPT_DIR/ami-id.$REGION"

log() { echo "[build-ami $REGION] $*" >&2; }

log "finding latest Debian 12 AMI..."
# Debian publishes only debian-12-amd64-* AMIs to AWS (no separate
# "genericcloud" variant like the upstream Debian Cloud images). The
# size win comes from the post-install strip pass in install.sh,
# not from a smaller base.
BASE_AMI=$(aws ec2 describe-images --region "$REGION" \
  --owners 136693071363 \
  --filters \
    "Name=name,Values=debian-12-amd64-*" \
    "Name=state,Values=available" \
    "Name=architecture,Values=x86_64" \
    "Name=virtualization-type,Values=hvm" \
  --query 'sort_by(Images, &CreationDate)[-1].ImageId' --output text)
log "base AMI: $BASE_AMI"

log "launching builder instance ($INSTANCE_TYPE, ${VOLUME_SIZE}GB)..."
BUILDER_ID=$(aws ec2 run-instances --region "$REGION" \
  --image-id "$BASE_AMI" \
  --instance-type "$INSTANCE_TYPE" \
  --count 1 \
  --instance-initiated-shutdown-behavior stop \
  --block-device-mappings "[{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{\"VolumeSize\":$VOLUME_SIZE,\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
  --user-data "file://$INSTALL_SH" \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=yscale-ami-builder},{Key=yscale-owner,Value=ami-build}]' \
  --query 'Instances[0].InstanceId' --output text)
log "builder: $BUILDER_ID — waiting for cloud-init + shutdown (15-min timeout)..."

# install.sh ends with `shutdown -h now`; wait for `stopped`. Allow
# up to 15 minutes for the install pass.
for i in $(seq 1 90); do
  STATE=$(aws ec2 describe-instances --region "$REGION" \
    --instance-ids "$BUILDER_ID" \
    --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || echo "unknown")
  case "$STATE" in
    stopped) log "builder stopped at iter=$i (~$((i*10)) s)"; break ;;
    running|pending|stopping) ;;
    *) log "unexpected state $STATE"; ;;
  esac
  if [ "$i" = "90" ]; then
    log "FATAL: builder did not stop within 15 min"
    log "leaving the builder running for debug — terminate manually: aws ec2 terminate-instances --region $REGION --instance-ids $BUILDER_ID"
    exit 1
  fi
  sleep 10
done

log "creating image from builder..."
TIMESTAMP=$(date -u +%Y%m%d-%H%M%S)
AMI_NAME="yscale-burst-$TIMESTAMP"
AMI_ID=$(aws ec2 create-image --region "$REGION" \
  --instance-id "$BUILDER_ID" \
  --name "$AMI_NAME" \
  --description "yscale burst baked AMI ($TIMESTAMP, Debian 12 + tailscale/kubelet/containerd/cilium + BPF precompile)" \
  --tag-specifications "ResourceType=image,Tags=[{Key=Name,Value=$AMI_NAME},{Key=yscale-owner,Value=ami-build}]" \
  --tag-specifications "ResourceType=snapshot,Tags=[{Key=Name,Value=$AMI_NAME},{Key=yscale-owner,Value=ami-build}]" \
  --query 'ImageId' --output text)
log "new AMI: $AMI_ID — waiting for 'available'..."

# Snapshot + register usually completes in ~3-5 min.
aws ec2 wait image-available --region "$REGION" --image-ids "$AMI_ID"
log "AMI $AMI_ID available"

log "terminating builder $BUILDER_ID"
aws ec2 terminate-instances --region "$REGION" --instance-ids "$BUILDER_ID" >/dev/null

# Store the AMI ID locally so aws.go can find it without an env var.
echo "$AMI_ID" > "$AMI_ID_FILE"
log "wrote AMI ID to $AMI_ID_FILE"

# Report final size (the snapshot, not the AMI metadata).
SNAP=$(aws ec2 describe-images --region "$REGION" --image-ids "$AMI_ID" \
  --query 'Images[0].BlockDeviceMappings[0].Ebs.SnapshotId' --output text)
SIZE=$(aws ec2 describe-snapshots --region "$REGION" --snapshot-ids "$SNAP" \
  --query 'Snapshots[0].VolumeSize' --output text)
log "snapshot $SNAP volume size: ${SIZE} GB (provisioned)"
log "done. Set YSCALE_AWS_AMI_${REGION//-/_} or use the ami-id file."
