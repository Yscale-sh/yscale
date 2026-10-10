#!/usr/bin/env bash
# Watch whether the EKS cluster's cloud-controller-manager PURGES yscale burst
# nodes after they join. This is the make-or-break compatibility check for
# running yscale on managed EKS (the LKE-CCM problem; see README "Two kinds of
# deletion"). Run this in one terminal, then submit a Workload in another and
# watch the burst node either persist (good) or get a RemovingNode event (bad).
set -euo pipefail

echo "==> burst nodes currently in the cluster (ys-burst-* / yscale.sh/burst-node):"
kubectl get nodes -l yscale.sh/burst-node 2>/dev/null || kubectl get nodes | grep -E 'ys-burst|NAME' || true
echo ""
echo "==> watching for CCM node-removal events (RemovingNode / DeletingNode / NodeNotReady)."
echo "    A burst node that VANISHES within ~90s of Ready means the CCM purged it."
echo "    Ctrl-C to stop."
echo ""
# --watch streams events as they arrive; grep keeps only the signal we care about.
kubectl get events -A --watch 2>/dev/null \
  | grep --line-buffered -iE 'RemovingNode|DeletingNode|removing node|ys-burst' \
  || true
