#!/usr/bin/env bash
set -euo pipefail

for command in docker kind kubectl; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 1
  fi
done

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CLUSTER=${KIND_CLUSTER:-yscale-kube-bench-smoke}
IMAGE=${IMAGE:-yscale-kube-bench:smoke}
RESULTS=${RESULTS:-"$ROOT/results/kind-smoke"}
KEEP_CLUSTER=${KEEP_CLUSTER:-false}

cleanup() {
  if [[ "$KEEP_CLUSTER" != "true" ]]; then
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
}

diagnose_cluster() {
  echo "kind smoke cluster diagnostics:" >&2
  kubectl get nodes,pods,services,endpoints -A -o wide >&2 || true
  kubectl -n kube-system logs -l k8s-app=kube-proxy --tail=100 --prefix >&2 || true
  kubectl -n kube-system logs -l k8s-app=kube-dns --tail=100 --prefix >&2 || true
  docker exec "${CLUSTER}-control-plane" sh -c \
    'iptables-save 2>/dev/null | grep -E "KUBE-(SERVICES|SVC|SEP)|10[.]96[.]" | tail -100' >&2 || true
}
trap cleanup EXIT

kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --wait 120s

docker build -t "$IMAGE" "$ROOT"
kind load docker-image "$IMAGE" --name "$CLUSTER"

CONFIG=$(mktemp)
trap 'rm -f "$CONFIG"; cleanup' EXIT
cat > "$CONFIG" <<JSON
{
  "apiVersion": "kube-bench.yscale.dev/v1alpha1",
  "kind": "BenchmarkConfig",
  "metadata": {"name": "kind-smoke"},
  "spec": {
    "namespace": "yscale-kube-bench",
    "includeControlPlane": true,
    "image": "$IMAGE",
    "imagePullPolicy": "Never",
    "timeoutSeconds": 300,
    "parallelism": 2,
    "inventory": {"hostInspection": false, "privileged": false},
    "reaction": {"rounds": 1, "readyTimeoutSeconds": 60, "pullImagePolicy": "Never", "clusterWave": true},
    "compute": {"durationSeconds": 1, "isolatedParallelism": 1, "clusterSteps": [0]},
    "network": {"durationSeconds": 1, "rttSamples": 20, "udpSamples": 20, "parallelism": 1, "matrix": "sample", "servicePath": true},
    "dns": {"queries": 10, "concurrency": 2, "targets": [{"name": "kubernetes.default.svc.cluster.local", "protocol": "udp", "class": "service"}]},
    "storage": {"durationSeconds": 1, "sizeMiB": 32, "ioEngine": "io_uring", "targets": [{"name": "ephemeral", "kind": "emptyDir", "mountPath": "/bench/ephemeral"}]},
    "transcode": {"profiles": [{"name": "h264-smoke", "encoder": "libx264", "width": 320, "height": 180, "fps": 30, "seconds": 1, "arguments": ["-preset", "ultrafast"], "hardware": false}]}
  }
}
JSON

mkdir -p "$RESULTS"
"$ROOT/bin/yscale-kube-bench" run \
  --context "kind-$CLUSTER" \
  --config "$CONFIG" \
  --output-dir "$RESULTS"

set +e
python3 - "$RESULTS/latest.json" <<'PY'
import json
import pathlib
import sys

report_path = pathlib.Path(sys.argv[1])
report = json.loads(report_path.read_text())
problems = []

if not report.get("nodes"):
    problems.append("inventory returned no nodes")

for suite_name in ("reaction", "compute", "network", "dns", "storage", "transcode"):
    suite = report.get(suite_name)
    if not isinstance(suite, dict):
        problems.append(f"{suite_name}: missing result")
        continue
    if suite.get("status") != "ok":
        problems.append(f"{suite_name}: status={suite.get('status')} error={suite.get('error', '')}")
        for result in suite.get("results", []):
            if result.get("status") != "ok":
                identity = result.get("path") or result.get("target") or result.get("name") or "unknown"
                problems.append(
                    f"{suite_name}/{identity}: status={result.get('status')} "
                    f"error={result.get('error', '')}"
                )

network_paths = {
    result.get("path")
    for result in report.get("network", {}).get("results", [])
    if result.get("status") == "ok"
}
missing_paths = {"same-node", "cluster-ip"} - network_paths
if missing_paths:
    problems.append("network: missing successful paths " + ", ".join(sorted(missing_paths)))

if problems:
    raise SystemExit("kind smoke report validation failed:\n- " + "\n- ".join(problems))

print("kind smoke report validation passed")
PY
status=$?
set -e
if (( status != 0 )); then
  diagnose_cluster
  exit "$status"
fi

echo "kind smoke reports: $RESULTS"
