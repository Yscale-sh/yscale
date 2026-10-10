#!/usr/bin/env bash
set -euo pipefail

# 1. Lint the chart
echo "=== Running helm lint ==="
helm lint deploy/helm/yscale-agent

# 2. Render the template
echo "=== Rendering template ==="
RENDERED=$(helm template deploy/helm/yscale-agent \
  --set gateway.enabled=true \
  --set gateway.kubeletProxyRouting.enabled=true \
  -s templates/kubelet-proxy-routing-daemonset.yaml)

# 3. Assert guards are present in the rendered output
echo "=== Asserting guards in rendered template ==="
declare -a GUARDS=(
  "expected exactly 1 candidate pod"
  'ITEMS_JSON=${CLEAN_LIST_JSON#*\"items\":\[}'
  "-T 10"
  "REMOVE_FORWARD_AFTER_ROUTE"
  "namespaces/\$NAMESPACE/pods/\$FINAL_NAME"
  "deletionTimestamp"
  "type\":\"Ready"
  "GET_UID"
  "CANDIDATE_COUNT"
)

for guard in "${GUARDS[@]}"; do
  if [[ "$RENDERED" != *"$guard"* ]]; then
    echo "FAIL: guard '$guard' not found in rendered template!" >&2
    exit 1
  fi
done
echo "All guards asserted successfully in rendered template!"

# 4. Extract shell script for execution testing
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

echo "$RENDERED" | awk '/            - \|/{flag=1; next} /          resources:/{flag=0} flag' | sed 's/^              //' > "$TMP_DIR/extracted.sh"

# Mock SA token file
mkdir -p "$TMP_DIR/mock-sa"
echo "dummy-token" > "$TMP_DIR/mock-sa/token"

# Replace token file path in script
sed -i "s|SA_TOKEN_FILE=/var/run/secrets/kubernetes.io/serviceaccount/token|SA_TOKEN_FILE=$TMP_DIR/mock-sa/token|" "$TMP_DIR/extracted.sh"

# Define mock harness prefix
cat << 'EOF' > "$TMP_DIR/harness.sh"
# Environment overrides
NODE_NAME="${MOCK_NODE_NAME:-node-local}"

# Command mocks
wget() {
  local url="${@: -1}"
  case "$url" in
    *"/pods?labelSelector="*)
      cat "$MOCK_PODS_LIST_FILE"
      ;;
    *"/pods/"*)
      if [ -f "$MOCK_POD_GET_FILE" ]; then
        cat "$MOCK_POD_GET_FILE"
      else
        return 1
      fi
      ;;
    *"/nodes/"*)
      echo '{"status":{"addresses":[{"type":"InternalIP","address":"10.0.0.5"}]}}'
      ;;
    *)
      echo "Unknown URL: $url" >&2
      return 1
      ;;
  esac
}

ip() {
  case "$*" in
    "route show exact"*)
      echo "${MOCK_IP_ROUTE_SHOW:-}"
      ;;
    "route replace"*)
      echo "MOCK-IP-ROUTE-REPLACE: $*"
      ;;
    *)
      ;;
  esac
}

mock_iptables() {
  case "$*" in
    "-S FORWARD")
      echo '-P FORWARD ACCEPT'
      return 0
      ;;
    "-S ts-input")
      return 1
      ;;
    -C*)
      # No managed rules exist in the harness. Returning success here would
      # make the controller's defensive remove-until-absent loops infinite.
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

iptables-nft() { mock_iptables "$@"; }
iptables-legacy() { mock_iptables "$@"; }
iptables() { mock_iptables "$@"; }

sleep() {
  echo "SLEEP_CALLED"
  exit 0
}
EOF

# Combine harness and extracted script (note we source it so functions are active)
cat "$TMP_DIR/harness.sh" "$TMP_DIR/extracted.sh" > "$TMP_DIR/run_test.sh"
chmod +x "$TMP_DIR/run_test.sh"

run_case() {
  local name="$1"
  local pods_list="$2"
  local pod_get="$3"
  local expected_log="$4"
  local mock_route_show="${5:-}"

  echo "Running test case: $name"

  echo "$pods_list" > "$TMP_DIR/pods_list.json"
  if [ -n "$pod_get" ]; then
    echo "$pod_get" > "$TMP_DIR/pod_get.json"
    export MOCK_POD_GET_FILE="$TMP_DIR/pod_get.json"
  else
    unset MOCK_POD_GET_FILE
  fi

  export MOCK_PODS_LIST_FILE="$TMP_DIR/pods_list.json"
  export MOCK_IP_ROUTE_SHOW="$mock_route_show"
  export MOCK_NODE_NAME="node-local"

  local output
  output=$(bash "$TMP_DIR/run_test.sh" 2>&1) || true

  if ! echo "$output" | grep -Ei "$expected_log" >/dev/null; then
    echo "FAIL: Expected log pattern '$expected_log' not found in output:" >&2
    echo "=== OUTPUT ===" >&2
    echo "$output" >&2
    echo "==============" >&2
    exit 1
  fi
  echo "PASS: $name"
}

# Case 1: 0 candidate pods in discovery
run_case "0 candidate pods" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[]}' \
  '' \
  "expected exactly 1 candidate pod, found 0"

# Case 2: 2 candidate pods in discovery
run_case "2 candidate pods" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"pod-1","uid":"uid-1"}},{"metadata":{"name":"pod-2","uid":"uid-2"}}]}' \
  '' \
  "expected exactly 1 candidate pod, found 2"

# Case 3: 1 candidate, GET fails (404)
run_case "GET fails" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"gateway-pod","uid":"uid-gw"}}]}' \
  '' \
  "exact GET for pod gateway-pod failed"

# Case 4: 1 candidate, UID mismatch
run_case "UID mismatch" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"gateway-pod","uid":"uid-expected"}}]}' \
  '{"metadata":{"name":"gateway-pod","uid":"uid-mismatch"},"spec":{"nodeName":"node-1"},"status":{"phase":"Running","podIP":"10.42.3.112","conditions":[{"type":"Ready","status":"True"}]}}' \
  "UID mismatch \(expected uid-expected, got uid-mismatch\)"

# Case 5: 1 candidate, terminating (has deletionTimestamp)
run_case "terminating pod" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"gateway-pod","uid":"uid-gw"}}]}' \
  '{"metadata":{"name":"gateway-pod","uid":"uid-gw","deletionTimestamp":"2026-07-17T01:42:00Z"},"spec":{"nodeName":"node-1"},"status":{"phase":"Running","podIP":"10.42.3.112","conditions":[{"type":"Ready","status":"True"}]}}' \
  "is terminating"

# Case 6: 1 candidate, phase is not Running
run_case "phase is not Running" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"gateway-pod","uid":"uid-gw"}}]}' \
  '{"metadata":{"name":"gateway-pod","uid":"uid-gw"},"spec":{"nodeName":"node-1"},"status":{"phase":"Pending","podIP":"10.42.3.112","conditions":[{"type":"Ready","status":"True"}]}}' \
  "phase is Pending \(expected Running\)"

# Case 7: 1 candidate, Ready is False
run_case "Ready is False" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"gateway-pod","uid":"uid-gw"}}]}' \
  '{"metadata":{"name":"gateway-pod","uid":"uid-gw"},"spec":{"nodeName":"node-1"},"status":{"phase":"Running","podIP":"10.42.3.112","conditions":[{"type":"Ready","status":"False"}]}}' \
  "is not Ready"

# Case 8: 1 candidate, empty nodeName
run_case "empty nodeName" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"gateway-pod","uid":"uid-gw"}}]}' \
  '{"metadata":{"name":"gateway-pod","uid":"uid-gw"},"spec":{"nodeName":""},"status":{"phase":"Running","podIP":"10.42.3.112","conditions":[{"type":"Ready","status":"True"}]}}' \
  "has empty nodeName"

# Case 9: 1 candidate, empty podIP
run_case "empty podIP" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"gateway-pod","uid":"uid-gw"}}]}' \
  '{"metadata":{"name":"gateway-pod","uid":"uid-gw"},"spec":{"nodeName":"node-1"},"status":{"phase":"Running","podIP":"","conditions":[{"type":"Ready","status":"True"}]}}' \
  "has empty nodeName.*or podIP"

# Case 10: 1 candidate, valid remote node next-hop resolution
run_case "valid remote gateway next-hop" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"gateway-pod","uid":"uid-gw"}}]}' \
  '{"metadata":{"name":"gateway-pod","uid":"uid-gw"},"spec":{"nodeName":"node-remote"},"status":{"phase":"Running","podIP":"10.42.3.112","conditions":[{"type":"Ready","status":"True"}]}}' \
  "MOCK-IP-ROUTE-REPLACE:.*via 10.0.0.5"

# Case 11: 1 candidate, valid local node next-hop resolution
run_case "valid local gateway next-hop" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"gateway-pod","uid":"uid-gw"}}]}' \
  '{"metadata":{"name":"gateway-pod","uid":"uid-gw"},"spec":{"nodeName":"node-local"},"status":{"phase":"Running","podIP":"10.42.3.112","conditions":[{"type":"Ready","status":"True"}]}}' \
  "MOCK-IP-ROUTE-REPLACE:.*via 10.42.3.112"

# Case 12: a realistic PodList contains a nested downwardAPI.items array.
# The old greedy extraction started at that nested field and found 0 pods.
run_case "valid PodList with nested items fields" \
  '{"kind":"PodList","apiVersion":"v1","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"gateway-pod","uid":"uid-gw"},"spec":{"volumes":[{"name":"kube-api-access","projected":{"sources":[{"downwardAPI":{"items":[{"path":"namespace","fieldRef":{"fieldPath":"metadata.namespace"}}]}}]}}]}}]}' \
  '{"metadata":{"name":"gateway-pod","uid":"uid-gw"},"spec":{"nodeName":"node-local"},"status":{"phase":"Running","podIP":"10.42.3.112","conditions":[{"type":"Ready","status":"True"}]}}' \
  "MOCK-IP-ROUTE-REPLACE:.*via 10.42.3.112"

echo "=== All regression test cases passed! ==="
