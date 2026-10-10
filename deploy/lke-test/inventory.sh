#!/usr/bin/env bash
# Exact, fail-closed Linode and Fabric inventory helpers for the LKE release gate.
# This file is sourced by verify.sh; listing functions never mutate provider state.
# shellcheck disable=SC2016 # jq variables are intentionally single-quoted for jq, not shell expansion.

LINODE_API_BASE="${LINODE_API_BASE:-https://api.linode.com/v4}"
LINODE_INVENTORY_MAX_PAGES="${LINODE_INVENTORY_MAX_PAGES:-100}"
LINODE_CURL_BIN="${LINODE_CURL_BIN:-curl}"
CENTRAL_CURL_BIN="${CENTRAL_CURL_BIN:-curl}"

central_auth_check() {
  local endpoint="$1" token="$2" http_endpoint status rc=0
  [[ -n "${token}" ]] || { echo "central auth preflight: empty token" >&2; return 1; }
  case "${endpoint}" in
    wss://*) http_endpoint="https://${endpoint#wss://}" ;;
    ws://127.0.0.1:*|ws://localhost:*|ws://\[::1\]:*) http_endpoint="http://${endpoint#ws://}" ;;
    ws://*)
      echo "central auth preflight: plaintext WebSocket is allowed only on loopback" >&2
      return 1
      ;;
    *)
      echo "central auth preflight: endpoint must use wss:// (or loopback ws://)" >&2
      return 1
      ;;
  esac

  status="$("${CENTRAL_CURL_BIN}" -sS --connect-timeout 10 --max-time 20 \
    -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer ${token}" \
    "${http_endpoint%/}/v1/agent/auth-check")" || rc=$?
  if (( rc != 0 )); then
    echo "central auth preflight: endpoint was unreachable (credential and URL redacted)" >&2
    return 1
  fi
  if [[ "${status}" != "204" ]]; then
    echo "central auth preflight: expected HTTP 204, got ${status} (body redacted)" >&2
    return 1
  fi
}

_linode_get() {
  local path="$1"
  "${LINODE_CURL_BIN}" -fsS --connect-timeout 10 --max-time 20 --max-filesize 1048576 \
    -H "Authorization: Bearer ${LINODE_TOKEN}" \
    -H "Accept: application/json" \
    "${LINODE_API_BASE}${path}"
}

_linode_list_ids() {
  local path="$1" filter="$2"
  shift 2
  local page=1 response pages separator
  separator="?"
  [[ "${path}" == *\?* ]] && separator="&"

  while (( page <= LINODE_INVENTORY_MAX_PAGES )); do
    if ! response="$(_linode_get "${path}${separator}page=${page}&page_size=100")"; then
      echo "Linode inventory request failed for page ${page}" >&2
      return 1
    fi
    if ! pages="$(jq -er --argjson expected "${page}" '
      select((.data | type) == "array")
      | select((.page | type) == "number" and (.page | floor) == .page and .page == $expected)
      | select((.pages | type) == "number" and (.pages | floor) == .pages and .pages >= .page)
      | .pages
    ' <<<"${response}")"; then
      echo "Linode inventory returned invalid pagination metadata for page ${page}" >&2
      return 1
    fi
    if ! jq -er "$@" "[${filter}] | .[]" <<<"${response}"; then
      # jq -e exits 4 for an empty stream; an empty page is valid.
      if ! jq -e "$@" "[${filter}] | type == \"array\"" <<<"${response}" >/dev/null; then
        echo "Linode inventory returned invalid JSON for page ${page}" >&2
        return 1
      fi
    fi
    (( page >= pages )) && return 0
    page=$(( page + 1 ))
  done

  echo "Linode inventory exceeds ${LINODE_INVENTORY_MAX_PAGES} pages; refusing a truncated audit" >&2
  return 1
}

linode_cluster_ids_by_label() {
  local label="$1"
  [[ -n "${label}" ]] || { echo "refusing empty LKE label audit" >&2; return 1; }
  _linode_list_ids "/lke/clusters" '.data[] | select(.label == $label) | .id' --arg label "${label}"
}

linode_burst_instance_ids() {
  local burst_id="$1"
  [[ -n "${burst_id}" ]] || { echo "refusing empty burst ID audit" >&2; return 1; }
  _linode_list_ids "/linode/instances" \
    '.data[] | select((.tags // [] | index("yscale-burst")) and (.tags // [] | index($burst_id))) | .id' \
    --arg burst_id "${burst_id}"
}

linode_attached_volume_ids() {
  local provider_id="$1"
  [[ "${provider_id}" =~ ^[1-9][0-9]*$ ]] || { echo "invalid provider ID for volume audit" >&2; return 1; }
  _linode_list_ids "/volumes" \
    '.data[] | select(.linode_id == $provider_id) | .id' \
    --argjson provider_id "${provider_id}"
}

linode_firewall_ids_by_label() {
  local label="$1"
  [[ -n "${label}" ]] || { echo "refusing empty firewall label audit" >&2; return 1; }
  _linode_list_ids "/networking/firewalls" '.data[] | select(.label == $label) | .id' --arg label "${label}"
}

linode_firewall_device_ids() {
  local firewall_id="$1" provider_id="$2"
  [[ "${firewall_id}" =~ ^[1-9][0-9]*$ && "${provider_id}" =~ ^[1-9][0-9]*$ ]] || {
    echo "invalid firewall/provider ID for device audit" >&2
    return 1
  }
  _linode_list_ids "/networking/firewalls/${firewall_id}/devices" \
    '.data[] | select(.entity.type == "linode" and .entity.id == $provider_id) | .id' \
    --argjson provider_id "${provider_id}"
}

linode_exact_instance_count() {
  local provider_id="$1" response_file http_code rc=0
  [[ "${provider_id}" =~ ^[1-9][0-9]*$ ]] || { echo "invalid provider ID for instance audit" >&2; return 1; }
  response_file="$(mktemp)"
  http_code="$("${LINODE_CURL_BIN}" -sS --connect-timeout 10 --max-time 20 --max-filesize 1048576 \
    -o "${response_file}" -w '%{http_code}' \
    -H "Authorization: Bearer ${LINODE_TOKEN}" -H "Accept: application/json" \
    "${LINODE_API_BASE}/linode/instances/${provider_id}")" || rc=$?
  rm -f "${response_file}"
  (( rc == 0 )) || { echo "Linode exact-instance inventory request failed" >&2; return 1; }
  case "${http_code}" in
    2??) printf '1\n' ;;
    404) printf '0\n' ;;
    *) echo "Linode exact-instance inventory returned HTTP ${http_code} (body redacted)" >&2; return 1 ;;
  esac
}

linode_delete_burst_instance() {
  local provider_id="$1"
  [[ "${provider_id}" =~ ^[1-9][0-9]*$ ]] || { echo "invalid provider ID for recovery deletion" >&2; return 1; }
  "${LINODE_CURL_BIN}" -fsS --connect-timeout 10 --max-time 20 -X DELETE \
    -H "Authorization: Bearer ${LINODE_TOKEN}" \
    "${LINODE_API_BASE}/linode/instances/${provider_id}" >/dev/null
}

fabric_node_count() {
  local hostname="$1" encoded_user response
  [[ -n "${hostname}" ]] || { echo "refusing empty Fabric hostname audit" >&2; return 1; }
  [[ -n "${FABRIC_URL:-}" && -n "${FABRIC_API_KEY:-}" && -n "${FABRIC_USER:-}" ]] || {
    echo "Fabric audit credentials are incomplete" >&2
    return 1
  }
  case "${FABRIC_URL}" in
    https://*|http://127.0.0.1:*|http://localhost:*|http://\[::1\]:*) ;;
    *) echo "Fabric audit URL must use HTTPS outside loopback" >&2; return 1 ;;
  esac
  encoded_user="$(jq -rn --arg value "${FABRIC_USER}" '$value | @uri')"
  if ! response="$(curl -fsS --connect-timeout 10 --max-time 20 --max-filesize 8388608 \
    -H "Authorization: Bearer ${FABRIC_API_KEY}" -H "Accept: application/json" \
    "${FABRIC_URL%/}/api/v1/node?user=${encoded_user}")"; then
    echo "Fabric node inventory request failed" >&2
    return 1
  fi
  if (( ${#response} > 8388608 )); then
    echo "Fabric node inventory response exceeded 8 MiB" >&2
    return 1
  fi
  jq -er --arg hostname "${hostname}" '
    select((.nodes | type) == "array")
    | [.nodes[] | select(.givenName == $hostname or .given_name == $hostname or .name == $hostname)]
    | length
  ' <<<"${response}" || {
    echo "Fabric node inventory returned an invalid envelope" >&2
    return 1
  }
}
