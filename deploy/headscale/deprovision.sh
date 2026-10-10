#!/usr/bin/env bash
# yscale:proprietary
# deprovision.sh — tear down Headscale control-plane VM(s) on Linode.
#
# The factory (provision.sh) tags every Headscale VM `yscale-headscale`
# — deliberately NOT `yscale-burst`, so the burst TTL reaper + leak-sweep
# never touch it. The flip side: nothing ELSE deletes these VMs either,
# so without this script they leak forever. This is that missing teardown.
#
# SAFETY: only ever deletes Linodes carrying the EXACT `yscale-headscale`
# tag. It can never touch a burst (tag `yscale-burst`) or a customer's own
# Linodes (no yscale tag). Mirrors the cleanup-safety invariant the Go
# backends follow: act solely on an owner tag yscale itself stamped, never
# a name pattern.
#
# Usage:
#   LINODE_TOKEN=... ./deprovision.sh <linode-id>        # delete one box by id
#   LINODE_TOKEN=... ./deprovision.sh --user <hs-user>   # delete the box(es) for one customer
#   LINODE_TOKEN=... ./deprovision.sh --all              # delete every yscale-headscale box (DESTRUCTIVE)
#   DRY_RUN=1 LINODE_TOKEN=... ./deprovision.sh --all    # print what would be deleted, do nothing
set -euo pipefail

: "${LINODE_TOKEN:?set LINODE_TOKEN}"
OWNER_TAG="yscale-headscale"
API="https://api.linode.com/v4"

log() { echo "[deprovision-headscale] $*" >&2; }

api() { curl -sS -H "Authorization: Bearer $LINODE_TOKEN" "$@"; }

# verify_owner_tag <linode-id> — exits 0 only if the instance carries the
# exact yscale-headscale tag. The hard safety gate: every delete path runs
# through here so we can never remove something we didn't provision.
verify_owner_tag() {
  local id="$1" tags
  tags="$(api "$API/linode/instances/$id" | jq -r '.tags[]?' 2>/dev/null || true)"
  echo "$tags" | grep -qx "$OWNER_TAG"
}

delete_one() {
  local id="$1"
  if ! verify_owner_tag "$id"; then
    log "REFUSING to delete Linode $id — it does not carry the '$OWNER_TAG' tag"
    return 1
  fi
  if [ "${DRY_RUN:-0}" = "1" ]; then
    log "DRY_RUN: would delete Linode $id (tag $OWNER_TAG verified)"
    return 0
  fi
  log "deleting Linode $id ..."
  local code
  code="$(api -o /dev/null -w '%{http_code}' -X DELETE "$API/linode/instances/$id")"
  if [ "$code" -ge 200 ] && [ "$code" -lt 300 ]; then
    log "deleted $id (HTTP $code)"
  else
    log "FAILED to delete Linode $id (HTTP $code)"
    return 1
  fi
}

# list_owned [hs-user] — print "id<TAB>label" of yscale-headscale Linodes,
# optionally filtered to one customer by the provision.sh label convention
# ("headscale-<user>-<ts>").
#
# The Linode account is SHARED, so the instance list can exceed a single page.
# We page through every result (reading .page/.pages) rather than truncating at
# page 1, otherwise owned Headscale boxes past the first page would be silently
# missed and leak billed control planes.
list_owned() {
  local want_user="${1:-}"
  local page=1 pages=1 resp
  while [ "$page" -le "$pages" ]; do
    resp="$(api "$API/linode/instances?page_size=500&page=$page")"
    pages="$(echo "$resp" | jq -r '.pages // 1')"
    echo "$resp" \
      | jq -r --arg tag "$OWNER_TAG" --arg user "$want_user" '
          .data[]
          | select(.tags | index($tag))
          | select($user == "" or (.label | startswith("headscale-" + $user + "-")))
          | "\(.id)\t\(.label)"'
    page=$((page + 1))
  done
}

case "${1:-}" in
  ""|-h|--help)
    grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
  --all)
    log "listing all $OWNER_TAG Linodes..."
    list_owned | while IFS=$'\t' read -r id label; do
      [ -z "$id" ] && continue
      log "  candidate: $id ($label)"
      delete_one "$id" || true
    done ;;
  --user)
    USER_ARG="${2:?--user requires a headscale user/customer id}"
    list_owned "$USER_ARG" | while IFS=$'\t' read -r id label; do
      [ -z "$id" ] && continue
      log "  candidate: $id ($label)"
      delete_one "$id" || true
    done ;;
  --*)
    # An unrecognised flag must never fall through to the literal-id branch
    # (where a typo like --use would be treated as a Linode id and silently
    # no-op). Fail loudly instead.
    log "unrecognised flag '$1' (expected --user, --all, --help, or a numeric Linode id)"
    exit 2 ;;
  *)
    # Treat the arg as a literal Linode id, but only if it actually looks like
    # one — a non-numeric value is an operator mistake, not an id.
    if ! [[ "$1" =~ ^[0-9]+$ ]]; then
      log "unrecognised argument '$1' (expected --user, --all, --help, or a numeric Linode id)"
      exit 2
    fi
    delete_one "$1" ;;
esac

log "done"
