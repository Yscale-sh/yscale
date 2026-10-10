#!/usr/bin/env bash
# Best-effort cost report for currently-running yscale bursts on Fly +
# Linode. Sums (duration so far × hourly rate) per burst; prints a
# table + total. Limited because Fly+AutoDestroy means destroyed
# machines are gone from the API — for retrospective historical cost
# use the provider dashboards (or wait for central-side cost tracking).
#
# Usage: scripts/cost-report.sh
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO}/.env}"
FLY_BURST_APP="${FLY_BURST_APP:-hs-personal-burst}"

env_get() {
  grep "^${1}=" "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"'
}

# Approximate hourly USD by Fly machine size (matches Fly's published prices for
# shared-cpu-* tiers). Add new rows as we use new sizes.
fly_rate_per_hour() {
  case "$1" in
    shared-cpu-1x:1024MB) echo 0.0019 ;;
    shared-cpu-1x:2048MB) echo 0.0039 ;;
    shared-cpu-2x:4096MB) echo 0.0084 ;;
    performance-1x:2048MB) echo 0.034 ;;
    performance-2x:4096MB) echo 0.068 ;;
    *) echo 0 ;;
  esac
}

now=$(date -u +%s)
total=0
fmt='%-12s %-22s %-22s %-10s %-12s %s\n'

printf "$fmt" backend burst_id name age rate_per_hr cost_so_far
printf '%.0s-' {1..100}; echo

# --- Fly ---
FLY_TOKEN=$(env_get FLYIO_TOKEN)
if [ -n "$FLY_TOKEN" ] && command -v flyctl >/dev/null; then
  export FLY_API_TOKEN="$FLY_TOKEN"
  while IFS=$'\t' read -r id name created size; do
    [ -z "$id" ] && continue
    started=$(date -u -j -f "%Y-%m-%dT%H:%M:%SZ" "$created" +%s 2>/dev/null || date -u -d "$created" +%s 2>/dev/null || echo $now)
    age_s=$((now - started))
    rate=$(fly_rate_per_hour "$size")
    cost=$(awk -v a=$age_s -v r=$rate 'BEGIN{ printf "%.5f", (a/3600)*r }')
    total=$(awk -v t=$total -v c=$cost 'BEGIN{ printf "%.5f", t+c }')
    age_h=$(printf '%dm%ds' $((age_s/60)) $((age_s%60)))
    printf "$fmt" flyio "$id" "$name" "$age_h" "\$$rate" "\$$cost"
  done < <(flyctl machine list -a "$FLY_BURST_APP" --json 2>/dev/null \
    | jq -r '.[] | select(.name | startswith("ys-burst-")) | "\(.id)\t\(.name)\t\(.created_at)\t\(.config.guest.cpu_kind // "shared-cpu")-\(.config.guest.cpus // 1)x:\(.config.guest.memory_mb // 1024)MB"' 2>/dev/null)
fi


echo
printf "running cost so far: \$%s\n" "$total"
echo "(destroyed machines aren't in this report — see Fly/Linode dashboards for historical billing)"
