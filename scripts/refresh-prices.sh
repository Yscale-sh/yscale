#!/usr/bin/env bash
# refresh-prices.sh — print the live upstream rates behind
# central/internal/pricing/catalog.go so the hand-curated tables can be
# verified / refreshed against the providers' own APIs.
#
#   AWS    — Pricing API (us-east-1 on-demand, Linux, Shared tenancy)
#   Linode — /v4/linode/types  (needs LINODE_TOKEN in env or ./.env)
#   Fly    — no public pricing API; see pricefeed/ (HTML scrape) or
#            fly.io/docs/about/pricing
#
# Read-only. Compare the output to catalog.go and update by hand (the
# values matched exactly as of 2026-06-09).
set -euo pipefail

# The strip set is a double-quote and a single-quote. Spell it as a
# double-quoted string; the '"'"' form this used to carry left an
# unterminated quote, which is a PARSE error — bash refused to run the
# whole file, so this script could never have worked.
: "${LINODE_TOKEN:=$(grep -E '^LINODE_TOKEN=' "$(dirname "$0")/../.env" 2>/dev/null | head -1 | cut -d= -f2- | tr -d "\"'")}" || true

echo "== AWS EC2 on-demand (us-east-1, Linux/Shared) =="
for it in t3.small t3.medium m7i.large m7i.xlarge m7i.2xlarge; do
  p=$(aws pricing get-products --region us-east-1 --service-code AmazonEC2 \
    --filters "Type=TERM_MATCH,Field=instanceType,Value=$it" \
      "Type=TERM_MATCH,Field=location,Value=US East (N. Virginia)" \
      "Type=TERM_MATCH,Field=operatingSystem,Value=Linux" \
      "Type=TERM_MATCH,Field=tenancy,Value=Shared" \
      "Type=TERM_MATCH,Field=preInstalledSw,Value=NA" \
      "Type=TERM_MATCH,Field=capacitystatus,Value=Used" \
    --query 'PriceList[0]' --output text 2>/dev/null \
    | jq -r '.terms.OnDemand[].priceDimensions[].pricePerUnit.USD' 2>/dev/null | head -1) || p=""
  # One provider's missing credential must not abort the whole report. Under
  # `set -euo pipefail` a failed command substitution kills the script, so a
  # caller without pricing:GetProducts would never reach the sections below.
  printf '  %-13s %s\n' "$it" "${p:-(no access)}"
done

echo "== Linode plans (hourly) =="
if [ -n "${LINODE_TOKEN:-}" ]; then
  curl -s -H "Authorization: Bearer $LINODE_TOKEN" "https://api.linode.com/v4/linode/types" \
    | jq -r '.data[] | select(.id|test("nanode-1$|standard-1$|standard-2$|standard-4$|rtx4000a1-s$|rtx6000-1$")) | "  \(.id) \(.price.hourly)/hr ($\(.price.monthly)/mo)"'
else
  echo "  (set LINODE_TOKEN to query)"
fi

# Azure Retail Prices needs NO auth and no SDK — the easiest source here.
# Windows and Spot rows share an armSkuName with the Linux pay-as-you-go
# row, so filtering them out is mandatory, not cosmetic: an unfiltered
# take-the-first-row would silently pick up a ~2x Windows rate.
AZURE_REGION="${AZURE_REGION:-eastus}"
echo "== Azure VM sizes (hourly, ${AZURE_REGION}, Linux pay-as-you-go) =="
for sku in Standard_B1ms Standard_B2s_v2 Standard_D2s_v5 Standard_D4s_v5 Standard_D8s_v5; do
  # Retry: back-to-back requests get throttled and the body comes back as
  # non-JSON, which jq reports as a parse error rather than an empty result.
  p="n/a"
  for _ in 1 2 3; do
    body=$(curl -s --max-time 20 -G "https://prices.azure.com/api/retail/prices" \
             --data-urlencode "\$filter=serviceName eq 'Virtual Machines' and armRegionName eq '${AZURE_REGION}' and armSkuName eq '${sku}' and priceType eq 'Consumption'")
    p=$(printf '%s' "$body" | jq -r '[.Items[]?
          | select((.productName|test("Windows")|not)
                   and (.skuName|test("Spot|Low Priority")|not))][0].retailPrice // "n/a"' 2>/dev/null || echo "n/a")
    [ "$p" != "n/a" ] && break
    sleep 2
  done
  printf '  %-17s %s\n' "$sku" "$p"
done

# GCP standard E2 types use separate per-vCPU and per-GiB SKUs. Shared-core
# e2-small/e2-medium instead have fixed machine prices; verify those against
# the public E2 pricing table when refreshing catalog.go.
echo "== GCP Compute Engine SKUs (E2 predefined, us-central1) =="
if [ -n "${GCP_BILLING_API_KEY:-}" ]; then
  curl -s "https://cloudbilling.googleapis.com/v1/services/6F81-5844-456A/skus?key=${GCP_BILLING_API_KEY}&currencyCode=USD&pageSize=500" \
    | jq -r '
      (.error.message // empty) as $e
      | if $e then "  ERROR: \($e)"
        else (.skus[]
          | select(.category.resourceGroup == "CPU" or .category.resourceGroup == "RAM")
          | select(.description | test("^E2 Instance (Core|Ram) running in Americas$"))
          | .pricingInfo[0].pricingExpression as $p
          | "  \(.description): \($p.tieredRates[-1].unitPrice.units // 0).\($p.tieredRates[-1].unitPrice.nanos // 0 | tostring | ("000000000"[0:9-length]) + .) per \($p.usageUnit)")
        end'
else
  echo "  (set GCP_BILLING_API_KEY to query; needs cloudbilling.googleapis.com enabled)"
fi
