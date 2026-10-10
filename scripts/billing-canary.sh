#!/usr/bin/env bash
# yscale:proprietary
#
# scripts/billing-canary.sh
# Drives the Stripe test-mode canary end to end from outside the go process.
#
# Responsibilities that the go test cannot own:
#   1. Own the private control directory (mode 0700 in a fresh temp dir) and
#      the caller-specified evidence path — the shell PRESERVES the operator's
#      absolute evidence path exactly as supplied and never overrides or
#      deletes the resulting file.
#   2. Own the Stripe CLI listener that captures the ephemeral signing secret
#      and forwards the bounded set of allowed webhook event types to the
#      loopback address the test binds.
#   3. Drive an isolated agent-browser session through the Stripe hosted
#      checkout page using the documented Stripe test card, then close the
#      session so no state persists between runs.
#
# The refund and every Stripe cleanup step (metadata clear on Checkout
# Session/PaymentIntent/Charge, test Customer delete, incomplete session
# expiry, and the post-cleanup ExternalCashSnapshot assertion) are performed
# INSIDE the build-tagged Go canary via a private stripe-go client. The
# shell never touches the Stripe API for any of that — a || true swallowed
# failure there would let a leaked Yscale-tagged object survive on the
# shared test account.
#
# The script never prints the Checkout URL, the Stripe PaymentIntent ID, the
# session ID, the ephemeral webhook secret, or the caller's Stripe API key.
# All of those live only in process env and inside the mode-0700 control dir.
#
# Every guard the canary asserts in Go (test-key only, test-DB suffix,
# shared-account sentinel, absolute evidence path) is re-checked here so the
# script fails closed BEFORE it launches Stripe CLI or agent-browser.
#
# Usage:
#   BILLING_TEST_DATABASE_URL=... \
#   BILLING_TEST_EXPECT_DATABASE=example_billing_test \
#   BILLING_TEST_ALLOW_DESTRUCTIVE=I_UNDERSTAND_DROP_BILLING_SCHEMA \
#   STRIPE_SECRET_KEY=sk_test_... \
#   STRIPE_ACCOUNT_ID=acct_... \
#   BILLING_CHECKOUT_SUCCESS_URL=https://... \
#   BILLING_CHECKOUT_CANCEL_URL=https://... \
#   YSCALE_STRIPE_CANARY_ALLOW_SHARED_ACCOUNT=I_UNDERSTAND \
#   YSCALE_STRIPE_CANARY_EVIDENCE_PATH=/absolute/path/to/evidence.json \
#     scripts/billing-canary.sh [--live]
#
# The Stripe webhook secret comes from `stripe listen --print-secret`; do NOT
# set STRIPE_WEBHOOK_SECRET yourself.

set -euo pipefail

live=0
if [[ "${1:-}" == "--live" ]]; then
  live=1
fi

require() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "billing-canary: ${name} is required" >&2
    exit 2
  fi
}

require BILLING_TEST_DATABASE_URL
require BILLING_TEST_EXPECT_DATABASE
require STRIPE_SECRET_KEY
require STRIPE_ACCOUNT_ID
require BILLING_CHECKOUT_SUCCESS_URL
require BILLING_CHECKOUT_CANCEL_URL
require YSCALE_STRIPE_CANARY_EVIDENCE_PATH

if [[ "${BILLING_TEST_ALLOW_DESTRUCTIVE:-}" != "I_UNDERSTAND_DROP_BILLING_SCHEMA" ]]; then
  echo "billing-canary: BILLING_TEST_ALLOW_DESTRUCTIVE must be I_UNDERSTAND_DROP_BILLING_SCHEMA" >&2
  exit 2
fi
if [[ "${BILLING_TEST_EXPECT_DATABASE}" != *_billing_test ]]; then
  echo "billing-canary: BILLING_TEST_EXPECT_DATABASE must end with _billing_test" >&2
  exit 2
fi
case "${STRIPE_SECRET_KEY}" in
  sk_test_*|rk_test_*) ;;
  *) echo "billing-canary: STRIPE_SECRET_KEY must be a Stripe test-mode key" >&2; exit 2 ;;
esac
case "${STRIPE_ACCOUNT_ID}" in
  acct_*) ;;
  *) echo "billing-canary: STRIPE_ACCOUNT_ID must begin with acct_" >&2; exit 2 ;;
esac
if [[ "${YSCALE_STRIPE_CANARY_ALLOW_SHARED_ACCOUNT:-}" != "I_UNDERSTAND" ]]; then
  echo "billing-canary: shared Stripe test accounts require YSCALE_STRIPE_CANARY_ALLOW_SHARED_ACCOUNT=I_UNDERSTAND" >&2
  exit 2
fi
case "${YSCALE_STRIPE_CANARY_EVIDENCE_PATH}" in
  /*) ;;
  *) echo "billing-canary: YSCALE_STRIPE_CANARY_EVIDENCE_PATH must be absolute" >&2; exit 2 ;;
esac

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
# The control directory is a fresh mode-0700 temp dir owned by this run. The
# evidence path is caller-supplied and PRESERVED exactly — the cleanup below
# removes the control dir only, never the evidence file.
control_root="$(mktemp -d -t yscale-billing-canary.XXXXXXXX)"
control_dir="${control_root}/control"
mkdir -m 0700 "${control_dir}"
umask 077

# Bounded status the operator sees on stderr; NO Stripe identifiers ever
# leave this process through stdout/stderr.
status="unknown"

cleanup() {
  if [[ -n "${STRIPE_LISTEN_PID:-}" ]] && kill -0 "${STRIPE_LISTEN_PID}" 2>/dev/null; then
    kill "${STRIPE_LISTEN_PID}" 2>/dev/null || true
    wait "${STRIPE_LISTEN_PID}" 2>/dev/null || true
  fi
  if [[ -n "${BROWSER_PID:-}" ]] && kill -0 "${BROWSER_PID}" 2>/dev/null; then
    kill "${BROWSER_PID}" 2>/dev/null || true
    wait "${BROWSER_PID}" 2>/dev/null || true
  fi
  # Close the isolated agent-browser session so no browser state persists
  # between canary runs. Ignore stderr; the session may already be closed
  # if the driver exited cleanly.
  if [[ -n "${BROWSER_SESSION:-}" ]] && command -v agent-browser >/dev/null 2>&1; then
    agent-browser --session "${BROWSER_SESSION}" close >/dev/null 2>&1 || true
  fi
  rm -rf -- "${control_root}"
  echo "billing-canary: status=${status}" >&2
}
trap cleanup EXIT

pick_port() {
  # Ask the kernel for a free loopback port. python is used only for the
  # cross-platform TCP socket call; the port is echoed on stdout, no other
  # data leaves the process.
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

test_args=(
  "-tags=stripecanary"
  "-run=TestStripeCanary"
  "-count=1"
  "-timeout=30m"
  "./central/internal/billingcanary/"
)

env_common=(
  "BILLING_TEST_DATABASE_URL=${BILLING_TEST_DATABASE_URL}"
  "BILLING_TEST_ALLOW_DESTRUCTIVE=${BILLING_TEST_ALLOW_DESTRUCTIVE}"
  "BILLING_TEST_EXPECT_DATABASE=${BILLING_TEST_EXPECT_DATABASE}"
  "STRIPE_SECRET_KEY=${STRIPE_SECRET_KEY}"
  "STRIPE_ACCOUNT_ID=${STRIPE_ACCOUNT_ID}"
  "BILLING_CHECKOUT_SUCCESS_URL=${BILLING_CHECKOUT_SUCCESS_URL}"
  "BILLING_CHECKOUT_CANCEL_URL=${BILLING_CHECKOUT_CANCEL_URL}"
  "YSCALE_STRIPE_CANARY_ALLOW_SHARED_ACCOUNT=${YSCALE_STRIPE_CANARY_ALLOW_SHARED_ACCOUNT}"
  "YSCALE_STRIPE_CANARY_EVIDENCE_PATH=${YSCALE_STRIPE_CANARY_EVIDENCE_PATH}"
)

if [[ "${live}" -eq 0 ]]; then
  # Deterministic mode. The go test wants a real signing secret for the
  # inline HMAC path; a locally derived one is enough — nothing forwards
  # from Stripe, no browser is launched, no Stripe API call is made.
  ephemeral_secret="whsec_$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  cd "${repo_root}"
  set +e
  env -i PATH="${PATH}" HOME="${HOME}" GOFLAGS="${GOFLAGS:-}" \
    "${env_common[@]}" \
    "STRIPE_WEBHOOK_SECRET=${ephemeral_secret}" \
    go test "${test_args[@]}"
  rc=$?
  set -e
  if [[ "${rc}" -eq 0 ]]; then status="pass"; else status="fail"; fi
  exit "${rc}"
fi

# Live mode below.

# stripe-go v86 rejects snapshot webhooks from a different API release train.
# `stripe listen --latest` currently selects this exact SDK version; the ready
# banner check below makes a future Stripe default fail closed before Checkout
# instead of silently forwarding payloads the production gateway will reject.
stripe_api_version="2026-07-29.dahlia"

if ! command -v stripe >/dev/null 2>&1; then
  echo "billing-canary: stripe CLI is required for --live" >&2
  exit 2
fi
if ! command -v agent-browser >/dev/null 2>&1; then
  echo "billing-canary: agent-browser is required for --live" >&2
  exit 2
fi

port="$(pick_port)"
listen_addr="127.0.0.1:${port}"
forward_url="http://${listen_addr}/v1/billing/webhooks/stripe"

# Step 1: obtain the ephemeral webhook signing secret. `stripe listen
# --print-secret` prints the secret and exits — it cannot forward. We
# capture the value once so the forwarding listener below can use the
# same secret.
webhook_secret="$(stripe listen --api-key "${STRIPE_SECRET_KEY}" --latest --print-secret 2>/dev/null | tr -d '[:space:]')"
if [[ -z "${webhook_secret}" || "${webhook_secret}" != whsec_* ]]; then
  echo "billing-canary: stripe listen --print-secret did not return a whsec_ value" >&2
  exit 3
fi

# Step 2: start the forwarding listener on the same api key, bounded to the
# exact event types the Yscale gateway accepts. A broader listener risks
# forwarding an out-of-contract event that the webhook handler will reject.
allowed_events="checkout.session.completed,refund.created,refund.updated"

listen_log="${control_root}/stripe-listen.log"
stripe listen \
  --api-key "${STRIPE_SECRET_KEY}" \
  --latest \
  --forward-to "${forward_url}" \
  --events "${allowed_events}" \
  --skip-verify \
  >/dev/null 2>"${listen_log}" &
STRIPE_LISTEN_PID=$!

# Wait up to 20s for the listener to attach; the CLI writes its ready
# banner to stderr.
for _ in $(seq 1 200); do
  if grep -q "Ready" "${listen_log}" 2>/dev/null; then
    break
  fi
  if ! kill -0 "${STRIPE_LISTEN_PID}" 2>/dev/null; then
    echo "billing-canary: stripe listen exited before becoming ready" >&2
    exit 3
  fi
  sleep 0.1
done
if ! grep -q "Ready" "${listen_log}" 2>/dev/null; then
  echo "billing-canary: stripe listen did not become ready" >&2
  exit 3
fi
if ! grep -Fq "Stripe API Version [${stripe_api_version}]" "${listen_log}" 2>/dev/null; then
  echo "billing-canary: stripe listen API version does not match stripe-go" >&2
  exit 3
fi

# Step 3: launch the agent-browser handler in an isolated session. Only the
# checkout-completion handshake runs here — the refund and metadata cleanup
# are executed inside the go canary via stripe-go. The isolated session is
# closed unconditionally by the EXIT trap so nothing persists between runs.
BROWSER_SESSION="canary-$$-$(head -c 4 /dev/urandom | od -An -tx1 | tr -d ' \n')"
(
  set -eu
  write_payment_done() {
    completion_tmp="${control_dir}/payment-done.tmp.$$"
    printf '%s\n%s\n' "${token}" "$1" >"${completion_tmp}"
    mv -f -- "${completion_tmp}" "${control_dir}/payment-done"
  }
  browser_fail() {
    # Failures stay bounded and private: no screenshot, no DOM element dump,
    # no snapshot sidecar. The stage tag on stderr is the entire failure
    # signal this handler emits, so the hosted Checkout URL and any Stripe
    # provider identifiers cannot leak through a diagnostic artifact.
    echo "billing-canary: browser_stage=$1" >&2
    write_payment_done cancelled
    exit 0
  }
  while :; do
    if [[ -s "${control_dir}/checkout-url" && -s "${control_dir}/payment-token" ]]; then
      token="$(head -n1 "${control_dir}/payment-token" | tr -d '\n')"
      url="$(head -n1 "${control_dir}/checkout-url" | tr -d '\n')"
      rm -f "${control_dir}/checkout-url" "${control_dir}/payment-token"
      # Drive the hosted checkout page using documented agent-browser verbs
      # in an isolated session; semantic selectors + Stripe's documented
      # test card. The URL is passed as an argument to `open`; nothing is
      # printed or logged.
      agent-browser --session "${BROWSER_SESSION}" open "${url}" >/dev/null 2>&1 \
        || browser_fail open
      agent-browser --session "${BROWSER_SESSION}" wait --load domcontentloaded >/dev/null 2>&1 \
        || browser_fail load
      agent-browser --session "${BROWSER_SESSION}" find label "Email" fill "yscale-canary@example.com" >/dev/null 2>&1 \
        || browser_fail email
      agent-browser --session "${BROWSER_SESSION}" find text "Card" click --exact >/dev/null 2>&1 \
        || browser_fail select_card
      agent-browser --session "${BROWSER_SESSION}" uncheck 'input[type="checkbox"]' >/dev/null 2>&1 \
        || browser_fail disable_link
      agent-browser --session "${BROWSER_SESSION}" find placeholder "1234 1234 1234 1234" fill "4242 4242 4242 4242" >/dev/null 2>&1 \
        || browser_fail card_number
      agent-browser --session "${BROWSER_SESSION}" find placeholder "MM / YY" fill "12 / 40" >/dev/null 2>&1 \
        || browser_fail expiration
      agent-browser --session "${BROWSER_SESSION}" find placeholder "CVC" fill "123" >/dev/null 2>&1 \
        || browser_fail cvc
      agent-browser --session "${BROWSER_SESSION}" find placeholder "Full name on card" fill "Yscale Canary" >/dev/null 2>&1 \
        || browser_fail cardholder_name
      (agent-browser --session "${BROWSER_SESSION}" find placeholder "ZIP" fill "42424" >/dev/null 2>&1 \
        || agent-browser --session "${BROWSER_SESSION}" find placeholder "ZIP code" fill "42424" >/dev/null 2>&1 \
        || agent-browser --session "${BROWSER_SESSION}" find placeholder "Postal code" fill "42424" >/dev/null 2>&1) \
        || browser_fail postal_code
      agent-browser --session "${BROWSER_SESSION}" scrollintoview '.SubmitButton' >/dev/null 2>&1 \
        || browser_fail submit_scroll
      # Give Stripe.js a beat to run client-side validation and enable the
      # Pay button before the rect read; a still-disabled button fails
      # closed below rather than being clicked prematurely.
      agent-browser --session "${BROWSER_SESSION}" wait 500 >/dev/null 2>&1 || true
      # Derive the .SubmitButton viewport rect in the DOM and only proceed
      # when the button is enabled and the centre lies on-screen. Output is
      # a strict "x,y" pair; anything else — missing button, disabled state,
      # zero-area rect, off-viewport centre — collapses to an empty string
      # and fails closed. A stray press Enter could otherwise submit an
      # unrelated field, so the low-level move/down/up is the only trusted
      # activation path here.
      rect_xy="$(agent-browser --session "${BROWSER_SESSION}" eval '
        (function() {
          var btn = document.querySelector(".SubmitButton");
          if (!btn) return "";
          if (btn.hasAttribute("disabled")) return "";
          if (btn.getAttribute("aria-disabled") === "true") return "";
          var r = btn.getBoundingClientRect();
          if (!r) return "";
          if (!(r.width > 0) || !(r.height > 0)) return "";
          var cx = Math.round(r.left + r.width / 2);
          var cy = Math.round(r.top + r.height / 2);
          if (!(cx > 0) || !(cy > 0)) return "";
          if (cx >= window.innerWidth) return "";
          if (cy >= window.innerHeight) return "";
          return cx + "," + cy;
        })()
      ' 2>/dev/null | tr -d $' \t\n\r"\047' || true)"
      if ! [[ "${rect_xy}" =~ ^[0-9]+,[0-9]+$ ]]; then
        browser_fail submit_rect
      fi
      cx="${rect_xy%,*}"
      cy="${rect_xy#*,}"
      agent-browser --session "${BROWSER_SESSION}" mouse move "${cx}" "${cy}" >/dev/null 2>&1 \
        || browser_fail submit_move
      agent-browser --session "${BROWSER_SESSION}" mouse down >/dev/null 2>&1 \
        || browser_fail submit_down
      agent-browser --session "${BROWSER_SESSION}" mouse up >/dev/null 2>&1 \
        || browser_fail submit_up
      # The browser handoff reports only that Stripe accepted the click
      # gesture. The signed webhook, exact ledger credit and cash
      # reconciliation below are the authoritative completion checks; a
      # success-URL navigation is not fulfillment evidence and can fail when
      # the canary intentionally uses a non-application HTTPS sentinel.
      agent-browser --session "${BROWSER_SESSION}" wait 1000 >/dev/null 2>&1 || true
      write_payment_done ok
      break
    fi
    sleep 0.5
  done
) &
BROWSER_PID=$!

cd "${repo_root}"
set +e
env -i PATH="${PATH}" HOME="${HOME}" GOFLAGS="${GOFLAGS:-}" \
  "${env_common[@]}" \
  "STRIPE_WEBHOOK_SECRET=${webhook_secret}" \
  "YSCALE_STRIPE_CANARY_LIVE=I_UNDERSTAND" \
  "YSCALE_STRIPE_CANARY_CONTROL_DIR=${control_dir}" \
  "YSCALE_STRIPE_CANARY_WEBHOOK_ADDR=${listen_addr}" \
  go test "${test_args[@]}"
rc=$?
set -e

if [[ "${rc}" -eq 0 ]]; then status="pass"; else status="fail"; fi
exit "${rc}"
