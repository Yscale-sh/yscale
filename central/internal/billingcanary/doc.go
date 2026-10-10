// yscale:proprietary

// Package billingcanary is the Stripe test-mode canary harness for the
// prepaid-credit purchase seam. It composes the same production packages an
// activated deployment wires — handlers.Accounts, handlers.BillingWebhooks,
// billing.StripeGateway, billing.WebhookProcessor, billing.Store, statement
// reads, and reconciliation — against an isolated Postgres database created
// solely for this canary. It never runs against a shared production or dev
// database, and it never touches Stripe livemode.
//
// The canary is expressed as a build-tagged integration test in this same
// package so it can legally import central/internal/billing and
// central/internal/handlers. Ordinary go builds and tests do not pick it up;
// only the wrapper script scripts/billing-canary.sh, which also coordinates a
// browser-driven Stripe hosted-checkout completion through a private control
// directory, ever passes -tags=stripecanary. See the runbook at
// docs/operations/billing-reconciliation.md for the exact command.
package billingcanary
