// Package billing provides a prepaid-credit ledger for yscale's GPU/CPU burst
// SaaS. All monetary amounts are integer micro-USD (1 USD = 1 000 000 µUSD)
// stored as int64. No floating-point arithmetic is ever performed on money.
//
// The package is backed by Postgres (pgx v5) and uses SQL constraints, row
// locks, and immutable ledger entries to guarantee:
//
//   - Tenant isolation: every account, hold, and ledger entry is scoped to a
//     customer_id and enforced by foreign-key + CHECK constraints.
//   - Crash consistency: all balance-mutating operations use row-locked
//     Postgres transactions.
//   - Idempotency: every grant, reserve, capture, release, expiry, and reversal
//     owns one key; a retry succeeds only when the complete semantic payload
//     matches.
//   - No overspend: concurrent reservations are serialized by
//     SELECT … FOR UPDATE on the account row, and a CHECK constraint ensures
//     the spendable balance never goes negative.
//
// This package does NOT expose HTTP routes, does NOT import the Stripe SDK, and
// does NOT wire into the existing JSONB state tables. It is designed to be
// integrated with handlers/controller in a subsequent, separately-gated change.
//
// See docs/architecture/credits-and-payments.md for the internal architecture
// reference, docs/operations/billing-reconciliation.md for internal runbooks,
// and docs/customer/billing-and-credits.md for the customer-safe preview.
package billing
