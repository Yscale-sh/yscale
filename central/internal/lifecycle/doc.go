// Package lifecycle stores durable workload lifecycle intent.
//
// The problem class is durable orchestration of non-transactional external
// side effects. PostgreSQL can make admission, operation records, outbox rows
// and lifecycle history atomic with each other, but it cannot make a cloud
// provider create exactly once. The implementable invariant is at-least-once
// execution with stable idempotency identities, transactional outbox records
// and fencing tokens — plus, for the one operation that spends money, a lease
// that is never silently redelivered.
//
// Seam invariants:
//   - (customer_id, cluster_id, idempotency_key) maps to exactly one canonical
//     workload and requested burst.
//   - A replay with the same canonical_version and payload_hash returns the
//     original IDs without creating rows.
//   - A key reused for a different canonical version or payload hash returns
//     ErrIdempotencyConflict and performs no mutation.
//   - Provider-create and outbox work are leased with fencing tokens; only the
//     current unexpired lease token may transition leased work.
//   - An expired lease is reclaimable for OUTBOX work and deliberately NOT for a
//     provider create. Publishing twice is harmless; creating twice buys a
//     second machine. A create whose attempt died holding the lease had already
//     been handed the request, so its outcome is ambiguous and it is left for an
//     operator — only pending and failed, which prove no resource exists, are
//     redelivered.
//   - Redis or any other broker is only a publication cache. The outbox row is
//     the authoritative copy until it is acknowledged.
//   - Provider absence is recorded the moment the provider confirms it, and the
//     transition is absorbing. The post-delete work that outlives that moment —
//     releases, receipts, settlement, node cleanup — commits in the SAME
//     transaction as a provider-delete cleanup outbox event, so no failure
//     behind a confirmed deletion can return the operation or the burst to a
//     live-cost state. The two outbox consumers filter on that event type in
//     opposite directions, so exactly one of them can claim a given row.
//   - lifecycle.lifecycle_events is append-only and written in the same
//     transaction as every state change made by this package.
//
// Runtime boundary: OpenStore verifies a pre-migrated schema and performs no
// DDL, so a request worker cannot create — or diverge — the state machine it
// reads. EnsureProviderDeleteSchema and GrantRuntimePrivileges belong to the
// privileged migration job (central/cmd/yscale-lifecycle-migrate).
//
// A booking made outside this package's admission path — the durable state
// store's burst record — enters through RequestProviderDeleteForBooking, which
// projects it into the workload/burst aggregate and records delete intent in
// one call. Provider identity is written once and re-read under lock; a caller
// cannot substitute it on a replay.
//
// Admission and that projection therefore write and read the SAME burst row,
// and must spell its identity identically: provider, canonical region (see
// CanonicalProviderDeleteRegion), cloud account, SKU and provider resource. A
// burst admitted under a different spelling is a paid machine the authoritative
// delete would refuse to touch.
package lifecycle
