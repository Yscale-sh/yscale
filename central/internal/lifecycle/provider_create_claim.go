package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// ClaimProviderCreateForBurst leases the provider-create operation of ONE burst.
//
// It is the SAME saga as ClaimProviderCreate — same row, same states, same
// lease token, same fencing rule, same append-only claim event — and only the
// selector differs. The generic claim exists for a background worker draining
// whatever is due; a request-inline admission path cannot use it, because the
// operation it would be handed is whichever one happens to be oldest, and
// executing another tenant's create with this request's resolved provider
// request is the one mistake that must be impossible.
//
// claimed=false with a nil error is not a failure. The returned operation
// carries the authoritative State the caller has to act on:
//
//   - succeeded    — a machine already exists for this burst; do not buy another.
//   - dead_letter  — the create is terminally resolved and awaits an operator.
//   - processing   — a lease belongs to another attempt. LeaseExpired says
//     whether that attempt is still live or died mid-flight; NEITHER is
//     reclaimable here (see below).
//   - pending/failed with next_attempt_at in the future — backoff, retry later.
//
// An EXPIRED processing lease is deliberately not reclaimable. The lease is
// taken before the request is dispatched, so an attempt that stopped renewing it
// may have died at any point — including after a provider accepted the create.
// Reclaiming that operation means issuing a second CreateNode for a burst that
// may already own a machine, and no backend central routes to supplies an
// idempotency key that would make the second call a no-op. Failing closed leaves
// one ambiguous operation for an operator; reclaiming leaks a paid machine that
// nothing records or reaps. Only pending and failed are retried, because both
// PROVE no resource was created: pending was never dispatched, and failed is
// written by a settlement whose attempt reported its create did not land.
//
// ErrNotFound means no provider-create operation was ever admitted for this
// burst, which is an admission invariant violation at any caller that admitted
// before claiming.
func (s *Store) ClaimProviderCreateForBurst(ctx context.Context, customerID, clusterID, burstID string, lease time.Duration) (ProviderCreateOperation, bool, error) {
	if err := s.assertReady(); err != nil {
		return ProviderCreateOperation{}, false, err
	}
	if err := validateLease(lease); err != nil {
		return ProviderCreateOperation{}, false, err
	}
	for label, value := range map[string]string{
		"customer ID": customerID, "cluster ID": clusterID, "burst ID": burstID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return ProviderCreateOperation{}, false, err
		}
	}
	token, err := randomToken()
	if err != nil {
		return ProviderCreateOperation{}, false, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ProviderCreateOperation{}, false, fmt.Errorf("lifecycle: begin scoped provider-create claim: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Lock the row unconditionally rather than filtering on claimability, so a
	// refusal can name the state that refused it. SKIP LOCKED is deliberately
	// absent: this caller wants to serialize against a concurrent worker on its
	// own operation, not skip past it and report "not found".
	var op ProviderCreateOperation
	var payloadText string
	var leaseToken pgtype.Text
	var lockedUntil pgtype.Timestamptz
	var claimable bool
	err = tx.QueryRow(ctx, `
		SELECT id, customer_id, cluster_id, workload_id, burst_id, provider, region, cloud_account_id, sku,
		       payload_version, payload::text, state, attempts, lease_token, locked_until,
		       next_attempt_at,
		       (state IN ('pending','failed') AND next_attempt_at <= now()) AS claimable,
		       (state='processing' AND locked_until <= now()) AS lease_expired
		FROM lifecycle.operations
		WHERE customer_id=$1 AND cluster_id=$2 AND burst_id=$3
		  AND operation_type='provider_create'
		FOR UPDATE`, customerID, clusterID, burstID).
		Scan(&op.ID, &op.CustomerID, &op.ClusterID, &op.WorkloadID, &op.BurstID,
			&op.Provider, &op.Region, &op.CloudAccountID, &op.SKU, &op.PayloadVersion, &payloadText,
			&op.State, &op.Attempts, &leaseToken, &lockedUntil, &op.NextAttemptAt,
			&claimable, &op.LeaseExpired)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderCreateOperation{}, false, ErrNotFound
	}
	if err != nil {
		return ProviderCreateOperation{}, false, fmt.Errorf("lifecycle: read scoped provider-create operation: %w", err)
	}
	op.Payload = []byte(payloadText)
	op.LeaseToken = leaseToken.String
	op.LockedUntil = lockedUntil.Time
	if !claimable {
		return op, false, nil
	}

	priorState := op.State
	// attempts counts DELIVERY attempts, and every claim that reaches here is
	// one: the only claimable states are pending and failed, so the request is
	// about to be handed to a caller that has not been handed it before.
	//
	// The state predicate repeats the claimability rule against the row this
	// transaction already holds. It cannot fire under the lock, and that is the
	// point — it is the write itself refusing to lease anything but a state that
	// proves no resource exists, rather than trusting a check made further up.
	err = tx.QueryRow(ctx, `
		UPDATE lifecycle.operations
		SET state='processing', attempts=attempts + 1,
		    locked_until=now()+make_interval(secs => $1), lease_token=$2, updated_at=now()
		WHERE id=$3 AND state IN ('pending','failed')
		RETURNING state, attempts, lease_token, locked_until, next_attempt_at`,
		lease.Seconds(), token, op.ID).
		Scan(&op.State, &op.Attempts, &leaseToken, &lockedUntil, &op.NextAttemptAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderCreateOperation{}, false, fmt.Errorf(
			"%w: provider-create operation %d left state %q while this transaction held its lock",
			ErrInvariantViolation, op.ID, priorState)
	}
	if err != nil {
		return ProviderCreateOperation{}, false, mapWriteError("claim scoped provider-create operation", err)
	}
	op.LeaseToken = leaseToken.String
	op.LockedUntil = lockedUntil.Time

	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: op.CustomerID, ClusterID: op.ClusterID,
		AggregateType: "operation", AggregateID: fmt.Sprint(op.ID),
		EventType: "provider_create.claimed", PriorState: priorState,
		NewState: OperationProcessing, Payload: []byte(`{}`),
	}); err != nil {
		return ProviderCreateOperation{}, false, err
	}
	if err := commit(ctx, tx, "scoped provider-create claim"); err != nil {
		return ProviderCreateOperation{}, false, err
	}
	return op, true, nil
}
