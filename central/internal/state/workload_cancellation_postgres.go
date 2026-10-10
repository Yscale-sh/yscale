package state

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func (p *pgPersister) requestWorkloadCancellation(ctx context.Context, req workloadCancellationRequest) (WorkloadCancellation, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return WorkloadCancellation{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	// Same tenant lock as membership changes, credential writes and offboarding.
	// Workload then burst locks also order cancellation against late bookings.
	if err := lockMembershipTenant(ctx, tx, req.customer); err != nil {
		return WorkloadCancellation{}, err
	}
	var data []byte
	if err := tx.QueryRow(ctx, `SELECT data FROM customers WHERE id=$1 AND NOT EXISTS (SELECT 1 FROM customer_tombstones WHERE id=$1) FOR UPDATE`, req.customer).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkloadCancellation{}, ErrNotFound
		}
		return WorkloadCancellation{}, err
	}
	var customer *Customer
	if err := json.Unmarshal(data, &customer); err != nil {
		return WorkloadCancellation{}, err
	}
	if customer == nil || customer.ID != req.customer {
		return WorkloadCancellation{}, errors.New("invalid cancellation customer binding")
	}
	if customer.Revoked() {
		return WorkloadCancellation{}, ErrNotFound
	}
	var member *TenantMembership
	if req.principal.AccountID != "" {
		member, err = queryHumanMembership(ctx, tx, req.principal.AccountID, req.customer)
		if err != nil {
			return WorkloadCancellation{}, err
		}
	}
	if err := tx.QueryRow(ctx, `SELECT data FROM workloads WHERE id=$1 AND data->>'CustomerID'=$2 FOR UPDATE`, req.workload, req.customer).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkloadCancellation{}, ErrNotFound
		}
		return WorkloadCancellation{}, err
	}
	var w *Workload
	if err := json.Unmarshal(data, &w); err != nil {
		return WorkloadCancellation{}, err
	}
	if w == nil || w.ID != req.workload {
		return WorkloadCancellation{}, errors.New("invalid cancellation workload binding")
	}
	// Decide authority before touching the provider booking. A refused caller
	// must not lock a sibling resource or depend on its decodability.
	result, err := prepareWorkloadCancellation(customer, member, w, nil, req)
	if err != nil {
		return WorkloadCancellation{}, err
	}
	var b *Burst
	if result.Decision.Allowed && w.BurstID != "" {
		err := tx.QueryRow(ctx, `SELECT data FROM bursts WHERE id=$1 FOR UPDATE`, w.BurstID).Scan(&data)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return WorkloadCancellation{}, err
		}
		if err == nil {
			if err := json.Unmarshal(data, &b); err != nil {
				return WorkloadCancellation{}, err
			}
			if b == nil {
				return WorkloadCancellation{}, errors.New("invalid cancellation burst")
			}
		}
	}
	if b != nil {
		result, err = prepareWorkloadCancellation(customer, member, w, b, req)
		if err != nil {
			return WorkloadCancellation{}, err
		}
	}
	if result.Decision.Allowed {
		if result.applied {
			if err := patchWorkloadTransitionTx(ctx, tx, result.Workload); err != nil {
				return WorkloadCancellation{}, err
			}
		}
		if result.Burst != nil {
			if _, err := tx.Exec(ctx, `UPDATE bursts SET data=data || jsonb_build_object('ReapPending',true,'ReapPendingStatus',$2::text), updated_at=now() WHERE id=$1`, result.Burst.ID, result.Burst.ReapPendingStatus); err != nil {
				return WorkloadCancellation{}, err
			}
		}
	}
	// Audit is last in the lock order and shares the state commit. Failure rolls
	// back the cancellation and pending cleanup, including a denied decision.
	if err := insertAuditTx(ctx, tx, result.Audit); err != nil {
		return WorkloadCancellation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkloadCancellation{}, err
	}
	return result, nil
}
