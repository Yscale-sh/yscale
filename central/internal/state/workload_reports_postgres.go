package state

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func (p *pgPersister) recordWorkloadReport(ctx context.Context, req workloadReportRequest) (WorkloadReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return WorkloadReport{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	// Same lock order as cancellation: credential/tenant authority, workload,
	// then booking. Late bookings wait for the workload and inherit completion.
	if err := lockMembershipTenant(ctx, tx, req.target.CustomerID); err != nil {
		return WorkloadReport{}, err
	}
	var data []byte
	if err := tx.QueryRow(ctx, `SELECT data FROM customers WHERE id=$1 AND NOT EXISTS (SELECT 1 FROM customer_tombstones WHERE id=$1) FOR UPDATE`, req.target.CustomerID).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkloadReport{}, ErrNotFound
		}
		return WorkloadReport{}, err
	}
	var c *Customer
	if err := json.Unmarshal(data, &c); err != nil {
		return WorkloadReport{}, err
	}
	if c == nil || c.ID != req.target.CustomerID {
		return WorkloadReport{}, errors.New("invalid report customer binding")
	}
	if c.Revoked() {
		return WorkloadReport{}, ErrNotFound
	}
	if err := tx.QueryRow(ctx, `SELECT data FROM workloads WHERE id=$1 AND data->>'CustomerID'=$2 FOR UPDATE`, req.target.WorkloadID, req.target.CustomerID).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkloadReport{}, ErrNotFound
		}
		return WorkloadReport{}, err
	}
	var w *Workload
	if err := json.Unmarshal(data, &w); err != nil {
		return WorkloadReport{}, err
	}
	if w == nil || w.ID != req.target.WorkloadID {
		return WorkloadReport{}, errors.New("invalid report workload binding")
	}
	// Reject authority/binding changes before depending on a provider booking.
	result, err := prepareWorkloadReport(c, w, nil, req)
	if err != nil {
		return WorkloadReport{}, err
	}
	if w.BurstID != "" {
		err := tx.QueryRow(ctx, `SELECT data FROM bursts WHERE id=$1 FOR UPDATE`, w.BurstID).Scan(&data)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return WorkloadReport{}, err
		}
		if err == nil {
			var b *Burst
			if err := json.Unmarshal(data, &b); err != nil {
				return WorkloadReport{}, err
			}
			if b == nil {
				return WorkloadReport{}, errors.New("invalid report burst")
			}
			result, err = prepareWorkloadReport(c, w, b, req)
			if err != nil {
				return WorkloadReport{}, err
			}
		}
	}
	if result.Applied {
		if err := patchWorkloadTransitionTx(ctx, tx, result.Workload); err != nil {
			return WorkloadReport{}, err
		}
	}
	if !req.transition.Start && result.Burst != nil {
		if _, err := tx.Exec(ctx, `UPDATE bursts SET data=data || jsonb_build_object('ReapPending',true,'ReapPendingStatus',$2::text), updated_at=now() WHERE id=$1`, result.Burst.ID, result.Burst.ReapPendingStatus); err != nil {
			return WorkloadReport{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkloadReport{}, err
	}
	return result, nil
}
