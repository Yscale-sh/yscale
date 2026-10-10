package state

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func prepareWorkloadRetryTx(ctx context.Context, tx pgx.Tx, customer, account, source string) (WorkloadRetryPreparation, error) {
	// Membership changes and offboarding share this tenant lock. Customer
	// policy and source writes serialize through their rows, in that order.
	if err := lockMembershipTenant(ctx, tx, customer); err != nil {
		return WorkloadRetryPreparation{}, err
	}
	var data []byte
	if err := tx.QueryRow(ctx, `SELECT data FROM customers WHERE id=$1 AND NOT EXISTS (SELECT 1 FROM customer_tombstones WHERE id=$1) FOR UPDATE`, customer).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkloadRetryPreparation{}, ErrNotFound
		}
		return WorkloadRetryPreparation{}, err
	}
	var c *Customer
	if err := json.Unmarshal(data, &c); err != nil {
		return WorkloadRetryPreparation{}, err
	}
	if c == nil || c.ID != customer {
		return WorkloadRetryPreparation{}, errors.New("invalid retry customer binding")
	}
	if c.Revoked() {
		return WorkloadRetryPreparation{}, ErrNotFound
	}
	m, err := queryHumanMembership(ctx, tx, account, customer)
	if err != nil {
		return WorkloadRetryPreparation{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT data FROM workloads WHERE id=$1 AND data->>'CustomerID'=$2 FOR UPDATE`, source, customer).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkloadRetryPreparation{}, ErrNotFound
		}
		return WorkloadRetryPreparation{}, err
	}
	var w *Workload
	if err := json.Unmarshal(data, &w); err != nil {
		return WorkloadRetryPreparation{}, err
	}
	if w == nil || w.ID != source {
		return WorkloadRetryPreparation{}, errors.New("invalid retry workload binding")
	}
	return prepareWorkloadRetry(c, m, w, customer, account, source)
}

func (p *pgPersister) prepareWorkloadRetry(ctx context.Context, customer, account, source string) (WorkloadRetryPreparation, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return WorkloadRetryPreparation{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	result, err := prepareWorkloadRetryTx(ctx, tx, customer, account, source)
	if err != nil {
		return WorkloadRetryPreparation{}, err
	}
	// Only refusals are journaled by preparation. An allowed read does not
	// authorize paid work and must not emit an accepted retry decision.
	if err := insertAuditTx(ctx, tx, result.Audit); err != nil {
		return WorkloadRetryPreparation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkloadRetryPreparation{}, err
	}
	return result, nil
}

func (p *pgPersister) reserveWorkloadRetry(ctx context.Context, approval *WorkloadRetryApproval, workloadID string, candidateMicroUSD int64) (string, *AuditEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return "", nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	current, err := prepareWorkloadRetryTx(ctx, tx, approval.customer, approval.account, approval.source)
	if err != nil {
		return "", nil, err
	}
	audit, decisionErr := checkWorkloadRetryApproval(current, approval, workloadID)
	if audit == nil {
		return "", nil, decisionErr
	}
	var id string
	if decisionErr == nil {
		id, err = p.reserveAdmissionTx(ctx, tx, approval.customer, workloadID, candidateMicroUSD)
		if err != nil {
			return "", nil, err
		}
	}
	// Audit is last in the lock order and shares the reservation commit.
	// Storage failure rolls back both; business refusal commits its audit.
	if err := insertAuditTx(ctx, tx, audit); err != nil {
		return "", nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", nil, err
	}
	return id, audit, decisionErr
}
