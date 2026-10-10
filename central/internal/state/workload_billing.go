package state

import (
	"context"
	"fmt"
	"time"
)

type workloadBillingManualAttentionPersister interface {
	markWorkloadBillingManualAttention(context.Context, string, string, int64) (bool, error)
}

// MarkWorkloadBillingManualAttention durably protects one exact paid workload
// association after its automatic economic repair budget is exhausted. It
// patches only Billing.ManualAttention so a stale lifecycle copy cannot erase
// a concurrent cost observation or status update.
func (s *Store) MarkWorkloadBillingManualAttention(
	ctx context.Context,
	customerID string,
	workloadRef string,
	holdID int64,
) (bool, error) {
	if customerID == "" || workloadRef == "" || holdID <= 0 {
		return false, fmt.Errorf("state: customer, workload reference, and hold are required for billing manual attention")
	}

	s.mu.Lock()
	p := s.persist
	mirrored := false
	if workload, ok := s.workloads[workloadRef]; ok &&
		workload.CustomerID == customerID && workload.Billing != nil &&
		workload.Billing.WorkloadRef == workloadRef && workload.Billing.HoldID == holdID {
		workload.Billing.ManualAttention = true
		mirrored = true
	}
	s.mu.Unlock()

	if p == nil {
		return mirrored, nil
	}
	marker, ok := p.(workloadBillingManualAttentionPersister)
	if !ok {
		return false, fmt.Errorf("state: durable billing manual-attention writer is unavailable")
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	marked, err := marker.markWorkloadBillingManualAttention(persistCtx, customerID, workloadRef, holdID)
	if err != nil {
		s.recordPersistenceFailure("workload_billing", "manual_attention", err)
		return false, err
	}
	return marked, nil
}

func markWorkloadBillingManualAttentionStmt(table string) string {
	return fmt.Sprintf(
		`UPDATE %[1]s
		 SET data = jsonb_set(data, '{Billing,ManualAttention}', 'true'::jsonb), updated_at = now()
		 WHERE id = $2
		   AND data->>'CustomerID' = $1
		   AND data->'Billing'->>'WorkloadRef' = $2
		   AND (data->'Billing'->>'HoldID')::bigint = $3`, table)
}

func (p *pgPersister) markWorkloadBillingManualAttention(
	ctx context.Context,
	customerID string,
	workloadRef string,
	holdID int64,
) (bool, error) {
	tag, err := p.pool.Exec(ctx, markWorkloadBillingManualAttentionStmt(tblWorkloads),
		customerID, workloadRef, holdID)
	if err != nil {
		return false, fmt.Errorf("mark workload %s billing manual attention: %w", workloadRef, err)
	}
	return tag.RowsAffected() == 1, nil
}
