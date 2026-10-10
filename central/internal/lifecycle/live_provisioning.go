package lifecycle

import (
	"context"
	"fmt"
	"time"
)

// LiveProvisioningRecord is the bounded, non-secret projection of a
// provider-create operation that may currently hold a paid resource.
type LiveProvisioningRecord struct {
	BurstID   string
	State     string
	CreatedAt time.Time
}

// LiveProvisioningOps returns every provider-create operation that is in-flight
// or may still hold a paid resource: processing, succeeded, and dead_letter,
// joined to lifecycle.bursts and excluding terminated bursts. Only bounded
// non-secret fields are returned; payload and provider error are never read.
//
// A terminated lifecycle burst is excluded because teardown has resolved it.
// manual_attention / dead_letter bursts remain visible until operator or
// teardown resolves the lifecycle burst — that is the correct behavior.
func (s *Store) LiveProvisioningOps(ctx context.Context) ([]LiveProvisioningRecord, error) {
	if err := s.assertReady(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT o.burst_id, o.state, o.created_at
		FROM lifecycle.operations o
		JOIN lifecycle.bursts b
		  ON o.customer_id = b.customer_id
		 AND o.cluster_id  = b.cluster_id
		 AND o.burst_id    = b.id
		WHERE o.operation_type = 'provider_create'
		  AND o.state IN ('processing','succeeded','dead_letter')
		  AND b.state <> 'terminated'
	`)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: live provisioning ops: %w", err)
	}
	defer rows.Close()

	var out []LiveProvisioningRecord
	for rows.Next() {
		var r LiveProvisioningRecord
		if err := rows.Scan(&r.BurstID, &r.State, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("lifecycle: scan live provisioning op: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lifecycle: live provisioning ops iteration: %w", err)
	}
	return out, nil
}
