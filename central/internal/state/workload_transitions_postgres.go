package state

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func preservedWorkloadTransitionExpr(table string) string {
	return fmt.Sprintf(`CASE WHEN
		COALESCE(%[1]s.data->>'CustomerID', '') = COALESCE(EXCLUDED.data->>'CustomerID', '')
		AND COALESCE(%[1]s.data->>'ClusterID', '') = COALESCE(EXCLUDED.data->>'ClusterID', '')
		AND COALESCE(%[1]s.data->>'BurstID', '') = COALESCE(EXCLUDED.data->>'BurstID', '')
	THEN CASE
		WHEN COALESCE(%[1]s.data->'FinishedAt', 'null'::jsonb) <> 'null'::jsonb
		THEN jsonb_build_object('Status', %[1]s.data->'Status', 'StartedAt', %[1]s.data->'StartedAt',
			'FinishedAt', %[1]s.data->'FinishedAt', 'Outcome', %[1]s.data->'Outcome')
		WHEN COALESCE(%[1]s.data->'StartedAt', 'null'::jsonb) <> 'null'::jsonb
		THEN jsonb_build_object('StartedAt', %[1]s.data->'StartedAt') || CASE
			WHEN COALESCE(EXCLUDED.data->'FinishedAt', 'null'::jsonb) = 'null'::jsonb
			THEN jsonb_build_object('Status', %[1]s.data->'Status') ELSE '{}'::jsonb END
		ELSE '{}'::jsonb END
	ELSE '{}'::jsonb END`, table)
}

func (p *pgPersister) transitionWorkload(ctx context.Context, req workloadTransition) (*Workload, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	predicate, key := "id = $1", req.WorkloadID
	if req.BurstID != "" {
		predicate, key = "data->>'BurstID' = $1", req.BurstID
	}
	args := []any{key}
	if req.Target != nil {
		predicate += ` AND data->>'CustomerID'=$2 AND COALESCE(data->>'ClusterID','')=$3 AND COALESCE(data->>'BurstID','')=$4`
		args = append(args, req.Target.CustomerID, req.Target.ClusterID, req.Target.BurstID)
	}
	// The burst index is not unique. Refuse ambiguity instead of terminalizing
	// whichever row LIMIT 1 happened to find. All guards run after row locking.
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT id, data FROM %s WHERE %s ORDER BY id LIMIT 2 FOR UPDATE`, tblWorkloads, predicate), args...)
	if err != nil {
		return nil, false, err
	}
	var current *Workload
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			rows.Close()
			return nil, false, err
		}
		var w *Workload
		if err := json.Unmarshal(data, &w); err != nil {
			rows.Close()
			return nil, false, fmt.Errorf("decode workload transition: %w", err)
		}
		if w == nil || w.ID != id || (req.BurstID != "" && w.BurstID != req.BurstID) || current != nil {
			rows.Close()
			return nil, false, fmt.Errorf("invalid or ambiguous workload transition binding")
		}
		current = w
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, false, err
	}
	if current == nil || !req.Target.matches(current) {
		return nil, false, ErrNotFound
	}
	applied := applyWorkloadTransition(current, req)
	if applied {
		if err := patchWorkloadTransitionTx(ctx, tx, current); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return current, applied, nil
}

func patchWorkloadTransitionTx(ctx context.Context, tx pgx.Tx, current *Workload) error {
	// Unknown metadata and independently written observations survive.
	patch, err := json.Marshal(map[string]any{
		"Status": current.Status, "StartedAt": current.StartedAt,
		"FinishedAt": current.FinishedAt, "Outcome": current.Outcome,
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET data=data || $2::jsonb, updated_at=now() WHERE id=$1`, tblWorkloads), current.ID, patch)
	return err
}
