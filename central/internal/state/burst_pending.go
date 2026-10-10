package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func sameBurstResource(a, b *Burst) bool {
	return a.ID == b.ID && a.CustomerID == b.CustomerID && a.ClusterID == b.ClusterID &&
		a.Backend == b.Backend && a.BackendID == b.BackendID && a.Region == b.Region &&
		a.CloudAccountID == b.CloudAccountID && a.SKU == b.SKU
}

// A pending teardown cannot be erased or redirected by a stale whole-booking
// writer. A late first booking behind a terminal workload joins the same
// existing recovery path, including when cancellation preceded provider create.
func preservePendingBurst(incoming, stored *Burst, workload *Workload) error {
	if stored != nil && stored.ReapPending {
		if stored.CustomerID != incoming.CustomerID || stored.ClusterID != incoming.ClusterID ||
			(stored.BackendID != "" && !sameBurstResource(stored, incoming)) {
			return errors.New("pending cleanup booking identity conflict")
		}
		incoming.ReapPending = true
		incoming.ReapPendingStatus = stored.ReapPendingStatus
		// Once a concrete resource is pending deletion, stale provisioning writes
		// cannot change what will be settled or which ancillary resources it owns.
		if stored.BackendID != "" {
			incoming.CreatedAt, incoming.HourlyUSD = stored.CreatedAt, stored.HourlyUSD
			manual := incoming.Billing != nil && stored.Billing != nil && incoming.Billing.HoldID == stored.Billing.HoldID && incoming.Billing.WorkloadRef == stored.Billing.WorkloadRef && incoming.Billing.ManualAttention
			incoming.Billing = cloneReadPointer(stored.Billing)
			if manual {
				incoming.Billing.ManualAttention = true
			}
			if stored.TerminalCost != nil {
				incoming.TerminalCost = cloneReadPointer(stored.TerminalCost)
			}
			incoming.AgentID, incoming.NodeName = stored.AgentID, stored.NodeName
			incoming.TSHostname, incoming.PodCIDR = stored.TSHostname, stored.PodCIDR
			incoming.MeshProvider, incoming.MeshLoginServer = stored.MeshProvider, stored.MeshLoginServer
		}
	}
	if workload != nil && workload.FinishedAt != nil {
		if workload.BurstID != incoming.ID || workload.CustomerID != incoming.CustomerID ||
			(workload.ClusterID != "" && incoming.ClusterID != "" && workload.ClusterID != incoming.ClusterID) {
			return errors.New("terminal workload booking identity conflict")
		}
		incoming.ReapPending = true
		if incoming.ReapPendingStatus == "" {
			incoming.ReapPendingStatus = workload.Status
		}
	}
	return nil
}

func (p *pgPersister) upsertBurst(b *Burst) error {
	copy := cloneBurstSnapshot(b)
	err := p.inTx("write burst with pending cleanup", func(ctx context.Context, tx pgx.Tx) error {
		// Cancellation locks workload before burst. The same order makes a late
		// provider result see either the prior cancellation or a burst the cancel
		// transaction will mark. Do not hold either lock over provider calls.
		rows, err := tx.Query(ctx, `SELECT id,data FROM workloads WHERE data->>'BurstID'=$1 ORDER BY id LIMIT 2 FOR UPDATE`, b.ID)
		if err != nil {
			return err
		}
		var workload *Workload
		for rows.Next() {
			var id string
			var data []byte
			if err := rows.Scan(&id, &data); err != nil {
				rows.Close()
				return err
			}
			var current *Workload
			if err := json.Unmarshal(data, &current); err != nil {
				rows.Close()
				return err
			}
			if workload != nil || current == nil || current.ID != id || current.BurstID != b.ID {
				rows.Close()
				return errors.New("invalid or ambiguous burst workload binding")
			}
			workload = current
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		// Reserve an absent row without overwriting a concurrent first booking.
		// The following locked read sees the winner even if its INSERT committed
		// after our workload lookup. A pre-read plus blind UPSERT loses that case.
		data, err := json.Marshal(copy)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO bursts (id,data,updated_at) VALUES ($1,$2,now()) ON CONFLICT (id) DO NOTHING`, b.ID, data); err != nil {
			return err
		}
		var stored *Burst
		err = tx.QueryRow(ctx, `SELECT data FROM bursts WHERE id=$1 FOR UPDATE`, b.ID).Scan(&data)
		if err != nil {
			return err
		}
		if err == nil {
			if err := json.Unmarshal(data, &stored); err != nil {
				return err
			}
			if stored == nil || stored.ID != b.ID {
				return errors.New("invalid pending cleanup booking")
			}
		}
		if err := preservePendingBurst(copy, stored, workload); err != nil {
			return err
		}
		data, err = json.Marshal(copy)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE bursts SET data=$2, updated_at=now() WHERE id=$1`, b.ID, data)
		return err
	})
	if err != nil {
		return fmt.Errorf("%w: write burst: %w", ErrPersistence, err)
	}
	*b = *copy
	return nil
}
