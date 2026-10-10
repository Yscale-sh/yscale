package state

import (
	"context"
	"time"
)

// WorkloadCostBasisRateRuntime names what the numbers in a WorkloadCost mean:
// the quoted hourly rate multiplied by the runtime measured from the burst's
// creation to the moment a reaper's claim on it was accepted. It is a STABLE
// string, stored on every observation and rendered to the customer, so a
// consumer reading an old record can tell which rule produced it. Changing how
// the number is derived means a new basis, never a redefinition of this one.
//
// What it is not: an invoice. Nothing here consults a price list, a plan, a
// discount or a ledger — it is the upstream rate central was quoted when the
// node was booked, held against how long the node existed.
const WorkloadCostBasisRateRuntime = "hourly_rate_x_runtime_to_reap_claim"

// WorkloadCostBasisRateRuntimeToProviderDelete is the authoritative lifecycle
// basis: upstream cost continues through queued/retrying/manual-attention and
// freezes only when the provider-delete receipt is committed.
const WorkloadCostBasisRateRuntimeToProviderDelete = "hourly_rate_x_runtime_to_provider_delete"

// WorkloadCost is the durable historical observation of what one workload's
// burst cost upstream. It is written once, by the single reaper that wins the
// burst's claim and then successfully hands teardown on (durably enqueued) or
// completes it inline — the same two paths that accrue the cost meter. A reap
// that fails and re-queues the burst records nothing, because the burst is
// still running and its cost is not final.
//
// Every field the observation needs to be read back years later is stored on
// it rather than joined at read time: the burst it came from is deleted by the
// claim that froze this, so Backend and HourlyUSD have no other source once
// the reap returns.
type WorkloadCost struct {
	// EstimatedUSD is HourlyUSD x Runtime, computed once at FrozenAt. Estimated
	// because the upstream provider's own invoice is the only authority on what
	// was actually charged; this is what central quoted and measured.
	EstimatedUSD float64
	// HourlyUSD is the rate the burst was booked at, carried so the estimate
	// stays reconstructible when a backend's pricing later moves.
	HourlyUSD float64
	// Runtime is the measured lifetime, never negative. A burst whose CreatedAt
	// is ahead of the reaping replica's clock reads as zero rather than as a
	// credit.
	Runtime time.Duration
	// FrozenAt is the UTC instant the observation was taken — the reap's own
	// moment, not when a broker later published or a statement later read it.
	FrozenAt time.Time
	Backend  string
	BurstID  string
	// Basis is WorkloadCostBasisRateRuntime on everything this code writes.
	Basis string
}

// RecordWorkloadCostForBurst freezes the cost observation onto the workload the
// burst named in c.BurstID backed. Reports whether this call recorded it.
//
// First write wins, in memory and durably. The teardown claim already elects
// one reaper per burst, so a second observation means a redelivered job or a
// retried reap of a burst that was reaped once — and the first record is the
// one taken at the moment the node stopped. Overwriting it would move a frozen
// timestamp forward every time a queue redelivered.
//
// Both halves are written because they answer different readers. The durable
// row is what every other replica, every restart and every later statement
// reads; this replica's map is what its own API serves until it restarts. A
// reap running on a replica that never held the workload writes only the
// durable half — the same cross-replica limit FinishWorkloadForBurst documents.
//
// A durable failure is REPORTED, not swallowed, and leaves the in-memory
// mirror in place: this process's view is correct either way, and the caller —
// which is a teardown that already happened — decides what a lost observation
// is worth. It must never decide to undo the teardown over it.
func (s *Store) RecordWorkloadCostForBurst(ctx context.Context, c WorkloadCost) (bool, error) {
	return s.recordWorkloadCostForBurst(ctx, c, false)
}

// EnsureWorkloadCostForBurst is the teardown-worker retry form. Unlike Record,
// an in-memory observation does not short-circuit it: that mirror may have been
// installed just before a failed Postgres write. It returns true only when the
// authoritative store confirms that this burst has a durable observation.
func (s *Store) EnsureWorkloadCostForBurst(ctx context.Context, c WorkloadCost) (bool, error) {
	return s.recordWorkloadCostForBurst(ctx, c, true)
}

func (s *Store) recordWorkloadCostForBurst(ctx context.Context, c WorkloadCost, ensureDurable bool) (bool, error) {
	if c.BurstID == "" {
		// Nothing to key on. A blank id would match every workload that never
		// got a burst, which is the opposite of an observation.
		return false, nil
	}

	s.mu.Lock()
	p := s.persist
	var mirrored bool
	observation := c
	for _, w := range s.workloads {
		if w.BurstID != c.BurstID {
			continue
		}
		if w.Cost != nil {
			if !ensureDurable {
				s.mu.Unlock()
				return false, nil
			}
			// Preserve the first observation on a retry. The in-memory copy does
			// not prove Postgres accepted it: a prior write may have failed after
			// this mirror was installed.
			observation = *w.Cost
			mirrored = true
			break
		}
		observed := c
		w.Cost = &observed
		mirrored = true
		break
	}
	s.mu.Unlock()

	if p == nil {
		// One process: its map IS the workload set.
		return mirrored, nil
	}
	// Teardown has already been accepted or completed at this point. The HTTP
	// request that initiated it may be cancelled as soon as the response can be
	// returned, but that must not cancel the historical write. Detach
	// cancellation while retaining the store's normal bounded operation budget.
	// A database failure is still returned for the caller to log and never rolls
	// teardown back.
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	recorded, err := p.recordWorkloadCost(persistCtx, c.BurstID, &observation)
	if err != nil {
		s.recordPersistenceFailure("workload_cost", "record", err)
		return mirrored, err
	}
	if recorded {
		return true, nil
	}
	if !ensureDurable {
		return mirrored, nil
	}
	// UPDATE ... WHERE Cost IS NULL returning zero rows means either no
	// workload matched or another attempt already made the receipt durable.
	// Read it back to distinguish those outcomes before a queue item is acked.
	w, err := p.workloadByBurst(persistCtx, c.BurstID)
	if err != nil {
		s.recordPersistenceFailure("workload_cost", "read_after_record", err)
		return false, err
	}
	if w == nil || w.Cost == nil {
		return false, err
	}
	return w.Cost.BurstID == c.BurstID, nil
}
