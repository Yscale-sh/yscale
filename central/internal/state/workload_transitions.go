package state

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type workloadTransition struct {
	WorkloadID       string
	BurstID          string
	Target           *WorkloadTransitionTarget
	Start            bool
	Status           string
	At               time.Time
	OnlyIfUnfinished bool
	Outcome          *WorkloadOutcome
}

// WorkloadTransitionTarget binds a transition to the identity the caller
// authorized. Even an empty legacy cluster/burst binding is checked exactly.
// This is not a human or connector authorization grant.
type WorkloadTransitionTarget struct {
	WorkloadID string
	CustomerID string
	ClusterID  string
	BurstID    string
}

func (target *WorkloadTransitionTarget) matches(w *Workload) bool {
	return target == nil || (w.ID == target.WorkloadID && w.CustomerID == target.CustomerID && w.ClusterID == target.ClusterID && w.BurstID == target.BurstID)
}

// Optional for legacy test backends, but a configured backend without this
// operation fails closed. A read followed by an upsert cannot elect a winner.
type workloadTransitionWriter interface {
	transitionWorkload(context.Context, workloadTransition) (*Workload, bool, error)
}

// StartWorkloadContext evaluates the first-start/never-unfinish guards
// against current durable state. The returned record is detached and belongs
// to the exact authorized target. Missing/foreign/rebound records are
// ErrNotFound, not successful no-ops.
func (s *Store) StartWorkloadContext(ctx context.Context, target WorkloadTransitionTarget, at time.Time) (*Workload, bool, error) {
	if target.CustomerID == "" || target.WorkloadID == "" {
		return nil, false, ErrNotFound
	}
	return s.transitionWorkload(ctx, workloadTransition{WorkloadID: target.WorkloadID, Target: &target, Start: true, At: at})
}

// FinishWorkloadContext commits status, timestamp and receipt together.
// Authorization remains the caller's responsibility; the identity predicates
// bind the mutation itself and the returned record to the authorized target.
func (s *Store) FinishWorkloadContext(ctx context.Context, target WorkloadTransitionTarget, status string, at time.Time, onlyIfUnfinished bool, outcome *WorkloadOutcome) (*Workload, bool, error) {
	if target.CustomerID == "" || target.WorkloadID == "" {
		return nil, false, ErrNotFound
	}
	return s.transitionWorkload(ctx, workloadTransition{WorkloadID: target.WorkloadID, Target: &target, Status: status, At: at, OnlyIfUnfinished: onlyIfUnfinished, Outcome: cloneWorkloadOutcome(outcome)})
}

// applyWorkloadTransition is shared by the locked in-memory and PostgreSQL
// paths. onlyIfUnfinished preserves the entire existing terminal observation;
// an explicit replacement keeps the first receipt for the same status and
// drops/replaces it when the reported status changes.
func applyWorkloadTransition(w *Workload, req workloadTransition) bool {
	if req.Start {
		if w.StartedAt != nil || w.FinishedAt != nil {
			return false
		}
		w.StartedAt = cloneReadPointer(&req.At)
		w.Status = "running"
		return true
	}
	if req.OnlyIfUnfinished && w.FinishedAt != nil {
		return false
	}
	switch {
	case req.Outcome != nil && (w.Outcome == nil || w.Status != req.Status):
		w.Outcome = cloneWorkloadOutcome(req.Outcome)
	case w.Outcome != nil && w.Status != req.Status:
		w.Outcome = nil
	}
	w.Status = req.Status
	w.FinishedAt = cloneReadPointer(&req.At)
	return true
}

func copyWorkloadTransition(dst, src *Workload) {
	dst.Status = src.Status
	dst.StartedAt = cloneReadPointer(src.StartedAt)
	dst.FinishedAt = cloneReadPointer(src.FinishedAt)
	dst.Outcome = cloneWorkloadOutcome(src.Outcome)
}

// Whole-record provisioning writes may arrive after connector observations.
// They cannot undo those observations. Deliberate terminal replacement uses
// FinishWorkloadContext/FinishWorkloadWithOutcome, not a stale full document.
func preserveWorkloadTransition(incoming, stored *Workload) {
	if incoming == nil || stored == nil || incoming.ID != stored.ID || incoming.CustomerID != stored.CustomerID || incoming.ClusterID != stored.ClusterID || incoming.BurstID != stored.BurstID {
		return
	}
	if stored.FinishedAt != nil {
		copyWorkloadTransition(incoming, stored)
	} else if stored.StartedAt != nil {
		incoming.StartedAt = cloneReadPointer(stored.StartedAt)
		if incoming.FinishedAt == nil {
			incoming.Status = stored.Status
		}
	}
}

func (s *Store) transitionWorkload(ctx context.Context, req workloadTransition) (*Workload, bool, error) {
	if (req.WorkloadID == "") == (req.BurstID == "") {
		return nil, false, ErrNotFound
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, false, err
	}
	var current *Workload
	var applied bool
	var err error
	if s.persist != nil {
		writer, ok := s.persist.(workloadTransitionWriter)
		if !ok {
			err = errors.New("durable workload transitions unavailable")
		} else {
			current, applied, err = writer.transitionWorkload(ctx, req)
		}
	} else {
		if req.WorkloadID != "" {
			current = s.workloads[req.WorkloadID]
		} else {
			for _, w := range s.workloads {
				if w.BurstID == req.BurstID {
					if current != nil {
						err = errors.New("multiple workloads bind the same burst")
						break
					}
					current = w
				}
			}
		}
		if err == nil && current != nil {
			if !req.Target.matches(current) {
				current = nil
			} else {
				current = cloneWorkload(current)
				applied = applyWorkloadTransition(current, req)
			}
		}
	}
	if err != nil {
		s.mu.Unlock()
		if errors.Is(err, ErrNotFound) {
			return nil, false, ErrNotFound
		}
		s.recordPersistenceFailure("workload", "transition", err)
		return nil, false, fmt.Errorf("%w: workload transition: %w", ErrPersistence, err)
	}
	if current == nil {
		s.mu.Unlock()
		return nil, false, ErrNotFound
	}
	// Publish only after commit and only these fields. Do not hydrate an entire
	// durable snapshot into the mutation cache or replace unrelated observations.
	if cached := s.workloads[current.ID]; cached != nil && cached.CustomerID == current.CustomerID && cached.ClusterID == current.ClusterID && cached.BurstID == current.BurstID {
		copyWorkloadTransition(cached, current)
	}
	observed := cloneWorkload(current)
	s.mu.Unlock()
	if applied {
		switch {
		case req.Start:
			s.observeWorkload(observed, ActionWorkloadStarted, "running")
		case req.BurstID != "":
			s.observeWorkload(observed, ActionWorkloadReaped, req.Status)
		default:
			s.observeWorkloadTerminal(observed, req.Status)
		}
	}
	return observed, applied, nil
}
