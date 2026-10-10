package state

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrInvalidWorkloadReport = errors.New("invalid workload report")

// WorkloadReport is the detached result of a current-authority transition.
// For completion, any existing booking is marked for recoverable cleanup in
// that same commit. Missing bookings are not proof of provider absence.
type WorkloadReport struct {
	Workload *Workload
	Burst    *Burst
	Applied  bool
}

type workloadReportRequest struct {
	target     WorkloadTransitionTarget
	principal  WorkloadActionPrincipal
	transition workloadTransition
}

type workloadReportWriter interface {
	recordWorkloadReport(context.Context, workloadReportRequest) (WorkloadReport, error)
}

func (s *Store) RecordWorkloadStarted(ctx context.Context, target WorkloadTransitionTarget, principal WorkloadActionPrincipal, at time.Time) (WorkloadReport, error) {
	return s.recordWorkloadReport(ctx, workloadReportRequest{target: target, principal: principal, transition: workloadTransition{Start: true, At: at}})
}

// RecordWorkloadCompletion preserves the first terminal observation regardless
// of cleanup progress. Replays still repair/retry the same cleanup intent; they
// cannot rewrite a cancellation or the first completion's timestamp/receipt.
func (s *Store) RecordWorkloadCompletion(ctx context.Context, target WorkloadTransitionTarget, principal WorkloadActionPrincipal, status string, at time.Time, outcome *WorkloadOutcome) (WorkloadReport, error) {
	if status != "succeeded" && status != "failed" {
		return WorkloadReport{}, ErrInvalidWorkloadReport
	}
	return s.recordWorkloadReport(ctx, workloadReportRequest{target: target, principal: principal, transition: workloadTransition{Status: status, At: at, OnlyIfUnfinished: true, Outcome: cloneWorkloadOutcome(outcome)}})
}

func (s *Store) recordWorkloadReport(ctx context.Context, req workloadReportRequest) (WorkloadReport, error) {
	if err := ctx.Err(); err != nil {
		return WorkloadReport{}, err
	}
	if req.target.CustomerID == "" || req.target.WorkloadID == "" || req.principal.AccountID != "" || req.principal.CredentialHash == "" {
		return WorkloadReport{}, ErrNotFound
	}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return WorkloadReport{}, err
	}
	var result WorkloadReport
	var err error
	if s.persist != nil {
		writer, ok := s.persist.(workloadReportWriter)
		if !ok {
			err = errors.New("durable workload reports unavailable")
		} else {
			result, err = writer.recordWorkloadReport(ctx, req)
		}
	} else if s.tombstoned[req.target.CustomerID] {
		err = ErrNotFound
	} else {
		w := s.workloads[req.target.WorkloadID]
		var b *Burst
		if w != nil {
			b = s.bursts[w.BurstID]
		}
		result, err = prepareWorkloadReport(s.customers[req.target.CustomerID], w, b, req)
	}
	if err != nil {
		s.mu.Unlock()
		if errors.Is(err, ErrNotFound) {
			return WorkloadReport{}, ErrNotFound
		}
		s.recordPersistenceFailure("workload", "report", err)
		return WorkloadReport{}, fmt.Errorf("%w: record workload report: %w", ErrPersistence, err)
	}
	if cached := s.workloads[req.target.WorkloadID]; workloadIdentityMatches(cached, result.Workload) {
		copyWorkloadTransition(cached, result.Workload)
	}
	if b := result.Burst; b != nil && !req.transition.Start {
		if cached := s.bursts[b.ID]; cached != nil && sameBurstResource(cached, b) {
			copy := cloneBurstSnapshot(cached)
			copy.ReapPending, copy.ReapPendingStatus = b.ReapPending, b.ReapPendingStatus
			s.bursts[b.ID] = copy
		}
	}
	s.mu.Unlock()
	if result.Applied {
		if req.transition.Start {
			s.observeWorkload(result.Workload, ActionWorkloadStarted, "running")
		} else {
			s.observeWorkloadTerminal(result.Workload, result.Workload.Status)
		}
	}
	return result, nil
}

func prepareWorkloadReport(c *Customer, w *Workload, b *Burst, req workloadReportRequest) (WorkloadReport, error) {
	if c == nil || c.ID != req.target.CustomerID || c.Revoked() || w == nil || !req.target.matches(w) || !workloadCredentialAuthorized(c, w, req.principal) {
		return WorkloadReport{}, ErrNotFound
	}
	result := WorkloadReport{Workload: cloneWorkload(w)}
	if b != nil {
		if !workloadBurstBindingMatches(w, b, req.principal.ClusterID) {
			return WorkloadReport{}, errors.New("invalid report burst binding")
		}
		result.Burst = cloneBurstSnapshot(b)
	}
	result.Applied = applyWorkloadTransition(result.Workload, req.transition)
	if !req.transition.Start && result.Burst != nil {
		result.Burst.ReapPending = true
		if result.Burst.ReapPendingStatus == "" {
			result.Burst.ReapPendingStatus = result.Workload.Status
		}
	}
	return result, nil
}
