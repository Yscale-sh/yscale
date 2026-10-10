package state

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WorkloadCancelPrincipal preserves the existing cancellation API name.
type WorkloadCancelPrincipal = WorkloadActionPrincipal

type WorkloadCancellation struct {
	Decision WorkloadCancelDecision
	Workload *Workload
	Burst    *Burst
	applied  bool
	Audit    *AuditEvent
}

type workloadCancellationRequest struct {
	customer, workload string
	principal          WorkloadCancelPrincipal
	at                 time.Time
}

type workloadCancellationWriter interface {
	requestWorkloadCancellation(context.Context, workloadCancellationRequest) (WorkloadCancellation, error)
}

// RequestWorkloadCancellation commits current authorization, the accepted
// audit, terminal status and the existing pending-reap marker together. The
// watchdog replays that marker into the lifecycle delete machine. No provider
// or lifecycle-database operation runs while the authorization locks are held.
func (s *Store) RequestWorkloadCancellation(ctx context.Context, customerID, workloadID string, principal WorkloadCancelPrincipal) (WorkloadCancellation, error) {
	if err := ctx.Err(); err != nil {
		return WorkloadCancellation{}, err
	}
	if customerID == "" || workloadID == "" {
		return WorkloadCancellation{}, ErrNotFound
	}
	req := workloadCancellationRequest{customer: customerID, workload: workloadID, principal: principal, at: time.Now().UTC()}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return WorkloadCancellation{}, err
	}
	var result WorkloadCancellation
	var err error
	if s.persist != nil {
		writer, ok := s.persist.(workloadCancellationWriter)
		if !ok {
			err = errors.New("durable workload cancellation unavailable")
		} else {
			result, err = writer.requestWorkloadCancellation(ctx, req)
		}
	} else {
		var member *TenantMembership
		if s.tombstoned[customerID] {
			err = ErrNotFound
		}
		if err == nil && principal.AccountID != "" {
			member, err = s.cachedMembershipForLocked(principal.AccountID, customerID)
		}
		if err == nil {
			w := s.workloads[workloadID]
			var b *Burst
			if w != nil {
				b = s.bursts[w.BurstID]
			}
			result, err = prepareWorkloadCancellation(s.customers[customerID], member, w, b, req)
		}
	}
	if err != nil {
		s.mu.Unlock()
		if errors.Is(err, ErrNotFound) {
			return WorkloadCancellation{}, ErrNotFound
		}
		return WorkloadCancellation{}, fmt.Errorf("%w: request workload cancellation: %w", ErrPersistence, err)
	}
	if result.Decision.Allowed {
		if cached := s.workloads[workloadID]; cached != nil && workloadIdentityMatches(cached, result.Workload) {
			copyWorkloadTransition(cached, result.Workload)
		}
		if b := result.Burst; b != nil {
			if cached := s.bursts[b.ID]; cached != nil && sameBurstResource(cached, b) {
				copy := cloneBurstSnapshot(cached)
				copy.ReapPending, copy.ReapPendingStatus = b.ReapPending, b.ReapPendingStatus
				s.bursts[b.ID] = copy
			}
		}
	}
	s.mu.Unlock()
	if result.applied {
		s.observeWorkloadTerminal(result.Workload, result.Workload.Status)
	}
	return result, nil
}

func workloadIdentityMatches(a, b *Workload) bool {
	return a != nil && b != nil && a.ID == b.ID && a.CustomerID == b.CustomerID && a.ClusterID == b.ClusterID && a.BurstID == b.BurstID
}

func terminalWorkloadBindingConflict(incoming, stored *Workload) bool {
	return stored != nil && stored.FinishedAt != nil && !workloadIdentityMatches(incoming, stored)
}

func prepareWorkloadCancellation(c *Customer, member *TenantMembership, w *Workload, b *Burst, req workloadCancellationRequest) (WorkloadCancellation, error) {
	if c == nil || c.ID != req.customer || c.Revoked() || w == nil || w.ID != req.workload || w.CustomerID != req.customer {
		return WorkloadCancellation{}, ErrNotFound
	}
	principal := req.principal
	decision := WorkloadCancelDecision{Allowed: true, WorkloadID: w.ID, BurstID: w.BurstID}
	if principal.AccountID != "" {
		if principal.ClusterID != "" || principal.CredentialHash != "" || member == nil || member.AccountID != principal.AccountID || member.CustomerID != c.ID {
			return WorkloadCancellation{}, ErrNotFound
		}
		decision = humanWorkloadCancelDecision(w, principal.AccountID, member.Role)
	} else {
		if !workloadCredentialAuthorized(c, w, principal) {
			return WorkloadCancellation{}, ErrNotFound
		}
	}
	result := WorkloadCancellation{Decision: decision, Workload: cloneWorkload(w)}
	if principal.AccountID != "" {
		outcome := OutcomeDenied
		if decision.Allowed {
			outcome = OutcomeAccepted
		}
		result.Audit = NewAuditEvent(AuditEvent{CustomerID: c.ID, Actor: HumanActor(principal.AccountID, c.ID), Action: ActionWorkloadCancel,
			Outcome: outcome, TargetKind: TargetWorkload, TargetID: w.ID, Detail: AuditDetail{Reason: decision.Reason, Role: decision.Role, BurstID: w.BurstID}})
		if err := validateAudit(result.Audit); err != nil {
			return WorkloadCancellation{}, err
		}
	}
	if !decision.Allowed {
		return result, nil
	}
	if b != nil {
		if !workloadBurstBindingMatches(w, b, principal.ClusterID) {
			return WorkloadCancellation{}, errors.New("invalid cancellation burst binding")
		}
		result.Burst = cloneBurstSnapshot(b)
		result.Burst.ReapPending = true
		if result.Burst.ReapPendingStatus == "" {
			result.Burst.ReapPendingStatus = "cancelled"
		}
	}
	result.applied = applyWorkloadTransition(result.Workload, workloadTransition{Status: "cancelled", At: req.at, OnlyIfUnfinished: true})
	return result, nil
}

func humanWorkloadCancelDecision(w *Workload, accountID, role string) WorkloadCancelDecision {
	decision := WorkloadCancelDecision{Role: role, WorkloadID: w.ID, BurstID: w.BurstID}
	if w.SubmittedBy != nil {
		submitter := *w.SubmittedBy
		decision.Submitter = &submitter
	}
	switch {
	case role == RoleOwner || role == RoleAdmin:
		decision.Allowed, decision.Reason = true, ReasonRoleAuthorized
	case role == RoleMember:
		if w.SubmittedBy.submittedByAccount(accountID) {
			decision.Allowed, decision.Reason = true, ReasonSubmitterMatch
		} else {
			decision.Reason = ReasonNotSubmitter
		}
	default:
		decision.Reason = ReasonRoleReadOnly
	}
	return decision
}
