package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrWorkloadRetryForbidden   = errors.New("workload retry forbidden")
	ErrWorkloadRetryChanged     = errors.New("workload retry source or policy changed")
	ErrWorkloadRetryNotTerminal = errors.New("workload retry source is not terminal")
)

// WorkloadRetryApproval is a server-created preparation witness, not an
// authorization grant. Admission rechecks it before recording an accepted
// retry. It cannot be populated from request JSON.
type WorkloadRetryApproval struct {
	customer, account, source, role string
	sourceHash, policyHash          string
}

type WorkloadRetryPreparation struct {
	Decision WorkloadRetryDecision
	Tenant   TenantSummary
	Approval *WorkloadRetryApproval
	Audit    *AuditEvent
}

type workloadRetryPersister interface {
	prepareWorkloadRetry(context.Context, string, string, string) (WorkloadRetryPreparation, error)
	reserveWorkloadRetry(context.Context, *WorkloadRetryApproval, string, int64) (string, *AuditEvent, error)
}

// PrepareWorkloadRetry reads a current, detached source and tenant policy.
// Refusals are journaled here; acceptance is deferred to ReserveWorkloadRetry.
func (s *Store) PrepareWorkloadRetry(ctx context.Context, customer, account, source string) (WorkloadRetryPreparation, error) {
	if err := ctx.Err(); err != nil {
		return WorkloadRetryPreparation{}, err
	}
	s.mu.RLock()
	p := s.persist
	if p != nil {
		s.mu.RUnlock()
		writer, ok := p.(workloadRetryPersister)
		if !ok {
			return WorkloadRetryPreparation{}, fmt.Errorf("%w: durable retry preparation unavailable", ErrPersistence)
		}
		result, err := writer.prepareWorkloadRetry(ctx, customer, account, source)
		return result, workloadRetryError(err)
	}
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return WorkloadRetryPreparation{}, err
	}
	m, err := s.cachedMembershipForLocked(account, customer)
	if err != nil || s.tombstoned[customer] {
		return WorkloadRetryPreparation{}, ErrNotFound
	}
	return prepareWorkloadRetry(s.customers[customer], m, s.workloads[source], customer, account, source)
}

// ReserveWorkloadRetry commits current retry authority, the accepted audit and
// the existing admission reservation together. No provider/lifecycle callback
// executes while those state locks are held.
func (s *Store) ReserveWorkloadRetry(ctx context.Context, approval *WorkloadRetryApproval, workloadID string, candidateMicroUSD int64) (string, *AuditEvent, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if approval == nil || approval.customer == "" || approval.account == "" || approval.source == "" || workloadID == "" {
		return "", nil, ErrNotFound
	}
	if err := ValidateCandidateMicroUSD(candidateMicroUSD); err != nil {
		return "", nil, err
	}
	s.mu.Lock()
	p := s.persist
	if p != nil {
		s.mu.Unlock()
		writer, ok := p.(workloadRetryPersister)
		if !ok {
			return "", nil, fmt.Errorf("%w: durable retry admission unavailable", ErrPersistence)
		}
		id, audit, err := writer.reserveWorkloadRetry(ctx, approval, workloadID, candidateMicroUSD)
		return id, audit, workloadRetryError(err)
	}
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	m, err := s.cachedMembershipForLocked(approval.account, approval.customer)
	if err != nil || s.tombstoned[approval.customer] {
		return "", nil, ErrNotFound
	}
	prepared, err := prepareWorkloadRetry(s.customers[approval.customer], m, s.workloads[approval.source], approval.customer, approval.account, approval.source)
	if err != nil {
		return "", nil, err
	}
	audit, decisionErr := checkWorkloadRetryApproval(prepared, approval, workloadID)
	if decisionErr != nil {
		return "", audit, decisionErr
	}
	id, err := s.reserveAdmissionLocked(approval.customer, workloadID, candidateMicroUSD)
	if err != nil {
		return "", nil, err
	}
	return id, audit, nil
}

func workloadRetryError(err error) error {
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrWorkloadRetryForbidden) || errors.Is(err, ErrWorkloadRetryChanged) || errors.Is(err, ErrWorkloadRetryNotTerminal) || errors.Is(err, ErrAdmissionSlotLimitReached) || errors.Is(err, ErrAdmissionRateLimitReached) || errors.Is(err, ErrAdmissionConflictingRate) {
		return err
	}
	return fmt.Errorf("%w: workload retry: %w", ErrPersistence, err)
}

func prepareWorkloadRetry(c *Customer, m *TenantMembership, w *Workload, customer, account, source string) (WorkloadRetryPreparation, error) {
	if c == nil || c.ID != customer || c.Revoked() || m == nil || m.CustomerID != customer || m.AccountID != account || w == nil || w.ID != source || w.CustomerID != customer {
		return WorkloadRetryPreparation{}, ErrNotFound
	}
	policy := humanWorkloadCancelDecision(w, account, m.Role)
	result := WorkloadRetryPreparation{Decision: WorkloadRetryDecision{Allowed: policy.Allowed, Reason: policy.Reason, Role: policy.Role, WorkloadID: w.ID, BurstID: w.BurstID, Source: *cloneWorkload(w), Submitter: policy.Submitter}, Tenant: tenantSummary(c)}
	if result.Decision.Allowed && !workloadRetrySourceTerminal(w) {
		result.Decision.Allowed = false
		result.Decision.Reason = ReasonWorkloadNotTerminal
	}
	if !result.Decision.Allowed {
		result.Audit = workloadRetryAudit(result.Decision, account, customer, "", OutcomeDenied)
		return result, validateAudit(result.Audit)
	}
	sourceHash, err := workloadRetrySourceHash(w)
	if err != nil {
		return WorkloadRetryPreparation{}, err
	}
	policyHash, err := workloadRetryPolicyHash(c)
	if err != nil {
		return WorkloadRetryPreparation{}, err
	}
	result.Approval = &WorkloadRetryApproval{customer: customer, account: account, source: source, role: m.Role, sourceHash: sourceHash, policyHash: policyHash}
	return result, nil
}

func workloadRetrySourceTerminal(w *Workload) bool {
	if w == nil || w.FinishedAt == nil {
		return false
	}
	switch w.Status {
	case "succeeded", "failed", "cancelled", "canceled":
		return true
	default:
		return false
	}
}

func workloadRetrySourceHash(w *Workload) (string, error) {
	// Only fields which authorize or determine the next run. Cost/telemetry
	// settling on a terminal source must not invalidate an unchanged retry.
	return retryFingerprint(struct {
		ID, CustomerID, ClusterID, BurstID, Status string
		Spec                                       []byte
		Submitter                                  *Actor
		Template                                   *TemplateRef
		Placement                                  *WorkloadPlacement
	}{w.ID, w.CustomerID, w.ClusterID, w.BurstID, w.Status, w.SpecYAML, w.SubmittedBy, w.TemplateRef, w.Placement})
}

func workloadRetryPolicyHash(c *Customer) (string, error) {
	return retryFingerprint(struct {
		Plan       string
		Namespaces []string
		Policy     *ClusterPolicy
		MaxBursts  int
		MaxHourly  float64
	}{c.Plan, c.WorkloadNamespaces, c.ClusterPolicy, c.MaxConcurrentBursts, c.MaxHourlyUSD})
}

func retryFingerprint(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func checkWorkloadRetryApproval(current WorkloadRetryPreparation, expected *WorkloadRetryApproval, workloadID string) (*AuditEvent, error) {
	var decisionErr error
	switch {
	case !current.Decision.Allowed:
		decisionErr = ErrWorkloadRetryForbidden
		if current.Decision.Reason == ReasonWorkloadNotTerminal {
			decisionErr = ErrWorkloadRetryNotTerminal
		}
	case current.Approval == nil || *current.Approval != *expected:
		current.Decision.Allowed = false
		current.Decision.Reason = ReasonWorkloadRetryChanged
		decisionErr = ErrWorkloadRetryChanged
	}
	outcome := OutcomeAccepted
	if decisionErr != nil {
		outcome = OutcomeDenied
	}
	audit := workloadRetryAudit(current.Decision, expected.account, expected.customer, workloadID, outcome)
	if err := validateAudit(audit); err != nil {
		return nil, err
	}
	return audit, decisionErr
}

func workloadRetryAudit(decision WorkloadRetryDecision, account, customer, workloadID, outcome string) *AuditEvent {
	return NewAuditEvent(AuditEvent{CustomerID: customer, Actor: HumanActor(account, customer), Action: ActionWorkloadRetry, Outcome: outcome, TargetKind: TargetWorkload, TargetID: decision.WorkloadID, Detail: AuditDetail{Reason: decision.Reason, Role: decision.Role, BurstID: decision.BurstID, RetryWorkloadID: workloadID}})
}
