package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func memoryRetryFixture(t *testing.T) (*Store, string) {
	t.Helper()
	s := emptyStore()
	a, err := s.UpsertAccount("synthetic-retry-issuer", "owner", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CreateTenant(&Customer{ID: "retry_tenant", Plan: "pro", WorkloadNamespaces: []string{"jobs"}, MaxConcurrentBursts: 1, MaxHourlyUSD: 1}, a.ID); err != nil {
		t.Fatal(err)
	}
	finished := time.Now().UTC()
	actor := HumanActor(a.ID, "retry_tenant")
	if err := s.PutWorkloadDurable(&Workload{ID: "retry_source", CustomerID: "retry_tenant", Status: "succeeded", FinishedAt: &finished, SubmittedBy: &actor, SpecYAML: []byte("synthetic-source")}); err != nil {
		t.Fatal(err)
	}
	return s, a.ID
}

func TestWorkloadRetryMemoryAndFailures(t *testing.T) {
	for _, scenario := range []string{"accepted", "unsupported-durable", "cancelled", "source-changed", "policy-changed", "nil-approval", "negative-rate", "nonterminal"} {
		t.Run(scenario, func(t *testing.T) {
			s, account := memoryRetryFixture(t)
			prepared, err := s.PrepareWorkloadRetry(context.Background(), "retry_tenant", account, "retry_source")
			if err != nil || prepared.Approval == nil || prepared.Audit != nil {
				t.Fatalf("preparation: %v", err)
			}
			// The detached projection is not the store's cache. Mutating it must
			// not silently rewrite authority or the source for another request.
			prepared.Decision.Source.SpecYAML[0] = 'X'
			prepared.Tenant.WorkloadNamespaces[0] = "wrong"
			current, _ := s.GetWorkload("retry_source")
			customer, _ := s.CustomerByID("retry_tenant")
			if string(current.SpecYAML) != "synthetic-source" || customer.WorkloadNamespaces[0] != "jobs" {
				t.Fatal("preparation shares mutable cache state")
			}
			ctx := context.Background()
			var want error
			rate := int64(1_000_000)
			switch scenario {
			case "unsupported-durable":
				s.persist = &accountSpyPersister{}
				want = ErrPersistence
				if _, err := s.PrepareWorkloadRetry(ctx, "retry_tenant", account, "retry_source"); !errors.Is(err, ErrPersistence) {
					t.Fatalf("preparation fell back to cache: %v", err)
				}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
				if _, err := s.PrepareWorkloadRetry(ctx, "retry_tenant", account, "retry_source"); !errors.Is(err, want) {
					t.Fatalf("preparation ignored cancellation: %v", err)
				}
			case "source-changed":
				current.SpecYAML = []byte("changed-source")
				if err := s.PutWorkloadDurable(current); err != nil {
					t.Fatal(err)
				}
				want = ErrWorkloadRetryChanged
			case "policy-changed":
				if _, err := s.SetCustomerWorkloadNamespaces("retry_tenant", []string{"changed"}, OperatorActor()); err != nil {
					t.Fatal(err)
				}
				want = ErrWorkloadRetryChanged
			case "nil-approval":
				prepared.Approval, want = nil, ErrNotFound
			case "negative-rate":
				rate, want = -1, ErrAdmissionInvalidRate
			case "nonterminal":
				// Model an older writer changing the authoritative source.
				s.mu.Lock()
				s.workloads["retry_source"].FinishedAt = nil
				s.mu.Unlock()
				want = ErrWorkloadRetryNotTerminal
			}
			id, audit, err := s.ReserveWorkloadRetry(ctx, prepared.Approval, "retry_new", rate)
			if !errors.Is(err, want) {
				t.Fatalf("retry err=%v, want=%v", err, want)
			}
			if want != nil {
				if id != "" || s.AdmissionReservationCount("retry_tenant") != 0 {
					t.Fatal("failed retry reserved capacity")
				}
				return
			}
			if id == "" || audit == nil || audit.Outcome != OutcomeAccepted || audit.Detail.RetryWorkloadID != "retry_new" || s.AdmissionReservationCount("retry_tenant") != 1 {
				t.Fatal("retry missing reservation/audit")
			}
			if err := s.ReleaseAdmission(ctx, id); err != nil {
				t.Fatal(err)
			}
		})
	}
}
