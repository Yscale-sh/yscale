package decider

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/internal/testkit"
	"github.com/yscale-sh/yscale/pkg/backends"
)

// A dispatched create is never replayed after its lease expires: central
// cannot distinguish a crash immediately before the provider call from one
// immediately after it. Provider inventory reconciliation owns convergence.
func TestPlanCrashAroundProviderCreateRefusesReplayAsAmbiguous(t *testing.T) {
	tests := []struct {
		name          string
		checkpoint    testkit.Checkpoint
		wantCreates   int
		wantResources int
		wantAfterHits int
	}{
		{
			name:       "before provider call",
			checkpoint: testkit.BeforeProviderCreateCall,
		},
		{
			name:          "after provider call",
			checkpoint:    testkit.AfterProviderCreateCall,
			wantCreates:   1,
			wantResources: 1,
			wantAfterHits: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, _, admission, trace, _, _ := newAdmissionSeamDecider(t)
			provider, err := testkit.NewFaultBackend(backends.TypeFlyIO, testkit.FaultPlan{
				CreateMode:        testkit.CreateAccepted,
				ServerAssignedIDs: []string{"provider-node-1"},
			})
			if err != nil {
				t.Fatal(err)
			}

			crash := installCrashBackend(t, d, provider, testkit.CrashPlan{
				Checkpoint: tc.checkpoint,
				Occurrence: 1,
			})
			// A real process dies before it can settle the returned injected
			// error. Refusing this fake settlement preserves that durable state.
			admission.failErr = errors.New("simulated process death before settlement")

			_, err = d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
			if err == nil || !d.CreateOutcomeAmbiguous(err) {
				t.Fatalf("crashed Plan error = %v, want an ambiguous create", err)
			}
			if !crash.Fired() {
				t.Fatalf("crash did not fire at %s", tc.checkpoint)
			}
			if got := crash.Count(testkit.BeforeProviderCreateCall); got != 1 {
				t.Fatalf("before-call checkpoints = %d, want 1", got)
			}
			if got := crash.Count(testkit.AfterProviderCreateCall); got != tc.wantAfterHits {
				t.Fatalf("after-call checkpoints = %d, want %d", got, tc.wantAfterHits)
			}
			assertProviderState(t, provider, tc.wantCreates, tc.wantResources)
			if admission.succeededResource != "" || admission.failCalls != 1 {
				t.Fatalf("settlement after crash = succeeded %q, failed calls %d; want no committed result",
					admission.succeededResource, admission.failCalls)
			}
			if got := trace.String(); got != "admit,claim,fail" {
				t.Fatalf("crashed seam order = %q, want admit,claim,fail", got)
			}

			workloadID := admission.admitted.WorkloadID
			burstID := admission.admitted.BurstID
			restartedCrash, err := crash.Restart(testkit.CrashPlan{})
			if err != nil {
				t.Fatal(err)
			}
			d.fly = newCrashBackend(t, provider, restartedCrash)
			trace.reset()
			admission.failErr = nil
			admission.failCalls = 0
			admission.admitResponse = lifecycle.AdmissionResponse{
				WorkloadID: workloadID,
				BurstID:    burstID,
			}
			admission.claimState = lifecycle.OperationProcessing
			admission.claimLeaseExpired = true

			_, err = d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
			var refusal *ProviderCreateNotClaimableError
			if !errors.As(err, &refusal) {
				t.Fatalf("restarted Plan error = %v, want ProviderCreateNotClaimableError", err)
			}
			if refusal.State != lifecycle.OperationProcessing || !refusal.LeaseExpired {
				t.Fatalf("restarted refusal = %+v, want processing with an expired lease", refusal)
			}
			if !d.CreateOutcomeAmbiguous(err) || !strings.Contains(err.Error(), "expired lease") {
				t.Fatalf("restarted refusal must remain explicitly ambiguous: %v", err)
			}
			assertProviderState(t, provider, tc.wantCreates, tc.wantResources)
			if admission.failCalls != 0 || admission.succeededResource != "" {
				t.Fatalf("restart attempted settlement: failed=%d succeeded=%q",
					admission.failCalls, admission.succeededResource)
			}
			if got := trace.String(); got != "admit,claim" {
				t.Fatalf("restarted seam order = %q, want admit,claim", got)
			}
			if restartedCrash.Count(testkit.BeforeProviderCreateCall) != 0 ||
				restartedCrash.Count(testkit.AfterProviderCreateCall) != 0 {
				t.Fatal("restarted Plan reached the provider call")
			}
		})
	}
}

func installCrashBackend(t *testing.T, d *Decider, provider backends.Backend, plan testkit.CrashPlan) *testkit.CrashController {
	t.Helper()
	crash, err := testkit.NewCrashController(plan)
	if err != nil {
		t.Fatal(err)
	}
	d.fly = newCrashBackend(t, provider, crash)
	return crash
}

func newCrashBackend(t *testing.T, provider backends.Backend, crash *testkit.CrashController) *testkit.CrashBackend {
	t.Helper()
	decorated, err := testkit.NewCrashBackend(provider, crash)
	if err != nil {
		t.Fatal(err)
	}
	return decorated
}

func assertProviderState(t *testing.T, provider *testkit.FaultBackend, wantCreates, wantResources int) {
	t.Helper()
	creates := 0
	for _, call := range provider.CallHistory() {
		if call.Operation == testkit.OperationCreate {
			creates++
		}
	}
	if creates != wantCreates {
		t.Fatalf("provider create calls = %d, want %d", creates, wantCreates)
	}
	if got := len(provider.Resources()); got != wantResources {
		t.Fatalf("provider resources = %d, want %d", got, wantResources)
	}
}
