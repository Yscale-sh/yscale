package testkit

import (
	"context"
	"errors"
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
)

func TestCrashBackendCrashBeforeCreateLeavesProviderUntouched(t *testing.T) {
	provider := mustBackend(t, FaultPlan{
		CreateMode:        CreateAccepted,
		ServerAssignedIDs: []string{"linode-100001"},
	})
	crash := mustController(t, CrashPlan{Checkpoint: BeforeProviderCreateCall, Occurrence: 1})
	decorated := mustCrashBackend(t, provider, crash)

	backendID, err := decorated.CreateNode(context.Background(), ownedSpec())
	if !errors.Is(err, ErrInjectedCrash) || backendID != "" {
		t.Fatalf("create = %q, %v", backendID, err)
	}
	if got := len(provider.Resources()); got != 0 {
		t.Fatalf("provider resources = %d, want 0", got)
	}
	if got := len(provider.CallHistory()); got != 0 {
		t.Fatalf("provider was called %d times before the crash", got)
	}
	if crash.Count(AfterProviderCreateCall) != 0 {
		t.Fatal("after-checkpoint ran despite crashing before the call")
	}
}

func TestCrashBackendCrashAfterCreatePreservesProviderStateAndWithholdsIdentity(t *testing.T) {
	provider := mustBackend(t, FaultPlan{
		CreateMode:        CreateAccepted,
		ServerAssignedIDs: []string{"linode-100002"},
	})
	crash := mustController(t, CrashPlan{Checkpoint: AfterProviderCreateCall, Occurrence: 1})
	decorated := mustCrashBackend(t, provider, crash)
	spec := ownedSpec()

	backendID, err := decorated.CreateNode(context.Background(), spec)
	if !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("create err = %v, want ErrInjectedCrash", err)
	}
	if backendID != "" {
		t.Fatalf("crash returned identity %q; the receipt must be withheld", backendID)
	}

	// The provider really created the resource. Only the caller lost it.
	resources := provider.Resources()
	if len(resources) != 1 || resources[0].ProviderID != "linode-100002" {
		t.Fatalf("provider resources = %+v, want the accepted create preserved", resources)
	}

	// A newly constructed worker recovers the identity through inventory, not
	// by deriving it from the request.
	restarted, err := crash.Restart(CrashPlan{})
	if err != nil {
		t.Fatal(err)
	}
	recovered := mustCrashBackend(t, provider, restarted)
	class, owned, err := provider.ClassifyCreateAmbiguity(context.Background(), OwnershipFromSpec(spec))
	if err != nil || class != AmbiguityOne || len(owned) != 1 {
		t.Fatalf("inventory = %s %+v %v", class, owned, err)
	}
	if owned[0].ProviderID == spec.Name || owned[0].ProviderID == spec.BurstID {
		t.Fatalf("provider ID %q was derived from the request", owned[0].ProviderID)
	}
	status, err := recovered.GetNodeStatus(context.Background(), owned[0].ProviderID)
	if err != nil || status == nil {
		t.Fatalf("recovered status = %+v, %v", status, err)
	}
}

func TestCrashBackendCrashAfterCreateWithholdsProviderError(t *testing.T) {
	provider := mustBackend(t, FaultPlan{CreateMode: CreateKnownFailed})
	crash := mustController(t, CrashPlan{Checkpoint: AfterProviderCreateCall, Occurrence: 1})
	decorated := mustCrashBackend(t, provider, crash)

	backendID, err := decorated.CreateNode(context.Background(), ownedSpec())
	if backendID != "" {
		t.Fatalf("create returned %q", backendID)
	}
	if !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("err = %v, want ErrInjectedCrash", err)
	}
	// The caller must not learn the create was a known failure: a crashed
	// worker has no receipt either way.
	if errors.Is(err, ErrCreateKnownFailed) {
		t.Fatal("crash leaked the provider outcome")
	}
}

func TestCrashBackendPassesThroughProviderOutcomesWhenDisarmed(t *testing.T) {
	tests := []struct {
		name    string
		plan    FaultPlan
		wantID  string
		wantErr error
	}{
		{
			name:   "accepted",
			plan:   FaultPlan{CreateMode: CreateAccepted, ServerAssignedIDs: []string{"linode-200001"}},
			wantID: "linode-200001",
		},
		{
			name:    "known failure",
			plan:    FaultPlan{CreateMode: CreateKnownFailed},
			wantErr: ErrCreateKnownFailed,
		},
		{
			name:    "ambiguous zero",
			plan:    FaultPlan{CreateMode: CreateAmbiguousZero},
			wantErr: ErrCreateAmbiguous,
		},
		{
			name:    "ambiguous one",
			plan:    FaultPlan{CreateMode: CreateAmbiguousOne, ServerAssignedIDs: []string{"linode-200002"}},
			wantErr: ErrCreateAmbiguous,
		},
		{
			name:    "ambiguous multiple",
			plan:    FaultPlan{CreateMode: CreateAmbiguousMultiple, ServerAssignedIDs: []string{"linode-200003", "linode-200004"}},
			wantErr: ErrCreateAmbiguous,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := mustBackend(t, test.plan)
			crash := mustController(t, CrashPlan{})
			decorated := mustCrashBackend(t, provider, crash)

			backendID, err := decorated.CreateNode(context.Background(), ownedSpec())
			if backendID != test.wantID {
				t.Fatalf("id = %q, want %q", backendID, test.wantID)
			}
			if test.wantErr == nil && err != nil {
				t.Fatalf("err = %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("err = %v, want %v", err, test.wantErr)
			}
			if crash.Count(BeforeProviderCreateCall) != 1 || crash.Count(AfterProviderCreateCall) != 1 {
				t.Fatalf("checkpoints = %+v", crash.Hits())
			}
		})
	}
}

func TestCrashBackendDeleteFaultModes(t *testing.T) {
	ctx := context.Background()

	t.Run("crash before delete leaves the resource alive", func(t *testing.T) {
		provider, id := providerWithResource(t, FaultPlan{
			CreateMode:        CreateAccepted,
			ServerAssignedIDs: []string{"linode-300001"},
		})
		crash := mustController(t, CrashPlan{Checkpoint: BeforeProviderDeleteCall, Occurrence: 1})
		decorated := mustCrashBackend(t, provider, crash)

		if err := decorated.DeleteNode(ctx, id); !errors.Is(err, ErrInjectedCrash) {
			t.Fatalf("delete err = %v", err)
		}
		if got := len(provider.Resources()); got != 1 {
			t.Fatalf("resources = %d, want the resource still alive", got)
		}
	})

	t.Run("crash after delete destroys the resource and withholds the receipt", func(t *testing.T) {
		provider, id := providerWithResource(t, FaultPlan{
			CreateMode:        CreateAccepted,
			ServerAssignedIDs: []string{"linode-300002"},
		})
		crash := mustController(t, CrashPlan{Checkpoint: AfterProviderDeleteCall, Occurrence: 1})
		decorated := mustCrashBackend(t, provider, crash)

		if err := decorated.DeleteNode(ctx, id); !errors.Is(err, ErrInjectedCrash) {
			t.Fatalf("delete err = %v", err)
		}
		if got := len(provider.Resources()); got != 0 {
			t.Fatalf("resources = %d, want 0", got)
		}
		// A restarted worker replays the delete. Like the live backends, the
		// fake treats already-absent as idempotent success.
		restarted, err := crash.Restart(CrashPlan{})
		if err != nil {
			t.Fatal(err)
		}
		recovered := mustCrashBackend(t, provider, restarted)
		if err := recovered.DeleteNode(ctx, id); err != nil {
			t.Fatalf("replayed delete err = %v, want idempotent success", err)
		}
		if _, err := recovered.GetNodeStatus(ctx, id); !errors.Is(err, ErrProviderResourceAbsent) {
			t.Fatalf("status after replay err = %v, want provider absence", err)
		}
	})

	t.Run("transient failures pass through", func(t *testing.T) {
		provider, id := providerWithResource(t, FaultPlan{
			CreateMode:              CreateAccepted,
			ServerAssignedIDs:       []string{"linode-300003"},
			DeleteTransientFailures: 2,
		})
		decorated := mustCrashBackend(t, provider, mustController(t, CrashPlan{}))
		for attempt := range 2 {
			if err := decorated.DeleteNode(ctx, id); !errors.Is(err, ErrDeleteTransient) {
				t.Fatalf("attempt %d err = %v", attempt+1, err)
			}
		}
		if err := decorated.DeleteNode(ctx, id); err != nil {
			t.Fatalf("third attempt err = %v", err)
		}
		if got := len(provider.Resources()); got != 0 {
			t.Fatalf("resources = %d, want 0", got)
		}
	})

	t.Run("persistent failures pass through unchanged", func(t *testing.T) {
		provider, id := providerWithResource(t, FaultPlan{
			CreateMode:        CreateAccepted,
			ServerAssignedIDs: []string{"linode-300004"},
			DeleteAlwaysFails: true,
		})
		decorated := mustCrashBackend(t, provider, mustController(t, CrashPlan{}))
		for attempt := range 4 {
			if err := decorated.DeleteNode(ctx, id); !errors.Is(err, ErrDeleteTransient) {
				t.Fatalf("attempt %d err = %v", attempt+1, err)
			}
		}
		// Retry exhaustion is not the decorator's call: the resource is still
		// there and the error never changes shape.
		if got := len(provider.Resources()); got != 1 {
			t.Fatalf("resources = %d, want the live-cost obligation preserved", got)
		}
	})

	t.Run("inventory failure stays explicit", func(t *testing.T) {
		provider, _ := providerWithResource(t, FaultPlan{
			CreateMode:         CreateAccepted,
			ServerAssignedIDs:  []string{"linode-300005"},
			InventoryExhausted: true,
		})
		decorated := mustCrashBackend(t, provider, mustController(t, CrashPlan{}))
		pooled, err := decorated.ListPooledNodes(ctx)
		if !errors.Is(err, ErrInventoryUnavailable) || pooled != nil {
			t.Fatalf("pooled = %+v, err = %v", pooled, err)
		}
		class, owned, err := provider.ClassifyCreateAmbiguity(ctx, Ownership{})
		if !errors.Is(err, ErrInventoryUnavailable) || class != AmbiguityUnknown || owned != nil {
			t.Fatalf("classification = %s %+v %v, want an inconclusive error", class, owned, err)
		}
	})
}

func TestCrashBackendDelegatesEveryOtherMethodExactly(t *testing.T) {
	ctx := context.Background()
	provider, id := providerWithResource(t, FaultPlan{
		CreateMode:        CreateAccepted,
		ServerAssignedIDs: []string{"linode-400001"},
	})
	crash := mustController(t, CrashPlan{})
	decorated := mustCrashBackend(t, provider, crash)

	if decorated.Name() != provider.Name() {
		t.Fatalf("name = %q, want %q", decorated.Name(), provider.Name())
	}
	if decorated.Unwrap() != backends.Backend(provider) {
		t.Fatal("Unwrap did not return the decorated backend")
	}

	status, err := decorated.GetNodeStatus(ctx, id)
	if err != nil || status == nil || status.Phase != backends.NodeRunning {
		t.Fatalf("status = %+v, %v", status, err)
	}
	if err := decorated.StopNode(ctx, id); err != nil {
		t.Fatal(err)
	}
	pooled, err := decorated.ListPooledNodes(ctx)
	if err != nil || len(pooled) != 1 || pooled[0].BackendID != id {
		t.Fatalf("pooled = %+v, %v", pooled, err)
	}
	if err := decorated.StartNode(ctx, id); err != nil {
		t.Fatal(err)
	}
	destroyed, err := decorated.CleanupOrphans(ctx, map[string]bool{id: true})
	if err != nil || destroyed != 0 {
		t.Fatalf("cleanup tracked = %d, %v", destroyed, err)
	}
	destroyed, err = decorated.CleanupOrphans(ctx, nil)
	if err != nil || destroyed != 1 {
		t.Fatalf("cleanup untracked = %d, %v", destroyed, err)
	}

	// Errors from delegated methods pass through untouched.
	if err := decorated.StartNode(ctx, "linode-absent"); !errors.Is(err, ErrProviderResourceAbsent) {
		t.Fatalf("start absent err = %v", err)
	}
	if err := decorated.StopNode(ctx, "linode-absent"); !errors.Is(err, ErrProviderResourceAbsent) {
		t.Fatalf("stop absent err = %v", err)
	}
	if _, err := decorated.GetNodeStatus(ctx, "linode-absent"); !errors.Is(err, ErrProviderResourceAbsent) {
		t.Fatalf("status absent err = %v", err)
	}

	// Only CreateNode and DeleteNode carry checkpoints.
	for _, checkpoint := range Checkpoints() {
		if crash.Count(checkpoint) != 0 {
			t.Fatalf("%s was entered by a delegated method", checkpoint)
		}
	}
}

func TestNewCrashBackendRejectsMissingCollaborators(t *testing.T) {
	provider := mustBackend(t, FaultPlan{})
	if _, err := NewCrashBackend(nil, mustController(t, CrashPlan{})); err == nil {
		t.Fatal("nil backend was accepted")
	}
	if _, err := NewCrashBackend(provider, nil); err == nil {
		t.Fatal("nil controller was accepted")
	}
}

func mustCrashBackend(t *testing.T, inner backends.Backend, crash *CrashController) *CrashBackend {
	t.Helper()
	decorated, err := NewCrashBackend(inner, crash)
	if err != nil {
		t.Fatal(err)
	}
	return decorated
}

// providerWithResource creates one resource through the undecorated provider
// so delete-path tests start from real provider state.
func providerWithResource(t *testing.T, plan FaultPlan) (*FaultBackend, string) {
	t.Helper()
	provider := mustBackend(t, plan)
	backendID, err := provider.CreateNode(context.Background(), ownedSpec())
	if err != nil {
		t.Fatal(err)
	}
	return provider, backendID
}
