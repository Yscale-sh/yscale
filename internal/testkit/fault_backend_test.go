package testkit

import (
	"context"
	"errors"
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
)

func TestCreateControlsUseServerAssignedIdentityAndExactOwnership(t *testing.T) {
	spec := ownedSpec()
	backend := mustBackend(t, FaultPlan{
		CreateMode:        CreateAccepted,
		ServerAssignedIDs: []string{"linode-987654"},
	})
	providerID, err := backend.CreateNode(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if providerID != "linode-987654" || providerID == spec.Name || providerID == spec.BurstID {
		t.Fatalf("provider ID = %q, want independent server-assigned identity", providerID)
	}

	// Mutating the request or returned resources must not change provider state.
	spec.NodeLabels["tenant"] = "mutated"
	resources := backend.Resources()
	if got := resources[0].Ownership.Labels["tenant"]; got != "tenant-a" {
		t.Fatalf("stored tenant label = %q, want tenant-a", got)
	}
	resources[0].Ownership.Labels["tenant"] = "also-mutated"
	if got := backend.Resources()[0].Ownership.Labels["tenant"]; got != "tenant-a" {
		t.Fatalf("resource copy mutated provider state: %q", got)
	}

	history := backend.CallHistory()
	if len(history) != 1 || history[0].Operation != OperationCreate || history[0].ProviderID != providerID || history[0].Outcome != string(CreateAccepted) {
		t.Fatalf("create history = %#v", history)
	}
	if got := history[0].Spec.NodeLabels["tenant"]; got != "tenant-a" {
		t.Fatalf("recorded spec was not stable: tenant = %q", got)
	}
}

func TestCreateFaultsClassifyExactOwnedInventory(t *testing.T) {
	tests := []struct {
		name      string
		mode      CreateMode
		wantClass AmbiguityClass
		wantCount int
	}{
		{name: "zero", mode: CreateAmbiguousZero, wantClass: AmbiguityZero},
		{name: "one", mode: CreateAmbiguousOne, wantClass: AmbiguityOne, wantCount: 1},
		{name: "multiple", mode: CreateAmbiguousMultiple, wantClass: AmbiguityMultiple, wantCount: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := mustBackend(t, FaultPlan{
				CreateMode:        test.mode,
				ServerAssignedIDs: []string{"provider-a", "provider-b"},
			})
			spec := ownedSpec()
			if providerID, err := backend.CreateNode(context.Background(), spec); providerID != "" || !errors.Is(err, ErrCreateAmbiguous) {
				t.Fatalf("CreateNode() = (%q, %v), want ambiguous without returned identity", providerID, err)
			}
			class, resources, err := backend.ClassifyCreateAmbiguity(context.Background(), OwnershipFromSpec(spec))
			if err != nil {
				t.Fatal(err)
			}
			if class != test.wantClass || len(resources) != test.wantCount {
				t.Fatalf("classification = (%q, %d), want (%q, %d)", class, len(resources), test.wantClass, test.wantCount)
			}
			for _, resource := range resources {
				if !ownershipEqual(resource.Ownership, OwnershipFromSpec(spec)) {
					t.Fatalf("resource ownership = %#v, want exact %#v", resource.Ownership, OwnershipFromSpec(spec))
				}
			}
		})
	}
}

func TestGeneratedServerIDsCannotCollideWithConfiguredIDs(t *testing.T) {
	backend := mustBackend(t, FaultPlan{
		CreateMode:        CreateAmbiguousMultiple,
		ServerAssignedIDs: []string{"srv-000001"},
	})
	spec := ownedSpec()
	if _, err := backend.CreateNode(context.Background(), spec); !errors.Is(err, ErrCreateAmbiguous) {
		t.Fatalf("CreateNode() error = %v, want ambiguous", err)
	}
	resources := backend.Resources()
	if len(resources) != 2 || resources[0].ProviderID == resources[1].ProviderID {
		t.Fatalf("ambiguous resources = %#v, want two distinct server identities", resources)
	}
}

func TestKnownFailedCreateLeavesNoResource(t *testing.T) {
	backend := mustBackend(t, FaultPlan{CreateMode: CreateKnownFailed})
	if providerID, err := backend.CreateNode(context.Background(), ownedSpec()); providerID != "" || !errors.Is(err, ErrCreateKnownFailed) {
		t.Fatalf("CreateNode() = (%q, %v), want known failure", providerID, err)
	}
	if resources := backend.Resources(); len(resources) != 0 {
		t.Fatalf("known failure left resources: %#v", resources)
	}
}

func TestDelayedStatusAndProviderAbsence(t *testing.T) {
	backend := mustBackend(t, FaultPlan{
		CreateMode:            CreateAccepted,
		ReadyAfterStatusCalls: 2,
	})
	providerID, err := backend.CreateNode(context.Background(), ownedSpec())
	if err != nil {
		t.Fatal(err)
	}
	for poll, want := range []backends.NodePhase{backends.NodeStarting, backends.NodeStarting, backends.NodeRunning} {
		status, err := backend.GetNodeStatus(context.Background(), providerID)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase != want {
			t.Fatalf("status poll %d = %q, want %q", poll+1, status.Phase, want)
		}
	}
	if !backend.MarkAbsent(providerID) {
		t.Fatal("MarkAbsent did not remove provider resource")
	}
	if _, err := backend.GetNodeStatus(context.Background(), providerID); !errors.Is(err, ErrProviderResourceAbsent) {
		t.Fatalf("status after provider absence = %v", err)
	}

	absent := mustBackend(t, FaultPlan{CreateMode: CreateAccepted, ResourceAbsentAfterCreate: true})
	absentID, err := absent.CreateNode(context.Background(), ownedSpec())
	if err != nil || absentID == "" {
		t.Fatalf("accepted-then-absent create = (%q, %v)", absentID, err)
	}
	if resources := absent.Resources(); len(resources) != 0 {
		t.Fatalf("accepted-then-absent resources = %#v", resources)
	}
}

func TestDeleteRetryOutcomesPreserveResourceUntilSuccess(t *testing.T) {
	backend := mustBackend(t, FaultPlan{
		CreateMode:              CreateAccepted,
		DeleteTransientFailures: 2,
	})
	providerID, err := backend.CreateNode(context.Background(), ownedSpec())
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := backend.DeleteNode(context.Background(), providerID); !errors.Is(err, ErrDeleteTransient) {
			t.Fatalf("delete attempt %d = %v, want transient failure", attempt, err)
		}
		if resources := backend.Resources(); len(resources) != 1 || resources[0].ProviderID != providerID {
			t.Fatalf("delete attempt %d changed stable resource: %#v", attempt, resources)
		}
	}
	if err := backend.DeleteNode(context.Background(), providerID); err != nil {
		t.Fatalf("third delete: %v", err)
	}
	if resources := backend.Resources(); len(resources) != 0 {
		t.Fatalf("successful delete left resources: %#v", resources)
	}
	history := backend.CallHistory()
	wantOutcomes := []string{string(CreateAccepted), "transient_failure", "transient_failure", "deleted"}
	if len(history) != len(wantOutcomes) {
		t.Fatalf("history length = %d, want %d", len(history), len(wantOutcomes))
	}
	for i, want := range wantOutcomes {
		if history[i].Outcome != want || history[i].Sequence != i+1 {
			t.Fatalf("history[%d] = %#v, want outcome %q sequence %d", i, history[i], want, i+1)
		}
	}
}

func TestPersistentDeleteFailureRetainsResource(t *testing.T) {
	backend := mustBackend(t, FaultPlan{CreateMode: CreateAccepted, DeleteAlwaysFails: true})
	providerID, err := backend.CreateNode(context.Background(), ownedSpec())
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		if err := backend.DeleteNode(context.Background(), providerID); !errors.Is(err, ErrDeleteTransient) {
			t.Fatalf("delete attempt %d = %v, want provider failure", attempt+1, err)
		}
	}
	if resources := backend.Resources(); len(resources) != 1 || resources[0].ProviderID != providerID {
		t.Fatalf("exhausted delete lost resource: %#v", resources)
	}
}

func TestInventoryFailureIsFailClosed(t *testing.T) {
	backend := mustBackend(t, FaultPlan{CreateMode: CreateAccepted, InventoryExhausted: true})
	providerID, err := backend.CreateNode(context.Background(), ownedSpec())
	if err != nil {
		t.Fatal(err)
	}
	class, resources, err := backend.ClassifyCreateAmbiguity(context.Background(), OwnershipFromSpec(ownedSpec()))
	if class != AmbiguityUnknown || resources != nil || !errors.Is(err, ErrInventoryUnavailable) {
		t.Fatalf("failed inventory classification = (%q, %#v, %v)", class, resources, err)
	}
	if pooled, err := backend.ListPooledNodes(context.Background()); pooled != nil || !errors.Is(err, ErrInventoryUnavailable) {
		t.Fatalf("failed pool list = (%#v, %v)", pooled, err)
	}
	if destroyed, err := backend.CleanupOrphans(context.Background(), nil); destroyed != 0 || !errors.Is(err, ErrInventoryUnavailable) {
		t.Fatalf("cleanup on inventory failure = (%d, %v)", destroyed, err)
	}
	if remaining := backend.Resources(); len(remaining) != 1 || remaining[0].ProviderID != providerID {
		t.Fatalf("fail-closed inventory changed resources: %#v", remaining)
	}
	history := backend.CallHistory()
	for _, operation := range []Operation{OperationInventoryOwned, OperationListPooled, OperationCleanupOrphans} {
		found := false
		for _, call := range history {
			if call.Operation == operation && call.Outcome == "inventory_unavailable" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("history lacks fail-closed %s call: %#v", operation, history)
		}
	}
}

func mustBackend(t *testing.T, plan FaultPlan) *FaultBackend {
	t.Helper()
	backend, err := NewFaultBackend("linode", plan)
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func ownedSpec() *backends.NodeSpec {
	return &backends.NodeSpec{
		Name:       "ys-burst-test",
		BurstID:    "burst_01test",
		ScopeHash:  "scope-a",
		ConfigHash: "config-a",
		Lifecycle:  backends.LifecycleCold,
		NodeLabels: map[string]string{
			"tenant":  "tenant-a",
			"cluster": "cluster-a",
		},
	}
}
