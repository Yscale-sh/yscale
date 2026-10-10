package decider

import (
	"context"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/central/internal/state"
)

// recordingProvider is a mesh.Provider that records the calls central makes.
type recordingProvider struct {
	login   string
	calls   []string
	deleted bool
}

func (p *recordingProvider) MintAuthKey(context.Context, []string, time.Duration) (string, error) {
	p.calls = append(p.calls, "mint")
	return "key", nil
}
func (p *recordingProvider) MintAuthKeyEphemeral(context.Context, []string, time.Duration, bool) (string, error) {
	p.calls = append(p.calls, "mint")
	return "key", nil
}
func (p *recordingProvider) FindDeviceByHostname(_ context.Context, hostname string) (string, error) {
	p.calls = append(p.calls, "find:"+hostname)
	if p.deleted {
		return "", nil
	}
	return "42", nil
}
func (p *recordingProvider) DeleteDevice(_ context.Context, id string) error {
	p.calls = append(p.calls, "delete:"+id)
	p.deleted = true
	return nil
}
func (p *recordingProvider) LoginServer() string { return p.login }

func factoryTenantStore(t *testing.T) *state.Store {
	t.Helper()
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_factory", Token: "tok_factory", Plan: "pro"})
	if err := s.SetCustomerMesh("cust_factory", &state.MeshEndpoint{
		Provider: state.MeshProviderFactory, LoginServer: "https://box.example", User: "cust_factory",
	}); err != nil {
		t.Fatal(err)
	}
	return s
}

// A factory tenant's keyless endpoint must never reach the direct box client,
// and with no factory wired there is no provider at all (fail closed).
func TestMeshForRoutesFactoryTenantsThroughTheFabricProvider(t *testing.T) {
	s := factoryTenantStore(t)
	d := New(Config{})
	d.store = s
	boxCalled := false
	d.WithBoxProvider(func(string, string, string) mesh.Provider { boxCalled = true; return nil })

	if prov := d.meshFor(d.customerByID("cust_factory")); prov != nil {
		t.Fatalf("no fabric provider wired: meshFor = %T, want nil", prov)
	}

	fabric := &recordingProvider{login: "https://box.example"}
	var gotID, gotLogin string
	d.WithFabricProvider(func(customerID, loginServer string) mesh.Provider {
		gotID, gotLogin = customerID, loginServer
		return fabric
	})
	if prov := d.meshFor(d.customerByID("cust_factory")); prov != fabric {
		t.Fatalf("meshFor = %v, want the fabric provider", prov)
	}
	if gotID != "cust_factory" || gotLogin != "https://box.example" {
		t.Fatalf("fabric provider built for %q/%q", gotID, gotLogin)
	}
	if boxCalled {
		t.Fatal("factory tenant was routed to the direct box client")
	}
}

// Teardown of a burst minted on a factory box resolves the same provider and
// removes the device through the factory.
func TestCleanupMeshDeletesFactoryBurstDeviceThroughTheFabric(t *testing.T) {
	d := New(Config{})
	d.store = factoryTenantStore(t)
	fabric := &recordingProvider{login: "https://box.example"}
	d.WithFabricProvider(func(string, string) mesh.Provider { return fabric })

	err := d.CleanupMesh(context.Background(), &state.Burst{
		CustomerID: "cust_factory", TSHostname: "ys-burst-abc",
		MeshProvider: "box", MeshLoginServer: "https://box.example",
	})
	if err != nil {
		t.Fatalf("CleanupMesh: %v", err)
	}
	if len(fabric.calls) < 2 || fabric.calls[0] != "find:ys-burst-abc" || fabric.calls[1] != "delete:42" {
		t.Fatalf("fabric calls = %v", fabric.calls)
	}
}
