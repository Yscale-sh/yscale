package state

import "testing"

func TestSetCustomerMesh(t *testing.T) {
	s := New()
	const id = "cust_test" // seeded by New()

	// Default: shared Tailscale path (nil mesh).
	c, err := s.CustomerByID(id)
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if c.Mesh != nil {
		t.Fatalf("expected nil Mesh by default, got %+v", c.Mesh)
	}

	// Attach a coordination-server endpoint (the factory write-side).
	ep := &MeshEndpoint{
		Provider:    "box",
		LoginServer: "https://box.example.com",
		APIKey:      "k",
		User:        "acme",
		BackendID:   "linode-1",
	}
	if err := s.SetCustomerMesh(id, ep); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}
	c, _ = s.CustomerByID(id)
	if c.Mesh == nil || c.Mesh.Provider != "box" || c.Mesh.LoginServer != "https://box.example.com" {
		t.Fatalf("mesh not recorded: %+v", c.Mesh)
	}

	// Clearing reverts to the shared Tailscale path.
	if err := s.SetCustomerMesh(id, nil); err != nil {
		t.Fatalf("SetCustomerMesh(nil): %v", err)
	}
	c, _ = s.CustomerByID(id)
	if c.Mesh != nil {
		t.Fatalf("expected Mesh cleared, got %+v", c.Mesh)
	}

	// Unknown customer -> ErrNotFound.
	if err := s.SetCustomerMesh("nope", ep); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for unknown customer, got %v", err)
	}
}

// TestAddCustomerPreservesRuntimeMesh pins the fix for the "restart wipes
// runtime coordination-server mesh" bug. A customer onboarded to the coordination-box factory at
// runtime has its endpoint only in the persisted record. On restart the
// env-seed path re-AddCustomer's the same ID with a fresh Customer{Mesh:nil};
// a blind overwrite would silently downgrade that customer to the shared SaaS
// mesh. AddCustomer must preserve an existing endpoint when the incoming record
// carries none.
func TestAddCustomerPreservesRuntimeMesh(t *testing.T) {
	s := New()
	const id = "cust_acme"

	s.AddCustomer(&Customer{ID: id, Token: "tok-acme", Plan: "pro"})
	// Runtime factory attaches a per-customer coordination box.
	ep := &MeshEndpoint{Provider: "box", LoginServer: "https://hs.acme", APIKey: "k", User: "acme", BackendID: "linode-9"}
	if err := s.SetCustomerMesh(id, ep); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}

	// Restart re-seeds the same customer from env with no mesh attached.
	s.AddCustomer(&Customer{ID: id, Token: "tok-acme", Plan: "pro"})

	c, err := s.CustomerByID(id)
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if c.Mesh == nil || c.Mesh.LoginServer != "https://hs.acme" {
		t.Fatalf("re-seed wiped the runtime-attached coordination mesh: %+v", c.Mesh)
	}
}

// An incoming record that DOES specify a mesh endpoint overrides the existing
// one (e.g. env-configured box vars wins), so preservation only kicks in for
// the nil case.
func TestAddCustomerExplicitMeshOverrides(t *testing.T) {
	s := New()
	const id = "cust_x"
	s.AddCustomer(&Customer{ID: id, Token: "t", Mesh: &MeshEndpoint{Provider: "box", LoginServer: "https://old"}})
	s.AddCustomer(&Customer{ID: id, Token: "t", Mesh: &MeshEndpoint{Provider: "box", LoginServer: "https://new"}})
	c, _ := s.CustomerByID(id)
	if c.Mesh == nil || c.Mesh.LoginServer != "https://new" {
		t.Fatalf("explicit incoming mesh should override, got %+v", c.Mesh)
	}
}
