package handlers

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/central/internal/state"
)

type loginMeshProvider struct {
	fakeMeshProvider
	login string
}

func (p *loginMeshProvider) LoginServer() string { return p.login }

// Factory-routed mesh: an agent/gateway on a factory-provisioned tenant mints through the
// factory and is told that tenant's login server; the keyless endpoint never
// reaches the direct box client, and no factory wired fails closed (503).
func TestMintKeyRoutesFactoryTenantThroughTheFabric(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_f", Token: "ysk_f", Plan: "pro", Mesh: &state.MeshEndpoint{
		Provider: state.MeshProviderFactory, LoginServer: "https://box.example", User: "cust_f",
	}}
	store.AddCustomer(cust)
	boxCalled := false
	h := &AgentAuth{
		Store:       store,
		Log:         quietLog(),
		BoxProvider: func(string, string, string) mesh.Provider { boxCalled = true; return nil },
	}
	if rec := callMint(h, cust, "c1"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("no factory wired: mint = %d, want 503: %s", rec.Code, rec.Body)
	}

	var gotID string
	h.FabricProvider = func(customerID, loginServer string) mesh.Provider {
		gotID = customerID
		return &loginMeshProvider{login: loginServer}
	}
	rec := callMint(h, cust, "c1")
	if rec.Code != http.StatusOK {
		t.Fatalf("factory mint = %d: %s", rec.Code, rec.Body)
	}
	var resp MintAuthKeyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.LoginServer != "https://box.example" || resp.AuthKey == "" || gotID != "cust_f" {
		t.Fatalf("mint response = %+v (tenant %q)", resp, gotID)
	}
	if boxCalled {
		t.Fatal("factory tenant reached the direct box client")
	}
}
