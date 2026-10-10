//go:build integration

package handlers

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestPostgresCredentialAuthCoreAcrossReplicas(t *testing.T) {
	a := coreReadStore(t)
	c, owner, suffix := coreReadTenant(t, a)
	oldTenantToken := c.Token
	clusterID := "cl-core-auth-" + suffix
	_, oldConnectorToken, _, err := a.RegisterTenantCluster(c.ID, owner, clusterID, "Reader", state.HumanActor(owner, c.ID))
	if err != nil {
		t.Fatal(err)
	}
	b := coreReadStore(t)
	probe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customer, err := CustomerFromContext(r.Context())
		if err != nil {
			t.Fatal(err)
		}
		w.Header().Set("X-Test-Tenant", customer.ID)
		w.Header().Set("X-Test-Clusters", strconv.Itoa(len(customer.RegisteredClusters)))
		w.WriteHeader(http.StatusNoContent)
	})
	mux := http.NewServeMux()
	mux.Handle("GET /tenant", Auth(b, probe))
	mux.Handle("GET /connector", ConnectorAuth(b, probe))
	request := func(path, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, r)
		return response
	}
	check := func(path, token string, want int) {
		t.Helper()
		if got := request(path, token).Code; got != want {
			t.Errorf("%s response = %d, want %d", path, got, want)
		}
	}
	check("/tenant", oldTenantToken, http.StatusNoContent)
	check("/connector", oldConnectorToken, http.StatusNoContent)
	newTenantToken, err := a.RotateCustomerCredential(c.ID, state.OperatorActor())
	if err != nil {
		t.Fatal(err)
	}
	_, newConnectorToken, _, err := a.RotateTenantClusterCredential(c.ID, owner, clusterID, state.HumanActor(owner, c.ID))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/tenant", "/connector"} {
		check(path, oldTenantToken, http.StatusUnauthorized)
		check(path, newTenantToken, http.StatusNoContent)
	}
	check("/connector", oldConnectorToken, http.StatusUnauthorized)
	check("/connector", newConnectorToken, http.StatusNoContent)
	check("/tenant", newConnectorToken, http.StatusUnauthorized)

	newCustomer, _, _ := coreReadTenant(t, a)
	if r := request("/tenant", newCustomer.Token); r.Code != http.StatusNoContent || r.Header().Get("X-Test-Tenant") != newCustomer.ID {
		t.Fatal("replica did not authenticate a newly created tenant")
	}
	if _, _, _, err := a.RegisterTenantCluster(c.ID, owner, "cl-core-next-"+suffix, "New cluster", state.HumanActor(owner, c.ID)); err != nil {
		t.Fatal(err)
	}
	if r := request("/connector", newConnectorToken); r.Code != http.StatusNoContent || r.Header().Get("X-Test-Clusters") != "2" {
		t.Fatal("connector authentication used the stale cluster registry")
	}
	if _, err := a.DeleteTenantCluster(c.ID, owner, clusterID, state.HumanActor(owner, c.ID)); err != nil {
		t.Fatal(err)
	}
	check("/connector", oldConnectorToken, http.StatusUnauthorized)
	check("/connector", newConnectorToken, http.StatusUnauthorized)
	if err := a.RevokeCustomer(c.ID); err != nil {
		t.Fatal(err)
	}
	check("/tenant", oldTenantToken, http.StatusUnauthorized)
	check("/tenant", newTenantToken, http.StatusUnauthorized)
	if err := a.DeleteRevokedCustomer(c.ID); err != nil {
		t.Fatal(err)
	}
	check("/tenant", newTenantToken, http.StatusUnauthorized)
	check("/connector", newConnectorToken, http.StatusUnauthorized)
	b.Close()
	check("/tenant", oldTenantToken, http.StatusServiceUnavailable)
	check("/connector", oldConnectorToken, http.StatusServiceUnavailable)
}
