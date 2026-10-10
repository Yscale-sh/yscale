//go:build integration

// yscale:proprietary

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestPostgresCredentialAuthAcrossReplicas(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	ctx := context.Background()
	a, err := state.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	tenant, oldTenantToken := "cust_auth_"+suffix, "test_auth_"+suffix
	a.AddCustomer(&state.Customer{ID: tenant, Token: oldTenantToken, Plan: "pro"})
	_, owners := rosterFixture(t, a, tenant, map[string]string{"credential-reader": state.RoleOwner})
	owner := owners["credential-reader"]
	clusterID := "cl-auth-" + suffix
	_, oldConnectorToken, _, err := a.RegisterTenantCluster(tenant, owner, clusterID, "Reader", state.HumanActor(owner, tenant))
	if err != nil {
		t.Fatal(err)
	}
	publisher, oldPublisherToken, _, err := a.CreateCatalogPublisher(tenant, owner, "Publisher")
	if err != nil {
		t.Fatal(err)
	}
	b, err := state.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	probe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if publisher, ok := catalogPublisherFromContext(r.Context()); ok {
			w.Header().Set("X-Test-Tenant", publisher.TenantID)
			w.WriteHeader(http.StatusNoContent)
			return
		}
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
	mux.Handle("GET /publisher/{tenant_id}", CatalogPublisherAuth(b, probe))
	request := func(path, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	check := func(t *testing.T, label, path, token string, want int) {
		t.Helper()
		if got := request(path, token).Code; got != want {
			t.Errorf("%s = %d, want %d", label, got, want)
		}
	}
	publisherPath := "/publisher/" + tenant
	check(t, "initial tenant", "/tenant", oldTenantToken, 204)
	check(t, "initial connector", "/connector", oldConnectorToken, 204)
	check(t, "initial publisher", publisherPath, oldPublisherToken, 204)

	newTenantToken, err := a.RotateCustomerCredential(tenant, state.OperatorActor())
	if err != nil {
		t.Fatal(err)
	}
	_, newConnectorToken, _, err := a.RotateTenantClusterCredential(tenant, owner, clusterID, state.HumanActor(owner, tenant))
	if err != nil {
		t.Fatal(err)
	}
	_, newPublisherToken, _, err := a.RotateCatalogPublisherCredential(tenant, owner, publisher.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("rotation", func(t *testing.T) {
		for _, tc := range []struct{ name, path, old, next string }{
			{"tenant", "/tenant", oldTenantToken, newTenantToken},
			{"legacy connector", "/connector", oldTenantToken, newTenantToken},
			{"scoped connector", "/connector", oldConnectorToken, newConnectorToken},
			{"publisher", publisherPath, oldPublisherToken, newPublisherToken},
		} {
			check(t, tc.name+" old token", tc.path, tc.old, 401)
			check(t, tc.name+" rotated token", tc.path, tc.next, 204)
		}
	})
	t.Run("credential-kinds-stay-isolated", func(t *testing.T) {
		check(t, "connector on tenant route", "/tenant", newConnectorToken, 401)
		check(t, "publisher on tenant route", "/tenant", newPublisherToken, 401)
		check(t, "publisher on connector route", "/connector", newPublisherToken, 401)
		check(t, "tenant on publisher route", publisherPath, newTenantToken, 401)
		check(t, "connector on publisher route", publisherPath, newConnectorToken, 401)
		check(t, "publisher names another tenant", "/publisher/other", newPublisherToken, 403)
	})
	t.Run("new-tenant-and-current-registry", func(t *testing.T) {
		newID, newToken := "cust_new_auth_"+suffix, "test_new_auth_"+suffix
		a.AddCustomer(&state.Customer{ID: newID, Token: newToken, Plan: "pro"})
		rec := request("/tenant", newToken)
		if rec.Code != 204 || rec.Header().Get("X-Test-Tenant") != newID {
			t.Errorf("tenant created after replica B loaded = %d, want correct tenant/204", rec.Code)
		}
		if _, _, _, err := a.RegisterTenantCluster(tenant, owner, "cl-next-"+suffix, "New cluster", state.HumanActor(owner, tenant)); err != nil {
			t.Fatal(err)
		}
		rec = request("/connector", newConnectorToken)
		if rec.Code != 204 || rec.Header().Get("X-Test-Clusters") != "2" {
			t.Errorf("current cluster registry = %d/%q, want 204/2", rec.Code, rec.Header().Get("X-Test-Clusters"))
		}
	})
	t.Run("removed-credentials", func(t *testing.T) {
		if _, err := a.DeleteCatalogPublisher(tenant, owner, publisher.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := a.DeleteTenantCluster(tenant, owner, clusterID, state.HumanActor(owner, tenant)); err != nil {
			t.Fatal(err)
		}
		check(t, "removed publisher old credential", publisherPath, oldPublisherToken, 401)
		check(t, "removed publisher new credential", publisherPath, newPublisherToken, 401)
		check(t, "removed connector old credential", "/connector", oldConnectorToken, 401)
		check(t, "removed connector new credential", "/connector", newConnectorToken, 401)
	})
	t.Run("offboard-and-final-delete", func(t *testing.T) {
		if err := a.RevokeCustomer(tenant); err != nil {
			t.Fatal(err)
		}
		check(t, "revoked tenant old token", "/tenant", oldTenantToken, 401)
		check(t, "revoked tenant new token", "/tenant", newTenantToken, 401)
		if err := a.DeleteRevokedCustomer(tenant); err != nil {
			t.Fatal(err)
		}
		check(t, "deleted tenant", "/tenant", oldTenantToken, 401)
		check(t, "deleted tenant connector", "/connector", oldConnectorToken, 401)
		check(t, "deleted tenant publisher", publisherPath, oldPublisherToken, 401)
	})
	t.Run("database-failure-is-not-auth-success-or-bad-token", func(t *testing.T) {
		b.Close()
		check(t, "closed database tenant", "/tenant", oldTenantToken, 503)
		check(t, "closed database connector", "/connector", oldConnectorToken, 503)
		check(t, "closed database publisher", publisherPath, oldPublisherToken, 503)
	})
}
