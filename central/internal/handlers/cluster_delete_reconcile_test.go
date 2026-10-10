// yscale:proprietary

package handlers

import (
	"net/http"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// A cluster delete is the one route-intent change no agent can report: by the
// time it lands the connector is being taken away, so nothing will ever
// reconnect and re-assert. Both delete surfaces therefore have to drive the
// reconciler themselves, and through the REMOVAL seam — an ordinary enqueue
// converges a gateway, it never withdraws one.
func TestTenantClusterDeleteEnqueuesRouteWithdrawal(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "view": state.RoleViewer,
	})
	rec := &recordingReconciler{}
	accounts.Reconciler = rec

	if res := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_alice/clusters",
		"human_alice", `{"cluster_id":"cl-prod","name":"Prod"}`); res.Code != http.StatusCreated {
		t.Fatalf("register = %d: %s", res.Code, res.Body)
	}
	if _, err := store.SetCustomerGatewayRoutes("cust_alice", "cl-prod", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed routes: %v", err)
	}

	// A REFUSED delete enqueues nothing: the routes are still the tenant's.
	if res := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_alice/clusters/cl-prod",
		"human_view", ""); res.Code != http.StatusForbidden {
		t.Fatalf("viewer delete = %d, want 403: %s", res.Code, res.Body)
	}
	if res := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_alice/clusters/cl-missing",
		"human_alice", ""); res.Code != http.StatusNotFound {
		t.Fatalf("delete of an unknown cluster = %d, want 404: %s", res.Code, res.Body)
	}
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Fatalf("refused deletes enqueued %+v, want nothing", calls)
	}

	if res := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_alice/clusters/cl-prod",
		"human_alice", ""); res.Code != http.StatusOK {
		t.Fatalf("owner delete = %d, want 200: %s", res.Code, res.Body)
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("enqueues after a successful delete = %+v, want exactly 1", calls)
	}
	if got := calls[0]; got.customerID != "cust_alice" || got.clusterID != "cl-prod" || !got.removal {
		t.Fatalf("enqueue = %+v, want a removal naming cust_alice/cl-prod", got)
	}
	if calls[0].force {
		t.Error("removal carried force; the delete moved the stored intent, so the ordinary gates already see the work")
	}
}

// The operator's hosted-capacity release is the same lifecycle delete and owes
// the same withdrawal.
func TestHostedClusterDeleteEnqueuesRouteWithdrawal(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro"})
	tn := newTenants(store)
	rec := &recordingReconciler{}
	tn.Reconciler = rec

	if res := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_h/hosted-clusters",
		`{"cluster_id":"hosted-h","namespace":"ys-h"}`); res.Code != http.StatusCreated {
		t.Fatalf("assign = %d: %s", res.Code, res.Body)
	}
	if _, err := store.SetCustomerGatewayRoutes("cust_h", "hosted-h", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed routes: %v", err)
	}

	if res := callHosted(tn, http.MethodDelete, "/v1/admin/tenants/cust_h/hosted-clusters/hosted-gone", ""); res.Code == http.StatusOK {
		t.Fatalf("delete of an unknown assignment = 200, want a refusal: %s", res.Body)
	}
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Fatalf("refused release enqueued %+v, want nothing", calls)
	}

	if res := callHosted(tn, http.MethodDelete, "/v1/admin/tenants/cust_h/hosted-clusters/hosted-h", ""); res.Code != http.StatusOK {
		t.Fatalf("release = %d, want 200: %s", res.Code, res.Body)
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("enqueues after a successful release = %+v, want exactly 1", calls)
	}
	if got := calls[0]; got.customerID != "cust_h" || got.clusterID != "hosted-h" || !got.removal {
		t.Fatalf("enqueue = %+v, want a removal naming cust_h/hosted-h", got)
	}
}

// Both handlers are wired for deployments that have no reconciler at all (the
// OSS build): the delete still lands, because the durable intent is already
// correct without it.
func TestClusterDeleteWithoutReconcilerStillSucceeds(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})
	if res := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_alice/clusters",
		"human_alice", `{"cluster_id":"cl-prod","name":"Prod"}`); res.Code != http.StatusCreated {
		t.Fatalf("register = %d: %s", res.Code, res.Body)
	}
	if res := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_alice/clusters/cl-prod",
		"human_alice", ""); res.Code != http.StatusOK {
		t.Fatalf("delete without a reconciler = %d, want 200: %s", res.Code, res.Body)
	}
}
