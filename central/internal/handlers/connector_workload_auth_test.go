package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestConnectorWorkloadReadUsesAuthorizedSnapshot(t *testing.T) {
	fx := callbackWorld(t)
	wls := &Workloads{Store: fx.store, Log: quietLog()}
	mux := http.NewServeMux()
	mux.Handle("GET /v1/workloads/{id}", ConnectorWorkloadAuth(fx.store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Deterministically change the row between middleware and projection.
		// The response must use the same snapshot that authorized this read.
		changed, err := fx.store.GetWorkload("wl_a")
		if err != nil {
			t.Fatal(err)
		}
		changed.ClusterID, changed.Status = "cl-sibling", "sibling-private"
		fx.store.PutWorkload(changed)
		wls.Get(w, r)
	})))
	fx.mux = mux
	rec := fx.call(t, http.MethodGet, "/v1/workloads/wl_a", fx.scopedToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("authorized snapshot response = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["cluster_id"] != "cl-a" || body["status"] != "provisioning" {
		t.Fatalf("response was not the authorized snapshot: cluster=%v status=%v", body["cluster_id"], body["status"])
	}
}

func TestConnectorWorkloadReadSnapshotCannotChangeRequestScope(t *testing.T) {
	for _, change := range []string{"workload", "tenant", "cluster", "store"} {
		t.Run(change, func(t *testing.T) {
			fx := callbackWorld(t)
			wls := &Workloads{Store: fx.store, Log: quietLog()}
			mux := http.NewServeMux()
			mux.Handle("GET /v1/workloads/{id}", ConnectorWorkloadAuth(fx.store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch change {
				case "workload":
					r.SetPathValue("id", "wl_sibling")
				case "tenant":
					r = r.WithContext(context.WithValue(r.Context(), ctxCustomer, &state.Customer{ID: "cust_b"}))
				case "cluster":
					r = r.WithContext(context.WithValue(r.Context(), ctxAgentCluster, "cl-sibling"))
				case "store":
					wls.Store = state.New()
				}
				wls.Get(w, r)
			})))
			fx.mux = mux
			rec := fx.call(t, http.MethodGet, "/v1/workloads/wl_a", fx.scopedToken, "")
			if rec.Code != http.StatusNotFound {
				t.Fatalf("changed request scope = %d, want 404", rec.Code)
			}
		})
	}
}

func TestConnectorWorkloadHeadUsesReadAuthorization(t *testing.T) {
	fx := callbackWorld(t)
	mux := http.NewServeMux()
	mux.Handle("GET /v1/workloads/{id}", ConnectorWorkloadAuth(fx.store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Context().Value(ctxConnectorWorkloadRead).(connectorWorkloadRead); !ok {
			t.Error("HEAD matched GET but skipped snapshot authorization")
		}
		w.WriteHeader(http.StatusOK)
	})))
	fx.mux = mux
	if rec := fx.call(t, http.MethodHead, "/v1/workloads/wl_a", fx.scopedToken, ""); rec.Code != http.StatusOK {
		t.Fatalf("HEAD own workload = %d", rec.Code)
	}
	if rec := fx.call(t, http.MethodHead, "/v1/workloads/wl_sibling", fx.scopedToken, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("HEAD sibling workload = %d", rec.Code)
	}
}

// callbackFixture is registryStore plus the world the two agent callbacks act
// on: a second cluster under the SAME tenant (the sibling a scoped credential
// must not reach), a neighbouring tenant, and one running workload on each.
type callbackFixture struct {
	registryFixture
	mux *http.ServeMux
	// scopedToken is bound to cl-a; legacyToken is cust_a's plain tenant token.
	scopedToken string
	legacyToken string
	reaper      *fakeReaper
}

// callbackWorld seeds workloads that are already running with a live burst
// behind them, so a refused callback is visibly a workload that did NOT start
// and a burst that was NOT reaped.
func callbackWorld(t *testing.T) callbackFixture {
	t.Helper()
	fx := registryStore(t)
	s := fx.store
	if _, _, _, err := s.RegisterTenantCluster("cust_a", fx.aliceID, "cl-sibling", "Sibling", state.HumanActor(fx.aliceID, "cust_a")); err != nil {
		t.Fatalf("RegisterTenantCluster sibling: %v", err)
	}
	workloads := []struct{ id, customer, cluster string }{
		{"wl_a", "cust_a", "cl-a"},             // the scoped credential's own
		{"wl_sibling", "cust_a", "cl-sibling"}, // same tenant, another cluster
		{"wl_b", "cust_b", "cl-a"},             // another tenant reusing the id
		{"wl_legacy", "cust_a", ""},            // written before routing recorded a cluster
	}
	for _, w := range workloads {
		s.PutWorkload(&state.Workload{
			ID: w.id, CustomerID: w.customer, ClusterID: w.cluster,
			Status: "provisioning", BurstID: "b_" + w.id, CreatedAt: time.Now().UTC(),
		})
		s.PutBurst(&state.Burst{ID: "b_" + w.id, CustomerID: w.customer, CreatedAt: time.Now(), NodeOnly: true})
	}

	reaper := &fakeReaper{}
	wls := &Workloads{Store: s, Decider: &fakeDecider{}, Reaper: reaper, Log: quietLog()}
	// The wiring under test, mirroring main.go: submit, status read, the two
	// callbacks and cancel are connector-aware; tenant-wide money surfaces stay
	// tenant-only.
	mux := http.NewServeMux()
	mux.Handle("POST /v1/workloads", ConnectorSubmitAuth(s, http.HandlerFunc(wls.Create)))
	mux.Handle("GET /v1/workloads/{id}", ConnectorWorkloadAuth(s, http.HandlerFunc(wls.Get)))
	mux.Handle("DELETE /v1/workloads/{id}", ConnectorWorkloadAuth(s, http.HandlerFunc(wls.Cancel)))
	mux.Handle("POST /v1/workloads/{id}/started", ConnectorWorkloadAuth(s, http.HandlerFunc(wls.Started)))
	mux.Handle("POST /v1/workloads/{id}/complete", ConnectorWorkloadAuth(s, http.HandlerFunc(wls.Complete)))
	mux.Handle("GET /v1/spend", Auth(s, http.HandlerFunc(wls.Spend)))
	mux.Handle("GET /v1/bursts", Auth(s, http.HandlerFunc(wls.Bursts)))
	mux.Handle("GET /v1/price/gpu", Auth(s, &GPUPrices{Log: quietLog()}))

	return callbackFixture{registryFixture: fx, mux: mux, scopedToken: fx.token, legacyToken: "tok_a", reaper: reaper}
}

// call drives a request through the mux so path values are resolved exactly as
// the server resolves them — the middleware reads {id} from the same match.
func (fx callbackFixture) call(t *testing.T, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	fx.mux.ServeHTTP(rec, req)
	return rec
}

// The credential the hosted connector actually holds can report the start and
// completion of a workload running on its own cluster — the seam that returned
// 401 when these routes sat behind tenant-only Auth.
func TestConnectorWorkloadAuthAllowsScopedCallbacksOnItsOwnCluster(t *testing.T) {
	fx := callbackWorld(t)

	if rec := fx.call(t, http.MethodPost, "/v1/workloads/wl_a/started", fx.scopedToken, ""); rec.Code != http.StatusOK {
		t.Fatalf("scoped started = %d, want 200: %s", rec.Code, rec.Body)
	}
	wl, err := fx.store.GetWorkload("wl_a")
	if err != nil || wl.Status != "running" || wl.StartedAt == nil {
		t.Fatalf("scoped started did not stamp the workload: %+v err=%v", wl, err)
	}

	if rec := fx.call(t, http.MethodPost, "/v1/workloads/wl_a/complete", fx.scopedToken, `{"phase":"Succeeded"}`); rec.Code != http.StatusOK {
		t.Fatalf("scoped complete = %d, want 200: %s", rec.Code, rec.Body)
	}
	if !fx.reaper.reaped("b_wl_a") {
		t.Fatal("scoped complete should have reaped the workload's burst")
	}
	wl, _ = fx.store.GetWorkload("wl_a")
	if wl.Status != "succeeded" {
		t.Fatalf("status after scoped complete = %q, want succeeded", wl.Status)
	}
}

// Every already-deployed connector presents the tenant token, which is bound to
// no cluster. It keeps working on both callbacks, including on the sibling
// cluster's workload and on a record that predates cluster routing.
func TestConnectorWorkloadAuthKeepsLegacyTenantTokenWorking(t *testing.T) {
	fx := callbackWorld(t)

	for _, id := range []string{"wl_sibling", "wl_legacy"} {
		if rec := fx.call(t, http.MethodPost, "/v1/workloads/"+id+"/started", fx.legacyToken, ""); rec.Code != http.StatusOK {
			t.Fatalf("legacy started %s = %d, want 200: %s", id, rec.Code, rec.Body)
		}
		if rec := fx.call(t, http.MethodPost, "/v1/workloads/"+id+"/complete", fx.legacyToken, `{"phase":"Succeeded"}`); rec.Code != http.StatusOK {
			t.Fatalf("legacy complete %s = %d, want 200: %s", id, rec.Code, rec.Body)
		}
		if !fx.reaper.reaped("b_" + id) {
			t.Fatalf("legacy complete %s should have reaped its burst", id)
		}
	}

	// A tenant token still cannot reach another tenant's workload.
	if rec := fx.call(t, http.MethodPost, "/v1/workloads/wl_b/started", fx.legacyToken, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("legacy token on another tenant's workload = %d, want 404", rec.Code)
	}
}

// A scoped credential reaches ONLY the workloads its own cluster is running.
// A sibling cluster's workload, another tenant's workload, an ambiguous legacy
// record with no cluster, and one that does not exist are all the same 404 —
// and none of them is started or reaped by the attempt.
func TestConnectorWorkloadAuthRefusesWorkloadsOutsideItsCluster(t *testing.T) {
	refusals := []struct {
		name string
		id   string
	}{
		{"same tenant, sibling cluster", "wl_sibling"},
		{"another tenant", "wl_b"},
		{"no cluster recorded", "wl_legacy"},
		{"unknown workload", "wl_does_not_exist"},
	}

	for _, tt := range refusals {
		t.Run(tt.name, func(t *testing.T) {
			fx := callbackWorld(t)

			rec := fx.call(t, http.MethodPost, "/v1/workloads/"+tt.id+"/started", fx.scopedToken, "")
			if rec.Code != http.StatusNotFound {
				t.Fatalf("scoped started = %d, want 404: %s", rec.Code, rec.Body)
			}
			rec = fx.call(t, http.MethodPost, "/v1/workloads/"+tt.id+"/complete", fx.scopedToken, `{"phase":"Succeeded"}`)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("scoped complete = %d, want 404: %s", rec.Code, rec.Body)
			}

			if fx.reaper.teardownCount("b_"+tt.id) != 0 {
				t.Fatal("refused callback tore down a burst it does not own")
			}
			wl, err := fx.store.GetWorkload(tt.id)
			if err != nil {
				return // the unknown-workload case has nothing to inspect
			}
			if wl.Status != "provisioning" || wl.StartedAt != nil || wl.FinishedAt != nil {
				t.Fatalf("refused callback mutated the workload: %+v", wl)
			}
			if _, err := fx.store.GetBurst("b_" + tt.id); err != nil {
				t.Fatal("refused callback removed the burst")
			}
		})
	}
}

// A legacy in-flight workload may predate persisted cluster routing. If that
// tenant has exactly one registered cluster, rotating its connector to a scoped
// credential must not strand the billed burst. The same empty record becomes
// ambiguous and is refused as soon as the tenant has sibling clusters (covered
// above).
func TestConnectorWorkloadAuthAllowsLegacyRecordForSoleRegisteredCluster(t *testing.T) {
	fx := registryStore(t)
	fx.store.PutWorkload(&state.Workload{
		ID: "wl_legacy", CustomerID: "cust_a", Status: "provisioning",
		BurstID: "b_legacy", CreatedAt: time.Now().UTC(),
	})
	fx.store.PutBurst(&state.Burst{ID: "b_legacy", CustomerID: "cust_a", CreatedAt: time.Now(), NodeOnly: true})
	reaper := &fakeReaper{}
	wls := &Workloads{Store: fx.store, Reaper: reaper, Log: quietLog()}
	mux := http.NewServeMux()
	mux.Handle("POST /v1/workloads/{id}/complete", ConnectorWorkloadAuth(fx.store, http.HandlerFunc(wls.Complete)))

	req := httptest.NewRequest(http.MethodPost, "/v1/workloads/wl_legacy/complete", strings.NewReader(`{"phase":"Succeeded"}`))
	req.Header.Set("Authorization", "Bearer "+fx.token)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("sole-cluster legacy complete = %d, want 200: %s", rec.Code, rec.Body)
	}
	if !reaper.reaped("b_legacy") {
		t.Fatal("sole-cluster legacy completion did not reap the burst")
	}
}

// Cancel is a connector route on the same terms as the callbacks: the
// credential cancels the work its OWN cluster is running, and the burst behind
// it is actually torn down.
func TestConnectorWorkloadAuthAllowsScopedCancelOnItsOwnCluster(t *testing.T) {
	fx := callbackWorld(t)

	if rec := fx.call(t, http.MethodDelete, "/v1/workloads/wl_a", fx.scopedToken, ""); rec.Code != http.StatusOK {
		t.Fatalf("scoped cancel = %d, want 200: %s", rec.Code, rec.Body)
	}
	if !fx.reaper.reaped("b_wl_a") {
		t.Fatal("scoped cancel did not reap the workload's burst")
	}
	wl, err := fx.store.GetWorkload("wl_a")
	if err != nil || wl.Status != "cancelled" {
		t.Fatalf("workload after scoped cancel = %+v err=%v, want cancelled", wl, err)
	}
}

// A scoped credential cancels nothing outside its own cluster. A sibling
// cluster's workload, another tenant's, an ambiguous record naming no cluster,
// and one that never existed are the SAME 404 — anything else would let a
// connector enumerate the tenant's other clusters by cancel.
func TestConnectorWorkloadAuthRefusesCancelOutsideItsCluster(t *testing.T) {
	refusals := []struct{ name, id string }{
		{"same tenant, sibling cluster", "wl_sibling"},
		{"another tenant", "wl_b"},
		{"no cluster recorded", "wl_legacy"},
		{"unknown workload", "wl_does_not_exist"},
	}

	for _, tt := range refusals {
		t.Run(tt.name, func(t *testing.T) {
			fx := callbackWorld(t)

			rec := fx.call(t, http.MethodDelete, "/v1/workloads/"+tt.id, fx.scopedToken, "")
			if rec.Code != http.StatusNotFound {
				t.Fatalf("scoped cancel = %d, want 404: %s", rec.Code, rec.Body)
			}
			if fx.reaper.teardownCount("b_"+tt.id) != 0 {
				t.Fatal("refused cancel tore down a burst it does not own")
			}
			wl, err := fx.store.GetWorkload(tt.id)
			if err != nil {
				return // the unknown-workload case has nothing to inspect
			}
			if wl.Status != "provisioning" || wl.FinishedAt != nil {
				t.Fatalf("refused cancel mutated the workload: %+v", wl)
			}
			if _, err := fx.store.GetBurst("b_" + tt.id); err != nil {
				t.Fatal("refused cancel removed the burst")
			}
		})
	}
}

// The legacy tenant token cancels exactly as it always did — including a
// sibling cluster's workload and a record that predates cluster routing — and
// still cannot reach another tenant's.
func TestConnectorWorkloadAuthKeepsLegacyTenantCancelWorking(t *testing.T) {
	fx := callbackWorld(t)

	for _, id := range []string{"wl_sibling", "wl_legacy"} {
		if rec := fx.call(t, http.MethodDelete, "/v1/workloads/"+id, fx.legacyToken, ""); rec.Code != http.StatusOK {
			t.Fatalf("legacy cancel %s = %d, want 200: %s", id, rec.Code, rec.Body)
		}
		if !fx.reaper.reaped("b_" + id) {
			t.Fatalf("legacy cancel %s should have reaped its burst", id)
		}
	}
	if rec := fx.call(t, http.MethodDelete, "/v1/workloads/wl_b", fx.legacyToken, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("legacy cancel of another tenant's workload = %d, want 404", rec.Code)
	}
}

// A scoped connector may read the workload its cluster is running so it can
// project central-authoritative status back to the CR. It still cannot read a
// sibling cluster or tenant, and tenant-wide money surfaces remain closed.
func TestConnectorWorkloadAuthAllowsOnlyOwnWorkloadRead(t *testing.T) {
	fx := callbackWorld(t)

	if rec := fx.call(t, http.MethodGet, "/v1/workloads/wl_a", fx.scopedToken, ""); rec.Code != http.StatusOK {
		t.Fatalf("scoped credential reading own workload = %d, want 200: %s", rec.Code, rec.Body)
	}
	for _, id := range []string{"wl_sibling", "wl_legacy", "wl_b"} {
		if rec := fx.call(t, http.MethodGet, "/v1/workloads/"+id, fx.scopedToken, ""); rec.Code != http.StatusNotFound {
			t.Fatalf("scoped credential reading %s = %d, want 404: %s", id, rec.Code, rec.Body)
		}
	}

	tenantOnly := []struct{ method, path string }{
		{http.MethodGet, "/v1/spend"},
		{http.MethodGet, "/v1/bursts"},
		{http.MethodGet, "/v1/price/gpu"},
	}
	for _, route := range tenantOnly {
		if rec := fx.call(t, route.method, route.path, fx.scopedToken, ""); rec.Code != http.StatusUnauthorized {
			t.Fatalf("scoped credential on %s %s = %d, want 401: %s", route.method, route.path, rec.Code, rec.Body)
		}
	}
	// The read route still answers the legacy tenant token — scoped reads are
	// additive and do not break the pre-connector credential contract.
	if rec := fx.call(t, http.MethodGet, "/v1/workloads/wl_a", fx.legacyToken, ""); rec.Code != http.StatusOK {
		t.Fatalf("tenant token on GET /v1/workloads/{id} = %d, want 200: %s", rec.Code, rec.Body)
	}
}

// The callbacks reject the same non-credentials every other authenticated route
// rejects, and a scoped credential naming a foreign cluster in X-Cluster-ID is
// refused before the handler runs.
func TestConnectorWorkloadAuthRefusesUnknownAndMismatchedCredentials(t *testing.T) {
	fx := callbackWorld(t)

	for _, token := range []string{"", "tok_wrong"} {
		if rec := fx.call(t, http.MethodPost, "/v1/workloads/wl_a/started", token, ""); rec.Code != http.StatusUnauthorized {
			t.Fatalf("token %q = %d, want 401", token, rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/workloads/wl_a/started", nil)
	req.Header.Set("Authorization", "Bearer "+fx.scopedToken)
	req.Header.Set(clusterIDHeader, "cl-sibling")
	rec := httptest.NewRecorder()
	fx.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("scoped credential naming another cluster = %d, want 403: %s", rec.Code, rec.Body)
	}
	if wl, _ := fx.store.GetWorkload("wl_a"); wl.StartedAt != nil {
		t.Fatal("refused header mismatch still started the workload")
	}
}
