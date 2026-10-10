//go:build integration

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestPostgresWorkloadReadsCoreAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	a := coreReadStore(t)
	c, owner, suffix := coreReadTenant(t, a)
	legacyToken := c.Token
	clusterID, siblingID := "cl-core-read-"+suffix, "cl-core-sibling-"+suffix
	_, connectorToken, _, err := a.RegisterTenantCluster(c.ID, owner, clusterID, "Reader", state.HumanActor(owner, c.ID))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := a.RegisterTenantCluster(c.ID, owner, siblingID, "Sibling", state.HumanActor(owner, c.ID)); err != nil {
		t.Fatal(err)
	}
	created := time.Now().UTC().Add(-time.Hour)
	const spec = "apiVersion: yscale.sh/v1\nkind: Workload\nmetadata:\n  name: read-test\nspec:\n  image: busybox:latest\n  size: small\n"
	old := &state.Workload{ID: "wl_core_old_" + suffix, CustomerID: c.ID, ClusterID: clusterID, BurstID: "burst_core_old_" + suffix, Status: "running", CreatedAt: created, SpecYAML: []byte(spec)}
	put := func(w *state.Workload) {
		t.Helper()
		if err := a.PutWorkloadDurable(w); err != nil {
			t.Fatal(err)
		}
	}
	put(old)
	if err := a.PutBurst(&state.Burst{ID: old.BurstID, CustomerID: c.ID, Backend: "flyio", CreatedAt: created, HourlyUSD: 1}); err != nil {
		t.Fatal(err)
	}
	b := coreReadStore(t)
	h := &Workloads{Store: b, Log: quietLog()}
	mux := http.NewServeMux()
	mux.Handle("GET /v1/workloads/{id}", ConnectorWorkloadAuth(b, http.HandlerFunc(h.Get)))
	read := func(id, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/v1/workloads/"+id, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, r)
		return response
	}
	finished := time.Now().UTC()
	old.Status, old.FinishedAt = "completed", &finished
	old.Cost = &state.WorkloadCost{BurstID: old.BurstID, EstimatedUSD: 0.25, HourlyUSD: 1}
	put(old)
	if err := a.DeleteBurst(old.BurstID); err != nil {
		t.Fatal(err)
	}
	newWorkload := &state.Workload{ID: "wl_core_new_" + suffix, CustomerID: c.ID, ClusterID: clusterID, Status: "provisioning", CreatedAt: created.Add(time.Minute), SpecYAML: []byte(spec)}
	foreign := &state.Workload{ID: "wl_core_foreign_" + suffix, CustomerID: "other_" + suffix, Status: "running", CreatedAt: created.Add(2 * time.Minute), SpecYAML: []byte(spec)}
	put(newWorkload)
	put(foreign)
	for _, expected := range []*state.Workload{old, newWorkload} {
		for _, token := range []string{legacyToken, connectorToken} {
			r := read(expected.ID, token)
			var body map[string]any
			if r.Code != http.StatusOK || json.Unmarshal(r.Body.Bytes(), &body) != nil || body["status"] != expected.Status {
				t.Fatalf("routed durable detail response=%d, status=%v", r.Code, body["status"])
			}
			if expected.Cost != nil && (body["cost"] == nil || body["spent_usd"] != nil) {
				t.Fatal("routed detail retained stale live spend instead of frozen cost")
			}
		}
		r := httptest.NewRecorder()
		h.Get(r, workloadGetReq(c, expected.ID))
		if r.Code != http.StatusOK {
			t.Fatalf("tenant detail=%d", r.Code)
		}
	}
	for _, id := range []string{foreign.ID, "wl_core_missing_" + suffix} {
		for _, token := range []string{legacyToken, connectorToken} {
			if r := read(id, token); r.Code != http.StatusNotFound {
				t.Errorf("foreign/missing workload response=%d", r.Code)
			}
		}
	}
	moved := *old
	moved.ClusterID = siblingID
	coreReadRebindWorkloadCluster(t, moved.ID, moved.ClusterID)
	if r := read(old.ID, connectorToken); r.Code != http.StatusNotFound {
		t.Errorf("stale connector binding response=%d", r.Code)
	}
	if r := read(old.ID, legacyToken); r.Code != http.StatusOK {
		t.Errorf("tenant lost sibling access: %d", r.Code)
	}
	coreReadRebindWorkloadCluster(t, old.ID, old.ClusterID)
	tieA, tieB := "wl_core_tie_a_"+suffix, "wl_core_tie_b_"+suffix
	for _, id := range []string{tieA, tieB} {
		at := created.Add(10 * time.Minute)
		if id == tieB {
			at = at.In(time.FixedZone("minus-seven", -7*60*60))
		}
		put(&state.Workload{ID: id, CustomerID: c.ID, CreatedAt: at})
	}
	rows, err := b.WorkloadSnapshotsForCustomer(ctx, c.ID, 2)
	if err != nil || len(rows) != 2 || rows[0].Workload.ID != tieA || rows[1].Workload.ID != tieB {
		t.Fatalf("durable ordering/limit failed: %v", err)
	}
	newWorkload.BurstID = "burst_core_new_" + suffix
	put(newWorkload)
	burst := &state.Burst{ID: newWorkload.BurstID, CustomerID: c.ID, CreatedAt: created, HourlyUSD: 2}
	if err := a.PutBurst(burst); err != nil {
		t.Fatal(err)
	}
	snapshot, err := b.WorkloadSnapshotForCustomer(ctx, c.ID, newWorkload.ID)
	if err != nil || snapshot.Burst == nil || snapshot.Burst.HourlyUSD != 2 {
		t.Fatalf("new durable burst not joined: %v", err)
	}
	burst.CustomerID = foreign.CustomerID
	if err := a.PutBurst(burst); err != nil {
		t.Fatal(err)
	}
	snapshot, err = b.WorkloadSnapshotForCustomer(ctx, c.ID, newWorkload.ID)
	if err != nil || snapshot.Burst != nil {
		t.Fatalf("foreign burst joined: %v", err)
	}
	if _, err := b.GetWorkload(newWorkload.ID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("snapshot hydrated mutation cache: %v", err)
	}
	if cached, err := b.GetWorkload(old.ID); err != nil || cached.Status != "running" {
		t.Fatalf("snapshot replaced cached workload: %v", err)
	}
	b.Close()
	for _, token := range []string{legacyToken, connectorToken} {
		if r := read(old.ID, token); r.Code != http.StatusServiceUnavailable {
			t.Errorf("closed database response=%d", r.Code)
		}
	}
	r := httptest.NewRecorder()
	h.Get(r, workloadGetReq(c, old.ID))
	if r.Code != http.StatusServiceUnavailable {
		t.Errorf("closed database direct handler=%d", r.Code)
	}
}
