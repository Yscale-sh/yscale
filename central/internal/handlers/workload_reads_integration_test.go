//go:build integration

// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// Replica B is started before A's mutations and never refreshes its local
// maps. The detail handler and human list/detail wrapper must read durable truth.
func TestPostgresWorkloadReadsAcrossReplicas(t *testing.T) {
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
	legacyToken := "test_read_" + suffix
	customer := &state.Customer{ID: "cust_read_" + suffix, Plan: "pro", Token: legacyToken}
	a.AddCustomer(customer)
	accountsA, accountIDs := rosterFixture(t, a, customer.ID, map[string]string{"read-replica": state.RoleOwner})
	owner := accountIDs["read-replica"]
	clusterID, siblingID := "cl-read-"+suffix, "cl-sibling-"+suffix
	_, connectorToken, _, err := a.RegisterTenantCluster(customer.ID, owner, clusterID, "Reader", state.HumanActor(owner, customer.ID))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := a.RegisterTenantCluster(customer.ID, owner, siblingID, "Sibling", state.HumanActor(owner, customer.ID)); err != nil {
		t.Fatal(err)
	}
	created := time.Now().UTC().Add(-time.Hour)
	old := &state.Workload{
		ID: "wl_old_" + suffix, CustomerID: customer.ID, ClusterID: clusterID, BurstID: "burst_old_" + suffix,
		Status: "running", CreatedAt: created, SpecYAML: []byte(tenantWorkloadYAML),
	}
	if err := a.PutWorkloadDurable(old); err != nil {
		t.Fatal(err)
	}
	if err := a.PutBurst(&state.Burst{ID: old.BurstID, CustomerID: customer.ID, Backend: "flyio", CreatedAt: created, HourlyUSD: 1}); err != nil {
		t.Fatal(err)
	}
	b, err := state.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	h := &Workloads{Store: b, Log: quietLog()}
	accountsB := &Accounts{Store: b, Workloads: h, Resolver: accountsA.Resolver, Issuer: testIssuer, Log: quietLog()}
	mux := http.NewServeMux()
	mux.Handle("GET /v1/workloads/{id}", ConnectorWorkloadAuth(b, http.HandlerFunc(h.Get)))
	connectorRead := func(id, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/v1/workloads/"+id, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	finished := time.Now().UTC()
	old.Status, old.FinishedAt = "completed", &finished
	old.Cost = &state.WorkloadCost{BurstID: old.BurstID, EstimatedUSD: 0.25, HourlyUSD: 1}
	if err := a.PutWorkloadDurable(old); err != nil {
		t.Fatal(err)
	}
	if err := a.DeleteBurst(old.BurstID); err != nil {
		t.Fatal(err)
	}
	newWorkload := &state.Workload{ID: "wl_new_" + suffix, CustomerID: customer.ID, ClusterID: clusterID, Status: "provisioning", CreatedAt: created.Add(time.Minute), SpecYAML: []byte(tenantWorkloadYAML)}
	if err := a.PutWorkloadDurable(newWorkload); err != nil {
		t.Fatal(err)
	}
	foreign := &state.Workload{ID: "wl_foreign_" + suffix, CustomerID: "other_" + suffix, Status: "running", CreatedAt: created.Add(2 * time.Minute), SpecYAML: []byte(tenantWorkloadYAML)}
	if err := a.PutWorkloadDurable(foreign); err != nil {
		t.Fatal(err)
	}

	for _, human := range []bool{false, true} {
		for _, expected := range []*state.Workload{old, newWorkload} {
			t.Run(fmt.Sprintf("detail-human=%v/%s", human, expected.Status), func(t *testing.T) {
				var rec *httptest.ResponseRecorder
				if human {
					rec = callTenantWorkload(accountsB, http.MethodGet, "/v1/tenants/"+customer.ID+"/workloads/"+expected.ID, "human_read-replica", "")
				} else {
					rec = httptest.NewRecorder()
					h.Get(rec, workloadGetReq(customer, expected.ID))
				}
				if rec.Code != http.StatusOK {
					t.Fatalf("detail on replica B = %d, want 200", rec.Code)
				}
				var body map[string]any
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if body["status"] != expected.Status {
					t.Errorf("replica B returned status %v, want %s", body["status"], expected.Status)
				}
				if expected.Cost != nil && (body["cost"] == nil || body["spent_usd"] != nil) {
					t.Errorf("replica B returned stale live spend instead of frozen cost")
				}
			})
		}
	}
	t.Run("human-list", func(t *testing.T) {
		rec := callTenantWorkload(accountsB, http.MethodGet, "/v1/tenants/"+customer.ID+"/workloads", "human_read-replica", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("list on replica B = %d", rec.Code)
		}
		response := decodeTenantWorkloads(t, rec)
		if len(response.Workloads) != 2 {
			t.Fatalf("replica B listed %d workloads, want 2", len(response.Workloads))
		}
		if response.Workloads[0].ID != newWorkload.ID || response.Workloads[1].ID != old.ID {
			t.Fatal("durable list order or tenant filter is wrong")
		}
		if response.Workloads[1].Status != "completed" || response.Workloads[1].Cost == nil || response.Workloads[1].SpentUSD != nil {
			t.Fatal("list retained the stale running/live-spend projection")
		}
	})
	t.Run("foreign-hidden", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.Get(rec, workloadGetReq(customer, foreign.ID))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("foreign workload = %d, want 404", rec.Code)
		}
	})
	t.Run("connector-route", func(t *testing.T) {
		for _, token := range []string{connectorToken, legacyToken} {
			for _, workload := range []*state.Workload{old, newWorkload} {
				rec := connectorRead(workload.ID, token)
				if rec.Code != http.StatusOK {
					t.Errorf("routed %s read = %d, want 200", workload.Status, rec.Code)
				}
			}
		}
		for _, id := range []string{foreign.ID, "wl_missing_" + suffix} {
			if rec := connectorRead(id, connectorToken); rec.Code != http.StatusNotFound {
				t.Errorf("routed foreign/missing read = %d, want 404", rec.Code)
			}
		}
		// B still caches this record as its own cluster. A newer durable row
		// must not pass that stale authorization and leak a sibling's response.
		moved := *old
		moved.ClusterID = siblingID
		coreReadRebindWorkloadCluster(t, moved.ID, moved.ClusterID)
		if rec := connectorRead(old.ID, connectorToken); rec.Code != http.StatusNotFound {
			t.Errorf("durably moved workload read = %d, want 404", rec.Code)
		}
		if rec := connectorRead(old.ID, legacyToken); rec.Code != http.StatusOK {
			t.Errorf("tenant credential lost sibling access: %d", rec.Code)
		}
		coreReadRebindWorkloadCluster(t, old.ID, old.ClusterID)
	})
	t.Run("snapshot-contract", func(t *testing.T) {
		// Equal instants with different UTC offsets must tie-break by ID.
		for _, id := range []string{"wl_tie_a_" + suffix, "wl_tie_b_" + suffix} {
			createdAt := created.Add(10 * time.Minute)
			if id == "wl_tie_b_"+suffix {
				createdAt = createdAt.In(time.FixedZone("minus-seven", -7*60*60))
			}
			if err := a.PutWorkloadDurable(&state.Workload{ID: id, CustomerID: customer.ID, CreatedAt: createdAt}); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := b.WorkloadSnapshotsForCustomer(ctx, customer.ID, 2)
		if err != nil || len(rows) != 2 || rows[0].Workload.ID != "wl_tie_a_"+suffix || rows[1].Workload.ID != "wl_tie_b_"+suffix {
			t.Fatalf("durable ordering/limit: %+v, %v", rows, err)
		}
		newWorkload.BurstID = "burst_new_" + suffix
		if err := a.PutWorkloadDurable(newWorkload); err != nil {
			t.Fatal(err)
		}
		burst := &state.Burst{ID: newWorkload.BurstID, CustomerID: customer.ID, CreatedAt: created, HourlyUSD: 2}
		if err := a.PutBurst(burst); err != nil {
			t.Fatal(err)
		}
		snapshot, err := b.WorkloadSnapshotForCustomer(ctx, customer.ID, newWorkload.ID)
		if err != nil || snapshot.Burst == nil || snapshot.Burst.HourlyUSD != 2 {
			t.Fatalf("new durable burst not joined: %+v, %v", snapshot, err)
		}
		burst.CustomerID = foreign.CustomerID
		if err := a.PutBurst(burst); err != nil {
			t.Fatal(err)
		}
		snapshot, err = b.WorkloadSnapshotForCustomer(ctx, customer.ID, newWorkload.ID)
		if err != nil || snapshot.Burst != nil {
			t.Fatalf("foreign burst leaked into snapshot: %+v, %v", snapshot, err)
		}
		if _, err := b.GetWorkload(newWorkload.ID); !errors.Is(err, state.ErrNotFound) {
			t.Fatalf("snapshot read hydrated the mutation cache: %v", err)
		}
		cached, err := b.GetWorkload(old.ID)
		if err != nil || cached.Status != "running" {
			t.Fatalf("snapshot replaced a mutation cache entry: %v", err)
		}
	})
	t.Run("database-failure-not-cache", func(t *testing.T) {
		b.Close()
		if rec := connectorRead(old.ID, connectorToken); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("closed database routed read = %d, want 503", rec.Code)
		}
		rec := httptest.NewRecorder()
		h.Get(rec, workloadGetReq(customer, old.ID))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("closed database = %d, want 503 without stale fallback", rec.Code)
		}
		for _, path := range []string{"/v1/tenants/" + customer.ID + "/workloads", "/v1/tenants/" + customer.ID + "/workloads/" + old.ID} {
			rec := callTenantWorkload(accountsB, http.MethodGet, path, "human_read-replica", "")
			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("closed database human read %s = %d, want 503", path, rec.Code)
			}
		}
	})
}
