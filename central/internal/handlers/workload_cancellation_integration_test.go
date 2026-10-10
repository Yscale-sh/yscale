//go:build integration

package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestPostgresWorkloadCancellationSurvivesDeleteIntentFailure(t *testing.T) {
	a := coreReadStore(t)
	customer, _, suffix := coreReadTenant(t, a)
	wl := &state.Workload{ID: "wl_cancel_" + suffix, CustomerID: customer.ID, BurstID: "burst_cancel_" + suffix, Status: "running"}
	if err := a.PutWorkloadDurable(wl); err != nil {
		t.Fatal(err)
	}
	burst := &state.Burst{ID: wl.BurstID, CustomerID: customer.ID, Backend: "linode", BackendID: "synthetic-resource", Region: "us-east", SKU: "synthetic-sku", CreatedAt: time.Now().UTC()}
	if err := a.PutBurst(burst); err != nil {
		t.Fatal(err)
	}
	b := coreReadStore(t) // predates the cancellation, including its recovery marker
	t.Cleanup(func() {
		if err := b.DeleteBurst(wl.BurstID); err != nil {
			t.Error(err)
		}
	})
	deletes := newFakeProviderDeletes()
	deletes.requestErr = errors.New("synthetic lifecycle outage")
	h := &Workloads{Store: a, Deletes: deletes, Reaper: &fakeReaper{}, Log: quietLog()}
	response := httptest.NewRecorder()
	h.Cancel(response, cancelReq(customer, wl.ID))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("cancel during lifecycle outage=%d", response.Code)
	}
	current, err := b.WorkloadSnapshotForCustomer(context.Background(), customer.ID, wl.ID)
	if err != nil || current.Workload.Status != "cancelled" || current.Burst == nil || !current.Burst.ReapPending {
		t.Errorf("accepted cancellation has no durable cleanup recovery marker: %v", err)
	}
	// The request process is gone. The existing watchdog on a stale replica
	// must retry this unbudgeted burst even with every safety-net cap disabled.
	a.Close()
	recovered := newFakeProviderDeletes()
	h = &Workloads{Store: b, Deletes: recovered, Reaper: &fakeReaper{}, Log: quietLog()}
	h.sweepExpiredBursts(context.Background(), 0, 0)
	if recovered.requests != 1 || recovered.get(wl.BurstID).ProviderResourceID != burst.BackendID {
		t.Error("watchdog did not recover the accepted cancellation")
	}
}

func TestPostgresWorkloadCancellationLateBookingStaysPending(t *testing.T) {
	a := coreReadStore(t)
	customer, _, suffix := coreReadTenant(t, a)
	id := "wl_cancel_late_" + suffix
	burstID := "burst_cancel_late_" + suffix
	if err := a.PutWorkloadDurable(&state.Workload{ID: id, CustomerID: customer.ID, BurstID: burstID, Status: "provisioning"}); err != nil {
		t.Fatal(err)
	}
	b := coreReadStore(t)
	t.Cleanup(func() {
		if err := b.DeleteBurst(burstID); err != nil {
			t.Error(err)
		}
	})
	h := &Workloads{Store: a, Deletes: newFakeProviderDeletes(), Reaper: &fakeReaper{}, Log: quietLog()}
	h.Cancel(httptest.NewRecorder(), cancelReq(customer, id))
	// A provider create that was already in flight publishes its result after
	// cancellation. No second HTTP cancel will arrive to notice that resource.
	if err := b.PutBurst(&state.Burst{ID: burstID, CustomerID: customer.ID, Backend: "linode", BackendID: "synthetic-late", Region: "us-east", SKU: "synthetic-sku", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	current, err := a.BurstSnapshotContext(context.Background(), burstID)
	if err != nil || !current.ReapPending {
		t.Errorf("late booking lost cancellation: %v", err)
	}
	recovered := newFakeProviderDeletes()
	h = &Workloads{Store: b, Deletes: recovered, Reaper: &fakeReaper{}, Log: quietLog()}
	h.sweepExpiredBursts(context.Background(), 0, 0)
	if recovered.requests != 1 {
		t.Error("watchdog did not recover the late booking")
	}
}

func TestPostgresWorkloadCancellationFindsUncachedBooking(t *testing.T) {
	a := coreReadStore(t)
	customer, _, suffix := coreReadTenant(t, a)
	id, burstID := "wl_cancel_new_"+suffix, "burst_cancel_new_"+suffix
	if err := a.PutWorkloadDurable(&state.Workload{ID: id, CustomerID: customer.ID, BurstID: burstID, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	b := coreReadStore(t)
	t.Cleanup(func() {
		if err := b.DeleteBurst(burstID); err != nil {
			t.Error(err)
		}
	})
	if err := a.PutBurst(&state.Burst{ID: burstID, CustomerID: customer.ID, Backend: "linode", BackendID: "synthetic-new", Region: "us-east", SKU: "synthetic-sku", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	deletes := newFakeProviderDeletes()
	h := &Workloads{Store: b, Deletes: deletes, Reaper: &fakeReaper{}, Log: quietLog()}
	response := httptest.NewRecorder()
	h.Cancel(response, cancelReq(customer, id))
	if response.Code != http.StatusOK || deletes.requests != 1 {
		t.Fatalf("uncached booking cancel=%d, delete requests=%d", response.Code, deletes.requests)
	}
}

func TestPostgresWorkloadCancellationRechecksAuthenticatedCredential(t *testing.T) {
	for _, mode := range []string{"tenant-auth", "connector-legacy", "connector-scoped"} {
		t.Run(mode, func(t *testing.T) {
			a := coreReadStore(t)
			customer, owner, suffix := coreReadTenant(t, a)
			token := "synthetic-core-read-" + suffix
			clusterID := ""
			if mode == "connector-scoped" {
				clusterID = "cluster-cancel-" + suffix
				_, minted, _, err := a.RegisterTenantCluster(customer.ID, owner, clusterID, "Synthetic cancel", state.HumanActor(owner, customer.ID))
				if err != nil {
					t.Fatal(err)
				}
				token = minted
			}
			id := "wl_cancel_rotation_" + suffix
			if err := a.PutWorkloadDurable(&state.Workload{ID: id, CustomerID: customer.ID, ClusterID: clusterID, Status: "running"}); err != nil {
				t.Fatal(err)
			}
			b := coreReadStore(t)
			deletes := newFakeProviderDeletes()
			h := &Workloads{Store: a, Deletes: deletes, Reaper: &fakeReaper{}, Log: quietLog()}
			reached := false
			action := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reached = true
				if mode == "connector-scoped" {
					if _, _, _, err := b.RotateTenantClusterCredential(customer.ID, owner, clusterID, state.HumanActor(owner, customer.ID)); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := b.RotateCustomerCredential(customer.ID, state.OperatorActor()); err != nil {
						t.Fatal(err)
					}
				}
				h.Cancel(w, r)
			})
			var gate http.Handler = Auth(a, action)
			if mode != "tenant-auth" {
				gate = ConnectorWorkloadAuth(a, action)
			}
			mux := http.NewServeMux()
			mux.Handle("DELETE /v1/workloads/{id}", gate)
			req := httptest.NewRequest(http.MethodDelete, "/v1/workloads/"+id, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, req)
			current, err := b.WorkloadSnapshotForCustomer(context.Background(), customer.ID, id)
			if !reached || response.Code != http.StatusNotFound || deletes.requests != 0 || err != nil || current.Workload.Status != "running" {
				t.Fatalf("post-auth rotation: reached=%v code=%d deletes=%d error=%v", reached, response.Code, deletes.requests, err)
			}
		})
	}
}
