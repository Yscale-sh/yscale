//go:build integration

package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestPostgresWorkloadReportsCurrentAuthority(t *testing.T) {
	for _, operation := range []string{"started", "complete"} {
		for _, change := range []string{"uncached", "rotated", "scoped-uncached", "scoped-rotated", "scoped-rebound", "legacy-single", "legacy-second", "revoked", "uncached-booking", "missing-booking"} {
			if operation == "started" && (change == "uncached-booking" || change == "missing-booking") {
				continue
			}
			t.Run(operation+"/"+change, func(t *testing.T) {
				a := coreReadStore(t)
				customer, owner, suffix := coreReadTenant(t, a)
				token, clusterID := "synthetic-core-read-"+suffix, ""
				if strings.HasPrefix(change, "scoped-") || strings.HasPrefix(change, "legacy-") {
					clusterID = "cluster-reports-" + suffix
					_, minted, _, err := a.RegisterTenantCluster(customer.ID, owner, clusterID, "Synthetic reports", state.HumanActor(owner, customer.ID))
					if err != nil {
						t.Fatal(err)
					}
					token = minted
				}
				b := coreReadStore(t)
				wl := &state.Workload{ID: "wl_report_" + suffix, CustomerID: customer.ID, ClusterID: clusterID, BurstID: "burst_report_" + suffix, Status: "provisioning", CreatedAt: time.Now().UTC()}
				if strings.HasPrefix(change, "legacy-") {
					wl.ClusterID = ""
				}
				if err := a.PutWorkloadDurable(wl); err != nil {
					t.Fatal(err)
				}
				if change == "uncached-booking" {
					b = coreReadStore(t)
				}
				if change != "missing-booking" {
					if err := a.PutBurst(&state.Burst{ID: wl.BurstID, CustomerID: customer.ID, ClusterID: clusterID, Backend: "linode", BackendID: "synthetic-resource", Region: "us-east", SKU: "synthetic-sku", CreatedAt: time.Now().UTC()}); err != nil {
						t.Fatal(err)
					}
				}
				if change != "uncached" && change != "scoped-uncached" && change != "uncached-booking" {
					b = coreReadStore(t)
				}
				t.Cleanup(func() {
					if err := a.DeleteBurst(wl.BurstID); err != nil {
						t.Error(err)
					}
				})
				deletes := newFakeProviderDeletes()
				h := &Workloads{Store: b, Deletes: deletes, Reaper: &fakeReaper{}, Log: quietLog()}
				want := http.StatusOK
				if change == "rotated" || change == "scoped-rotated" || change == "scoped-rebound" || change == "legacy-second" || change == "revoked" {
					want = http.StatusNotFound
				}
				if change == "missing-booking" {
					want = http.StatusServiceUnavailable
				}
				action := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch change {
					case "rotated":
						if _, err := a.RotateCustomerCredential(customer.ID, state.OperatorActor()); err != nil {
							t.Fatal(err)
						}
					case "scoped-rotated":
						if _, _, _, err := a.RotateTenantClusterCredential(customer.ID, owner, clusterID, state.HumanActor(owner, customer.ID)); err != nil {
							t.Fatal(err)
						}
					case "scoped-rebound":
						changed := *wl
						changed.ClusterID = "different-cluster-" + suffix
						if err := a.PutWorkloadDurable(&changed); err != nil {
							t.Fatal(err)
						}
					case "legacy-second":
						if _, _, _, err := a.RegisterTenantCluster(customer.ID, owner, "second-"+suffix, "Synthetic second", state.HumanActor(owner, customer.ID)); err != nil {
							t.Fatal(err)
						}
					case "revoked":
						if err := a.RevokeCustomer(customer.ID); err != nil {
							t.Fatal(err)
						}
					}
					if operation == "started" {
						h.Started(w, r)
					} else {
						h.Complete(w, r)
					}
				})
				mux := http.NewServeMux()
				mux.Handle("POST /v1/workloads/{id}/"+operation, ConnectorWorkloadAuth(b, action))
				req := httptest.NewRequest(http.MethodPost, "/v1/workloads/"+wl.ID+"/"+operation, strings.NewReader(`{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded"}}}`))
				req.Header.Set("Authorization", "Bearer "+token)
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, req)
				if response.Code != want {
					t.Errorf("report=%d, want %d", response.Code, want)
				}
				current, err := a.WorkloadSnapshotForCustomer(context.Background(), customer.ID, wl.ID)
				if err != nil {
					t.Fatal(err)
				}
				if want == http.StatusNotFound {
					if deletes.requests != 0 || current.Workload.Status != "provisioning" {
						t.Error("refused report changed workload or requested deletion")
					}
				} else if operation == "started" {
					if current.Workload.StartedAt == nil || current.Workload.Status != "running" {
						t.Error("uncached start was not recorded")
					}
				} else {
					if current.Workload.FinishedAt == nil || current.Workload.Outcome == nil {
						t.Error("completion receipt was lost")
					}
					if change != "missing-booking" && deletes.requests != 1 {
						t.Errorf("completion requested %d deletes, want one", deletes.requests)
					}
					if strings.Contains(response.Body.String(), "already_reaped") {
						t.Error("completion claimed absence without proof")
					}
				}
			})
		}
	}
}

func TestPostgresWorkloadReportsCompletionRecoveryAndReplay(t *testing.T) {
	a := coreReadStore(t)
	customer, _, suffix := coreReadTenant(t, a)
	wl := &state.Workload{ID: "wl_report_recovery_" + suffix, CustomerID: customer.ID, BurstID: "burst_report_recovery_" + suffix, Status: "running"}
	if err := a.PutWorkloadDurable(wl); err != nil {
		t.Fatal(err)
	}
	if err := a.PutBurst(&state.Burst{ID: wl.BurstID, CustomerID: customer.ID, Backend: "linode", BackendID: "synthetic-recovery", Region: "us-east", SKU: "synthetic-sku", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	b := coreReadStore(t)
	t.Cleanup(func() {
		if err := b.DeleteBurst(wl.BurstID); err != nil {
			t.Error(err)
		}
	})
	deletes := newFakeProviderDeletes()
	deletes.requestErr = errors.New("synthetic lifecycle outage")
	h := &Workloads{Store: a, Deletes: deletes, Reaper: &fakeReaper{}, Log: quietLog()}
	response := httptest.NewRecorder()
	h.Complete(response, completeReq(customer, wl.ID, `{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded"}}}`))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("outage response=%d", response.Code)
	}
	first, err := b.WorkloadSnapshotForCustomer(context.Background(), customer.ID, wl.ID)
	if err != nil || first.Workload.FinishedAt == nil || first.Burst == nil || !first.Burst.ReapPending || first.Burst.ReapPendingStatus != "succeeded" {
		t.Fatalf("completion has no durable cleanup recovery: %v", err)
	}
	// Replayed observations must not change the accepted terminal record merely
	// because provider deletion is still pending.
	h.Complete(httptest.NewRecorder(), completeReq(customer, wl.ID, `{"phase":"Failed","outcome":{"compute":{"result":"failed"}}}`))
	second, err := b.WorkloadSnapshotForCustomer(context.Background(), customer.ID, wl.ID)
	if err != nil || second.Workload.Status != "succeeded" || !second.Workload.FinishedAt.Equal(*first.Workload.FinishedAt) || second.Workload.Outcome.Compute.Result != "succeeded" {
		t.Errorf("pending-delete replay overwrote first completion: %v", err)
	}
	a.Close()
	recovered := newFakeProviderDeletes()
	h = &Workloads{Store: b, Deletes: recovered, Reaper: &fakeReaper{}, Log: quietLog()}
	h.sweepExpiredBursts(context.Background(), 0, 0)
	if recovered.requests != 1 {
		t.Error("watchdog did not recover completion without budget caps")
	}
}
