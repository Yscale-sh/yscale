//go:build integration

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestPostgresWorkloadTransitionsHTTPFailureDoesNotReap(t *testing.T) {
	for _, operation := range []string{"start", "complete", "complete-no-burst", "cancel", "cancel-no-burst"} {
		for _, fault := range []string{"outage", "write-rejected"} {
			t.Run(operation+"/"+fault, func(t *testing.T) {
				dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
				if dsn == "" {
					t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
				}
				ctx := context.Background()
				s, err := state.NewPostgres(ctx, dsn)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(s.Close)
				suffix := fmt.Sprint(time.Now().UnixNano())
				customerID, workloadID, burstID := "cust_transition_"+suffix, "wl_transition_"+suffix, "burst_transition_"+suffix
				cust, _, err := s.CreateTenant(&state.Customer{ID: customerID, Token: "synthetic-transition-" + suffix}, "")
				if err != nil {
					t.Fatal(err)
				}
				s.PutWorkload(&state.Workload{ID: workloadID, CustomerID: customerID, BurstID: burstID, Status: "provisioning"})
				if !strings.HasSuffix(operation, "-no-burst") {
					if err := s.PutBurst(&state.Burst{ID: burstID, CustomerID: customerID, CreatedAt: time.Now().UTC()}); err != nil {
						t.Fatal(err)
					}
				}
				reader, err := state.NewPostgres(ctx, dsn)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(reader.Close)
				if fault == "outage" {
					s.Close()
				} else {
					pool, err := pgxpool.New(ctx, dsn)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(pool.Close)
					// Both names are generated above, never request input. Reject
					// only this fixture's transition; durable burst claims still work.
					constraint := "transition_failure_" + suffix
					if _, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE workloads ADD CONSTRAINT %s CHECK (id <> '%s' OR data->>'Status' = 'provisioning')`, constraint, workloadID)); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if _, err := pool.Exec(ctx, "ALTER TABLE workloads DROP CONSTRAINT "+constraint); err != nil {
							t.Error(err)
						}
					})
				}
				reaper := &fakeReaper{}
				h := &Workloads{Store: s, Reaper: reaper, Log: quietLog()}
				r := httptest.NewRequest(http.MethodPost, "/v1/workloads/"+workloadID, strings.NewReader(`{"phase":"Succeeded"}`))
				r.SetPathValue("id", workloadID)
				r = r.WithContext(context.WithValue(ctx, ctxCustomer, cust))
				response := httptest.NewRecorder()
				switch operation {
				case "start":
					h.Started(response, r)
				case "complete", "complete-no-burst":
					h.Complete(response, r)
				default:
					r.Method = http.MethodDelete
					h.Cancel(response, r)
				}
				if response.Code != http.StatusServiceUnavailable || reaper.teardownCount(burstID) != 0 {
					t.Fatalf("failed transition response=%d, teardown calls=%d", response.Code, reaper.teardownCount(burstID))
				}
				local, err := s.GetWorkload(workloadID)
				if err != nil || local.Status != "provisioning" || local.FinishedAt != nil || local.StartedAt != nil {
					t.Fatal("failed HTTP transition was published locally")
				}
				current, err := reader.WorkloadSnapshotForCustomer(ctx, customerID, workloadID)
				if err != nil || current.Workload.Status != "provisioning" || (!strings.HasSuffix(operation, "-no-burst") && current.Burst == nil) {
					t.Fatal("failed HTTP transition changed the durable workload or claimed its burst")
				}
			})
		}
	}
}
