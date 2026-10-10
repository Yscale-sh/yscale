package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/broker"
)

// costFixture is a store holding one running workload on one burst, aged so a
// reap has a runtime to measure.
func costFixture(t *testing.T, age time.Duration, hourlyUSD float64) *state.Store {
	t.Helper()
	store := state.New()
	store.PutWorkload(&state.Workload{
		ID: "wl_cost", CustomerID: state.DevCustomerID, BurstID: "b_cost", Status: "running",
		CreatedAt: time.Now().Add(-age).UTC(),
	})
	if err := store.PutBurst(&state.Burst{
		ID: "b_cost", CustomerID: state.DevCustomerID, Backend: "linode", BackendID: "n1",
		CreatedAt: time.Now().Add(-age).UTC(), HourlyUSD: hourlyUSD,
	}); err != nil {
		t.Fatalf("seed burst: %v", err)
	}
	return store
}

func workloadCost(t *testing.T, store *state.Store, id string) *state.WorkloadCost {
	t.Helper()
	w, err := store.GetWorkload(id)
	if err != nil {
		t.Fatalf("GetWorkload %s: %v", id, err)
	}
	return w.Cost
}

// scrapeCostTotal reads yscale_cloud_cost_usd_total for one backend off the
// meter's own /metrics handler — the same series a dashboard reads. A backend
// with no series has accrued nothing: the meter only touches the counter for a
// burst with a measurable lifetime.
func scrapeCostTotal(t *testing.T, m *cost.Meter, backend string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := `yscale_cloud_cost_usd_total{backend="` + backend + `"} `
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		return v
	}
	return 0
}

// Both paths that definitively reap a burst — durable enqueue and inline
// teardown — must leave the run's cost on the workload, because the burst that
// carried the rate and the start time is deleted by the claim that reaped it.
func TestReapFreezesTheWorkloadCost(t *testing.T) {
	const (
		age    = 30 * time.Minute
		hourly = 2.0
	)
	tests := []struct {
		name    string
		durable bool
	}{
		{name: "durable teardown queue", durable: true},
		{name: "inline teardown", durable: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := costFixture(t, age, hourly)
			meter := cost.NewMeter()
			h := &Workloads{Store: store, Reaper: &fakeReaper{}, Log: quietLog(), Cost: meter}
			if tc.durable {
				h.Teardowns = broker.NewMemory(time.Hour)
			}
			before := time.Now().UTC()

			if !h.reapBurst(context.Background(), "b_cost", "workload Succeeded").Reaped() {
				t.Fatal("reapBurst should have reaped the burst")
			}

			observed := workloadCost(t, store, "wl_cost")
			if observed == nil {
				t.Fatal("the reap recorded no cost; the burst is gone and nothing can re-derive it")
			}
			if observed.BurstID != "b_cost" || observed.Backend != "linode" || observed.HourlyUSD != hourly {
				t.Errorf("observation = %+v, want the reaped burst's id, backend and rate", observed)
			}
			if observed.Basis != state.WorkloadCostBasisRateRuntime {
				t.Errorf("basis = %q, want %q", observed.Basis, state.WorkloadCostBasisRateRuntime)
			}
			if observed.FrozenAt.Before(before) || observed.FrozenAt.After(time.Now().UTC()) {
				t.Errorf("frozen at %v, want an instant inside the reap (%v … now)", observed.FrozenAt, before)
			}
			if observed.FrozenAt.Location() != time.UTC {
				t.Errorf("frozen at %v, want UTC — a local-zone stamp is a different instant to every reader", observed.FrozenAt)
			}
			// The runtime is measured, so it is at least the burst's age and no
			// more than that plus the test's own elapsed time.
			if observed.Runtime < age || observed.Runtime > age+time.Minute {
				t.Errorf("runtime = %v, want ~%v (measured from the burst's CreatedAt)", observed.Runtime, age)
			}
			if want := hourly * observed.Runtime.Hours(); math.Abs(observed.EstimatedUSD-want) > 1e-9 {
				t.Errorf("usd = %v, want %v (rate x runtime)", observed.EstimatedUSD, want)
			}

			// The meter and the record are two readings of ONE measurement: a
			// second clock read here would let a dashboard and a customer's
			// history disagree about the same node.
			if got := scrapeCostTotal(t, meter, "linode"); math.Abs(got-observed.EstimatedUSD) > 1e-9 {
				t.Errorf("meter accrued %v but the record says %v; they were measured twice", got, observed.EstimatedUSD)
			}
		})
	}
}

// The requeue invariant: a teardown that failed leaves the backend node running
// and the burst record back in the store for a later sweep. Nothing about that
// run is final, so freezing a cost for it would publish a total for a node that
// is still burning money.
func TestFailedTeardownRecordsNoCostAndRequeuesTheBurst(t *testing.T) {
	store := costFixture(t, 30*time.Minute, 2.0)
	reaper := &fakeReaper{}
	reaper.setErr(errors.New("provider refused the delete"))
	h := &Workloads{Store: store, Reaper: reaper, Log: quietLog(), Cost: cost.NewMeter()}

	if got := h.reapBurst(context.Background(), "b_cost", "workload Failed"); got != ReapOutcomeRequeued {
		t.Fatalf("reapBurst = %v, want ReapOutcomeRequeued for a teardown that failed", got)
	}
	if observed := workloadCost(t, store, "wl_cost"); observed != nil {
		t.Errorf("a failed reap froze a cost: %+v", observed)
	}
	if _, err := store.GetBurst("b_cost"); err != nil {
		t.Errorf("the burst was not re-queued for a later reap: %v", err)
	}
}

// A reaper that loses the claim tore nothing down — the winner did, and the
// winner's observation is the one that counts. A loser that recorded its own
// would be a second measurement of one node.
func TestLostClaimRecordsNoCost(t *testing.T) {
	store := costFixture(t, 30*time.Minute, 2.0)
	h := &Workloads{Store: store, Reaper: &fakeReaper{}, Log: quietLog(), Cost: cost.NewMeter()}
	ctx := context.Background()

	if !h.reapBurst(ctx, "b_cost", "workload Succeeded").Reaped() {
		t.Fatal("the first reap should win the claim")
	}
	frozen := workloadCost(t, store, "wl_cost")
	if frozen == nil {
		t.Fatal("the winning reap recorded nothing")
	}

	if got := h.reapBurst(ctx, "b_cost", "workload cancelled"); got != ReapOutcomeNotOwned {
		t.Fatalf("the second reap = %v, want ReapOutcomeNotOwned", got)
	}
	after := workloadCost(t, store, "wl_cost")
	if after == nil || *after != *frozen {
		t.Errorf("observation = %+v, want the winner's %+v unchanged", after, frozen)
	}
}

// A burst whose CreatedAt is ahead of this replica's clock — the two are
// different machines, and the row may have been written by either — must read
// as zero runtime. A negative one would credit the cost counter and store a
// negative estimate, which is a refund nobody issued.
func TestReapClampsRuntimeForABurstFromTheFuture(t *testing.T) {
	store := state.New()
	store.PutWorkload(&state.Workload{
		ID: "wl_future", CustomerID: state.DevCustomerID, BurstID: "b_future", Status: "running",
	})
	if err := store.PutBurst(&state.Burst{
		ID: "b_future", CustomerID: state.DevCustomerID, Backend: "flyio", BackendID: "n1",
		CreatedAt: time.Now().Add(time.Hour).UTC(), HourlyUSD: 3.0,
	}); err != nil {
		t.Fatalf("seed burst: %v", err)
	}
	meter := cost.NewMeter()
	h := &Workloads{Store: store, Reaper: &fakeReaper{}, Log: quietLog(), Cost: meter}

	if !h.reapBurst(context.Background(), "b_future", "workload Succeeded").Reaped() {
		t.Fatal("reapBurst should have reaped the burst")
	}
	observed := workloadCost(t, store, "wl_future")
	if observed == nil {
		t.Fatal("the reap recorded no cost")
	}
	if observed.Runtime != 0 || observed.EstimatedUSD != 0 {
		t.Errorf("observation = runtime %v / $%v, want 0 / $0", observed.Runtime, observed.EstimatedUSD)
	}
	if got := scrapeCostTotal(t, meter, "flyio"); got != 0 {
		t.Errorf("meter accrued %v for a burst with no measurable runtime, want 0", got)
	}
}

// A reap with nothing to record against — a nodeOnly burst, or a workload row
// this replica never held and no durable backend to reach — still tears the
// node down and still reports success. The observation is bookkeeping; the
// teardown is the job.
func TestReapWithNoWorkloadStillTearsDown(t *testing.T) {
	store := state.New()
	if err := store.PutBurst(&state.Burst{
		ID: "b_nodeonly", CustomerID: state.DevCustomerID, Backend: "linode", BackendID: "n1",
		CreatedAt: time.Now().Add(-time.Hour).UTC(), HourlyUSD: 1.0, NodeOnly: true,
	}); err != nil {
		t.Fatalf("seed burst: %v", err)
	}
	reaper := &fakeReaper{}
	h := &Workloads{Store: store, Reaper: reaper, Log: quietLog(), Cost: cost.NewMeter()}

	if !h.reapBurst(context.Background(), "b_nodeonly", "budget exceeded").Reaped() {
		t.Fatal("a burst with no workload must still be reaped")
	}
	if !reaper.reaped("b_nodeonly") {
		t.Error("the node was not torn down")
	}
}

// The customer-facing lifecycle, end to end: the agent reports the job finished
// and the workload it reads back afterwards carries what the run cost.
func TestCompleteFreezesTheWorkloadCost(t *testing.T) {
	store := costFixture(t, 15*time.Minute, 4.0)
	cust, err := store.CustomerByID(state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	h := &Workloads{Store: store, Reaper: &fakeReaper{}, Log: quietLog(), Cost: cost.NewMeter()}

	rec := httptest.NewRecorder()
	h.Complete(rec, completeReq(cust, "wl_cost", `{"phase":"Succeeded"}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("Complete status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}

	w, err := store.GetWorkload("wl_cost")
	if err != nil {
		t.Fatal(err)
	}
	if w.Status != "succeeded" || w.FinishedAt == nil {
		t.Fatalf("workload = %+v, want the terminal record", w)
	}
	if w.Cost == nil {
		t.Fatal("the completed workload has no cost observation")
	}
	if w.Cost.FrozenAt.IsZero() || w.Cost.Basis != state.WorkloadCostBasisRateRuntime {
		t.Errorf("observation = %+v, want a frozen UTC stamp and the stated basis", w.Cost)
	}
	// GET /v1/workloads/{id} omits live spend once the burst is reaped and
	// exposes the same frozen tenant-safe cost observation used by history.
	get := httptest.NewRecorder()
	h.Get(get, completeReq(cust, "wl_cost", ""))
	if get.Code != http.StatusOK {
		t.Fatalf("Get status = %d, want 200", get.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(get.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode get: %v (body %q)", err, get.Body.String())
	}
	if _, present := body["spent_usd"]; present {
		t.Errorf("GET /v1/workloads/{id} retained live spend for a reaped burst: %v", body)
	}
	gotCost, present := body["cost"].(map[string]any)
	if !present {
		t.Fatalf("GET /v1/workloads/{id} omitted frozen cost for a reaped burst: %v", body)
	}
	if got, ok := gotCost["usd"].(float64); !ok || got < 0.99 || got > 1.01 {
		t.Errorf("cost.usd = %v, want approximately 1.0", gotCost["usd"])
	}
	for _, key := range []string{"id", "status", "burst_id", "created_at", "started_at"} {
		if _, present := body[key]; !present {
			t.Errorf("GET /v1/workloads/{id} dropped its %q key: %v", key, body)
		}
	}
}
