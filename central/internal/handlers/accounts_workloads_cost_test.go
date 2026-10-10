// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/state"
)

type spendCleanupDeletes struct {
	*stubCleanupSummaryDeletes
	*stubProviderDeleteGetter
}

func TestTenantWorkloadSpendFollowsProviderLifetime(t *testing.T) {
	created := time.Now().Add(-30 * time.Minute).UTC()
	deleted := time.Now().Add(-time.Minute).UTC()
	frozen := &state.WorkloadCost{EstimatedUSD: 0.75, HourlyUSD: 2, FrozenAt: deleted}
	for _, phase := range []string{"running", "succeeded", "failed", "cancelled"} {
		for _, tt := range []struct {
			name, cleanup string
			deletedAt     *time.Time
			cost          *state.WorkloadCost
			burstCost     *state.WorkloadCost
			missingBurst  bool
			foreignBurst  bool
			wantLive      bool
		}{
			{name: "no cleanup yet", wantLive: true},
			{name: "queued", cleanup: "queued", wantLive: true},
			{name: "deleting", cleanup: "deleting", wantLive: true},
			{name: "retrying", cleanup: "retrying", wantLive: true},
			{name: "manual attention", cleanup: "manual_attention", wantLive: true},
			{name: "timestamp without terminal state", cleanup: "retrying", deletedAt: &deleted, wantLive: true},
			{name: "terminal state without timestamp", cleanup: "terminated", wantLive: true},
			{name: "provider absent with stale burst", cleanup: "terminated", deletedAt: &deleted},
			{name: "frozen workload with stale burst", cost: frozen},
			{name: "frozen burst awaiting receipt repair", burstCost: frozen},
			{name: "burst retired", missingBurst: true},
			{name: "cross tenant burst linkage", foreignBurst: true},
		} {
			t.Run(phase+"/"+tt.name, func(t *testing.T) {
				store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
				var finishedAt *time.Time
				if phase != "running" {
					finished := created.Add(10 * time.Minute)
					finishedAt = &finished
				}
				store.PutWorkload(&state.Workload{
					ID: "wl_spend", CustomerID: "cust_console", ClusterID: "cluster_console", BurstID: "burst_spend",
					Status: phase, CreatedAt: created, FinishedAt: finishedAt, SpecYAML: []byte(tenantWorkloadYAML), Cost: tt.cost,
				})
				if !tt.missingBurst {
					customerID := "cust_console"
					if tt.foreignBurst {
						customerID = "cust_other"
					}
					store.PutBurst(&state.Burst{ID: "burst_spend", CustomerID: customerID, CreatedAt: created, HourlyUSD: 2, TerminalCost: tt.burstCost})
				}
				summaries := map[lifecycle.ProviderDeleteSummaryRef]lifecycle.ProviderDeleteSummary{}
				records := map[string]lifecycle.ProviderDeleteRecord{}
				if tt.cleanup != "" {
					summaries[lifecycle.ProviderDeleteSummaryRef{ClusterID: "cluster_console", BurstID: "burst_spend"}] = lifecycle.ProviderDeleteSummary{State: tt.cleanup, DeletedAt: tt.deletedAt}
					records["cust_console/cluster_console/burst_spend"] = lifecycle.ProviderDeleteRecord{State: tt.cleanup, DeletedAt: tt.deletedAt}
				}
				accounts.Workloads.Deletes = &spendCleanupDeletes{
					stubCleanupSummaryDeletes: &stubCleanupSummaryDeletes{summaries: map[string]map[lifecycle.ProviderDeleteSummaryRef]lifecycle.ProviderDeleteSummary{"cust_console": summaries}},
					stubProviderDeleteGetter:  &stubProviderDeleteGetter{records: records},
				}
				for _, path := range []string{"/v1/tenants/cust_console/workloads", "/v1/tenants/cust_console/workloads/wl_spend"} {
					rec := callTenantWorkload(accounts, http.MethodGet, path, "human_alice", "")
					if rec.Code != http.StatusOK {
						t.Fatalf("%s status = %d", path, rec.Code)
					}
					var body map[string]any
					if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
						t.Fatal(err)
					}
					if rows, ok := body["workloads"].([]any); ok {
						if len(rows) != 1 {
							t.Fatalf("%s returned %d workloads", path, len(rows))
						}
						body = rows[0].(map[string]any)
					}
					spent, present := body["spent_usd"].(float64)
					if present != tt.wantLive || (present && (spent < 0.99 || spent > 1.01)) {
						t.Errorf("%s spent_usd = %v (present %v), want live %v at about 1.00", path, spent, present, tt.wantLive)
					}
				}
			})
		}
	}
}

// An active workload has no frozen cost receipt, but it does expose central's
// live estimate and latest validated GPU sample. Observed 0% must survive the
// wire shape rather than being confused with missing telemetry.
func TestTenantWorkloadListRendersLiveSpendAndIdleGPU(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	created := time.Now().Add(-30 * time.Minute).UTC()
	heartbeat := time.Now().Add(-15 * time.Second).UTC()
	putTenantWorkload(store, "wl_live", "cust_console", "burst_live", created)
	store.PutBurst(&state.Burst{
		ID: "burst_live", CustomerID: "cust_console", CreatedAt: created,
		HourlyUSD: 2, GPUUtilPercent: 0, LastHeartbeatAt: &heartbeat,
	})

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	response := decodeTenantWorkloads(t, rec)
	if len(response.Workloads) != 1 || response.Workloads[0].Cost != nil {
		t.Fatalf("workloads = %+v, want one active record with no frozen cost", response.Workloads)
	}
	got := response.Workloads[0]
	if got.SpentUSD == nil || *got.SpentUSD < 0.99 || *got.SpentUSD > 1.01 {
		t.Fatalf("spent_usd = %v, want about 1.00", got.SpentUSD)
	}
	if got.GPUUtilPercent == nil || *got.GPUUtilPercent != 0 {
		t.Fatalf("gpu_util_percent = %v, want observed 0", got.GPUUtilPercent)
	}
	if got.LastHeartbeatAt == nil || !got.LastHeartbeatAt.Equal(heartbeat) {
		t.Fatalf("last_heartbeat_at = %v, want %v", got.LastHeartbeatAt, heartbeat)
	}

	// The wire shape, not just the Go struct: omitempty is what keeps the key
	// out, and a console distinguishes absent from zero only if it really is.
	var envelope struct {
		Workloads []map[string]json.RawMessage `json:"workloads"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if _, present := envelope.Workloads[0]["cost"]; present {
		t.Errorf("a workload with no observation rendered a cost key: %v", envelope.Workloads[0])
	}
	for _, key := range []string{"spent_usd", "gpu_util_percent", "last_heartbeat_at"} {
		if _, present := envelope.Workloads[0][key]; !present {
			t.Errorf("active workload omitted %s: %v", key, envelope.Workloads[0])
		}
	}
}

func TestTenantWorkloadListLiveSummaryFailsClosed(t *testing.T) {
	tests := []struct {
		name  string
		burst state.Burst
	}{
		{name: "negative rate", burst: state.Burst{HourlyUSD: -1}},
		{name: "nan rate", burst: state.Burst{HourlyUSD: math.NaN()}},
		{name: "future creation", burst: state.Burst{HourlyUSD: 1, CreatedAt: time.Now().Add(time.Hour)}},
		{name: "invalid utilization", burst: state.Burst{HourlyUSD: 1, GPUUtilPercent: 101}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
			created := time.Now().Add(-time.Minute).UTC()
			putTenantWorkload(store, "wl_live", "cust_console", "burst_live", created)
			tt.burst.ID = "burst_live"
			tt.burst.CustomerID = "cust_console"
			if tt.burst.CreatedAt.IsZero() {
				tt.burst.CreatedAt = created
			}
			if tt.name == "invalid utilization" {
				heartbeat := time.Now().UTC()
				tt.burst.LastHeartbeatAt = &heartbeat
			}
			store.PutBurst(&tt.burst)

			rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
			response := decodeTenantWorkloads(t, rec)
			got := response.Workloads[0]
			if tt.name == "invalid utilization" {
				if got.SpentUSD == nil {
					t.Fatal("valid spend was omitted with invalid telemetry")
				}
			} else if got.SpentUSD != nil {
				t.Fatalf("invalid spend input rendered %v", *got.SpentUSD)
			}
			if got.GPUUtilPercent != nil || got.LastHeartbeatAt != nil {
				t.Fatalf("invalid or absent telemetry rendered util=%v heartbeat=%v", got.GPUUtilPercent, got.LastHeartbeatAt)
			}
		})
	}
}

func TestTenantWorkloadListLiveSummaryIsTenantScoped(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	created := time.Now().Add(-time.Hour).UTC()
	putTenantWorkload(store, "wl_live", "cust_console", "burst_live", created)
	// A burst id is globally unique. Replacing this one with another tenant's
	// record simulates corrupted cross-tenant linkage; the tenant-scoped burst
	// snapshot must omit it rather than join on id alone.
	store.PutBurst(&state.Burst{
		ID: "burst_live", CustomerID: "cust_other", CreatedAt: created, HourlyUSD: 10,
	})

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	response := decodeTenantWorkloads(t, rec)
	got := response.Workloads[0]
	if got.SpentUSD != nil || got.GPUUtilPercent != nil || got.LastHeartbeatAt != nil {
		t.Fatalf("cross-tenant burst rendered live fields: %+v", got)
	}
}

// A recorded run renders every field the figure was derived from, so a customer
// reading it can check the arithmetic without central's help: the rate, the
// runtime, the instant it was frozen, the backend, the burst, and the basis
// that says what the number means.
func TestTenantWorkloadListRendersTheFrozenCost(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	created := time.Now().Add(-2 * time.Hour).UTC()
	putTenantWorkload(store, "wl_costed", "cust_console", "burst_costed", created)

	frozenAt := created.Add(90 * time.Minute)
	observed := state.WorkloadCost{
		EstimatedUSD: 2.25,
		HourlyUSD:    1.5,
		Runtime:      90 * time.Minute,
		FrozenAt:     frozenAt,
		Backend:      "linode",
		BurstID:      "burst_costed",
		Basis:        state.WorkloadCostBasisRateRuntime,
	}
	if recorded, err := store.RecordWorkloadCostForBurst(context.Background(), observed); err != nil || !recorded {
		t.Fatalf("RecordWorkloadCostForBurst = %v, err %v; want true/nil", recorded, err)
	}

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	response := decodeTenantWorkloads(t, rec)
	if len(response.Workloads) != 1 {
		t.Fatalf("workloads = %+v, want one", response.Workloads)
	}
	got := response.Workloads[0].Cost
	if got == nil {
		t.Fatal("the recorded run rendered no cost")
	}
	want := TenantWorkloadCost{
		USD: 2.25, HourlyUSD: 1.5, RuntimeSeconds: 5400, FrozenAt: frozenAt,
		Backend: "linode", BurstID: "burst_costed", Basis: state.WorkloadCostBasisRateRuntime,
	}
	if *got != want {
		t.Errorf("cost = %+v, want %+v", *got, want)
	}
	if response.Workloads[0].SpentUSD != nil {
		t.Fatalf("terminal frozen record also rendered live spend %v", *response.Workloads[0].SpentUSD)
	}

	// The JSON names are the contract the console reads; a rename is a silently
	// empty panel, not a build failure.
	var envelope struct {
		Workloads []struct {
			Cost map[string]any `json:"cost"`
		} `json:"workloads"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	rendered := envelope.Workloads[0].Cost
	for key, want := range map[string]any{
		"usd":             2.25,
		"hourly_usd":      1.5,
		"runtime_seconds": float64(5400),
		"backend":         "linode",
		"burst_id":        "burst_costed",
		"basis":           state.WorkloadCostBasisRateRuntime,
	} {
		if rendered[key] != want {
			t.Errorf("cost[%q] = %v, want %v", key, rendered[key], want)
		}
	}
	if stamp, _ := rendered["frozen_at"].(string); stamp != frozenAt.Format(time.RFC3339Nano) {
		t.Errorf("cost[\"frozen_at\"] = %v, want %v", rendered["frozen_at"], frozenAt.Format(time.RFC3339Nano))
	}
	if len(rendered) != 7 {
		t.Errorf("cost object has %d fields (%v); the rendered record is exactly the seven above", len(rendered), rendered)
	}
}

// The tenant list is per-tenant, and so is the history on it: one tenant's
// recorded spend must not appear on another's run. The observation is written
// by burst id, which is the one key that does not carry a tenant with it.
func TestTenantWorkloadCostDoesNotCrossWorkloads(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	now := time.Now().UTC()
	putTenantWorkload(store, "wl_costed", "cust_console", "burst_costed", now)
	putTenantWorkload(store, "wl_untouched", "cust_console", "burst_untouched", now.Add(-time.Minute))

	if _, err := store.RecordWorkloadCostForBurst(context.Background(), state.WorkloadCost{
		EstimatedUSD: 2.25, HourlyUSD: 1.5, Runtime: 90 * time.Minute, FrozenAt: now,
		Backend: "linode", BurstID: "burst_costed", Basis: state.WorkloadCostBasisRateRuntime,
	}); err != nil {
		t.Fatal(err)
	}

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	response := decodeTenantWorkloads(t, rec)
	if len(response.Workloads) != 2 {
		t.Fatalf("workloads = %+v, want two", response.Workloads)
	}
	for _, w := range response.Workloads {
		switch w.ID {
		case "wl_costed":
			if w.Cost == nil || w.Cost.BurstID != "burst_costed" {
				t.Errorf("the reaped run = %+v, want its own observation", w.Cost)
			}
		case "wl_untouched":
			if w.Cost != nil {
				t.Errorf("a run nobody reaped carries a cost: %+v", w.Cost)
			}
		default:
			t.Errorf("unexpected workload %q", w.ID)
		}
	}
}
