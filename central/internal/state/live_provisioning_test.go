package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

// scrapeAt is a fixed clock. Every age below is stated relative to it, so a
// failure names a duration rather than a wall time.
var scrapeAt = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

func provisioningBurst(id string, age time.Duration) *Burst {
	return &Burst{ID: id, BackendID: "vm-" + id, Status: BurstStatusProvisioning, CreatedAt: scrapeAt.Add(-age)}
}

// provisioningPersister is a test fake that serves a fixed durable burst set
// and resolves workloads by burst id. It supports error injection for both
// burst listing and workload lookups.
type provisioningPersister struct {
	auditRecorder
	bursts    []*Burst
	err       error
	workloads map[string]*Workload // burst id → workload
	wlErr     error                // injected error for workloadByBurst
}

func (p *provisioningPersister) upsertCustomer(*Customer) error                        { return nil }
func (p *provisioningPersister) createTenant(*Customer, *TenantMembership, bool) error { return nil }
func (p *provisioningPersister) revokeCustomer(*Customer) error                        { return nil }
func (p *provisioningPersister) finalizeRevokedCustomer(string) error                  { return nil }
func (p *provisioningPersister) deleteCustomerAndMemberships(string) error             { return nil }
func (p *provisioningPersister) upsertAccount(*Account) error                          { return nil }
func (p *provisioningPersister) upsertMembership(*TenantMembership, *AuditEvent) error {
	return nil
}
func (p *provisioningPersister) deleteMembership(string, *AuditEvent) error { return nil }
func (p *provisioningPersister) upsertWorkload(*Workload) error             { return nil }
func (p *provisioningPersister) stampWorkloadPodObservation(context.Context, string, string, string, string, string, time.Time) (int64, error) {
	return 1, nil
}
func (p *provisioningPersister) getWorkloadPodObservation(context.Context, string, string, string, string) (*PodObservation, bool, error) {
	return nil, false, nil
}
func (p *provisioningPersister) upsertBurst(*Burst) error         { return nil }
func (p *provisioningPersister) deleteBurst(string) error         { return nil }
func (p *provisioningPersister) upsertPV(*PersistentVolume) error { return nil }
func (p *provisioningPersister) deletePV(string) error            { return nil }

func (p *provisioningPersister) updateBurstNodePhase(context.Context, BurstNodePhaseUpdate) (*burstNodePhaseResult, error) {
	return nil, nil
}
func (p *provisioningPersister) updateBurstGPUTelemetry(context.Context, BurstGPUTelemetryUpdate) (bool, error) {
	return false, nil
}

func (p *provisioningPersister) burstOwnedBy(context.Context, string, string) (bool, error) {
	return false, nil
}

func (p *provisioningPersister) burstForTenant(context.Context, string, string) (*Burst, bool, error) {
	return nil, false, nil
}

func (p *provisioningPersister) loadAll(context.Context) (*snapshot, error) {
	return &snapshot{}, nil
}

func (p *provisioningPersister) Close() {}

func (p *provisioningPersister) claimBurst(context.Context, string) (*Burst, bool, error) {
	return nil, false, nil
}
func (p *provisioningPersister) recordBurstReapReceipt(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *provisioningPersister) burstReapRecorded(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *provisioningPersister) reservePodSlot(context.Context, int, string, time.Duration) (bool, error) {
	return false, nil
}
func (p *provisioningPersister) releasePodSlot(context.Context, string) error { return nil }
func (p *provisioningPersister) claimIdempotency(context.Context, *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	return nil, true, nil
}
func (p *provisioningPersister) completeIdempotency(context.Context, string, int, []byte, time.Time) error {
	return nil
}
func (p *provisioningPersister) deleteIdempotency(context.Context, string) error { return nil }
func (p *provisioningPersister) reservedPodSlots(context.Context, time.Duration) (map[int]bool, error) {
	return nil, nil
}
func (p *provisioningPersister) recordWorkloadCost(context.Context, string, *WorkloadCost) (bool, error) {
	return false, nil
}

func (p *provisioningPersister) listBursts(context.Context) ([]*Burst, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.bursts, nil
}

func (p *provisioningPersister) workloadByBurst(_ context.Context, burstID string) (*Workload, error) {
	if p.wlErr != nil {
		return nil, p.wlErr
	}
	if p.workloads == nil {
		return nil, nil
	}
	w, ok := p.workloads[burstID]
	if !ok {
		return nil, nil
	}
	cp := *w
	return &cp, nil
}

// TestLiveProvisioningReadsDurableState is the multi-replica property.
func TestLiveProvisioningReadsDurableState(t *testing.T) {
	s := New()
	if err := s.PutBurst(provisioningBurst("burst_mem_only", 40*time.Minute)); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	s.PutWorkload(&Workload{ID: "wl_mem", BurstID: "burst_mem_only"})

	s.persist = &provisioningPersister{
		bursts: []*Burst{
			provisioningBurst("burst_other_replica", 10*time.Minute),
			provisioningBurst("burst_other_replica_2", 90*time.Second),
		},
		workloads: map[string]*Workload{
			"burst_other_replica":   {ID: "wl_1", BurstID: "burst_other_replica"},
			"burst_other_replica_2": {ID: "wl_2", BurstID: "burst_other_replica_2"},
		},
	}

	active, oldest, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 2 {
		t.Errorf("active = %d, want 2 — the durable set is every replica's bursts, not this one's map", active)
	}
	if oldest != 10*time.Minute {
		t.Errorf("oldestAge = %v, want 10m; 40m would mean the in-memory map answered", oldest)
	}
}

// TestLiveProvisioningDurableWorkloadOverridesStaleMemory proves the workload
// half of the multi-replica property. A replica-local copy must not keep a
// burst provisioning after another replica records /started durably.
func TestLiveProvisioningDurableWorkloadOverridesStaleMemory(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl_local", BurstID: "burst_shared"})
	startedAt := scrapeAt.Add(-time.Minute)
	s.persist = &provisioningPersister{
		bursts: []*Burst{provisioningBurst("burst_shared", 5*time.Minute)},
		workloads: map[string]*Workload{
			"burst_shared": {ID: "wl_durable", BurstID: "burst_shared", StartedAt: &startedAt},
		},
	}

	active, _, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 0 {
		t.Errorf("active = %d, want 0 — durable /started must override stale replica memory", active)
	}
}

// TestLiveProvisioningFallsBackToMemory covers the OSS/in-memory store.
func TestLiveProvisioningFallsBackToMemory(t *testing.T) {
	s := New()
	for _, b := range []*Burst{
		provisioningBurst("burst_a", 7*time.Minute),
		provisioningBurst("burst_b", 20*time.Second),
		{ID: "burst_running", Status: BurstStatusRunning, CreatedAt: scrapeAt.Add(-3 * time.Hour)},
	} {
		if err := s.PutBurst(b); err != nil {
			t.Fatalf("PutBurst %s: %v", b.ID, err)
		}
	}
	s.PutWorkload(&Workload{ID: "wl_a", BurstID: "burst_a"})
	s.PutWorkload(&Workload{ID: "wl_b", BurstID: "burst_b"})
	startedAt := scrapeAt.Add(-2 * time.Hour)
	s.PutWorkload(&Workload{ID: "wl_running", BurstID: "burst_running", StartedAt: &startedAt})

	active, oldest, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 2 {
		t.Errorf("active = %d, want 2 — with no durable backend the in-memory map is the truth", active)
	}
	if oldest != 7*time.Minute {
		t.Errorf("oldestAge = %v, want 7m", oldest)
	}
}

// TestLiveProvisioningManagedJoiningAndRunningStillActive proves that managed
// bursts in Joining or Running phase with nil StartedAt are still counted.
func TestLiveProvisioningManagedJoiningAndRunningStillActive(t *testing.T) {
	s := New()
	for _, b := range []*Burst{
		{ID: "burst_joining", Status: BurstStatusJoining, CreatedAt: scrapeAt.Add(-5 * time.Minute)},
		{ID: "burst_running", Status: BurstStatusRunning, CreatedAt: scrapeAt.Add(-3 * time.Minute)},
	} {
		if err := s.PutBurst(b); err != nil {
			t.Fatalf("PutBurst %s: %v", b.ID, err)
		}
	}
	s.PutWorkload(&Workload{ID: "wl_joining", BurstID: "burst_joining"})
	s.PutWorkload(&Workload{ID: "wl_running", BurstID: "burst_running"})

	active, oldest, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 2 {
		t.Errorf("active = %d, want 2 — managed Joining/Running with nil StartedAt must count", active)
	}
	if oldest != 5*time.Minute {
		t.Errorf("oldestAge = %v, want 5m", oldest)
	}
}

// TestLiveProvisioningManagedStartedAtExits proves that a managed burst
// whose workload has StartedAt set is no longer active.
func TestLiveProvisioningManagedStartedAtExits(t *testing.T) {
	s := New()
	if err := s.PutBurst(&Burst{
		ID: "burst_started", Status: BurstStatusRunning, CreatedAt: scrapeAt.Add(-10 * time.Minute),
	}); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	startedAt := scrapeAt.Add(-8 * time.Minute)
	s.PutWorkload(&Workload{ID: "wl_started", BurstID: "burst_started", StartedAt: &startedAt})

	active, _, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 0 {
		t.Errorf("active = %d, want 0 — StartedAt means provisioning is over", active)
	}
}

// TestLiveProvisioningTerminalWorkloadExits proves a burst whose workload
// is terminal is not active.
func TestLiveProvisioningTerminalWorkloadExits(t *testing.T) {
	s := New()
	if err := s.PutBurst(&Burst{
		ID: "burst_done", Status: BurstStatusProvisioning, CreatedAt: scrapeAt.Add(-10 * time.Minute),
	}); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	finishedAt := scrapeAt.Add(-5 * time.Minute)
	s.PutWorkload(&Workload{ID: "wl_done", BurstID: "burst_done", FinishedAt: &finishedAt})

	active, _, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 0 {
		t.Errorf("active = %d, want 0 — terminal workload means provisioning is over", active)
	}
}

func TestLiveProvisioningTerminalStatusWithoutTimestampExits(t *testing.T) {
	s := New()
	if err := s.PutBurst(provisioningBurst("burst_terminal_status", 10*time.Minute)); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	s.PutWorkload(&Workload{
		ID: "wl_terminal_status", BurstID: "burst_terminal_status", Status: "failed",
	})

	active, _, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 0 {
		t.Errorf("active = %d, want 0 — a terminal status must not be counted when its timestamp is missing", active)
	}
}

func TestLiveProvisioningStateReturnsAllKnownBurstIDs(t *testing.T) {
	s := New()
	if err := s.PutBurst(provisioningBurst("burst_started", 10*time.Minute)); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	startedAt := scrapeAt.Add(-time.Minute)
	s.PutWorkload(&Workload{ID: "wl_started", BurstID: "burst_started", StartedAt: &startedAt})

	active, oldest, known, err := s.LiveProvisioningState(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioningState: %v", err)
	}
	if active != 0 || oldest != 0 {
		t.Fatalf("active=%d oldest=%v, want 0/0", active, oldest)
	}
	if !known["burst_started"] {
		t.Fatal("known burst set omitted a started burst; lifecycle dedupe could resurrect it")
	}
}

// TestLiveProvisioningNodeOnlyJoiningActive proves a nodeOnly burst in
// Joining phase is counted as active.
func TestLiveProvisioningNodeOnlyJoiningActive(t *testing.T) {
	s := New()
	if err := s.PutBurst(&Burst{
		ID: "burst_no", Status: BurstStatusJoining, NodeOnly: true,
		CreatedAt: scrapeAt.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}

	active, _, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 1 {
		t.Errorf("active = %d, want 1 — nodeOnly Joining must count", active)
	}
}

// TestLiveProvisioningNodeOnlyRunningExits proves a nodeOnly burst in Running
// phase exits active provisioning.
func TestLiveProvisioningNodeOnlyRunningExits(t *testing.T) {
	s := New()
	if err := s.PutBurst(&Burst{
		ID: "burst_no_run", Status: BurstStatusRunning, NodeOnly: true,
		CreatedAt: scrapeAt.Add(-5 * time.Minute),
	}); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}

	active, _, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 0 {
		t.Errorf("active = %d, want 0 — nodeOnly Running means node Ready, provisioning is over", active)
	}
}

// TestLiveProvisioningMissingWorkloadConservativelyActive proves a burst
// with no matching workload (just booked) is conservatively active.
func TestLiveProvisioningMissingWorkloadConservativelyActive(t *testing.T) {
	s := New()
	if err := s.PutBurst(provisioningBurst("burst_orphan", 30*time.Second)); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}

	active, _, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 1 {
		t.Errorf("active = %d, want 1 — missing workload must be conservatively active", active)
	}
}

// TestLiveProvisioningClockEdges pins ages that must never be fabricated.
func TestLiveProvisioningClockEdges(t *testing.T) {
	tests := []struct {
		name       string
		rows       []*Burst
		wantActive int
		wantOldest time.Duration
		why        string
	}{
		{
			name: "zero CreatedAt is counted but never aged",
			rows: []*Burst{
				{ID: "burst_legacy", Status: BurstStatusProvisioning},
			},
			wantActive: 1,
			why:        "a legacy row with no CreatedAt IS provisioning, but there is nothing to measure from",
		},
		{
			name: "CreatedAt in the future contributes no age",
			rows: []*Burst{
				{ID: "burst_skewed", Status: BurstStatusProvisioning, CreatedAt: scrapeAt.Add(time.Hour)},
			},
			wantActive: 1,
			why:        "clock skew between replicas must not export a negative age",
		},
		{
			name: "a real age still wins over zero and future rows",
			rows: []*Burst{
				{ID: "burst_legacy", Status: BurstStatusProvisioning},
				{ID: "burst_skewed", Status: BurstStatusProvisioning, CreatedAt: scrapeAt.Add(time.Hour)},
				provisioningBurst("burst_real", 8*time.Minute),
			},
			wantActive: 3,
			wantOldest: 8 * time.Minute,
			why:        "the unageable rows must not suppress the one burst that can be aged",
		},
		{
			name: "created exactly at scrape time is age zero, not negative",
			rows: []*Burst{
				{ID: "burst_now", Status: BurstStatusProvisioning, CreatedAt: scrapeAt},
			},
			wantActive: 1,
			why:        "a burst created this instant is 0s old",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			wls := make(map[string]*Workload)
			for _, b := range tc.rows {
				wls[b.ID] = &Workload{ID: "wl_" + b.ID, BurstID: b.ID}
			}
			s.persist = &provisioningPersister{bursts: tc.rows, workloads: wls}

			active, oldest, err := s.LiveProvisioning(context.Background(), scrapeAt)
			if err != nil {
				t.Fatalf("LiveProvisioning: %v", err)
			}
			if active != tc.wantActive {
				t.Errorf("active = %d, want %d", active, tc.wantActive)
			}
			if oldest != tc.wantOldest {
				t.Errorf("oldestAge = %v, want %v — %s", oldest, tc.wantOldest, tc.why)
			}
			if oldest < 0 {
				t.Errorf("oldestAge = %v is negative; the alert threshold comparison would be meaningless", oldest)
			}
		})
	}
}

// TestLiveProvisioningReadFailureIsHonest is the fail-closed contract.
func TestLiveProvisioningReadFailureIsHonest(t *testing.T) {
	s := New()
	if err := s.PutBurst(provisioningBurst("burst_mem_only", time.Hour)); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	readErr := errors.New("postgres is down")
	s.persist = &provisioningPersister{
		bursts: []*Burst{provisioningBurst("burst_durable", time.Hour)},
		err:    readErr,
	}

	active, oldest, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if !errors.Is(err, readErr) {
		t.Fatalf("err = %v, want %v — a failed durable read must not be reported as a fleet with nothing provisioning", err, readErr)
	}
	if active != 0 || oldest != 0 {
		t.Errorf("active=%d oldestAge=%v alongside an error, want 0/0", active, oldest)
	}
}

// TestLiveProvisioningWorkloadLookupFailureIsHonest proves that a durable
// workload lookup failure makes the whole view unknown.
func TestLiveProvisioningWorkloadLookupFailureIsHonest(t *testing.T) {
	s := New()
	wlErr := errors.New("workload lookup broken")
	s.persist = &provisioningPersister{
		bursts:    []*Burst{provisioningBurst("burst_1", 5*time.Minute)},
		workloads: map[string]*Workload{},
		wlErr:     wlErr,
	}

	_, _, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if !errors.Is(err, wlErr) {
		t.Fatalf("err = %v, want %v — a workload lookup failure must fail the whole view", err, wlErr)
	}
}

// TestLiveProvisioningEmptyFleet proves the honest zero.
func TestLiveProvisioningEmptyFleet(t *testing.T) {
	s := New()
	s.persist = &provisioningPersister{}

	active, oldest, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 0 || oldest != 0 {
		t.Errorf("active=%d oldestAge=%v, want 0/0", active, oldest)
	}
}

// TestLiveProvisioningOSSBehavior proves OSS/in-memory: no persister wired,
// workloads come from the in-memory map.
func TestLiveProvisioningOSSBehavior(t *testing.T) {
	s := New()
	if err := s.PutBurst(provisioningBurst("burst_oss", 3*time.Minute)); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	s.PutWorkload(&Workload{ID: "wl_oss", BurstID: "burst_oss"})

	active, oldest, err := s.LiveProvisioning(context.Background(), scrapeAt)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 1 {
		t.Errorf("active = %d, want 1", active)
	}
	if oldest != 3*time.Minute {
		t.Errorf("oldestAge = %v, want 3m", oldest)
	}
}
