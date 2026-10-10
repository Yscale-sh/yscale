package cost

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeStateSource is a minimal ProvisioningSource for testing.
type fakeStateSource struct {
	active int
	oldest time.Duration
	ids    map[string]bool
	err    error
}

func (f *fakeStateSource) LiveProvisioningState(_ context.Context, _ time.Time) (int, time.Duration, map[string]bool, error) {
	return f.active, f.oldest, f.ids, f.err
}

// fakeLifecycleSource stands in for lifecycle.Store.LiveProvisioningOps.
type fakeLifecycleSource struct {
	ops []LifecycleProvisioningRecord
	err error
}

func (f *fakeLifecycleSource) LiveProvisioningOps(_ context.Context) ([]LifecycleProvisioningRecord, error) {
	return f.ops, f.err
}

var compositeNow = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

// TestCompositeOSSFallthrough proves that when lifecycle is nil, the
// composite falls through to state alone.
func TestCompositeOSSFallthrough(t *testing.T) {
	state := &fakeStateSource{active: 2, oldest: 5 * time.Minute}
	c := NewCompositeSource(state, nil)

	active, oldest, err := c.LiveProvisioning(context.Background(), compositeNow)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if active != 2 {
		t.Errorf("active = %d, want 2", active)
	}
	if oldest != 5*time.Minute {
		t.Errorf("oldest = %v, want 5m", oldest)
	}
}

// TestCompositeProcessingCreateWithNoStateBurst proves that a lifecycle
// processing create op with no corresponding state burst is counted.
func TestCompositeProcessingCreateWithNoStateBurst(t *testing.T) {
	state := &fakeStateSource{active: 0, oldest: 0}
	lc := &fakeLifecycleSource{
		ops: []LifecycleProvisioningRecord{
			{BurstID: "burst_lc_1", CreatedAt: compositeNow.Add(-3 * time.Minute)},
		},
	}
	c := NewCompositeSource(state, lc)
	active, oldest, err := c.LiveProvisioning(context.Background(), compositeNow)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if active != 1 {
		t.Errorf("active = %d, want 1 — processing create with no state burst must count", active)
	}
	if oldest != 3*time.Minute {
		t.Errorf("oldest = %v, want 3m", oldest)
	}
}

// TestCompositeSucceededCreateWithNoStateBurst proves that a lifecycle
// succeeded create op with no corresponding state burst (crash before booking)
// is counted.
func TestCompositeSucceededCreateWithNoStateBurst(t *testing.T) {
	state := &fakeStateSource{active: 0, oldest: 0}
	lc := &fakeLifecycleSource{
		ops: []LifecycleProvisioningRecord{
			{BurstID: "burst_succeeded", CreatedAt: compositeNow.Add(-7 * time.Minute)},
		},
	}
	c := NewCompositeSource(state, lc)
	active, oldest, err := c.LiveProvisioning(context.Background(), compositeNow)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if active != 1 {
		t.Errorf("active = %d, want 1 — succeeded create with no state burst must count", active)
	}
	if oldest != 7*time.Minute {
		t.Errorf("oldest = %v, want 7m", oldest)
	}
}

// TestCompositeDedupeWhenStateBurstExists proves that a lifecycle record
// whose BurstID exists in state is not double-counted.
func TestCompositeDedupeWhenStateBurstExists(t *testing.T) {
	state := &fakeStateSource{active: 1, oldest: 5 * time.Minute, ids: map[string]bool{"burst_known": true}}
	lc := &fakeLifecycleSource{
		ops: []LifecycleProvisioningRecord{
			{BurstID: "burst_known", CreatedAt: compositeNow.Add(-5 * time.Minute)},
		},
	}
	c := NewCompositeSource(state, lc)
	active, oldest, err := c.LiveProvisioning(context.Background(), compositeNow)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if active != 1 {
		t.Errorf("active = %d, want 1 — lifecycle record with known state burst must not double-count", active)
	}
	if oldest != 5*time.Minute {
		t.Errorf("oldest = %v, want 5m", oldest)
	}
}

// TestCompositeStartedWorkloadNotResurrected proves that a started workload
// is not resurrected by a succeeded lifecycle create op.
func TestCompositeStartedWorkloadNotResurrected(t *testing.T) {
	// State says 0 active (the burst's workload has started).
	state := &fakeStateSource{active: 0, oldest: 0, ids: map[string]bool{"burst_started": true}}
	lc := &fakeLifecycleSource{
		ops: []LifecycleProvisioningRecord{
			{BurstID: "burst_started", CreatedAt: compositeNow.Add(-10 * time.Minute)},
		},
	}
	// The burst IS known in state — state decided it's not provisioning.
	c := NewCompositeSource(state, lc)
	active, _, err := c.LiveProvisioning(context.Background(), compositeNow)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if active != 0 {
		t.Errorf("active = %d, want 0 — started workload must not be resurrected by lifecycle op", active)
	}
}

// TestCompositeStateSourceFailure proves that a state source failure
// propagates as an error.
func TestCompositeStateSourceFailure(t *testing.T) {
	stateErr := errors.New("state broken")
	state := &fakeStateSource{err: stateErr}
	lc := &fakeLifecycleSource{}
	c := NewCompositeSource(state, lc)
	_, _, err := c.LiveProvisioning(context.Background(), compositeNow)
	if err == nil || !errors.Is(err, stateErr) {
		t.Fatalf("err = %v, want wrapping %v", err, stateErr)
	}
}

// TestCompositeLifecycleSourceFailure proves that a lifecycle source failure
// propagates as an error (source_up 0).
func TestCompositeLifecycleSourceFailure(t *testing.T) {
	state := &fakeStateSource{active: 1, oldest: time.Minute}
	lcErr := errors.New("lifecycle broken")
	lc := &fakeLifecycleSource{err: lcErr}
	c := NewCompositeSource(state, lc)
	_, _, err := c.LiveProvisioning(context.Background(), compositeNow)
	if err == nil || !errors.Is(err, lcErr) {
		t.Fatalf("err = %v, want wrapping %v", err, lcErr)
	}
}

// TestCompositeMixedStateAndLifecycle proves the full composite: state has
// some active bursts, lifecycle has a processing op for an unknown burst.
func TestCompositeMixedStateAndLifecycle(t *testing.T) {
	state := &fakeStateSource{active: 2, oldest: 5 * time.Minute, ids: map[string]bool{
		"burst_known": true,
		"burst_a":     true,
		"burst_b":     true,
	}}
	lc := &fakeLifecycleSource{
		ops: []LifecycleProvisioningRecord{
			{BurstID: "burst_known", CreatedAt: compositeNow.Add(-5 * time.Minute)},
			{BurstID: "burst_prebooking", CreatedAt: compositeNow.Add(-8 * time.Minute)},
		},
	}
	c := NewCompositeSource(state, lc)
	active, oldest, err := c.LiveProvisioning(context.Background(), compositeNow)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if active != 3 {
		t.Errorf("active = %d, want 3 — 2 from state + 1 pre-booking lifecycle op", active)
	}
	if oldest != 8*time.Minute {
		t.Errorf("oldest = %v, want 8m — the pre-booking lifecycle op is oldest", oldest)
	}
}

func TestCompositeDeduplicatesLifecycleRecordsByBurstID(t *testing.T) {
	state := &fakeStateSource{}
	lifecycle := &fakeLifecycleSource{ops: []LifecycleProvisioningRecord{
		{BurstID: "burst_duplicate", CreatedAt: compositeNow.Add(-2 * time.Minute)},
		{BurstID: "burst_duplicate", CreatedAt: compositeNow.Add(-3 * time.Minute)},
	}}

	active, oldest, err := NewCompositeSource(state, lifecycle).LiveProvisioning(context.Background(), compositeNow)
	if err != nil {
		t.Fatalf("LiveProvisioning: %v", err)
	}
	if active != 1 || oldest != 3*time.Minute {
		t.Fatalf("active=%d oldest=%v, want one record with the oldest duplicated age", active, oldest)
	}
}

func TestCompositeRequiresStateSource(t *testing.T) {
	for name, source := range map[string]*CompositeSource{
		"nil receiver": nil,
		"nil state":    NewCompositeSource(nil, nil),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := source.LiveProvisioning(context.Background(), compositeNow); err == nil {
				t.Fatal("missing state source reported a healthy snapshot")
			}
		})
	}
}
