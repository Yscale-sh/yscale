package state

import (
	"sync"
	"testing"
	"time"
)

// GetWorkload must return a COPY, not the live map pointer. Handlers fetch a
// workload and read its fields while the reaper watchdog concurrently finishes
// it; sharing the pointer is a data race on Status/FinishedAt. Returning a
// snapshot means a caller mutating the result can never corrupt stored state
// (or race a concurrent writer).
func TestGetWorkloadReturnsCopy(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})

	got, err := s.GetWorkload("wl1")
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	got.Status = "mutated-by-caller"

	again, _ := s.GetWorkload("wl1")
	if again.Status != "running" {
		t.Errorf("stored status = %q; a caller mutating the returned copy must not change the store", again.Status)
	}
}

func TestWorkloadsForCustomerReturnsSortedCopies(t *testing.T) {
	s := New()
	base := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	s.PutWorkload(&Workload{ID: "wl_b", CustomerID: "cust_a", CreatedAt: base, Status: "running", SpecYAML: []byte("spec-b")})
	s.PutWorkload(&Workload{ID: "wl_a", CustomerID: "cust_a", CreatedAt: base, Status: "pending", SpecYAML: []byte("spec-a")})
	s.PutWorkload(&Workload{ID: "wl_new", CustomerID: "cust_a", CreatedAt: base.Add(time.Minute), Status: "running", SpecYAML: []byte("spec-new")})
	s.PutWorkload(&Workload{ID: "wl_other", CustomerID: "cust_b", CreatedAt: base.Add(time.Hour)})

	got := s.WorkloadsForCustomer("cust_a", 100)
	if len(got) != 3 {
		t.Fatalf("workloads = %+v, want three for cust_a", got)
	}
	if got[0].ID != "wl_new" || got[1].ID != "wl_a" || got[2].ID != "wl_b" {
		t.Fatalf("order = [%s %s %s], want newest first then ID", got[0].ID, got[1].ID, got[2].ID)
	}

	got[0].Status = "caller-mutated"
	got[0].SpecYAML[0] = 'X'
	again, err := s.GetWorkload("wl_new")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != "running" || string(again.SpecYAML) != "spec-new" {
		t.Fatalf("returned record was live: status=%q spec=%q", again.Status, again.SpecYAML)
	}
	if empty := s.WorkloadsForCustomer("cust_none", 100); empty == nil || len(empty) != 0 {
		t.Fatalf("empty list = %#v, want non-nil empty slice", empty)
	}
	if limited := s.WorkloadsForCustomer("cust_a", 1); len(limited) != 1 || limited[0].ID != got[0].ID {
		t.Fatalf("limited workloads = %#v, want newest record only", limited)
	}
	if none := s.WorkloadsForCustomer("cust_a", 0); none == nil || len(none) != 0 {
		t.Fatalf("zero-limit workloads = %#v, want non-nil empty slice", none)
	}
}

// FinishWorkload sets Status + FinishedAt atomically under the store lock so the
// three reap paths (Complete, Cancel, watchdog) never mutate a shared pointer
// concurrently. onlyIfUnfinished=true is the watchdog's "don't clobber an
// already-finished workload" guard, evaluated under the same lock.
func TestFinishWorkload(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})
	now := time.Now().UTC()

	if !s.FinishWorkload("wl1", "succeeded", now, false) {
		t.Fatal("FinishWorkload should apply to a running workload")
	}
	got, _ := s.GetWorkload("wl1")
	if got.Status != "succeeded" || got.FinishedAt == nil {
		t.Fatalf("status=%q finishedAt=%v, want succeeded + set", got.Status, got.FinishedAt)
	}

	// onlyIfUnfinished must refuse to overwrite an already-finished workload.
	if s.FinishWorkload("wl1", "failed", now, true) {
		t.Error("FinishWorkload(onlyIfUnfinished) should not re-finish an already-finished workload")
	}
	got, _ = s.GetWorkload("wl1")
	if got.Status != "succeeded" {
		t.Errorf("status = %q, want unchanged 'succeeded'", got.Status)
	}

	// Unknown id → false.
	if s.FinishWorkload("nope", "failed", now, false) {
		t.Error("FinishWorkload on unknown id should return false")
	}
}

// StartWorkload stamps StartedAt + flips Status to "running" atomically under
// the store lock, exactly once: a repeat report (the agent re-delivers on watch
// resync) must not re-stamp the timestamp.
func TestStartWorkload(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "provisioning"})
	now := time.Now().UTC()

	if !s.StartWorkload("wl1", now) {
		t.Fatal("StartWorkload should apply to an unstarted workload")
	}
	got, _ := s.GetWorkload("wl1")
	if got.Status != "running" || got.StartedAt == nil || !got.StartedAt.Equal(now) {
		t.Fatalf("status=%q startedAt=%v, want running + %v", got.Status, got.StartedAt, now)
	}

	// Second report must be refused and leave the original stamp intact.
	if s.StartWorkload("wl1", now.Add(time.Minute)) {
		t.Error("StartWorkload should refuse a second stamp")
	}
	got, _ = s.GetWorkload("wl1")
	if !got.StartedAt.Equal(now) {
		t.Errorf("startedAt = %v, want original %v", got.StartedAt, now)
	}

	// Unknown id → false.
	if s.StartWorkload("nope", now) {
		t.Error("StartWorkload on unknown id should return false")
	}
}

// A late started report (the Job finished before the agent's start report
// landed) must never resurrect a terminal workload: Status and FinishedAt
// stand, StartedAt stays nil.
func TestStartWorkloadRefusesAfterFinish(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "provisioning"})
	now := time.Now().UTC()

	if !s.FinishWorkload("wl1", "succeeded", now, false) {
		t.Fatal("FinishWorkload should apply")
	}
	if s.StartWorkload("wl1", now.Add(time.Second)) {
		t.Error("StartWorkload should refuse an already-finished workload")
	}
	got, _ := s.GetWorkload("wl1")
	if got.Status != "succeeded" || got.StartedAt != nil {
		t.Errorf("status=%q startedAt=%v, want succeeded + nil", got.Status, got.StartedAt)
	}
}

// Under -race, concurrent finishers + readers on the same workload must not race
// and exactly one onlyIfUnfinished finisher must win.
func TestFinishWorkloadConcurrent(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})
	now := time.Now().UTC()

	var wins int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if s.FinishWorkload("wl1", "failed", now, true) {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
		go func() {
			defer wg.Done()
			_, _ = s.GetWorkload("wl1") // concurrent reader
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Errorf("onlyIfUnfinished winners = %d, want exactly 1", wins)
	}
}
