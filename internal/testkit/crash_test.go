package testkit

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestCrashControllerRecordsCheckpointOrder(t *testing.T) {
	controller := mustController(t, CrashPlan{})
	path := []Checkpoint{
		BeforeAdmission, AfterAdmission,
		BeforeProviderCreateClaim, AfterProviderCreateClaim,
		BeforeProviderCreateCall, AfterProviderCreateCall,
		BeforeProviderCreateMark, AfterProviderCreateMark,
		BeforeCommandDispatch, AfterCommandDispatch,
		BeforeWorkloadCompletion, AfterWorkloadCompletion,
		BeforeProviderDeleteRequest, AfterProviderDeleteRequest,
		BeforeProviderDeleteClaim, AfterProviderDeleteClaim,
		BeforeProviderDeleteCall, AfterProviderDeleteCall,
		BeforeProviderDeleteMark, AfterProviderDeleteMark,
		BeforeSettlement, AfterSettlement,
	}
	for _, checkpoint := range path {
		if err := controller.Enter(checkpoint); err != nil {
			t.Fatalf("%s: %v", checkpoint, err)
		}
	}
	if got := controller.Order(); !checkpointsEqual(got, path) {
		t.Fatalf("order = %v, want %v", got, path)
	}
	if got := controller.Order(); !checkpointsEqual(got, Checkpoints()) {
		t.Fatalf("canonical order = %v, want %v", got, Checkpoints())
	}
	for i, hit := range controller.Hits() {
		if hit.Sequence != i+1 || hit.Occurrence != 1 || hit.Crashed {
			t.Fatalf("hit %d = %+v", i, hit)
		}
	}
	if controller.Fired() {
		t.Fatal("disarmed controller reported a crash")
	}
}

func TestCrashControllerFiresOnceAtConfiguredOccurrence(t *testing.T) {
	controller := mustController(t, CrashPlan{
		Checkpoint: AfterProviderCreateCall,
		Occurrence: 2,
	})
	if err := controller.Enter(AfterProviderCreateCall); err != nil {
		t.Fatalf("first occurrence must not crash: %v", err)
	}
	err := controller.Enter(AfterProviderCreateCall)
	if !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("second occurrence err = %v, want ErrInjectedCrash", err)
	}
	// Recovery must not be blocked by a spent crash.
	for i := range 3 {
		if err := controller.Enter(AfterProviderCreateCall); err != nil {
			t.Fatalf("occurrence %d after crash: %v", i+3, err)
		}
	}
	if !controller.Fired() {
		t.Fatal("controller did not record the crash")
	}
	if got := controller.Count(AfterProviderCreateCall); got != 5 {
		t.Fatalf("count = %d, want 5", got)
	}
	crashed := 0
	for _, hit := range controller.Hits() {
		if hit.Crashed {
			crashed++
			if hit.Occurrence != 2 || hit.Sequence != 2 {
				t.Fatalf("crash hit = %+v", hit)
			}
		}
	}
	if crashed != 1 {
		t.Fatalf("crashed hits = %d, want exactly 1", crashed)
	}
}

func TestCrashControllerCrashesOnlyItsOwnCheckpoint(t *testing.T) {
	controller := mustController(t, CrashPlan{
		Checkpoint: AfterProviderDeleteMark,
		Occurrence: 1,
	})
	for _, checkpoint := range Checkpoints() {
		if checkpoint == AfterProviderDeleteMark {
			continue
		}
		if err := controller.Enter(checkpoint); err != nil {
			t.Fatalf("%s crashed but was not configured: %v", checkpoint, err)
		}
	}
	if err := controller.Enter(AfterProviderDeleteMark); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("configured checkpoint err = %v", err)
	}
}

func TestCrashControllerRestartIsFresh(t *testing.T) {
	controller := mustController(t, CrashPlan{Checkpoint: AfterAdmission, Occurrence: 1})
	if err := controller.Enter(BeforeAdmission); err != nil {
		t.Fatal(err)
	}
	if err := controller.Enter(AfterAdmission); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("armed crash err = %v", err)
	}

	restarted, err := controller.Restart(CrashPlan{})
	if err != nil {
		t.Fatal(err)
	}
	if restarted == controller {
		t.Fatal("restart returned the same controller")
	}
	if restarted.Fired() || len(restarted.Hits()) != 0 || restarted.Count(BeforeAdmission) != 0 {
		t.Fatalf("restarted controller carried state: fired=%v hits=%d", restarted.Fired(), len(restarted.Hits()))
	}
	if err := restarted.Enter(AfterAdmission); err != nil {
		t.Fatalf("restarted controller crashed with a disarmed plan: %v", err)
	}
	// The crashed controller keeps its own log; nothing bled across.
	if len(controller.Hits()) != 2 || controller.Count(AfterAdmission) != 1 {
		t.Fatalf("original controller mutated by restart: %+v", controller.Hits())
	}

	rearmed, err := restarted.Restart(CrashPlan{Checkpoint: AfterProviderCreateClaim, Occurrence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := rearmed.Enter(AfterProviderCreateClaim); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("rearmed restart err = %v", err)
	}
}

func TestCrashControllerConcurrentHitsAreTotallyOrdered(t *testing.T) {
	const goroutines = 8
	const perGoroutine = 25
	controller := mustController(t, CrashPlan{
		Checkpoint: BeforeCommandDispatch,
		Occurrence: goroutines * perGoroutine / 2,
	})

	var mu sync.Mutex
	var crashes, failures int
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perGoroutine {
				err := controller.Enter(BeforeCommandDispatch)
				mu.Lock()
				switch {
				case errors.Is(err, ErrInjectedCrash):
					crashes++
				case err != nil:
					failures++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if failures != 0 {
		t.Fatalf("non-crash errors = %d", failures)
	}
	if crashes != 1 {
		t.Fatalf("crashes = %d, want exactly 1", crashes)
	}
	hits := controller.Hits()
	if len(hits) != goroutines*perGoroutine {
		t.Fatalf("hits = %d, want %d", len(hits), goroutines*perGoroutine)
	}
	// Sequence and occurrence are both dense and strictly increasing, so the
	// log is a total order even though the goroutines interleaved.
	for i, hit := range hits {
		if hit.Sequence != i+1 || hit.Occurrence != i+1 {
			t.Fatalf("hit %d = %+v, want sequence/occurrence %d", i, hit, i+1)
		}
	}
	if got := controller.Count(BeforeCommandDispatch); got != goroutines*perGoroutine {
		t.Fatalf("count = %d", got)
	}
}

func TestCrashControllerConcurrentDistinctCheckpoints(t *testing.T) {
	controller := mustController(t, CrashPlan{})
	path := Checkpoints()

	var wg sync.WaitGroup
	for _, checkpoint := range path {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				if err := controller.Enter(checkpoint); err != nil {
					t.Errorf("%s: %v", checkpoint, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	hits := controller.Hits()
	if len(hits) != len(path)*10 {
		t.Fatalf("hits = %d, want %d", len(hits), len(path)*10)
	}
	seen := make(map[Checkpoint]int, len(path))
	for i, hit := range hits {
		if hit.Sequence != i+1 {
			t.Fatalf("hit %d sequence = %d", i, hit.Sequence)
		}
		seen[hit.Checkpoint]++
		if hit.Occurrence != seen[hit.Checkpoint] {
			t.Fatalf("hit %d = %+v, want occurrence %d", i, hit, seen[hit.Checkpoint])
		}
	}
	for _, checkpoint := range path {
		if controller.Count(checkpoint) != 10 {
			t.Fatalf("%s count = %d, want 10", checkpoint, controller.Count(checkpoint))
		}
	}
}

func TestCrashControllerRejectsUnusablePlans(t *testing.T) {
	tests := []struct {
		name string
		plan CrashPlan
	}{
		{name: "unknown checkpoint", plan: CrashPlan{Checkpoint: "after_typo", Occurrence: 1}},
		{name: "zero occurrence", plan: CrashPlan{Checkpoint: AfterAdmission}},
		{name: "negative occurrence", plan: CrashPlan{Checkpoint: AfterAdmission, Occurrence: -1}},
		{name: "occurrence without checkpoint", plan: CrashPlan{Occurrence: 3}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewCrashController(test.plan); err == nil {
				t.Fatalf("plan %+v was accepted", test.plan)
			}
		})
	}
}

func TestCrashControllerRejectsUnknownCheckpointEntry(t *testing.T) {
	controller := mustController(t, CrashPlan{})
	err := controller.Enter("after_admision")
	if err == nil || errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("typo checkpoint err = %v, want a non-crash rejection", err)
	}
	if len(controller.Hits()) != 0 {
		t.Fatalf("rejected checkpoint was recorded: %+v", controller.Hits())
	}
}

func TestCheckpointsAreUniqueAndPaired(t *testing.T) {
	path := Checkpoints()
	if len(path)%2 != 0 {
		t.Fatalf("checkpoint count %d is not a set of before/after pairs", len(path))
	}
	seen := make(map[Checkpoint]struct{}, len(path))
	for i := 0; i < len(path); i += 2 {
		before, after := path[i], path[i+1]
		for _, checkpoint := range []Checkpoint{before, after} {
			if _, dup := seen[checkpoint]; dup {
				t.Fatalf("duplicate checkpoint %q", checkpoint)
			}
			seen[checkpoint] = struct{}{}
			if !ValidCheckpoint(checkpoint) {
				t.Fatalf("%q is not reported valid", checkpoint)
			}
		}
		suffix, ok := strings.CutPrefix(string(after), "after_")
		if !ok {
			t.Fatalf("%q is not an after checkpoint", after)
		}
		if want := Checkpoint("before_" + suffix); before != want {
			t.Fatalf("pair %q/%q is not a before/after pair", before, after)
		}
	}
	if ValidCheckpoint("") || ValidCheckpoint("before_nothing") {
		t.Fatal("unknown checkpoints reported valid")
	}
	// Checkpoints returns a fresh slice each call.
	path[0] = "mutated"
	if Checkpoints()[0] != BeforeAdmission {
		t.Fatal("Checkpoints exposed package state")
	}
}

func mustController(t *testing.T, plan CrashPlan) *CrashController {
	t.Helper()
	controller, err := NewCrashController(plan)
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func checkpointsEqual(got, want []Checkpoint) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
