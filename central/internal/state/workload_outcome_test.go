package state

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func count(n int64) *int64 { return &n }

func succeededWithFailedExport() *WorkloadOutcome {
	return &WorkloadOutcome{
		Compute: WorkloadComputeOutcome{Result: WorkloadResultSucceeded},
		Artifacts: &WorkloadArtifactOutcome{
			Result:          WorkloadResultFailed,
			Reason:          "upload rejected by destination",
			ObjectsUploaded: count(3),
			BytesUploaded:   count(4096),
		},
	}
}

// The receipt and the terminal status it explains are written in one critical
// section, so a reader can never see one without the other.
func TestFinishWorkloadWithOutcomeStoresReceipt(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})
	now := time.Now().UTC()

	if !s.FinishWorkloadWithOutcome("wl1", "succeeded", now, false, succeededWithFailedExport()) {
		t.Fatal("FinishWorkloadWithOutcome should apply to a running workload")
	}

	got, err := s.GetWorkload("wl1")
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if got.Status != "succeeded" || got.FinishedAt == nil {
		t.Fatalf("status=%q finishedAt=%v, want succeeded + set", got.Status, got.FinishedAt)
	}
	if got.Outcome == nil {
		t.Fatal("terminal status stored without the receipt that came with it")
	}
	if got.Outcome.Compute.Result != WorkloadResultSucceeded {
		t.Errorf("compute result = %q, want %q", got.Outcome.Compute.Result, WorkloadResultSucceeded)
	}
	if got.Outcome.Artifacts == nil || got.Outcome.Artifacts.Result != WorkloadResultFailed {
		t.Fatalf("artifacts = %+v, want a failed export beside a succeeded compute", got.Outcome.Artifacts)
	}
	if *got.Outcome.Artifacts.ObjectsUploaded != 3 || *got.Outcome.Artifacts.BytesUploaded != 4096 {
		t.Errorf("counts = %d objects / %d bytes, want 3 / 4096",
			*got.Outcome.Artifacts.ObjectsUploaded, *got.Outcome.Artifacts.BytesUploaded)
	}
}

// A plain finish — cancel, the watchdog, an agent reporting only the phase —
// stores no receipt. Central never derives one from the status it just wrote.
func TestFinishWorkloadStoresNoReceipt(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})

	if !s.FinishWorkload("wl1", "succeeded", time.Now().UTC(), false) {
		t.Fatal("FinishWorkload should apply")
	}
	got, _ := s.GetWorkload("wl1")
	if got.Outcome != nil {
		t.Fatalf("outcome = %+v, want nil: nobody reported one", got.Outcome)
	}
}

// First observation to carry a receipt wins, matching the rule the terminal
// status itself follows: a redelivered report cannot rewrite what was recorded.
func TestFinishWorkloadWithOutcomeFirstWriteWins(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})
	now := time.Now().UTC()

	s.FinishWorkloadWithOutcome("wl1", "succeeded", now, false, &WorkloadOutcome{
		Compute: WorkloadComputeOutcome{Result: WorkloadResultSucceeded, Reason: "first"},
	})
	s.FinishWorkloadWithOutcome("wl1", "succeeded", now.Add(time.Minute), false, &WorkloadOutcome{
		Compute: WorkloadComputeOutcome{Result: WorkloadResultSucceeded, Reason: "replayed"},
		Artifacts: &WorkloadArtifactOutcome{
			Result: WorkloadResultSucceeded, ObjectsUploaded: count(9),
		},
	})

	got, _ := s.GetWorkload("wl1")
	if got.Outcome == nil || got.Outcome.Compute.Reason != "first" {
		t.Fatalf("outcome = %+v, want the first receipt retained", got.Outcome)
	}
	if got.Outcome.Artifacts != nil {
		t.Errorf("artifacts = %+v, want nil: the replay must not add to the stored receipt", got.Outcome.Artifacts)
	}
}

// A receipt must not outlive the observation it described — but the finish that
// displaces it is an observation too. When the winning status carries its own
// receipt, that receipt is what the record keeps: dropping both would leave a
// terminal status explained by nothing, and the explanation was reported.
func TestFinishWorkloadReplacesReceiptOnDifferentTerminalStatus(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})
	now := time.Now().UTC()

	s.FinishWorkloadWithOutcome("wl1", "succeeded", now, false, succeededWithFailedExport())

	// The watchdog failing a run a completion had already marked succeeded, and
	// carrying what it observed.
	s.FinishWorkloadWithOutcome("wl1", "failed", now.Add(time.Minute), false, &WorkloadOutcome{
		Compute: WorkloadComputeOutcome{Result: WorkloadResultFailed, Reason: "node lost"},
	})

	got, _ := s.GetWorkload("wl1")
	if got.Status != "failed" {
		t.Fatalf("status = %q, want failed", got.Status)
	}
	if got.Outcome == nil {
		t.Fatal("the winning observation's receipt was dropped with the stale one")
	}
	if got.Outcome.Compute.Result != WorkloadResultFailed || got.Outcome.Compute.Reason != "node lost" {
		t.Fatalf("compute = %+v, want the receipt the failing finish carried", got.Outcome.Compute)
	}
	if got.Outcome.Artifacts != nil {
		t.Errorf("artifacts = %+v, want nil: the succeeded receipt's export must not survive", got.Outcome.Artifacts)
	}
}

// The replacing receipt is copied in like any other, so the caller that supplied
// it keeps no pointer into the stored record.
func TestFinishWorkloadReplacementReceiptIsCloned(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})
	now := time.Now().UTC()

	s.FinishWorkloadWithOutcome("wl1", "succeeded", now, false, &WorkloadOutcome{
		Compute: WorkloadComputeOutcome{Result: WorkloadResultSucceeded},
	})
	replacement := succeededWithFailedExport()
	replacement.Compute.Result = WorkloadResultFailed
	s.FinishWorkloadWithOutcome("wl1", "failed", now.Add(time.Minute), false, replacement)

	replacement.Compute.Reason = "mutated-by-caller"
	replacement.Artifacts.Result = "mutated-by-caller"
	*replacement.Artifacts.ObjectsUploaded = 99

	got, _ := s.GetWorkload("wl1")
	if got.Outcome.Compute.Reason != "" ||
		got.Outcome.Artifacts.Result != WorkloadResultFailed ||
		*got.Outcome.Artifacts.ObjectsUploaded != 3 {
		t.Fatalf("stored replacement followed the caller's struct: %+v", got.Outcome)
	}
}

// A receipt must not outlive the observation it described: a later finish that
// carries none and lands a DIFFERENT terminal status drops it rather than
// leaving it to explain an event that no longer stands.
func TestFinishWorkloadDropsReceiptOnDifferentTerminalStatus(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})
	now := time.Now().UTC()

	s.FinishWorkloadWithOutcome("wl1", "succeeded", now, false, succeededWithFailedExport())

	// Same status again (a re-finish on the same observation) keeps it.
	s.FinishWorkload("wl1", "succeeded", now.Add(time.Second), false)
	if got, _ := s.GetWorkload("wl1"); got.Outcome == nil {
		t.Fatal("re-finishing the same terminal status dropped the receipt")
	}

	// The cancel path catching a record up writes a different status.
	s.FinishWorkload("wl1", "cancelled", now.Add(time.Minute), false)
	got, _ := s.GetWorkload("wl1")
	if got.Status != "cancelled" {
		t.Fatalf("status = %q, want cancelled", got.Status)
	}
	if got.Outcome != nil {
		t.Fatalf("outcome = %+v, want nil: a succeeded receipt must not survive onto a cancelled record", got.Outcome)
	}
}

// The watchdog's guard still wins: an already-finished workload keeps both its
// status and the receipt recorded with it.
func TestFinishWorkloadOnlyIfUnfinishedKeepsReceipt(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})
	now := time.Now().UTC()

	s.FinishWorkloadWithOutcome("wl1", "succeeded", now, false, succeededWithFailedExport())
	if s.FinishWorkload("wl1", "failed", now.Add(time.Minute), true) {
		t.Error("onlyIfUnfinished should not re-finish an already-finished workload")
	}
	got, _ := s.GetWorkload("wl1")
	if got.Status != "succeeded" || got.Outcome == nil || got.Outcome.Artifacts == nil {
		t.Fatalf("status=%q outcome=%+v, want the first observation intact", got.Status, got.Outcome)
	}
}

// Issue #93 pin: the cancel path uses onlyIfUnfinished=true, and the guard has
// to preserve every field the terminal record depends on — status, the
// finished_at that fixes when the run ended, and the receipt that explains it.
// A previous fix that only kept status would still let a late cancel move
// finished_at forward and drop the outcome, which is what timed central
// evidence out in the incident.
func TestFinishWorkloadOnlyIfUnfinishedPreservesTerminalTimestampAndOutcome(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
	}{
		{name: "cancel after succeeded", status: "succeeded"},
		{name: "cancel after failed", status: "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})
			completedAt := time.Date(2026, 8, 2, 12, 11, 48, 0, time.UTC)
			outcome := succeededWithFailedExport()
			if tc.status == "failed" {
				outcome = &WorkloadOutcome{Compute: WorkloadComputeOutcome{Result: WorkloadResultFailed, Reason: "exit code 137"}}
			}
			if !s.FinishWorkloadWithOutcome("wl1", tc.status, completedAt, false, outcome) {
				t.Fatalf("seed finish should apply to a running workload")
			}

			// The racing cancel — the burst-present branch of Workloads.Cancel now
			// calls this exact shape.
			cancelAt := completedAt.Add(629 * time.Millisecond)
			if s.FinishWorkload("wl1", "cancelled", cancelAt, true) {
				t.Error("onlyIfUnfinished cancel returned true against an already-finished workload")
			}

			got, err := s.GetWorkload("wl1")
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != tc.status {
				t.Errorf("status = %q, want %q — a late cancel rewrote a terminal status", got.Status, tc.status)
			}
			if got.FinishedAt == nil || !got.FinishedAt.Equal(completedAt) {
				t.Errorf("finished_at = %v, want the original %v — a late cancel moved the terminal timestamp",
					got.FinishedAt, completedAt)
			}
			if got.Outcome == nil {
				t.Fatalf("outcome dropped by a cancel that should have been a no-op")
			}
			if got.Outcome.Compute.Result == "" {
				t.Errorf("outcome.compute.result = empty, want the receipt intact: %+v", got.Outcome)
			}
		})
	}
}

// Nobody holds a pointer into a stored receipt: not the caller that supplied it,
// and not a reader that got it back. The counts are pointers, so a struct copy
// alone would share writable ints.
func TestWorkloadOutcomeCloneIsolation(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl1", CustomerID: "c", Status: "running"})

	supplied := succeededWithFailedExport()
	s.FinishWorkloadWithOutcome("wl1", "succeeded", time.Now().UTC(), false, supplied)

	supplied.Compute.Result = "mutated-by-caller"
	supplied.Artifacts.Result = "mutated-by-caller"
	*supplied.Artifacts.ObjectsUploaded = 99

	got, _ := s.GetWorkload("wl1")
	if got.Outcome.Compute.Result != WorkloadResultSucceeded ||
		got.Outcome.Artifacts.Result != WorkloadResultFailed ||
		*got.Outcome.Artifacts.ObjectsUploaded != 3 {
		t.Fatalf("stored receipt followed the caller's struct: %+v", got.Outcome.Artifacts)
	}

	got.Outcome.Compute.Result = "mutated-by-reader"
	got.Outcome.Artifacts.Result = "mutated-by-reader"
	*got.Outcome.Artifacts.BytesUploaded = 1

	again, _ := s.GetWorkload("wl1")
	if again.Outcome.Compute.Result != WorkloadResultSucceeded ||
		again.Outcome.Artifacts.Result != WorkloadResultFailed ||
		*again.Outcome.Artifacts.BytesUploaded != 4096 {
		t.Fatalf("a reader mutating its copy changed the store: %+v", again.Outcome.Artifacts)
	}

	listed := s.WorkloadsForCustomer("c", 10)
	if len(listed) != 1 || listed[0].Outcome == nil {
		t.Fatalf("list = %+v, want one record carrying the receipt", listed)
	}
	*listed[0].Outcome.Artifacts.ObjectsUploaded = 77
	if final, _ := s.GetWorkload("wl1"); *final.Outcome.Artifacts.ObjectsUploaded != 3 {
		t.Errorf("listed copies share the stored counts: %d", *final.Outcome.Artifacts.ObjectsUploaded)
	}
}

// The whole workload is stored as one JSON document, so the receipt has to
// survive marshal → unmarshal exactly, and a record written before it existed
// has to stay a record with no receipt rather than an empty one.
func TestWorkloadOutcomeJSONRoundTrip(t *testing.T) {
	stored := &Workload{ID: "wl1", CustomerID: "c", Status: "succeeded", Outcome: succeededWithFailedExport()}
	var back Workload
	mustRoundTrip(t, stored, &back)

	if back.Outcome == nil || back.Outcome.Compute.Result != WorkloadResultSucceeded {
		t.Fatalf("compute lost in round-trip: %+v", back.Outcome)
	}
	if back.Outcome.Artifacts == nil ||
		back.Outcome.Artifacts.Result != WorkloadResultFailed ||
		back.Outcome.Artifacts.Reason != "upload rejected by destination" ||
		*back.Outcome.Artifacts.ObjectsUploaded != 3 ||
		*back.Outcome.Artifacts.BytesUploaded != 4096 {
		t.Fatalf("artifact result lost in round-trip: %+v", back.Outcome.Artifacts)
	}

	// A count of zero is an observation, not an absence: it must come back as
	// zero and not as "not counted".
	counted := &Workload{ID: "wl2", Outcome: &WorkloadOutcome{
		Compute:   WorkloadComputeOutcome{Result: WorkloadResultSucceeded},
		Artifacts: &WorkloadArtifactOutcome{Result: WorkloadResultSucceeded, ObjectsUploaded: count(0)},
	}}
	var zeroed Workload
	mustRoundTrip(t, counted, &zeroed)
	if zeroed.Outcome.Artifacts.ObjectsUploaded == nil || *zeroed.Outcome.Artifacts.ObjectsUploaded != 0 {
		t.Errorf("counted-zero came back as %v, want 0", zeroed.Outcome.Artifacts.ObjectsUploaded)
	}

	var legacy Workload
	if err := json.Unmarshal([]byte(`{"ID":"wl_old","Status":"succeeded"}`), &legacy); err != nil {
		t.Fatalf("legacy workload document must stay readable: %v", err)
	}
	if legacy.Outcome != nil {
		t.Errorf("legacy record gained a receipt: %+v", legacy.Outcome)
	}
	encoded, err := json.Marshal(&legacy)
	if err != nil || strings.Contains(string(encoded), "Outcome") {
		t.Errorf("re-encoded legacy record = %s (err %v), want no Outcome key", encoded, err)
	}
}
