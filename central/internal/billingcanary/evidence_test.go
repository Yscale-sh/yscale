// yscale:proprietary

package billingcanary

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEvidenceSinkOrdersAndDerivesHealth(t *testing.T) {
	sink := NewEvidenceSink(time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC))
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("Add err = %v", err)
		}
	}
	must(sink.Add(ScenarioResult{Name: "settle_exactly_once", Outcome: OutcomePass, DurationMS: 42}))
	must(sink.Add(ScenarioResult{Name: "cross_tenant_denied", Outcome: OutcomePass, DurationMS: 8}))
	must(sink.Add(ScenarioResult{Name: "invalid_signature_rejected", Outcome: OutcomePass, DurationMS: 5, Counts: map[string]int64{"status_400": 1}}))

	snap := sink.Snapshot(time.Date(2026, 8, 25, 12, 0, 5, 0, time.UTC))
	if !snap.Healthy {
		t.Fatalf("Healthy = false, want true")
	}
	if !snap.Complete {
		t.Fatalf("Complete = false, want true when nothing was skipped")
	}
	if got := snap.Scenarios[0].Name; got != "cross_tenant_denied" {
		t.Fatalf("first scenario after sort = %q", got)
	}

	must(sink.Add(ScenarioResult{Name: "kill_switch", Outcome: OutcomeFail, DurationMS: 12, Note: "unexpected 200"}))
	snap = sink.Snapshot(time.Date(2026, 8, 25, 12, 0, 6, 0, time.UTC))
	if snap.Healthy {
		t.Fatalf("Healthy = true, want false after a failure")
	}
	if snap.Complete {
		t.Fatalf("Complete = true, want false after a failure")
	}
}

func TestEvidenceSinkSkippedIsIncompleteButHealthy(t *testing.T) {
	sink := NewEvidenceSink(time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC))
	if err := sink.Add(ScenarioResult{Name: "cross_tenant_denied", Outcome: OutcomePass}); err != nil {
		t.Fatalf("Add pass: %v", err)
	}
	if err := sink.Add(ScenarioResult{Name: "settle_exactly_once", Outcome: OutcomeSkipped, Note: "live sentinel absent"}); err != nil {
		t.Fatalf("Add skipped: %v", err)
	}
	snap := sink.Snapshot(time.Date(2026, 8, 25, 12, 0, 1, 0, time.UTC))
	if !snap.Healthy {
		t.Fatalf("Healthy = false, want true (a skipped scenario does not indict health)")
	}
	if snap.Complete {
		t.Fatalf("Complete = true, want false when a scenario was skipped")
	}
}

func TestEvidenceSinkRejectsForbiddenName(t *testing.T) {
	sink := NewEvidenceSink(time.Now())
	if err := sink.Add(ScenarioResult{Name: "pi_leak_scenario", Outcome: OutcomePass}); err == nil {
		t.Fatalf("Add accepted forbidden scenario name")
	}
	if err := sink.Add(ScenarioResult{Name: "reversal_applied", Outcome: OutcomePass, Note: "cs_test_leak"}); err == nil {
		t.Fatalf("Add accepted forbidden note")
	}
	if err := sink.Add(ScenarioResult{Name: "reversal_applied", Outcome: OutcomePass, Counts: map[string]int64{"Bad Key": 1}}); err == nil {
		t.Fatalf("Add accepted forbidden count key")
	}
}

func TestWriteFileEnforcesCapAndPerm(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "evidence.json")
	sink := NewEvidenceSink(time.Now())
	if err := sink.Add(ScenarioResult{Name: "settle", Outcome: OutcomePass}); err != nil {
		t.Fatalf("Add err = %v", err)
	}
	if err := WriteFile(path, sink.Snapshot(time.Now())); err != nil {
		t.Fatalf("WriteFile err = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat err = %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("evidence file perm = %o, want 0600", perm)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile err = %v", err)
	}
	if len(body) > MaxEvidenceBytes {
		t.Errorf("evidence too large: %d bytes", len(body))
	}
	var decoded Evidence
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("evidence not valid JSON: %v", err)
	}
	if decoded.Mode != "test" {
		t.Errorf("mode = %q, want test", decoded.Mode)
	}
	if !strings.Contains(string(body), "\"settle\"") {
		t.Errorf("evidence missing scenario name")
	}
}
