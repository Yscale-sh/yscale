package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/internal/evidence"
)

func TestSanitizeFilenameCollisionSafe(t *testing.T) {
	a := sanitizeFilename("flyio-cpu-nano", "cases/flyio-cpu-nano.yaml", "yt-1234")
	b := sanitizeFilename("flyio-cpu-nano", "cases/flyio-cpu-nano.yaml", "yt-5678")
	if a == b {
		t.Fatalf("filenames must differ per runID: %q == %q", a, b)
	}
	if !strings.HasSuffix(a, ".json") {
		t.Fatalf("filename must end in .json: %q", a)
	}
}

func TestSanitizeFilenameRemovesUnsafeCharacters(t *testing.T) {
	name := sanitizeFilename("../../etc/passwd", "../../etc/passwd.yaml", "yt/evil")
	if strings.Contains(name, "/") {
		t.Fatalf("unsafe characters in filename: %q", name)
	}
}

func TestSanitizeFilenameTruncatesLongNames(t *testing.T) {
	long := strings.Repeat("a", 300)
	name := sanitizeFilename(long, long+".yaml", "yt-1")
	if len(name) > 220 {
		t.Fatalf("filename too long: %d chars", len(name))
	}
}

func TestSanitizeFilenameCollisionResistant(t *testing.T) {
	a := sanitizeFilename("foo/bar", "foo/bar.yaml", "yt-1")
	b := sanitizeFilename("foo_bar", "foo_bar.yaml", "yt-1")
	if a == b {
		t.Fatalf("distinct case names must not collide after sanitization: %q == %q", a, b)
	}
}

func TestSanitizeFilenameDeterministic(t *testing.T) {
	a := sanitizeFilename("test-case", "cases/test-case.yaml", "yt-123")
	b := sanitizeFilename("test-case", "cases/test-case.yaml", "yt-123")
	if a != b {
		t.Fatalf("same inputs must produce same filename: %q != %q", a, b)
	}
}

func TestSanitizeFilenameSameStemDifferentPaths(t *testing.T) {
	a := sanitizeFilename("gpu-test", "cases/linode/gpu-test.yaml", "yt-1")
	b := sanitizeFilename("gpu-test", "cases/flyio/gpu-test.yaml", "yt-1")
	if a == b {
		t.Fatalf("same stem from different directories must not collide: %q == %q", a, b)
	}
}

func completeEvidenceObs(t *testing.T) (CaseResult, evidenceObservations) {
	t.Helper()
	now := time.Now().UTC()
	admitted := now.Add(-10 * time.Minute)
	providerCreated := now.Add(-9 * time.Minute)
	nodeReady := now.Add(-8 * time.Minute)
	started := now.Add(-6 * time.Minute)
	finished := now.Add(-4 * time.Minute)
	requested := now.Add(-2 * time.Minute)
	deleted := now

	centralEv := &WorkloadEvidence{
		Status:     "succeeded",
		CreatedAt:  &admitted,
		StartedAt:  &started,
		FinishedAt: &finished,
		Placement: &PlacementEvidence{
			GPUProduct: "NVIDIA RTX 4000 Ada",
			Receipt: &PlacementReceiptEvidence{
				Selected: SelectedEvidence{
					Provider:              "linode",
					Region:                "us-east",
					SKU:                   "g2-gpu-rtx4000a1-s",
					GPUKind:               "rtx4000ada",
					GPUCount:              1,
					HourlyMicroUSD:        520000,
					MaximumChargeMicroUSD: 200000,
				},
				QuoteID:        "q_test_001",
				PricingVersion: 1,
			},
		},
		Cost: &CostEvidence{
			USD:       0.125,
			HourlyUSD: 0.52,
			Basis:     "metered",
		},
		Cleanup: &CleanupEvidence{
			State:              "terminated",
			RequestedAt:        &requested,
			DeletedAt:          &deleted,
			ProviderCreatedAt:  &providerCreated,
			DurableReapReceipt: true,
		},
		Outcome: &OutcomeEvidence{
			Compute: ComputeOutcomeEvidence{Result: "succeeded"},
		},
	}

	res := CaseResult{
		Case:    Case{Name: "linode-gpu-rtx4000", Path: "cases/linode-gpu-rtx4000.yaml"},
		Phase:   "Succeeded",
		Backend: "linode",
		BurstID: "burst_abc123def456",
		Reaped:  true,
	}

	obs := evidenceObservations{
		CentralEvidence:   centralEv,
		TargetClusterType: "k3s",
		NodeReadyAt:       &nodeReady,
		GPUReadyAt:        &nodeReady,
		ObservedNode: &evidence.ObservedNode{
			Name:            "ys-burst-abc123def456",
			GPUAllocatable:  1,
			GPUPresentLabel: true,
			GPUProductLabel: "NVIDIA-RTX-4000-Ada-Generation",
		},
		AccessProbe: &evidence.AccessProbe{
			NodeName:    "ys-burst-abc123def456",
			Logs:        true,
			Exec:        true,
			PortForward: true,
			ObservedAt:  nodeReady.Add(30 * time.Second),
		},
	}

	return res, obs
}

func TestWriteEvidenceSucceededCaseDowngradesWithoutInventory(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-1234", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed (no complete inventory)", artifactResult)
	}

	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-1234"))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode = %o, want 600", got)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Result != evidence.ResultFailed {
		t.Fatalf("result = %q, want failed", artifact.Result)
	}
	hasDiag := false
	for _, d := range artifact.Diagnostics {
		if d.Code == "evidence.incomplete" {
			hasDiag = true
		}
	}
	if !hasDiag {
		t.Fatal("downgraded artifact should carry evidence.incomplete diagnostic")
	}
}

func TestWriteEvidenceTargetClusterTypeFromObservation(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.TargetClusterType = "eks"
	commitSHA := strings.Repeat("a", 40)

	_, err := writeEvidence(dir, res, "yt-tct", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-tct"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.TargetClusterType != "eks" {
		t.Fatalf("target_cluster_type = %q, want eks (from observation, not hardcoded)", artifact.TargetClusterType)
	}
}

func TestWriteEvidenceCostFromReceiptAndTerminal(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	commitSHA := strings.Repeat("a", 40)

	_, err := writeEvidence(dir, res, "yt-cost", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-cost"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Cost.QuotedRateMicroUSDPerHour == nil {
		t.Fatal("quoted rate is nil")
	}
	if *artifact.Cost.QuotedRateMicroUSDPerHour != 520000 {
		t.Fatalf("quoted_rate = %d, want 520000 (from receipt)", *artifact.Cost.QuotedRateMicroUSDPerHour)
	}
	if artifact.Cost.MaximumMicroUSD == nil {
		t.Fatal("maximum is nil")
	}
	if *artifact.Cost.MaximumMicroUSD != 200000 {
		t.Fatalf("maximum = %d, want 200000 (from receipt, not terminal)", *artifact.Cost.MaximumMicroUSD)
	}
	if artifact.Cost.TerminalMicroUSD == nil {
		t.Fatal("terminal is nil")
	}
	if *artifact.Cost.TerminalMicroUSD != 125000 {
		t.Fatalf("terminal = %d, want 125000 (from terminal cost)", *artifact.Cost.TerminalMicroUSD)
	}
}

func TestWriteEvidenceQuoteProvenance(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	commitSHA := strings.Repeat("a", 40)

	_, err := writeEvidence(dir, res, "yt-qp", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-qp"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Cost.QuoteID != "q_test_001" {
		t.Fatalf("quote_id = %q, want q_test_001", artifact.Cost.QuoteID)
	}
	if artifact.Cost.PricingVersion != 1 {
		t.Fatalf("pricing_version = %d, want 1", artifact.Cost.PricingVersion)
	}
	if artifact.Cost.Basis != "metered" {
		t.Fatalf("basis = %q, want metered", artifact.Cost.Basis)
	}
}

func TestWriteEvidenceIdentityFromReceipt(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	commitSHA := strings.Repeat("a", 40)

	_, err := writeEvidence(dir, res, "yt-id", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-id"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Provider != "linode" {
		t.Fatalf("provider = %q, want linode", artifact.Provider)
	}
	if artifact.Region != "us-east" {
		t.Fatalf("region = %q, want us-east", artifact.Region)
	}
	if artifact.SKU != "g2-gpu-rtx4000a1-s" {
		t.Fatalf("sku = %q, want g2-gpu-rtx4000a1-s", artifact.SKU)
	}
	if artifact.GPUProduct != "NVIDIA RTX 4000 Ada" {
		t.Fatalf("gpu_product = %q, want NVIDIA RTX 4000 Ada", artifact.GPUProduct)
	}
	if artifact.GPUCount != 1 {
		t.Fatalf("gpu_count = %d, want 1 (from receipt)", artifact.GPUCount)
	}
}

func TestWriteEvidenceNoInventoryOverclaim(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	commitSHA := strings.Repeat("a", 40)

	_, err := writeEvidence(dir, res, "yt-inv", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-inv"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Inventory.Complete {
		t.Fatal("inventory must not be marked complete without granular provider audit")
	}
}

func TestWriteEvidenceFailedArtifact(t *testing.T) {
	dir := t.TempDir()
	res := CaseResult{
		Case:  Case{Name: "flyio-cpu-nano", Path: "cases/flyio-cpu-nano.yaml"},
		Phase: "Timeout",
		Err:   "timed out",
	}

	commitSHA := strings.Repeat("b", 40)
	artifactResult, err := writeEvidence(dir, res, "yt-5678", commitSHA, evidenceObservations{})
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed", artifactResult)
	}

	path := filepath.Join(dir, sanitizeFilename("flyio-cpu-nano", "cases/flyio-cpu-nano.yaml", "yt-5678"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Result != evidence.ResultFailed {
		t.Fatalf("result = %q, want failed", artifact.Result)
	}
	if artifact.TerminalState != "Timeout" {
		t.Fatalf("terminal_state = %q, want Timeout", artifact.TerminalState)
	}
}

func TestWriteEvidenceIncompletePassedFallsBackToFailed(t *testing.T) {
	dir := t.TempDir()
	res := CaseResult{
		Case:   Case{Name: "incomplete-pass", Path: "cases/incomplete-pass.yaml"},
		Phase:  "Succeeded",
		Reaped: true,
	}

	commitSHA := strings.Repeat("b", 40)
	artifactResult, err := writeEvidence(dir, res, "yt-inc", commitSHA, evidenceObservations{})
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed", artifactResult)
	}

	path := filepath.Join(dir, sanitizeFilename("incomplete-pass", "cases/incomplete-pass.yaml", "yt-inc"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Result != evidence.ResultFailed {
		t.Fatalf("result = %q, want failed (incomplete evidence for a passed case)", artifact.Result)
	}
	hasDiag := false
	for _, d := range artifact.Diagnostics {
		if d.Code == "evidence.incomplete" {
			hasDiag = true
		}
	}
	if !hasDiag {
		t.Fatal("incomplete evidence should carry evidence.incomplete diagnostic")
	}
}

func TestWriteEvidenceArtifactResultSignalsDowngrade(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-sig", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed — fail-closed signal for incomplete artifact", artifactResult)
	}

	res.ArtifactResult = string(artifactResult)
	if casePassed(res) {
		t.Fatal("casePassed must return false when ArtifactResult is failed")
	}
}

func TestCasePassedUnchangedWhenEvidenceDisabled(t *testing.T) {
	res := CaseResult{
		Phase:  "Succeeded",
		Reaped: true,
	}
	if !casePassed(res) {
		t.Fatal("casePassed should return true when evidence is disabled (ArtifactResult empty)")
	}
}

func TestWriteEvidenceParallelSafety(t *testing.T) {
	dir := t.TempDir()
	commitSHA := strings.Repeat("c", 40)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			res := CaseResult{
				Case:  Case{Name: "parallel-case", Path: "cases/parallel-case.yaml"},
				Phase: "Error",
				Err:   "test",
			}
			runID := "yt-par-" + strings.Repeat("0", n)
			if _, err := writeEvidence(dir, res, runID, commitSHA, evidenceObservations{}); err != nil {
				t.Errorf("parallel write %d: %v", n, err)
			}
		}(i)
	}
	wg.Wait()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 8 {
		t.Fatalf("expected 8 unique files, got %d", len(entries))
	}
}

func TestWriteEvidenceParallelSameStemDifferentPaths(t *testing.T) {
	dir := t.TempDir()
	commitSHA := strings.Repeat("c", 40)
	paths := []string{"cases/linode/gpu-test.yaml", "cases/flyio/gpu-test.yaml"}

	var wg sync.WaitGroup
	for _, p := range paths {
		wg.Add(1)
		go func(casePath string) {
			defer wg.Done()
			res := CaseResult{
				Case:  Case{Name: "gpu-test", Path: casePath},
				Phase: "Error",
				Err:   "test",
			}
			if _, err := writeEvidence(dir, res, "yt-dup", commitSHA, evidenceObservations{}); err != nil {
				t.Errorf("write %s: %v", casePath, err)
			}
		}(p)
	}
	wg.Wait()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 unique files for same stem with different paths, got %d", len(entries))
	}
}

func TestWriteEvidenceCompleteInventoryPassesWhenSupplied(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-inv-pass", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultPassed {
		t.Fatalf("artifact result = %q, want passed (complete inventory supplied)", artifactResult)
	}

	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-inv-pass"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Result != evidence.ResultPassed {
		t.Fatalf("result = %q, want passed", artifact.Result)
	}
	if !artifact.Inventory.Complete {
		t.Fatal("inventory should be complete")
	}
}

// TestWriteEvidenceAdmittedFromReceiptIssuedAt replays the exact timestamps
// observed on yt-1787572018-f5a27e995365c9fd, where a real RTX 4000 Ada burst
// and clean 547ms reap failed evidence with "lifecycle timestamps are out of
// order" because Workload.CreatedAt landed 3.28ms after provider_created_at.
// The sealed placement receipt's issued_at is the authoritative admission
// observation and is strictly before both, so a passed artifact must use it
// and lifecycle ordering must hold without any clamping or reordering.
func TestWriteEvidenceAdmittedFromReceiptIssuedAt(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)

	issued := time.Date(2026, 8, 24, 11, 47, 1, 800_000_000, time.UTC)
	providerCreated := time.Date(2026, 8, 24, 11, 47, 1, 802_655_000, time.UTC)
	workloadCreated := time.Date(2026, 8, 24, 11, 47, 1, 805_930_377, time.UTC)
	nodeReady := time.Date(2026, 8, 24, 11, 48, 5, 0, time.UTC)
	started := time.Date(2026, 8, 24, 11, 48, 30, 0, time.UTC)
	finished := time.Date(2026, 8, 24, 11, 49, 0, 0, time.UTC)
	requested := time.Date(2026, 8, 24, 11, 49, 1, 0, time.UTC)
	deleted := time.Date(2026, 8, 24, 11, 49, 1, 547_000_000, time.UTC)

	obs.CentralEvidence.CreatedAt = &workloadCreated
	obs.CentralEvidence.StartedAt = &started
	obs.CentralEvidence.FinishedAt = &finished
	obs.CentralEvidence.Placement.Receipt.IssuedAt = issued
	obs.CentralEvidence.Cleanup.ProviderCreatedAt = &providerCreated
	obs.CentralEvidence.Cleanup.RequestedAt = &requested
	obs.CentralEvidence.Cleanup.DeletedAt = &deleted
	obs.NodeReadyAt = &nodeReady
	obs.GPUReadyAt = &nodeReady
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}

	commitSHA := strings.Repeat("a", 40)
	artifactResult, err := writeEvidence(dir, res, "yt-1787572018-f5a27e995365c9fd", commitSHA, obs)
	if err != nil {
		t.Fatalf("writeEvidence: %v", err)
	}
	if artifactResult != evidence.ResultPassed {
		t.Fatalf("artifact result = %q, want passed (issued_at should preserve ordering)", artifactResult)
	}

	path := filepath.Join(dir, sanitizeFilename(res.Case.Name, res.Case.Path, "yt-1787572018-f5a27e995365c9fd"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Timestamps.Admitted == nil || !artifact.Timestamps.Admitted.Equal(issued) {
		t.Fatalf("admitted = %v, want %v (from placement.receipt.issued_at)", artifact.Timestamps.Admitted, issued)
	}
	if artifact.Timestamps.ProviderCreated == nil || !artifact.Timestamps.ProviderCreated.Equal(providerCreated) {
		t.Fatalf("provider_created = %v, want %v (unmodified)", artifact.Timestamps.ProviderCreated, providerCreated)
	}
	if artifact.Timestamps.Admitted.After(*artifact.Timestamps.ProviderCreated) {
		t.Fatalf("admitted %v after provider_created %v — ordering not preserved",
			artifact.Timestamps.Admitted, artifact.Timestamps.ProviderCreated)
	}
}

// TestWriteEvidenceAdmittedFallsBackToCreatedAt proves the fallback path: on a
// workload evidence document without a sealed receipt (legacy or refused
// placement), the writer still records StageAdmitted from Workload.CreatedAt.
func TestWriteEvidenceAdmittedFallsBackToCreatedAt(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	obs.CentralEvidence.Placement.Receipt.IssuedAt = time.Time{}

	commitSHA := strings.Repeat("a", 40)
	if _, err := writeEvidence(dir, res, "yt-fallback", commitSHA, obs); err != nil {
		t.Fatalf("writeEvidence: %v", err)
	}
	path := filepath.Join(dir, sanitizeFilename(res.Case.Name, res.Case.Path, "yt-fallback"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Timestamps.Admitted == nil || !artifact.Timestamps.Admitted.Equal(*obs.CentralEvidence.CreatedAt) {
		t.Fatalf("admitted = %v, want %v (fallback to CreatedAt when issued_at is zero)",
			artifact.Timestamps.Admitted, obs.CentralEvidence.CreatedAt)
	}
}

// TestPlacementReceiptEvidenceDecodesIssuedAt proves the wire projection now
// carries placement.receipt.issued_at end-to-end from the workload detail
// response the central handler emits.
func TestPlacementReceiptEvidenceDecodesIssuedAt(t *testing.T) {
	issued := time.Date(2026, 8, 24, 11, 47, 1, 800_000_000, time.UTC)
	raw := []byte(`{"placement":{"receipt":{"selected":{"provider":"linode"},"quote_id":"q_x","issued_at":"2026-08-24T11:47:01.8Z","pricing_version":1}}}`)
	var ev WorkloadEvidence
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ev.Placement == nil || ev.Placement.Receipt == nil {
		t.Fatal("placement.receipt was not decoded")
	}
	if !ev.Placement.Receipt.IssuedAt.Equal(issued) {
		t.Fatalf("issued_at = %v, want %v", ev.Placement.Receipt.IssuedAt, issued)
	}
}

func TestWriteEvidenceNilInventoryStaysFailClosed(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-inv-nil", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed (nil inventory must not pass)", artifactResult)
	}
}

func TestWriteEvidenceNoWriteWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("precondition: dir should be empty")
	}
}

func TestWriteEvidenceRetainsOutcomeReceipt(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	objects := int64(4)
	bytesUploaded := int64(0)
	obs.CentralEvidence.Outcome = &OutcomeEvidence{
		Compute: ComputeOutcomeEvidence{Result: "succeeded"},
		Artifacts: &ArtifactOutcomeEvidence{
			Result:          "succeeded",
			ObjectsUploaded: &objects,
			BytesUploaded:   &bytesUploaded,
		},
	}
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-outcome", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultPassed {
		t.Fatalf("artifact result = %q, want passed", artifactResult)
	}

	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-outcome"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Outcome == nil {
		t.Fatal("outcome receipt was not retained")
	}
	if artifact.Outcome.Compute.Result != evidence.OutcomeResultSucceeded {
		t.Fatalf("outcome.compute.result = %q, want succeeded", artifact.Outcome.Compute.Result)
	}
	if artifact.Outcome.Artifacts == nil {
		t.Fatal("outcome.artifacts receipt was not retained")
	}
	if got := artifact.Outcome.Artifacts.ObjectsUploaded; got == nil || *got != 4 {
		t.Fatalf("objects_uploaded = %v, want 4", got)
	}
	if got := artifact.Outcome.Artifacts.BytesUploaded; got == nil || *got != 0 {
		t.Fatalf("bytes_uploaded = %v, want counted zero", got)
	}
}

func TestWriteEvidenceMissingOutcomeCannotPass(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	obs.CentralEvidence.Outcome = nil
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-noreceipt", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed (missing outcome cannot be fabricated)", artifactResult)
	}
}

func TestWriteEvidenceExcludesFreeFormReasonByConstruction(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	// Central's real response also carries a reason string; the client type
	// never declares one, so json.Unmarshal simply discards it upstream.
	obs.CentralEvidence.Outcome = &OutcomeEvidence{
		Compute: ComputeOutcomeEvidence{Result: "succeeded"},
	}
	commitSHA := strings.Repeat("a", 40)
	if _, err := writeEvidence(dir, res, "yt-redacted", commitSHA, obs); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-redacted"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "reason") {
		t.Fatal("retained artifact contains a reason field the schema forbids")
	}
}

func TestWriteEvidenceInvalidOutcomeResultFailsClosed(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	obs.CentralEvidence.Outcome = &OutcomeEvidence{
		Compute: ComputeOutcomeEvidence{Result: "unknown"},
	}
	commitSHA := strings.Repeat("a", 40)
	if _, err := writeEvidence(dir, res, "yt-bogus", commitSHA, obs); err == nil {
		t.Fatal("writeEvidence accepted an invalid outcome.compute.result")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid outcome must not have replaced any artifact on disk (got %d entries)", len(entries))
	}
}

func TestMapOutcomePreservesNilVersusCountedZero(t *testing.T) {
	zero := int64(0)
	source := &OutcomeEvidence{
		Compute: ComputeOutcomeEvidence{Result: "succeeded"},
		Artifacts: &ArtifactOutcomeEvidence{
			Result:          "succeeded",
			ObjectsUploaded: &zero,
			BytesUploaded:   nil,
		},
	}
	mapped := mapOutcome(source)
	if mapped == nil || mapped.Artifacts == nil {
		t.Fatal("mapped outcome lost its artifact receipt")
	}
	if mapped.Artifacts.BytesUploaded != nil {
		t.Fatal("mapOutcome fabricated a bytes_uploaded value from nil")
	}
	if mapped.Artifacts.ObjectsUploaded == nil {
		t.Fatal("mapOutcome dropped a counted-zero objects_uploaded")
	}
	if *mapped.Artifacts.ObjectsUploaded != 0 {
		t.Fatalf("mapped counted-zero = %d, want 0", *mapped.Artifacts.ObjectsUploaded)
	}
	// Prove the pointer is not shared with the source: mutating the source
	// after mapping must not touch the retained value.
	zero = 99
	if *mapped.Artifacts.ObjectsUploaded != 0 {
		t.Fatal("mapped outcome shares pointer with source; nil-vs-zero is unsafe")
	}
}

func TestWriteEvidenceRetainsObservedNodeReceipt(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-obs", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultPassed {
		t.Fatalf("artifact result = %q, want passed", artifactResult)
	}

	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-obs"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.ObservedNode == nil {
		t.Fatal("observed_node receipt was not retained on disk")
	}
	if artifact.ObservedNode.Name != "ys-burst-abc123def456" {
		t.Fatalf("observed_node.name = %q, want ys-burst-abc123def456", artifact.ObservedNode.Name)
	}
	if artifact.ObservedNode.GPUAllocatable != 1 {
		t.Fatalf("observed_node.gpu_allocatable = %d, want 1", artifact.ObservedNode.GPUAllocatable)
	}
	if !artifact.ObservedNode.GPUPresentLabel {
		t.Fatal("observed_node.gpu_present_label = false, want true")
	}
	if artifact.ObservedNode.GPUProductLabel != "NVIDIA-RTX-4000-Ada-Generation" {
		t.Fatalf("observed_node.gpu_product_label = %q, want hardware observation", artifact.ObservedNode.GPUProductLabel)
	}
	// JSON body must contain the structured receipt.
	body := string(raw)
	for _, want := range []string{`"observed_node"`, `"gpu_allocatable"`, `"gpu_present_label"`, `"gpu_product_label"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("artifact JSON is missing %q", want)
		}
	}
}

func TestWriteEvidenceRetainsAllAccessProbeDimensions(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-access", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultPassed {
		t.Fatalf("artifact result = %q, want passed", artifactResult)
	}

	path := filepath.Join(dir, sanitizeFilename(res.Case.Name, res.Case.Path, "yt-access"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.AccessProbe == nil {
		t.Fatal("access_probe receipt was not retained")
	}
	if artifact.AccessProbe.NodeName != artifact.ObservedNode.Name || !artifact.AccessProbe.Logs || !artifact.AccessProbe.Exec || !artifact.AccessProbe.PortForward || artifact.AccessProbe.ObservedAt.IsZero() {
		t.Fatalf("access_probe receipt is incomplete: %+v", artifact.AccessProbe)
	}
}

func TestWriteEvidenceProbeFailureRetainsBoundedReceiptAndDiagnostic(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	res.Err = "port-forward failed: token=secret response body=http://private"
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	obs.AccessProbe.PortForward = false
	obs.AccessProbeFailureStage = "access_probe.port_forward"
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-access-failed", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed", artifactResult)
	}

	path := filepath.Join(dir, sanitizeFilename(res.Case.Name, res.Case.Path, "yt-access-failed"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.AccessProbe == nil || !artifact.AccessProbe.Logs || !artifact.AccessProbe.Exec || artifact.AccessProbe.PortForward {
		t.Fatalf("failed artifact lost its partial access receipt: %+v", artifact.AccessProbe)
	}
	if len(artifact.Diagnostics) != 1 || artifact.Diagnostics[0].Code != "access_probe.failed" || artifact.Diagnostics[0].Stage != "access_probe.port_forward" {
		t.Fatalf("bounded access diagnostic = %+v", artifact.Diagnostics)
	}
	for _, forbidden := range []string{"token=secret", "response body", "http://private"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("artifact leaked probe payload %q", forbidden)
		}
	}
}

func TestWriteEvidenceMissingObservedNodeCannotPass(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	obs.ObservedNode = nil // runner never captured a valid observation
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-noobs", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed (missing observed_node cannot be fabricated)", artifactResult)
	}
}

func TestWriteEvidenceObservedLabelFalseCannotPass(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	obs.ObservedNode.GPUPresentLabel = false
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-labelfalse", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed when the observed label is false", artifactResult)
	}
}

func TestWriteEvidenceObservedProductMissingCannotPass(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	obs.ObservedNode.GPUProductLabel = ""
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-productmissing", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed when observed product is absent", artifactResult)
	}
}

func TestWriteEvidenceObservedAllocatableShortCannotPass(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	obs.ObservedNode.GPUAllocatable = 0
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-allocshort", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed when observed allocatable < requested gpu_count", artifactResult)
	}
}

func TestWriteEvidenceObservedNameEmptyIsRejected(t *testing.T) {
	// An empty observed_node.name is unbounded per the schema. The writer
	// must reject it in both the passed AND failed build paths so no
	// unbounded receipt ever reaches disk.
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	obs.Inventory = &evidence.Inventory{Complete: true, MeshProvider: evidence.MeshProviderTailscale}
	obs.ObservedNode.Name = ""
	commitSHA := strings.Repeat("a", 40)
	if _, err := writeEvidence(dir, res, "yt-nameempty", commitSHA, obs); err == nil {
		t.Fatal("writeEvidence accepted an unbounded observed_node.name")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unbounded receipt reached disk: %d entries", len(entries))
	}
}

func TestWriteEvidenceFailedRetainsPartialObservedNode(t *testing.T) {
	dir := t.TempDir()
	res, obs := completeEvidenceObs(t)
	// No inventory → the pipeline downgrades this artifact to failed. The
	// partial observed-node observation must be retained rather than dropped
	// so the operator sees what the runner did see.
	obs.ObservedNode = &evidence.ObservedNode{
		Name:            "ys-burst-abc123def456",
		GPUAllocatable:  1,
		GPUPresentLabel: false,
	}
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-failedpartial", commitSHA, obs)
	if err != nil {
		t.Fatal(err)
	}
	if artifactResult != evidence.ResultFailed {
		t.Fatalf("artifact result = %q, want failed", artifactResult)
	}

	path := filepath.Join(dir, sanitizeFilename("linode-gpu-rtx4000", "cases/linode-gpu-rtx4000.yaml", "yt-failedpartial"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.ObservedNode == nil {
		t.Fatal("failed artifact dropped a bounded partial observed_node")
	}
	if artifact.ObservedNode.GPUPresentLabel {
		t.Fatal("failed artifact fabricated GPUPresentLabel=true")
	}
}

func TestObservedNodeFromSnapshotIsNilForNilSnapshot(t *testing.T) {
	if got := observedNodeFromSnapshot(nil); got != nil {
		t.Fatalf("observedNodeFromSnapshot(nil) = %+v, want nil (never fabricated)", got)
	}
}

func TestObservedNodeFromSnapshotCopiesFieldsVerbatim(t *testing.T) {
	snap := &GPUNodeSnapshot{
		NodeName:        "ys-burst-abc123def456",
		GPUAllocatable:  2,
		GPUPresentLabel: true,
		GPUProductLabel: "NVIDIA-RTX-4000-Ada-Generation",
	}
	got := observedNodeFromSnapshot(snap)
	if got == nil {
		t.Fatal("expected a non-nil observed node")
	}
	if got.Name != snap.NodeName || got.GPUAllocatable != snap.GPUAllocatable ||
		got.GPUPresentLabel != snap.GPUPresentLabel || got.GPUProductLabel != snap.GPUProductLabel {
		t.Fatalf("observedNodeFromSnapshot = %+v, want %+v", got, snap)
	}
}

func TestWriteEvidenceNeverContainsDiagnosticText(t *testing.T) {
	dir := t.TempDir()
	res := CaseResult{
		Case:       Case{Name: "no-secrets", Path: "cases/no-secrets.yaml"},
		Phase:      "Failed",
		Err:        "super secret error: token=abc123",
		Diagnostic: "pod logs with secret content and provider payload",
	}

	commitSHA := strings.Repeat("d", 40)
	_, err := writeEvidence(dir, res, "yt-redact", commitSHA, evidenceObservations{})
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, sanitizeFilename("no-secrets", "cases/no-secrets.yaml", "yt-redact"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := string(raw)
	for _, secret := range []string{"super secret", "token=abc123", "pod logs", "provider payload"} {
		if strings.Contains(content, secret) {
			t.Fatalf("artifact contains redacted content: %q", secret)
		}
	}
}
