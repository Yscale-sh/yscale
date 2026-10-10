package launch

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// repoRoot walks up from the test working directory to the module root so
// local-file evidence in fixtures and the real checked-in manifest resolve
// exactly as the CLI resolves them from the repository root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test working directory")
		}
		dir = parent
	}
}

func loadFixture(t *testing.T, name string) *Manifest {
	t.Helper()
	manifest, err := LoadManifest(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("load fixture %s: %v", name, err)
	}
	return manifest
}

func mustSchemaValid(t *testing.T, m *Manifest, name string) {
	t.Helper()
	if errs := ValidateSchema(m); len(errs) > 0 {
		t.Fatalf("%s: expected schema-valid, got: %s", name, strings.Join(errs, " | "))
	}
}

func requireFailureContaining(t *testing.T, report ReadinessReport, want string) {
	t.Helper()
	if report.Ready {
		t.Fatalf("expected NOT READY, got ready (fail-open)")
	}
	for _, failure := range report.Failures {
		if strings.Contains(failure, want) {
			return
		}
	}
	t.Fatalf("no failure contains %q; failures: %s", want, strings.Join(report.Failures, " | "))
}

func TestSyntheticFixtureIsSchemaValid(t *testing.T) {
	mustSchemaValid(t, loadFixture(t, "synthetic-ready.json"), "synthetic-ready")
}

func TestSyntheticManifestNeverPassesWithoutAllowSynthetic(t *testing.T) {
	report := EvaluateReadiness(loadFixture(t, "synthetic-ready.json"), ReadinessOptions{Root: repoRoot(t)})
	requireFailureContaining(t, report, "synthetic")
}

func TestSyntheticManifestPassesOnlyWithAllowSyntheticAndStaysMarked(t *testing.T) {
	report := EvaluateReadiness(loadFixture(t, "synthetic-ready.json"), ReadinessOptions{
		Root:           repoRoot(t),
		AllowSynthetic: true,
	})
	if !report.Ready {
		t.Fatalf("expected the valid synthetic fixture to pass with -allow-synthetic; failures: %s",
			strings.Join(report.Failures, " | "))
	}
	if len(report.ProvenGates) != len(RequiredGateIDs()) {
		t.Fatalf("proven gates = %d, want %d", len(report.ProvenGates), len(RequiredGateIDs()))
	}
	rendered := report.Render()
	if !strings.Contains(rendered, "SYNTHETIC") {
		t.Fatalf("synthetic pass must be clearly marked SYNTHETIC; got:\n%s", rendered)
	}
}

// Stripping "synthetic": true from a shape-valid fixture must not produce a
// certifiable manifest: fixture artifacts can never prove a real launch.
func TestFixtureEvidenceCannotProveRealManifest(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	manifest.Synthetic = false
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t)})
	requireFailureContaining(t, report, "validator test fixture")
	if len(report.ProvenGates) == len(RequiredGateIDs()) {
		t.Fatal("a de-synthesized fixture must not prove all gates")
	}
}

func TestMissingLocalArtifactFailsClosed(t *testing.T) {
	manifest := loadFixture(t, "missing-local-artifact.json")
	mustSchemaValid(t, manifest, "missing-local-artifact")
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "does not exist")
}

func TestEvidenceSHA256MismatchFailsClosed(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	manifest.Gates[0].Evidence[0].SHA256 = strings.Repeat("0", 64)
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "sha256 mismatch")
}

// External references are supplements: a gate whose only evidence is an
// external URL, or whose local evidence carries no pinned hash, is unproven.
func TestExternalOnlyOrUnhashedEvidenceCannotProveGate(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	manifest.Gates[0].Evidence = []EvidenceRef{{Type: EvidenceExternal, Ref: "https://example.test/run/1", Note: "external only"}}
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "can never prove a gate")

	manifest = loadFixture(t, "synthetic-ready.json")
	manifest.Gates[0].Evidence[0].SHA256 = ""
	report = EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "proven without sha256-pinned local evidence")
}

func TestProvenGateWithoutBoundCommitFails(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	manifest.Gates[0].BoundCommit = ""
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "proven without bound_commit")
}

func TestStaleBoundCommitFailsClosed(t *testing.T) {
	manifest := loadFixture(t, "stale-bound-commit.json")
	mustSchemaValid(t, manifest, "stale-bound-commit")
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "does not match release.commit")
}

func TestReleaseImagesMustBeCompleteAndUnique(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	manifest.ReleaseImages = nil
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "release_images: empty")

	manifest = loadFixture(t, "synthetic-ready.json")
	manifest.ReleaseImages = manifest.ReleaseImages[:2] // drop hosted-controller
	report = EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, `release_images: missing component "hosted-controller"`)

	manifest = loadFixture(t, "synthetic-ready.json")
	manifest.ReleaseImages = append(manifest.ReleaseImages, manifest.ReleaseImages[0])
	report = EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, `release_images: duplicate component "cloud"`)

	manifest = loadFixture(t, "synthetic-ready.json")
	manifest.ReleaseImages[0].Component = "sidecar"
	report = EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, `release_images: unknown component "sidecar"`)
}

func TestZeroQualifiedCellsCannotBeReady(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	manifest.SupportedCells = nil
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "zero qualified configurations cannot be ready")
}

func TestUnprovenSubcheckFailsProvenGate(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	gate := manifest.GateByID(GateTenantIsolation28)
	if gate == nil || len(gate.Subchecks) == 0 {
		t.Fatal("fixture must carry tenant-isolation-28 subchecks")
	}
	gate.Subchecks[0].Status = GateStatusPending
	gate.Subchecks[0].Missing = "live mesh denial run still missing"
	gate.Subchecks[0].Evidence = nil
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "gate tenant-isolation-28 subcheck two-tenant-denial-live-mesh: status=pending")
	for _, proven := range report.ProvenGates {
		if proven == GateTenantIsolation28 {
			t.Fatal("a gate with an unproven subcheck must not count as proven")
		}
	}
}

func TestQualifiedCellRequiresLifecycleRegionInstance(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	manifest.SupportedCells[0].Lifecycle = ""
	manifest.SupportedCells[0].Region = ""
	manifest.SupportedCells[0].InstanceType = ""
	joined := strings.Join(ValidateSchema(manifest), " | ")
	for _, want := range []string{"must state its lifecycle", "exact region", "exact instance_type"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("schema errors missing %q; got: %s", want, joined)
		}
	}
}

func TestDotDotEvidencePathRejectedBySchema(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	manifest.Gates[0].Evidence[0].Path = "../../../etc/passwd"
	joined := strings.Join(ValidateSchema(manifest), " | ")
	if !strings.Contains(joined, "clean repo-root-relative path") {
		t.Fatalf("expected dot-dot path rejection, got: %s", joined)
	}
}

// A symlink inside the root that resolves outside of it must be rejected even
// though the manifest path itself looks clean.
func TestSymlinkEscapeRejected(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("outside the repo"), 0o600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(secret, filepath.Join(root, "proof.txt")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	manifest := loadFixture(t, "synthetic-ready.json")
	manifest.Gates[0].Evidence = []EvidenceRef{{Type: EvidenceLocalFile, Path: "proof.txt", Note: "escaping symlink"}}
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: root, AllowSynthetic: true})
	requireFailureContaining(t, report, "outside the repo root")
}

func TestMalformedTreeFailsSchema(t *testing.T) {
	manifest := loadFixture(t, "malformed-tree.json")
	errs := ValidateSchema(manifest)
	joined := strings.Join(errs, " | ")
	if !strings.Contains(joined, "release.tree") {
		t.Fatalf("expected a release.tree schema error, got: %s", joined)
	}
}

func TestMutableImagePinsFailSchema(t *testing.T) {
	manifest := loadFixture(t, "mutable-image-tags.json")
	errs := ValidateSchema(manifest)
	joined := strings.Join(errs, " | ")
	for _, want := range []string{
		"cloud:latest",
		"agent:v1.2.3",
		"has no @sha256: digest pin",
		"mutable tag-only image reference",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("schema errors missing %q; got: %s", want, joined)
		}
	}
}

func TestImageRefValidationTable(t *testing.T) {
	cases := []struct {
		ref   string
		valid bool
	}{
		{"ghcr.io/o/repo@sha256:" + strings.Repeat("a", 64), true},
		{"ghcr.io/o/repo:latest@sha256:" + strings.Repeat("a", 64), false},
		{"ghcr.io/o/repo:latest", false},
		{"ghcr.io/o/repo:v1.0.0", false},
		{"ghcr.io/o/repo", false},
		{"ghcr.io/o/repo@sha256:" + strings.Repeat("A", 64), false},
		{"ghcr.io/o/repo@sha256:short", false},
		{"registry.example:5000/o/repo@sha256:" + strings.Repeat("b", 64), true},
	}
	for _, tc := range cases {
		errs := validateImageRef("ref", tc.ref)
		if tc.valid && len(errs) > 0 {
			t.Errorf("ref %q: expected valid, got %v", tc.ref, errs)
		}
		if !tc.valid && len(errs) == 0 {
			t.Errorf("ref %q: expected rejection, got none", tc.ref)
		}
	}
}

func TestPendingGatesAreListedExactly(t *testing.T) {
	manifest := loadFixture(t, "pending-gates.json")
	mustSchemaValid(t, manifest, "pending-gates")
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	if report.Ready {
		t.Fatal("expected NOT READY with pending gates")
	}
	pendingLine := regexp.MustCompile(`^gate ([a-z0-9-]+): status=pending`)
	got := map[string]bool{}
	for _, failure := range report.Failures {
		if match := pendingLine.FindStringSubmatch(failure); match != nil {
			got[match[1]] = true
			if !strings.Contains(failure, "needs:") {
				t.Errorf("pending failure line is not actionable (no needs:): %s", failure)
			}
		} else {
			t.Errorf("unexpected non-pending failure: %s", failure)
		}
	}
	want := map[string]bool{"post-merge-ci": true, "oss-export": true}
	if len(got) != len(want) {
		t.Fatalf("pending gates = %v, want %v", got, want)
	}
	for id := range want {
		if !got[id] {
			t.Fatalf("pending gate %s missing from failures: %v", id, report.Failures)
		}
	}
	if len(report.ProvenGates) != 11 {
		t.Fatalf("proven gates = %d, want 11", len(report.ProvenGates))
	}
}

func TestQualifiedCellWithPendingGateRejected(t *testing.T) {
	manifest := loadFixture(t, "unqualified-cell.json")
	errs := ValidateSchema(manifest)
	joined := strings.Join(errs, " | ")
	if !strings.Contains(joined, `requires gate "provider-qualification" to be proven`) {
		t.Fatalf("schema must flag the qualified-this-release/pending-gate inconsistency; got: %s", joined)
	}
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "cannot claim this-release qualification")
}

func TestNetworkingLiteRejected(t *testing.T) {
	manifest := loadFixture(t, "networking-lite.json")
	errs := ValidateSchema(manifest)
	joined := strings.Join(errs, " | ")
	if !strings.Contains(joined, `networking "lite" is not supported`) {
		t.Fatalf("expected networking-lite rejection, got: %s", joined)
	}
}

func TestAbsentAndWaivedGatesFailClosed(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	manifest.Gates = manifest.Gates[1:] // drop release-commit entirely
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "gate release-commit: absent from manifest")

	manifest = loadFixture(t, "synthetic-ready.json")
	manifest.Gates[0].Required = false
	report = EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	requireFailureContaining(t, report, "required=false")
}

// The real checked-in manifest must be schema-valid and must FAIL readiness:
// retained build receipts do not prove live launch gates. Drift between the
// docs and this policy is caught by reading the real file.
func TestRealManifestSchemaValidAndReadinessFails(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "docs", "release", "manifest.json")
	manifest, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("load real manifest: %v", err)
	}
	mustSchemaValid(t, manifest, "real manifest")
	if manifest.Synthetic {
		t.Fatal("the real manifest must never be marked synthetic")
	}
	if manifest.Release.Commit != strings.Repeat("0", 40) {
		t.Fatalf("unexpected release.commit %q", manifest.Release.Commit)
	}
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: root})
	if report.Ready {
		t.Fatal("the real manifest must FAIL readiness: live launch gates remain unproven")
	}
	proven := map[string]bool{}
	if len(report.ProvenGates) != len(proven) {
		t.Fatalf("public manifest must not import private proof: got proven gates %v", report.ProvenGates)
	}
	for _, id := range report.ProvenGates {
		if !proven[id] {
			t.Fatalf("unexpected proven gate: %s", id)
		}
	}
	failures := strings.Join(report.Failures, "\n")
	for _, id := range RequiredGateIDs() {
		if proven[id] {
			continue
		}
		if !strings.Contains(failures, "gate "+id+": status=pending") {
			t.Fatalf("real manifest readiness must list pending gate %s; failures:\n%s", id, failures)
		}
	}
	for _, want := range []string{
		"gate tenant-isolation-28 subcheck two-tenant-denial-live-mesh: status=pending",
		"gate workload-ops-38 subcheck port-forward-deployed-mesh: status=pending",
		"zero qualified configurations cannot be ready",
	} {
		if !strings.Contains(failures, want) {
			t.Fatalf("real manifest readiness must contain %q; failures:\n%s", want, failures)
		}
	}
}

func TestRealManifestHasNoQualifiedCells(t *testing.T) {
	root := repoRoot(t)
	manifest, err := LoadManifest(filepath.Join(root, "docs", "release", "manifest.json"))
	if err != nil {
		t.Fatalf("load real manifest: %v", err)
	}
	for _, cell := range manifest.SupportedCells {
		if cell.Status == CellQualifiedThisRelease {
			t.Fatalf("cell %s/%s/%s claims qualified-this-release; this release has zero qualified cells",
				cell.Provider, cell.Target, cell.Compute)
		}
		if cell.Lifecycle == LifecycleWarmReuse {
			t.Fatalf("cell %s/%s/%s claims warm-reuse; warm reuse needs its own qualification evidence",
				cell.Provider, cell.Target, cell.Compute)
		}
	}
}

func TestHistoricalCrossCloudEvidenceDoesNotQualifyAWSEKS(t *testing.T) {
	manifest, err := LoadManifest(filepath.Join(repoRoot(t), "internal/launch/testdata/historical-cross-cloud.json"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"aws-ec2/gke/cpu": CellHistoricallyTested,
		"linode/eks/cpu":  CellHistoricallyTested,
		"aws-ec2/eks/cpu": CellUnsupported,
	}
	for _, cell := range manifest.SupportedCells {
		key := cell.Provider + "/" + cell.Target + "/" + cell.Compute
		if status, ok := want[key]; ok {
			if cell.Status != status {
				t.Errorf("%s status=%s, want %s", key, cell.Status, status)
			}
			delete(want, key)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing explicit configurations: %v", want)
	}
}

func TestRealMatrixDocMatchesManifest(t *testing.T) {
	root := repoRoot(t)
	manifestRel := "docs/release/manifest.json"
	manifest, err := LoadManifest(filepath.Join(root, filepath.FromSlash(manifestRel)))
	if err != nil {
		t.Fatalf("load real manifest: %v", err)
	}
	doc, err := os.ReadFile(filepath.Join(root, "docs", "release", "supported-configurations.md"))
	if err != nil {
		t.Fatalf("read checked-in matrix doc: %v", err)
	}
	if err := VerifyMatrix(manifest, manifestRel, doc); err != nil {
		t.Fatalf("checked-in matrix doc drifted from the manifest: %v", err)
	}
	if err := VerifyMatrix(manifest, manifestRel, append([]byte("tampered\n"), doc...)); err == nil {
		t.Fatal("a tampered doc must fail verification")
	}
	text := string(doc)
	for _, want := range []string{
		"go run ./cmd/yscale-launch-readiness readiness -manifest docs/release/manifest.json",
		"PRE-RELEASE",
		"Qualified-this-release configurations recorded in the manifest: none.",
		"docs/operations.md",
		"docs/self-hosted-mesh.md",
		"All-zero Git IDs are unset placeholders",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("matrix doc missing required reference %q", want)
		}
	}
}

// The generated prose is derived from the manifest, so a manifest with
// qualified cells must state the count instead of contradicting its table.
func TestMatrixProseDerivesFromQualifiedCells(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	generated, err := GenerateMatrix(manifest, "testdata/synthetic-ready.json")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if !strings.Contains(generated, "Qualified-this-release configurations recorded in the manifest: 1") {
		t.Fatalf("matrix prose must state the derived qualified count; got:\n%s", generated)
	}
	if strings.Contains(generated, "recorded in the manifest: none") {
		t.Fatal("matrix prose contradicts the qualified table")
	}
}

func TestMatrixGenerationIsDeterministicAndSchemaGated(t *testing.T) {
	manifest := loadFixture(t, "synthetic-ready.json")
	first, err := GenerateMatrix(manifest, "testdata/synthetic-ready.json")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	second, err := GenerateMatrix(manifest, "testdata/synthetic-ready.json")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if first != second {
		t.Fatal("matrix generation is not deterministic")
	}
	if _, err := GenerateMatrix(loadFixture(t, "malformed-tree.json"), "testdata/malformed-tree.json"); err == nil {
		t.Fatal("matrix generation must refuse a schema-invalid manifest")
	}
}

func TestSchemaValidityIsNotReadiness(t *testing.T) {
	manifest := loadFixture(t, "pending-gates.json")
	mustSchemaValid(t, manifest, "pending-gates")
	report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
	if report.Ready {
		t.Fatal("schema-valid manifest with pending gates must not be ready")
	}
}
