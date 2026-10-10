package launch

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ReadinessOptions configures one readiness evaluation.
type ReadinessOptions struct {
	// Root is the repository root against which local-file evidence paths
	// are resolved. Empty means the current working directory.
	Root string

	// AllowSynthetic permits a manifest marked "synthetic": true to pass.
	// This is a test-only escape hatch; the result is still marked
	// SYNTHETIC everywhere. Without it a synthetic manifest never passes.
	AllowSynthetic bool
}

// ReadinessReport is the deterministic outcome of a readiness evaluation.
type ReadinessReport struct {
	Ready            bool
	Synthetic        bool
	SyntheticAllowed bool
	ProvenGates      []string
	// Failures is the actionable list: one bounded line per missing,
	// pending, stale, or unverifiable item. Empty only when Ready.
	Failures []string
}

// EvaluateReadiness applies the fail-closed launch policy to a manifest.
// Every failure mode collapses to NOT READY with an actionable line:
// schema errors, missing or unproven gates or subchecks, stale or absent
// identity bindings, unverifiable or unconfined local evidence, incomplete
// release image pins, zero qualified cells, inconsistent cells, and
// unsanctioned synthetic manifests.
func EvaluateReadiness(m *Manifest, opts ReadinessOptions) ReadinessReport {
	report := ReadinessReport{}
	fail := func(format string, args ...any) {
		report.Failures = append(report.Failures, fmt.Sprintf(format, args...))
	}
	if m == nil {
		fail("manifest is nil")
		return report
	}
	report.Synthetic = m.Synthetic
	report.SyntheticAllowed = opts.AllowSynthetic

	for _, err := range ValidateSchema(m) {
		fail("schema: %s", err)
	}

	if m.Synthetic && !opts.AllowSynthetic {
		fail("manifest is marked synthetic: a synthetic manifest can never certify launch readiness (test-only -allow-synthetic not set)")
	}

	root := opts.Root
	if root == "" {
		root = "."
	}
	if !m.Synthetic {
		report.Failures = append(report.Failures, verifyGitIdentity(root, m.Release)...)
	}

	// Release identity coherence: every bound_commit must match
	// release.commit exactly. A stale or mismatched binding is a failure,
	// not a warning.
	for i := range m.Gates {
		gate := &m.Gates[i]
		if gate.BoundCommit != "" && gate.BoundCommit != m.Release.Commit {
			fail("gate %s: bound_commit %s does not match release.commit %s — stale or mismatched release identity",
				gate.ID, gate.BoundCommit, m.Release.Commit)
		}
	}
	for i := range m.SupportedCells {
		cell := &m.SupportedCells[i]
		if cell.BoundCommit != "" && cell.BoundCommit != m.Release.Commit {
			fail("cell %s/%s/%s: bound_commit %s does not match release.commit %s",
				cell.Provider, cell.Target, cell.Compute, cell.BoundCommit, m.Release.Commit)
		}
	}

	// The new release's image pins must be complete: exactly one
	// digest-pinned entry per required component. An empty or partial set
	// means there is no deployable release to be ready with.
	report.Failures = append(report.Failures, verifyReleaseImages(m.ReleaseImages)...)

	// Every gate in the closed set must be present, required, and proven
	// with verifiable evidence. Absence is failure; required=false is
	// failure; pending and failed are failure.
	for _, id := range RequiredGateIDs() {
		gate := m.GateByID(id)
		if gate == nil {
			fail("gate %s: absent from manifest — needs: the gate recorded with proven status and verifiable evidence (fail closed)", id)
			continue
		}
		if !gate.Required {
			fail("gate %s: marked required=false — every launch gate is required; waiving a gate is a failure", id)
			continue
		}
		subcheckFailures := evaluateSubchecks(root, m.Synthetic, gate)
		switch gate.Status {
		case GateStatusProven:
			var gateFailures []string
			if gate.BoundCommit == "" {
				gateFailures = append(gateFailures,
					fmt.Sprintf("gate %s: proven without bound_commit — needs: bound_commit equal to release.commit %s", id, m.Release.Commit))
			}
			if !m.Synthetic {
				gateFailures = append(gateFailures, fixtureEvidenceFailures(fmt.Sprintf("gate %s", id), gate.Evidence)...)
			}
			gateFailures = append(gateFailures, verifyProvenEvidence(root, fmt.Sprintf("gate %s", id), gate.Evidence)...)
			gateFailures = append(gateFailures, subcheckFailures...)
			if len(gateFailures) > 0 {
				report.Failures = append(report.Failures, gateFailures...)
				continue
			}
			report.ProvenGates = append(report.ProvenGates, id)
		case GateStatusPending:
			fail("gate %s: status=pending — needs: %s", id, missingText(gate.Missing))
			report.Failures = append(report.Failures, verifyLocalEvidence(root, fmt.Sprintf("gate %s (supporting evidence)", id), gate.Evidence)...)
			report.Failures = append(report.Failures, subcheckFailures...)
		case GateStatusFailed:
			fail("gate %s: status=failed — needs: %s", id, missingText(gate.Missing))
			report.Failures = append(report.Failures, subcheckFailures...)
		default:
			fail("gate %s: status %q is not provable", id, gate.Status)
		}
	}

	// Supported cells: local evidence must exist, and this-release
	// qualification is only coherent with a proven provider-qualification
	// gate (also flagged by schema; enforced again here). A launch with
	// zero qualified configurations cannot be ready.
	providerQualificationProven := false
	if gate := m.GateByID(GateProviderQualification); gate != nil && gate.Required && gate.Status == GateStatusProven {
		providerQualificationProven = true
	}
	qualifiedCells := 0
	for i := range m.SupportedCells {
		cell := &m.SupportedCells[i]
		label := fmt.Sprintf("cell %s/%s/%s", cell.Provider, cell.Target, cell.Compute)
		if !m.Synthetic {
			report.Failures = append(report.Failures, fixtureEvidenceFailures(label, cell.Evidence)...)
		}
		if cell.Status == CellQualifiedThisRelease {
			qualifiedCells++
			// The aggregate provider gate cannot substitute for proof of
			// each specific configuration advertised as qualified.
			report.Failures = append(report.Failures, verifyProvenEvidence(root, label, cell.Evidence)...)
			if !providerQualificationProven {
				fail("%s: claims %s while gate %s is unproven — a cell cannot claim this-release qualification without the gate",
					label, CellQualifiedThisRelease, GateProviderQualification)
			}
			if cell.BoundCommit != m.Release.Commit {
				fail("%s: %s evidence is not bound to release.commit %s",
					label, CellQualifiedThisRelease, m.Release.Commit)
			}
		} else {
			report.Failures = append(report.Failures, verifyLocalEvidence(root, label, cell.Evidence)...)
		}
	}
	if qualifiedCells == 0 {
		fail("supported_cells: no cell has status=%s — needs: at least one release-qualified configuration; a launch with zero qualified configurations cannot be ready",
			CellQualifiedThisRelease)
	}

	report.Ready = len(report.Failures) == 0
	return report
}

// fixtureEvidencePrefix marks artifacts that exist only to exercise this
// validator's own tests. They can never certify a real launch.
const fixtureEvidencePrefix = "internal/launch/testdata/"

// fixtureEvidenceFailures rejects local-file evidence that points into the
// validator's own test fixtures. Stripping "synthetic": true from a fixture
// must never produce a certifiable manifest.
func fixtureEvidenceFailures(label string, refs []EvidenceRef) []string {
	var errs []string
	for _, ref := range refs {
		if ref.Type == EvidenceLocalFile && strings.HasPrefix(ref.Path, fixtureEvidencePrefix) {
			errs = append(errs, fmt.Sprintf(
				"%s: local evidence %s is a validator test fixture — fixture artifacts can never prove a real launch",
				label, ref.Path))
		}
	}
	return errs
}

// evaluateSubchecks returns one actionable failure line per subcheck that is
// not independently proven with hashed local evidence. When a gate has
// subchecks, every one of them must pass before the gate can be proven.
func evaluateSubchecks(root string, synthetic bool, gate *Gate) []string {
	var failures []string
	seen := make(map[string]bool, len(gate.Subchecks))
	for _, subcheck := range gate.Subchecks {
		seen[subcheck.ID] = true
	}
	for _, required := range RequiredSubcheckIDs(gate.ID) {
		if !seen[required] {
			failures = append(failures, fmt.Sprintf("gate %s: required subcheck %s is absent; omitting evidence cannot waive this check", gate.ID, required))
		}
	}
	for i := range gate.Subchecks {
		subcheck := &gate.Subchecks[i]
		label := fmt.Sprintf("gate %s subcheck %s", gate.ID, subcheck.ID)
		switch subcheck.Status {
		case GateStatusProven:
			if !synthetic {
				failures = append(failures, fixtureEvidenceFailures(label, subcheck.Evidence)...)
			}
			failures = append(failures, verifyProvenEvidence(root, label, subcheck.Evidence)...)
		case GateStatusPending, GateStatusFailed:
			failures = append(failures, fmt.Sprintf("%s: status=%s — needs: %s", label, subcheck.Status, missingText(subcheck.Missing)))
			failures = append(failures, verifyLocalEvidence(root, label+" (supporting evidence)", subcheck.Evidence)...)
		default:
			failures = append(failures, fmt.Sprintf("%s: status %q is not provable", label, subcheck.Status))
		}
	}
	return failures
}

// verifyReleaseImages requires exactly one digest-pinned entry for every
// required component: no empty set, no duplicates, no unknown components.
func verifyReleaseImages(images []ComponentImage) []string {
	var errs []string
	required := RequiredImageComponents()
	if len(images) == 0 {
		errs = append(errs, fmt.Sprintf(
			"release_images: empty — needs: one digest-pinned release image for each of %s",
			strings.Join(required, ", ")))
		return errs
	}
	known := map[string]bool{}
	for _, component := range required {
		known[component] = true
	}
	counts := map[string]int{}
	for _, image := range images {
		counts[image.Component]++
		if !known[image.Component] {
			errs = append(errs, fmt.Sprintf(
				"release_images: unknown component %q — needs: only the closed component set %s",
				image.Component, strings.Join(required, ", ")))
		}
	}
	for _, component := range required {
		switch {
		case counts[component] == 0:
			errs = append(errs, fmt.Sprintf(
				"release_images: missing component %q — needs: its digest-pinned release image", component))
		case counts[component] > 1:
			errs = append(errs, fmt.Sprintf(
				"release_images: duplicate component %q — needs: exactly one digest-pinned image per component", component))
		}
	}
	return errs
}

func missingText(missing string) string {
	if text := strings.TrimSpace(missing); text != "" {
		return text
	}
	return "no missing-evidence description recorded; record what would prove this item"
}

// verifyProvenEvidence enforces the proof rule for a proven gate or subcheck:
// every local-file reference must verify (confined to root, existing, hash
// match when pinned), and at least one local-file reference with a pinned
// sha256 must fully verify. External references are supplements only and can
// never prove anything by themselves.
func verifyProvenEvidence(root, label string, refs []EvidenceRef) []string {
	errs := verifyLocalEvidence(root, label, refs)
	hashedProof := false
	if len(errs) == 0 {
		for _, ref := range refs {
			if ref.Type == EvidenceLocalFile && ref.SHA256 != "" {
				hashedProof = true
				break
			}
		}
	}
	if len(errs) == 0 && !hashedProof {
		errs = append(errs, fmt.Sprintf(
			"%s: proven without sha256-pinned local evidence — external references are supplements and can never prove a gate; needs: at least one local-file artifact with a matching sha256 checked into the repo",
			label))
	}
	return errs
}

// verifyLocalEvidence checks that every local-file evidence reference stays
// confined to the repository root (including through symlinks), resolves to
// an existing regular file, and matches its sha256 when one is pinned.
// External references are recorded locators and produce no local
// verification.
func verifyLocalEvidence(root, label string, refs []EvidenceRef) []string {
	var errs []string
	for _, ref := range refs {
		if ref.Type != EvidenceLocalFile {
			continue
		}
		resolved, err := resolveWithinRoot(root, ref.Path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, fmt.Sprintf("%s: local evidence %s does not exist under repo root — needs: the artifact checked in at that path", label, ref.Path))
			} else {
				errs = append(errs, fmt.Sprintf("%s: local evidence %s rejected: %v", label, ref.Path, err))
			}
			continue
		}
		info, err := os.Stat(resolved)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: local evidence %s does not exist under repo root — needs: the artifact checked in at that path", label, ref.Path))
			continue
		}
		if !info.Mode().IsRegular() {
			errs = append(errs, fmt.Sprintf("%s: local evidence %s is not a regular file", label, ref.Path))
			continue
		}
		if ref.SHA256 != "" {
			sum, err := fileSHA256(resolved)
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s: local evidence %s could not be hashed: %v", label, ref.Path, err))
				continue
			}
			if !strings.EqualFold(sum, ref.SHA256) {
				errs = append(errs, fmt.Sprintf("%s: local evidence %s sha256 mismatch (manifest %s, file %s) — evidence content drifted", label, ref.Path, strings.ToLower(ref.SHA256), sum))
			}
		}
	}
	return errs
}

// resolveWithinRoot canonicalizes root and the candidate path (following
// symlinks on both sides) and requires the canonical candidate to stay inside
// the canonical root. Absolute paths, dot-dot escapes, and symlinks that
// resolve outside the repository are all rejected.
func resolveWithinRoot(root, relPath string) (string, error) {
	if filepath.IsAbs(relPath) {
		return "", fmt.Errorf("absolute paths are not allowed")
	}
	cleaned := filepath.Clean(filepath.FromSlash(relPath))
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes the repo root")
	}
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("repo root cannot be resolved: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(filepath.Join(canonicalRoot, cleaned))
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(canonicalRoot, canonical)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path resolves outside the repo root")
	}
	return canonical, nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// Render formats the report for operators. Synthetic evaluations are loudly
// marked so a test-only pass can never read as a real launch certification.
// The exact "launch readiness: NOT READY (fail closed)" prefix is a stable
// contract consumed by the launch gate script; do not change it.
func (r ReadinessReport) Render() string {
	var b strings.Builder
	if r.Synthetic {
		b.WriteString("SYNTHETIC MANIFEST: test-only evaluation of a synthetic fixture; this output certifies nothing about the real launch.\n")
	}
	if r.Ready {
		fmt.Fprintf(&b, "launch readiness: READY — all %d required gates proven\n", len(r.ProvenGates))
		if r.Synthetic {
			b.WriteString("SYNTHETIC result — not a launch certification.\n")
		}
		return b.String()
	}
	fmt.Fprintf(&b, "launch readiness: NOT READY (fail closed) — %d unresolved item(s), %d/%d required gates proven\n",
		len(r.Failures), len(r.ProvenGates), len(RequiredGateIDs()))
	for _, failure := range r.Failures {
		b.WriteString("  ")
		b.WriteString(failure)
		b.WriteString("\n")
	}
	return b.String()
}
