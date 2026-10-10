package launch

import "testing"

func TestQualifiedCellRequiresHashedLocalEvidence(t *testing.T) {
	for _, name := range []string{"external-only", "unhashed-local"} {
		t.Run(name, func(t *testing.T) {
			manifest := loadFixture(t, "synthetic-ready.json")
			cell := &manifest.SupportedCells[0]
			if cell.Status != CellQualifiedThisRelease {
				t.Fatal("fixture must contain a qualified configuration")
			}
			proof := cell.Evidence[0]
			proof.SHA256 = ""
			if name == "external-only" {
				proof.Type = EvidenceExternal
				proof.Path = ""
				proof.Ref = "https://example.invalid/unverified"
			}
			cell.Evidence = []EvidenceRef{proof}
			mustSchemaValid(t, manifest, name)
			report := EvaluateReadiness(manifest, ReadinessOptions{Root: repoRoot(t), AllowSynthetic: true})
			requireFailureContaining(t, report, "proven without sha256-pinned local evidence")
		})
	}
}
