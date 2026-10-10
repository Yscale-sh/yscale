package launch

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRequiredSubchecksCannotBeOmitted(t *testing.T) {
	root := repoRoot(t)
	for _, gateID := range []string{GateTenantIsolation28, GateWorkloadOps38} {
		t.Run(gateID, func(t *testing.T) {
			manifest, err := LoadManifest(filepath.Join(root, "internal/launch/testdata/synthetic-ready.json"))
			if err != nil {
				t.Fatal(err)
			}
			gate := manifest.GateByID(gateID)
			gate.Subchecks = nil
			report := EvaluateReadiness(manifest, ReadinessOptions{Root: root, AllowSynthetic: true})
			if report.Ready || !strings.Contains(strings.Join(report.Failures, "\n"), "required subcheck") {
				t.Fatalf("omitting all subchecks waived a gate: %+v", report)
			}
		})
	}
}

func TestConfigurationUniquenessIncludesRegionAndInstance(t *testing.T) {
	manifest, err := LoadManifest(filepath.Join(repoRoot(t), "internal/launch/testdata/synthetic-ready.json"))
	if err != nil {
		t.Fatal(err)
	}
	cell := manifest.SupportedCells[0]
	cell.Region = "alternate-test-region"
	manifest.SupportedCells = append(manifest.SupportedCells, cell)
	if errs := ValidateSchema(manifest); len(errs) != 0 {
		t.Fatalf("distinct region must be a distinct configuration: %v", errs)
	}
	manifest.SupportedCells = append(manifest.SupportedCells, cell)
	if errs := ValidateSchema(manifest); len(errs) == 0 {
		t.Fatal("exactly duplicated configuration was accepted")
	}
}
