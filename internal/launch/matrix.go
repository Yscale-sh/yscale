package launch

import (
	"fmt"
	"sort"
	"strings"
)

// GenerateMatrix renders the supported-configuration matrix document from a
// schema-valid manifest. Generation is deterministic: the same manifest and
// manifestPath always produce the same bytes, so a checked-in copy can be
// verified byte-for-byte (after whitespace normalization).
//
// manifestPath is embedded verbatim in the regeneration and readiness
// commands, so callers should pass the repo-root-relative path.
func GenerateMatrix(m *Manifest, manifestPath string) (string, error) {
	if errs := ValidateSchema(m); len(errs) > 0 {
		return "", fmt.Errorf("manifest is not schema-valid; refusing to generate a matrix from it: %s", errs[0])
	}

	cells := make([]SupportedCell, len(m.SupportedCells))
	copy(cells, m.SupportedCells)
	sort.SliceStable(cells, func(i, j int) bool {
		if a, b := cellStatusRank(cells[i].Status), cellStatusRank(cells[j].Status); a != b {
			return a < b
		}
		if cells[i].Provider != cells[j].Provider {
			return cells[i].Provider < cells[j].Provider
		}
		if cells[i].Target != cells[j].Target {
			return cells[i].Target < cells[j].Target
		}
		return cells[i].Compute < cells[j].Compute
	})

	var b strings.Builder
	fmt.Fprintf(&b, "# Supported configurations — %s\n\n", m.ManifestID)
	b.WriteString("<!-- GENERATED FILE: do not edit by hand. Edit the manifest and regenerate. -->\n\n")
	fmt.Fprintf(&b, "Generated from `%s` for release candidate commit\n`%s`\n(tested head `%s`,\ntree `%s`).\n\n",
		manifestPath, m.Release.Commit, m.Release.TestedHead, m.Release.Tree)
	b.WriteString("This is a machine-generated PRE-RELEASE evidence record: it restates the\n")
	b.WriteString("manifest's recorded gate and cell status and makes no claim beyond what that\n")
	b.WriteString("manifest proves. It is not a deployment, general-availability, or\n")
	b.WriteString("customer-readiness statement.\n\n")

	// Counts and claims below are derived from the manifest so this prose
	// can never silently contradict the tables it accompanies.
	qualified := filterCells(cells, CellQualifiedThisRelease)
	if len(qualified) == 0 {
		b.WriteString("Qualified-this-release configurations recorded in the manifest: none.\n\n")
	} else {
		fmt.Fprintf(&b, "Qualified-this-release configurations recorded in the manifest: %d (see the first table below).\n\n", len(qualified))
	}

	b.WriteString("## Exact missing launch proof\n\n")
	b.WriteString("The ONE command that lists exactly what proof is still missing for launch:\n\n")
	fmt.Fprintf(&b, "```\ngo run ./cmd/yscale-launch-readiness readiness -manifest %s\n```\n\n", manifestPath)

	writeSection(&b, "Qualified this release", qualified,
		"None. The manifest records zero qualified-this-release configurations.")
	writeSection(&b, "Historically tested (NOT qualified for this release)", filterCells(cells, CellHistoricallyTested),
		"None recorded.")
	writeSection(&b, "Unsupported", filterCells(cells, CellUnsupported),
		"None recorded.")

	if len(m.MatrixNotes) > 0 {
		b.WriteString("## Notes\n\n")
		for _, note := range m.MatrixNotes {
			fmt.Fprintf(&b, "- %s\n", note)
		}
		b.WriteString("\n")
	}

	fmt.Fprintf(&b, "Regenerate: `go run ./cmd/yscale-launch-readiness matrix -manifest %s`\n", manifestPath)
	fmt.Fprintf(&b, "Verify a checked-in copy: `go run ./cmd/yscale-launch-readiness matrix -manifest %s -verify -doc <path-to-this-file>`\n", manifestPath)
	return b.String(), nil
}

// VerifyMatrix checks that doc matches the deterministic generation from the
// manifest, after normalizing trailing whitespace. Any drift is an error that
// names the first differing line.
func VerifyMatrix(m *Manifest, manifestPath string, doc []byte) error {
	want, err := GenerateMatrix(m, manifestPath)
	if err != nil {
		return err
	}
	wantLines := normalizeLines(want)
	gotLines := normalizeLines(string(doc))
	limit := len(wantLines)
	if len(gotLines) < limit {
		limit = len(gotLines)
	}
	for i := 0; i < limit; i++ {
		if wantLines[i] != gotLines[i] {
			return fmt.Errorf("matrix drift at line %d: doc has %q, manifest generates %q", i+1, gotLines[i], wantLines[i])
		}
	}
	if len(gotLines) != len(wantLines) {
		return fmt.Errorf("matrix drift: doc has %d line(s), manifest generates %d line(s)", len(gotLines), len(wantLines))
	}
	return nil
}

func cellStatusRank(status string) int {
	switch status {
	case CellQualifiedThisRelease:
		return 0
	case CellHistoricallyTested:
		return 1
	default:
		return 2
	}
}

func filterCells(cells []SupportedCell, status string) []SupportedCell {
	var out []SupportedCell
	for _, cell := range cells {
		if cell.Status == status {
			out = append(out, cell)
		}
	}
	return out
}

func writeSection(b *strings.Builder, title string, cells []SupportedCell, emptyText string) {
	fmt.Fprintf(b, "## %s\n\n", title)
	if len(cells) == 0 {
		b.WriteString(emptyText)
		b.WriteString("\n\n")
		return
	}
	b.WriteString("| Provider | Target | Compute | Networking | Lifecycle | Region | Instance | Status | Evidence | Notes |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|\n")
	for _, cell := range cells {
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			escapeTableCell(cell.Provider),
			escapeTableCell(cell.Target),
			escapeTableCell(cell.Compute),
			escapeTableCell(cell.Networking),
			renderSpecificity(cell, cell.Lifecycle),
			renderSpecificity(cell, cell.Region),
			renderSpecificity(cell, cell.InstanceType),
			escapeTableCell(cell.Status),
			escapeTableCell(renderEvidence(cell.Evidence)),
			escapeTableCell(cell.Notes))
	}
	b.WriteString("\n")
}

// renderSpecificity renders a lifecycle/region/instance value. Historical
// evidence that never stated the value renders as unspecified rather than
// implying a fact the evidence does not contain.
func renderSpecificity(cell SupportedCell, value string) string {
	if value != "" {
		return escapeTableCell(value)
	}
	if cell.Status == CellHistoricallyTested {
		return "unspecified (historical)"
	}
	return "—"
}

func renderEvidence(refs []EvidenceRef) string {
	if len(refs) == 0 {
		return "—"
	}
	parts := make([]string, 0, len(refs))
	for _, ref := range refs {
		switch ref.Type {
		case EvidenceLocalFile:
			parts = append(parts, fmt.Sprintf("`%s`", ref.Path))
		case EvidenceExternal:
			parts = append(parts, ref.Ref)
		default:
			parts = append(parts, fmt.Sprintf("(unknown evidence type %q)", ref.Type))
		}
	}
	return strings.Join(parts, "; ")
}

func escapeTableCell(value string) string {
	if value == "" {
		return "—"
	}
	return strings.ReplaceAll(value, "|", "\\|")
}

func normalizeLines(text string) []string {
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " \t")
	}
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}
