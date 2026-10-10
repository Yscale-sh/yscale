package launch

import (
	"fmt"
	"path"
	"strings"

	"github.com/yscale-sh/yscale/internal/evidence"
)

// ValidateSchema checks manifest shape, identifiers, digest pins, closed-set
// membership, and internal consistency that is decidable without touching the
// filesystem. An empty return means the manifest is schema-valid.
//
// Schema validity is explicitly NOT readiness: a manifest full of pending
// gates is schema-valid and still completely unready.
func ValidateSchema(m *Manifest) []string {
	var errs []string
	report := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	if m == nil {
		return []string{"manifest is nil"}
	}
	if m.SchemaVersion != ManifestSchemaVersion {
		report("schema_version must be %d, got %d", ManifestSchemaVersion, m.SchemaVersion)
	}
	if err := evidence.ValidateBoundedMetadata("manifest_id", m.ManifestID); err != nil {
		report("%v", err)
	}
	if m.GeneratedAt != "" {
		if err := evidence.ValidateBoundedMetadata("generated_at", m.GeneratedAt); err != nil {
			report("%v", err)
		}
	}

	errs = append(errs, validateRelease(m.Release)...)
	if m.Baseline != nil {
		errs = append(errs, validateBaseline(m.Baseline)...)
	}
	for i, image := range m.ReleaseImages {
		errs = append(errs, validateComponentImage(fmt.Sprintf("release_images[%d]", i), image)...)
	}

	gateStatus := map[string]string{}
	seenGates := map[string]bool{}
	for i, gate := range m.Gates {
		prefix := fmt.Sprintf("gates[%d]", i)
		if !IsGateID(gate.ID) {
			report("%s: id %q is not in the closed gate set", prefix, gate.ID)
		} else if seenGates[gate.ID] {
			report("%s: duplicate gate id %q", prefix, gate.ID)
		} else {
			seenGates[gate.ID] = true
			gateStatus[gate.ID] = gate.Status
		}
		errs = append(errs, validateGate(prefix, gate)...)
	}

	seenCells := map[string]bool{}
	for i, cell := range m.SupportedCells {
		prefix := fmt.Sprintf("supported_cells[%d]", i)
		errs = append(errs, validateCell(prefix, cell, m.Release.Commit)...)
		key := strings.Join([]string{cell.Provider, cell.Target, cell.Compute, cell.Networking, cell.Lifecycle, cell.Region, cell.InstanceType}, "|")
		if seenCells[key] {
			report("%s: duplicate cell for provider=%s target=%s compute=%s",
				prefix, cell.Provider, cell.Target, cell.Compute)
		}
		seenCells[key] = true
		// Consistency: a cell cannot claim this-release qualification while
		// the provider-qualification gate is unproven. This is flagged here
		// in schema mode AND enforced again in readiness.
		if cell.Status == CellQualifiedThisRelease && gateStatus[GateProviderQualification] != GateStatusProven {
			report("%s: status %q requires gate %q to be proven (status is %q)",
				prefix, CellQualifiedThisRelease, GateProviderQualification,
				gateStatus[GateProviderQualification])
		}
	}

	for i, note := range m.MatrixNotes {
		if err := evidence.ValidateBoundedMetadata(fmt.Sprintf("matrix_notes[%d]", i), note); err != nil {
			report("%v", err)
		}
	}
	return errs
}

func validateRelease(release Release) []string {
	var errs []string
	for _, field := range []struct {
		name  string
		value string
	}{
		{"release.commit", release.Commit},
		{"release.tested_head", release.TestedHead},
		{"release.tree", release.Tree},
	} {
		if !commit40Pattern.MatchString(field.value) {
			errs = append(errs, fmt.Sprintf("%s must be a 40-hex Git object ID, got %q", field.name, field.value))
		}
	}
	return errs
}

func validateBaseline(baseline *Baseline) []string {
	var errs []string
	seen := map[string]bool{}
	for i, image := range baseline.DeployedImages {
		prefix := fmt.Sprintf("baseline.deployed_images[%d]", i)
		errs = append(errs, validateComponentImage(prefix, image)...)
		if seen[image.Component] {
			errs = append(errs, fmt.Sprintf("%s: duplicate component %q", prefix, image.Component))
		}
		seen[image.Component] = true
	}
	if baseline.InfraRevision != "" && !commit40Pattern.MatchString(baseline.InfraRevision) {
		errs = append(errs, fmt.Sprintf("baseline.infra_revision must be a 40-hex revision, got %q", baseline.InfraRevision))
	}
	if baseline.AgentSourcePin != "" && !commit40Pattern.MatchString(baseline.AgentSourcePin) {
		errs = append(errs, fmt.Sprintf("baseline.agent_source_pin must be a 40-hex revision, got %q", baseline.AgentSourcePin))
	}
	return errs
}

func validateComponentImage(prefix string, image ComponentImage) []string {
	var errs []string
	if err := evidence.ValidateBoundedMetadata(prefix+".component", image.Component); err != nil {
		errs = append(errs, err.Error())
	}
	errs = append(errs, validateImageRef(prefix+".ref", image.Ref)...)
	return errs
}

// validateImageRef enforces immutable digest pins: every image reference must
// carry an @sha256:<64-hex> digest, must never be a tag-only reference, and
// must never carry a mutable tag such as :latest at all.
func validateImageRef(field, ref string) []string {
	if err := evidence.ValidateBoundedMetadata(field, ref); err != nil {
		return []string{err.Error()}
	}
	parts := strings.Split(ref, "@")
	if len(parts) == 1 {
		if refHasTag(ref) {
			return []string{fmt.Sprintf("%s: %q is a mutable tag-only image reference; an @sha256: digest pin is required", field, ref)}
		}
		return []string{fmt.Sprintf("%s: %q has no @sha256: digest pin", field, ref)}
	}
	if len(parts) != 2 {
		return []string{fmt.Sprintf("%s: %q has multiple @ separators", field, ref)}
	}
	name, digest := parts[0], parts[1]
	var errs []string
	if !imageDigestSuffx.MatchString(digest) {
		errs = append(errs, fmt.Sprintf("%s: digest %q is not sha256:<64 lowercase hex>", field, digest))
	}
	if refHasTag(name) {
		errs = append(errs, fmt.Sprintf("%s: %q carries a mutable tag; pin by digest only", field, ref))
	}
	if strings.TrimSpace(name) == "" {
		errs = append(errs, fmt.Sprintf("%s: image name is empty", field))
	}
	return errs
}

// refHasTag reports whether the name portion of an image reference carries a
// tag. A colon in the final path segment is a tag; a colon before the first
// slash is a registry port, which is not a tag.
func refHasTag(name string) bool {
	lastSegment := name
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		lastSegment = name[idx+1:]
	}
	return strings.Contains(lastSegment, ":")
}

func validateGate(prefix string, gate Gate) []string {
	var errs []string
	switch gate.Status {
	case GateStatusProven:
		if len(gate.Evidence) == 0 {
			errs = append(errs, fmt.Sprintf("%s (%s): proven gate has no evidence", prefix, gate.ID))
		}
	case GateStatusPending, GateStatusFailed:
		if strings.TrimSpace(gate.Missing) == "" {
			errs = append(errs, fmt.Sprintf("%s (%s): unproven gate must carry an actionable missing description", prefix, gate.ID))
		}
	default:
		errs = append(errs, fmt.Sprintf("%s (%s): status %q is not in {proven, pending, failed}", prefix, gate.ID, gate.Status))
	}
	if gate.Missing != "" {
		if err := evidence.ValidateBoundedMetadata(prefix+".missing", gate.Missing); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if gate.BoundCommit != "" && !commit40Pattern.MatchString(gate.BoundCommit) {
		errs = append(errs, fmt.Sprintf("%s (%s): bound_commit must be a 40-hex Git object ID, got %q", prefix, gate.ID, gate.BoundCommit))
	}
	for i, ref := range gate.Evidence {
		errs = append(errs, validateEvidenceRef(fmt.Sprintf("%s.evidence[%d]", prefix, i), ref)...)
	}
	seenSubchecks := map[string]bool{}
	for i, subcheck := range gate.Subchecks {
		subPrefix := fmt.Sprintf("%s.subchecks[%d]", prefix, i)
		if err := evidence.ValidateBoundedMetadata(subPrefix+".id", subcheck.ID); err != nil {
			errs = append(errs, err.Error())
		} else if seenSubchecks[subcheck.ID] {
			errs = append(errs, fmt.Sprintf("%s: duplicate subcheck id %q", subPrefix, subcheck.ID))
		} else {
			seenSubchecks[subcheck.ID] = true
		}
		switch subcheck.Status {
		case GateStatusProven:
			if len(subcheck.Evidence) == 0 {
				errs = append(errs, fmt.Sprintf("%s (%s): proven subcheck has no evidence", subPrefix, subcheck.ID))
			}
		case GateStatusPending, GateStatusFailed:
			if strings.TrimSpace(subcheck.Missing) == "" {
				errs = append(errs, fmt.Sprintf("%s (%s): unproven subcheck must carry an actionable missing description", subPrefix, subcheck.ID))
			}
		default:
			errs = append(errs, fmt.Sprintf("%s (%s): status %q is not in {proven, pending, failed}", subPrefix, subcheck.ID, subcheck.Status))
		}
		if subcheck.Missing != "" {
			if err := evidence.ValidateBoundedMetadata(subPrefix+".missing", subcheck.Missing); err != nil {
				errs = append(errs, err.Error())
			}
		}
		for j, ref := range subcheck.Evidence {
			errs = append(errs, validateEvidenceRef(fmt.Sprintf("%s.evidence[%d]", subPrefix, j), ref)...)
		}
	}
	return errs
}

func validateCell(prefix string, cell SupportedCell, releaseCommit string) []string {
	var errs []string
	report := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}
	switch cell.Provider {
	case ProviderFlyio, ProviderLinode, ProviderAWSEC2, ProviderGCP, ProviderAzure:
	default:
		report("%s: provider %q is not in {flyio, linode, aws-ec2, gcp, azure}", prefix, cell.Provider)
	}
	switch cell.Target {
	case TargetK3s, TargetLKE, TargetEKS, TargetGKE:
	default:
		report("%s: target %q is not in {k3s, lke, eks, gke}", prefix, cell.Target)
	}
	switch cell.Compute {
	case ComputeCPU, ComputeGPU:
	default:
		report("%s: compute %q is not in {cpu, gpu}", prefix, cell.Compute)
	}
	if cell.Networking != NetworkingFull {
		report("%s: networking %q is not supported; only %q is (networking-lite is not a supported configuration)",
			prefix, cell.Networking, NetworkingFull)
	}
	switch cell.Lifecycle {
	case "", LifecycleColdDestroy, LifecycleWarmReuse:
	default:
		report("%s: lifecycle %q is not in {cold-destroy, warm-reuse}", prefix, cell.Lifecycle)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"region", cell.Region},
		{"instance_type", cell.InstanceType},
	} {
		if field.value != "" {
			if err := evidence.ValidateBoundedMetadata(prefix+"."+field.name, field.value); err != nil {
				report("%v", err)
			}
		}
	}
	switch cell.Status {
	case CellQualifiedThisRelease:
		if len(cell.Evidence) == 0 {
			report("%s: %s cell has no evidence", prefix, CellQualifiedThisRelease)
		}
		// Qualification is specific: one result never certifies other
		// regions, SKUs, or a different lifecycle.
		if cell.Lifecycle == "" {
			report("%s: %s cell must state its lifecycle (cold-destroy or warm-reuse)", prefix, CellQualifiedThisRelease)
		}
		if cell.Region == "" {
			report("%s: %s cell must state the exact region it was qualified in", prefix, CellQualifiedThisRelease)
		}
		if cell.InstanceType == "" {
			report("%s: %s cell must state the exact instance_type it was qualified on", prefix, CellQualifiedThisRelease)
		}
		if cell.BoundCommit == "" {
			report("%s: %s cell must set bound_commit to the release commit", prefix, CellQualifiedThisRelease)
		} else if !commit40Pattern.MatchString(cell.BoundCommit) {
			report("%s: bound_commit must be a 40-hex Git object ID, got %q", prefix, cell.BoundCommit)
		} else if cell.BoundCommit != releaseCommit {
			report("%s: %s cell bound_commit %q does not match release.commit %q",
				prefix, CellQualifiedThisRelease, cell.BoundCommit, releaseCommit)
		}
	case CellHistoricallyTested:
		if len(cell.Evidence) == 0 {
			report("%s: %s cell has no evidence", prefix, CellHistoricallyTested)
		}
		if cell.BoundCommit != "" && !commit40Pattern.MatchString(cell.BoundCommit) {
			report("%s: bound_commit must be a 40-hex Git object ID, got %q", prefix, cell.BoundCommit)
		}
	case CellUnsupported:
		if cell.BoundCommit != "" {
			report("%s: unsupported cell must not claim a bound_commit", prefix)
		}
	default:
		report("%s: status %q is not in {qualified-this-release, historically-tested, unsupported}", prefix, cell.Status)
	}
	if cell.Notes != "" {
		if err := evidence.ValidateBoundedMetadata(prefix+".notes", cell.Notes); err != nil {
			report("%v", err)
		}
	}
	for i, ref := range cell.Evidence {
		errs = append(errs, validateEvidenceRef(fmt.Sprintf("%s.evidence[%d]", prefix, i), ref)...)
	}
	return errs
}

func validateEvidenceRef(prefix string, ref EvidenceRef) []string {
	var errs []string
	report := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}
	switch ref.Type {
	case EvidenceLocalFile:
		if err := evidence.ValidateBoundedMetadata(prefix+".path", ref.Path); err != nil {
			report("%v", err)
			break
		}
		cleaned := path.Clean(ref.Path)
		if path.IsAbs(ref.Path) || cleaned != ref.Path || strings.HasPrefix(cleaned, "..") ||
			strings.Contains(ref.Path, "\\") {
			report("%s.path: %q must be a clean repo-root-relative path", prefix, ref.Path)
		}
		if ref.Ref != "" {
			report("%s: local-file evidence must not carry an external ref", prefix)
		}
	case EvidenceExternal:
		if err := evidence.ValidateBoundedMetadata(prefix+".ref", ref.Ref); err != nil {
			report("%v", err)
		}
		if ref.Path != "" {
			report("%s: external evidence must not carry a local path", prefix)
		}
		if ref.SHA256 != "" {
			report("%s: external evidence cannot pin a local sha256", prefix)
		}
	default:
		report("%s: type %q is not in {local-file, external}", prefix, ref.Type)
	}
	if ref.SHA256 != "" && !sha256HexPattern.MatchString(ref.SHA256) {
		report("%s.sha256: %q is not 64 hex characters", prefix, ref.SHA256)
	}
	if ref.Note != "" {
		if err := evidence.ValidateBoundedMetadata(prefix+".note", ref.Note); err != nil {
			report("%v", err)
		}
	}
	return errs
}
