// Package launch defines the machine-readable release-evidence manifest for a
// launch candidate and the deterministic, fail-closed local validators over it.
//
// A manifest records a release identity, baseline pins, a closed set of launch
// gates, and the supported-configuration cells. Schema validity means only that
// the manifest is well-formed; it is explicitly NOT readiness. Readiness is
// fail-closed: every required gate must be proven with verifiable evidence, and
// anything missing, pending, stale, or unverifiable is a failure.
package launch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
)

// ManifestSchemaVersion is the only accepted manifest schema version.
const ManifestSchemaVersion = 1

// MaxManifestBytes bounds a manifest file so a validator never slurps an
// unbounded input.
const MaxManifestBytes = 1 << 20

// Gate identifiers form a closed set. Readiness requires every one of them to
// be present, required, and proven; an absent gate is a failure, never a skip.
const (
	GateReleaseCommit         = "release-commit"
	GatePostMergeCI           = "post-merge-ci"
	GateImageReceipts         = "image-receipts"
	GateOSSExport             = "oss-export"
	GateInfraRevision         = "infra-revision"
	GateRollbackRehearsal     = "rollback-rehearsal"
	GateRestoreRehearsal      = "restore-rehearsal"
	GateProviderQualification = "provider-qualification"
	GateTenantIsolation28     = "tenant-isolation-28"
	GateWorkloadOps38         = "workload-ops-38"
	GateCredential184         = "credential-184"
	GateCustomerPolicies      = "customer-policies"
)

// RequiredGateIDs returns the closed gate set in canonical order. Callers get
// a fresh slice so the canonical order cannot be mutated.
func RequiredGateIDs() []string {
	return []string{
		GateReleaseCommit,
		GatePostMergeCI,
		GateImageReceipts,
		GateOSSExport,
		GateInfraRevision,
		GateRollbackRehearsal,
		GateRestoreRehearsal,
		GateProviderQualification,
		GateTenantIsolation28,
		GateWorkloadOps38,
		GateCredential184,
		GateCustomerPolicies,
	}
}

// IsGateID reports whether id belongs to the closed gate set.
func IsGateID(id string) bool {
	for _, known := range RequiredGateIDs() {
		if id == known {
			return true
		}
	}
	return false
}

// RequiredSubcheckIDs prevents an abbreviated checklist from waiving the
// independently required trust and workload-operation checks.
func RequiredSubcheckIDs(gateID string) []string {
	switch gateID {
	case GateTenantIsolation28:
		return []string{"two-tenant-denial-live-mesh", "admission-cap-concurrency",
			"credential-scan-db-backup-log-image", "crash-cleanup-ambiguous-create-delete",
			"offboard-kill-switch-drill-scoped", "threat-model-rbac-review"}
	case GateWorkloadOps38:
		return []string{"logs-deployed-mesh", "exec-deployed-mesh", "port-forward-deployed-mesh"}
	default:
		return nil
	}
}

// Gate statuses form a closed set. Only "proven" can ever count toward
// readiness; "pending" and "failed" are both unready.
const (
	GateStatusProven  = "proven"
	GateStatusPending = "pending"
	GateStatusFailed  = "failed"
)

// Supported-cell axis values form closed sets. Provider and target are
// separate axes: aws-ec2→eks, aws-ec2→gke, and linode→lke are distinct cells.
const (
	ProviderFlyio  = "flyio"
	ProviderLinode = "linode"
	ProviderAWSEC2 = "aws-ec2"
	ProviderGCP    = "gcp"
	ProviderAzure  = "azure"

	TargetK3s = "k3s"
	TargetLKE = "lke"
	TargetEKS = "eks"
	TargetGKE = "gke"

	ComputeCPU = "cpu"
	ComputeGPU = "gpu"

	// NetworkingFull is the only supported networking mode. networking-lite
	// (or any other value) is not a supported configuration.
	NetworkingFull = "full"

	// Cell lifecycle values form a closed set. Qualification is specific:
	// a cold-destroy result never certifies warm reuse, which needs its own
	// evidence and budget.
	LifecycleColdDestroy = "cold-destroy"
	LifecycleWarmReuse   = "warm-reuse"

	CellQualifiedThisRelease = "qualified-this-release"
	CellHistoricallyTested   = "historically-tested"
	CellUnsupported          = "unsupported"
)

// RequiredImageComponents is the closed component set that release_images
// must cover exactly once each before readiness can pass.
func RequiredImageComponents() []string {
	return []string{"cloud", "cluster-agent", "hosted-controller"}
}

// Evidence reference types.
const (
	EvidenceLocalFile = "local-file"
	EvidenceExternal  = "external"
)

var (
	commit40Pattern  = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	sha256HexPattern = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)
	imageDigestSuffx = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// Manifest is the machine-readable release-evidence record for one launch
// candidate.
type Manifest struct {
	SchemaVersion int    `json:"schema_version"`
	ManifestID    string `json:"manifest_id"`
	GeneratedAt   string `json:"generated_at,omitempty"`

	// Synthetic marks a test-only manifest. A synthetic manifest can never
	// pass readiness unless the evaluator is explicitly told to allow it,
	// and even then the result is marked SYNTHETIC. It exists so the
	// fail-closed success path is testable without certifying a real launch.
	Synthetic bool `json:"synthetic,omitempty"`

	Release  Release   `json:"release"`
	Baseline *Baseline `json:"baseline,omitempty"`

	// ReleaseImages are the NEW release's digest pins as recorded from the
	// build pipelines. Recording a digest here is not receipt proof: the
	// image-receipts gate separately governs whether receipt evidence is
	// recorded in-tree, and readiness fails while that gate is unproven.
	ReleaseImages []ComponentImage `json:"release_images,omitempty"`

	Gates          []Gate          `json:"gates"`
	SupportedCells []SupportedCell `json:"supported_cells"`

	// MatrixNotes are bounded free-text lines rendered verbatim into the
	// generated supported-configuration matrix document.
	MatrixNotes []string `json:"matrix_notes,omitempty"`
}

// Release is the identity of the launch candidate. All three IDs are full
// 40-hex Git object IDs; anything else is malformed.
type Release struct {
	Commit     string `json:"commit"`
	TestedHead string `json:"tested_head"`
	Tree       string `json:"tree"`
}

// Baseline records what is currently deployed BEFORE this release rolls out.
// Baseline pins are never the new release's images.
type Baseline struct {
	DeployedImages []ComponentImage `json:"deployed_images,omitempty"`
	InfraRevision  string           `json:"infra_revision,omitempty"`
	AgentSourcePin string           `json:"agent_source_pin,omitempty"`
}

// ComponentImage is a digest-pinned image reference for a named component.
type ComponentImage struct {
	Component string `json:"component"`
	Ref       string `json:"ref"`
}

// Gate is one launch gate. A gate that is not proven must say, actionably,
// what evidence is missing.
type Gate struct {
	ID          string        `json:"id"`
	Required    bool          `json:"required"`
	Status      string        `json:"status"`
	BoundCommit string        `json:"bound_commit,omitempty"`
	Missing     string        `json:"missing,omitempty"`
	Evidence    []EvidenceRef `json:"evidence,omitempty"`

	// Subchecks are named component checks of a gate. When present, EVERY
	// subcheck must independently satisfy the proven rules before the gate
	// can count as proven; a single unproven subcheck fails the gate.
	Subchecks []Subcheck `json:"subchecks,omitempty"`
}

// Subcheck is one named component check inside a gate.
type Subcheck struct {
	ID       string        `json:"id"`
	Status   string        `json:"status"`
	Missing  string        `json:"missing,omitempty"`
	Evidence []EvidenceRef `json:"evidence,omitempty"`
}

// SupportedCell is one point in the supported-configuration matrix.
// Qualification is specific: a qualified-this-release cell must state its
// lifecycle, region, and instance type, because one result never certifies
// other regions, SKUs, or warm reuse. Historically-tested cells carry these
// fields only when the historical evidence actually states them.
type SupportedCell struct {
	Provider     string        `json:"provider"`
	Target       string        `json:"target"`
	Compute      string        `json:"compute"`
	Networking   string        `json:"networking"`
	Lifecycle    string        `json:"lifecycle,omitempty"`
	Region       string        `json:"region,omitempty"`
	InstanceType string        `json:"instance_type,omitempty"`
	Status       string        `json:"status"`
	BoundCommit  string        `json:"bound_commit,omitempty"`
	Evidence     []EvidenceRef `json:"evidence,omitempty"`
	Notes        string        `json:"notes,omitempty"`
}

// EvidenceRef points at one piece of evidence. A local-file reference must
// exist under the repository root when readiness runs; an optional sha256 pins
// its exact content. An external reference is a bounded recorded locator that
// the deterministic local validator cannot itself confirm.
type EvidenceRef struct {
	Type   string `json:"type"`
	Path   string `json:"path,omitempty"`
	Ref    string `json:"ref,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
	Note   string `json:"note,omitempty"`
}

// LoadManifest reads and strictly decodes a manifest file. Unknown fields,
// oversized inputs, and trailing data are all rejected so a manifest cannot
// smuggle unvalidated content past the schema.
func LoadManifest(path string) (*Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, MaxManifestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	if len(raw) > MaxManifestBytes {
		return nil, fmt.Errorf("manifest exceeds %d bytes", MaxManifestBytes)
	}
	return ParseManifest(raw)
}

// ParseManifest strictly decodes manifest bytes.
func ParseManifest(raw []byte) (*Manifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("manifest has trailing data after the JSON document")
	}
	return &manifest, nil
}

// GateByID returns the first gate with the given ID, or nil.
func (m *Manifest) GateByID(id string) *Gate {
	for i := range m.Gates {
		if m.Gates[i].ID == id {
			return &m.Gates[i]
		}
	}
	return nil
}
