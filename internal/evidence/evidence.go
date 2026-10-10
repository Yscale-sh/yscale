// Package evidence defines the retained GPU golden-path evidence contract.
package evidence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	SchemaVersion      = 7
	MaxArtifactBytes   = 64 << 10
	MaxDiagnostics     = 32
	maxIdentityBytes   = 256
	maxDiagnosticBytes = 64
)

// MeshProvider identifies which mesh coordination plane the inventory audit
// actually queried. A passed artifact carries an explicit value so it proves
// which plane was checked instead of leaving the reader to assume Tailscale
// SaaS.
const (
	MeshProviderTailscale = "tailscale"
	MeshProviderFabric    = "fabric"
)

// IsValidMeshProvider reports whether provider is one of the two coordination
// planes the evidence path can audit. A switch keeps the set closed: callers
// cannot mutate validation policy through an exported map.
func IsValidMeshProvider(provider string) bool {
	switch provider {
	case MeshProviderTailscale, MeshProviderFabric:
		return true
	default:
		return false
	}
}

type Result string

const (
	ResultPassed Result = "passed"
	ResultFailed Result = "failed"
)

// Outcome result strings, matching the stable strings central persists on a
// workload receipt. Kept as bare string constants so a retained artifact can be
// read against a legacy record without a version table.
const (
	OutcomeResultSucceeded = "succeeded"
	OutcomeResultFailed    = "failed"
	OutcomeResultSkipped   = "skipped"
)

// Artifact is the single evidence shape shared by deterministic and live tests.
// A field is populated only from an observation supplied by the caller.
type Artifact struct {
	SchemaVersion     int                 `json:"schema_version"`
	RunID             string              `json:"run_id"`
	CommitSHA         string              `json:"commit_sha"`
	Result            Result              `json:"result"`
	TerminalState     string              `json:"terminal_state"`
	Provider          string              `json:"provider"`
	TargetClusterType string              `json:"target_cluster_type"`
	Region            string              `json:"region"`
	SKU               string              `json:"sku"`
	GPUProduct        string              `json:"gpu_product"`
	GPUCount          int                 `json:"gpu_count"`
	Timestamps        LifecycleTimestamps `json:"timestamps"`
	Cost              Cost                `json:"cost"`
	Inventory         Inventory           `json:"inventory"`
	Outcome           *Outcome            `json:"outcome,omitempty"`
	ObservedNode      *ObservedNode       `json:"observed_node,omitempty"`
	AccessProbe       *AccessProbe        `json:"access_probe,omitempty"`
	Diagnostics       []Diagnostic        `json:"diagnostics"`
}

// ObservedNode is the retained receipt of the burst Node the workload actually
// ran on, captured before fast teardown can delete it. It is deliberately
// narrow — a bounded node name, the observed nvidia.com/gpu allocatable count,
// whether the agent's NodeWatcher had marked nvidia.com/gpu.present exactly
// "true", and the standard hardware-observed nvidia.com/gpu.product value — so
// a passed artifact carries independent proof distinct from central's placement
// receipt. Nil when no valid observation was made; a failed artifact may retain
// a partial observation but never fabricates one.
type ObservedNode struct {
	Name            string `json:"name"`
	GPUAllocatable  int64  `json:"gpu_allocatable"`
	GPUPresentLabel bool   `json:"gpu_present_label"`
	GPUProductLabel string `json:"gpu_product_label"`
}

// AccessProbe is the retained receipt for the Kubernetes apiserver-to-kubelet
// streaming surface exercised against the exact observed burst node. It keeps
// only bounded facts: no log body, exec output, HTTP response, token, URL,
// kubeconfig, or free-form error crosses into the artifact.
type AccessProbe struct {
	NodeName    string    `json:"node_name"`
	Logs        bool      `json:"logs"`
	Exec        bool      `json:"exec"`
	PortForward bool      `json:"port_forward"`
	ObservedAt  time.Time `json:"observed_at"`
}

// Outcome is the retained projection of central's stored run receipt: what the
// compute Job did, and separately what the artifact export did. It is
// deliberately narrow — bounded result strings, nullable counts, no free-form
// reason strings — so a passed artifact carries an INDEPENDENT proof of the
// terminal event rather than reasserting the workload's terminal_state.
type Outcome struct {
	Compute   ComputeOutcome   `json:"compute"`
	Artifacts *ArtifactOutcome `json:"artifacts,omitempty"`
}

// ComputeOutcome carries only the compute result the receipt claimed. The
// central response also renders a free-form reason string; it is intentionally
// absent here so the retained artifact cannot smuggle it in.
type ComputeOutcome struct {
	Result string `json:"result"`
}

// ArtifactOutcome mirrors the export receipt. Counts stay pointers so nil (the
// agent did not count) never collapses into a stored 0 (the agent counted
// zero). No reason field, by construction.
type ArtifactOutcome struct {
	Result          string `json:"result"`
	ObjectsUploaded *int64 `json:"objects_uploaded"`
	BytesUploaded   *int64 `json:"bytes_uploaded"`
}

// Pointer values distinguish an observed zero price from missing evidence.
type Cost struct {
	QuotedRateMicroUSDPerHour *int64 `json:"quoted_rate_micro_usd_per_hour"`
	MaximumMicroUSD           *int64 `json:"maximum_micro_usd"`
	TerminalMicroUSD          *int64 `json:"terminal_micro_usd"`
	QuoteID                   string `json:"quote_id"`
	PricingVersion            int    `json:"pricing_version"`
	Basis                     string `json:"basis"`
}

type LifecycleTimestamps struct {
	Admitted         *time.Time `json:"admitted"`
	ProviderCreated  *time.Time `json:"provider_created"`
	NodeReady        *time.Time `json:"node_ready"`
	GPUReady         *time.Time `json:"gpu_ready"`
	WorkloadStarted  *time.Time `json:"workload_started"`
	WorkloadFinished *time.Time `json:"workload_finished"`
	DeleteRequested  *time.Time `json:"delete_requested"`
	ProviderAbsent   *time.Time `json:"provider_absent"`
}

type Inventory struct {
	Complete                     bool `json:"complete"`
	ProviderResourcesRemaining   int  `json:"provider_resources_remaining"`
	KubernetesNodesRemaining     int  `json:"kubernetes_nodes_remaining"`
	MeshDevicesRemaining         int  `json:"mesh_devices_remaining"`
	PodCIDRReservationsRemaining int  `json:"pod_cidr_reservations_remaining"`
	AccountLeasesRemaining       int  `json:"account_leases_remaining"`
	// MeshProvider names the coordination plane the mesh audit actually
	// queried — either "tailscale" (the SaaS control plane) or "fabric"
	// (a per-customer self-hosted box). A passed artifact must carry a
	// value accepted by IsValidMeshProvider so a reader can prove which plane was
	// audited instead of assuming Tailscale SaaS. Optional (omitempty) so
	// failed/legacy artifacts without a provenance stamp are still writable.
	MeshProvider string `json:"mesh_provider,omitempty"`
}

// Diagnostic intentionally carries no free-form provider payload. Code and
// stage are bounded redacted identifiers suitable for operator correlation.
type Diagnostic struct {
	Code  string `json:"code"`
	Stage string `json:"stage"`
}

type Identity struct {
	Provider          string
	TargetClusterType string
	Region            string
	SKU               string
	GPUProduct        string
	GPUCount          int
}

type LifecycleStage string

const (
	StageAdmitted         LifecycleStage = "admitted"
	StageProviderCreated  LifecycleStage = "provider_created"
	StageNodeReady        LifecycleStage = "node_ready"
	StageGPUReady         LifecycleStage = "gpu_ready"
	StageWorkloadStarted  LifecycleStage = "workload_started"
	StageWorkloadFinished LifecycleStage = "workload_finished"
	StageDeleteRequested  LifecycleStage = "delete_requested"
	StageProviderAbsent   LifecycleStage = "provider_absent"
)

// Collector is a fail-closed input seam. It never invents identity, price,
// lifecycle, or inventory observations on behalf of a runner.
type Collector struct {
	artifact Artifact
}

func NewCollector(runID, commitSHA string) *Collector {
	return &Collector{artifact: Artifact{
		SchemaVersion: SchemaVersion,
		RunID:         runID,
		CommitSHA:     commitSHA,
		Diagnostics:   []Diagnostic{},
	}}
}

func (c *Collector) SetIdentity(identity Identity) {
	c.artifact.Provider = identity.Provider
	c.artifact.TargetClusterType = identity.TargetClusterType
	c.artifact.Region = identity.Region
	c.artifact.SKU = identity.SKU
	c.artifact.GPUProduct = identity.GPUProduct
	c.artifact.GPUCount = identity.GPUCount
}

func (c *Collector) Observe(stage LifecycleStage, observedAt time.Time) error {
	if observedAt.IsZero() {
		return fmt.Errorf("%s timestamp is zero", stage)
	}
	observedAt = observedAt.UTC()
	switch stage {
	case StageAdmitted:
		c.artifact.Timestamps.Admitted = &observedAt
	case StageProviderCreated:
		c.artifact.Timestamps.ProviderCreated = &observedAt
	case StageNodeReady:
		c.artifact.Timestamps.NodeReady = &observedAt
	case StageGPUReady:
		c.artifact.Timestamps.GPUReady = &observedAt
	case StageWorkloadStarted:
		c.artifact.Timestamps.WorkloadStarted = &observedAt
	case StageWorkloadFinished:
		c.artifact.Timestamps.WorkloadFinished = &observedAt
	case StageDeleteRequested:
		c.artifact.Timestamps.DeleteRequested = &observedAt
	case StageProviderAbsent:
		c.artifact.Timestamps.ProviderAbsent = &observedAt
	default:
		return fmt.Errorf("unknown lifecycle stage %q", stage)
	}
	return nil
}

func (c *Collector) SetCost(cost Cost) {
	c.artifact.Cost = cloneCost(cost)
}

func (c *Collector) SetInventory(inventory Inventory) {
	c.artifact.Inventory = inventory
}

func (c *Collector) SetTerminalState(state string) {
	c.artifact.TerminalState = state
}

// SetOutcome retains the receipt exactly as observed. A nil clears any prior
// receipt so a caller can only ever store what central actually returned. The
// counts are pointer-cloned so a caller mutating its input never mutates the
// stored receipt.
func (c *Collector) SetOutcome(outcome *Outcome) {
	c.artifact.Outcome = cloneOutcome(outcome)
}

// SetObservedNode retains the observed-node receipt exactly as captured. A nil
// clears any prior receipt so the artifact can only ever store what the runner
// actually observed. The struct is value-cloned so a caller mutating its input
// never mutates the stored receipt.
func (c *Collector) SetObservedNode(observed *ObservedNode) {
	c.artifact.ObservedNode = cloneObservedNode(observed)
}

// SetAccessProbe retains the bounded streaming-surface receipt exactly as
// observed. A nil clears a prior receipt; the value is cloned so caller
// mutation cannot alter a built artifact.
func (c *Collector) SetAccessProbe(probe *AccessProbe) {
	c.artifact.AccessProbe = cloneAccessProbe(probe)
}

func (c *Collector) AddDiagnostic(diagnostic Diagnostic) error {
	if err := validateDiagnostic(diagnostic); err != nil {
		return err
	}
	if len(c.artifact.Diagnostics) == MaxDiagnostics {
		return fmt.Errorf("diagnostics exceed maximum of %d", MaxDiagnostics)
	}
	c.artifact.Diagnostics = append(c.artifact.Diagnostics, diagnostic)
	return nil
}

func (c *Collector) Build(result Result) (Artifact, error) {
	artifact := cloneArtifact(c.artifact)
	artifact.Result = result
	if err := Validate(artifact); err != nil {
		return Artifact{}, err
	}
	return artifact, nil
}

var diagnosticToken = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)
var commitSHA = regexp.MustCompile(`^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$`)
var validTerminalStates = map[string]bool{
	"Succeeded": true,
	"Failed":    true,
	"Cancelled": true,
	"Timeout":   true,
	"Error":     true,
}

// Validate rejects any passed artifact that lacks authoritative observations.
func Validate(artifact Artifact) error {
	if artifact.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported schema_version %d", artifact.SchemaVersion)
	}
	if err := validateIdentity("run_id", artifact.RunID); err != nil {
		return err
	}
	if err := validateIdentity("commit_sha", artifact.CommitSHA); err != nil {
		return err
	}
	if !commitSHA.MatchString(artifact.CommitSHA) {
		return fmt.Errorf("commit_sha must be a full Git object ID")
	}
	if artifact.Result != ResultPassed && artifact.Result != ResultFailed {
		return fmt.Errorf("unsupported result %q", artifact.Result)
	}
	if artifact.TerminalState != "" && !validTerminalStates[artifact.TerminalState] {
		return fmt.Errorf("unsupported terminal_state %q", artifact.TerminalState)
	}
	if len(artifact.Diagnostics) > MaxDiagnostics {
		return fmt.Errorf("diagnostics exceed maximum of %d", MaxDiagnostics)
	}
	for i, diagnostic := range artifact.Diagnostics {
		if err := validateDiagnostic(diagnostic); err != nil {
			return fmt.Errorf("diagnostics[%d]: %w", i, err)
		}
	}
	if err := validateOptionalFields(artifact); err != nil {
		return err
	}
	if err := validateOutcome(artifact.Outcome); err != nil {
		return err
	}
	if err := validateObservedNode(artifact.ObservedNode); err != nil {
		return err
	}
	if err := validateAccessProbe(artifact.AccessProbe); err != nil {
		return err
	}
	if artifact.Result == ResultPassed {
		if err := validatePassed(artifact); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(artifact)
	if err != nil {
		return fmt.Errorf("marshal evidence: %w", err)
	}
	if len(raw) > MaxArtifactBytes {
		return fmt.Errorf("artifact size %d exceeds %d bytes", len(raw), MaxArtifactBytes)
	}
	return nil
}

func validatePassed(artifact Artifact) error {
	if artifact.TerminalState != "Succeeded" {
		return fmt.Errorf("passed evidence: terminal_state must be %q, got %q", "Succeeded", artifact.TerminalState)
	}
	identities := map[string]string{
		"provider":            artifact.Provider,
		"target_cluster_type": artifact.TargetClusterType,
		"region":              artifact.Region,
		"sku":                 artifact.SKU,
		"gpu_product":         artifact.GPUProduct,
	}
	for name, value := range identities {
		if err := validateIdentity(name, value); err != nil {
			return fmt.Errorf("passed evidence: %w", err)
		}
	}
	if artifact.GPUCount <= 0 {
		return fmt.Errorf("passed evidence: gpu_count must be positive")
	}
	for name, observedAt := range timestampFields(artifact.Timestamps) {
		if observedAt == nil || observedAt.IsZero() {
			return fmt.Errorf("passed evidence: timestamp %s was not observed", name)
		}
	}
	if artifact.Cost.QuotedRateMicroUSDPerHour == nil || artifact.Cost.MaximumMicroUSD == nil || artifact.Cost.TerminalMicroUSD == nil {
		return fmt.Errorf("passed evidence: quoted rate, maximum, and terminal cost must be observed")
	}
	if *artifact.Cost.TerminalMicroUSD > *artifact.Cost.MaximumMicroUSD {
		return fmt.Errorf("passed evidence: terminal_micro_usd exceeds maximum_micro_usd")
	}
	if err := validateIdentity("quote_id", artifact.Cost.QuoteID); err != nil {
		return fmt.Errorf("passed evidence: %w", err)
	}
	if artifact.Cost.PricingVersion <= 0 {
		return fmt.Errorf("passed evidence: pricing_version must be positive")
	}
	if err := validateIdentity("basis", artifact.Cost.Basis); err != nil {
		return fmt.Errorf("passed evidence: %w", err)
	}
	if !artifact.Inventory.Complete {
		return fmt.Errorf("passed evidence: inventory is incomplete")
	}
	if !IsValidMeshProvider(artifact.Inventory.MeshProvider) {
		return fmt.Errorf("passed evidence: inventory.mesh_provider must be %q or %q, got %q",
			MeshProviderTailscale, MeshProviderFabric, artifact.Inventory.MeshProvider)
	}
	counts := inventoryCounts(artifact.Inventory)
	for name, count := range counts {
		if count != 0 {
			return fmt.Errorf("passed evidence: %s is %d, want zero", name, count)
		}
	}
	if artifact.Outcome == nil {
		return fmt.Errorf("passed evidence: outcome receipt is required")
	}
	if artifact.Outcome.Compute.Result != OutcomeResultSucceeded {
		return fmt.Errorf("passed evidence: outcome.compute.result must be %q, got %q",
			OutcomeResultSucceeded, artifact.Outcome.Compute.Result)
	}
	if artifact.Outcome.Artifacts != nil && artifact.Outcome.Artifacts.Result != OutcomeResultSucceeded {
		return fmt.Errorf("passed evidence: outcome.artifacts.result must be %q when present, got %q",
			OutcomeResultSucceeded, artifact.Outcome.Artifacts.Result)
	}
	if artifact.ObservedNode == nil {
		return fmt.Errorf("passed evidence: observed_node receipt is required")
	}
	if err := validateIdentity("observed_node.name", artifact.ObservedNode.Name); err != nil {
		return fmt.Errorf("passed evidence: %w", err)
	}
	if !artifact.ObservedNode.GPUPresentLabel {
		return fmt.Errorf("passed evidence: observed_node.gpu_present_label must be true")
	}
	if err := validateIdentity("observed_node.gpu_product_label", artifact.ObservedNode.GPUProductLabel); err != nil {
		return fmt.Errorf("passed evidence: %w", err)
	}
	if artifact.ObservedNode.GPUAllocatable < int64(artifact.GPUCount) {
		return fmt.Errorf("passed evidence: observed_node.gpu_allocatable=%d, want >= gpu_count=%d",
			artifact.ObservedNode.GPUAllocatable, artifact.GPUCount)
	}
	if artifact.AccessProbe == nil {
		return fmt.Errorf("passed evidence: access_probe receipt is required")
	}
	if artifact.AccessProbe.NodeName != artifact.ObservedNode.Name {
		return fmt.Errorf("passed evidence: access_probe.node_name=%q does not match observed_node.name=%q",
			artifact.AccessProbe.NodeName, artifact.ObservedNode.Name)
	}
	for name, passed := range map[string]bool{
		"logs":         artifact.AccessProbe.Logs,
		"exec":         artifact.AccessProbe.Exec,
		"port_forward": artifact.AccessProbe.PortForward,
	} {
		if !passed {
			return fmt.Errorf("passed evidence: access_probe.%s must be true", name)
		}
	}
	return nil
}

// validateObservedNode enforces the bounded fields when a receipt is present.
// A nil receipt is a valid absence — passed artifacts require it separately.
// Failed artifacts may retain a partial receipt so long as it is not fabricated:
// a non-empty name must still meet the bounded-identity rules and the
// allocatable count must not be negative.
func validateObservedNode(observed *ObservedNode) error {
	if observed == nil {
		return nil
	}
	if err := validateIdentity("observed_node.name", observed.Name); err != nil {
		return err
	}
	if observed.GPUAllocatable < 0 {
		return fmt.Errorf("observed_node.gpu_allocatable must not be negative")
	}
	if observed.GPUProductLabel != "" {
		if err := validateIdentity("observed_node.gpu_product_label", observed.GPUProductLabel); err != nil {
			return err
		}
	}
	return nil
}

// validateAccessProbe accepts a failed artifact's partial receipt while
// keeping every retained field bounded and the single observation timestamp
// explicitly UTC. Passed artifacts impose the three-positive and exact-node
// requirements separately in validatePassed.
func validateAccessProbe(probe *AccessProbe) error {
	if probe == nil {
		return nil
	}
	if err := validateIdentity("access_probe.node_name", probe.NodeName); err != nil {
		return err
	}
	if probe.ObservedAt.IsZero() {
		return fmt.Errorf("access_probe.observed_at must not be zero")
	}
	if probe.ObservedAt.Location() != time.UTC {
		return fmt.Errorf("access_probe.observed_at must be UTC")
	}
	return nil
}

var validOutcomeComputeResults = map[string]bool{
	OutcomeResultSucceeded: true,
	OutcomeResultFailed:    true,
}

var validOutcomeArtifactResults = map[string]bool{
	OutcomeResultSucceeded: true,
	OutcomeResultFailed:    true,
	OutcomeResultSkipped:   true,
}

// validateOutcome enforces the bounded result strings and non-negative counts.
// It never invents a receipt: a nil outcome is a valid absence.
func validateOutcome(outcome *Outcome) error {
	if outcome == nil {
		return nil
	}
	if !validOutcomeComputeResults[outcome.Compute.Result] {
		return fmt.Errorf("outcome.compute.result must be %q or %q, got %q",
			OutcomeResultSucceeded, OutcomeResultFailed, outcome.Compute.Result)
	}
	if outcome.Artifacts != nil {
		if !validOutcomeArtifactResults[outcome.Artifacts.Result] {
			return fmt.Errorf("outcome.artifacts.result must be %q, %q, or %q, got %q",
				OutcomeResultSucceeded, OutcomeResultFailed, OutcomeResultSkipped,
				outcome.Artifacts.Result)
		}
		if outcome.Artifacts.ObjectsUploaded != nil && *outcome.Artifacts.ObjectsUploaded < 0 {
			return fmt.Errorf("outcome.artifacts.objects_uploaded must not be negative")
		}
		if outcome.Artifacts.BytesUploaded != nil && *outcome.Artifacts.BytesUploaded < 0 {
			return fmt.Errorf("outcome.artifacts.bytes_uploaded must not be negative")
		}
	}
	return nil
}

func validateOptionalFields(artifact Artifact) error {
	for name, value := range map[string]string{
		"provider":            artifact.Provider,
		"target_cluster_type": artifact.TargetClusterType,
		"region":              artifact.Region,
		"sku":                 artifact.SKU,
		"gpu_product":         artifact.GPUProduct,
	} {
		if value != "" {
			if err := validateIdentity(name, value); err != nil {
				return err
			}
		}
	}
	if artifact.GPUCount < 0 {
		return fmt.Errorf("gpu_count must not be negative")
	}
	for name, value := range map[string]*int64{
		"quoted_rate_micro_usd_per_hour": artifact.Cost.QuotedRateMicroUSDPerHour,
		"maximum_micro_usd":              artifact.Cost.MaximumMicroUSD,
		"terminal_micro_usd":             artifact.Cost.TerminalMicroUSD,
	} {
		if value != nil && *value < 0 {
			return fmt.Errorf("%s must not be negative", name)
		}
	}
	if artifact.Cost.QuoteID != "" {
		if err := validateIdentity("quote_id", artifact.Cost.QuoteID); err != nil {
			return err
		}
	}
	if artifact.Cost.Basis != "" {
		if err := validateIdentity("basis", artifact.Cost.Basis); err != nil {
			return err
		}
	}
	if artifact.Cost.PricingVersion < 0 {
		return fmt.Errorf("pricing_version must not be negative")
	}
	for name, count := range inventoryCounts(artifact.Inventory) {
		if count < 0 {
			return fmt.Errorf("%s must not be negative", name)
		}
	}
	if artifact.Inventory.MeshProvider != "" && !IsValidMeshProvider(artifact.Inventory.MeshProvider) {
		return fmt.Errorf("inventory.mesh_provider must be %q or %q, got %q",
			MeshProviderTailscale, MeshProviderFabric, artifact.Inventory.MeshProvider)
	}
	ordered := []*time.Time{
		artifact.Timestamps.Admitted,
		artifact.Timestamps.ProviderCreated,
		artifact.Timestamps.NodeReady,
		artifact.Timestamps.GPUReady,
		artifact.Timestamps.WorkloadStarted,
		artifact.Timestamps.WorkloadFinished,
		artifact.Timestamps.DeleteRequested,
		artifact.Timestamps.ProviderAbsent,
	}
	var previous *time.Time
	for _, observedAt := range ordered {
		if observedAt == nil {
			continue
		}
		if observedAt.IsZero() {
			return fmt.Errorf("lifecycle timestamp must not be zero")
		}
		if previous != nil && observedAt.Before(*previous) {
			return fmt.Errorf("lifecycle timestamps are out of order")
		}
		previous = observedAt
	}
	return nil
}

func validateIdentity(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if value != strings.TrimSpace(value) || len(value) > maxIdentityBytes || strings.ContainsAny(value, "\r\n\x00") {
		return fmt.Errorf("%s is unbounded", name)
	}
	return nil
}

func validateDiagnostic(diagnostic Diagnostic) error {
	for name, value := range map[string]string{"code": diagnostic.Code, "stage": diagnostic.Stage} {
		if len(value) == 0 || len(value) > maxDiagnosticBytes || !diagnosticToken.MatchString(value) {
			return fmt.Errorf("%s must be a bounded redacted identifier", name)
		}
	}
	return nil
}

func inventoryCounts(inventory Inventory) map[string]int {
	return map[string]int{
		"provider_resources_remaining":    inventory.ProviderResourcesRemaining,
		"kubernetes_nodes_remaining":      inventory.KubernetesNodesRemaining,
		"mesh_devices_remaining":          inventory.MeshDevicesRemaining,
		"pod_cidr_reservations_remaining": inventory.PodCIDRReservationsRemaining,
		"account_leases_remaining":        inventory.AccountLeasesRemaining,
	}
}

func timestampFields(timestamps LifecycleTimestamps) map[string]*time.Time {
	return map[string]*time.Time{
		"admitted":          timestamps.Admitted,
		"provider_created":  timestamps.ProviderCreated,
		"node_ready":        timestamps.NodeReady,
		"gpu_ready":         timestamps.GPUReady,
		"workload_started":  timestamps.WorkloadStarted,
		"workload_finished": timestamps.WorkloadFinished,
		"delete_requested":  timestamps.DeleteRequested,
		"provider_absent":   timestamps.ProviderAbsent,
	}
}

func cloneArtifact(artifact Artifact) Artifact {
	clone := artifact
	clone.Timestamps = cloneTimestamps(artifact.Timestamps)
	clone.Cost = cloneCost(artifact.Cost)
	clone.Outcome = cloneOutcome(artifact.Outcome)
	clone.ObservedNode = cloneObservedNode(artifact.ObservedNode)
	clone.AccessProbe = cloneAccessProbe(artifact.AccessProbe)
	clone.Diagnostics = append([]Diagnostic(nil), artifact.Diagnostics...)
	return clone
}

func cloneAccessProbe(probe *AccessProbe) *AccessProbe {
	if probe == nil {
		return nil
	}
	clone := *probe
	return &clone
}

// cloneObservedNode deep-copies the receipt so a caller mutating its input
// never mutates the stored value. Returns nil for nil so absence stays absence.
func cloneObservedNode(observed *ObservedNode) *ObservedNode {
	if observed == nil {
		return nil
	}
	clone := *observed
	return &clone
}

// cloneOutcome deep-copies the receipt so nil vs counted-zero counts survive
// the copy: a struct-only copy would share the pointer targets and a writer to
// the input would silently mutate the retained receipt.
func cloneOutcome(outcome *Outcome) *Outcome {
	if outcome == nil {
		return nil
	}
	clone := *outcome
	if outcome.Artifacts != nil {
		artifacts := *outcome.Artifacts
		artifacts.ObjectsUploaded = cloneInt64Pointer(outcome.Artifacts.ObjectsUploaded)
		artifacts.BytesUploaded = cloneInt64Pointer(outcome.Artifacts.BytesUploaded)
		clone.Artifacts = &artifacts
	}
	return &clone
}

func cloneInt64Pointer(n *int64) *int64 {
	if n == nil {
		return nil
	}
	v := *n
	return &v
}

func cloneTimestamps(timestamps LifecycleTimestamps) LifecycleTimestamps {
	clone := timestamps
	fields := []struct {
		source **time.Time
		target **time.Time
	}{
		{&timestamps.Admitted, &clone.Admitted},
		{&timestamps.ProviderCreated, &clone.ProviderCreated},
		{&timestamps.NodeReady, &clone.NodeReady},
		{&timestamps.GPUReady, &clone.GPUReady},
		{&timestamps.WorkloadStarted, &clone.WorkloadStarted},
		{&timestamps.WorkloadFinished, &clone.WorkloadFinished},
		{&timestamps.DeleteRequested, &clone.DeleteRequested},
		{&timestamps.ProviderAbsent, &clone.ProviderAbsent},
	}
	for _, field := range fields {
		if *field.source != nil {
			value := **field.source
			*field.target = &value
		}
	}
	return clone
}

func cloneCost(cost Cost) Cost {
	clone := cost
	if cost.QuotedRateMicroUSDPerHour != nil {
		value := *cost.QuotedRateMicroUSDPerHour
		clone.QuotedRateMicroUSDPerHour = &value
	}
	if cost.MaximumMicroUSD != nil {
		value := *cost.MaximumMicroUSD
		clone.MaximumMicroUSD = &value
	}
	if cost.TerminalMicroUSD != nil {
		value := *cost.TerminalMicroUSD
		clone.TerminalMicroUSD = &value
	}
	return clone
}

// ValidateBoundedMetadata checks that a value meets bounded identity
// constraints: non-empty, trimmed, within size limits, no control characters.
func ValidateBoundedMetadata(name, value string) error {
	return validateIdentity(name, value)
}

// WriteAtomic validates, bounds, and atomically replaces an artifact file.
func WriteAtomic(path string, artifact Artifact) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("evidence path is required")
	}
	if err := Validate(artifact); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal evidence: %w", err)
	}
	raw = append(raw, '\n')
	if len(raw) > MaxArtifactBytes {
		return fmt.Errorf("artifact size %d exceeds %d bytes", len(raw), MaxArtifactBytes)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create evidence directory: %w", err)
	}
	temporary, err := os.CreateTemp(dir, ".evidence-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary evidence: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("secure temporary evidence: %w", err)
	}
	if _, err := temporary.Write(raw); err != nil {
		temporary.Close()
		return fmt.Errorf("write temporary evidence: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync temporary evidence: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary evidence: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace evidence: %w", err)
	}
	return nil
}
