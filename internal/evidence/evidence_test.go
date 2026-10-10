package evidence

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCollectorBuildsGoldenPathContract(t *testing.T) {
	collector := completeCollector(t)
	artifact, err := collector.Build(ResultPassed)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	var shape map[string]any
	if err := json.Unmarshal(raw, &shape); err != nil {
		t.Fatal(err)
	}
	if got := shape["schema_version"]; got != float64(7) {
		t.Fatalf("schema_version = %v, want 7", got)
	}
	for _, field := range []string{
		"run_id", "commit_sha", "result", "terminal_state", "provider", "target_cluster_type",
		"region", "sku", "gpu_product", "gpu_count", "timestamps", "cost",
		"inventory", "outcome", "observed_node", "access_probe", "diagnostics",
	} {
		if _, ok := shape[field]; !ok {
			t.Errorf("artifact is missing %q", field)
		}
	}
	outcome, ok := shape["outcome"].(map[string]any)
	if !ok {
		t.Fatalf("outcome is not a JSON object: %T", shape["outcome"])
	}
	compute, ok := outcome["compute"].(map[string]any)
	if !ok {
		t.Fatalf("outcome.compute is not a JSON object: %T", outcome["compute"])
	}
	if compute["result"] != OutcomeResultSucceeded {
		t.Errorf("outcome.compute.result = %v, want %q", compute["result"], OutcomeResultSucceeded)
	}
	if _, ok := compute["reason"]; ok {
		t.Errorf("outcome.compute must not carry a reason field (retained by construction)")
	}
	if _, ok := outcome["artifacts"]; ok {
		t.Errorf("outcome.artifacts must be omitted when the receipt has none")
	}
	observed, ok := shape["observed_node"].(map[string]any)
	if !ok {
		t.Fatalf("observed_node is not a JSON object: %T", shape["observed_node"])
	}
	if observed["name"] != testObservedNodeName {
		t.Errorf("observed_node.name = %v, want %q", observed["name"], testObservedNodeName)
	}
	if observed["gpu_allocatable"] != float64(1) {
		t.Errorf("observed_node.gpu_allocatable = %v, want 1", observed["gpu_allocatable"])
	}
	if observed["gpu_present_label"] != true {
		t.Errorf("observed_node.gpu_present_label = %v, want true", observed["gpu_present_label"])
	}
	if observed["gpu_product_label"] != testObservedGPUProductLabel {
		t.Errorf("observed_node.gpu_product_label = %v, want %q", observed["gpu_product_label"], testObservedGPUProductLabel)
	}
	access, ok := shape["access_probe"].(map[string]any)
	if !ok {
		t.Fatalf("access_probe is not a JSON object: %T", shape["access_probe"])
	}
	if access["node_name"] != testObservedNodeName || access["logs"] != true || access["exec"] != true || access["port_forward"] != true {
		t.Errorf("access_probe did not retain the exact-node three-positive receipt: %+v", access)
	}
	inventory, ok := shape["inventory"].(map[string]any)
	if !ok {
		t.Fatalf("inventory is not a JSON object: %T", shape["inventory"])
	}
	if inventory["mesh_provider"] != MeshProviderTailscale {
		t.Errorf("inventory.mesh_provider = %v, want %q", inventory["mesh_provider"], MeshProviderTailscale)
	}
}

func TestCollectorCannotPassIncompleteEvidence(t *testing.T) {
	collector := NewCollector("gpu-e2e-incomplete", strings.Repeat("a", 40))
	if _, err := collector.Build(ResultPassed); err == nil {
		t.Fatal("incomplete evidence passed validation")
	}
	if _, err := collector.Build(ResultFailed); err != nil {
		t.Fatalf("failed evidence should retain partial observations: %v", err)
	}
}

func TestPassedEvidenceRequiresEveryObservationAndZeroLeak(t *testing.T) {
	base, err := completeCollector(t).Build(ResultPassed)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*Artifact){
		"terminal state":    func(a *Artifact) { a.TerminalState = "Failed" },
		"provider identity": func(a *Artifact) { a.Provider = "" },
		"cluster identity":  func(a *Artifact) { a.TargetClusterType = "" },
		"region identity":   func(a *Artifact) { a.Region = "" },
		"sku identity":      func(a *Artifact) { a.SKU = "" },
		"gpu identity":      func(a *Artifact) { a.GPUProduct = "" },
		"gpu count":         func(a *Artifact) { a.GPUCount = 0 },
		"admitted":          func(a *Artifact) { a.Timestamps.Admitted = nil },
		"provider created":  func(a *Artifact) { a.Timestamps.ProviderCreated = nil },
		"node ready":        func(a *Artifact) { a.Timestamps.NodeReady = nil },
		"gpu ready":         func(a *Artifact) { a.Timestamps.GPUReady = nil },
		"workload started":  func(a *Artifact) { a.Timestamps.WorkloadStarted = nil },
		"workload finished": func(a *Artifact) { a.Timestamps.WorkloadFinished = nil },
		"delete requested":  func(a *Artifact) { a.Timestamps.DeleteRequested = nil },
		"provider absent":   func(a *Artifact) { a.Timestamps.ProviderAbsent = nil },
		"quoted rate":       func(a *Artifact) { a.Cost.QuotedRateMicroUSDPerHour = nil },
		"maximum cost":      func(a *Artifact) { a.Cost.MaximumMicroUSD = nil },
		"terminal cost":     func(a *Artifact) { a.Cost.TerminalMicroUSD = nil },
		"quote_id":          func(a *Artifact) { a.Cost.QuoteID = "" },
		"pricing_version":   func(a *Artifact) { a.Cost.PricingVersion = 0 },
		"basis":             func(a *Artifact) { a.Cost.Basis = "" },
		"complete inventory": func(a *Artifact) {
			a.Inventory.Complete = false
		},
		"provider resource": func(a *Artifact) { a.Inventory.ProviderResourcesRemaining = 1 },
		"kubernetes node":   func(a *Artifact) { a.Inventory.KubernetesNodesRemaining = 1 },
		"mesh device":       func(a *Artifact) { a.Inventory.MeshDevicesRemaining = 1 },
		"mesh provider absent": func(a *Artifact) {
			a.Inventory.MeshProvider = ""
		},
		"mesh provider unknown": func(a *Artifact) {
			a.Inventory.MeshProvider = "netmaker"
		},
		"pod cidr":       func(a *Artifact) { a.Inventory.PodCIDRReservationsRemaining = 1 },
		"account lease":  func(a *Artifact) { a.Inventory.AccountLeasesRemaining = 1 },
		"outcome absent": func(a *Artifact) { a.Outcome = nil },
		"outcome compute failed": func(a *Artifact) {
			a.Outcome = &Outcome{Compute: ComputeOutcome{Result: OutcomeResultFailed}}
		},
		"outcome artifact failed": func(a *Artifact) {
			a.Outcome = &Outcome{
				Compute:   ComputeOutcome{Result: OutcomeResultSucceeded},
				Artifacts: &ArtifactOutcome{Result: OutcomeResultFailed},
			}
		},
		"outcome artifact skipped": func(a *Artifact) {
			a.Outcome = &Outcome{
				Compute:   ComputeOutcome{Result: OutcomeResultSucceeded},
				Artifacts: &ArtifactOutcome{Result: OutcomeResultSkipped},
			}
		},
		"observed node absent": func(a *Artifact) { a.ObservedNode = nil },
		"observed node name empty": func(a *Artifact) {
			a.ObservedNode = &ObservedNode{Name: "", GPUAllocatable: 1, GPUPresentLabel: true, GPUProductLabel: testObservedGPUProductLabel}
		},
		"observed node label false": func(a *Artifact) {
			a.ObservedNode = &ObservedNode{Name: testObservedNodeName, GPUAllocatable: 1, GPUPresentLabel: false, GPUProductLabel: testObservedGPUProductLabel}
		},
		"observed node allocatable short": func(a *Artifact) {
			a.ObservedNode = &ObservedNode{Name: testObservedNodeName, GPUAllocatable: 0, GPUPresentLabel: true, GPUProductLabel: testObservedGPUProductLabel}
		},
		"observed node product absent": func(a *Artifact) {
			a.ObservedNode.GPUProductLabel = ""
		},
		"access probe absent":       func(a *Artifact) { a.AccessProbe = nil },
		"access logs false":         func(a *Artifact) { a.AccessProbe.Logs = false },
		"access exec false":         func(a *Artifact) { a.AccessProbe.Exec = false },
		"access port-forward false": func(a *Artifact) { a.AccessProbe.PortForward = false },
		"access observed_at zero":   func(a *Artifact) { a.AccessProbe.ObservedAt = time.Time{} },
		"access node mismatch": func(a *Artifact) {
			a.AccessProbe.NodeName = "ys-burst-different"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			artifact := cloneArtifact(base)
			mutate(&artifact)
			if err := Validate(artifact); err == nil {
				t.Fatal("passed evidence validation unexpectedly succeeded")
			}
		})
	}
}

func TestFailedEvidenceRetainsBudgetOverrun(t *testing.T) {
	artifact, err := completeCollector(t).Build(ResultPassed)
	if err != nil {
		t.Fatal(err)
	}
	overrun := *artifact.Cost.MaximumMicroUSD + 1
	artifact.Cost.TerminalMicroUSD = &overrun
	artifact.Result = ResultFailed
	if err := Validate(artifact); err != nil {
		t.Fatalf("failed evidence rejected the observed budget overrun: %v", err)
	}
	artifact.Result = ResultPassed
	if err := Validate(artifact); err == nil {
		t.Fatal("passed evidence accepted a budget overrun")
	}
}

func TestDiagnosticsAreBoundedRedactedIdentifiers(t *testing.T) {
	collector := NewCollector("gpu-e2e-diagnostics", strings.Repeat("b", 40))
	for _, diagnostic := range []Diagnostic{
		{Code: "provider.inventory_unavailable", Stage: "cleanup"},
		{Code: "credential=secret", Stage: "cleanup"},
		{Code: "provider_payload", Stage: strings.Repeat("x", maxDiagnosticBytes+1)},
	} {
		err := collector.AddDiagnostic(diagnostic)
		if diagnostic.Code == "provider.inventory_unavailable" {
			if err != nil {
				t.Fatalf("safe diagnostic rejected: %v", err)
			}
		} else if err == nil {
			t.Fatalf("unsafe diagnostic %#v accepted", diagnostic)
		}
	}
	for len(collector.artifact.Diagnostics) < MaxDiagnostics {
		if err := collector.AddDiagnostic(Diagnostic{Code: "cleanup.retry", Stage: "cleanup"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := collector.AddDiagnostic(Diagnostic{Code: "cleanup.retry", Stage: "cleanup"}); err == nil {
		t.Fatal("diagnostic count was not bounded")
	}
}

func TestWriteAtomicIsBoundedAndDoesNotReplaceWithInvalidEvidence(t *testing.T) {
	artifact, err := completeCollector(t).Build(ResultPassed)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "nested", "evidence.json")
	if err := WriteAtomic(path, artifact); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("evidence permissions = %o, want 600", got)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > MaxArtifactBytes {
		t.Fatalf("artifact size = %d, want <= %d", len(raw), MaxArtifactBytes)
	}

	invalid := cloneArtifact(artifact)
	invalid.Inventory.ProviderResourcesRemaining = 1
	if err := WriteAtomic(path, invalid); err == nil {
		t.Fatal("invalid passed evidence was written")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(raw) {
		t.Fatal("invalid evidence replaced the prior artifact")
	}
}

func TestTerminalStateRequired(t *testing.T) {
	collector := completeCollector(t)
	collector.SetTerminalState("")
	if _, err := collector.Build(ResultPassed); err == nil {
		t.Fatal("passed evidence without terminal_state should fail")
	}
}

func TestTerminalStateInvalidRejected(t *testing.T) {
	collector := completeCollector(t)
	collector.SetTerminalState("Running")
	if _, err := collector.Build(ResultFailed); err == nil {
		t.Fatal("invalid terminal_state should be rejected")
	}
}

func TestTerminalStateFailedAccepted(t *testing.T) {
	collector := NewCollector("test-failed", strings.Repeat("e", 40))
	collector.SetTerminalState("Failed")
	if _, err := collector.Build(ResultFailed); err != nil {
		t.Fatalf("failed evidence with Failed terminal_state should be accepted: %v", err)
	}
}

func TestTerminalStateTimeoutAccepted(t *testing.T) {
	collector := NewCollector("test-timeout", strings.Repeat("f", 40))
	collector.SetTerminalState("Timeout")
	if _, err := collector.Build(ResultFailed); err != nil {
		t.Fatalf("failed evidence with Timeout terminal_state should be accepted: %v", err)
	}
}

func TestOutcomeInvalidComputeResultRejected(t *testing.T) {
	for _, bogus := range []string{"", "Succeeded", "SUCCEEDED", "running", "unknown", "canceled"} {
		bogus := bogus
		t.Run(bogus, func(t *testing.T) {
			collector := completeCollector(t)
			collector.SetOutcome(&Outcome{Compute: ComputeOutcome{Result: bogus}})
			if _, err := collector.Build(ResultFailed); err == nil {
				t.Fatalf("invalid outcome.compute.result %q was accepted", bogus)
			}
		})
	}
}

func TestOutcomeInvalidArtifactResultRejected(t *testing.T) {
	for _, bogus := range []string{"", "SUCCEEDED", "canceled", "Skipped"} {
		bogus := bogus
		t.Run(bogus, func(t *testing.T) {
			collector := completeCollector(t)
			collector.SetOutcome(&Outcome{
				Compute:   ComputeOutcome{Result: OutcomeResultSucceeded},
				Artifacts: &ArtifactOutcome{Result: bogus},
			})
			if _, err := collector.Build(ResultFailed); err == nil {
				t.Fatalf("invalid outcome.artifacts.result %q was accepted", bogus)
			}
		})
	}
}

func TestOutcomePassedRequiresComputeSucceeded(t *testing.T) {
	collector := completeCollector(t)
	collector.SetOutcome(nil)
	if _, err := collector.Build(ResultPassed); err == nil {
		t.Fatal("passed evidence without an outcome receipt should fail")
	}
}

func TestOutcomePassedRequiresArtifactSucceeded(t *testing.T) {
	collector := completeCollector(t)
	collector.SetOutcome(&Outcome{
		Compute: ComputeOutcome{Result: OutcomeResultSucceeded},
		Artifacts: &ArtifactOutcome{
			Result:          OutcomeResultFailed,
			ObjectsUploaded: int64Ptr(0),
			BytesUploaded:   int64Ptr(0),
		},
	})
	if _, err := collector.Build(ResultPassed); err == nil {
		t.Fatal("passed evidence with a failed artifact receipt should fail")
	}
}

func TestOutcomeArtifactSucceededWithCountedZeroAccepted(t *testing.T) {
	collector := completeCollector(t)
	collector.SetOutcome(&Outcome{
		Compute: ComputeOutcome{Result: OutcomeResultSucceeded},
		Artifacts: &ArtifactOutcome{
			Result:          OutcomeResultSucceeded,
			ObjectsUploaded: int64Ptr(0),
			BytesUploaded:   int64Ptr(0),
		},
	})
	artifact, err := collector.Build(ResultPassed)
	if err != nil {
		t.Fatalf("succeeded artifact with counted-zero uploads should pass: %v", err)
	}
	if artifact.Outcome == nil || artifact.Outcome.Artifacts == nil {
		t.Fatal("artifact outcome is nil")
	}
	if got := artifact.Outcome.Artifacts.ObjectsUploaded; got == nil || *got != 0 {
		t.Fatalf("objects_uploaded = %v, want counted zero", got)
	}
	if got := artifact.Outcome.Artifacts.BytesUploaded; got == nil || *got != 0 {
		t.Fatalf("bytes_uploaded = %v, want counted zero", got)
	}
}

func TestOutcomeCloningPreservesNilVersusCountedZero(t *testing.T) {
	source := &Outcome{
		Compute: ComputeOutcome{Result: OutcomeResultSucceeded},
		Artifacts: &ArtifactOutcome{
			Result:          OutcomeResultSucceeded,
			ObjectsUploaded: int64Ptr(0),
			BytesUploaded:   nil,
		},
	}
	clone := cloneOutcome(source)
	if clone == nil || clone.Artifacts == nil {
		t.Fatal("clone lost the artifact receipt")
	}
	if clone.Artifacts.BytesUploaded != nil {
		t.Fatal("nil bytes_uploaded was fabricated by cloning")
	}
	if clone.Artifacts.ObjectsUploaded == nil {
		t.Fatal("counted-zero objects_uploaded was dropped by cloning")
	}
	if *clone.Artifacts.ObjectsUploaded != 0 {
		t.Fatalf("counted-zero clone = %d, want 0", *clone.Artifacts.ObjectsUploaded)
	}
	// Mutating the source must not touch the clone — proves pointers were copied,
	// not shared.
	*source.Artifacts.ObjectsUploaded = 99
	if *clone.Artifacts.ObjectsUploaded != 0 {
		t.Fatal("clone shares pointer with source; nil-vs-zero invariant is unsafe")
	}
}

func int64Ptr(v int64) *int64 { return &v }

func TestObservedNodePassedRejectsMissingReceipt(t *testing.T) {
	collector := completeCollector(t)
	collector.SetObservedNode(nil)
	if _, err := collector.Build(ResultPassed); err == nil {
		t.Fatal("passed evidence without an observed_node receipt should fail")
	}
}

func TestObservedNodePassedRejectsInvalidName(t *testing.T) {
	for _, name := range []string{"", " ", "with\nnewline", strings.Repeat("x", maxIdentityBytes+1)} {
		name := name
		t.Run(name, func(t *testing.T) {
			collector := completeCollector(t)
			collector.SetObservedNode(&ObservedNode{
				Name:            name,
				GPUAllocatable:  1,
				GPUPresentLabel: true,
				GPUProductLabel: testObservedGPUProductLabel,
			})
			if _, err := collector.Build(ResultPassed); err == nil {
				t.Fatalf("passed evidence accepted invalid observed_node.name %q", name)
			}
		})
	}
}

func TestObservedNodePassedRejectsFalseLabel(t *testing.T) {
	collector := completeCollector(t)
	collector.SetObservedNode(&ObservedNode{
		Name:            testObservedNodeName,
		GPUAllocatable:  1,
		GPUPresentLabel: false,
		GPUProductLabel: testObservedGPUProductLabel,
	})
	if _, err := collector.Build(ResultPassed); err == nil {
		t.Fatal("passed evidence accepted gpu_present_label=false")
	}
}

func TestObservedNodePassedRejectsInsufficientAllocatable(t *testing.T) {
	collector := completeCollector(t)
	collector.SetObservedNode(&ObservedNode{
		Name:            testObservedNodeName,
		GPUAllocatable:  0,
		GPUPresentLabel: true,
		GPUProductLabel: testObservedGPUProductLabel,
	})
	if _, err := collector.Build(ResultPassed); err == nil {
		t.Fatal("passed evidence accepted observed allocatable < gpu_count")
	}
}

func TestObservedNodeFailedAllowsPartialReceipt(t *testing.T) {
	collector := completeCollector(t)
	collector.SetObservedNode(&ObservedNode{
		Name:            testObservedNodeName,
		GPUAllocatable:  0,
		GPUPresentLabel: false,
		GPUProductLabel: "",
	})
	if _, err := collector.Build(ResultFailed); err != nil {
		t.Fatalf("failed evidence rejected a bounded partial observation: %v", err)
	}
}

func TestObservedNodeFailedAllowsAbsentReceipt(t *testing.T) {
	collector := completeCollector(t)
	collector.SetObservedNode(nil)
	artifact, err := collector.Build(ResultFailed)
	if err != nil {
		t.Fatalf("failed evidence rejected an absent receipt: %v", err)
	}
	if artifact.ObservedNode != nil {
		t.Fatal("failed artifact fabricated an observed_node receipt")
	}
}

func TestObservedNodeFailedRejectsMalformed(t *testing.T) {
	collector := completeCollector(t)
	collector.SetObservedNode(&ObservedNode{
		Name:            "", // empty name is unbounded even for a partial receipt
		GPUAllocatable:  1,
		GPUPresentLabel: true,
		GPUProductLabel: testObservedGPUProductLabel,
	})
	if _, err := collector.Build(ResultFailed); err == nil {
		t.Fatal("failed evidence accepted an unbounded observed_node.name")
	}
	collector.SetObservedNode(&ObservedNode{
		Name:            testObservedNodeName,
		GPUAllocatable:  -1,
		GPUPresentLabel: true,
		GPUProductLabel: testObservedGPUProductLabel,
	})
	if _, err := collector.Build(ResultFailed); err == nil {
		t.Fatal("failed evidence accepted a negative gpu_allocatable")
	}
}

func TestObservedNodeSetIsIsolatedFromCaller(t *testing.T) {
	source := &ObservedNode{
		Name:            testObservedNodeName,
		GPUAllocatable:  1,
		GPUPresentLabel: true,
		GPUProductLabel: testObservedGPUProductLabel,
	}
	collector := completeCollector(t)
	collector.SetObservedNode(source)
	source.Name = "mutated-after-set"
	source.GPUAllocatable = 99
	source.GPUPresentLabel = false
	source.GPUProductLabel = "mutated-product"
	artifact, err := collector.Build(ResultPassed)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.ObservedNode == nil {
		t.Fatal("observed_node was lost during build")
	}
	if artifact.ObservedNode.Name != testObservedNodeName {
		t.Fatalf("observed_node.name was mutated after SetObservedNode: %q", artifact.ObservedNode.Name)
	}
	if artifact.ObservedNode.GPUAllocatable != 1 {
		t.Fatalf("observed_node.gpu_allocatable was mutated after SetObservedNode: %d", artifact.ObservedNode.GPUAllocatable)
	}
	if !artifact.ObservedNode.GPUPresentLabel {
		t.Fatal("observed_node.gpu_present_label was mutated after SetObservedNode")
	}
	if artifact.ObservedNode.GPUProductLabel != testObservedGPUProductLabel {
		t.Fatalf("observed_node.gpu_product_label was mutated after SetObservedNode: %q", artifact.ObservedNode.GPUProductLabel)
	}
}

func TestObservedNodeArtifactCloneIsIsolated(t *testing.T) {
	artifact, err := completeCollector(t).Build(ResultPassed)
	if err != nil {
		t.Fatal(err)
	}
	clone := cloneArtifact(artifact)
	clone.ObservedNode.Name = "mutated-clone"
	clone.ObservedNode.GPUAllocatable = 99
	clone.ObservedNode.GPUPresentLabel = false
	clone.ObservedNode.GPUProductLabel = "mutated-product"
	if artifact.ObservedNode.Name != testObservedNodeName {
		t.Fatal("mutating clone.observed_node.name touched the original")
	}
	if artifact.ObservedNode.GPUAllocatable != 1 {
		t.Fatal("mutating clone.observed_node.gpu_allocatable touched the original")
	}
	if !artifact.ObservedNode.GPUPresentLabel {
		t.Fatal("mutating clone.observed_node.gpu_present_label touched the original")
	}
	if artifact.ObservedNode.GPUProductLabel != testObservedGPUProductLabel {
		t.Fatal("mutating clone.observed_node.gpu_product_label touched the original")
	}
}

func TestAccessProbeFailedArtifactRetainsOnlyBoundedPartialReceipt(t *testing.T) {
	collector := NewCollector("gpu-e2e-partial", strings.Repeat("a", 40))
	observedAt := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	input := &AccessProbe{
		NodeName:   testObservedNodeName,
		Logs:       true,
		Exec:       false,
		ObservedAt: observedAt,
	}
	collector.SetAccessProbe(input)
	artifact, err := collector.Build(ResultFailed)
	if err != nil {
		t.Fatalf("failed artifact rejected bounded partial access receipt: %v", err)
	}
	if artifact.AccessProbe == nil || !artifact.AccessProbe.Logs || artifact.AccessProbe.Exec || artifact.AccessProbe.PortForward {
		t.Fatalf("partial receipt changed: %+v", artifact.AccessProbe)
	}
	input.NodeName = "mutated"
	if artifact.AccessProbe.NodeName != testObservedNodeName {
		t.Fatal("access receipt shares caller-owned storage")
	}
	raw, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"log body", "exec output", "response body", "token=", "kubeconfig", "http://", "error"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("access receipt leaked payload field %q", forbidden)
		}
	}
}

func TestAccessProbeRejectsNonUTCObservation(t *testing.T) {
	collector := NewCollector("gpu-e2e-partial", strings.Repeat("a", 40))
	collector.SetAccessProbe(&AccessProbe{
		NodeName:   testObservedNodeName,
		ObservedAt: time.Date(2026, 9, 1, 10, 0, 0, 0, time.FixedZone("offset", 3600)),
	})
	if _, err := collector.Build(ResultFailed); err == nil {
		t.Fatal("access receipt accepted a non-UTC observed_at")
	}
}

func TestTerminalStatePassedRequiresSucceeded(t *testing.T) {
	collector := completeCollector(t)
	collector.SetTerminalState("Failed")
	if _, err := collector.Build(ResultPassed); err == nil {
		t.Fatal("passed evidence with Failed terminal_state should fail")
	}
}

func completeCollector(t *testing.T) *Collector {
	t.Helper()
	collector := NewCollector("gpu-e2e-test", strings.Repeat("a", 40))
	collector.SetTerminalState("Succeeded")
	collector.SetIdentity(Identity{
		Provider:          "linode",
		TargetClusterType: "k3s",
		Region:            "us-east",
		SKU:               "g2-gpu-rtx4000a1-s",
		GPUProduct:        "RTX 4000 Ada",
		GPUCount:          1,
	})
	start := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	stages := []LifecycleStage{
		StageAdmitted,
		StageProviderCreated,
		StageNodeReady,
		StageGPUReady,
		StageWorkloadStarted,
		StageWorkloadFinished,
		StageDeleteRequested,
		StageProviderAbsent,
	}
	for i, stage := range stages {
		if err := collector.Observe(stage, start.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	quotedRate := int64(520000)
	maximum := int64(200000)
	terminal := int64(125000)
	collector.SetCost(Cost{
		QuotedRateMicroUSDPerHour: &quotedRate,
		MaximumMicroUSD:           &maximum,
		TerminalMicroUSD:          &terminal,
		QuoteID:                   "q_test_001",
		PricingVersion:            1,
		Basis:                     "metered",
	})
	collector.SetInventory(Inventory{Complete: true, MeshProvider: MeshProviderTailscale})
	collector.SetOutcome(&Outcome{Compute: ComputeOutcome{Result: OutcomeResultSucceeded}})
	collector.SetObservedNode(&ObservedNode{
		Name:            testObservedNodeName,
		GPUAllocatable:  1,
		GPUPresentLabel: true,
		GPUProductLabel: testObservedGPUProductLabel,
	})
	collector.SetAccessProbe(&AccessProbe{
		NodeName:    testObservedNodeName,
		Logs:        true,
		Exec:        true,
		PortForward: true,
		ObservedAt:  start.Add(3 * time.Minute),
	})
	if err := collector.AddDiagnostic(Diagnostic{Code: "cleanup.complete", Stage: "cleanup"}); err != nil {
		t.Fatal(err)
	}
	return collector
}

const (
	testObservedNodeName        = "ys-burst-abc123def456"
	testObservedGPUProductLabel = "NVIDIA-RTX-4000-Ada-Generation"
)
