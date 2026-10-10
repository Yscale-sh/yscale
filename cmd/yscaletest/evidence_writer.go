package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"time"

	"github.com/yscale-sh/yscale/internal/evidence"
)

var safeFilename = regexp.MustCompile(`[^a-zA-Z0-9._-]`)

func sanitizeFilename(caseName, casePath, runID string) string {
	name := safeFilename.ReplaceAllString(caseName, "_")
	id := safeFilename.ReplaceAllString(runID, "_")
	if len(name) > 128 {
		name = name[:128]
	}
	if len(id) > 64 {
		id = id[:64]
	}
	h := sha256.Sum256([]byte(casePath + "\x00" + caseName + "\x00" + runID))
	return name + "_" + id + "_" + hex.EncodeToString(h[:8]) + ".json"
}

type evidenceObservations struct {
	CentralEvidence   *WorkloadEvidence
	TargetClusterType string
	NodeReadyAt       *time.Time
	GPUReadyAt        *time.Time
	// Inventory is the optional authoritative inventory observation.
	// Production passes nil until a granular provider audit proves every
	// category; a focused test may supply a complete Inventory to prove
	// the observation chain can produce result:"passed". Do not derive
	// inventory from Reaped.
	Inventory *evidence.Inventory
	// ObservedNode is the retained snapshot of the burst Node the workload
	// actually ran on. Nil when the runner never captured a valid
	// observation (no exact burst node, no positive GPU allocatable,
	// gpu-not-ready taint present, or nvidia.com/gpu.present not exactly
	// "true"). The writer never fabricates one from centralEv or Inventory.
	ObservedNode *evidence.ObservedNode
	// AccessProbe is the bounded logs/exec/port-forward receipt captured
	// before terminal cleanup can remove the exact burst node. The failure
	// stage is a closed identifier; no operational error text is retained.
	AccessProbe             *evidence.AccessProbe
	AccessProbeFailureStage string
}

func writeEvidence(dir string, res CaseResult, runID, commitSHA string, obs evidenceObservations) (evidence.Result, error) {
	c := evidence.NewCollector(runID, commitSHA)

	c.SetTerminalState(res.Phase)

	centralEv := obs.CentralEvidence
	if centralEv != nil && centralEv.Placement != nil && centralEv.Placement.Receipt != nil {
		sel := centralEv.Placement.Receipt.Selected
		c.SetIdentity(evidence.Identity{
			Provider:          sel.Provider,
			TargetClusterType: obs.TargetClusterType,
			Region:            sel.Region,
			SKU:               sel.SKU,
			GPUProduct:        centralEv.Placement.GPUProduct,
			GPUCount:          sel.GPUCount,
		})
	}

	nodeReadyAt := obs.NodeReadyAt
	gpuReadyAt := obs.GPUReadyAt

	if centralEv != nil {
		// StageAdmitted is the quote-issuance moment recorded on the
		// placement receipt. Workload.CreatedAt is written AFTER central
		// returns from the provider create call, so on a fast path it can
		// land microseconds later than provider_created_at (observed live in
		// yt-1787572018-f5a27e995365c9fd). issued_at is sealed before both
		// and preserves lifecycle ordering without clamping or reordering.
		if centralEv.Placement != nil && centralEv.Placement.Receipt != nil && !centralEv.Placement.Receipt.IssuedAt.IsZero() {
			_ = c.Observe(evidence.StageAdmitted, centralEv.Placement.Receipt.IssuedAt)
		} else if centralEv.CreatedAt != nil {
			_ = c.Observe(evidence.StageAdmitted, *centralEv.CreatedAt)
		}
		if centralEv.Cleanup != nil && centralEv.Cleanup.ProviderCreatedAt != nil {
			_ = c.Observe(evidence.StageProviderCreated, *centralEv.Cleanup.ProviderCreatedAt)
		}
		if nodeReadyAt != nil {
			_ = c.Observe(evidence.StageNodeReady, *nodeReadyAt)
		}
		if gpuReadyAt != nil {
			_ = c.Observe(evidence.StageGPUReady, *gpuReadyAt)
		}
		if centralEv.StartedAt != nil {
			_ = c.Observe(evidence.StageWorkloadStarted, *centralEv.StartedAt)
		}
		if centralEv.FinishedAt != nil {
			_ = c.Observe(evidence.StageWorkloadFinished, *centralEv.FinishedAt)
		}
		if centralEv.Cleanup != nil {
			if centralEv.Cleanup.RequestedAt != nil {
				_ = c.Observe(evidence.StageDeleteRequested, *centralEv.Cleanup.RequestedAt)
			}
			if centralEv.Cleanup.DeletedAt != nil {
				_ = c.Observe(evidence.StageProviderAbsent, *centralEv.Cleanup.DeletedAt)
			}
		}

		if centralEv.Placement != nil && centralEv.Placement.Receipt != nil {
			sel := centralEv.Placement.Receipt.Selected
			hourly := sel.HourlyMicroUSD
			maximum := sel.MaximumChargeMicroUSD
			cost := evidence.Cost{
				QuotedRateMicroUSDPerHour: &hourly,
				MaximumMicroUSD:           &maximum,
				QuoteID:                   centralEv.Placement.Receipt.QuoteID,
				PricingVersion:            centralEv.Placement.Receipt.PricingVersion,
			}
			if centralEv.Cost != nil {
				terminalMicro := int64(math.Round(centralEv.Cost.USD * 1_000_000))
				cost.TerminalMicroUSD = &terminalMicro
				cost.Basis = centralEv.Cost.Basis
			}
			c.SetCost(cost)
		}

		if centralEv.Outcome != nil {
			c.SetOutcome(mapOutcome(centralEv.Outcome))
		}
	} else {
		if nodeReadyAt != nil {
			_ = c.Observe(evidence.StageNodeReady, *nodeReadyAt)
		}
		if gpuReadyAt != nil {
			_ = c.Observe(evidence.StageGPUReady, *gpuReadyAt)
		}
	}

	if obs.Inventory != nil {
		c.SetInventory(*obs.Inventory)
	}

	if obs.ObservedNode != nil {
		c.SetObservedNode(obs.ObservedNode)
	}
	if obs.AccessProbe != nil {
		c.SetAccessProbe(obs.AccessProbe)
	}
	if obs.AccessProbeFailureStage != "" {
		_ = c.AddDiagnostic(evidence.Diagnostic{
			Code:  "access_probe.failed",
			Stage: obs.AccessProbeFailureStage,
		})
	}

	result := evidence.ResultPassed
	if !casePassed(res) {
		result = evidence.ResultFailed
	}

	artifact, err := c.Build(result)
	if err != nil {
		_ = c.AddDiagnostic(evidence.Diagnostic{Code: "evidence.incomplete", Stage: "build"})
		artifact, err = c.Build(evidence.ResultFailed)
		if err != nil {
			return evidence.ResultFailed, fmt.Errorf("build failed evidence: %w", err)
		}
	}

	path := filepath.Join(dir, sanitizeFilename(res.Case.Name, res.Case.Path, runID))
	if err := evidence.WriteAtomic(path, artifact); err != nil {
		return evidence.ResultFailed, err
	}
	return artifact.Result, nil
}

// mapOutcome projects central's response into the retained receipt. Only the
// bounded result strings and the nullable counts cross the boundary — the
// free-form reason strings central renders alongside them are NEVER forwarded,
// and the counts stay pointer-copied so nil (uncounted) and 0 (counted zero)
// remain distinguishable through the artifact.
func mapOutcome(source *OutcomeEvidence) *evidence.Outcome {
	if source == nil {
		return nil
	}
	out := &evidence.Outcome{
		Compute: evidence.ComputeOutcome{Result: source.Compute.Result},
	}
	if source.Artifacts != nil {
		out.Artifacts = &evidence.ArtifactOutcome{
			Result:          source.Artifacts.Result,
			ObjectsUploaded: copyInt64Pointer(source.Artifacts.ObjectsUploaded),
			BytesUploaded:   copyInt64Pointer(source.Artifacts.BytesUploaded),
		}
	}
	return out
}

func copyInt64Pointer(n *int64) *int64 {
	if n == nil {
		return nil
	}
	v := *n
	return &v
}
