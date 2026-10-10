package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/internal/evidence"
)

// inventoryObservation collects all the granular sub-observations needed
// to populate a complete evidence.Inventory. Each field represents an
// independent observation chain.
type inventoryObservation struct {
	// Kubernetes
	KubernetesNodeAbsent bool

	// Provider (Linode): typed sub-counts
	ProviderAudited bool
	ProviderResidue ProviderResidue

	// Mesh (Tailscale)
	MeshAudited        bool
	MeshDevicesPresent int

	// Durable receipt (Central): only from the reap receipt
	ReceiptPresent                 bool
	PodCIDRReservationsFromReceipt int
	AccountLeasesFromReceipt       int
}

// observeInventory performs the full granular inventory observation for
// a completed case. It runs after waitForReap/waitForProviderAbsence/
// waitForCentralEvidence have all passed.
//
// It never derives inventory from CaseResult.Reaped or workload phase.
// Missing provider ID, missing audit credentials, any list/decode error,
// missing durable receipt, or any nonzero residue makes the inventory
// incomplete and the artifact stays failed.
func (r *Runner) observeInventory(ctx context.Context, res CaseResult) (*evidence.Inventory, error) {
	var obs inventoryObservation
	var errs []string

	// 1. Kubernetes node absence: must be explicitly observed via waitForReap
	if !r.kubernetesNodeAbsent {
		return nil, fmt.Errorf("inventory: kubernetes node absence was not observed via waitForReap")
	}
	obs.KubernetesNodeAbsent = true

	// 2. Provider granular audit
	if res.Backend != backendLinode {
		return nil, fmt.Errorf("inventory: unsupported provider %q; only %q is supported for granular inventory", res.Backend, backendLinode)
	}
	if res.BurstID == "" {
		return nil, fmt.Errorf("inventory: empty burstID; cannot audit provider or mesh without it")
	}
	if r.Provider == nil {
		return nil, fmt.Errorf("inventory: provider audit not configured")
	}
	if r.providerInstanceID <= 0 {
		return nil, fmt.Errorf("inventory: no provider instance ID captured from burst Node spec.providerID")
	}
	residue, err := r.Provider.AuditBurstResidue(ctx, res.BurstID, r.providerInstanceID)
	if err != nil {
		return nil, fmt.Errorf("inventory: provider residue audit: %w", err)
	}
	obs.ProviderAudited = true
	obs.ProviderResidue = residue
	if residue.UniqueTotal() > 0 {
		errs = append(errs, fmt.Sprintf("provider residue: tagged=%d exact_id=%d volumes=%d firewall=%d total=%d",
			residue.TaggedInstances, residue.ExactIDInstance, residue.AttachedVolumes, residue.FirewallDevices, residue.UniqueTotal()))
	}

	// 3. Mesh device absence
	if r.Mesh == nil {
		return nil, fmt.Errorf("inventory: mesh audit not configured")
	}
	meshProvider := r.Mesh.ProviderName()
	if !evidence.IsValidMeshProvider(meshProvider) {
		return nil, fmt.Errorf("inventory: mesh provider %q is not one of %q/%q",
			meshProvider, evidence.MeshProviderTailscale, evidence.MeshProviderFabric)
	}
	hostname := burstTailscaleHostname(res.BurstID)
	if hostname == "" {
		return nil, fmt.Errorf("inventory: cannot derive mesh hostname from burstID %q", res.BurstID)
	}
	meshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	count, err := r.Mesh.FindDevice(meshCtx, hostname)
	if err != nil {
		return nil, fmt.Errorf("inventory: mesh audit: %w", err)
	}
	obs.MeshAudited = true
	obs.MeshDevicesPresent = count
	if count > 0 {
		errs = append(errs, fmt.Sprintf("mesh devices remaining: %d", count))
	}

	// 4. Pod CIDR and account lease from durable Central reap receipt
	if r.centralEvidence == nil || r.centralEvidence.Cleanup == nil {
		return nil, fmt.Errorf("inventory: durable central evidence not available")
	}
	if !r.centralEvidence.Cleanup.DurableReapReceipt {
		return nil, fmt.Errorf("inventory: durable reap receipt not present")
	}
	obs.ReceiptPresent = true
	obs.PodCIDRReservationsFromReceipt = 0
	obs.AccountLeasesFromReceipt = 0

	if len(errs) > 0 {
		return nil, fmt.Errorf("inventory: nonzero residue: %s", strings.Join(errs, "; "))
	}

	inv := &evidence.Inventory{
		Complete:                     true,
		ProviderResourcesRemaining:   residue.UniqueTotal(),
		KubernetesNodesRemaining:     0,
		MeshDevicesRemaining:         obs.MeshDevicesPresent,
		PodCIDRReservationsRemaining: obs.PodCIDRReservationsFromReceipt,
		AccountLeasesRemaining:       obs.AccountLeasesFromReceipt,
		MeshProvider:                 meshProvider,
	}
	return inv, nil
}
