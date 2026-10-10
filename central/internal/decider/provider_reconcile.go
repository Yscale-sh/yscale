package decider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/central/internal/credentialcipher"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/linode"
)

type ProviderReconciliationStore interface {
	ProviderInventoryTime(ctx context.Context) (time.Time, error)
	ProtectedProviderResources(ctx context.Context) ([]lifecycle.ProviderResourceRef, error)
	ProviderCreatesForReconciliation(ctx context.Context, provider, cloudAccountID string) ([]lifecycle.ProviderCreateReconciliation, error)
	ReconcileExpiredProviderCreateSucceeded(ctx context.Context, operationID int64, providerResourceID string) error
	ObserveProviderOrphan(ctx context.Context, obs lifecycle.ProviderResourceObservation) (lifecycle.ProviderOrphanRecord, error)
	ClaimProviderOrphanDelete(ctx context.Context, ref lifecycle.ProviderResourceRef, lease time.Duration) (lifecycle.ProviderOrphanRecord, bool, error)
	MarkProviderOrphanDeleteFailed(ctx context.Context, ref lifecycle.ProviderResourceRef, leaseToken, safeError string) error
	MarkProviderOrphanDeleted(ctx context.Context, ref lifecycle.ProviderResourceRef, leaseToken string, deletedAt time.Time) error
	ReconcileProviderOrphansAbsent(ctx context.Context, provider, cloudAccountID string, observedProviderResourceIDs []string, absentAt time.Time) (int, error)
	RecordProviderInventoryFailure(ctx context.Context, provider, cloudAccountID, safeError string) error
	RecordProviderInventorySuccess(ctx context.Context, provider, cloudAccountID string, observed, quarantined, deleted int) error
}

const providerOrphanDeleteLease = 5 * time.Minute

type ProviderReconcileOptions struct {
	// Now is a fixed clock for deterministic tests. Runtime callers leave
	// it zero so inventory boundaries come from the shared PostgreSQL clock.
	Now   time.Time
	Grace time.Duration
	Log   *slog.Logger
}

type ProviderReconcileSummary struct {
	Targets     int
	Observed    int
	Quarantined int
	Deleted     int
	Failed      int
}

type providerInventoryTarget struct {
	provider       string
	cloudAccountID string
	backend        backends.Backend
}

func (d *Decider) WithLifecycleReconciliation(store ProviderReconciliationStore) *Decider {
	d.reconciliation = store
	return d
}

func (d *Decider) ReconcileProviders(ctx context.Context, opts ProviderReconcileOptions) (ProviderReconcileSummary, error) {
	if d.reconciliation == nil {
		return ProviderReconcileSummary{}, nil
	}
	if !opts.Now.IsZero() {
		opts.Now = opts.Now.UTC()
	}
	if opts.Grace <= 0 {
		opts.Grace = backends.OrphanGracePeriod
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	protected, err := d.protectedProviderResources(ctx)
	if err != nil {
		return ProviderReconcileSummary{}, err
	}
	targets, targetFailures, targetErr := d.reconciliationTargets(ctx, opts)
	var summary ProviderReconcileSummary
	summary.Targets = len(targets) + targetFailures
	summary.Failed = targetFailures
	var errs []error
	if targetErr != nil {
		errs = append(errs, targetErr)
	}
	for _, target := range targets {
		targetSummary, err := d.reconcileProviderTarget(ctx, target, protected, opts)
		summary.Observed += targetSummary.Observed
		summary.Quarantined += targetSummary.Quarantined
		summary.Deleted += targetSummary.Deleted
		summary.Failed += targetSummary.Failed
		if err != nil {
			errs = append(errs, err)
		}
	}
	return summary, errors.Join(errs...)
}

func (d *Decider) protectedProviderResources(ctx context.Context) (map[string]bool, error) {
	refs, err := d.reconciliation.ProtectedProviderResources(ctx)
	if err != nil {
		return nil, fmt.Errorf("provider reconciliation skipped: durable protected resources unavailable: %w", err)
	}
	protected := make(map[string]bool, len(refs))
	for _, ref := range refs {
		protected[providerResourceKey(ref.Provider, ref.CloudAccountID, ref.ProviderResourceID)] = true
	}
	if d.store == nil {
		return protected, nil
	}
	bursts, ok, err := d.store.DurableBursts(ctx)
	if err != nil {
		return nil, fmt.Errorf("provider reconciliation skipped: durable burst bookings unavailable: %w", err)
	}
	if !ok {
		bursts = d.store.ListBursts()
	}
	for _, b := range bursts {
		if b == nil || b.BackendID == "" {
			continue
		}
		protected[providerResourceKey(b.Backend, b.CloudAccountID, b.BackendID)] = true
	}
	return protected, nil
}

func (d *Decider) reconciliationTargets(ctx context.Context, opts ProviderReconcileOptions) ([]providerInventoryTarget, int, error) {
	targets := make([]providerInventoryTarget, 0, 6)
	seen := make(map[string]bool)
	failed := 0
	var errs []error
	add := func(provider, cloudAccountID string, be backends.Backend) {
		if be == nil {
			return
		}
		key := provider + "\x00" + cloudAccountID
		if seen[key] {
			return
		}
		seen[key] = true
		targets = append(targets, providerInventoryTarget{provider: provider, cloudAccountID: cloudAccountID, backend: be})
	}
	add(backends.TypeFlyIO, "", d.fly)
	add(backends.TypeLinode, "", d.linode)
	add(backends.TypeAWS, "", d.aws)
	add(backends.TypeGCP, "", d.gcp)
	add(backends.TypeAzure, "", d.azure)
	if d.store == nil {
		return targets, failed, nil
	}
	for _, tenantAccount := range d.store.LinodeCloudAccounts() {
		account := tenantAccount.Account
		if account == nil || account.ID == "" {
			continue
		}
		if d.credentialCipher == nil {
			failed++
			if err := d.recordProviderInventoryFailure(ctx, opts, backends.TypeLinode, account.ID,
				"tenant_credentials_unavailable", nil); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		token, err := d.credentialCipher.Decrypt(account.CredentialCiphertext,
			credentialcipher.AdditionalData(tenantAccount.CustomerID, account.ID, account.Provider))
		if err != nil {
			failed++
			if recErr := d.recordProviderInventoryFailure(ctx, opts, backends.TypeLinode, account.ID,
				"tenant_credentials_decrypt_failed", err); recErr != nil {
				errs = append(errs, recErr)
			}
			continue
		}
		if d.linodeForAccount != nil {
			add(backends.TypeLinode, account.ID, d.linodeForAccount(token, linode.Config{
				Region: account.Region, CPUImage: account.CPUImage, GPUImage: account.GPUImage,
			}))
			continue
		}
		add(backends.TypeLinode, account.ID, linode.NewWithConfig(token, linode.Config{
			Region: account.Region, CPUImage: account.CPUImage, GPUImage: account.GPUImage,
		}))
	}
	return targets, failed, errors.Join(errs...)
}

func (d *Decider) reconcileProviderTarget(ctx context.Context, target providerInventoryTarget, protected map[string]bool, opts ProviderReconcileOptions) (ProviderReconcileSummary, error) {
	var summary ProviderReconcileSummary
	inventory, ok := target.backend.(backends.InventoryBackend)
	if !ok {
		summary.Failed++
		return summary, d.recordProviderInventoryFailure(ctx, opts, target.provider, target.cloudAccountID,
			"inventory_backend_unavailable", nil)
	}
	startedAt, err := d.providerInventoryTime(ctx, opts)
	if err != nil {
		summary.Failed++
		return summary, errors.Join(err, d.recordProviderInventoryFailure(ctx, opts, target.provider, target.cloudAccountID,
			"inventory_clock_failed", err))
	}
	owned, err := inventory.ListOwnedNodes(ctx)
	if err != nil {
		summary.Failed++
		return summary, d.recordProviderInventoryFailure(ctx, opts, target.provider, target.cloudAccountID,
			"list_owned_nodes_failed", err)
	}
	summary.Observed = len(owned)
	observedAt, err := d.providerInventoryTime(ctx, opts)
	if err == nil && observedAt.Before(startedAt) {
		err = errors.New("provider inventory clock moved backwards")
	}
	if err != nil {
		summary.Failed++
		return summary, errors.Join(err, d.recordProviderInventoryFailure(ctx, opts, target.provider, target.cloudAccountID,
			"inventory_clock_failed", err))
	}
	if err := validateProviderInventory(target, owned, observedAt); err != nil {
		summary.Failed++
		return summary, errors.Join(err, d.recordProviderInventoryFailure(ctx, opts, target.provider, target.cloudAccountID,
			"inventory_invalid", err))
	}
	ambiguous, err := d.correlateProviderCreates(ctx, target, owned, protected, opts, observedAt)
	if err != nil {
		summary.Failed++
		return summary, err
	}
	observedIDs := make([]string, 0, len(owned))
	for _, node := range owned {
		observedIDs = append(observedIDs, node.BackendID)
		ref := lifecycle.ProviderResourceRef{
			Provider: target.provider, CloudAccountID: target.cloudAccountID, ProviderResourceID: node.BackendID,
		}
		key := providerResourceKey(ref.Provider, ref.CloudAccountID, ref.ProviderResourceID)
		if protected[key] {
			continue
		}
		record, err := d.reconciliation.ObserveProviderOrphan(ctx, lifecycle.ProviderResourceObservation{
			Provider: ref.Provider, CloudAccountID: ref.CloudAccountID, ProviderResourceID: ref.ProviderResourceID,
			ResourceName: node.Name, ProviderCreatedAt: node.CreatedAt, ObservedAt: observedAt,
		})
		if err != nil {
			summary.Failed++
			return summary, errors.Join(err, d.recordProviderInventoryFailure(ctx, opts, target.provider, target.cloudAccountID,
				"observe_orphan_failed", err))
		}
		if record.State == lifecycle.ProviderOrphanDeleted {
			continue
		}
		summary.Quarantined++
		if ambiguous[key] || observedAt.Sub(record.FirstObservedAt) < opts.Grace {
			continue
		}
		claim, claimed, err := d.reconciliation.ClaimProviderOrphanDelete(ctx, ref, providerOrphanDeleteLease)
		if err != nil {
			summary.Failed++
			return summary, err
		}
		if !claimed {
			continue
		}
		if err := target.backend.DeleteNode(ctx, node.BackendID); err != nil {
			summary.Failed++
			if recErr := d.reconciliation.MarkProviderOrphanDeleteFailed(ctx, ref, claim.LeaseToken,
				safeProviderReconciliationError("delete_node_failed", target.provider, target.cloudAccountID)); recErr != nil {
				return summary, recErr
			}
			opts.Log.Warn("provider orphan delete failed",
				"provider", target.provider, "cloud_account_id", target.cloudAccountID,
				"resource_id", node.BackendID, "error", err)
			continue
		}
		deletedAt, err := d.providerInventoryTime(ctx, opts)
		if err == nil && deletedAt.Before(observedAt) {
			err = errors.New("provider deletion clock moved backwards")
		}
		if err != nil {
			summary.Failed++
			return summary, errors.Join(err, d.recordProviderInventoryFailure(ctx, opts, target.provider, target.cloudAccountID,
				"delete_receipt_clock_failed", err))
		}
		if err := d.reconciliation.MarkProviderOrphanDeleted(ctx, ref, claim.LeaseToken, deletedAt); err != nil {
			summary.Failed++
			return summary, err
		}
		protected[providerResourceKey(ref.Provider, ref.CloudAccountID, ref.ProviderResourceID)] = true
		summary.Deleted++
	}
	absentDeleted, err := d.reconciliation.ReconcileProviderOrphansAbsent(ctx, target.provider, target.cloudAccountID, observedIDs, startedAt)
	if err != nil {
		summary.Failed++
		return summary, err
	}
	summary.Deleted += absentDeleted
	if err := d.reconciliation.RecordProviderInventorySuccess(ctx, target.provider, target.cloudAccountID,
		summary.Observed, summary.Quarantined, summary.Deleted); err != nil {
		summary.Failed++
		opts.Log.Warn("provider reconciliation result could not be persisted",
			"provider", target.provider, "cloud_account_id", target.cloudAccountID, "error", err)
		return summary, err
	}
	return summary, nil
}

// Validate the entire snapshot before adoption, observation, or deletion. A
// malformed later entry must not be discovered after earlier paid mutations.
func validateProviderInventory(target providerInventoryTarget, owned []backends.OwnedNode, observedAt time.Time) error {
	seen := make(map[string]struct{}, len(owned))
	for _, node := range owned {
		// The persistence layer trims descriptors; provider calls use the raw
		// ID. Require one canonical identity rather than silently changing it.
		if node.BackendID != strings.TrimSpace(node.BackendID) {
			return fmt.Errorf("%w: inventory resource ID must be canonical", lifecycle.ErrInvalidArgument)
		}
		if err := lifecycle.ValidateProviderResourceObservation(lifecycle.ProviderResourceObservation{
			Provider: target.provider, CloudAccountID: target.cloudAccountID, ProviderResourceID: node.BackendID,
			ResourceName: node.Name, ProviderCreatedAt: node.CreatedAt, ObservedAt: observedAt,
		}); err != nil {
			return err
		}
		if _, duplicate := seen[node.BackendID]; duplicate {
			return fmt.Errorf("%w: inventory contains a duplicate resource ID", lifecycle.ErrInvalidArgument)
		}
		seen[node.BackendID] = struct{}{}
	}
	return nil
}

func (d *Decider) providerInventoryTime(ctx context.Context, opts ProviderReconcileOptions) (time.Time, error) {
	if !opts.Now.IsZero() {
		return opts.Now, nil
	}
	at, err := d.reconciliation.ProviderInventoryTime(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if at.IsZero() {
		return time.Time{}, errors.New("provider inventory clock returned no timestamp")
	}
	return at.UTC(), nil
}

// correlateProviderCreates adds known/live owners to protected and returns
// unresolved candidate identities separately. Those candidates still receive
// durable quarantine observations, but age alone cannot authorize their delete.
func (d *Decider) correlateProviderCreates(ctx context.Context, target providerInventoryTarget, owned []backends.OwnedNode, protected map[string]bool, opts ProviderReconcileOptions, observedAt time.Time) (map[string]bool, error) {
	ambiguous := make(map[string]bool)
	creates, err := d.reconciliation.ProviderCreatesForReconciliation(ctx, target.provider, target.cloudAccountID)
	if err != nil {
		// Saving a failure receipt does not make this an empty successful read.
		// Without current create ownership, the caller must not sweep or mark
		// orphans absent, even when recording the lookup failure succeeds.
		return ambiguous, errors.Join(err, d.recordProviderInventoryFailure(ctx, opts, target.provider, target.cloudAccountID,
			"provider_create_list_failed", err))
	}
	for _, create := range creates {
		if create.ProviderResourceID != "" {
			key := providerResourceKey(create.Provider, create.CloudAccountID, create.ProviderResourceID)
			protected[key] = true
			continue
		}
		if create.State == lifecycle.OperationDeadLetter || create.BurstID == "" {
			continue
		}
		var matches []backends.OwnedNode
		for _, node := range owned {
			if node.BurstID == create.BurstID {
				matches = append(matches, node)
			}
		}
		if create.State == lifecycle.OperationProcessing && !create.LeaseExpired {
			for _, match := range matches {
				key := providerResourceKey(target.provider, target.cloudAccountID, match.BackendID)
				protected[key] = true
			}
			continue
		}
		if create.State != lifecycle.OperationProcessing {
			continue
		}
		if len(matches) > 1 {
			for _, match := range matches {
				ambiguous[providerResourceKey(target.provider, target.cloudAccountID, match.BackendID)] = true
			}
			opts.Log.Warn("expired provider-create has multiple exact-owned candidates; retaining quarantine",
				"provider", target.provider, "cloud_account_id", target.cloudAccountID,
				"operation_id", create.ID, "candidates", len(matches))
			continue
		}
		if len(matches) != 1 {
			continue
		}
		match := matches[0]
		// Persist this inventory's positive evidence before adoption checks an
		// older orphan-absence receipt. A prior delete attempt remains fenced.
		if _, err := d.reconciliation.ObserveProviderOrphan(ctx, lifecycle.ProviderResourceObservation{
			Provider: target.provider, CloudAccountID: target.cloudAccountID,
			ProviderResourceID: match.BackendID, ResourceName: match.Name,
			ProviderCreatedAt: match.CreatedAt, ObservedAt: observedAt,
		}); err != nil {
			return ambiguous, errors.Join(err, d.recordProviderInventoryFailure(ctx, opts, target.provider, target.cloudAccountID,
				"observe_create_candidate_failed", err))
		}
		if err := d.reconciliation.ReconcileExpiredProviderCreateSucceeded(ctx, create.ID, match.BackendID); err != nil {
			return ambiguous, errors.Join(err, d.recordProviderInventoryFailure(ctx, opts, target.provider, target.cloudAccountID,
				"provider_create_reconcile_failed", err))
		}
		key := providerResourceKey(target.provider, target.cloudAccountID, match.BackendID)
		protected[key] = true
		opts.Log.Warn("expired provider-create reconciled to exact-owned resource",
			"provider", target.provider, "cloud_account_id", target.cloudAccountID,
			"operation_id", create.ID, "resource_id", match.BackendID)
	}
	return ambiguous, nil
}

func providerResourceKey(provider, cloudAccountID, providerResourceID string) string {
	return provider + "\x00" + cloudAccountID + "\x00" + providerResourceID
}

func (d *Decider) recordProviderInventoryFailure(ctx context.Context, opts ProviderReconcileOptions, provider, cloudAccountID, stage string, cause error) error {
	if cause != nil {
		opts.Log.Warn("provider reconciliation target failed",
			"provider", provider, "cloud_account_id", cloudAccountID,
			"stage", stage, "error", cause)
	}
	if err := d.reconciliation.RecordProviderInventoryFailure(ctx, provider, cloudAccountID,
		safeProviderReconciliationError(stage, provider, cloudAccountID)); err != nil {
		return fmt.Errorf("provider reconciliation failure receipt could not be persisted: %w", err)
	}
	return nil
}

func safeProviderReconciliationError(stage, provider, cloudAccountID string) string {
	if cloudAccountID == "" {
		cloudAccountID = "default"
	}
	return fmt.Sprintf("stage=%s provider=%s cloud_account_id=%s", stage, provider, cloudAccountID)
}
