package lifecycle

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const protectedProviderResourcesSQL = `
	SELECT provider, cloud_account_id, provider_resource_id
	FROM lifecycle.bursts
	WHERE state='provider_created' AND provider_resource_id <> ''
	UNION
	SELECT provider, cloud_account_id, provider_resource_id
	FROM lifecycle.provider_deletes
	WHERE state <> 'terminated' AND provider_resource_id <> ''
	UNION
	SELECT provider, cloud_account_id, provider_resource_id
	FROM lifecycle.operations
	WHERE operation_type='provider_create'
	  AND state IN ('processing','succeeded','dead_letter')
	  AND provider_resource_id <> ''`

// lockProviderResourceTx serializes ownership publication with orphan deletion,
// including when no owner/orphan row exists yet. The JSON tuple is unambiguous;
// a hash collision only serializes unrelated resources. Callers use READ
// COMMITTED and read the opposing state in a separate statement AFTER this
// lock returns, so a waiter sees the preceding transaction's committed result.
// No database lock is held across a provider call.
func lockProviderResourceTx(ctx context.Context, tx pgx.Tx, ref ProviderResourceRef) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended(
		json_build_array('yscale:lifecycle:provider-resource', $1::text, $2::text, $3::text)::text, 0))`,
		ref.Provider, ref.CloudAccountID, ref.ProviderResourceID); err != nil {
		return fmt.Errorf("lifecycle: lock provider resource ownership: %w", err)
	}
	return nil
}

// providerResourceProtectedTx reads the same ownership projection as the sweep.
// Callers hold the resource fence before taking this fresh statement snapshot.
func providerResourceProtectedTx(ctx context.Context, tx pgx.Tx, ref ProviderResourceRef) (bool, error) {
	var protected bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM (`+protectedProviderResourcesSQL+`) AS owned
		WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3)`,
		ref.Provider, ref.CloudAccountID, ref.ProviderResourceID).Scan(&protected); err != nil {
		return false, fmt.Errorf("lifecycle: check current provider resource ownership: %w", err)
	}
	return protected, nil
}

// guardProviderResourceOwnershipTx must precede burst/workload locks on every
// path that first publishes a provider identity. An orphan delete claim is
// durable authorization for an external call: lease expiry or a failed response
// cannot revoke a call that may already be in flight. Such resources stay with
// orphan reconciliation instead of becoming newly owned live capacity.
func guardProviderResourceOwnershipTx(ctx context.Context, tx pgx.Tx, ref ProviderResourceRef) error {
	if err := lockProviderResourceTx(ctx, tx, ref); err != nil {
		return err
	}
	var deletionStarted bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM lifecycle.provider_orphans
		WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3
		  AND (state <> 'quarantined' OR attempts > 0))`,
		ref.Provider, ref.CloudAccountID, ref.ProviderResourceID).Scan(&deletionStarted); err != nil {
		return fmt.Errorf("lifecycle: check orphan deletion before ownership: %w", err)
	}
	if deletionStarted {
		return fmt.Errorf("%w: provider resource has an orphan deletion claim or absence receipt", ErrInvariantViolation)
	}
	return nil
}
