//go:build integration

package lifecycle

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	testRuntimeRole     = "yscale_lifecycle_runtime_test"
	testRuntimePassword = "lifecycle-runtime-test"
)

// TestPostgresRuntimeMigrationBoundary exercises the production migration and
// runtime-open seam against PostgreSQL. Unit tests pin the emitted grants; this
// test proves PostgreSQL interprets them as intended.
func TestPostgresRuntimeMigrationBoundary(t *testing.T) {
	ownerDSN := os.Getenv("LIFECYCLE_TEST_DATABASE_URL")
	runtimeDSN := os.Getenv("LIFECYCLE_TEST_RUNTIME_DATABASE_URL")
	if ownerDSN == "" || runtimeDSN == "" {
		t.Skip("set lifecycle owner and runtime test database URLs")
	}
	if os.Getenv("LIFECYCLE_TEST_ALLOW_DESTRUCTIVE") != "DROP_LIFECYCLE_SCHEMA" {
		t.Fatal("set LIFECYCLE_TEST_ALLOW_DESTRUCTIVE=DROP_LIFECYCLE_SCHEMA for the isolated test database")
	}

	ctx := context.Background()
	owner, err := pgxpool.New(ctx, ownerDSN)
	if err != nil {
		t.Fatal(err)
	}

	var databaseName string
	if err := owner.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("LIFECYCLE_TEST_EXPECT_DATABASE"); expected == "" || expected != databaseName {
		t.Fatalf("LIFECYCLE_TEST_EXPECT_DATABASE must exactly match current database %q", databaseName)
	}
	if !strings.HasSuffix(databaseName, "_lifecycle_test") {
		t.Fatalf("refusing destructive integration test against non-test database %q", databaseName)
	}

	role := pgx.Identifier{testRuntimeRole}.Sanitize()
	dropRole := func() error {
		var exists bool
		if err := owner.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$1)`, testRuntimeRole).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return nil
		}
		if _, err := owner.Exec(ctx, "DROP OWNED BY "+role); err != nil {
			return err
		}
		_, err := owner.Exec(ctx, "DROP ROLE "+role)
		return err
	}
	if _, err := owner.Exec(ctx, `DROP SCHEMA IF EXISTS lifecycle CASCADE`); err != nil {
		t.Fatal(err)
	}
	if err := dropRole(); err != nil {
		t.Fatalf("remove stale runtime test role: %v", err)
	}
	t.Cleanup(func() {
		defer owner.Close()
		if _, err := owner.Exec(context.Background(), `DROP SCHEMA IF EXISTS lifecycle CASCADE`); err != nil {
			t.Errorf("cleanup lifecycle schema: %v", err)
		}
		if err := dropRole(); err != nil {
			t.Errorf("cleanup runtime test role: %v", err)
		}
	})

	if _, err := owner.Exec(ctx, "CREATE ROLE "+role+" LOGIN PASSWORD '"+testRuntimePassword+"'"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureProviderDeleteSchema(ctx, owner); err != nil {
		t.Fatal(err)
	}

	// Recreate the additive PR #90 -> runtime-integration migration seam. Seed
	// rows first, then remove the columns/index that #90 did not have; the old
	// rows carry cloud-account routing only in their immutable JSON.
	legacy := testAdmission("runtime_upgrade")
	legacy.BurstSpec = []byte(`{"CloudAccountID":"account_upgrade","cpu":1}`)
	ownerStore := NewStore(owner)
	if _, err := ownerStore.AdmitWorkload(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	create, ok, err := ownerStore.ClaimProviderCreate(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim legacy provider create: op=%+v ok=%v err=%v", create, ok, err)
	}
	if err := ownerStore.MarkProviderCreateSucceeded(ctx, create.ID, create.LeaseToken, "linode_upgrade"); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Exec(ctx, `
		INSERT INTO lifecycle.provider_deletes
		(customer_id, cluster_id, workload_id, burst_id, provider, region, sku,
		 provider_resource_id, payload_version, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,$9::jsonb)`,
		legacy.CustomerID, legacy.ClusterID, legacy.WorkloadID, legacy.BurstID,
		legacy.Provider, legacy.Region, legacy.SKU, "linode_upgrade",
		`{"burst":{"CloudAccountID":"account_upgrade"}}`); err != nil {
		t.Fatalf("seed legacy provider delete: %v", err)
	}
	if _, err := owner.Exec(ctx, `
		DROP INDEX IF EXISTS lifecycle.idx_lifecycle_provider_deletes_nonterminal_resources;
		CREATE OR REPLACE FUNCTION lifecycle.validate_burst_update()
		RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		    IF to_jsonb(OLD) - 'cloud_account_id' IS DISTINCT FROM
		       to_jsonb(NEW) - 'cloud_account_id' THEN
		        RAISE EXCEPTION 'legacy burst field changed during migration';
		    END IF;
		    RETURN NEW;
		END;
		$$;
		CREATE OR REPLACE FUNCTION lifecycle.validate_provider_delete_update()
		RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		    IF to_jsonb(OLD) - 'cloud_account_id' IS DISTINCT FROM
		       to_jsonb(NEW) - 'cloud_account_id' THEN
		        RAISE EXCEPTION 'legacy provider-delete field changed during migration';
		    END IF;
		    RETURN NEW;
		END;
		$$;
		CREATE OR REPLACE FUNCTION lifecycle.validate_operation_update()
		RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		    IF to_jsonb(OLD) - 'cloud_account_id' IS DISTINCT FROM
		       to_jsonb(NEW) - 'cloud_account_id' THEN
		        RAISE EXCEPTION 'legacy operation field changed during migration';
		    END IF;
		    RETURN NEW;
		END;
		$$;
		ALTER TABLE lifecycle.provider_deletes DROP COLUMN cloud_account_id;
		ALTER TABLE lifecycle.bursts DROP COLUMN cloud_account_id;
		ALTER TABLE lifecycle.operations DROP COLUMN cloud_account_id`); err != nil {
		t.Fatalf("construct legacy lifecycle schema: %v", err)
	}
	if err := EnsureProviderDeleteSchema(ctx, owner); err != nil {
		t.Fatalf("upgrade legacy lifecycle schema: %v", err)
	}
	var burstAccount, deleteAccount string
	var nonterminalIndex bool
	if err := owner.QueryRow(ctx, `
		SELECT b.cloud_account_id, d.cloud_account_id,
		       to_regclass('lifecycle.idx_lifecycle_provider_deletes_nonterminal_resources') IS NOT NULL
		FROM lifecycle.bursts b
		JOIN lifecycle.provider_deletes d
		  ON d.customer_id=b.customer_id
		 AND d.cluster_id=b.cluster_id
		 AND d.burst_id=b.id
		WHERE b.id=$1`, legacy.BurstID).Scan(&burstAccount, &deleteAccount, &nonterminalIndex); err != nil {
		t.Fatal(err)
	}
	if burstAccount != "account_upgrade" || deleteAccount != "account_upgrade" || !nonterminalIndex {
		t.Fatalf("legacy upgrade burst_account=%q delete_account=%q nonterminal_index=%v",
			burstAccount, deleteAccount, nonterminalIndex)
	}
	if err := GrantRuntimePrivileges(ctx, owner, testRuntimeRole); err != nil {
		t.Fatal(err)
	}

	store, err := OpenStore(ctx, runtimeDSN)
	if err != nil {
		t.Fatalf("open least-privilege runtime store: %v", err)
	}
	defer store.Close()
	if inventoryAt, err := store.ProviderInventoryTime(ctx); err != nil || inventoryAt.IsZero() {
		t.Fatalf("runtime role cannot read authoritative inventory time: at=%s err=%v", inventoryAt, err)
	}

	if ownerStore, err := OpenStore(ctx, ownerDSN); err == nil {
		ownerStore.Close()
		t.Fatal("database owner was accepted as a lifecycle runtime role")
	} else if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("owner rejection = %v, want ErrInvariantViolation", err)
	}

	runtimeAdmission := testAdmission("runtime_boundary")
	if _, err := store.AdmitWorkload(ctx, runtimeAdmission); err != nil {
		t.Fatalf("runtime role cannot execute lifecycle store writes: %v", err)
	}
	runtimeCreate, claimed, err := store.ClaimProviderCreateForBurst(ctx, runtimeAdmission.CustomerID,
		runtimeAdmission.ClusterID, runtimeAdmission.BurstID, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("runtime role cannot claim create: claimed=%v err=%v", claimed, err)
	}
	if err := store.MarkProviderCreateSucceeded(ctx, runtimeCreate.ID, runtimeCreate.LeaseToken, "runtime_resource_fence"); err != nil {
		t.Fatalf("runtime role cannot publish resource ownership: %v", err)
	}
	runtimeRef := ProviderResourceRef{Provider: runtimeAdmission.Provider,
		CloudAccountID: runtimeAdmission.CloudAccountID, ProviderResourceID: "runtime_resource_fence"}
	observeResourceFenceOrphan(t, ctx, store, runtimeRef)
	if _, claimed, err := store.ClaimProviderOrphanDelete(ctx, runtimeRef, time.Minute); err != nil || claimed {
		t.Fatalf("runtime role did not protect current ownership: claimed=%v err=%v", claimed, err)
	}
	runtimeRef.ProviderResourceID = "runtime_unowned_resource"
	observeResourceFenceOrphan(t, ctx, store, runtimeRef)
	orphanClaim, claimed, err := store.ClaimProviderOrphanDelete(ctx, runtimeRef, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("runtime role cannot claim a genuine orphan: claimed=%v err=%v", claimed, err)
	}
	if err := store.MarkProviderOrphanDeleteFailed(ctx, runtimeRef, orphanClaim.LeaseToken, "synthetic runtime retry"); err != nil {
		t.Fatalf("runtime role cannot record orphan delete failure: %v", err)
	}
	orphanClaim, claimed, err = store.ClaimProviderOrphanDelete(ctx, runtimeRef, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("runtime role cannot retry a genuine orphan: claimed=%v err=%v", claimed, err)
	}
	if err := store.MarkProviderOrphanDeleted(ctx, runtimeRef, orphanClaim.LeaseToken, time.Time{}); err != nil {
		t.Fatalf("runtime role cannot settle orphan deletion: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE TABLE lifecycle.runtime_escape (id integer)`); err == nil {
		t.Fatal("runtime role created a lifecycle relation")
	}
	if _, err := store.pool.Exec(ctx, `DELETE FROM lifecycle.workloads`); err == nil {
		t.Fatal("runtime role deleted lifecycle rows")
	}
	var canUpdateEvents bool
	if err := store.pool.QueryRow(ctx,
		`SELECT has_table_privilege(current_user, 'lifecycle.lifecycle_events', 'UPDATE')`).Scan(&canUpdateEvents); err != nil {
		t.Fatal(err)
	}
	if canUpdateEvents {
		t.Fatal("runtime role has UPDATE on the append-only lifecycle event log")
	}
	if _, err := store.pool.Exec(ctx, `UPDATE lifecycle.lifecycle_events SET new_state=new_state`); err == nil {
		t.Fatal("runtime role updated the append-only lifecycle event log")
	}
	if _, err := owner.Exec(ctx, `REVOKE USAGE ON SEQUENCE lifecycle.outbox_id_seq FROM `+role); err != nil {
		t.Fatal(err)
	}
	if underprivileged, err := OpenStore(ctx, runtimeDSN); err == nil {
		underprivileged.Close()
		t.Fatal("runtime role without lifecycle sequence usage was accepted")
	} else if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("missing sequence usage rejection = %v, want ErrInvariantViolation", err)
	}
	if _, err := owner.Exec(ctx, `GRANT USAGE ON SEQUENCE lifecycle.outbox_id_seq TO `+role); err != nil {
		t.Fatal(err)
	}

	// Effective privileges matter, not only direct grants. Prove startup also
	// rejects an UPDATE inherited through PUBLIC, which a direct-role audit
	// would otherwise miss.
	if _, err := owner.Exec(ctx, `GRANT UPDATE ON lifecycle.lifecycle_events TO PUBLIC`); err != nil {
		t.Fatal(err)
	}
	if elevated, err := OpenStore(ctx, runtimeDSN); err == nil {
		elevated.Close()
		t.Fatal("runtime role with inherited append-only UPDATE was accepted")
	} else if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("inherited UPDATE rejection = %v, want ErrInvariantViolation", err)
	}
	if _, err := owner.Exec(ctx, `REVOKE UPDATE ON lifecycle.lifecycle_events FROM PUBLIC`); err != nil {
		t.Fatal(err)
	}
}
