//go:build integration

package lifecycle

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The scoped claim is what an inline admission path uses to lease its OWN
// provider-create operation, so its crash boundary is the one that decides
// whether a crashed attempt is resumable and whether a settled one can be
// bought a second time.
func TestPostgresScopedProviderCreateClaim(t *testing.T) {
	dsn := os.Getenv("LIFECYCLE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set LIFECYCLE_TEST_DATABASE_URL to an isolated Postgres database")
	}
	if os.Getenv("LIFECYCLE_TEST_ALLOW_DESTRUCTIVE") != "DROP_LIFECYCLE_SCHEMA" {
		t.Fatal("set LIFECYCLE_TEST_ALLOW_DESTRUCTIVE=DROP_LIFECYCLE_SCHEMA for the isolated test database")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var databaseName string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("LIFECYCLE_TEST_EXPECT_DATABASE"); expected == "" || expected != databaseName {
		t.Fatalf("LIFECYCLE_TEST_EXPECT_DATABASE must exactly match current database %q", databaseName)
	}
	if !strings.HasSuffix(databaseName, "_lifecycle_test") {
		t.Fatalf("refusing destructive integration test against non-test database %q", databaseName)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS lifecycle CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS lifecycle CASCADE`); err != nil {
			t.Errorf("cleanup lifecycle schema: %v", err)
		}
		pool.Close()
	})
	if err := EnsureSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)

	req := testAdmission("scoped_create")
	if _, err := store.AdmitWorkload(ctx, req); err != nil {
		t.Fatal(err)
	}

	// Every argument that names whose work this is has to be refused before a
	// query runs, and an out-of-range lease with it.
	for _, bad := range []struct {
		name       string
		customerID string
		clusterID  string
		burstID    string
		lease      time.Duration
	}{
		{"empty customer", "", req.ClusterID, req.BurstID, time.Minute},
		{"empty cluster", req.CustomerID, "", req.BurstID, time.Minute},
		{"empty burst", req.CustomerID, req.ClusterID, "", time.Minute},
		{"injection in burst id", req.CustomerID, req.ClusterID, "burst_1' OR '1'='1", time.Minute},
		{"lease below floor", req.CustomerID, req.ClusterID, req.BurstID, time.Millisecond},
		{"lease above ceiling", req.CustomerID, req.ClusterID, req.BurstID, time.Hour},
	} {
		if _, claimed, err := store.ClaimProviderCreateForBurst(ctx, bad.customerID, bad.clusterID, bad.burstID, bad.lease); !errors.Is(err, ErrInvalidArgument) || claimed {
			t.Fatalf("%s: claimed=%v err=%v, want ErrInvalidArgument", bad.name, claimed, err)
		}
	}

	// A burst nobody admitted is not silently claimable, and neither is another
	// tenant's: the scoped claim is the call an inline path trusts not to hand
	// it work that is not its own.
	if _, _, err := store.ClaimProviderCreateForBurst(ctx, req.CustomerID, req.ClusterID, "burst_never_admitted", time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("claim for an unadmitted burst = %v, want ErrNotFound", err)
	}
	if _, _, err := store.ClaimProviderCreateForBurst(ctx, "cust_someone_else", req.ClusterID, req.BurstID, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant claim = %v, want ErrNotFound", err)
	}

	op, claimed, err := store.ClaimProviderCreateForBurst(ctx, req.CustomerID, req.ClusterID, req.BurstID, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("scoped claim: claimed=%v err=%v", claimed, err)
	}
	if op.BurstID != req.BurstID || op.State != OperationProcessing || op.LeaseToken == "" {
		t.Fatalf("scoped claim returned %+v", op)
	}
	if op.Attempts != 1 {
		t.Fatalf("attempts after first claim = %d, want 1", op.Attempts)
	}
	if string(op.Payload) == "" {
		t.Fatal("scoped claim returned no admitted payload to execute")
	}

	// A live lease belongs to exactly one attempt. A second one is refused
	// without an error, and told which state refused it.
	held, claimed, err := store.ClaimProviderCreateForBurst(ctx, req.CustomerID, req.ClusterID, req.BurstID, time.Minute)
	if err != nil || claimed {
		t.Fatalf("second claim under a live lease: claimed=%v err=%v", claimed, err)
	}
	if held.State != OperationProcessing {
		t.Fatalf("refused claim reported state %q, want processing", held.State)
	}

	// CRASH BOUNDARY. The attempt holding the lease dies. The lease expires and
	// the operation must NOT become executable again: the request had already
	// been handed out, so a provider may have accepted the create before the
	// attempt stopped writing. Reclaiming it would issue a second CreateNode for
	// a burst that may already own a machine, and no backend central routes to
	// supplies an idempotency key that would make the second call a no-op.
	staleToken := op.LeaseToken
	if _, err := pool.Exec(ctx,
		`UPDATE lifecycle.operations SET locked_until = now() - interval '1 second' WHERE id=$1`, op.ID); err != nil {
		t.Fatal(err)
	}
	stranded, claimed, err := store.ClaimProviderCreateForBurst(ctx, req.CustomerID, req.ClusterID, req.BurstID, time.Minute)
	if err != nil || claimed {
		t.Fatalf("expired create lease was reclaimed for a second CreateNode: claimed=%v err=%v", claimed, err)
	}
	if stranded.State != OperationProcessing || !stranded.LeaseExpired {
		t.Fatalf("refusal reported state=%q leaseExpired=%v, want processing with an expired lease so the ambiguity is legible",
			stranded.State, stranded.LeaseExpired)
	}
	if stranded.Attempts != 1 {
		t.Fatalf("attempts after a refused reclaim = %d, want the original 1", stranded.Attempts)
	}
	if string(stranded.Payload) != string(op.Payload) {
		t.Fatal("the refusal does not carry the admitted request an operator has to reconcile against")
	}
	// The refusal must not have re-leased the row: a fresh token here would mean
	// the caller was handed a fence it could execute under.
	var storedToken, storedState string
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(lease_token,''), state FROM lifecycle.operations WHERE id=$1`, op.ID).
		Scan(&storedToken, &storedState); err != nil {
		t.Fatal(err)
	}
	if storedToken != staleToken || storedState != OperationProcessing {
		t.Fatalf("a refused reclaim mutated the operation: token=%q state=%q", storedToken, storedState)
	}

	// The dead attempt waking up late still cannot settle anything: the fence is
	// the lease DEADLINE, not just the token.
	if err := store.MarkProviderCreateSucceeded(ctx, op.ID, staleToken, "linode_stale"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("settle under an expired lease = %v, want ErrLeaseLost", err)
	}

	// Only an operator moves it on. Restoring the lease is what an operator's
	// resolution amounts to, and is how this test reaches the settled state the
	// replay guard below is about.
	if _, err := pool.Exec(ctx,
		`UPDATE lifecycle.operations SET locked_until = now() + interval '1 minute' WHERE id=$1`, op.ID); err != nil {
		t.Fatal(err)
	}
	const providerResourceID = "linode_123456"
	if err := store.MarkProviderCreateSucceeded(ctx, op.ID, staleToken, providerResourceID); err != nil {
		t.Fatal(err)
	}

	// The replay guard: once a machine exists for this burst, no later attempt
	// can lease the operation and buy a second one.
	settled, claimed, err := store.ClaimProviderCreateForBurst(ctx, req.CustomerID, req.ClusterID, req.BurstID, time.Minute)
	if err != nil || claimed {
		t.Fatalf("claim after success: claimed=%v err=%v", claimed, err)
	}
	if settled.State != OperationSucceeded {
		t.Fatalf("refused claim after success reported state %q, want succeeded", settled.State)
	}

	var burstState, burstResource string
	if err := pool.QueryRow(ctx, `
		SELECT state, provider_resource_id FROM lifecycle.bursts
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3`,
		req.CustomerID, req.ClusterID, req.BurstID).Scan(&burstState, &burstResource); err != nil {
		t.Fatal(err)
	}
	if burstState != BurstProviderCreated || burstResource != providerResourceID {
		t.Fatalf("burst after settlement = %s/%s, want provider_created/%s", burstState, burstResource, providerResourceID)
	}
}
