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

// Admission and the authoritative delete write and read the SAME burst row.
// Admission creates it; a later reap projects the booked resource onto it and
// refuses the delete if the stored identity disagrees with the booking. So the
// account and the region admission records are not bookkeeping — they are the
// values that decide, months later, whether a tenant's paid machine can be torn
// down at all.
//
// The two cases that would diverge are exactly the two here: a BYOC burst, whose
// account admission previously did not record; and a workload that pinned no
// region, where "unspecified" has more than one plausible spelling.
func TestPostgresAdmissionProjectsTheIdentityDeleteChecks(t *testing.T) {
	ctx := context.Background()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)

	for _, test := range []struct {
		name           string
		suffix         string
		region         string
		cloudAccountID string
		wantRegion     string
	}{
		{
			name:           "BYOC burst in a pinned region",
			suffix:         "byoc",
			region:         "us-east",
			cloudAccountID: "ca_tenant_0123456789ab",
			wantRegion:     "us-east",
		},
		{
			name:       "platform burst that pinned no region",
			suffix:     "default_region",
			region:     "",
			wantRegion: unspecifiedProviderRegion,
		},
		{
			name:           "BYOC burst that pinned no region",
			suffix:         "byoc_default_region",
			region:         "",
			cloudAccountID: "ca_tenant_ba9876543210",
			wantRegion:     unspecifiedProviderRegion,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := testAdmission(test.suffix)
			req.Region = test.region
			req.CloudAccountID = test.cloudAccountID
			if _, err := store.AdmitWorkload(ctx, req); err != nil {
				t.Fatalf("admit: %v", err)
			}

			var storedRegion, storedAccount string
			if err := pool.QueryRow(ctx, `
				SELECT region, cloud_account_id FROM lifecycle.bursts
				WHERE customer_id=$1 AND cluster_id=$2 AND id=$3`,
				req.CustomerID, req.ClusterID, req.BurstID).Scan(&storedRegion, &storedAccount); err != nil {
				t.Fatal(err)
			}
			if storedRegion != test.wantRegion || storedAccount != test.cloudAccountID {
				t.Fatalf("admitted burst identity = %s/%s, want %s/%s",
					storedRegion, storedAccount, test.wantRegion, test.cloudAccountID)
			}
			// The operation row routes the create, so it must name the same region.
			var operationRegion string
			if err := pool.QueryRow(ctx, `
				SELECT region FROM lifecycle.operations
				WHERE customer_id=$1 AND cluster_id=$2 AND burst_id=$3 AND operation_type='provider_create'`,
				req.CustomerID, req.ClusterID, req.BurstID).Scan(&operationRegion); err != nil {
				t.Fatal(err)
			}
			if operationRegion != test.wantRegion {
				t.Fatalf("admitted operation region = %q, want %q", operationRegion, test.wantRegion)
			}

			// The create lands, exactly as the request path would settle it.
			op, claimed, err := store.ClaimProviderCreateForBurst(ctx, req.CustomerID, req.ClusterID, req.BurstID, time.Minute)
			if err != nil || !claimed {
				t.Fatalf("claim: claimed=%v err=%v", claimed, err)
			}
			providerResourceID := "linode_" + test.suffix
			if err := store.MarkProviderCreateSucceeded(ctx, op.ID, op.LeaseToken, providerResourceID); err != nil {
				t.Fatal(err)
			}

			// The reap. Every field is derived the way handlers derive it from a
			// *state.Burst: the canonical region, the booked account, and NO
			// workload id, because the durable burst record carries none.
			response, err := store.RequestProviderDeleteForBooking(ctx, ProviderDeleteBooking{
				CustomerID:         req.CustomerID,
				ClusterID:          req.ClusterID,
				BurstID:            req.BurstID,
				Provider:           req.Provider,
				Region:             CanonicalProviderDeleteRegion(test.region),
				CloudAccountID:     test.cloudAccountID,
				SKU:                req.SKU,
				ProviderResourceID: providerResourceID,
				Spec:               []byte(`{"ID":"` + req.BurstID + `"}`),
				Reason:             "workload completed",
				Payload:            []byte(`{"Burst":{"ID":"` + req.BurstID + `"}}`),
				Actor:              "reaper",
			})
			if errors.Is(err, ErrIdentityConflict) {
				t.Fatalf("an admitted burst could not be reaped: %v — the paid machine is now undeletable", err)
			}
			if err != nil {
				t.Fatalf("request provider delete for an admitted burst: %v", err)
			}
			if !response.Inserted || response.State != ProviderDeleteQueued {
				t.Fatalf("delete request = %+v", response)
			}

			// The projection kept the admitted identity rather than overwriting it
			// with the booking's, and the delete row carries the same account and
			// region the success path re-checks.
			var deleteRegion, deleteAccount, deleteWorkload string
			if err := pool.QueryRow(ctx, `
				SELECT region, cloud_account_id, workload_id FROM lifecycle.provider_deletes
				WHERE customer_id=$1 AND cluster_id=$2 AND burst_id=$3`,
				req.CustomerID, req.ClusterID, req.BurstID).
				Scan(&deleteRegion, &deleteAccount, &deleteWorkload); err != nil {
				t.Fatal(err)
			}
			if deleteRegion != test.wantRegion || deleteAccount != test.cloudAccountID {
				t.Fatalf("delete identity = %s/%s, want the admitted %s/%s",
					deleteRegion, deleteAccount, test.wantRegion, test.cloudAccountID)
			}
			if deleteWorkload != req.WorkloadID {
				t.Fatalf("delete workload = %q, want the admitted %q", deleteWorkload, req.WorkloadID)
			}
		})
	}
}

// A booking that DOES name a workload is still held to it: relaxing the check
// for a derived value must not relax it for an asserted one, or a caller could
// queue a delete against an aggregate it named wrongly and never be told.
func TestPostgresBookingStillRefusesAWrongAssertedIdentity(t *testing.T) {
	ctx := context.Background()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)

	req := testAdmission("asserted_identity")
	req.CloudAccountID = "ca_tenant_asserted"
	if _, err := store.AdmitWorkload(ctx, req); err != nil {
		t.Fatal(err)
	}
	op, claimed, err := store.ClaimProviderCreateForBurst(ctx, req.CustomerID, req.ClusterID, req.BurstID, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}
	const providerResourceID = "linode_asserted"
	if err := store.MarkProviderCreateSucceeded(ctx, op.ID, op.LeaseToken, providerResourceID); err != nil {
		t.Fatal(err)
	}

	booking := ProviderDeleteBooking{
		CustomerID:         req.CustomerID,
		ClusterID:          req.ClusterID,
		BurstID:            req.BurstID,
		Provider:           req.Provider,
		Region:             req.Region,
		CloudAccountID:     req.CloudAccountID,
		SKU:                req.SKU,
		ProviderResourceID: providerResourceID,
		Spec:               []byte(`{"ID":"` + req.BurstID + `"}`),
		Reason:             "workload completed",
		Payload:            []byte(`{"Burst":{"ID":"` + req.BurstID + `"}}`),
	}
	for name, mutate := range map[string]func(*ProviderDeleteBooking){
		"asserted workload": func(b *ProviderDeleteBooking) { b.WorkloadID = "wl_someone_elses" },
		"cloud account":     func(b *ProviderDeleteBooking) { b.CloudAccountID = "ca_someone_elses" },
		"region":            func(b *ProviderDeleteBooking) { b.Region = "eu-west" },
		"provider resource": func(b *ProviderDeleteBooking) { b.ProviderResourceID = "linode_someone_elses" },
	} {
		t.Run(name, func(t *testing.T) {
			wrong := booking
			mutate(&wrong)
			if _, err := store.RequestProviderDeleteForBooking(ctx, wrong); !errors.Is(err, ErrIdentityConflict) {
				t.Fatalf("a booking with a substituted %s = %v, want ErrIdentityConflict", name, err)
			}
		})
	}
}

func TestPostgresProviderCreatedAtReceipts(t *testing.T) {
	ctx := context.Background()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)

	t.Run("managed create records provider confirmation time", func(t *testing.T) {
		req := testAdmission("managed_created_at")
		if _, err := store.AdmitWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}
		op, claimed, err := store.ClaimProviderCreateForBurst(ctx, req.CustomerID, req.ClusterID, req.BurstID, time.Minute)
		if err != nil || !claimed {
			t.Fatalf("claim: claimed=%v err=%v", claimed, err)
		}
		before := time.Now().UTC()
		if err := store.MarkProviderCreateSucceeded(ctx, op.ID, op.LeaseToken, "linode_managed_created_at"); err != nil {
			t.Fatal(err)
		}
		after := time.Now().UTC()
		createdAt, err := store.GetBurstProviderCreatedAt(ctx, req.CustomerID, req.ClusterID, req.BurstID)
		if err != nil {
			t.Fatal(err)
		}
		if createdAt == nil || createdAt.Before(before) || createdAt.After(after) {
			t.Fatalf("provider_created_at = %v, want between %s and %s", createdAt, before, after)
		}
	})

	t.Run("compatibility booking preserves first truthful time on replay", func(t *testing.T) {
		req := testAdmission("compat_created_at")
		first := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
		booking := ProviderDeleteBooking{
			CustomerID:         req.CustomerID,
			ClusterID:          req.ClusterID,
			BurstID:            req.BurstID,
			Provider:           req.Provider,
			Region:             req.Region,
			CloudAccountID:     req.CloudAccountID,
			SKU:                req.SKU,
			ProviderResourceID: "linode_compat_created_at",
			ProviderCreatedAt:  first,
			Spec:               []byte(`{"ID":"` + req.BurstID + `"}`),
			Reason:             "workload completed",
			Payload:            []byte(`{"Burst":{"ID":"` + req.BurstID + `"}}`),
		}
		if _, err := store.RequestProviderDeleteForBooking(ctx, booking); err != nil {
			t.Fatal(err)
		}
		booking.ProviderCreatedAt = first.Add(time.Hour)
		if _, err := store.RequestProviderDeleteForBooking(ctx, booking); err != nil {
			t.Fatalf("replay booking: %v", err)
		}
		createdAt, err := store.GetBurstProviderCreatedAt(ctx, req.CustomerID, req.ClusterID, req.BurstID)
		if err != nil {
			t.Fatal(err)
		}
		if createdAt == nil || !createdAt.Equal(first) {
			t.Fatalf("provider_created_at = %v, want first receipt %s", createdAt, first)
		}
	})
}

// Replay is what a retried submission looks like at this seam. The same key with
// the same identity must return the same IDs and create nothing new; the same
// key with a DIFFERENT cloud account is a create routed to different credentials
// and must be refused, or a tenant's retry silently lands on another's account.
func TestPostgresAdmissionReplayIsKeyedOnTheCloudAccount(t *testing.T) {
	ctx := context.Background()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)

	req := testAdmission("replay_account")
	req.CloudAccountID = "ca_tenant_replay"
	first, err := store.AdmitWorkload(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Inserted {
		t.Fatal("first admission did not insert")
	}

	replay := req
	replay.WorkloadID = "wl_retry"
	replay.BurstID = "burst_retry"
	second, err := store.AdmitWorkload(ctx, replay)
	if err != nil {
		t.Fatalf("identical replay rejected: %v", err)
	}
	if second.Inserted || second.WorkloadID != first.WorkloadID || second.BurstID != first.BurstID {
		t.Fatalf("replay returned %+v, want the original %+v", second, first)
	}

	rerouted := req
	rerouted.CloudAccountID = "ca_other_tenant"
	if _, err := store.AdmitWorkload(ctx, rerouted); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("replay under a different cloud account = %v, want ErrIdempotencyConflict", err)
	}

	// A region that only differs by canonicalization is the SAME admission, not
	// a conflict: both spellings of "unspecified" have to hash identically or a
	// retry of an unpinned workload would be refused as a changed request.
	unpinned := testAdmission("replay_region")
	unpinned.Region = ""
	if _, err := store.AdmitWorkload(ctx, unpinned); err != nil {
		t.Fatal(err)
	}
	respelled := unpinned
	respelled.Region = "   "
	respelled.WorkloadID = "wl_region_retry"
	respelled.BurstID = "burst_region_retry"
	if _, err := store.AdmitWorkload(ctx, respelled); err != nil {
		t.Fatalf("replay of an unpinned region rejected: %v", err)
	}
	explicit := unpinned
	explicit.Region = unspecifiedProviderRegion
	explicit.WorkloadID = "wl_region_explicit"
	explicit.BurstID = "burst_region_explicit"
	if _, err := store.AdmitWorkload(ctx, explicit); err != nil {
		t.Fatalf("replay naming the canonical region rejected: %v", err)
	}
}

// newProjectionTestPool applies the same destructive-database guards the other
// lifecycle integration tests use, and installs the delete schema because these
// tests cross the create/delete seam.
func newProjectionTestPool(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("LIFECYCLE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set LIFECYCLE_TEST_DATABASE_URL to an isolated Postgres database")
	}
	if os.Getenv("LIFECYCLE_TEST_ALLOW_DESTRUCTIVE") != "DROP_LIFECYCLE_SCHEMA" {
		t.Fatal("set LIFECYCLE_TEST_ALLOW_DESTRUCTIVE=DROP_LIFECYCLE_SCHEMA for the isolated test database")
	}
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
	if err := EnsureProviderDeleteSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}
