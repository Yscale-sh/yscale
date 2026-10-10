//go:build integration

package handlers

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestPostgresDeleteBookingUsesCurrentDurableIdentity(t *testing.T) {
	for _, scenario := range []string{"updated", "removed", "outage", "cancelled", "invalid-id", "null", "uncached-with-unrelated-corruption"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			a := coreReadStore(t)
			suffix := fmt.Sprint(time.Now().UnixNano())
			id := "burst_delete_read_" + suffix
			original := state.Burst{ID: id, CustomerID: "customer_" + suffix, ClusterID: "cluster_original",
				Backend: "linode", BackendID: "synthetic-old", Region: "us-east", SKU: "synthetic-sku",
				CloudAccountID: "account_original", CreatedAt: time.Now().UTC()}
			if scenario != "uncached-with-unrelated-corruption" {
				if err := a.PutBurst(&original); err != nil {
					t.Fatal(err)
				}
			}
			b := coreReadStore(t)
			pool, err := pgxpool.New(ctx, os.Getenv("YSCALE_TEST_DATABASE_URL"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			t.Cleanup(func() {
				// Remove only this subtest's generated rows, including malformed
				// JSON fixtures that would otherwise poison the next boot read.
				if _, err := pool.Exec(context.Background(), `DELETE FROM bursts WHERE id=$1 OR id=$2`, id, id+"_unrelated"); err != nil {
					t.Error(err)
				}
			})
			current := original
			current.BackendID, current.Region, current.CloudAccountID = "synthetic-current", "us-west", "account_current"
			current.ClusterID, current.SKU = "cluster_current", "synthetic-current-sku"
			want := ReapOutcomeUnknown
			switch scenario {
			case "updated", "uncached-with-unrelated-corruption":
				if err := a.PutBurst(&current); err != nil {
					t.Fatal(err)
				}
				want = ReapOutcomeDeletePending
				if scenario == "uncached-with-unrelated-corruption" {
					if _, err := pool.Exec(ctx, `INSERT INTO bursts (id,data) VALUES ($1,'{"CreatedAt":false}'::jsonb)`, id+"_unrelated"); err != nil {
						t.Fatal(err)
					}
				}
			case "removed":
				if err := a.DeleteBurst(id); err != nil {
					t.Fatal(err)
				}
				want = ReapOutcomeNotOwned
			case "outage":
				b.Close()
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "invalid-id":
				if _, err := pool.Exec(ctx, `UPDATE bursts SET data=jsonb_set(data,'{ID}','"different-burst"') WHERE id=$1`, id); err != nil {
					t.Fatal(err)
				}
			case "null":
				if _, err := pool.Exec(ctx, `UPDATE bursts SET data='null'::jsonb WHERE id=$1`, id); err != nil {
					t.Fatal(err)
				}
			}
			deletes, reaper := newFakeProviderDeletes(), &fakeReaper{}
			h := &Workloads{Store: b, Deletes: deletes, Reaper: reaper, Log: quietLog()}
			if got := h.ReapBurst(ctx, id, "workload cancelled"); got != want {
				t.Fatalf("reap outcome=%v, want %v; delete requests=%d", got, want, deletes.requests)
			}
			if want == ReapOutcomeDeletePending {
				record := deletes.get(id)
				if record.ProviderResourceID != current.BackendID || record.Region != current.Region ||
					record.CloudAccountID != current.CloudAccountID || record.ClusterID != current.ClusterID || record.SKU != current.SKU {
					t.Error("delete intent used stale provider identity")
				}
				job, err := decodeProviderDeleteJob(record.Payload)
				if err != nil || job.Burst.BackendID != current.BackendID || job.Burst.CloudAccountID != current.CloudAccountID {
					t.Error("worker payload used stale provider identity")
				}
			} else if deletes.requests != 0 {
				t.Error("failed or absent durable read reached delete intent")
			}
			if reaper.teardownCount(id) != 0 {
				t.Error("booking lookup performed inline teardown")
			}
			cached, err := b.GetBurst(id)
			if scenario == "uncached-with-unrelated-corruption" {
				if err == nil {
					t.Error("durable read hydrated the mutation cache")
				}
			} else if err != nil || cached.BackendID != original.BackendID {
				t.Error("durable read changed the existing mutation cache")
			}
		})
	}
}
