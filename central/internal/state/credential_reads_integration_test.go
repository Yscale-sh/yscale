//go:build integration

package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgresCredentialReadContracts(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	ctx := context.Background()
	pool := freshSchemaPool(t, dsn, fmt.Sprintf("credential_reads_%d", time.Now().UnixNano()), 4)
	p := &pgPersister{pool: pool, customerCredentialCipher: testCustomerCredentialCipher(t, 0x73)}
	if err := p.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	s := emptyStore()
	s.persist = p
	const token = "synthetic-credential-read-token"
	original := &Customer{ID: "credential-owner", Token: token,
		Mesh: &MeshEndpoint{Provider: "box", APIKey: "synthetic-original-mesh-key"},
	}
	s.AddCustomer(original)
	updated := &Customer{ID: original.ID, TokenHash: HashCustomerToken(token),
		Mesh: &MeshEndpoint{Provider: "box", APIKey: "synthetic-updated-mesh-key"},
	}
	put := func(c *Customer) {
		t.Helper()
		// These deliberate replacement/corruption fixtures are based on the
		// current row; a blind stale replacement is no longer a valid writer.
		err := pool.QueryRow(ctx, "SELECT data::text FROM customers WHERE id=$1", c.ID).Scan(&c.persistedData)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		if err := p.upsertCustomer(c); err != nil {
			t.Fatal(err)
		}
	}
	rowData := func() string {
		t.Helper()
		var data string
		if err := pool.QueryRow(ctx, "SELECT data::text FROM customers WHERE id=$1", original.ID).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	put(updated)
	before := rowData()
	c, err := s.AuthCustomerContext(ctx, token)
	if err != nil || c.Mesh == nil || c.Mesh.APIKey != updated.Mesh.APIKey {
		t.Fatalf("durable encrypted mesh resolution failed: %v", err)
	}
	if rowData() != before || original.Mesh.APIKey != "synthetic-original-mesh-key" || s.customers[original.ID] != original {
		t.Fatal("authentication rewrote durable data or hydrated the mutation cache")
	}

	t.Run("duplicate-verifier", func(t *testing.T) {
		put(&Customer{ID: "duplicate-owner", TokenHash: updated.TokenHash})
		if _, err := s.AuthCustomer(token); !errors.Is(err, ErrNotFound) {
			t.Errorf("ambiguous tenant verifier = %v, want no grant", err)
		}
		if _, err := pool.Exec(ctx, "DELETE FROM customers WHERE id=$1", "duplicate-owner"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("duplicate-scoped-verifier", func(t *testing.T) {
		duplicate := *updated
		duplicate.RegisteredClusters = []*RegisteredCluster{
			{ClusterID: "first", CredentialHash: HashClusterCredential("synthetic-connector")},
			{ClusterID: "second", CredentialHash: HashClusterCredential("synthetic-connector")},
		}
		duplicate.CatalogPublishers = []*CatalogPublisher{
			{ID: "first", CredentialHash: HashCatalogPublisherCredential("synthetic-publisher")},
			{ID: "second", CredentialHash: HashCatalogPublisherCredential("synthetic-publisher")},
		}
		put(&duplicate)
		if _, _, err := s.AuthClusterCredential("synthetic-connector"); !errors.Is(err, ErrNotFound) {
			t.Errorf("ambiguous connector = %v, want no grant", err)
		}
		if _, _, err := s.AuthCatalogPublisher("synthetic-publisher"); !errors.Is(err, ErrNotFound) {
			t.Errorf("ambiguous publisher = %v, want no grant", err)
		}
		put(updated)
	})
	t.Run("null-and-non-array-registries", func(t *testing.T) {
		for _, value := range []string{"null", `{}`, `"not-an-array"`} {
			if _, err := pool.Exec(ctx, "UPDATE customers SET data=jsonb_set(jsonb_set(data,'{RegisteredClusters}',$2::jsonb),'{CatalogPublishers}',$2::jsonb) WHERE id=$1", original.ID, value); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.AuthClusterCredential("synthetic-connector"); !errors.Is(err, ErrNotFound) {
				t.Errorf("invalid registry connector = %v", err)
			}
			if _, _, err := s.AuthCatalogPublisher("synthetic-publisher"); !errors.Is(err, ErrNotFound) {
				t.Errorf("invalid registry publisher = %v", err)
			}
		}
		put(updated)
	})
	t.Run("cipher-failure-does-not-use-cache", func(t *testing.T) {
		broken := *updated
		broken.Mesh = &MeshEndpoint{Provider: "box", APIKeyCiphertext: "invalid-test-envelope"}
		put(&broken)
		if _, err := s.AuthCustomer(token); !errors.Is(err, ErrPersistence) {
			t.Errorf("invalid credential envelope = %v, want backend failure", err)
		}
		put(updated)
	})
	t.Run("legacy-material-requires-migration", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "UPDATE customers SET data=jsonb_set(data,'{Token}',to_jsonb($2::text)) WHERE id=$1", original.ID, token); err != nil {
			t.Fatal(err)
		}
		before := rowData()
		if _, err := s.AuthCustomer(token); !errors.Is(err, ErrPersistence) {
			t.Errorf("unmigrated credential = %v", err)
		}
		if rowData() != before {
			t.Fatal("authentication performed a migration write")
		}
		put(updated)
	})
	t.Run("row-owner-mismatch", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "UPDATE customers SET data=jsonb_set(data,'{ID}',to_jsonb($2::text)) WHERE id=$1", original.ID, "different-owner"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AuthCustomer(token); !errors.Is(err, ErrPersistence) {
			t.Errorf("mismatched row owner = %v", err)
		}
		put(updated)
	})
	t.Run("tombstone-denies-an-existing-row", func(t *testing.T) {
		row := *updated
		row.RegisteredClusters = []*RegisteredCluster{{ClusterID: "retired", CredentialHash: HashClusterCredential("synthetic-connector")}}
		row.CatalogPublishers = []*CatalogPublisher{{ID: "retired", CredentialHash: HashCatalogPublisherCredential("synthetic-publisher")}}
		put(&row)
		if _, err := pool.Exec(ctx, "INSERT INTO customer_tombstones(id) VALUES($1)", original.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AuthCustomer(token); !errors.Is(err, ErrNotFound) {
			t.Errorf("tombstoned tenant = %v", err)
		}
		if _, _, err := s.AuthClusterCredential("synthetic-connector"); !errors.Is(err, ErrNotFound) {
			t.Errorf("tombstoned connector = %v", err)
		}
		if _, _, err := s.AuthCatalogPublisher("synthetic-publisher"); !errors.Is(err, ErrNotFound) {
			t.Errorf("tombstoned publisher = %v", err)
		}
	})
	var indexCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema()
		AND indexname IN ('customers_token_hash','customers_cluster_credentials','customers_publisher_credentials')`).Scan(&indexCount); err != nil || indexCount != 3 {
		t.Fatalf("credential indexes = %d, err=%v", indexCount, err)
	}
}
