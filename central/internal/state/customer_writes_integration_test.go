//go:build integration

package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

const customerWriteTenant = "synthetic-write-owner"
const customerWriteToken = "synthetic-write-old-token"

func customerWriteFixture(t *testing.T) (*Store, *pgPersister, string) {
	t.Helper()
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	pool := freshSchemaPool(t, dsn, fmt.Sprintf("customer_writes_%d", time.Now().UnixNano()), 4)
	p := &pgPersister{pool: pool, customerCredentialCipher: testCustomerCredentialCipher(t, 0x74)}
	if err := p.ensureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	a := emptyStore()
	a.persist = p
	owner, err := a.UpsertAccount("synthetic-write-issuer", "synthetic-write-subject", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = a.CreateTenant(&Customer{ID: customerWriteTenant, Token: customerWriteToken,
		RegisteredClusters: []*RegisteredCluster{{ClusterID: "synthetic-cluster", CredentialHash: HashClusterCredential("synthetic-connector")}},
		CatalogPublishers:  []*CatalogPublisher{{ID: "synthetic-publisher", Name: "Publisher", CredentialHash: HashCatalogPublisherCredential("synthetic-publisher-token")}},
		Mesh:               &MeshEndpoint{Provider: "box", APIKey: "synthetic-write-mesh-key"},
		LinodeCloudAccount: &CloudAccount{ID: "synthetic-cloud-account"},
	}, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	return a, p, owner.ID
}

func customerWriteReplica(t *testing.T, p *pgPersister) *Store {
	t.Helper()
	q := &pgPersister{pool: p.pool, customerCredentialCipher: p.customerCredentialCipher}
	snap, err := q.loadAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := q.prepareLoadedCustomerCredentials(snap.Customers); err != nil {
		t.Fatal(err)
	}
	b := emptyStore()
	b.persist = q
	b.applySnapshot(snap)
	return b
}

func customerWriteAuditCount(t *testing.T, p *pgPersister) int {
	t.Helper()
	var n int
	if err := p.pool.QueryRow(context.Background(), "SELECT count(*) FROM tenant_audit WHERE customer_id=$1", customerWriteTenant).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPostgresStaleCustomerWrites(t *testing.T) {
	for _, operation := range []string{"audited-settings", "connector-disconnect", "connector-claim", "gateway-report", "cloud-disconnect"} {
		t.Run(operation, func(t *testing.T) {
			a, p, owner := customerWriteFixture(t)
			b := customerWriteReplica(t, p)
			newToken, err := a.RotateCustomerCredential(customerWriteTenant, OperatorActor())
			if err != nil {
				t.Fatal(err)
			}
			beforeAudit := customerWriteAuditCount(t, p)
			write := func() error {
				switch operation {
				case "audited-settings":
					_, _, err := b.SetCustomerLimits(customerWriteTenant, 3, 7, HumanActor(owner, customerWriteTenant))
					return err
				case "connector-disconnect":
					b.MarkClusterDisconnected(customerWriteTenant, "synthetic-cluster", time.Now(), time.Now())
					return nil // this best-effort metadata API has no error result
				case "connector-claim":
					_, _, err := b.ClaimAgentCluster(customerWriteTenant, "synthetic-cluster", true, time.Now())
					return err
				case "gateway-report":
					_, err := b.SetCustomerGatewayRoutes(customerWriteTenant, "synthetic-cluster", []string{"10.83.0.0/24"})
					return err
				default:
					return b.DisconnectLinodeCloudAccount(customerWriteTenant, "synthetic-cloud-account", time.Now(), OperatorActor())
				}
			}
			err = write()
			if operation != "connector-disconnect" && !errors.Is(err, ErrPersistence) {
				t.Errorf("stale whole-customer write = %v; want retryable refusal", err)
			}
			if customerWriteAuditCount(t, p) != beforeAudit {
				t.Error("refused write committed a phantom audit event")
			}
			if _, err := a.AuthCustomer(customerWriteToken); !errors.Is(err, ErrNotFound) {
				t.Errorf("old tenant credential after stale write = %v; want denied", err)
			}
			if _, err := b.AuthCustomer(newToken); err != nil {
				t.Errorf("new tenant credential after stale write = %v; want accepted", err)
			}
			if err := write(); err != nil {
				t.Fatalf("retry after refresh: %v", err)
			}
			current, err := b.AuthCustomer(newToken)
			if err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "audited-settings":
				if current.MaxConcurrentBursts != 3 || current.MaxHourlyUSD != 7 {
					t.Error("retry did not persist the requested limits")
				}
			case "connector-disconnect":
				if current.RegisteredClusters[0].LastDisconnectedAt == nil {
					t.Error("retry did not persist disconnect metadata")
				}
			case "connector-claim":
				if current.RegisteredClusters[0].LastConnectedAt == nil {
					t.Error("retry did not persist connection metadata")
				}
			case "gateway-report":
				if len(current.GatewayRoutes) != 1 || current.GatewayRoutes[0] != "10.83.0.0/24" {
					t.Error("retry did not persist gateway routes")
				}
			case "cloud-disconnect":
				if current.LinodeCloudAccount != nil {
					t.Error("retry did not remove the cloud account")
				}
			}
			if _, _, err := b.SetCustomerLimits(customerWriteTenant, 4, 8, OperatorActor()); err != nil {
				t.Fatalf("next write did not inherit the committed precondition: %v", err)
			}
			if current.Mesh == nil || current.Mesh.APIKey != "synthetic-write-mesh-key" {
				t.Error("refresh lost the authenticated mesh credential")
			}
		})
	}
}

func TestPostgresCustomerWriteCannotRestoreRemovedConnector(t *testing.T) {
	a, p, owner := customerWriteFixture(t)
	b := customerWriteReplica(t, p)
	if _, err := a.DeleteTenantCluster(customerWriteTenant, owner, "synthetic-cluster", HumanActor(owner, customerWriteTenant)); err != nil {
		t.Fatal(err)
	}
	before := customerWriteAuditCount(t, p)
	b.MarkClusterDisconnected(customerWriteTenant, "synthetic-cluster", time.Now(), time.Now())
	// Once refreshed, a later disconnect is a no-op, not a re-creation.
	b.MarkClusterDisconnected(customerWriteTenant, "synthetic-cluster", time.Now(), time.Now())
	if _, _, err := b.AuthClusterCredential("synthetic-connector"); !errors.Is(err, ErrNotFound) {
		t.Errorf("removed connector = %v; want denied", err)
	}
	if len(b.clusterCreds) != 0 || len(b.clusterOwners) != 0 {
		t.Error("conflict refresh retained removed connector indexes")
	}
	if customerWriteAuditCount(t, p) != before {
		t.Error("disconnect added an audit event")
	}
	if _, _, err := b.SetCustomerLimits(customerWriteTenant, 2, 6, HumanActor(owner, customerWriteTenant)); err != nil {
		t.Fatalf("refresh lost the active owner's membership: %v", err)
	}
}

func TestPostgresCustomerWriteConcurrentUpdates(t *testing.T) {
	a, p, _ := customerWriteFixture(t)
	b := customerWriteReplica(t, p)
	writers := []func() error{
		func() error { _, _, err := a.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); return err },
		func() error {
			_, err := b.SetCustomerWorkloadNamespaces(customerWriteTenant, []string{"batch"}, OperatorActor())
			return err
		},
	}
	before := customerWriteAuditCount(t, p)
	type result struct {
		index int
		err   error
	}
	results, start := make(chan result, 2), make(chan struct{})
	for i, write := range writers {
		go func() { <-start; results <- result{i, write()} }()
	}
	close(start)
	succeeded, loser := 0, -1
	for range writers {
		r := <-results
		if r.err == nil {
			succeeded++
		} else if errors.Is(r.err, ErrPersistence) {
			loser = r.index
		} else {
			t.Fatalf("concurrent write: %v", r.err)
		}
	}
	if succeeded != 1 || loser < 0 {
		t.Fatalf("concurrent writes: %d accepted, loser=%d", succeeded, loser)
	}
	if customerWriteAuditCount(t, p) != before+1 {
		t.Fatal("conflict appended an audit event")
	}
	if err := writers[loser](); err != nil {
		t.Fatalf("retry: %v", err)
	}
	current, err := a.AuthCustomer(customerWriteToken)
	if err != nil {
		t.Fatal(err)
	}
	if current.MaxConcurrentBursts != 2 || current.MaxHourlyUSD != 6 || len(current.WorkloadNamespaces) != 1 || current.WorkloadNamespaces[0] != "batch" {
		t.Error("retry lost one of the independent updates")
	}
	if customerWriteAuditCount(t, p) != before+2 {
		t.Error("accepted writes did not each append exactly one audit")
	}
}

func TestPostgresCustomerWriteAuditRollback(t *testing.T) {
	a, p, _ := customerWriteFixture(t)
	live, _ := a.CustomerByID(customerWriteTenant)
	before := *live
	ctx := context.Background()
	// Existing rows are retained; only this private fixture's new audit inserts
	// fail. This forces the failure AFTER the customer UPDATE in its transaction.
	if _, err := p.pool.Exec(ctx, "ALTER TABLE tenant_audit ADD CONSTRAINT customer_write_fixture_reject CHECK (false) NOT VALID"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("audit failure = %v", err)
	}
	after, _ := a.CustomerByID(customerWriteTenant)
	current, err := a.AuthCustomer(customerWriteToken)
	if err != nil {
		t.Fatal(err)
	}
	if before.persistedData != after.persistedData || current.MaxConcurrentBursts != 0 || after.MaxConcurrentBursts != 0 {
		t.Error("rollback advanced the working copy or durable row")
	}
	if _, err := p.pool.Exec(ctx, "ALTER TABLE tenant_audit DROP CONSTRAINT customer_write_fixture_reject"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
}

func TestPostgresCustomerWriteSnapshotProvenance(t *testing.T) {
	a, p, _ := customerWriteFixture(t)
	live, _ := a.CustomerByID(customerWriteTenant)
	stale := *live
	if _, _, err := a.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	stale.Name = "stale copy"
	var conflict *customerWriteConflict
	if err := p.upsertCustomer(&stale); !errors.As(err, &conflict) {
		t.Fatalf("old copy inherited a newer precondition: %v", err)
	}
	if err := p.upsertCustomer(&Customer{ID: customerWriteTenant, Token: "synthetic-other-token"}); !errors.As(err, &conflict) {
		t.Fatalf("unloaded replacement = %v; want refused", err)
	}
	if _, err := a.AuthCustomer(customerWriteToken); err != nil {
		t.Fatal("blind replacement changed credentials")
	}
	// A loaded copy is update-only even if an out-of-band row removal omitted a
	// tombstone. This synthetic deletion is scoped to the isolated test schema.
	if _, err := p.pool.Exec(context.Background(), "DELETE FROM customers WHERE id=$1", customerWriteTenant); err != nil {
		t.Fatal(err)
	}
	if err := p.upsertCustomer(&stale); !errors.As(err, &conflict) || conflict.current != nil {
		t.Fatalf("stale update recreated missing row: %v", err)
	}
	if _, err := a.AuthCustomer(customerWriteToken); !errors.Is(err, ErrNotFound) {
		t.Error("missing row was re-created")
	}
}

func TestPostgresCustomerWriteMigrationCannotRestoreCredentials(t *testing.T) {
	_, p, _ := customerWriteFixture(t)
	ctx := context.Background()
	const id = "synthetic-legacy-writer"
	const legacy = `{"ID":"synthetic-legacy-writer","Token":"synthetic-legacy-token","Mesh":{"Provider":"box","APIKey":"synthetic-legacy-mesh-key"}}`
	if _, err := p.pool.Exec(ctx, "INSERT INTO customers (id,data) VALUES ($1,$2::jsonb)", id, legacy); err != nil {
		t.Fatal(err)
	}
	q := &pgPersister{pool: p.pool, customerCredentialCipher: p.customerCredentialCipher}
	stale, err := q.loadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a := customerWriteReplica(t, p) // startup migration commits first
	newToken, err := a.RotateCustomerCredential(id, OperatorActor())
	if err != nil {
		t.Fatal(err)
	}
	var conflict *customerWriteConflict
	if err := q.prepareLoadedCustomerCredentials(stale.Customers); !errors.As(err, &conflict) {
		t.Fatalf("stale startup migration = %v; want refused", err)
	}
	if _, err := a.AuthCustomer("synthetic-legacy-token"); !errors.Is(err, ErrNotFound) {
		t.Error("stale migration restored legacy credential")
	}
	if _, err := a.AuthCustomer(newToken); err != nil {
		t.Error("stale migration replaced rotated credential")
	}
	var plaintext bool
	if err := p.pool.QueryRow(ctx, "SELECT data ? 'Token' OR (data->'Mesh') ? 'APIKey' FROM customers WHERE id=$1", id).Scan(&plaintext); err != nil {
		t.Fatal(err)
	}
	if plaintext {
		t.Error("migration retained durable plaintext")
	}
}

func TestPostgresCustomerWriteInvalidRefreshFailsClosed(t *testing.T) {
	for _, corruption := range []string{"mesh-envelope", "cluster-registry", "publisher-registry", "owner-identity"} {
		t.Run(corruption, func(t *testing.T) {
			a, p, _ := customerWriteFixture(t)
			path, value := "{Mesh,APIKeyCiphertext}", `"invalid-envelope"`
			switch corruption {
			case "cluster-registry":
				path, value = "{RegisteredClusters}", `[null]`
			case "publisher-registry":
				path, value = "{CatalogPublishers}", `[null]`
			case "owner-identity":
				path, value = "{ID}", `"synthetic-wrong-owner"`
			}
			if _, err := p.pool.Exec(context.Background(), "UPDATE customers SET data=jsonb_set(data,$2::text[],$3::jsonb) WHERE id=$1", customerWriteTenant, path, value); err != nil {
				t.Fatal(err)
			}
			if _, _, err := a.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); !errors.Is(err, ErrPersistence) {
				t.Fatalf("invalid refresh = %v; want refused", err)
			}
			var restored bool
			if err := p.pool.QueryRow(context.Background(), "SELECT data #> $2::text[] <> $3::jsonb FROM customers WHERE id=$1", customerWriteTenant, path, value).Scan(&restored); err != nil {
				t.Fatal(err)
			}
			if restored {
				t.Error("stale write replaced invalid durable state")
			}
		})
	}
}

func TestPostgresCustomerWriteScopedConnectorRotation(t *testing.T) {
	a, p, owner := customerWriteFixture(t)
	b := customerWriteReplica(t, p)
	_, connectorToken, _, err := a.RotateTenantClusterCredential(customerWriteTenant, owner, "synthetic-cluster", HumanActor(owner, customerWriteTenant))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("stale write = %v", err)
	}
	if _, _, err := b.AuthClusterCredential("synthetic-connector"); !errors.Is(err, ErrNotFound) {
		t.Error("old connector credential restored")
	}
	if _, _, err := b.AuthClusterCredential(connectorToken); err != nil {
		t.Error("new connector credential lost")
	}
	if _, ok := b.clusterCreds[HashClusterCredential(connectorToken)]; !ok {
		t.Error("connector index not refreshed")
	}
	if _, _, err := b.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

func TestPostgresCustomerWriteRevocationAndFinalization(t *testing.T) {
	a, p, _ := customerWriteFixture(t)
	b := customerWriteReplica(t, p)
	if err := a.RevokeCustomer(customerWriteTenant); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("stale revoked write = %v", err)
	}
	if len(b.membershipsByTenant) != 0 || len(b.membershipsByAccount) != 0 || len(b.customersByTok) != 0 {
		t.Error("revocation refresh retained grants")
	}
	if _, _, err := b.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry on revoked tenant = %v", err)
	}
	if err := b.SetCustomerMesh(customerWriteTenant, nil); err != nil {
		t.Fatalf("operator cleanup after refresh: %v", err)
	}
	if err := a.DeleteRevokedCustomer(customerWriteTenant); err != nil {
		t.Fatal(err)
	}
	b.MarkClusterDisconnected(customerWriteTenant, "synthetic-cluster", time.Now(), time.Now())
	if !b.tombstoned[customerWriteTenant] || b.customers[customerWriteTenant] != nil || len(b.clusterCreds) != 0 || len(b.catalogPublisherCreds) != 0 {
		t.Error("finalization refresh did not retire the local customer and indexes")
	}
}

func TestPostgresCustomerWriteCloudDisconnectRefresh(t *testing.T) {
	a, p, _ := customerWriteFixture(t)
	b := customerWriteReplica(t, p)
	if err := a.DisconnectLinodeCloudAccount(customerWriteTenant, "synthetic-cloud-account", time.Now(), OperatorActor()); err != nil {
		t.Fatal(err)
	}
	if err := b.DisconnectLinodeCloudAccount(customerWriteTenant, "synthetic-cloud-account", time.Now(), OperatorActor()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("stale disconnect = %v", err)
	}
	if err := b.DisconnectLinodeCloudAccount(customerWriteTenant, "synthetic-cloud-account", time.Now(), OperatorActor()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry after removal = %v", err)
	}
}

func TestPostgresCustomerWriteRevokeFallbackPreservesCurrentFields(t *testing.T) {
	a, p, _ := customerWriteFixture(t)
	live, _ := a.CustomerByID(customerWriteTenant)
	stale := *live
	if _, _, err := a.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	stale.RevokedAt = &now
	data, err := p.customerData(&stale)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the exact ON CONFLICT arm used when a row appeared between
	// revokeCustomer's UPDATE miss and its fallback INSERT.
	if _, err := p.pool.Exec(context.Background(), insertRevokedCustomerStmt(tblCustomers, tblTombstones), customerWriteTenant, string(data)); err != nil {
		t.Fatal(err)
	}
	var revoked bool
	var limit int
	if err := p.pool.QueryRow(context.Background(), "SELECT data->'RevokedAt' <> 'null'::jsonb, (data->>'MaxConcurrentBursts')::int FROM customers WHERE id=$1", customerWriteTenant).Scan(&revoked, &limit); err != nil {
		t.Fatal(err)
	}
	if !revoked || limit != 2 {
		t.Error("revoke fallback replaced fields from the concurrent writer")
	}
}
