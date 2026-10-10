package state

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/cost"
)

// TestApplySnapshotRebuildsIndexes covers the reload path the Postgres backend
// feeds (loadAll → applySnapshot) WITHOUT needing a database: the derived
// indexes (token, PV cache-key, PV name) must be rebuilt from the primary
// records, or a reloaded store would 404 lookups that worked before restart.
func TestApplySnapshotRebuildsIndexes(t *testing.T) {
	s := emptyStore()
	s.applySnapshot(&snapshot{
		Customers: []*Customer{{ID: "cust_a", Token: "tok_a", Plan: "pro"}},
		Bursts:    []*Burst{{ID: "burst_a", CustomerID: "cust_a"}},
		PVs: []*PersistentVolume{
			{ID: "pv_a", TenantID: "cust_a", CacheKey: "ck1", Name: "vol1", State: "ready"},
		},
	})
	if _, err := s.AuthCustomer("tok_a"); err != nil {
		t.Errorf("token index not rebuilt: %v", err)
	}
	if _, err := s.GetBurst("burst_a"); err != nil {
		t.Errorf("burst not loaded: %v", err)
	}
	if s.pvsByCacheKey["cust_a"]["ck1"] == nil {
		t.Error("PV cache-key index not rebuilt")
	}
	if s.pvsByName["cust_a"]["vol1"] == nil {
		t.Error("PV name index not rebuilt")
	}
}

// TestEntityJSONRoundTrip validates the JSONB encoding the Postgres backend
// relies on: each entity must survive json.Marshal → json.Unmarshal intact,
// including time pointers, the customer mesh, and the workload's []byte spec.
// A field that doesn't round-trip would silently corrupt durable state.
func TestEntityJSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	cust := &Customer{ID: "c", Token: "t", Plan: "pro",
		Mesh: &MeshEndpoint{Provider: "box", LoginServer: "https://b", APIKey: "k", User: "u", BackendID: "9"}}
	var c2 Customer
	mustRoundTrip(t, cust, &c2)
	if !c2.HasMeshBox() || c2.Mesh.LoginServer != "https://b" {
		t.Errorf("customer mesh lost in round-trip: %+v", c2.Mesh)
	}

	burst := &Burst{ID: "b", CustomerID: "c", Backend: "linode", CreatedAt: now, HourlyUSD: 0.62, Deadline: time.Hour}
	var b2 Burst
	mustRoundTrip(t, burst, &b2)
	if !b2.CreatedAt.Equal(now) || b2.HourlyUSD != 0.62 || b2.Deadline != time.Hour {
		t.Errorf("burst fields lost in round-trip: %+v", b2)
	}

	wl := &Workload{ID: "w", CustomerID: "c", Status: "Running", FinishedAt: &now, SpecYAML: []byte("kind: Workload")}
	var w2 Workload
	mustRoundTrip(t, wl, &w2)
	if w2.FinishedAt == nil || string(w2.SpecYAML) != "kind: Workload" {
		t.Errorf("workload fields lost in round-trip: %+v", w2)
	}
}

func mustRoundTrip(t *testing.T, in, out any) {
	t.Helper()
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}

// TestInMemoryNoPersister documents that New() is pure in-memory: no durability
// backend, mutations never panic, and Close() is a safe no-op. This is the
// store every unit test and OSS-local run uses.
func TestInMemoryNoPersister(t *testing.T) {
	s := New()
	if s.persist != nil {
		t.Fatal("New() must have no persister")
	}
	s.PutBurst(&Burst{ID: "b", CustomerID: "cust_test"})
	if _, err := s.GetBurst("b"); err != nil {
		t.Fatalf("in-memory put/get failed: %v", err)
	}
	s.Close() // must not panic on a persister-less store
}

// TestPostgresRoundTrip proves the durability goal against a real Postgres: a
// fresh Store built from the same DSN recovers customers (with mesh), in-flight
// workloads, and bursts — and deletes / mesh-clears persist too. Mirrors the
// old JSON round-trip test, now backed by Postgres.
//
// Gated on YSCALE_TEST_DATABASE_URL (a throwaway test database) so the default
// `go test` run needs no database. IDs are fixed + idempotent so reruns against
// a reused database are safe.
func TestPostgresRoundTrip(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	cipher := testCustomerCredentialCipher(t, 0x43)

	s1, err := NewPostgresWithCredentialCipher(ctx, dsn, cipher)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	s1.AddCustomer(&Customer{
		ID: "cust_pt", Token: "tok_pt", Plan: "pro",
		Mesh: &MeshEndpoint{Provider: "box", LoginServer: "https://box", APIKey: "k", User: "u"},
	})
	s1.PutBurst(&Burst{
		ID: "burst_pt", CustomerID: "cust_pt", Backend: "linode", BackendID: "999",
		CreatedAt: now, Status: "provisioning", HourlyUSD: 0.62,
	})
	s1.PutWorkload(&Workload{
		ID: "wl_pt", CustomerID: "cust_pt", Status: "Running", BurstID: "burst_pt",
		CreatedAt: now, SpecYAML: []byte("kind: Workload"),
	})
	s1.Close()

	// A brand-new Store from the same DSN must recover everything.
	s2, err := NewPostgresWithCredentialCipher(ctx, dsn, cipher)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	c, err := s2.CustomerByID("cust_pt")
	if err != nil {
		t.Fatalf("customer not reloaded: %v", err)
	}
	if !c.HasMeshBox() || c.Mesh.LoginServer != "https://box" {
		t.Fatalf("mesh not reloaded: %+v", c.Mesh)
	}
	if _, err := s2.AuthCustomer("tok_pt"); err != nil {
		t.Fatalf("token index not rebuilt on load: %v", err)
	}
	b, err := s2.GetBurst("burst_pt")
	if err != nil {
		t.Fatalf("burst not reloaded (would be stranded): %v", err)
	}
	if b.Backend != "linode" || b.HourlyUSD != 0.62 || !b.CreatedAt.Equal(now) {
		t.Fatalf("burst fields wrong after reload: %+v", b)
	}
	w, err := s2.GetWorkload("wl_pt")
	if err != nil {
		t.Fatalf("workload not reloaded: %v", err)
	}
	if string(w.SpecYAML) != "kind: Workload" {
		t.Fatalf("workload SpecYAML wrong after reload: %q", w.SpecYAML)
	}

	// Deletes and mesh-clears persist.
	s2.DeleteBurst("burst_pt")
	if err := s2.SetCustomerMesh("cust_pt", nil); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}
	s2.Close()

	s3, err := NewPostgresWithCredentialCipher(ctx, dsn, cipher)
	if err != nil {
		t.Fatalf("reload after delete/clear: %v", err)
	}
	defer s3.Close()
	if _, err := s3.GetBurst("burst_pt"); err == nil {
		t.Fatal("deleted burst should not reload")
	}
	c3, err := s3.CustomerByID("cust_pt")
	if err != nil {
		t.Fatalf("customer lost after mesh clear: %v", err)
	}
	if c3.HasMeshBox() {
		t.Fatal("cleared mesh should stay cleared after reload")
	}
}

func TestPostgresMigratesCustomerCredentialsAndRawRowsContainNoPlaintext(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	const customerID = "cust_credential_migration_pg"
	const token = "legacy_postgres_customer_token"
	const apiKey = "legacy_postgres_mesh_admin_key"
	ctx := context.Background()
	cipher := testCustomerCredentialCipher(t, 0x43)

	bootstrap, err := NewPostgresWithCredentialCipher(ctx, dsn, cipher)
	if err != nil {
		t.Fatal(err)
	}
	pool := bootstrap.persist.(*pgPersister).pool
	if _, err := pool.Exec(ctx, `DELETE FROM `+tblCustomers+` WHERE id = $1`, customerID); err != nil {
		bootstrap.Close()
		t.Fatal(err)
	}
	legacy, err := json.Marshal(map[string]any{
		"ID": customerID, "Token": token, "Plan": "pro",
		"Mesh": map[string]any{"Provider": "box", "LoginServer": "https://mesh.example", "APIKey": apiKey, "User": "tenant"},
	})
	if err != nil {
		bootstrap.Close()
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO `+tblCustomers+` (id, data, updated_at) VALUES ($1, $2, now())`, customerID, string(legacy)); err != nil {
		bootstrap.Close()
		t.Fatal(err)
	}
	bootstrap.Close()
	t.Cleanup(func() {
		cleanup, err := NewPostgresWithCredentialCipher(context.Background(), dsn, cipher)
		if err == nil {
			_, _ = cleanup.persist.(*pgPersister).pool.Exec(context.Background(), `DELETE FROM `+tblCustomers+` WHERE id = $1`, customerID)
			cleanup.Close()
		}
	})

	migrated, err := NewPostgresWithCredentialCipher(ctx, dsn, cipher)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	if got, err := migrated.AuthCustomer(token); err != nil || got.ID != customerID {
		t.Fatalf("auth after migration = (%v, %v)", got, err)
	}
	customer, err := migrated.CustomerByID(customerID)
	if err != nil || customer.Mesh == nil || customer.Mesh.APIKey != apiKey {
		t.Fatalf("mesh after migration = (%+v, %v)", customer, err)
	}
	var raw string
	if err := migrated.persist.(*pgPersister).pool.QueryRow(ctx,
		`SELECT data::text FROM `+tblCustomers+` WHERE id = $1`, customerID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{token, apiKey, `"Token":`, `"APIKey":`} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("raw customer row contains plaintext credential marker %q: %s", forbidden, raw)
		}
	}
	for _, required := range []string{`"TokenHash"`, `"APIKeyCiphertext"`} {
		if !strings.Contains(raw, required) {
			t.Fatalf("raw customer row lacks %s: %s", required, raw)
		}
	}

	wrongCipher := testCustomerCredentialCipher(t, 0x44)
	if wrong, err := NewPostgresWithCredentialCipher(ctx, dsn, wrongCipher); err == nil {
		wrong.Close()
		t.Fatal("Postgres opened encrypted customer credentials with the wrong master key")
	}
}

// burstFailPersister is a persister whose burst write-throughs fail on demand.
// Only the burst methods carry state: bursts are the one entity whose durable
// failures are reported to the caller instead of logged and swallowed.
type burstFailPersister struct {
	auditRecorder
	listErr   error
	upsertErr error
	deleteErr error
	upserts   []string
	deletes   []string
	claims    []string

	// The node-phase seam scripts what the conditional UPDATE decided. rows is
	// keyed by burst id and stands in for the durable table: an id that is
	// absent is a row the statement could not match — claimed, another tenant's,
	// or already terminal — which is exactly the case the in-memory map must not
	// be allowed to override.
	phaseErr   error
	phaseRows  map[string]*Burst
	phaseCalls []BurstNodePhaseUpdate

	// The ownership seam reads the same stand-in table. ownerErr is the durable
	// backend being unreachable, which callers must not read as "not this
	// tenant's burst".
	ownerErr   error
	ownerCalls []string
	reapErr    error
	reaps      map[string]string
}

func (p *burstFailPersister) upsertCustomer(*Customer) error                        { return nil }
func (p *burstFailPersister) createTenant(*Customer, *TenantMembership, bool) error { return nil }
func (p *burstFailPersister) revokeCustomer(*Customer) error                        { return nil }
func (p *burstFailPersister) finalizeRevokedCustomer(string) error                  { return nil }
func (p *burstFailPersister) deleteCustomerAndMemberships(string) error             { return nil }
func (p *burstFailPersister) upsertAccount(*Account) error                          { return nil }
func (p *burstFailPersister) upsertMembership(*TenantMembership, *AuditEvent) error {
	return nil
}
func (p *burstFailPersister) deleteMembership(string, *AuditEvent) error { return nil }
func (p *burstFailPersister) upsertWorkload(*Workload) error             { return nil }
func (p *burstFailPersister) stampWorkloadPodObservation(_ context.Context, _ string, _ string, _ string, _ string, _ string, _ time.Time) (int64, error) {
	return 1, nil
}
func (p *burstFailPersister) getWorkloadPodObservation(context.Context, string, string, string, string) (*PodObservation, bool, error) {
	return nil, false, nil
}
func (p *burstFailPersister) workloadByBurst(context.Context, string) (*Workload, error) {
	return nil, nil
}
func (p *burstFailPersister) recordWorkloadCost(context.Context, string, *WorkloadCost) (bool, error) {
	return false, nil
}

func (p *burstFailPersister) upsertBurst(b *Burst) error {
	p.upserts = append(p.upserts, b.ID)
	return p.upsertErr
}

// listBursts satisfies the runtime read the orphan sweep uses. The fake keeps
// no rows, so an empty set is honest: these tests exercise the write path.
func (p *burstFailPersister) listBursts(context.Context) ([]*Burst, error) {
	if p.listErr != nil {
		return nil, p.listErr
	}
	return nil, nil
}

func (p *burstFailPersister) deleteBurst(id string) error {
	p.deletes = append(p.deletes, id)
	return p.deleteErr
}

func (p *burstFailPersister) updateBurstNodePhase(_ context.Context, u BurstNodePhaseUpdate) (*burstNodePhaseResult, error) {
	p.phaseCalls = append(p.phaseCalls, u)
	if p.phaseErr != nil {
		return nil, p.phaseErr
	}
	row, ok := p.phaseRows[u.BurstID]
	if !ok {
		return nil, nil
	}
	phaseFresh := nodePhaseIsFresh(row.NodePhaseAt, row.NodePhaseSourceTimestamped, u.ObservedAt, u.SourceTimestamped)
	occupancyTime := u.ReceiptTime.UTC()
	if occupancyTime.IsZero() {
		occupancyTime = u.ObservedAt.UTC()
	}
	occupancyFresh := u.OccupancyObserved &&
		(row.OccupancyObservedAt == nil || occupancyTime.After(*row.OccupancyObservedAt))
	gpuApplicable := u.GPUAllocatable && u.GPUAllocatableAt != nil && u.ClusterID != ""
	if !phaseFresh && !occupancyFresh && !gpuApplicable {
		return nil, nil
	}
	updated := *row
	if phaseFresh {
		updated.NodePhase = u.Phase
		updated.NodePhaseReason = u.Reason
		at := u.ObservedAt.UTC()
		updated.NodePhaseAt = &at
		updated.NodePhaseSourceTimestamped = u.SourceTimestamped
		updated.Status = BurstStatusForNodePhase(u.Phase)
	}
	if occupancyFresh {
		updated.OccupancyObservedAt = &occupancyTime
	}
	p.phaseRows[u.BurstID] = &updated
	return &burstNodePhaseResult{Burst: &updated, PhaseApplied: phaseFresh}, nil
}

func (p *burstFailPersister) updateBurstGPUTelemetry(context.Context, BurstGPUTelemetryUpdate) (bool, error) {
	return false, nil
}

func (p *burstFailPersister) burstOwnedBy(_ context.Context, burstID, customerID string) (bool, error) {
	p.ownerCalls = append(p.ownerCalls, burstID)
	if p.ownerErr != nil {
		return false, p.ownerErr
	}
	row, ok := p.phaseRows[burstID]
	return ok && row.CustomerID == customerID, nil
}

// burstForTenant answers from the same rows and fails on the same switch as
// burstOwnedBy: the two are the same durable read, and a test that could make
// one fail without the other would be staging a state no backend produces.
func (p *burstFailPersister) burstForTenant(_ context.Context, burstID, customerID string) (*Burst, bool, error) {
	p.ownerCalls = append(p.ownerCalls, burstID)
	if p.ownerErr != nil {
		return nil, false, p.ownerErr
	}
	row, ok := p.phaseRows[burstID]
	if !ok || row.CustomerID != customerID {
		return nil, false, nil
	}
	cp := *row
	return &cp, true, nil
}

// claimBurst holds no rows, so every claim honestly loses. The claim contract
// has its own fake and its own table in claim_burst_test.go; these tests are
// about the write path.
func (p *burstFailPersister) claimBurst(_ context.Context, id string) (*Burst, bool, error) {
	p.claims = append(p.claims, id)
	return nil, false, nil
}
func (p *burstFailPersister) recordBurstReapReceipt(_ context.Context, burstID, customerID string) (bool, error) {
	if p.reapErr != nil {
		return false, p.reapErr
	}
	if p.reaps == nil {
		p.reaps = make(map[string]string)
	}
	if owner, exists := p.reaps[burstID]; exists {
		return owner == customerID, nil
	}
	p.reaps[burstID] = customerID
	return true, nil
}
func (p *burstFailPersister) burstReapRecorded(_ context.Context, burstID, customerID string) (bool, error) {
	if p.reapErr != nil {
		return false, p.reapErr
	}
	return p.reaps[burstID] == customerID, nil
}

// The pod-slot trio has its own fake and its own table in pod_slots_test.go;
// these tests are about the burst write path.
func (p *burstFailPersister) reservePodSlot(context.Context, int, string, time.Duration) (bool, error) {
	return false, nil
}
func (p *burstFailPersister) releasePodSlot(context.Context, string) error { return nil }

// The idempotency trio: this fake's subject is elsewhere, so the claim seam is
// stubbed to "nothing was ever claimed".
func (p *burstFailPersister) claimIdempotency(context.Context, *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	return nil, true, nil
}
func (p *burstFailPersister) completeIdempotency(context.Context, string, int, []byte, time.Time) error {
	return nil
}
func (p *burstFailPersister) deleteIdempotency(context.Context, string) error { return nil }

func (p *burstFailPersister) reservedPodSlots(context.Context, time.Duration) (map[int]bool, error) {
	return nil, nil
}

func (p *burstFailPersister) upsertPV(*PersistentVolume) error { return nil }
func (p *burstFailPersister) deletePV(string) error            { return nil }

// loadAll is the boot read. These fakes hold no durable set to reload, so an
// empty snapshot is honest — including its tombstones: nothing here ever
// retires an id.
func (p *burstFailPersister) loadAll(context.Context) (*snapshot, error) { return &snapshot{}, nil }

func (p *burstFailPersister) Close() {}

// storeWith builds a store wired to a fresh burstFailPersister, or to no
// persister at all when durable is false (the in-memory / OSS / test store).
// Returns nil for the persister in that case so subtests can skip its asserts.
func storeWith(durable bool, upsertErr, deleteErr error) (*Store, *burstFailPersister) {
	s := emptyStore()
	if !durable {
		return s, nil
	}
	p := &burstFailPersister{upsertErr: upsertErr, deleteErr: deleteErr}
	s.persist = p
	return s, p
}

var errPersist = errors.New("postgres is down")

// TestPutBurstReportsPersistFailure pins the contract multi-replica central
// depends on: a burst that did not reach the durable backend must NOT look like
// a successful write. The in-memory record is still written — this process is
// the only thing that can still reap the VM it describes, so dropping it here
// would strand exactly the node the error is warning about.
func TestPutBurstReportsPersistFailure(t *testing.T) {
	tests := []struct {
		name      string
		durable   bool // false = no persister wired (in-memory / OSS store)
		upsertErr error
	}{
		{name: "nil persister is a successful no-op"},
		{name: "healthy persister", durable: true},
		{name: "failed upsert", durable: true, upsertErr: errPersist},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, p := storeWith(tc.durable, tc.upsertErr, nil)

			err := s.PutBurst(&Burst{ID: "b1", CustomerID: "cust_a", Backend: "linode", BackendID: "999"})
			if !errors.Is(err, tc.upsertErr) {
				t.Fatalf("PutBurst err = %v, want %v", err, tc.upsertErr)
			}
			// The in-memory record stands either way.
			got, gerr := s.GetBurst("b1")
			if gerr != nil {
				t.Fatalf("burst missing from memory after PutBurst: %v", gerr)
			}
			if got.BackendID != "999" {
				t.Errorf("in-memory burst wrong: %+v", got)
			}
			if p != nil && len(p.upserts) != 1 {
				t.Errorf("durable upserts = %v, want exactly 1", p.upserts)
			}
		})
	}
}

func TestPersistenceFailureMetricRecordsBackendFaultsOnly(t *testing.T) {
	s, _ := storeWith(true, errPersist, nil)
	meter := cost.NewMeter()
	s.Cost = meter

	if err := s.PutBurst(&Burst{ID: "b_metric", CustomerID: "cust_a"}); !errors.Is(err, errPersist) {
		t.Fatalf("PutBurst error = %v, want persistence failure", err)
	}
	// A database-enforced uniqueness refusal is a healthy business decision,
	// not an outage and must not page the operator.
	s.recordPersistenceFailure("customer", "create", ErrCustomerExists)
	s.recordPersistenceFailure("customer", "create", ErrAccountEmailUnverified)

	rec := httptest.NewRecorder()
	meter.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `yscale_state_persistence_failures_total{entity="burst",operation="upsert"} 1`) {
		t.Fatalf("burst persistence failure metric missing: %s", body)
	}
	if strings.Contains(body, `entity="customer",operation="create"`) {
		t.Fatalf("business-rule refusal was counted as a persistence outage: %s", body)
	}
}

// TestBurstDeleteReportsPersistFailure covers DeleteBurst: the in-memory
// removal stands on a durable failure and the error is reported, so the caller
// knows the row may come back on restart. ClaimBurst no longer routes through
// deleteBurst — the database elects the reap winner — and its contract lives in
// TestClaimBurst.
func TestBurstDeleteReportsPersistFailure(t *testing.T) {
	tests := []struct {
		name      string
		durable   bool // false = no persister wired (in-memory / OSS store)
		deleteErr error
	}{
		{name: "nil persister is a successful no-op"},
		{name: "healthy persister", durable: true},
		{name: "failed delete", durable: true, deleteErr: errPersist},
	}
	for _, tc := range tests {
		t.Run("DeleteBurst/"+tc.name, func(t *testing.T) {
			s, p := storeWith(tc.durable, nil, tc.deleteErr)
			s.bursts["b1"] = &Burst{ID: "b1", CustomerID: "cust_a"}

			if err := s.DeleteBurst("b1"); !errors.Is(err, tc.deleteErr) {
				t.Fatalf("DeleteBurst err = %v, want %v", err, tc.deleteErr)
			}
			if _, err := s.GetBurst("b1"); err != ErrNotFound {
				t.Errorf("burst still in memory after DeleteBurst: %v", err)
			}
			if p != nil && len(p.deletes) != 1 {
				t.Errorf("durable deletes = %v, want exactly 1", p.deletes)
			}
		})
	}
}

func TestBurstJSONBackwardsCompatibility(t *testing.T) {
	// A JSON payload representing an old Burst record without NodeOnly
	oldJSON := `{"ID":"b_old","CustomerID":"c","Backend":"linode","HourlyUSD":0.62}`
	var b Burst
	if err := json.Unmarshal([]byte(oldJSON), &b); err != nil {
		t.Fatalf("unmarshal old JSON: %v", err)
	}
	if b.NodeOnly {
		t.Errorf("expected NodeOnly to be false for old record, got true")
	}
	if b.ID != "b_old" || b.HourlyUSD != 0.62 {
		t.Errorf("expected other fields to remain intact, got %+v", b)
	}
}

func TestBurstNodeOnlyRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	burst := &Burst{ID: "b_nodeonly", CustomerID: "c", Backend: "linode", CreatedAt: now, HourlyUSD: 0.62, Deadline: time.Hour, NodeOnly: true}
	var b2 Burst
	mustRoundTrip(t, burst, &b2)
	if !b2.NodeOnly {
		t.Errorf("expected NodeOnly to be true after round-trip, got false")
	}
}
