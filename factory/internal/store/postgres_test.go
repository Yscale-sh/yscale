package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type fakePersister struct {
	boxes    []Box
	deleted  []string
	policies []PolicyState
	jobs     []Job
	audits   []Audit
	ops      []string // ordered log of durable ops, e.g. "upsertBox:<login>" / "deleteBox:<login>"
}

func (p *fakePersister) UpsertBox(box Box) error {
	p.boxes = append(p.boxes, cloneBox(box))
	p.ops = append(p.ops, "upsertBox:"+box.LoginServer)
	return nil
}
func (p *fakePersister) DeleteBox(loginServer string) error {
	p.deleted = append(p.deleted, loginServer)
	p.ops = append(p.ops, "deleteBox:"+loginServer)
	return nil
}
func (p *fakePersister) UpsertPolicyState(state PolicyState) error {
	p.policies = append(p.policies, clonePolicyState(state))
	return nil
}
func (p *fakePersister) UpsertJob(job Job) error { p.jobs = append(p.jobs, job); return nil }
func (p *fakePersister) AppendAudit(audit Audit) error {
	p.audits = append(p.audits, audit)
	return nil
}
func (p *fakePersister) Close() {}

// TestMemoryStoreWritesThrough verifies every mutation's durable counterpart
// without requiring Postgres. Persistence remains best effort; this fake only
// observes the records passed to the Persister seam.
func TestMemoryStoreWritesThrough(t *testing.T) {
	p := &fakePersister{}
	s, err := NewWithKEKAndPersister(bytes.Repeat([]byte{42}, kekSize), p)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetBox(Box{TenantID: "tenant", LoginServer: "manual", Status: StatusReady}, "manual-key"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetBoxStatus("manual", StatusDegraded); err != nil {
		t.Fatal(err)
	}
	if _, started, err := s.BeginDeprovision("tenant", "manual"); err != nil || !started {
		t.Fatalf("BeginDeprovision: started=%v err=%v", started, err)
	}
	if err := s.SetPolicyState(PolicyState{TenantID: "tenant", PushedRoutes: []string{"10.0.0.0/8"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetJob(Job{ID: "set-job", TenantID: "tenant", Kind: "replace", Status: JobPending}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnqueueJob(Job{ID: "queued-job", TenantID: "tenant", Kind: "deprovision"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimJob(); err != nil {
		t.Fatal(err)
	}
	job, staged, err := s.EnsureFabric("fabric-tenant", "create-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PromoteFabric(staged.LoginServer, Box{TenantID: "fabric-tenant", LoginServer: "real", Status: StatusReady}, "real-key"); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendAudit(Audit{TenantID: "tenant", Action: "changed"}); err != nil {
		t.Fatal(err)
	}

	if len(p.boxes) != 5 || len(p.deleted) != 1 || p.deleted[0] != staged.LoginServer {
		t.Fatalf("box write-throughs: upserts=%d deletes=%v", len(p.boxes), p.deleted)
	}
	if len(p.policies) != 1 || len(p.jobs) != 5 || len(p.audits) != 1 {
		t.Fatalf("write-throughs: policies=%d jobs=%d audits=%d", len(p.policies), len(p.jobs), len(p.audits))
	}
	if p.jobs[len(p.jobs)-1].ID != job.ID || p.jobs[len(p.jobs)-1].LoginServer != "real" {
		t.Fatalf("promoted job was not persisted: %#v", p.jobs[len(p.jobs)-1])
	}
	if bytes.Contains(p.boxes[0].apiKeyEnc, []byte("manual-key")) {
		t.Fatal("Persister received plaintext API key")
	}
}

// PromoteFabric must persist the validated box BEFORE deleting the staging
// record: the box's api_key_enc is not recoverable by rebuild, so a crash
// mid-promotion must never leave the durable store with the staging box gone
// and the real box unwritten. Worst case is a harmless leftover staging box.
func TestPromoteFabricPersistsBoxBeforeDeletingStaging(t *testing.T) {
	p := &fakePersister{}
	s, err := NewWithKEKAndPersister(bytes.Repeat([]byte{7}, kekSize), p)
	if err != nil {
		t.Fatal(err)
	}
	_, staged, err := s.EnsureFabric("tenant-a", "create-1")
	if err != nil {
		t.Fatal(err)
	}
	staging := staged.LoginServer
	const real = "https://203-0-113-9.ip.linodeusercontent.com"
	p.ops = nil // ignore EnsureFabric's writes; observe only the promotion
	if err := s.PromoteFabric(staging, Box{TenantID: "tenant-a", LoginServer: real, Status: StatusReady}, "real-key"); err != nil {
		t.Fatal(err)
	}

	upsertReal, deleteStaging := -1, -1
	for i, op := range p.ops {
		if op == "upsertBox:"+real {
			upsertReal = i
		}
		if op == "deleteBox:"+staging {
			deleteStaging = i
		}
	}
	if upsertReal < 0 || deleteStaging < 0 {
		t.Fatalf("promotion ops = %v, want both an upsert of the real box and a staging delete", p.ops)
	}
	if upsertReal > deleteStaging {
		t.Fatalf("promotion persisted staging delete (%d) before the real box (%d): %v — a crash between them loses the box", deleteStaging, upsertReal, p.ops)
	}
}

func TestPostgresPersisterRoundTrip(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FACTORY_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	requireFactoryTestDatabase(t, ctx, pool)
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS audit, policy_state, jobs, boxes CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP TABLE IF EXISTS audit, policy_state, jobs, boxes CASCADE`); err != nil {
			t.Errorf("cleanup factory tables: %v", err)
		}
	})

	p, err := NewPostgresPersister(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s, err := NewWithKEKAndPersister(bytes.Repeat([]byte{19}, kekSize), p)
	if err != nil {
		t.Fatal(err)
	}
	const plaintext = "plaintext-must-never-reach-postgres"
	box := Box{TenantID: "tenant-pg", LoginServer: "box-pg", Hostname: "box", Backend: "linode", BackendID: "123", HSUser: "tenant-pg", Status: StatusReady, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)}
	if err := s.SetBox(box, plaintext); err != nil {
		t.Fatal(err)
	}
	sealed, err := s.GetBox(box.LoginServer)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	if err := p.pool.QueryRow(ctx, `SELECT api_key_enc FROM boxes WHERE login_server = $1`, box.LoginServer).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(persisted, sealed.apiKeyEnc) || bytes.Contains(persisted, []byte(plaintext)) {
		t.Fatal("Postgres api_key_enc was not exactly the KEK-sealed ciphertext")
	}

	job := Job{ID: "job-pg", TenantID: "tenant-pg", LoginServer: box.LoginServer, Kind: "provision", Status: JobPending, IdempotencyKey: "create-pg", RequestHash: "hash", CreatedAt: box.CreatedAt, UpdatedAt: box.CreatedAt}
	if err := s.SetJob(job); err != nil {
		t.Fatal(err)
	}
	duplicate := job
	duplicate.ID = "job-pg-duplicate"
	if err := p.UpsertJob(duplicate); err == nil {
		t.Fatal("jobs tenant/idempotency unique constraint accepted a duplicate")
	}

	reloaded, err := NewWithKEKAndPersister(bytes.Repeat([]byte{19}, kekSize), p)
	if err != nil {
		t.Fatal(err)
	}
	if err := reloaded.LoadFrom(ctx, p); err != nil {
		t.Fatal(err)
	}
	if got, err := reloaded.DecryptAPIKey(box.LoginServer); err != nil || got != plaintext {
		t.Fatalf("box was not restored with its sealed API key: %q, %v", got, err)
	}
	if got, err := reloaded.GetJob(job.ID); err != nil || got.IdempotencyKey != job.IdempotencyKey {
		t.Fatalf("job was not restored: %#v, %v", got, err)
	}
	if err := p.DeleteBox(box.LoginServer); err != nil {
		t.Fatal(err)
	}
	deleted, err := NewWithKEKAndPersister(bytes.Repeat([]byte{19}, kekSize), p)
	if err != nil {
		t.Fatal(err)
	}
	if err := deleted.LoadFrom(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := deleted.GetBox(box.LoginServer); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted box reloaded: %v", err)
	}
}

func requireFactoryTestDatabase(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if os.Getenv("FACTORY_TEST_ALLOW_DESTRUCTIVE") != "DROP_FACTORY_TABLES" {
		t.Fatal("set FACTORY_TEST_ALLOW_DESTRUCTIVE=DROP_FACTORY_TABLES for the isolated test database")
	}
	var databaseName string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("FACTORY_TEST_EXPECT_DATABASE"); expected == "" || expected != databaseName {
		t.Fatalf("FACTORY_TEST_EXPECT_DATABASE must exactly match current database %q", databaseName)
	}
	if !strings.HasSuffix(databaseName, "_factory_test") {
		t.Fatalf("refusing destructive integration test against non-test database %q", databaseName)
	}
}
