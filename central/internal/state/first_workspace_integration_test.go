//go:build integration

package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func firstWorkspaceFixture(t *testing.T) (*Store, *Store, *pgPersister, *Account) {
	t.Helper()
	a, p := accountWriteFixture(t)
	account, err := a.UpsertAccount(accountWriteIssuer, "first-workspace", AccountProfile{Email: "before@synthetic.invalid", EmailVerified: true})
	if err != nil {
		t.Fatal(err)
	}
	return a, accountWriteReplica(t, p), p, account
}

func TestPostgresFirstWorkspaceRejectsChangedVerification(t *testing.T) {
	a, b, p, account := firstWorkspaceFixture(t)
	if _, err := a.UpsertAccount(account.Issuer, account.Subject, AccountProfile{Email: account.Email}); err != nil {
		t.Fatal(err)
	}
	tenant := &Customer{ID: SelfServiceTenantID(account.ID), Token: "synthetic-first-workspace-key", Email: account.Email}
	if _, _, err := b.CreateFirstTenant(tenant, account.ID); !errors.Is(err, ErrAccountEmailUnverified) || errors.Is(err, ErrPersistence) {
		t.Fatalf("stale verified profile did not produce an authorization refusal: %v", err)
	}
	var count int
	if err := p.pool.QueryRow(context.Background(), `SELECT count(*) FROM customers WHERE id=$1`, tenant.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("refused tenant persisted: count=%d err=%v", count, err)
	}
	// The same current unverified principal is still eligible for explicitly
	// operator-controlled provisioning. Its supplied contact remains intact.
	created, _, err := b.CreateTenant(&Customer{ID: "synthetic-operator-tenant", Token: "synthetic-operator-key", Email: "operator@synthetic.invalid"}, account.ID)
	if err != nil || created.Email != "operator@synthetic.invalid" {
		t.Fatalf("operator provisioning changed: %+v, %v", created, err)
	}
}

func TestPostgresFirstWorkspaceUsesCurrentContact(t *testing.T) {
	a, b, p, account := firstWorkspaceFixture(t)
	if _, err := a.UpsertAccount(account.Issuer, account.Subject, AccountProfile{Email: "current@synthetic.invalid", EmailVerified: true}); err != nil {
		t.Fatal(err)
	}
	tenant := &Customer{ID: SelfServiceTenantID(account.ID), Token: "synthetic-first-workspace-key", Email: account.Email}
	created, _, err := b.CreateFirstTenant(tenant, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	var email string
	if err := p.pool.QueryRow(context.Background(), `SELECT data->>'Email' FROM customers WHERE id=$1`, tenant.ID).Scan(&email); err != nil {
		t.Fatal(err)
	}
	if email != "current@synthetic.invalid" || created.Email != email {
		t.Fatalf("workspace contact came from stale profile: SQL=%q returned=%q", email, created.Email)
	}
}

func TestPostgresFirstWorkspaceIgnoresRemovedCachedGrant(t *testing.T) {
	a, _, p, account := firstWorkspaceFixture(t)
	other, err := a.ResolveAccount(accountWriteIssuer, "operator-owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.CreateTenant(&Customer{ID: "synthetic-other-tenant", Token: "synthetic-other-key"}, other.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AddTenantMembership(account.ID, "synthetic-other-tenant", RoleMember); err != nil {
		t.Fatal(err)
	}
	b := accountWriteReplica(t, p)
	if _, err := a.DeleteTenantMembership(account.ID, "synthetic-other-tenant", OperatorActor()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.CreateFirstTenant(&Customer{ID: SelfServiceTenantID(account.ID), Token: "synthetic-first-key"}, account.ID); err != nil {
		t.Fatalf("removed cached grant refused eligible workspace: %v", err)
	}
}

func TestPostgresFirstWorkspaceRequiresOwner(t *testing.T) {
	a, _ := accountWriteFixture(t)
	if _, _, err := a.CreateFirstTenant(&Customer{ID: "synthetic-no-owner", Token: "synthetic-no-owner-key"}, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("first workspace allowed an absent owner: %v", err)
	}
}

func assertFirstWorkspaceAbsent(t *testing.T, s *Store, p *pgPersister, id string) {
	t.Helper()
	var tenants, members int
	if err := p.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM customers WHERE id=$1),
		(SELECT count(*) FROM tenant_memberships WHERE data->>'CustomerID'=$1)`, id).Scan(&tenants, &members); err != nil {
		t.Fatal(err)
	}
	if tenants != 0 || members != 0 || s.customers[id] != nil || len(s.membershipsByTenant[id]) != 0 {
		t.Fatalf("refused workspace left state: tenants=%d memberships=%d", tenants, members)
	}
}

func waitFirstWorkspaceRowLock(t *testing.T, p *pgPersister, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("operation completed before the account lock was released: %v", err)
		default:
		}
		var waiting bool
		if err := p.pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
			WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%FROM accounts%FOR UPDATE%' AND pid<>pg_backend_pid())`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("operation did not enter an actual account-row lock wait")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPostgresFirstWorkspaceRereadsAfterProfileLock(t *testing.T) {
	_, b, p, account := firstWorkspaceFixture(t)
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `UPDATE accounts SET data=jsonb_set(data,'{EmailVerified}','false') WHERE id=$1`, account.ID); err != nil {
		t.Fatal(err)
	}
	candidate := &Customer{ID: SelfServiceTenantID(account.ID), Token: "synthetic-first-key", Email: account.Email}
	done := make(chan error, 1)
	go func() { _, _, err := b.CreateFirstTenant(candidate, account.ID); done <- err }()
	waitFirstWorkspaceRowLock(t, p, done)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrAccountEmailUnverified) {
		t.Fatalf("waiting create missed committed verification change: %v", err)
	}
	assertFirstWorkspaceAbsent(t, b, p, candidate.ID)
	if candidate.Email != account.Email || !b.accounts[account.ID].EmailVerified {
		t.Fatal("refusal published an account or contact mutation")
	}
}

type firstWorkspaceTraceKey struct{}
type firstWorkspaceTrace struct {
	once           sync.Once
	afterOwnerLock func()
}

func (t *firstWorkspaceTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, firstWorkspaceTraceKey{}, strings.Contains(d.SQL, "FROM accounts") && strings.Contains(d.SQL, "FOR UPDATE"))
}
func (t *firstWorkspaceTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if match, _ := ctx.Value(firstWorkspaceTraceKey{}).(bool); match {
		t.once.Do(t.afterOwnerLock)
	}
}

func TestPostgresFirstWorkspaceFencesProfileUntilCommit(t *testing.T) {
	a, b, p, account := firstWorkspaceFixture(t)
	done := make(chan error, 1)
	var observed bool
	trace := &firstWorkspaceTrace{afterOwnerLock: func() {
		go func() {
			_, err := a.UpsertAccount(account.Issuer, account.Subject, AccountProfile{Email: "later@synthetic.invalid"})
			done <- err
		}()
		waitFirstWorkspaceRowLock(t, p, done)
		observed = true
	}}
	cfg := p.pool.Config()
	cfg.ConnConfig.Tracer = trace
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	b.persist = &pgPersister{pool: pool}
	created, _, err := b.CreateFirstTenant(&Customer{ID: SelfServiceTenantID(account.ID), Token: "synthetic-first-key", Email: account.Email}, account.ID)
	if !observed {
		t.Fatal("fixture did not observe the competing profile writer blocked by creation")
	}
	if err != nil || created.Email != account.Email {
		t.Fatalf("authorized create: %+v, %v", created, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	current, err := a.AccountByID(account.ID)
	if err != nil || current.EmailVerified || current.Email != "later@synthetic.invalid" {
		t.Fatalf("later profile writer did not resume: %+v, %v", current, err)
	}
}

func TestPostgresFirstWorkspaceFindsCurrentUncachedOwner(t *testing.T) {
	a, p := accountWriteFixture(t)
	b := accountWriteReplica(t, p)
	account, err := a.UpsertAccount(accountWriteIssuer, "created-after-replica", AccountProfile{Email: "current@synthetic.invalid", EmailVerified: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.AccountByID(account.ID); err != nil {
		t.Fatal(err)
	}
	if b.accounts[account.ID] != nil {
		t.Fatal("read unexpectedly hydrated the fixture")
	}
	if _, _, err := b.CreateFirstTenant(&Customer{ID: SelfServiceTenantID(account.ID), Token: "synthetic-first-key"}, account.ID); err != nil {
		t.Fatalf("current uncached owner refused: %v", err)
	}
}

func TestPostgresFirstWorkspaceConcurrentDifferentIDs(t *testing.T) {
	_, _, p, account := firstWorkspaceFixture(t)
	const attempts = 8
	done := make(chan error, attempts)
	start := make(chan struct{})
	for i := range attempts {
		go func() {
			b := emptyStore()
			b.persist = &pgPersister{pool: p.pool}
			<-start
			_, _, err := b.CreateFirstTenant(&Customer{ID: fmt.Sprintf("synthetic-first-%d", i), Token: "synthetic-first-key"}, account.ID)
			done <- err
		}()
	}
	close(start)
	winners := 0
	for range attempts {
		err := <-done
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrAccountHasTenant) {
			t.Fatalf("concurrent first workspace: %v", err)
		}
	}
	var tenants, members int
	if err := p.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM customers),(SELECT count(*) FROM tenant_memberships)`).Scan(&tenants, &members); err != nil {
		t.Fatal(err)
	}
	if winners != 1 || tenants != 1 || members != 1 {
		t.Fatalf("concurrent first workspaces: winners=%d tenants=%d members=%d", winners, tenants, members)
	}
}

func TestPostgresFirstWorkspaceRollbackAndCorruption(t *testing.T) {
	for _, scenario := range []string{"insert-failure", "deleted-owner", "null-owner", "wrong-identity", "invalid-verified"} {
		t.Run(scenario, func(t *testing.T) {
			a, b, p, account := firstWorkspaceFixture(t)
			if _, err := a.UpsertAccount(account.Issuer, account.Subject, AccountProfile{Email: "current@synthetic.invalid", EmailVerified: true}); err != nil {
				t.Fatal(err)
			}
			candidate := &Customer{ID: SelfServiceTenantID(account.ID), Token: "synthetic-first-key", Email: account.Email}
			query := ""
			want := ErrPersistence
			switch scenario {
			case "insert-failure":
				query = `ALTER TABLE customers ADD CONSTRAINT synthetic_refuse CHECK (false) NOT VALID`
			case "deleted-owner":
				query = `DELETE FROM accounts`
				want = ErrNotFound
			case "null-owner":
				query = `UPDATE accounts SET data='null'`
			case "wrong-identity":
				query = `UPDATE accounts SET data=jsonb_set(data,'{Subject}','"wrong"')`
			case "invalid-verified":
				query = `UPDATE accounts SET data=jsonb_set(data,'{EmailVerified}','"true"')`
			}
			// Every table here is inside this synthetic fixture's private schema.
			if _, err := p.pool.Exec(context.Background(), query); err != nil {
				t.Fatal(err)
			}
			if _, _, err := b.CreateFirstTenant(candidate, account.ID); !errors.Is(err, want) {
				t.Fatalf("invalid create = %v, want %v", err, want)
			}
			assertFirstWorkspaceAbsent(t, b, p, candidate.ID)
			if candidate.Email != account.Email || candidate.persistedData != "" {
				t.Fatal("refused create published its staged contact/document")
			}
		})
	}
}

func TestPostgresFirstWorkspaceLockTimeoutAndOutage(t *testing.T) {
	_, b, p, account := firstWorkspaceFixture(t)
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, account.ID); err != nil {
		t.Fatal(err)
	}
	candidate := &Customer{ID: SelfServiceTenantID(account.ID), Token: "synthetic-first-key", Email: account.Email}
	start := time.Now()
	if _, _, err := b.CreateFirstTenant(candidate, account.ID); !errors.Is(err, ErrPersistence) || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 7*time.Second {
		t.Fatalf("blocked first workspace was not bounded: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	assertFirstWorkspaceAbsent(t, b, p, candidate.ID)
	if _, _, err := b.CreateFirstTenant(candidate, account.ID); err != nil {
		t.Fatalf("retry after lock release failed: %v", err)
	}
	p.pool.Close()
	if _, _, err := b.CreateFirstTenant(&Customer{ID: "synthetic-outage-tenant", Token: "synthetic-outage-key"}, account.ID); !errors.Is(err, ErrPersistence) {
		t.Fatalf("cached eligibility bypassed unavailable database: %v", err)
	}
}

func TestPostgresFirstWorkspaceIgnoresStaleUnverifiedCache(t *testing.T) {
	a, p := accountWriteFixture(t)
	account, err := a.ResolveAccount(accountWriteIssuer, "unverified-at-boot")
	if err != nil {
		t.Fatal(err)
	}
	b := accountWriteReplica(t, p)
	if _, err := a.UpsertAccount(account.Issuer, account.Subject, AccountProfile{Email: "verified@synthetic.invalid", EmailVerified: true}); err != nil {
		t.Fatal(err)
	}
	if b.accounts[account.ID].EmailVerified {
		t.Fatal("fixture cache unexpectedly refreshed")
	}
	created, _, err := b.CreateFirstTenant(&Customer{ID: SelfServiceTenantID(account.ID), Token: "synthetic-first-key"}, account.ID)
	if err != nil || created.Email != "verified@synthetic.invalid" {
		t.Fatalf("stale unverified cache denied current owner: %+v, %v", created, err)
	}
}
