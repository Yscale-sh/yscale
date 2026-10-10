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

const accountWriteIssuer = "synthetic-account-write-issuer"
const accountWriteSubject = "synthetic-account-write-subject"

func accountWriteFixture(t *testing.T) (*Store, *pgPersister) {
	t.Helper()
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	p := &pgPersister{pool: freshSchemaPool(t, dsn, fmt.Sprintf("account_writes_%d", time.Now().UnixNano()), 8)}
	if err := p.ensureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := emptyStore()
	s.persist = p
	return s, p
}

func accountWriteReplica(t *testing.T, p *pgPersister) *Store {
	t.Helper()
	snap, err := p.loadAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s := emptyStore()
	s.applySnapshot(snap)
	s.persist = &pgPersister{pool: p.pool}
	return s
}

func TestPostgresAccountWritesResolvePreservesCurrentProfile(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached-%v", cached), func(t *testing.T) {
			a, p := accountWriteFixture(t)
			if cached {
				if _, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Email: "old@synthetic.invalid"}); err != nil {
					t.Fatal(err)
				}
			}
			b := accountWriteReplica(t, p)
			current, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Email: "current@synthetic.invalid", EmailVerified: true, Name: "Current Human"})
			if err != nil {
				t.Fatal(err)
			}
			resolved, err := b.ResolveAccount(accountWriteIssuer, accountWriteSubject)
			if err != nil {
				t.Fatal(err)
			}
			if *resolved != *current {
				t.Errorf("resolve returned stale or fabricated profile/metadata")
			}
			fresh, err := a.AccountByIdentity(accountWriteIssuer, accountWriteSubject)
			if err != nil {
				t.Fatal(err)
			}
			if *fresh != *current {
				t.Error("operator resolution changed the durable issuer profile")
			}
			if b.accounts[current.ID] == nil || *b.accounts[current.ID] != *current {
				t.Error("successful resolve did not refresh local account indexes")
			}
		})
	}
}

func TestPostgresAccountWritesNoOpUsesCurrentRow(t *testing.T) {
	a, p := accountWriteFixture(t)
	oldProfile := AccountProfile{Email: "old@synthetic.invalid", Name: "Old Human"}
	first, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, oldProfile)
	if err != nil {
		t.Fatal(err)
	}
	b := accountWriteReplica(t, p)
	if _, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Email: "changed@synthetic.invalid", Name: "Changed Human"}); err != nil {
		t.Fatal(err)
	}
	// This is an explicit new issuer refresh, not a copied Account envelope.
	// The old cache calls it a no-op, but current SQL must store what was asked.
	got, err := b.UpsertAccount(accountWriteIssuer, accountWriteSubject, oldProfile)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := a.AccountByIdentity(accountWriteIssuer, accountWriteSubject)
	if err != nil {
		t.Fatal(err)
	}
	if got.Email != durable.Email || got.Name != durable.Name || !got.CreatedAt.Equal(first.CreatedAt) {
		t.Error("cached no-op acknowledged a profile not stored durably")
	}
	if !got.UpdatedAt.Equal(durable.UpdatedAt) {
		t.Error("cached no-op returned stale update metadata")
	}
}

func TestPostgresAccountWritesPreserveCreationOnUnseenAccount(t *testing.T) {
	a, p := accountWriteFixture(t)
	b := accountWriteReplica(t, p)
	first, err := a.ResolveAccount(accountWriteIssuer, accountWriteSubject)
	if err != nil {
		t.Fatal(err)
	}
	got, err := b.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Email: "signed-in@synthetic.invalid", EmailVerified: true})
	if err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(first.CreatedAt) {
		t.Error("issuer refresh replaced the account's original creation timestamp")
	}
	durable, err := a.AccountByIdentity(accountWriteIssuer, accountWriteSubject)
	if err != nil {
		t.Fatal(err)
	}
	if !durable.CreatedAt.Equal(first.CreatedAt) {
		t.Error("immutable creation timestamp changed in PostgreSQL")
	}
}

func TestPostgresAccountWritesNoOpFailsClosedOnOutage(t *testing.T) {
	a, p := accountWriteFixture(t)
	profile := AccountProfile{Email: "human@synthetic.invalid", EmailVerified: true}
	if _, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, profile); err != nil {
		t.Fatal(err)
	}
	p.pool.Close()
	if _, err := a.ResolveAccount(accountWriteIssuer, accountWriteSubject); !errors.Is(err, ErrPersistence) {
		t.Errorf("resolve cache bypassed unavailable authority: %v", err)
	}
	if _, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, profile); !errors.Is(err, ErrPersistence) {
		t.Errorf("profile no-op bypassed unavailable authority: %v", err)
	}
}

func TestPostgresAccountWritesConcurrentCreation(t *testing.T) {
	for _, resolve := range []bool{false, true} {
		t.Run(fmt.Sprintf("resolve-%v", resolve), func(t *testing.T) {
			a, p := accountWriteFixture(t)
			b := accountWriteReplica(t, p)
			profile := AccountProfile{Email: "concurrent@synthetic.invalid", EmailVerified: true, Name: "Concurrent Human"}
			type outcome struct {
				account *Account
				err     error
			}
			start, done := make(chan struct{}), make(chan outcome, 2)
			for i, s := range []*Store{a, b} {
				go func() {
					<-start
					var account *Account
					var err error
					if resolve && i == 0 {
						account, err = s.ResolveAccount(accountWriteIssuer, accountWriteSubject)
					} else {
						account, err = s.UpsertAccount(accountWriteIssuer, accountWriteSubject, profile)
					}
					done <- outcome{account, err}
				}()
			}
			close(start)
			first, second := <-done, <-done
			if first.err != nil || second.err != nil {
				t.Fatalf("concurrent account operations failed: %v / %v", first.err, second.err)
			}
			if first.account.ID != second.account.ID || !first.account.CreatedAt.Equal(second.account.CreatedAt) {
				t.Fatal("concurrent operations disagreed on immutable account metadata")
			}
			current, err := a.AccountByIdentity(accountWriteIssuer, accountWriteSubject)
			if err != nil {
				t.Fatal(err)
			}
			if current.Email != profile.Email || current.Name != profile.Name || !current.EmailVerified {
				t.Fatal("concurrent resolution erased issuer profile")
			}
			var count int
			if err := p.pool.QueryRow(context.Background(), `SELECT count(*) FROM accounts`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("account count=%d err=%v", count, err)
			}
		})
	}
}

func accountWriteVersion(t *testing.T, p *pgPersister, id string) string {
	t.Helper()
	var version string
	if err := p.pool.QueryRow(context.Background(), `SELECT xmin::text FROM accounts WHERE id=$1`, id).Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version
}

func TestPostgresAccountWritesNoOpDoesNotRewrite(t *testing.T) {
	a, p := accountWriteFixture(t)
	profile := AccountProfile{Email: "noop@synthetic.invalid", EmailVerified: true, Name: "Same Human"}
	current, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, profile)
	if err != nil {
		t.Fatal(err)
	}
	version := accountWriteVersion(t, p, current.ID)
	for range 3 {
		// Empty caches cannot turn the same durable profile into a new write.
		b := emptyStore()
		b.persist = &pgPersister{pool: p.pool}
		got, err := b.UpsertAccount(accountWriteIssuer, accountWriteSubject, profile)
		if err != nil {
			t.Fatal(err)
		}
		if *got != *current {
			t.Fatal("true no-op changed metadata")
		}
		got.Name = "caller-only"
		if b.accounts[current.ID].Name != profile.Name || b.accountsByIdentity[identityKey(accountWriteIssuer, accountWriteSubject)].Name != profile.Name {
			t.Fatal("returned account aliases cache")
		}
		resolved, err := b.ResolveAccount(accountWriteIssuer, accountWriteSubject)
		if err != nil || *resolved != *current {
			t.Fatalf("resolve did not read current profile: %v", err)
		}
	}
	if accountWriteVersion(t, p, current.ID) != version {
		t.Fatal("resolve/no-op physically rewrote the account row")
	}
}

func TestPostgresAccountWritesPatchOnlyProfile(t *testing.T) {
	a, p := accountWriteFixture(t)
	first, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Email: "clear@synthetic.invalid", EmailVerified: true, Name: "Clear Me"})
	if err != nil {
		t.Fatal(err)
	}
	future := time.Now().UTC().Add(time.Hour)
	if _, err := p.pool.Exec(context.Background(), `UPDATE accounts SET data=data || jsonb_build_object('SyntheticMetadata','preserve-me','UpdatedAt',$2::text) WHERE id=$1`, first.ID, future.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	cleared, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if cleared.Email != "" || cleared.Name != "" || cleared.EmailVerified || !cleared.CreatedAt.Equal(first.CreatedAt) || !cleared.UpdatedAt.After(future) {
		t.Fatal("profile patch changed creation metadata or regressed update time")
	}
	var metadata string
	var omitted bool
	if err := p.pool.QueryRow(context.Background(), `SELECT data->>'SyntheticMetadata',NOT(data ? 'Email' OR data ? 'EmailVerified' OR data ? 'Name') FROM accounts WHERE id=$1`, first.ID).Scan(&metadata, &omitted); err != nil {
		t.Fatal(err)
	}
	if metadata != "preserve-me" || !omitted {
		t.Fatal("profile patch replaced unrelated metadata or retained cleared profile keys")
	}
}

func TestPostgresAccountWritesRollback(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(fmt.Sprintf("exists-%v", exists), func(t *testing.T) {
			s, p := accountWriteFixture(t)
			original := AccountProfile{Email: "original@synthetic.invalid", EmailVerified: true}
			var before *Account
			if exists {
				var err error
				before, err = s.UpsertAccount(accountWriteIssuer, accountWriteSubject, original)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := p.pool.Exec(context.Background(), `ALTER TABLE accounts ADD CONSTRAINT synthetic_refuse_write CHECK(false) NOT VALID`); err != nil {
				t.Fatal(err)
			}
			if _, err := s.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Email: "rejected@synthetic.invalid"}); !errors.Is(err, ErrPersistence) {
				t.Fatalf("write refusal=%v", err)
			}
			current, err := s.AccountByIdentity(accountWriteIssuer, accountWriteSubject)
			if exists {
				if err != nil || *current != *before || *s.accounts[before.ID] != *before {
					t.Fatal("failed refresh changed durable/cached profile")
				}
				if _, err := s.ResolveAccount(accountWriteIssuer, accountWriteSubject); err != nil {
					t.Fatalf("identity resolve unexpectedly attempted a write: %v", err)
				}
				if _, err := s.UpsertAccount(accountWriteIssuer, accountWriteSubject, original); err != nil {
					t.Fatalf("true no-op unexpectedly attempted a write: %v", err)
				}
			} else {
				if !errors.Is(err, ErrNotFound) || len(s.accounts) != 0 || len(s.accountsByIdentity) != 0 {
					t.Fatal("failed insert published a phantom account")
				}
			}
			if _, err := p.pool.Exec(context.Background(), `ALTER TABLE accounts DROP CONSTRAINT synthetic_refuse_write`); err != nil {
				t.Fatal(err)
			}
			if _, err := s.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Email: "accepted@synthetic.invalid"}); err != nil {
				t.Fatalf("retry after database recovery: %v", err)
			}
		})
	}
}

func TestPostgresAccountWritesRecheckAfterIssuerLock(t *testing.T) {
	s, p := accountWriteFixture(t)
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1,$2)`, accountIssuerLockClass, auditLockID(accountWriteIssuer)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	var resolved *Account
	go func() {
		var err error
		resolved, err = s.ResolveAccount(accountWriteIssuer, accountWriteSubject)
		done <- err
	}()
	waitMembershipLock(t, p, accountIssuerLockClass, accountWriteIssuer, done)
	// An account appeared after the resolver's initial read but before it can
	// lock. This synthetic transaction stands in for the winning issuer write.
	current := &Account{ID: accountID(accountWriteIssuer, accountWriteSubject), Issuer: accountWriteIssuer, Subject: accountWriteSubject, Email: "winner@synthetic.invalid", EmailVerified: true, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if _, err := tx.Exec(ctx, `INSERT INTO accounts(id,data) VALUES($1,$2)`, current.ID, current); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if *resolved != *current {
		t.Fatal("resolve overwrote the account that appeared while waiting")
	}
}

func TestPostgresAccountWritesBoundedLockWait(t *testing.T) {
	s, p := accountWriteFixture(t)
	profile := AccountProfile{Email: "original@synthetic.invalid"}
	current, err := s.UpsertAccount(accountWriteIssuer, accountWriteSubject, profile)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := p.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(context.Background(), `SELECT pg_advisory_xact_lock_shared($1,$2)`, accountIssuerLockClass, auditLockID(accountWriteIssuer)); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = s.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Email: "new@synthetic.invalid"})
	if !errors.Is(err, ErrPersistence) || !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 7*time.Second {
		t.Fatalf("account write did not respect operation deadline: %v", err)
	}
	if *s.accounts[current.ID] != *current {
		t.Fatal("timed-out writer published a cached profile")
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Email: "retry@synthetic.invalid"}); err != nil {
		t.Fatalf("account locks leaked after timeout: %v", err)
	}
}

func TestPostgresAccountWritesRejectMalformedIdentity(t *testing.T) {
	for i, query := range []string{
		`UPDATE accounts SET data='null'`,
		`UPDATE accounts SET data=jsonb_set(data,'{ID}','"different-id"')`,
		`UPDATE accounts SET data=jsonb_set(data,'{Issuer}','"different-issuer"')`,
		`UPDATE accounts SET data=jsonb_set(data,'{Subject}','"different-subject"')`,
		`UPDATE accounts SET data=jsonb_set(data,'{UpdatedAt}','"not-a-time"')`,
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			s, p := accountWriteFixture(t)
			profile := AccountProfile{Email: "human@synthetic.invalid", EmailVerified: true}
			original, err := s.UpsertAccount(accountWriteIssuer, accountWriteSubject, profile)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.pool.Exec(context.Background(), query); err != nil {
				t.Fatal(err)
			}
			version := accountWriteVersion(t, p, original.ID)
			if _, err := s.ResolveAccount(accountWriteIssuer, accountWriteSubject); !errors.Is(err, ErrPersistence) {
				t.Errorf("resolve accepted malformed account: %v", err)
			}
			if _, err := s.UpsertAccount(accountWriteIssuer, accountWriteSubject, profile); !errors.Is(err, ErrPersistence) {
				t.Errorf("profile refresh accepted malformed account: %v", err)
			}
			if accountWriteVersion(t, p, original.ID) != version || *s.accounts[original.ID] != *original {
				t.Fatal("refusal repaired/replaced malformed data or altered local state")
			}
		})
	}
}

func TestPostgresAccountWritesLegacyEnvelopeRefused(t *testing.T) {
	s, p := accountWriteFixture(t)
	current, err := s.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Email: "original@synthetic.invalid", EmailVerified: true})
	if err != nil {
		t.Fatal(err)
	}
	stale := *current
	stale.Email = "stale@synthetic.invalid"
	if err := p.upsertAccount(&stale); !errors.Is(err, ErrPersistence) {
		t.Fatalf("legacy envelope write succeeded: %v", err)
	}
	durable, err := s.AccountByIdentity(accountWriteIssuer, accountWriteSubject)
	if err != nil || *durable != *current {
		t.Fatal("legacy envelope changed durable account")
	}
}
