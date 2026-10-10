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

func TestPostgresHumanAuthorizationReadContracts(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	ctx := context.Background()
	pool := freshSchemaPool(t, dsn, fmt.Sprintf("human_authz_%d", time.Now().UnixNano()), 4)
	p := &pgPersister{pool: pool}
	if err := p.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	s := emptyStore()
	s.persist = p
	account, err := s.UpsertAccount("synthetic-issuer", "synthetic-human", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	tenant, member, err := s.CreateTenant(&Customer{ID: "synthetic-human-tenant", Token: "synthetic-human-test-key"}, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	type fixtureRow struct{ table, id, data string }
	rows := []fixtureRow{{"accounts", account.ID, ""}, {"customers", tenant.ID, ""}, {"tenant_memberships", member.ID, ""}}
	for i := range rows {
		if err := pool.QueryRow(ctx, "SELECT data::text FROM "+rows[i].table+" WHERE id=$1", rows[i].id).Scan(&rows[i].data); err != nil {
			t.Fatal(err)
		}
	}
	restoreFixture := func(t *testing.T) {
		t.Helper()
		for _, row := range rows {
			if _, err := pool.Exec(ctx, "INSERT INTO "+row.table+" (id,data) VALUES ($1,$2::jsonb) ON CONFLICT (id) DO UPDATE SET data=excluded.data", row.id, row.data); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, role := range []string{RoleOwner, RoleAdmin, RoleMember, RoleViewer} {
		t.Run("role-"+role, func(t *testing.T) {
			if _, err := pool.Exec(ctx, `UPDATE tenant_memberships SET data=jsonb_set(data,'{Role}',to_jsonb($2::text)) WHERE id=$1`, member.ID, role); err != nil {
				t.Fatal(err)
			}
			current, err := s.MembershipForContext(ctx, account.ID, tenant.ID)
			if err != nil || current.Role != role {
				t.Fatalf("durable role not observed: %v", err)
			}
			if s.membershipsByAccount[account.ID][tenant.ID].Role != RoleOwner {
				t.Fatal("read hydrated the membership mutation map")
			}
			current.Role = "caller-local-change"
			again, err := s.MembershipFor(account.ID, tenant.ID)
			if err != nil || again.Role != role {
				t.Fatalf("returned membership was not detached: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name, query string
		want        error
	}{
		{"member-id", `UPDATE tenant_memberships SET data=jsonb_set(data,'{ID}','"different-member"')`, ErrPersistence},
		{"member-account", `UPDATE tenant_memberships SET data=jsonb_set(data,'{AccountID}','"different-account"')`, ErrPersistence},
		{"member-tenant", `UPDATE tenant_memberships SET data=jsonb_set(data,'{CustomerID}','"different-tenant"')`, ErrPersistence},
		{"unknown-role", `UPDATE tenant_memberships SET data=jsonb_set(data,'{Role}','"super-admin"')`, ErrPersistence},
		{"null-member", `UPDATE tenant_memberships SET data='null'`, ErrPersistence},
		{"invalid-grant-date", `UPDATE tenant_memberships SET data=jsonb_set(data,'{CreatedAt}','"not-a-time"')`, ErrPersistence},
		{"account-id", `UPDATE accounts SET data=jsonb_set(data,'{ID}','"different-account"')`, ErrPersistence},
		{"account-issuer", `UPDATE accounts SET data=jsonb_set(data,'{Issuer}','"different-issuer"')`, ErrPersistence},
		{"account-subject", `UPDATE accounts SET data=jsonb_set(data,'{Subject}','"different-subject"')`, ErrPersistence},
		{"null-account", `UPDATE accounts SET data='null'`, ErrPersistence},
		{"tenant-id", `UPDATE customers SET data=jsonb_set(data,'{ID}','"different-tenant"')`, ErrPersistence},
		{"null-tenant", `UPDATE customers SET data='null'`, ErrPersistence},
		{"revoked-tenant", `UPDATE customers SET data=jsonb_set(data,'{RevokedAt}','"2026-09-05T00:00:00Z"')`, ErrNotFound},
		{"missing-member", `DELETE FROM tenant_memberships`, ErrNotFound},
		{"missing-account", `DELETE FROM accounts`, ErrNotFound},
		{"missing-tenant", `DELETE FROM customers`, ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restoreFixture(t)
			// Each table contains one synthetic row in this test's private
			// schema, never the application's schema or a shared customer.
			if _, err := pool.Exec(ctx, tc.query); err != nil {
				t.Fatal(err)
			}
			if _, err := s.MembershipFor(account.ID, tenant.ID); !errors.Is(err, tc.want) {
				t.Fatalf("malformed/missing durable authority = %v, want %v", err, tc.want)
			}
			if tc.name == "account-id" || tc.name == "account-issuer" || tc.name == "account-subject" || tc.name == "null-account" {
				if _, err := s.AccountByIdentity(account.Issuer, account.Subject); !errors.Is(err, ErrPersistence) {
					t.Fatalf("invalid account identity was accepted: %v", err)
				}
			}
		})
	}
	t.Run("context-deadline", func(t *testing.T) {
		restoreFixture(t)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
			t.Fatal(err)
		}
		for _, read := range []func(context.Context) error{
			func(ctx context.Context) error {
				_, err := s.AccountByIdentityContext(ctx, account.Issuer, account.Subject)
				return err
			},
			func(ctx context.Context) error {
				_, err := s.MembershipForContext(ctx, account.ID, tenant.ID)
				return err
			},
		} {
			readCtx, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
			started := time.Now()
			err := read(readCtx)
			cancel()
			if !errors.Is(err, ErrPersistence) || !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 2*time.Second {
				t.Fatalf("blocked authorization did not respect the request deadline: %v", err)
			}
		}
	})
	t.Run("tombstone", func(t *testing.T) {
		restoreFixture(t)
		if _, err := pool.Exec(ctx, `INSERT INTO customer_tombstones (id, revoked_at) VALUES ($1,now())`, tenant.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.MembershipFor(account.ID, tenant.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("tombstoned tenant retained human authority: %v", err)
		}
	})
	pool.Close()
	if _, err := s.AccountByIdentity(account.Issuer, account.Subject); !errors.Is(err, ErrPersistence) {
		t.Fatalf("unavailable account database used cached authority: %v", err)
	}
	if _, err := s.MembershipFor(account.ID, tenant.ID); !errors.Is(err, ErrPersistence) {
		t.Fatalf("unavailable membership database used cached authority: %v", err)
	}
}
