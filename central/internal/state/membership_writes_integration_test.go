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

const membershipWriteTenant = "synthetic-membership-tenant"
const membershipWriteIssuer = "synthetic-membership-issuer"

func membershipWriteFixture(t *testing.T) (*Store, *pgPersister, map[string]string) {
	t.Helper()
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	p := &pgPersister{pool: freshSchemaPool(t, dsn, fmt.Sprintf("member_writes_%d", time.Now().UnixNano()), 8)}
	if err := p.ensureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := emptyStore()
	s.persist = p
	ids := map[string]string{}
	for _, name := range []string{"owner", "admin", "target", "newcomer"} {
		a, err := s.UpsertAccount(membershipWriteIssuer, name, AccountProfile{Email: name + "@synthetic.invalid", EmailVerified: true})
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = a.ID
	}
	if _, _, err := s.CreateTenant(&Customer{ID: membershipWriteTenant, Token: "synthetic-member-test-token"}, ids["owner"]); err != nil {
		t.Fatal(err)
	}
	for name, role := range map[string]string{"admin": RoleAdmin, "target": RoleViewer} {
		if _, err := s.AddTenantMembership(ids[name], membershipWriteTenant, role); err != nil {
			t.Fatal(err)
		}
	}
	return s, p, ids
}

func membershipWriteReplica(t *testing.T, p *pgPersister) *Store {
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

func membershipWriteAuditCount(t *testing.T, p *pgPersister) int {
	t.Helper()
	var n int
	if err := p.pool.QueryRow(context.Background(), `SELECT count(*) FROM tenant_audit WHERE customer_id=$1`, membershipWriteTenant).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPostgresMembershipWritesUseCurrentTargets(t *testing.T) {
	for _, op := range []string{"human-remove", "human-role", "operator-role-deleted", "operator-grant-conflict", "operator-remove-new", "operator-role-noop"} {
		t.Run(op, func(t *testing.T) {
			a, p, ids := membershipWriteFixture(t)
			b := membershipWriteReplica(t, p)
			by := HumanActor(ids["admin"], membershipWriteTenant)
			want := ErrOwnerRestricted
			var err error
			switch op {
			case "human-remove", "human-role", "operator-grant-conflict", "operator-role-noop":
				_, _, err = a.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleOwner, OperatorActor())
			case "operator-role-deleted":
				_, err = a.DeleteTenantMembership(ids["target"], membershipWriteTenant, OperatorActor())
			case "operator-remove-new":
				_, err = a.AddTenantMembership(ids["newcomer"], membershipWriteTenant, RoleViewer)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := membershipWriteAuditCount(t, p)
			switch op {
			case "human-remove":
				_, err = b.RemoveTenantMembership(membershipWriteTenant, ids["admin"], ids["target"], by)
				want = ErrOwnerProtected
			case "human-role":
				_, _, err = b.SetTenantMemberRole(membershipWriteTenant, ids["admin"], ids["target"], RoleMember, by)
			case "operator-role-deleted":
				_, _, err = b.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleMember, OperatorActor())
				want = ErrNotFound
			case "operator-grant-conflict":
				_, _, err = b.GrantTenantMembership(ids["target"], membershipWriteTenant, RoleViewer, OperatorActor())
				want = ErrRoleConflict
			case "operator-remove-new":
				var removed *TenantMembership
				removed, err = b.DeleteTenantMembership(ids["newcomer"], membershipWriteTenant, OperatorActor())
				if err != nil || removed == nil {
					t.Fatalf("new durable membership removal = %v, %v; want actual removal", removed, err)
				}
				if _, err := membershipWriteReplica(t, p).MembershipFor(ids["newcomer"], membershipWriteTenant); !errors.Is(err, ErrNotFound) {
					t.Fatal("removal left the durable grant present")
				}
				return
			case "operator-role-noop":
				var previous string
				_, previous, err = b.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleViewer, OperatorActor())
				if err != nil || previous != RoleOwner || membershipWriteAuditCount(t, p) != before+1 {
					t.Fatalf("stale no-op skipped current role change: previous=%s err=%v", previous, err)
				}
				return
			}
			if !errors.Is(err, want) {
				t.Errorf("stale target mutation = %v; want %v", err, want)
			}
			if membershipWriteAuditCount(t, p) != before {
				t.Error("refused mutation committed an audit row")
			}
			current, readErr := membershipWriteReplica(t, p).MembershipFor(ids["target"], membershipWriteTenant)
			if op == "operator-role-deleted" {
				if !errors.Is(readErr, ErrNotFound) {
					t.Error("role update resurrected a deleted grant")
				}
			} else if readErr != nil || current.Role != RoleOwner {
				t.Error("refused mutation altered the current owner's grant")
			}
		})
	}
}

func TestPostgresMembershipWritesKeepLastOwner(t *testing.T) {
	for _, op := range []string{"role", "delete"} {
		t.Run(op, func(t *testing.T) {
			a, p, ids := membershipWriteFixture(t)
			if _, _, err := a.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleOwner, OperatorActor()); err != nil {
				t.Fatal(err)
			}
			b := membershipWriteReplica(t, p)
			if _, _, err := a.SetTenantMembershipRole(ids["owner"], membershipWriteTenant, RoleViewer, OperatorActor()); err != nil {
				t.Fatal(err)
			}
			var err error
			if op == "role" {
				_, _, err = b.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleViewer, OperatorActor())
			} else {
				_, err = b.DeleteTenantMembership(ids["target"], membershipWriteTenant, OperatorActor())
			}
			if !errors.Is(err, ErrLastOwner) {
				t.Fatalf("last owner mutation = %v; want refusal", err)
			}
		})
	}
}

func TestPostgresMembershipWritesResolveCurrentEmail(t *testing.T) {
	a, p, ids := membershipWriteFixture(t)
	b := membershipWriteReplica(t, p)
	if _, err := a.UpsertAccount(membershipWriteIssuer, "newcomer", AccountProfile{Email: "changed@synthetic.invalid", EmailVerified: true}); err != nil {
		t.Fatal(err)
	}
	by := HumanActor(ids["owner"], membershipWriteTenant)
	if _, _, err := b.AddTenantMemberByEmail(membershipWriteTenant, ids["owner"], membershipWriteIssuer, "newcomer@synthetic.invalid", RoleMember, by); !errors.Is(err, ErrTargetNotFound) {
		t.Errorf("stale verified-email grant = %v; want target not found", err)
	}
	m, created, err := b.AddTenantMemberByEmail(membershipWriteTenant, ids["owner"], membershipWriteIssuer, " CHANGED@synthetic.invalid ", RoleMember, by)
	if err != nil || !created || m.AccountID != ids["newcomer"] {
		t.Errorf("current verified-email grant failed: created=%v err=%v", created, err)
	}
}

func TestPostgresMembershipWritesConcurrentOwners(t *testing.T) {
	for _, human := range []bool{false, true} {
		for _, remove := range []bool{false, true} {
			t.Run(fmt.Sprintf("human-%v-remove-%v", human, remove), func(t *testing.T) {
				a, p, ids := membershipWriteFixture(t)
				if _, _, err := a.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleOwner, OperatorActor()); err != nil {
					t.Fatal(err)
				}
				b := membershipWriteReplica(t, p)
				before := membershipWriteAuditCount(t, p)
				start, results := make(chan struct{}), make(chan error, 2)
				for i, s := range []*Store{a, b} {
					target := []string{ids["owner"], ids["target"]}[i]
					go func() {
						<-start
						var err error
						switch {
						case human && remove:
							_, err = s.RemoveTenantMembership(membershipWriteTenant, target, target, HumanActor(target, membershipWriteTenant))
						case human:
							_, _, err = s.SetTenantMemberRole(membershipWriteTenant, target, target, RoleViewer, HumanActor(target, membershipWriteTenant))
						case remove:
							_, err = s.DeleteTenantMembership(target, membershipWriteTenant, OperatorActor())
						default:
							_, _, err = s.SetTenantMembershipRole(target, membershipWriteTenant, RoleViewer, OperatorActor())
						}
						results <- err
					}()
				}
				close(start)
				wins, refused := 0, 0
				for range 2 {
					err := <-results
					if err == nil {
						wins++
					} else if errors.Is(err, ErrLastOwner) {
						refused++
					} else {
						t.Errorf("unexpected race error: %v", err)
					}
				}
				if wins != 1 || refused != 1 || membershipWriteAuditCount(t, p) != before+1 {
					t.Fatalf("wins=%d refused=%d; want one audited change", wins, refused)
				}
				fresh := membershipWriteReplica(t, p)
				if fresh.ownerCountLocked(membershipWriteTenant) != 1 {
					t.Fatal("concurrent mutations lost the last durable owner")
				}
			})
		}
	}
}

func TestPostgresMembershipWritesConcurrentGrant(t *testing.T) {
	a, p, ids := membershipWriteFixture(t)
	b := membershipWriteReplica(t, p)
	before := membershipWriteAuditCount(t, p)
	type outcome struct {
		m       *TenantMembership
		created bool
		err     error
	}
	start, results := make(chan struct{}), make(chan outcome, 2)
	for _, s := range []*Store{a, b} {
		go func() {
			<-start
			m, created, err := s.GrantTenantMembership(ids["newcomer"], membershipWriteTenant, RoleMember, OperatorActor())
			results <- outcome{m, created, err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || first.created == second.created || !first.m.CreatedAt.Equal(second.m.CreatedAt) {
		t.Fatalf("grant race failed: first=%v second=%v created=%v/%v", first.err, second.err, first.created, second.created)
	}
	if membershipWriteAuditCount(t, p) != before+1 {
		t.Fatal("idempotent regrant wrote an extra audit event")
	}
	first.m.Role = "caller-only-change"
	if a.membershipsByTenant[membershipWriteTenant][ids["newcomer"]].Role != RoleMember || b.membershipsByTenant[membershipWriteTenant][ids["newcomer"]].Role != RoleMember {
		t.Fatal("returned grant aliases a mutation cache")
	}
}

func membershipWriteCalls(s *Store, ids map[string]string) []func() error {
	by := HumanActor(ids["owner"], membershipWriteTenant)
	return []func() error{
		func() error {
			_, _, err := s.GrantTenantMembership(ids["newcomer"], membershipWriteTenant, RoleMember, OperatorActor())
			return err
		},
		func() error {
			_, _, err := s.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleViewer, OperatorActor())
			return err
		},
		func() error {
			_, err := s.DeleteTenantMembership(ids["newcomer"], membershipWriteTenant, OperatorActor())
			return err
		},
		func() error {
			_, _, err := s.AddTenantMemberByEmail(membershipWriteTenant, ids["owner"], membershipWriteIssuer, "newcomer@synthetic.invalid", RoleMember, by)
			return err
		},
		func() error {
			_, _, err := s.SetTenantMemberRole(membershipWriteTenant, ids["owner"], ids["target"], RoleViewer, by)
			return err
		},
		func() error {
			_, err := s.RemoveTenantMembership(membershipWriteTenant, ids["owner"], ids["newcomer"], by)
			return err
		},
	}
}

func TestPostgresMembershipWritesRefuseUnavailableTenant(t *testing.T) {
	for _, state := range []string{"revoked", "finalized", "deleted", "outage"} {
		t.Run(state, func(t *testing.T) {
			a, p, ids := membershipWriteFixture(t)
			b := membershipWriteReplica(t, p)
			want := ErrNotFound
			switch state {
			case "revoked", "finalized":
				if err := a.RevokeCustomer(membershipWriteTenant); err != nil {
					t.Fatal(err)
				}
				if state == "finalized" {
					if err := a.DeleteRevokedCustomer(membershipWriteTenant); err != nil {
						t.Fatal(err)
					}
				}
			case "deleted":
				a.DeleteCustomer(membershipWriteTenant)
			case "outage":
				p.pool.Close()
				want = ErrPersistence
			}
			for i, call := range membershipWriteCalls(b, ids) {
				if err := call(); !errors.Is(err, want) {
					t.Errorf("writer %d on %s = %v; want %v", i, state, err, want)
				}
			}
			if state != "outage" {
				var count int
				if err := p.pool.QueryRow(context.Background(), `SELECT count(*) FROM tenant_memberships WHERE data->>'CustomerID'=$1`, membershipWriteTenant).Scan(&count); err != nil || count != 0 {
					t.Fatalf("unavailable tenant retained %d grants: %v", count, err)
				}
			}
		})
	}
}

func TestPostgresMembershipWritesAuditRollback(t *testing.T) {
	for _, op := range []membershipOperation{membershipGrant, membershipRole, membershipRemove} {
		t.Run(fmt.Sprint(op), func(t *testing.T) {
			s, p, ids := membershipWriteFixture(t)
			before := membershipWriteAuditCount(t, p)
			if _, err := p.pool.Exec(context.Background(), `ALTER TABLE tenant_audit ADD CONSTRAINT synthetic_reject_append CHECK(false) NOT VALID`); err != nil {
				t.Fatal(err)
			}
			call := func() error {
				switch op {
				case membershipGrant:
					_, _, err := s.GrantTenantMembership(ids["newcomer"], membershipWriteTenant, RoleMember, OperatorActor())
					return err
				case membershipRole:
					_, _, err := s.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleMember, OperatorActor())
					return err
				default:
					_, err := s.DeleteTenantMembership(ids["target"], membershipWriteTenant, OperatorActor())
					return err
				}
			}
			if err := call(); !errors.Is(err, ErrPersistence) {
				t.Fatalf("audit refusal = %v", err)
			}
			if membershipWriteAuditCount(t, p) != before {
				t.Fatal("failed audit appended an event")
			}
			fresh := membershipWriteReplica(t, p)
			for _, view := range []*Store{s, fresh} {
				if view.membershipsByTenant[membershipWriteTenant][ids["target"]].Role != RoleViewer || view.membershipsByTenant[membershipWriteTenant][ids["newcomer"]] != nil {
					t.Fatal("audit failure altered durable or cached memberships")
				}
			}
			if _, err := p.pool.Exec(context.Background(), `ALTER TABLE tenant_audit DROP CONSTRAINT synthetic_reject_append`); err != nil {
				t.Fatal(err)
			}
			if err := call(); err != nil {
				t.Fatalf("retry after audit recovery: %v", err)
			}
			if membershipWriteAuditCount(t, p) != before+1 {
				t.Fatal("successful retry did not append exactly once")
			}
		})
	}
}

func waitMembershipLock(t *testing.T, p *pgPersister, class int32, key string, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("write completed before lock release: %v", err)
		default:
		}
		var waiting bool
		if err := p.pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_locks
			WHERE locktype='advisory' AND classid=$1::oid AND objid=$2::oid AND NOT granted
			AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, uint32(class), uint32(auditLockID(key))).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("membership operation never waited on the database lock")
}

func TestPostgresMembershipWritesAuthorizeAfterWaiting(t *testing.T) {
	s, p, ids := membershipWriteFixture(t)
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := lockMembershipTenant(ctx, tx, membershipWriteTenant); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.RemoveTenantMembership(membershipWriteTenant, ids["admin"], ids["target"], HumanActor(ids["admin"], membershipWriteTenant))
		done <- err
	}()
	waitMembershipLock(t, p, membershipLockClass, membershipWriteTenant, done)
	if _, err := tx.Exec(ctx, `UPDATE tenant_memberships SET data=jsonb_set(data,'{Role}','"viewer"') WHERE id=$1`, membershipID(ids["admin"], membershipWriteTenant)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("caller demoted while waiting still mutated: %v", err)
	}
	if _, err := s.MembershipFor(ids["target"], membershipWriteTenant); err != nil {
		t.Fatal("refused removal deleted target")
	}
}

func TestPostgresMembershipWritesEmailRegistrySerialization(t *testing.T) {
	s, p, ids := membershipWriteFixture(t)
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1,$2)`, accountIssuerLockClass, auditLockID(membershipWriteIssuer)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := s.AddTenantMemberByEmail(membershipWriteTenant, ids["owner"], membershipWriteIssuer, "newcomer@synthetic.invalid", RoleMember, HumanActor(ids["owner"], membershipWriteTenant))
		done <- err
	}()
	waitMembershipLock(t, p, accountIssuerLockClass, membershipWriteIssuer, done)
	if _, err := tx.Exec(ctx, `UPDATE accounts SET data=jsonb_set(data,'{EmailVerified}','false') WHERE id=$1`, ids["newcomer"]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("email grant ignored current verification: %v", err)
	}
	// The opposite direction: profile writers must also take this lock. No
	// profile update may commit through the shared lock held by an email grant.
	tx, err = p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1,$2)`, accountIssuerLockClass, auditLockID(membershipWriteIssuer)); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, err := s.UpsertAccount(membershipWriteIssuer, "newcomer", AccountProfile{Email: "new-address@synthetic.invalid", EmailVerified: true})
		done <- err
	}()
	waitMembershipLock(t, p, accountIssuerLockClass, membershipWriteIssuer, done)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPostgresMembershipWritesTenantLifecycleSerialization(t *testing.T) {
	for _, op := range []string{"revoke", "finalize", "delete", "create"} {
		t.Run(op, func(t *testing.T) {
			s, p, _ := membershipWriteFixture(t)
			ctx := context.Background()
			if op == "finalize" {
				if err := s.RevokeCustomer(membershipWriteTenant); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := p.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if err := lockMembershipTenant(ctx, tx, membershipWriteTenant); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				var err error
				switch op {
				case "revoke":
					err = s.RevokeCustomer(membershipWriteTenant)
				case "finalize":
					err = s.DeleteRevokedCustomer(membershipWriteTenant)
				case "delete":
					err = p.deleteCustomerAndMemberships(membershipWriteTenant)
				case "create":
					err = p.createTenant(&Customer{ID: membershipWriteTenant, Token: "synthetic-duplicate"}, nil, false)
				}
				done <- err
			}()
			waitMembershipLock(t, p, membershipLockClass, membershipWriteTenant, done)
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			err = <-done
			if op == "create" {
				if !errors.Is(err, ErrCustomerExists) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresMembershipWritesFirstTenantSerialization(t *testing.T) {
	s, p, ids := membershipWriteFixture(t)
	b := membershipWriteReplica(t, p)
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var id string
	if err := tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, ids["newcomer"]).Scan(&id); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := b.CreateFirstTenant(&Customer{ID: "synthetic-first-tenant", Token: "synthetic-first-token"}, ids["newcomer"])
		done <- err
	}()
	// Wait for the actual row-lock wait, not an arbitrary sleep.
	deadline := time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%FOR UPDATE%' AND pid<>pg_backend_pid())`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first tenant did not lock account")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A grant committed before that lock is released must prevent the waiting
	// zero-membership decision. This is a synthetic transaction fixture only.
	m := &TenantMembership{ID: membershipID(ids["newcomer"], membershipWriteTenant), AccountID: ids["newcomer"], CustomerID: membershipWriteTenant, Role: RoleMember, CreatedAt: time.Now().UTC()}
	if _, err := tx.Exec(ctx, `INSERT INTO tenant_memberships(id,data) VALUES($1,$2)`, m.ID, m); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrAccountHasTenant) {
		t.Fatalf("first workspace missed serialized grant: %v", err)
	}
	// The real grant path must also wait on the same account row.
	tx, err = p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, ids["newcomer"]).Scan(&id); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _, err := s.GrantTenantMembership(ids["newcomer"], membershipWriteTenant, RoleMember, OperatorActor())
		done <- err
	}()
	deadline = time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT data FROM accounts%FOR UPDATE%' AND pid<>pg_backend_pid())`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("grant did not lock account")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPostgresMembershipWritesBoundedLockWait(t *testing.T) {
	s, p, ids := membershipWriteFixture(t)
	tx, err := p.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err := lockMembershipTenant(context.Background(), tx, membershipWriteTenant); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, _, err = s.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleMember, OperatorActor())
	if !errors.Is(err, ErrPersistence) || !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 7*time.Second {
		t.Fatalf("lock wait was not bounded: %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleMember, OperatorActor()); err != nil {
		t.Fatalf("transaction locks leaked after timeout: %v", err)
	}
}

func TestPostgresMembershipWritesValidateRows(t *testing.T) {
	for _, query := range []string{
		`UPDATE tenant_memberships SET data=jsonb_set(data,'{Role}','"unknown"') WHERE data->>'Role'='viewer'`,
		`UPDATE tenant_memberships SET data=jsonb_set(data,'{ID}','"different-key"') WHERE data->>'Role'='owner'`,
		`UPDATE tenant_memberships SET data=jsonb_set(data,'{CustomerID}','"different-tenant"') WHERE data->>'Role'='viewer'`,
		`UPDATE accounts SET data=jsonb_set(data,'{Subject}','"different-subject"') WHERE data->>'Subject'='owner'`,
		`DELETE FROM accounts WHERE data->>'Subject'='owner'`,
		`UPDATE customers SET data='null'`,
	} {
		t.Run(fmt.Sprintf("case-%d", len(query)), func(t *testing.T) {
			s, p, ids := membershipWriteFixture(t)
			if _, err := p.pool.Exec(context.Background(), query); err != nil {
				t.Fatal(err)
			}
			before := membershipWriteAuditCount(t, p)
			if _, _, err := s.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleMember, OperatorActor()); !errors.Is(err, ErrPersistence) {
				t.Fatalf("malformed authority accepted: %v", err)
			}
			if membershipWriteAuditCount(t, p) != before {
				t.Fatal("malformed authority wrote audit")
			}
		})
	}
}

func TestPostgresMembershipWritesEmailIdentityContracts(t *testing.T) {
	for _, tc := range []struct {
		name, issuer, email string
		verified, allow     bool
	}{
		{"new-account", membershipWriteIssuer, "fresh@synthetic.invalid", true, true},
		{"ambiguous", membershipWriteIssuer, "newcomer@synthetic.invalid", true, false},
		{"unverified", membershipWriteIssuer, "unverified@synthetic.invalid", false, false},
		{"wrong-issuer", "different-synthetic-issuer", "other@synthetic.invalid", true, false},
		{"unicode", membershipWriteIssuer, "ſynthetic@synthetic.invalid", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, p, ids := membershipWriteFixture(t)
			b := membershipWriteReplica(t, p)
			account, err := a.UpsertAccount(tc.issuer, "fresh-subject", AccountProfile{Email: tc.email, EmailVerified: tc.verified})
			if err != nil {
				t.Fatal(err)
			}
			email := tc.email
			if tc.name == "unicode" {
				email = "SYNTHETIC@synthetic.invalid"
			}
			m, created, err := b.AddTenantMemberByEmail(membershipWriteTenant, ids["owner"], membershipWriteIssuer, email, RoleMember, HumanActor(ids["owner"], membershipWriteTenant))
			if tc.allow {
				if err != nil || !created || m.AccountID != account.ID {
					t.Fatalf("current identity not granted: %v", err)
				}
				if _, err := b.MembershipFor(account.ID, membershipWriteTenant); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrTargetNotFound) {
				t.Fatalf("ambiguous/unverified/foreign identity accepted: %v", err)
			}
		})
	}
}

func TestPostgresMembershipWritesLegacyPathsFailClosed(t *testing.T) {
	s, p, ids := membershipWriteFixture(t)
	m, err := s.MembershipFor(ids["target"], membershipWriteTenant)
	if err != nil {
		t.Fatal(err)
	}
	before := membershipWriteAuditCount(t, p)
	m.Role = RoleOwner
	if err := p.upsertMembership(m, nil); !errors.Is(err, ErrPersistence) {
		t.Fatalf("legacy cached upsert allowed: %v", err)
	}
	if err := p.deleteMembership(m.ID, nil); !errors.Is(err, ErrPersistence) {
		t.Fatalf("legacy cached delete allowed: %v", err)
	}
	if membershipWriteAuditCount(t, p) != before {
		t.Fatal("legacy write appended an event")
	}
	current, err := s.MembershipFor(ids["target"], membershipWriteTenant)
	if err != nil || current.Role != RoleViewer {
		t.Fatal("legacy write modified authority")
	}
}
