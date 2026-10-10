//go:build integration

package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func accountReadFixture(t *testing.T) (*Store, *Store, *pgPersister, string, string, string) {
	t.Helper()
	a, p := accountWriteFixture(t)
	owner, err := a.ResolveAccount(accountWriteIssuer, "read-owner")
	if err != nil {
		t.Fatal(err)
	}
	member, err := a.UpsertAccount(accountWriteIssuer, "read-member", AccountProfile{Name: "Before"})
	if err != nil {
		t.Fatal(err)
	}
	const tenant = "synthetic-account-read-tenant"
	if _, _, err := a.CreateTenant(&Customer{ID: tenant, Token: "synthetic-account-read-key", Name: "Before", Plan: "pilot", WorkloadNamespaces: []string{"before"}}, owner.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AddTenantMembership(member.ID, tenant, RoleViewer); err != nil {
		t.Fatal(err)
	}
	return a, accountWriteReplica(t, p), p, tenant, owner.ID, member.ID
}

func TestPostgresAccountReadsTenantSettingsAndGrants(t *testing.T) {
	a, b, p, tenant, owner, member := accountReadFixture(t)
	ctx := context.Background()
	if _, err := p.pool.Exec(ctx, `UPDATE customers SET data=data || '{"Name":"After","Plan":"new-plan","MaxConcurrentBursts":7,"MaxHourlyUSD":0.75,"WorkloadNamespaces":["jobs","batch"]}'::jsonb WHERE id=$1`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.SetTenantMembershipRole(member, tenant, RoleAdmin, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	rows, err := b.AccountTenantsContext(ctx, member)
	if err != nil || len(rows) != 1 {
		t.Fatalf("tenants: %v, %v", rows, err)
	}
	row := rows[0]
	if row.Membership.Role != RoleAdmin || row.Tenant.Name != "After" || row.Tenant.Plan != "new-plan" || row.Tenant.MaxConcurrentBursts != 7 || row.Tenant.MaxHourlyUSD != .75 || !slices.Equal(row.Tenant.WorkloadNamespaces, []string{"jobs", "batch"}) {
		t.Fatalf("stale joined projection: %+v", row)
	}
	summary, err := b.TenantSummaryByID(tenant)
	if err != nil || !reflect.DeepEqual(summary, row.Tenant) {
		t.Fatalf("tenant summary: %+v, %v", summary, err)
	}
	row.Tenant.WorkloadNamespaces[0] = "caller-local"
	rows[0].Membership.Role = RoleOwner
	current, err := b.AccountTenantsContext(ctx, member)
	if err != nil || current[0].Membership.Role != RoleAdmin || current[0].Tenant.WorkloadNamespaces[0] != "jobs" {
		t.Fatalf("caller mutated durable data: %+v, %v", current, err)
	}
	if b.customers[tenant].Name != "Before" || b.membershipsByAccount[member][tenant].Role != RoleViewer {
		t.Fatal("read hydrated mutation caches")
	}
	for _, read := range []func() ([]*TenantMembership, error){
		func() ([]*TenantMembership, error) { return b.MembershipsForAccountContext(ctx, member) },
		func() ([]*TenantMembership, error) { return b.MembershipsForTenantContext(ctx, tenant) },
	} {
		grants, err := read()
		if err != nil {
			t.Fatal(err)
		}
		for _, grant := range grants {
			if grant.AccountID == member && grant.Role != RoleAdmin {
				t.Fatalf("stale grant: %+v", grant)
			}
		}
	}
	if _, err := a.DeleteTenantMembership(member, tenant, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	if got, err := b.AccountTenantsContext(ctx, member); err != nil || len(got) != 0 {
		t.Fatalf("deleted grant persisted: %+v, %v", got, err)
	}
	if got := b.MembershipsForAccount(member); len(got) != 0 {
		t.Fatalf("legacy list retained deleted grant: %+v", got)
	}
	if _, err := p.pool.Exec(ctx, `INSERT INTO customer_tombstones (id,revoked_at) VALUES ($1,now())`, tenant); err != nil {
		t.Fatal(err)
	}
	if got, err := b.AccountTenantsContext(ctx, owner); err != nil || len(got) != 0 {
		t.Fatalf("tombstoned tenant advertised: %+v, %v", got, err)
	}
	if _, err := b.TenantSummaryByID(tenant); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tombstoned summary: %v", err)
	}
}

func TestPostgresAccountReadsRosterMissingProfile(t *testing.T) {
	_, b, p, tenant, owner, member := accountReadFixture(t)
	if _, err := p.pool.Exec(context.Background(), `DELETE FROM accounts WHERE id=$1`, member); err != nil {
		t.Fatal(err)
	}
	got, err := b.TenantRosterFor(tenant, owner, RosterQuery{})
	if err != nil || len(got.Members) != 2 {
		t.Fatalf("dangling grant was hidden: %+v, %v", got, err)
	}
	for _, row := range got.Members {
		if row.AccountID == member && (row.Role != RoleViewer || row.Name != "" || row.Email != "") {
			t.Fatalf("missing profile: %+v", row)
		}
	}
}

func TestPostgresAccountReadsNewIdentityAndGrant(t *testing.T) {
	a, b, _, tenant, _, _ := accountReadFixture(t)
	account, err := a.ResolveAccount(accountWriteIssuer, "new-after-replica")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.AddTenantMembership(account.ID, tenant, RoleMember); err != nil {
		t.Fatal(err)
	}
	if got, err := b.AccountByID(account.ID); err != nil || got.ID != account.ID {
		t.Fatalf("new account: %+v, %v", got, err)
	}
	if got, err := b.AccountTenantsContext(context.Background(), account.ID); err != nil || len(got) != 1 || got[0].Membership.Role != RoleMember {
		t.Fatalf("new grant: %+v, %v", got, err)
	}
	if b.accounts[account.ID] != nil || len(b.membershipsByAccount[account.ID]) != 0 {
		t.Fatal("new rows hydrated mutation maps")
	}
	const newTenant = "a-synthetic-new-tenant"
	if _, _, err := a.CreateTenant(&Customer{ID: newTenant, Token: "synthetic-new-tenant-key"}, account.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := b.AccountTenantsContext(context.Background(), account.ID); err != nil || len(got) != 2 || got[0].Tenant.ID != newTenant {
		t.Fatalf("new tenant missing or unsorted: %+v, %v", got, err)
	}
	if b.customers[newTenant] != nil {
		t.Fatal("tenant read hydrated mutation map")
	}
	if _, err := a.ResolveAccount(accountWriteIssuer, "no-grants"); err != nil {
		t.Fatal(err)
	}
	if got, err := b.AccountTenantsContext(context.Background(), accountID(accountWriteIssuer, "no-grants")); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("known account without grants: %+v, %v", got, err)
	}
	if _, err := b.AccountTenantsContext(context.Background(), "unknown-account"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown principal: %v", err)
	}
	if err := a.RevokeCustomer(newTenant); err != nil {
		t.Fatal(err)
	}
	if got, err := b.TenantSummaryByID(newTenant); err != nil || !got.Revoked {
		t.Fatalf("revoked summary contract changed: %+v, %v", got, err)
	}
}

type accountReadTraceKey struct{}
type accountReadTrace struct {
	once      sync.Once
	afterAuth func()
}

func (t *accountReadTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, accountReadTraceKey{}, strings.Contains(d.SQL, "SELECT m.data, a.data, c.data->>'ID'"))
}
func (t *accountReadTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if matched, _ := ctx.Value(accountReadTraceKey{}).(bool); matched {
		t.once.Do(t.afterAuth)
	}
}

func TestPostgresAccountReadsRosterHasOneSnapshot(t *testing.T) {
	for _, change := range []string{"remove-target", "revoke-tenant"} {
		t.Run(change, func(t *testing.T) {
			a, b, p, tenant, owner, member := accountReadFixture(t)
			var changed bool
			tracer := &accountReadTrace{afterAuth: func() {
				var err error
				if change == "remove-target" {
					_, err = a.DeleteTenantMembership(member, tenant, OperatorActor())
				} else {
					err = a.RevokeCustomer(tenant)
				}
				if err != nil {
					t.Fatal(err)
				}
				changed = true
			}}
			cfg := p.pool.Config()
			cfg.ConnConfig.Tracer = tracer
			pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			b.persist = &pgPersister{pool: pool}
			roster, err := b.TenantRosterFor(tenant, owner, RosterQuery{})
			if !changed {
				t.Fatal("fixture never crossed the authorization/page boundary")
			}
			if err != nil || len(roster.Members) != 2 {
				t.Fatalf("roster straddled committed %s: %+v, %v", change, roster, err)
			}
			roster, err = b.TenantRosterFor(tenant, owner, RosterQuery{})
			if change == "remove-target" {
				if err != nil || len(roster.Members) != 1 {
					t.Fatalf("next snapshot ignored deletion: %+v, %v", roster, err)
				}
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatalf("next snapshot ignored revoke: %v", err)
			}
		})
	}
}

func TestPostgresAccountReadsCancellationAndOutage(t *testing.T) {
	_, b, p, tenant, owner, _ := accountReadFixture(t)
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `LOCK TABLE accounts, customers IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	for name, read := range accountProjectionReads(b, owner, tenant) {
		t.Run(name, func(t *testing.T) {
			readCtx, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
			defer cancel()
			start := time.Now()
			err := read(readCtx)
			if !errors.Is(err, ErrPersistence) || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
				t.Fatalf("blocked read did not respect deadline: %v", err)
			}
		})
	}
	start := time.Now()
	if _, err := b.TenantRosterFor(tenant, owner, RosterQuery{}); !errors.Is(err, ErrPersistence) || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 7*time.Second {
		t.Fatalf("default read deadline: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for name, read := range accountProjectionReads(b, owner, tenant) {
		if err := read(ctx); err != nil {
			t.Fatalf("%s failed after lock released: %v", name, err)
		}
	}
	p.pool.Close()
	for name, read := range accountProjectionReads(b, owner, tenant) {
		if err := read(ctx); !errors.Is(err, ErrPersistence) {
			t.Fatalf("%s cache bypassed closed pool: %v", name, err)
		}
	}
}

func TestPostgresAccountReadsCorruptRowsFailClosed(t *testing.T) {
	for _, tc := range []struct{ name, table, field, value string }{
		{"account-binding", "accounts", "Subject", `"wrong"`},
		{"account-profile", "accounts", "Email", `42`},
		{"member-binding", "tenant_memberships", "ID", `"wrong"`},
		{"member-role", "tenant_memberships", "Role", `"super-admin"`},
		{"member-date", "tenant_memberships", "CreatedAt", `"not-a-date"`},
		{"tenant-binding", "customers", "ID", `"wrong"`},
		{"tenant-settings", "customers", "WorkloadNamespaces", `42`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, b, p, tenant, owner, _ := accountReadFixture(t)
			id := owner
			if tc.table == "customers" {
				id = tenant
			}
			if tc.table == "tenant_memberships" {
				id = membershipID(owner, tenant)
			}
			query := fmt.Sprintf("UPDATE %s SET data=jsonb_set(data,ARRAY[$2],$3::jsonb) WHERE id=$1", tc.table)
			if _, err := p.pool.Exec(context.Background(), query, id, tc.field, tc.value); err != nil {
				t.Fatal(err)
			}
			got, err := b.AccountTenantsContext(context.Background(), owner)
			if !errors.Is(err, ErrPersistence) || got != nil {
				t.Fatalf("malformed account response was accepted: %+v, %v", got, err)
			}
			if tc.name != "tenant-settings" {
				if _, err := b.TenantRosterFor(tenant, owner, RosterQuery{}); !errors.Is(err, ErrPersistence) {
					t.Fatalf("malformed roster was accepted: %v", err)
				}
			}
		})
	}
}

func TestPostgresAccountReadsProjectionExcludesCredentials(t *testing.T) {
	_, b, p, tenant, owner, _ := accountReadFixture(t)
	if _, err := p.pool.Exec(context.Background(), `UPDATE customers SET data=data || '{"Mesh":{"APIKey":"synthetic-never-project-mesh"},"LinodeCloudAccount":{"Token":"synthetic-never-project-provider"},"UnrelatedPrivateField":"synthetic-never-project-other"}'::jsonb WHERE id=$1`, tenant); err != nil {
		t.Fatal(err)
	}
	rows, err := b.AccountTenantsContext(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "synthetic-never-project") || strings.Contains(string(encoded), "synthetic-account-read-key") {
		t.Fatal("private customer field escaped projection")
	}
}

func TestPostgresAccountReadsRosterPagination(t *testing.T) {
	_, b, p, tenant, owner, member := accountReadFixture(t)
	ctx := context.Background()
	ids := []string{owner, member}
	batch := &pgx.Batch{}
	for i := range MaxRosterLimit + 7 {
		a, _ := accountAfterRefresh(accountWriteIssuer, fmt.Sprintf("page-%04d", i), nil, &AccountProfile{Name: "Page member"}, time.Now())
		m := TenantMembership{ID: membershipID(a.ID, tenant), AccountID: a.ID, CustomerID: tenant, Role: RoleViewer, CreatedAt: a.CreatedAt}
		accountData, _ := json.Marshal(a)
		memberData, _ := json.Marshal(m)
		batch.Queue(`INSERT INTO accounts (id,data) VALUES ($1,$2)`, a.ID, accountData)
		batch.Queue(`INSERT INTO tenant_memberships (id,data) VALUES ($1,$2)`, m.ID, memberData)
		ids = append(ids, a.ID)
	}
	results := p.pool.SendBatch(ctx, batch)
	if err := results.Close(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	for _, q := range []RosterQuery{{}, {Limit: -1}, {Limit: 1}, {Limit: MaxRosterLimit + 100}} {
		got, err := b.TenantRosterFor(tenant, owner, q)
		limit := rosterLimit(q.Limit)
		if err != nil || len(got.Members) != limit || got.NextAfter != ids[limit-1] {
			t.Fatalf("page bounds %+v: size=%d cursor=%q err=%v", q, len(got.Members), got.NextAfter, err)
		}
	}
	var seen []string
	after := ""
	for {
		got, err := b.TenantRosterFor(tenant, owner, RosterQuery{Limit: 37, After: after})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range got.Members {
			seen = append(seen, row.AccountID)
		}
		if got.NextAfter == "" {
			break
		}
		if got.NextAfter <= after {
			t.Fatal("cursor did not advance")
		}
		after = got.NextAfter
	}
	if !slices.Equal(seen, ids) {
		t.Fatal("page traversal lost, duplicated or reordered grants")
	}
	if got, err := b.TenantRosterFor(tenant, owner, RosterQuery{After: ids[len(ids)-1]}); err != nil || len(got.Members) != 0 || got.NextAfter != "" {
		t.Fatalf("last page: %+v, %v", got, err)
	}
	// Corrupt data beyond limit+1 must not be decoded by an unrelated page.
	// This fails if the implementation loads the whole roster before slicing.
	last := ids[len(ids)-1]
	if last == owner {
		last = ids[len(ids)-2]
	}
	if _, err := p.pool.Exec(ctx, `UPDATE accounts SET data='null' WHERE id=$1`, last); err != nil {
		t.Fatal(err)
	}
	if got, err := b.TenantRosterFor(tenant, owner, RosterQuery{Limit: 1}); err != nil || len(got.Members) != 1 {
		t.Fatalf("first page read beyond SQL bound: %+v, %v", got, err)
	}
	if got, err := b.TenantRosterFor(tenant, owner, RosterQuery{After: ids[len(ids)-3]}); !errors.Is(err, ErrPersistence) || len(got.Members) != 0 {
		t.Fatalf("corrupt page returned partial data: %+v, %v", got, err)
	}
}

func TestPostgresAccountReadsTenantJoinHasOneSnapshot(t *testing.T) {
	_, b, p, tenant, _, member := accountReadFixture(t)
	ctx := context.Background()
	// Each revision binds its name and role. A reader using independent SQL
	// queries can combine them even though writers commit both atomically.
	change := func(role string) error {
		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `UPDATE customers SET data=jsonb_set(data,'{Name}',to_jsonb($2::text)) WHERE id=$1`, tenant, role); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE tenant_memberships SET data=jsonb_set(data,'{Role}',to_jsonb($2::text)) WHERE id=$1`, membershipID(member, tenant), role); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if err := change(RoleViewer); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		for i := range 40 {
			role := RoleViewer
			if i%2 == 0 {
				role = RoleAdmin
			}
			if err := change(role); err != nil {
				finished <- err
				return
			}
		}
		finished <- nil
	}()
	defer func() {
		if err := <-finished; err != nil {
			t.Error(err)
		}
	}()
	for range 100 {
		got, err := b.AccountTenantsContext(ctx, member)
		if err != nil || len(got) != 1 {
			t.Fatalf("joined account snapshot: %+v, %v", got, err)
		}
		if got[0].Membership.Role != got[0].Tenant.Name {
			t.Fatalf("mixed committed revisions: %+v", got)
		}
	}
}

func TestPostgresAccountReadsProfileAcrossReplicas(t *testing.T) {
	a, p := accountWriteFixture(t)
	account, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Name: "Before"})
	if err != nil {
		t.Fatal(err)
	}
	b := accountWriteReplica(t, p)
	if _, err := a.UpsertAccount(accountWriteIssuer, accountWriteSubject, AccountProfile{Name: "After"}); err != nil {
		t.Fatal(err)
	}
	got, err := b.AccountByID(account.ID)
	if err != nil || got.Name != "After" {
		t.Fatalf("durable profile not observed: got %v, err %v", got, err)
	}
	if b.accounts[account.ID].Name != "Before" {
		t.Fatal("read hydrated mutation cache")
	}
	p.pool.Close()
	if _, err := b.AccountByID(account.ID); !errors.Is(err, ErrPersistence) {
		t.Fatalf("account cache bypassed SQL outage: %v", err)
	}
}

func TestPostgresAccountReadsRosterAcrossReplicas(t *testing.T) {
	a, p := accountWriteFixture(t)
	owner, err := a.ResolveAccount(accountWriteIssuer, "owner")
	if err != nil {
		t.Fatal(err)
	}
	member, err := a.UpsertAccount(accountWriteIssuer, "member", AccountProfile{Name: "Before"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.CreateTenant(&Customer{ID: "synthetic-roster-tenant", Token: "synthetic-roster-key"}, owner.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AddTenantMembership(member.ID, "synthetic-roster-tenant", RoleViewer); err != nil {
		t.Fatal(err)
	}
	b := accountWriteReplica(t, p)
	if _, err := a.UpsertAccount(accountWriteIssuer, "member", AccountProfile{Name: "After"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.SetTenantMembershipRole(member.ID, "synthetic-roster-tenant", RoleAdmin, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	got, err := b.TenantRosterFor("synthetic-roster-tenant", owner.ID, RosterQuery{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range got.Members {
		if row.AccountID == member.ID && (row.Name != "After" || row.Role != RoleAdmin) {
			t.Fatalf("roster returned stale durable member: %+v", row)
		}
	}
	if _, err := a.DeleteTenantMembership(member.ID, "synthetic-roster-tenant", OperatorActor()); err != nil {
		t.Fatal(err)
	}
	got, err = b.TenantRosterFor("synthetic-roster-tenant", owner.ID, RosterQuery{})
	if err != nil || len(got.Members) != 1 {
		t.Fatalf("removed member still in roster: %v, %v", got, err)
	}
}
