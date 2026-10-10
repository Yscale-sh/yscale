package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// accountSpyPersister records the rows a store writes so a test can replay them
// through the real load path (JSON → snapshot → applySnapshot) and prove
// durability without a database. Rows are stored marshalled, exactly as the
// JSONB backend does, so a field that doesn't survive encoding fails here too.
type accountSpyPersister struct {
	auditRecorder
	credentialMu sync.RWMutex
	accounts     map[string][]byte
	memberships  map[string][]byte
	customers    map[string][]byte
	workloads    map[string]*Workload
	// tombstones is customer_tombstones: ids a finished offboard retired for
	// good, keyed the same way and never removed. Rows live here rather than in
	// customers precisely because the customers row is gone by then.
	tombstones  map[string]time.Time
	upsertErr   error // when set, membership write-throughs fail
	deleteErr   error // when set, membership deletes fail
	accountErr  error // when set, account write-throughs fail
	createErr   error // when set, the durable create transaction fails
	revokeErr   error // when set, the revoke transaction fails
	offboardErr error // when set, the final-delete transaction fails
	// accountWrites counts durable account writes, so a test can prove a poll
	// that changed nothing wrote nothing.
	accountWrites int
	// beforeRevoke runs inside the revoke transaction, before it commits. It is
	// how a test parks a revoke mid-flight and drives another customer writer at
	// it, which is the race the store's customer lock exists to lose.
	beforeRevoke func()
}

func newAccountSpy() *accountSpyPersister {
	return &accountSpyPersister{
		accounts:    make(map[string][]byte),
		memberships: make(map[string][]byte),
		customers:   make(map[string][]byte),
		tombstones:  make(map[string]time.Time),
	}
}

func (p *accountSpyPersister) upsertAccount(a *Account) error {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	if p.accountErr != nil {
		return p.accountErr
	}
	b, err := json.Marshal(a)
	if err != nil {
		return err
	}
	p.accounts[a.ID] = b
	p.accountWrites++
	return nil
}

// upsertMembership mirrors the Postgres transaction: the grant and its journal
// row land together or not at all, so a refused audit append leaves the store
// with no grant to roll back to.
func (p *accountSpyPersister) upsertMembership(m *TenantMembership, ev *AuditEvent) error {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	if p.upsertErr != nil {
		return p.upsertErr
	}
	if err := p.appendAudit(ev); err != nil {
		return err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	p.memberships[m.ID] = b
	return nil
}

// deleteMembership mirrors the targeted Postgres delete: one row by id, and on
// failure the row is left exactly where it was — which is what lets a test prove
// the store put the in-memory grant back.
func (p *accountSpyPersister) deleteMembership(id string, ev *AuditEvent) error {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	if p.deleteErr != nil {
		return p.deleteErr
	}
	if err := p.appendAudit(ev); err != nil {
		return err
	}
	delete(p.memberships, id)
	return nil
}

// upsertCustomerAudited is the customer document and its journal row in one
// step, with upsertCustomer's guards intact — a tombstoned id is still refused,
// and a refusal writes no audit row for a change that did not happen.
func (p *accountSpyPersister) upsertCustomerAudited(c *Customer, ev *AuditEvent) error {
	return p.withAudit(ev, func() error { return p.upsertCustomer(c) })
}

// upsertCustomer mirrors upsertCustomerStmt's two guards. A tombstoned id is
// refused outright — the row is gone, so an unguarded write would INSERT a live
// customer back — and a stored RevokedAt survives a whole-document write that
// does not carry one, because revocation is one-way.
func (p *accountSpyPersister) upsertCustomer(c *Customer) error {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	if _, retired := p.tombstones[c.ID]; retired {
		return fmt.Errorf("%w: %s", ErrCustomerTombstoned, c.ID)
	}
	row := *c
	if raw, ok := p.customers[c.ID]; ok {
		var stored Customer
		if err := json.Unmarshal(raw, &stored); err != nil {
			return err
		}
		if stored.Revoked() {
			row.RevokedAt = stored.RevokedAt
		}
	}
	b, err := json.Marshal(&row)
	if err != nil {
		return err
	}
	p.customers[c.ID] = b
	return nil
}

// createTenant mirrors the Postgres transaction: the id itself is the
// uniqueness check (so a row this store never held in memory still refuses the
// create), any grant left over for that id is cleared with the insert, the
// owner's account must already have a row, and a refusal changes nothing at all
// — no customer, no grant.
func (p *accountSpyPersister) createTenant(c *Customer, owner *TenantMembership, requireFirst bool) error {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	if requireFirst && owner == nil {
		return ErrNotFound
	}
	if p.createErr != nil {
		return p.createErr
	}
	if _, taken := p.customers[c.ID]; taken {
		return fmt.Errorf("%w: %s", ErrCustomerExists, c.ID)
	}
	// A retired id is taken too, and answers the same conflict: its row is gone,
	// so the check above cannot see it.
	if _, retired := p.tombstones[c.ID]; retired {
		return fmt.Errorf("%w: %s", ErrCustomerExists, c.ID)
	}
	candidate := *c
	var ownerRow []byte
	var err error
	if owner != nil {
		if _, ok := p.accounts[owner.AccountID]; !ok {
			return fmt.Errorf("account %q: %w", owner.AccountID, ErrNotFound)
		}
		if requireFirst {
			account, err := decodeAccountBinding(owner.AccountID, p.accounts[owner.AccountID])
			if err != nil {
				return err
			}
			hasTenant := false
			for _, raw := range p.memberships {
				var existing TenantMembership
				if err := json.Unmarshal(raw, &existing); err != nil {
					return err
				}
				if existing.AccountID == owner.AccountID {
					hasTenant = true
				}
			}
			if err := firstWorkspaceEligibility(account, hasTenant); err != nil {
				return err
			}
			candidate.Email = account.Email
		}
		if ownerRow, err = json.Marshal(owner); err != nil {
			return err
		}
	}
	b, err := json.Marshal(&candidate)
	if err != nil {
		return err
	}
	if err := p.dropMemberships(c.ID); err != nil {
		return err
	}
	p.customers[c.ID] = b
	if owner != nil {
		p.memberships[owner.ID] = ownerRow
	}
	c.Email = candidate.Email
	return nil
}

// revokeCustomer mirrors the Postgres transaction: on failure NOTHING changes,
// and on success the stamp and every membership naming the tenant move
// together. A spy that wrote the customer first and then failed would hide
// exactly the bug this is here to catch.
//
// Like the real one it PATCHES RevokedAt onto the stored row rather than
// writing the caller's whole snapshot back, so a test can tell the difference
// between a revoke that clobbered a concurrent mesh/route write and one that
// did not. A row that was never written is inserted whole.
func (p *accountSpyPersister) revokeCustomer(c *Customer) error {
	if p.beforeRevoke != nil {
		p.beforeRevoke()
	}
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	if p.revokeErr != nil {
		return p.revokeErr
	}
	row := *c
	stored, ok := p.customers[c.ID]
	if ok {
		row = Customer{}
		if err := json.Unmarshal(stored, &row); err != nil {
			return err
		}
		row.RevokedAt = c.RevokedAt
	}
	// The insert half is guarded like the real one: a retired id has no row to
	// patch, and writing the stamped record back would give it one again.
	if _, retired := p.tombstones[c.ID]; !ok && retired {
		return nil
	}
	b, err := json.Marshal(&row)
	if err != nil {
		return err
	}
	if err := p.dropMemberships(c.ID); err != nil {
		return err
	}
	p.customers[c.ID] = b
	return nil
}

// finalizeRevokedCustomer mirrors the Postgres transaction: on failure NOTHING
// changes, and on success the tombstone, the customer and every membership
// naming it move together. The tombstone carries the stamp off the row it
// retires, and outlives it.
func (p *accountSpyPersister) finalizeRevokedCustomer(id string) error {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	if p.offboardErr != nil {
		return p.offboardErr
	}
	if _, already := p.tombstones[id]; !already {
		// The stamp is read off the row when there is one; retiring the id does
		// not depend on it still being there.
		var revokedAt time.Time
		if raw, ok := p.customers[id]; ok {
			var row Customer
			if err := json.Unmarshal(raw, &row); err != nil {
				return err
			}
			if row.RevokedAt != nil {
				revokedAt = *row.RevokedAt
			}
		}
		p.tombstones[id] = revokedAt
	}
	return p.deleteCustomerAndMembershipsLocked(id)
}

// deleteCustomerAndMemberships mirrors the legacy Postgres transaction: on
// failure NOTHING is removed, on success the customer and every membership
// naming it go together, and no tombstone is left — the id stays reusable.
func (p *accountSpyPersister) deleteCustomerAndMemberships(id string) error {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	return p.deleteCustomerAndMembershipsLocked(id)
}

func (p *accountSpyPersister) deleteCustomerAndMembershipsLocked(id string) error {
	if p.offboardErr != nil {
		return p.offboardErr
	}
	if err := p.dropMemberships(id); err != nil {
		return err
	}
	delete(p.customers, id)
	return nil
}

func (p *accountSpyPersister) dropMemberships(customerID string) error {
	for mid, raw := range p.memberships {
		var m TenantMembership
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		if m.CustomerID == customerID {
			delete(p.memberships, mid)
		}
	}
	return nil
}

func (p *accountSpyPersister) upsertWorkload(w *Workload) error {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	if p.workloads == nil {
		p.workloads = make(map[string]*Workload)
	}
	current := cloneWorkload(w)
	preserveWorkloadTransition(current, p.workloads[w.ID])
	p.workloads[w.ID] = current
	return nil
}

func (p *accountSpyPersister) transitionWorkload(ctx context.Context, req workloadTransition) (*Workload, bool, error) {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	w, err := transitionTestRow(p.workloads, req)
	if err != nil || w == nil {
		return nil, false, err
	}
	applied := applyWorkloadTransition(w, req)
	if applied {
		p.workloads[w.ID] = cloneWorkload(w)
	}
	return w, applied, nil
}
func (p *accountSpyPersister) stampWorkloadPodObservation(context.Context, string, string, string, string, string, time.Time) (int64, error) {
	return 1, nil
}
func (p *accountSpyPersister) getWorkloadPodObservation(context.Context, string, string, string, string) (*PodObservation, bool, error) {
	return nil, false, nil
}
func (p *accountSpyPersister) workloadByBurst(context.Context, string) (*Workload, error) {
	return nil, nil
}
func (p *accountSpyPersister) recordWorkloadCost(context.Context, string, *WorkloadCost) (bool, error) {
	return false, nil
}
func (p *accountSpyPersister) upsertBurst(*Burst) error { return nil }
func (p *accountSpyPersister) deleteBurst(string) error { return nil }
func (p *accountSpyPersister) updateBurstNodePhase(context.Context, BurstNodePhaseUpdate) (*burstNodePhaseResult, error) {
	return nil, nil
}
func (p *accountSpyPersister) updateBurstGPUTelemetry(context.Context, BurstGPUTelemetryUpdate) (bool, error) {
	return false, nil
}
func (p *accountSpyPersister) burstOwnedBy(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *accountSpyPersister) burstForTenant(context.Context, string, string) (*Burst, bool, error) {
	return nil, false, nil
}
func (p *accountSpyPersister) claimBurst(context.Context, string) (*Burst, bool, error) {
	return nil, false, nil
}
func (p *accountSpyPersister) recordBurstReapReceipt(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *accountSpyPersister) burstReapRecorded(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *accountSpyPersister) listBursts(context.Context) ([]*Burst, error) { return nil, nil }
func (p *accountSpyPersister) reservePodSlot(context.Context, int, string, time.Duration) (bool, error) {
	return true, nil
}
func (p *accountSpyPersister) releasePodSlot(context.Context, string) error { return nil }

// The idempotency trio: this fake's subject is elsewhere, so the claim seam is
// stubbed to "nothing was ever claimed".
func (p *accountSpyPersister) claimIdempotency(context.Context, *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	return nil, true, nil
}
func (p *accountSpyPersister) completeIdempotency(context.Context, string, int, []byte, time.Time) error {
	return nil
}
func (p *accountSpyPersister) deleteIdempotency(context.Context, string) error { return nil }

func (p *accountSpyPersister) reservedPodSlots(context.Context, time.Duration) (map[int]bool, error) {
	return nil, nil
}
func (p *accountSpyPersister) upsertPV(*PersistentVolume) error { return nil }
func (p *accountSpyPersister) deletePV(string) error            { return nil }
func (p *accountSpyPersister) Close()                           {}

// loadAll is the spy's half of the boot read: everything it recorded, decoded
// back into the snapshot shape the Postgres path produces. The retired ids come
// with it — they are the one collection with no row of their own left to carry
// them, so a restart that dropped them here would test a store that cannot tell
// a retired id from a free one.
func (p *accountSpyPersister) loadAll(context.Context) (*snapshot, error) {
	snap := &snapshot{}
	for _, raw := range p.customers {
		var c Customer
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, fmt.Errorf("decode customer row: %w", err)
		}
		snap.Customers = append(snap.Customers, &c)
	}
	for id := range p.tombstones {
		snap.Tombstones = append(snap.Tombstones, id)
	}
	for _, raw := range p.accounts {
		var a Account
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, fmt.Errorf("decode account row: %w", err)
		}
		snap.Accounts = append(snap.Accounts, &a)
	}
	for _, raw := range p.memberships {
		var m TenantMembership
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("decode membership row: %w", err)
		}
		snap.Memberships = append(snap.Memberships, &m)
	}
	return snap, nil
}

// snapshotOf is the boot read with a caller-chosen customer set, for tests that
// drive a restart from records they built rather than from the rows the spy
// happens to hold.
func (p *accountSpyPersister) snapshotOf(t *testing.T, customers ...*Customer) *snapshot {
	t.Helper()
	snap := p.recordedSnapshot(t)
	snap.Customers = customers
	return snap
}

// recordedSnapshot is the boot read over the rows the spy actually holds, which
// is what a restart reads: it shows whether a delete really removed the rows or
// only the in-memory copies.
func (p *accountSpyPersister) recordedSnapshot(t *testing.T) *snapshot {
	t.Helper()
	snap, err := p.loadAll(context.Background())
	if err != nil {
		t.Fatalf("load recorded rows: %v", err)
	}
	return snap
}

// storeWithSpy builds an in-memory store wired to a recording persister, plus
// one customer to hang memberships on.
func storeWithSpy(customerIDs ...string) (*Store, *accountSpyPersister) {
	s := emptyStore()
	spy := newAccountSpy()
	s.persist = spy
	for _, id := range customerIDs {
		s.AddCustomer(&Customer{ID: id, Token: "tok_" + id, Plan: "pro"})
	}
	return s, spy
}

// An account plus its memberships must come back after a restart, with the
// identity index rebuilt — a human whose account didn't reload would be handed
// a brand-new empty account on their next sign-in.
func TestAccountsSurviveRestart(t *testing.T) {
	s, spy := storeWithSpy("cust_a", "cust_b")
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-1",
		AccountProfile{Email: "human@acme.com", EmailVerified: true, Name: "A Human"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s.AddTenantMembership(acct.ID, "cust_a", RoleOwner); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}
	if _, err := s.AddTenantMembership(acct.ID, "cust_b", RoleViewer); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}

	reloaded := emptyStore()
	reloaded.applySnapshot(spy.snapshotOf(t,
		&Customer{ID: "cust_a", Token: "tok_cust_a", Plan: "pro"},
		&Customer{ID: "cust_b", Token: "tok_cust_b", Plan: "pro"},
	))

	got, err := reloaded.AccountByIdentity("https://id.yscale.sh", "sub-1")
	if err != nil {
		t.Fatalf("identity index not rebuilt after restart: %v", err)
	}
	if got.ID != acct.ID || got.Email != "human@acme.com" || !got.EmailVerified || got.Name != "A Human" {
		t.Fatalf("account fields lost across restart: %+v", got)
	}
	members := reloaded.MembershipsForAccount(acct.ID)
	if len(members) != 2 {
		t.Fatalf("memberships after restart = %d, want 2", len(members))
	}
	// Sorted by customer id, so the pairing is deterministic.
	if members[0].CustomerID != "cust_a" || members[0].Role != RoleOwner {
		t.Errorf("first membership = %+v, want cust_a/owner", members[0])
	}
	if members[1].CustomerID != "cust_b" || members[1].Role != RoleViewer {
		t.Errorf("second membership = %+v, want cust_b/viewer", members[1])
	}
}

// The account id is derived from (issuer, subject), so the same human always
// lands on the same durable row — that is what stops two central replicas from
// minting two accounts for one sign-in and splitting their memberships.
func TestUpsertAccountIsIdempotentPerIdentity(t *testing.T) {
	s, spy := storeWithSpy()
	first, err := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{Email: "old@acme.com"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	second, err := s.UpsertAccount("https://id.yscale.sh", "sub-1",
		AccountProfile{Email: "new@acme.com", EmailVerified: true})
	if err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("same identity minted two accounts: %s vs %s", first.ID, second.ID)
	}
	if len(spy.accounts) != 1 {
		t.Fatalf("durable account rows = %d, want 1", len(spy.accounts))
	}
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Errorf("CreatedAt moved on re-upsert: %v → %v", first.CreatedAt, second.CreatedAt)
	}
	// The profile refreshes from the issuer; the identity does not.
	if second.Email != "new@acme.com" || !second.EmailVerified {
		t.Errorf("profile not refreshed: %+v", second)
	}
	if second.Subject != "sub-1" || second.Issuer != "https://id.yscale.sh" {
		t.Errorf("identity changed on re-upsert: %+v", second)
	}
}

// Email is not an identity. Two humans at different issuers — or one address
// re-assigned to a new person — must never collapse onto one account.
func TestAccountsAreKeyedOnIssuerAndSubjectNotEmail(t *testing.T) {
	s, _ := storeWithSpy()
	a, err := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{Email: "shared@acme.com"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	b, err := s.UpsertAccount("https://id.yscale.sh", "sub-2", AccountProfile{Email: "shared@acme.com"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	other, err := s.UpsertAccount("https://other.example", "sub-1", AccountProfile{Email: "shared@acme.com"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if a.ID == b.ID || a.ID == other.ID || b.ID == other.ID {
		t.Fatalf("distinct identities collided: %s / %s / %s", a.ID, b.ID, other.ID)
	}
}

// A blank half of the identity key is refused everywhere — a resolver that
// returned an empty `sub` must not be able to mint or reach a shared account.
func TestBlankIdentityIsRejected(t *testing.T) {
	s, _ := storeWithSpy()
	for _, tc := range []struct{ issuer, subject string }{
		{"", "sub-1"},
		{"https://id.yscale.sh", ""},
		{"   ", "sub-1"},
		{"https://id.yscale.sh", "  "},
		{"", ""},
	} {
		if _, err := s.UpsertAccount(tc.issuer, tc.subject, AccountProfile{}); !errors.Is(err, ErrInvalidIdentity) {
			t.Errorf("UpsertAccount(%q,%q) err = %v, want ErrInvalidIdentity", tc.issuer, tc.subject, err)
		}
		if _, err := s.AccountByIdentity(tc.issuer, tc.subject); !errors.Is(err, ErrInvalidIdentity) {
			t.Errorf("AccountByIdentity(%q,%q) err = %v, want ErrInvalidIdentity", tc.issuer, tc.subject, err)
		}
	}
}

func TestMembershipRoleValidation(t *testing.T) {
	s, _ := storeWithSpy("cust_a")
	acct, _ := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{})

	for _, role := range []string{RoleOwner, RoleAdmin, RoleMember, RoleViewer} {
		if !ValidRole(role) {
			t.Errorf("ValidRole(%q) = false, want true", role)
		}
	}
	for _, role := range []string{"", "OWNER", "root", "superuser", "own er"} {
		if ValidRole(role) {
			t.Errorf("ValidRole(%q) = true, want false", role)
		}
		if _, err := s.AddTenantMembership(acct.ID, "cust_a", role); !errors.Is(err, ErrInvalidRole) {
			t.Errorf("AddTenantMembership role %q err = %v, want ErrInvalidRole", role, err)
		}
	}
}

// Re-adding the same binding is idempotent; re-adding it with a different role
// is refused, so a re-provision cannot silently demote an owner.
func TestDuplicateMembershipIsIdempotentButConflictingRoleIsRejected(t *testing.T) {
	s, spy := storeWithSpy("cust_a")
	acct, _ := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{})

	first, err := s.AddTenantMembership(acct.ID, "cust_a", RoleOwner)
	if err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}
	again, err := s.AddTenantMembership(acct.ID, "cust_a", RoleOwner)
	if err != nil {
		t.Fatalf("re-adding the same membership should be idempotent: %v", err)
	}
	if again.ID != first.ID {
		t.Fatalf("duplicate membership minted a second row: %s vs %s", first.ID, again.ID)
	}
	if len(spy.memberships) != 1 {
		t.Fatalf("durable membership rows = %d, want 1", len(spy.memberships))
	}
	if _, err := s.AddTenantMembership(acct.ID, "cust_a", RoleViewer); !errors.Is(err, ErrRoleConflict) {
		t.Fatalf("conflicting role err = %v, want ErrRoleConflict", err)
	}
	// The original binding is untouched by the rejected write.
	members := s.MembershipsForAccount(acct.ID)
	if len(members) != 1 || members[0].Role != RoleOwner {
		t.Fatalf("memberships after rejected role change = %+v", members)
	}
}

// Both ends of a membership must exist. A row naming a missing account or a
// missing tenant is the dangling record the load path has to discard, so it is
// never created in the first place.
func TestMembershipRequiresBothEnds(t *testing.T) {
	s, _ := storeWithSpy("cust_a")
	acct, _ := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{})

	if _, err := s.AddTenantMembership("acct_missing", "cust_a", RoleOwner); !errors.Is(err, ErrNotFound) {
		t.Errorf("membership for unknown account err = %v, want ErrNotFound", err)
	}
	if _, err := s.AddTenantMembership(acct.ID, "cust_missing", RoleOwner); !errors.Is(err, ErrNotFound) {
		t.Errorf("membership for unknown customer err = %v, want ErrNotFound", err)
	}
}

// A durable membership write that fails must not leave the binding live in
// memory: nothing re-creates a membership, so an in-memory-only grant would
// disappear at the next restart with no error ever surfaced.
func TestMembershipWriteFailureRollsBack(t *testing.T) {
	s, spy := storeWithSpy("cust_a")
	acct, _ := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{})
	spy.upsertErr = errors.New("postgres down")

	if _, err := s.AddTenantMembership(acct.ID, "cust_a", RoleOwner); err == nil {
		t.Fatal("AddTenantMembership should report a failed durable write")
	}
	if got := s.MembershipsForAccount(acct.ID); len(got) != 0 {
		t.Fatalf("membership survived a failed durable write: %+v", got)
	}
	// And the store is still usable once the backend recovers.
	spy.upsertErr = nil
	if _, err := s.AddTenantMembership(acct.ID, "cust_a", RoleOwner); err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}
	if got := s.MembershipsForAccount(acct.ID); len(got) != 1 {
		t.Fatalf("memberships after retry = %d, want 1", len(got))
	}
}

// One human's memberships are theirs alone: an account sees only the tenants it
// is bound to, and an unknown account sees nothing.
func TestMembershipLookupIsolation(t *testing.T) {
	s, _ := storeWithSpy("cust_a", "cust_b")
	alice, _ := s.UpsertAccount("https://id.yscale.sh", "alice", AccountProfile{})
	bob, _ := s.UpsertAccount("https://id.yscale.sh", "bob", AccountProfile{})

	if _, err := s.AddTenantMembership(alice.ID, "cust_a", RoleOwner); err != nil {
		t.Fatalf("alice membership: %v", err)
	}
	if _, err := s.AddTenantMembership(bob.ID, "cust_b", RoleAdmin); err != nil {
		t.Fatalf("bob membership: %v", err)
	}

	aliceTenants := s.MembershipsForAccount(alice.ID)
	if len(aliceTenants) != 1 || aliceTenants[0].CustomerID != "cust_a" {
		t.Fatalf("alice sees %+v, want only cust_a", aliceTenants)
	}
	bobTenants := s.MembershipsForAccount(bob.ID)
	if len(bobTenants) != 1 || bobTenants[0].CustomerID != "cust_b" {
		t.Fatalf("bob sees %+v, want only cust_b", bobTenants)
	}
	if got := s.MembershipsForAccount("acct_nobody"); len(got) != 0 {
		t.Fatalf("unknown account sees %+v, want nothing", got)
	}
	// A cluster token still resolves a Customer and never an Account, and the
	// reverse: the two credential namespaces do not cross.
	if _, err := s.AuthCustomer("tok_cust_a"); err != nil {
		t.Errorf("cluster token stopped authenticating: %v", err)
	}
	if _, err := s.AccountByIdentity("https://id.yscale.sh", "tok_cust_a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a cluster token resolved an account: %v", err)
	}
}

// Offboarding a tenant removes its memberships — in memory and durably — while
// leaving the human's account and their OTHER tenants alone.
func TestOffboardRemovesMemberships(t *testing.T) {
	s, spy := storeWithSpy("cust_a", "cust_b")
	alice, _ := s.UpsertAccount("https://id.yscale.sh", "alice", AccountProfile{})
	bob, _ := s.UpsertAccount("https://id.yscale.sh", "bob", AccountProfile{})
	for _, m := range []struct {
		account, customer, role string
	}{
		{alice.ID, "cust_a", RoleOwner},
		{alice.ID, "cust_b", RoleViewer},
		{bob.ID, "cust_a", RoleAdmin},
	} {
		if _, err := s.AddTenantMembership(m.account, m.customer, m.role); err != nil {
			t.Fatalf("seed membership: %v", err)
		}
	}

	s.DeleteCustomer("cust_a")

	aliceTenants := s.MembershipsForAccount(alice.ID)
	if len(aliceTenants) != 1 || aliceTenants[0].CustomerID != "cust_b" {
		t.Fatalf("alice after offboard = %+v, want only cust_b", aliceTenants)
	}
	if got := s.MembershipsForAccount(bob.ID); len(got) != 0 {
		t.Fatalf("bob kept a membership in the offboarded tenant: %+v", got)
	}
	// The account survives — a human outlives any one tenant.
	if _, err := s.AccountByIdentity("https://id.yscale.sh", "bob"); err != nil {
		t.Errorf("offboard deleted a human account: %v", err)
	}
	if len(spy.memberships) != 1 {
		t.Fatalf("durable membership rows after offboard = %d, want 1", len(spy.memberships))
	}
	// And the cleanup holds across a restart.
	reloaded := emptyStore()
	reloaded.applySnapshot(spy.snapshotOf(t, &Customer{ID: "cust_b", Token: "tok_cust_b", Plan: "pro"}))
	if got := reloaded.MembershipsForAccount(alice.ID); len(got) != 1 || got[0].CustomerID != "cust_b" {
		t.Fatalf("alice after restart = %+v, want only cust_b", got)
	}
}

// A durable membership whose account is missing, or whose tenant is missing or
// revoked, is left unindexed and left ALONE. Unindexed it grants nothing, so
// the danger is already handled, and load is a read path: deleting there would
// turn a grant that is merely unreadable right now — a half-restored database,
// a customer row still to be written — into a lost one. Removing rows is the
// writers' job: TestRevokeCustomerIsAtomicAndFailsClosed covers the revoke
// taking a tenant's grants with it, and TestReusedCustomerIDCannotInheritAStaleGrant
// covers the create clearing anything left over for an id it reissues.
func TestApplySnapshotKeepsDanglingMembershipRows(t *testing.T) {
	spy := newAccountSpy()
	s := emptyStore()
	s.persist = spy
	revokedAt := time.Now().UTC()
	snap := &snapshot{
		Customers: []*Customer{
			{ID: "cust_live", Token: "tok_live", Plan: "pro"},
			{ID: "cust_revoked", Token: "tok_revoked", Plan: "pro", RevokedAt: &revokedAt},
		},
		Accounts: []*Account{{
			ID: "acct_live", Issuer: "https://id.yscale.sh", Subject: "sub-1", CreatedAt: time.Now().UTC(),
		}},
		Memberships: []*TenantMembership{
			{ID: "mbr_ok", AccountID: "acct_live", CustomerID: "cust_live", Role: RoleOwner},
			{ID: "mbr_gone_tenant", AccountID: "acct_live", CustomerID: "cust_offboarded", Role: RoleOwner},
			{ID: "mbr_gone_account", AccountID: "acct_missing", CustomerID: "cust_live", Role: RoleAdmin},
			{ID: "mbr_revoked_tenant", AccountID: "acct_live", CustomerID: "cust_revoked", Role: RoleOwner},
		},
	}
	for _, m := range snap.Memberships {
		// nil audit event: seeding a row is not a grant anyone made.
		if err := spy.upsertMembership(m, nil); err != nil {
			t.Fatalf("seed membership row: %v", err)
		}
	}
	s.applySnapshot(snap)

	// Inspect the index built from this supplied boot snapshot; the public
	// list now reads the persister, whose fixture intentionally lacks parents.
	got := s.membershipsByAccount["acct_live"]
	if len(got) != 1 || got["cust_live"] == nil || got["cust_live"].CustomerID != "cust_live" {
		t.Fatalf("indexed memberships = %+v, want only the live one", got)
	}
	if len(s.membershipsByTenant) != 1 || s.membershipsByTenant["cust_live"] == nil {
		t.Fatalf("by-tenant index = %v, want only cust_live", s.membershipsByTenant)
	}
	for _, id := range []string{"mbr_gone_tenant", "mbr_gone_account", "mbr_revoked_tenant", "mbr_ok"} {
		if _, ok := spy.memberships[id]; !ok {
			t.Errorf("%s was deleted at load — removing a grant is a writer's decision, not a reader's", id)
		}
	}
}

// Returned records are copies: a caller that mutates one must not be able to
// reach into the store and change a role or an identity.
func TestAccountReadsReturnCopies(t *testing.T) {
	s, _ := storeWithSpy("cust_a")
	acct, _ := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{Email: "a@acme.com"})
	if _, err := s.AddTenantMembership(acct.ID, "cust_a", RoleViewer); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}

	acct.Subject = "tampered"
	s.MembershipsForAccount(acct.ID)[0].Role = RoleOwner

	stored, err := s.AccountByIdentity("https://id.yscale.sh", "sub-1")
	if err != nil {
		t.Fatalf("AccountByIdentity: %v", err)
	}
	if stored.Subject != "sub-1" {
		t.Errorf("caller mutated the stored account subject: %q", stored.Subject)
	}
	if role := s.MembershipsForAccount(stored.ID)[0].Role; role != RoleViewer {
		t.Errorf("caller escalated a stored role to %q", role)
	}
}

// TestPostgresAccountRoundTrip proves the durability goal for the SaaS account
// tables against a real Postgres: accounts + memberships reload, and an offboard
// stays offboarded. Gated on YSCALE_TEST_DATABASE_URL like the other Postgres
// integration tests; ids are fixed and derived, so reruns are idempotent.
func TestPostgresAccountRoundTrip(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()

	s1, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	s1.AddCustomer(&Customer{ID: "cust_acct1", Token: "tok_acct1", Plan: "pro"})
	s1.AddCustomer(&Customer{ID: "cust_acct2", Token: "tok_acct2", Plan: "free"})
	acct, err := s1.UpsertAccount("https://id.yscale.sh", "sub-pg",
		AccountProfile{Email: "pg@acme.com", EmailVerified: true, Name: "PG Human"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s1.AddTenantMembership(acct.ID, "cust_acct1", RoleOwner); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}
	if _, err := s1.AddTenantMembership(acct.ID, "cust_acct2", RoleViewer); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}
	s1.Close()

	s2, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, err := s2.AccountByIdentity("https://id.yscale.sh", "sub-pg")
	if err != nil {
		t.Fatalf("account not reloaded: %v", err)
	}
	if got.ID != acct.ID || got.Email != "pg@acme.com" || !got.EmailVerified {
		t.Fatalf("account fields wrong after reload: %+v", got)
	}
	if n := len(s2.MembershipsForAccount(acct.ID)); n != 2 {
		t.Fatalf("memberships after reload = %d, want 2", n)
	}
	s2.DeleteCustomer("cust_acct1")
	s2.Close()

	s3, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload after offboard: %v", err)
	}
	defer s3.Close()
	members := s3.MembershipsForAccount(acct.ID)
	if len(members) != 1 || members[0].CustomerID != "cust_acct2" {
		t.Fatalf("memberships after offboard reload = %+v, want only cust_acct2", members)
	}
	// Clean up so a reused test database doesn't accumulate tenants.
	s3.DeleteCustomer("cust_acct2")
}

// A provisioning caller knows only a `sub`, so resolving an owner must not
// write profile data. Upserting an empty profile from there would blank the
// email/name Yscale ID cached for a human who has already signed in.
func TestResolveAccountPreservesIssuerProfile(t *testing.T) {
	s, spy := storeWithSpy()
	signedIn, err := s.UpsertAccount("https://id.yscale.sh", "sub-1",
		AccountProfile{Email: "human@acme.com", EmailVerified: true, Name: "A Human"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	writes := spy.accountWrites

	resolved, err := s.ResolveAccount("https://id.yscale.sh", "sub-1")
	if err != nil {
		t.Fatalf("ResolveAccount: %v", err)
	}
	if resolved.ID != signedIn.ID {
		t.Fatalf("resolve minted a second account: %s vs %s", resolved.ID, signedIn.ID)
	}
	if resolved.Email != "human@acme.com" || !resolved.EmailVerified || resolved.Name != "A Human" {
		t.Fatalf("resolve wiped the issuer profile: %+v", resolved)
	}
	// The stored row, not just the returned copy.
	stored, err := s.AccountByIdentity("https://id.yscale.sh", "sub-1")
	if err != nil {
		t.Fatalf("AccountByIdentity: %v", err)
	}
	if stored.Email != "human@acme.com" || stored.Name != "A Human" {
		t.Fatalf("stored profile wiped by resolve: %+v", stored)
	}
	if spy.accountWrites != writes {
		t.Errorf("resolving an existing account wrote %d durable row(s); it should write none",
			spy.accountWrites-writes)
	}
}

// Resolving a human who has never signed in still has to produce the account
// the owner membership will point at.
func TestResolveAccountCreatesWithEmptyProfile(t *testing.T) {
	s, spy := storeWithSpy()
	a, err := s.ResolveAccount("https://id.yscale.sh", "sub-new")
	if err != nil {
		t.Fatalf("ResolveAccount: %v", err)
	}
	if a.Email != "" || a.Name != "" || a.EmailVerified {
		t.Errorf("resolve invented profile data: %+v", a)
	}
	if len(spy.accounts) != 1 {
		t.Fatalf("durable account rows = %d, want 1", len(spy.accounts))
	}
	// And the issuer still owns the profile afterwards.
	refreshed, err := s.UpsertAccount("https://id.yscale.sh", "sub-new",
		AccountProfile{Email: "new@acme.com", Name: "New Human"})
	if err != nil {
		t.Fatalf("UpsertAccount after resolve: %v", err)
	}
	if refreshed.ID != a.ID || refreshed.Email != "new@acme.com" {
		t.Errorf("sign-in did not refresh the resolved account: %+v", refreshed)
	}
}

// A signed-in dashboard polls /v1/account. Re-writing an identical row on every
// poll is a durable write per poll for no new information.
func TestUpsertAccountSkipsWriteWhenProfileUnchanged(t *testing.T) {
	s, spy := storeWithSpy()
	p := AccountProfile{Email: "human@acme.com", EmailVerified: true, Name: "A Human"}
	for i := range 3 {
		if _, err := s.UpsertAccount("https://id.yscale.sh", "sub-1", p); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}
	if spy.accountWrites != 1 {
		t.Fatalf("durable account writes over 3 identical polls = %d, want 1", spy.accountWrites)
	}
	// A profile the issuer actually changed still lands.
	p.Name = "Renamed Human"
	if _, err := s.UpsertAccount("https://id.yscale.sh", "sub-1", p); err != nil {
		t.Fatalf("changed-profile upsert: %v", err)
	}
	if spy.accountWrites != 2 {
		t.Fatalf("durable account writes after a real change = %d, want 2", spy.accountWrites)
	}
	got, err := s.AccountByIdentity("https://id.yscale.sh", "sub-1")
	if err != nil {
		t.Fatalf("AccountByIdentity: %v", err)
	}
	if got.Name != "Renamed Human" {
		t.Errorf("changed profile not applied: %+v", got)
	}
}

// An account write that the backend refused must fail the call, not log and
// succeed: the owner membership is hung off this row, and a membership on an
// account that never landed reads fine until a restart drops it as dangling.
func TestAccountPersistFailureFailsClosed(t *testing.T) {
	s, spy := storeWithSpy("cust_a")
	backendDown := errors.New("connection refused")
	spy.accountErr = backendDown

	_, err := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{Email: "a@acme.com"})
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("UpsertAccount error = %v, want ErrPersistence", err)
	}
	if _, err := s.AccountByIdentity("https://id.yscale.sh", "sub-1"); !errors.Is(err, ErrNotFound) {
		t.Fatal("account left in memory after a failed durable write — a membership could be " +
			"hung off a row that never landed")
	}
	if _, err := s.ResolveAccount("https://id.yscale.sh", "sub-2"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("ResolveAccount error = %v, want ErrPersistence (the owner-grant path)", err)
	}

	// A refresh of an EXISTING account rolls back too: the stored profile is
	// whatever last landed durably, never a value only this replica saw.
	spy.accountErr = nil
	if _, err := s.UpsertAccount("https://id.yscale.sh", "sub-1",
		AccountProfile{Email: "a@acme.com", Name: "A Human"}); err != nil {
		t.Fatalf("UpsertAccount with a healthy backend: %v", err)
	}
	spy.accountErr = backendDown
	if _, err := s.UpsertAccount("https://id.yscale.sh", "sub-1",
		AccountProfile{Email: "b@acme.com", Name: "B Human"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("refresh error = %v, want ErrPersistence", err)
	}
	got, err := s.AccountByIdentity("https://id.yscale.sh", "sub-1")
	if err != nil {
		t.Fatalf("existing account lost after a failed refresh: %v", err)
	}
	if got.Email != "a@acme.com" || got.Name != "A Human" {
		t.Errorf("in-memory profile moved despite a failed durable write: %+v", got)
	}
}

// A revoke moves the customer and its grants together or not at all. The
// dangerous outcome is the split one: a failed membership delete followed by a
// successful customer write leaves a durable grant naming a tenant nobody can
// reach, and the next tenant provisioned with that id inherits it.
func TestRevokeCustomerIsAtomicAndFailsClosed(t *testing.T) {
	s, spy := storeWithSpy("cust_reused")
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{Email: "a@acme.com"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s.AddTenantMembership(acct.ID, "cust_reused", RoleOwner); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}

	down := errors.New("connection refused")
	spy.revokeErr = down
	if err := s.RevokeCustomer("cust_reused"); !errors.Is(err, ErrPersistence) || !errors.Is(err, down) {
		t.Fatalf("RevokeCustomer error = %v, want both ErrPersistence and the cause", err)
	}
	if len(spy.customers) != 1 || len(spy.memberships) != 1 {
		t.Fatalf("failed revoke split durable state: %d customer row(s), %d membership row(s) — "+
			"a grant without its tenant is what a reused id resurrects",
			len(spy.customers), len(spy.memberships))
	}
	if _, err := s.AuthCustomer("tok_cust_reused"); err != nil {
		t.Error("the token stopped authenticating though the durable revoke failed — " +
			"memory and Postgres now disagree about whether the tenant is live")
	}
	if got := s.MembershipsForAccount(acct.ID); len(got) != 1 {
		t.Errorf("memberships after a failed revoke = %d, want 1 (nothing was removed)", len(got))
	}

	// The retry takes the grant and leaves the record.
	spy.revokeErr = nil
	if err := s.RevokeCustomer("cust_reused"); err != nil {
		t.Fatalf("retry revoke: %v", err)
	}
	if _, err := s.AuthCustomer("tok_cust_reused"); !errors.Is(err, ErrNotFound) {
		t.Error("a revoked tenant's token still authenticates")
	}
	if got := s.MembershipsForAccount(acct.ID); len(got) != 0 {
		t.Errorf("memberships after revoke = %+v, want none", got)
	}
	if len(spy.memberships) != 0 {
		t.Errorf("durable membership rows after revoke = %d, want 0", len(spy.memberships))
	}
	if len(spy.customers) != 1 {
		t.Errorf("durable customer rows after revoke = %d, want the revoked record kept for retry",
			len(spy.customers))
	}
	c, err := s.CustomerByID("cust_reused")
	if err != nil {
		t.Fatalf("revoked customer must stay reachable by id so a partial offboard can be retried: %v", err)
	}
	if !c.Revoked() {
		t.Error("customer is not stamped revoked")
	}
}

// Revoke is not delete, and the difference has to survive a restart: the record
// comes back so cleanup can be retried, the token does not come back at all.
func TestRevokedCustomerSurvivesRestartWithoutItsToken(t *testing.T) {
	s, spy := storeWithSpy("cust_gone")
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s.AddTenantMembership(acct.ID, "cust_gone", RoleOwner); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}
	if err := s.RevokeCustomer("cust_gone"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}

	restarted := emptyStore()
	restarted.applySnapshot(spy.recordedSnapshot(t))
	if _, err := restarted.AuthCustomer("tok_cust_gone"); !errors.Is(err, ErrNotFound) {
		t.Fatal("a revoked tenant's token authenticates again after a restart — " +
			"the snapshot rebuilt the token index from the row")
	}
	c, err := restarted.CustomerByID("cust_gone")
	if err != nil {
		t.Fatalf("revoked record lost across restart, so the offboard cannot be retried: %v", err)
	}
	if !c.Revoked() {
		t.Error("RevokedAt did not survive the JSON round trip")
	}
	if got := restarted.MembershipsForAccount(acct.ID); len(got) != 0 {
		t.Errorf("memberships after restart = %+v, want none", got)
	}
	// And nothing can bind a new human to a revoked tenant.
	if _, err := restarted.AddTenantMembership(acct.ID, "cust_gone", RoleOwner); !errors.Is(err, ErrNotFound) {
		t.Errorf("membership on a revoked tenant = %v, want ErrNotFound", err)
	}
}

// The two halves of an offboard are separate calls so a partial one can be
// retried. Revoking twice is a no-op, and the final delete is what frees the id.
func TestRevokeThenFinalDeleteIsRetryable(t *testing.T) {
	s, spy := storeWithSpy("cust_x")
	if err := s.RevokeCustomer("cust_x"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	revokedAt := mustCustomer(t, s, "cust_x").RevokedAt
	if err := s.RevokeCustomer("cust_x"); err != nil {
		t.Fatalf("repeat revoke must be a no-op, got %v", err)
	}
	if got := mustCustomer(t, s, "cust_x").RevokedAt; got != revokedAt {
		t.Error("repeat revoke re-stamped RevokedAt; the first revocation is the real one")
	}

	// A final delete that cannot reach the backend leaves the revoked record in
	// place — still unauthenticated, still there to try again.
	down := errors.New("connection refused")
	spy.offboardErr = down
	if err := s.DeleteRevokedCustomer("cust_x"); !errors.Is(err, ErrPersistence) || !errors.Is(err, down) {
		t.Fatalf("DeleteRevokedCustomer error = %v, want both ErrPersistence and the cause", err)
	}
	if _, err := s.CustomerByID("cust_x"); err != nil {
		t.Error("the record vanished from memory though its row was not deleted")
	}
	if _, err := s.AuthCustomer("tok_cust_x"); !errors.Is(err, ErrNotFound) {
		t.Error("a failed final delete brought the token back")
	}

	spy.offboardErr = nil
	if err := s.DeleteRevokedCustomer("cust_x"); err != nil {
		t.Fatalf("retry final delete: %v", err)
	}
	if _, err := s.CustomerByID("cust_x"); !errors.Is(err, ErrNotFound) {
		t.Error("customer still present after a successful final delete")
	}
	if len(spy.customers) != 0 {
		t.Errorf("durable customer rows = %d, want 0", len(spy.customers))
	}
	// Idempotent: a retry after success reports the tenant gone rather than failing.
	if err := s.DeleteRevokedCustomer("cust_x"); err != nil {
		t.Errorf("repeat final delete = %v, want nil", err)
	}
}

// The final delete frees an id, so it must never run on a tenant that was not
// revoked first — that would drop a working tenant's row while its token still
// authenticates.
func TestDeleteRevokedCustomerRefusesAnActiveTenant(t *testing.T) {
	s, spy := storeWithSpy("cust_live")
	if err := s.DeleteRevokedCustomer("cust_live"); !errors.Is(err, ErrCustomerActive) {
		t.Fatalf("DeleteRevokedCustomer on an active tenant = %v, want ErrCustomerActive", err)
	}
	if _, err := s.AuthCustomer("tok_cust_live"); err != nil {
		t.Errorf("the live tenant's token stopped working: %v", err)
	}
	if len(spy.customers) != 1 {
		t.Errorf("durable customer rows = %d, want the live tenant untouched", len(spy.customers))
	}
}

// A tenant provisioned for a partner exists only if its row landed: the token
// is handed out once and nothing re-drives it, so a customer that lives in one
// process's memory is a cluster that stops authenticating at the next restart.
// CreateTenant therefore indexes nothing until the create commits, and
// reports ErrPersistence wrapped so the caller can still read the cause.
func TestCreateTenantFailsClosedOnPersistFailure(t *testing.T) {
	s, spy := storeWithSpy()
	down := errors.New("connection refused")
	spy.createErr = down

	got, _, err := s.CreateTenant(&Customer{ID: "cust_new", Token: "tok_new", Plan: "pro"}, "")
	if !errors.Is(err, ErrPersistence) || !errors.Is(err, down) {
		t.Fatalf("CreateTenant error = %v, want both ErrPersistence and the cause", err)
	}
	if got != nil {
		t.Errorf("a customer was returned for a write that never landed: %+v", got)
	}
	if _, err := s.CustomerByID("cust_new"); !errors.Is(err, ErrNotFound) {
		t.Error("the tenant survived in memory though its row was refused")
	}
	if _, err := s.AuthCustomer("tok_new"); !errors.Is(err, ErrNotFound) {
		t.Error("the token of a tenant that was never persisted still authenticates")
	}
	// And the grant that would have followed cannot be written at all.
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s.AddTenantMembership(acct.ID, "cust_new", RoleOwner); !errors.Is(err, ErrNotFound) {
		t.Errorf("membership on a refused tenant = %v, want ErrNotFound", err)
	}

	// The retry, once the backend is back, is an ordinary provision.
	spy.createErr = nil
	if _, _, err := s.CreateTenant(&Customer{ID: "cust_new", Token: "tok_new", Plan: "pro"}, ""); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, err := s.AuthCustomer("tok_new"); err != nil {
		t.Errorf("retried tenant does not authenticate: %v", err)
	}
	if len(spy.customers) != 1 {
		t.Errorf("durable customer rows = %d, want 1", len(spy.customers))
	}
}

// An id in use is refused whether the tenant holding it is live or revoked, and
// the refusal is a conflict rather than a backend fault. Overwriting instead
// would take a working tenant's token off the air, or hand a partner an id
// whose old resources are still being cleaned up.
func TestCreateTenantRefusesAnIDInUse(t *testing.T) {
	s, spy := storeWithSpy("cust_live")
	_, _, err := s.CreateTenant(&Customer{ID: "cust_live", Token: "tok_rotated", Plan: "enterprise"}, "")
	if !errors.Is(err, ErrCustomerExists) {
		t.Fatalf("re-creating a live id = %v, want ErrCustomerExists", err)
	}
	if errors.Is(err, ErrPersistence) {
		t.Error("a taken id is a conflict, not a backend failure — handlers answer 409 on it, not 503")
	}
	c, err := s.AuthCustomer("tok_cust_live")
	if err != nil {
		t.Fatalf("the live tenant's token stopped authenticating after someone else's create: %v", err)
	}
	if c.Plan != "pro" {
		t.Errorf("plan = %q, want the pre-existing pro", c.Plan)
	}
	if _, err := s.AuthCustomer("tok_rotated"); !errors.Is(err, ErrNotFound) {
		t.Error("the token from the refused create authenticates")
	}

	// Revoked, but not yet finally deleted: the id is still spoken for.
	if err := s.RevokeCustomer("cust_live"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if _, _, err := s.CreateTenant(&Customer{ID: "cust_live", Token: "tok_reuse", Plan: "pro"}, ""); !errors.Is(err, ErrCustomerExists) {
		t.Fatalf("re-creating a revoked id = %v, want ErrCustomerExists", err)
	}
	if _, err := s.AuthCustomer("tok_reuse"); !errors.Is(err, ErrNotFound) {
		t.Error("a create refused against a revoked id still installed its token")
	}
	if len(spy.customers) != 1 {
		t.Errorf("durable customer rows = %d, want 1", len(spy.customers))
	}
}

// Rows written before the revoke transaction existed — an older central deleted
// the customer and left its grants behind — are the one way a membership can
// outlive its tenant. Load leaves them alone (they may yet be recoverable), so
// the durable create is what has to make sure a reissued id starts clean.
//
// Driven through the real persister seam: the store boots from rows on the
// backend and provisions through CreateTenant, rather than a snapshot
// assembled by hand into a state nothing produces.
func TestReusedCustomerIDCannotInheritAStaleGrant(t *testing.T) {
	spy := newAccountSpy()
	acct := &Account{ID: "acct_old", Issuer: "https://id.yscale.sh", Subject: "sub-1", CreatedAt: time.Now().UTC()}
	stale := &TenantMembership{
		ID: membershipID("acct_old", "cust_recycled"), AccountID: "acct_old",
		CustomerID: "cust_recycled", Role: RoleOwner, CreatedAt: time.Now().UTC(),
	}
	if err := spy.upsertAccount(acct); err != nil {
		t.Fatalf("seed account row: %v", err)
	}
	if err := spy.upsertMembership(stale, nil); err != nil {
		t.Fatalf("seed membership row: %v", err)
	}

	s := emptyStore()
	s.persist = spy
	s.applySnapshot(spy.recordedSnapshot(t))
	if len(spy.memberships) != 1 {
		t.Fatal("load deleted the dangling row; a grant whose tenant may still come back is not load's to throw away")
	}
	if got := s.MembershipsForAccount("acct_old"); len(got) != 0 {
		t.Fatalf("dangling grant was indexed at load: %+v", got)
	}

	if _, _, err := s.CreateTenant(&Customer{ID: "cust_recycled", Token: "tok_fresh", Plan: "pro"}, ""); err != nil {
		t.Fatalf("provisioning a free id: %v", err)
	}
	if len(spy.memberships) != 0 {
		t.Error("the create left a stale grant on the id it just reissued")
	}
	if got := s.MembershipsForAccount("acct_old"); len(got) != 0 {
		t.Errorf("old human holds a grant on the new tenant: %+v", got)
	}

	// And it stays gone across the restart that would otherwise index it, now
	// that both ends of the row resolve again.
	restarted := emptyStore()
	restarted.applySnapshot(spy.recordedSnapshot(t))
	if got := restarted.MembershipsForAccount("acct_old"); len(got) != 0 {
		t.Errorf("reused id resurrected %d grant(s) after restart: %+v", len(got), got)
	}
	if _, err := restarted.AuthCustomer("tok_fresh"); err != nil {
		t.Errorf("the reissued tenant does not authenticate after restart: %v", err)
	}
}

func mustCustomer(t *testing.T, s *Store, id string) *Customer {
	t.Helper()
	c, err := s.CustomerByID(id)
	if err != nil {
		t.Fatalf("CustomerByID(%s): %v", id, err)
	}
	return c
}

// The customer lifecycle against a REAL Postgres. The spy above mirrors the two
// new transactions, but it cannot check the things that make them work: that
// `INSERT ... ON CONFLICT DO NOTHING` reports zero rows for a taken id inside a
// transaction, that rolling back takes the membership delete with it, that the
// JSONB `data->>'CustomerID'` predicate finds rows this process never held, and
// that RevokedAt survives the JSONB round trip into the token index decision.
func TestPostgresCustomerLifecycle(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	const (
		id = "cust_lifecycle"
		// The id the offboarded tenant's leftovers get attached to. It cannot be
		// `id`: that one is retired for good the moment the final delete lands.
		reissued = "cust_lifecycle_next"
	)

	s1, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	cleanupTenantRows(t, ctx, dsn, id)
	cleanupTenantRows(t, ctx, dsn, reissued)

	if _, _, err := s1.CreateTenant(&Customer{ID: id, Token: "tok_life", Plan: "pro"}, ""); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	acct, err := s1.UpsertAccount("https://id.yscale.sh", "sub-life", AccountProfile{Email: "life@acme.com"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s1.AddTenantMembership(acct.ID, id, RoleOwner); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}
	// A taken id is refused by the INSERT, as a conflict rather than a fault.
	if _, _, err := s1.CreateTenant(&Customer{ID: id, Token: "tok_dup", Plan: "pro"}, ""); !errors.Is(err, ErrCustomerExists) {
		t.Fatalf("duplicate create = %v, want ErrCustomerExists", err)
	}
	if _, err := s1.AuthCustomer("tok_dup"); !errors.Is(err, ErrNotFound) {
		t.Error("the refused create installed its token anyway")
	}
	if err := s1.RevokeCustomer(id); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	s1.Close()

	// A restart reads the revoked row back: the record returns, the token does
	// not, and the grant the revoke deleted is gone from the table.
	s2, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload after revoke: %v", err)
	}
	if _, err := s2.AuthCustomer("tok_life"); !errors.Is(err, ErrNotFound) {
		t.Fatal("a revoked tenant's token authenticates again after a restart")
	}
	c, err := s2.CustomerByID(id)
	if err != nil {
		t.Fatalf("revoked record lost across restart: %v", err)
	}
	if !c.Revoked() {
		t.Error("RevokedAt did not survive the JSONB round trip")
	}
	if got := s2.MembershipsForAccount(acct.ID); len(got) != 0 {
		t.Errorf("grants after revoke = %+v, want none", got)
	}
	// Still spoken for until the final delete frees it.
	if _, _, err := s2.CreateTenant(&Customer{ID: id, Token: "tok_early", Plan: "pro"}, ""); !errors.Is(err, ErrCustomerExists) {
		t.Fatalf("create on a revoked id = %v, want ErrCustomerExists", err)
	}
	if err := s2.DeleteRevokedCustomer(id); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}
	// The row is gone — and the id is retired with it, not freed. Nothing in
	// the customers table can refuse this create; the tombstone is what does.
	if _, _, err := s2.CreateTenant(&Customer{ID: id, Token: "tok_reissued", Plan: "pro"}, ""); !errors.Is(err, ErrCustomerExists) {
		t.Fatalf("create on a retired id = %v, want ErrCustomerExists", err)
	}

	// A create still clears whatever is left over for the id it takes — seeded
	// here as a row written directly, the way an older central left one. This
	// runs on a fresh id because a retired one never comes back.
	stale := &TenantMembership{
		ID: membershipID(acct.ID, reissued), AccountID: acct.ID, CustomerID: reissued,
		Role: RoleOwner, CreatedAt: time.Now().UTC(),
	}
	// This deliberately corrupt fixture emulates an older writer. The current
	// Store and persister refuse grants to missing tenants.
	staleData, err := json.Marshal(stale)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.persist.(*pgPersister).pool.Exec(ctx, `INSERT INTO tenant_memberships(id,data) VALUES($1,$2)`, stale.ID, staleData); err != nil {
		t.Fatalf("seed stale membership row: %v", err)
	}
	if _, _, err := s2.CreateTenant(&Customer{ID: reissued, Token: "tok_reissued", Plan: "pro"}, ""); err != nil {
		t.Fatalf("creating the next tenant: %v", err)
	}
	s2.Close()

	s3, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload after the next create: %v", err)
	}
	defer s3.Close()
	if _, err := s3.AuthCustomer("tok_reissued"); err != nil {
		t.Errorf("the new tenant does not authenticate after restart: %v", err)
	}
	if got := s3.MembershipsForAccount(acct.ID); len(got) != 0 {
		t.Errorf("a fresh id resurrected %d grant(s): %+v", len(got), got)
	}
	// The retired id stays retired across a restart: the tombstone is a row,
	// not process state.
	if _, err := s3.CustomerByID(id); !errors.Is(err, ErrNotFound) {
		t.Error("the offboarded tenant's record came back after a restart")
	}
	if _, _, err := s3.CreateTenant(&Customer{ID: id, Token: "tok_late", Plan: "pro"}, ""); !errors.Is(err, ErrCustomerExists) {
		t.Errorf("a retired id was reissued after a restart: %v", err)
	}
}

// customerRow decodes what the backend actually holds for one customer — the
// row a restart would read, not the in-memory copy.
func (p *accountSpyPersister) customerRow(t *testing.T, id string) *Customer {
	t.Helper()
	raw, ok := p.customers[id]
	if !ok {
		t.Fatalf("no durable row for %s", id)
	}
	var c Customer
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("decode customer row: %v", err)
	}
	return &c
}

// A tenant and its first owner are one durable operation. Two writes would have
// a state between them — a tenant with a token already minted and no owner —
// and undoing that state is what fails next when the database is what is wrong.
func TestCreateTenantWritesTenantAndOwnerTogether(t *testing.T) {
	s, spy := storeWithSpy()
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-owner", AccountProfile{Email: "owner@acme.com"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	cust, owner, err := s.CreateTenant(&Customer{ID: "cust_new", Token: "tok_new", Plan: "pro"}, acct.ID)
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if cust == nil || owner == nil || owner.AccountID != acct.ID || owner.Role != RoleOwner {
		t.Fatalf("CreateTenant returned customer %+v, owner %+v", cust, owner)
	}
	if _, err := s.AuthCustomer("tok_new"); err != nil {
		t.Fatalf("the provisioned token does not authenticate: %v", err)
	}
	if got := s.MembershipsForAccount(acct.ID); len(got) != 1 || got[0].CustomerID != "cust_new" {
		t.Fatalf("owner grants = %+v, want one on cust_new", got)
	}
	if len(spy.memberships) != 1 || len(spy.customers) != 1 {
		t.Fatalf("durable rows = %d customer(s), %d grant(s), want 1 and 1",
			len(spy.customers), len(spy.memberships))
	}

	// And both come back on the restart that is the whole reason they are
	// durable: the owner's profile is untouched, because provisioning knows only
	// a `sub` and has no business writing profile data.
	restarted := emptyStore()
	restarted.applySnapshot(spy.recordedSnapshot(t))
	if got := restarted.MembershipsForAccount(acct.ID); len(got) != 1 {
		t.Fatalf("owner grants after restart = %+v, want the one it was given", got)
	}
	reloaded, err := restarted.AccountByIdentity("https://id.yscale.sh", "sub-owner")
	if err != nil {
		t.Fatalf("owner account after restart: %v", err)
	}
	if reloaded.Email != "owner@acme.com" {
		t.Errorf("owner email = %q, want the cached profile preserved", reloaded.Email)
	}
}

// A refused transaction leaves NEITHER half. There is no rollback path to get
// wrong: the tenant, the token and the grant were never anywhere.
func TestCreateTenantWithOwnerFailsClosed(t *testing.T) {
	s, spy := storeWithSpy()
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-owner", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	down := errors.New("connection refused")
	spy.createErr = down

	cust, owner, err := s.CreateTenant(&Customer{ID: "cust_new", Token: "tok_new", Plan: "pro"}, acct.ID)
	if !errors.Is(err, ErrPersistence) || !errors.Is(err, down) {
		t.Fatalf("CreateTenant error = %v, want both ErrPersistence and the cause", err)
	}
	if cust != nil || owner != nil {
		t.Errorf("records returned for a transaction that never committed: %+v / %+v", cust, owner)
	}
	if _, err := s.CustomerByID("cust_new"); !errors.Is(err, ErrNotFound) {
		t.Error("the tenant survived in memory though its transaction was refused")
	}
	if _, err := s.AuthCustomer("tok_new"); !errors.Is(err, ErrNotFound) {
		t.Error("the token of a tenant that was never persisted authenticates")
	}
	if got := s.MembershipsForAccount(acct.ID); len(got) != 0 {
		t.Errorf("grants after a refused create = %+v, want none", got)
	}
	if len(spy.customers) != 0 || len(spy.memberships) != 0 {
		t.Errorf("durable rows after a refused create = %d customer(s), %d grant(s), want none",
			len(spy.customers), len(spy.memberships))
	}

	// Retried against a working backend it is an ordinary provision.
	spy.createErr = nil
	if _, _, err := s.CreateTenant(&Customer{ID: "cust_new", Token: "tok_new", Plan: "pro"}, acct.ID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := s.MembershipsForAccount(acct.ID); len(got) != 1 {
		t.Fatalf("owner grants after the retry = %+v, want one", got)
	}
}

// The owner's account is checked inside the transaction, not against this
// replica's memory. A grant written on top of an account row that never landed
// reads fine until a restart drops it as dangling — and the tenant it was
// created for is already live by then.
func TestCreateTenantRefusesAnOwnerWithNoAccountRow(t *testing.T) {
	s, spy := storeWithSpy()
	// An account this store has in memory and the backend has never seen: the
	// shape a dropped account write leaves behind.
	ghost := &Account{ID: "acct_ghost", Issuer: "https://id.yscale.sh", Subject: "ghost", CreatedAt: time.Now().UTC()}
	s.accounts[ghost.ID] = ghost
	s.accountsByIdentity[identityKey(ghost.Issuer, ghost.Subject)] = ghost

	if _, _, err := s.CreateTenant(&Customer{ID: "cust_ghost", Token: "tok_ghost"}, ghost.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("create for an owner with no account row = %v, want ErrNotFound", err)
	}
	if _, err := s.CustomerByID("cust_ghost"); err == nil {
		t.Fatal("the tenant was created for an owner the backend does not have")
	}
	if len(spy.customers) != 0 || len(spy.memberships) != 0 {
		t.Errorf("durable rows = %d customer(s), %d grant(s), want none", len(spy.customers), len(spy.memberships))
	}
	if _, err := s.AuthCustomer("tok_ghost"); !errors.Is(err, ErrNotFound) {
		t.Error("the refused create installed its token anyway")
	}

	// An owner nobody has ever heard of is refused before the backend is asked.
	if _, _, err := s.CreateTenant(&Customer{ID: "cust_nobody", Token: "tok_nobody"}, "acct_nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("create for an unknown owner = %v, want ErrNotFound", err)
	}
}

// A revoke and a mesh attach are both writers of the same customer row, and
// they used to be able to overlap: the mesh write reads the record, the revoke
// commits, the mesh write lands its pre-revoke snapshot on top, and the stamp
// that keeps an offboarded token from authenticating is quietly gone from the
// durable copy. The customer lock is what orders them; this parks a revoke
// mid-transaction and drives a mesh attach straight at it.
func TestRevokeAndCustomerWriteCannotClobberEachOther(t *testing.T) {
	s, spy := storeWithSpy("cust_x")
	entered := make(chan struct{})
	release := make(chan struct{})
	spy.beforeRevoke = func() {
		close(entered)
		<-release
	}

	revoked := make(chan error, 1)
	go func() { revoked <- s.RevokeCustomer("cust_x") }()
	<-entered

	attached := make(chan error, 1)
	go func() {
		attached <- s.SetCustomerMesh("cust_x", &MeshEndpoint{
			Provider: "box", LoginServer: "https://hs.acme", User: "acme", BackendID: "linode-1",
		})
	}()
	select {
	case err := <-attached:
		t.Fatalf("a customer write ran while a revoke transaction was still open: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-revoked; err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := <-attached; err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}

	row := spy.customerRow(t, "cust_x")
	if row.RevokedAt == nil {
		t.Error("the mesh write clobbered the revoke stamp; this tenant's token comes back at the next restart")
	}
	if row.Mesh == nil || row.Mesh.BackendID != "linode-1" {
		t.Errorf("durable mesh = %+v, want the attach that ran after the revoke", row.Mesh)
	}
}

// The other direction, and the one no in-process lock can order: a write that
// landed on ANOTHER replica after this one read the record. The revoke patches
// its one field rather than writing its whole snapshot back, so what it does
// not know about it also cannot erase.
func TestRevokeDoesNotOverwriteFieldsItNeverSaw(t *testing.T) {
	s, spy := storeWithSpy("cust_y")
	// A mesh attach performed by another central: the durable row has it, this
	// store's in-memory copy does not.
	if err := spy.upsertCustomer(&Customer{
		ID: "cust_y", Token: "tok_cust_y", Plan: "pro",
		Mesh: &MeshEndpoint{Provider: "box", User: "acme", BackendID: "linode-9"},
	}); err != nil {
		t.Fatalf("seed the other replica's write: %v", err)
	}

	if err := s.RevokeCustomer("cust_y"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	row := spy.customerRow(t, "cust_y")
	if row.RevokedAt == nil {
		t.Fatal("the revoke did not stamp the durable row")
	}
	if row.Mesh == nil || row.Mesh.BackendID != "linode-9" {
		t.Errorf("durable mesh = %+v, want the other replica's attach left alone — "+
			"a whole-row revoke would have dropped the box this tenant is still on", row.Mesh)
	}
}

// A SHAPE check on the customer upsert, and only that: it reads the statement
// string this package builds and proves it carries both guards. It executes
// nothing and proves nothing about Postgres.
// TestPostgresWholeCustomerWriteCannotUndoARevoke and
// TestPostgresTombstoneRefusesEveryCustomerWriter are what execute the SQL, and
// both skip unless YSCALE_TEST_DATABASE_URL names a throwaway database; the
// unconditional behavioural cover is the spy-backed tests above and below.
func TestUpsertCustomerStatementShapeCarriesBothGuards(t *testing.T) {
	stmt := upsertCustomerStmt(tblCustomers, tblTombstones)
	// The tombstone guard: an id whose row a finished offboard deleted has
	// nothing for ON CONFLICT to catch, so the insert itself must be suppressed.
	if !strings.Contains(stmt, `WHERE NOT EXISTS (SELECT 1 FROM `+tblTombstones+` WHERE id = $1)`) {
		t.Errorf("statement would INSERT a fresh row for a permanently retired id:\n%s", stmt)
	}
	// The row guard: the stored row is the source of the preserved stamp, not
	// the incoming document — an incoming stamp is exactly what a stale writer
	// lacks. COALESCE folds an absent and a null RevokedAt into one case.
	if !strings.Contains(stmt, `jsonb_set($2::jsonb, '{RevokedAt}', `+tblCustomers+`.data->'RevokedAt')`) ||
		!strings.Contains(stmt, `COALESCE(`+tblCustomers+`.data->'RevokedAt', 'null'::jsonb) <> 'null'::jsonb`) {
		t.Errorf("statement does not carry the stored RevokedAt onto the incoming document:\n%s", stmt)
	}
	// And it is still an upsert: a customer that is not revoked takes the
	// incoming document whole, and a row that isn't there yet is inserted.
	if !strings.Contains(stmt, "ON CONFLICT (id) DO UPDATE") || !strings.Contains(stmt, "ELSE $2::jsonb") {
		t.Errorf("statement is not the guarded upsert callers expect:\n%s", stmt)
	}
}

// The same kind of SHAPE check across all three customer inserts — it parses
// nothing and executes nothing — for the one property that has no behavioural
// cover without a database: the jsonb cast on $2.
//
// The tombstone guard is what makes the cast load-bearing. Guarding the insert
// means writing INSERT ... SELECT instead of INSERT ... VALUES, and a parameter
// in a select list takes no type from the column it lands in — it resolves to
// text, which Postgres refuses to assign to a jsonb column. That is the whole
// statement failing to parse, on every customer write, so it is worth an
// unconditional check that costs nothing. The guard itself is checked here too,
// since these two statements have no shape test of their own.
func TestCustomerInsertStatementShapesCastTheJSONBParameter(t *testing.T) {
	statements := map[string]string{
		"upsertCustomerStmt":        upsertCustomerStmt(tblCustomers, tblTombstones),
		"createCustomerStmt":        createCustomerStmt(tblCustomers, tblTombstones),
		"insertRevokedCustomerStmt": insertRevokedCustomerStmt(tblCustomers, tblTombstones),
	}
	for name, stmt := range statements {
		if !strings.Contains(stmt, "SELECT $1, $2::jsonb, now()") {
			t.Errorf("%s: the inserted document is not cast to jsonb:\n%s", name, stmt)
		}
		// EVERY occurrence, not just the insert's: a `DO UPDATE SET data = $2`
		// left uncast in the same statement makes Postgres deduce two different
		// types for one parameter, which fails the same way.
		for i := strings.Index(stmt, "$2"); i >= 0; {
			if !strings.HasPrefix(stmt[i:], "$2::jsonb") {
				t.Errorf("%s: $2 is used without a jsonb cast at offset %d:\n%s", name, i, stmt)
			}
			next := strings.Index(stmt[i+2:], "$2")
			if next < 0 {
				break
			}
			i += 2 + next
		}
		if !strings.Contains(stmt, `WHERE NOT EXISTS (SELECT 1 FROM `+tblTombstones+` WHERE id = $1)`) {
			t.Errorf("%s: statement would write a row for a permanently retired id:\n%s", name, stmt)
		}
	}
}

// The same guarantee in memory, on the path that actually re-adds an id: the
// env-seeded dev/OSS config re-drives AddCustomer on every boot, with a record
// that knows nothing about a revoke that happened since.
func TestAddCustomerCannotReviveARevokedTenant(t *testing.T) {
	s, spy := storeWithSpy("cust_z")
	if err := s.RevokeCustomer("cust_z"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	s.AddCustomer(&Customer{ID: "cust_z", Token: "tok_cust_z", Plan: "pro"})

	if _, err := s.AuthCustomer("tok_cust_z"); !errors.Is(err, ErrNotFound) {
		t.Errorf("re-adding a revoked id put its token back in the auth index: %v", err)
	}
	if c := mustCustomer(t, s, "cust_z"); !c.Revoked() {
		t.Error("the re-added record is not revoked; this tenant is active again")
	}
	// The snapshot written durably carries the stamp too, so a restart reading
	// it back does not index the token either.
	if row := spy.customerRow(t, "cust_z"); row.RevokedAt == nil {
		t.Error("the durable write dropped the revoke stamp")
	}
	restarted := emptyStore()
	restarted.applySnapshot(spy.recordedSnapshot(t))
	if _, err := restarted.AuthCustomer("tok_cust_z"); !errors.Is(err, ErrNotFound) {
		t.Errorf("the revoked token authenticates after a restart: %v", err)
	}
}

// The state the row guard cannot cover, because there is no row: one replica
// finishes an offboard, and another — still holding the customer in memory —
// writes the record back. Every earlier guard reads the customers row, and the
// finalize deleted it, so an unguarded write is a plain INSERT of a live tenant
// with a working token. The tombstone is what refuses it.
func TestFinalizedTenantCannotBeResurrectedByAStaleReplica(t *testing.T) {
	s, spy := storeWithSpy("cust_gone")
	if err := s.RevokeCustomer("cust_gone"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := s.DeleteRevokedCustomer("cust_gone"); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}

	// The other replica's write, straight at the durable layer: its in-memory
	// copy predates the offboard, so the document it carries is the live one.
	stale := &Customer{ID: "cust_gone", Token: "tok_cust_gone", Plan: "pro"}
	err := spy.upsertCustomer(stale)
	if !errors.Is(err, ErrCustomerTombstoned) {
		t.Fatalf("upsertCustomer on a retired id = %v, want ErrCustomerTombstoned", err)
	}
	if _, ok := spy.customers["cust_gone"]; ok {
		t.Fatal("a stale replica's write recreated a customer row for a permanently retired id")
	}

	// And a restart reads nothing back: no row, so no token in the auth index.
	restarted := emptyStore()
	restarted.applySnapshot(spy.recordedSnapshot(t))
	if _, err := restarted.AuthCustomer("tok_cust_gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a retired tenant's token authenticates after a restart: %v", err)
	}
	if _, err := restarted.CustomerByID("cust_gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a retired tenant's record came back after a restart: %v", err)
	}
}

// The same tombstone from the other direction: an id a finished offboard
// retired is never handed to a new tenant. The primary key cannot say so — the
// row it would have refused is exactly what the offboard deleted — so without
// the tombstone the create finds a free id and takes it, and the new tenant
// inherits whatever still names the old one.
func TestFinalizedTenantIDIsNeverReissued(t *testing.T) {
	s, spy := storeWithSpy("cust_reuse")
	if err := s.RevokeCustomer("cust_reuse"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := s.DeleteRevokedCustomer("cust_reuse"); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}
	// Gone from memory: nothing local is left to refuse the id.
	if _, err := s.CustomerByID("cust_reuse"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the offboarded record is still in memory: %v", err)
	}

	_, _, err := s.CreateTenant(&Customer{ID: "cust_reuse", Token: "tok_new", Plan: "pro"}, "")
	if !errors.Is(err, ErrCustomerExists) {
		t.Fatalf("CreateTenant on a retired id = %v, want ErrCustomerExists", err)
	}
	// A refused create leaves nothing behind, in memory or durably.
	if _, err := s.CustomerByID("cust_reuse"); !errors.Is(err, ErrNotFound) {
		t.Error("the refused create indexed a tenant anyway")
	}
	if _, ok := spy.customers["cust_reuse"]; ok {
		t.Error("the refused create wrote a customer row")
	}
	if _, err := s.AuthCustomer("tok_new"); !errors.Is(err, ErrNotFound) {
		t.Error("the refused create's token authenticates")
	}
	// Retiring an id is permanent — a second offboard of the same id, and any
	// later create, get the same answer.
	if _, _, err := s.CreateTenant(&Customer{ID: "cust_reuse", Token: "tok_new2"}, ""); !errors.Is(err, ErrCustomerExists) {
		t.Errorf("a retired id came back later: %v", err)
	}
}

// The same refusal on the path with no error to return: legacy AddCustomer. The
// env-seeded dev/OSS config re-drives it on every boot from config that still
// names the id and knows nothing about the offboard. The durable layer refuses
// the write (ErrCustomerTombstoned), so without an in-memory refusal the result
// is the worst shape available — a live customer with a working token in this
// process's memory and nowhere else.
func TestAddCustomerCannotReviveAFinalizedTenant(t *testing.T) {
	s, spy := storeWithSpy("cust_seed")
	if err := s.RevokeCustomer("cust_seed"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := s.DeleteRevokedCustomer("cust_seed"); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}

	c := s.AddCustomer(&Customer{ID: "cust_seed", Token: "tok_seed_again", Plan: "pro"})
	if _, err := s.AuthCustomer("tok_seed_again"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a re-seeded retired id authenticates: %v", err)
	}
	if _, err := s.CustomerByID("cust_seed"); !errors.Is(err, ErrNotFound) {
		t.Error("a retired id is registered again")
	}
	if _, ok := spy.customers["cust_seed"]; ok {
		t.Error("the refused re-seed wrote a customer row")
	}
	// The returned pointer is the caller's own record, NOT a registered
	// customer. The enterprise seed path hands it straight to the coordination
	// attach, which resolves by id — and finds nothing to attach to.
	if c == nil {
		t.Fatal("AddCustomer returned nil")
	}
	if err := s.SetCustomerMesh(c.ID, &MeshEndpoint{Provider: "test", User: "u"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetCustomerMesh on a retired id = %v, want ErrNotFound", err)
	}
	if _, _, err := s.CreateTenant(&Customer{ID: "cust_seed", Token: "tok_seed3"}, ""); !errors.Is(err, ErrCustomerExists) {
		t.Errorf("CreateTenant on a retired id = %v, want ErrCustomerExists", err)
	}
}

// And the refusal outlives the process. Nothing in the customer rows can carry
// it — the row is what the offboard deleted — so the boot read carries the
// retired ids themselves, and the store indexes them before anything else.
func TestARetiredIDStaysRetiredAcrossARestart(t *testing.T) {
	s, spy := storeWithSpy("cust_boot")
	if err := s.RevokeCustomer("cust_boot"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := s.DeleteRevokedCustomer("cust_boot"); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}

	snap := spy.recordedSnapshot(t)
	if len(snap.Tombstones) != 1 || snap.Tombstones[0] != "cust_boot" {
		t.Fatalf("boot read carried tombstones %v, want [cust_boot]", snap.Tombstones)
	}
	restarted := emptyStore()
	restarted.persist = spy
	restarted.applySnapshot(snap)

	restarted.AddCustomer(&Customer{ID: "cust_boot", Token: "tok_boot_again", Plan: "pro"})
	if _, err := restarted.AuthCustomer("tok_boot_again"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a re-seeded retired id authenticates after a restart: %v", err)
	}
	if _, err := restarted.CustomerByID("cust_boot"); !errors.Is(err, ErrNotFound) {
		t.Error("a retired id is registered again after a restart")
	}
	if _, _, err := restarted.CreateTenant(&Customer{ID: "cust_boot", Token: "tok_boot2"}, ""); !errors.Is(err, ErrCustomerExists) {
		t.Errorf("CreateTenant on a retired id after a restart = %v, want ErrCustomerExists", err)
	}
}

// A snapshot carrying BOTH a tombstone and a customers row for one id is not a
// state any writer can produce — the finalize deletes the row in the tombstone's
// transaction, and every writer after it is refused — so it means a row was
// restored underneath the retirement. Indexing it would put a retired tenant's
// token back in the auth index, which is the one outcome the tombstone exists to
// prevent.
func TestApplySnapshotIgnoresACustomerRowForARetiredID(t *testing.T) {
	s := emptyStore()
	s.applySnapshot(&snapshot{
		Customers:  []*Customer{{ID: "cust_zombie", Token: "tok_zombie", Plan: "pro"}},
		Tombstones: []string{"cust_zombie"},
	})
	if _, err := s.AuthCustomer("tok_zombie"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a restored row put a retired tenant's token back in the auth index: %v", err)
	}
	if _, err := s.CustomerByID("cust_zombie"); !errors.Is(err, ErrNotFound) {
		t.Error("a restored row registered a retired id")
	}
}

// The store with NO persister — the OSS/dev and unit-test shape — retires ids
// the same way. There is no table to read a tombstone back from here, so if the
// finalize did not record the id in memory nothing would, and the id would be
// handed straight back out.
func TestInMemoryStoreRetiresIDsWithoutAPersister(t *testing.T) {
	s := New()
	if s.persist != nil {
		t.Fatal("New() must have no persister")
	}
	s.AddCustomer(&Customer{ID: "cust_mem", Token: "tok_mem", Plan: "pro"})
	if err := s.RevokeCustomer("cust_mem"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := s.DeleteRevokedCustomer("cust_mem"); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}

	s.AddCustomer(&Customer{ID: "cust_mem", Token: "tok_mem2", Plan: "pro"})
	for _, tok := range []string{"tok_mem", "tok_mem2"} {
		if _, err := s.AuthCustomer(tok); !errors.Is(err, ErrNotFound) {
			t.Errorf("token %q authenticates on a retired id: %v", tok, err)
		}
	}
	if _, _, err := s.CreateTenant(&Customer{ID: "cust_mem", Token: "tok_mem3"}, ""); !errors.Is(err, ErrCustomerExists) {
		t.Errorf("CreateTenant on a retired id = %v, want ErrCustomerExists", err)
	}
	// Retirement is per id: the seeded dev customer is untouched.
	if _, err := s.AuthCustomer(DefaultDevToken); err != nil {
		t.Errorf("the dev seed customer stopped authenticating: %v", err)
	}
}

// First boot seeds the dev customer straight into the indexes, before the store
// is shared, so that path carries the tombstone check itself: an operator who
// offboarded the dev tenant retired its id, and re-seeding it is the one
// remaining way to hand that id a working token.
func TestDevSeedSkipsARetiredID(t *testing.T) {
	s := emptyStore()
	s.applySnapshot(&snapshot{Tombstones: []string{DevCustomerID}})
	s.seedTestCustomer(DefaultDevToken)

	if _, err := s.AuthCustomer(DefaultDevToken); !errors.Is(err, ErrNotFound) {
		t.Errorf("the first-boot seed reactivated a retired dev id: %v", err)
	}
	if _, err := s.CustomerByID(DevCustomerID); !errors.Is(err, ErrNotFound) {
		t.Error("the first-boot seed registered a retired dev id")
	}
}

// A revoke aimed at a tenant ANOTHER replica already finalized has no row to
// patch, and the fallback insert would give the retired id a row again —
// revoked, but present, and holding an id the tombstone says is gone. It is
// guarded, and the revoke still reports success: access is gone, which is all
// the caller asked for.
func TestRevokingAnAlreadyFinalizedTenantDoesNotRecreateItsRow(t *testing.T) {
	s, spy := storeWithSpy("cust_raced")
	if err := s.RevokeCustomer("cust_raced"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := s.DeleteRevokedCustomer("cust_raced"); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}

	stamp := time.Now().UTC()
	revoked := &Customer{ID: "cust_raced", Token: "tok_cust_raced", Plan: "pro", RevokedAt: &stamp}
	if err := spy.revokeCustomer(revoked); err != nil {
		t.Fatalf("revokeCustomer on a retired id = %v, want the no-op success", err)
	}
	if _, ok := spy.customers["cust_raced"]; ok {
		t.Error("a late revoke gave a permanently retired id a customer row again")
	}
}

// Legacy DeleteCustomer is the OSS/dev remove-this-record verb, not an
// offboard, and its callers re-drive themselves from config. It leaves no
// tombstone, so the id it removed is still usable.
func TestLegacyDeleteCustomerLeavesTheIDReusable(t *testing.T) {
	s, spy := storeWithSpy("cust_dev")
	s.DeleteCustomer("cust_dev")
	if _, retired := spy.tombstones["cust_dev"]; retired {
		t.Fatal("the legacy delete path retired an id; dev re-seeding cannot use it again")
	}
	if _, _, err := s.CreateTenant(&Customer{ID: "cust_dev", Token: "tok_dev2", Plan: "pro"}, ""); err != nil {
		t.Fatalf("CreateTenant after a legacy delete: %v", err)
	}
}

// Re-adding an id with a new token is a rotation, and the token index is keyed
// by TOKEN: the old entry has to go, or the credential the operator believes
// they replaced keeps authenticating as this customer, with nothing left to
// remove it by.
func TestReindexingACustomerDropsItsOldToken(t *testing.T) {
	s, _ := storeWithSpy("cust_rot")
	// Rotated twice, because "the previous token" is not the contract — EVERY
	// token this id ever had has to be out of the index, and each rotation is
	// the only moment the one it replaces can be removed.
	s.AddCustomer(&Customer{ID: "cust_rot", Token: "tok_rotated", Plan: "pro"})
	s.AddCustomer(&Customer{ID: "cust_rot", Token: "tok_rotated_again", Plan: "pro"})

	retired := []string{"tok_cust_rot", "tok_rotated"}
	for _, tok := range retired {
		if _, err := s.AuthCustomer(tok); !errors.Is(err, ErrNotFound) {
			t.Errorf("replaced token %q still authenticates: %v", tok, err)
		}
	}
	c, err := s.AuthCustomer("tok_rotated_again")
	if err != nil {
		t.Fatalf("the new token does not authenticate: %v", err)
	}
	if c.ID != "cust_rot" {
		t.Errorf("token resolved to %q, want cust_rot", c.ID)
	}

	// And a revoke after a rotation leaves NO token behind: the revoke unindexes
	// the record's current token, and each rotation already took the one before.
	if err := s.RevokeCustomer("cust_rot"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	all := append(retired, "tok_rotated_again")
	for _, tok := range all {
		if _, err := s.AuthCustomer(tok); !errors.Is(err, ErrNotFound) {
			t.Errorf("token %q authenticates after the revoke: %v", tok, err)
		}
	}
	if err := s.DeleteRevokedCustomer("cust_rot"); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}
	for _, tok := range all {
		if _, err := s.AuthCustomer(tok); !errors.Is(err, ErrNotFound) {
			t.Errorf("token %q authenticates after the final delete: %v", tok, err)
		}
	}
}

// cleanupTenantRows drops one tenant's rows — INCLUDING its tombstone — when
// the test ends. These tests use fixed ids, and a tombstone is permanent by
// design, so without clearing it the second run of a suite would be refused at
// CreateTenant. Test fixtures are the one caller allowed to do this; there is no
// product path that removes a tombstone.
func cleanupTenantRows(t *testing.T, ctx context.Context, dsn, id string) {
	t.Helper()
	t.Cleanup(func() {
		clean, err := NewPostgres(ctx, dsn)
		if err != nil {
			return
		}
		defer clean.Close()
		clean.DeleteCustomer(id)
		p, ok := clean.persist.(*pgPersister)
		if !ok {
			return
		}
		if _, err := p.pool.Exec(ctx,
			fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, tblTombstones), id); err != nil {
			t.Errorf("clear tombstone %s: %v", id, err)
		}
	})
}

// The tombstone against a REAL Postgres, on every writer that could bring a
// finalized tenant back: the offboard deletes the customers row, so from then
// on nothing in the customers table can refuse anything, and the tombstone is
// the only record that the id was ever used. Gated on YSCALE_TEST_DATABASE_URL
// like the other Postgres integration tests.
func TestPostgresTombstoneRefusesEveryCustomerWriter(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	const id = "cust_tombstone"

	s1, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	defer s1.Close()
	cleanupTenantRows(t, ctx, dsn, id)

	if _, _, err := s1.CreateTenant(&Customer{ID: id, Token: "tok_tomb", Plan: "pro"}, ""); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if err := s1.RevokeCustomer(id); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := s1.DeleteRevokedCustomer(id); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}

	p, ok := s1.persist.(*pgPersister)
	if !ok {
		t.Fatalf("persister = %T, want *pgPersister", s1.persist)
	}
	// The row really is gone: without a tombstone there would be nothing left
	// for any of the writers below to conflict with.
	var rows int
	if err := p.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = $1`, tblCustomers), id).Scan(&rows); err != nil {
		t.Fatalf("count customer rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("customer rows after the final delete = %d, want 0", rows)
	}
	// And the tombstone carries the moment access was cut, not just the delete.
	var revokedAt *time.Time
	if err := p.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT revoked_at FROM %s WHERE id = $1`, tblTombstones), id).Scan(&revokedAt); err != nil {
		t.Fatalf("read tombstone: %v", err)
	}
	if revokedAt == nil {
		t.Error("tombstone revoked_at is null; the stamp off the retired row was lost")
	}

	// 1. The stale replica's whole-document write: an unguarded upsert here is a
	//    plain INSERT of a live tenant with a working token.
	stale := &Customer{ID: id, Token: "tok_tomb", Plan: "pro"}
	if err := p.upsertCustomer(stale); !errors.Is(err, ErrCustomerTombstoned) {
		t.Errorf("upsertCustomer on a retired id = %v, want ErrCustomerTombstoned", err)
	}
	// 2. A create drawing the same id.
	if _, _, err := s1.CreateTenant(&Customer{ID: id, Token: "tok_tomb2", Plan: "pro"}, ""); !errors.Is(err, ErrCustomerExists) {
		t.Errorf("CreateTenant on a retired id = %v, want ErrCustomerExists", err)
	}
	// 3. A revoke from a replica that never saw the finalize: its fallback
	//    insert would give the id a row again.
	stamp := time.Now().UTC()
	if err := p.revokeCustomer(&Customer{ID: id, Token: "tok_tomb", RevokedAt: &stamp}); err != nil {
		t.Errorf("revokeCustomer on a retired id = %v, want the no-op success", err)
	}
	if err := p.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = $1`, tblCustomers), id).Scan(&rows); err != nil {
		t.Fatalf("re-count customer rows: %v", err)
	}
	if rows != 0 {
		t.Errorf("customer rows after the refused writers = %d, want 0 — a retired id has a row again", rows)
	}

	// A restart sees nothing: no row, no token, no tenant.
	s2, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer s2.Close()
	if _, err := s2.AuthCustomer("tok_tomb"); !errors.Is(err, ErrNotFound) {
		t.Error("a permanently retired tenant's token authenticates after a restart")
	}
	if _, err := s2.CustomerByID(id); !errors.Is(err, ErrNotFound) {
		t.Error("a permanently retired tenant's record came back after a restart")
	}
}

// A real stale whole-document writer is refused after revocation. A freshly
// loaded revoked tenant still permits operator cleanup. Gated on the same
// throwaway PostgreSQL fixture as the other integration tests.
func TestPostgresWholeCustomerWriteCannotUndoARevoke(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	const id = "cust_monotonic"

	s1, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	cleanupTenantRows(t, ctx, dsn, id)
	if _, _, err := s1.CreateTenant(&Customer{ID: id, Token: "tok_mono", Plan: "pro"}, ""); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	live, err := s1.CustomerByID(id)
	if err != nil {
		t.Fatal(err)
	}
	stale := *live
	if err := s1.RevokeCustomer(id); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}

	// Another central, still holding the pre-revoke record, attaches a mesh and
	// writes the whole document back.
	p, ok := s1.persist.(*pgPersister)
	if !ok {
		t.Fatalf("persister = %T, want *pgPersister", s1.persist)
	}
	stale.Mesh = &MeshEndpoint{Provider: "box", User: "acme", BackendID: "linode-7"}
	var conflict *customerWriteConflict
	if err := p.upsertCustomer(&stale); !errors.As(err, &conflict) {
		t.Fatalf("stale whole-document write = %v; want conflict", err)
	}
	s1.Close()

	s2, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer s2.Close()
	if _, err := s2.AuthCustomer("tok_mono"); !errors.Is(err, ErrNotFound) {
		t.Fatal("a stale whole-document write brought a revoked tenant's token back to life")
	}
	c, err := s2.CustomerByID(id)
	if err != nil {
		t.Fatalf("revoked record lost: %v", err)
	}
	if !c.Revoked() {
		t.Error("the durable row is no longer revoked")
	}
	if c.Mesh != nil {
		t.Error("refused stale write changed the mesh")
	}
	if err := s2.SetCustomerMesh(id, stale.Mesh); err != nil {
		t.Fatalf("cleanup from a current revoked record: %v", err)
	}
	var data []byte
	if err := s2.persist.(*pgPersister).pool.QueryRow(ctx, "SELECT data FROM customers WHERE id=$1", id).Scan(&data); err != nil {
		t.Fatal(err)
	}
	var stored Customer
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	if !stored.Revoked() || stored.Mesh == nil || stored.Mesh.BackendID != "linode-7" {
		t.Error("fresh cleanup did not persist its mesh while preserving revocation")
	}
	if err := s2.DeleteRevokedCustomer(id); err != nil {
		t.Errorf("DeleteRevokedCustomer: %v", err)
	}
}

// The account surface reads a tenant while an offboard is revoking it. Under
// -race this is the check that nothing hands out a live customer pointer for a
// caller to read after the lock is gone.
func TestCustomerWritersAndSummaryReadersDoNotRace(t *testing.T) {
	s, _ := storeWithSpy()
	const tenants = 8
	for i := range tenants {
		s.AddCustomer(&Customer{ID: fmt.Sprintf("cust_%d", i), Token: fmt.Sprintf("tok_%d", i), Plan: "pro"})
	}

	var wg sync.WaitGroup
	for i := range tenants {
		id := fmt.Sprintf("cust_%d", i)
		wg.Add(4)
		go func() {
			defer wg.Done()
			if err := s.RevokeCustomer(id); err != nil {
				t.Errorf("RevokeCustomer(%s): %v", id, err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := s.SetCustomerMesh(id, &MeshEndpoint{Provider: "box", User: id}); err != nil &&
				!errors.Is(err, ErrNotFound) {
				t.Errorf("SetCustomerMesh(%s): %v", id, err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := s.SetCustomerGatewayRoutes(id, id+"-cluster", []string{"10.42.0.0/24"}); err != nil &&
				!errors.Is(err, ErrNotFound) {
				t.Errorf("SetCustomerGatewayRoutes(%s): %v", id, err)
			}
		}()
		go func() {
			defer wg.Done()
			for range 20 {
				if _, err := s.TenantSummaryByID(id); err != nil && !errors.Is(err, ErrNotFound) {
					t.Errorf("TenantSummaryByID(%s): %v", id, err)
				}
				if _, err := s.AuthCustomer("tok_" + id); err != nil && !errors.Is(err, ErrNotFound) {
					t.Errorf("AuthCustomer(%s): %v", id, err)
				}
			}
		}()
	}
	wg.Wait()

	for i := range tenants {
		id := fmt.Sprintf("cust_%d", i)
		sum, err := s.TenantSummaryByID(id)
		if err != nil {
			t.Fatalf("TenantSummaryByID(%s) after the storm: %v", id, err)
		}
		if !sum.Revoked {
			t.Errorf("%s is not revoked; a concurrent write undid it", id)
		}
	}
}

// The summary is a value taken under the lock, so what a caller reads cannot
// change under it — and it carries no credential.
func TestTenantSummaryByIDIsACopy(t *testing.T) {
	s, _ := storeWithSpy()
	s.AddCustomer(&Customer{
		ID: "cust_sum", Token: "tok_sum", Plan: "enterprise",
		MaxConcurrentBursts: 3, MaxHourlyUSD: 7,
	})

	sum, err := s.TenantSummaryByID("cust_sum")
	if err != nil {
		t.Fatalf("TenantSummaryByID: %v", err)
	}
	if sum.Plan != "enterprise" || sum.MaxConcurrentBursts != 3 || sum.MaxHourlyUSD != 7 || sum.Revoked {
		t.Fatalf("summary = %+v, want the live tenant's plan and limits", sum)
	}
	if err := s.RevokeCustomer("cust_sum"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if sum.Revoked {
		t.Error("the summary changed under its holder; it is a view of the live record, not a copy")
	}
	after, err := s.TenantSummaryByID("cust_sum")
	if err != nil {
		t.Fatalf("TenantSummaryByID after revoke: %v", err)
	}
	if !after.Revoked {
		t.Error("a fresh summary does not report the revoke")
	}
	if _, err := s.TenantSummaryByID("cust_missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("summary for an unknown tenant = %v, want ErrNotFound", err)
	}
}

func TestSelfServiceTenantIDCollisionAndSummaryNameAreDurable(t *testing.T) {
	s, spy := storeWithSpy()
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-owner", AccountProfile{Email: "owner@acme.com", EmailVerified: true})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	id := SelfServiceTenantID(acct.ID)
	if id == "" || id != SelfServiceTenantID(acct.ID) {
		t.Fatalf("self-service tenant id is not deterministic: %q", id)
	}

	cust, owner, err := s.CreateTenant(&Customer{
		ID: id, Token: "tok_self", Email: acct.Email, Plan: "trial", Name: "Acme Labs",
		MaxConcurrentBursts: 1, MaxHourlyUSD: 1.0,
	}, acct.ID)
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if cust.ID != id || owner == nil || owner.AccountID != acct.ID || owner.Role != RoleOwner {
		t.Fatalf("created customer/owner = %+v / %+v", cust, owner)
	}
	sum, err := s.TenantSummaryByID(id)
	if err != nil {
		t.Fatalf("TenantSummaryByID: %v", err)
	}
	if sum.Name != "Acme Labs" || sum.Plan != "trial" || sum.MaxConcurrentBursts != 1 || sum.MaxHourlyUSD != 1.0 {
		t.Fatalf("summary = %+v, want self-service display name and trial limits", sum)
	}

	restarted := emptyStore()
	restarted.applySnapshot(spy.recordedSnapshot(t))
	loaded, err := restarted.TenantSummaryByID(id)
	if err != nil {
		t.Fatalf("TenantSummaryByID after restart: %v", err)
	}
	if loaded.Name != "Acme Labs" {
		t.Fatalf("loaded summary name = %q, want durable display name", loaded.Name)
	}
	if _, _, err := s.CreateTenant(&Customer{ID: id, Token: "tok_second", Plan: "trial"}, acct.ID); !errors.Is(err, ErrCustomerExists) {
		t.Fatalf("second create with deterministic id = %v, want ErrCustomerExists", err)
	}
	if len(spy.customers) != 1 || len(spy.memberships) != 1 {
		t.Fatalf("durable rows after collision = %d customer(s), %d grant(s), want 1 and 1",
			len(spy.customers), len(spy.memberships))
	}
}
