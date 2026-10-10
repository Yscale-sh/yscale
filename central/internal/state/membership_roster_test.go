package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// rosterStore builds a store with one tenant and the named (subject → role)
// members already granted, returning the account ids by subject.
func rosterStore(t *testing.T, customerID string, roles map[string]string) (*Store, *accountSpyPersister, map[string]string) {
	t.Helper()
	s, spy := storeWithSpy(customerID)
	ids := make(map[string]string, len(roles))
	for sub, role := range roles {
		acct, err := s.UpsertAccount("https://id.yscale.sh", sub, AccountProfile{Email: sub + "@acme.com"})
		if err != nil {
			t.Fatalf("UpsertAccount %s: %v", sub, err)
		}
		if _, err := s.AddTenantMembership(acct.ID, customerID, role); err != nil {
			t.Fatalf("AddTenantMembership %s: %v", sub, err)
		}
		ids[sub] = acct.ID
	}
	return s, spy, ids
}

// A roster is sorted by account id and handed out as copies. Mutating what a
// caller got back must not reach into the store, and a revoked tenant has no
// roster at all — the revoke takes the grants with the tenant.
func TestMembershipsForTenantIsSortedAndCopied(t *testing.T) {
	s, _, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "bob": RoleAdmin, "carol": RoleViewer,
	})

	roster := s.MembershipsForTenant("cust_r")
	if len(roster) != 3 {
		t.Fatalf("roster = %d entries, want 3", len(roster))
	}
	for i := 1; i < len(roster); i++ {
		if roster[i-1].AccountID >= roster[i].AccountID {
			t.Fatalf("roster is not sorted by account id: %+v", roster)
		}
	}
	roster[0].Role = "not-a-role"
	for _, m := range s.MembershipsForTenant("cust_r") {
		if m.Role == "not-a-role" {
			t.Fatal("MembershipsForTenant handed out the stored record, not a copy")
		}
	}

	if got := s.MembershipsForTenant("cust_missing"); len(got) != 0 {
		t.Errorf("roster of an unknown tenant = %+v, want empty", got)
	}
	if err := s.RevokeCustomer("cust_r"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if got := s.MembershipsForTenant("cust_r"); len(got) != 0 {
		t.Errorf("roster of a revoked tenant = %+v, want empty", got)
	}
	if _, err := s.MembershipFor(ids["alice"], "cust_r"); !errors.Is(err, ErrNotFound) {
		t.Errorf("MembershipFor on a revoked tenant = %v, want ErrNotFound", err)
	}
}

// MembershipFor collapses "no such tenant", "revoked tenant" and "not a member"
// into one answer. Telling them apart is how a caller enumerates tenant ids.
func TestMembershipForIsIndistinguishableAcrossTheThreeMisses(t *testing.T) {
	s, _, ids := rosterStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	s.AddCustomer(&Customer{ID: "cust_other", Token: "tok_other", Plan: "pro"})
	if err := s.RevokeCustomer("cust_other"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	stranger, err := s.UpsertAccount("https://id.yscale.sh", "stranger", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	for _, tc := range []struct{ name, account, tenant string }{
		{"unknown tenant", ids["alice"], "cust_nope"},
		{"revoked tenant", ids["alice"], "cust_other"},
		{"not a member", stranger.ID, "cust_r"},
	} {
		if _, err := s.MembershipFor(tc.account, tc.tenant); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: MembershipFor = %v, want ErrNotFound", tc.name, err)
		}
	}
}

// A member or viewer holds a grant but no authority over the roster, and the
// error says so — distinct from the not-found a stranger gets.
func TestRemoveTenantMembershipRequiresOwnerOrAdmin(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember, "view": RoleViewer,
	})

	for _, caller := range []string{"mem", "view"} {
		removed, err := s.RemoveTenantMembership("cust_r", ids[caller], ids["alice"], SystemActor())
		if !errors.Is(err, ErrNotAuthorized) {
			t.Errorf("%s removing the owner = %v, want ErrNotAuthorized", caller, err)
		}
		// The refusal is about the CALLER's role, so it is not the
		// owner-protected one an admin gets — and it removed nothing.
		if errors.Is(err, ErrOwnerProtected) {
			t.Errorf("%s removing the owner = %v, want the plain not-authorized refusal", caller, err)
		}
		if removed != nil {
			t.Errorf("%s removing the owner reported %+v removed", caller, removed)
		}
	}
	// A stranger is ErrNotFound, not ErrNotAuthorized: they must not learn the
	// tenant exists.
	stranger, err := s.UpsertAccount("https://id.yscale.sh", "stranger", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s.RemoveTenantMembership("cust_r", stranger.ID, ids["alice"], SystemActor()); !errors.Is(err, ErrNotFound) {
		t.Errorf("stranger removing a member = %v, want ErrNotFound", err)
	}
	if _, err := s.RemoveTenantMembership("cust_nope", ids["alice"], ids["mem"], SystemActor()); !errors.Is(err, ErrNotFound) {
		t.Errorf("removal on an unknown tenant = %v, want ErrNotFound", err)
	}
	if len(s.MembershipsForTenant("cust_r")) != 3 || len(spy.memberships) != 3 {
		t.Fatalf("a refused removal changed the roster: memory=%d durable=%d",
			len(s.MembershipsForTenant("cust_r")), len(spy.memberships))
	}
}

// An admin manages the roster but is not an owner's peer: only an owner may
// remove an owner. What an admin MAY remove goes, durably and idempotently.
func TestAdminCannotRemoveAnOwner(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "adm": RoleAdmin, "mem": RoleMember,
	})

	// One call, both assertions on ITS error: two calls would prove two
	// properties of two different refusals and say nothing about either one.
	// An admin MAY manage this roster, so the refusal is the owner-protected
	// one — the distinction a truthful 403 message is built on.
	_, err := s.RemoveTenantMembership("cust_r", ids["adm"], ids["alice"], SystemActor())
	if !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("admin removing an owner = %v, want ErrNotAuthorized", err)
	}
	if !errors.Is(err, ErrOwnerProtected) {
		t.Fatalf("admin removing an owner = %v, want ErrOwnerProtected", err)
	}
	if _, err := s.MembershipFor(ids["alice"], "cust_r"); err != nil {
		t.Fatalf("the owner's grant was removed anyway: %v", err)
	}

	removed, err := s.RemoveTenantMembership("cust_r", ids["adm"], ids["mem"], SystemActor())
	if err != nil {
		t.Fatalf("admin removing a member = %v, want success", err)
	}
	if removed == nil || removed.AccountID != ids["mem"] || removed.Role != RoleMember {
		t.Fatalf("removal reported %+v, want the member's grant", removed)
	}
	if _, err := s.MembershipFor(ids["mem"], "cust_r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the member's grant survived: %v", err)
	}
	if _, still := spy.memberships[membershipID(ids["mem"], "cust_r")]; still {
		t.Fatal("the durable row survived the removal")
	}
	// Idempotent: removing it again is a no-op success, not an error — and it
	// says so, reporting nothing removed, so a caller cannot audit-log a
	// deletion that already happened (possibly at someone else's hand).
	again, err := s.RemoveTenantMembership("cust_r", ids["adm"], ids["mem"], SystemActor())
	if err != nil {
		t.Fatalf("second removal = %v, want a no-op success", err)
	}
	if again != nil {
		t.Fatalf("second removal reported %+v removed, want nothing", again)
	}
	// Idempotency does NOT leak the tenant: a stranger asking about an absent
	// target still gets not-found, never the no-op success.
	stranger, err := s.UpsertAccount("https://id.yscale.sh", "stranger", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s.RemoveTenantMembership("cust_r", stranger.ID, ids["mem"], SystemActor()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stranger removing an absent target = %v, want ErrNotFound", err)
	}
}

// An owner may stand down while another owner remains — including removing
// themselves — and the floor holds the moment they are the last one.
func TestOwnerCanRemoveSelfWhenAnotherOwnerRemains(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "bob": RoleOwner,
	})

	removed, err := s.RemoveTenantMembership("cust_r", ids["alice"], ids["alice"], SystemActor())
	if err != nil {
		t.Fatalf("owner removing self = %v, want success", err)
	}
	if removed == nil || removed.Role != RoleOwner {
		t.Fatalf("removal reported %+v, want the departing owner's grant", removed)
	}
	if _, err := s.MembershipFor(ids["alice"], "cust_r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the departing owner kept their grant: %v", err)
	}
	if _, still := spy.memberships[membershipID(ids["alice"], "cust_r")]; still {
		t.Fatal("the departing owner's durable row survived")
	}
	if _, err := s.RemoveTenantMembership("cust_r", ids["bob"], ids["bob"], SystemActor()); !errors.Is(err, ErrLastOwner) {
		t.Fatalf("last owner removing self = %v, want ErrLastOwner", err)
	}
}

// The last owner is refused whoever asks, and the refusal changes nothing in
// memory or on disk — a tenant with no owner cannot be repaired from here.
func TestLastOwnerRemovalIsRefusedAndChangesNothing(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "adm": RoleAdmin,
	})
	ownerRow := membershipID(ids["alice"], "cust_r")
	before := string(spy.memberships[ownerRow])

	if _, err := s.RemoveTenantMembership("cust_r", ids["alice"], ids["alice"], SystemActor()); !errors.Is(err, ErrLastOwner) {
		t.Fatalf("owner removing self = %v, want ErrLastOwner", err)
	}
	if _, err := s.MembershipFor(ids["alice"], "cust_r"); err != nil {
		t.Fatalf("the last owner's grant was removed: %v", err)
	}
	if got := string(spy.memberships[ownerRow]); got != before {
		t.Fatalf("the durable owner row changed: %q → %q", before, got)
	}
	// The other way at an owner is closed too: an admin cannot reach one at all,
	// and the refusal is the owner-protected one rather than the owner floor —
	// an admin is stopped before the count is ever consulted.
	_, err := s.RemoveTenantMembership("cust_r", ids["adm"], ids["alice"], SystemActor())
	if !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("admin removing the last owner = %v, want ErrNotAuthorized", err)
	}
	if !errors.Is(err, ErrOwnerProtected) {
		t.Fatalf("admin removing the last owner = %v, want ErrOwnerProtected", err)
	}
	if errors.Is(err, ErrLastOwner) {
		t.Fatalf("the admin's refusal consulted the owner count: %v", err)
	}
	if len(s.MembershipsForTenant("cust_r")) != 2 {
		t.Fatal("the roster changed under a refused removal")
	}
	// It survives a restart, which is the state the invariant actually protects.
	reloaded := emptyStore()
	reloaded.applySnapshot(spy.snapshotOf(t, &Customer{ID: "cust_r", Token: "tok_cust_r", Plan: "pro"}))
	if _, err := reloaded.MembershipFor(ids["alice"], "cust_r"); err != nil {
		t.Fatalf("the last owner's grant did not survive the restart: %v", err)
	}
}

// Two owners removing each other at the same time is the race the whole
// operation is locked for: read-then-write would let both see "another owner
// exists" and commit, leaving a tenant nobody owns.
func TestConcurrentOwnerRemovalsLeaveOneOwner(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "bob": RoleOwner,
	})

	var wg sync.WaitGroup
	errs := make([]error, 2)
	removed := make([]*TenantMembership, 2)
	start := make(chan struct{})
	for i, sub := range []string{"alice", "bob"} {
		wg.Add(1)
		go func(i int, sub string) {
			defer wg.Done()
			<-start
			removed[i], errs[i] = s.RemoveTenantMembership("cust_r", ids[sub], ids[sub], SystemActor())
		}(i, sub)
	}
	close(start)
	wg.Wait()

	won, refused := 0, 0
	for i, err := range errs {
		switch {
		case err == nil:
			won++
			if removed[i] == nil || removed[i].Role != RoleOwner {
				t.Fatalf("the winning removal reported %+v, want the owner grant it deleted", removed[i])
			}
		case errors.Is(err, ErrLastOwner):
			refused++
			if removed[i] != nil {
				t.Fatalf("the refused removal reported %+v removed", removed[i])
			}
		default:
			t.Fatalf("unexpected error from a concurrent removal: %v", err)
		}
	}
	if won != 1 || refused != 1 {
		t.Fatalf("concurrent removals: %d succeeded, %d refused; want exactly 1 and 1 (%v)", won, refused, errs)
	}
	roster := s.MembershipsForTenant("cust_r")
	if len(roster) != 1 || roster[0].Role != RoleOwner {
		t.Fatalf("roster after the race = %+v, want exactly one owner", roster)
	}
	if len(spy.memberships) != 1 {
		t.Fatalf("durable rows after the race = %d, want 1", len(spy.memberships))
	}
}

// A refused durable delete puts the grant back in BOTH indexes. A removal that
// only landed in memory would be access somebody was told was revoked, handed
// straight back at the next restart.
func TestMembershipRemovalFailsClosedAndRestoresTheGrant(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember,
	})
	spy.deleteErr = errors.New("connection refused")

	refused, err := s.RemoveTenantMembership("cust_r", ids["alice"], ids["mem"], SystemActor())
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("removal against a refusing backend = %v, want ErrPersistence", err)
	}
	if refused != nil {
		t.Fatalf("a rolled-back removal reported %+v removed", refused)
	}
	m, err := s.MembershipFor(ids["mem"], "cust_r")
	if err != nil {
		t.Fatalf("the grant was not restored in memory: %v", err)
	}
	if m.Role != RoleMember {
		t.Fatalf("restored grant = %+v, want the member role back", m)
	}
	tenants := s.MembershipsForAccount(ids["mem"])
	if len(tenants) != 1 || tenants[0].CustomerID != "cust_r" {
		t.Fatalf("the account index was not restored: %+v", tenants)
	}
	if len(s.MembershipsForTenant("cust_r")) != 2 {
		t.Fatal("the tenant index was not restored")
	}
	reloaded := emptyStore()
	reloaded.applySnapshot(spy.snapshotOf(t, &Customer{ID: "cust_r", Token: "tok_cust_r", Plan: "pro"}))
	if _, err := reloaded.MembershipFor(ids["mem"], "cust_r"); err != nil {
		t.Fatalf("the grant did not survive the restart it was never removed from: %v", err)
	}

	// With the backend healthy the same removal sticks, across a restart too.
	spy.deleteErr = nil
	stuck, err := s.RemoveTenantMembership("cust_r", ids["alice"], ids["mem"], SystemActor())
	if err != nil {
		t.Fatalf("removal against a healthy backend = %v", err)
	}
	if stuck == nil || stuck.AccountID != ids["mem"] {
		t.Fatalf("the durable removal reported %+v, want the member's grant", stuck)
	}
	restarted := emptyStore()
	restarted.applySnapshot(spy.snapshotOf(t, &Customer{ID: "cust_r", Token: "tok_cust_r", Plan: "pro"}))
	if _, err := restarted.MembershipFor(ids["mem"], "cust_r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a removed membership came back after a restart: %v", err)
	}
	if _, err := restarted.MembershipFor(ids["alice"], "cust_r"); err != nil {
		t.Fatalf("the owner's grant was lost with the member's: %v", err)
	}
}

// A removed human no longer holds the tenant on their own account surface — the
// removal revokes visibility, it does not just edit a roster — and their other
// tenants are untouched.
func TestRemovedMemberNoLongerHoldsTheTenant(t *testing.T) {
	s, _, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember,
	})
	s.AddCustomer(&Customer{ID: "cust_keep", Token: "tok_keep", Plan: "pro"})
	if _, err := s.AddTenantMembership(ids["mem"], "cust_keep", RoleViewer); err != nil {
		t.Fatalf("second membership: %v", err)
	}

	if _, err := s.RemoveTenantMembership("cust_r", ids["alice"], ids["mem"], SystemActor()); err != nil {
		t.Fatalf("RemoveTenantMembership: %v", err)
	}
	tenants := s.MembershipsForAccount(ids["mem"])
	if len(tenants) != 1 || tenants[0].CustomerID != "cust_keep" {
		t.Fatalf("tenants after removal = %+v, want only cust_keep", tenants)
	}
}

// The roster read answers the whole question at once: the tenant is live, this
// caller manages it, and here is every member with the profile fields a roster
// renders. What it hands back is values, so nothing a caller does to a page can
// reach a stored record.
func TestTenantRosterForSnapshotsTheWholeRoster(t *testing.T) {
	s, _, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "bob": RoleAdmin, "carol": RoleViewer,
	})

	roster, err := s.TenantRosterFor("cust_r", ids["alice"], RosterQuery{})
	if err != nil {
		t.Fatalf("TenantRosterFor: %v", err)
	}
	if len(roster.Members) != 3 {
		t.Fatalf("roster = %+v, want 3 members", roster.Members)
	}
	if roster.NextAfter != "" {
		t.Errorf("a complete roster reported more to come: %q", roster.NextAfter)
	}
	for i := 1; i < len(roster.Members); i++ {
		if roster.Members[i-1].AccountID >= roster.Members[i].AccountID {
			t.Fatalf("roster is not ordered by account id: %+v", roster.Members)
		}
	}
	byID := make(map[string]TenantMember, len(roster.Members))
	for _, m := range roster.Members {
		byID[m.AccountID] = m
	}
	owner, ok := byID[ids["alice"]]
	if !ok {
		t.Fatalf("the owner is missing from the roster: %+v", roster.Members)
	}
	if owner.Role != RoleOwner || owner.Email != "alice@acme.com" || owner.CreatedAt.IsZero() {
		t.Errorf("owner row = %+v, want the role, email and grant time", owner)
	}

	// The page is a copy all the way down: editing it changes nothing stored.
	roster.Members[0].Role = "not-a-role"
	again, err := s.TenantRosterFor("cust_r", ids["alice"], RosterQuery{})
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	for _, m := range again.Members {
		if m.Role == "not-a-role" {
			t.Fatal("the roster handed out a shared record, not a copy")
		}
	}
	if m, err := s.MembershipFor(again.Members[0].AccountID, "cust_r"); err != nil || m.Role == "not-a-role" {
		t.Fatalf("a caller reached into a stored membership: %+v (%v)", m, err)
	}
}

// The roster read refuses exactly the way MembershipFor does, because it is the
// same check: no grant is the uniform not-found, and a grant that does not
// manage the roster is the distinguishable not-authorized.
func TestTenantRosterForRefusesLikeMembershipFor(t *testing.T) {
	s, _, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember, "view": RoleViewer,
	})
	s.AddCustomer(&Customer{ID: "cust_dead", Token: "tok_dead", Plan: "pro"})
	if _, err := s.AddTenantMembership(ids["alice"], "cust_dead", RoleOwner); err != nil {
		t.Fatalf("membership on the doomed tenant: %v", err)
	}
	if err := s.RevokeCustomer("cust_dead"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	stranger, err := s.UpsertAccount("https://id.yscale.sh", "stranger", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	for _, tc := range []struct{ name, caller, tenant string }{
		{"unknown tenant", ids["alice"], "cust_nope"},
		{"revoked tenant", ids["alice"], "cust_dead"},
		{"not a member", stranger.ID, "cust_r"},
	} {
		roster, err := s.TenantRosterFor(tc.tenant, tc.caller, RosterQuery{})
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: TenantRosterFor = %v, want ErrNotFound", tc.name, err)
		}
		if len(roster.Members) != 0 {
			t.Errorf("%s: a refused read returned %+v", tc.name, roster.Members)
		}
	}
	for _, caller := range []string{"mem", "view"} {
		roster, err := s.TenantRosterFor("cust_r", ids[caller], RosterQuery{})
		if !errors.Is(err, ErrNotAuthorized) {
			t.Errorf("%s reading the roster = %v, want ErrNotAuthorized", caller, err)
		}
		if len(roster.Members) != 0 {
			t.Errorf("%s: a refused read returned %+v", caller, roster.Members)
		}
	}
}

// The read that used to be three calls cannot straddle a revoke. Every answer
// is either the roster as it stood or the uniform not-found; a success carrying
// an empty roster — "you manage this tenant, it has nobody on it" — is the
// outcome the single lock exists to make unreachable.
func TestTenantRosterForDoesNotStraddleARevoke(t *testing.T) {
	s, _, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "bob": RoleAdmin, "carol": RoleViewer,
	})

	var wg sync.WaitGroup
	start := make(chan struct{})
	empties := make([]bool, 8)
	for i := range empties {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			roster, err := s.TenantRosterFor("cust_r", ids["alice"], RosterQuery{})
			empties[i] = err == nil && len(roster.Members) == 0
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		if err := s.RevokeCustomer("cust_r"); err != nil {
			t.Errorf("RevokeCustomer: %v", err)
		}
	}()
	close(start)
	wg.Wait()

	for i, empty := range empties {
		if empty {
			t.Fatalf("read %d succeeded with an empty roster — the read straddled the revoke", i)
		}
	}
}

// A membership whose account row is gone still holds access, so it is still on
// the roster. Skipping it — which the per-member account lookup this replaced
// did — under-reports who can reach the tenant, and the row an operator needs
// to remove is the one that disappears.
func TestTenantRosterForKeepsAMemberWithNoAccountRow(t *testing.T) {
	s, _, ids := rosterStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	// Exercise the intentionally in-memory fixture below. PostgreSQL's
	// missing-profile contract is covered by TestPostgresAccountReadsRosterMissingProfile.
	s.persist = nil
	orphan := &TenantMembership{
		ID: membershipID("acct_ghost", "cust_r"), AccountID: "acct_ghost",
		CustomerID: "cust_r", Role: RoleAdmin, CreatedAt: time.Now().UTC(),
	}
	s.mu.Lock()
	s.indexMembership(orphan)
	s.mu.Unlock()

	roster, err := s.TenantRosterFor("cust_r", ids["alice"], RosterQuery{})
	if err != nil {
		t.Fatalf("TenantRosterFor: %v", err)
	}
	if len(roster.Members) != 2 {
		t.Fatalf("roster = %+v, want the owner and the profile-less member", roster.Members)
	}
	var ghost TenantMember
	for _, m := range roster.Members {
		if m.AccountID == "acct_ghost" {
			ghost = m
		}
	}
	if ghost.Role != RoleAdmin {
		t.Fatalf("the member with no account row was dropped or mangled: %+v", roster.Members)
	}
	if ghost.Email != "" || ghost.Name != "" {
		t.Errorf("a member with no account row was rendered with a profile: %+v", ghost)
	}
}

// Pages are deterministic and the response is bounded whatever the caller asks
// for: the cursor resumes exactly where the last page stopped, NextAfter is set
// only while there is more, and no query produces the whole roster of a tenant
// larger than the cap.
func TestTenantRosterForPagesDeterministicallyAndStaysBounded(t *testing.T) {
	roles := map[string]string{"alice": RoleOwner}
	for i := 0; i < DefaultRosterLimit+5; i++ {
		roles[fmt.Sprintf("sub%03d", i)] = RoleMember
	}
	s, _, ids := rosterStore(t, "cust_r", roles)
	total := len(roles)

	// The default bounds a roster nobody asked to bound.
	first, err := s.TenantRosterFor("cust_r", ids["alice"], RosterQuery{})
	if err != nil {
		t.Fatalf("TenantRosterFor: %v", err)
	}
	if len(first.Members) != DefaultRosterLimit {
		t.Fatalf("default page = %d members, want %d", len(first.Members), DefaultRosterLimit)
	}
	if first.NextAfter != first.Members[DefaultRosterLimit-1].AccountID {
		t.Fatalf("next cursor = %q, want the last id on the page", first.NextAfter)
	}
	// A limit past the cap is clamped rather than honored: no caller gets an
	// unbounded roster, whatever it passes.
	if page, err := s.TenantRosterFor("cust_r", ids["alice"], RosterQuery{Limit: MaxRosterLimit * 10}); err != nil {
		t.Fatalf("TenantRosterFor: %v", err)
	} else if len(page.Members) > MaxRosterLimit {
		t.Fatalf("page = %d members, want at most %d", len(page.Members), MaxRosterLimit)
	}

	// Walking the cursor visits every member exactly once, in order.
	seen := make(map[string]bool, total)
	last, cursor := "", ""
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatal("the cursor walk did not terminate")
		}
		page, err := s.TenantRosterFor("cust_r", ids["alice"], RosterQuery{Limit: 7, After: cursor})
		if err != nil {
			t.Fatalf("page after %q: %v", cursor, err)
		}
		if len(page.Members) > 7 {
			t.Fatalf("page = %d members, want at most 7", len(page.Members))
		}
		for _, m := range page.Members {
			if seen[m.AccountID] {
				t.Fatalf("%s appeared on two pages", m.AccountID)
			}
			if m.AccountID <= last {
				t.Fatalf("pages are out of order at %s (after %s)", m.AccountID, last)
			}
			seen[m.AccountID], last = true, m.AccountID
		}
		if page.NextAfter == "" {
			break
		}
		cursor = page.NextAfter
	}
	if len(seen) != total {
		t.Fatalf("the cursor walk saw %d of %d members", len(seen), total)
	}
	// The same query twice is the same page.
	a, _ := s.TenantRosterFor("cust_r", ids["alice"], RosterQuery{Limit: 5})
	b, _ := s.TenantRosterFor("cust_r", ids["alice"], RosterQuery{Limit: 5})
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatalf("identical queries returned different pages:\n%v\n%v", a, b)
	}
}

// The grant path reports whether IT created the grant, from inside the write
// lock. An operator route answers 201-vs-200 off that, and audits only a real
// creation, so a retry cannot claim a grant it did not make.
func TestGrantTenantMembershipReportsWhoCreatedIt(t *testing.T) {
	s, spy := storeWithSpy("cust_r")
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-grant", AccountProfile{Email: "g@acme.com"})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	m, created, err := s.GrantTenantMembership(acct.ID, "cust_r", RoleAdmin, SystemActor())
	if err != nil || !created {
		t.Fatalf("first grant = (%+v, %v, %v), want a created grant", m, created, err)
	}
	if m.Role != RoleAdmin || m.CustomerID != "cust_r" {
		t.Fatalf("granted membership = %+v", m)
	}
	if _, ok := spy.memberships[m.ID]; !ok {
		t.Fatal("the grant was not written durably")
	}
	again, created, err := s.GrantTenantMembership(acct.ID, "cust_r", RoleAdmin, SystemActor())
	if err != nil {
		t.Fatalf("identical re-grant = %v, want idempotent success", err)
	}
	if created {
		t.Fatal("an idempotent re-grant reported itself as the creator")
	}
	if again.ID != m.ID || !again.CreatedAt.Equal(m.CreatedAt) {
		t.Fatalf("re-grant returned a different record: %+v vs %+v", again, m)
	}
	if _, created, err := s.GrantTenantMembership(acct.ID, "cust_r", RoleViewer, SystemActor()); !errors.Is(err, ErrRoleConflict) || created {
		t.Fatalf("conflicting re-grant = (%v, %v), want ErrRoleConflict and no creation", created, err)
	}
	// AddTenantMembership is the same operation with the flag dropped.
	if _, err := s.AddTenantMembership(acct.ID, "cust_r", RoleAdmin); err != nil {
		t.Fatalf("AddTenantMembership on the same grant = %v, want idempotent success", err)
	}
	// Both ends must exist, and a revoked tenant is as good as absent.
	if _, _, err := s.GrantTenantMembership("acct_missing", "cust_r", RoleMember, SystemActor()); !errors.Is(err, ErrNotFound) {
		t.Errorf("grant to an unknown account = %v, want ErrNotFound", err)
	}
	if _, _, err := s.GrantTenantMembership(acct.ID, "cust_missing", RoleMember, SystemActor()); !errors.Is(err, ErrNotFound) {
		t.Errorf("grant on an unknown tenant = %v, want ErrNotFound", err)
	}
	if _, _, err := s.GrantTenantMembership(acct.ID, "cust_r", "superuser", SystemActor()); !errors.Is(err, ErrInvalidRole) {
		t.Errorf("grant with an unrecognised role = %v, want ErrInvalidRole", err)
	}
	if err := s.RevokeCustomer("cust_r"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if _, _, err := s.GrantTenantMembership(acct.ID, "cust_r", RoleMember, SystemActor()); !errors.Is(err, ErrNotFound) {
		t.Errorf("grant on a revoked tenant = %v, want ErrNotFound", err)
	}
}

// Identical grants racing produce ONE created report. The flag comes off the
// insert under the lock, so it cannot be two callers both reading "absent" and
// both claiming the creation in an audit log.
func TestConcurrentIdenticalGrantsCreateOnce(t *testing.T) {
	s, spy := storeWithSpy("cust_r")
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-race", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	const callers = 6
	var wg sync.WaitGroup
	creates := make([]bool, callers)
	errs := make([]error, callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, creates[i], errs[i] = s.GrantTenantMembership(acct.ID, "cust_r", RoleMember, SystemActor())
		}(i)
	}
	close(start)
	wg.Wait()

	got := 0
	for i, created := range creates {
		if errs[i] != nil {
			t.Fatalf("grant %d: %v", i, errs[i])
		}
		if created {
			got++
		}
	}
	if got != 1 {
		t.Fatalf("%d of %d concurrent identical grants reported a creation, want exactly 1", got, callers)
	}
	if len(spy.memberships) != 1 {
		t.Fatalf("durable rows = %d, want 1", len(spy.memberships))
	}
}

// A refused durable write leaves no grant and no creation to report.
func TestGrantTenantMembershipFailsClosed(t *testing.T) {
	s, spy := storeWithSpy("cust_r")
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-fail", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	spy.upsertErr = errors.New("connection refused")

	m, created, err := s.GrantTenantMembership(acct.ID, "cust_r", RoleMember, SystemActor())
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("grant against a refusing backend = %v, want ErrPersistence", err)
	}
	if created || m != nil {
		t.Fatalf("a refused grant reported (%+v, %v)", m, created)
	}
	if _, err := s.MembershipFor(acct.ID, "cust_r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a refused grant left access behind: %v", err)
	}
	if len(spy.memberships) != 0 {
		t.Fatalf("durable rows after a refused grant = %d, want 0", len(spy.memberships))
	}
}

// The operator correction: a role change lands in memory and durably, and
// setting the role a member already holds writes nothing. Both answer with the
// role that was replaced, which is the only way a caller can audit a real
// change without a pre-read.
func TestSetTenantMembershipRoleChangesAndIsIdempotent(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember,
	})
	memberRow := membershipID(ids["mem"], "cust_r")
	granted := string(spy.memberships[memberRow])

	m, previous, err := s.SetTenantMembershipRole(ids["mem"], "cust_r", RoleAdmin, SystemActor())
	if err != nil {
		t.Fatalf("promote member to admin: %v", err)
	}
	if previous != RoleMember || m.Role != RoleAdmin {
		t.Fatalf("promotion reported %q → %q, want member → admin", previous, m.Role)
	}
	if stored, err := s.MembershipFor(ids["mem"], "cust_r"); err != nil || stored.Role != RoleAdmin {
		t.Fatalf("the store still holds %+v (%v), want the admin role", stored, err)
	}
	if string(spy.memberships[memberRow]) == granted {
		t.Fatal("the durable row was not rewritten with the new role")
	}
	// The grant's identity and birth are untouched: this corrects a role, it
	// does not re-issue the membership.
	if m.ID != memberRow || m.AccountID != ids["mem"] || m.CustomerID != "cust_r" {
		t.Fatalf("the role change rewrote the grant's identity: %+v", m)
	}

	rewritten := string(spy.memberships[memberRow])
	same, previous, err := s.SetTenantMembershipRole(ids["mem"], "cust_r", RoleAdmin, SystemActor())
	if err != nil {
		t.Fatalf("setting the role a member already holds: %v", err)
	}
	if previous != RoleAdmin || same.Role != RoleAdmin {
		t.Fatalf("no-op reported %q → %q, want admin → admin", previous, same.Role)
	}
	if got := string(spy.memberships[memberRow]); got != rewritten {
		t.Fatalf("an idempotent set rewrote the durable row: %q → %q", rewritten, got)
	}
	if !same.CreatedAt.Equal(m.CreatedAt) {
		t.Errorf("the grant time moved under a role change: %v → %v", m.CreatedAt, same.CreatedAt)
	}

	// It is the durable state, not just this process's memory.
	reloaded := emptyStore()
	reloaded.applySnapshot(spy.snapshotOf(t, &Customer{ID: "cust_r", Token: "tok_cust_r", Plan: "pro"}))
	if stored, err := reloaded.MembershipFor(ids["mem"], "cust_r"); err != nil || stored.Role != RoleAdmin {
		t.Fatalf("the new role did not survive a restart: %+v (%v)", stored, err)
	}
}

// The correction route's refusals. An operator holds no membership, so nothing
// here consults a caller — but the tenant's own rules still hold: the closed
// role set, the uniform miss, and the owner floor.
func TestSetTenantMembershipRoleRefusesTheMissesAndTheLastOwner(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember,
	})
	s.AddCustomer(&Customer{ID: "cust_dead", Token: "tok_cust_dead", Plan: "pro"})
	if err := s.RevokeCustomer("cust_dead"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	stranger, err := s.UpsertAccount("https://id.yscale.sh", "sub-stranger", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	for _, tc := range []struct {
		name, customer, account, role string
		want                          error
	}{
		{"a role outside the set", "cust_r", ids["mem"], "superuser", ErrInvalidRole},
		{"an empty role", "cust_r", ids["mem"], "", ErrInvalidRole},
		{"an unknown tenant", "cust_nope", ids["mem"], RoleAdmin, ErrNotFound},
		{"a revoked tenant", "cust_dead", ids["mem"], RoleAdmin, ErrNotFound},
		{"an account with no grant", "cust_r", stranger.ID, RoleAdmin, ErrNotFound},
		{"an account that does not exist", "cust_r", "acct_nobody", RoleAdmin, ErrNotFound},
		{"demoting the last owner", "cust_r", ids["alice"], RoleAdmin, ErrLastOwner},
	} {
		m, previous, err := s.SetTenantMembershipRole(tc.account, tc.customer, tc.role, SystemActor())
		if !errors.Is(err, tc.want) {
			t.Errorf("%s = %v, want %v", tc.name, err, tc.want)
		}
		if m != nil || previous != "" {
			t.Errorf("%s reported (%+v, %q) on a refusal", tc.name, m, previous)
		}
	}
	// Nothing a refused call named moved, in memory or durably.
	if m, err := s.MembershipFor(ids["alice"], "cust_r"); err != nil || m.Role != RoleOwner {
		t.Fatalf("the last owner was demoted anyway: %+v (%v)", m, err)
	}
	if m, err := s.MembershipFor(ids["mem"], "cust_r"); err != nil || m.Role != RoleMember {
		t.Fatalf("a refused role change landed: %+v (%v)", m, err)
	}
	reloaded := emptyStore()
	reloaded.applySnapshot(spy.snapshotOf(t, &Customer{ID: "cust_r", Token: "tok_cust_r", Plan: "pro"}))
	if m, err := reloaded.MembershipFor(ids["alice"], "cust_r"); err != nil || m.Role != RoleOwner {
		t.Fatalf("the durable owner row changed under a refusal: %+v (%v)", m, err)
	}

	// The floor is the LAST owner, not the owner role: with a second owner the
	// same demotion is allowed.
	if _, _, err := s.SetTenantMembershipRole(ids["mem"], "cust_r", RoleOwner, SystemActor()); err != nil {
		t.Fatalf("promoting a second owner: %v", err)
	}
	if _, previous, err := s.SetTenantMembershipRole(ids["alice"], "cust_r", RoleAdmin, SystemActor()); err != nil || previous != RoleOwner {
		t.Fatalf("demoting one of two owners = (%q, %v), want the owner role replaced", previous, err)
	}
}

// A refused durable write leaves the role exactly as it was, in both indexes
// and on disk. A role that only landed in memory is authority the next restart
// quietly takes back — or hands out.
func TestSetTenantMembershipRoleFailsClosed(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember,
	})
	memberRow := membershipID(ids["mem"], "cust_r")
	before := string(spy.memberships[memberRow])
	spy.upsertErr = errors.New("connection refused")

	m, previous, err := s.SetTenantMembershipRole(ids["mem"], "cust_r", RoleOwner, SystemActor())
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("role change against a refusing backend = %v, want ErrPersistence", err)
	}
	if m != nil || previous != "" {
		t.Fatalf("a rolled-back role change reported (%+v, %q)", m, previous)
	}
	if stored, err := s.MembershipFor(ids["mem"], "cust_r"); err != nil || stored.Role != RoleMember {
		t.Fatalf("the in-memory role was not rolled back: %+v (%v)", stored, err)
	}
	// The tenant index holds the same pointer, so a half-rolled-back change
	// would show up as two disagreeing roles for one grant.
	for _, held := range s.MembershipsForTenant("cust_r") {
		if held.AccountID == ids["mem"] && held.Role != RoleMember {
			t.Fatalf("the tenant index kept the failed role: %+v", held)
		}
	}
	if got := string(spy.memberships[memberRow]); got != before {
		t.Fatalf("the durable row changed under a refused write: %q → %q", before, got)
	}

	// With the backend healthy the same change sticks, across a restart too.
	spy.upsertErr = nil
	if _, previous, err := s.SetTenantMembershipRole(ids["mem"], "cust_r", RoleOwner, SystemActor()); err != nil || previous != RoleMember {
		t.Fatalf("role change against a healthy backend = (%q, %v)", previous, err)
	}
	restarted := emptyStore()
	restarted.applySnapshot(spy.snapshotOf(t, &Customer{ID: "cust_r", Token: "tok_cust_r", Plan: "pro"}))
	if stored, err := restarted.MembershipFor(ids["mem"], "cust_r"); err != nil || stored.Role != RoleOwner {
		t.Fatalf("the durable role after the retry = %+v (%v), want owner", stored, err)
	}
}

// The operator removal: it proves the tenant is live BEFORE it decides an
// absent grant is a no-op, so a tenant that does not exist is never reported as
// a successful revocation.
func TestDeleteTenantMembershipIsIdempotentBehindALiveTenant(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember,
	})
	s.AddCustomer(&Customer{ID: "cust_dead", Token: "tok_cust_dead", Plan: "pro"})
	if err := s.RevokeCustomer("cust_dead"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}

	removed, err := s.DeleteTenantMembership(ids["mem"], "cust_r", SystemActor())
	if err != nil {
		t.Fatalf("operator removal: %v", err)
	}
	if removed == nil || removed.AccountID != ids["mem"] || removed.Role != RoleMember {
		t.Fatalf("removal reported %+v, want the member's grant", removed)
	}
	if _, err := s.MembershipFor(ids["mem"], "cust_r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the grant survived the removal: %v", err)
	}
	if _, still := spy.memberships[membershipID(ids["mem"], "cust_r")]; still {
		t.Fatal("the durable row survived the removal")
	}

	// Again is a no-op success that reports nothing removed, so a retry cannot
	// be audited as a revocation.
	again, err := s.DeleteTenantMembership(ids["mem"], "cust_r", SystemActor())
	if err != nil || again != nil {
		t.Fatalf("second removal = (%+v, %v), want an idempotent no-op", again, err)
	}
	// A human who never had a grant on this live tenant is the same no-op.
	if absent, err := s.DeleteTenantMembership("acct_nobody", "cust_r", SystemActor()); err != nil || absent != nil {
		t.Fatalf("removing a stranger = (%+v, %v), want an idempotent no-op", absent, err)
	}
	// But a tenant that is not there is ErrNotFound, not a no-op: 204 on a
	// tenant that does not exist reports a revocation on nothing.
	for _, customer := range []string{"cust_nope", "cust_dead"} {
		if _, err := s.DeleteTenantMembership(ids["mem"], customer, SystemActor()); !errors.Is(err, ErrNotFound) {
			t.Errorf("removal on %s = %v, want ErrNotFound", customer, err)
		}
	}

	reloaded := emptyStore()
	reloaded.applySnapshot(spy.snapshotOf(t, &Customer{ID: "cust_r", Token: "tok_cust_r", Plan: "pro"}))
	if _, err := reloaded.MembershipFor(ids["mem"], "cust_r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a removed membership came back after a restart: %v", err)
	}
	if _, err := reloaded.MembershipFor(ids["alice"], "cust_r"); err != nil {
		t.Fatalf("the owner's grant was lost with the member's: %v", err)
	}
}

// The owner floor holds against the operator too. An operator CAN remove an
// owner — unlike an admin, they answer to no role — but not the last one: an
// ownerless tenant is unrecoverable through the human surface, and the route
// that would have to fix it is the one that broke it.
func TestDeleteTenantMembershipRefusesTheLastOwner(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember,
	})
	ownerRow := membershipID(ids["alice"], "cust_r")
	before := string(spy.memberships[ownerRow])

	if _, err := s.DeleteTenantMembership(ids["alice"], "cust_r", SystemActor()); !errors.Is(err, ErrLastOwner) {
		t.Fatalf("operator removing the last owner = %v, want ErrLastOwner", err)
	}
	if _, err := s.MembershipFor(ids["alice"], "cust_r"); err != nil {
		t.Fatalf("the last owner's grant was removed: %v", err)
	}
	if got := string(spy.memberships[ownerRow]); got != before {
		t.Fatalf("the durable owner row changed: %q → %q", before, got)
	}

	// With a second owner in place the same removal is allowed — the rule is
	// the floor, not the role.
	if _, _, err := s.SetTenantMembershipRole(ids["mem"], "cust_r", RoleOwner, SystemActor()); err != nil {
		t.Fatalf("promoting a second owner: %v", err)
	}
	removed, err := s.DeleteTenantMembership(ids["alice"], "cust_r", SystemActor())
	if err != nil {
		t.Fatalf("operator removing one of two owners = %v, want success", err)
	}
	if removed == nil || removed.Role != RoleOwner {
		t.Fatalf("removal reported %+v, want the owner grant it deleted", removed)
	}
	roster := s.MembershipsForTenant("cust_r")
	if len(roster) != 1 || roster[0].Role != RoleOwner {
		t.Fatalf("roster = %+v, want exactly the remaining owner", roster)
	}
}

// A refused durable delete puts the operator's removal back in both indexes,
// exactly as the human one does.
func TestDeleteTenantMembershipFailsClosed(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember,
	})
	spy.deleteErr = errors.New("connection refused")

	refused, err := s.DeleteTenantMembership(ids["mem"], "cust_r", SystemActor())
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("removal against a refusing backend = %v, want ErrPersistence", err)
	}
	if refused != nil {
		t.Fatalf("a rolled-back removal reported %+v removed", refused)
	}
	if _, err := s.MembershipFor(ids["mem"], "cust_r"); err != nil {
		t.Fatalf("the grant was not restored in the account index: %v", err)
	}
	if len(s.MembershipsForTenant("cust_r")) != 2 {
		t.Fatal("the grant was not restored in the tenant index")
	}
	reloaded := emptyStore()
	reloaded.applySnapshot(spy.snapshotOf(t, &Customer{ID: "cust_r", Token: "tok_cust_r", Plan: "pro"}))
	if _, err := reloaded.MembershipFor(ids["mem"], "cust_r"); err != nil {
		t.Fatalf("the grant did not survive the restart it was never removed from: %v", err)
	}
}

// The owner floor is a property of the whole member set, so the operator routes
// hold it under concurrency the same way the human removal does: two calls
// racing to take the last two owners away — one by demotion, one by removal —
// cannot both win, whichever order they land in.
func TestConcurrentOperatorOwnerChangesLeaveOneOwner(t *testing.T) {
	s, spy, ids := rosterStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "bob": RoleOwner,
	})

	var wg sync.WaitGroup
	errs := make([]error, 2)
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, _, errs[0] = s.SetTenantMembershipRole(ids["alice"], "cust_r", RoleAdmin, SystemActor())
	}()
	go func() {
		defer wg.Done()
		<-start
		_, errs[1] = s.DeleteTenantMembership(ids["bob"], "cust_r", SystemActor())
	}()
	close(start)
	wg.Wait()

	won, refused := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			won++
		case errors.Is(err, ErrLastOwner):
			refused++
		default:
			t.Fatalf("unexpected error from a concurrent operator change: %v", err)
		}
	}
	if won != 1 || refused != 1 {
		t.Fatalf("concurrent operator changes: %d succeeded, %d refused (%v)", won, refused, errs)
	}
	// Whichever landed first, the tenant still has an owner — that is the
	// invariant, not which call won.
	owners := 0
	for _, m := range s.MembershipsForTenant("cust_r") {
		if m.Role == RoleOwner {
			owners++
		}
	}
	if owners < 1 {
		t.Fatalf("the race left the tenant ownerless (%v)", errs)
	}
	reloaded := emptyStore()
	reloaded.applySnapshot(spy.snapshotOf(t, &Customer{ID: "cust_r", Token: "tok_cust_r", Plan: "pro"}))
	durable := 0
	for _, m := range reloaded.MembershipsForTenant("cust_r") {
		if m.Role == RoleOwner {
			durable++
		}
	}
	if durable != owners {
		t.Fatalf("durable owners = %d, in-memory owners = %d", durable, owners)
	}
}

// TestPostgresRosterGrantAndRemoval proves the durable half of the roster
// against a real Postgres: a grant made through the operator seam survives a
// restart, the roster read sees it, and a removal stays removed after a reload
// rather than coming back with the process. Gated on YSCALE_TEST_DATABASE_URL
// like the other Postgres integration tests; ids are fixed and derived, so
// reruns are idempotent.
func TestPostgresRosterGrantAndRemoval(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()

	s1, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	s1.AddCustomer(&Customer{ID: "cust_roster_pg", Token: "tok_roster_pg", Plan: "pro"})
	owner, err := s1.UpsertAccount("https://id.yscale.sh", "sub-roster-owner",
		AccountProfile{Email: "owner@acme.com", Name: "Owner"})
	if err != nil {
		t.Fatalf("UpsertAccount owner: %v", err)
	}
	member, err := s1.UpsertAccount("https://id.yscale.sh", "sub-roster-member",
		AccountProfile{Email: "member@acme.com", Name: "Member"})
	if err != nil {
		t.Fatalf("UpsertAccount member: %v", err)
	}
	if _, created, err := s1.GrantTenantMembership(owner.ID, "cust_roster_pg", RoleOwner, SystemActor()); err != nil || !created {
		t.Fatalf("grant owner = (%v, %v), want a created grant", created, err)
	}
	if _, created, err := s1.GrantTenantMembership(member.ID, "cust_roster_pg", RoleMember, SystemActor()); err != nil || !created {
		t.Fatalf("grant member = (%v, %v), want a created grant", created, err)
	}
	// Idempotent against the durable row, not just the in-memory index.
	if _, created, err := s1.GrantTenantMembership(member.ID, "cust_roster_pg", RoleMember, SystemActor()); err != nil || created {
		t.Fatalf("re-grant = (%v, %v), want an idempotent non-creation", created, err)
	}
	s1.Close()

	s2, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	roster, err := s2.TenantRosterFor("cust_roster_pg", owner.ID, RosterQuery{})
	if err != nil {
		t.Fatalf("roster after reload: %v", err)
	}
	if len(roster.Members) != 2 {
		t.Fatalf("roster after reload = %+v, want 2 members", roster.Members)
	}
	for _, m := range roster.Members {
		if m.Email == "" || m.CreatedAt.IsZero() {
			t.Errorf("row lost its profile or grant time across the reload: %+v", m)
		}
	}
	removed, err := s2.RemoveTenantMembership("cust_roster_pg", owner.ID, member.ID, SystemActor())
	if err != nil {
		t.Fatalf("RemoveTenantMembership: %v", err)
	}
	if removed == nil || removed.AccountID != member.ID {
		t.Fatalf("removal reported %+v, want the member's grant", removed)
	}
	if absent, err := s2.RemoveTenantMembership("cust_roster_pg", owner.ID, member.ID, SystemActor()); err != nil || absent != nil {
		t.Fatalf("re-removal = (%+v, %v), want an idempotent no-op", absent, err)
	}
	s2.Close()

	s3, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload after removal: %v", err)
	}
	defer s3.Close()
	roster, err = s3.TenantRosterFor("cust_roster_pg", owner.ID, RosterQuery{})
	if err != nil {
		t.Fatalf("roster after removal reload: %v", err)
	}
	if len(roster.Members) != 1 || roster.Members[0].AccountID != owner.ID {
		t.Fatalf("roster after removal reload = %+v, want only the owner", roster.Members)
	}
	// The removed human's own view agrees: the tenant is closed to them.
	if _, err := s3.TenantRosterFor("cust_roster_pg", member.ID, RosterQuery{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the removed member still reaches the roster: %v", err)
	}
	// Clean up so a reused test database doesn't accumulate tenants.
	s3.DeleteCustomer("cust_roster_pg")
}

// TestPostgresOperatorRoleUpdateAndRemoval proves the operator correction pair
// against a real Postgres. The role change matters most here: it is the only
// membership write that REWRITES a row rather than inserting or deleting one,
// so an upsert that silently inserted a second row, or one that did not update
// the stored document, would look identical in memory and only show up on the
// reload a restart does. Gated on YSCALE_TEST_DATABASE_URL like the other
// Postgres integration tests; ids are fixed and derived, so reruns are
// idempotent.
func TestPostgresOperatorRoleUpdateAndRemoval(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()

	s1, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	s1.AddCustomer(&Customer{ID: "cust_operator_pg", Token: "tok_operator_pg", Plan: "pro"})
	owner, err := s1.UpsertAccount("https://id.yscale.sh", "sub-operator-owner",
		AccountProfile{Email: "owner@acme.com", Name: "Owner"})
	if err != nil {
		t.Fatalf("UpsertAccount owner: %v", err)
	}
	member, err := s1.UpsertAccount("https://id.yscale.sh", "sub-operator-member",
		AccountProfile{Email: "member@acme.com", Name: "Member"})
	if err != nil {
		t.Fatalf("UpsertAccount member: %v", err)
	}
	for id, role := range map[string]string{owner.ID: RoleOwner, member.ID: RoleMember} {
		if _, created, err := s1.GrantTenantMembership(id, "cust_operator_pg", role, SystemActor()); err != nil || !created {
			t.Fatalf("grant %s = (%v, %v), want a created grant", role, created, err)
		}
	}
	granted, _, err := s1.SetTenantMembershipRole(member.ID, "cust_operator_pg", RoleAdmin, SystemActor())
	if err != nil {
		t.Fatalf("promote member to admin: %v", err)
	}
	// The last owner cannot be demoted here either, and the refusal must not
	// have written anything on its way out.
	if _, _, err := s1.SetTenantMembershipRole(owner.ID, "cust_operator_pg", RoleViewer, SystemActor()); !errors.Is(err, ErrLastOwner) {
		t.Fatalf("demoting the last owner = %v, want ErrLastOwner", err)
	}
	s1.Close()

	s2, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	roster, err := s2.TenantRosterFor("cust_operator_pg", owner.ID, RosterQuery{})
	if err != nil {
		t.Fatalf("roster after reload: %v", err)
	}
	if len(roster.Members) != 2 {
		t.Fatalf("roster after reload = %+v, want 2 members — the update inserted a row", roster.Members)
	}
	for _, m := range roster.Members {
		switch m.AccountID {
		case member.ID:
			if m.Role != RoleAdmin {
				t.Errorf("the role change did not survive the reload: %+v", m)
			}
			if !m.CreatedAt.Equal(granted.CreatedAt) {
				t.Errorf("the grant time moved across the update: %v → %v", granted.CreatedAt, m.CreatedAt)
			}
		case owner.ID:
			if m.Role != RoleOwner {
				t.Errorf("the refused demotion landed durably: %+v", m)
			}
		default:
			t.Errorf("unexpected member after reload: %+v", m)
		}
	}
	removed, err := s2.DeleteTenantMembership(member.ID, "cust_operator_pg", SystemActor())
	if err != nil {
		t.Fatalf("operator removal: %v", err)
	}
	if removed == nil || removed.Role != RoleAdmin {
		t.Fatalf("removal reported %+v, want the promoted grant", removed)
	}
	if absent, err := s2.DeleteTenantMembership(member.ID, "cust_operator_pg", SystemActor()); err != nil || absent != nil {
		t.Fatalf("re-removal = (%+v, %v), want an idempotent no-op", absent, err)
	}
	s2.Close()

	s3, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload after removal: %v", err)
	}
	defer s3.Close()
	roster, err = s3.TenantRosterFor("cust_operator_pg", owner.ID, RosterQuery{})
	if err != nil {
		t.Fatalf("roster after removal reload: %v", err)
	}
	if len(roster.Members) != 1 || roster.Members[0].AccountID != owner.ID {
		t.Fatalf("roster after removal reload = %+v, want only the owner", roster.Members)
	}
	// Clean up so a reused test database doesn't accumulate tenants.
	s3.DeleteCustomer("cust_operator_pg")
}
