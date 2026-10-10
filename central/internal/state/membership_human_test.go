package state

import (
	"errors"
	"strings"
	"testing"
)

const humanIssuer = "https://id.yscale.sh"

// manageStore builds a tenant with the named (subject → role) members, plus a
// set of unrelated accounts nobody has been granted anything on yet, so a test
// can add one by email. Members and outsiders alike get a VERIFIED address,
// which is what the grant path requires.
func manageStore(t *testing.T, customerID string, roles map[string]string, outsiders ...string) (*Store, *accountSpyPersister, map[string]string) {
	t.Helper()
	s, spy := storeWithSpy(customerID)
	ids := make(map[string]string, len(roles)+len(outsiders))
	upsert := func(sub string) *Account {
		t.Helper()
		acct, err := s.UpsertAccount(humanIssuer, sub, AccountProfile{
			Email: sub + "@acme.com", EmailVerified: true, Name: strings.ToUpper(sub),
		})
		if err != nil {
			t.Fatalf("UpsertAccount %s: %v", sub, err)
		}
		ids[sub] = acct.ID
		return acct
	}
	for sub, role := range roles {
		acct := upsert(sub)
		if _, err := s.AddTenantMembership(acct.ID, customerID, role); err != nil {
			t.Fatalf("AddTenantMembership %s: %v", sub, err)
		}
	}
	for _, sub := range outsiders {
		upsert(sub)
	}
	return s, spy, ids
}

// An owner adds a colleague by address, and the grant is attributed to the
// human who asked — not to the system, and not to the person being added.
func TestAddTenantMemberByEmailGrantsAndAttributes(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner}, "newbie")

	m, created, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer,
		"newbie@acme.com", RoleMember, HumanActor(ids["alice"], "cust_r"))
	if err != nil || !created {
		t.Fatalf("owner adding a member = (%+v, %v, %v), want a created grant", m, created, err)
	}
	if m.AccountID != ids["newbie"] || m.Role != RoleMember || m.CustomerID != "cust_r" {
		t.Fatalf("granted membership = %+v", m)
	}
	if _, ok := spy.memberships[m.ID]; !ok {
		t.Fatal("the grant was not written durably")
	}
	grants := spy.eventsWith(ActionMembershipGrant)
	// One for the fixture's own owner grant (SystemActor), one for this call.
	if len(grants) != 2 {
		t.Fatalf("grant rows = %d, want 2", len(grants))
	}
	ev := grants[1]
	if ev.Actor.Kind != ActorHuman || ev.Actor.AccountID != ids["alice"] || ev.Actor.CustomerID != "cust_r" {
		t.Errorf("grant actor = %+v, want the calling owner as a human", ev.Actor)
	}
	if ev.TargetKind != TargetMembership || ev.TargetID != ids["newbie"] || ev.Detail.Role != RoleMember {
		t.Errorf("grant row = %+v", ev)
	}
	if ev.Outcome != OutcomeAccepted {
		t.Errorf("grant outcome = %q, want %q", ev.Outcome, OutcomeAccepted)
	}
}

// The address is matched the way a human types it, and only ever against an
// account of THIS issuer whose address the issuer confirmed.
func TestAddTenantMemberByEmailNormalizesAndRequiresVerified(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner}, "newbie")
	if _, err := s.UpsertAccount(humanIssuer, "unverified", AccountProfile{
		Email: "unverified@acme.com", EmailVerified: false,
	}); err != nil {
		t.Fatalf("UpsertAccount unverified: %v", err)
	}
	if _, err := s.UpsertAccount("https://id.other.example", "elsewhere", AccountProfile{
		Email: "elsewhere@acme.com", EmailVerified: true,
	}); err != nil {
		t.Fatalf("UpsertAccount elsewhere: %v", err)
	}

	m, created, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer,
		"  NewBie@Acme.com ", RoleViewer, HumanActor(ids["alice"], "cust_r"))
	if err != nil || !created || m.AccountID != ids["newbie"] {
		t.Fatalf("case- and space-shifted address = (%+v, %v, %v), want the same grant", m, created, err)
	}

	// The three misses answer identically: an unverified address is a claim, an
	// account of another issuer is another central's human, and an unknown one
	// is nobody. Telling them apart would make this route an address oracle.
	for _, miss := range []struct{ name, email string }{
		{"unknown", "nobody@acme.com"},
		{"unverified", "unverified@acme.com"},
		{"another issuer", "elsewhere@acme.com"},
	} {
		_, created, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer,
			miss.email, RoleMember, HumanActor(ids["alice"], "cust_r"))
		if !errors.Is(err, ErrTargetNotFound) || created {
			t.Errorf("%s address = (%v, %v), want ErrTargetNotFound and no grant", miss.name, created, err)
		}
	}
	if _, _, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer,
		"   ", RoleMember, HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrInvalidIdentity) {
		t.Errorf("blank address = %v, want ErrInvalidIdentity", err)
	}
}

// Two verified accounts on one address is a question the store must not answer
// for a manager: either choice hands a tenant to a human they did not mean.
func TestAddTenantMemberByEmailRefusesAnAmbiguousAddress(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	for _, sub := range []string{"twin-a", "twin-b"} {
		if _, err := s.UpsertAccount(humanIssuer, sub, AccountProfile{
			Email: "shared@acme.com", EmailVerified: true,
		}); err != nil {
			t.Fatalf("UpsertAccount %s: %v", sub, err)
		}
	}

	before := len(spy.memberships)
	_, created, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer,
		"shared@acme.com", RoleMember, HumanActor(ids["alice"], "cust_r"))
	if !errors.Is(err, ErrTargetNotFound) || created {
		t.Fatalf("ambiguous address = (%v, %v), want ErrTargetNotFound and no grant", created, err)
	}
	if len(spy.memberships) != before {
		t.Fatalf("an ambiguous address wrote %d membership rows", len(spy.memberships)-before)
	}
}

// Adding the same person with the same role again changes nothing and claims
// nothing; adding them with a different role is a conflict, because changing a
// role is the other route's job.
func TestAddTenantMemberByEmailIsIdempotent(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner}, "newbie")
	by := HumanActor(ids["alice"], "cust_r")

	first, created, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer, "newbie@acme.com", RoleAdmin, by)
	if err != nil || !created {
		t.Fatalf("first grant = (%v, %v)", created, err)
	}
	again, created, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer, "newbie@acme.com", RoleAdmin, by)
	if err != nil {
		t.Fatalf("identical re-add = %v, want idempotent success", err)
	}
	if created {
		t.Error("an idempotent re-add reported itself as the creator")
	}
	if again.ID != first.ID || !again.CreatedAt.Equal(first.CreatedAt) {
		t.Errorf("re-add returned a different record: %+v vs %+v", again, first)
	}
	if got := len(spy.eventsWith(ActionMembershipGrant)); got != 2 {
		t.Errorf("grant rows after a re-add = %d, want 2 (the fixture's and the first add)", got)
	}
	if _, _, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer,
		"newbie@acme.com", RoleViewer, by); !errors.Is(err, ErrRoleConflict) {
		t.Errorf("re-add with a different role = %v, want ErrRoleConflict", err)
	}
}

// Who may manage a roster, and what the owner role costs to hand out. An admin
// runs the roster; the owner seat is the one thing they cannot reach, or the
// ceiling they are under is one self-promotion away from gone.
func TestRosterMutationsRequireOwnerOrAdminAndProtectTheOwnerRole(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "adm": RoleAdmin, "mem": RoleMember, "view": RoleViewer,
	}, "newbie")

	// A member and a viewer hold a grant, so they are refused as unauthorized
	// rather than told the tenant does not exist.
	for _, sub := range []string{"mem", "view"} {
		by := HumanActor(ids[sub], "cust_r")
		if _, _, err := s.AddTenantMemberByEmail("cust_r", ids[sub], humanIssuer,
			"newbie@acme.com", RoleMember, by); !errors.Is(err, ErrNotAuthorized) {
			t.Errorf("%s adding a member = %v, want ErrNotAuthorized", sub, err)
		}
		if _, _, err := s.SetTenantMemberRole("cust_r", ids[sub], ids["view"], RoleMember, by); !errors.Is(err, ErrNotAuthorized) {
			t.Errorf("%s changing a role = %v, want ErrNotAuthorized", sub, err)
		}
	}
	// A non-member and an unknown tenant are the uniform ErrNotFound: nothing
	// about the tenant is disclosed to someone with no grant on it.
	stranger, err := s.UpsertAccount(humanIssuer, "stranger", AccountProfile{Email: "stranger@acme.com", EmailVerified: true})
	if err != nil {
		t.Fatalf("UpsertAccount stranger: %v", err)
	}
	if _, _, err := s.AddTenantMemberByEmail("cust_r", stranger.ID, humanIssuer, "newbie@acme.com",
		RoleMember, HumanActor(stranger.ID, "cust_r")); !errors.Is(err, ErrNotFound) {
		t.Errorf("a non-member adding a member = %v, want ErrNotFound", err)
	}
	if _, _, err := s.AddTenantMemberByEmail("cust_missing", ids["alice"], humanIssuer, "newbie@acme.com",
		RoleMember, HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrNotFound) {
		t.Errorf("an unknown tenant = %v, want ErrNotFound", err)
	}

	admin := HumanActor(ids["adm"], "cust_r")
	// The admin's own working range.
	if _, created, err := s.AddTenantMemberByEmail("cust_r", ids["adm"], humanIssuer,
		"newbie@acme.com", RoleMember, admin); err != nil || !created {
		t.Fatalf("admin adding a member = (%v, %v), want a created grant", created, err)
	}
	if m, previous, err := s.SetTenantMemberRole("cust_r", ids["adm"], ids["newbie"], RoleAdmin, admin); err != nil ||
		previous != RoleMember || m.Role != RoleAdmin {
		t.Fatalf("admin promoting a member to admin = (%+v, %q, %v)", m, previous, err)
	}
	// And its edges: minting an owner, promoting to owner, and touching one.
	if _, _, err := s.AddTenantMemberByEmail("cust_r", ids["adm"], humanIssuer,
		"stranger@acme.com", RoleOwner, admin); !errors.Is(err, ErrOwnerRestricted) {
		t.Errorf("admin adding an owner = %v, want ErrOwnerRestricted", err)
	}
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["adm"], ids["newbie"], RoleOwner, admin); !errors.Is(err, ErrOwnerRestricted) {
		t.Errorf("admin promoting to owner = %v, want ErrOwnerRestricted", err)
	}
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["adm"], ids["alice"], RoleViewer, admin); !errors.Is(err, ErrOwnerRestricted) {
		t.Errorf("admin demoting an owner = %v, want ErrOwnerRestricted", err)
	}
	if _, _, err := s.AddTenantMemberByEmail("cust_r", ids["adm"], humanIssuer,
		"alice@acme.com", RoleAdmin, admin); !errors.Is(err, ErrOwnerRestricted) {
		t.Errorf("admin re-adding the owner as an admin = %v, want ErrOwnerRestricted", err)
	}
	// ErrOwnerRestricted wraps ErrNotAuthorized, so a caller matching only the
	// broader error still refuses.
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["adm"], ids["alice"], RoleAdmin, admin); !errors.Is(err, ErrNotAuthorized) {
		t.Errorf("ErrOwnerRestricted no longer matches ErrNotAuthorized: %v", err)
	}
	if m, err := s.MembershipFor(ids["alice"], "cust_r"); err != nil || m.Role != RoleOwner {
		t.Fatalf("the owner's grant survived none of that: %+v %v", m, err)
	}
	// An owner may do what the admin could not.
	owner := HumanActor(ids["alice"], "cust_r")
	if m, previous, err := s.SetTenantMemberRole("cust_r", ids["alice"], ids["newbie"], RoleOwner, owner); err != nil ||
		previous != RoleAdmin || m.Role != RoleOwner {
		t.Fatalf("owner promoting to owner = (%+v, %q, %v)", m, previous, err)
	}
	for _, ev := range spy.eventsWith(ActionMembershipRoleChange) {
		if ev.Actor.Kind != ActorHuman || ev.Actor.AccountID == "" {
			t.Errorf("role change row is not attributed to a human: %+v", ev.Actor)
		}
	}
}

// A role change to the role already held writes nothing, and an account with no
// grant on this tenant has no role to change — which is not the idempotent
// success an already-absent REMOVAL gets.
func TestSetTenantMemberRoleIsIdempotentAndNeedsATarget(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember,
	}, "newbie")
	by := HumanActor(ids["alice"], "cust_r")

	m, previous, err := s.SetTenantMemberRole("cust_r", ids["alice"], ids["mem"], RoleMember, by)
	if err != nil || previous != RoleMember || m.Role != RoleMember {
		t.Fatalf("setting the role already held = (%+v, %q, %v)", m, previous, err)
	}
	if got := len(spy.eventsWith(ActionMembershipRoleChange)); got != 0 {
		t.Errorf("a no-op role change wrote %d journal rows, want 0", got)
	}
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["alice"], ids["newbie"], RoleAdmin, by); !errors.Is(err, ErrTargetNotFound) {
		t.Errorf("changing the role of a non-member = %v, want ErrTargetNotFound", err)
	}
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["alice"], "acct_nobody", RoleAdmin, by); !errors.Is(err, ErrNotFound) {
		t.Errorf("ErrTargetNotFound no longer matches ErrNotFound: %v", err)
	}
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["alice"], ids["mem"], "superuser", by); !errors.Is(err, ErrInvalidRole) {
		t.Errorf("an unrecognised role = %v, want ErrInvalidRole", err)
	}
	if _, _, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer,
		"newbie@acme.com", "superuser", by); !errors.Is(err, ErrInvalidRole) {
		t.Errorf("granting an unrecognised role = %v, want ErrInvalidRole", err)
	}
}

// The owner floor holds on the human path too: the last owner cannot demote
// themselves, because an ownerless tenant is unrecoverable through this surface
// — nothing left could grant the role back.
func TestSetTenantMemberRoleRefusesTheLastOwner(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner, "adm": RoleAdmin})
	by := HumanActor(ids["alice"], "cust_r")

	if _, _, err := s.SetTenantMemberRole("cust_r", ids["alice"], ids["alice"], RoleAdmin, by); !errors.Is(err, ErrLastOwner) {
		t.Fatalf("the last owner demoting themselves = %v, want ErrLastOwner", err)
	}
	if m, err := s.MembershipFor(ids["alice"], "cust_r"); err != nil || m.Role != RoleOwner {
		t.Fatalf("a refused demotion changed the grant: %+v %v", m, err)
	}
	if got := len(spy.eventsWith(ActionMembershipRoleChange)); got != 0 {
		t.Errorf("a refused demotion wrote %d journal rows, want 0", got)
	}
	// With a second owner in place it goes through, and the floor still holds
	// for whoever is left.
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["alice"], ids["adm"], RoleOwner, by); err != nil {
		t.Fatalf("promoting a second owner: %v", err)
	}
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["alice"], ids["alice"], RoleMember, by); err != nil {
		t.Fatalf("standing down with another owner in place: %v", err)
	}
	adminBy := HumanActor(ids["adm"], "cust_r")
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["adm"], ids["adm"], RoleViewer, adminBy); !errors.Is(err, ErrLastOwner) {
		t.Errorf("the remaining owner demoting themselves = %v, want ErrLastOwner", err)
	}
}

// A refused durable write leaves no grant, no role change and no journal row.
// The caller is told to retry; the store is exactly as it was.
func TestRosterMutationsFailClosed(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "mem": RoleMember,
	}, "newbie")
	by := HumanActor(ids["alice"], "cust_r")
	rows := len(spy.memberships)

	spy.upsertErr = errors.New("connection refused")
	if _, created, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer,
		"newbie@acme.com", RoleMember, by); !errors.Is(err, ErrPersistence) || created {
		t.Fatalf("grant against a refusing backend = (%v, %v), want ErrPersistence", created, err)
	}
	if _, err := s.MembershipFor(ids["newbie"], "cust_r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a refused grant left access behind: %v", err)
	}
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["alice"], ids["mem"], RoleAdmin, by); !errors.Is(err, ErrPersistence) {
		t.Fatalf("role change against a refusing backend = %v, want ErrPersistence", err)
	}
	if m, err := s.MembershipFor(ids["mem"], "cust_r"); err != nil || m.Role != RoleMember {
		t.Fatalf("a refused role change stuck in memory: %+v %v", m, err)
	}
	if len(spy.memberships) != rows {
		t.Errorf("durable rows = %d, want the %d the fixture wrote", len(spy.memberships), rows)
	}

	// The journal is the other half of the same write: a refused audit row takes
	// the change with it, so nothing is granted unrecorded.
	spy.upsertErr = nil
	spy.appendErr = errors.New("journal unavailable")
	if _, _, err := s.AddTenantMemberByEmail("cust_r", ids["alice"], humanIssuer,
		"newbie@acme.com", RoleMember, by); !errors.Is(err, ErrPersistence) {
		t.Fatalf("grant against a refusing journal = %v, want ErrPersistence", err)
	}
	if _, err := s.MembershipFor(ids["newbie"], "cust_r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a grant landed while the journal was refusing: %v", err)
	}
	if _, _, err := s.SetTenantMemberRole("cust_r", ids["alice"], ids["mem"], RoleAdmin, by); !errors.Is(err, ErrPersistence) {
		t.Fatalf("role change against a refusing journal = %v, want ErrPersistence", err)
	}
	if m, err := s.MembershipFor(ids["mem"], "cust_r"); err != nil || m.Role != RoleMember {
		t.Fatalf("a role change landed while the journal was refusing: %+v %v", m, err)
	}
}

// Usage is every member's to read and no outsider's, and the snapshot is taken
// under the lock that authorized it — the limits and the running bursts are one
// instant, and the records handed back are copies.
func TestTenantUsageForIsMemberScopedAndCopied(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "view": RoleViewer,
	})
	s.AddCustomer(&Customer{ID: "cust_other", Token: "tok_other", Plan: "pro"})
	for _, b := range []*Burst{
		{ID: "burst_1", CustomerID: "cust_r", HourlyUSD: 1.5},
		{ID: "burst_2", CustomerID: "cust_r", HourlyUSD: 2},
		{ID: "burst_other", CustomerID: "cust_other", HourlyUSD: 99},
	} {
		if err := s.PutBurst(b); err != nil {
			t.Fatalf("PutBurst %s: %v", b.ID, err)
		}
	}

	// A viewer reads it: usage is not a management surface.
	usage, err := s.TenantUsageFor("cust_r", ids["view"])
	if err != nil {
		t.Fatalf("viewer reading usage: %v", err)
	}
	if usage.Role != RoleViewer {
		t.Errorf("usage role = %q, want %q", usage.Role, RoleViewer)
	}
	if len(usage.Bursts) != 2 {
		t.Fatalf("bursts = %+v, want this tenant's two", usage.Bursts)
	}
	for _, b := range usage.Bursts {
		if b.CustomerID != "cust_r" {
			t.Fatalf("usage carried another tenant's burst: %+v", b)
		}
	}
	usage.Bursts[0].HourlyUSD = 1000
	if again, err := s.TenantUsageFor("cust_r", ids["alice"]); err != nil {
		t.Fatalf("owner reading usage: %v", err)
	} else {
		for _, b := range again.Bursts {
			if b.HourlyUSD == 1000 {
				t.Fatal("TenantUsageFor handed out the stored burst, not a copy")
			}
		}
	}

	// The three ways a caller has no business with a tenant are one answer here
	// too, so usage cannot be used to enumerate tenant ids.
	stranger, err := s.UpsertAccount(humanIssuer, "stranger", AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount stranger: %v", err)
	}
	for _, tc := range []struct{ name, tenant, account string }{
		{"unknown tenant", "cust_missing", ids["alice"]},
		{"not a member", "cust_other", ids["alice"]},
		{"no account at all", "cust_r", stranger.ID},
	} {
		if _, err := s.TenantUsageFor(tc.tenant, tc.account); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s = %v, want ErrNotFound", tc.name, err)
		}
	}
	if err := s.RevokeCustomer("cust_r"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if _, err := s.TenantUsageFor("cust_r", ids["alice"]); !errors.Is(err, ErrNotFound) {
		t.Errorf("usage of a revoked tenant = %v, want ErrNotFound", err)
	}
}
