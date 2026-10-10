package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// The self-service tenant id is a pure function of the account id: two
// replicas — or two racing requests — creating a first workspace for the same
// human must derive the same primary key, so the create's uniqueness check can
// collide them onto one tenant instead of buying two.
func TestSelfServiceTenantIDIsDeterministicPerAccount(t *testing.T) {
	id := SelfServiceTenantID("acct_one")
	if id != SelfServiceTenantID("acct_one") {
		t.Fatal("the same account derived two different tenant ids")
	}
	if !strings.HasPrefix(id, "cust_") {
		t.Fatalf("id = %q, want a cust_ prefix", id)
	}
	if id == SelfServiceTenantID("acct_two") {
		t.Fatal("two accounts derived the same tenant id")
	}
}

func TestFirstWorkspaceRequiresCurrentVerifiedOwner(t *testing.T) {
	for _, durable := range []bool{false, true} {
		for _, verified := range []bool{false, true} {
			t.Run(fmt.Sprintf("durable-%v-verified-%v", durable, verified), func(t *testing.T) {
				s := emptyStore()
				if durable {
					s.persist = newAccountSpy()
				}
				account, err := s.UpsertAccount("synthetic-workspace-issuer", "owner", AccountProfile{Email: "owner@synthetic.invalid", EmailVerified: verified})
				if err != nil {
					t.Fatal(err)
				}
				candidate := &Customer{ID: SelfServiceTenantID(account.ID), Token: "synthetic-workspace-key", Email: "caller-supplied@synthetic.invalid"}
				created, member, err := s.CreateFirstTenant(candidate, account.ID)
				if !verified {
					if !errors.Is(err, ErrAccountEmailUnverified) || errors.Is(err, ErrPersistence) || created != nil || member != nil {
						t.Fatalf("unverified workspace: %v", err)
					}
					if s.customers[candidate.ID] != nil || len(s.membershipsByAccount[account.ID]) != 0 || candidate.Email != "caller-supplied@synthetic.invalid" {
						t.Fatal("refused workspace changed state")
					}
					return
				}
				if err != nil || member == nil || member.Role != RoleOwner || created.Email != account.Email {
					t.Fatalf("verified workspace/contact: %+v, %v", created, err)
				}
			})
		}
	}
}

func TestFirstWorkspaceMissingOwnerAndOperatorContract(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable-%v", durable), func(t *testing.T) {
			s := emptyStore()
			if durable {
				s.persist = newAccountSpy()
			}
			for _, owner := range []string{"", "unknown-account"} {
				if _, _, err := s.CreateFirstTenant(&Customer{ID: "synthetic-first", Token: "synthetic-key"}, owner); !errors.Is(err, ErrNotFound) {
					t.Fatalf("absent owner: %v", err)
				}
			}
			account, err := s.ResolveAccount("synthetic-workspace-issuer", "operator-owner")
			if err != nil {
				t.Fatal(err)
			}
			// Operator provisioning deliberately allows an unverified owner and
			// its supplied contact; the self-service eligibility policy is not
			// imposed on tightly controlled pilot tenant provisioning.
			created, _, err := s.CreateTenant(&Customer{ID: "synthetic-operator", Token: "synthetic-operator-key", Email: "operator-contact@synthetic.invalid"}, account.ID)
			if err != nil || created.Email != "operator-contact@synthetic.invalid" {
				t.Fatalf("operator contract changed: %+v, %v", created, err)
			}
		})
	}
}

func TestFirstWorkspaceDoesNotInventAnEmailPresencePolicy(t *testing.T) {
	s := emptyStore()
	a, err := s.UpsertAccount("synthetic-workspace-issuer", "owner", AccountProfile{EmailVerified: true})
	if err != nil {
		t.Fatal(err)
	}
	if created, _, err := s.CreateFirstTenant(&Customer{ID: "synthetic-empty-contact", Token: "synthetic-key"}, a.ID); err != nil || created.Email != "" {
		t.Fatalf("verified boolean contract changed: %+v, %v", created, err)
	}
}

// Concurrent creates on the deterministic id admit exactly one tenant; every
// loser is a conflict, not a second workspace and not a backend fault.
func TestConcurrentSelfServiceCreatesYieldOneTenant(t *testing.T) {
	s, spy := storeWithSpy()
	acct, err := s.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{EmailVerified: true})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	id := SelfServiceTenantID(acct.ID)

	const attempts = 8
	results := make(chan error, attempts)
	var wg sync.WaitGroup
	for n := 0; n < attempts; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, _, err := s.CreateFirstTenant(&Customer{
				ID: id, Token: fmt.Sprintf("tok_attempt_%d", n), Plan: "trial",
				Name: "Acme", MaxConcurrentBursts: 1, MaxHourlyUSD: 1,
			}, acct.ID)
			results <- err
		}(n)
	}
	wg.Wait()
	close(results)

	var created, conflicts int
	for err := range results {
		switch {
		case err == nil:
			created++
		case errors.Is(err, ErrCustomerExists):
			conflicts++
		default:
			t.Fatalf("CreateTenant: %v", err)
		}
	}
	if created != 1 || conflicts != attempts-1 {
		t.Fatalf("created = %d, conflicts = %d, want exactly one winner", created, conflicts)
	}
	if len(spy.customers) != 1 {
		t.Fatalf("durable customer rows = %d, want 1", len(spy.customers))
	}
	memberships := s.MembershipsForAccount(acct.ID)
	if len(memberships) != 1 || memberships[0].CustomerID != id || memberships[0].Role != RoleOwner {
		t.Fatalf("memberships = %+v, want one owner grant on %s", memberships, id)
	}
}

// A replica that has not loaded another replica's new membership must still
// refuse a first-workspace create. The persister check is the authority here;
// the local map is only the fast-path refusal.
func TestCreateFirstTenantRefusesDurableMembershipMissingFromReplica(t *testing.T) {
	source, spy := storeWithSpy()
	acct, err := source.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{EmailVerified: true})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, _, err := source.CreateTenant(&Customer{ID: "cust_existing", Token: "tok_existing", Plan: "pro"}, acct.ID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	replica := emptyStore()
	replica.persist = spy
	replicaAcct, err := replica.UpsertAccount("https://id.yscale.sh", "sub-1", AccountProfile{EmailVerified: true})
	if err != nil {
		t.Fatalf("replica UpsertAccount: %v", err)
	}
	// This assertion is about the mutation cache, not the now-durable list API.
	if got := replica.membershipsByAccount[replicaAcct.ID]; len(got) != 0 {
		t.Fatalf("replica unexpectedly loaded memberships: %+v", got)
	}
	newID := SelfServiceTenantID(replicaAcct.ID)
	if _, _, err := replica.CreateFirstTenant(&Customer{ID: newID, Token: "tok_new", Plan: "trial"}, replicaAcct.ID); !errors.Is(err, ErrAccountHasTenant) {
		t.Fatalf("CreateFirstTenant = %v, want ErrAccountHasTenant", err)
	}
	if _, ok := spy.customers[newID]; ok {
		t.Fatalf("refused first-workspace create persisted customer %s", newID)
	}
}

// The display name is part of the tenant document: it lands in the durable row
// and travels on the summary the account surface renders — without either, a
// console could only show the workspace as its opaque id.
func TestTenantDisplayNameIsDurableAndSummarised(t *testing.T) {
	s, spy := storeWithSpy()
	if _, _, err := s.CreateTenant(&Customer{
		ID: "cust_named", Token: "tok_named", Plan: "trial", Name: "Acme Research",
	}, ""); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	summary, err := s.TenantSummaryByID("cust_named")
	if err != nil {
		t.Fatalf("TenantSummaryByID: %v", err)
	}
	if summary.Name != "Acme Research" {
		t.Fatalf("summary name = %q, want the stored display name", summary.Name)
	}
	var row Customer
	if err := json.Unmarshal(spy.customers["cust_named"], &row); err != nil {
		t.Fatalf("decode durable row: %v", err)
	}
	if row.Name != "Acme Research" {
		t.Fatalf("durable name = %q, want the stored display name", row.Name)
	}
}
