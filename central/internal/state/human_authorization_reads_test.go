package state

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func (p *accountSpyPersister) readAccountIdentity(ctx context.Context, issuer, subject string) (*Account, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.credentialMu.RLock()
	defer p.credentialMu.RUnlock()
	data, ok := p.accounts[accountID(issuer, subject)]
	if !ok {
		return nil, ErrNotFound
	}
	var account Account
	if err := json.Unmarshal(data, &account); err != nil {
		return nil, err
	}
	return &account, nil
}

func (p *accountSpyPersister) readHumanMembership(ctx context.Context, account, customer string) (*TenantMembership, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.credentialMu.RLock()
	defer p.credentialMu.RUnlock()
	if _, retired := p.tombstones[customer]; retired {
		return nil, ErrNotFound
	}
	var tenant Customer
	data := p.customers[customer]
	if len(data) == 0 || len(p.accounts[account]) == 0 {
		return nil, ErrNotFound
	}
	if err := json.Unmarshal(data, &tenant); err != nil {
		return nil, err
	}
	if tenant.Revoked() {
		return nil, ErrNotFound
	}
	data = p.memberships[membershipID(account, customer)]
	if len(data) == 0 {
		return nil, ErrNotFound
	}
	var member TenantMembership
	if err := json.Unmarshal(data, &member); err != nil {
		return nil, err
	}
	return &member, nil
}

func TestHumanAuthorizationRequiresDurableReader(t *testing.T) {
	s, _ := storeWithSpy("cust_human_reader")
	account, err := s.UpsertAccount("synthetic-issuer", "synthetic-subject", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddTenantMembership(account.ID, "cust_human_reader", RoleAdmin); err != nil {
		t.Fatal(err)
	}
	s.persist = struct{ persister }{}
	if _, err := s.AccountByIdentity("synthetic-issuer", "synthetic-subject"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("missing durable account reader fell back to cache: %v", err)
	}
	if _, err := s.MembershipFor(account.ID, "cust_human_reader"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("missing durable membership reader fell back to cache: %v", err)
	}
	if _, err := s.TenantRosterFor("cust_human_reader", account.ID, RosterQuery{}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("locked authorization fell back to cache: %v", err)
	}
}

func TestHumanAuthorizationReadsDoNotHydrateMutationMaps(t *testing.T) {
	s, p := storeWithSpy("cust_human_reader")
	account, err := s.UpsertAccount("synthetic-issuer", "synthetic-subject", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	member, err := s.AddTenantMembership(account.ID, "cust_human_reader", RoleAdmin)
	if err != nil {
		t.Fatal(err)
	}
	member.Role = RoleViewer
	if err := p.upsertMembership(member, nil); err != nil {
		t.Fatal(err)
	}
	current, err := s.MembershipFor(account.ID, "cust_human_reader")
	if err != nil || current.Role != RoleViewer {
		t.Fatalf("durable role was not observed: %v", err)
	}
	if _, err := s.TenantRosterFor("cust_human_reader", account.ID, RosterQuery{}); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("cached administrator retained roster access: %v", err)
	}
	if cached := s.membershipsByAccount[account.ID]["cust_human_reader"]; cached.Role != RoleAdmin {
		t.Fatal("authorization read hydrated the mutation map")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.MembershipForContext(ctx, account.ID, "cust_human_reader"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled membership read = %v", err)
	}
	if _, err := s.AccountByIdentityContext(ctx, account.Issuer, account.Subject); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled account read = %v", err)
	}
}
