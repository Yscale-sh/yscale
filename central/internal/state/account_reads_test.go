package state

import (
	"context"
	"errors"
	"testing"
)

func (p *accountSpyPersister) accountReadView(ctx context.Context) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.credentialMu.RLock()
	defer p.credentialMu.RUnlock()
	snap, err := p.loadAll(ctx)
	if err != nil {
		return nil, err
	}
	s := emptyStore()
	s.applySnapshot(snap)
	// Durable roster reads retain a dangling grant on a live tenant, even
	// though the boot mutation index intentionally excludes it.
	for _, m := range snap.Memberships {
		if c := s.customers[m.CustomerID]; c != nil && !c.Revoked() && !s.tombstoned[c.ID] {
			s.indexMembership(m)
		}
	}
	return s, nil
}

func (p *accountSpyPersister) readAccountByID(ctx context.Context, id string) (*Account, error) {
	s, err := p.accountReadView(ctx)
	if err != nil {
		return nil, err
	}
	return s.AccountByIDContext(ctx, id)
}

func (p *accountSpyPersister) readTenantSummary(ctx context.Context, id string) (TenantSummary, error) {
	s, err := p.accountReadView(ctx)
	if err != nil {
		return TenantSummary{}, err
	}
	return s.TenantSummaryByIDContext(ctx, id)
}

func (p *accountSpyPersister) readAccountTenants(ctx context.Context, id string) ([]AccountTenant, error) {
	s, err := p.accountReadView(ctx)
	if err != nil {
		return nil, err
	}
	return s.AccountTenantsContext(ctx, id)
}

func (p *accountSpyPersister) readTenantRoster(ctx context.Context, tenant, caller string, q RosterQuery) (TenantRoster, error) {
	s, err := p.accountReadView(ctx)
	if err != nil {
		return TenantRoster{}, err
	}
	return s.TenantRosterForContext(ctx, tenant, caller, q)
}

func (p *accountSpyPersister) readMemberships(ctx context.Context, account, customer string) ([]*TenantMembership, error) {
	s, err := p.accountReadView(ctx)
	if err != nil {
		return nil, err
	}
	return s.membershipsContext(ctx, account, customer)
}

func TestAccountReadsRequireDurableReader(t *testing.T) {
	s, _ := storeWithSpy("synthetic-account-reads")
	a, err := s.ResolveAccount("synthetic-issuer", "synthetic-subject")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddTenantMembership(a.ID, "synthetic-account-reads", RoleOwner); err != nil {
		t.Fatal(err)
	}
	s.persist = struct{ persister }{}
	for name, read := range accountProjectionReads(s, a.ID, "synthetic-account-reads") {
		t.Run(name, func(t *testing.T) {
			if err := read(context.Background()); !errors.Is(err, ErrPersistence) {
				t.Fatalf("cached projection fallback: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := read(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled projection: %v", err)
			}
		})
	}
	if s.MembershipsForAccount(a.ID) != nil || s.MembershipsForTenant("synthetic-account-reads") != nil {
		t.Fatal("legacy list fell back to cached grants")
	}
}

func accountProjectionReads(s *Store, account, tenant string) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"account":             func(ctx context.Context) error { _, err := s.AccountByIDContext(ctx, account); return err },
		"tenant":              func(ctx context.Context) error { _, err := s.TenantSummaryByIDContext(ctx, tenant); return err },
		"account-tenants":     func(ctx context.Context) error { _, err := s.AccountTenantsContext(ctx, account); return err },
		"account-memberships": func(ctx context.Context) error { _, err := s.MembershipsForAccountContext(ctx, account); return err },
		"tenant-memberships":  func(ctx context.Context) error { _, err := s.MembershipsForTenantContext(ctx, tenant); return err },
		"roster": func(ctx context.Context) error {
			_, err := s.TenantRosterForContext(ctx, tenant, account, RosterQuery{})
			return err
		},
	}
}
