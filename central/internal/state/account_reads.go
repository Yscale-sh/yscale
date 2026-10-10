package state

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// AccountTenant binds the current grant and the credential-free tenant view in
// one snapshot. Joining two independent store reads can resurrect an old role
// or combine it with settings from a different tenant revision.
type AccountTenant struct {
	Membership TenantMembership
	Tenant     TenantSummary
}

type accountProjectionReader interface {
	readAccountByID(context.Context, string) (*Account, error)
	readTenantSummary(context.Context, string) (TenantSummary, error)
	readAccountTenants(context.Context, string) ([]AccountTenant, error)
	readTenantRoster(context.Context, string, string, RosterQuery) (TenantRoster, error)
	readMemberships(context.Context, string, string) ([]*TenantMembership, error)
}

// projectionReader never holds a process lock across I/O or publishes a read
// into the mutation maps. A configured backend without this capability fails
// closed; nil is reserved for the intentionally in-memory Store.
func (s *Store) projectionReader(ctx context.Context) (accountProjectionReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return nil, nil
	}
	reader, ok := p.(accountProjectionReader)
	if !ok {
		return nil, fmt.Errorf("%w: durable account projection reader unavailable", ErrPersistence)
	}
	return reader, nil
}

func (s *Store) AccountByIDContext(ctx context.Context, id string) (*Account, error) {
	reader, err := s.projectionReader(ctx)
	if err != nil {
		return nil, err
	}
	if reader != nil {
		return reader.readAccountByID(ctx, id)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	a := s.accounts[id]
	if a == nil {
		return nil, ErrNotFound
	}
	copy := *a
	return &copy, nil
}

func (s *Store) TenantSummaryByIDContext(ctx context.Context, id string) (TenantSummary, error) {
	reader, err := s.projectionReader(ctx)
	if err != nil {
		return TenantSummary{}, err
	}
	if reader != nil {
		return reader.readTenantSummary(ctx, id)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.customers[id]
	if c == nil || s.tombstoned[id] {
		return TenantSummary{}, ErrNotFound
	}
	return tenantSummary(c), nil
}

func tenantSummary(c *Customer) TenantSummary {
	return TenantSummary{
		ID: c.ID, Name: c.Name, Plan: c.Plan,
		MaxConcurrentBursts: c.MaxConcurrentBursts, MaxHourlyUSD: c.MaxHourlyUSD,
		Revoked: c.Revoked(), WorkloadNamespaces: append([]string(nil), c.WorkloadNamespaces...),
		ClusterPolicy: copyClusterPolicy(c.ClusterPolicy), TemplateCatalog: copyTemplateCatalog(c.TemplateCatalog),
	}
}

func (s *Store) AccountTenantsContext(ctx context.Context, accountID string) ([]AccountTenant, error) {
	reader, err := s.projectionReader(ctx)
	if err != nil {
		return nil, err
	}
	if reader != nil {
		return reader.readAccountTenants(ctx, accountID)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.accounts[accountID] == nil {
		return nil, ErrNotFound
	}
	out := make([]AccountTenant, 0, len(s.membershipsByAccount[accountID]))
	for customer, member := range s.membershipsByAccount[accountID] {
		c := s.customers[customer]
		if c == nil || c.Revoked() || s.tombstoned[customer] {
			continue
		}
		out = append(out, AccountTenant{Membership: *member, Tenant: tenantSummary(c)})
	}
	slices.SortFunc(out, func(a, b AccountTenant) int { return strings.Compare(a.Tenant.ID, b.Tenant.ID) })
	return out, nil
}

func (s *Store) MembershipsForAccountContext(ctx context.Context, accountID string) ([]*TenantMembership, error) {
	return s.membershipsContext(ctx, accountID, "")
}

func (s *Store) MembershipsForTenantContext(ctx context.Context, customerID string) ([]*TenantMembership, error) {
	return s.membershipsContext(ctx, "", customerID)
}

func (s *Store) membershipsContext(ctx context.Context, accountID, customerID string) ([]*TenantMembership, error) {
	reader, err := s.projectionReader(ctx)
	if err != nil {
		return nil, err
	}
	if reader != nil {
		return reader.readMemberships(ctx, accountID, customerID)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := s.membershipsByAccount[accountID]
	if customerID != "" {
		rows = s.membershipsByTenant[customerID]
	}
	out := make([]*TenantMembership, 0, len(rows))
	for _, member := range rows {
		copy := *member
		out = append(out, &copy)
	}
	slices.SortFunc(out, func(a, b *TenantMembership) int {
		if customerID != "" {
			return strings.Compare(a.AccountID, b.AccountID)
		}
		return strings.Compare(a.CustomerID, b.CustomerID)
	})
	return out, nil
}

func (s *Store) TenantRosterForContext(ctx context.Context, customerID, callerAccountID string, q RosterQuery) (TenantRoster, error) {
	reader, err := s.projectionReader(ctx)
	if err != nil {
		return TenantRoster{}, err
	}
	q.Limit = rosterLimit(q.Limit)
	if reader != nil {
		return reader.readTenantRoster(ctx, customerID, callerAccountID, q)
	}
	return s.cachedTenantRosterFor(customerID, callerAccountID, q)
}

func rosterLimit(limit int) int {
	if limit <= 0 {
		return DefaultRosterLimit
	}
	return min(limit, MaxRosterLimit)
}
