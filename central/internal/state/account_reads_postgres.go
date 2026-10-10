package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Only human-surface fields cross this SQL boundary. Never SELECT the customer
// document: it also contains provider, connector and mesh credentials.
const tenantSummarySQL = `jsonb_build_object(
	'ID', c.data->'ID', 'Name', c.data->'Name', 'Plan', c.data->'Plan',
	'MaxConcurrentBursts', c.data->'MaxConcurrentBursts', 'MaxHourlyUSD', c.data->'MaxHourlyUSD',
	'Revoked', c.data->>'RevokedAt' IS NOT NULL, 'WorkloadNamespaces', c.data->'WorkloadNamespaces',
	'ClusterPolicy', c.data->'ClusterPolicy', 'TemplateCatalog', c.data->'TemplateCatalog')`

func decodeTenantSummary(id string, data []byte) (TenantSummary, error) {
	var summary TenantSummary
	if json.Unmarshal(data, &summary) != nil || summary.ID != id || id == "" {
		return TenantSummary{}, fmt.Errorf("%w: invalid tenant summary binding", ErrPersistence)
	}
	return summary, nil
}

func decodeMembershipBinding(id string, data []byte, account, customer string) (*TenantMembership, error) {
	var member TenantMembership
	if json.Unmarshal(data, &member) != nil || member.ID != id || member.AccountID == "" || member.CustomerID == "" ||
		membershipID(member.AccountID, member.CustomerID) != id || !ValidRole(member.Role) ||
		(account != "" && member.AccountID != account) || (customer != "" && member.CustomerID != customer) {
		return nil, fmt.Errorf("%w: invalid membership projection binding", ErrPersistence)
	}
	return &member, nil
}

func (p *pgPersister) readAccountByID(ctx context.Context, id string) (*Account, error) {
	ctx, cancel := context.WithTimeout(ctx, humanAuthorizationReadTimeout)
	defer cancel()
	var data []byte
	if err := p.pool.QueryRow(ctx, `SELECT data FROM accounts WHERE id=$1`, id).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("%w: read account projection: %w", ErrPersistence, err)
	}
	return decodeAccountBinding(id, data)
}

func (p *pgPersister) readTenantSummary(ctx context.Context, id string) (TenantSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, humanAuthorizationReadTimeout)
	defer cancel()
	var data []byte
	err := p.pool.QueryRow(ctx, `SELECT `+tenantSummarySQL+` FROM customers c
		WHERE c.id=$1 AND NOT EXISTS (SELECT 1 FROM customer_tombstones t WHERE t.id=c.id)`, id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return TenantSummary{}, ErrNotFound
	}
	if err != nil {
		return TenantSummary{}, fmt.Errorf("%w: read tenant summary: %w", ErrPersistence, err)
	}
	return decodeTenantSummary(id, data)
}

func (p *pgPersister) readAccountTenants(ctx context.Context, account string) ([]AccountTenant, error) {
	ctx, cancel := context.WithTimeout(ctx, humanAuthorizationReadTimeout)
	defer cancel()
	// The account anchor distinguishes a new human with no grants from a
	// missing principal. Every grant, setting and liveness check shares this
	// one statement snapshot; no N+1 reads and no partial-success response.
	rows, err := p.pool.Query(ctx, `SELECT a.data, m.id, m.data, c.id,
		CASE WHEN c.id IS NULL THEN NULL ELSE `+tenantSummarySQL+` END
		FROM accounts a
		LEFT JOIN tenant_memberships m ON m.data->>'AccountID'=a.id
		LEFT JOIN customers c ON c.id=m.data->>'CustomerID' AND c.data->>'RevokedAt' IS NULL
			AND NOT EXISTS (SELECT 1 FROM customer_tombstones t WHERE t.id=c.id)
		WHERE a.id=$1 ORDER BY m.data->>'CustomerID' COLLATE "C"`, account)
	if err != nil {
		return nil, fmt.Errorf("%w: read account tenants: %w", ErrPersistence, err)
	}
	defer rows.Close()
	out := make([]AccountTenant, 0)
	found := false
	for rows.Next() {
		var principal, memberData, tenantData []byte
		var memberID, tenantID *string
		if err := rows.Scan(&principal, &memberID, &memberData, &tenantID, &tenantData); err != nil {
			return nil, fmt.Errorf("%w: scan account tenants: %w", ErrPersistence, err)
		}
		if _, err := decodeAccountBinding(account, principal); err != nil {
			return nil, err
		}
		found = true
		if memberID == nil {
			continue
		}
		member, err := decodeMembershipBinding(*memberID, memberData, account, "")
		if err != nil {
			return nil, err
		}
		// Missing, revoked and tombstoned tenants are not advertised as live.
		if tenantID == nil {
			continue
		}
		tenant, err := decodeTenantSummary(*tenantID, tenantData)
		if err != nil {
			return nil, err
		}
		if tenant.ID != member.CustomerID {
			return nil, fmt.Errorf("%w: inconsistent account tenant", ErrPersistence)
		}
		out = append(out, AccountTenant{Membership: *member, Tenant: tenant})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: read account tenants: %w", ErrPersistence, err)
	}
	if !found {
		return nil, ErrNotFound
	}
	return out, nil
}

type membershipProjection struct {
	member  *TenantMembership
	account *Account
}

type membershipQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// Caller supplies one total operation deadline. A nonzero limit bounds the SQL
// rows, not just the final JSON response. C collation matches Go byte ordering.
func readMembershipProjections(ctx context.Context, db membershipQueryer, account, customer, after string, limit int) ([]membershipProjection, error) {
	rows, err := db.Query(ctx, `SELECT m.id, m.data, a.data, c.id, c.data->>'ID'
		FROM tenant_memberships m
		JOIN customers c ON c.id=m.data->>'CustomerID' AND c.data->>'RevokedAt' IS NULL
			AND NOT EXISTS (SELECT 1 FROM customer_tombstones t WHERE t.id=c.id)
		LEFT JOIN accounts a ON a.id=m.data->>'AccountID'
		WHERE ($1<>'' OR $2<>'') AND ($1='' OR m.data->>'AccountID'=$1) AND ($2='' OR m.data->>'CustomerID'=$2)
		AND ($1='' OR a.id IS NOT NULL)
		AND m.data->>'AccountID' COLLATE "C" > $3
		ORDER BY (CASE WHEN $2='' THEN m.data->>'CustomerID' ELSE m.data->>'AccountID' END) COLLATE "C"
		LIMIT NULLIF($4,0)`, account, customer, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]membershipProjection, 0)
	for rows.Next() {
		var id, tenant string
		var binding *string
		var data, principal []byte
		if err := rows.Scan(&id, &data, &principal, &tenant, &binding); err != nil {
			return nil, err
		}
		member, err := decodeMembershipBinding(id, data, account, customer)
		if err != nil {
			return nil, err
		}
		if binding == nil || *binding != tenant || member.CustomerID != tenant {
			return nil, fmt.Errorf("%w: invalid roster tenant binding", ErrPersistence)
		}
		row := membershipProjection{member: member}
		// Preserve the existing missing-profile contract: a dangling grant is
		// still shown, without profile fields. A present but corrupt account
		// fails the complete response instead of masquerading as absent.
		if len(principal) != 0 {
			row.account, err = decodeAccountBinding(member.AccountID, principal)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (p *pgPersister) readMemberships(ctx context.Context, account, customer string) ([]*TenantMembership, error) {
	ctx, cancel := context.WithTimeout(ctx, humanAuthorizationReadTimeout)
	defer cancel()
	rows, err := readMembershipProjections(ctx, p.pool, account, customer, "", 0)
	if err != nil {
		return nil, fmt.Errorf("%w: read memberships: %w", ErrPersistence, err)
	}
	out := make([]*TenantMembership, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.member)
	}
	return out, nil
}

func (p *pgPersister) readTenantRoster(ctx context.Context, customer, caller string, q RosterQuery) (TenantRoster, error) {
	ctx, cancel := context.WithTimeout(ctx, humanAuthorizationReadTimeout)
	defer cancel()
	// Authorization and the bounded page must not observe different commits.
	// This read-only MVCC transaction takes no writer/advisory locks; a revoke
	// concurrent with it linearizes before or after the entire snapshot.
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return TenantRoster{}, fmt.Errorf("%w: begin roster read: %w", ErrPersistence, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	member, err := queryHumanMembership(ctx, tx, caller, customer)
	if err != nil {
		return TenantRoster{}, err
	}
	if member.Role != RoleOwner && member.Role != RoleAdmin {
		return TenantRoster{}, ErrNotAuthorized
	}
	limit := rosterLimit(q.Limit)
	rows, err := readMembershipProjections(ctx, tx, "", customer, q.After, limit+1)
	if err != nil {
		return TenantRoster{}, fmt.Errorf("%w: read roster: %w", ErrPersistence, err)
	}
	out := TenantRoster{Members: make([]TenantMember, 0, min(limit, len(rows)))}
	for i, row := range rows {
		if i == limit {
			out.NextAfter = out.Members[i-1].AccountID
			break
		}
		member := TenantMember{AccountID: row.member.AccountID, Role: row.member.Role, CreatedAt: row.member.CreatedAt}
		if row.account != nil {
			member.Email, member.Name = row.account.Email, row.account.Name
		}
		out.Members = append(out.Members, member)
	}
	if err := tx.Commit(ctx); err != nil {
		return TenantRoster{}, fmt.Errorf("%w: commit roster read: %w", ErrPersistence, err)
	}
	return out, nil
}
