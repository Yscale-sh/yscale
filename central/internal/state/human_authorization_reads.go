package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const humanAuthorizationReadTimeout = 5 * time.Second

// Human authorization reads never hydrate the replica's mutation maps. A
// configured durable backend must implement both reads; failure cannot fall
// back to an old account or membership left in this process.
type humanAuthorizationReader interface {
	readAccountIdentity(context.Context, string, string) (*Account, error)
	readHumanMembership(context.Context, string, string) (*TenantMembership, error)
}

func (s *Store) AccountByIdentityContext(ctx context.Context, issuer, subject string) (*Account, error) {
	issuer, subject, err := normalizeIdentity(issuer, subject)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	if p := s.persist; p != nil {
		s.mu.RUnlock()
		reader, ok := p.(humanAuthorizationReader)
		if !ok {
			return nil, fmt.Errorf("%w: durable human authorization reader unavailable", ErrPersistence)
		}
		return reader.readAccountIdentity(ctx, issuer, subject)
	}
	defer s.mu.RUnlock()
	a := s.accountsByIdentity[identityKey(issuer, subject)]
	if a == nil {
		return nil, ErrNotFound
	}
	copied := *a
	return &copied, nil
}

func (s *Store) MembershipForContext(ctx context.Context, accountID, customerID string) (*TenantMembership, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	if p := s.persist; p != nil {
		s.mu.RUnlock()
		return readHumanMembership(ctx, p, accountID, customerID)
	}
	defer s.mu.RUnlock()
	m, err := s.cachedMembershipForLocked(accountID, customerID)
	if err != nil {
		return nil, err
	}
	copied := *m
	return &copied, nil
}

func readHumanMembership(ctx context.Context, p persister, accountID, customerID string) (*TenantMembership, error) {
	reader, ok := p.(humanAuthorizationReader)
	if !ok {
		return nil, fmt.Errorf("%w: durable human authorization reader unavailable", ErrPersistence)
	}
	return reader.readHumanMembership(ctx, accountID, customerID)
}

func (p *pgPersister) readAccountIdentity(ctx context.Context, issuer, subject string) (*Account, error) {
	ctx, cancel := context.WithTimeout(ctx, humanAuthorizationReadTimeout)
	defer cancel()
	var data []byte
	id := accountID(issuer, subject)
	if err := p.pool.QueryRow(ctx, `SELECT data FROM accounts WHERE id=$1`, id).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("%w: read human account: %w", ErrPersistence, err)
	}
	var account Account
	if err := json.Unmarshal(data, &account); err != nil || account.ID != id ||
		account.Issuer != issuer || account.Subject != subject {
		return nil, fmt.Errorf("%w: invalid human account binding", ErrPersistence)
	}
	return &account, nil
}

func (p *pgPersister) readHumanMembership(ctx context.Context, account, customer string) (*TenantMembership, error) {
	ctx, cancel := context.WithTimeout(ctx, humanAuthorizationReadTimeout)
	defer cancel()
	return queryHumanMembership(ctx, p.pool, account, customer)
}

type humanMembershipQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func queryHumanMembership(ctx context.Context, db humanMembershipQueryer, account, customer string) (*TenantMembership, error) {
	var memberData, accountData []byte
	var customerID string
	id := membershipID(account, customer)
	// Membership, account binding, tenant liveness and tombstone exclusion
	// share one statement snapshot. Credentials never leave the customer row.
	err := db.QueryRow(ctx, `
		SELECT m.data, a.data, c.data->>'ID'
		FROM tenant_memberships m
		JOIN accounts a ON a.id=$2
		JOIN customers c ON c.id=$3
		WHERE m.id=$1 AND c.data->>'RevokedAt' IS NULL
		AND NOT EXISTS (SELECT 1 FROM customer_tombstones t WHERE t.id=c.id)`, id, account, customer).
		Scan(&memberData, &accountData, &customerID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("%w: read human membership: %w", ErrPersistence, err)
	}
	var member TenantMembership
	var principal Account
	if json.Unmarshal(memberData, &member) != nil || json.Unmarshal(accountData, &principal) != nil ||
		member.ID != id || member.AccountID != account || member.CustomerID != customer || !ValidRole(member.Role) ||
		principal.ID != account || customerID != customer {
		return nil, fmt.Errorf("%w: invalid human membership binding", ErrPersistence)
	}
	issuer, subject, err := normalizeIdentity(principal.Issuer, principal.Subject)
	if err != nil || issuer != principal.Issuer || subject != principal.Subject || accountID(issuer, subject) != account {
		return nil, fmt.Errorf("%w: invalid membership account identity", ErrPersistence)
	}
	return &member, nil
}
