package state

import (
	"encoding/json"
	"fmt"
	"time"
)

// Carry the operation, not a cached Account envelope. A nil profile resolves
// identity only; a non-nil profile is an explicit refresh from the issuer.
type accountWriter interface {
	resolveAccountIdentity(issuer, subject string, profile *AccountProfile) (*Account, error)
}

func (s *Store) resolveDurableAccountLocked(issuer, subject string, profile *AccountProfile) (*Account, error) {
	p, ok := s.persist.(accountWriter)
	if !ok {
		return nil, fmt.Errorf("%w: durable account writer unavailable", ErrPersistence)
	}
	a, err := p.resolveAccountIdentity(issuer, subject, profile)
	if err == nil && (a == nil || a.ID != accountID(issuer, subject) || a.Issuer != issuer || a.Subject != subject) {
		err = fmt.Errorf("%w: invalid resolved account binding", ErrPersistence)
	}
	if err != nil {
		return nil, s.recordPersistenceFailure("account", "resolve", fmt.Errorf("%w: %w", ErrPersistence, err))
	}
	// Publication follows commit. Resolve and true no-ops also refresh the
	// replica's account indexes, without sharing the returned mutation pointer.
	key := identityKey(issuer, subject)
	cached := s.accountsByIdentity[key]
	if cached == nil {
		cached = &Account{}
	}
	*cached = *a
	s.accounts[a.ID], s.accountsByIdentity[key] = cached, cached
	return a, nil
}

func decodeAccountBinding(id string, data []byte) (*Account, error) {
	var a Account
	if err := json.Unmarshal(data, &a); err != nil || a.ID != id {
		return nil, fmt.Errorf("%w: invalid account binding", ErrPersistence)
	}
	issuer, subject, err := normalizeIdentity(a.Issuer, a.Subject)
	if err != nil || a.Issuer != issuer || a.Subject != subject || accountID(issuer, subject) != id {
		return nil, fmt.Errorf("%w: invalid account identity", ErrPersistence)
	}
	return &a, nil
}

// AccountProfile is a complete issuer refresh, not a partial patch. Compare it
// with current durable state, never the replica's cache. Only a real change
// advances UpdatedAt; identity and CreatedAt remain the committed values.
func accountAfterRefresh(issuer, subject string, current *Account, profile *AccountProfile, now time.Time) (*Account, bool) {
	now = now.UTC()
	next := Account{ID: accountID(issuer, subject), Issuer: issuer, Subject: subject, CreatedAt: now, UpdatedAt: now}
	if current != nil {
		next = *current
		if profile == nil || (current.Email == profile.Email && current.EmailVerified == profile.EmailVerified && current.Name == profile.Name) {
			return &next, false
		}
		// A replica's clock may lag the last writer. Never regress the stored
		// observation timestamp or make a real update look unchanged.
		if !now.After(current.UpdatedAt) {
			now = current.UpdatedAt.Add(time.Nanosecond)
		}
		next.UpdatedAt = now
	}
	if profile != nil {
		next.Email, next.EmailVerified, next.Name = profile.Email, profile.EmailVerified, profile.Name
	}
	return &next, true
}
