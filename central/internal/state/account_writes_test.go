package state

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func (p *accountSpyPersister) resolveAccountIdentity(issuer, subject string, profile *AccountProfile) (*Account, error) {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	var current *Account
	if data, ok := p.accounts[accountID(issuer, subject)]; ok {
		var err error
		current, err = decodeAccountBinding(accountID(issuer, subject), data)
		if err != nil {
			return nil, err
		}
	}
	result, changed := accountAfterRefresh(issuer, subject, current, profile, time.Now())
	if !changed {
		return result, nil
	}
	if p.accountErr != nil {
		return nil, p.accountErr
	}
	data, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	p.accounts[result.ID] = data
	p.accountWrites++
	return result, nil
}

func TestAccountWritesRequireDurableCapability(t *testing.T) {
	s, _ := storeWithSpy()
	profile := AccountProfile{Email: "synthetic@invalid", EmailVerified: true}
	if _, err := s.UpsertAccount("synthetic-issuer", "synthetic-subject", profile); err != nil {
		t.Fatal(err)
	}
	s.persist = struct{ persister }{s.persist}
	if _, err := s.ResolveAccount("synthetic-issuer", "synthetic-subject"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("resolve used cached profile without durable capability: %v", err)
	}
	if _, err := s.UpsertAccount("synthetic-issuer", "synthetic-subject", profile); !errors.Is(err, ErrPersistence) {
		t.Fatalf("profile no-op used cache without durable capability: %v", err)
	}
}

func TestAccountAfterRefreshPreservesMetadataAndDetaches(t *testing.T) {
	stamp := time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC)
	current := &Account{ID: accountID("issuer", "subject"), Issuer: "issuer", Subject: "subject", Email: "old@invalid", CreatedAt: stamp, UpdatedAt: stamp.Add(time.Hour)}
	for _, profile := range []*AccountProfile{nil, {Email: "old@invalid"}} {
		got, changed := accountAfterRefresh("issuer", "subject", current, profile, stamp.Add(-time.Hour))
		if changed || got == current || *got != *current {
			t.Fatal("resolve/no-op changed or aliased stored metadata")
		}
	}
	got, changed := accountAfterRefresh("issuer", "subject", current, &AccountProfile{}, stamp.Add(-time.Hour))
	if !changed || got.Email != "" || !got.CreatedAt.Equal(current.CreatedAt) || !got.UpdatedAt.After(current.UpdatedAt) {
		t.Fatal("explicit empty refresh or backward-clock contract failed")
	}
	if current.Email != "old@invalid" {
		t.Fatal("candidate mutated source account")
	}
}

type accountResultPersister struct {
	persister
	result *Account
}

func (p accountResultPersister) resolveAccountIdentity(string, string, *AccountProfile) (*Account, error) {
	return p.result, nil
}

func TestAccountWritesRejectInvalidBackendResult(t *testing.T) {
	for _, result := range []*Account{nil, {ID: "wrong", Issuer: "issuer", Subject: "subject"},
		{ID: accountID("issuer", "subject"), Issuer: "different-issuer", Subject: "subject"},
		{ID: accountID("issuer", "subject"), Issuer: "issuer", Subject: "different-subject"}} {
		s := emptyStore()
		s.persist = accountResultPersister{result: result}
		if _, err := s.ResolveAccount("issuer", "subject"); !errors.Is(err, ErrPersistence) {
			t.Fatalf("invalid backend result accepted: %v", err)
		}
		if len(s.accounts) != 0 || len(s.accountsByIdentity) != 0 {
			t.Fatal("invalid backend result poisoned account indexes")
		}
	}
}
