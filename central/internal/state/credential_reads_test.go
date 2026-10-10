package state

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Recording backends resolve the rows they actually committed, not a Store's
// cache. This preserves write-failure tests while exercising durable auth.
func (p *accountSpyPersister) readCredential(ctx context.Context, digest string, kind credentialKind) (*Customer, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	p.credentialMu.RLock()
	defer p.credentialMu.RUnlock()
	var candidates []*Customer
	for id, raw := range p.customers {
		if _, retired := p.tombstones[id]; retired {
			continue
		}
		var c Customer
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, "", err
		}
		candidates = append(candidates, &c)
	}
	return recordedCredential(candidates, digest, kind)
}

func (p *gatewayRouteSpyPersister) readCredential(ctx context.Context, digest string, kind credentialKind) (*Customer, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	p.credentialMu.RLock()
	defer p.credentialMu.RUnlock()
	seen := make(map[string]bool)
	var candidates []*Customer
	for i := len(p.customers) - 1; i >= 0; i-- {
		c := p.customers[i]
		if !seen[c.ID] {
			seen[c.ID] = true
			candidates = append(candidates, c)
		}
	}
	return recordedCredential(candidates, digest, kind)
}

func recordedCredential(candidates []*Customer, digest string, kind credentialKind) (*Customer, string, error) {
	var found *Customer
	var binding string
	for _, c := range candidates {
		for _, scope := range credentialBindings(c, digest, kind) {
			if found != nil {
				return nil, "", ErrNotFound
			}
			found, binding = c, scope
		}
	}
	if found == nil {
		return nil, "", ErrNotFound
	}
	return found, binding, nil
}

type credentialTestPersister struct {
	persister
	read func(context.Context, string, credentialKind) (*Customer, string, error)
}

func (p credentialTestPersister) readCredential(ctx context.Context, digest string, kind credentialKind) (*Customer, string, error) {
	return p.read(ctx, digest, kind)
}

func TestCatalogPublisherCredentialHashCompatibility(t *testing.T) {
	if got := HashCatalogPublisherCredential("abc"); got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatal("catalog publisher verifier format changed")
	}
}

func TestCredentialReadsUseDurableAuthority(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cached", Token: "test-token"})
	ctx := context.Background()
	for _, tc := range []struct {
		kind credentialKind
		hash string
		call func() error
	}{
		{tenantCredential, HashCustomerToken("test-token"), func() error { _, err := s.AuthCustomer("test-token"); return err }},
		{clusterCredential, HashClusterCredential("test-token"), func() error { _, _, err := s.AuthClusterCredential("test-token"); return err }},
		// The shared context resolver exists in both builds. The non-context
		// publisher entry point is deliberately disabled by the public overlay;
		// its private wrapper contract is tested in catalog_publishers_test.go.
		{publisherCredential, HashCatalogPublisherCredential("test-token"), func() error { _, _, err := s.AuthCatalogPublisherContext(ctx, "test-token"); return err }},
	} {
		called := false
		s.persist = credentialTestPersister{read: func(_ context.Context, digest string, kind credentialKind) (*Customer, string, error) {
			called = true
			if digest != tc.hash || digest == "test-token" || kind != tc.kind {
				t.Fatal("credential kind/verifier was not preserved")
			}
			return nil, "", ErrNotFound
		}}
		if err := tc.call(); !called || !errors.Is(err, ErrNotFound) {
			t.Fatalf("durable miss fell back to cache: called=%v err=%v", called, err)
		}
		wantErr := errors.New("read failed")
		s.persist = credentialTestPersister{read: func(context.Context, string, credentialKind) (*Customer, string, error) {
			return nil, "", wantErr
		}}
		if err := tc.call(); !errors.Is(err, wantErr) {
			t.Fatalf("durable error fell back: %v", err)
		}
	}
	s.persist = credentialTestPersister{read: func(context.Context, string, credentialKind) (*Customer, string, error) {
		return &Customer{ID: "durable"}, "", nil
	}}
	if c, err := s.AuthCustomerContext(ctx, "test-token"); err != nil || c.ID != "durable" {
		t.Fatalf("durable lookup: %v", err)
	}
	if s.customersByTok[HashCustomerToken("test-token")].ID != "cached" || s.customers["durable"] != nil {
		t.Fatal("authentication hydrated the mutation cache")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.AuthCustomerContext(cancelled, "test-token"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled lookup: %v", err)
	}
	s.persist = struct{ persister }{}
	if _, err := s.AuthCustomer("test-token"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("unsupported backend fell back: %v", err)
	}
	if _, err := s.AuthCustomer(""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty token: %v", err)
	}
}

func TestCredentialBindingsFailClosed(t *testing.T) {
	c := &Customer{ID: "tenant", TokenHash: "tenant-hash",
		RegisteredClusters: []*RegisteredCluster{nil, {ClusterID: "cluster", CredentialHash: "cluster-hash"}},
		CatalogPublishers:  []*CatalogPublisher{nil, {ID: "publisher", CredentialHash: "publisher-hash"}},
	}
	for _, tc := range []struct {
		kind            credentialKind
		digest, binding string
	}{
		{tenantCredential, "tenant-hash", ""}, {clusterCredential, "cluster-hash", "cluster"}, {publisherCredential, "publisher-hash", "publisher"},
	} {
		got := credentialBindings(c, tc.digest, tc.kind)
		if len(got) != 1 || got[0] != tc.binding {
			t.Fatal("wrong credential binding")
		}
		if len(credentialBindings(c, "wrong", tc.kind)) != 0 {
			t.Fatal("unknown credential matched")
		}
	}
	now := time.Now()
	c.RevokedAt = &now
	if len(credentialBindings(c, "cluster-hash", clusterCredential)) != 0 {
		t.Fatal("revoked tenant retained a credential")
	}
	c.RevokedAt = nil
	c.RegisteredClusters[1].ClusterID = ""
	if len(credentialBindings(c, "cluster-hash", clusterCredential)) != 0 {
		t.Fatal("empty scope authenticated")
	}
}
