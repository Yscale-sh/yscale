package state

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// The recording backend makes the same current-snapshot decision atomically,
// including no-op and audit rollback semantics. Its mutex stands in for the
// PostgreSQL locks; it does not use the calling Store's cached membership set.
func (p *accountSpyPersister) mutateMembership(req membershipMutation) (membershipMutationResult, error) {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	snap, err := p.loadAll(context.Background())
	if err != nil {
		return membershipMutationResult{}, err
	}
	view := emptyStore()
	view.applySnapshot(snap)
	result, event, err := applyMembershipMutation(view, req)
	if err != nil || event == nil {
		return result, err
	}
	if req.operation == membershipRemove {
		err = p.deleteErr
	} else {
		err = p.upsertErr
	}
	if err != nil {
		return membershipMutationResult{}, err
	}
	data, err := json.Marshal(result.member)
	if err != nil {
		return membershipMutationResult{}, err
	}
	if err := p.appendAudit(event); err != nil {
		return membershipMutationResult{}, err
	}
	if req.operation == membershipRemove {
		delete(p.memberships, result.member.ID)
	} else {
		p.memberships[result.member.ID] = data
	}
	return result, nil
}

func TestMembershipWritesRequireDurableCapability(t *testing.T) {
	s, _, ids := auditFixture(t, "synthetic-membership-capability", map[string]string{"owner": RoleOwner, "target": RoleMember})
	// Hide the optional writer without removing the underlying durable backend.
	s.persist = struct{ persister }{s.persist}
	for _, call := range []func() error{
		func() error {
			_, _, err := s.GrantTenantMembership(ids["target"], "synthetic-membership-capability", RoleMember, OperatorActor())
			return err
		},
		func() error {
			_, _, err := s.SetTenantMembershipRole(ids["target"], "synthetic-membership-capability", RoleMember, OperatorActor())
			return err
		},
		func() error {
			_, err := s.DeleteTenantMembership(ids["target"], "synthetic-membership-capability", OperatorActor())
			return err
		},
		func() error {
			_, _, err := s.AddTenantMemberByEmail("synthetic-membership-capability", ids["owner"], "https://id.yscale.sh", "target@acme.com", RoleMember, OperatorActor())
			return err
		},
		func() error {
			_, _, err := s.SetTenantMemberRole("synthetic-membership-capability", ids["owner"], ids["target"], RoleMember, OperatorActor())
			return err
		},
		func() error {
			_, err := s.RemoveTenantMembership("synthetic-membership-capability", ids["owner"], ids["target"], OperatorActor())
			return err
		},
	} {
		if err := call(); !errors.Is(err, ErrPersistence) {
			t.Fatalf("missing durable writer used cached authority: %v", err)
		}
	}
}
