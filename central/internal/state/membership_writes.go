package state

import (
	"errors"
	"fmt"
)

type membershipOperation uint8

const (
	membershipGrant membershipOperation = iota
	membershipRole
	membershipRemove
)

// Human authority is explicit, never inferred from a nonempty caller ID. An
// empty caller on a human request must not become an operator request.
type membershipMutation struct {
	operation                                     membershipOperation
	human                                         bool
	customer, caller, target, issuer, email, role string
	by                                            Actor
}

type membershipMutationResult struct {
	member   *TenantMembership
	created  bool
	previous string
}

type membershipMutator interface {
	mutateMembership(membershipMutation) (membershipMutationResult, error)
}

// Callers hold s.mu. Configured backends must decide and commit together;
// falling back to the cache would turn a missing capability into stale access.
func (s *Store) mutateMembershipLocked(req membershipMutation) (membershipMutationResult, error) {
	p, ok := s.persist.(membershipMutator)
	if !ok {
		return membershipMutationResult{}, fmt.Errorf("%w: durable membership writer unavailable", ErrPersistence)
	}
	result, err := p.mutateMembership(req)
	if err != nil {
		for _, domain := range []error{ErrNotFound, ErrNotAuthorized, ErrLastOwner, ErrRoleConflict, ErrInvalidRole, ErrInvalidIdentity} {
			if errors.Is(err, domain) {
				return membershipMutationResult{}, err
			}
		}
		return membershipMutationResult{}, s.recordPersistenceFailure("membership", "mutate", fmt.Errorf("%w: %w", ErrPersistence, err))
	}
	// Publish only a committed result, including the current state of a no-op.
	// The returned membership must not alias the pointer shared by both indexes.
	if req.operation == membershipRemove {
		s.unindexMembership(&TenantMembership{AccountID: req.target, CustomerID: req.customer})
	} else if result.member != nil {
		cached := *result.member
		s.indexMembership(&cached)
	}
	return result, nil
}

// A transaction builds this isolated, nonpersistent view from validated SQL
// rows. The existing in-memory methods remain the single permission, no-op,
// conflict and last-owner policy implementation, with no recursive SQL calls.
func newMembershipView(customer string) *Store {
	return &Store{
		customers:            map[string]*Customer{customer: {ID: customer}},
		accounts:             make(map[string]*Account),
		membershipsByAccount: make(map[string]map[string]*TenantMembership),
		membershipsByTenant:  make(map[string]map[string]*TenantMembership),
	}
}

// Check human authority before expanding the view with any email lookup.
// applyMembershipMutation repeats the check through the public policy path.
func (req membershipMutation) authorize(view *Store) error {
	if !req.human {
		return nil
	}
	caller, err := view.MembershipFor(req.caller, req.customer)
	if err != nil {
		return err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		return ErrNotAuthorized
	}
	if req.operation != membershipRemove && req.role == RoleOwner && caller.Role != RoleOwner {
		return ErrOwnerRestricted
	}
	return nil
}

func applyMembershipMutation(view *Store, req membershipMutation) (membershipMutationResult, *AuditEvent, error) {
	var result membershipMutationResult
	var err error
	var callerRole string
	if req.human {
		if caller := view.membershipsByTenant[req.customer][req.caller]; caller != nil {
			callerRole = caller.Role
		}
	}
	switch req.operation {
	case membershipGrant:
		if req.human {
			result.member, result.created, err = view.AddTenantMemberByEmail(req.customer, req.caller, req.issuer, req.email, req.role, req.by)
		} else {
			result.member, result.created, err = view.GrantTenantMembership(req.target, req.customer, req.role, req.by)
		}
	case membershipRole:
		if req.human {
			result.member, result.previous, err = view.SetTenantMemberRole(req.customer, req.caller, req.target, req.role, req.by)
		} else {
			result.member, result.previous, err = view.SetTenantMembershipRole(req.target, req.customer, req.role, req.by)
		}
	case membershipRemove:
		if req.human {
			result.member, err = view.RemoveTenantMembership(req.customer, req.caller, req.target, req.by)
		} else {
			result.member, err = view.DeleteTenantMembership(req.target, req.customer, req.by)
		}
	default:
		err = fmt.Errorf("%w: invalid membership operation", ErrPersistence)
	}
	if err != nil || result.member == nil {
		return result, nil, err
	}
	event := AuditEvent{CustomerID: req.customer, Actor: req.by, Outcome: OutcomeAccepted,
		TargetKind: TargetMembership, TargetID: result.member.AccountID}
	switch req.operation {
	case membershipGrant:
		if !result.created {
			return result, nil, nil
		}
		event.Action, event.Detail.Role = ActionMembershipGrant, result.member.Role
	case membershipRole:
		if result.previous == result.member.Role {
			return result, nil, nil
		}
		event.Action = ActionMembershipRoleChange
		event.Detail = AuditDetail{Role: result.member.Role, PreviousRole: result.previous}
	case membershipRemove:
		event.Action = ActionMembershipRemove
		event.Detail = AuditDetail{Role: callerRole, PreviousRole: result.member.Role}
	}
	return result, NewAuditEvent(event), nil
}
