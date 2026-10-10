package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const membershipLockClass int32 = 0x59534d42    // YSMB; distinct from audit locks
const accountIssuerLockClass int32 = 0x59534149 // YSAI

// Lock order: tenant membership set -> issuer registry (email grants only) ->
// account row -> audit. Creation, revoke and final/legacy deletion participate
// even when no customer or membership row yet exists. Hash collisions serialize
// unrelated tenants; they cannot weaken the invariant. Transaction locks expire
// on rollback/commit and every caller uses a bounded context.
func lockMembershipTenant(ctx context.Context, tx pgx.Tx, customer string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1,$2)`, membershipLockClass, auditLockID(customer))
	return err
}

func loadMembershipView(ctx context.Context, tx pgx.Tx, req membershipMutation) (*Store, error) {
	var id *string
	var revoked *string
	err := tx.QueryRow(ctx, `SELECT data->>'ID', data->>'RevokedAt' FROM customers
		WHERE id=$1 AND NOT EXISTS (SELECT 1 FROM customer_tombstones WHERE id=$1)
		FOR UPDATE`, req.customer).Scan(&id, &revoked)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && revoked != nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if id == nil || *id != req.customer {
		return nil, fmt.Errorf("%w: invalid membership customer", ErrPersistence)
	}
	view := newMembershipView(req.customer)
	rows, err := tx.Query(ctx, `SELECT m.id, m.data, a.data FROM tenant_memberships m
		LEFT JOIN accounts a ON a.id=m.data->>'AccountID'
		WHERE m.data->>'CustomerID'=$1 OR m.id=$2 OR m.id=$3`, req.customer,
		membershipID(req.caller, req.customer), membershipID(req.target, req.customer))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var data, principal []byte
		if err := rows.Scan(&key, &data, &principal); err != nil {
			return nil, err
		}
		var member TenantMembership
		if json.Unmarshal(data, &member) != nil || member.ID != key || member.CustomerID != req.customer ||
			membershipID(member.AccountID, member.CustomerID) != key || !ValidRole(member.Role) {
			return nil, fmt.Errorf("%w: invalid membership binding", ErrPersistence)
		}
		a, err := decodeAccountBinding(member.AccountID, principal)
		if err != nil {
			return nil, err
		}
		view.accounts[a.ID] = a
		view.indexMembership(&member)
	}
	return view, rows.Err()
}

func loadMembershipGrantTarget(ctx context.Context, tx pgx.Tx, view *Store, req membershipMutation) error {
	target := req.target
	if req.human {
		// All profile writes take the exclusive issuer lock. Holding its shared
		// form through commit excludes reassignment AND phantom duplicate emails.
		// Match in Go to preserve TrimSpace/EqualFold (including Unicode), rather
		// than silently changing the identity contract to a SQL collation.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1,$2)`, accountIssuerLockClass, auditLockID(req.issuer)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT id,data FROM accounts WHERE data->>'Issuer'=$1`, req.issuer)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			var data []byte
			if err := rows.Scan(&id, &data); err != nil {
				return err
			}
			a, err := decodeAccountBinding(id, data)
			if err != nil {
				return err
			}
			view.accounts[id] = a
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		a, err := view.verifiedAccountByEmailLocked(req.issuer, req.email)
		if err != nil {
			return err
		}
		target = a.ID
	}
	// Same row lock as first-workspace creation, so an existing-tenant grant
	// and a zero-membership workspace decision cannot both miss each other.
	var data []byte
	if err := tx.QueryRow(ctx, `SELECT data FROM accounts WHERE id=$1 FOR UPDATE`, target).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	a, err := decodeAccountBinding(target, data)
	if err != nil {
		return err
	}
	view.accounts[target] = a
	return nil
}

func (p *pgPersister) mutateMembership(req membershipMutation) (membershipMutationResult, error) {
	var result membershipMutationResult
	err := p.inTx("mutate membership", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockMembershipTenant(ctx, tx, req.customer); err != nil {
			return err
		}
		view, err := loadMembershipView(ctx, tx, req)
		if err != nil {
			return err
		}
		if err := req.authorize(view); err != nil {
			return err
		}
		if req.operation == membershipGrant {
			if err := loadMembershipGrantTarget(ctx, tx, view, req); err != nil {
				return err
			}
		}
		var event *AuditEvent
		result, event, err = applyMembershipMutation(view, req)
		if err != nil || event == nil {
			return err
		}
		var tag pgconn.CommandTag
		if req.operation == membershipRemove {
			tag, err = tx.Exec(ctx, `DELETE FROM tenant_memberships WHERE id=$1`, result.member.ID)
		} else {
			var data []byte
			data, err = json.Marshal(result.member)
			if err == nil {
				if req.operation == membershipGrant {
					tag, err = tx.Exec(ctx, `INSERT INTO tenant_memberships(id,data,updated_at) VALUES($1,$2,now())`, result.member.ID, data)
				} else {
					tag, err = tx.Exec(ctx, `UPDATE tenant_memberships SET data=$2,updated_at=now() WHERE id=$1`, result.member.ID, data)
				}
			}
		}
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: membership mutation did not affect exactly one row", ErrPersistence)
		}
		return insertAuditTx(ctx, tx, event)
	})
	if err != nil {
		return membershipMutationResult{}, err
	}
	return result, nil
}
