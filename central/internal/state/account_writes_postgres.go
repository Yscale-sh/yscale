package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func readAccountForWrite(ctx context.Context, tx pgx.Tx, issuer, subject string, lock bool) (*Account, error) {
	id := accountID(issuer, subject)
	query := `SELECT data FROM accounts WHERE id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	var data []byte
	err := tx.QueryRow(ctx, query, id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	a, err := decodeAccountBinding(id, data)
	if err != nil {
		return nil, err
	}
	if a.Issuer != issuer || a.Subject != subject {
		return nil, fmt.Errorf("%w: account does not match requested identity", ErrPersistence)
	}
	return a, nil
}

func (p *pgPersister) resolveAccountIdentity(issuer, subject string, profile *AccountProfile) (*Account, error) {
	var result *Account
	err := p.inTx("resolve account identity", func(ctx context.Context, tx pgx.Tx) error {
		current, err := readAccountForWrite(ctx, tx, issuer, subject, false)
		if err != nil {
			return err
		}
		var changed bool
		result, changed = accountAfterRefresh(issuer, subject, current, profile, time.Now())
		if !changed {
			// An authoritative read is the no-op's linearization point. Polls
			// and existing-identity resolution need no exclusive issuer lock.
			return nil
		}
		// The same issuer lock used by email grants excludes reassignment and
		// phantom duplicates until this write commits. Acquire it before the
		// account row, matching the membership/first-workspace lock protocol.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1,$2)`, accountIssuerLockClass, auditLockID(issuer)); err != nil {
			return err
		}
		current, err = readAccountForWrite(ctx, tx, issuer, subject, true)
		if err != nil {
			return err
		}
		result, changed = accountAfterRefresh(issuer, subject, current, profile, time.Now())
		if !changed {
			return nil
		}
		if current == nil {
			data, err := json.Marshal(result)
			if err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `INSERT INTO accounts(id,data,updated_at) VALUES($1,$2,now())`, result.ID, data)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("%w: account insert did not create one row", ErrPersistence)
			}
			return nil
		}
		// Patch only profile fields and their update time. Preserve immutable
		// identity/creation fields and any other durable metadata not represented
		// by this binary. Empty profile values retain the existing omitempty
		// storage contract by removing the old keys before merging the patch.
		patch := map[string]any{"UpdatedAt": result.UpdatedAt}
		if result.Email != "" {
			patch["Email"] = result.Email
		}
		if result.EmailVerified {
			patch["EmailVerified"] = true
		}
		if result.Name != "" {
			patch["Name"] = result.Name
		}
		data, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE accounts SET
			data=(data-'Email'-'EmailVerified'-'Name') || $2::jsonb, updated_at=now()
			WHERE id=$1`, result.ID, data)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: account update did not affect one row", ErrPersistence)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
