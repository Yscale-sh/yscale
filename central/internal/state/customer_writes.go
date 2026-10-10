package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// A conflict carries a fresh, validated working copy, never an automatic replay
// of the caller's old authorization or mutation decision. Its error text must
// not include documents, credential verifiers or encrypted material.
type customerWriteConflict struct {
	current    *Customer
	tombstoned bool
}

func (*customerWriteConflict) Error() string { return "customer changed; retry from current state" }

func (e *customerWriteConflict) Unwrap() error {
	if e.tombstoned {
		return ErrCustomerTombstoned
	}
	return ErrPersistence
}

type customerWriteQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Existing copies use UPDATE only: a concurrent deletion must not turn a stale
// update into an insert. New copies may insert, but cannot replace an existing
// row without a loaded precondition. JSONB equality also covers unknown fields
// and avoids comparing re-encrypted mesh keys or noncanonical JSON encodings.
func (p *pgPersister) writeCustomer(ctx context.Context, q customerWriteQuery, c *Customer, data []byte) (string, error) {
	stmt := upsertCustomerStmt(tblCustomers, tblTombstones)
	var expected any
	if c.persistedData != "" {
		expected = c.persistedData
		stmt = fmt.Sprintf(`UPDATE %s SET data = CASE
			WHEN COALESCE(data->'RevokedAt', 'null'::jsonb) <> 'null'::jsonb
			THEN jsonb_set($2::jsonb, '{RevokedAt}', data->'RevokedAt')
			ELSE $2::jsonb END, updated_at = now()
			WHERE id = $1 AND data = $3::jsonb
			AND NOT EXISTS (SELECT 1 FROM %s WHERE id = $1)
			RETURNING data`, tblCustomers, tblTombstones)
	}
	var written string
	err := q.QueryRow(ctx, stmt, c.ID, string(data), expected).Scan(&written)
	if err == nil {
		return written, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("write customer %s: %w", c.ID, err)
	}
	return "", p.loadCustomerWriteConflict(ctx, q, c.ID)
}

func (p *pgPersister) loadCustomerWriteConflict(ctx context.Context, q customerWriteQuery, id string) error {
	var current []byte
	conflict := &customerWriteConflict{}
	if err := q.QueryRow(ctx, fmt.Sprintf(`SELECT
		(SELECT data FROM %s WHERE id = $1),
		EXISTS (SELECT 1 FROM %s WHERE id = $1)`, tblCustomers, tblTombstones), id).
		Scan(&current, &conflict.tombstoned); err != nil {
		return fmt.Errorf("refresh customer write conflict: %w", err)
	}
	if !conflict.tombstoned && len(current) != 0 {
		var loaded Customer
		if err := json.Unmarshal(current, &loaded); err != nil {
			return fmt.Errorf("decode current customer: %w", err)
		}
		if loaded.ID != id {
			return errors.New("current customer identity mismatch")
		}
		legacy, err := p.prepareLoadedCustomerCredential(&loaded)
		if err != nil {
			return err
		}
		if legacy {
			return errors.New("current customer credentials require startup migration")
		}
		for _, cluster := range loaded.RegisteredClusters {
			if cluster == nil || cluster.ClusterID == "" {
				return errors.New("current customer has an invalid cluster registry")
			}
		}
		for _, publisher := range loaded.CatalogPublishers {
			if publisher == nil || publisher.ID == "" {
				return errors.New("current customer has an invalid publisher registry")
			}
		}
		loaded.persistedData = string(current)
		conflict.current = &loaded
	}
	return conflict
}

// Callers hold custMu, but not mu. Successful persistence advances only the
// precondition; each setter still owns publishing its changed fields. Replacing
// its Customer pointer on success would invalidate the setter's auth indexing.
func (s *Store) acceptCustomerWrite(c *Customer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if live := s.customers[c.ID]; live != nil {
		live.persistedData = c.persistedData
	}
}

// A refused write refreshes the working set for the NEXT attempt. Do not use
// indexCustomerLocked here: its dev-seed merge would restore removed fields.
// Active tenants keep their memberships; revoked/missing ones cannot keep grants.
func (s *Store) refreshCustomerWrite(id string, err error) bool {
	var conflict *customerWriteConflict
	if !errors.As(err, &conflict) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old := s.customers[id]; old != nil {
		delete(s.customersByTok, old.TokenHash)
		s.unindexClustersLocked(old)
		s.unindexCatalogPublishersLocked(old)
	}
	delete(s.customers, id)
	if conflict.tombstoned {
		s.tombstoned[id] = true
	}
	if c := conflict.current; c != nil {
		s.customers[id] = c
		if !c.Revoked() && c.TokenHash != "" {
			s.customersByTok[c.TokenHash] = c
		}
		s.indexClustersLocked(c)
		s.indexCatalogPublishersLocked(c)
	}
	if conflict.current == nil || conflict.current.Revoked() {
		s.forgetMembershipsLocked(id)
	}
	return true
}
