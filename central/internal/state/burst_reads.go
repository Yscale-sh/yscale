package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type burstSnapshotReader interface {
	readBurstSnapshot(context.Context, string) (*Burst, error)
}

// BurstSnapshotContext reads one current booking for internal reconciliation.
// It is not an authorization grant. A configured backend is authoritative even
// when the local map has a record: absence and read failure never fall back to
// that map. The detached result must not be written back as a mutation.
func (s *Store) BurstSnapshotContext(ctx context.Context, id string) (*Burst, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, ErrNotFound
	}
	s.mu.RLock()
	if err := ctx.Err(); err != nil {
		s.mu.RUnlock()
		return nil, err
	}
	if p := s.persist; p != nil {
		s.mu.RUnlock()
		reader, ok := p.(burstSnapshotReader)
		if !ok {
			return nil, fmt.Errorf("%w: durable burst snapshot reader unavailable", ErrPersistence)
		}
		b, err := reader.readBurstSnapshot(ctx, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, ErrNotFound
			}
			return nil, fmt.Errorf("%w: read burst snapshot: %w", ErrPersistence, err)
		}
		if b == nil || b.ID != id || b.CustomerID == "" {
			return nil, fmt.Errorf("%w: invalid burst snapshot binding", ErrPersistence)
		}
		return cloneBurstSnapshot(b), nil
	}
	defer s.mu.RUnlock()
	b := s.bursts[id]
	if b == nil {
		return nil, ErrNotFound
	}
	if b.ID != id || b.CustomerID == "" {
		return nil, fmt.Errorf("%w: invalid burst snapshot binding", ErrPersistence)
	}
	return cloneBurstSnapshot(b), nil
}

func cloneBurstSnapshot(b *Burst) *Burst {
	cp := *b
	cp.Billing = cloneReadPointer(b.Billing)
	cp.TerminalCost = cloneReadPointer(b.TerminalCost)
	cp.LastHeartbeatAt = cloneReadPointer(b.LastHeartbeatAt)
	cp.NodePhaseAt = cloneReadPointer(b.NodePhaseAt)
	cp.OccupancyObservedAt = cloneReadPointer(b.OccupancyObservedAt)
	return &cp
}

func (p *pgPersister) readBurstSnapshot(ctx context.Context, id string) (*Burst, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var data []byte
	// A primary-key lookup, not a scan of every tenant's live bookings. An
	// unrelated malformed row must not prevent cleanup of this known resource.
	if err := p.pool.QueryRow(ctx, `SELECT data FROM bursts WHERE id=$1`, id).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var b *Burst
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("decode burst snapshot: %w", err)
	}
	if b == nil || b.ID != id || b.CustomerID == "" {
		return nil, errors.New("invalid durable burst binding")
	}
	return b, nil
}
