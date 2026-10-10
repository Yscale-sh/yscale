package state

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// The two states a claim is ever in. There is no third: a submission that
// bailed out before it could reach a provider deletes its claim outright (see
// ReleaseIdempotent), because a key that was never spent must stay spendable.
const (
	IdempotencyInProgress = "in_progress"
	IdempotencyCompleted  = "completed"
)

// MaxIdempotencyResponseBytes bounds the stored response body. It is generous
// against what CreateWorkloadResponse actually renders — the point is that a
// scheduling error carrying a pathological upstream message cannot turn the
// claim table into a log sink, not to trim ordinary answers.
const MaxIdempotencyResponseBytes = 8 << 10

var (
	// ErrIdempotencyInvalid rejects a claim the store cannot key or replay.
	ErrIdempotencyInvalid = errors.New("invalid idempotency claim")
	// ErrIdempotencyRaced means the claim could not be resolved to a winner:
	// the row was inserted and removed again underneath this call. Fail-closed
	// on purpose — the caller must NOT provision on an unknown claim outcome.
	ErrIdempotencyRaced = errors.New("idempotency claim raced")
)

// IdempotencyClaim is the durable record of one keyed workload submission, and
// the thing that decides whether a retry provisions a second paid node or
// replays the first one's answer.
//
// It holds no credential and no request header. The raw Idempotency-Key never
// lands here: ID is a digest over (tenant, submitter, the cluster binding the
// submitter authenticated as, key) and KeyDigest is a digest of the key alone,
// so an operator reading the table can correlate a claim to a key they already
// hold without the table being able to hand anyone a key they do not. RequestHash is what binds the key to ONE request — a
// second submission under the same key with a different canonical spec is a
// client bug, and answering it with the first one's response would run the
// wrong workload under the right id.
type IdempotencyClaim struct {
	// ID is the scope digest and the primary key: one tenant's submitter using
	// a key another tenant's submitter also chose must not collide with them.
	ID         string
	CustomerID string
	// Actor is the principal the key is scoped to, stored so the scope is
	// readable rather than only computable.
	Actor Actor
	// KeyDigest is the digest of the key alone — scope-independent, so the same
	// key used under two identities is recognisable as the same key.
	KeyDigest string
	// RequestHash is the digest of the validated, namespace-pinned canonical
	// request this key is bound to.
	RequestHash string
	// WorkloadID is minted WITH the claim, before anything is provisioned, so a
	// retry that arrives while the owner is still running — or after it died —
	// has an identity to hand back and poll.
	WorkloadID string
	State      string
	// Status and Response are the terminal answer, set once when the submission
	// finishes. Response is the exact bytes the first caller was written, so a
	// replay is byte-identical rather than merely equivalent.
	Status      int
	Response    []byte
	CreatedAt   time.Time
	CompletedAt time.Time
}

func (c *IdempotencyClaim) clone() *IdempotencyClaim {
	if c == nil {
		return nil
	}
	out := *c
	if c.Response != nil {
		out.Response = append([]byte(nil), c.Response...)
	}
	return &out
}

// ClaimIdempotent atomically records claim as a new in-progress submission.
//
// won=true means this caller is the ONE owner and may go on to provision. It is
// the whole point of the call: of N concurrent retries of the same keyed
// request — across replicas, across a browser that gave up and reloaded —
// exactly one gets it, and the rest come back with the claim that already
// exists so they can replay it or wait on it.
//
// won=false always returns the existing claim, never nil. An error means the
// outcome is unknown, and a caller that provisions on one has defeated the
// contract: there is no safe default here, so the refusal has to travel.
//
// The atomicity lives in the database when there is one, exactly as ClaimBurst's
// does, because two centrals each consulting their own map is how one key buys
// two nodes. With no durable backend there is one process, so the store lock IS
// the atomicity — the self-hosted and test shape, race-safe on its own terms.
func (s *Store) ClaimIdempotent(ctx context.Context, claim *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	if claim == nil || claim.ID == "" || claim.WorkloadID == "" || claim.RequestHash == "" {
		return nil, false, fmt.Errorf("%w: id, workload id and request hash are required", ErrIdempotencyInvalid)
	}
	claim.State = IdempotencyInProgress
	claim.Status = 0
	claim.Response = nil
	claim.CompletedAt = time.Time{}
	if claim.CreatedAt.IsZero() {
		claim.CreatedAt = time.Now().UTC()
	}

	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return s.claimIdempotentInMemory(claim)
	}
	// Not holding s.mu across the round trip, for the reason ClaimBurst gives:
	// the decision is the database's, so the lock would only block every other
	// store reader behind a network call.
	existing, won, err := p.claimIdempotency(ctx, claim)
	if err != nil {
		s.recordPersistenceFailure("idempotency", "claim", err)
	}
	return existing, won, err
}

// claimIdempotentInMemory is the entire claim with no durable backend: one
// process, so the mutex decides it.
func (s *Store) claimIdempotentInMemory(claim *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.idempotency[claim.ID]; ok {
		return existing.clone(), false, nil
	}
	s.idempotency[claim.ID] = claim.clone()
	return nil, true, nil
}

// CompleteIdempotent stores the terminal status and response body of a claim
// this caller owns, so every later retry under the same key replays it instead
// of provisioning again.
//
// Callers must pass a context that is NOT the request's: the failure this whole
// path exists for is a client that timed out and retried, and recording the
// outcome on a cancelled context is exactly the case where the record would be
// lost and the retry would provision a second node.
func (s *Store) CompleteIdempotent(ctx context.Context, id string, status int, body []byte, at time.Time) error {
	if id == "" || status == 0 {
		return fmt.Errorf("%w: completing needs an id and a status", ErrIdempotencyInvalid)
	}
	if len(body) > MaxIdempotencyResponseBytes {
		return fmt.Errorf("%w: response is %d bytes, over the %d-byte bound",
			ErrIdempotencyInvalid, len(body), MaxIdempotencyResponseBytes)
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		claim, ok := s.idempotency[id]
		// Write-once, matching the durable statement's guard: two retries of one
		// submission must not be handed two different answers, and the second
		// writer here is a bug rather than a race to arbitrate.
		if !ok || claim.State != IdempotencyInProgress {
			return fmt.Errorf("%w: idempotency claim %s is gone or already terminal", ErrNotFound, id)
		}
		claim.State = IdempotencyCompleted
		claim.Status = status
		claim.Response = append([]byte(nil), body...)
		claim.CompletedAt = at
		return nil
	}
	return s.recordPersistenceFailure("idempotency", "complete", p.completeIdempotency(ctx, id, status, body, at))
}

// ReleaseIdempotent drops a claim whose submission bailed out BEFORE it could
// reach the provider, freeing the key for a real retry.
//
// It is only ever correct on a path that provably precedes provider creation. A
// claim released after an ambiguous provider call is a key that buys a second
// node; a claim left in place after a refusal that provisioned nothing is a
// tenant permanently unable to resubmit under the key their client picked. The
// callers pick the side, and only one of the two is recoverable.
//
// Idempotent: an already-absent claim is not an error.
func (s *Store) ReleaseIdempotent(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("%w: releasing needs an id", ErrIdempotencyInvalid)
	}
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		s.mu.Lock()
		delete(s.idempotency, id)
		s.mu.Unlock()
		return nil
	}
	return s.recordPersistenceFailure("idempotency", "delete", p.deleteIdempotency(ctx, id))
}
