// Package lifecycletest places crash checkpoints around the real
// lifecycle.Store API so durable convergence can be exercised across simulated
// process death.
//
// This package deliberately owns no lifecycle behaviour. It holds no operation
// state, defines no states or transitions, retries nothing, and never
// substitutes an in-memory model for the store. Every method forwards to the
// identical lifecycle.Store method and adds nothing but a before/after
// checkpoint pair, so a test that passes here passes against the production
// code path and not against a test double.
package lifecycletest

import (
	"context"
	"fmt"
	"time"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/internal/testkit"
)

// Store is a checkpointed view of *lifecycle.Store.
//
// A crash at an "after" checkpoint withholds the return value of a call that
// has already committed. That is the whole point: the caller loses the lease
// token, the admission response, or the delete receipt while the database
// keeps the committed row, which is exactly what a killed worker experiences.
type Store struct {
	inner *lifecycle.Store
	crash *testkit.CrashController
}

func NewStore(inner *lifecycle.Store, crash *testkit.CrashController) (*Store, error) {
	if inner == nil {
		return nil, fmt.Errorf("lifecycle store is required")
	}
	if crash == nil {
		return nil, fmt.Errorf("crash controller is required")
	}
	return &Store{inner: inner, crash: crash}, nil
}

// Inner returns the wrapped store. Read-only and operator methods
// (GetProviderDelete, ListProviderDeleteManualAttention, RetryProviderDelete,
// the outbox API) are reached through it directly and carry no checkpoints.
func (s *Store) Inner() *lifecycle.Store {
	return s.inner
}

// Crash returns the controller currently driving this wrapper.
func (s *Store) Crash() *testkit.CrashController {
	return s.crash
}

func (s *Store) AdmitWorkload(ctx context.Context, req lifecycle.AdmissionRequest) (lifecycle.AdmissionResponse, error) {
	if err := s.crash.Enter(testkit.BeforeAdmission); err != nil {
		return lifecycle.AdmissionResponse{}, err
	}
	resp, err := s.inner.AdmitWorkload(ctx, req)
	if err != nil {
		return resp, err
	}
	if crashErr := s.crash.Enter(testkit.AfterAdmission); crashErr != nil {
		return lifecycle.AdmissionResponse{}, crashErr
	}
	return resp, nil
}

func (s *Store) ClaimProviderCreate(ctx context.Context, lease time.Duration) (lifecycle.ProviderCreateOperation, bool, error) {
	if err := s.crash.Enter(testkit.BeforeProviderCreateClaim); err != nil {
		return lifecycle.ProviderCreateOperation{}, false, err
	}
	op, ok, err := s.inner.ClaimProviderCreate(ctx, lease)
	if err != nil || !ok {
		return op, ok, err
	}
	if crashErr := s.crash.Enter(testkit.AfterProviderCreateClaim); crashErr != nil {
		// The lease is committed and held in the database; the token is lost.
		// Expiry makes the dispatched create ambiguous; it must remain fenced
		// until provider reconciliation or an operator resolves it.
		return lifecycle.ProviderCreateOperation{}, false, crashErr
	}
	return op, ok, err
}

func (s *Store) MarkProviderCreateSucceeded(ctx context.Context, id int64, leaseToken, providerResourceID string) error {
	if err := s.crash.Enter(testkit.BeforeProviderCreateMark); err != nil {
		return err
	}
	err := s.inner.MarkProviderCreateSucceeded(ctx, id, leaseToken, providerResourceID)
	if err != nil {
		return err
	}
	if crashErr := s.crash.Enter(testkit.AfterProviderCreateMark); crashErr != nil {
		return crashErr
	}
	return nil
}

func (s *Store) MarkProviderCreateFailed(ctx context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error {
	if err := s.crash.Enter(testkit.BeforeProviderCreateMark); err != nil {
		return err
	}
	err := s.inner.MarkProviderCreateFailed(ctx, id, leaseToken, safeError, retryAt, maxAttempts)
	if err != nil {
		return err
	}
	if crashErr := s.crash.Enter(testkit.AfterProviderCreateMark); crashErr != nil {
		return crashErr
	}
	return nil
}

func (s *Store) RequestProviderDelete(ctx context.Context, req lifecycle.ProviderDeleteRequest) (lifecycle.ProviderDeleteResponse, error) {
	if err := s.crash.Enter(testkit.BeforeProviderDeleteRequest); err != nil {
		return lifecycle.ProviderDeleteResponse{}, err
	}
	resp, err := s.inner.RequestProviderDelete(ctx, req)
	if err != nil {
		return resp, err
	}
	if crashErr := s.crash.Enter(testkit.AfterProviderDeleteRequest); crashErr != nil {
		return lifecycle.ProviderDeleteResponse{}, crashErr
	}
	return resp, nil
}

func (s *Store) ClaimProviderDelete(ctx context.Context, lease time.Duration) (lifecycle.ProviderDeleteRecord, bool, error) {
	if err := s.crash.Enter(testkit.BeforeProviderDeleteClaim); err != nil {
		return lifecycle.ProviderDeleteRecord{}, false, err
	}
	record, ok, err := s.inner.ClaimProviderDelete(ctx, lease)
	if err != nil || !ok {
		return record, ok, err
	}
	if crashErr := s.crash.Enter(testkit.AfterProviderDeleteClaim); crashErr != nil {
		return lifecycle.ProviderDeleteRecord{}, false, crashErr
	}
	return record, ok, err
}

func (s *Store) MarkProviderDeleteSucceeded(ctx context.Context, id int64, leaseToken string) (time.Time, error) {
	if err := s.crash.Enter(testkit.BeforeProviderDeleteMark); err != nil {
		return time.Time{}, err
	}
	deletedAt, err := s.inner.MarkProviderDeleteSucceeded(ctx, id, leaseToken)
	if err != nil {
		return time.Time{}, err
	}
	if crashErr := s.crash.Enter(testkit.AfterProviderDeleteMark); crashErr != nil {
		return time.Time{}, crashErr
	}
	return deletedAt, nil
}

func (s *Store) MarkProviderDeleteFailed(ctx context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error {
	if err := s.crash.Enter(testkit.BeforeProviderDeleteMark); err != nil {
		return err
	}
	err := s.inner.MarkProviderDeleteFailed(ctx, id, leaseToken, safeError, retryAt, maxAttempts)
	if err != nil {
		return err
	}
	if crashErr := s.crash.Enter(testkit.AfterProviderDeleteMark); crashErr != nil {
		return crashErr
	}
	return nil
}
