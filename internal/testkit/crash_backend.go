package testkit

import (
	"context"
	"fmt"

	"github.com/yscale-sh/yscale/pkg/backends"
)

// CrashBackend decorates any backends.Backend — the deterministic FaultBackend
// or a live provider — with simulated process death immediately before and
// immediately after the two calls that can create or destroy billable provider
// state.
//
// It makes no lifecycle decision. It never derives a provider ID, never
// classifies create ambiguity, and never decides that retries are exhausted;
// those belong to the worker and to the durable store.
//
// The after-call crash is the interesting one: the wrapped backend has already
// run, so provider state is exactly what the provider left it as, and only the
// return value and error are withheld. That is precisely the ambiguity a
// newly constructed worker has to resolve by inventorying the provider.
type CrashBackend struct {
	inner backends.Backend
	crash *CrashController
}

var _ backends.Backend = (*CrashBackend)(nil)

func NewCrashBackend(inner backends.Backend, crash *CrashController) (*CrashBackend, error) {
	if inner == nil {
		return nil, fmt.Errorf("inner backend is required")
	}
	if crash == nil {
		return nil, fmt.Errorf("crash controller is required")
	}
	return &CrashBackend{inner: inner, crash: crash}, nil
}

// Unwrap returns the decorated backend so a test can inspect provider state
// through the provider's own seam rather than through this decorator.
func (b *CrashBackend) Unwrap() backends.Backend {
	return b.inner
}

// CreateNode crashes before the provider call (nothing was created) or after
// it (the resource exists, the identity is withheld).
func (b *CrashBackend) CreateNode(ctx context.Context, spec *backends.NodeSpec) (string, error) {
	if err := b.crash.Enter(BeforeProviderCreateCall); err != nil {
		return "", err
	}
	backendID, err := b.inner.CreateNode(ctx, spec)
	if crashErr := b.crash.Enter(AfterProviderCreateCall); crashErr != nil {
		// Withhold both the identity and the provider's own error. The caller
		// must not learn whether the create succeeded.
		return "", crashErr
	}
	return backendID, err
}

// DeleteNode crashes before the provider call (the resource still exists) or
// after it (the resource is gone, the receipt is withheld).
func (b *CrashBackend) DeleteNode(ctx context.Context, backendID string) error {
	if err := b.crash.Enter(BeforeProviderDeleteCall); err != nil {
		return err
	}
	err := b.inner.DeleteNode(ctx, backendID)
	if crashErr := b.crash.Enter(AfterProviderDeleteCall); crashErr != nil {
		return crashErr
	}
	return err
}

func (b *CrashBackend) StartNode(ctx context.Context, backendID string) error {
	return b.inner.StartNode(ctx, backendID)
}

func (b *CrashBackend) StopNode(ctx context.Context, backendID string) error {
	return b.inner.StopNode(ctx, backendID)
}

func (b *CrashBackend) GetNodeStatus(ctx context.Context, backendID string) (*backends.NodeStatus, error) {
	return b.inner.GetNodeStatus(ctx, backendID)
}

func (b *CrashBackend) ListPooledNodes(ctx context.Context) ([]backends.PooledNode, error) {
	return b.inner.ListPooledNodes(ctx)
}

func (b *CrashBackend) CleanupOrphans(ctx context.Context, trackedBackendIDs map[string]bool) (int, error) {
	return b.inner.CleanupOrphans(ctx, trackedBackendIDs)
}

func (b *CrashBackend) Name() string {
	return b.inner.Name()
}
