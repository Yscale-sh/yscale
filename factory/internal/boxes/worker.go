// Package boxes contains the cloud-provisioning seam for factory lifecycle work.
package boxes

import (
	"context"
	"fmt"
	"sync"

	"github.com/yscale-sh/yscale/factory/internal/store"
)

// Worker provisions and deprovisions dedicated tenant fabrics. Production
// workers will perform cloud work; the API only depends on this seam.
type Worker interface {
	Provision(ctx context.Context, tenantID, idempotencyKey string) (store.Job, error)
	Deprovision(ctx context.Context, tenantID, loginServer string) (store.Job, error)
}

// Registrar exchanges the Headscale admin key that is generated only on the
// box. The registration token is minted before the box is created; the key is
// awaited only after its final login server is known.
type Registrar interface {
	// NewRegistration mints a single-use, short-TTL handoff token for this
	// tenant's box, to be embedded in the box's user_data. Pre-box-creation.
	NewRegistration(ctx context.Context, tenantID string) (token string, err error)
	// AwaitKey blocks until the box hands back its admin key via the token, or
	// ctx expires. It must verify the handoff login server and tenant match the
	// worker's provisioned values before returning the key.
	AwaitKey(ctx context.Context, token, expectedLoginServer, tenantID string) (apiKey string, err error)
}

type unavailableRegistrar struct{}

func (unavailableRegistrar) NewRegistration(context.Context, string) (string, error) {
	return "", unavailableRegistrarError()
}

func (unavailableRegistrar) AwaitKey(context.Context, string, string, string) (string, error) {
	return "", unavailableRegistrarError()
}

// FakeWorker is a local/test worker. It creates encrypted fake credentials and
// runs no cloud or network operations.
type FakeWorker struct {
	store store.Store

	mu            sync.Mutex
	provisioned   map[string]bool
	provisionHook func(context.Context, store.Box) error
}

// NewFakeWorker constructs a fake lifecycle worker backed by the supplied store.
func NewFakeWorker(s store.Store) *FakeWorker {
	return &FakeWorker{store: s, provisioned: make(map[string]bool)}
}

// SetProvisionHook controls when a provision completes. A nil hook advances a
// new box to ready immediately; a test can install a no-op hook, inspect the
// provisioning state, then call AdvanceProvision itself.
func (w *FakeWorker) SetProvisionHook(hook func(context.Context, store.Box) error) {
	w.mu.Lock()
	w.provisionHook = hook
	w.mu.Unlock()
}

func (w *FakeWorker) Provision(ctx context.Context, tenantID, idempotencyKey string) (store.Job, error) {
	job, box, err := w.store.EnsureFabric(tenantID, idempotencyKey)
	if err != nil {
		return store.Job{}, err
	}

	w.mu.Lock()
	if w.provisioned[job.ID] {
		w.mu.Unlock()
		return job, nil
	}
	w.provisioned[job.ID] = true
	hook := w.provisionHook
	w.mu.Unlock()

	box.Hostname = "fake-" + job.ID[:12]
	box.Backend = "fake"
	box.BackendID = "fake-" + job.ID
	box.HSUser = tenantID
	if err := w.store.SetBox(box, "fake-api-key-"+job.ID); err != nil {
		return store.Job{}, err
	}
	if hook != nil {
		if err := hook(ctx, box); err != nil {
			return store.Job{}, err
		}
		return job, nil
	}
	if err := w.store.SetBoxStatus(box.LoginServer, store.StatusReady); err != nil {
		return store.Job{}, err
	}
	return job, nil
}

// AdvanceProvision marks a fake fabric ready after a test-held provisioning window.
func (w *FakeWorker) AdvanceProvision(ctx context.Context, tenantID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	box, err := w.store.GetFabric(tenantID)
	if err != nil {
		return err
	}
	return w.store.SetBoxStatus(box.LoginServer, store.StatusReady)
}

func (w *FakeWorker) Deprovision(ctx context.Context, tenantID, loginServer string) (store.Job, error) {
	if err := ctx.Err(); err != nil {
		return store.Job{}, err
	}
	box, started, err := w.store.BeginDeprovision(tenantID, loginServer)
	if err != nil {
		return store.Job{}, err
	}
	if !started {
		// Box already decommissioning/dead — idempotent: a concurrent or
		// repeated DELETE enqueues no second teardown job.
		return store.Job{}, nil
	}
	job, err := w.store.EnqueueJob(store.Job{
		TenantID:    tenantID,
		LoginServer: loginServer,
		Kind:        "deprovision",
	})
	if err != nil {
		return store.Job{}, err
	}
	if err := w.store.SetBoxStatus(box.LoginServer, store.StatusDead); err != nil {
		return store.Job{}, fmt.Errorf("fake worker: mark box dead: %w", err)
	}
	return job, nil
}

var _ Worker = (*FakeWorker)(nil)
