package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/factory/internal/store"
)

// blockingWorker stages the fabric like the Linode worker, then blocks the way
// a real create + boot + ops key handoff does (minutes, not seconds).
type blockingWorker struct {
	store   store.Store
	started chan context.Context
	release chan struct{}
	calls   atomic.Int32
}

func (b *blockingWorker) Provision(ctx context.Context, tenantID, idempotencyKey string) (store.Job, error) {
	b.calls.Add(1)
	job, _, err := b.store.EnsureFabric(tenantID, idempotencyKey)
	if err != nil {
		return store.Job{}, err
	}
	b.started <- ctx
	select {
	case <-b.release:
		return job, nil
	case <-ctx.Done():
		return job, ctx.Err()
	}
}

func (b *blockingWorker) Deprovision(context.Context, string, string) (store.Job, error) {
	return store.Job{}, nil
}

// Central gives each factory call a short budget. Provisioning must outlive
// that request instead of being cancelled after the billable box exists, and
// a retried POST must not start a second provision.
func TestProvisionOutlivesCallerRequest(t *testing.T) {
	saved := provisionSyncWait
	provisionSyncWait = 50 * time.Millisecond
	defer func() { provisionSyncWait = saved }()

	s, err := store.NewWithKEK(bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	worker := &blockingWorker{store: s, started: make(chan context.Context, 2), release: make(chan struct{})}
	h := NewHandler(s, worker, "test-token")

	post := func(ctx context.Context) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-a/fabric", nil).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer test-token")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	reqCtx, cancelRequest := context.WithCancel(context.Background())
	responded := make(chan *httptest.ResponseRecorder, 1)
	go func() { responded <- post(reqCtx) }()

	var provisionCtx context.Context
	select {
	case provisionCtx = <-worker.started:
	case <-time.After(5 * time.Second):
		t.Fatal("provision never started")
	}
	var rec *httptest.ResponseRecorder
	select {
	case rec = <-responded:
	case <-time.After(5 * time.Second):
		t.Fatal("POST blocked on the whole provision instead of answering 202")
	}
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d body=%s, want 202 while provisioning", rec.Code, rec.Body.String())
	}

	cancelRequest() // the caller's budget expires
	time.Sleep(20 * time.Millisecond)
	if err := provisionCtx.Err(); err != nil {
		t.Fatalf("provision context cancelled with the request: %v", err)
	}

	if again := post(context.Background()); again.Code != http.StatusAccepted {
		t.Fatalf("retried POST status = %d, want 202", again.Code)
	}
	if n := worker.calls.Load(); n != 1 {
		t.Fatalf("retried POST started %d provisions, want 1", n)
	}
	close(worker.release)
}
