package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
)

type refusingConnectorCommandLedger struct {
	*state.Store
}

func (refusingConnectorCommandLedger) PutConnectorCommands(context.Context, []state.ConnectorCommand) error {
	return errors.New("command ledger unavailable")
}

func (refusingConnectorCommandLedger) SubmitWorkloadWithConnectorCommands(context.Context, *state.Workload, *state.AuditEvent, []state.ConnectorCommand) error {
	return errors.New("command ledger unavailable")
}

// fakeDecider provisions a burst per Plan call, handing back a unique burst id so
// concurrent Creates don't collide. delay widens the admit→persist window so a
// racing Create must be rejected on the reservation, not the persisted burst.
type fakeDecider struct {
	mu        sync.Mutex
	calls     int
	delay     time.Duration
	err       error
	ambiguous bool
	nilQuote  bool
}

func (d *fakeDecider) Plan(_ context.Context, wl *workload.Workload, _ PlanOptions) (*Plan, error) {
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	d.mu.Lock()
	d.calls++
	id := fmt.Sprintf("burst_%d", d.calls)
	err := d.err
	d.mu.Unlock()
	if err != nil {
		return nil, err
	}
	p := &Plan{
		BurstID:   id,
		Backend:   "linode",
		BackendID: "vm-" + id,
		// No NodeName: these tests exercise admission/reservation, not node
		// draining. A NodeName would make reapBurst attempt a synchronous
		// drain_node that blocks on an ack the fake agent never sends (node
		// draining has its own coverage).
		HourlyUSD: 0.62,
	}
	// Mirrors the real decider: the declared budget rides the plan onto the
	// stored burst, which is the only route a deadline reaches the watchdog by.
	if wl.Spec.Budget != nil {
		p.Deadline = wl.Spec.Budget.Deadline
		p.MaxUSD = wl.Spec.Budget.MaxUSD
	}
	return p, nil
}

// Quote makes the shared create fake exercise the same side-effect-free price
// seam required by capped production tenants. PlanQuoted delegates to Plan so
// existing call-count and delay assertions still describe provider creates,
// not quote lookups.
func (d *fakeDecider) Quote(context.Context, *workload.Workload, PlanOptions) (*BurstQuote, error) {
	if d.nilQuote {
		return nil, nil
	}
	return &BurstQuote{Backend: "linode", SKU: "g6-standard-1", HourlyUSD: 0.62}, nil
}

func (d *fakeDecider) PlanQuoted(ctx context.Context, wl *workload.Workload, opts PlanOptions, _ *BurstQuote) (*Plan, error) {
	return d.Plan(ctx, wl, opts)
}

func (d *fakeDecider) CreateOutcomeAmbiguous(error) bool { return d.ambiguous }

func newCreateReq(cust *state.Customer) *http.Request {
	const body = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: create-test
spec:
  image: busybox
  size: small`
	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(body))
	return req.WithContext(context.WithValue(context.Background(), ctxCustomer, cust))
}

// TestCreate_ConcurrentAdmissionOneWins is the race regression for Bug 1: N
// simultaneous Creates for a tenant capped at one concurrent burst must yield
// EXACTLY one success. Before the fix, admission was check-then-act — every
// goroutine read spend below the cap before any burst persisted, so all N were
// admitted. Run under `go test -race`.
func TestCreate_ConcurrentAdmissionOneWins(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_race", Token: "t", MaxConcurrentBursts: 1}
	store.AddCustomer(cust)
	store.AddAgent(&state.Agent{
		ID: "agent_race", CustomerID: cust.ID, ClusterID: "cluster_race",
		Send: make(chan protocol.Envelope, 256), // ample: a spurious extra winner won't fill it and mask the bug
	})
	h := &Workloads{
		Store:   store,
		Decider: &fakeDecider{delay: 20 * time.Millisecond},
		Reaper:  &fakeReaper{},
		Log:     quietLog(),
	}

	const n = 8
	start := make(chan struct{})
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := httptest.NewRecorder()
			<-start // fire all goroutines together to maximize contention
			h.Create(rec, newCreateReq(cust))
			codes[i] = rec.Code // distinct index per goroutine → no shared write
		}(i)
	}
	close(start)
	wg.Wait()

	accepted, rejected := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusAccepted:
			accepted++
		case http.StatusTooManyRequests:
			rejected++
		default:
			t.Errorf("unexpected status %d", c)
		}
	}
	if accepted != 1 {
		t.Fatalf("exactly one Create must win a 1-burst cap; got %d accepted, %d rejected (of %d)", accepted, rejected, n)
	}
	if got := len(store.BurstsForCustomer(cust.ID)); got != 1 {
		t.Fatalf("store must hold exactly one burst; got %d", got)
	}
}

func TestCreate_RejectsCandidateRateBeforeProviderPlan(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_rate_guard", Token: "t", MaxConcurrentBursts: 5, MaxHourlyUSD: 0.50}
	store.AddCustomer(cust)
	store.AddAgent(&state.Agent{
		ID: "agent_rate_guard", CustomerID: cust.ID, ClusterID: "cluster_rate_guard",
		Send: make(chan protocol.Envelope, 4),
	})
	dec := &fakeDecider{}
	h := &Workloads{Store: store, Decider: dec, Reaper: &fakeReaper{}, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.Create(rec, newCreateReq(cust))

	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("over-cap candidate = %d, want 402: %s", rec.Code, rec.Body.String())
	}
	if got := dec.planCalls(); got != 0 {
		t.Fatalf("provider Plan calls = %d, want 0", got)
	}
	if got := len(store.BurstsForCustomer(cust.ID)); got != 0 {
		t.Fatalf("persisted bursts = %d, want 0", got)
	}
	if got := h.adm().reservationCount(); got != 0 {
		t.Fatalf("admission reservations = %d, want 0", got)
	}
}

func TestCreate_RejectsMissingQuoteBeforeProviderPlan(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_missing_quote", Token: "t", MaxHourlyUSD: 1}
	store.AddCustomer(cust)
	store.AddAgent(&state.Agent{
		ID: "agent_missing_quote", CustomerID: cust.ID, ClusterID: "cluster_missing_quote",
		Send: make(chan protocol.Envelope, 4),
	})
	dec := &fakeDecider{nilQuote: true}
	h := &Workloads{Store: store, Decider: dec, Reaper: &fakeReaper{}, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.Create(rec, newCreateReq(cust))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing quote = %d, want 503: %s", rec.Code, rec.Body.String())
	}
	if got := dec.planCalls(); got != 0 {
		t.Fatalf("provider Plan calls = %d, want 0", got)
	}
	if got := h.adm().reservationCount(); got != 0 {
		t.Fatalf("admission reservations = %d, want 0", got)
	}
}

func TestCreate_ReleasesReservationAfterProvenPlanFailure(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_plan_failure", Token: "t", MaxConcurrentBursts: 1}
	store.AddCustomer(cust)
	store.AddAgent(&state.Agent{
		ID: "agent_plan_failure", CustomerID: cust.ID, ClusterID: "cluster_plan_failure",
		Send: make(chan protocol.Envelope, 4),
	})
	dec := &fakeDecider{err: errors.New("provider refused create")}
	h := &Workloads{Store: store, Decider: dec, Reaper: &fakeReaper{}, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.Create(rec, newCreateReq(cust))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed Plan = %d, want 503: %s", rec.Code, rec.Body.String())
	}
	if got := dec.planCalls(); got != 1 {
		t.Fatalf("provider Plan calls = %d, want 1", got)
	}
	if got := h.adm().reservationCount(); got != 0 {
		t.Fatalf("reservation leaked after proven Plan failure: %d", got)
	}
}

// TestCreate_ReapsBurstOnDispatchFailure pins Bug 2: when a post-provision
// dispatch fails, Create must reap the just-created burst (no orphaned node
// billing forever) and return an error, not leave it live.
func TestCreate_ReapsBurstOnDispatchFailure(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_fail", Token: "t"}
	store.AddCustomer(cust)
	// Unbuffered Send with no reader → enqueue's non-blocking send fails, so
	// pushBurstAnnounce returns an error: the post-provision dispatch failure.
	store.AddAgent(&state.Agent{
		ID: "agent_fail", CustomerID: cust.ID, ClusterID: "cluster_fail",
		Send: make(chan protocol.Envelope),
	})
	reaper := &fakeReaper{}
	h := &Workloads{Store: store, Decider: &fakeDecider{}, Reaper: reaper, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.Create(rec, newCreateReq(cust))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("dispatch failure must return 503; got %d", rec.Code)
	}
	if got := len(reaper.teardowns); got != 1 {
		t.Fatalf("burst must be torn down exactly once on dispatch failure; got %d teardowns", got)
	}
	if got := len(store.ListBursts()); got != 0 {
		t.Fatalf("no burst may remain in the store after a reaped dispatch failure; got %d", got)
	}

	var resp CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "failed" {
		t.Errorf("response status = %q, want failed", resp.Status)
	}
	wl, err := store.GetWorkload(resp.ID)
	if err != nil || wl.Status != "failed" || wl.FinishedAt == nil {
		t.Errorf("workload should be marked failed, got %+v (err %v)", wl, err)
	}
	if n := h.adm().reservationCount(); n != 0 {
		t.Errorf("admission reservation leaked after dispatch failure: %d remain", n)
	}
}

func TestCreateRequiresDurablyPendingConnectorCommands(t *testing.T) {
	t.Run("durably pending is accepted despite the old queue capacity", func(t *testing.T) {
		store := state.New()
		cust := &state.Customer{ID: "cust_durable_pending", Token: "t"}
		store.AddCustomer(cust)
		store.AddAgent(&state.Agent{
			ID: "agent_durable_pending", CustomerID: cust.ID, ClusterID: "cluster_durable_pending",
			Send: make(chan protocol.Envelope),
		})
		h := &Workloads{Store: store, Commands: store, Decider: &fakeDecider{}, Reaper: &fakeReaper{}, Log: quietLog()}
		rec := httptest.NewRecorder()
		h.Create(rec, newCreateReq(cust))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("durably pending create = %d: %s", rec.Code, rec.Body.String())
		}
		var response CreateWorkloadResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		statuses, err := store.ListConnectorCommandsForWorkload(context.Background(), cust.ID, response.ID)
		if err != nil || len(statuses) != 2 {
			t.Fatalf("durable commands = (%+v,%v), want announce/create", statuses, err)
		}
		for _, status := range statuses {
			if status.State != state.ConnectorCommandPending {
				t.Fatalf("accepted command %s in state %s, want pending", status.ID, status.State)
			}
		}
		getReq := httptest.NewRequest(http.MethodGet, "/v1/workloads/"+response.ID, nil)
		getReq.SetPathValue("id", response.ID)
		getReq = getReq.WithContext(context.WithValue(getReq.Context(), ctxCustomer, cust))
		getRec := httptest.NewRecorder()
		h.Get(getRec, getReq)
		var detail map[string]any
		if err := json.Unmarshal(getRec.Body.Bytes(), &detail); err != nil {
			t.Fatal(err)
		}
		if exposed, ok := detail["connector_commands"].([]any); !ok || len(exposed) != 2 {
			t.Fatalf("workload detail connector_commands = %#v, want two safe statuses", detail["connector_commands"])
		}
	})

	t.Run("neither acknowledged nor pending fails closed", func(t *testing.T) {
		store := state.New()
		cust := &state.Customer{ID: "cust_durable_refused", Token: "t"}
		store.AddCustomer(cust)
		store.AddAgent(&state.Agent{
			ID: "agent_durable_refused", CustomerID: cust.ID, ClusterID: "cluster_durable_refused",
			Send: make(chan protocol.Envelope, 2),
		})
		reaper := &fakeReaper{}
		h := &Workloads{
			Store: store, Commands: refusingConnectorCommandLedger{Store: store},
			Decider: &fakeDecider{}, Reaper: reaper, Log: quietLog(),
		}
		rec := httptest.NewRecorder()
		h.Create(rec, newCreateReq(cust))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("refused ledger create = %d, want 503: %s", rec.Code, rec.Body.String())
		}
		if len(reaper.teardowns) != 1 || len(store.ListBursts()) != 0 {
			t.Fatalf("failed command persistence did not compensate: teardowns=%d bursts=%d", len(reaper.teardowns), len(store.ListBursts()))
		}
	})
}

// TestCreate_AdmitsUpToCapSequentially proves the reservation accounting doesn't
// over-reject: with the cap released as bursts reap, a tenant can keep launching.
func TestCreate_AdmitsUpToCapSequentially(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_seq", Token: "t", MaxConcurrentBursts: 1}
	store.AddCustomer(cust)
	store.AddAgent(&state.Agent{
		ID: "agent_seq", CustomerID: cust.ID, ClusterID: "cluster_seq",
		Send: make(chan protocol.Envelope, 256),
	})
	h := &Workloads{Store: store, Decider: &fakeDecider{}, Reaper: &fakeReaper{}, Log: quietLog()}

	// First Create is admitted.
	rec1 := httptest.NewRecorder()
	h.Create(rec1, newCreateReq(cust))
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("first Create should be admitted; got %d", rec1.Code)
	}
	// Second is rejected — tenant is at its 1-burst cap.
	rec2 := httptest.NewRecorder()
	h.Create(rec2, newCreateReq(cust))
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second Create should hit the cap; got %d", rec2.Code)
	}
	// Reap the running burst, freeing the slot; a third Create is admitted again.
	for _, b := range store.BurstsForCustomer(cust.ID) {
		h.reapBurst(context.Background(), b.ID, "test")
	}
	rec3 := httptest.NewRecorder()
	h.Create(rec3, newCreateReq(cust))
	if rec3.Code != http.StatusAccepted {
		t.Fatalf("third Create should be admitted after the slot freed; got %d", rec3.Code)
	}
}
