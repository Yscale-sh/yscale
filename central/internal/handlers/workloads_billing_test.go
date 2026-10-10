package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
)

type orderedBilling struct {
	events     *[]string
	reserveErr error
	releaseErr error
	captureErr error
	releases   int
	captures   map[string]int64
	hold       billing.Hold
}

func (b *orderedBilling) EnsureAccount(context.Context, string) error { return nil }
func (b *orderedBilling) ReserveCredit(_ context.Context, r billing.ReservationRequest) (billing.Hold, error) {
	*b.events = append(*b.events, "reserve")
	if b.reserveErr != nil {
		return billing.Hold{}, b.reserveErr
	}
	return billing.Hold{ID: 9, CustomerID: r.CustomerID, WorkloadRef: r.WorkloadRef, AmountMicroUSD: r.PriceQuote.MaximumChargeMicroUSD, State: billing.HoldPending}, nil
}
func (b *orderedBilling) ReleaseHold(context.Context, string, int64, string, string) error {
	b.releases++
	return b.releaseErr
}
func (b *orderedBilling) CaptureHold(_ context.Context, _ string, _ int64, amount int64, key, _ string) error {
	if b.captureErr != nil {
		return b.captureErr
	}
	if b.captures == nil {
		b.captures = map[string]int64{}
	}
	if prior, ok := b.captures[key]; ok && prior != amount {
		return billing.ErrIdempotencyConflict
	}
	b.captures[key] = amount
	return nil
}
func (b *orderedBilling) GetHold(context.Context, string, int64) (billing.Hold, error) {
	if b.hold.ID != 0 {
		return b.hold, nil
	}
	return billing.Hold{}, billing.ErrNotFound
}

type gateClosingBilling struct {
	inner *orderedBilling
	gate  *PaidGate
}

func (g *gateClosingBilling) EnsureAccount(ctx context.Context, id string) error {
	return g.inner.EnsureAccount(ctx, id)
}
func (g *gateClosingBilling) ReserveCredit(ctx context.Context, r billing.ReservationRequest) (billing.Hold, error) {
	h, err := g.inner.ReserveCredit(ctx, r)
	if err == nil {
		g.gate.Disable()
	}
	return h, err
}
func (g *gateClosingBilling) ReleaseHold(ctx context.Context, a string, b int64, c, d string) error {
	return g.inner.ReleaseHold(ctx, a, b, c, d)
}
func (g *gateClosingBilling) CaptureHold(ctx context.Context, a string, b, c int64, d, e string) error {
	return g.inner.CaptureHold(ctx, a, b, c, d, e)
}
func (g *gateClosingBilling) GetHold(ctx context.Context, a string, b int64) (billing.Hold, error) {
	return g.inner.GetHold(ctx, a, b)
}

type orderedQuotedDecider struct {
	events         *[]string
	cloudAccountID string
	planErr        error
	ambiguous      bool
	planStarted    chan struct{}
	planRelease    <-chan struct{}
	planStartOnce  sync.Once
	planCalls      atomic.Int32
}

func (d *orderedQuotedDecider) Quote(context.Context, *workload.Workload, PlanOptions) (*BurstQuote, error) {
	*d.events = append(*d.events, "quote")
	now := time.Now().UTC()
	return &BurstQuote{Backend: "linode", CloudAccountID: d.cloudAccountID, SKU: "g6-standard-1", HourlyUSD: .01,
		Price: billing.PriceQuote{QuoteID: "quote_safe", PricingVersion: 1, Currency: "USD", Provider: "linode", SKU: "g6-standard-1",
			ProviderRateMicroUSDPerHour: 10000, CustomerRateMicroUSDPerHour: 10000, MaximumDurationSeconds: 3600,
			MaximumChargeMicroUSD: 10000, IssuedAt: now, ValidUntil: now.Add(time.Minute)}}, nil
}
func (d *orderedQuotedDecider) Plan(ctx context.Context, wl *workload.Workload, opts PlanOptions) (*Plan, error) {
	q, err := d.Quote(ctx, wl, opts)
	if err != nil {
		return nil, err
	}
	return d.PlanQuoted(ctx, wl, opts, q)
}
func (d *orderedQuotedDecider) PlanQuoted(context.Context, *workload.Workload, PlanOptions, *BurstQuote) (*Plan, error) {
	*d.events = append(*d.events, "provider-create")
	d.planCalls.Add(1)
	if d.planStarted != nil {
		d.planStartOnce.Do(func() { close(d.planStarted) })
	}
	if d.planRelease != nil {
		<-d.planRelease
	}
	if d.planErr != nil {
		return nil, d.planErr
	}
	return &Plan{BurstID: "burst_bill", Backend: "linode", BackendID: "vm_bill", CloudAccountID: d.cloudAccountID,
		HourlyUSD: .01, SKU: "g6-standard-1"}, nil
}
func (d *orderedQuotedDecider) CreateOutcomeAmbiguous(error) bool { return d.ambiguous }

func readyPaidGate() *PaidGate {
	g := NewPaidGate()
	g.Enable()
	for i := 0; i < prereqCount; i++ {
		g.SetReady(PaidGatePrerequisite(i))
	}
	return g
}

func billingCreateFixture(t *testing.T, dec *orderedQuotedDecider, ledger *orderedBilling) (*Workloads, *state.Customer) {
	t.Helper()
	store := state.New()
	cust := &state.Customer{ID: "cust_bill", Token: "tok"}
	store.AddCustomer(cust)
	store.AddAgent(&state.Agent{ID: "agent_bill", CustomerID: cust.ID, ClusterID: "cluster_bill", Send: make(chan protocol.Envelope, 4)})
	return &Workloads{Store: store, Decider: dec, Reaper: &fakeReaper{}, Log: quietLog(), Billing: ledger, EnforcePrepaidBilling: true, PaidGate: readyPaidGate()}, cust
}

func TestPrepaidBurstNilGateFailsClosed(t *testing.T) {
	events := []string{}
	ledger := &orderedBilling{events: &events}
	h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events}, ledger)
	h.PaidGate = nil
	rec := submitTo(h, cust, submitOpts{key: "nil-gate"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil gate: status = %d, want 503", rec.Code)
	}
	if strings.Contains(strings.Join(events, ","), "reserve") {
		t.Fatal("nil gate: reserve was called")
	}
	if strings.Contains(strings.Join(events, ","), "provider-create") {
		t.Fatal("nil gate: provider-create was called")
	}
	if !strings.Contains(rec.Body.String(), "paid_runtime_not_ready") {
		t.Fatalf("nil gate: response missing code: %s", rec.Body)
	}
}

func TestPrepaidBurstNotReadyGateFailsClosed(t *testing.T) {
	events := []string{}
	ledger := &orderedBilling{events: &events}
	h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events}, ledger)
	h.PaidGate = NewPaidGate()
	h.PaidGate.Enable()
	rec := submitTo(h, cust, submitOpts{key: "notready-gate"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("not-ready gate: status = %d, want 503", rec.Code)
	}
	if strings.Contains(strings.Join(events, ","), "reserve") {
		t.Fatal("not-ready gate: reserve was called")
	}
	if strings.Contains(strings.Join(events, ","), "provider-create") {
		t.Fatal("not-ready gate: provider-create was called")
	}
	if !strings.Contains(rec.Body.String(), "paid_runtime_not_ready") {
		t.Fatalf("not-ready gate: response missing code: %s", rec.Body)
	}
}

func TestPrepaidBurstGateClosureDuringReserveReleasesHoldOnce(t *testing.T) {
	events := []string{}
	gate := readyPaidGate()
	ledger := &orderedBilling{events: &events}
	h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events}, ledger)
	h.PaidGate = gate
	// Close the gate after reserve: the orderedBilling.ReserveCredit returns
	// successfully, then the second gate check finds it closed.
	closingLedger := &gateClosingBilling{inner: ledger, gate: gate}
	h.Billing = closingLedger
	rec := submitTo(h, cust, submitOpts{key: "gate-closure"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("gate closure after reserve: status = %d, want 503", rec.Code)
	}
	if closingLedger.inner.releases != 1 {
		t.Fatalf("hold releases = %d, want exactly 1", closingLedger.inner.releases)
	}
	if strings.Contains(strings.Join(events, ","), "provider-create") {
		t.Fatal("gate closure after reserve: provider-create was called")
	}
	if !strings.Contains(rec.Body.String(), "paid_runtime_not_ready") {
		t.Fatalf("gate closure: response missing code: %s", rec.Body)
	}
}

func TestPrepaidBurstGateClosureReleaseFailureKeepsDurableObligation(t *testing.T) {
	events := []string{}
	gate := readyPaidGate()
	ledger := &orderedBilling{events: &events, releaseErr: errors.New("billing unavailable")}
	dec := &orderedQuotedDecider{events: &events}
	h, cust := billingCreateFixture(t, dec, ledger)
	h.PaidGate = gate
	h.Billing = &gateClosingBilling{inner: ledger, gate: gate}

	rec := submitTo(h, cust, submitOpts{key: "gate-release-failure"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("gate release failure: status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "paid_runtime_not_ready") {
		t.Fatalf("gate release failure: response missing paid_runtime_not_ready: %s", rec.Body)
	}
	if ledger.releases != 1 || strings.Contains(strings.Join(events, ","), "provider-create") {
		t.Fatalf("gate release failure: releases=%d events=%v", ledger.releases, events)
	}
	if calls := dec.planCalls.Load(); calls != 0 {
		t.Fatalf("PlanQuoted was called %d times during release failure; must be zero", calls)
	}
	rows := h.Store.WorkloadsForCustomer(cust.ID, 10)
	if len(rows) != 1 || rows[0].Billing == nil || !rows[0].Billing.ManualAttention ||
		rows[0].Billing.HoldID != 9 || !rows[0].Billing.AuthoritativeUsageRequired {
		t.Fatalf("durable release obligation = %+v", rows)
	}
	if rows[0].Status != "failed" {
		t.Fatalf("durable obligation status = %q, want failed", rows[0].Status)
	}

	reserveCount := strings.Count(strings.Join(events, ","), "reserve")
	replay := submitTo(h, cust, submitOpts{key: "gate-release-failure"})
	if replay.Code != http.StatusServiceUnavailable || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay = %d headers=%v body=%s", replay.Code, replay.Header(), replay.Body)
	}
	if got := strings.Count(strings.Join(events, ","), "reserve"); got != reserveCount {
		t.Fatalf("retry reserved another hold: before=%d after=%d events=%v", reserveCount, got, events)
	}
	if calls := dec.planCalls.Load(); calls != 0 {
		t.Fatalf("replay called PlanQuoted %d times; a terminalized claim must not reach the provider", calls)
	}
}

func TestPrepaidBurstGateClosureWaitsForProviderAdmissionLease(t *testing.T) {
	events := []string{}
	started := make(chan struct{})
	releasePlan := make(chan struct{})
	dec := &orderedQuotedDecider{events: &events, planStarted: started, planRelease: releasePlan}
	ledger := &orderedBilling{events: &events}
	h, cust := billingCreateFixture(t, dec, ledger)

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- submitTo(h, cust, submitOpts{key: "gate-lease-first"})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first provider admission did not start")
	}

	disableStarted := make(chan struct{})
	disableDone := make(chan struct{})
	go func() {
		close(disableStarted)
		h.PaidGate.Disable()
		close(disableDone)
	}()
	<-disableStarted
	select {
	case <-disableDone:
		t.Fatal("gate closure completed while provider admission lease was held")
	case <-time.After(100 * time.Millisecond):
	}

	close(releasePlan)
	select {
	case rec := <-firstDone:
		if rec.Code != http.StatusAccepted {
			t.Fatalf("first create = %d: %s", rec.Code, rec.Body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first create did not finish")
	}
	select {
	case <-disableDone:
	case <-time.After(2 * time.Second):
		t.Fatal("gate closure did not finish after provider admission released")
	}

	second := submitTo(h, cust, submitOpts{key: "gate-lease-second"})
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-closure create = %d, want 503: %s", second.Code, second.Body)
	}
	if !strings.Contains(second.Body.String(), "paid_runtime_not_ready") {
		t.Fatalf("post-closure create: response missing paid_runtime_not_ready: %s", second.Body)
	}
	if calls := dec.planCalls.Load(); calls != 1 {
		t.Fatalf("provider admission calls = %d, want 1", calls)
	}
}

// ClearReady is the operator kill-switch path (a single prerequisite going
// unready). It must exhibit the same wait-on-in-flight-admission linearization
// the Disable path is proven to above: a paid provider create that was already
// under the lease cannot be racing a partial closure.
func TestPrepaidBurstClearReadyWaitsForProviderAdmissionLease(t *testing.T) {
	events := []string{}
	started := make(chan struct{})
	releasePlan := make(chan struct{})
	dec := &orderedQuotedDecider{events: &events, planStarted: started, planRelease: releasePlan}
	ledger := &orderedBilling{events: &events}
	h, cust := billingCreateFixture(t, dec, ledger)

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- submitTo(h, cust, submitOpts{key: "clear-lease-first"})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first provider admission did not start")
	}

	clearStarted := make(chan struct{})
	clearDone := make(chan struct{})
	go func() {
		close(clearStarted)
		h.PaidGate.ClearReady(PrereqOperatorKillSwitch)
		close(clearDone)
	}()
	<-clearStarted
	select {
	case <-clearDone:
		t.Fatal("ClearReady completed while provider admission lease was held")
	case <-time.After(100 * time.Millisecond):
	}

	close(releasePlan)
	select {
	case rec := <-firstDone:
		if rec.Code != http.StatusAccepted {
			t.Fatalf("first create = %d: %s", rec.Code, rec.Body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first create did not finish")
	}
	select {
	case <-clearDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ClearReady did not finish after provider admission released")
	}

	second := submitTo(h, cust, submitOpts{key: "clear-lease-second"})
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-clear create = %d, want 503: %s", second.Code, second.Body)
	}
	if !strings.Contains(second.Body.String(), "paid_runtime_not_ready") {
		t.Fatalf("post-clear create: response missing paid_runtime_not_ready: %s", second.Body)
	}
	if calls := dec.planCalls.Load(); calls != 1 {
		t.Fatalf("provider admission calls = %d, want 1", calls)
	}
}

func TestPrepaidBurstBYOCAcceptedWithClosedGate(t *testing.T) {
	events := []string{}
	ledger := &orderedBilling{events: &events}
	h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events, cloudAccountID: "cloud_byoc"}, ledger)
	h.PaidGate = NewPaidGate()
	h.PaidGate.Enable()
	rec := submitTo(h, cust, submitOpts{key: "byoc-closed-gate"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("BYOC with closed gate: status = %d, want 202: %s", rec.Code, rec.Body)
	}
}

func TestPrepaidBurstHostedAcceptedWithClosedGate(t *testing.T) {
	events := []string{}
	ledger := &orderedBilling{events: &events}
	h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events}, ledger)
	h.PaidGate = NewPaidGate()
	h.PaidGate.Enable()
	h.Store.RemoveAgent("agent_bill")
	h.Store.AddAgent(&state.Agent{ID: "agent_bill", CustomerID: cust.ID, ClusterID: "hosted-bill", Send: make(chan protocol.Envelope, 4)})
	row, _, err := h.Store.AssignHostedCluster(cust.ID, "hosted-bill", "", "ys-bill", state.OperatorActor())
	if err != nil {
		t.Fatal(err)
	}
	spec := strings.Replace(idemSpec, "  name: idem-test", "  name: idem-test\n  namespace: "+row.HostedNamespace, 1)
	rec := submitTo(h, cust, submitOpts{key: "hosted-closed-gate", clusterID: "hosted-bill", spec: spec})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("hosted with closed gate: status = %d, want 202: %s", rec.Code, rec.Body)
	}
}

func TestPrepaidBurstOrdersQuoteReserveBeforeProviderCreate(t *testing.T) {
	events := []string{}
	ledger := &orderedBilling{events: &events}
	h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events}, ledger)
	rec := submitTo(h, cust, submitOpts{key: "billing-order"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create = %d: %s", rec.Code, rec.Body)
	}
	if got := strings.Join(events, ","); got != "quote,reserve,provider-create" {
		t.Fatalf("ordering = %s", got)
	}
	bursts := h.Store.BurstsForCustomer(cust.ID)
	if len(bursts) != 1 || bursts[0].Billing == nil || bursts[0].Billing.HoldID != 9 ||
		!bursts[0].Billing.AuthoritativeUsageRequired {
		t.Fatalf("billing association = %+v", bursts)
	}
}

func TestPrepaidBurstBYOCBypassesLedger(t *testing.T) {
	events := []string{}
	ledger := &orderedBilling{events: &events}
	h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events, cloudAccountID: "cloud_byoc"}, ledger)
	rec := submitTo(h, cust, submitOpts{key: "billing-byoc"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create = %d: %s", rec.Code, rec.Body)
	}
	if got := strings.Join(events, ","); got != "quote,provider-create" {
		t.Fatalf("BYOC events = %s", got)
	}
}

func TestPrepaidBurstHostedCapacityBypassesLedger(t *testing.T) {
	events := []string{}
	ledger := &orderedBilling{events: &events}
	h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events}, ledger)
	h.Store.RemoveAgent("agent_bill")
	h.Store.AddAgent(&state.Agent{ID: "agent_bill", CustomerID: cust.ID, ClusterID: "hosted-bill", Send: make(chan protocol.Envelope, 4)})
	row, _, err := h.Store.AssignHostedCluster(cust.ID, "hosted-bill", "", "ys-bill", state.OperatorActor())
	if err != nil {
		t.Fatal(err)
	}
	spec := strings.Replace(idemSpec, "  name: idem-test", "  name: idem-test\n  namespace: "+row.HostedNamespace, 1)
	rec := submitTo(h, cust, submitOpts{key: "billing-hosted", clusterID: "hosted-bill", spec: spec})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create = %d: %s", rec.Code, rec.Body)
	}
	if got := strings.Join(events, ","); got != "quote,provider-create" {
		// The legacy Plan implementation on this fake delegates to Quote; the
		// absence of reserve is the invariant this test pins.
		t.Fatalf("hosted events = %s", got)
	}
}

func TestPrepaidBurstInsufficientCreditAndCreateCompensation(t *testing.T) {
	t.Run("insufficient never creates", func(t *testing.T) {
		events := []string{}
		ledger := &orderedBilling{events: &events, reserveErr: billing.ErrInsufficientCredit}
		h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events}, ledger)
		rec := submitTo(h, cust, submitOpts{key: "billing-empty"})
		if rec.Code != http.StatusPaymentRequired || strings.Contains(strings.Join(events, ","), "provider-create") {
			t.Fatalf("result=%d events=%v", rec.Code, events)
		}
	})
	t.Run("proven failure releases", func(t *testing.T) {
		events := []string{}
		ledger := &orderedBilling{events: &events}
		h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events, planErr: errors.New("provider refused")}, ledger)
		_ = submitTo(h, cust, submitOpts{key: "billing-refused"})
		if ledger.releases != 1 {
			t.Fatalf("releases = %d", ledger.releases)
		}
	})
	t.Run("proven failure release error persists durable obligation", func(t *testing.T) {
		events := []string{}
		ledger := &orderedBilling{events: &events, releaseErr: errors.New("billing unavailable")}
		dec := &orderedQuotedDecider{events: &events, planErr: errors.New("provider refused")}
		h, cust := billingCreateFixture(t, dec, ledger)
		rec := submitTo(h, cust, submitOpts{key: "billing-release-fail"})
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		if ledger.releases != 1 {
			t.Fatalf("releases = %d", ledger.releases)
		}
		rows := h.Store.WorkloadsForCustomer(cust.ID, 10)
		if len(rows) != 1 || rows[0].Billing == nil || !rows[0].Billing.ManualAttention {
			t.Fatalf("proven failure release obligation = %+v", rows)
		}
		if rows[0].Status != "failed" {
			t.Fatalf("status = %s", rows[0].Status)
		}

		reserveCount := strings.Count(strings.Join(events, ","), "reserve")
		replay := submitTo(h, cust, submitOpts{key: "billing-release-fail"})
		if replay.Code != http.StatusServiceUnavailable || replay.Header().Get("Idempotency-Replayed") != "true" {
			t.Fatalf("replay = %d headers=%v body=%s", replay.Code, replay.Header(), replay.Body)
		}
		if got := strings.Count(strings.Join(events, ","), "reserve"); got != reserveCount || dec.planCalls.Load() != 1 || ledger.releases != 1 {
			t.Fatalf("replay repeated side effects: reserves before=%d after=%d plan calls=%d releases=%d events=%v", reserveCount, got, dec.planCalls.Load(), ledger.releases, events)
		}
	})
	t.Run("ambiguous retains", func(t *testing.T) {
		events := []string{}
		ledger := &orderedBilling{events: &events}
		h, cust := billingCreateFixture(t, &orderedQuotedDecider{events: &events, planErr: errors.New("connection lost"), ambiguous: true}, ledger)
		_ = submitTo(h, cust, submitOpts{key: "billing-ambiguous"})
		if ledger.releases != 0 {
			t.Fatalf("releases = %d", ledger.releases)
		}
		rows := h.Store.WorkloadsForCustomer(cust.ID, 10)
		if len(rows) != 1 || rows[0].Billing == nil || !rows[0].Billing.ManualAttention {
			t.Fatalf("ambiguous association = %+v", rows)
		}
	})
}

func TestTerminalSettlementReplayDoesNotDoubleCapture(t *testing.T) {
	events := []string{}
	ledger := &orderedBilling{events: &events}
	b := &state.Burst{ID: "burst_settle", CustomerID: "cust_bill", Billing: &state.WorkloadBilling{HoldID: 7, ReservedMicroUSD: 100000}}
	cost := &state.WorkloadCost{BurstID: b.ID, EstimatedUSD: .05}
	if err := settleBurstBilling(context.Background(), ledger, b, cost); err != nil {
		t.Fatal(err)
	}
	if err := settleBurstBilling(context.Background(), ledger, b, cost); err != nil {
		t.Fatal(err)
	}
	if len(ledger.captures) != 1 || ledger.captures["burst-capture:"+b.ID] != 50000 {
		t.Fatalf("captures = %#v", ledger.captures)
	}
}

func TestInlineProvisioningCompensationSettlesOrPersistsRetry(t *testing.T) {
	t.Run("settles exactly once economically", func(t *testing.T) {
		events := []string{}
		ledger := &orderedBilling{events: &events}
		store := state.New()
		burst := &state.Burst{ID: "burst_comp_ok", CustomerID: "cust_bill", Backend: "linode", BackendID: "node-1",
			CreatedAt: time.Now().Add(-time.Hour), HourlyUSD: .05,
			Billing: &state.WorkloadBilling{HoldID: 5, ReservedMicroUSD: 100000}}
		h := &Workloads{Store: store, Reaper: &fakeReaper{}, Billing: ledger, Log: quietLog()}
		if !h.compensateProvisionedBurst(t.Context(), burst, "persist failed") {
			t.Fatal("compensation did not complete")
		}
		if !h.compensateProvisionedBurst(t.Context(), burst, "persist retry") {
			t.Fatal("replayed compensation did not complete")
		}
		if len(ledger.captures) != 1 || ledger.captures["burst-capture:"+burst.ID] < 49000 {
			t.Fatalf("economic captures = %#v", ledger.captures)
		}
	})

	t.Run("failed settlement leaves durable ordinary retry", func(t *testing.T) {
		events := []string{}
		ledger := &orderedBilling{events: &events, captureErr: errors.New("ledger unavailable")}
		store := state.New()
		burst := &state.Burst{ID: "burst_comp_retry", CustomerID: "cust_bill", Backend: "linode", BackendID: "node-2",
			CreatedAt: time.Now().Add(-time.Hour), HourlyUSD: .05,
			Billing: &state.WorkloadBilling{HoldID: 6, ReservedMicroUSD: 100000}}
		h := &Workloads{Store: store, Reaper: &fakeReaper{}, Billing: ledger, Log: quietLog()}
		if h.compensateProvisionedBurst(t.Context(), burst, "persist failed") {
			t.Fatal("failed settlement reported clean completion")
		}
		stored, err := store.GetBurst(burst.ID)
		if err != nil || stored.TerminalCost == nil || stored.Billing == nil || stored.Billing.HoldID != 6 {
			t.Fatalf("durable retry = %+v, err=%v", stored, err)
		}
	})
}
