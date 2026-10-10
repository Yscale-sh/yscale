package decider

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/pkg/backends"
)

// seamTrace records the order of the two things that must never be observed in
// the wrong sequence: the durable admission write and the paid provider call.
type seamTrace struct{ events []string }

func (s *seamTrace) add(event string) { s.events = append(s.events, event) }
func (s *seamTrace) String() string   { return strings.Join(s.events, ",") }
func (s *seamTrace) reset()           { s.events = nil }

// seamBackend is a recordingBackend that also stamps the trace, so "the create
// happened after admission" is an assertion rather than an inference.
type seamBackend struct {
	*recordingBackend
	trace *seamTrace
}

func (b *seamBackend) CreateNode(ctx context.Context, spec *backends.NodeSpec) (string, error) {
	b.trace.add("create")
	return b.recordingBackend.CreateNode(ctx, spec)
}

// fakeAdmission stands in for *lifecycle.Store across the process-death
// boundaries this seam exists for — a store that admits and then refuses the
// claim, a claim that hands back a crashed attempt's payload, a settlement that
// fails after the machine is already running.
type fakeAdmission struct {
	trace *seamTrace

	admitted      lifecycle.AdmissionRequest
	admitResponse lifecycle.AdmissionResponse
	admitErr      error

	// replayPayload is the request a crashed predecessor admitted; when set,
	// the claim returns it instead of what this attempt just resolved.
	replayPayload []byte
	replayBurstID string

	// claimState non-empty refuses the claim and reports that state;
	// claimLeaseExpired makes that refusal a crashed attempt's dead lease.
	claimState        string
	claimLeaseExpired bool
	claimErr          error

	succeededResource string
	succeededLease    string
	succeedErr        error

	failCalls         int
	failedError       string
	failedLease       string
	failedMaxAttempts int
	failedRetryAt     time.Time
	failErr           error
}

func (f *fakeAdmission) AdmitWorkload(_ context.Context, req lifecycle.AdmissionRequest) (lifecycle.AdmissionResponse, error) {
	f.trace.add("admit")
	f.admitted = req
	if f.admitErr != nil {
		return lifecycle.AdmissionResponse{}, f.admitErr
	}
	resp := f.admitResponse
	if resp.WorkloadID == "" {
		resp.WorkloadID = req.WorkloadID
		resp.BurstID = req.BurstID
		resp.Inserted = true
	}
	return resp, nil
}

func (f *fakeAdmission) ClaimProviderCreateForBurst(_ context.Context, customerID, clusterID, burstID string, _ time.Duration) (lifecycle.ProviderCreateOperation, bool, error) {
	f.trace.add("claim")
	if f.claimErr != nil {
		return lifecycle.ProviderCreateOperation{}, false, f.claimErr
	}
	if f.claimState != "" {
		return lifecycle.ProviderCreateOperation{
			BurstID: burstID, State: f.claimState, LeaseExpired: f.claimLeaseExpired,
		}, false, nil
	}
	op := lifecycle.ProviderCreateOperation{
		ID: 7, CustomerID: customerID, ClusterID: clusterID,
		WorkloadID: f.admitted.WorkloadID, BurstID: burstID,
		Provider: f.admitted.Provider, Region: f.admitted.Region, SKU: f.admitted.SKU,
		PayloadVersion: f.admitted.CanonicalVersion, Payload: f.admitted.ProviderCreatePayload,
		State: lifecycle.OperationProcessing, Attempts: 1, LeaseToken: "lease-1",
	}
	if f.replayPayload != nil {
		op.Payload = f.replayPayload
		op.BurstID = f.replayBurstID
	}
	return op, true, nil
}

func (f *fakeAdmission) MarkProviderCreateSucceeded(_ context.Context, _ int64, leaseToken, providerResourceID string) error {
	f.trace.add("succeed")
	f.succeededLease = leaseToken
	f.succeededResource = providerResourceID
	return f.succeedErr
}

func (f *fakeAdmission) MarkProviderCreateFailed(_ context.Context, _ int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error {
	f.trace.add("fail")
	f.failCalls++
	f.failedLease = leaseToken
	f.failedError = safeError
	f.failedRetryAt = retryAt
	f.failedMaxAttempts = maxAttempts
	return f.failErr
}

func newAdmissionSeamDecider(t *testing.T) (*Decider, *seamBackend, *fakeAdmission, *seamTrace, *providerCalls, func()) {
	t.Helper()
	return newAdmissionSeamDeciderFor(t, backends.TypeFlyIO)
}

// newAdmissionSeamDeciderFor wires the seam onto one backend so the shared
// outcome contract can be exercised without a provider call.
func newAdmissionSeamDeciderFor(t *testing.T, backendName string) (*Decider, *seamBackend, *fakeAdmission, *seamTrace, *providerCalls, func()) {
	t.Helper()
	trace := &seamTrace{}
	ts, meshCalls, closeTS := newTestTailscale(t, "")
	t.Cleanup(closeTS)
	backend := &seamBackend{
		recordingBackend: &recordingBackend{name: backendName, createID: "node-1"},
		trace:            trace,
	}
	admission := &fakeAdmission{trace: trace}
	d := New(Config{})
	d.ts = ts
	switch backendName {
	case backends.TypeFlyIO:
		d.fly = backend
	case backends.TypeLinode:
		d.linode = backend
	case backends.TypeAWS:
		d.aws = backend
	case backends.TypeGCP:
		d.gcp = backend
		d.gcpRegion = "us-central1"
	case backends.TypeAzure:
		d.azure = backend
	default:
		t.Fatalf("unsupported admission seam backend %q", backendName)
	}
	d.WithLifecycleAdmission(admission)
	return d, backend, admission, trace, meshCalls, closeTS
}

const seamClusterID = "cluster_seam"

func seamPlanOptions() handlers.PlanOptions {
	return handlers.PlanOptions{CustomerID: "cust_seam", ClusterID: seamClusterID, WorkloadID: "wl_seam"}
}

func decodeSeamRequest(t *testing.T, payload []byte) providerCreateRequest {
	t.Helper()
	var request providerCreateRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		t.Fatalf("admitted payload is not a provider request: %v", err)
	}
	return request
}

// The whole point of the seam: durable intent commits, is leased, and only then
// is a machine bought — and the settlement that records the machine is the last
// thing that happens.
func TestPlanAdmitsAndLeasesBeforeTheProviderCreate(t *testing.T) {
	d, backend, admission, trace, _, _ := newAdmissionSeamDecider(t)

	plan, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := trace.String(); got != "admit,claim,create,succeed" {
		t.Fatalf("seam order = %q, want admit,claim,create,succeed", got)
	}
	if admission.succeededResource != plan.BackendID || admission.succeededLease != "lease-1" {
		t.Fatalf("settlement recorded %q under lease %q, want %q under lease-1",
			admission.succeededResource, admission.succeededLease, plan.BackendID)
	}

	// The admitted record names the exact machine that was bought.
	request := decodeSeamRequest(t, admission.admitted.ProviderCreatePayload)
	if request.BurstID != plan.BurstID || request.NodeName != backend.lastSpec.Name ||
		request.Backend != plan.Backend || request.SKU != plan.SKU || request.Tier != plan.Tier {
		t.Fatalf("admitted request %+v does not describe the created node %+v", request, backend.lastSpec)
	}
	if admission.admitted.BurstID != plan.BurstID || admission.admitted.WorkloadID != "wl_seam" {
		t.Fatalf("admission identity = %s/%s, want wl_seam/%s",
			admission.admitted.WorkloadID, admission.admitted.BurstID, plan.BurstID)
	}
	// The lifecycle key is the durable workload id, never the client's
	// Idempotency-Key, which this layer is not given and must not store.
	if admission.admitted.IdempotencyKey != "wl_seam" {
		t.Fatalf("admission idempotency key = %q, want the durable workload id", admission.admitted.IdempotencyKey)
	}

	// Resolution before admission is side-effect free: neither the minted mesh
	// credential nor the allocated /24 exists yet, so neither can appear in the
	// record that was written before them.
	payload := string(admission.admitted.ProviderCreatePayload)
	if backend.lastSpec.TSAuthKey == "" || strings.Contains(payload, backend.lastSpec.TSAuthKey) {
		t.Fatalf("admitted payload carries the minted mesh auth key: %s", payload)
	}
	if plan.PodCIDR == "" || strings.Contains(payload, plan.PodCIDR) {
		t.Fatalf("admitted payload carries the allocated pod CIDR: %s", payload)
	}
}

// If durable admission cannot commit, no provider call occurs — and neither
// does the credential mint or the /24 allocation that precede it.
func TestPlanBuysNothingWhenAdmissionCannotCommit(t *testing.T) {
	d, backend, admission, trace, meshCalls, _ := newAdmissionSeamDecider(t)
	admission.admitErr = errors.New("database unavailable")

	if _, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions()); err == nil {
		t.Fatal("Plan succeeded with a refusing admission store")
	}
	if backend.createCalls != 0 || meshCalls.mint.Load() != 0 || len(d.reservedSlots) != 0 {
		t.Fatalf("side effects past a refused admission: creates=%d mints=%d CIDRs=%d",
			backend.createCalls, meshCalls.mint.Load(), len(d.reservedSlots))
	}
	if got := trace.String(); got != "admit" {
		t.Fatalf("seam order = %q, want admit only", got)
	}
}

// Authoritative admission with no durable identity to admit under is a create
// that cannot be replay-protected, so it does not happen.
func TestPlanFailsClosedWithoutADurableWorkloadIdentity(t *testing.T) {
	d, backend, _, trace, _, _ := newAdmissionSeamDecider(t)

	opts := seamPlanOptions()
	opts.WorkloadID = ""
	_, err := d.Plan(context.Background(), minimalPlanWorkload(), opts)
	if !errors.Is(err, ErrAdmissionIdentityMissing) {
		t.Fatalf("Plan error = %v, want ErrAdmissionIdentityMissing", err)
	}
	if backend.createCalls != 0 || trace.String() != "" {
		t.Fatalf("unkeyed submission reached the seam: creates=%d trace=%q", backend.createCalls, trace)
	}
}

// A replay whose operation is already settled must not buy a second machine,
// and the refusal has to name the state that refused it.
func TestPlanRefusesASecondCreateForAnAlreadySettledOperation(t *testing.T) {
	d, backend, admission, _, meshCalls, _ := newAdmissionSeamDecider(t)
	admission.admitResponse = lifecycle.AdmissionResponse{WorkloadID: "wl_seam", BurstID: "burst_original"}
	admission.claimState = lifecycle.OperationSucceeded

	_, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	var notClaimable *ProviderCreateNotClaimableError
	if !errors.As(err, &notClaimable) {
		t.Fatalf("Plan error = %v, want a ProviderCreateNotClaimableError", err)
	}
	if notClaimable.State != lifecycle.OperationSucceeded || notClaimable.BurstID != "burst_original" {
		t.Fatalf("refusal = %+v, want burst_original/succeeded", notClaimable)
	}
	// A machine already exists under this submission, so the refusal is an
	// ambiguous create: the caller must keep the hold, not refund it.
	if !d.CreateOutcomeAmbiguous(err) {
		t.Fatalf("refusing a settled replay must stay ambiguous for billing: %v", err)
	}
	if backend.createCalls != 0 || meshCalls.mint.Load() != 0 {
		t.Fatalf("a settled replay reached side effects: creates=%d mints=%d", backend.createCalls, meshCalls.mint.Load())
	}
}

// CRASH BOUNDARY: an attempt died after committing intent. The retry resumes
// the ORIGINAL admitted request rather than the one it just resolved, so the
// machine bought is the machine that was admitted.
func TestPlanResumesTheOriginallyAdmittedRequestOnReplay(t *testing.T) {
	d, backend, admission, trace, _, _ := newAdmissionSeamDecider(t)

	first, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	if err != nil {
		t.Fatalf("first Plan: %v", err)
	}
	originalPayload := admission.admitted.ProviderCreatePayload
	originalNodeName := backend.lastSpec.Name

	// The predecessor's record is what admission now returns: same workload,
	// same burst, not newly inserted.
	trace.reset()
	backend.createCalls = 0
	admission.admitResponse = lifecycle.AdmissionResponse{WorkloadID: "wl_seam", BurstID: first.BurstID}
	admission.replayPayload = originalPayload
	admission.replayBurstID = first.BurstID

	resumed, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	if err != nil {
		t.Fatalf("resumed Plan: %v", err)
	}
	if resumed.BurstID != first.BurstID {
		t.Fatalf("resumed burst = %s, want the admitted %s", resumed.BurstID, first.BurstID)
	}
	if backend.createCalls != 1 || backend.lastSpec.BurstID != first.BurstID || backend.lastSpec.Name != originalNodeName {
		t.Fatalf("resumed create used %+v, want the admitted burst %s / node %s",
			backend.lastSpec, first.BurstID, originalNodeName)
	}
	// A resumed attempt mints its own ephemera; only the identity is inherited.
	if backend.lastSpec.TSAuthKey == "" || backend.lastSpec.PodCIDR == "" {
		t.Fatalf("resumed create carried no fresh credential/allocation: %+v", backend.lastSpec)
	}
	if got := trace.String(); got != "admit,claim,create,succeed" {
		t.Fatalf("resumed seam order = %q", got)
	}
}

// A failed create is settled terminally under its lease, and the ambiguity
// classification is recorded rather than left to be re-derived from provider
// error prose.
func TestPlanSettlesAFailedCreateTerminallyAndRecordsAmbiguity(t *testing.T) {
	d, backend, admission, trace, _, _ := newAdmissionSeamDecider(t)
	backend.createErr = errors.New("provider refused the create")

	_, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	if err == nil {
		t.Fatal("Plan succeeded despite a failing provider create")
	}
	if !d.CreateOutcomeAmbiguous(err) {
		t.Fatalf("a non-Linode create failure must stay ambiguous for billing: %v", err)
	}
	if admission.failCalls != 1 || admission.failedLease != "lease-1" {
		t.Fatalf("settlement calls=%d lease=%q, want 1 under lease-1", admission.failCalls, admission.failedLease)
	}
	if admission.failedMaxAttempts != 1 {
		t.Fatalf("maxAttempts = %d, want 1 so the operation dead-letters instead of leaving executable intent",
			admission.failedMaxAttempts)
	}
	if !strings.Contains(admission.failedError, "provider_create_ambiguous") {
		t.Fatalf("durable error %q does not record the ambiguity classification", admission.failedError)
	}
	if !admission.failedRetryAt.After(time.Now()) {
		t.Fatalf("retryAt %v is not in the future; the store rejects that", admission.failedRetryAt)
	}
	if got := trace.String(); got != "admit,claim,create,fail" {
		t.Fatalf("seam order = %q, want admit,claim,create,fail", got)
	}
}

// CRASH BOUNDARY: the machine exists but the settlement that records it does
// not. That is an ambiguous create, not a clean failure — the caller must
// retain the billing hold and route it to manual attention.
func TestPlanReportsAnUnsettleableSuccessAsAmbiguous(t *testing.T) {
	d, backend, admission, trace, _, _ := newAdmissionSeamDecider(t)
	admission.succeedErr = errors.New("database unavailable")

	_, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	if err == nil {
		t.Fatal("Plan reported success while the created node went unrecorded")
	}
	if !d.CreateOutcomeAmbiguous(err) {
		t.Fatalf("an unrecorded live node must be reported as ambiguous: %v", err)
	}
	if backend.createCalls != 1 {
		t.Fatalf("creates = %d, want exactly 1", backend.createCalls)
	}
	if got := trace.String(); got != "admit,claim,create,succeed" {
		t.Fatalf("seam order = %q", got)
	}
}

// A refusal raised while the lease is held settles the operation, so nothing
// executable is left behind for a leased worker to act on after the submitter
// has already been told the burst failed.
func TestPlanSettlesARefusalRaisedInsideTheLeasedWindow(t *testing.T) {
	d, backend, admission, trace, _, closeTS := newAdmissionSeamDecider(t)
	// A dead coordination server fails the mint — the first credential side
	// effect after admission.
	closeTS()

	if _, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions()); err == nil {
		t.Fatal("Plan succeeded with no reachable mesh provider")
	}
	if backend.createCalls != 0 {
		t.Fatalf("creates = %d, want 0", backend.createCalls)
	}
	if admission.failCalls != 1 || admission.failedMaxAttempts != 1 {
		t.Fatalf("leased refusal settled %d time(s) with maxAttempts=%d, want 1/1",
			admission.failCalls, admission.failedMaxAttempts)
	}
	if got := trace.String(); got != "admit,claim,fail" {
		t.Fatalf("seam order = %q, want admit,claim,fail", got)
	}
}

// CRASH BOUNDARY, THE EXPENSIVE ONE. A prior attempt died holding the create
// lease. The lease has since expired, but the request had already been handed
// out, so a provider may have accepted the create before that attempt stopped
// writing. This submission must NOT be given the operation to execute again:
// no backend central routes to offers an idempotency key, so a second
// CreateNode is a second machine the tenant pays for and nothing reaps.
func TestPlanNeverReclaimsAnExpiredCreateLeaseForASecondCreateNode(t *testing.T) {
	d, backend, admission, trace, meshCalls, _ := newAdmissionSeamDecider(t)
	admission.admitResponse = lifecycle.AdmissionResponse{WorkloadID: "wl_seam", BurstID: "burst_crashed"}
	admission.claimState = lifecycle.OperationProcessing
	admission.claimLeaseExpired = true

	_, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	if err == nil {
		t.Fatal("Plan proceeded on a dead attempt's operation")
	}
	if backend.createCalls != 0 || meshCalls.mint.Load() != 0 {
		t.Fatalf("a stranded create reached side effects: creates=%d mints=%d", backend.createCalls, meshCalls.mint.Load())
	}
	if got := trace.String(); got != "admit,claim" {
		t.Fatalf("seam order = %q, want admit,claim — nothing may be settled under a lease this attempt does not hold", got)
	}

	// The machine may exist, so the answer is ambiguous: the caller keeps the
	// hold and routes the submission to manual attention rather than refunding
	// a burst that could be billing.
	if !d.CreateOutcomeAmbiguous(err) {
		t.Fatalf("an expired create lease must be ambiguous, not a clean failure: %v", err)
	}
	var notClaimable *ProviderCreateNotClaimableError
	if !errors.As(err, &notClaimable) {
		t.Fatalf("Plan error = %v, want a ProviderCreateNotClaimableError", err)
	}
	if !notClaimable.LeaseExpired || notClaimable.State != lifecycle.OperationProcessing {
		t.Fatalf("refusal = %+v, want processing with an expired lease", notClaimable)
	}
	// An operator reading one log line has to be able to tell a dead attempt
	// from a live one; both are "processing".
	if !strings.Contains(err.Error(), "expired lease") {
		t.Fatalf("refusal is not legible as a stranded attempt: %v", err)
	}
}

// A live attempt holds the lease. Also refused, also ambiguous — but it is a
// different situation and must not read as the stranded one.
func TestPlanRefusesALiveAttemptWithoutClaimingItStranded(t *testing.T) {
	d, _, admission, _, _, _ := newAdmissionSeamDecider(t)
	admission.admitResponse = lifecycle.AdmissionResponse{WorkloadID: "wl_seam", BurstID: "burst_inflight"}
	admission.claimState = lifecycle.OperationProcessing

	_, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	var notClaimable *ProviderCreateNotClaimableError
	if !errors.As(err, &notClaimable) || notClaimable.LeaseExpired {
		t.Fatalf("Plan error = %v (%+v), want a live-attempt refusal", err, notClaimable)
	}
	if strings.Contains(err.Error(), "expired lease") {
		t.Fatalf("a live attempt is reported as stranded: %v", err)
	}
	if !d.CreateOutcomeAmbiguous(err) {
		t.Fatalf("an in-flight create may hold a machine and must stay ambiguous: %v", err)
	}
}

// Settlement is not best effort. A failure the store could not record is a
// failure nothing authoritative holds, so answering with the clean refusal
// would release the tenant's hold for a burst whose outcome is unknown.
func TestPlanReportsAnUnsettleableFailureAsAmbiguous(t *testing.T) {
	// Even the strongest provider-neutral clean-failure signal cannot be
	// reported as clean when nothing recorded the settlement.
	d, backend, admission, _, _, _ := newAdmissionSeamDeciderFor(t, backends.TypeLinode)
	backend.createErr = backends.MarkCreateProvenZeroResource(errors.New("provider refused the create"))
	admission.failErr = errors.New("database unavailable")

	wl := minimalPlanWorkload()
	wl.Spec.Backend = backends.TypeLinode
	_, err := d.Plan(context.Background(), wl, seamPlanOptions())
	if err == nil {
		t.Fatal("Plan succeeded despite a failing provider create")
	}
	if backend.createCalls != 1 {
		t.Fatalf("creates = %d, want exactly 1", backend.createCalls)
	}
	if admission.failCalls != 1 {
		t.Fatalf("settlement calls = %d, want 1", admission.failCalls)
	}
	if !d.CreateOutcomeAmbiguous(err) {
		t.Fatalf("an unrecorded failure must be ambiguous so the hold is retained: %v", err)
	}
	if !strings.Contains(err.Error(), "provider_create_ambiguous") {
		t.Fatalf("the answer does not name the ambiguity: %v", err)
	}
	// The durable safe error still records what the PROVIDER proved; only the
	// answer travelling back to the caller escalates. And that answer carries
	// the settlement's own failure, not the provider's raw response.
	if !strings.Contains(admission.failedError, "provider_create_failed") {
		t.Fatalf("durable error %q lost the provider's own known-failed classification", admission.failedError)
	}
	if strings.Contains(err.Error(), "provider refused the create") {
		t.Fatalf("the settlement failure carries the raw provider payload: %v", err)
	}
}

// The counterpart: when settlement DOES commit, a provider-proven failure stays
// a clean failure and the caller's hold is released. Escalating this one would
// strand credit on every ordinary rejected create.
func TestPlanKeepsASettledProvenFailureClean(t *testing.T) {
	for _, provider := range []string{
		backends.TypeFlyIO, backends.TypeLinode, backends.TypeAWS,
		backends.TypeGCP, backends.TypeAzure,
	} {
		t.Run(provider, func(t *testing.T) {
			d, backend, admission, _, _, _ := newAdmissionSeamDeciderFor(t, provider)
			backend.createErr = backends.MarkCreateProvenZeroResource(errors.New("provider refused the create"))

			wl := minimalPlanWorkload()
			wl.Spec.Backend = provider
			_, err := d.Plan(context.Background(), wl, seamPlanOptions())
			if err == nil {
				t.Fatal("Plan succeeded despite a failing provider create")
			}
			if d.CreateOutcomeAmbiguous(err) {
				t.Fatalf("a settled, provider-proven failure must stay clean: %v", err)
			}
			if !strings.Contains(admission.failedError, "provider_create_failed") {
				t.Fatalf("durable error %q does not record the known-failed classification", admission.failedError)
			}
		})
	}
}

// The same rule for a refusal raised before the provider is ever reached. Here
// nothing was created at all — and it STILL cannot be reported as a clean
// failure, because the operation the refusal was supposed to close is left
// holding an unresolved lease.
func TestPlanReportsAnUnsettleableLeasedRefusalAsAmbiguous(t *testing.T) {
	d, backend, admission, _, _, closeTS := newAdmissionSeamDecider(t)
	admission.failErr = errors.New("database unavailable")
	closeTS()

	_, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	if err == nil {
		t.Fatal("Plan succeeded with no reachable mesh provider")
	}
	if backend.createCalls != 0 {
		t.Fatalf("creates = %d, want 0", backend.createCalls)
	}
	if !d.CreateOutcomeAmbiguous(err) {
		t.Fatalf("an unsettled leased refusal must be ambiguous: %v", err)
	}
	if !strings.Contains(err.Error(), "did not commit") {
		t.Fatalf("the answer does not say the settlement failed: %v", err)
	}
}

// The admitted identity is what a later authoritative delete re-checks against
// the stored burst. It must carry the tenant's cloud account, and it must spell
// "no region" the way the provider-delete projection spells it — while the plan
// the caller books keeps the real routing region, empty and all.
func TestPlanAdmitsTheIdentityAuthoritativeDeleteWillCheck(t *testing.T) {
	d, backend, admission, _, _, _ := newAdmissionSeamDecider(t)
	// Linode's unpinned region is selected from live capacity, unlike Fly's
	// fixed configured region. Preserve the canonical unknown-region contract.
	d.fly, d.linode = nil, backend
	backend.name = backends.TypeLinode
	wl := minimalPlanWorkload()
	wl.Spec.Backend = backends.TypeLinode

	// This workload pins no region, which is the case with two plausible
	// spellings and therefore the one that strands a machine.
	plan, err := d.Plan(context.Background(), wl, seamPlanOptions())
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.Region != "" {
		t.Fatalf("plan region = %q, want the workload's own unpinned region — #98 books from this value", plan.Region)
	}
	if admission.admitted.Region != lifecycle.CanonicalProviderDeleteRegion(plan.Region) {
		t.Fatalf("admitted region = %q, want the canonical form %q that the delete projection derives from the same plan",
			admission.admitted.Region, lifecycle.CanonicalProviderDeleteRegion(plan.Region))
	}
	request := decodeSeamRequest(t, admission.admitted.ProviderCreatePayload)
	if request.Region != admission.admitted.Region {
		t.Fatalf("admitted payload region %q disagrees with the operation row's %q",
			request.Region, admission.admitted.Region)
	}
	// The region is an ADMISSION concern only: the provider call still routes on
	// the resolved resource region, so recording an absence cannot become a
	// guess at which datacentre to buy in.
	if request.Resources.Region != "" {
		t.Fatalf("the canonical admission region leaked into the provider call: %q", request.Resources.Region)
	}
}

func TestPlanAdmitsKnownConfiguredProviderRegion(t *testing.T) {
	d, _, admission, _, _, _ := newAdmissionSeamDecider(t)
	d.cfg.FlyRegion = "iad"
	d.gcpRegion = "us-central1"
	plan, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	if err != nil {
		t.Fatal(err)
	}
	request := decodeSeamRequest(t, admission.admitted.ProviderCreatePayload)
	if plan.Region != "iad" || request.Region != "iad" || admission.admitted.Region != "iad" {
		t.Fatalf("configured region drifted: plan=%q request=%q admission=%q", plan.Region, request.Region, admission.admitted.Region)
	}
	if request.Resources.Region != "" {
		t.Fatal("reporting a provider default must not introduce a workload region pin")
	}
}

// A BYOC burst is created with the tenant's own cloud credentials, and the
// authoritative delete has to be routed back through the same account. Admission
// is where that account becomes durable, so it has to reach the shared admission
// identity and not just the opaque payload.
func TestAdmitProviderCreateCarriesTheTenantCloudAccount(t *testing.T) {
	d, _, admission, _, _, _ := newAdmissionSeamDecider(t)

	request := resolveProviderCreateRequest(
		minimalPlanWorkload(), seamPlanOptions(), "burst_byoc",
		backends.TypeLinode, "ca_tenant_0123456789ab",
		lifecycle.CanonicalProviderDeleteRegion(""), "g6-standard-2", "shape_hash", "placement_digest",
		backends.ResourceRequirements{CPUMillis: 1000, MemoryMB: 1024},
	)
	if _, err := d.admitProviderCreate(context.Background(), minimalPlanWorkload(), seamPlanOptions(), request); err != nil {
		t.Fatalf("admitProviderCreate: %v", err)
	}
	if admission.admitted.CloudAccountID != "ca_tenant_0123456789ab" {
		t.Fatalf("admitted cloud account = %q, want the tenant's — a later delete would read an identity conflict",
			admission.admitted.CloudAccountID)
	}
	if admission.admitted.Region != "region-unspecified" {
		t.Fatalf("admitted region = %q, want the projection's canonical absence", admission.admitted.Region)
	}
	payload := decodeSeamRequest(t, admission.admitted.ProviderCreatePayload)
	if payload.CloudAccountID != admission.admitted.CloudAccountID {
		t.Fatalf("payload account %q disagrees with the admission identity %q",
			payload.CloudAccountID, admission.admitted.CloudAccountID)
	}
}

// The compatibility seam: with no lifecycle store wired, the inline OSS/dev
// path provisions exactly as it always did, including for the unkeyed legacy
// submission that carries no durable workload identity at all.
func TestPlanWithoutLifecycleAdmissionKeepsTheInlinePath(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeFlyIO)

	plan, err := d.Plan(context.Background(), minimalPlanWorkload(), handlers.PlanOptions{})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if backend.createCalls != 1 || plan.BackendID != "node-1" || meshCalls.mint.Load() != 1 {
		t.Fatalf("inline path changed: creates=%d backendID=%q mints=%d",
			backend.createCalls, plan.BackendID, meshCalls.mint.Load())
	}
	if plan.BurstID == "" || plan.PodCIDR == "" || plan.NodeName == "" {
		t.Fatalf("inline plan is incomplete: %+v", plan)
	}
}

// --- Node identity labels ---

// Labels survive the durable request -> nodeSpec path: the admitted payload
// carries them, and nodeSpec renders them into the provider call.
func TestNodeIdentityLabelsSurviveDurableRequestToNodeSpec(t *testing.T) {
	d, backend, _, _, _, _ := newAdmissionSeamDecider(t)

	plan, err := d.Plan(context.Background(), minimalPlanWorkload(), seamPlanOptions())
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	spec := backend.lastSpec
	if spec.NodeLabels == nil {
		t.Fatal("NodeLabels is nil on the created NodeSpec")
	}
	if spec.NodeLabels["yscale.sh/burst-id"] != plan.BurstID {
		t.Errorf("burst-id label = %q, want %q", spec.NodeLabels["yscale.sh/burst-id"], plan.BurstID)
	}
	if spec.NodeLabels["yscale.sh/cluster-id"] != seamClusterID {
		t.Errorf("cluster-id label = %q, want %q", spec.NodeLabels["yscale.sh/cluster-id"], seamClusterID)
	}
	if spec.NodeLabels["yscale.sh/provider-class"] != "flyio" {
		t.Errorf("provider-class label = %q, want %q", spec.NodeLabels["yscale.sh/provider-class"], "flyio")
	}
	if _, ok := spec.NodeLabels["yscale.sh/gpu-kind"]; ok {
		t.Error("CPU workload should not have gpu-kind label")
	}
	if _, ok := spec.NodeLabels["yscale.sh/gpu-count"]; ok {
		t.Error("CPU workload should not have gpu-count label")
	}
	if _, ok := spec.NodeLabels["nvidia.com/gpu.present"]; ok {
		t.Error("CPU workload should not have nvidia.com/gpu.present label")
	}
}

// CPU labels omit GPU identity entirely.
func TestCPUWorkloadOmitsGPULabels(t *testing.T) {
	req := resolveProviderCreateRequest(
		minimalPlanWorkload(), seamPlanOptions(), "burst_cpu",
		backends.TypeLinode, "", "us-ord", "g6-standard-2", "shape_hash", "",
		backends.ResourceRequirements{CPUMillis: 1000, MemoryMB: 1024},
	)
	labels := req.nodeIdentityLabels(seamClusterID)
	for _, key := range []string{"yscale.sh/gpu-kind", "yscale.sh/gpu-count", "nvidia.com/gpu.present"} {
		if _, ok := labels[key]; ok {
			t.Errorf("CPU request should not carry label %s", key)
		}
	}
}

// GPU labels are present and correct for GPU requests.
func TestGPUWorkloadCarriesGPULabels(t *testing.T) {
	req := resolveProviderCreateRequest(
		minimalPlanWorkload(), seamPlanOptions(), "burst_gpu",
		backends.TypeLinode, "", "us-ord", "gpu-rtx4000-ada-x1", "shape_hash", "",
		backends.ResourceRequirements{
			CPUMillis: 4000, MemoryMB: 16384,
			GPU: &backends.GPUSpec{Kind: "l4", Count: 2},
		},
	)
	labels := req.nodeIdentityLabels(seamClusterID)
	if labels["yscale.sh/gpu-kind"] != "l4" {
		t.Errorf("gpu-kind = %q, want l4", labels["yscale.sh/gpu-kind"])
	}
	if labels["yscale.sh/gpu-count"] != "2" {
		t.Errorf("gpu-count = %q, want 2", labels["yscale.sh/gpu-count"])
	}
	if _, ok := labels["nvidia.com/gpu.present"]; ok {
		t.Error("requested GPU shape must not claim observed NVIDIA readiness")
	}
}

// Labels survive a JSON round-trip (durable serialization).
func TestNodeLabelsSerializeAndDeserialize(t *testing.T) {
	req := resolveProviderCreateRequest(
		minimalPlanWorkload(), seamPlanOptions(), "burst_rt",
		backends.TypeFlyIO, "", "iad", "small", "shape_hash", "",
		backends.ResourceRequirements{
			CPUMillis: 2000, MemoryMB: 2048,
			GPU: &backends.GPUSpec{Kind: "a100", Count: 8},
		},
	)
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded providerCreateRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if strings.Contains(string(data), "node_labels") {
		t.Fatalf("derived labels changed the durable v1 payload: %s", data)
	}
	labels := decoded.nodeIdentityLabels(seamClusterID)
	if labels["yscale.sh/burst-id"] != req.nodeIdentityLabels(seamClusterID)["yscale.sh/burst-id"] {
		t.Error("burst-id label did not survive round-trip")
	}
	if labels["yscale.sh/gpu-kind"] != "a100" {
		t.Error("gpu-kind label did not survive round-trip")
	}
	if labels["yscale.sh/gpu-count"] != "8" {
		t.Error("gpu-count label did not survive round-trip")
	}
}

// Invalid label values fail closed before admission or provider mutation.
func TestInvalidNodeIdentityLabelsFailClosed(t *testing.T) {
	req := resolveProviderCreateRequest(
		minimalPlanWorkload(), seamPlanOptions(), "burst_bad",
		backends.TypeFlyIO, "", "iad", "small", "shape_hash", "",
		backends.ResourceRequirements{
			CPUMillis: 2000, MemoryMB: 2048,
			GPU: &backends.GPUSpec{Kind: "has spaces", Count: 1},
		},
	)
	if err := validateNodeIdentityLabels(req.nodeIdentityLabels(seamClusterID)); err == nil {
		t.Fatal("expected validation to reject label value with spaces")
	}
}

func TestMissingClusterIdentityLabelFailsClosed(t *testing.T) {
	d, backend, _, _, _, _ := newAdmissionSeamDecider(t)
	opts := seamPlanOptions()
	opts.ClusterID = ""
	if _, err := d.Plan(context.Background(), minimalPlanWorkload(), opts); err == nil {
		t.Fatal("authoritative admission accepted a missing cluster identity")
	}
	if backend.createCalls != 0 {
		t.Fatalf("provider calls = %d, want zero", backend.createCalls)
	}
}

// Oversized label values fail closed.
func TestOversizedLabelValueFailsClosed(t *testing.T) {
	longKind := strings.Repeat("a", 64)
	req := resolveProviderCreateRequest(
		minimalPlanWorkload(), seamPlanOptions(), "burst_long",
		backends.TypeFlyIO, "", "iad", "small", "shape_hash", "",
		backends.ResourceRequirements{
			CPUMillis: 2000, MemoryMB: 2048,
			GPU: &backends.GPUSpec{Kind: longKind, Count: 1},
		},
	)
	if err := validateNodeIdentityLabels(req.nodeIdentityLabels(seamClusterID)); err == nil {
		t.Fatal("expected validation to reject oversized label value")
	}
}

// Cloud account IDs never appear in the node identity labels.
func TestNoForbiddenAccountValuesInLabels(t *testing.T) {
	req := resolveProviderCreateRequest(
		minimalPlanWorkload(), seamPlanOptions(), "burst_byoc",
		backends.TypeLinode, "ca_tenant_0123456789ab", "us-ord", "g6-standard-2", "shape_hash", "",
		backends.ResourceRequirements{CPUMillis: 1000, MemoryMB: 1024},
	)
	labels := req.nodeIdentityLabels(seamClusterID)
	for key, val := range labels {
		if strings.Contains(val, "ca_tenant") {
			t.Errorf("label %s=%q contains a cloud account ID", key, val)
		}
	}
	if labels["yscale.sh/provider-class"] != "linode" {
		t.Errorf("provider-class = %q, want the backend family not the account", labels["yscale.sh/provider-class"])
	}
}

// Provider class maps backend names to stable families.
func TestBackendFamilyMapping(t *testing.T) {
	cases := []struct {
		backend string
		want    string
	}{
		{backends.TypeLinode, "linode"},
		{backends.TypeFlyIO, "flyio"},
		{backends.TypeAWS, "aws"},
		{backends.TypeGCP, "gcp"},
		{backends.TypeAzure, "azure"},
	}
	for _, tc := range cases {
		got := backendFamily(tc.backend)
		if got != tc.want {
			t.Errorf("backendFamily(%q) = %q, want %q", tc.backend, got, tc.want)
		}
	}
}

// Region is present in labels when known, absent when empty.
func TestRegionLabelPresence(t *testing.T) {
	withRegion := resolveProviderCreateRequest(
		minimalPlanWorkload(), seamPlanOptions(), "burst_r",
		backends.TypeFlyIO, "", "us-ord", "small", "shape_hash", "",
		backends.ResourceRequirements{CPUMillis: 1000, MemoryMB: 1024, Region: "us-ord"},
	)
	if withRegion.nodeIdentityLabels(seamClusterID)["yscale.sh/region"] != "us-ord" {
		t.Errorf("region label = %q, want us-ord", withRegion.nodeIdentityLabels(seamClusterID)["yscale.sh/region"])
	}

	noRegion := resolveProviderCreateRequest(
		minimalPlanWorkload(), seamPlanOptions(), "burst_nr",
		backends.TypeFlyIO, "", "", "small", "shape_hash", "",
		backends.ResourceRequirements{CPUMillis: 1000, MemoryMB: 1024},
	)
	if _, ok := noRegion.nodeIdentityLabels(seamClusterID)["yscale.sh/region"]; ok {
		t.Error("empty region should not produce a region label")
	}
}

func TestAdmittedNodeIdentityLabelsFailClosedBeforeProviderMutation(t *testing.T) {
	d, backend, admission, _, _, _ := newAdmissionSeamDecider(t)
	request := resolveProviderCreateRequest(
		minimalPlanWorkload(), seamPlanOptions(), "burst_bad_replay",
		backends.TypeFlyIO, "", "iad", "small", "shape_hash", "",
		backends.ResourceRequirements{
			CPUMillis: 2000, MemoryMB: 2048, Region: "iad",
			GPU: &backends.GPUSpec{Kind: "l4", Count: 1},
		},
	)
	replay := request
	replay.Resources.GPU = &backends.GPUSpec{Kind: "has spaces", Count: 1}
	payload, err := json.Marshal(replay)
	if err != nil {
		t.Fatalf("marshal replay payload: %v", err)
	}
	admission.replayPayload = payload
	admission.replayBurstID = request.BurstID

	_, err = d.admitProviderCreate(context.Background(), minimalPlanWorkload(), seamPlanOptions(), request)
	if err == nil || !strings.Contains(err.Error(), "admitted node identity label validation") {
		t.Fatalf("admitProviderCreate error = %v, want admitted label validation failure", err)
	}
	if backend.createCalls != 0 {
		t.Fatalf("provider CreateNode called %d times", backend.createCalls)
	}
	if admission.failCalls != 1 {
		t.Fatalf("failed settlement calls = %d, want 1", admission.failCalls)
	}
}
