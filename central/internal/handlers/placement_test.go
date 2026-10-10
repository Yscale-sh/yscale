package handlers

import (
	"context"
	"encoding/json"
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

// receiptDecider is a QuotedDecider that produces real placement receipts, so
// the surface under test is exercised against the same typed contract the real
// decider emits. rateMicroUSD stands in for the price catalog: moving it is how
// a test reproduces "the decision changed between preview and launch".
type receiptDecider struct {
	mu           sync.Mutex
	quotes       int
	plans        int
	rateMicroUSD int64
}

func (d *receiptDecider) rate() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.rateMicroUSD == 0 {
		return 620_000
	}
	return d.rateMicroUSD
}

func (d *receiptDecider) setRate(micro int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rateMicroUSD = micro
}

func (d *receiptDecider) Quote(_ context.Context, _ *workload.Workload, opts PlanOptions) (*BurstQuote, error) {
	d.mu.Lock()
	d.quotes++
	n := d.quotes
	d.mu.Unlock()
	rate := d.rate()
	receipt := state.PlacementReceipt{
		Version:            state.PlacementReceiptVersion,
		Tenant:             opts.CustomerID,
		RequestedClusterID: opts.ClusterPlacement.RequestedClusterID,
		GrantedClusterID:   opts.ClusterPlacement.GrantedClusterID,
		ClusterMode:        opts.ClusterPlacement.Mode,
		ClusterRule:        opts.ClusterPlacement.Rule,
		ClusterRuleVersion: opts.ClusterPlacement.RuleVersion,
		Requested:          state.PlacementRequest{CPUMillis: 1000, MemoryMB: 2048},
		Constraints: state.PlacementConstraints{
			AccountMode: state.PlacementAccountPlatform, AccountEligible: true,
		},
		Candidates: []state.PlacementCandidate{
			{Provider: "linode", Region: "us-ord", SKU: "g6-standard-2", HourlyMicroUSD: rate, Selected: true},
			{Provider: "aws", Reason: state.PlacementRejectProviderNotAllowed},
		},
		Selected: state.PlacementSelection{
			Provider: "linode", Region: "us-ord", SKU: "g6-standard-2",
			AccountMode: state.PlacementAccountPlatform, HourlyMicroUSD: rate,
			MaximumDurationSeconds: 3600, MaximumChargeMicroUSD: rate,
		},
		PricingVersion:         state.PlacementPricingVersion,
		CandidateSetVersion:    state.PlacementCandidateSetVersion,
		AvailabilityConfidence: state.PlacementAvailabilityCreateTimeOnly,
	}
	if err := receipt.Seal(fmt.Sprintf("quote_%d", n), time.Now().UTC(), 5*time.Minute); err != nil {
		return nil, err
	}
	return &BurstQuote{
		Backend: "linode", Region: "us-ord", SKU: "g6-standard-2", HourlyUSD: 0.62,
		Placement: &receipt,
	}, nil
}

func (d *receiptDecider) Plan(ctx context.Context, wl *workload.Workload, opts PlanOptions) (*Plan, error) {
	quote, err := d.Quote(ctx, wl, opts)
	if err != nil {
		return nil, err
	}
	return d.PlanQuoted(ctx, wl, opts, quote)
}

func (d *receiptDecider) PlanQuoted(_ context.Context, wl *workload.Workload, _ PlanOptions, quote *BurstQuote) (*Plan, error) {
	d.mu.Lock()
	d.plans++
	id := fmt.Sprintf("burst_receipt_%d", d.plans)
	d.mu.Unlock()
	p := &Plan{
		BurstID: id, Backend: "linode", BackendID: "vm-" + id,
		HourlyUSD: 0.62, SKU: "g6-standard-2", Placement: quote.Placement,
	}
	if wl.Spec.Budget != nil {
		p.Deadline = wl.Spec.Budget.Deadline
		p.MaxUSD = wl.Spec.Budget.MaxUSD
	}
	return p, nil
}

func (d *receiptDecider) CreateOutcomeAmbiguous(error) bool { return false }

func (d *receiptDecider) counts() (int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.quotes, d.plans
}

const previewSpec = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: preview-job
spec:
  image: busybox
  size: small`

// The operator secret this deployment signs previews with, and one belonging to
// somebody else entirely — a second deployment, or an attacker who worked out
// the token's shape but does not hold the key.
const (
	previewSigningKey = "preview-launch-signing-key-for-handler-tests-01"
	foreignSigningKey = "a-completely-different-operator-secret-42424242"
)

type previewFixture struct {
	store   *state.Store
	cust    *state.Customer
	h       *Workloads
	dec     *receiptDecider
	journal *failingJournal
	// signer is the deployment's own key, held separately so a test can mint the
	// exact credential an attacker would have to produce — including on the
	// fixture whose HANDLER has no signer at all.
	signer *state.PlacementTokenSigner
}

func newPreviewFixture(t *testing.T) *previewFixture {
	t.Helper()
	store := state.New()
	cust := &state.Customer{ID: "cust_preview", Token: "tok_preview", MaxConcurrentBursts: 5, MaxHourlyUSD: 50}
	store.AddCustomer(cust)
	store.AddCustomer(&state.Customer{ID: "cust_stranger", Token: "tok_stranger"})
	store.AddAgent(&state.Agent{
		ID: "agent_preview", CustomerID: cust.ID, ClusterID: "cluster_preview",
		Send: make(chan protocol.Envelope, 32),
	})
	store.AddAgent(&state.Agent{
		ID: "agent_stranger", CustomerID: "cust_stranger", ClusterID: "cluster_stranger",
		Send: make(chan protocol.Envelope, 32),
	})
	dec := &receiptDecider{}
	journal := &failingJournal{Store: store}
	signer, err := state.NewPlacementTokenSigner(previewSigningKey)
	if err != nil {
		t.Fatalf("NewPlacementTokenSigner: %v", err)
	}
	return &previewFixture{
		store: store, cust: cust, dec: dec, journal: journal, signer: signer,
		h: &Workloads{Store: store, Decider: dec, Reaper: &fakeReaper{}, Log: quietLog(), Journal: journal, PlacementTokens: signer},
	}
}

// newUnsignedPreviewFixture is the deployment an operator started without
// YSCALE_PLACEMENT_TOKEN_KEY: it can decide placements but cannot vouch for one.
func newUnsignedPreviewFixture(t *testing.T) *previewFixture {
	t.Helper()
	f := newPreviewFixture(t)
	f.h.PlacementTokens = nil
	return f
}

// mint signs claims with an arbitrary key, so a test can present a token that is
// well-formed in every respect except the one under test.
func mint(t *testing.T, key string, c state.PlacementLaunchClaims) string {
	t.Helper()
	signer, err := state.NewPlacementTokenSigner(key)
	if err != nil {
		t.Fatalf("NewPlacementTokenSigner: %v", err)
	}
	token, err := signer.Sign(c)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return token
}

// claimsOf is what a client can see of an issuance: everything in the preview
// response body. It is deliberately built from the PUBLIC receipt, because that
// is exactly the material an attacker holds.
func claimsOf(tenant string, p *state.PublicPlacementReceipt) state.PlacementLaunchClaims {
	return state.PlacementLaunchClaims{
		Version:   p.Version,
		Tenant:    tenant,
		QuoteID:   p.QuoteID,
		Digest:    p.Digest,
		IssuedAt:  p.IssuedAt,
		ExpiresAt: p.ExpiresAt,
	}
}

func (f *previewFixture) preview(clusterID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/cust_preview/placement-preview", strings.NewReader(previewSpec))
	if clusterID != "" {
		req.Header.Set(clusterIDHeader, clusterID)
	}
	ctx := context.WithValue(context.Background(), ctxCustomer, f.cust)
	rec := httptest.NewRecorder()
	f.h.PreviewPlacement(rec, req.WithContext(ctx))
	return rec
}

// launch submits under the credential a client is holding. An empty token is the
// path that never previewed: the header is not sent at all.
func (f *previewFixture) launch(key, token string) *httptest.ResponseRecorder {
	if token == "" {
		return f.launchRaw(key)
	}
	return f.launchRaw(key, token)
}

// launchRaw presents the token header verbatim and as many times as given, so a
// test can send the malformed, blank, duplicated and forged shapes a client
// could send — including a header that is present but empty, which must not be
// read as "never previewed".
func (f *previewFixture) launchRaw(key string, tokens ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(previewSpec))
	req.Header.Set(idempotencyHeader, key)
	for _, token := range tokens {
		req.Header.Add(placementTokenHeader, token)
	}
	ctx := withSubmitter(context.WithValue(context.Background(), ctxCustomer, f.cust), submitter{
		Actor: state.HumanActor("acct_preview", f.cust.ID), Role: state.RoleOwner,
	})
	rec := httptest.NewRecorder()
	f.h.Create(rec, req.WithContext(ctx))
	return rec
}

// assertNothingSpent is the assertion every refusal shares: a launch that was
// turned away must not have reserved admission, taken a billing hold, recorded
// provider-create intent, or left a workload or burst behind.
func (f *previewFixture) assertNothingSpent(t *testing.T) {
	t.Helper()
	if _, plans := f.dec.counts(); plans != 0 {
		t.Fatalf("a refused launch still provisioned %d plan(s)", plans)
	}
	if got := len(f.store.BurstsForCustomer(f.cust.ID)); got != 0 {
		t.Fatalf("a refused launch left %d burst(s)", got)
	}
	if got := len(f.store.WorkloadsForCustomer(f.cust.ID, 10)); got != 0 {
		t.Fatalf("a refused launch left %d workload(s)", got)
	}
}

func decodePreview(t *testing.T, rec *httptest.ResponseRecorder) PlacementPreviewResponse {
	t.Helper()
	var out PlacementPreviewResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode preview response: %v (%s)", err, rec.Body)
	}
	return out
}

func decodeCreate(t *testing.T, rec *httptest.ResponseRecorder) CreateWorkloadResponse {
	t.Helper()
	var out CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode create response: %v (%s)", err, rec.Body)
	}
	return out
}

// A preview is a READ. It must answer the same decision a launch would make and
// leave nothing at all behind: no claim, no reservation, no hold, no burst, no
// workload, no audit row, no provider call.
func TestPreviewPlacementHasNoSideEffects(t *testing.T) {
	f := newPreviewFixture(t)

	rec := f.preview("")
	if rec.Code != http.StatusOK {
		t.Fatalf("preview status = %d, want 200: %s", rec.Code, rec.Body)
	}
	body := decodePreview(t, rec)
	if body.Status != "ok" || body.Placement == nil {
		t.Fatalf("preview answered no decision: %+v", body)
	}
	if body.Placement.Digest == "" || body.Placement.ExpiresAt.IsZero() {
		t.Fatalf("preview answered without the issuance it decided: %+v", body.Placement)
	}
	// The response provides EXACTLY what a launch returns: one opaque token. A
	// client never reassembles a credential out of fields it read off the answer.
	if body.LaunchToken == "" {
		t.Fatal("preview answered without the launch token a launch presents back")
	}
	claims, err := f.signer.Verify(body.LaunchToken)
	if err != nil {
		t.Fatalf("the issued token does not verify under this deployment's key: %v", err)
	}
	if claims.Tenant != f.cust.ID || claims.Digest != body.Placement.Digest ||
		claims.QuoteID != body.Placement.QuoteID ||
		!claims.ExpiresAt.Equal(body.Placement.ExpiresAt.Truncate(time.Second)) {
		t.Fatalf("the token does not authenticate the issuance it was handed out with: %+v", claims)
	}
	// Nothing central holds for itself may ride out in the credential.
	if strings.Contains(body.LaunchToken, previewSigningKey) {
		t.Fatal("the launch token contains the signing key")
	}
	if strings.Contains(rec.Body.String(), "account_binding") {
		t.Fatalf("the preview response exposes the internal account binding: %s", rec.Body)
	}
	if body.Placement.GrantedClusterID != "cluster_preview" {
		t.Fatalf("granted cluster = %q, want the routed one", body.Placement.GrantedClusterID)
	}
	if body.Placement.Selected.Provider == "" || body.Placement.Selected.HourlyMicroUSD <= 0 {
		t.Fatalf("preview decided nothing to show: %+v", body.Placement.Selected)
	}

	if _, plans := f.dec.counts(); plans != 0 {
		t.Fatalf("preview provisioned %d plan(s)", plans)
	}
	if got := len(f.store.BurstsForCustomer(f.cust.ID)); got != 0 {
		t.Fatalf("preview left %d burst(s)", got)
	}
	if got := len(f.store.WorkloadsForCustomer(f.cust.ID, 10)); got != 0 {
		t.Fatalf("preview left %d workload(s)", got)
	}
	f.journal.mu.Lock()
	events := len(f.journal.events)
	f.journal.mu.Unlock()
	if events != 0 {
		t.Fatalf("preview wrote %d audit row(s); an observation is not a decision", events)
	}
	select {
	case env := <-f.store.AgentsForCustomer(f.cust.ID)[0].Send:
		t.Fatalf("preview dispatched %s to the connector", env.Type)
	default:
	}
}

// A preview may not become a way to discover, or place work on, another
// tenant's cluster. It is the same refusal the launch path makes, on the same
// header, because it is literally the same routing call.
func TestPreviewPlacementRefusesAnotherTenantsCluster(t *testing.T) {
	f := newPreviewFixture(t)

	rec := f.preview("cluster_stranger")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant preview status = %d, want 403: %s", rec.Code, rec.Body)
	}
	body := decodePreview(t, rec)
	if body.Placement != nil {
		t.Fatalf("a refused cross-tenant preview still returned a decision: %+v", body.Placement)
	}
	// A refusal is not an issuance, so it hands out no credential to launch with.
	if body.LaunchToken != "" {
		t.Fatal("a refused preview still handed out a launch token")
	}
	if strings.Contains(rec.Body.String(), "agent_stranger") {
		t.Fatalf("the refusal names another tenant's connector: %s", rec.Body)
	}
}

// The whole point of the digest: a launch that presents an unchanged decision
// runs, and the decision it ran under is persisted and rendered back.
func TestLaunchAcceptsAnUnchangedPreviewAndPersistsTheReceipt(t *testing.T) {
	f := newPreviewFixture(t)

	preview := decodePreview(t, f.preview(""))
	digest := preview.Placement.Digest

	rec := f.launch("preview-launch-1", preview.LaunchToken)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("launch status = %d, want 202: %s", rec.Code, rec.Body)
	}
	created := decodeCreate(t, rec)
	if created.Placement == nil || created.Placement.Receipt == nil {
		t.Fatalf("launch answered without the decision it ran: %s", rec.Body)
	}
	if created.Placement.Receipt.Digest != digest {
		t.Fatalf("launch ran under digest %q, previewed %q",
			created.Placement.Receipt.Digest, digest)
	}

	// One quote for the preview and one for the verification, and the launch is
	// bound to the SECOND one rather than quoting a third time — a decision
	// nobody checked must never be the one that runs.
	if quotes, plans := f.dec.counts(); quotes != 2 || plans != 1 {
		t.Fatalf("quotes=%d plans=%d, want 2/1: the launch must reuse the verified decision", quotes, plans)
	}

	stored, err := f.store.GetWorkload(created.ID)
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if stored.Placement == nil || stored.Placement.Receipt == nil {
		t.Fatal("the workload records no placement receipt")
	}
	if stored.Placement.Receipt.Digest != digest {
		t.Fatalf("persisted digest = %q, want %q", stored.Placement.Receipt.Digest, digest)
	}
	// The compatibility fields are untouched by the receipt riding alongside.
	if stored.Placement.GrantedClusterID != "cluster_preview" {
		t.Fatalf("stored cluster placement = %+v", stored.Placement)
	}

	// Workload detail renders the same typed receipt.
	detail := httptest.NewRequest(http.MethodGet, "/v1/workloads/"+created.ID, nil)
	detail.SetPathValue("id", created.ID)
	detailRec := httptest.NewRecorder()
	f.h.Get(detailRec, detail.WithContext(context.WithValue(context.Background(), ctxCustomer, f.cust)))
	if detailRec.Code != http.StatusOK {
		t.Fatalf("workload detail = %d: %s", detailRec.Code, detailRec.Body)
	}
	var rendered struct {
		Placement *PlacementResponse `json:"placement"`
	}
	if err := json.Unmarshal(detailRec.Body.Bytes(), &rendered); err != nil {
		t.Fatalf("decode workload detail: %v", err)
	}
	if rendered.Placement == nil || rendered.Placement.Receipt == nil ||
		rendered.Placement.Receipt.Digest != digest {
		t.Fatalf("workload detail does not render the persisted decision: %s", detailRec.Body)
	}
	// The journal references the same decision by digest rather than restating it.
	f.journal.mu.Lock()
	defer f.journal.mu.Unlock()
	found := false
	for _, ev := range f.journal.events {
		if ev.Action == state.ActionWorkloadSubmit && ev.Outcome == state.OutcomeAccepted {
			found = ev.Detail.PlacementDigest == digest
		}
	}
	if !found {
		t.Fatalf("no accepted submit event references the placement digest: %+v", f.journal.events)
	}
}

// A decision that moved between preview and launch must be refused with the
// stable code and the CURRENT decision — before the reservation, the hold, the
// mesh key and the provider call.
func TestLaunchRefusesAChangedPlacementBeforeAnyMutation(t *testing.T) {
	f := newPreviewFixture(t)
	preview := decodePreview(t, f.preview(""))

	// What a catalog refresh looks like from here.
	f.dec.setRate(990_000)

	rec := f.launch("preview-launch-changed", preview.LaunchToken)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body)
	}
	body := decodeCreate(t, rec)
	if body.Code != PlacementChangedCode {
		t.Fatalf("code = %q, want %q", body.Code, PlacementChangedCode)
	}
	if body.Placement == nil || body.Placement.Receipt == nil {
		t.Fatalf("the refusal did not return the current decision: %s", rec.Body)
	}
	if body.Placement.Receipt.Digest == preview.Placement.Digest {
		t.Fatal("the refusal returned the stale decision it just rejected")
	}
	if body.Placement.Receipt.Selected.HourlyMicroUSD != 990_000 {
		t.Fatalf("the returned decision is not the current one: %+v", body.Placement.Receipt.Selected)
	}

	f.assertNothingSpent(t)

	// The key is unspent: nothing was provisioned, so the client's retry must be
	// able to use it. The 409 carried the current decision but NO credential —
	// a refusal is not an issuance — so the retry previews again, which is the
	// only way to obtain one.
	fresh := decodePreview(t, f.preview(""))
	if fresh.Placement.Selected.HourlyMicroUSD != 990_000 {
		t.Fatalf("the fresh preview is not the current decision: %+v", fresh.Placement.Selected)
	}
	current := decodeCreate(t, f.launch("preview-launch-changed", fresh.LaunchToken))
	if current.Status != "provisioning" {
		t.Fatalf("resubmitting under the current decision = %+v", current)
	}
}

// Everything that never previewed keeps behaving exactly as it did: the cluster
// credential, the CLI, and any client that has never heard of a launch token.
// It takes its own fresh decision, and it does so whether or not this deployment
// holds a signing key.
func TestLaunchWithoutAPreviewCredentialIsUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		newf func(*testing.T) *previewFixture
	}{
		{"with a signing key configured", newPreviewFixture},
		{"with none configured", newUnsignedPreviewFixture},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.newf(t)

			rec := f.launch("preview-launch-none", "")
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
			}
			quotes, plans := f.dec.counts()
			if plans != 1 {
				t.Fatalf("plans = %d, want 1", plans)
			}
			// No credential means no verification quote: an unconsulted launch
			// must not pay for one.
			if quotes != 1 {
				t.Fatalf("quotes = %d, want the single one Plan itself makes", quotes)
			}
			created := decodeCreate(t, rec)
			if created.Placement == nil || created.Placement.Receipt == nil {
				t.Fatal("a launch that never previewed still records the decision it made")
			}
		})
	}
}

// A credential that is malformed, partial or duplicated is a client bug,
// answered before anything is decided — and, critically, never read as the
// no-preview path, which would launch a submission that meant to be checked.
//
// Presence of the header, not its content, is what says a preview was meant to
// be presented: a header the client emptied is a dropped credential, not an
// absent one.
func TestLaunchRejectsMalformedOrPartialPreviewCredentials(t *testing.T) {
	genuine := mint(t, previewSigningKey, state.PlacementLaunchClaims{
		Version: state.PlacementReceiptVersion, Tenant: "cust_preview",
		QuoteID: "quote_1", Digest: strings.Repeat("ab", 32),
		IssuedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
	})
	segments := strings.Split(genuine, ".")

	for _, tc := range []struct {
		name   string
		tokens []string
	}{
		{"a header presented empty", []string{""}},
		{"a header presented blank", []string{"   "}},
		{"a value that is not a token", []string{"not-a-token"}},
		{"the bare digest a client used to send", []string{strings.Repeat("ab", 32)}},
		{"an envelope with no payload or signature", []string{"yspl1"}},
		{"a payload with no signature", []string{segments[0] + "." + segments[1]}},
		{"a signature with no payload", []string{segments[0] + ".." + segments[2]}},
		{"a truncated token", []string{genuine[:len(genuine)-8]}},
		{"an unknown envelope version", []string{"yspl9." + segments[1] + "." + segments[2]}},
		{"the same token presented twice", []string{genuine, genuine}},
		{"two different tokens", []string{genuine, segments[0] + "." + segments[1] + ".AAAA"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPreviewFixture(t)
			rec := f.launchRaw("preview-launch-bad", tc.tokens...)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
			}
			if code := decodeCreate(t, rec).Code; code != PlacementDigestInvalidCode {
				t.Fatalf("code = %q, want %q", code, PlacementDigestInvalidCode)
			}
			// Refused before the verification quote — so before admission, the
			// billing hold and any provider-create intent.
			if quotes, plans := f.dec.counts(); plans != 0 || quotes != 0 {
				t.Fatalf("a malformed credential reached quotes=%d plans=%d", quotes, plans)
			}
			f.assertNothingSpent(t)
		})
	}
}

// THE regression this credential exists for.
//
// Under the previous two-header contract a client presented the digest it was
// shown alongside an expires_at it typed itself. The digest is stable for as
// long as the decision holds, so a preview captured hours ago could be paired
// with any plausible near-future timestamp and was indistinguishable from a
// preview taken a second ago. The expiry now travels INSIDE a signature, so
// re-timing a stale preview means forging one — and forging one requires the
// operator's key.
func TestLaunchRefusesAStaleDigestPairedWithAFabricatedWindow(t *testing.T) {
	f := newPreviewFixture(t)
	stale := decodePreview(t, f.preview(""))

	// Everything the attacker holds: the whole preview body — digest, quote id,
	// receipt version, tenant — plus a clock and a plausible five-minute window.
	// The only thing they do not hold is the key.
	now := time.Now().UTC()
	reTimed := claimsOf(f.cust.ID, stale.Placement)
	reTimed.IssuedAt = now
	reTimed.ExpiresAt = now.Add(5 * time.Minute)

	rec := f.launchRaw("preview-launch-refabricated", mint(t, foreignSigningKey, reTimed))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
	}
	if code := decodeCreate(t, rec).Code; code != PlacementDigestInvalidCode {
		t.Fatalf("code = %q, want %q", code, PlacementDigestInvalidCode)
	}
	// Only the preview's own quote was ever paid for: the forgery never reached
	// the decision path, let alone admission or billing.
	if quotes, _ := f.dec.counts(); quotes != 1 {
		t.Fatalf("quotes = %d, want only the preview's own", quotes)
	}
	f.assertNothingSpent(t)
	if strings.Contains(rec.Body.String(), previewSigningKey) {
		t.Fatalf("the refusal leaks the signing key: %s", rec.Body)
	}
}

// The same attack from the other direction: the genuine token central issued,
// with its payload rewritten to a fresh window and the original signature left
// in place — and every other single-field edit a holder could attempt.
func TestLaunchRefusesATamperedToken(t *testing.T) {
	f := newPreviewFixture(t)
	preview := decodePreview(t, f.preview(""))
	genuine := preview.LaunchToken
	segments := strings.Split(genuine, ".")

	reTimed := claimsOf(f.cust.ID, preview.Placement)
	reTimed.IssuedAt = time.Now().UTC()
	reTimed.ExpiresAt = reTimed.IssuedAt.Add(time.Hour)
	// A second issuance central itself would sign — a longer window, for the
	// same tenant and the same decision. Its halves are the material an attacker
	// splices onto the token they hold.
	reTimedSegments := strings.Split(mint(t, previewSigningKey, reTimed), ".")

	otherDecision := claimsOf(f.cust.ID, preview.Placement)
	otherDecision.Digest = strings.Repeat("cd", 32)
	otherSegments := strings.Split(mint(t, previewSigningKey, otherDecision), ".")

	for _, tc := range []struct{ name, token string }{
		{"a re-timed payload under the original signature",
			segments[0] + "." + reTimedSegments[1] + "." + segments[2]},
		{"the original payload under a re-timed issuance's signature",
			segments[0] + "." + segments[1] + "." + reTimedSegments[2]},
		{"a signature lifted from another decision this key also signed",
			segments[0] + "." + segments[1] + "." + otherSegments[2]},
		{"a payload lifted from another decision this key also signed",
			segments[0] + "." + otherSegments[1] + "." + segments[2]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newPreviewFixture(t)
			rec := g.launchRaw("preview-launch-tampered", tc.token)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
			}
			if code := decodeCreate(t, rec).Code; code != PlacementDigestInvalidCode {
				t.Fatalf("code = %q, want %q", code, PlacementDigestInvalidCode)
			}
			if quotes, _ := g.dec.counts(); quotes != 0 {
				t.Fatalf("a tampered token reached %d quote(s)", quotes)
			}
			g.assertNothingSpent(t)
		})
	}
}

// A token is bound to the account it was issued to. Without that, one tenant's
// genuine, unexpired preview is a bearer credential any other tenant can replay
// onto their own launch — and a digest alone would not notice, since two tenants
// can be shown the same shape at the same price.
func TestLaunchRefusesATokenIssuedToAnotherTenant(t *testing.T) {
	f := newPreviewFixture(t)
	preview := decodePreview(t, f.preview(""))

	// Genuinely signed by THIS deployment's key, for a different account.
	elsewhere := claimsOf("cust_stranger", preview.Placement)
	rec := f.launchRaw("preview-launch-crossbound", mint(t, previewSigningKey, elsewhere))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
	}
	if code := decodeCreate(t, rec).Code; code != PlacementDigestInvalidCode {
		t.Fatalf("code = %q, want %q", code, PlacementDigestInvalidCode)
	}
	if quotes, _ := f.dec.counts(); quotes != 1 {
		t.Fatalf("quotes = %d — a cross-bound token reached the decision path", quotes)
	}
	f.assertNothingSpent(t)
	// The refusal must not confirm which account the token belonged to.
	if strings.Contains(rec.Body.String(), "cust_stranger") {
		t.Fatalf("the refusal names the account the token was issued to: %s", rec.Body)
	}
}

// A deployment that cannot authenticate a presented credential refuses it. The
// alternative — honoring it because there is no key to check it with — is
// exactly the substitution the credential exists to prevent.
func TestLaunchRefusesAPreviewCredentialWhenSigningIsUnavailable(t *testing.T) {
	f := newUnsignedPreviewFixture(t)

	// The preview still answers the decision; it just cannot vouch for it.
	preview := decodePreview(t, f.preview(""))
	if preview.Status != "ok" || preview.Placement == nil {
		t.Fatalf("preview stopped answering without a key: %+v", preview)
	}
	if preview.LaunchToken != "" {
		t.Fatal("a deployment with no signing key handed out a launch token")
	}

	// A token that is valid under the key an operator MEANT to configure is
	// still refused, because this process cannot check it.
	token := mint(t, previewSigningKey, claimsOf(f.cust.ID, preview.Placement))
	rec := f.launchRaw("preview-launch-unsigned", token)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", rec.Code, rec.Body)
	}
	if quotes, _ := f.dec.counts(); quotes != 1 {
		t.Fatalf("quotes = %d — an unauthenticatable launch reached the decision path", quotes)
	}
	f.assertNothingSpent(t)
}

// An EXPIRED but perfectly genuine token is the one a stale client presents: the
// decision may be byte-identical, so the digest still matches and only the
// window it was issued under can refuse it. It is answered with its own code —
// what the client must do is the same as for a changed placement, but why
// differs and a console should be able to say so — and refused before the
// reservation, the hold, the mesh key and the provider call.
func TestLaunchRefusesAnExpiredPreviewBeforeAnyMutation(t *testing.T) {
	f := newPreviewFixture(t)
	preview := decodePreview(t, f.preview(""))

	// The same unchanged decision, under a window central really did sign and
	// that really has closed.
	closed := claimsOf(f.cust.ID, preview.Placement)
	closed.IssuedAt = time.Now().UTC().Add(-2 * time.Hour)
	closed.ExpiresAt = closed.IssuedAt.Add(5 * time.Minute)

	rec := f.launchRaw("preview-launch-expired", mint(t, previewSigningKey, closed))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body)
	}
	if code := decodeCreate(t, rec).Code; code != PlacementExpiredCode {
		t.Fatalf("code = %q, want %q", code, PlacementExpiredCode)
	}
	// Nothing was recomputed, reserved, held, planned or provisioned: the expiry
	// is decided on the authenticated window alone. The preview's own quote is
	// the only one anything has cost.
	if quotes, plans := f.dec.counts(); quotes != 1 || plans != 0 {
		t.Fatalf("quotes=%d plans=%d — an expired preview reached the decision path", quotes, plans)
	}
	f.assertNothingSpent(t)

	// The idempotency key is unspent, so the same client can preview again and
	// resubmit under the window it is actually holding.
	fresh := decodePreview(t, f.preview(""))
	if got := decodeCreate(t, f.launch("preview-launch-expired", fresh.LaunchToken)); got.Status != "provisioning" {
		t.Fatalf("resubmitting under a live preview = %+v", got)
	}
}

// The launch is held to the AUTHENTICATED window, not the fresh one the
// verification computed — so the decision the decider is asked to commit
// carries the customer's own deadline into the last check before admission.
func TestLaunchNarrowsTheVerifiedWindowToTheAuthenticatedOne(t *testing.T) {
	f := newPreviewFixture(t)
	preview := decodePreview(t, f.preview(""))

	rec := f.launch("preview-launch-window", preview.LaunchToken)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	created := decodeCreate(t, rec)
	stored, err := f.store.GetWorkload(created.ID)
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if stored.Placement == nil || stored.Placement.Receipt == nil {
		t.Fatal("the workload records no placement receipt")
	}
	// Second resolution: the token carries the window as a Unix timestamp, which
	// is the value the launch is bound to.
	authenticated := preview.Placement.ExpiresAt.Truncate(time.Second)
	if got := stored.Placement.Receipt.ExpiresAt; !got.Equal(authenticated) {
		t.Fatalf("launched under window %s, authenticated %s — the verification's own"+
			" window would outlive the preview the customer held", got, authenticated)
	}
	// Narrowing the window must not disturb the decision it names.
	if stored.Placement.Receipt.Digest != preview.Placement.Digest {
		t.Fatalf("narrowing the window moved the digest: %q vs %q",
			stored.Placement.Receipt.Digest, preview.Placement.Digest)
	}
	if err := stored.Placement.Receipt.Validate(); err != nil {
		t.Fatalf("the persisted receipt no longer validates: %v", err)
	}
}

// A receipt central cannot vouch for is rendered as ABSENT, never as a decision
// it is standing behind. The compatibility fields stay readable either way.
func TestStoredPlacementDropsAnUnverifiableReceipt(t *testing.T) {
	tampered := &state.PlacementReceipt{
		Version:                state.PlacementReceiptVersion,
		Tenant:                 "cust_preview",
		CandidateSetVersion:    state.PlacementCandidateSetVersion,
		AvailabilityConfidence: state.PlacementAvailabilityCreateTimeOnly,
		Selected:               state.PlacementSelection{Provider: "linode", HourlyMicroUSD: 1},
		Digest:                 strings.Repeat("0", 64),
	}
	rendered := storedPlacementResponse(&state.WorkloadPlacement{
		RequestedClusterID: "cluster_preview", GrantedClusterID: "cluster_preview",
		Mode: state.ClusterPlacementModeAuto, Receipt: tampered,
	})
	if rendered == nil || rendered.GrantedClusterID != "cluster_preview" {
		t.Fatalf("the cluster placement stopped rendering: %+v", rendered)
	}
	if rendered.Receipt != nil {
		t.Fatal("a receipt whose digest does not cover it was rendered as a decision")
	}
}

func TestStoredPlacementResponseDerivesGPUProductWithoutChangingReceipt(t *testing.T) {
	receipt := &state.PlacementReceipt{
		Version: state.PlacementReceiptVersion,
		Constraints: state.PlacementConstraints{
			AccountMode: state.PlacementAccountPlatform,
		},
		Candidates: []state.PlacementCandidate{{
			Provider: "linode", SKU: "g2-gpu-rtx4000a1-s", Selected: true,
		}},
		Selected: state.PlacementSelection{
			Provider: "linode", SKU: "g2-gpu-rtx4000a1-s",
		},
		CandidateSetVersion:    state.PlacementCandidateSetVersion,
		AvailabilityConfidence: state.PlacementAvailabilityCreateTimeOnly,
	}
	if err := receipt.Seal("quote_gpu_product", time.Now().UTC(), time.Minute); err != nil {
		t.Fatal(err)
	}
	originalDigest := receipt.Digest

	rendered := storedPlacementResponse(&state.WorkloadPlacement{Receipt: receipt})
	if rendered == nil || rendered.GPUProduct != "NVIDIA RTX 4000 Ada" {
		t.Fatalf("gpu_product = %q, want NVIDIA RTX 4000 Ada", rendered.GPUProduct)
	}
	if receipt.Digest != originalDigest || receipt.Validate() != nil {
		t.Fatal("rendering gpu_product mutated or invalidated the sealed receipt")
	}
}
