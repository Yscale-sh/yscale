//go:build stripecanary

// yscale:proprietary

// TestStripeCanary is the ONE entry point for the Stripe test-mode canary.
// It never runs under `go test ./...`; the build tag stripecanary is set only
// by scripts/billing-canary.sh, which owns the wrapper responsibilities the
// test cannot perform inside a single go process (running the Stripe CLI
// listener bounded to allowed event types, and driving agent-browser through
// the hosted-checkout page in an isolated session).
//
// The canary is expressed as a Go integration test rather than a root-level
// cmd so it can legally import central/internal/billing and
// central/internal/handlers. A separate binary under cmd/ cannot cross the
// central/internal boundary, and duplicating the seams there would create a
// parallel billing surface the runbook explicitly forbids.
//
// A LoadConfig guard rejection is a HARD failure here. The tag stripecanary is
// only ever set by the wrapper, which is itself only ever run intentionally;
// a silent Skip would let a misconfigured invocation report green.

package billingcanary

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	stripe "github.com/stripe/stripe-go/v86"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestStripeCanary(t *testing.T) {
	cfg, err := LoadConfig(os.Getenv)
	if err != nil {
		// The build tag is only set by scripts/billing-canary.sh; any guard
		// rejection here is a real operator error. Skipping would report the
		// run green, which is exactly the failure mode this canary exists to
		// stop.
		t.Fatalf("LoadConfig: %v", err)
	}
	startedAt := time.Now().UTC()
	sink := NewEvidenceSink(startedAt)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	pool, err := pgxpool.New(ctx, cfg.DatabaseDSN)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := VerifyDatabase(ctx, pool, cfg.ExpectDatabaseName); err != nil {
		t.Fatalf("VerifyDatabase: %v", err)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS billing CASCADE`); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS billing CASCADE`); err != nil {
			t.Errorf("cleanup schema: %v", err)
		}
	})
	if err := billing.EnsureSchema(ctx, pool, false); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	billingStore := billing.NewStore(pool, false)

	gateway, err := billing.NewStripeGateway(billing.StripeConfig{
		SecretKey: cfg.StripeSecretKey, WebhookSecret: cfg.StripeWebhookSecret,
		AccountID: cfg.StripeAccountID, SuccessURL: cfg.SuccessURL, CancelURL: cfg.CancelURL,
		LiveMode: false, MinMicroUSD: cfg.MinMicroUSD, MaxMicroUSD: cfg.MaxMicroUSD,
	})
	if err != nil {
		t.Fatalf("NewStripeGateway: %v", err)
	}

	stateStore := state.New()
	// The discard logger keeps redacted diagnostics out of stdout: no default
	// slog handler is ever wired, so a stray Info/Debug call inside billing
	// cannot leak a provider identifier through the canary's stream.
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	fixture := setupTenants(t, stateStore, billingStore)

	// The webhook mux is wrapped in a capture middleware so the LIVE scenario
	// can, at the exact HTTP seam Stripe reaches, keep the signed request in
	// memory once and replay it through BillingWebhooks a second time. The
	// filter is bounded: only the tenant-A settlement event is captured.
	webhooks := &handlers.BillingWebhooks{Store: billingStore, Gateway: gateway, Log: log}
	capture := newWebhookCapture()
	accounts := &handlers.Accounts{
		Store: stateStore, Billing: billingStore,
		Checkout:            gateway,
		CheckoutMinMicroUSD: cfg.MinMicroUSD,
		CheckoutMaxMicroUSD: cfg.MaxMicroUSD,
		Resolver:            fixture.resolver,
		Issuer:              fixture.issuer,
		Log:                 log,
	}

	mux := http.NewServeMux()
	mux.Handle("POST /v1/tenants/{tenant_id}/billing/checkouts", http.HandlerFunc(accounts.HandleCreateTenantCheckout))
	mux.Handle("GET /v1/tenants/{tenant_id}/billing/checkouts/{id}", http.HandlerFunc(accounts.HandleGetTenantCheckout))
	mux.Handle("GET /v1/tenants/{tenant_id}/billing", http.HandlerFunc(accounts.HandleGetTenantBilling))
	mux.Handle("GET /v1/tenants/{tenant_id}/billing/statement", http.HandlerFunc(accounts.HandleGetTenantBillingStatement))
	mux.Handle("POST /v1/billing/webhooks/stripe", capture.wrap(http.HandlerFunc(webhooks.HandleStripe), fixture.tenantA))
	srv := newTestServer(t, mux, cfg.WebhookListenAddr)
	t.Cleanup(srv.Close)

	// Kill switch: a companion mux built with a nil Checkout leaves the
	// purchase route absent (404). The canary exercises the same handler seam
	// the runbook mandates and records the outcome without touching Stripe.
	killed := &handlers.Accounts{
		Store: stateStore, Billing: billingStore, Checkout: nil,
		Resolver: fixture.resolver, Issuer: fixture.issuer, Log: log,
	}
	killedMux := http.NewServeMux()
	killedMux.Handle("POST /v1/tenants/{tenant_id}/billing/checkouts", http.HandlerFunc(killed.HandleCreateTenantCheckout))
	killedSrv := httptest.NewServer(killedMux)
	t.Cleanup(killedSrv.Close)

	env := scenarioEnv{
		ctx: ctx, cfg: cfg, srv: srv, killedSrv: killedSrv, sink: sink,
		billing: billingStore, gateway: gateway, fixture: fixture, pool: pool,
		log: log, capture: capture,
	}
	// Live scenarios run FIRST so tenant A begins at a Stripe-authoritative
	// zero balance and no locally crafted webhook fixture can poison the
	// inbox for the settlement replay assertion. Deterministic scenarios run
	// after and cleanly reuse the fixture tenants.
	if cfg.LiveScenarioEnabled {
		runLiveScenarios(t, env)
	} else {
		record(t, sink, ScenarioResult{Name: "settle_exactly_once", Outcome: OutcomeSkipped, Note: "live sentinel absent"})
		record(t, sink, ScenarioResult{Name: "full_refund_exactly_once", Outcome: OutcomeSkipped, Note: "live sentinel absent"})
	}
	runDeterministicScenarios(t, env)

	if err := WriteFile(cfg.EvidencePath, sink.Snapshot(time.Now())); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

type scenarioEnv struct {
	ctx       context.Context
	cfg       Config
	srv       *httptest.Server
	killedSrv *httptest.Server
	sink      *EvidenceSink
	billing   *billing.Store
	gateway   *billing.StripeGateway
	fixture   *tenantFixture
	pool      *pgxpool.Pool
	log       *slog.Logger
	capture   *webhookCapture
}

type tenantFixture struct {
	issuer     string
	resolver   *stubResolver
	tenantA    string
	tenantB    string
	tokenAOwn  string
	tokenBOwn  string
	accountAID string
	accountBID string
}

// stubResolver stands in for Yscale ID's OIDC userinfo. It maps opaque
// process-only tokens to subjects; no live OIDC round-trip.
type stubResolver struct {
	subjects map[string]string
}

func (r *stubResolver) Resolve(_ context.Context, token string) (handlers.Identity, error) {
	sub, ok := r.subjects[token]
	if !ok {
		return handlers.Identity{}, handlers.ErrUnauthenticated
	}
	return handlers.Identity{Subject: sub, EmailVerified: true, Email: sub + "@example.test"}, nil
}

func setupTenants(t *testing.T, stateStore *state.Store, billingStore *billing.Store) *tenantFixture {
	t.Helper()
	const issuer = "https://id.example.test"
	tokenA, tokenB := "canary_owner_a", "canary_owner_b"
	resolver := &stubResolver{subjects: map[string]string{
		tokenA: "canary-subject-a", tokenB: "canary-subject-b",
	}}
	accountA, err := stateStore.UpsertAccount(issuer, "canary-subject-a", state.AccountProfile{Email: "canary-a@example.test"})
	if err != nil {
		t.Fatalf("UpsertAccount A: %v", err)
	}
	accountB, err := stateStore.UpsertAccount(issuer, "canary-subject-b", state.AccountProfile{Email: "canary-b@example.test"})
	if err != nil {
		t.Fatalf("UpsertAccount B: %v", err)
	}
	tenantA := &state.Customer{ID: "canary-tenant-a", Token: "canary_tenant_a_token", Plan: "pro"}
	tenantB := &state.Customer{ID: "canary-tenant-b", Token: "canary_tenant_b_token", Plan: "pro"}
	if _, _, err := stateStore.CreateTenant(tenantA, accountA.ID); err != nil {
		t.Fatalf("CreateTenant A: %v", err)
	}
	if _, _, err := stateStore.CreateTenant(tenantB, accountB.ID); err != nil {
		t.Fatalf("CreateTenant B: %v", err)
	}
	if err := billingStore.EnsureAccount(context.Background(), tenantA.ID); err != nil {
		t.Fatalf("EnsureAccount A: %v", err)
	}
	if err := billingStore.EnsureAccount(context.Background(), tenantB.ID); err != nil {
		t.Fatalf("EnsureAccount B: %v", err)
	}
	return &tenantFixture{
		issuer: issuer, resolver: resolver,
		tenantA: tenantA.ID, tenantB: tenantB.ID,
		tokenAOwn: tokenA, tokenBOwn: tokenB,
		accountAID: accountA.ID, accountBID: accountB.ID,
	}
}

func runDeterministicScenarios(t *testing.T, env scenarioEnv) {
	scenarioCrossTenantDenied(t, env)
	scenarioInvalidSignatureRejected(t, env)
	scenarioKillSwitchAbsent(t, env)
	scenarioDuplicateDeliveryNoDoubleCredit(t, env)
	scenarioStatementArithmetic(t, env)
	scenarioExternalReconciliationHealthy(t, env)
}

// scenarioCrossTenantDenied asserts the checkout route refuses to mint a
// session for a tenant the caller has no membership on. The signed-in token
// belongs to tenant A; the URL targets tenant B.
func scenarioCrossTenantDenied(t *testing.T, env scenarioEnv) {
	start := time.Now()
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"amount_micro_usd":%d,"idempotency_key":"canary-cross-tenant"}`, env.cfg.CheckoutAmountMicroUSD)))
	req, _ := http.NewRequestWithContext(env.ctx, http.MethodPost,
		env.srv.URL+"/v1/tenants/"+env.fixture.tenantB+"/billing/checkouts", body)
	req.Header.Set("Authorization", "Bearer "+env.fixture.tokenAOwn)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fail(t, env.sink, "cross_tenant_denied", start, "request failed")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		fail(t, env.sink, "cross_tenant_denied", start, "status was not 404")
		return
	}
	record(t, env.sink, ScenarioResult{
		Name: "cross_tenant_denied", Outcome: OutcomePass,
		DurationMS: sinceMS(start), Counts: map[string]int64{"status": int64(resp.StatusCode)},
	})
}

// scenarioInvalidSignatureRejected asserts the webhook route rejects a POST
// whose Stripe-Signature does not verify against the gateway secret.
func scenarioInvalidSignatureRejected(t *testing.T, env scenarioEnv) {
	start := time.Now()
	body := []byte(`{"id":"evt_bogus","type":"checkout.session.completed","data":{"object":{"id":"cs_bogus"}}}`)
	req, _ := http.NewRequestWithContext(env.ctx, http.MethodPost,
		env.srv.URL+"/v1/billing/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", "t=0,v1=deadbeef")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fail(t, env.sink, "invalid_signature_rejected", start, "request failed")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		fail(t, env.sink, "invalid_signature_rejected", start, "status was not 400")
		return
	}
	record(t, env.sink, ScenarioResult{
		Name: "invalid_signature_rejected", Outcome: OutcomePass,
		DurationMS: sinceMS(start), Counts: map[string]int64{"status": int64(resp.StatusCode)},
	})
}

// scenarioKillSwitchAbsent asserts the checkout route disappears (404) when
// the Accounts handler is wired without a CheckoutGateway. This is the same
// kill switch stripeCheckoutFromEnv enforces at boot.
func scenarioKillSwitchAbsent(t *testing.T, env scenarioEnv) {
	start := time.Now()
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"amount_micro_usd":%d,"idempotency_key":"canary-kill-switch"}`, env.cfg.CheckoutAmountMicroUSD)))
	req, _ := http.NewRequestWithContext(env.ctx, http.MethodPost,
		env.killedSrv.URL+"/v1/tenants/"+env.fixture.tenantA+"/billing/checkouts", body)
	req.Header.Set("Authorization", "Bearer "+env.fixture.tokenAOwn)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fail(t, env.sink, "kill_switch_absent", start, "request failed")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		fail(t, env.sink, "kill_switch_absent", start, "status was not 404")
		return
	}
	record(t, env.sink, ScenarioResult{
		Name: "kill_switch_absent", Outcome: OutcomePass,
		DurationMS: sinceMS(start),
		Flags:      map[string]bool{"route_absent": true},
	})
}

// scenarioDuplicateDeliveryNoDoubleCredit signs the same well-formed webhook
// twice against tenant B and confirms the RecordVerifiedWebhook path dedupes
// the second insert without granting credit twice. Tenant B is used here so
// the LIVE scenario's tenant-A assertions on the same seam are unaffected.
func scenarioDuplicateDeliveryNoDoubleCredit(t *testing.T, env scenarioEnv) {
	start := time.Now()
	eventTime := time.Now().UTC().Truncate(time.Second)
	sessionID := "cs_test_canary_dupe_" + fmt.Sprintf("%d", eventTime.Unix())
	tenant := env.fixture.tenantB
	object := map[string]any{
		"id":                  sessionID,
		"client_reference_id": tenant,
		"metadata": map[string]string{
			"yscale_customer_id": tenant,
			"yscale_checkout_id": "co_canary_dupe_" + fmt.Sprintf("%d", eventTime.Unix()),
		},
	}
	event := map[string]any{
		"id":          fmt.Sprintf("evt_canary_dupe_%d", eventTime.Unix()),
		"object":      "event",
		"api_version": stripe.APIVersion,
		"type":        "checkout.session.completed",
		"created":     eventTime.Unix(),
		"livemode":    false,
		"account":     env.cfg.StripeAccountID,
		"data":        map[string]any{"object": object},
	}
	payload, err := json.Marshal(event)
	if err != nil {
		fail(t, env.sink, "duplicate_delivery_no_double_credit", start, "encode event")
		return
	}
	sig := stripeSignature(payload, env.cfg.StripeWebhookSecret, eventTime)
	deliver := func() (int, bool) {
		req, _ := http.NewRequestWithContext(env.ctx, http.MethodPost,
			env.srv.URL+"/v1/billing/webhooks/stripe", bytes.NewReader(payload))
		req.Header.Set("Stripe-Signature", sig)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, false
		}
		defer resp.Body.Close()
		var decoded struct {
			Received bool `json:"received"`
			Inserted bool `json:"inserted"`
		}
		body, _ := io.ReadAll(resp.Body)
		_ = json.Unmarshal(body, &decoded)
		return resp.StatusCode, decoded.Inserted
	}
	status1, inserted1 := deliver()
	status2, inserted2 := deliver()
	if status1 != http.StatusOK || status2 != http.StatusOK {
		fail(t, env.sink, "duplicate_delivery_no_double_credit", start, "webhook returned non-200")
		return
	}
	if !inserted1 || inserted2 {
		fail(t, env.sink, "duplicate_delivery_no_double_credit", start, "dedup did not flip inserted flag")
		return
	}
	acct, err := env.billing.GetAccount(env.ctx, tenant)
	if err != nil {
		fail(t, env.sink, "duplicate_delivery_no_double_credit", start, "read account")
		return
	}
	if acct.BalanceMicroUSD != 0 {
		fail(t, env.sink, "duplicate_delivery_no_double_credit", start, "balance mutated by dedup path")
		return
	}
	record(t, env.sink, ScenarioResult{
		Name: "duplicate_delivery_no_double_credit", Outcome: OutcomePass,
		DurationMS: sinceMS(start),
		Counts: map[string]int64{
			"deliveries_accepted": 2, "inbox_rows_created": 1,
		},
	})
}

// scenarioStatementArithmetic seeds a small manual service-credit grant on
// tenant B, reads the statement over the enclosing window, and asserts the
// closing net credit equals the seeded amount and the balance materialisation
// agrees with the statement watermark.
func scenarioStatementArithmetic(t *testing.T, env scenarioEnv) {
	start := time.Now()
	tenant := env.fixture.tenantB
	amount := env.cfg.CheckoutAmountMicroUSD
	key := fmt.Sprintf("canary-statement-%d", start.UnixNano())
	if err := env.billing.GrantCredit(env.ctx, billing.CreditGrantRequest{
		CustomerID: tenant, AmountMicroUSD: amount, IdempotencyKey: key,
		Provider: "yscale", ProviderAccountID: "canary", LiveMode: false,
		PaymentObjectID: "svc:" + key,
	}); err != nil {
		fail(t, env.sink, "statement_arithmetic", start, "grant credit")
		return
	}
	from := start.Add(-time.Hour).UTC()
	to := start.Add(time.Hour).UTC()
	statement, err := env.billing.GetStatement(env.ctx, tenant, from, to)
	if err != nil {
		fail(t, env.sink, "statement_arithmetic", start, "get statement")
		return
	}
	if statement.ClosingNetCreditMicroUSD-statement.OpeningNetCreditMicroUSD != amount {
		fail(t, env.sink, "statement_arithmetic", start, "closing minus opening mismatch")
		return
	}
	acct, err := env.billing.GetAccount(env.ctx, tenant)
	if err != nil {
		fail(t, env.sink, "statement_arithmetic", start, "read account")
		return
	}
	if acct.BalanceMicroUSD-acct.DebtMicroUSD != statement.ClosingNetCreditMicroUSD {
		fail(t, env.sink, "statement_arithmetic", start, "materialised vs statement drift")
		return
	}
	record(t, env.sink, ScenarioResult{
		Name: "statement_arithmetic", Outcome: OutcomePass,
		DurationMS: sinceMS(start),
		Counts: map[string]int64{
			"entries":                         int64(len(statement.Entries)),
			"closing_matches_materialization": 1,
		},
	})
}

// fixedCashProvider is a canary-only ExternalCashProvider that returns the
// snapshot the reconciler expects. The provider identifiers stay in memory
// and never appear in evidence — only counts do.
type fixedCashProvider struct {
	objects []billing.ExternalCashObject
}

func (f fixedCashProvider) ExternalCashSnapshot(_ context.Context, max int) ([]billing.ExternalCashObject, error) {
	if len(f.objects) > max {
		return nil, fmt.Errorf("canary cash snapshot exceeds cap")
	}
	return f.objects, nil
}

// localExternalCashSnapshot gives the deterministic provider fixture the
// complete external-funding history already present in this ephemeral test
// database. Live mode runs first and leaves its fully reversed funding row in
// the ledger after removing the shared-account metadata; omitting that row
// here would manufacture a ledger-only difference unrelated to the healthy
// comparator scenario being exercised.
func localExternalCashSnapshot(ctx context.Context, pool *pgxpool.Pool) ([]billing.ExternalCashObject, error) {
	const maxObjects = 1000
	rows, err := pool.Query(ctx, `
		SELECT provider, provider_account_id, livemode, payment_object_id,
		       customer_id, credited_micro_usd, reversed_micro_usd
		FROM billing.funding_sources
		WHERE provider <> 'yscale'
		ORDER BY provider, provider_account_id, livemode, payment_object_id
		LIMIT $1`, maxObjects+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	objects := make([]billing.ExternalCashObject, 0)
	for rows.Next() {
		var object billing.ExternalCashObject
		if err := rows.Scan(&object.Provider, &object.ProviderAccountID, &object.LiveMode,
			&object.PaymentObjectID, &object.CustomerID, &object.CreditedMicroUSD,
			&object.ReversedMicroUSD); err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(objects) > maxObjects {
		return nil, fmt.Errorf("canary local cash snapshot exceeds cap")
	}
	return objects, nil
}

// scenarioExternalReconciliationHealthy seeds a stripe-source credit on
// tenant B and hands the reconciler a provider snapshot that matches the
// funding row. Tenant B keeps this deterministic assertion isolated from any
// live cash on tenant A. A healthy summary is TotalDifferences() == 0.
func scenarioExternalReconciliationHealthy(t *testing.T, env scenarioEnv) {
	start := time.Now()
	tenant := env.fixture.tenantB
	amount := env.cfg.CheckoutAmountMicroUSD
	paymentID := fmt.Sprintf("pi_canary_%d", start.UnixNano())
	key := fmt.Sprintf("canary-recon-%d", start.UnixNano())
	if err := env.billing.GrantCredit(env.ctx, billing.CreditGrantRequest{
		CustomerID: tenant, AmountMicroUSD: amount, IdempotencyKey: key,
		Provider: "stripe", ProviderAccountID: env.cfg.StripeAccountID, LiveMode: false,
		PaymentObjectID: paymentID,
	}); err != nil {
		fail(t, env.sink, "external_reconciliation_healthy", start, "seed grant")
		return
	}
	objects, err := localExternalCashSnapshot(env.ctx, env.pool)
	if err != nil {
		fail(t, env.sink, "external_reconciliation_healthy", start, "read local cash fixture")
		return
	}
	provider := fixedCashProvider{objects: objects}
	summary, err := env.billing.ReconcileExternalCash(env.ctx, provider)
	if err != nil {
		fail(t, env.sink, "external_reconciliation_healthy", start, "reconcile")
		return
	}
	if summary.TotalDifferences() != 0 {
		fail(t, env.sink, "external_reconciliation_healthy", start, "reconcile reported drift")
		return
	}
	record(t, env.sink, ScenarioResult{
		Name: "external_reconciliation_healthy", Outcome: OutcomePass,
		DurationMS: sinceMS(start),
		Flags:      map[string]bool{"reconciler_healthy": true},
	})
}

// runLiveScenarios drives the browser-controlled hosted-checkout completion,
// exercises the exact-once webhook contract, reconciles Stripe cash against
// the ledger, drives the full refund through the real StripeGateway seam,
// reconciles again, and then guarantees Stripe has no Yscale-tagged residue.
//
// This is the ONLY path that talks to Stripe. It uses ONE browser handoff
// (checkout completion); the refund and every metadata/customer/session
// cleanup step is executed directly by a private stripe-go client in this
// process, so the shell wrapper never needs to know a PaymentIntent ID.
func runLiveScenarios(t *testing.T, env scenarioEnv) {
	settleStart := time.Now()

	// Preflight: the shared Stripe test account currently has no
	// Yscale-tagged cash. A stray object here means either a previous run
	// leaked, or another team is using our metadata namespace — either way
	// the canary must not add another object on top and must not proceed.
	pre, err := env.gateway.ExternalCashSnapshot(env.ctx, 1000)
	if err != nil {
		fail(t, env.sink, "settle_exactly_once", settleStart, "preflight snapshot failed")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "preflight snapshot failed")
		return
	}
	if len(pre) != 0 {
		fail(t, env.sink, "settle_exactly_once", settleStart, "shared account has residual yscale-tagged cash")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "preflight rejected")
		return
	}

	tenant := env.fixture.tenantA
	// The live tenant must begin at zero — deterministic fixtures run AFTER
	// this scenario, so nothing in the ledger has touched tenant A yet. We
	// still verify explicitly: a nonzero balance here means a residue from
	// EnsureAccount or a prior canary run and the settlement assertion
	// could not be trusted.
	if pre, err := env.billing.GetAccount(env.ctx, tenant); err != nil {
		fail(t, env.sink, "settle_exactly_once", settleStart, "read pre-balance")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	} else if pre.BalanceMicroUSD != 0 {
		fail(t, env.sink, "settle_exactly_once", settleStart, "tenant did not begin at zero")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}

	handoff, err := NewBrowserHandoff(env.cfg.ControlDir)
	if err != nil {
		fail(t, env.sink, "settle_exactly_once", settleStart, "browser handoff init")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "browser handoff init")
		return
	}
	defer handoff.Cleanup()
	cleaner := &stripeCleanup{
		client: stripe.NewClient(env.cfg.StripeSecretKey), gateway: env.gateway,
		startedAt: settleStart, yscaleCustomerID: tenant,
	}
	defer cleaner.run(t, env.sink)

	idem := fmt.Sprintf("canary-live-%d", settleStart.UnixNano())
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"amount_micro_usd":%d,"idempotency_key":%q}`,
		env.cfg.CheckoutAmountMicroUSD, idem)))
	req, _ := http.NewRequestWithContext(env.ctx, http.MethodPost,
		env.srv.URL+"/v1/tenants/"+tenant+"/billing/checkouts", body)
	req.Header.Set("Authorization", "Bearer "+env.fixture.tokenAOwn)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fail(t, env.sink, "settle_exactly_once", settleStart, "create checkout")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}
	var created struct {
		ID  string `json:"id"`
		URL string `json:"url"`
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated || json.Unmarshal(raw, &created) != nil || created.URL == "" {
		fail(t, env.sink, "settle_exactly_once", settleStart, "checkout response invalid")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}

	// Deferred cleanup runs regardless of what fails below. It looks the
	// PaymentIntent up from the ledger row the settled webhook created (or,
	// if we never got that far, from the CheckoutSession the wrapper
	// published), refunds any unrefunded balance, clears yscale metadata off
	// the Checkout Session, PaymentIntent, and copied Charge, expires an
	// incomplete session, deletes the created test customer, and asserts
	// via a final ExternalCashSnapshot that no Yscale-tagged object remains.
	// Every cleanup error is a test failure — a silent || true would let a
	// leaked identifier survive on the shared account.
	//
	// The CheckoutSession ID is seeded IMMEDIATELY: BeginCheckout has already
	// created a live Stripe object, and any early-return failure below still
	// has to clean it up.
	if sessionID, err := lookupCheckoutSessionID(env.ctx, env.pool, tenant); err == nil {
		cleaner.sessionID = sessionID
	}

	if err := handoff.PublishCheckoutURL(created.URL); err != nil {
		fail(t, env.sink, "settle_exactly_once", settleStart, "publish url")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}
	ok, err := handoff.WaitForCompletion(env.ctx, time.Duration(env.cfg.LiveTimeoutSeconds)*time.Second)
	if err != nil || !ok {
		fail(t, env.sink, "settle_exactly_once", settleStart, "browser did not confirm")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}

	processor := &billing.WebhookProcessor{Store: env.billing, Resolver: env.gateway, Log: env.log}
	// Wait until the settlement webhook produces a nonzero balance on tenant
	// A. The poll returns as soon as the expected state is reached and
	// propagates any processor error — no fixed 30s sleep. The upper bound
	// is the live browser timeout, which already gives the wrapper enough
	// slack to complete.
	pollBudget := time.Duration(env.cfg.LiveTimeoutSeconds) * time.Second
	if err := pollUntilBalance(env.ctx, env.billing, processor, tenant, env.cfg.CheckoutAmountMicroUSD, pollBudget); err != nil {
		fail(t, env.sink, "settle_exactly_once", settleStart, "did not settle to expected balance")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}

	// Exact-once replay: the capture middleware kept the signed
	// checkout.session.completed request in memory. Feeding that same body
	// and signature through the real BillingWebhooks handler a second time
	// must return inserted=false and MUST NOT double-grant.
	replayed := env.capture.replay()
	if replayed == nil {
		fail(t, env.sink, "settle_exactly_once", settleStart, "no settlement webhook was captured")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}
	replayReq, _ := http.NewRequestWithContext(env.ctx, http.MethodPost,
		env.srv.URL+"/v1/billing/webhooks/stripe", bytes.NewReader(replayed.body))
	replayReq.Header.Set("Stripe-Signature", replayed.signature)
	replayReq.Header.Set("Content-Type", "application/json")
	replayResp, err := http.DefaultClient.Do(replayReq)
	if err != nil {
		fail(t, env.sink, "settle_exactly_once", settleStart, "replay request failed")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}
	var replayDecoded struct {
		Received bool `json:"received"`
		Inserted bool `json:"inserted"`
	}
	replayBody, _ := io.ReadAll(replayResp.Body)
	replayResp.Body.Close()
	_ = json.Unmarshal(replayBody, &replayDecoded)
	if replayResp.StatusCode != http.StatusOK {
		fail(t, env.sink, "settle_exactly_once", settleStart, "replay was not accepted")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}
	if replayDecoded.Inserted {
		fail(t, env.sink, "settle_exactly_once", settleStart, "replay inserted a duplicate")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}
	// Drive one more processor pass and confirm the balance did not double.
	// pollUntilBalance returns immediately when the current balance already
	// matches the expected value.
	if err := pollUntilBalance(env.ctx, env.billing, processor, tenant, env.cfg.CheckoutAmountMicroUSD, 5*time.Second); err != nil {
		fail(t, env.sink, "settle_exactly_once", settleStart, "post-replay balance drift")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}

	// Reconcile Stripe cash against the ledger BEFORE any cleanup mutates
	// metadata. A nonzero difference here means the settled webhook did not
	// map to the exact funding row and admissions upstream would freeze.
	if err := requireReconciled(env.ctx, env.billing, env.gateway); err != nil {
		fail(t, env.sink, "settle_exactly_once", settleStart, "post-settle reconciler reported drift")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}

	// Discover the PaymentIntent so the refund can be issued in-process. The
	// value is used only to call Stripe and to seed the cleaner; it never
	// touches stdout or evidence.
	paymentIntent, err := lookupSettledPaymentIntent(env.ctx, env.pool, env.cfg.StripeAccountID, tenant)
	if err != nil || paymentIntent == "" {
		fail(t, env.sink, "settle_exactly_once", settleStart, "look up payment intent")
		fail(t, env.sink, "full_refund_exactly_once", settleStart, "prerequisite failed")
		return
	}
	cleaner.paymentIntent = paymentIntent

	record(t, env.sink, ScenarioResult{
		Name: "settle_exactly_once", Outcome: OutcomePass, DurationMS: sinceMS(settleStart),
		Flags: map[string]bool{"replay_deduped": true, "reconciled": true},
	})

	// Refund the full amount via the production Stripe surface. The
	// StripeGateway itself is the business seam; the canary uses a private
	// stripe-go client only as test orchestration (issuing the refund the
	// way a support agent would from the dashboard).
	refundStart := time.Now()
	refund, err := cleaner.client.V1Refunds.Create(env.ctx, &stripe.RefundCreateParams{
		PaymentIntent: stripe.String(paymentIntent),
	})
	if err != nil || refund == nil || refund.ID == "" {
		fail(t, env.sink, "full_refund_exactly_once", refundStart, "create refund")
		return
	}
	if err := pollUntilBalance(env.ctx, env.billing, processor, tenant, 0, pollBudget); err != nil {
		fail(t, env.sink, "full_refund_exactly_once", refundStart, "refund did not zero balance")
		return
	}
	if err := requireReconciled(env.ctx, env.billing, env.gateway); err != nil {
		fail(t, env.sink, "full_refund_exactly_once", refundStart, "post-refund reconciler reported drift")
		return
	}
	record(t, env.sink, ScenarioResult{
		Name: "full_refund_exactly_once", Outcome: OutcomePass, DurationMS: sinceMS(refundStart),
		Flags: map[string]bool{"balance_zeroed": true, "reconciled": true},
	})
}

// stripeCleanup owns every teardown step the canary must perform on the
// shared Stripe test account. Its run method is the deferred cleanup path
// and must record every failure — no silent || true short-circuits.
type stripeCleanup struct {
	client           *stripe.Client
	gateway          *billing.StripeGateway
	startedAt        time.Time
	yscaleCustomerID string
	paymentIntent    string
	sessionID        string
}

func (c *stripeCleanup) run(t *testing.T, sink *EvidenceSink) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Cleanup is deliberately best-effort across every step. One failed API
	// call must not prevent the remaining metadata clears or customer delete;
	// the scenario still fails at the end if any step failed.
	failures := int64(0)
	markFailure := func() { failures++ }
	paymentIntents := make(map[string]struct{})
	customers := make(map[string]struct{})
	if c.paymentIntent != "" {
		paymentIntents[c.paymentIntent] = struct{}{}
	}

	// Resolve provider objects from the Checkout Session first. If the local
	// AttachCheckout/response path failed after Stripe created the session,
	// there is no provider_session_id to seed the cleaner; recover it with a
	// bounded, run-window metadata sweep instead.
	sessionIDs := make([]string, 0, 1)
	if c.sessionID != "" {
		sessionIDs = append(sessionIDs, c.sessionID)
	} else if discovered, err := c.matchingSessionIDs(ctx); err != nil {
		markFailure()
	} else {
		sessionIDs = append(sessionIDs, discovered...)
	}
	for _, sessionID := range sessionIDs {
		retrieve := &stripe.CheckoutSessionRetrieveParams{}
		retrieve.AddExpand("payment_intent.latest_charge")
		retrieve.AddExpand("customer")
		session, err := c.client.V1CheckoutSessions.Retrieve(ctx, sessionID, retrieve)
		if err != nil || session == nil {
			markFailure()
		} else {
			if session.PaymentIntent != nil && session.PaymentIntent.ID != "" {
				paymentIntents[session.PaymentIntent.ID] = struct{}{}
			}
			if session.Customer != nil && session.Customer.ID != "" {
				customers[session.Customer.ID] = struct{}{}
			}
		}

		updated, err := c.client.V1CheckoutSessions.Update(ctx, sessionID, &stripe.CheckoutSessionUpdateParams{
			Metadata: map[string]string{"yscale_checkout_id": "", "yscale_customer_id": ""},
		})
		if err != nil || updated == nil || hasYscaleMetadata(updated.Metadata) {
			markFailure()
		}
		if session != nil && session.Status == stripe.CheckoutSessionStatusOpen {
			if _, err := c.client.V1CheckoutSessions.Expire(ctx, sessionID, nil); err != nil {
				markFailure()
			}
		}
	}

	for paymentIntent := range paymentIntents {
		retrieve := &stripe.PaymentIntentRetrieveParams{}
		retrieve.AddExpand("latest_charge")
		retrieve.AddExpand("customer")
		intent, err := c.client.V1PaymentIntents.Retrieve(ctx, paymentIntent, retrieve)
		if err != nil || intent == nil {
			markFailure()
		} else {
			if intent.Customer != nil && intent.Customer.ID != "" {
				customers[intent.Customer.ID] = struct{}{}
			}
			if intent.LatestCharge != nil && intent.LatestCharge.AmountCaptured > intent.LatestCharge.AmountRefunded {
				remaining := intent.LatestCharge.AmountCaptured - intent.LatestCharge.AmountRefunded
				if _, err := c.client.V1Refunds.Create(ctx, &stripe.RefundCreateParams{
					PaymentIntent: stripe.String(paymentIntent), Amount: stripe.Int64(remaining),
				}); err != nil {
					markFailure()
				}
			}

			updated, err := c.client.V1PaymentIntents.Update(ctx, paymentIntent, &stripe.PaymentIntentUpdateParams{
				Metadata: map[string]string{"yscale_checkout_id": "", "yscale_customer_id": ""},
			})
			if err != nil || updated == nil || hasYscaleMetadata(updated.Metadata) {
				markFailure()
			}
			if intent.LatestCharge != nil && intent.LatestCharge.ID != "" {
				updatedCharge, err := c.client.V1Charges.Update(ctx, intent.LatestCharge.ID, &stripe.ChargeUpdateParams{
					Metadata: map[string]string{"yscale_checkout_id": "", "yscale_customer_id": ""},
				})
				if err != nil || updatedCharge == nil || hasYscaleMetadata(updatedCharge.Metadata) {
					markFailure()
				}
			}
		}
	}

	for customerID := range customers {
		deleted, err := c.client.V1Customers.Delete(ctx, customerID, nil)
		if err != nil || deleted == nil || !deleted.Deleted {
			markFailure()
		}
	}

	post, err := c.gateway.ExternalCashSnapshot(ctx, 1000)
	postEmpty := err == nil && len(post) == 0
	if !postEmpty {
		markFailure()
	}
	remainingSessions, sessionErr := c.matchingSessionIDs(ctx)
	sessionMetadataEmpty := sessionErr == nil && len(remainingSessions) == 0
	if !sessionMetadataEmpty {
		markFailure()
	}

	result := ScenarioResult{
		Name: "cleanup_no_residue", Outcome: OutcomePass, DurationMS: sinceMS(start),
		Flags: map[string]bool{
			"post_snapshot_empty": postEmpty, "session_metadata_empty": sessionMetadataEmpty,
		},
	}
	if failures != 0 {
		result.Outcome = OutcomeFail
		result.Note = "one or more cleanup steps failed"
		result.Counts = map[string]int64{"failed_steps": failures}
	}
	record(t, sink, result)
	if failures != 0 {
		t.Errorf("scenario cleanup_no_residue failed")
	}
}

func (c *stripeCleanup) matchingSessionIDs(ctx context.Context) ([]string, error) {
	if c.client == nil || c.yscaleCustomerID == "" || c.startedAt.IsZero() {
		return nil, fmt.Errorf("canary cleanup session scan is not configured")
	}
	const maxSessions = 1000
	params := &stripe.CheckoutSessionListParams{
		ListParams: stripe.ListParams{Limit: stripe.Int64(100)},
		CreatedRange: &stripe.RangeQueryParams{
			GreaterThanOrEqual: c.startedAt.Add(-2 * time.Minute).Unix(),
		},
	}
	ids := make([]string, 0, 1)
	scanned := 0
	for session, err := range c.client.V1CheckoutSessions.List(ctx, params).All(ctx) {
		if err != nil {
			return nil, err
		}
		scanned++
		if scanned > maxSessions {
			return nil, fmt.Errorf("canary cleanup session scan exceeds cap")
		}
		if session == nil {
			return nil, fmt.Errorf("canary cleanup session scan returned nil object")
		}
		customerTag := session.Metadata["yscale_customer_id"]
		checkoutTag := session.Metadata["yscale_checkout_id"]
		if customerTag == c.yscaleCustomerID {
			if checkoutTag == "" || session.ID == "" {
				return nil, fmt.Errorf("canary cleanup found partial session identity")
			}
			ids = append(ids, session.ID)
			continue
		}
		if customerTag == "" && checkoutTag != "" {
			return nil, fmt.Errorf("canary cleanup found ambiguous session identity")
		}
	}
	return ids, nil
}

func hasYscaleMetadata(metadata map[string]string) bool {
	return metadata["yscale_checkout_id"] != "" || metadata["yscale_customer_id"] != ""
}

// webhookCapture wraps the /v1/billing/webhooks/stripe seam and keeps at most
// ONE settlement request for the target tenant in memory. It filters on the
// Yscale identity tag so the deterministic scenarios (which run after live
// and target tenant B) cannot displace or observe the captured request.
type webhookCapture struct {
	mu     sync.Mutex
	stored *capturedWebhook
}

type capturedWebhook struct {
	body      []byte
	signature string
}

func newWebhookCapture() *webhookCapture { return &webhookCapture{} }

func (c *webhookCapture) wrap(next http.Handler, tenantA string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err == nil {
			signature := r.Header.Get("Stripe-Signature")
			c.maybeStore(body, signature, tenantA)
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next.ServeHTTP(w, r)
	})
}

func (c *webhookCapture) maybeStore(body []byte, signature, tenantA string) {
	var envelope struct {
		Type string `json:"type"`
		Data struct {
			Object struct {
				Metadata map[string]string `json:"metadata"`
			} `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return
	}
	if envelope.Type != "checkout.session.completed" {
		return
	}
	if envelope.Data.Object.Metadata["yscale_customer_id"] != tenantA {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stored != nil {
		return
	}
	stored := &capturedWebhook{
		body:      append([]byte(nil), body...),
		signature: signature,
	}
	c.stored = stored
}

func (c *webhookCapture) replay() *capturedWebhook {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stored
}

// requireReconciled runs Store.ReconcileExternalCash against the real
// StripeGateway and fails if TotalDifferences() is nonzero.
func requireReconciled(ctx context.Context, store *billing.Store, gateway *billing.StripeGateway) error {
	summary, err := store.ReconcileExternalCash(ctx, gateway)
	if err != nil {
		return err
	}
	if summary.TotalDifferences() != 0 {
		return fmt.Errorf("reconciler reported %d differences", summary.TotalDifferences())
	}
	return nil
}

// pollUntilBalance drains the WebhookProcessor and polls the ledger until the
// tenant's balance equals want, or budget elapses. It returns as soon as the
// expected state is observed; any processor error propagates immediately.
func pollUntilBalance(ctx context.Context, store *billing.Store, processor *billing.WebhookProcessor, tenant string, want int64, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	for {
		acct, err := store.GetAccount(ctx, tenant)
		if err != nil {
			return err
		}
		if acct.BalanceMicroUSD == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("balance %d did not reach %d within %s", acct.BalanceMicroUSD, want, budget)
		}
		processed, err := processor.ProcessOne(ctx)
		if err != nil {
			return err
		}
		if !processed {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// newTestServer starts an httptest.Server bound either to the caller-supplied
// loopback address (live mode) or a kernel-assigned port (deterministic mode).
// The wrapper script needs a fixed address so its Stripe CLI listener can
// forward signed events at a URL the operator whitelisted.
func newTestServer(t *testing.T, handler http.Handler, addr string) *httptest.Server {
	t.Helper()
	if addr == "" {
		return httptest.NewServer(handler)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("bind %s: %v", addr, err)
	}
	srv := httptest.NewUnstartedServer(handler)
	_ = srv.Listener.Close()
	srv.Listener = ln
	srv.Start()
	return srv
}

// lookupSettledPaymentIntent finds the PaymentIntent identifier the
// WebhookProcessor recorded for this tenant's live purchase.
func lookupSettledPaymentIntent(ctx context.Context, pool *pgxpool.Pool, providerAccountID, tenant string) (string, error) {
	var id string
	err := pool.QueryRow(ctx, `
		SELECT payment_object_id
		FROM billing.funding_sources
		WHERE customer_id=$1 AND provider='stripe' AND provider_account_id=$2 AND livemode=false
		ORDER BY id DESC LIMIT 1`, tenant, providerAccountID).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

// lookupCheckoutSessionID finds the Stripe Checkout Session identifier for a
// tenant's most recent checkout. The value is used only to seed the deferred
// cleaner (so it can expire an open session and clear metadata) and never
// appears in stdout or evidence.
func lookupCheckoutSessionID(ctx context.Context, pool *pgxpool.Pool, tenant string) (string, error) {
	var id string
	err := pool.QueryRow(ctx, `
		SELECT provider_session_id
		FROM billing.checkout_sessions
		WHERE customer_id=$1
		ORDER BY id DESC LIMIT 1`, tenant).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

func sinceMS(start time.Time) int64 { return time.Since(start).Milliseconds() }

func record(t *testing.T, sink *EvidenceSink, result ScenarioResult) {
	if err := sink.Add(result); err != nil {
		t.Fatalf("evidence add %s: %v", result.Name, err)
	}
}

func fail(t *testing.T, sink *EvidenceSink, name string, start time.Time, note string) {
	safe := SafeNote(note)
	if err := sink.Add(ScenarioResult{Name: name, Outcome: OutcomeFail, DurationMS: sinceMS(start), Note: safe}); err != nil {
		t.Fatalf("evidence add %s: %v", name, err)
	}
	t.Errorf("scenario %s failed: %s", name, safe)
}
