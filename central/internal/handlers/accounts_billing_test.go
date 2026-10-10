// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/state"
)

type billingFake struct {
	account        billing.Account
	holds          []billing.Hold
	grant          billing.CreditGrantRequest
	checkout       billing.Checkout
	statement      billing.Statement
	begin          billing.BeginCheckoutRequest
	attached       billing.ProviderCheckoutSession
	reads          int
	grants         int
	begins         int
	checkoutReads  int
	statementReads int
	err            error
}

type checkoutGatewayFake struct {
	store   *billingFake
	request billing.ProviderCheckoutRequest
	session billing.ProviderCheckoutSession
	calls   int
	err     error
}

func (f *checkoutGatewayFake) ProviderAccountID() string { return "acct_test" }
func (f *checkoutGatewayFake) LiveMode() bool            { return false }
func (f *checkoutGatewayFake) CreateCheckout(_ context.Context, request billing.ProviderCheckoutRequest) (billing.ProviderCheckoutSession, error) {
	if f.store != nil && f.store.begins == 0 {
		return billing.ProviderCheckoutSession{}, errors.New("provider called before durable intent")
	}
	f.calls++
	f.request = request
	if f.session.ID == "" {
		f.session = billing.ProviderCheckoutSession{ID: "cs_test", URL: "https://checkout.stripe.com/c/pay/test"}
	}
	return f.session, f.err
}
func (f *checkoutGatewayFake) VerifyWebhook([]byte, string) (billing.VerifiedWebhookEvent, error) {
	return billing.VerifiedWebhookEvent{}, errors.New("unused")
}
func (f *checkoutGatewayFake) ResolveWebhook(context.Context, billing.WebhookDelivery) (billing.WebhookResolution, error) {
	return billing.WebhookResolution{}, errors.New("unused")
}

func (f *billingFake) EnsureAccount(context.Context, string) error { return f.err }
func (f *billingFake) GetAccount(context.Context, string) (billing.Account, error) {
	f.reads++
	return f.account, f.err
}
func (f *billingFake) ListOpenHolds(context.Context, string, int) ([]billing.Hold, error) {
	return f.holds, f.err
}
func (f *billingFake) GetStatement(_ context.Context, customerID string, from, to time.Time) (billing.Statement, error) {
	f.statementReads++
	if f.err != nil {
		return billing.Statement{}, f.err
	}
	statement := f.statement
	statement.CustomerID, statement.PeriodStart, statement.PeriodEnd = customerID, from, to
	return statement, nil
}
func (f *billingFake) GetHold(_ context.Context, customerID string, holdID int64) (billing.Hold, error) {
	for _, hold := range f.holds {
		if hold.CustomerID == customerID && hold.ID == holdID {
			return hold, f.err
		}
	}
	return billing.Hold{}, billing.ErrNotFound
}
func (f *billingFake) GrantCredit(_ context.Context, req billing.CreditGrantRequest) error {
	f.grants++
	f.grant = req
	return f.err
}
func (f *billingFake) BeginCheckout(_ context.Context, req billing.BeginCheckoutRequest) (billing.Checkout, bool, error) {
	f.begins++
	f.begin = req
	if f.checkout.ID == "" {
		f.checkout = billing.Checkout{
			ID: "co_local", CustomerID: req.CustomerID, IdempotencyKey: req.IdempotencyKey,
			AmountMicroUSD: req.AmountMicroUSD, Currency: "USD", Provider: req.Provider,
			ProviderAccountID: req.ProviderAccountID, LiveMode: req.LiveMode,
			State: billing.CheckoutPendingProvider, CreatedAt: time.Unix(10, 0).UTC(), UpdatedAt: time.Unix(10, 0).UTC(),
		}
		return f.checkout, true, f.err
	}
	return f.checkout, false, f.err
}
func (f *billingFake) AttachCheckout(_ context.Context, customerID, checkoutID string, session billing.ProviderCheckoutSession) (billing.Checkout, error) {
	f.attached = session
	if f.err != nil {
		return billing.Checkout{}, f.err
	}
	if f.checkout.ID != checkoutID || f.checkout.CustomerID != customerID {
		return billing.Checkout{}, billing.ErrNotFound
	}
	f.checkout.ProviderSessionID = session.ID
	f.checkout.URL = session.URL
	f.checkout.State = billing.CheckoutOpen
	return f.checkout, nil
}
func (f *billingFake) GetCheckout(_ context.Context, customerID, checkoutID string) (billing.Checkout, error) {
	f.checkoutReads++
	if f.err != nil {
		return billing.Checkout{}, f.err
	}
	if f.checkout.ID != checkoutID || f.checkout.CustomerID != customerID {
		return billing.Checkout{}, billing.ErrNotFound
	}
	return f.checkout, nil
}
func (f *billingFake) LiveMode() bool { return true }

func TestTenantBillingSummaryIsMemberSafeAndTenantIsolated(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_bill", Token: "tenant-a", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "tenant-b", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_bill", map[string]string{"viewer": state.RoleViewer})
	fake := &billingFake{
		account: billing.Account{CustomerID: "cust_bill", Currency: "USD", BalanceMicroUSD: 900, HeldMicroUSD: 200, UpdatedAt: time.Unix(10, 0).UTC()},
		holds:   []billing.Hold{{ID: 7, CustomerID: "cust_bill", WorkloadRef: "wl_safe", AmountMicroUSD: 200, State: billing.HoldPending, ExpiresAt: time.Unix(20, 0).UTC(), CreatedAt: time.Unix(5, 0).UTC(), PriceQuote: billing.PriceQuote{Provider: "private-provider", SKU: "private-sku"}}},
	}
	accounts.Billing = fake
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/billing", http.HandlerFunc(accounts.HandleGetTenantBilling))

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/cust_bill/billing", nil)
	req.Header.Set("Authorization", "Bearer human_viewer")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"spendable_micro_usd":700`) || !strings.Contains(rec.Body.String(), `"workload_id":"wl_safe"`) {
		t.Fatalf("billing summary = %d: %s", rec.Code, rec.Body)
	}
	for _, secret := range []string{"private-provider", "private-sku", "tenant-a"} {
		if strings.Contains(rec.Body.String(), secret) {
			t.Fatalf("billing summary leaked %q: %s", secret, rec.Body)
		}
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/tenants/cust_other/billing", nil)
	req.Header.Set("Authorization", "Bearer human_viewer")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound || fake.reads != 1 {
		t.Fatalf("cross-tenant billing = %d reads=%d", rec.Code, fake.reads)
	}
}

func TestTenantBillingStatementJSONAndCSVAreBoundedTenantSafeSnapshots(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_bill", Token: "tenant-a", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "tenant-b", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_bill", map[string]string{"viewer": state.RoleViewer})
	holdID := int64(7)
	fake := &billingFake{statement: billing.Statement{
		Currency: "USD", GeneratedAt: time.Unix(30, 0).UTC(), LedgerWatermark: 12,
		OpeningNetCreditMicroUSD: 100, OpeningHeldMicroUSD: 0,
		ClosingNetCreditMicroUSD: 60, ClosingHeldMicroUSD: 0,
		Entries: []billing.StatementEntry{{
			LedgerID: 12, CreatedAt: time.Unix(20, 0).UTC(), EntryType: "capture", AmountMicroUSD: 40,
			BalanceDeltaMicroUSD: -40, HeldDeltaMicroUSD: -100, NetCreditMicroUSD: 60,
			HeldMicroUSD: 0, WorkloadID: "wl_safe", HoldID: &holdID,
		}},
	}}
	accounts.Billing = fake
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/billing/statement", http.HandlerFunc(accounts.HandleGetTenantBillingStatement))
	mux.Handle("GET /v1/tenants/{tenant_id}/billing/statement.csv", http.HandlerFunc(accounts.HandleGetTenantBillingStatementCSV))

	call := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer human_viewer")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	if rec := call("/v1/tenants/cust_bill/billing/statement?from=2026-01-01T00:00:00Z"); rec.Code != http.StatusBadRequest || fake.statementReads != 0 {
		t.Fatalf("missing bound = %d reads=%d: %s", rec.Code, fake.statementReads, rec.Body)
	}
	if rec := call("/v1/tenants/cust_bill/billing/statement?from=2026-01-01T00:00:00Z&to=2027-01-03T00:00:00Z"); rec.Code != http.StatusBadRequest || fake.statementReads != 0 {
		t.Fatalf("oversized period = %d reads=%d: %s", rec.Code, fake.statementReads, rec.Body)
	}
	for _, invalid := range []string{
		"?from=2026-01-01T00:00:00Z&from=2026-01-02T00:00:00Z&to=2026-02-01T00:00:00Z",
		"?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z&cursor=guess",
	} {
		if rec := call("/v1/tenants/cust_bill/billing/statement" + invalid); rec.Code != http.StatusBadRequest || fake.statementReads != 0 {
			t.Fatalf("invalid query %q = %d reads=%d: %s", invalid, rec.Code, fake.statementReads, rec.Body)
		}
	}
	query := "?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z"
	rec := call("/v1/tenants/cust_bill/billing/statement" + query)
	if rec.Code != http.StatusOK || fake.statementReads != 1 ||
		!strings.Contains(rec.Body.String(), `"ledger_watermark":12`) ||
		!strings.Contains(rec.Body.String(), `"balance_delta_micro_usd":-40`) ||
		!strings.Contains(rec.Body.String(), `"workload_id":"wl_safe"`) {
		t.Fatalf("statement JSON = %d reads=%d: %s", rec.Code, fake.statementReads, rec.Body)
	}
	rec = call("/v1/tenants/cust_bill/billing/statement.csv" + query)
	if rec.Code != http.StatusOK || fake.statementReads != 2 ||
		rec.Header().Get("Content-Type") != "text/csv; charset=utf-8" ||
		!strings.Contains(rec.Body.String(), "opening_net_credit_micro_usd") ||
		!strings.Contains(rec.Body.String(), "capture,40,-40,-100,60,0,wl_safe,7") {
		t.Fatalf("statement CSV = %d reads=%d headers=%v: %s", rec.Code, fake.statementReads, rec.Header(), rec.Body)
	}
	for _, secret := range []string{"provider_account", "payment_object", "operation_key", "external_ref", "tenant-a"} {
		if strings.Contains(rec.Body.String(), secret) {
			t.Fatalf("statement leaked %q: %s", secret, rec.Body)
		}
	}
	if rec := call("/v1/tenants/cust_other/billing/statement" + query); rec.Code != http.StatusNotFound || fake.statementReads != 2 {
		t.Fatalf("cross-tenant statement JSON = %d reads=%d: %s", rec.Code, fake.statementReads, rec.Body)
	}
	if rec := call("/v1/tenants/cust_other/billing/statement.csv" + query); rec.Code != http.StatusNotFound || fake.statementReads != 2 {
		t.Fatalf("cross-tenant statement CSV = %d reads=%d: %s", rec.Code, fake.statementReads, rec.Body)
	}
	fake.err = billing.ErrStatementTooLarge
	if rec := call("/v1/tenants/cust_bill/billing/statement" + query); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("large statement = %d: %s", rec.Code, rec.Body)
	}
}

func TestSpreadsheetSafeRejectsFormulaPrefixes(t *testing.T) {
	for input, want := range map[string]string{
		"safe": "safe", "": "", "=1+1": "'=1+1", "  @cmd": "'  @cmd", "+sum": "'+sum", "-2": "'-2",
	} {
		if got := spreadsheetSafe(input); got != want {
			t.Fatalf("spreadsheetSafe(%q)=%q want %q", input, got, want)
		}
	}
}

func TestServiceCreditGrantStrictShapeAndVerifiedOperatorContext(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_bill", Token: "tenant-a", Plan: "pro"})
	fake := &billingFake{}
	a := &Accounts{Store: store, Billing: fake, Log: quietLog()}
	mux := http.NewServeMux()
	mux.Handle("POST /v1/operator/tenants/{tenant_id}/billing/service-credits", http.HandlerFunc(a.HandleGrantServiceCredit))

	call := func(body string, operator bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/operator/tenants/cust_bill/billing/service-credits", strings.NewReader(body))
		if operator {
			req = req.WithContext(context.WithValue(req.Context(), ctxOperatorAccount, "acct_operator"))
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	if rec := call(`{"amount_micro_usd":100,"idempotency_key":"credit-1"}`, false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unverified grant = %d: %s", rec.Code, rec.Body)
	}
	if rec := call(`{"amount_micro_usd":100,"idempotency_key":"credit-1","payment":"secret"}`, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown payment field = %d: %s", rec.Code, rec.Body)
	}
	if rec := call(`{"amount_micro_usd":100,"idempotency_key":"`+strings.Repeat("x", maxServiceCreditBodyBytes)+`"}`, true); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized grant = %d: %s", rec.Code, rec.Body)
	}
	rec := call(`{"amount_micro_usd":100,"idempotency_key":"credit-1"}`, true)
	if rec.Code != http.StatusOK || fake.grants != 1 {
		t.Fatalf("grant = %d grants=%d: %s", rec.Code, fake.grants, rec.Body)
	}
	if fake.grant.Provider != "yscale" || fake.grant.ProviderAccountID != "acct_operator" || fake.grant.PaymentObjectID != serviceCreditObjectID("credit-1") || !fake.grant.LiveMode {
		t.Fatalf("grant attribution = %+v", fake.grant)
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || response["currency"] != "USD" {
		t.Fatalf("response = %v err=%v", response, err)
	}
	if strings.Contains(rec.Body.String(), "acct_operator") || strings.Contains(rec.Body.String(), "payment") {
		t.Fatalf("grant response leaked metadata: %s", rec.Body)
	}
}

func TestTenantCheckoutPersistsBeforeProviderAndIsExactlyReplayable(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_bill", Token: "tenant-a", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "tenant-b", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_bill", map[string]string{
		"owner": state.RoleOwner, "viewer": state.RoleViewer,
	})
	fake := &billingFake{}
	gateway := &checkoutGatewayFake{store: fake}
	accounts.Billing = fake
	accounts.Checkout = gateway
	accounts.CheckoutMinMicroUSD = 5_000_000
	accounts.CheckoutMaxMicroUSD = 20_000_000
	mux := http.NewServeMux()
	mux.Handle("POST /v1/tenants/{tenant_id}/billing/checkouts", http.HandlerFunc(accounts.HandleCreateTenantCheckout))
	mux.Handle("GET /v1/tenants/{tenant_id}/billing/checkouts/{id}", http.HandlerFunc(accounts.HandleGetTenantCheckout))

	call := func(token, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/tenants/cust_bill/billing/checkouts", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	if rec := call("human_viewer", `{"amount_micro_usd":5000000,"idempotency_key":"buy-1"}`); rec.Code != http.StatusForbidden || fake.begins != 0 || gateway.calls != 0 {
		t.Fatalf("viewer purchase = %d begins=%d provider=%d: %s", rec.Code, fake.begins, gateway.calls, rec.Body)
	}
	if rec := call("human_owner", `{"amount_micro_usd":5000001,"idempotency_key":"buy-1"}`); rec.Code != http.StatusBadRequest || fake.begins != 0 {
		t.Fatalf("fractional-cent purchase = %d begins=%d: %s", rec.Code, fake.begins, rec.Body)
	}
	if rec := call("human_owner", `{"amount_micro_usd":5000000,"idempotency_key":"buy-1","provider_account_id":"acct_other"}`); rec.Code != http.StatusBadRequest || fake.begins != 0 {
		t.Fatalf("caller-selected provider account = %d begins=%d: %s", rec.Code, fake.begins, rec.Body)
	}
	rec := call("human_owner", `{"amount_micro_usd":5000000,"idempotency_key":"buy-1"}`)
	if rec.Code != http.StatusCreated || fake.begins != 1 || gateway.calls != 1 {
		t.Fatalf("purchase = %d begins=%d provider=%d: %s", rec.Code, fake.begins, gateway.calls, rec.Body)
	}
	if gateway.request.CheckoutID != "co_local" || gateway.request.CustomerID != "cust_bill" ||
		gateway.request.AmountMicroUSD != 5_000_000 || fake.begin.ProviderAccountID != "acct_test" || fake.begin.LiveMode {
		t.Fatalf("provider request=%+v intent=%+v", gateway.request, fake.begin)
	}
	for _, forbidden := range []string{"cs_test", "acct_test", "idempotency_key", "buy-1"} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("checkout response leaked %q: %s", forbidden, rec.Body)
		}
	}
	rec = call("human_owner", `{"amount_micro_usd":5000000,"idempotency_key":"buy-1"}`)
	if rec.Code != http.StatusOK || fake.begins != 2 || gateway.calls != 1 {
		t.Fatalf("exact replay = %d begins=%d provider=%d: %s", rec.Code, fake.begins, gateway.calls, rec.Body)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/cust_bill/billing/checkouts/co_local", nil)
	req.Header.Set("Authorization", "Bearer human_viewer")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"state":"open"`) ||
		!strings.Contains(rec.Body.String(), `"url":"https://checkout.stripe.com/`) {
		t.Fatalf("checkout query = %d: %s", rec.Code, rec.Body)
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/tenants/cust_other/billing/checkouts/co_local", nil)
	req.Header.Set("Authorization", "Bearer human_viewer")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound || fake.checkoutReads != 1 {
		t.Fatalf("cross-tenant checkout GET = %d reads=%d: %s", rec.Code, fake.checkoutReads, rec.Body)
	}
	// A tenant-a member cannot open a checkout against tenant-b: the tenant
	// membership check runs before the billing fake and the checkout gateway.
	crossReq := httptest.NewRequest(http.MethodPost, "/v1/tenants/cust_other/billing/checkouts",
		strings.NewReader(`{"amount_micro_usd":5000000,"idempotency_key":"buy-cross"}`))
	crossReq.Header.Set("Authorization", "Bearer human_owner")
	crossRec := httptest.NewRecorder()
	mux.ServeHTTP(crossRec, crossReq)
	if crossRec.Code != http.StatusNotFound || fake.begins != 2 || gateway.calls != 1 {
		t.Fatalf("cross-tenant checkout POST = %d begins=%d provider=%d: %s", crossRec.Code, fake.begins, gateway.calls, crossRec.Body)
	}
}

func TestTenantCheckoutProviderFailureLeavesQueryablePendingIntent(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_bill", Token: "tenant-a", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_bill", map[string]string{"owner": state.RoleOwner})
	fake := &billingFake{}
	gateway := &checkoutGatewayFake{store: fake, err: errors.New("provider down")}
	accounts.Billing, accounts.Checkout = fake, gateway
	mux := http.NewServeMux()
	mux.Handle("POST /v1/tenants/{tenant_id}/billing/checkouts", http.HandlerFunc(accounts.HandleCreateTenantCheckout))
	mux.Handle("GET /v1/tenants/{tenant_id}/billing/checkouts/{id}", http.HandlerFunc(accounts.HandleGetTenantCheckout))

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/cust_bill/billing/checkouts", strings.NewReader(`{"amount_micro_usd":5000000,"idempotency_key":"buy-retry"}`))
	req.Header.Set("Authorization", "Bearer human_owner")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || fake.checkout.State != billing.CheckoutPendingProvider ||
		!strings.Contains(rec.Body.String(), `"id":"co_local"`) {
		t.Fatalf("provider failure = %d checkout=%+v: %s", rec.Code, fake.checkout, rec.Body)
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/tenants/cust_bill/billing/checkouts/co_local", nil)
	req.Header.Set("Authorization", "Bearer human_owner")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"state":"pending_provider"`) || strings.Contains(rec.Body.String(), `"url"`) {
		t.Fatalf("pending query = %d: %s", rec.Code, rec.Body)
	}
}

func TestTenantWorkloadBillingReceiptJSONShape(t *testing.T) {
	raw, err := json.Marshal(TenantWorkload{Billing: &TenantWorkloadBillingReceipt{
		HoldID: 9, State: billing.HoldCaptured, ReservedMicroUSD: 100, CapturedMicroUSD: 80,
		Currency: "USD", QuoteID: "quote_safe", PricingVersion: 2,
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"hold_id", "state", "reserved_micro_usd", "captured_micro_usd", "currency", "quote_id", "pricing_version"} {
		if !strings.Contains(string(raw), `"`+field+`"`) {
			t.Fatalf("receipt missing %s: %s", field, raw)
		}
	}
}

func TestTenantWorkloadBillingReceiptOmitsFrozenProviderMetadata(t *testing.T) {
	a := &Accounts{Billing: &billingFake{holds: []billing.Hold{{
		ID: 9, CustomerID: "cust_safe", AmountMicroUSD: 100, CapturedMicroUSD: 80, State: billing.HoldCaptured,
		PriceQuote: billing.PriceQuote{Provider: "secret-provider", SKU: "secret-sku", Region: "secret-region"},
	}}}}
	receipt := a.tenantWorkloadBilling(context.Background(), &state.Workload{CustomerID: "cust_safe", Billing: &state.WorkloadBilling{
		HoldID: 9, ReservedMicroUSD: 100, Currency: "USD", QuoteID: "quote_safe", PricingVersion: 2,
	}})
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-provider", "secret-sku", "secret-region", "provider", "payment"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("receipt leaked %q: %s", secret, raw)
		}
	}
}
