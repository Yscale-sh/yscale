// yscale:proprietary

package billing

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	stripe "github.com/stripe/stripe-go/v86"
)

func stripeTestConfig() StripeConfig {
	return StripeConfig{
		SecretKey: "sk_test_safe", WebhookSecret: "whsec_test_safe", AccountID: "acct_test",
		SuccessURL: "https://app.example.test/billing/success", CancelURL: "https://app.example.test/billing/cancel",
		LiveMode: false, MinMicroUSD: 5_000_000, MaxMicroUSD: 20_000_000,
	}
}

func TestStripeCheckoutUsesServerDerivedContractAndDeterministicKey(t *testing.T) {
	var requestErr string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/checkout/sessions" {
			requestErr = fmt.Sprintf("request %s %s", r.Method, r.URL.Path)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			requestErr = err.Error()
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		checks := map[string]string{
			"Idempotency-Key":                                   r.Header.Get("Idempotency-Key"),
			"client_reference_id":                               r.Form.Get("client_reference_id"),
			"metadata[yscale_checkout_id]":                      r.Form.Get("metadata[yscale_checkout_id]"),
			"metadata[yscale_customer_id]":                      r.Form.Get("metadata[yscale_customer_id]"),
			"payment_intent_data[metadata][yscale_checkout_id]": r.Form.Get("payment_intent_data[metadata][yscale_checkout_id]"),
			"line_items[0][price_data][unit_amount]":            r.Form.Get("line_items[0][price_data][unit_amount]"),
			"line_items[0][price_data][currency]":               r.Form.Get("line_items[0][price_data][currency]"),
		}
		want := map[string]string{
			"Idempotency-Key": "yscale-checkout:co_safe", "client_reference_id": "cust_safe",
			"metadata[yscale_checkout_id]": "co_safe", "metadata[yscale_customer_id]": "cust_safe",
			"payment_intent_data[metadata][yscale_checkout_id]": "co_safe",
			"line_items[0][price_data][unit_amount]":            "500", "line_items[0][price_data][currency]": "usd",
		}
		for key, expected := range want {
			if checks[key] != expected {
				requestErr = fmt.Sprintf("%s=%q want %q", key, checks[key], expected)
				http.Error(w, "contract mismatch", http.StatusBadRequest)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"cs_test_safe","object":"checkout.session","url":"https://checkout.stripe.com/c/pay/safe","livemode":false,"amount_total":500,"currency":"usd","client_reference_id":"cust_safe","metadata":{"yscale_checkout_id":"co_safe","yscale_customer_id":"cust_safe"}}`)
	}))
	defer server.Close()
	backends := stripe.NewBackendsWithConfig(&stripe.BackendConfig{
		URL: stripe.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripe.Int64(0),
		LeveledLogger: &stripe.LeveledLogger{Level: stripe.LevelNull},
	})
	gateway, err := newStripeGateway(stripeTestConfig(), backends)
	if err != nil {
		t.Fatal(err)
	}
	session, err := gateway.CreateCheckout(context.Background(), ProviderCheckoutRequest{
		CheckoutID: "co_safe", CustomerID: "cust_safe", AmountMicroUSD: 5_000_000,
	})
	if err != nil || requestErr != "" {
		t.Fatalf("create checkout=%+v err=%v request=%s", session, err, requestErr)
	}
	if session.ID != "cs_test_safe" || !strings.HasPrefix(session.URL, "https://checkout.stripe.com/") {
		t.Fatalf("session=%+v", session)
	}
}

func TestStripeWebhookVerificationBindsRawBodyAccountModeTypeAndMapping(t *testing.T) {
	gateway, err := NewStripeGateway(stripeTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(fmt.Sprintf(`{"id":"evt_safe","object":"event","account":"acct_test","api_version":%q,"created":%d,"livemode":false,"type":"checkout.session.completed","data":{"object":{"id":"cs_safe","client_reference_id":"cust_safe","metadata":{"yscale_checkout_id":"co_safe","yscale_customer_id":"cust_safe"}}}}`, stripe.APIVersion, time.Now().Unix()))
	signed := stripe.GenerateTestSignedPayload(&stripe.UnsignedPayload{Payload: payload, Secret: stripeTestConfig().WebhookSecret})
	event, err := gateway.VerifyWebhook(payload, signed.Header)
	if err != nil {
		t.Fatal(err)
	}
	if event.Provider != "stripe" || event.ProviderAccountID != "acct_test" || event.EventID != "evt_safe" ||
		event.ObjectID != "cs_safe" || event.CustomerID != "cust_safe" || event.LiveMode || string(event.Payload) != string(payload) {
		t.Fatalf("verified event=%+v", event)
	}
	disputePayload := []byte(fmt.Sprintf(`{"id":"evt_dispute","object":"event","account":"acct_test","api_version":%q,"created":%d,"livemode":false,"type":"charge.dispute.funds_withdrawn","data":{"object":{"id":"dp_safe"}}}`, stripe.APIVersion, time.Now().Unix()))
	disputeSigned := stripe.GenerateTestSignedPayload(&stripe.UnsignedPayload{Payload: disputePayload, Secret: stripeTestConfig().WebhookSecret})
	disputeEvent, err := gateway.VerifyWebhook(disputePayload, disputeSigned.Header)
	if err != nil || disputeEvent.EventType != "charge.dispute.funds_withdrawn" || disputeEvent.ObjectID != "dp_safe" {
		t.Fatalf("verified dispute=%+v err=%v", disputeEvent, err)
	}

	for name, mutation := range map[string]func(string) string{
		"wrong account": func(raw string) string {
			return strings.Replace(raw, `"account":"acct_test"`, `"account":"acct_other"`, 1)
		},
		"wrong mode": func(raw string) string { return strings.Replace(raw, `"livemode":false`, `"livemode":true`, 1) },
		"unknown type": func(raw string) string {
			return strings.Replace(raw, `"checkout.session.completed"`, `"customer.created"`, 1)
		},
		"missing map": func(raw string) string {
			return strings.Replace(raw, `"yscale_checkout_id":"co_safe"`, `"other":"co_safe"`, 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := []byte(mutation(string(payload)))
			signed := stripe.GenerateTestSignedPayload(&stripe.UnsignedPayload{Payload: changed, Secret: stripeTestConfig().WebhookSecret})
			if _, err := gateway.VerifyWebhook(changed, signed.Header); !errors.Is(err, ErrWebhookRejected) {
				t.Fatalf("rejection=%v", err)
			}
		})
	}
	if _, err := gateway.VerifyWebhook(payload, "t=1,v1=bad"); !errors.Is(err, ErrWebhookRejected) {
		t.Fatalf("invalid signature=%v", err)
	}
	old := stripe.GenerateTestSignedPayload(&stripe.UnsignedPayload{
		Payload: payload, Secret: stripeTestConfig().WebhookSecret, Timestamp: time.Now().Add(-10 * time.Minute),
	})
	if _, err := gateway.VerifyWebhook(payload, old.Header); !errors.Is(err, ErrWebhookRejected) {
		t.Fatalf("stale signature=%v", err)
	}
}

func TestStripeWebhookResolutionReadsCurrentEconomicObjects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/checkout/sessions/cs_safe":
			_, _ = fmt.Fprint(w, `{"id":"cs_safe","object":"checkout.session","livemode":false,"amount_total":500,"currency":"usd","status":"complete","payment_status":"paid","client_reference_id":"cust_safe","payment_intent":{"id":"pi_safe","object":"payment_intent"},"metadata":{"yscale_checkout_id":"co_safe","yscale_customer_id":"cust_safe"}}`)
		case "/v1/refunds/re_safe":
			_, _ = fmt.Fprint(w, `{"id":"re_safe","object":"refund","amount":200,"currency":"usd","status":"succeeded","payment_intent":{"id":"pi_safe","object":"payment_intent"}}`)
		case "/v1/disputes/dp_withdrawn":
			_, _ = fmt.Fprint(w, `{"id":"dp_withdrawn","object":"dispute","amount":500,"currency":"usd","livemode":false,"status":"under_review","reason":"general","payment_intent":{"id":"pi_safe","object":"payment_intent"}}`)
		case "/v1/disputes/dp_restored":
			_, _ = fmt.Fprint(w, `{"id":"dp_restored","object":"dispute","amount":500,"currency":"usd","livemode":false,"status":"won","payment_intent":{"id":"pi_safe","object":"payment_intent"}}`)
		case "/v1/disputes/dp_warning":
			_, _ = fmt.Fprint(w, `{"id":"dp_warning","object":"dispute","amount":500,"currency":"usd","livemode":false,"status":"warning_needs_response","payment_intent":{"id":"pi_safe","object":"payment_intent"}}`)
		case "/v1/disputes/dp_chargeback":
			_, _ = fmt.Fprint(w, `{"id":"dp_chargeback","object":"dispute","amount":500,"currency":"usd","livemode":false,"status":"lost","reason":"fraudulent","payment_intent":{"id":"pi_safe","object":"payment_intent"},"payment_method_details":{"type":"card","card":{"case_type":"chargeback"}}}`)
		case "/v1/disputes/dp_settlement_failure":
			_, _ = fmt.Fprint(w, `{"id":"dp_settlement_failure","object":"dispute","amount":500,"currency":"usd","livemode":false,"status":"needs_response","reason":"insufficient_funds","payment_intent":{"id":"pi_safe","object":"payment_intent"},"payment_method_details":{"type":"us_bank_account"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	backends := stripe.NewBackendsWithConfig(&stripe.BackendConfig{
		URL: stripe.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripe.Int64(0),
		LeveledLogger: &stripe.LeveledLogger{Level: stripe.LevelNull},
	})
	gateway, err := newStripeGateway(stripeTestConfig(), backends)
	if err != nil {
		t.Fatal(err)
	}
	checkout, err := gateway.ResolveWebhook(context.Background(), WebhookDelivery{
		Provider: "stripe", ProviderAccountID: "acct_test", EventType: "checkout.session.completed", ObjectID: "cs_safe",
	})
	if err != nil || checkout.Kind != WebhookCheckoutSettled || checkout.PaymentObjectID != "pi_safe" ||
		checkout.CheckoutID != "co_safe" || checkout.CustomerID != "cust_safe" || checkout.AmountMicroUSD != 5_000_000 {
		t.Fatalf("checkout resolution=%+v err=%v", checkout, err)
	}
	refund, err := gateway.ResolveWebhook(context.Background(), WebhookDelivery{
		Provider: "stripe", ProviderAccountID: "acct_test", EventType: "refund.updated", ObjectID: "re_safe",
	})
	if err != nil || refund.Kind != WebhookRefundSucceeded || refund.PaymentObjectID != "pi_safe" ||
		refund.ReversalObjectID != "re_safe" || refund.AmountMicroUSD != 2_000_000 {
		t.Fatalf("refund resolution=%+v err=%v", refund, err)
	}
	for name, fixture := range map[string]struct {
		object       string
		kind         WebhookResolutionKind
		reversalKind ReversalKind
	}{
		"withdrawn":          {object: "dp_withdrawn", kind: WebhookDisputeWithdrawn, reversalKind: ReversalDispute},
		"restored":           {object: "dp_restored", kind: WebhookDisputeRestored, reversalKind: ReversalDispute},
		"warning":            {object: "dp_warning", kind: WebhookNoEconomicChange, reversalKind: ReversalDispute},
		"chargeback":         {object: "dp_chargeback", kind: WebhookDisputeWithdrawn, reversalKind: ReversalChargeback},
		"settlement failure": {object: "dp_settlement_failure", kind: WebhookDisputeWithdrawn, reversalKind: ReversalSettlementFailure},
	} {
		t.Run(name, func(t *testing.T) {
			resolution, err := gateway.ResolveWebhook(context.Background(), WebhookDelivery{
				Provider: "stripe", ProviderAccountID: "acct_test", EventType: "charge.dispute.updated", ObjectID: fixture.object,
			})
			if err != nil || resolution.Kind != fixture.kind || resolution.PaymentObjectID != "pi_safe" ||
				resolution.ReversalObjectID != fixture.object || resolution.AmountMicroUSD != 5_000_000 ||
				resolution.ReversalKind != fixture.reversalKind {
				t.Fatalf("dispute resolution=%+v err=%v", resolution, err)
			}
		})
	}
	if _, err := gateway.ResolveWebhook(context.Background(), WebhookDelivery{
		Provider: "stripe", ProviderAccountID: "acct_other", EventType: "refund.updated", ObjectID: "re_safe",
	}); !errors.Is(err, ErrEnvironmentMismatch) {
		t.Fatalf("cross-account resolution=%v", err)
	}
}

func TestStripeExternalCashSnapshotIncludesRefundsAndWithdrawnDisputes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/payment_intents":
			_, _ = fmt.Fprint(w, `{"object":"list","has_more":false,"url":"/v1/payment_intents","data":[{"id":"pi_cash","object":"payment_intent","livemode":false,"amount_received":500,"currency":"usd","status":"succeeded","metadata":{"yscale_checkout_id":"co_cash","yscale_customer_id":"cust_cash"},"latest_charge":{"id":"ch_cash","object":"charge","livemode":false,"currency":"usd","paid":true,"status":"succeeded","amount_captured":500,"amount_refunded":100}},{"id":"pi_pending","object":"payment_intent","livemode":false,"amount_received":0,"currency":"usd","status":"processing","metadata":{"yscale_checkout_id":"co_pending","yscale_customer_id":"cust_cash"}}]}`)
		case "/v1/disputes":
			_, _ = fmt.Fprint(w, `{"object":"list","has_more":false,"url":"/v1/disputes","data":[{"id":"dp_cash","object":"dispute","livemode":false,"amount":200,"currency":"usd","status":"under_review","payment_intent":{"id":"pi_cash","object":"payment_intent"}}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	backends := stripe.NewBackendsWithConfig(&stripe.BackendConfig{
		URL: stripe.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripe.Int64(0),
		LeveledLogger: &stripe.LeveledLogger{Level: stripe.LevelNull},
	})
	gateway, err := newStripeGateway(stripeTestConfig(), backends)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := gateway.ExternalCashSnapshot(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("objects=%+v", objects)
	}
	object := objects[0]
	if object.Provider != "stripe" || object.ProviderAccountID != "acct_test" || object.LiveMode ||
		object.PaymentObjectID != "pi_cash" || object.CustomerID != "cust_cash" ||
		object.CreditedMicroUSD != 5_000_000 || object.ReversedMicroUSD != 3_000_000 {
		t.Fatalf("cash object=%+v", object)
	}
	if _, err := gateway.ExternalCashSnapshot(context.Background(), 1); err == nil {
		t.Fatal("bounded provider scan accepted more objects than its limit")
	}

	partialServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"object":"list","has_more":false,"data":[{"id":"pi_partial","object":"payment_intent","livemode":false,"amount_received":0,"status":"processing","metadata":{"yscale_checkout_id":"co_partial"}}]}`)
	}))
	defer partialServer.Close()
	partialBackends := stripe.NewBackendsWithConfig(&stripe.BackendConfig{
		URL: stripe.String(partialServer.URL), HTTPClient: partialServer.Client(), MaxNetworkRetries: stripe.Int64(0),
		LeveledLogger: &stripe.LeveledLogger{Level: stripe.LevelNull},
	})
	partialGateway, err := newStripeGateway(stripeTestConfig(), partialBackends)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partialGateway.ExternalCashSnapshot(context.Background(), 10); err == nil {
		t.Fatal("partial Yscale payment identity reconciled healthy")
	}
}

func TestStripeGatewayConfigurationFailsClosed(t *testing.T) {
	for name, mutate := range map[string]func(*StripeConfig){
		"missing secret":  func(c *StripeConfig) { c.SecretKey = "" },
		"live key":        func(c *StripeConfig) { c.SecretKey = "sk_live_wrong_mode" },
		"http success":    func(c *StripeConfig) { c.SuccessURL = "http://app.example.test/success" },
		"fractional min":  func(c *StripeConfig) { c.MinMicroUSD++ },
		"inverted bounds": func(c *StripeConfig) { c.MaxMicroUSD = c.MinMicroUSD - microUSDPerCent },
	} {
		t.Run(name, func(t *testing.T) {
			config := stripeTestConfig()
			mutate(&config)
			if _, err := NewStripeGateway(config); err == nil {
				t.Fatal("invalid Stripe configuration accepted")
			}
		})
	}
}

func TestStripeCashProviderConfigurationIsReadOnlyAndFailsClosed(t *testing.T) {
	provider, err := NewStripeCashProvider(StripeCashConfig{
		SecretKey: "sk_test_safe", AccountID: "acct_test", LiveMode: false,
	})
	if err != nil || provider == nil {
		t.Fatalf("test cash provider = %v, %v", provider, err)
	}
	if provider.webhookSecret != "" || provider.successURL != "" || provider.cancelURL != "" {
		t.Fatal("cash-only provider unexpectedly activated checkout configuration")
	}
	for name, config := range map[string]StripeCashConfig{
		"missing secret":  {AccountID: "acct_test"},
		"missing account": {SecretKey: "sk_test_safe"},
		"wrong key mode":  {SecretKey: "sk_live_wrong_mode", AccountID: "acct_test"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewStripeCashProvider(config); err == nil {
				t.Fatal("invalid Stripe cash configuration accepted")
			}
		})
	}
}
