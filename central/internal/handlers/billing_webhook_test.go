// yscale:proprietary

package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/billing"
)

type webhookGatewayFake struct {
	event   billing.VerifiedWebhookEvent
	payload []byte
	sig     string
	err     error
}

func (f *webhookGatewayFake) ProviderAccountID() string { return "acct_test" }
func (f *webhookGatewayFake) LiveMode() bool            { return false }
func (f *webhookGatewayFake) CreateCheckout(context.Context, billing.ProviderCheckoutRequest) (billing.ProviderCheckoutSession, error) {
	return billing.ProviderCheckoutSession{}, errors.New("unused")
}
func (f *webhookGatewayFake) VerifyWebhook(payload []byte, signature string) (billing.VerifiedWebhookEvent, error) {
	f.payload = append([]byte(nil), payload...)
	f.sig = signature
	return f.event, f.err
}
func (f *webhookGatewayFake) ResolveWebhook(context.Context, billing.WebhookDelivery) (billing.WebhookResolution, error) {
	return billing.WebhookResolution{}, errors.New("unused")
}

type webhookStoreFake struct {
	event    billing.VerifiedWebhookEvent
	recorded int
	err      error
}

func (f *webhookStoreFake) RecordVerifiedWebhook(_ context.Context, event billing.VerifiedWebhookEvent) (int64, bool, error) {
	f.recorded++
	f.event = event
	return 7, true, f.err
}

func TestBillingWebhookVerifiesBeforeDurableAcceptance(t *testing.T) {
	payload := []byte(`{"id":"evt_test"}`)
	event := billing.VerifiedWebhookEvent{
		Provider: "stripe", ProviderAccountID: "acct_test", EventID: "evt_test",
		EventType: "checkout.session.completed", ObjectID: "cs_test", CustomerID: "cust_bill",
		APIVersion: "current", ProviderCreatedAt: time.Unix(10, 0).UTC(), Payload: payload,
	}
	gateway := &webhookGatewayFake{event: event}
	store := &webhookStoreFake{}
	handler := &BillingWebhooks{Store: store, Gateway: gateway, Log: quietLog()}

	req := httptest.NewRequest(http.MethodPost, "/v1/billing/webhooks/stripe", strings.NewReader(string(payload)))
	req.Header.Set("Stripe-Signature", "signed")
	rec := httptest.NewRecorder()
	handler.HandleStripe(rec, req)
	if rec.Code != http.StatusOK || store.recorded != 1 || gateway.sig != "signed" || string(gateway.payload) != string(payload) {
		t.Fatalf("accepted webhook = %d verified=%q recorded=%d: %s", rec.Code, gateway.sig, store.recorded, rec.Body)
	}
	if string(store.event.Payload) != string(payload) || !strings.Contains(rec.Body.String(), `"inserted":true`) {
		t.Fatalf("durable event=%+v response=%s", store.event, rec.Body)
	}

	gateway.err = billing.ErrWebhookRejected
	req = httptest.NewRequest(http.MethodPost, "/v1/billing/webhooks/stripe", strings.NewReader(string(payload)))
	req.Header.Set("Stripe-Signature", "invalid")
	rec = httptest.NewRecorder()
	handler.HandleStripe(rec, req)
	if rec.Code != http.StatusBadRequest || store.recorded != 1 {
		t.Fatalf("rejected webhook = %d recorded=%d: %s", rec.Code, store.recorded, rec.Body)
	}
}

func TestBillingWebhookBoundsRawBodyBeforeVerification(t *testing.T) {
	gateway := &webhookGatewayFake{}
	store := &webhookStoreFake{}
	handler := &BillingWebhooks{Store: store, Gateway: gateway, Log: quietLog()}
	req := httptest.NewRequest(http.MethodPost, "/v1/billing/webhooks/stripe", strings.NewReader(strings.Repeat("x", maxBillingWebhookBodyBytes+1)))
	req.Header.Set("Stripe-Signature", "signed")
	rec := httptest.NewRecorder()
	handler.HandleStripe(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge || len(gateway.payload) != 0 || store.recorded != 0 {
		t.Fatalf("oversized webhook = %d verified=%d recorded=%d: %s", rec.Code, len(gateway.payload), store.recorded, rec.Body)
	}
}
