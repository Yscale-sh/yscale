// yscale:proprietary

package billing

import (
	"context"
	"errors"
	"testing"
	"time"
)

type processorStoreFake struct {
	delivery          WebhookDelivery
	checkout          Checkout
	source            FundingSource
	grant             CreditGrantRequest
	reversal          CreditReversalRequest
	dispute           CreditDisputeRequest
	state             CheckoutState
	processed, failed int
	safeError         string
}

func (f *processorStoreFake) ClaimNextWebhook(context.Context, time.Duration) (WebhookDelivery, bool, error) {
	return f.delivery, true, nil
}
func (f *processorStoreFake) MarkWebhookProcessed(context.Context, int64, string) error {
	f.processed++
	return nil
}
func (f *processorStoreFake) MarkWebhookFailed(_ context.Context, _ int64, _ string, safe string, _ time.Time, _ int) error {
	f.failed++
	f.safeError = safe
	return nil
}
func (f *processorStoreFake) GetCheckoutByProviderSession(context.Context, string, string, bool, string) (Checkout, error) {
	return f.checkout, nil
}
func (f *processorStoreFake) SetCheckoutState(_ context.Context, _, _ string, state CheckoutState) error {
	f.state = state
	return nil
}
func (f *processorStoreFake) GrantCredit(_ context.Context, request CreditGrantRequest) error {
	f.grant = request
	return nil
}
func (f *processorStoreFake) FindFundingSource(context.Context, string, string, bool, string) (FundingSource, error) {
	return f.source, nil
}
func (f *processorStoreFake) ReverseCredit(_ context.Context, request CreditReversalRequest) error {
	f.reversal = request
	return nil
}
func (f *processorStoreFake) ApplyDispute(_ context.Context, request CreditDisputeRequest) error {
	f.dispute = request
	return nil
}

type processorResolverFake struct {
	resolution WebhookResolution
	err        error
}

func (f *processorResolverFake) ProviderAccountID() string { return "acct_test" }
func (f *processorResolverFake) LiveMode() bool            { return false }
func (f *processorResolverFake) CreateCheckout(context.Context, ProviderCheckoutRequest) (ProviderCheckoutSession, error) {
	return ProviderCheckoutSession{}, errors.New("unused")
}
func (f *processorResolverFake) VerifyWebhook([]byte, string) (VerifiedWebhookEvent, error) {
	return VerifiedWebhookEvent{}, errors.New("unused")
}
func (f *processorResolverFake) ResolveWebhook(context.Context, WebhookDelivery) (WebhookResolution, error) {
	return f.resolution, f.err
}

func processorDelivery(eventType, objectID string) WebhookDelivery {
	return WebhookDelivery{ID: 7, Provider: "stripe", ProviderAccountID: "acct_test", EventType: eventType, ObjectID: objectID, LeaseToken: "lease", Attempts: 1}
}

func TestWebhookProcessorSettledCheckoutCreditsThenCompletes(t *testing.T) {
	store := &processorStoreFake{
		delivery: processorDelivery("checkout.session.completed", "cs_one"),
		checkout: Checkout{ID: "co_one", CustomerID: "cust_one", AmountMicroUSD: 5_000_000, State: CheckoutOpen},
	}
	resolver := &processorResolverFake{resolution: WebhookResolution{
		Kind: WebhookCheckoutSettled, ProviderSessionID: "cs_one", CheckoutID: "co_one",
		CustomerID: "cust_one", PaymentObjectID: "pi_one", AmountMicroUSD: 5_000_000,
	}}
	processor := &WebhookProcessor{Store: store, Resolver: resolver, now: func() time.Time { return time.Unix(100, 0) }}
	if ok, err := processor.ProcessOne(context.Background()); err != nil || !ok {
		t.Fatalf("process settled: ok=%v err=%v", ok, err)
	}
	if store.grant.CustomerID != "cust_one" || store.grant.PaymentObjectID != "pi_one" ||
		store.grant.AmountMicroUSD != 5_000_000 || store.state != CheckoutSettled || store.processed != 1 || store.failed != 0 {
		t.Fatalf("grant=%+v state=%s processed=%d failed=%d", store.grant, store.state, store.processed, store.failed)
	}
}

func TestWebhookProcessorRefundUsesOriginalFundingTenant(t *testing.T) {
	store := &processorStoreFake{
		delivery: processorDelivery("refund.updated", "re_one"),
		source:   FundingSource{CustomerID: "cust_funded", CreditedMicroUSD: 5_000_000},
	}
	resolver := &processorResolverFake{resolution: WebhookResolution{
		Kind: WebhookRefundSucceeded, PaymentObjectID: "pi_one", ReversalObjectID: "re_one", AmountMicroUSD: 2_000_000,
	}}
	processor := &WebhookProcessor{Store: store, Resolver: resolver}
	if ok, err := processor.ProcessOne(context.Background()); err != nil || !ok {
		t.Fatalf("process refund: ok=%v err=%v", ok, err)
	}
	if store.reversal.CustomerID != "cust_funded" || store.reversal.PaymentObjectID != "pi_one" ||
		store.reversal.ReversalObjectID != "re_one" || store.reversal.Kind != ReversalRefund || store.processed != 1 {
		t.Fatalf("reversal=%+v processed=%d", store.reversal, store.processed)
	}
}

func TestWebhookProcessorDisputeConvergesOriginalFundingTenant(t *testing.T) {
	for name, fixture := range map[string]struct {
		kind         WebhookResolutionKind
		state        DisputeState
		reversalKind ReversalKind
	}{
		"withdrawn":          {kind: WebhookDisputeWithdrawn, state: DisputeWithdrawn, reversalKind: ReversalDispute},
		"restored":           {kind: WebhookDisputeRestored, state: DisputeRestored, reversalKind: ReversalDispute},
		"chargeback":         {kind: WebhookDisputeWithdrawn, state: DisputeWithdrawn, reversalKind: ReversalChargeback},
		"settlement failure": {kind: WebhookDisputeWithdrawn, state: DisputeWithdrawn, reversalKind: ReversalSettlementFailure},
	} {
		t.Run(name, func(t *testing.T) {
			store := &processorStoreFake{
				delivery: processorDelivery("charge.dispute.updated", "dp_one"),
				source:   FundingSource{CustomerID: "cust_funded", CreditedMicroUSD: 5_000_000},
			}
			resolver := &processorResolverFake{resolution: WebhookResolution{
				Kind: fixture.kind, PaymentObjectID: "pi_one", ReversalObjectID: "dp_one", AmountMicroUSD: 5_000_000,
				ReversalKind: fixture.reversalKind,
			}}
			processor := &WebhookProcessor{Store: store, Resolver: resolver}
			if ok, err := processor.ProcessOne(context.Background()); err != nil || !ok {
				t.Fatalf("process dispute: ok=%v err=%v", ok, err)
			}
			if store.dispute.CustomerID != "cust_funded" || store.dispute.PaymentObjectID != "pi_one" ||
				store.dispute.DisputeObjectID != "dp_one" || store.dispute.State != fixture.state ||
				store.dispute.Kind != fixture.reversalKind || store.processed != 1 {
				t.Fatalf("dispute=%+v processed=%d", store.dispute, store.processed)
			}
		})
	}
}

func TestWebhookProcessorFailuresStayLeasedAndSafe(t *testing.T) {
	store := &processorStoreFake{delivery: processorDelivery("checkout.session.completed", "cs_secret")}
	processor := &WebhookProcessor{Store: store, Resolver: &processorResolverFake{err: errors.New("provider raw secret")}, now: func() time.Time { return time.Unix(100, 0) }}
	if ok, err := processor.ProcessOne(context.Background()); err != nil || !ok {
		t.Fatalf("failed resolution: ok=%v err=%v", ok, err)
	}
	if store.failed != 1 || store.processed != 0 || store.safeError != "provider state resolution failed" {
		t.Fatalf("failed=%d processed=%d safe=%q", store.failed, store.processed, store.safeError)
	}
}
