// yscale:proprietary

package billing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type webhookProcessorStore interface {
	ClaimNextWebhook(context.Context, time.Duration) (WebhookDelivery, bool, error)
	MarkWebhookProcessed(context.Context, int64, string) error
	MarkWebhookFailed(context.Context, int64, string, string, time.Time, int) error
	GetCheckoutByProviderSession(context.Context, string, string, bool, string) (Checkout, error)
	SetCheckoutState(context.Context, string, string, CheckoutState) error
	GrantCredit(context.Context, CreditGrantRequest) error
	FindFundingSource(context.Context, string, string, bool, string) (FundingSource, error)
	ReverseCredit(context.Context, CreditReversalRequest) error
	ApplyDispute(context.Context, CreditDisputeRequest) error
}

type WebhookProcessor struct {
	Store       webhookProcessorStore
	Resolver    CheckoutGateway
	Log         *slog.Logger
	Lease       time.Duration
	Poll        time.Duration
	MaxAttempts int
	now         func() time.Time
}

func (p *WebhookProcessor) ProcessOne(ctx context.Context) (bool, error) {
	if p == nil || p.Store == nil || p.Resolver == nil {
		return false, fmt.Errorf("%w: webhook processor is not configured", ErrInvalidArgument)
	}
	lease := p.Lease
	if lease == 0 {
		lease = time.Minute
	}
	delivery, ok, err := p.Store.ClaimNextWebhook(ctx, lease)
	if err != nil || !ok {
		return ok, err
	}
	resolution, err := p.Resolver.ResolveWebhook(ctx, delivery)
	if err != nil {
		return true, p.fail(ctx, delivery, "provider state resolution failed")
	}
	if err := p.apply(ctx, delivery, resolution); err != nil {
		return true, p.fail(ctx, delivery, "economic application failed")
	}
	if err := p.Store.MarkWebhookProcessed(ctx, delivery.ID, delivery.LeaseToken); err != nil {
		return true, err
	}
	return true, nil
}

func (p *WebhookProcessor) apply(ctx context.Context, delivery WebhookDelivery, resolution WebhookResolution) error {
	switch resolution.Kind {
	case WebhookNoEconomicChange:
		return nil
	case WebhookCheckoutSettled, WebhookCheckoutFailed, WebhookCheckoutExpired:
		checkout, err := p.Store.GetCheckoutByProviderSession(ctx, delivery.Provider, delivery.ProviderAccountID, delivery.LiveMode, resolution.ProviderSessionID)
		if err != nil {
			return err
		}
		if checkout.ID != resolution.CheckoutID || checkout.CustomerID != resolution.CustomerID ||
			checkout.AmountMicroUSD != resolution.AmountMicroUSD {
			return ErrEconomicObjectConflict
		}
		state := CheckoutFailed
		if resolution.Kind == WebhookCheckoutSettled {
			if err := p.Store.GrantCredit(ctx, CreditGrantRequest{
				CustomerID: checkout.CustomerID, AmountMicroUSD: checkout.AmountMicroUSD,
				IdempotencyKey: economicOperationKey("stripe-payment", delivery.ProviderAccountID, resolution.PaymentObjectID),
				Provider:       delivery.Provider, ProviderAccountID: delivery.ProviderAccountID,
				LiveMode: delivery.LiveMode, PaymentObjectID: resolution.PaymentObjectID,
			}); err != nil {
				return err
			}
			state = CheckoutSettled
		} else if resolution.Kind == WebhookCheckoutExpired {
			state = CheckoutExpired
		}
		return p.Store.SetCheckoutState(ctx, checkout.CustomerID, checkout.ID, state)

	case WebhookRefundSucceeded:
		source, err := p.Store.FindFundingSource(ctx, delivery.Provider, delivery.ProviderAccountID, delivery.LiveMode, resolution.PaymentObjectID)
		if err != nil {
			return err
		}
		return p.Store.ReverseCredit(ctx, CreditReversalRequest{
			CustomerID: source.CustomerID, AmountMicroUSD: resolution.AmountMicroUSD,
			IdempotencyKey:  economicOperationKey("stripe-refund", delivery.ProviderAccountID, resolution.ReversalObjectID),
			FundingProvider: delivery.Provider, FundingProviderAccountID: delivery.ProviderAccountID,
			FundingLiveMode: delivery.LiveMode, PaymentObjectID: resolution.PaymentObjectID,
			ReversalProvider: delivery.Provider, ReversalProviderAccountID: delivery.ProviderAccountID,
			ReversalLiveMode: delivery.LiveMode, ReversalObjectID: resolution.ReversalObjectID, Kind: ReversalRefund,
		})

	case WebhookDisputeWithdrawn, WebhookDisputeRestored:
		source, err := p.Store.FindFundingSource(ctx, delivery.Provider, delivery.ProviderAccountID, delivery.LiveMode, resolution.PaymentObjectID)
		if err != nil {
			return err
		}
		state := DisputeWithdrawn
		operation := "stripe-dispute-withdrawal"
		if resolution.Kind == WebhookDisputeRestored {
			state = DisputeRestored
			operation = "stripe-dispute-restoration"
		}
		reversalKind := resolution.ReversalKind
		if reversalKind == "" {
			reversalKind = ReversalDispute
		}
		return p.Store.ApplyDispute(ctx, CreditDisputeRequest{
			CustomerID: source.CustomerID, AmountMicroUSD: resolution.AmountMicroUSD,
			IdempotencyKey:  economicOperationKey(operation, delivery.ProviderAccountID, resolution.ReversalObjectID),
			FundingProvider: delivery.Provider, FundingProviderAccountID: delivery.ProviderAccountID,
			FundingLiveMode: delivery.LiveMode, PaymentObjectID: resolution.PaymentObjectID,
			DisputeProvider: delivery.Provider, DisputeProviderAccountID: delivery.ProviderAccountID,
			DisputeLiveMode: delivery.LiveMode, DisputeObjectID: resolution.ReversalObjectID,
			Kind: reversalKind, State: state,
		})
	default:
		return fmt.Errorf("%w: unknown webhook resolution", ErrInvalidArgument)
	}
}

func (p *WebhookProcessor) fail(ctx context.Context, delivery WebhookDelivery, safeError string) error {
	now := time.Now
	if p.now != nil {
		now = p.now
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 10
	}
	shift := delivery.Attempts - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 8 {
		shift = 8
	}
	delay := time.Second * time.Duration(1<<shift)
	return p.Store.MarkWebhookFailed(ctx, delivery.ID, delivery.LeaseToken, safeError, now().Add(delay), maxAttempts)
}

func (p *WebhookProcessor) Run(ctx context.Context) {
	poll := p.Poll
	if poll <= 0 {
		poll = 2 * time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		for processed := 0; processed < 100; processed++ {
			ok, err := p.ProcessOne(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) && p.Log != nil {
					p.Log.Error("billing: webhook processor pass failed", "error", err)
				}
				break
			}
			if !ok {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func economicOperationKey(kind, providerAccountID, objectID string) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + providerAccountID + "\x00" + objectID))
	return kind + ":" + hex.EncodeToString(sum[:])
}
