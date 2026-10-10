// yscale:proprietary

package billing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	stripe "github.com/stripe/stripe-go/v86"
)

const (
	stripeProvider           = "stripe"
	stripeSignatureTolerance = 5 * time.Minute
)

type StripeConfig struct {
	SecretKey     string
	WebhookSecret string
	AccountID     string
	SuccessURL    string
	CancelURL     string
	LiveMode      bool
	MinMicroUSD   int64
	MaxMicroUSD   int64
}

// StripeCashConfig is the smaller credential surface needed for read-only
// external-cash reconciliation. It deliberately carries no webhook secret or
// checkout URLs: enabling the paid-runtime safety proof must not implicitly
// expose checkout or webhook routes.
type StripeCashConfig struct {
	SecretKey string
	AccountID string
	LiveMode  bool
}

type StripeGateway struct {
	client        *stripe.Client
	webhookSecret string
	accountID     string
	successURL    string
	cancelURL     string
	liveMode      bool
	minMicroUSD   int64
	maxMicroUSD   int64
}

func NewStripeGateway(config StripeConfig) (*StripeGateway, error) {
	return newStripeGateway(config, nil)
}

// NewStripeCashProvider constructs a read-only Stripe cash reconciler without
// activating checkout. The returned gateway implements ExternalCashProvider;
// checkout methods remain unusable because their required configuration is
// intentionally absent.
func NewStripeCashProvider(config StripeCashConfig) (*StripeGateway, error) {
	for label, value := range map[string]string{
		"secret key": config.SecretKey,
		"account ID": config.AccountID,
	} {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("%w: Stripe %s is required", ErrInvalidArgument, label)
		}
	}
	if err := validateStripeKeyMode(config.SecretKey, config.LiveMode); err != nil {
		return nil, err
	}
	return &StripeGateway{
		client:    stripe.NewClient(config.SecretKey),
		accountID: config.AccountID,
		liveMode:  config.LiveMode,
	}, nil
}

func newStripeGateway(config StripeConfig, backends *stripe.Backends) (*StripeGateway, error) {
	for label, value := range map[string]string{
		"secret key": config.SecretKey, "webhook secret": config.WebhookSecret,
		"account ID": config.AccountID, "success URL": config.SuccessURL, "cancel URL": config.CancelURL,
	} {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("%w: Stripe %s is required", ErrInvalidArgument, label)
		}
	}
	if err := validateStripeKeyMode(config.SecretKey, config.LiveMode); err != nil {
		return nil, err
	}
	for label, raw := range map[string]string{"success URL": config.SuccessURL, "cancel URL": config.CancelURL} {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || len(raw) > 2048 {
			return nil, fmt.Errorf("%w: Stripe %s must be bounded HTTPS without userinfo", ErrInvalidArgument, label)
		}
	}
	if config.MinMicroUSD <= 0 || config.MaxMicroUSD < config.MinMicroUSD ||
		config.MinMicroUSD%microUSDPerCent != 0 || config.MaxMicroUSD%microUSDPerCent != 0 {
		return nil, fmt.Errorf("%w: Stripe checkout bounds must be positive whole cents", ErrInvalidAmount)
	}
	options := []stripe.ClientOption{}
	if backends != nil {
		options = append(options, stripe.WithBackends(backends))
	}
	return &StripeGateway{
		client: stripe.NewClient(config.SecretKey, options...), webhookSecret: config.WebhookSecret,
		accountID: config.AccountID, successURL: config.SuccessURL, cancelURL: config.CancelURL,
		liveMode: config.LiveMode, minMicroUSD: config.MinMicroUSD, maxMicroUSD: config.MaxMicroUSD,
	}, nil
}

func validateStripeKeyMode(secretKey string, liveMode bool) error {
	prefixes := []string{"sk_test_", "rk_test_"}
	if liveMode {
		prefixes = []string{"sk_live_", "rk_live_"}
	}
	if !strings.HasPrefix(secretKey, prefixes[0]) && !strings.HasPrefix(secretKey, prefixes[1]) {
		return fmt.Errorf("%w: Stripe key mode does not match billing mode", ErrEnvironmentMismatch)
	}
	return nil
}

func (g *StripeGateway) ProviderAccountID() string { return g.accountID }
func (g *StripeGateway) LiveMode() bool            { return g.liveMode }

func (g *StripeGateway) CreateCheckout(ctx context.Context, request ProviderCheckoutRequest) (ProviderCheckoutSession, error) {
	if g == nil || g.client == nil {
		return ProviderCheckoutSession{}, fmt.Errorf("billing: Stripe checkout is not configured")
	}
	if err := validateID("checkout ID", request.CheckoutID); err != nil {
		return ProviderCheckoutSession{}, err
	}
	if err := validateID("customer ID", request.CustomerID); err != nil {
		return ProviderCheckoutSession{}, err
	}
	if request.AmountMicroUSD < g.minMicroUSD || request.AmountMicroUSD > g.maxMicroUSD ||
		request.AmountMicroUSD%microUSDPerCent != 0 {
		return ProviderCheckoutSession{}, fmt.Errorf("%w: checkout amount is outside configured whole-cent bounds", ErrInvalidAmount)
	}
	metadata := map[string]string{
		"yscale_checkout_id": request.CheckoutID,
		"yscale_customer_id": request.CustomerID,
	}
	params := &stripe.CheckoutSessionCreateParams{
		SuccessURL:        stripe.String(g.successURL),
		CancelURL:         stripe.String(g.cancelURL),
		ClientReferenceID: stripe.String(request.CustomerID),
		CustomerCreation:  stripe.String(string(stripe.CheckoutSessionCustomerCreationAlways)),
		Mode:              stripe.String(string(stripe.CheckoutSessionModePayment)),
		Metadata:          metadata,
		PaymentIntentData: &stripe.CheckoutSessionCreatePaymentIntentDataParams{Metadata: metadata},
		LineItems: []*stripe.CheckoutSessionCreateLineItemParams{{
			Quantity: stripe.Int64(1),
			PriceData: &stripe.CheckoutSessionCreateLineItemPriceDataParams{
				Currency:   stripe.String(string(stripe.CurrencyUSD)),
				UnitAmount: stripe.Int64(request.AmountMicroUSD / microUSDPerCent),
				ProductData: &stripe.CheckoutSessionCreateLineItemPriceDataProductDataParams{
					Name: stripe.String("Yscale prepaid burst credit"),
				},
			},
		}},
	}
	params.SetIdempotencyKey("yscale-checkout:" + request.CheckoutID)
	session, err := g.client.V1CheckoutSessions.Create(ctx, params)
	if err != nil {
		return ProviderCheckoutSession{}, fmt.Errorf("billing: create Stripe checkout: %w", err)
	}
	if session == nil || session.ID == "" || session.URL == "" || session.Livemode != g.liveMode ||
		session.AmountTotal != request.AmountMicroUSD/microUSDPerCent || session.Currency != stripe.CurrencyUSD ||
		session.ClientReferenceID != request.CustomerID || session.Metadata["yscale_checkout_id"] != request.CheckoutID ||
		session.Metadata["yscale_customer_id"] != request.CustomerID {
		return ProviderCheckoutSession{}, fmt.Errorf("billing: Stripe checkout response violated the purchase contract")
	}
	parsed, parseErr := url.Parse(session.URL)
	if parseErr != nil || parsed.Scheme != "https" || (parsed.Host != "checkout.stripe.com" && !strings.HasSuffix(parsed.Host, ".stripe.com")) {
		return ProviderCheckoutSession{}, fmt.Errorf("billing: Stripe checkout returned an invalid hosted URL")
	}
	return ProviderCheckoutSession{ID: session.ID, URL: session.URL}, nil
}

var allowedStripeWebhookTypes = map[stripe.EventType]struct{}{
	stripe.EventTypeCheckoutSessionCompleted:             {},
	stripe.EventTypeCheckoutSessionAsyncPaymentSucceeded: {},
	stripe.EventTypeCheckoutSessionAsyncPaymentFailed:    {},
	stripe.EventTypeCheckoutSessionExpired:               {},
	stripe.EventTypeRefundCreated:                        {},
	stripe.EventTypeRefundUpdated:                        {},
	stripe.EventTypeRefundFailed:                         {},
	stripe.EventTypeChargeDisputeCreated:                 {},
	stripe.EventTypeChargeDisputeClosed:                  {},
	stripe.EventTypeChargeDisputeFundsWithdrawn:          {},
	stripe.EventTypeChargeDisputeFundsReinstated:         {},
	stripe.EventTypeChargeDisputeUpdated:                 {},
	stripe.EventTypePaymentIntentPaymentFailed:           {},
}

func (g *StripeGateway) VerifyWebhook(payload []byte, signature string) (VerifiedWebhookEvent, error) {
	if g == nil || g.client == nil || len(payload) == 0 || len(payload) > maxWebhookPayloadBytes || signature == "" {
		return VerifiedWebhookEvent{}, ErrWebhookRejected
	}
	event, err := g.client.ConstructEvent(payload, signature, g.webhookSecret, stripe.WithTolerance(stripeSignatureTolerance))
	if err != nil {
		return VerifiedWebhookEvent{}, fmt.Errorf("%w: signature, timestamp, or API version", ErrWebhookRejected)
	}
	if event.Livemode != g.liveMode || (event.Account != "" && event.Account != g.accountID) {
		return VerifiedWebhookEvent{}, fmt.Errorf("%w: account or livemode", ErrWebhookRejected)
	}
	if _, ok := allowedStripeWebhookTypes[event.Type]; !ok {
		return VerifiedWebhookEvent{}, fmt.Errorf("%w: event type", ErrWebhookRejected)
	}
	if event.ID == "" || event.Created <= 0 || event.Data == nil || len(event.Data.Raw) == 0 {
		return VerifiedWebhookEvent{}, fmt.Errorf("%w: incomplete event", ErrWebhookRejected)
	}
	var object struct {
		ID                string            `json:"id"`
		ClientReferenceID string            `json:"client_reference_id"`
		Metadata          map[string]string `json:"metadata"`
	}
	if err := json.Unmarshal(event.Data.Raw, &object); err != nil || object.ID == "" {
		return VerifiedWebhookEvent{}, fmt.Errorf("%w: invalid event object", ErrWebhookRejected)
	}
	customerID := object.Metadata["yscale_customer_id"]
	if strings.HasPrefix(string(event.Type), "checkout.session.") {
		if customerID == "" || object.Metadata["yscale_checkout_id"] == "" || object.ClientReferenceID != customerID {
			return VerifiedWebhookEvent{}, fmt.Errorf("%w: checkout mapping", ErrWebhookRejected)
		}
	}
	return VerifiedWebhookEvent{
		Provider: stripeProvider, ProviderAccountID: g.accountID, EventID: event.ID,
		EventType: string(event.Type), ObjectID: object.ID, CustomerID: customerID,
		APIVersion: event.APIVersion, LiveMode: event.Livemode,
		ProviderCreatedAt: time.Unix(event.Created, 0).UTC(), Payload: payload,
	}, nil
}

func (g *StripeGateway) ResolveWebhook(ctx context.Context, delivery WebhookDelivery) (WebhookResolution, error) {
	if g == nil || g.client == nil || delivery.Provider != stripeProvider ||
		delivery.ProviderAccountID != g.accountID || delivery.LiveMode != g.liveMode {
		return WebhookResolution{}, ErrEnvironmentMismatch
	}
	switch {
	case strings.HasPrefix(delivery.EventType, "checkout.session."):
		params := &stripe.CheckoutSessionRetrieveParams{}
		params.AddExpand("payment_intent")
		session, err := g.client.V1CheckoutSessions.Retrieve(ctx, delivery.ObjectID, params)
		if err != nil {
			return WebhookResolution{}, fmt.Errorf("billing: retrieve Stripe checkout: %w", err)
		}
		if session == nil || session.ID != delivery.ObjectID || session.Livemode != g.liveMode ||
			session.Currency != stripe.CurrencyUSD || session.AmountTotal <= 0 ||
			session.ClientReferenceID == "" || session.Metadata["yscale_customer_id"] != session.ClientReferenceID ||
			session.Metadata["yscale_checkout_id"] == "" {
			return WebhookResolution{}, fmt.Errorf("billing: Stripe checkout state violated the purchase contract")
		}
		amount, err := safeMul(session.AmountTotal, microUSDPerCent)
		if err != nil {
			return WebhookResolution{}, err
		}
		resolution := WebhookResolution{
			Kind: WebhookNoEconomicChange, ProviderSessionID: session.ID,
			CheckoutID: session.Metadata["yscale_checkout_id"], CustomerID: session.ClientReferenceID,
			AmountMicroUSD: amount,
		}
		if session.PaymentStatus == stripe.CheckoutSessionPaymentStatusPaid {
			if session.PaymentIntent == nil || session.PaymentIntent.ID == "" {
				return WebhookResolution{}, fmt.Errorf("billing: settled Stripe checkout has no payment intent")
			}
			resolution.Kind = WebhookCheckoutSettled
			resolution.PaymentObjectID = session.PaymentIntent.ID
		} else if delivery.EventType == string(stripe.EventTypeCheckoutSessionAsyncPaymentFailed) {
			resolution.Kind = WebhookCheckoutFailed
		} else if session.Status == stripe.CheckoutSessionStatusExpired || delivery.EventType == string(stripe.EventTypeCheckoutSessionExpired) {
			resolution.Kind = WebhookCheckoutExpired
		}
		return resolution, nil

	case strings.HasPrefix(delivery.EventType, "refund."):
		refund, err := g.client.V1Refunds.Retrieve(ctx, delivery.ObjectID, nil)
		if err != nil {
			return WebhookResolution{}, fmt.Errorf("billing: retrieve Stripe refund: %w", err)
		}
		if refund == nil || refund.ID != delivery.ObjectID || refund.Currency != stripe.CurrencyUSD || refund.Amount <= 0 {
			return WebhookResolution{}, fmt.Errorf("billing: Stripe refund state violated the purchase contract")
		}
		if refund.Status != stripe.RefundStatusSucceeded {
			return WebhookResolution{Kind: WebhookNoEconomicChange}, nil
		}
		if refund.PaymentIntent == nil || refund.PaymentIntent.ID == "" {
			return WebhookResolution{}, fmt.Errorf("billing: succeeded Stripe refund has no payment intent")
		}
		amount, err := safeMul(refund.Amount, microUSDPerCent)
		if err != nil {
			return WebhookResolution{}, err
		}
		return WebhookResolution{
			Kind: WebhookRefundSucceeded, PaymentObjectID: refund.PaymentIntent.ID,
			ReversalObjectID: refund.ID, AmountMicroUSD: amount,
		}, nil

	case strings.HasPrefix(delivery.EventType, "charge.dispute."):
		params := &stripe.DisputeRetrieveParams{}
		params.AddExpand("payment_intent")
		dispute, err := g.client.V1Disputes.Retrieve(ctx, delivery.ObjectID, params)
		if err != nil {
			return WebhookResolution{}, fmt.Errorf("billing: retrieve Stripe dispute: %w", err)
		}
		if dispute == nil || dispute.ID != delivery.ObjectID || dispute.Livemode != g.liveMode ||
			dispute.Currency != stripe.CurrencyUSD || dispute.Amount <= 0 ||
			dispute.PaymentIntent == nil || dispute.PaymentIntent.ID == "" {
			return WebhookResolution{}, fmt.Errorf("billing: Stripe dispute state violated the purchase contract")
		}
		amount, err := safeMul(dispute.Amount, microUSDPerCent)
		if err != nil {
			return WebhookResolution{}, err
		}
		resolution := WebhookResolution{
			PaymentObjectID: dispute.PaymentIntent.ID, ReversalObjectID: dispute.ID,
			AmountMicroUSD: amount, ReversalKind: stripeDisputeReversalKind(dispute),
		}
		switch dispute.Status {
		case stripe.DisputeStatusNeedsResponse, stripe.DisputeStatusUnderReview, stripe.DisputeStatusLost:
			resolution.Kind = WebhookDisputeWithdrawn
		case stripe.DisputeStatusWon, stripe.DisputeStatusWarningClosed, stripe.DisputeStatusPrevented:
			resolution.Kind = WebhookDisputeRestored
		case stripe.DisputeStatusWarningNeedsResponse, stripe.DisputeStatusWarningUnderReview:
			resolution.Kind = WebhookNoEconomicChange
		default:
			return WebhookResolution{}, fmt.Errorf("billing: Stripe dispute has unsupported status %q", dispute.Status)
		}
		return resolution, nil

	case delivery.EventType == string(stripe.EventTypePaymentIntentPaymentFailed):
		return WebhookResolution{Kind: WebhookNoEconomicChange}, nil
	default:
		return WebhookResolution{}, fmt.Errorf("billing: Stripe event type %q has no economic processor", delivery.EventType)
	}
}

// ExternalCashSnapshot scans the complete bounded set of PaymentIntents and
// disputes for this Stripe account. Only PaymentIntents carrying both Yscale
// identity tags are economic objects; partial tags fail closed. Provider IDs
// stay in memory for reconciliation and are never logged here.
func (g *StripeGateway) ExternalCashSnapshot(ctx context.Context, maxObjects int) ([]ExternalCashObject, error) {
	if g == nil || g.client == nil {
		return nil, fmt.Errorf("billing: Stripe cash reconciliation is not configured")
	}
	if maxObjects <= 0 {
		return nil, fmt.Errorf("%w: external cash limit must be positive", ErrInvalidArgument)
	}

	params := &stripe.PaymentIntentListParams{ListParams: stripe.ListParams{Limit: stripe.Int64(100)}}
	params.AddExpand("data.latest_charge")
	byPayment := make(map[string]ExternalCashObject)
	scanned := 0
	for intent, err := range g.client.V1PaymentIntents.List(ctx, params).All(ctx) {
		if err != nil {
			return nil, fmt.Errorf("billing: list Stripe payment intents: %w", err)
		}
		scanned++
		if scanned > maxObjects {
			return nil, fmt.Errorf("billing: Stripe payment-intent scan exceeds %d objects", maxObjects)
		}
		if intent == nil {
			return nil, fmt.Errorf("billing: Stripe returned an empty payment intent")
		}
		checkoutID := intent.Metadata["yscale_checkout_id"]
		customerID := intent.Metadata["yscale_customer_id"]
		if checkoutID == "" && customerID == "" {
			continue
		}
		if checkoutID == "" || customerID == "" {
			return nil, fmt.Errorf("billing: Stripe payment intent has partial Yscale identity")
		}
		if err := validateID("checkout ID", checkoutID); err != nil {
			return nil, fmt.Errorf("billing: Stripe payment intent identity: %w", err)
		}
		if err := validateID("customer ID", customerID); err != nil {
			return nil, fmt.Errorf("billing: Stripe payment intent identity: %w", err)
		}
		if intent.Livemode != g.liveMode {
			return nil, ErrEnvironmentMismatch
		}
		if intent.Status != stripe.PaymentIntentStatusSucceeded {
			if intent.AmountReceived == 0 {
				continue
			}
			return nil, fmt.Errorf("billing: Stripe payment intent has cash in a nonterminal state")
		}
		if intent.ID == "" || intent.Currency != stripe.CurrencyUSD || intent.AmountReceived <= 0 ||
			intent.LatestCharge == nil || intent.LatestCharge.ID == "" ||
			intent.LatestCharge.Currency != stripe.CurrencyUSD || intent.LatestCharge.Livemode != g.liveMode ||
			!intent.LatestCharge.Paid || intent.LatestCharge.Status != stripe.ChargeStatusSucceeded ||
			intent.LatestCharge.AmountCaptured != intent.AmountReceived ||
			intent.LatestCharge.AmountRefunded < 0 || intent.LatestCharge.AmountRefunded > intent.AmountReceived {
			return nil, fmt.Errorf("billing: Stripe payment intent violated the settled-cash contract")
		}
		credited, err := safeMul(intent.AmountReceived, microUSDPerCent)
		if err != nil {
			return nil, err
		}
		reversed, err := safeMul(intent.LatestCharge.AmountRefunded, microUSDPerCent)
		if err != nil {
			return nil, err
		}
		if _, duplicate := byPayment[intent.ID]; duplicate {
			return nil, fmt.Errorf("billing: Stripe returned a duplicate payment intent")
		}
		byPayment[intent.ID] = ExternalCashObject{
			Provider: stripeProvider, ProviderAccountID: g.accountID, LiveMode: g.liveMode,
			PaymentObjectID: intent.ID, CustomerID: customerID,
			CreditedMicroUSD: credited, ReversedMicroUSD: reversed,
		}
	}

	disputeParams := &stripe.DisputeListParams{ListParams: stripe.ListParams{Limit: stripe.Int64(100)}}
	disputeParams.AddExpand("data.payment_intent")
	scanned = 0
	for dispute, err := range g.client.V1Disputes.List(ctx, disputeParams).All(ctx) {
		if err != nil {
			return nil, fmt.Errorf("billing: list Stripe disputes: %w", err)
		}
		scanned++
		if scanned > maxObjects {
			return nil, fmt.Errorf("billing: Stripe dispute scan exceeds %d objects", maxObjects)
		}
		if dispute == nil || dispute.PaymentIntent == nil || dispute.PaymentIntent.ID == "" {
			return nil, fmt.Errorf("billing: Stripe dispute has no payment intent")
		}
		object, managed := byPayment[dispute.PaymentIntent.ID]
		if !managed {
			continue
		}
		if dispute.Livemode != g.liveMode || dispute.Currency != stripe.CurrencyUSD || dispute.Amount <= 0 {
			return nil, fmt.Errorf("billing: Stripe dispute violated the cash contract")
		}
		withdrawn, err := stripeDisputeFundsWithdrawn(dispute.Status)
		if err != nil {
			return nil, err
		}
		if !withdrawn {
			continue
		}
		amount, err := safeMul(dispute.Amount, microUSDPerCent)
		if err != nil {
			return nil, err
		}
		if object.ReversedMicroUSD > object.CreditedMicroUSD-amount {
			return nil, fmt.Errorf("billing: Stripe reversals exceed settled cash")
		}
		object.ReversedMicroUSD += amount
		byPayment[dispute.PaymentIntent.ID] = object
	}

	objects := make([]ExternalCashObject, 0, len(byPayment))
	for _, object := range byPayment {
		objects = append(objects, object)
	}
	return objects, nil
}

func stripeDisputeFundsWithdrawn(status stripe.DisputeStatus) (bool, error) {
	switch status {
	case stripe.DisputeStatusNeedsResponse, stripe.DisputeStatusUnderReview, stripe.DisputeStatusLost:
		return true, nil
	case stripe.DisputeStatusWon, stripe.DisputeStatusWarningClosed, stripe.DisputeStatusPrevented,
		stripe.DisputeStatusWarningNeedsResponse, stripe.DisputeStatusWarningUnderReview:
		return false, nil
	default:
		return false, fmt.Errorf("billing: Stripe dispute has unsupported reconciliation status %q", status)
	}
}

// stripeDisputeReversalKind preserves the economic cause Stripe exposes on a
// freshly retrieved Dispute. Stripe represents rare post-success ACH failures
// as disputes with one of these three reasons; a card case explicitly marked
// chargeback remains distinguishable from a generic dispute.
func stripeDisputeReversalKind(dispute *stripe.Dispute) ReversalKind {
	if dispute == nil {
		return ReversalDispute
	}
	switch dispute.Reason {
	case stripe.DisputeReasonBankCannotProcess,
		stripe.DisputeReasonIncorrectAccountDetails,
		stripe.DisputeReasonInsufficientFunds:
		return ReversalSettlementFailure
	}
	if dispute.PaymentMethodDetails != nil && dispute.PaymentMethodDetails.Card != nil &&
		dispute.PaymentMethodDetails.Card.CaseType == stripe.DisputePaymentMethodDetailsCardCaseTypeChargeback {
		return ReversalChargeback
	}
	return ReversalDispute
}
