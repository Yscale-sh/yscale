package billing

import (
	"context"
	"time"
)

type HoldState string

const (
	HoldPending  HoldState = "pending"
	HoldCaptured HoldState = "captured"
	HoldReleased HoldState = "released"
	HoldExpired  HoldState = "expired"
)

func CanTransition(from, to HoldState) bool {
	return from == HoldPending && (to == HoldCaptured || to == HoldReleased || to == HoldExpired)
}

type Account struct {
	CustomerID      string
	Currency        string
	BalanceMicroUSD int64 // Unspent credit, including the portion currently held.
	HeldMicroUSD    int64
	DebtMicroUSD    int64 // Reversed cash that had already been consumed.
	Frozen          bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

const (
	MaxStatementEntries = 1000
	MaxStatementPeriod  = 366 * 24 * time.Hour
)

// StatementEntry is one customer-safe ledger movement. Provider identities,
// payment object IDs, operation keys, and internal external references never
// cross this read boundary.
type StatementEntry struct {
	LedgerID             int64
	CreatedAt            time.Time
	EntryType            string
	AmountMicroUSD       int64
	BalanceDeltaMicroUSD int64
	HeldDeltaMicroUSD    int64
	NetCreditMicroUSD    int64
	HeldMicroUSD         int64
	WorkloadID           string
	HoldID               *int64
}

// Statement is a bounded, repeatable-read snapshot over [PeriodStart,
// PeriodEnd). LedgerWatermark identifies the newest tenant ledger row visible
// in the same snapshot, even when it falls outside the requested period.
type Statement struct {
	CustomerID               string
	Currency                 string
	PeriodStart              time.Time
	PeriodEnd                time.Time
	GeneratedAt              time.Time
	LedgerWatermark          int64
	OpeningNetCreditMicroUSD int64
	OpeningHeldMicroUSD      int64
	ClosingNetCreditMicroUSD int64
	ClosingHeldMicroUSD      int64
	Entries                  []StatementEntry
}

type CheckoutState string

const (
	CheckoutPendingProvider CheckoutState = "pending_provider"
	CheckoutOpen            CheckoutState = "open"
	CheckoutSettled         CheckoutState = "settled"
	CheckoutFailed          CheckoutState = "failed"
	CheckoutExpired         CheckoutState = "expired"
)

// Checkout is the tenant-visible state of one prepaid-credit purchase. The
// provider session identifier is intentionally internal; callers receive only
// Yscale's stable ID and the provider-hosted URL while the session is open.
type Checkout struct {
	ID                string
	CustomerID        string
	IdempotencyKey    string
	AmountMicroUSD    int64
	Currency          string
	Provider          string
	ProviderAccountID string
	ProviderSessionID string
	URL               string
	LiveMode          bool
	State             CheckoutState
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type BeginCheckoutRequest struct {
	CustomerID        string
	IdempotencyKey    string
	AmountMicroUSD    int64
	Provider          string
	ProviderAccountID string
	LiveMode          bool
}

// ProviderCheckoutRequest contains only server-derived economic identity.
// A browser cannot select provider account, mode, currency, redirect URLs, or
// provider metadata.
type ProviderCheckoutRequest struct {
	CheckoutID     string
	CustomerID     string
	AmountMicroUSD int64
}

type ProviderCheckoutSession struct {
	ID  string
	URL string
}

// CheckoutGateway is the narrow external payment boundary used by the HTTP
// handlers. The implementation owns account and livemode configuration.
type CheckoutGateway interface {
	ProviderAccountID() string
	LiveMode() bool
	CreateCheckout(context.Context, ProviderCheckoutRequest) (ProviderCheckoutSession, error)
	VerifyWebhook(payload []byte, signature string) (VerifiedWebhookEvent, error)
	ResolveWebhook(context.Context, WebhookDelivery) (WebhookResolution, error)
}

type WebhookResolutionKind string

const (
	WebhookNoEconomicChange WebhookResolutionKind = "no_economic_change"
	WebhookCheckoutSettled  WebhookResolutionKind = "checkout_settled"
	WebhookCheckoutFailed   WebhookResolutionKind = "checkout_failed"
	WebhookCheckoutExpired  WebhookResolutionKind = "checkout_expired"
	WebhookRefundSucceeded  WebhookResolutionKind = "refund_succeeded"
	WebhookDisputeWithdrawn WebhookResolutionKind = "dispute_withdrawn"
	WebhookDisputeRestored  WebhookResolutionKind = "dispute_restored"
)

// WebhookResolution is a fresh provider read, never a conclusion drawn from
// the delivered payload. The worker maps these provider objects back to local
// checkout/funding rows before moving money.
type WebhookResolution struct {
	Kind              WebhookResolutionKind
	ReversalKind      ReversalKind
	ProviderSessionID string
	CheckoutID        string
	CustomerID        string
	PaymentObjectID   string
	ReversalObjectID  string
	AmountMicroUSD    int64
}

type FundingSource struct {
	CustomerID       string
	CreditedMicroUSD int64
	ReversedMicroUSD int64
}

type CreditGrantRequest struct {
	CustomerID        string
	AmountMicroUSD    int64
	IdempotencyKey    string
	Provider          string // "stripe" or "yscale" for an approved service credit.
	ProviderAccountID string
	LiveMode          bool
	PaymentObjectID   string // Canonical PaymentIntent/balance transaction/ticket ID.
}

type ReversalKind string

const (
	ReversalRefund            ReversalKind = "refund"
	ReversalDispute           ReversalKind = "dispute"
	ReversalChargeback        ReversalKind = "chargeback"
	ReversalSettlementFailure ReversalKind = "settlement_failure"
)

type CreditReversalRequest struct {
	CustomerID                string
	AmountMicroUSD            int64
	IdempotencyKey            string
	FundingProvider           string
	FundingProviderAccountID  string
	FundingLiveMode           bool
	PaymentObjectID           string
	ReversalProvider          string
	ReversalProviderAccountID string
	ReversalLiveMode          bool
	ReversalObjectID          string // Canonical refund/dispute/chargeback object ID.
	Kind                      ReversalKind
}

type DisputeState string

const (
	DisputeWithdrawn DisputeState = "withdrawn"
	DisputeRestored  DisputeState = "restored"
)

// CreditDisputeRequest converges one canonical provider dispute to the latest
// provider-observed cash state. A restored state is terminal and dominates a
// stale concurrent withdrawal observation.
type CreditDisputeRequest struct {
	CustomerID               string
	AmountMicroUSD           int64
	IdempotencyKey           string
	FundingProvider          string
	FundingProviderAccountID string
	FundingLiveMode          bool
	PaymentObjectID          string
	DisputeProvider          string
	DisputeProviderAccountID string
	DisputeLiveMode          bool
	DisputeObjectID          string
	Kind                     ReversalKind
	State                    DisputeState
}

func (a Account) SpendableMicroUSD() int64 { return a.BalanceMicroUSD - a.HeldMicroUSD }

type Hold struct {
	ID               int64
	CustomerID       string
	WorkloadRef      string
	AmountMicroUSD   int64
	CapturedMicroUSD int64
	State            HoldState
	ExpiresAt        time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	PriceQuote       PriceQuote
}

type ReservationRequest struct {
	CustomerID     string
	WorkloadRef    string
	IdempotencyKey string
	ExpiresAt      time.Time
	PriceQuote     PriceQuote
}

// PriceQuote is generated by trusted server-side pricing code. A future public
// API may sign this representation, but browsers never choose these fields.
// The hold amount is exactly MaximumChargeMicroUSD.
type PriceQuote struct {
	QuoteID                     string    `json:"quote_id"`
	PricingVersion              int       `json:"pricing_version"`
	Currency                    string    `json:"currency"`
	Provider                    string    `json:"provider"`
	SKU                         string    `json:"sku"`
	Region                      string    `json:"region"`
	ProviderRateMicroUSDPerHour int64     `json:"provider_rate_micro_usd_per_hour"`
	CustomerRateMicroUSDPerHour int64     `json:"customer_rate_micro_usd_per_hour"`
	PlatformFeeBasisPoints      int       `json:"platform_fee_basis_points"`
	TaxMicroUSD                 int64     `json:"tax_micro_usd"`
	MaximumDurationSeconds      int64     `json:"maximum_duration_seconds"`
	MaximumChargeMicroUSD       int64     `json:"maximum_charge_micro_usd"`
	IssuedAt                    time.Time `json:"issued_at"`
	ValidUntil                  time.Time `json:"valid_until"`
}

type VerifiedWebhookEvent struct {
	Provider          string
	ProviderAccountID string
	EventID           string
	EventType         string
	ObjectID          string
	CustomerID        string
	APIVersion        string
	LiveMode          bool
	ProviderCreatedAt time.Time
	// Payload is the exact raw body after signature verification. The deployment
	// must encrypt its storage and restrict retention/access. The package never
	// includes this field in logs or errors.
	Payload []byte
}

type WebhookDelivery struct {
	ID                int64
	Provider          string
	ProviderAccountID string
	LiveMode          bool
	APIVersion        string
	EventID           string
	EventType         string
	ObjectID          string
	CustomerID        string
	Payload           []byte
	Attempts          int
	LeaseToken        string
}
