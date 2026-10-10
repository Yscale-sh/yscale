// yscale:proprietary

package billing

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const microUSDPerCent int64 = 10_000

// AssertCheckoutSchema verifies the pre-migrated purchase table using the
// runtime role. Activating checkout against an older schema fails startup
// instead of deferring the first discovery to a customer's payment request.
func (s *Store) AssertCheckoutSchema(ctx context.Context) error {
	if err := s.assertEnvironment(ctx); err != nil {
		return err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, customer_id, idempotency_key, amount_micro_usd, currency,
		       provider, provider_account_id, provider_session_id, checkout_url,
		       livemode, state, created_at, updated_at
		FROM billing.checkout_sessions WHERE FALSE`)
	if err != nil {
		return fmt.Errorf("billing: verify checkout schema: %w", err)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("billing: verify checkout schema: %w", err)
	}
	return nil
}

// BeginCheckout commits the local purchase intent before any provider call.
// The customer-scoped key is exactly replayable and conflicting semantic
// retries fail closed.
func (s *Store) BeginCheckout(ctx context.Context, request BeginCheckoutRequest) (Checkout, bool, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return Checkout{}, false, err
	}
	if request.LiveMode != s.liveMode {
		return Checkout{}, false, ErrEnvironmentMismatch
	}
	if err := validateMoneyRequest(request.CustomerID, request.AmountMicroUSD, request.IdempotencyKey); err != nil {
		return Checkout{}, false, err
	}
	if request.AmountMicroUSD%microUSDPerCent != 0 {
		return Checkout{}, false, fmt.Errorf("%w: checkout amount must be whole USD cents", ErrInvalidAmount)
	}
	for label, value := range map[string]string{
		"provider": request.Provider, "provider account ID": request.ProviderAccountID,
	} {
		if err := validateID(label, value); err != nil {
			return Checkout{}, false, err
		}
	}
	token, err := randomToken()
	if err != nil {
		return Checkout{}, false, err
	}
	checkoutID := "co_" + token
	var checkout Checkout
	err = s.pool.QueryRow(ctx, `
		INSERT INTO billing.checkout_sessions
		(id, customer_id, idempotency_key, amount_micro_usd, provider, provider_account_id, livemode)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (customer_id, idempotency_key) DO NOTHING
		RETURNING id, customer_id, idempotency_key, amount_micro_usd, currency,
		          provider, provider_account_id, COALESCE(provider_session_id,''),
		          COALESCE(checkout_url,''), livemode, state, created_at, updated_at`,
		checkoutID, request.CustomerID, request.IdempotencyKey, request.AmountMicroUSD,
		request.Provider, request.ProviderAccountID, request.LiveMode).Scan(
		&checkout.ID, &checkout.CustomerID, &checkout.IdempotencyKey, &checkout.AmountMicroUSD,
		&checkout.Currency, &checkout.Provider, &checkout.ProviderAccountID,
		&checkout.ProviderSessionID, &checkout.URL, &checkout.LiveMode, &checkout.State,
		&checkout.CreatedAt, &checkout.UpdatedAt)
	if err == nil {
		return checkout, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Checkout{}, false, fmt.Errorf("billing: begin checkout: %w", err)
	}
	checkout, err = s.getCheckoutByKey(ctx, request.CustomerID, request.IdempotencyKey)
	if err != nil {
		return Checkout{}, false, err
	}
	if checkout.AmountMicroUSD != request.AmountMicroUSD || checkout.Provider != request.Provider ||
		checkout.ProviderAccountID != request.ProviderAccountID || checkout.LiveMode != request.LiveMode {
		return Checkout{}, false, ErrIdempotencyConflict
	}
	return checkout, false, nil
}

// AttachCheckout records the provider result exactly once. A provider
// idempotency retry may return the same session; a different result is an
// economic-object conflict and is never substituted into the intent.
func (s *Store) AttachCheckout(ctx context.Context, customerID, checkoutID string, provider ProviderCheckoutSession) (Checkout, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return Checkout{}, err
	}
	if err := validateID("customer ID", customerID); err != nil {
		return Checkout{}, err
	}
	if err := validateID("checkout ID", checkoutID); err != nil {
		return Checkout{}, err
	}
	if err := validateID("provider session ID", provider.ID); err != nil {
		return Checkout{}, err
	}
	parsed, err := url.Parse(provider.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || len(provider.URL) > 4096 {
		return Checkout{}, fmt.Errorf("%w: provider checkout URL must be bounded HTTPS", ErrInvalidArgument)
	}
	var checkout Checkout
	err = s.pool.QueryRow(ctx, `
		UPDATE billing.checkout_sessions
		SET provider_session_id=$1, checkout_url=$2, state='open', updated_at=now()
		WHERE id=$3 AND customer_id=$4 AND state='pending_provider'
		RETURNING id, customer_id, idempotency_key, amount_micro_usd, currency,
		          provider, provider_account_id, provider_session_id, checkout_url,
		          livemode, state, created_at, updated_at`, provider.ID, provider.URL,
		checkoutID, customerID).Scan(&checkout.ID, &checkout.CustomerID, &checkout.IdempotencyKey,
		&checkout.AmountMicroUSD, &checkout.Currency, &checkout.Provider,
		&checkout.ProviderAccountID, &checkout.ProviderSessionID, &checkout.URL,
		&checkout.LiveMode, &checkout.State, &checkout.CreatedAt, &checkout.UpdatedAt)
	if err == nil {
		return checkout, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Checkout{}, ErrEconomicObjectConflict
		}
		return Checkout{}, fmt.Errorf("billing: attach checkout provider session: %w", err)
	}
	checkout, err = s.GetCheckout(ctx, customerID, checkoutID)
	if err != nil {
		return Checkout{}, err
	}
	if checkout.ProviderSessionID != provider.ID {
		return Checkout{}, ErrEconomicObjectConflict
	}
	return checkout, nil
}

func (s *Store) GetCheckout(ctx context.Context, customerID, checkoutID string) (Checkout, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return Checkout{}, err
	}
	if err := validateID("customer ID", customerID); err != nil {
		return Checkout{}, err
	}
	if err := validateID("checkout ID", checkoutID); err != nil {
		return Checkout{}, err
	}
	return scanCheckout(s.pool.QueryRow(ctx, `
		SELECT id, customer_id, idempotency_key, amount_micro_usd, currency,
		       provider, provider_account_id, COALESCE(provider_session_id,''),
		       COALESCE(checkout_url,''), livemode, state, created_at, updated_at
		FROM billing.checkout_sessions WHERE customer_id=$1 AND id=$2`, customerID, checkoutID))
}

func (s *Store) GetCheckoutByProviderSession(ctx context.Context, provider, providerAccountID string, liveMode bool, providerSessionID string) (Checkout, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return Checkout{}, err
	}
	if liveMode != s.liveMode {
		return Checkout{}, ErrEnvironmentMismatch
	}
	for label, value := range map[string]string{"provider": provider, "provider account ID": providerAccountID, "provider session ID": providerSessionID} {
		if err := validateID(label, value); err != nil {
			return Checkout{}, err
		}
	}
	return scanCheckout(s.pool.QueryRow(ctx, `
		SELECT id, customer_id, idempotency_key, amount_micro_usd, currency,
		       provider, provider_account_id, provider_session_id, checkout_url,
		       livemode, state, created_at, updated_at
		FROM billing.checkout_sessions
		WHERE provider=$1 AND provider_account_id=$2 AND livemode=$3 AND provider_session_id=$4`,
		provider, providerAccountID, liveMode, providerSessionID))
}

func (s *Store) SetCheckoutState(ctx context.Context, customerID, checkoutID string, state CheckoutState) error {
	if err := s.assertEnvironment(ctx); err != nil {
		return err
	}
	if err := validateID("customer ID", customerID); err != nil {
		return err
	}
	if err := validateID("checkout ID", checkoutID); err != nil {
		return err
	}
	if state != CheckoutSettled && state != CheckoutFailed && state != CheckoutExpired {
		return fmt.Errorf("%w: invalid terminal checkout state", ErrInvalidArgument)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE billing.checkout_sessions SET state=$1, updated_at=now()
		WHERE customer_id=$2 AND id=$3 AND state='open'`, state, customerID, checkoutID)
	if err != nil {
		return fmt.Errorf("billing: set checkout state: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	checkout, err := s.GetCheckout(ctx, customerID, checkoutID)
	if err != nil {
		return err
	}
	if checkout.State != state {
		return ErrIllegalTransition
	}
	return nil
}

func (s *Store) FindFundingSource(ctx context.Context, provider, providerAccountID string, liveMode bool, paymentObjectID string) (FundingSource, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return FundingSource{}, err
	}
	if liveMode != s.liveMode {
		return FundingSource{}, ErrEnvironmentMismatch
	}
	for label, value := range map[string]string{"provider": provider, "provider account ID": providerAccountID, "payment object ID": paymentObjectID} {
		if err := validateID(label, value); err != nil {
			return FundingSource{}, err
		}
	}
	var source FundingSource
	err := s.pool.QueryRow(ctx, `
		SELECT customer_id, credited_micro_usd, reversed_micro_usd
		FROM billing.funding_sources
		WHERE provider=$1 AND provider_account_id=$2 AND livemode=$3 AND payment_object_id=$4`,
		provider, providerAccountID, liveMode, paymentObjectID).
		Scan(&source.CustomerID, &source.CreditedMicroUSD, &source.ReversedMicroUSD)
	if errors.Is(err, pgx.ErrNoRows) {
		return FundingSource{}, ErrNotFound
	}
	if err != nil {
		return FundingSource{}, fmt.Errorf("billing: find funding source: %w", err)
	}
	return source, nil
}

func (s *Store) getCheckoutByKey(ctx context.Context, customerID, key string) (Checkout, error) {
	return scanCheckout(s.pool.QueryRow(ctx, `
		SELECT id, customer_id, idempotency_key, amount_micro_usd, currency,
		       provider, provider_account_id, COALESCE(provider_session_id,''),
		       COALESCE(checkout_url,''), livemode, state, created_at, updated_at
		FROM billing.checkout_sessions WHERE customer_id=$1 AND idempotency_key=$2`, customerID, key))
}

type checkoutRow interface {
	Scan(...any) error
}

func scanCheckout(row checkoutRow) (Checkout, error) {
	var checkout Checkout
	err := row.Scan(&checkout.ID, &checkout.CustomerID, &checkout.IdempotencyKey,
		&checkout.AmountMicroUSD, &checkout.Currency, &checkout.Provider,
		&checkout.ProviderAccountID, &checkout.ProviderSessionID, &checkout.URL,
		&checkout.LiveMode, &checkout.State, &checkout.CreatedAt, &checkout.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Checkout{}, ErrNotFound
	}
	if err != nil {
		return Checkout{}, fmt.Errorf("billing: read checkout: %w", err)
	}
	if checkout.Currency != "USD" || checkout.AmountMicroUSD <= 0 ||
		checkout.AmountMicroUSD%microUSDPerCent != 0 || strings.TrimSpace(checkout.ID) == "" {
		return Checkout{}, ErrInvariantViolation
	}
	return checkout, nil
}
