package billing

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool        *pgxpool.Pool
	liveMode    bool
	protectHold func(customerID, workloadRef string) bool
}

const (
	canonicalOperationVersion = 1
	maxWebhookPayloadBytes    = 1 << 20
	maxWebhookErrorBytes      = 1024
)

// NewStore binds one process/database to either test or live money. Mixing
// modes in an account balance is forbidden even though provider objects also
// carry a mode namespace.
func NewStore(pool *pgxpool.Pool, liveMode bool) *Store {
	return &Store{pool: pool, liveMode: liveMode}
}

// OpenStore opens the separately provisioned billing database and verifies its
// immutable environment sentinel. It intentionally performs no DDL; schema
// migration belongs to an out-of-band privileged job.
func OpenStore(ctx context.Context, dsn string, liveMode bool) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("billing: open database: %w", err)
	}
	store := NewStore(pool, liveMode)
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("billing: ping database: %w", err)
	}
	if err := store.assertEnvironment(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("billing: verify database: %w", err)
	}
	return store, nil
}

func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) LiveMode() bool { return s != nil && s.liveMode }

// SetHoldProtection supplies the state-side liveness check used by expiry.
// It is configured once at startup and must be side-effect-free.
func (s *Store) SetHoldProtection(fn func(customerID, workloadRef string) bool) {
	s.protectHold = fn
}

func (s *Store) EnsureAccount(ctx context.Context, customerID string) error {
	if err := s.assertEnvironment(ctx); err != nil {
		return err
	}
	if err := validateID("customer ID", customerID); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO billing.accounts (customer_id) VALUES ($1)
		ON CONFLICT (customer_id) DO NOTHING`, customerID)
	if err != nil {
		return fmt.Errorf("billing: ensure account: %w", err)
	}
	return nil
}

func (s *Store) GetAccount(ctx context.Context, customerID string) (Account, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return Account{}, err
	}
	if err := validateID("customer ID", customerID); err != nil {
		return Account{}, err
	}
	return getAccount(ctx, s.pool, customerID, false)
}

// ListOpenHolds returns pending holds for one tenant only, oldest first.
func (s *Store) ListOpenHolds(ctx context.Context, customerID string, limit int) ([]Hold, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return nil, err
	}
	if err := validateID("customer ID", customerID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, customer_id, workload_ref, amount_micro_usd, captured_micro_usd,
		       state, expires_at, created_at, updated_at, price_quote
		FROM billing.holds
		WHERE customer_id=$1 AND state='pending'
		ORDER BY created_at, id LIMIT $2`, customerID, limit)
	if err != nil {
		return nil, fmt.Errorf("billing: list open holds: %w", err)
	}
	defer rows.Close()
	holds := make([]Hold, 0)
	for rows.Next() {
		hold, err := scanHold(rows)
		if err != nil {
			return nil, err
		}
		holds = append(holds, hold)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("billing: list open holds: %w", err)
	}
	return holds, nil
}

// GrantCredit records settled cash or an approved service credit. If an
// account has debt from a prior reversal, the grant pays debt before becoming
// spendable. A frozen account remains frozen pending explicit reconciliation.
func (s *Store) GrantCredit(ctx context.Context, request CreditGrantRequest) error {
	_, err := s.GrantCreditOnce(ctx, request)
	return err
}

// GrantCreditOnce is GrantCredit with an applied bit for callers that need to
// distinguish a new economic mutation from an exact replay.
func (s *Store) GrantCreditOnce(ctx context.Context, request CreditGrantRequest) (bool, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return false, err
	}
	if request.LiveMode != s.liveMode {
		return false, ErrEnvironmentMismatch
	}
	if err := validateMoneyRequest(request.CustomerID, request.AmountMicroUSD, request.IdempotencyKey); err != nil {
		return false, err
	}
	if err := validateID("funding provider", request.Provider); err != nil {
		return false, err
	}
	if err := validateID("provider account ID", request.ProviderAccountID); err != nil {
		return false, err
	}
	if err := validateID("payment object ID", request.PaymentObjectID); err != nil {
		return false, err
	}
	payloadHash, err := operationHash("grant", request.CustomerID, request)
	if err != nil {
		return false, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("billing: begin grant: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	duplicate, _, err := claimOperation(ctx, tx, request.IdempotencyKey, request.CustomerID, "grant", payloadHash)
	if err != nil {
		return false, err
	}
	if duplicate {
		return false, nil
	}
	account, err := getAccount(ctx, tx, request.CustomerID, true)
	if err != nil {
		return false, err
	}

	debtPayment := min(request.AmountMicroUSD, account.DebtMicroUSD)
	newDebt := account.DebtMicroUSD - debtPayment
	newBalance, err := safeAdd(account.BalanceMicroUSD, request.AmountMicroUSD-debtPayment)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE billing.accounts
		SET balance_micro_usd=$1, debt_micro_usd=$2, updated_at=now()
		WHERE customer_id=$3`, newBalance, newDebt, request.CustomerID); err != nil {
		return false, fmt.Errorf("billing: apply grant: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO billing.funding_sources
		(customer_id, provider, provider_account_id, livemode, payment_object_id,
		 credited_micro_usd, grant_operation_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, request.CustomerID, request.Provider,
		request.ProviderAccountID, request.LiveMode, request.PaymentObjectID,
		request.AmountMicroUSD, request.IdempotencyKey); err != nil {
		return false, mapEconomicWriteError("record funding source", err)
	}
	if err := insertLedger(ctx, tx, request.CustomerID, "credit", request.AmountMicroUSD,
		request.IdempotencyKey, nil, economicReference(request.Provider, request.ProviderAccountID, request.LiveMode, request.PaymentObjectID)); err != nil {
		return false, err
	}
	if err := commit(ctx, tx, "grant"); err != nil {
		return false, err
	}
	return true, nil
}

// ReserveCredit atomically freezes a quote and moves spendable credit into a
// hold before any provider resource may be created.
func (s *Store) ReserveCredit(ctx context.Context, request ReservationRequest) (Hold, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return Hold{}, err
	}
	if err := validateMutationRequest(request.CustomerID, request.IdempotencyKey); err != nil {
		return Hold{}, err
	}
	if err := validateID("workload reference", request.WorkloadRef); err != nil {
		return Hold{}, err
	}
	if err := validatePriceQuote(request.PriceQuote); err != nil {
		return Hold{}, err
	}
	quote, err := json.Marshal(request.PriceQuote)
	if err != nil {
		return Hold{}, fmt.Errorf("billing: encode price quote: %w", err)
	}
	payloadHash, err := operationHash("reserve", request.CustomerID, struct {
		WorkloadRef string     `json:"workload_ref"`
		ExpiresNano int64      `json:"expires_unix_nano"`
		PriceQuote  PriceQuote `json:"price_quote"`
	}{request.WorkloadRef, request.ExpiresAt.UTC().UnixNano(), request.PriceQuote})
	if err != nil {
		return Hold{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Hold{}, fmt.Errorf("billing: begin reserve: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	duplicate, priorHoldID, err := claimOperation(ctx, tx, request.IdempotencyKey, request.CustomerID, "reserve", payloadHash)
	if err != nil {
		return Hold{}, err
	}
	if duplicate {
		if priorHoldID == nil {
			return Hold{}, fmt.Errorf("%w: reservation operation has no hold", ErrInvariantViolation)
		}
		return getHold(ctx, tx, request.CustomerID, *priorHoldID, false)
	}
	var expiryValid bool
	if request.ExpiresAt.IsZero() {
		return Hold{}, fmt.Errorf("%w: hold expiry must be in the future", ErrInvalidArgument)
	}
	if err := tx.QueryRow(ctx, `SELECT $1::timestamptz > now()`, request.ExpiresAt).Scan(&expiryValid); err != nil {
		return Hold{}, fmt.Errorf("billing: validate hold expiry: %w", err)
	}
	if !expiryValid {
		return Hold{}, fmt.Errorf("%w: hold expiry must be in the future", ErrInvalidArgument)
	}
	var quoteIssued, quoteValid bool
	if err := tx.QueryRow(ctx, `
		SELECT $1::timestamptz <= now()+interval '30 seconds', $2::timestamptz > now()`,
		request.PriceQuote.IssuedAt, request.PriceQuote.ValidUntil).Scan(&quoteIssued, &quoteValid); err != nil {
		return Hold{}, fmt.Errorf("billing: validate quote lifetime: %w", err)
	}
	if !quoteIssued || !quoteValid {
		return Hold{}, fmt.Errorf("%w: price quote is future-issued or expired", ErrInvalidArgument)
	}

	account, err := getAccount(ctx, tx, request.CustomerID, true)
	if err != nil {
		return Hold{}, err
	}
	if account.Frozen {
		return Hold{}, ErrAccountFrozen
	}
	amount := request.PriceQuote.MaximumChargeMicroUSD
	if amount > account.SpendableMicroUSD() {
		return Hold{}, fmt.Errorf("%w: requested=%d spendable=%d", ErrInsufficientCredit, amount, account.SpendableMicroUSD())
	}
	newHeld, err := safeAdd(account.HeldMicroUSD, amount)
	if err != nil {
		return Hold{}, err
	}

	var hold Hold
	var storedQuote []byte
	err = tx.QueryRow(ctx, `
		INSERT INTO billing.holds
		(customer_id, workload_ref, amount_micro_usd, reservation_key, price_quote, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		RETURNING id, customer_id, workload_ref, amount_micro_usd, captured_micro_usd,
		          state, expires_at, created_at, updated_at, price_quote`,
		request.CustomerID, request.WorkloadRef, amount,
		request.IdempotencyKey, quote, request.ExpiresAt).
		Scan(&hold.ID, &hold.CustomerID, &hold.WorkloadRef, &hold.AmountMicroUSD,
			&hold.CapturedMicroUSD, &hold.State, &hold.ExpiresAt, &hold.CreatedAt, &hold.UpdatedAt,
			&storedQuote)
	if err != nil {
		return Hold{}, mapWriteError("create hold", err)
	}
	if err := json.Unmarshal(storedQuote, &hold.PriceQuote); err != nil {
		return Hold{}, fmt.Errorf("billing: decode stored quote: %w", err)
	}
	if err := validateStoredQuote(hold); err != nil {
		return Hold{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE billing.operations SET result_hold_id=$1
		WHERE customer_id=$2 AND idempotency_key=$3`, hold.ID, request.CustomerID, request.IdempotencyKey); err != nil {
		return Hold{}, fmt.Errorf("billing: store reservation result: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE billing.accounts SET held_micro_usd=$1, updated_at=now()
		WHERE customer_id=$2`, newHeld, request.CustomerID); err != nil {
		return Hold{}, fmt.Errorf("billing: apply reservation: %w", err)
	}
	if err := insertLedger(ctx, tx, request.CustomerID, "reserve", amount, request.IdempotencyKey, &hold.ID, request.WorkloadRef); err != nil {
		return Hold{}, err
	}
	if err := commit(ctx, tx, "reserve"); err != nil {
		return Hold{}, err
	}
	return hold, nil
}

// CaptureHold consumes actual metered usage once. A partial capture releases
// the unused remainder. Frozen accounts may still capture an existing hold so
// consumed provider usage is not silently lost.
func (s *Store) CaptureHold(ctx context.Context, customerID string, holdID, amount int64, key, usageRef string) error {
	if err := s.assertEnvironment(ctx); err != nil {
		return err
	}
	if err := validateMoneyRequest(customerID, amount, key); err != nil {
		return err
	}
	if holdID <= 0 {
		return fmt.Errorf("%w: invalid hold ID", ErrInvalidArgument)
	}
	payloadHash, err := operationHash("capture", customerID, struct {
		HoldID   int64  `json:"hold_id"`
		Amount   int64  `json:"amount_micro_usd"`
		UsageRef string `json:"usage_ref"`
	}{holdID, amount, usageRef})
	if err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("billing: begin capture: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	duplicate, _, err := claimOperation(ctx, tx, key, customerID, "capture", payloadHash)
	if err != nil || duplicate {
		return err
	}
	hold, err := getHold(ctx, tx, customerID, holdID, true)
	if err != nil {
		return err
	}
	if hold.State != HoldPending {
		return fmt.Errorf("%w: hold %d is %s", ErrIllegalTransition, holdID, hold.State)
	}
	expired, err := holdIsExpired(ctx, tx, customerID, holdID)
	if err != nil {
		return err
	}
	if expired {
		return ErrHoldExpired
	}
	if amount > hold.AmountMicroUSD {
		return fmt.Errorf("%w: capture=%d held=%d", ErrCaptureExceedsHold, amount, hold.AmountMicroUSD)
	}
	account, err := getAccount(ctx, tx, customerID, true)
	if err != nil {
		return err
	}
	if account.HeldMicroUSD < hold.AmountMicroUSD || account.BalanceMicroUSD < amount {
		return fmt.Errorf("%w: account cannot settle hold %d", ErrInvariantViolation, holdID)
	}
	newBalance := account.BalanceMicroUSD - amount
	newHeld := account.HeldMicroUSD - hold.AmountMicroUSD
	if newHeld > newBalance {
		return fmt.Errorf("%w: capture would leave held credit above balance", ErrInvariantViolation)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE billing.accounts SET balance_micro_usd=$1, held_micro_usd=$2, updated_at=now()
		WHERE customer_id=$3`, newBalance, newHeld, customerID); err != nil {
		return fmt.Errorf("billing: apply capture: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE billing.holds SET state='captured', captured_micro_usd=$1, updated_at=now()
		WHERE id=$2 AND customer_id=$3`, amount, holdID, customerID); err != nil {
		return fmt.Errorf("billing: capture hold: %w", err)
	}
	if err := insertLedger(ctx, tx, customerID, "capture", amount, key, &holdID, usageRef); err != nil {
		return err
	}
	return commit(ctx, tx, "capture")
}

func (s *Store) ReleaseHold(ctx context.Context, customerID string, holdID int64, key, reasonRef string) error {
	if err := s.assertEnvironment(ctx); err != nil {
		return err
	}
	if err := validateMutationRequest(customerID, key); err != nil {
		return err
	}
	if holdID <= 0 {
		return fmt.Errorf("%w: invalid hold ID", ErrInvalidArgument)
	}
	payloadHash, err := operationHash("release", customerID, struct {
		HoldID int64  `json:"hold_id"`
		Reason string `json:"reason_ref"`
	}{holdID, reasonRef})
	if err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("billing: begin release: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	duplicate, _, err := claimOperation(ctx, tx, key, customerID, "release", payloadHash)
	if err != nil || duplicate {
		return err
	}
	hold, err := getHold(ctx, tx, customerID, holdID, true)
	if err != nil {
		return err
	}
	if hold.State != HoldPending {
		return fmt.Errorf("%w: hold %d is %s", ErrIllegalTransition, holdID, hold.State)
	}
	account, err := getAccount(ctx, tx, customerID, true)
	if err != nil {
		return err
	}
	if account.HeldMicroUSD < hold.AmountMicroUSD {
		return fmt.Errorf("%w: held balance below hold %d", ErrInvariantViolation, holdID)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE billing.accounts SET held_micro_usd=held_micro_usd-$1, updated_at=now()
		WHERE customer_id=$2`, hold.AmountMicroUSD, customerID); err != nil {
		return fmt.Errorf("billing: apply release: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE billing.holds SET state='released', updated_at=now() WHERE id=$1`, holdID); err != nil {
		return fmt.Errorf("billing: release hold: %w", err)
	}
	if err := insertLedger(ctx, tx, customerID, "release", hold.AmountMicroUSD, key, &holdID, reasonRef); err != nil {
		return err
	}
	return commit(ctx, tx, "release")
}

// ExpireStaleHolds reclaims at most limit stale holds. Individual failures are
// returned (joined) rather than hidden; successful holds remain committed.
func (s *Store) ExpireStaleHolds(ctx context.Context, limit int) (int, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return 0, err
	}
	if limit <= 0 || limit > 1000 {
		return 0, fmt.Errorf("%w: expiry limit must be 1..1000", ErrInvalidArgument)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, customer_id, workload_ref FROM billing.holds
		WHERE state='pending' AND expires_at <= now()
		ORDER BY expires_at, id LIMIT $1`, limit)
	if err != nil {
		return 0, fmt.Errorf("billing: list expired holds: %w", err)
	}
	type candidate struct {
		id                      int64
		customerID, workloadRef string
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.id, &item.customerID, &item.workloadRef); err != nil {
			rows.Close()
			return 0, fmt.Errorf("billing: scan expired hold: %w", err)
		}
		candidates = append(candidates, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, fmt.Errorf("billing: iterate expired holds: %w", err)
	}

	expired := 0
	var failures []error
	for _, item := range candidates {
		if s.protectHold != nil && s.protectHold(item.customerID, item.workloadRef) {
			continue
		}
		changed, err := s.expireHold(ctx, item.id)
		if err != nil {
			failures = append(failures, fmt.Errorf("hold %d: %w", item.id, err))
			continue
		}
		if changed {
			expired++
		}
	}
	return expired, errors.Join(failures...)
}

func (s *Store) expireHold(ctx context.Context, holdID int64) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	hold, err := getHoldByID(ctx, tx, holdID, true)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if hold.State != HoldPending {
		return false, nil
	}
	expired, err := holdIsExpired(ctx, tx, hold.CustomerID, hold.ID)
	if err != nil {
		return false, err
	}
	if !expired {
		return false, nil
	}
	key := fmt.Sprintf("sys:expire-hold:%d", hold.ID)
	payloadHash, err := operationHash("expire", hold.CustomerID, struct {
		HoldID int64 `json:"hold_id"`
	}{hold.ID})
	if err != nil {
		return false, err
	}
	duplicate, _, err := claimOperation(ctx, tx, key, hold.CustomerID, "expire", payloadHash)
	if err != nil || duplicate {
		return false, err
	}
	account, err := getAccount(ctx, tx, hold.CustomerID, true)
	if err != nil {
		return false, err
	}
	if account.HeldMicroUSD < hold.AmountMicroUSD {
		return false, fmt.Errorf("%w: held balance below expired hold", ErrInvariantViolation)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE billing.accounts SET held_micro_usd=held_micro_usd-$1, updated_at=now()
		WHERE customer_id=$2`, hold.AmountMicroUSD, hold.CustomerID); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE billing.holds SET state='expired', updated_at=now() WHERE id=$1`, hold.ID); err != nil {
		return false, err
	}
	if err := insertLedger(ctx, tx, hold.CustomerID, "expiry", hold.AmountMicroUSD, key, &hold.ID, "ttl"); err != nil {
		return false, err
	}
	if err := commit(ctx, tx, "expire"); err != nil {
		return false, err
	}
	return true, nil
}

// ReverseCredit applies one canonical refund/dispute object against its
// original funding source. Ordinary refunds can only return unused credit and
// do not freeze a healthy account. Disputes, chargebacks, and unexpected failed
// settlements always freeze; consumed exposure becomes explicit debt.
func (s *Store) ReverseCredit(ctx context.Context, request CreditReversalRequest) error {
	if err := s.assertEnvironment(ctx); err != nil {
		return err
	}
	if request.FundingLiveMode != s.liveMode || request.ReversalLiveMode != s.liveMode {
		return ErrEnvironmentMismatch
	}
	if err := validateMoneyRequest(request.CustomerID, request.AmountMicroUSD, request.IdempotencyKey); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"funding provider": request.FundingProvider, "payment object ID": request.PaymentObjectID,
		"funding provider account": request.FundingProviderAccountID,
		"reversal provider":        request.ReversalProvider, "reversal object ID": request.ReversalObjectID,
		"reversal provider account": request.ReversalProviderAccountID,
	} {
		if err := validateID(label, value); err != nil {
			return err
		}
	}
	if !validReversalKind(request.Kind) {
		return fmt.Errorf("%w: invalid reversal kind", ErrInvalidArgument)
	}
	payloadHash, err := operationHash("reversal", request.CustomerID, request)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("billing: begin reversal: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	duplicate, _, err := claimOperation(ctx, tx, request.IdempotencyKey, request.CustomerID, "reversal", payloadHash)
	if err != nil || duplicate {
		return err
	}
	source, err := lockFundingSource(ctx, tx, request)
	if err != nil {
		return err
	}
	if _, err := applyCreditReversalTx(ctx, tx, request, source); err != nil {
		return err
	}
	return commit(ctx, tx, "reversal")
}

type lockedFundingSource struct {
	ID       int64
	Credited int64
	Reversed int64
}

type reversalEffects struct {
	BalanceReduction int64
	DebtIncrease     int64
}

func lockFundingSource(ctx context.Context, tx pgx.Tx, request CreditReversalRequest) (lockedFundingSource, error) {
	var source lockedFundingSource
	err := tx.QueryRow(ctx, `
		SELECT id, credited_micro_usd, reversed_micro_usd
		FROM billing.funding_sources
		WHERE customer_id=$1 AND provider=$2 AND provider_account_id=$3
		  AND livemode=$4 AND payment_object_id=$5
		FOR UPDATE`, request.CustomerID, request.FundingProvider,
		request.FundingProviderAccountID, request.FundingLiveMode, request.PaymentObjectID).
		Scan(&source.ID, &source.Credited, &source.Reversed)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedFundingSource{}, ErrNotFound
	}
	if err != nil {
		return lockedFundingSource{}, fmt.Errorf("billing: lock funding source: %w", err)
	}
	return source, nil
}

func applyCreditReversalTx(ctx context.Context, tx pgx.Tx, request CreditReversalRequest, source lockedFundingSource) (reversalEffects, error) {
	newReversed, err := safeAdd(source.Reversed, request.AmountMicroUSD)
	if err != nil {
		return reversalEffects{}, err
	}
	if newReversed > source.Credited {
		return reversalEffects{}, fmt.Errorf("%w: reversal total exceeds original credit", ErrEconomicObjectConflict)
	}
	account, err := getAccount(ctx, tx, request.CustomerID, true)
	if err != nil {
		return reversalEffects{}, err
	}
	covered := min(request.AmountMicroUSD, account.SpendableMicroUSD())
	debtIncrease := request.AmountMicroUSD - covered
	newDebt := account.DebtMicroUSD
	freeze := account.Frozen
	if request.Kind == ReversalRefund {
		// A verified provider refund is authoritative even if credit was already
		// consumed. A fully covered refund leaves a healthy account usable;
		// uncovered cash becomes debt and freezes admission.
		if debtIncrease != 0 {
			newDebt, err = safeAdd(account.DebtMicroUSD, debtIncrease)
			if err != nil {
				return reversalEffects{}, err
			}
			freeze = true
		}
	} else {
		newDebt, err = safeAdd(account.DebtMicroUSD, debtIncrease)
		if err != nil {
			return reversalEffects{}, err
		}
		freeze = true
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO billing.credit_reversals
		(customer_id, funding_source_id, provider, provider_account_id, livemode,
		 reversal_object_id, reversal_kind, amount_micro_usd, operation_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, request.CustomerID, source.ID,
		request.ReversalProvider, request.ReversalProviderAccountID, request.ReversalLiveMode,
		request.ReversalObjectID, string(request.Kind), request.AmountMicroUSD,
		request.IdempotencyKey); err != nil {
		return reversalEffects{}, mapEconomicWriteError("record credit reversal", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE billing.funding_sources SET reversed_micro_usd=$1 WHERE id=$2 AND customer_id=$3`,
		newReversed, source.ID, request.CustomerID); err != nil {
		return reversalEffects{}, fmt.Errorf("billing: update funding reversal total: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE billing.accounts
		SET balance_micro_usd=$1, debt_micro_usd=$2, frozen=$3, updated_at=now()
		WHERE customer_id=$4`, account.BalanceMicroUSD-covered, newDebt, freeze, request.CustomerID); err != nil {
		return reversalEffects{}, fmt.Errorf("billing: apply reversal: %w", err)
	}
	if err := insertLedger(ctx, tx, request.CustomerID, "reversal", request.AmountMicroUSD,
		request.IdempotencyKey, nil, economicReference(request.ReversalProvider, request.ReversalProviderAccountID, request.ReversalLiveMode, request.ReversalObjectID)); err != nil {
		return reversalEffects{}, err
	}
	return reversalEffects{BalanceReduction: covered, DebtIncrease: debtIncrease}, nil
}

// ApplyDispute converges one canonical dispute to the provider's latest cash
// state. The canonical row serializes replicas and makes restored terminal, so
// a stale withdrawal observation cannot reverse credit after reinstatement.
func (s *Store) ApplyDispute(ctx context.Context, request CreditDisputeRequest) error {
	if err := s.assertEnvironment(ctx); err != nil {
		return err
	}
	if request.FundingLiveMode != s.liveMode || request.DisputeLiveMode != s.liveMode {
		return ErrEnvironmentMismatch
	}
	if err := validateMoneyRequest(request.CustomerID, request.AmountMicroUSD, request.IdempotencyKey); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"funding provider": request.FundingProvider, "payment object ID": request.PaymentObjectID,
		"funding provider account": request.FundingProviderAccountID,
		"dispute provider":         request.DisputeProvider, "dispute object ID": request.DisputeObjectID,
		"dispute provider account": request.DisputeProviderAccountID,
	} {
		if err := validateID(label, value); err != nil {
			return err
		}
	}
	if request.State != DisputeWithdrawn && request.State != DisputeRestored {
		return fmt.Errorf("%w: invalid dispute state", ErrInvalidArgument)
	}
	if request.Kind == "" {
		request.Kind = ReversalDispute
	}
	if request.Kind != ReversalDispute && request.Kind != ReversalChargeback && request.Kind != ReversalSettlementFailure {
		return fmt.Errorf("%w: invalid dispute reversal kind", ErrInvalidArgument)
	}

	reversal := CreditReversalRequest{
		CustomerID: request.CustomerID, AmountMicroUSD: request.AmountMicroUSD,
		IdempotencyKey:  request.IdempotencyKey,
		FundingProvider: request.FundingProvider, FundingProviderAccountID: request.FundingProviderAccountID,
		FundingLiveMode: request.FundingLiveMode, PaymentObjectID: request.PaymentObjectID,
		ReversalProvider: request.DisputeProvider, ReversalProviderAccountID: request.DisputeProviderAccountID,
		ReversalLiveMode: request.DisputeLiveMode, ReversalObjectID: request.DisputeObjectID,
		Kind: request.Kind,
	}
	operationType := "reversal"
	payload := any(reversal)
	if request.State == DisputeRestored {
		operationType = "restoration"
		payload = request
	}
	payloadHash, err := operationHash(operationType, request.CustomerID, payload)
	if err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("billing: begin dispute convergence: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	source, err := lockFundingSource(ctx, tx, reversal)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO billing.credit_disputes
		(customer_id, funding_source_id, provider, provider_account_id, livemode,
		 dispute_object_id, amount_micro_usd, state)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'observed')
		ON CONFLICT (provider, provider_account_id, livemode, dispute_object_id) DO NOTHING`,
		request.CustomerID, source.ID, request.DisputeProvider, request.DisputeProviderAccountID,
		request.DisputeLiveMode, request.DisputeObjectID, request.AmountMicroUSD); err != nil {
		return mapEconomicWriteError("record dispute observation", err)
	}
	var customerID, paymentObjectID, state string
	var fundingSourceID, amount, balanceReduction, debtIncrease int64
	if err := tx.QueryRow(ctx, `
		SELECT d.customer_id, d.funding_source_id, f.payment_object_id, d.amount_micro_usd,
		       d.state, d.balance_reduction_micro_usd, d.debt_increase_micro_usd
		FROM billing.credit_disputes d
		JOIN billing.funding_sources f ON f.id=d.funding_source_id AND f.customer_id=d.customer_id
		WHERE d.provider=$1 AND d.provider_account_id=$2 AND d.livemode=$3 AND d.dispute_object_id=$4
		FOR UPDATE OF d`, request.DisputeProvider, request.DisputeProviderAccountID,
		request.DisputeLiveMode, request.DisputeObjectID).
		Scan(&customerID, &fundingSourceID, &paymentObjectID, &amount, &state, &balanceReduction, &debtIncrease); err != nil {
		return fmt.Errorf("billing: lock dispute state: %w", err)
	}
	if customerID != request.CustomerID || fundingSourceID != source.ID || paymentObjectID != request.PaymentObjectID || amount != request.AmountMicroUSD {
		return ErrEconomicObjectConflict
	}

	switch request.State {
	case DisputeWithdrawn:
		if state == string(DisputeWithdrawn) || state == string(DisputeRestored) {
			return commit(ctx, tx, "dispute withdrawal replay")
		}
		duplicate, _, err := claimOperation(ctx, tx, request.IdempotencyKey, request.CustomerID, "reversal", payloadHash)
		if err != nil {
			return err
		}
		if duplicate {
			return fmt.Errorf("%w: dispute reversal operation exists without state", ErrInvariantViolation)
		}
		effects, err := applyCreditReversalTx(ctx, tx, reversal, source)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE billing.credit_disputes
			SET state='withdrawn', balance_reduction_micro_usd=$1, debt_increase_micro_usd=$2,
			    reversal_operation_key=$3, updated_at=now()
			WHERE provider=$4 AND provider_account_id=$5 AND livemode=$6 AND dispute_object_id=$7`,
			effects.BalanceReduction, effects.DebtIncrease, request.IdempotencyKey,
			request.DisputeProvider, request.DisputeProviderAccountID, request.DisputeLiveMode,
			request.DisputeObjectID); err != nil {
			return fmt.Errorf("billing: mark dispute withdrawn: %w", err)
		}
		return commit(ctx, tx, "dispute withdrawal")

	case DisputeRestored:
		if state == string(DisputeRestored) {
			return commit(ctx, tx, "dispute restoration replay")
		}
		if state == "observed" {
			if _, err := tx.Exec(ctx, `
				UPDATE billing.credit_disputes SET state='restored', updated_at=now()
				WHERE provider=$1 AND provider_account_id=$2 AND livemode=$3 AND dispute_object_id=$4`,
				request.DisputeProvider, request.DisputeProviderAccountID, request.DisputeLiveMode,
				request.DisputeObjectID); err != nil {
				return fmt.Errorf("billing: mark undiscounted dispute restored: %w", err)
			}
			return commit(ctx, tx, "dispute restored without withdrawal")
		}
		if balanceReduction+debtIncrease != request.AmountMicroUSD || source.Reversed < request.AmountMicroUSD {
			return fmt.Errorf("%w: dispute reversal effects cannot be restored", ErrInvariantViolation)
		}
		duplicate, _, err := claimOperation(ctx, tx, request.IdempotencyKey, request.CustomerID, "restoration", payloadHash)
		if err != nil {
			return err
		}
		if duplicate {
			return fmt.Errorf("%w: dispute restoration operation exists without state", ErrInvariantViolation)
		}
		account, err := getAccount(ctx, tx, request.CustomerID, true)
		if err != nil {
			return err
		}
		debtReduction := min(account.DebtMicroUSD, debtIncrease)
		// Grants can repay dispute-created debt while the dispute is open. If
		// Stripe later reinstates the funds, that repaid portion becomes
		// spendable instead of trying to subtract debt that no longer exists.
		balanceIncrease, err := safeAdd(balanceReduction, debtIncrease-debtReduction)
		if err != nil {
			return err
		}
		newBalance, err := safeAdd(account.BalanceMicroUSD, balanceIncrease)
		if err != nil {
			return err
		}
		newDebt := account.DebtMicroUSD - debtReduction
		if _, err := tx.Exec(ctx, `
			UPDATE billing.funding_sources SET reversed_micro_usd=reversed_micro_usd-$1
			WHERE id=$2 AND customer_id=$3`, request.AmountMicroUSD, source.ID, request.CustomerID); err != nil {
			return fmt.Errorf("billing: restore funding source: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE billing.credit_disputes
			SET state='restored', restoration_operation_key=$1, updated_at=now()
			WHERE provider=$2 AND provider_account_id=$3 AND livemode=$4 AND dispute_object_id=$5`,
			request.IdempotencyKey, request.DisputeProvider, request.DisputeProviderAccountID,
			request.DisputeLiveMode, request.DisputeObjectID); err != nil {
			return fmt.Errorf("billing: mark dispute restored: %w", err)
		}
		var unresolvedFreeze bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM billing.credit_reversals r
				LEFT JOIN billing.credit_disputes d
				  ON d.provider=r.provider AND d.provider_account_id=r.provider_account_id
				 AND d.livemode=r.livemode AND d.dispute_object_id=r.reversal_object_id
				WHERE r.customer_id=$1
				  AND r.reversal_kind IN ('dispute','chargeback','settlement_failure')
				  AND (d.id IS NULL OR d.state <> 'restored')
			)`, request.CustomerID).Scan(&unresolvedFreeze); err != nil {
			return fmt.Errorf("billing: inspect unresolved reversals: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE billing.accounts
			SET balance_micro_usd=$1, debt_micro_usd=$2, frozen=$3, updated_at=now()
			WHERE customer_id=$4`, newBalance, newDebt, newDebt != 0 || unresolvedFreeze,
			request.CustomerID); err != nil {
			return fmt.Errorf("billing: apply dispute restoration: %w", err)
		}
		if err := insertLedger(ctx, tx, request.CustomerID, "restoration", request.AmountMicroUSD,
			request.IdempotencyKey, nil, economicReference(request.DisputeProvider, request.DisputeProviderAccountID,
				request.DisputeLiveMode, request.DisputeObjectID)); err != nil {
			return err
		}
		return commit(ctx, tx, "dispute restoration")
	}
	return fmt.Errorf("%w: unsupported dispute state", ErrInvalidArgument)
}

func (s *Store) GetHold(ctx context.Context, customerID string, holdID int64) (Hold, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return Hold{}, err
	}
	if err := validateID("customer ID", customerID); err != nil {
		return Hold{}, err
	}
	return getHold(ctx, s.pool, customerID, holdID, false)
}

// RecordVerifiedWebhook stores the exact body only after the HTTP handler has
// verified Stripe's signature against that raw body. Duplicate deliveries are
// accepted only when all immutable metadata and the body hash match.
func (s *Store) RecordVerifiedWebhook(ctx context.Context, event VerifiedWebhookEvent) (id int64, inserted bool, err error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return 0, false, err
	}
	if event.LiveMode != s.liveMode {
		return 0, false, ErrEnvironmentMismatch
	}
	for label, value := range map[string]string{
		"provider": event.Provider, "event ID": event.EventID,
		"provider account ID": event.ProviderAccountID,
		"event type":          event.EventType, "object ID": event.ObjectID,
	} {
		if err := validateID(label, value); err != nil {
			return 0, false, err
		}
	}
	if event.ProviderCreatedAt.IsZero() || len(event.Payload) == 0 ||
		len(event.Payload) > maxWebhookPayloadBytes || !json.Valid(event.Payload) {
		return 0, false, fmt.Errorf("%w: webhook timestamp and JSON body of at most %d bytes are required",
			ErrInvalidArgument, maxWebhookPayloadBytes)
	}
	event.ProviderCreatedAt = event.ProviderCreatedAt.UTC().Truncate(time.Microsecond)
	hash := sha256.Sum256(event.Payload)
	hashText := hex.EncodeToString(hash[:])
	err = s.pool.QueryRow(ctx, `
		INSERT INTO billing.webhook_inbox
		(provider, provider_account_id, provider_event_id, event_type, object_id,
		 customer_id, api_version, livemode, provider_created_at, payload, payload_hash)
		VALUES ($1,$2,$3,$4,$5,NULLIF($6,''),$7,$8,$9,$10,$11)
		ON CONFLICT (provider, provider_account_id, livemode, provider_event_id) DO NOTHING
		RETURNING id`, event.Provider, event.ProviderAccountID, event.EventID, event.EventType, event.ObjectID,
		event.CustomerID, event.APIVersion, event.LiveMode, event.ProviderCreatedAt,
		event.Payload, hashText).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, fmt.Errorf("billing: record webhook: %w", err)
	}
	var existing VerifiedWebhookEvent
	var existingHash string
	err = s.pool.QueryRow(ctx, `
		SELECT id, provider, provider_account_id, provider_event_id, event_type, object_id,
		       COALESCE(customer_id,''), api_version, livemode, provider_created_at, payload_hash
		FROM billing.webhook_inbox
		WHERE provider=$1 AND provider_account_id=$2 AND livemode=$3 AND provider_event_id=$4`,
		event.Provider, event.ProviderAccountID, event.LiveMode, event.EventID).
		Scan(&id, &existing.Provider, &existing.ProviderAccountID, &existing.EventID, &existing.EventType,
			&existing.ObjectID, &existing.CustomerID, &existing.APIVersion,
			&existing.LiveMode, &existing.ProviderCreatedAt, &existingHash)
	if err != nil {
		return 0, false, fmt.Errorf("billing: read duplicate webhook: %w", err)
	}
	if existing.EventType != event.EventType || existing.ObjectID != event.ObjectID ||
		existing.CustomerID != event.CustomerID || existing.APIVersion != event.APIVersion ||
		existing.LiveMode != event.LiveMode || !existing.ProviderCreatedAt.Equal(event.ProviderCreatedAt) ||
		existingHash != hashText {
		return 0, false, ErrIdempotencyConflict
	}
	return id, false, nil
}

// ClaimNextWebhook leases one due delivery. An expired processing lease is
// reclaimable, so a crashed worker cannot strand money events forever.
func (s *Store) ClaimNextWebhook(ctx context.Context, lease time.Duration) (WebhookDelivery, bool, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return WebhookDelivery{}, false, err
	}
	if lease < time.Second || lease > 15*time.Minute {
		return WebhookDelivery{}, false, fmt.Errorf("%w: webhook lease must be 1s..15m", ErrInvalidArgument)
	}
	token, err := randomToken()
	if err != nil {
		return WebhookDelivery{}, false, err
	}
	var delivery WebhookDelivery
	err = s.pool.QueryRow(ctx, `
		WITH candidate AS (
		    SELECT id FROM billing.webhook_inbox
		    WHERE ((state IN ('pending','failed') AND next_attempt_at <= now())
		       OR (state='processing' AND locked_until <= now()))
		    ORDER BY next_attempt_at, id
		    FOR UPDATE SKIP LOCKED LIMIT 1
		)
		UPDATE billing.webhook_inbox AS w
		SET state='processing', attempts=w.attempts+1,
		    locked_until=now()+make_interval(secs => $1), lease_token=$2, last_error=''
		FROM candidate WHERE w.id=candidate.id
		RETURNING w.id, w.provider, w.provider_account_id, w.livemode, w.api_version,
		          w.provider_event_id, w.event_type, w.object_id,
		          COALESCE(w.customer_id,''), w.payload, w.attempts, w.lease_token`,
		lease.Seconds(), token).
		Scan(&delivery.ID, &delivery.Provider, &delivery.ProviderAccountID,
			&delivery.LiveMode, &delivery.APIVersion, &delivery.EventID, &delivery.EventType,
			&delivery.ObjectID, &delivery.CustomerID, &delivery.Payload,
			&delivery.Attempts, &delivery.LeaseToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return WebhookDelivery{}, false, nil
	}
	if err != nil {
		return WebhookDelivery{}, false, fmt.Errorf("billing: claim webhook: %w", err)
	}
	return delivery, true, nil
}

func (s *Store) MarkWebhookProcessed(ctx context.Context, id int64, leaseToken string) error {
	if err := s.assertEnvironment(ctx); err != nil {
		return err
	}
	if id <= 0 || leaseToken == "" {
		return fmt.Errorf("%w: webhook ID and lease token required", ErrInvalidArgument)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE billing.webhook_inbox
		SET state='processed', processed_at=now(), locked_until=NULL, lease_token=NULL
		WHERE id=$1 AND state='processing' AND lease_token=$2 AND locked_until > now()`, id, leaseToken)
	if err != nil {
		return fmt.Errorf("billing: complete webhook: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) MarkWebhookFailed(ctx context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error {
	if err := s.assertEnvironment(ctx); err != nil {
		return err
	}
	if id <= 0 || leaseToken == "" || !retryAt.After(time.Now()) || maxAttempts < 1 || maxAttempts > 100 {
		return fmt.Errorf("%w: webhook ID, lease, future retry, and max attempts 1..100 are required", ErrInvalidArgument)
	}
	safeError = truncateUTF8(safeError, maxWebhookErrorBytes)
	tag, err := s.pool.Exec(ctx, `
		UPDATE billing.webhook_inbox
		SET state=CASE WHEN attempts >= $5 THEN 'dead_letter' ELSE 'failed' END,
		    next_attempt_at=$1, last_error=$2,
		    locked_until=NULL, lease_token=NULL
		WHERE id=$3 AND state='processing' AND lease_token=$4 AND locked_until > now()`,
		retryAt, safeError, id, leaseToken, maxAttempts)
	if err != nil {
		return fmt.Errorf("billing: fail webhook: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) assertEnvironment(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("%w: nil billing store", ErrInvalidArgument)
	}
	var storedMode bool
	if err := s.pool.QueryRow(ctx, `
		SELECT livemode FROM billing.environment WHERE singleton`).Scan(&storedMode); err != nil {
		return fmt.Errorf("billing: verify database environment: %w", err)
	}
	if storedMode != s.liveMode {
		return ErrEnvironmentMismatch
	}
	return nil
}

func claimOperation(ctx context.Context, tx pgx.Tx, key, customerID, operationType, payloadHash string) (bool, *int64, error) {
	var inserted string
	err := tx.QueryRow(ctx, `
		INSERT INTO billing.operations
		(idempotency_key, customer_id, operation_type, canonical_version, payload_hash)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING idempotency_key`,
		key, customerID, operationType, canonicalOperationVersion, payloadHash).Scan(&inserted)
	if err == nil {
		return false, nil, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, nil, fmt.Errorf("billing: claim idempotency key: %w", err)
	}
	var existingCustomer, existingType, existingHash string
	var existingVersion int
	var resultHoldID *int64
	err = tx.QueryRow(ctx, `
		SELECT customer_id, operation_type, canonical_version, payload_hash, result_hold_id
		FROM billing.operations WHERE customer_id=$1 AND idempotency_key=$2`, customerID, key).
		Scan(&existingCustomer, &existingType, &existingVersion, &existingHash, &resultHoldID)
	if err != nil {
		return false, nil, fmt.Errorf("billing: read idempotency key: %w", err)
	}
	if existingCustomer != customerID || existingType != operationType ||
		existingVersion != canonicalOperationVersion || existingHash != payloadHash {
		return false, nil, ErrIdempotencyConflict
	}
	return true, resultHoldID, nil
}

func getAccount(ctx context.Context, db dbtx, customerID string, lock bool) (Account, error) {
	query := `SELECT customer_id, currency, balance_micro_usd, held_micro_usd, debt_micro_usd,
	                 frozen, created_at, updated_at
	          FROM billing.accounts WHERE customer_id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	var account Account
	err := db.QueryRow(ctx, query, customerID).Scan(&account.CustomerID, &account.Currency, &account.BalanceMicroUSD,
		&account.HeldMicroUSD, &account.DebtMicroUSD, &account.Frozen,
		&account.CreatedAt, &account.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrNotFound
	}
	if err != nil {
		return Account{}, fmt.Errorf("billing: read account: %w", err)
	}
	if account.HeldMicroUSD > account.BalanceMicroUSD {
		return Account{}, ErrInvariantViolation
	}
	return account, nil
}

func getHold(ctx context.Context, db dbtx, customerID string, holdID int64, lock bool) (Hold, error) {
	query := `SELECT id, customer_id, workload_ref, amount_micro_usd, captured_micro_usd,
	                 state, expires_at, created_at, updated_at, price_quote
	          FROM billing.holds WHERE id=$1 AND customer_id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	return scanHold(db.QueryRow(ctx, query, holdID, customerID))
}

func getHoldByID(ctx context.Context, db dbtx, holdID int64, lock bool) (Hold, error) {
	query := `SELECT id, customer_id, workload_ref, amount_micro_usd, captured_micro_usd,
	                 state, expires_at, created_at, updated_at, price_quote
	          FROM billing.holds WHERE id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	return scanHold(db.QueryRow(ctx, query, holdID))
}

func holdIsExpired(ctx context.Context, db dbtx, customerID string, holdID int64) (bool, error) {
	var expired bool
	err := db.QueryRow(ctx, `
		SELECT expires_at <= now() FROM billing.holds
		WHERE id=$1 AND customer_id=$2`, holdID, customerID).Scan(&expired)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, fmt.Errorf("billing: check hold expiry: %w", err)
	}
	return expired, nil
}

func scanHold(row pgx.Row) (Hold, error) {
	var hold Hold
	var quote []byte
	err := row.Scan(&hold.ID, &hold.CustomerID, &hold.WorkloadRef, &hold.AmountMicroUSD,
		&hold.CapturedMicroUSD, &hold.State, &hold.ExpiresAt, &hold.CreatedAt, &hold.UpdatedAt, &quote)
	if errors.Is(err, pgx.ErrNoRows) {
		return Hold{}, ErrNotFound
	}
	if err != nil {
		return Hold{}, fmt.Errorf("billing: read hold: %w", err)
	}
	if err := json.Unmarshal(quote, &hold.PriceQuote); err != nil {
		return Hold{}, fmt.Errorf("billing: decode hold quote: %w", err)
	}
	if err := validateStoredQuote(hold); err != nil {
		return Hold{}, err
	}
	return hold, nil
}

func insertLedger(ctx context.Context, tx pgx.Tx, customerID, entryType string, amount int64, key string, holdID *int64, externalRef string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO billing.ledger
		(customer_id, entry_type, amount_micro_usd, operation_key, hold_id, external_ref)
		VALUES ($1,$2,$3,$4,$5,$6)`, customerID, entryType, amount, key, holdID, externalRef)
	if err != nil {
		return fmt.Errorf("billing: append %s ledger entry: %w", entryType, err)
	}
	return nil
}

func economicReference(provider, providerAccountID string, liveMode bool, objectID string) string {
	mode := "test"
	if liveMode {
		mode = "live"
	}
	return provider + "/" + providerAccountID + "/" + mode + "/" + objectID
}

func validateMoneyRequest(customerID string, amount int64, key string) error {
	if err := validateMutationRequest(customerID, key); err != nil {
		return err
	}
	return ValidatePositiveAmount(amount)
}

func validReversalKind(kind ReversalKind) bool {
	return kind == ReversalRefund || kind == ReversalDispute ||
		kind == ReversalChargeback || kind == ReversalSettlementFailure
}

func validateMutationRequest(customerID, key string) error {
	if err := validateID("customer ID", customerID); err != nil {
		return err
	}
	if strings.TrimSpace(key) == "" || len(key) > 255 {
		return fmt.Errorf("%w: idempotency key must be 1..255 bytes", ErrInvalidArgument)
	}
	if strings.HasPrefix(strings.ToLower(key), "sys:") {
		return fmt.Errorf("%w: idempotency key uses reserved system prefix", ErrInvalidArgument)
	}
	return nil
}

func validateID(label, value string) error {
	if strings.TrimSpace(value) == "" || len(value) > 255 {
		return fmt.Errorf("%w: %s must be 1..255 bytes", ErrInvalidArgument, label)
	}
	return nil
}

func validatePriceQuote(quote PriceQuote) error {
	for label, value := range map[string]string{
		"quote ID": quote.QuoteID, "provider": quote.Provider, "SKU": quote.SKU,
		"region": quote.Region,
	} {
		if err := validateID(label, value); err != nil {
			return err
		}
	}
	if quote.PricingVersion <= 0 || quote.Currency != "USD" {
		return fmt.Errorf("%w: positive pricing version and USD currency required", ErrInvalidArgument)
	}
	if err := ValidatePositiveAmount(quote.ProviderRateMicroUSDPerHour); err != nil {
		return fmt.Errorf("%w: provider rate", err)
	}
	if err := ValidatePositiveAmount(quote.CustomerRateMicroUSDPerHour); err != nil {
		return fmt.Errorf("%w: customer rate", err)
	}
	if quote.PlatformFeeBasisPoints < 0 || quote.PlatformFeeBasisPoints > 10_000 ||
		quote.TaxMicroUSD < 0 || quote.MaximumDurationSeconds <= 0 ||
		quote.MaximumDurationSeconds > 30*24*60*60 || quote.IssuedAt.IsZero() ||
		quote.ValidUntil.IsZero() || !quote.ValidUntil.After(quote.IssuedAt) ||
		quote.ValidUntil.Sub(quote.IssuedAt) > 15*time.Minute {
		return fmt.Errorf("%w: invalid fee, tax, duration, or quote lifetime", ErrInvalidArgument)
	}
	fee, err := ceilMulDiv(quote.ProviderRateMicroUSDPerHour, int64(quote.PlatformFeeBasisPoints), 10_000)
	if err != nil {
		return err
	}
	expectedRate, err := safeAdd(quote.ProviderRateMicroUSDPerHour, fee)
	if err != nil {
		return err
	}
	if quote.CustomerRateMicroUSDPerHour != expectedRate {
		return fmt.Errorf("%w: customer rate does not match provider rate plus fee", ErrInvalidArgument)
	}
	usageCharge, err := ceilMulDiv(quote.CustomerRateMicroUSDPerHour, quote.MaximumDurationSeconds, 3600)
	if err != nil {
		return err
	}
	expectedMaximum, err := safeAdd(usageCharge, quote.TaxMicroUSD)
	if err != nil {
		return err
	}
	if quote.MaximumChargeMicroUSD != expectedMaximum {
		return fmt.Errorf("%w: maximum charge does not match rate, duration, and tax", ErrInvalidArgument)
	}
	return nil
}

func validateStoredQuote(hold Hold) error {
	if err := validatePriceQuote(hold.PriceQuote); err != nil {
		return fmt.Errorf("%w: invalid stored price quote: %v", ErrInvariantViolation, err)
	}
	if hold.PriceQuote.MaximumChargeMicroUSD != hold.AmountMicroUSD {
		return fmt.Errorf("%w: stored quote maximum differs from hold", ErrInvariantViolation)
	}
	return nil
}

func ceilMulDiv(a, b, divisor int64) (int64, error) {
	if a < 0 || b < 0 || divisor <= 0 {
		return 0, ErrInvalidAmount
	}
	whole, err := safeMul(a/divisor, b)
	if err != nil {
		return 0, err
	}
	remainderProduct, err := safeMul(a%divisor, b)
	if err != nil {
		return 0, err
	}
	remainder := remainderProduct / divisor
	if remainderProduct%divisor != 0 {
		remainder++
	}
	return safeAdd(whole, remainder)
}

func safeMul(a, b int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, ErrInvalidAmount
	}
	if a != 0 && b > maxInt64/a {
		return 0, ErrAmountOverflow
	}
	return a * b, nil
}

func operationHash(operationType, customerID string, payload any) (string, error) {
	encoded, err := json.Marshal(struct {
		Version       int    `json:"canonical_version"`
		OperationType string `json:"operation_type"`
		CustomerID    string `json:"customer_id"`
		Payload       any    `json:"payload"`
	}{canonicalOperationVersion, operationType, customerID, payload})
	if err != nil {
		return "", fmt.Errorf("billing: hash operation: %w", err)
	}
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:]), nil
}

func truncateUTF8(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	value = strings.ReplaceAll(value, "\x00", "\uFFFD")
	value = strings.ToValidUTF8(value, "\uFFFD")
	if len(value) <= maxBytes {
		return value
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}

func randomToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("billing: create random token: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func mapWriteError(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return fmt.Errorf("%w: %s already exists", ErrIdempotencyConflict, operation)
	}
	return fmt.Errorf("billing: %s: %w", operation, err)
}

func mapEconomicWriteError(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return fmt.Errorf("%w: %s already recorded", ErrEconomicObjectConflict, operation)
	}
	return fmt.Errorf("billing: %s: %w", operation, err)
}

func commit(ctx context.Context, tx pgx.Tx, operation string) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("billing: commit %s: %w", operation, err)
	}
	return nil
}
