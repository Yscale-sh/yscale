// yscale:proprietary

package billing

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
)

const maxExternalCashObjects = 1000

// ExternalCashObject is one provider-observed or ledger-recorded cash object.
// IDs are required for in-memory matching but must never be written to metrics
// or ordinary reconciliation logs.
type ExternalCashObject struct {
	Provider          string
	ProviderAccountID string
	LiveMode          bool
	PaymentObjectID   string
	CustomerID        string
	CreditedMicroUSD  int64
	ReversedMicroUSD  int64
}

// ExternalCashProvider returns a complete, bounded snapshot of the provider's
// Yscale-owned cash objects. A partial scan is an error, never a healthy view.
type ExternalCashProvider interface {
	ExternalCashSnapshot(context.Context, int) ([]ExternalCashObject, error)
}

// ExternalCashSummary contains only bounded counts and is safe for metrics and
// operator logs. Provider and payment identifiers stay inside the comparator.
type ExternalCashSummary struct {
	ProviderOnly int64
	LedgerOnly   int64
	Identity     int64
	Amount       int64
}

func (s ExternalCashSummary) TotalDifferences() int64 {
	return s.ProviderOnly + s.LedgerOnly + s.Identity + s.Amount
}

// ReconciliationSummary contains only bounded aggregate counts. It deliberately
// carries no tenant or economic-object identifiers, making it safe for metrics
// and operational logs.
type ReconciliationSummary struct {
	AccountBalances  int64
	HeldBalances     int64
	FundingReversals int64
	Operations       int64
	EconomicObjects  int64
	ExternalCash     ExternalCashSummary
	UsageCapture     UsageCaptureSummary
}

func (s ReconciliationSummary) TotalDifferences() int64 {
	return s.AccountBalances + s.HeldBalances + s.FundingReversals + s.Operations +
		s.EconomicObjects + s.ExternalCash.TotalDifferences() + s.UsageCapture.TotalDifferences()
}

func (s ReconciliationSummary) Healthy() bool { return s.TotalDifferences() == 0 }

// ReconcileLedger compares every mutable billing materialization with the
// append-only ledger and its canonical economic objects. It never repairs or
// otherwise writes economic state; callers must close paid admission and page
// an operator when any count is non-zero.
func (s *Store) ReconcileLedger(ctx context.Context) (ReconciliationSummary, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return ReconciliationSummary{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return ReconciliationSummary{}, fmt.Errorf("billing: begin ledger reconciliation: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var summary ReconciliationSummary
	err = tx.QueryRow(ctx, `
		WITH
		ledger_net AS (
			SELECT customer_id, sum(CASE
				WHEN entry_type IN ('credit','restoration') THEN amount_micro_usd
				WHEN entry_type IN ('capture','reversal') THEN -amount_micro_usd
				ELSE 0 END) AS amount
			FROM billing.ledger GROUP BY customer_id
		),
		pending_holds AS (
			SELECT customer_id, sum(amount_micro_usd) AS amount
			FROM billing.holds WHERE state='pending' GROUP BY customer_id
		),
		reversal_totals AS (
			SELECT customer_id, funding_source_id, sum(amount_micro_usd) AS amount
			FROM billing.credit_reversals GROUP BY customer_id, funding_source_id
		),
		restoration_totals AS (
			SELECT customer_id, funding_source_id, sum(amount_micro_usd) AS amount
			FROM billing.credit_disputes
			WHERE restoration_operation_key IS NOT NULL
			GROUP BY customer_id, funding_source_id
		),
		object_differences AS (
			SELECT f.id
			FROM billing.funding_sources f
			LEFT JOIN billing.ledger l ON l.customer_id=f.customer_id
			 AND l.operation_key=f.grant_operation_key
			WHERE l.id IS NULL OR l.entry_type<>'credit' OR l.amount_micro_usd<>f.credited_micro_usd
			UNION ALL
			SELECT r.id
			FROM billing.credit_reversals r
			LEFT JOIN billing.ledger l ON l.customer_id=r.customer_id
			 AND l.operation_key=r.operation_key
			WHERE l.id IS NULL OR l.entry_type<>'reversal' OR l.amount_micro_usd<>r.amount_micro_usd
			UNION ALL
			SELECT d.id
			FROM billing.credit_disputes d
			LEFT JOIN billing.ledger l ON l.customer_id=d.customer_id
			 AND l.operation_key=d.restoration_operation_key
			WHERE d.restoration_operation_key IS NOT NULL
			 AND (l.id IS NULL OR l.entry_type<>'restoration' OR l.amount_micro_usd<>d.amount_micro_usd)
		)
		SELECT
			(SELECT count(*) FROM billing.accounts a LEFT JOIN ledger_net n USING (customer_id)
			 WHERE a.balance_micro_usd-a.debt_micro_usd <> coalesce(n.amount,0)),
			(SELECT count(*) FROM billing.accounts a LEFT JOIN pending_holds h USING (customer_id)
			 WHERE a.held_micro_usd <> coalesce(h.amount,0)),
			(SELECT count(*) FROM billing.funding_sources f
			 LEFT JOIN reversal_totals r ON r.customer_id=f.customer_id AND r.funding_source_id=f.id
			 LEFT JOIN restoration_totals d ON d.customer_id=f.customer_id AND d.funding_source_id=f.id
			 WHERE f.reversed_micro_usd <> coalesce(r.amount,0)-coalesce(d.amount,0)),
			(SELECT count(*) FROM billing.operations o LEFT JOIN billing.ledger l
			 ON l.customer_id=o.customer_id AND l.operation_key=o.idempotency_key WHERE l.id IS NULL),
			(SELECT count(*) FROM object_differences)
	`).Scan(&summary.AccountBalances, &summary.HeldBalances, &summary.FundingReversals,
		&summary.Operations, &summary.EconomicObjects)
	if err != nil {
		return ReconciliationSummary{}, fmt.Errorf("billing: reconcile ledger: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ReconciliationSummary{}, fmt.Errorf("billing: commit read-only reconciliation: %w", err)
	}
	return summary, nil
}

// ReconcileExternalCash compares canonical funding rows with a fresh provider
// snapshot. It reads local rows both before and after the provider scan so a
// concurrent webhook cannot produce a falsely healthy result. It never writes
// billing state.
func (s *Store) ReconcileExternalCash(ctx context.Context, provider ExternalCashProvider) (ExternalCashSummary, error) {
	before, err := s.externalCashSnapshot(ctx)
	if err != nil {
		return ExternalCashSummary{}, err
	}
	if provider == nil {
		if len(before) == 0 {
			return ExternalCashSummary{}, nil
		}
		return ExternalCashSummary{}, fmt.Errorf("billing: external cash provider is unavailable")
	}
	external, err := provider.ExternalCashSnapshot(ctx, maxExternalCashObjects)
	if err != nil {
		return ExternalCashSummary{}, fmt.Errorf("billing: read external cash snapshot: %w", err)
	}
	after, err := s.externalCashSnapshot(ctx)
	if err != nil {
		return ExternalCashSummary{}, err
	}
	if !slices.Equal(before, after) {
		return ExternalCashSummary{}, fmt.Errorf("billing: local funding changed during external reconciliation")
	}
	return compareExternalCash(before, external)
}

func (s *Store) externalCashSnapshot(ctx context.Context) ([]ExternalCashObject, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT provider, provider_account_id, livemode, payment_object_id,
		       customer_id, credited_micro_usd, reversed_micro_usd
		FROM billing.funding_sources
		WHERE provider <> 'yscale'
		ORDER BY provider, provider_account_id, livemode, payment_object_id
		LIMIT $1`, maxExternalCashObjects+1)
	if err != nil {
		return nil, fmt.Errorf("billing: read external funding snapshot: %w", err)
	}
	defer rows.Close()
	objects := make([]ExternalCashObject, 0)
	for rows.Next() {
		var object ExternalCashObject
		if err := rows.Scan(&object.Provider, &object.ProviderAccountID, &object.LiveMode,
			&object.PaymentObjectID, &object.CustomerID, &object.CreditedMicroUSD,
			&object.ReversedMicroUSD); err != nil {
			return nil, fmt.Errorf("billing: scan external funding snapshot: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("billing: read external funding snapshot: %w", err)
	}
	if len(objects) > maxExternalCashObjects {
		return nil, fmt.Errorf("billing: external funding snapshot exceeds %d objects", maxExternalCashObjects)
	}
	return objects, nil
}

type externalCashKey struct {
	provider, account, payment string
	live                       bool
}

func compareExternalCash(local, external []ExternalCashObject) (ExternalCashSummary, error) {
	toMap := func(label string, objects []ExternalCashObject) (map[externalCashKey]ExternalCashObject, error) {
		out := make(map[externalCashKey]ExternalCashObject, len(objects))
		for _, object := range objects {
			if object.Provider == "" || object.ProviderAccountID == "" || object.PaymentObjectID == "" ||
				object.CustomerID == "" || object.CreditedMicroUSD <= 0 || object.ReversedMicroUSD < 0 ||
				object.ReversedMicroUSD > object.CreditedMicroUSD {
				return nil, fmt.Errorf("billing: invalid %s external cash object", label)
			}
			key := externalCashKey{provider: object.Provider, account: object.ProviderAccountID,
				payment: object.PaymentObjectID, live: object.LiveMode}
			if _, exists := out[key]; exists {
				return nil, fmt.Errorf("billing: duplicate %s external cash object", label)
			}
			out[key] = object
		}
		return out, nil
	}
	localByKey, err := toMap("ledger", local)
	if err != nil {
		return ExternalCashSummary{}, err
	}
	externalByKey, err := toMap("provider", external)
	if err != nil {
		return ExternalCashSummary{}, err
	}
	var summary ExternalCashSummary
	for key, localObject := range localByKey {
		externalObject, ok := externalByKey[key]
		if !ok {
			summary.LedgerOnly++
			continue
		}
		delete(externalByKey, key)
		if localObject.CustomerID != externalObject.CustomerID {
			summary.Identity++
		}
		if localObject.CreditedMicroUSD != externalObject.CreditedMicroUSD ||
			localObject.ReversedMicroUSD != externalObject.ReversedMicroUSD {
			summary.Amount++
		}
	}
	summary.ProviderOnly = int64(len(externalByKey))
	return summary, nil
}
