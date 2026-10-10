// yscale:proprietary

package billing

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// GetStatement returns a complete statement for one tenant and refuses to
// truncate. Callers must narrow the date range when it contains more than the
// fixed entry cap.
func (s *Store) GetStatement(ctx context.Context, customerID string, start, end time.Time) (Statement, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return Statement{}, err
	}
	if err := validateID("customer ID", customerID); err != nil {
		return Statement{}, err
	}
	if start.IsZero() || end.IsZero() || !start.Before(end) || end.Sub(start) > MaxStatementPeriod {
		return Statement{}, fmt.Errorf("%w: statement period must be positive and at most 366 days", ErrInvalidArgument)
	}
	start, end = start.UTC(), end.UTC()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Statement{}, fmt.Errorf("billing: begin statement snapshot: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	statement := Statement{CustomerID: customerID, PeriodStart: start, PeriodEnd: end}
	var entryCount int64
	err = tx.QueryRow(ctx, `
		SELECT a.currency, transaction_timestamp(),
		       COALESCE((SELECT max(l.id) FROM billing.ledger l WHERE l.customer_id=a.customer_id), 0),
		       (SELECT count(*) FROM billing.ledger l
		        WHERE l.customer_id=a.customer_id AND l.created_at >= $2 AND l.created_at < $3)
		FROM billing.accounts a WHERE a.customer_id=$1`, customerID, start, end).
		Scan(&statement.Currency, &statement.GeneratedAt, &statement.LedgerWatermark, &entryCount)
	if err != nil {
		if err == pgx.ErrNoRows {
			return Statement{}, ErrNotFound
		}
		return Statement{}, fmt.Errorf("billing: read statement bounds: %w", err)
	}
	if entryCount > MaxStatementEntries {
		return Statement{}, fmt.Errorf("%w: %d entries exceeds %d", ErrStatementTooLarge, entryCount, MaxStatementEntries)
	}

	err = tx.QueryRow(ctx, `
		SELECT
			COALESCE(sum(CASE WHEN l.created_at < $2 THEN CASE
				WHEN l.entry_type IN ('credit','restoration') THEN l.amount_micro_usd
				WHEN l.entry_type IN ('capture','reversal') THEN -l.amount_micro_usd ELSE 0 END ELSE 0 END), 0),
			COALESCE(sum(CASE WHEN l.created_at < $2 THEN CASE
				WHEN l.entry_type='reserve' THEN l.amount_micro_usd
				WHEN l.entry_type='capture' THEN -COALESCE(h.amount_micro_usd,0)
				WHEN l.entry_type IN ('release','expiry') THEN -l.amount_micro_usd ELSE 0 END ELSE 0 END), 0),
			COALESCE(sum(CASE WHEN l.created_at < $3 THEN CASE
				WHEN l.entry_type IN ('credit','restoration') THEN l.amount_micro_usd
				WHEN l.entry_type IN ('capture','reversal') THEN -l.amount_micro_usd ELSE 0 END ELSE 0 END), 0),
			COALESCE(sum(CASE WHEN l.created_at < $3 THEN CASE
				WHEN l.entry_type='reserve' THEN l.amount_micro_usd
				WHEN l.entry_type='capture' THEN -COALESCE(h.amount_micro_usd,0)
				WHEN l.entry_type IN ('release','expiry') THEN -l.amount_micro_usd ELSE 0 END ELSE 0 END), 0)
		FROM billing.ledger l
		LEFT JOIN billing.holds h ON h.customer_id=l.customer_id AND h.id=l.hold_id
		WHERE l.customer_id=$1`, customerID, start, end).
		Scan(&statement.OpeningNetCreditMicroUSD, &statement.OpeningHeldMicroUSD,
			&statement.ClosingNetCreditMicroUSD, &statement.ClosingHeldMicroUSD)
	if err != nil {
		return Statement{}, fmt.Errorf("billing: read statement totals: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT l.id, l.created_at, l.entry_type, l.amount_micro_usd,
		       CASE WHEN l.entry_type IN ('credit','restoration') THEN l.amount_micro_usd
		            WHEN l.entry_type IN ('capture','reversal') THEN -l.amount_micro_usd ELSE 0 END,
		       CASE WHEN l.entry_type='reserve' THEN l.amount_micro_usd
		            WHEN l.entry_type='capture' THEN -COALESCE(h.amount_micro_usd,0)
		            WHEN l.entry_type IN ('release','expiry') THEN -l.amount_micro_usd ELSE 0 END,
		       l.hold_id, COALESCE(h.workload_ref,'')
		FROM billing.ledger l
		LEFT JOIN billing.holds h ON h.customer_id=l.customer_id AND h.id=l.hold_id
		WHERE l.customer_id=$1 AND l.created_at >= $2 AND l.created_at < $3
		ORDER BY l.created_at, l.id`, customerID, start, end)
	if err != nil {
		return Statement{}, fmt.Errorf("billing: list statement entries: %w", err)
	}
	defer rows.Close()

	runningNet, runningHeld := statement.OpeningNetCreditMicroUSD, statement.OpeningHeldMicroUSD
	statement.Entries = make([]StatementEntry, 0, entryCount)
	for rows.Next() {
		var entry StatementEntry
		if err := rows.Scan(&entry.LedgerID, &entry.CreatedAt, &entry.EntryType, &entry.AmountMicroUSD,
			&entry.BalanceDeltaMicroUSD, &entry.HeldDeltaMicroUSD, &entry.HoldID, &entry.WorkloadID); err != nil {
			return Statement{}, fmt.Errorf("billing: scan statement entry: %w", err)
		}
		runningNet += entry.BalanceDeltaMicroUSD
		runningHeld += entry.HeldDeltaMicroUSD
		entry.NetCreditMicroUSD, entry.HeldMicroUSD = runningNet, runningHeld
		statement.Entries = append(statement.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return Statement{}, fmt.Errorf("billing: iterate statement entries: %w", err)
	}
	if runningNet != statement.ClosingNetCreditMicroUSD || runningHeld != statement.ClosingHeldMicroUSD {
		return Statement{}, fmt.Errorf("%w: statement running totals do not close", ErrInvariantViolation)
	}
	if err := tx.Commit(ctx); err != nil {
		return Statement{}, fmt.Errorf("billing: commit statement snapshot: %w", err)
	}
	return statement, nil
}
