package state

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
)

const (
	// maxBillingUsageReceipts bounds the small diagnostic snapshot API used by
	// callers that want a handful of receipts. It is not a reconciliation
	// ceiling: BillingUsageReceiptSnapshot pages past it.
	maxBillingUsageReceipts = 1000
	// billingUsagePageSize bounds one database round trip of the complete
	// keyset-paged snapshot. Workload rows with a billing association are
	// retained after termination, so a lifetime total ceiling would eventually
	// fail every paid reconciliation pass.
	billingUsagePageSize = 500
	// maxBillingUsagePageSize bounds any single page query.
	maxBillingUsagePageSize = maxBillingUsageReceipts + 1
)

// BillingUsageReceipt is the bounded, payment-provider-free projection used to
// reconcile a durable workload cost with its prepaid hold. It deliberately
// carries no workload spec or customer credentials.
type BillingUsageReceipt struct {
	CustomerID                 string
	WorkloadID                 string
	WorkloadRef                string
	HoldID                     int64
	ReservedMicroUSD           int64
	ManualAttention            bool
	AuthoritativeUsageRequired bool
	BurstID                    string
	CostPresent                bool
	CostBurstID                string
	Backend                    string
	Basis                      string
	EstimatedUSD               float64
	HourlyUSD                  float64
}

// billingUsagePage returns one keyset page of receipts strictly after
// afterWorkloadID, ordered by the stable unique workload key.
type billingUsagePage func(afterWorkloadID string, pageLimit int) ([]BillingUsageReceipt, error)

type billingUsagePersister interface {
	// readBillingUsageReceipts opens one read-only, repeatable-read view of the
	// durable workload rows and calls read with a page reader bound to it.
	// Assembling a snapshot from several pages of separate transactions would
	// let a concurrent workload write tear it, and a torn snapshot reads as a
	// reconciliation difference that closes paid admission.
	readBillingUsageReceipts(ctx context.Context, read func(page billingUsagePage) error) error
}

// BillingUsageReceipts returns a bounded diagnostic snapshot of workload rows
// that carry a billing association, and fails closed when more than limit rows
// exist. It is for callers that want a small, cheap view; managed usage
// reconciliation must use BillingUsageReceiptSnapshot instead, because a total
// limit here is a lifetime ceiling.
//
// durable=false means this is the single-process in-memory store; managed
// billing reconciliation must fail closed on that result rather than compare it
// with a durable billing database.
func (s *Store) BillingUsageReceipts(ctx context.Context, limit int) (receipts []BillingUsageReceipt, durable bool, err error) {
	if limit <= 0 || limit > maxBillingUsageReceipts {
		return nil, false, fmt.Errorf("state: billing usage receipt limit must be 1..%d", maxBillingUsageReceipts)
	}
	receipts, durable, err = s.billingUsageReceipts(ctx, limit+1)
	if err != nil {
		return nil, durable, err
	}
	if len(receipts) > limit {
		return nil, durable, fmt.Errorf("state: billing usage snapshot exceeds %d receipts", limit)
	}
	return receipts, durable, nil
}

// BillingUsageReceiptSnapshot returns the complete deterministic snapshot of
// every workload row carrying a billing association. Each database query stays
// bounded by a fixed page size, but the assembled snapshot has no total-row
// ceiling, so reconciliation stays complete after any number of lifetime paid
// workloads.
func (s *Store) BillingUsageReceiptSnapshot(ctx context.Context) ([]BillingUsageReceipt, bool, error) {
	return s.billingUsageReceipts(ctx, 0)
}

// billingUsageReceipts pages the durable store until exhausted. stopAfter > 0
// stops early once that many rows are held, which is only enough for the
// bounded diagnostic API's overflow check.
func (s *Store) billingUsageReceipts(ctx context.Context, stopAfter int) ([]BillingUsageReceipt, bool, error) {
	s.mu.RLock()
	p := s.persist
	if p == nil {
		receipts := make([]BillingUsageReceipt, 0)
		for _, workload := range s.workloads {
			if workload.Billing != nil {
				receipts = append(receipts, billingUsageReceipt(workload))
			}
		}
		s.mu.RUnlock()
		sort.Slice(receipts, func(i, j int) bool { return receipts[i].WorkloadID < receipts[j].WorkloadID })
		return receipts, false, nil
	}
	s.mu.RUnlock()
	reader, ok := p.(billingUsagePersister)
	if !ok {
		return nil, true, fmt.Errorf("state: durable billing usage receipt reader is unavailable")
	}
	pageLimit := billingUsagePageSize
	if stopAfter > 0 && stopAfter < pageLimit {
		pageLimit = stopAfter
	}
	receipts := make([]BillingUsageReceipt, 0)
	err := reader.readBillingUsageReceipts(ctx, func(page billingUsagePage) error {
		seen := make(map[string]struct{})
		cursor := ""
		for {
			rows, err := page(cursor, pageLimit)
			if err != nil {
				return err
			}
			for _, receipt := range rows {
				// A row without the stable key cannot advance the cursor, so
				// the next page would repeat it forever.
				if receipt.WorkloadID == "" {
					return fmt.Errorf("state: durable billing usage receipt has no workload key")
				}
				if _, duplicate := seen[receipt.WorkloadID]; duplicate {
					return fmt.Errorf("state: durable billing usage receipts are not keyset ordered")
				}
				seen[receipt.WorkloadID] = struct{}{}
				cursor = receipt.WorkloadID
				receipts = append(receipts, receipt)
			}
			if len(rows) < pageLimit || (stopAfter > 0 && len(receipts) >= stopAfter) {
				return nil
			}
		}
	})
	if err != nil {
		return nil, true, err
	}
	return receipts, true, nil
}

func billingUsageReceipt(workload *Workload) BillingUsageReceipt {
	receipt := BillingUsageReceipt{
		CustomerID: workload.CustomerID, WorkloadID: workload.ID, BurstID: workload.BurstID,
		WorkloadRef: workload.Billing.WorkloadRef, HoldID: workload.Billing.HoldID,
		ReservedMicroUSD: workload.Billing.ReservedMicroUSD, ManualAttention: workload.Billing.ManualAttention,
		AuthoritativeUsageRequired: workload.Billing.AuthoritativeUsageRequired,
	}
	if workload.Cost != nil {
		receipt.CostPresent = true
		receipt.CostBurstID = workload.Cost.BurstID
		receipt.Backend = workload.Cost.Backend
		receipt.Basis = workload.Cost.Basis
		receipt.EstimatedUSD = workload.Cost.EstimatedUSD
		receipt.HourlyUSD = workload.Cost.HourlyUSD
	}
	return receipt
}

// listBillingUsageReceiptsStmt reads one keyset page. The workload primary key
// is the stable unique cursor, and it is both the ORDER BY and the strict
// comparison, so a page boundary can neither skip nor repeat a row. No OFFSET:
// an offset walk over a concurrently written table drops and duplicates rows.
func listBillingUsageReceiptsStmt(table string) string {
	return fmt.Sprintf(`
		SELECT id, data FROM %s
		WHERE data ? 'Billing' AND data->'Billing' <> 'null'::jsonb
		  AND id > $1
		ORDER BY id
		LIMIT $2`, table)
}

func (p *pgPersister) readBillingUsageReceipts(ctx context.Context, read func(billingUsagePage) error) error {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("state: begin durable billing usage snapshot: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	return read(func(afterWorkloadID string, pageLimit int) ([]BillingUsageReceipt, error) {
		return listBillingUsageReceipts(ctx, tx, afterWorkloadID, pageLimit)
	})
}

func listBillingUsageReceipts(ctx context.Context, tx pgx.Tx, afterWorkloadID string, pageLimit int) ([]BillingUsageReceipt, error) {
	if pageLimit <= 0 || pageLimit > maxBillingUsagePageSize {
		return nil, fmt.Errorf("state: billing usage page limit must be 1..%d", maxBillingUsagePageSize)
	}
	rows, err := tx.Query(ctx, listBillingUsageReceiptsStmt(tblWorkloads), afterWorkloadID, pageLimit)
	if err != nil {
		return nil, fmt.Errorf("state: read durable billing usage receipts: %w", err)
	}
	defer rows.Close()
	receipts := make([]BillingUsageReceipt, 0, pageLimit)
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, fmt.Errorf("state: scan durable billing usage receipt: %w", err)
		}
		var workload Workload
		if err := json.Unmarshal(data, &workload); err != nil {
			return nil, fmt.Errorf("state: decode durable billing usage receipt: %w", err)
		}
		if workload.Billing == nil {
			return nil, fmt.Errorf("state: durable billing usage receipt lost its billing association")
		}
		if workload.ID != id {
			return nil, fmt.Errorf("state: durable billing usage receipt key does not match its row")
		}
		receipts = append(receipts, billingUsageReceipt(&workload))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: read durable billing usage receipts: %w", err)
	}
	return receipts, nil
}
