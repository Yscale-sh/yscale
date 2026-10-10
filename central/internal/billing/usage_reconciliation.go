// yscale:proprietary

package billing

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
)

// usageCapturePageSize bounds one database round trip of the hold/capture scan.
// It is a page bound, not a lifetime ceiling: the assembled snapshot keeps
// paging until the keyset scan is exhausted, so reconciliation stays complete
// after any number of lifetime holds. Terminal holds are retained forever, so a
// total-row ceiling here would permanently close paid admission.
const usageCapturePageSize = 500

// maxUsageCapturePageSize bounds any single page query.
const maxUsageCapturePageSize = 5000

// UsageReceipt is the durable state-side association between a workload cost
// and its prepaid hold. Authoritative is true only for a cost frozen at the
// provider-delete receipt boundary. AuthoritativeUsageRequired is the durable
// activation watermark written with every new prepaid reservation.
type UsageReceipt struct {
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
	Authoritative              bool
	EstimatedUSD               float64
	HourlyUSD                  float64
}

// UsageReceiptSource returns the complete deterministic snapshot of durable
// workload usage receipts. There is deliberately no caller-supplied total
// limit: any such limit is a lifetime ceiling that silently truncates paid
// reconciliation once enough terminal workloads accumulate. Implementations
// must page internally with bounded queries and return every row. Partial or
// in-memory views must return an error.
type UsageReceiptSource interface {
	UsageReceiptSnapshot(context.Context) ([]UsageReceipt, error)
}

// UsageCaptureSummary is safe for metrics and logs: it contains no tenant,
// workload, burst, hold, provider, or payment identifiers.
type UsageCaptureSummary struct {
	HoldMissing            int64
	UsageMissing           int64
	Association            int64
	NonAuthoritative       int64
	CaptureAmount          int64
	Unsettled              int64
	LegacyNonAuthoritative int64
}

func (s UsageCaptureSummary) TotalDifferences() int64 {
	return s.HoldMissing + s.UsageMissing + s.Association + s.NonAuthoritative + s.CaptureAmount + s.Unsettled
}

type usageCapture struct {
	CustomerID                  string
	HoldID                      int64
	WorkloadRef                 string
	ReservedMicroUSD            int64
	State                       HoldState
	Provider                    string
	ProviderRateMicroUSDPerHour int64
	CapturedMicroUSD            int64
	CaptureLedgerID             int64
	CaptureLedgerMicroUSD       int64
	CaptureExternalRef          string
}

type usageKey struct {
	customerID string
	holdID     int64
}

// usageCaptureCursor is the keyset position in the hold/capture scan order
// (h.customer_id, h.id, coalesce(l.id,0)). The coalesced ledger id is the
// tie-breaker that keeps the tuple unique whether a hold has zero, one, or
// several capture ledger entries, so a page boundary can neither skip nor
// duplicate a row. Ledger ids are BIGSERIAL and start at 1, so 0 is reserved
// for the LEFT JOIN's unmatched row.
type usageCaptureCursor struct {
	customerID string
	holdID     int64
	ledgerID   int64
}

// ReconcileUsageCaptures compares durable workload-cost receipts with billing
// holds and capture ledger entries. It reads both stores twice and refuses a
// result when either changes during the cross-store scan. It never repairs or
// writes economic state.
func (s *Store) ReconcileUsageCaptures(ctx context.Context, source UsageReceiptSource) (UsageCaptureSummary, error) {
	if source == nil {
		return UsageCaptureSummary{}, fmt.Errorf("billing: durable usage receipt source is unavailable")
	}
	beforeReceipts, err := source.UsageReceiptSnapshot(ctx)
	if err != nil {
		return UsageCaptureSummary{}, fmt.Errorf("billing: read usage receipt snapshot: %w", err)
	}
	beforeCaptures, err := s.usageCaptureSnapshot(ctx)
	if err != nil {
		return UsageCaptureSummary{}, err
	}
	afterReceipts, err := source.UsageReceiptSnapshot(ctx)
	if err != nil {
		return UsageCaptureSummary{}, fmt.Errorf("billing: reread usage receipt snapshot: %w", err)
	}
	afterCaptures, err := s.usageCaptureSnapshot(ctx)
	if err != nil {
		return UsageCaptureSummary{}, err
	}
	sortUsageReceipts(beforeReceipts)
	sortUsageReceipts(afterReceipts)
	if !slices.Equal(beforeReceipts, afterReceipts) {
		return UsageCaptureSummary{}, fmt.Errorf("billing: workload usage changed during reconciliation")
	}
	if !slices.Equal(beforeCaptures, afterCaptures) {
		return UsageCaptureSummary{}, fmt.Errorf("billing: hold capture changed during reconciliation")
	}
	return compareUsageCaptures(beforeReceipts, beforeCaptures)
}

func sortUsageReceipts(receipts []UsageReceipt) {
	slices.SortFunc(receipts, func(a, b UsageReceipt) int {
		if a.CustomerID != b.CustomerID {
			return compareString(a.CustomerID, b.CustomerID)
		}
		if a.HoldID < b.HoldID {
			return -1
		}
		if a.HoldID > b.HoldID {
			return 1
		}
		return compareString(a.WorkloadID, b.WorkloadID)
	})
}

func compareString(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// usageCaptureSnapshotStmt is the keyset page of the hold/capture scan. The
// cursor tuple and the ORDER BY use the same three expressions, so the scan is
// a total order with no OFFSET and no lifetime ceiling. The comparison lives in
// WHERE rather than in the LEFT JOIN's ON clause: it must filter joined rows,
// not the join itself, or an already-paged hold would reappear unmatched.
const usageCaptureSnapshotStmt = `
	SELECT h.customer_id, h.id, h.workload_ref, h.amount_micro_usd, h.state,
	       h.price_quote->>'provider',
	       (h.price_quote->>'provider_rate_micro_usd_per_hour')::bigint,
	       h.captured_micro_usd,
	       coalesce(l.id, 0), coalesce(l.amount_micro_usd, 0), coalesce(l.external_ref, '')
	FROM billing.holds h
	LEFT JOIN billing.ledger l ON l.customer_id=h.customer_id AND l.hold_id=h.id AND l.entry_type='capture'
	WHERE (h.customer_id, h.id, coalesce(l.id, 0)) > ($1::text, $2::bigint, $3::bigint)
	ORDER BY h.customer_id, h.id, coalesce(l.id, 0)
	LIMIT $4`

// usageCaptureSnapshot assembles the complete hold/capture snapshot from
// bounded keyset pages inside one repeatable-read transaction, so every page
// observes the same database snapshot and a concurrent reservation cannot tear
// the assembled result.
func (s *Store) usageCaptureSnapshot(ctx context.Context) ([]usageCapture, error) {
	if err := s.assertEnvironment(ctx); err != nil {
		return nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("billing: begin usage capture snapshot: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	captures := make([]usageCapture, 0)
	scan := newUsageCaptureScan()
	for {
		page, err := usageCapturePage(ctx, tx, scan.cursor, usageCapturePageSize)
		if err != nil {
			return nil, err
		}
		for _, capture := range page {
			if err := scan.advance(capture); err != nil {
				return nil, err
			}
			captures = append(captures, capture)
		}
		if len(page) < usageCapturePageSize {
			return captures, nil
		}
	}
}

// usageCaptureScan tracks the keyset position across pages and fails closed
// when the database hands back a row that cannot follow the previous one.
// Ordering within a customer is checked on the two integer components only:
// customer_id ordering belongs to the database collation, so the cross-customer
// check is "this customer was already finished", which no collation can make
// spuriously true.
type usageCaptureScan struct {
	cursor        usageCaptureCursor
	seenCustomers map[string]struct{}
}

func newUsageCaptureScan() *usageCaptureScan {
	return &usageCaptureScan{seenCustomers: make(map[string]struct{})}
}

func (s *usageCaptureScan) advance(capture usageCapture) error {
	next := usageCaptureCursor{
		customerID: capture.CustomerID,
		holdID:     capture.HoldID,
		ledgerID:   capture.CaptureLedgerID,
	}
	// A row without a keyset position cannot advance the cursor, so the next
	// page would repeat it forever.
	if next.customerID == "" || next.holdID <= 0 || next.ledgerID < 0 {
		return fmt.Errorf("billing: usage capture snapshot row has no keyset position")
	}
	if next.customerID == s.cursor.customerID {
		if next.holdID < s.cursor.holdID ||
			(next.holdID == s.cursor.holdID && next.ledgerID <= s.cursor.ledgerID) {
			return fmt.Errorf("billing: usage capture snapshot is not keyset ordered")
		}
	} else if _, revisited := s.seenCustomers[next.customerID]; revisited {
		return fmt.Errorf("billing: usage capture snapshot is not keyset ordered")
	}
	s.seenCustomers[next.customerID] = struct{}{}
	s.cursor = next
	return nil
}

func usageCapturePage(ctx context.Context, tx pgx.Tx, cursor usageCaptureCursor, limit int) ([]usageCapture, error) {
	if limit <= 0 || limit > maxUsageCapturePageSize {
		return nil, fmt.Errorf("billing: usage capture page limit must be 1..%d", maxUsageCapturePageSize)
	}
	rows, err := tx.Query(ctx, usageCaptureSnapshotStmt,
		cursor.customerID, cursor.holdID, cursor.ledgerID, limit)
	if err != nil {
		return nil, fmt.Errorf("billing: read usage capture snapshot: %w", err)
	}
	defer rows.Close()
	page := make([]usageCapture, 0, limit)
	for rows.Next() {
		var capture usageCapture
		if err := rows.Scan(&capture.CustomerID, &capture.HoldID, &capture.WorkloadRef,
			&capture.ReservedMicroUSD, &capture.State, &capture.Provider,
			&capture.ProviderRateMicroUSDPerHour, &capture.CapturedMicroUSD,
			&capture.CaptureLedgerID, &capture.CaptureLedgerMicroUSD,
			&capture.CaptureExternalRef); err != nil {
			return nil, fmt.Errorf("billing: scan usage capture snapshot: %w", err)
		}
		page = append(page, capture)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("billing: read usage capture snapshot: %w", err)
	}
	return page, nil
}

func compareUsageCaptures(receipts []UsageReceipt, captures []usageCapture) (UsageCaptureSummary, error) {
	receiptByKey := make(map[usageKey]UsageReceipt, len(receipts))
	for _, receipt := range receipts {
		if receipt.CustomerID == "" || receipt.WorkloadID == "" || receipt.WorkloadRef == "" ||
			receipt.HoldID <= 0 || receipt.ReservedMicroUSD <= 0 {
			return UsageCaptureSummary{}, fmt.Errorf("billing: invalid durable usage receipt")
		}
		key := usageKey{customerID: receipt.CustomerID, holdID: receipt.HoldID}
		if _, exists := receiptByKey[key]; exists {
			return UsageCaptureSummary{}, fmt.Errorf("billing: duplicate durable usage receipt")
		}
		receiptByKey[key] = receipt
	}
	captureByKey := make(map[usageKey]usageCapture, len(captures))
	for _, capture := range captures {
		if capture.CustomerID == "" || capture.HoldID <= 0 || capture.WorkloadRef == "" ||
			capture.ReservedMicroUSD <= 0 || capture.Provider == "" || capture.ProviderRateMicroUSDPerHour <= 0 {
			return UsageCaptureSummary{}, fmt.Errorf("billing: invalid hold capture receipt")
		}
		key := usageKey{customerID: capture.CustomerID, holdID: capture.HoldID}
		if _, exists := captureByKey[key]; exists {
			return UsageCaptureSummary{}, fmt.Errorf("billing: duplicate hold capture receipt")
		}
		captureByKey[key] = capture
	}

	var summary UsageCaptureSummary
	for key, receipt := range receiptByKey {
		capture, ok := captureByKey[key]
		if !ok {
			summary.HoldMissing++
			continue
		}
		delete(captureByKey, key)
		associationMismatch := receipt.WorkloadRef != receipt.WorkloadID ||
			capture.WorkloadRef != receipt.WorkloadRef || capture.ReservedMicroUSD != receipt.ReservedMicroUSD
		if !receipt.CostPresent {
			if capture.CaptureLedgerID != 0 {
				summary.CaptureAmount++
			}
			switch capture.State {
			case HoldCaptured:
				summary.UsageMissing++
			case HoldPending:
				if receipt.ManualAttention {
					summary.Unsettled++
				}
			case HoldReleased, HoldExpired:
			default:
				return UsageCaptureSummary{}, fmt.Errorf("billing: invalid hold capture state")
			}
			if associationMismatch {
				summary.Association++
			}
			continue
		}

		if receipt.BurstID == "" || receipt.CostBurstID == "" || receipt.Backend == "" {
			return UsageCaptureSummary{}, fmt.Errorf("billing: invalid terminal usage receipt")
		}
		if !receipt.Authoritative && receipt.AuthoritativeUsageRequired {
			summary.NonAuthoritative++
		} else if !receipt.Authoritative {
			// Rows written before usage reconciliation was activated cannot be
			// upgraded to provider-delete evidence after the fact. They remain in
			// every identity, rate, cap, and capture comparison below, but do not
			// permanently close admission on the one proof their writer never
			// promised to record. Every newly reserved hold persists the marker.
			summary.LegacyNonAuthoritative++
		}
		if receipt.BurstID != receipt.CostBurstID || receipt.Backend != capture.Provider ||
			(capture.CaptureLedgerID != 0 && capture.CaptureExternalRef != receipt.BurstID) {
			associationMismatch = true
		}
		if associationMismatch {
			summary.Association++
		}
		expectedRate, err := TrustedHourlyMicroUSD(receipt.HourlyUSD)
		if err != nil {
			return UsageCaptureSummary{}, err
		}
		expectedCapture, err := TrustedCaptureMicroUSD(receipt.EstimatedUSD, receipt.ReservedMicroUSD)
		if err != nil {
			return UsageCaptureSummary{}, err
		}
		if expectedRate != capture.ProviderRateMicroUSDPerHour {
			summary.CaptureAmount++
		}
		if capture.State == HoldPending {
			summary.Unsettled++
			continue
		}
		if expectedCapture == 0 {
			if capture.State != HoldReleased || capture.CapturedMicroUSD != 0 || capture.CaptureLedgerID != 0 {
				summary.CaptureAmount++
			}
			continue
		}
		if capture.State != HoldCaptured || capture.CapturedMicroUSD != expectedCapture ||
			capture.CaptureLedgerID == 0 || capture.CaptureLedgerMicroUSD != expectedCapture {
			summary.CaptureAmount++
		}
	}
	for _, capture := range captureByKey {
		if capture.State == HoldPending || capture.State == HoldCaptured {
			summary.UsageMissing++
		}
	}
	return summary, nil
}
