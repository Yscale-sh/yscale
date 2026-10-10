// yscale:proprietary

package billing

import (
	"context"
	"strings"
	"testing"
)

// The hold/capture scan is the half of usage reconciliation that used to stop
// at 1000 lifetime rows. Terminal holds are retained forever, so that ceiling
// turned into a permanent closed paid gate once a real account crossed it. The
// replacement must page, and its page must be a keyset page: an OFFSET walk
// over a table another request is writing drops and repeats rows, which reads
// as a reconciliation difference and closes the same gate for a different
// reason.
func TestUsageCaptureSnapshotStmtIsKeysetPaged(t *testing.T) {
	for _, want := range []string{
		"WHERE (h.customer_id, h.id, coalesce(l.id, 0)) > ($1::text, $2::bigint, $3::bigint)",
		"ORDER BY h.customer_id, h.id, coalesce(l.id, 0)",
		"LIMIT $4",
	} {
		if !strings.Contains(usageCaptureSnapshotStmt, want) {
			t.Errorf("usage capture page statement is missing %q", want)
		}
	}
	if strings.Contains(strings.ToUpper(usageCaptureSnapshotStmt), "OFFSET") {
		t.Error("usage capture page uses OFFSET; keyset paging is the contract")
	}
	// The cursor tuple and the sort key have to be the same three expressions
	// or a page boundary lands between two rows the database considers equal.
	cursor := "(h.customer_id, h.id, coalesce(l.id, 0))"
	order := "ORDER BY h.customer_id, h.id, coalesce(l.id, 0)"
	if !strings.Contains(usageCaptureSnapshotStmt, cursor) ||
		!strings.Contains(usageCaptureSnapshotStmt, order) {
		t.Fatal("cursor tuple and sort key drifted apart")
	}
	// The join predicate must stay in ON; moving the cursor into ON would turn
	// the LEFT JOIN into a full scan of already-paged holds with null ledgers.
	joinIdx := strings.Index(usageCaptureSnapshotStmt, "LEFT JOIN")
	whereIdx := strings.Index(usageCaptureSnapshotStmt, "WHERE")
	if joinIdx < 0 || whereIdx < joinIdx {
		t.Fatal("cursor comparison must follow the LEFT JOIN in a WHERE clause")
	}
}

func TestUsageCapturePageLimitIsValidatedAndBounded(t *testing.T) {
	for _, limit := range []int{0, -1, maxUsageCapturePageSize + 1} {
		if _, err := usageCapturePage(context.Background(), nil, usageCaptureCursor{}, limit); err == nil {
			t.Errorf("page limit %d accepted", limit)
		}
	}
	if usageCapturePageSize <= 0 || usageCapturePageSize > maxUsageCapturePageSize {
		t.Fatalf("usageCapturePageSize=%d is outside the validated page bound", usageCapturePageSize)
	}
}

func usageCaptureRow(customerID string, holdID, ledgerID int64) usageCapture {
	return usageCapture{CustomerID: customerID, HoldID: holdID, CaptureLedgerID: ledgerID}
}

// The tie-breaker exists because one hold produces zero rows-with-a-ledger-id
// (LEFT JOIN null, coalesced to 0), one row, or several. Each of those must
// advance the cursor, or a page that ends inside a multi-capture hold either
// repeats the hold or skips the rest of its ledger entries.
func TestUsageCaptureScanAdvancesAcrossLedgerFanout(t *testing.T) {
	scan := newUsageCaptureScan()
	for _, row := range []usageCapture{
		usageCaptureRow("tenant-a", 1, 0), // hold with no capture ledger entry
		usageCaptureRow("tenant-a", 2, 7), // hold with exactly one
		usageCaptureRow("tenant-a", 3, 8), // hold with several
		usageCaptureRow("tenant-a", 3, 9),
		usageCaptureRow("tenant-a", 3, 10),
		usageCaptureRow("tenant-b", 1, 0), // hold ids restart per customer
	} {
		if err := scan.advance(row); err != nil {
			t.Fatalf("advance(%+v): %v", row, err)
		}
	}
	if scan.cursor != (usageCaptureCursor{customerID: "tenant-b", holdID: 1, ledgerID: 0}) {
		t.Fatalf("cursor after scan = %+v", scan.cursor)
	}
}

func TestUsageCaptureScanRefusesRowsThatCannotFollow(t *testing.T) {
	tests := []struct {
		name string
		rows []usageCapture
	}{
		{"repeated row", []usageCapture{usageCaptureRow("tenant-a", 3, 9), usageCaptureRow("tenant-a", 3, 9)}},
		{"ledger id goes backwards", []usageCapture{usageCaptureRow("tenant-a", 3, 9), usageCaptureRow("tenant-a", 3, 8)}},
		{"hold id goes backwards", []usageCapture{usageCaptureRow("tenant-a", 3, 0), usageCaptureRow("tenant-a", 2, 0)}},
		{"customer revisited after finishing", []usageCapture{
			usageCaptureRow("tenant-a", 1, 0), usageCaptureRow("tenant-b", 1, 0), usageCaptureRow("tenant-a", 2, 0),
		}},
		{"no customer", []usageCapture{usageCaptureRow("", 1, 0)}},
		{"no hold", []usageCapture{usageCaptureRow("tenant-a", 0, 0)}},
		{"negative ledger id", []usageCapture{usageCaptureRow("tenant-a", 1, -1)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scan := newUsageCaptureScan()
			var err error
			for _, row := range tc.rows {
				if err = scan.advance(row); err != nil {
					break
				}
			}
			if err == nil {
				t.Fatal("scan accepted a row that cannot follow its cursor")
			}
		})
	}
}

type countingUsageSource struct {
	receipts []UsageReceipt
	calls    int
	err      error
}

func (s *countingUsageSource) UsageReceiptSnapshot(ctx context.Context) ([]UsageReceipt, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]UsageReceipt(nil), s.receipts...), nil
}

// The source contract carries no total limit any more. A caller that still
// wanted to pass one would be reintroducing the ceiling, so the compiler is the
// check: this asserts the shape rather than a value.
func TestUsageReceiptSourceTakesNoTotalLimit(t *testing.T) {
	var source UsageReceiptSource = &countingUsageSource{}
	if _, err := source.UsageReceiptSnapshot(context.Background()); err != nil {
		t.Fatalf("complete snapshot: %v", err)
	}
}

func TestReconcileUsageCapturesRefusesUnavailableSource(t *testing.T) {
	var store *Store
	if _, err := store.ReconcileUsageCaptures(context.Background(), nil); err == nil {
		t.Fatal("reconciliation ran without a durable usage receipt source")
	}
}

// A cancelled attempt must surface as an error, not as a zero-difference pass:
// an empty snapshot compared against an empty capture set looks healthy.
func TestReconcileUsageCapturesPropagatesSourceFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := &countingUsageSource{}
	var store *Store
	if _, err := store.ReconcileUsageCaptures(ctx, source); err == nil {
		t.Fatal("cancelled reconciliation reported success")
	}
	if source.calls != 1 {
		t.Fatalf("source calls = %d, want the first read to fail closed", source.calls)
	}
}
