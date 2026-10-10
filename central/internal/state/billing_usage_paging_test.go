package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// pagingUsagePersister serves real keyset pages from an ordered receipt list
// and records exactly what each page was asked for, so a test can prove the
// cursor advanced by the stable workload key rather than by an offset.
type pagingUsagePersister struct {
	*workloadPersister
	receipts []BillingUsageReceipt

	cursors    []string
	pageLimits []int
	// views counts the read-only snapshot views opened. Every page of one
	// assembled snapshot must come from the same view.
	views int
	err   error
	// misorder replaces the served page with one that repeats an already-seen
	// workload key, which is what a non-deterministic order would produce.
	misorder bool
	// raw, when set, is returned verbatim as every page, so a test can hand
	// back a row the keyset walk could never have asked for.
	raw []BillingUsageReceipt
}

func newPagingUsagePersister(receipts []BillingUsageReceipt) *pagingUsagePersister {
	return &pagingUsagePersister{workloadPersister: &workloadPersister{}, receipts: receipts}
}

func (p *pagingUsagePersister) readBillingUsageReceipts(ctx context.Context, read func(billingUsagePage) error) error {
	p.views++
	return read(func(after string, pageLimit int) ([]BillingUsageReceipt, error) {
		return p.page(ctx, after, pageLimit)
	})
}

func (p *pagingUsagePersister) page(ctx context.Context, after string, pageLimit int) ([]BillingUsageReceipt, error) {
	p.cursors = append(p.cursors, after)
	p.pageLimits = append(p.pageLimits, pageLimit)
	if p.err != nil {
		return nil, p.err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pageLimit <= 0 || pageLimit > maxBillingUsagePageSize {
		return nil, fmt.Errorf("state: billing usage page limit must be 1..%d", maxBillingUsagePageSize)
	}
	if p.raw != nil {
		return p.raw, nil
	}
	if p.misorder {
		return []BillingUsageReceipt{p.receipts[0], p.receipts[0]}, nil
	}
	page := make([]BillingUsageReceipt, 0, pageLimit)
	for _, receipt := range p.receipts {
		if receipt.WorkloadID <= after {
			continue
		}
		page = append(page, receipt)
		if len(page) == pageLimit {
			break
		}
	}
	return page, nil
}

func usageReceiptFixtures(n int) []BillingUsageReceipt {
	receipts := make([]BillingUsageReceipt, 0, n)
	for i := 0; i < n; i++ {
		receipts = append(receipts, BillingUsageReceipt{
			CustomerID: "tenant-paged", WorkloadID: fmt.Sprintf("wl_%06d", i),
			WorkloadRef: fmt.Sprintf("wl_%06d", i), HoldID: int64(i + 1), ReservedMicroUSD: 1_000,
		})
	}
	return receipts
}

func pagedStore(t *testing.T, receipts []BillingUsageReceipt) (*Store, *pagingUsagePersister) {
	t.Helper()
	s := New()
	p := newPagingUsagePersister(receipts)
	s.persist = p
	return s, p
}

// The lifetime ceiling was the bug: 1000 terminal paid workloads are retained,
// so the 1001st permanently turned every reconciliation pass into an error and
// closed the paid gate. The complete snapshot must return all of them.
func TestBillingUsageReceiptSnapshotIsCompletePastTheOldCeiling(t *testing.T) {
	receipts := usageReceiptFixtures(1001)
	s, p := pagedStore(t, receipts)
	got, durable, err := s.BillingUsageReceiptSnapshot(context.Background())
	if err != nil || !durable {
		t.Fatalf("snapshot durable=%v err=%v", durable, err)
	}
	if len(got) != len(receipts) {
		t.Fatalf("snapshot returned %d receipts, want %d", len(got), len(receipts))
	}
	for i := range got {
		if got[i] != receipts[i] {
			t.Fatalf("receipt %d = %+v, want %+v", i, got[i], receipts[i])
		}
	}
	// Every query stayed bounded by the fixed page size even though the total
	// did not, and every page came from one snapshot view, so a concurrent
	// workload write cannot tear the assembled result into a false difference.
	for i, limit := range p.pageLimits {
		if limit != billingUsagePageSize {
			t.Fatalf("page %d asked for %d rows, want the fixed page size %d", i, limit, billingUsagePageSize)
		}
	}
	if p.views != 1 {
		t.Fatalf("snapshot opened %d read views, want exactly 1", p.views)
	}
}

// Page boundaries are where a keyset walk skips or repeats. Exercise the exact
// multiples: one row short of a page, exactly a page, and one row past it.
func TestBillingUsageReceiptSnapshotPageBoundariesAreExact(t *testing.T) {
	for _, total := range []int{
		0, 1,
		billingUsagePageSize - 1, billingUsagePageSize, billingUsagePageSize + 1,
		2 * billingUsagePageSize, 2*billingUsagePageSize + 1,
	} {
		t.Run(fmt.Sprintf("total=%d", total), func(t *testing.T) {
			receipts := usageReceiptFixtures(total)
			s, p := pagedStore(t, receipts)
			got, _, err := s.BillingUsageReceiptSnapshot(context.Background())
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			if len(got) != total {
				t.Fatalf("snapshot returned %d receipts, want %d", len(got), total)
			}
			seen := make(map[string]int, total)
			for _, receipt := range got {
				seen[receipt.WorkloadID]++
			}
			for id, count := range seen {
				if count != 1 {
					t.Fatalf("workload %s appeared %d times across pages", id, count)
				}
			}
			// A total that is an exact multiple of the page size needs one more
			// query to learn the scan is finished; a partial page ends it.
			wantPages := total/billingUsagePageSize + 1
			if len(p.cursors) != wantPages {
				t.Fatalf("issued %d page queries, want %d", len(p.cursors), wantPages)
			}
			// The first page starts at the empty cursor and every later page
			// resumes strictly after the previous page's last workload key.
			if p.cursors[0] != "" {
				t.Fatalf("first page cursor = %q, want the empty cursor", p.cursors[0])
			}
			for page := 1; page < len(p.cursors); page++ {
				want := got[page*billingUsagePageSize-1].WorkloadID
				if p.cursors[page] != want {
					t.Fatalf("page %d resumed at %q, want %q", page, p.cursors[page], want)
				}
			}
		})
	}
}

func TestBillingUsageReceiptSnapshotFailsClosedOnUnorderedPages(t *testing.T) {
	s, p := pagedStore(t, usageReceiptFixtures(4))
	p.misorder = true
	if _, _, err := s.BillingUsageReceiptSnapshot(context.Background()); err == nil {
		t.Fatal("a page repeating a workload key was accepted")
	}

	s, p = pagedStore(t, usageReceiptFixtures(4))
	p.raw = []BillingUsageReceipt{{CustomerID: "tenant-paged", HoldID: 1}}
	if _, _, err := s.BillingUsageReceiptSnapshot(context.Background()); err == nil {
		t.Fatal("a receipt with no workload key was accepted")
	}
}

func TestBillingUsageReceiptSnapshotPropagatesReadFailures(t *testing.T) {
	readErr := errors.New("postgres is down")
	s, p := pagedStore(t, usageReceiptFixtures(4))
	p.err = readErr
	_, durable, err := s.BillingUsageReceiptSnapshot(context.Background())
	if !errors.Is(err, readErr) || !durable {
		t.Fatalf("snapshot durable=%v err=%v, want the read failure surfaced as durable", durable, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s, _ = pagedStore(t, usageReceiptFixtures(1001))
	if _, _, err := s.BillingUsageReceiptSnapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled snapshot err=%v, want context.Canceled", err)
	}
}

// The in-memory store still answers, and still says durable=false, so managed
// reconciliation fails closed on it instead of comparing a single process's
// view against the durable billing database.
func TestBillingUsageReceiptSnapshotMarksInMemoryStateNonDurable(t *testing.T) {
	store := New()
	store.PutWorkload(&Workload{ID: "wl_b", CustomerID: "tenant-one",
		Billing: &WorkloadBilling{HoldID: 2, WorkloadRef: "wl_b", ReservedMicroUSD: 1}})
	store.PutWorkload(&Workload{ID: "wl_a", CustomerID: "tenant-one",
		Billing: &WorkloadBilling{HoldID: 1, WorkloadRef: "wl_a", ReservedMicroUSD: 1}})
	store.PutWorkload(&Workload{ID: "wl_unbilled", CustomerID: "tenant-one"})
	receipts, durable, err := store.BillingUsageReceiptSnapshot(context.Background())
	if err != nil || durable {
		t.Fatalf("in-memory snapshot durable=%v err=%v", durable, err)
	}
	if len(receipts) != 2 || receipts[0].WorkloadID != "wl_a" || receipts[1].WorkloadID != "wl_b" {
		t.Fatalf("in-memory snapshot = %+v", receipts)
	}
}

// The bounded diagnostic API keeps its ceiling — callers like provider-delete
// want a small cheap view — but it must not read the whole table to find out it
// overflowed.
func TestBillingUsageReceiptsKeepsItsBoundedContract(t *testing.T) {
	s, p := pagedStore(t, usageReceiptFixtures(1001))
	if _, _, err := s.BillingUsageReceipts(context.Background(), 10); err == nil {
		t.Fatal("bounded API returned a truncated snapshot instead of failing closed")
	}
	if len(p.pageLimits) != 1 || p.pageLimits[0] != 11 {
		t.Fatalf("bounded API page limits = %v, want a single 11-row probe", p.pageLimits)
	}
	for _, limit := range []int{0, -1, maxBillingUsageReceipts + 1} {
		if _, _, err := s.BillingUsageReceipts(context.Background(), limit); err == nil {
			t.Errorf("bounded API accepted limit %d", limit)
		}
	}
	s, _ = pagedStore(t, usageReceiptFixtures(10))
	got, durable, err := s.BillingUsageReceipts(context.Background(), 10)
	if err != nil || !durable || len(got) != 10 {
		t.Fatalf("bounded API exact fit = %d receipts durable=%v err=%v", len(got), durable, err)
	}
}

// A durable store whose persister cannot serve receipts must be an error, never
// an empty snapshot that compares clean against a populated billing database.
func TestBillingUsageReceiptSnapshotRefusesPersisterWithoutReader(t *testing.T) {
	s := New()
	s.persist = &workloadPersister{}
	if _, durable, err := s.BillingUsageReceiptSnapshot(context.Background()); err == nil || !durable {
		t.Fatalf("durable=%v err=%v, want a durable read failure", durable, err)
	}
}

func TestListBillingUsageReceiptsStmtIsKeysetPaged(t *testing.T) {
	stmt := listBillingUsageReceiptsStmt(tblWorkloads)
	for _, want := range []string{
		"SELECT id, data FROM " + tblWorkloads,
		"data ? 'Billing' AND data->'Billing' <> 'null'::jsonb",
		"AND id > $1",
		"ORDER BY id",
		"LIMIT $2",
	} {
		if !strings.Contains(stmt, want) {
			t.Errorf("durable receipt page statement is missing %q", want)
		}
	}
	if strings.Contains(strings.ToUpper(stmt), "OFFSET") {
		t.Error("durable receipt page uses OFFSET; keyset paging is the contract")
	}
}

func TestDurableBillingUsagePageLimitIsValidatedAndBounded(t *testing.T) {
	for _, limit := range []int{0, -1, maxBillingUsagePageSize + 1} {
		if _, err := listBillingUsageReceipts(context.Background(), nil, "", limit); err == nil {
			t.Errorf("durable page limit %d accepted", limit)
		}
	}
	if billingUsagePageSize <= 0 || billingUsagePageSize > maxBillingUsagePageSize {
		t.Fatalf("billingUsagePageSize=%d is outside the validated page bound", billingUsagePageSize)
	}
}
