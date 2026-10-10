//go:build integration

package billing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresBillingInvariants(t *testing.T) {
	dsn := os.Getenv("BILLING_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set BILLING_TEST_DATABASE_URL to an isolated Postgres database")
	}
	if os.Getenv("BILLING_TEST_ALLOW_DESTRUCTIVE") != "DROP_BILLING_SCHEMA" {
		t.Fatal("set BILLING_TEST_ALLOW_DESTRUCTIVE=DROP_BILLING_SCHEMA for the isolated test database")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var databaseName string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("BILLING_TEST_EXPECT_DATABASE"); expected == "" || expected != databaseName {
		t.Fatalf("BILLING_TEST_EXPECT_DATABASE must exactly match current database %q", databaseName)
	}
	if !strings.HasSuffix(databaseName, "_billing_test") {
		t.Fatalf("refusing destructive integration test against non-test database %q", databaseName)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS billing CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS billing CASCADE`); err != nil {
			t.Errorf("cleanup billing schema: %v", err)
		}
		pool.Close()
	})
	if err := EnsureSchema(ctx, pool, false); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool, false)

	reset := func(t *testing.T) {
		t.Helper()
		_, err := pool.Exec(ctx, `
			TRUNCATE billing.webhook_inbox, billing.checkout_sessions, billing.ledger, billing.holds,
			         billing.operations, billing.accounts RESTART IDENTITY CASCADE`)
		if err != nil {
			t.Fatal(err)
		}
	}
	account := func(t *testing.T, customer string, credit int64) {
		t.Helper()
		if err := store.EnsureAccount(ctx, customer); err != nil {
			t.Fatal(err)
		}
		if credit > 0 {
			if err := store.GrantCredit(ctx, CreditGrantRequest{
				CustomerID: customer, AmountMicroUSD: credit, IdempotencyKey: "grant:" + customer,
				Provider: "yscale", ProviderAccountID: "internal", LiveMode: false,
				PaymentObjectID: "seed:" + customer,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	quote := func(id string, amount int64) PriceQuote {
		now := time.Now().UTC().Truncate(time.Microsecond)
		return PriceQuote{
			QuoteID: "quote:" + id, PricingVersion: 1, Currency: "USD", Provider: "linode",
			SKU: "rtx4000ada", Region: "us-east", ProviderRateMicroUSDPerHour: amount,
			CustomerRateMicroUSDPerHour: amount, PlatformFeeBasisPoints: 0,
			MaximumDurationSeconds: 3600, MaximumChargeMicroUSD: amount,
			IssuedAt: now, ValidUntil: now.Add(5 * time.Minute),
		}
	}

	t.Run("empty environment sentinel fails closed", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `DROP SCHEMA billing CASCADE; CREATE SCHEMA billing;
			CREATE TABLE billing.environment (singleton BOOLEAN, livemode BOOLEAN)`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS billing CASCADE`); err != nil {
				t.Errorf("drop partial schema: %v", err)
				return
			}
			if err := EnsureSchema(ctx, pool, false); err != nil {
				t.Errorf("restore billing schema: %v", err)
			}
		}()
		if err := EnsureSchema(ctx, pool, false); !errors.Is(err, ErrInvariantViolation) {
			t.Fatalf("empty environment sentinel did not fail closed: %v", err)
		}
	})

	t.Run("database environment is immutable and fail closed", func(t *testing.T) {
		reset(t)
		if err := EnsureSchema(ctx, pool, true); !errors.Is(err, ErrEnvironmentMismatch) {
			t.Fatalf("live schema opened test database: %v", err)
		}
		if err := NewStore(pool, true).EnsureAccount(ctx, "wrong-mode"); !errors.Is(err, ErrEnvironmentMismatch) {
			t.Fatalf("live store wrote test database: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE billing.environment SET livemode=TRUE WHERE singleton`); err == nil {
			t.Fatal("database environment mode accepted mutation")
		}
	})

	t.Run("schema upgrades pre-restoration constraints", func(t *testing.T) {
		reset(t)
		if _, err := pool.Exec(ctx, `
			DROP TABLE billing.credit_disputes;
			ALTER TABLE billing.operations DROP CONSTRAINT operations_operation_type_check;
			ALTER TABLE billing.operations ADD CONSTRAINT operations_operation_type_check
				CHECK (operation_type IN ('grant','reserve','capture','release','expire','reversal'));
			ALTER TABLE billing.ledger DROP CONSTRAINT ledger_entry_type_check;
			ALTER TABLE billing.ledger ADD CONSTRAINT ledger_entry_type_check
				CHECK (entry_type IN ('credit','reserve','capture','release','expiry','reversal'));
			ALTER TABLE billing.ledger DROP CONSTRAINT ledger_check;
			ALTER TABLE billing.ledger ADD CONSTRAINT ledger_check
				CHECK ((entry_type IN ('reserve','capture','release','expiry') AND hold_id IS NOT NULL) OR
				       (entry_type IN ('credit','reversal') AND hold_id IS NULL));`); err != nil {
			t.Fatal(err)
		}
		if err := EnsureSchema(ctx, pool, false); err != nil {
			t.Fatalf("upgrade schema: %v", err)
		}
		var disputeTable bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('billing.credit_disputes') IS NOT NULL`).Scan(&disputeTable); err != nil {
			t.Fatal(err)
		}
		if !disputeTable {
			t.Fatal("upgrade did not create credit_disputes")
		}
		for _, constraint := range []struct {
			table string
			name  string
		}{
			{table: "operations", name: "operations_operation_type_check"},
			{table: "ledger", name: "ledger_entry_type_check"},
			{table: "ledger", name: "ledger_check"},
		} {
			var definition string
			if err := pool.QueryRow(ctx, `
				SELECT pg_get_constraintdef(oid)
				FROM pg_constraint
				WHERE conrelid=to_regclass($1) AND conname=$2`,
				"billing."+constraint.table, constraint.name).Scan(&definition); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(definition, "restoration") {
				t.Fatalf("constraint %s was not widened: %s", constraint.name, definition)
			}
		}
	})

	t.Run("concurrent reservations cannot overspend", func(t *testing.T) {
		reset(t)
		account(t, "tenant-a", 100)
		start := make(chan struct{})
		results := make(chan error, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, err := store.ReserveCredit(ctx, ReservationRequest{
					CustomerID: "tenant-a", WorkloadRef: fmt.Sprintf("workload-%d", i),
					IdempotencyKey: fmt.Sprintf("reserve-%d", i),
					ExpiresAt:      time.Now().Add(time.Minute), PriceQuote: quote(fmt.Sprintf("race-%d", i), 80),
				})
				results <- err
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		var succeeded, rejected int
		for err := range results {
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrInsufficientCredit):
				rejected++
			default:
				t.Fatalf("unexpected reservation result: %v", err)
			}
		}
		if succeeded != 1 || rejected != 1 {
			t.Fatalf("succeeded=%d rejected=%d", succeeded, rejected)
		}
		got, err := store.GetAccount(ctx, "tenant-a")
		if err != nil || got.HeldMicroUSD != 80 || got.SpendableMicroUSD() != 20 {
			t.Fatalf("account=%+v err=%v", got, err)
		}
	})

	t.Run("idempotency matches the complete semantic payload", func(t *testing.T) {
		reset(t)
		account(t, "tenant-a", 0)
		account(t, "tenant-b", 0)
		grant := CreditGrantRequest{CustomerID: "tenant-a", AmountMicroUSD: 100,
			IdempotencyKey: "payment:pi_1", Provider: "stripe", ProviderAccountID: "acct_test",
			LiveMode: false, PaymentObjectID: "pi_1"}
		if err := store.GrantCredit(ctx, grant); err != nil {
			t.Fatal(err)
		}
		if err := store.GrantCredit(ctx, grant); err != nil {
			t.Fatalf("exact retry: %v", err)
		}
		var fundingRows, ledgerRows int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM billing.funding_sources WHERE customer_id=$1`, "tenant-a").Scan(&fundingRows); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM billing.ledger WHERE customer_id=$1 AND entry_type='credit'`, "tenant-a").Scan(&ledgerRows); err != nil {
			t.Fatal(err)
		}
		replayed, err := store.GetAccount(ctx, "tenant-a")
		if err != nil || replayed.BalanceMicroUSD != 100 || fundingRows != 1 || ledgerRows != 1 {
			t.Fatalf("exact replay changed economics: account=%+v funding=%d ledger=%d err=%v", replayed, fundingRows, ledgerRows, err)
		}
		for _, call := range []func() error{
			func() error {
				changed := grant
				changed.AmountMicroUSD = 101
				return store.GrantCredit(ctx, changed)
			},
			func() error {
				changed := grant
				changed.PaymentObjectID = "pi_other"
				return store.GrantCredit(ctx, changed)
			},
		} {
			if err := call(); !errors.Is(err, ErrIdempotencyConflict) {
				t.Fatalf("conflicting retry = %v", err)
			}
		}
		got, _ := store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 100 {
			t.Fatalf("duplicate changed balance: %+v", got)
		}
		secondKey := grant
		secondKey.IdempotencyKey = "payment:pi_1:again"
		if err := store.GrantCredit(ctx, secondKey); !errors.Is(err, ErrEconomicObjectConflict) {
			t.Fatalf("same payment object under a new key = %v", err)
		}
		if err := store.GrantCredit(ctx, CreditGrantRequest{
			CustomerID: "tenant-b", AmountMicroUSD: 100, IdempotencyKey: "payment:pi_1",
			Provider: "stripe", ProviderAccountID: "acct_test", LiveMode: false,
			PaymentObjectID: "pi_tenant_b",
		}); err != nil {
			t.Fatalf("tenant-scoped key should be reusable: %v", err)
		}
		got, _ = store.GetAccount(ctx, "tenant-b")
		if got.BalanceMicroUSD != 100 {
			t.Fatalf("tenant-scoped grant missing: %+v", got)
		}
		liveGrant := grant
		liveGrant.IdempotencyKey = "payment:pi_1:live"
		liveGrant.LiveMode = true
		if err := store.GrantCredit(ctx, liveGrant); !errors.Is(err, ErrEnvironmentMismatch) {
			t.Fatalf("live grant entered test-mode store: %v", err)
		}
	})

	t.Run("partial refunds are bounded and do not equal disputes", func(t *testing.T) {
		reset(t)
		account(t, "tenant-a", 100)
		refund := CreditReversalRequest{
			CustomerID: "tenant-a", AmountMicroUSD: 40, IdempotencyKey: "refund:1",
			FundingProvider: "yscale", FundingProviderAccountID: "internal", FundingLiveMode: false,
			PaymentObjectID: "seed:tenant-a", ReversalProvider: "stripe",
			ReversalProviderAccountID: "acct_test", ReversalLiveMode: false,
			ReversalObjectID: "re_1", Kind: ReversalRefund,
		}
		if err := store.ReverseCredit(ctx, refund); err != nil {
			t.Fatal(err)
		}
		if err := store.ReverseCredit(ctx, refund); err != nil {
			t.Fatalf("refund retry: %v", err)
		}
		got, _ := store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 60 || got.Frozen || got.DebtMicroUSD != 0 {
			t.Fatalf("ordinary refund account=%+v", got)
		}
		refund.AmountMicroUSD = 60
		refund.IdempotencyKey = "refund:2"
		refund.ReversalObjectID = "re_2"
		if err := store.ReverseCredit(ctx, refund); err != nil {
			t.Fatal(err)
		}
		refund.AmountMicroUSD = 1
		refund.IdempotencyKey = "refund:3"
		refund.ReversalObjectID = "re_3"
		if err := store.ReverseCredit(ctx, refund); !errors.Is(err, ErrEconomicObjectConflict) {
			t.Fatalf("refunds exceeded original funding: %v", err)
		}

		account(t, "tenant-b", 100)
		hold, err := store.ReserveCredit(ctx, ReservationRequest{
			CustomerID: "tenant-b", WorkloadRef: "spent-before-refund",
			IdempotencyKey: "reserve:spent-before-refund", ExpiresAt: time.Now().Add(time.Minute),
			PriceQuote: quote("spent-before-refund", 80),
		})
		if err != nil {
			t.Fatal(err)
		}
		if hold.PriceQuote.QuoteID != "quote:spent-before-refund" || hold.PriceQuote.MaximumChargeMicroUSD != hold.AmountMicroUSD {
			t.Fatalf("hold did not return its authoritative quote: %+v", hold)
		}
		if err := store.CaptureHold(ctx, "tenant-b", hold.ID, 80, "capture:spent-before-refund", "usage"); err != nil {
			t.Fatal(err)
		}
		if err := store.ReverseCredit(ctx, CreditReversalRequest{
			CustomerID: "tenant-b", AmountMicroUSD: 100, IdempotencyKey: "refund:spent",
			FundingProvider: "yscale", FundingProviderAccountID: "internal", FundingLiveMode: false,
			PaymentObjectID: "seed:tenant-b", ReversalProvider: "stripe",
			ReversalProviderAccountID: "acct_test", ReversalLiveMode: false,
			ReversalObjectID: "re_spent", Kind: ReversalRefund,
		}); err != nil {
			t.Fatalf("authoritative refund after spend: %v", err)
		}
		got, _ = store.GetAccount(ctx, "tenant-b")
		if got.BalanceMicroUSD != 0 || got.DebtMicroUSD != 80 || !got.Frozen {
			t.Fatalf("spent refund account=%+v", got)
		}
	})

	t.Run("concurrent exact retries apply once", func(t *testing.T) {
		reset(t)
		account(t, "tenant-a", 0)
		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errs <- store.GrantCredit(ctx, CreditGrantRequest{
					CustomerID: "tenant-a", AmountMicroUSD: 100, IdempotencyKey: "payment:once",
					Provider: "stripe", ProviderAccountID: "acct_test", LiveMode: false,
					PaymentObjectID: "pi_once",
				})
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent grant retry: %v", err)
			}
		}
		got, _ := store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 100 {
			t.Fatalf("concurrent retry applied more than once: %+v", got)
		}

		start = make(chan struct{})
		holds := make(chan Hold, 2)
		errs = make(chan error, 2)
		expiresAt := time.Now().Add(time.Minute)
		onceQuote := quote("once", 80)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				hold, err := store.ReserveCredit(ctx, ReservationRequest{
					CustomerID: "tenant-a", WorkloadRef: "job-once",
					IdempotencyKey: "reserve:once", ExpiresAt: expiresAt, PriceQuote: onceQuote,
				})
				holds <- hold
				errs <- err
			}()
		}
		close(start)
		wg.Wait()
		close(holds)
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent reserve retry: %v", err)
			}
		}
		var firstID int64
		for hold := range holds {
			if firstID == 0 {
				firstID = hold.ID
			} else if hold.ID != firstID {
				t.Fatalf("exact retries returned holds %d and %d", firstID, hold.ID)
			}
		}
		got, _ = store.GetAccount(ctx, "tenant-a")
		if got.HeldMicroUSD != 80 {
			t.Fatalf("reservation retry applied more than once: %+v", got)
		}
	})

	t.Run("capture release and tenant isolation", func(t *testing.T) {
		reset(t)
		account(t, "tenant-a", 100)
		account(t, "tenant-b", 100)
		hold, err := store.ReserveCredit(ctx, ReservationRequest{
			CustomerID: "tenant-a", WorkloadRef: "job-a",
			IdempotencyKey: "reserve:job-a", ExpiresAt: time.Now().Add(time.Minute), PriceQuote: quote("job-a", 80),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.GetHold(ctx, "tenant-b", hold.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-tenant read = %v", err)
		}
		// Every cross-tenant mutation against tenant A's pending hold, funding
		// source and the ledger those objects touch must return ErrNotFound and
		// leave both accounts, the hold, tenant A's funding source and both
		// tenants' ledgers unchanged.
		snapshotFunding := func(t *testing.T, tenant, paymentObjectID string) (int64, int64) {
			t.Helper()
			var credited, reversed int64
			if err := pool.QueryRow(ctx, `
				SELECT credited_micro_usd, reversed_micro_usd
				FROM billing.funding_sources
				WHERE customer_id=$1 AND payment_object_id=$2`, tenant, paymentObjectID).Scan(&credited, &reversed); err != nil {
				t.Fatalf("snapshot funding %s/%s: %v", tenant, paymentObjectID, err)
			}
			return credited, reversed
		}
		snapshotLedgerRows := func(t *testing.T, tenant string) int64 {
			t.Helper()
			var rows int64
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM billing.ledger WHERE customer_id=$1`, tenant).Scan(&rows); err != nil {
				t.Fatalf("snapshot ledger %s: %v", tenant, err)
			}
			return rows
		}
		snapshotOperationRows := func(t *testing.T, tenant string) int64 {
			t.Helper()
			var rows int64
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM billing.operations WHERE customer_id=$1`, tenant).Scan(&rows); err != nil {
				t.Fatalf("snapshot operations %s: %v", tenant, err)
			}
			return rows
		}
		beforeA, err := store.GetAccount(ctx, "tenant-a")
		if err != nil {
			t.Fatal(err)
		}
		beforeB, err := store.GetAccount(ctx, "tenant-b")
		if err != nil {
			t.Fatal(err)
		}
		beforeHold, err := store.GetHold(ctx, "tenant-a", hold.ID)
		if err != nil {
			t.Fatal(err)
		}
		beforeCredited, beforeReversed := snapshotFunding(t, "tenant-a", "seed:tenant-a")
		beforeLedgerA := snapshotLedgerRows(t, "tenant-a")
		beforeLedgerB := snapshotLedgerRows(t, "tenant-b")
		beforeOperationsA := snapshotOperationRows(t, "tenant-a")
		beforeOperationsB := snapshotOperationRows(t, "tenant-b")

		if err := store.CaptureHold(ctx, "tenant-b", hold.ID, 1, "capture:cross-tenant", "usage-x"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-tenant capture = %v", err)
		}
		if err := store.ReleaseHold(ctx, "tenant-b", hold.ID, "release:cross-tenant", "provision-failed"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-tenant release = %v", err)
		}
		if err := store.ReverseCredit(ctx, CreditReversalRequest{
			CustomerID: "tenant-b", AmountMicroUSD: 10, IdempotencyKey: "refund:cross-tenant",
			FundingProvider: "yscale", FundingProviderAccountID: "internal", FundingLiveMode: false,
			PaymentObjectID: "seed:tenant-a", ReversalProvider: "stripe",
			ReversalProviderAccountID: "acct_test", ReversalLiveMode: false,
			ReversalObjectID: "re_cross_tenant", Kind: ReversalRefund,
		}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-tenant reverse = %v", err)
		}

		afterA, err := store.GetAccount(ctx, "tenant-a")
		if err != nil {
			t.Fatal(err)
		}
		afterB, err := store.GetAccount(ctx, "tenant-b")
		if err != nil {
			t.Fatal(err)
		}
		afterHold, err := store.GetHold(ctx, "tenant-a", hold.ID)
		if err != nil {
			t.Fatal(err)
		}
		afterCredited, afterReversed := snapshotFunding(t, "tenant-a", "seed:tenant-a")
		if afterA != beforeA || afterB != beforeB || afterHold != beforeHold ||
			afterCredited != beforeCredited || afterReversed != beforeReversed ||
			snapshotLedgerRows(t, "tenant-a") != beforeLedgerA ||
			snapshotLedgerRows(t, "tenant-b") != beforeLedgerB ||
			snapshotOperationRows(t, "tenant-a") != beforeOperationsA ||
			snapshotOperationRows(t, "tenant-b") != beforeOperationsB {
			t.Fatalf("cross-tenant mutation changed state: accountA %+v→%+v accountB %+v→%+v hold %+v→%+v funding=(%d,%d)→(%d,%d)",
				beforeA, afterA, beforeB, afterB, beforeHold, afterHold,
				beforeCredited, beforeReversed, afterCredited, afterReversed)
		}

		if err := store.CaptureHold(ctx, "tenant-a", hold.ID, 81, "capture:too-much", "usage-a"); !errors.Is(err, ErrCaptureExceedsHold) {
			t.Fatalf("over-capture = %v", err)
		}
		if err := store.CaptureHold(ctx, "tenant-a", hold.ID, 60, "capture:job-a", "usage-a"); err != nil {
			t.Fatal(err)
		}
		if err := store.CaptureHold(ctx, "tenant-a", hold.ID, 60, "capture:job-a", "usage-a"); err != nil {
			t.Fatalf("capture retry: %v", err)
		}
		if err := store.CaptureHold(ctx, "tenant-a", hold.ID, 59, "capture:job-a", "usage-a"); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("conflicting capture = %v", err)
		}
		got, _ := store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 40 || got.HeldMicroUSD != 0 {
			t.Fatalf("captured account=%+v", got)
		}

		hold, err = store.ReserveCredit(ctx, ReservationRequest{
			CustomerID: "tenant-b", WorkloadRef: "job-b",
			IdempotencyKey: "reserve:job-b", ExpiresAt: time.Now().Add(time.Minute), PriceQuote: quote("job-b", 40),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.ReleaseHold(ctx, "tenant-b", hold.ID, "release:job-b", "provision-failed"); err != nil {
			t.Fatal(err)
		}
		if err := store.ReleaseHold(ctx, "tenant-b", hold.ID, "release:job-b", "provision-failed"); err != nil {
			t.Fatalf("release retry: %v", err)
		}
		if err := store.ReleaseHold(ctx, "tenant-b", hold.ID, "release:job-b-again", "provision-failed"); !errors.Is(err, ErrIllegalTransition) {
			t.Fatalf("terminal hold released with new key: %v", err)
		}
	})

	t.Run("expiry is one shot", func(t *testing.T) {
		reset(t)
		account(t, "tenant-a", 100)
		_, err := store.ReserveCredit(ctx, ReservationRequest{
			CustomerID: "tenant-a", WorkloadRef: "short",
			IdempotencyKey: "reserve:short", ExpiresAt: time.Now().Add(75 * time.Millisecond), PriceQuote: quote("short", 70),
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		count, err := store.ExpireStaleHolds(ctx, 10)
		if err != nil || count != 1 {
			t.Fatalf("expiry count=%d err=%v", count, err)
		}
		count, err = store.ExpireStaleHolds(ctx, 10)
		if err != nil || count != 0 {
			t.Fatalf("second expiry count=%d err=%v", count, err)
		}
		got, _ := store.GetAccount(ctx, "tenant-a")
		if got.HeldMicroUSD != 0 || got.BalanceMicroUSD != 100 {
			t.Fatalf("expired account=%+v", got)
		}
	})

	t.Run("reversal records debt and freezes without losing held usage", func(t *testing.T) {
		reset(t)
		account(t, "tenant-a", 100)
		hold, err := store.ReserveCredit(ctx, ReservationRequest{
			CustomerID: "tenant-a", WorkloadRef: "running",
			IdempotencyKey: "reserve:running", ExpiresAt: time.Now().Add(time.Minute), PriceQuote: quote("running", 40),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.ReverseCredit(ctx, CreditReversalRequest{
			CustomerID: "tenant-a", AmountMicroUSD: 100, IdempotencyKey: "reverse:charge",
			FundingProvider: "yscale", FundingProviderAccountID: "internal", FundingLiveMode: false,
			PaymentObjectID: "seed:tenant-a", ReversalProvider: "stripe",
			ReversalProviderAccountID: "acct_test", ReversalLiveMode: false,
			ReversalObjectID: "dp_1", Kind: ReversalDispute,
		}); err != nil {
			t.Fatal(err)
		}
		got, _ := store.GetAccount(ctx, "tenant-a")
		if !got.Frozen || got.BalanceMicroUSD != 40 || got.HeldMicroUSD != 40 || got.DebtMicroUSD != 40 {
			t.Fatalf("reversed account=%+v", got)
		}
		if _, err := store.ReserveCredit(ctx, ReservationRequest{
			CustomerID: "tenant-a", WorkloadRef: "blocked",
			IdempotencyKey: "reserve:blocked", ExpiresAt: time.Now().Add(time.Minute), PriceQuote: quote("blocked", 1),
		}); !errors.Is(err, ErrAccountFrozen) {
			t.Fatalf("frozen reservation = %v", err)
		}
		if err := store.CaptureHold(ctx, "tenant-a", hold.ID, 20, "capture:running", "usage-running"); err != nil {
			t.Fatalf("existing held usage must remain capturable: %v", err)
		}
		if err := store.GrantCredit(ctx, CreditGrantRequest{
			CustomerID: "tenant-a", AmountMicroUSD: 40, IdempotencyKey: "grant:repay",
			Provider: "stripe", ProviderAccountID: "acct_test", LiveMode: false,
			PaymentObjectID: "pi_repay",
		}); err != nil {
			t.Fatal(err)
		}
		got, _ = store.GetAccount(ctx, "tenant-a")
		if got.DebtMicroUSD != 0 || got.BalanceMicroUSD != 20 || !got.Frozen {
			t.Fatalf("debt repayment account=%+v", got)
		}
	})

	t.Run("dispute withdrawal and restoration converge across reordering", func(t *testing.T) {
		reset(t)
		account(t, "tenant-a", 100)
		request := CreditDisputeRequest{
			CustomerID: "tenant-a", AmountMicroUSD: 100, IdempotencyKey: "dispute:dp_one:withdrawn",
			FundingProvider: "yscale", FundingProviderAccountID: "internal", FundingLiveMode: false,
			PaymentObjectID: "seed:tenant-a", DisputeProvider: "stripe",
			DisputeProviderAccountID: "acct_test", DisputeLiveMode: false,
			DisputeObjectID: "dp_one", State: DisputeWithdrawn,
		}
		if err := store.ApplyDispute(ctx, request); err != nil {
			t.Fatal(err)
		}
		if err := store.ApplyDispute(ctx, request); err != nil {
			t.Fatalf("withdrawal replay: %v", err)
		}
		got, _ := store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 0 || got.DebtMicroUSD != 0 || !got.Frozen {
			t.Fatalf("withdrawn account=%+v", got)
		}
		request.State = DisputeRestored
		request.IdempotencyKey = "dispute:dp_one:restored"
		if err := store.ApplyDispute(ctx, request); err != nil {
			t.Fatal(err)
		}
		if err := store.ApplyDispute(ctx, request); err != nil {
			t.Fatalf("restoration replay: %v", err)
		}
		got, _ = store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 100 || got.DebtMicroUSD != 0 || got.Frozen {
			t.Fatalf("restored account=%+v", got)
		}
		request.State = DisputeWithdrawn
		request.IdempotencyKey = "dispute:dp_one:stale-withdrawal"
		if err := store.ApplyDispute(ctx, request); err != nil {
			t.Fatalf("stale withdrawal: %v", err)
		}
		got, _ = store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 100 || got.Frozen {
			t.Fatalf("stale withdrawal changed restored account=%+v", got)
		}
		var reversals, restorations int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM billing.ledger WHERE customer_id=$1 AND entry_type='reversal'`, "tenant-a").Scan(&reversals); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM billing.ledger WHERE customer_id=$1 AND entry_type='restoration'`, "tenant-a").Scan(&restorations); err != nil {
			t.Fatal(err)
		}
		if reversals != 1 || restorations != 1 {
			t.Fatalf("reversal ledger=%d restoration ledger=%d", reversals, restorations)
		}

		reset(t)
		account(t, "tenant-a", 100)
		hold, err := store.ReserveCredit(ctx, ReservationRequest{
			CustomerID: "tenant-a", WorkloadRef: "spent-before-dispute",
			IdempotencyKey: "reserve:spent-before-dispute", ExpiresAt: time.Now().Add(time.Minute),
			PriceQuote: quote("spent-before-dispute", 80),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CaptureHold(ctx, "tenant-a", hold.ID, 80, "capture:spent-before-dispute", "usage"); err != nil {
			t.Fatal(err)
		}
		request = CreditDisputeRequest{
			CustomerID: "tenant-a", AmountMicroUSD: 100, IdempotencyKey: "dispute:dp_spent:withdrawn",
			FundingProvider: "yscale", FundingProviderAccountID: "internal", FundingLiveMode: false,
			PaymentObjectID: "seed:tenant-a", DisputeProvider: "stripe",
			DisputeProviderAccountID: "acct_test", DisputeLiveMode: false,
			DisputeObjectID: "dp_spent", State: DisputeWithdrawn,
		}
		if err := store.ApplyDispute(ctx, request); err != nil {
			t.Fatal(err)
		}
		got, _ = store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 0 || got.DebtMicroUSD != 80 || !got.Frozen {
			t.Fatalf("spent withdrawal account=%+v", got)
		}
		if err := store.GrantCredit(ctx, CreditGrantRequest{
			CustomerID: "tenant-a", AmountMicroUSD: 40, IdempotencyKey: "payment:debt-repayment",
			Provider: "stripe", ProviderAccountID: "acct_test", LiveMode: false,
			PaymentObjectID: "pi_debt_repayment",
		}); err != nil {
			t.Fatal(err)
		}
		request.State = DisputeRestored
		request.IdempotencyKey = "dispute:dp_spent:restored"
		if err := store.ApplyDispute(ctx, request); err != nil {
			t.Fatal(err)
		}
		got, _ = store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 60 || got.DebtMicroUSD != 0 || got.Frozen {
			t.Fatalf("spent restoration account=%+v", got)
		}

		reset(t)
		account(t, "tenant-a", 100)
		request = CreditDisputeRequest{
			CustomerID: "tenant-a", AmountMicroUSD: 100, IdempotencyKey: "dispute:dp_reordered:restored",
			FundingProvider: "yscale", FundingProviderAccountID: "internal", FundingLiveMode: false,
			PaymentObjectID: "seed:tenant-a", DisputeProvider: "stripe",
			DisputeProviderAccountID: "acct_test", DisputeLiveMode: false,
			DisputeObjectID: "dp_reordered", State: DisputeRestored,
		}
		if err := store.ApplyDispute(ctx, request); err != nil {
			t.Fatal(err)
		}
		request.State = DisputeWithdrawn
		request.IdempotencyKey = "dispute:dp_reordered:withdrawn"
		if err := store.ApplyDispute(ctx, request); err != nil {
			t.Fatal(err)
		}
		got, _ = store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 100 || got.Frozen {
			t.Fatalf("restoration-first reorder changed account=%+v", got)
		}

		reset(t)
		account(t, "tenant-a", 100)
		start := make(chan struct{})
		errs := make(chan error, 2)
		for _, state := range []DisputeState{DisputeWithdrawn, DisputeRestored} {
			state := state
			go func() {
				<-start
				errs <- store.ApplyDispute(ctx, CreditDisputeRequest{
					CustomerID: "tenant-a", AmountMicroUSD: 100,
					IdempotencyKey:  "dispute:dp_concurrent:" + string(state),
					FundingProvider: "yscale", FundingProviderAccountID: "internal", FundingLiveMode: false,
					PaymentObjectID: "seed:tenant-a", DisputeProvider: "stripe",
					DisputeProviderAccountID: "acct_test", DisputeLiveMode: false,
					DisputeObjectID: "dp_concurrent", State: state,
				})
			}()
		}
		close(start)
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatalf("concurrent dispute convergence: %v", err)
			}
		}
		got, _ = store.GetAccount(ctx, "tenant-a")
		if got.BalanceMicroUSD != 100 || got.DebtMicroUSD != 0 || got.Frozen {
			t.Fatalf("concurrent dispute did not converge restored=%+v", got)
		}
	})

	t.Run("dispute withdrawal preserves reversal classification", func(t *testing.T) {
		for _, kind := range []ReversalKind{ReversalChargeback, ReversalSettlementFailure} {
			t.Run(string(kind), func(t *testing.T) {
				reset(t)
				account(t, "tenant-a", 100)
				request := CreditDisputeRequest{
					CustomerID: "tenant-a", AmountMicroUSD: 100,
					IdempotencyKey:  "dispute:" + string(kind),
					FundingProvider: "yscale", FundingProviderAccountID: "internal", FundingLiveMode: false,
					PaymentObjectID: "seed:tenant-a", DisputeProvider: "stripe",
					DisputeProviderAccountID: "acct_test", DisputeLiveMode: false,
					DisputeObjectID: "dp_" + string(kind), Kind: kind, State: DisputeWithdrawn,
				}
				if err := store.ApplyDispute(ctx, request); err != nil {
					t.Fatal(err)
				}
				if err := store.ApplyDispute(ctx, request); err != nil {
					t.Fatalf("replay: %v", err)
				}
				var storedKind ReversalKind
				var reversals int
				if err := pool.QueryRow(ctx, `SELECT reversal_kind FROM billing.credit_reversals WHERE customer_id=$1`, "tenant-a").Scan(&storedKind); err != nil {
					t.Fatal(err)
				}
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM billing.ledger WHERE customer_id=$1 AND entry_type='reversal'`, "tenant-a").Scan(&reversals); err != nil {
					t.Fatal(err)
				}
				got, err := store.GetAccount(ctx, "tenant-a")
				if err != nil {
					t.Fatal(err)
				}
				if storedKind != kind || reversals != 1 || got.BalanceMicroUSD != 0 || !got.Frozen {
					t.Fatalf("kind=%q reversals=%d account=%+v", storedKind, reversals, got)
				}
			})
		}
	})

	t.Run("verified webhook is deduplicated and lease protected", func(t *testing.T) {
		reset(t)
		created := time.Now().UTC().Truncate(time.Microsecond)
		event := VerifiedWebhookEvent{
			Provider: "stripe", ProviderAccountID: "acct_test",
			EventID: "evt_1", EventType: "checkout.session.completed",
			ObjectID: "cs_1", CustomerID: "tenant-a", APIVersion: "2026-06-30",
			LiveMode: false, ProviderCreatedAt: created, Payload: []byte(`{"id":"evt_1"}`),
		}
		id, inserted, err := store.RecordVerifiedWebhook(ctx, event)
		if err != nil || !inserted {
			t.Fatalf("insert webhook id=%d inserted=%v err=%v", id, inserted, err)
		}
		duplicateID, inserted, err := store.RecordVerifiedWebhook(ctx, event)
		if err != nil || inserted || duplicateID != id {
			t.Fatalf("duplicate webhook id=%d inserted=%v err=%v", duplicateID, inserted, err)
		}
		event.Payload = []byte(`{"id":"evt_1","changed":true}`)
		if _, _, err := store.RecordVerifiedWebhook(ctx, event); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("conflicting webhook = %v", err)
		}
		delivery, ok, err := store.ClaimNextWebhook(ctx, time.Minute)
		if err != nil || !ok || delivery.ID != id {
			t.Fatalf("claim=%+v ok=%v err=%v", delivery, ok, err)
		}
		if err := store.MarkWebhookProcessed(ctx, id, "wrong-token"); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("wrong lease = %v", err)
		}
		if err := store.MarkWebhookProcessed(ctx, id, delivery.LeaseToken); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := store.ClaimNextWebhook(ctx, time.Minute); err != nil || ok {
			t.Fatalf("processed webhook reclaimed: ok=%v err=%v", ok, err)
		}
		wrongMode := event
		wrongMode.LiveMode = true
		wrongMode.Payload = []byte(`{"id":"evt_1"}`)
		if _, _, err := store.RecordVerifiedWebhook(ctx, wrongMode); !errors.Is(err, ErrEnvironmentMismatch) {
			t.Fatalf("live webhook entered test-mode store: %v", err)
		}

		event.EventID = "evt_2"
		event.ObjectID = "cs_2"
		event.Payload = []byte(`{"id":"evt_2"}`)
		if _, _, err := store.RecordVerifiedWebhook(ctx, event); err != nil {
			t.Fatal(err)
		}
		delivery, ok, err = store.ClaimNextWebhook(ctx, time.Minute)
		if err != nil || !ok {
			t.Fatalf("claim retry fixture: ok=%v err=%v", ok, err)
		}
		retryAt := time.Now().Add(75 * time.Millisecond)
		if err := store.MarkWebhookFailed(ctx, delivery.ID, delivery.LeaseToken, "temporary upstream failure", retryAt, 2); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := store.ClaimNextWebhook(ctx, time.Minute); err != nil || ok {
			t.Fatalf("webhook reclaimed before retry: ok=%v err=%v", ok, err)
		}
		time.Sleep(100 * time.Millisecond)
		delivery, ok, err = store.ClaimNextWebhook(ctx, time.Minute)
		if err != nil || !ok || delivery.Attempts != 2 {
			t.Fatalf("failed webhook not retried: delivery=%+v ok=%v err=%v", delivery, ok, err)
		}
		if err := store.MarkWebhookFailed(ctx, delivery.ID, delivery.LeaseToken, "permanent validation failure", time.Now().Add(time.Minute), 2); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := store.ClaimNextWebhook(ctx, time.Minute); err != nil || ok {
			t.Fatalf("dead-letter webhook reclaimed: ok=%v err=%v", ok, err)
		}

		oversized := event
		oversized.EventID = "evt_oversized"
		oversized.ObjectID = "cs_oversized"
		oversized.Payload = []byte(`{"data":"` + strings.Repeat("x", maxWebhookPayloadBytes) + `"}`)
		if _, _, err := store.RecordVerifiedWebhook(ctx, oversized); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("oversized Store payload = %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO billing.webhook_inbox
			(provider, provider_account_id, provider_event_id, event_type, object_id,
			 livemode, provider_created_at, payload, payload_hash)
			VALUES ('stripe','acct_test','evt_raw_oversized','test','obj',FALSE,now(),$1,$2)`,
			strings.Repeat("x", maxWebhookPayloadBytes+1), strings.Repeat("0", 64)); err == nil {
			t.Fatal("database accepted oversized webhook payload")
		}
	})

	t.Run("checkout intent precedes provider and replays exactly", func(t *testing.T) {
		reset(t)
		account(t, "tenant-a", 0)
		account(t, "tenant-b", 0)
		request := BeginCheckoutRequest{
			CustomerID: "tenant-a", IdempotencyKey: "buy:one", AmountMicroUSD: 5_000_000,
			Provider: "stripe", ProviderAccountID: "acct_test", LiveMode: false,
		}
		checkout, inserted, err := store.BeginCheckout(ctx, request)
		if err != nil || !inserted || checkout.State != CheckoutPendingProvider || checkout.ProviderSessionID != "" {
			t.Fatalf("begin checkout=%+v inserted=%v err=%v", checkout, inserted, err)
		}
		replayed, inserted, err := store.BeginCheckout(ctx, request)
		if err != nil || inserted || replayed.ID != checkout.ID {
			t.Fatalf("replay checkout=%+v inserted=%v err=%v", replayed, inserted, err)
		}
		conflict := request
		conflict.AmountMicroUSD += microUSDPerCent
		if _, _, err := store.BeginCheckout(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("conflicting checkout retry=%v", err)
		}
		attached, err := store.AttachCheckout(ctx, "tenant-a", checkout.ID, ProviderCheckoutSession{
			ID: "cs_test_one", URL: "https://checkout.stripe.com/c/pay/one",
		})
		if err != nil || attached.State != CheckoutOpen || attached.ProviderSessionID != "cs_test_one" {
			t.Fatalf("attach checkout=%+v err=%v", attached, err)
		}
		if _, err := store.AttachCheckout(ctx, "tenant-a", checkout.ID, ProviderCheckoutSession{
			ID: "cs_test_one", URL: "https://checkout.stripe.com/c/pay/one",
		}); err != nil {
			t.Fatalf("exact attach replay=%v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE billing.checkout_sessions SET amount_micro_usd=6000000 WHERE id=$1`, checkout.ID); err == nil {
			t.Fatal("checkout identity accepted mutation")
		}
		if _, err := pool.Exec(ctx, `UPDATE billing.checkout_sessions SET provider_session_id='cs_other' WHERE id=$1`, checkout.ID); err == nil {
			t.Fatal("checkout provider session accepted mutation")
		}
		if _, err := store.GetCheckout(ctx, "tenant-b", checkout.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-tenant checkout read=%v", err)
		}
		second, _, err := store.BeginCheckout(ctx, BeginCheckoutRequest{
			CustomerID: "tenant-b", IdempotencyKey: "buy:two", AmountMicroUSD: 5_000_000,
			Provider: "stripe", ProviderAccountID: "acct_test", LiveMode: false,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.AttachCheckout(ctx, "tenant-b", second.ID, ProviderCheckoutSession{
			ID: "cs_test_one", URL: "https://checkout.stripe.com/c/pay/two",
		}); !errors.Is(err, ErrEconomicObjectConflict) {
			t.Fatalf("provider session reused across tenants=%v", err)
		}
		if _, err := store.AttachCheckout(ctx, "tenant-b", checkout.ID, ProviderCheckoutSession{
			ID: "cs_test_two", URL: "https://checkout.stripe.com/c/pay/three",
		}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-tenant attach on tenant-a checkout = %v", err)
		}
		aAfter, err := store.GetCheckout(ctx, "tenant-a", checkout.ID)
		if err != nil || aAfter.ProviderSessionID != "cs_test_one" || aAfter.State != CheckoutOpen {
			t.Fatalf("tenant-a checkout mutated by cross-tenant attach: %+v err=%v", aAfter, err)
		}
		bAfter, err := store.GetCheckout(ctx, "tenant-b", second.ID)
		if err != nil || bAfter.ProviderSessionID != "" || bAfter.State != CheckoutPendingProvider {
			t.Fatalf("tenant-b checkout entered open state without a valid attach: %+v err=%v", bAfter, err)
		}
		for _, tenant := range []string{"tenant-a", "tenant-b"} {
			account, err := store.GetAccount(ctx, tenant)
			if err != nil || account.BalanceMicroUSD != 0 {
				t.Fatalf("checkout creation credited %s: %+v err=%v", tenant, account, err)
			}
		}
	})

	t.Run("leased settled payment and refund converge exactly once", func(t *testing.T) {
		reset(t)
		account(t, "tenant-paid", 0)
		checkout, _, err := store.BeginCheckout(ctx, BeginCheckoutRequest{
			CustomerID: "tenant-paid", IdempotencyKey: "buy:paid", AmountMicroUSD: 5_000_000,
			Provider: "stripe", ProviderAccountID: "acct_test", LiveMode: false,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.AttachCheckout(ctx, "tenant-paid", checkout.ID, ProviderCheckoutSession{
			ID: "cs_paid", URL: "https://checkout.stripe.com/c/pay/paid",
		}); err != nil {
			t.Fatal(err)
		}
		record := func(eventID, eventType, objectID string) {
			t.Helper()
			payload := []byte(fmt.Sprintf(`{"id":%q}`, eventID))
			if _, _, err := store.RecordVerifiedWebhook(ctx, VerifiedWebhookEvent{
				Provider: "stripe", ProviderAccountID: "acct_test", EventID: eventID,
				EventType: eventType, ObjectID: objectID, LiveMode: false,
				ProviderCreatedAt: time.Now().UTC(), Payload: payload,
			}); err != nil {
				t.Fatal(err)
			}
		}
		resolver := &processorResolverFake{resolution: WebhookResolution{
			Kind: WebhookCheckoutSettled, ProviderSessionID: "cs_paid", CheckoutID: checkout.ID,
			CustomerID: "tenant-paid", PaymentObjectID: "pi_paid", AmountMicroUSD: 5_000_000,
		}}
		processor := &WebhookProcessor{Store: store, Resolver: resolver}
		for _, eventID := range []string{"evt_paid_1", "evt_paid_2"} {
			record(eventID, "checkout.session.completed", "cs_paid")
			if ok, err := processor.ProcessOne(ctx); err != nil || !ok {
				t.Fatalf("settled %s: ok=%v err=%v", eventID, ok, err)
			}
		}
		account, err := store.GetAccount(ctx, "tenant-paid")
		if err != nil || account.BalanceMicroUSD != 5_000_000 {
			t.Fatalf("duplicate settlement account=%+v err=%v", account, err)
		}
		resolver.resolution = WebhookResolution{
			Kind: WebhookRefundSucceeded, PaymentObjectID: "pi_paid", ReversalObjectID: "re_paid", AmountMicroUSD: 2_000_000,
		}
		for _, eventID := range []string{"evt_refund_1", "evt_refund_2"} {
			record(eventID, "refund.updated", "re_paid")
			if ok, err := processor.ProcessOne(ctx); err != nil || !ok {
				t.Fatalf("refund %s: ok=%v err=%v", eventID, ok, err)
			}
		}
		account, err = store.GetAccount(ctx, "tenant-paid")
		if err != nil || account.BalanceMicroUSD != 3_000_000 || account.DebtMicroUSD != 0 {
			t.Fatalf("duplicate refund account=%+v err=%v", account, err)
		}
	})

	t.Run("statement snapshot closes exact ledger and held totals", func(t *testing.T) {
		reset(t)
		account(t, "tenant-statement", 100)
		hold, err := store.ReserveCredit(ctx, ReservationRequest{
			CustomerID: "tenant-statement", WorkloadRef: "wl_statement",
			IdempotencyKey: "reserve:statement", ExpiresAt: time.Now().Add(time.Minute),
			PriceQuote: quote("statement", 100),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CaptureHold(ctx, "tenant-statement", hold.ID, 40, "capture:statement", "private-provider-ref"); err != nil {
			t.Fatal(err)
		}
		statement, err := store.GetStatement(ctx, "tenant-statement", time.Now().Add(-time.Minute), time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if statement.CustomerID != "tenant-statement" || statement.Currency != "USD" ||
			statement.OpeningNetCreditMicroUSD != 0 || statement.OpeningHeldMicroUSD != 0 ||
			statement.ClosingNetCreditMicroUSD != 60 || statement.ClosingHeldMicroUSD != 0 ||
			statement.LedgerWatermark == 0 || len(statement.Entries) != 3 {
			t.Fatalf("statement=%+v", statement)
		}
		capture := statement.Entries[2]
		if capture.EntryType != "capture" || capture.AmountMicroUSD != 40 ||
			capture.BalanceDeltaMicroUSD != -40 || capture.HeldDeltaMicroUSD != -100 ||
			capture.NetCreditMicroUSD != 60 || capture.HeldMicroUSD != 0 ||
			capture.WorkloadID != "wl_statement" || capture.HoldID == nil || *capture.HoldID != hold.ID {
			t.Fatalf("capture entry=%+v", capture)
		}
		if _, err := store.GetStatement(ctx, "tenant-other", time.Now().Add(-time.Minute), time.Now().Add(time.Minute)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-tenant statement=%v", err)
		}
		if _, err := store.GetStatement(ctx, "tenant-statement", time.Now().Add(-MaxStatementPeriod), time.Now().Add(time.Second)); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("store accepted oversized statement period=%v", err)
		}
	})

	t.Run("statement refuses silent truncation", func(t *testing.T) {
		reset(t)
		account(t, "tenant-statement-cap", 0)
		if _, err := pool.Exec(ctx, `
			WITH inserted AS (INSERT INTO billing.operations
			(idempotency_key, customer_id, operation_type, canonical_version, payload_hash)
			SELECT 'cap:' || g, 'tenant-statement-cap', 'grant', 1, repeat('0', 64)
			FROM generate_series(1, $1) AS g RETURNING idempotency_key)
			INSERT INTO billing.ledger
			(customer_id, entry_type, amount_micro_usd, operation_key)
			SELECT 'tenant-statement-cap', 'credit', 1, idempotency_key FROM inserted`, MaxStatementEntries+1); err != nil {
			t.Fatal(err)
		}
		_, err := store.GetStatement(ctx, "tenant-statement-cap", time.Now().Add(-time.Minute), time.Now().Add(time.Minute))
		if !errors.Is(err, ErrStatementTooLarge) {
			t.Fatalf("oversized statement=%v", err)
		}
	})

	t.Run("database rejects ledger mutation", func(t *testing.T) {
		reset(t)
		account(t, "tenant-a", 100)
		hold, err := store.ReserveCredit(ctx, ReservationRequest{
			CustomerID: "tenant-a", WorkloadRef: "immutable",
			IdempotencyKey: "reserve:immutable", ExpiresAt: time.Now().Add(time.Minute), PriceQuote: quote("immutable", 10),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.ReleaseHold(ctx, "tenant-a", hold.ID, "release:immutable", "test"); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE billing.ledger SET amount_micro_usd=1`); err == nil {
			t.Fatal("append-only ledger accepted UPDATE")
		}
		if _, err := pool.Exec(ctx, `UPDATE billing.holds SET state='pending' WHERE id=$1`, hold.ID); err == nil {
			t.Fatal("terminal hold transition trigger accepted illegal UPDATE")
		}
	})
}
