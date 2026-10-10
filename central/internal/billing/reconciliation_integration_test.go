//go:build integration

// yscale:proprietary

package billing

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type integrationCashProvider struct {
	objects []ExternalCashObject
}

type integrationUsageSource struct {
	receipts []UsageReceipt
}

func (s integrationUsageSource) UsageReceiptSnapshot(context.Context) ([]UsageReceipt, error) {
	return append([]UsageReceipt(nil), s.receipts...), nil
}

func (p integrationCashProvider) ExternalCashSnapshot(context.Context, int) ([]ExternalCashObject, error) {
	return p.objects, nil
}

func TestLedgerReconciliationReportsWithoutRepairing(t *testing.T) {
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
	defer pool.Close()
	var databaseName string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("BILLING_TEST_EXPECT_DATABASE"); expected == "" || expected != databaseName || !strings.HasSuffix(databaseName, "_billing_test") {
		t.Fatalf("refusing destructive reconciliation test against database %q", databaseName)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS billing CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS billing CASCADE`) })
	if err := EnsureSchema(ctx, pool, false); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool, false)
	if err := store.EnsureAccount(ctx, "tenant-reconcile"); err != nil {
		t.Fatal(err)
	}
	if err := store.GrantCredit(ctx, CreditGrantRequest{
		CustomerID: "tenant-reconcile", AmountMicroUSD: 1_000_000, IdempotencyKey: "grant:reconcile",
		Provider: "stripe", ProviderAccountID: "acct_test", LiveMode: false, PaymentObjectID: "pi_reconcile",
	}); err != nil {
		t.Fatal(err)
	}

	assertHealthy := func() {
		t.Helper()
		summary, err := store.ReconcileLedger(ctx)
		if err != nil || !summary.Healthy() {
			t.Fatalf("healthy ledger reconciliation = %+v, %v", summary, err)
		}
	}
	exec := func(query string) {
		t.Helper()
		if _, err := pool.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	assertHealthy()

	if _, err := pool.Exec(ctx, `UPDATE billing.accounts SET balance_micro_usd=balance_micro_usd+1 WHERE customer_id='tenant-reconcile'`); err != nil {
		t.Fatal(err)
	}
	summary, err := store.ReconcileLedger(ctx)
	if err != nil || summary.AccountBalances != 1 {
		t.Fatalf("account mismatch summary = %+v, %v", summary, err)
	}
	var balance int64
	if err := pool.QueryRow(ctx, `SELECT balance_micro_usd FROM billing.accounts WHERE customer_id='tenant-reconcile'`).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 1_000_001 {
		t.Fatalf("reconciliation rewrote balance: got %d", balance)
	}
	exec(`UPDATE billing.accounts SET balance_micro_usd=1000000, held_micro_usd=1 WHERE customer_id='tenant-reconcile'`)
	summary, err = store.ReconcileLedger(ctx)
	if err != nil || summary.HeldBalances != 1 {
		t.Fatalf("held mismatch summary = %+v, %v", summary, err)
	}
	exec(`UPDATE billing.accounts SET held_micro_usd=0 WHERE customer_id='tenant-reconcile'`)

	exec(`UPDATE billing.funding_sources SET reversed_micro_usd=1 WHERE customer_id='tenant-reconcile'`)
	summary, err = store.ReconcileLedger(ctx)
	if err != nil || summary.FundingReversals != 1 {
		t.Fatalf("funding mismatch summary = %+v, %v", summary, err)
	}
	exec(`UPDATE billing.funding_sources SET reversed_micro_usd=0, credited_micro_usd=credited_micro_usd+1 WHERE customer_id='tenant-reconcile'`)
	summary, err = store.ReconcileLedger(ctx)
	if err != nil || summary.EconomicObjects != 1 {
		t.Fatalf("economic-object mismatch summary = %+v, %v", summary, err)
	}
	exec(`UPDATE billing.funding_sources SET credited_micro_usd=1000000 WHERE customer_id='tenant-reconcile'`)
	// Simulate a privileged restore/maintenance fault that bypassed the normal
	// append-only trigger. Production runtime roles cannot perform either ALTER.
	exec(`ALTER TABLE billing.ledger DISABLE TRIGGER billing_ledger_immutable`)
	exec(`DELETE FROM billing.ledger WHERE customer_id='tenant-reconcile' AND operation_key='grant:reconcile'`)
	exec(`ALTER TABLE billing.ledger ENABLE TRIGGER billing_ledger_immutable`)
	summary, err = store.ReconcileLedger(ctx)
	if err != nil || summary.Operations != 1 || summary.EconomicObjects != 1 {
		t.Fatalf("missing-ledger summary = %+v, %v", summary, err)
	}
	exec(`INSERT INTO billing.ledger
		(customer_id, entry_type, amount_micro_usd, operation_key, external_ref)
		VALUES ('tenant-reconcile','credit',1000000,'grant:reconcile','stripe/acct_test/test/pi_reconcile')`)
	assertHealthy()

	matching := ExternalCashObject{
		Provider: "stripe", ProviderAccountID: "acct_test", LiveMode: false,
		PaymentObjectID: "pi_reconcile", CustomerID: "tenant-reconcile", CreditedMicroUSD: 1_000_000,
	}
	external, err := store.ReconcileExternalCash(ctx, integrationCashProvider{objects: []ExternalCashObject{matching}})
	if err != nil || external.TotalDifferences() != 0 {
		t.Fatalf("healthy external reconciliation = %+v, %v", external, err)
	}
	mismatched := matching
	mismatched.ReversedMicroUSD = 1
	external, err = store.ReconcileExternalCash(ctx, integrationCashProvider{objects: []ExternalCashObject{mismatched}})
	if err != nil || external.Amount != 1 {
		t.Fatalf("external amount mismatch = %+v, %v", external, err)
	}
	var reversed int64
	if err := pool.QueryRow(ctx, `SELECT reversed_micro_usd FROM billing.funding_sources WHERE payment_object_id='pi_reconcile'`).Scan(&reversed); err != nil {
		t.Fatal(err)
	}
	if reversed != 0 {
		t.Fatalf("external reconciliation rewrote funding source: %d", reversed)
	}
	if _, err := store.ReconcileExternalCash(ctx, nil); err == nil {
		t.Fatal("external funding reconciled healthy without a provider")
	}

	if err := store.EnsureAccount(ctx, "tenant-usage"); err != nil {
		t.Fatal(err)
	}
	if err := store.GrantCredit(ctx, CreditGrantRequest{
		CustomerID: "tenant-usage", AmountMicroUSD: 1_000_000, IdempotencyKey: "grant:usage",
		Provider: "yscale", ProviderAccountID: "internal", PaymentObjectID: "seed:usage",
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	hold, err := store.ReserveCredit(ctx, ReservationRequest{
		CustomerID: "tenant-usage", WorkloadRef: "workload-usage", IdempotencyKey: "reserve:usage",
		ExpiresAt: now.Add(2 * time.Hour), PriceQuote: PriceQuote{
			QuoteID: "quote:usage", PricingVersion: 1, Currency: "USD", Provider: "linode",
			SKU: "rtx4000ada", Region: "us-east", ProviderRateMicroUSDPerHour: 1_000_000,
			CustomerRateMicroUSDPerHour: 1_000_000, MaximumDurationSeconds: 3600,
			MaximumChargeMicroUSD: 1_000_000, IssuedAt: now, ValidUntil: now.Add(time.Minute),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CaptureHold(ctx, "tenant-usage", hold.ID, 250_000, "capture:usage", "burst-usage"); err != nil {
		t.Fatal(err)
	}
	usageReceipt := UsageReceipt{
		CustomerID: "tenant-usage", WorkloadID: "workload-usage", WorkloadRef: "workload-usage",
		HoldID: hold.ID, ReservedMicroUSD: 1_000_000, BurstID: "burst-usage", CostPresent: true,
		CostBurstID: "burst-usage", Backend: "linode", Authoritative: true,
		EstimatedUSD: 0.25, HourlyUSD: 1,
	}
	usage, err := store.ReconcileUsageCaptures(ctx, integrationUsageSource{receipts: []UsageReceipt{usageReceipt}})
	if err != nil || usage.TotalDifferences() != 0 {
		t.Fatalf("healthy usage reconciliation = %+v, %v", usage, err)
	}
	usageReceipt.EstimatedUSD = 0.5
	usage, err = store.ReconcileUsageCaptures(ctx, integrationUsageSource{receipts: []UsageReceipt{usageReceipt}})
	if err != nil || usage.CaptureAmount != 1 {
		t.Fatalf("usage amount mismatch = %+v, %v", usage, err)
	}
	var captured int64
	if err := pool.QueryRow(ctx, `SELECT captured_micro_usd FROM billing.holds WHERE id=$1`, hold.ID).Scan(&captured); err != nil {
		t.Fatal(err)
	}
	if captured != 250_000 {
		t.Fatalf("usage reconciliation rewrote capture: %d", captured)
	}
}
