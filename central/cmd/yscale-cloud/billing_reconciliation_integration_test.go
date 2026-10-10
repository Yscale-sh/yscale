//go:build integration

// yscale:proprietary

// The regression this file exists for: managed usage reconciliation used to
// read at most 1000 durable workload receipts and 1000 billing hold rows, and
// errored above that. Terminal holds and terminal workload rows are retained on
// purpose, so the ceiling was a lifetime one — the 1001st paid workload turned
// every reconciliation pass into a hard error, which closes the paid admission
// gate and stops the product taking money. Proving the fix needs both durable
// stores at once, so it lives here rather than in either package.

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/state"
)

// scaleReceipts is one past the retired 1000-row ceiling. Every row is a
// matched, terminal, authoritative receipt, so a complete reconciliation must
// report exactly zero differences.
const scaleReceipts = 1001

const (
	scaleCustomer     = "tenant-usage-scale"
	scaleReserveMicro = int64(1_000) // the hold amount is MaximumChargeMicroUSD
	scaleCaptureMicro = int64(500)   // TrustedCaptureMicroUSD(0.0005, 1_000)
	scaleHourlyUSD    = 0.001        // TrustedHourlyMicroUSD -> 1_000
	scaleEstimatedUSD = 0.0005
	scaleBackend      = "linode"
)

type scaleCashProvider struct {
	objects []billing.ExternalCashObject
}

func (p scaleCashProvider) ExternalCashSnapshot(context.Context, int) ([]billing.ExternalCashObject, error) {
	return p.objects, nil
}

func TestUsageReconciliationStaysCompletePastAThousandPaidWorkloads(t *testing.T) {
	stateDSN := os.Getenv("YSCALE_TEST_DATABASE_URL")
	billingDSN := os.Getenv("BILLING_TEST_DATABASE_URL")
	if stateDSN == "" || billingDSN == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL and BILLING_TEST_DATABASE_URL to throwaway Postgres databases to run this integration test")
	}
	if os.Getenv("BILLING_TEST_ALLOW_DESTRUCTIVE") != "DROP_BILLING_SCHEMA" ||
		os.Getenv("YSCALE_TEST_ALLOW_DESTRUCTIVE") != "DELETE_BILLING_WORKLOADS" {
		t.Fatal("set BILLING_TEST_ALLOW_DESTRUCTIVE=DROP_BILLING_SCHEMA and YSCALE_TEST_ALLOW_DESTRUCTIVE=DELETE_BILLING_WORKLOADS; this test owns every billing-associated workload row")
	}
	ctx := context.Background()

	billingPool, err := pgxpool.New(ctx, billingDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(billingPool.Close)
	var databaseName string
	if err := billingPool.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("BILLING_TEST_EXPECT_DATABASE"); expected == "" || expected != databaseName ||
		!strings.HasSuffix(databaseName, "_billing_test") {
		t.Fatalf("refusing destructive reconciliation test against database %q", databaseName)
	}
	if _, err := billingPool.Exec(ctx, `DROP SCHEMA IF EXISTS billing CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = billingPool.Exec(context.Background(), `DROP SCHEMA IF EXISTS billing CASCADE`) })
	if err := billing.EnsureSchema(ctx, billingPool, false); err != nil {
		t.Fatal(err)
	}
	billingStore := billing.NewStore(billingPool, false)

	workloadStore, err := state.NewPostgres(ctx, stateDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workloadStore.Close)
	statePool, err := pgxpool.New(ctx, stateDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(statePool.Close)
	var stateDatabaseName string
	if err := statePool.QueryRow(ctx, `SELECT current_database()`).Scan(&stateDatabaseName); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("YSCALE_TEST_EXPECT_DATABASE"); expected == "" || expected != stateDatabaseName ||
		!strings.HasSuffix(stateDatabaseName, "_test") {
		t.Fatalf("refusing destructive state reconciliation test against database %q", stateDatabaseName)
	}
	// Reconciliation compares the whole durable receipt set with the whole hold
	// set, so a receipt left behind by another test is a real difference. This
	// test owns them for its duration.
	clearBillingWorkloads := func() {
		if _, err := statePool.Exec(context.Background(),
			`DELETE FROM workloads WHERE data ? 'Billing' AND data->'Billing' <> 'null'::jsonb`); err != nil {
			t.Fatal(err)
		}
	}
	clearBillingWorkloads()
	t.Cleanup(clearBillingWorkloads)

	if err := billingStore.EnsureAccount(ctx, scaleCustomer); err != nil {
		t.Fatal(err)
	}
	if err := billingStore.GrantCredit(ctx, billing.CreditGrantRequest{
		CustomerID: scaleCustomer, AmountMicroUSD: scaleReserveMicro * (scaleReceipts + 1),
		IdempotencyKey: "grant:usage-scale", Provider: "yscale", ProviderAccountID: "internal",
		PaymentObjectID: "seed:usage-scale",
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < scaleReceipts; i++ {
		workloadID := fmt.Sprintf("wl_usage_scale_%04d", i)
		burstID := fmt.Sprintf("burst_usage_scale_%04d", i)
		now := time.Now().UTC().Truncate(time.Microsecond)
		hold, err := billingStore.ReserveCredit(ctx, billing.ReservationRequest{
			CustomerID: scaleCustomer, WorkloadRef: workloadID,
			IdempotencyKey: "reserve:" + workloadID, ExpiresAt: now.Add(2 * time.Hour),
			PriceQuote: billing.PriceQuote{
				QuoteID: "quote:" + workloadID, PricingVersion: 1, Currency: "USD",
				Provider: scaleBackend, SKU: "rtx4000ada", Region: "us-east",
				ProviderRateMicroUSDPerHour: 1_000, CustomerRateMicroUSDPerHour: 1_000,
				MaximumDurationSeconds: 3600, MaximumChargeMicroUSD: scaleReserveMicro,
				IssuedAt: now, ValidUntil: now.Add(10 * time.Minute),
			},
		})
		if err != nil {
			t.Fatalf("reserve %s: %v", workloadID, err)
		}
		if err := billingStore.CaptureHold(ctx, scaleCustomer, hold.ID, scaleCaptureMicro,
			"capture:"+workloadID, burstID); err != nil {
			t.Fatalf("capture %s: %v", workloadID, err)
		}
		if err := workloadStore.PutWorkloadDurable(&state.Workload{
			ID: workloadID, CustomerID: scaleCustomer, BurstID: burstID,
			Billing: &state.WorkloadBilling{
				HoldID: hold.ID, WorkloadRef: workloadID, ReservedMicroUSD: scaleReserveMicro,
				AuthoritativeUsageRequired: true,
			},
			Cost: &state.WorkloadCost{
				BurstID: burstID, Backend: scaleBackend,
				Basis:        state.WorkloadCostBasisRateRuntimeToProviderDelete,
				EstimatedUSD: scaleEstimatedUSD, HourlyUSD: scaleHourlyUSD,
			},
		}); err != nil {
			t.Fatalf("persist %s: %v", workloadID, err)
		}
	}

	source := durableUsageReceiptSource{store: workloadStore}
	receipts, err := source.UsageReceiptSnapshot(ctx)
	if err != nil {
		t.Fatalf("complete receipt snapshot: %v", err)
	}
	if len(receipts) != scaleReceipts {
		t.Fatalf("complete receipt snapshot returned %d receipts, want %d; the lifetime ceiling is back",
			len(receipts), scaleReceipts)
	}

	usage, err := billingStore.ReconcileUsageCaptures(ctx, source)
	if err != nil {
		t.Fatalf("usage reconciliation over %d matched receipts: %v", scaleReceipts, err)
	}
	if usage.TotalDifferences() != 0 || usage.NonAuthoritative != 0 || usage.LegacyNonAuthoritative != 0 {
		t.Fatalf("usage reconciliation over %d matched receipts = %+v, want zero differences",
			scaleReceipts, usage)
	}

	// The paid gate is the thing the ceiling used to close. Drive the real
	// monitor and prove it opens on row counts past the retired ceiling.
	gate := newTestGate()
	monitor := newBillingReconcileMonitor(billingStore, gate, cost.NewMeter(), nil, time.Minute,
		// Internal yscale seed grants are intentionally absent from the external
		// cash snapshot; only provider-owned cash objects belong in this seam.
		scaleCashProvider{}, source)
	if !prereqMissing(t, gate, "billing_reconciliation") {
		t.Fatal("billing reconciliation should start closed")
	}
	monitor.runOnce(ctx)
	if prereqMissing(t, gate, "billing_reconciliation") {
		t.Fatalf("paid admission stayed closed after a clean pass over %d paid workloads", scaleReceipts)
	}
}
