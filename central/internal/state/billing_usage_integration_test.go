//go:build integration

package state

import (
	"context"
	"os"
	"testing"
)

func TestPostgresBillingUsageReceipts(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	store, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	defer store.Close()
	workload := &Workload{
		ID: "wl_billing_usage_receipt", CustomerID: "tenant-billing-usage", BurstID: "burst-billing-usage",
		Billing: &WorkloadBilling{HoldID: 9001, WorkloadRef: "wl_billing_usage_receipt", ReservedMicroUSD: 750_000},
		Cost: &WorkloadCost{BurstID: "burst-billing-usage", Backend: "linode",
			Basis: WorkloadCostBasisRateRuntimeToProviderDelete, EstimatedUSD: 0.25, HourlyUSD: 0.75},
	}
	if err := store.PutWorkloadDurable(workload); err != nil {
		t.Fatalf("persist workload: %v", err)
	}
	receipts, durable, err := store.BillingUsageReceipts(ctx, 1000)
	if err != nil || !durable {
		t.Fatalf("durable receipts=%+v durable=%v err=%v", receipts, durable, err)
	}
	for _, receipt := range receipts {
		if receipt.WorkloadID == workload.ID {
			if !receipt.CostPresent || receipt.HoldID != 9001 || receipt.CostBurstID != workload.BurstID {
				t.Fatalf("durable receipt projection=%+v", receipt)
			}
			return
		}
	}
	t.Fatalf("durable receipt for %q not found", workload.ID)
}
