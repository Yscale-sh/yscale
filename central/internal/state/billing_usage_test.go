package state

import (
	"context"
	"testing"
)

func TestBillingUsageReceiptsProjectsAndBoundsInMemoryState(t *testing.T) {
	store := New()
	store.PutWorkload(&Workload{ID: "wl_none", CustomerID: "tenant-one"})
	store.PutWorkload(&Workload{
		ID: "wl_billed", CustomerID: "tenant-one", BurstID: "burst-one",
		Billing: &WorkloadBilling{HoldID: 7, WorkloadRef: "wl_billed", ReservedMicroUSD: 900, AuthoritativeUsageRequired: true},
		Cost: &WorkloadCost{BurstID: "burst-one", Backend: "linode", Basis: WorkloadCostBasisRateRuntimeToProviderDelete,
			EstimatedUSD: 0.0005, HourlyUSD: 0.9},
	})
	receipts, durable, err := store.BillingUsageReceipts(context.Background(), 10)
	if err != nil || durable || len(receipts) != 1 {
		t.Fatalf("receipts=%+v durable=%v err=%v", receipts, durable, err)
	}
	got := receipts[0]
	if got.WorkloadID != "wl_billed" || got.HoldID != 7 || !got.CostPresent ||
		got.CostBurstID != "burst-one" || got.Basis != WorkloadCostBasisRateRuntimeToProviderDelete ||
		!got.AuthoritativeUsageRequired {
		t.Fatalf("receipt projection=%+v", got)
	}
	if _, _, err := store.BillingUsageReceipts(context.Background(), 0); err == nil {
		t.Fatal("zero limit accepted")
	}
	if _, _, err := store.BillingUsageReceipts(context.Background(), 1001); err == nil {
		t.Fatal("oversized limit accepted")
	}
	if _, _, err := store.BillingUsageReceipts(context.Background(), 1); err != nil {
		t.Fatalf("exact bounded snapshot: %v", err)
	}
	store.PutWorkload(&Workload{ID: "wl_billed_two", CustomerID: "tenant-one",
		Billing: &WorkloadBilling{HoldID: 8, WorkloadRef: "wl_billed_two", ReservedMicroUSD: 1}})
	if _, _, err := store.BillingUsageReceipts(context.Background(), 1); err == nil {
		t.Fatal("partial in-memory snapshot accepted")
	}
}
