// yscale:proprietary

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/state"
)

func newBillingMonitorAt(t *testing.T, stale time.Duration, now *time.Time) *billingReconcileMonitor {
	t.Helper()
	return &billingReconcileMonitor{
		gate: newTestGate(), now: func() time.Time { return *now },
		staleThreshold: stale,
	}
}

func TestDurableUsageReceiptSourceRejectsInMemoryState(t *testing.T) {
	source := durableUsageReceiptSource{store: state.New()}
	if _, err := source.UsageReceiptSnapshot(context.Background()); err == nil {
		t.Fatal("in-memory state accepted as durable usage source")
	}
	if _, err := (durableUsageReceiptSource{}).UsageReceiptSnapshot(context.Background()); err == nil {
		t.Fatal("absent workload state accepted as durable usage source")
	}
}

type scaleUsageSource struct{ receipts []billing.UsageReceipt }

func (s scaleUsageSource) UsageReceiptSnapshot(context.Context) ([]billing.UsageReceipt, error) {
	return append([]billing.UsageReceipt(nil), s.receipts...), nil
}

// Row count is not a health signal. A complete snapshot well past the retired
// 1000-row lifetime ceiling with no differences has to leave paid admission
// open; the old behaviour errored on the count alone and closed the gate.
func TestBillingReconcileStaysReadyPastTheRetiredRowCeiling(t *testing.T) {
	receipts := make([]billing.UsageReceipt, 0, 1001)
	for i := 0; i < 1001; i++ {
		receipts = append(receipts, billing.UsageReceipt{
			CustomerID: "tenant-scale", WorkloadID: "wl", HoldID: int64(i + 1), ReservedMicroUSD: 1_000,
		})
	}
	source := scaleUsageSource{receipts: receipts}
	now := time.Unix(1_700_000_000, 0)
	m := newBillingMonitorAt(t, time.Minute, &now)
	m.meter = cost.NewMeter()
	m.attemptTimeout = time.Minute
	m.reconcile = func(ctx context.Context) (billing.ReconciliationSummary, error) {
		snapshot, err := source.UsageReceiptSnapshot(ctx)
		if err != nil {
			return billing.ReconciliationSummary{}, err
		}
		if len(snapshot) != len(receipts) {
			return billing.ReconciliationSummary{}, errors.New("snapshot truncated")
		}
		return billing.ReconciliationSummary{}, nil
	}
	m.runOnce(context.Background())
	if prereqMissing(t, m.gate, "billing_reconciliation") {
		t.Fatalf("paid admission closed after a clean pass over %d receipts", len(receipts))
	}
}

func TestBillingReconcileHealthyPassOpensGate(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	m := newBillingMonitorAt(t, time.Minute, &now)
	if !prereqMissing(t, m.gate, "billing_reconciliation") {
		t.Fatal("billing reconciliation should start closed")
	}
	m.observe(billing.ReconciliationSummary{}, nil)
	if prereqMissing(t, m.gate, "billing_reconciliation") {
		t.Fatal("zero-difference pass did not open billing reconciliation")
	}
}

func TestBillingReconcileDifferenceAndErrorCloseGate(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	m := newBillingMonitorAt(t, time.Minute, &now)
	m.observe(billing.ReconciliationSummary{}, nil)
	m.observe(billing.ReconciliationSummary{AccountBalances: 1}, nil)
	if !prereqMissing(t, m.gate, "billing_reconciliation") {
		t.Fatal("materialization difference did not close billing reconciliation")
	}
	m.observe(billing.ReconciliationSummary{}, nil)
	m.observe(billing.ReconciliationSummary{ExternalCash: billing.ExternalCashSummary{Amount: 1}}, nil)
	if !prereqMissing(t, m.gate, "billing_reconciliation") {
		t.Fatal("external cash difference did not close billing reconciliation")
	}
	m.observe(billing.ReconciliationSummary{}, nil)
	m.observe(billing.ReconciliationSummary{UsageCapture: billing.UsageCaptureSummary{CaptureAmount: 1}}, nil)
	if !prereqMissing(t, m.gate, "billing_reconciliation") {
		t.Fatal("usage capture difference did not close billing reconciliation")
	}
	m.observe(billing.ReconciliationSummary{}, nil)
	m.observe(billing.ReconciliationSummary{}, errors.New("database unavailable"))
	if !prereqMissing(t, m.gate, "billing_reconciliation") {
		t.Fatal("query error did not close billing reconciliation")
	}
}

func TestBillingReconcileStalenessClosesAndRecoveryReopens(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	m := newBillingMonitorAt(t, time.Minute, &now)
	m.observe(billing.ReconciliationSummary{}, nil)
	now = now.Add(time.Minute + time.Second)
	m.checkStaleness()
	if !prereqMissing(t, m.gate, "billing_reconciliation") {
		t.Fatal("stale reconciliation did not close gate")
	}
	now = now.Add(time.Second)
	m.observe(billing.ReconciliationSummary{}, nil)
	if prereqMissing(t, m.gate, "billing_reconciliation") {
		t.Fatal("fresh healthy reconciliation did not reopen gate")
	}
}
