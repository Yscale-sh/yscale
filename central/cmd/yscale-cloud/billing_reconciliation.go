// yscale:proprietary

package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
)

const billingReconcileAttemptTimeout = 15 * time.Second

type billingReconcileMonitor struct {
	mu             sync.Mutex
	gate           *handlers.PaidGate
	meter          *cost.Meter
	log            *slog.Logger
	reconcile      func(context.Context) (billing.ReconciliationSummary, error)
	now            func() time.Time
	interval       time.Duration
	attemptTimeout time.Duration
	staleThreshold time.Duration
	lastSuccess    time.Time
	everSucceeded  bool
	stale          bool
}

func newBillingReconcileMonitor(store *billing.Store, gate *handlers.PaidGate, meter *cost.Meter, log *slog.Logger, interval time.Duration, cashProvider billing.ExternalCashProvider, usageSource billing.UsageReceiptSource) *billingReconcileMonitor {
	if interval <= 0 {
		interval = time.Minute
	}
	return &billingReconcileMonitor{
		gate: gate, meter: meter, log: log, reconcile: func(ctx context.Context) (billing.ReconciliationSummary, error) {
			summary, err := store.ReconcileLedger(ctx)
			if err != nil {
				return billing.ReconciliationSummary{}, err
			}
			summary.ExternalCash, err = store.ReconcileExternalCash(ctx, cashProvider)
			if err != nil {
				return summary, err
			}
			summary.UsageCapture, err = store.ReconcileUsageCaptures(ctx, usageSource)
			return summary, err
		},
		now: time.Now, interval: interval, attemptTimeout: billingReconcileAttemptTimeout,
		staleThreshold: effectiveStaleThreshold(interval, billingReconcileAttemptTimeout),
	}
}

func (m *billingReconcileMonitor) runOnce(ctx context.Context) {
	attemptCtx, cancel := context.WithTimeout(ctx, m.attemptTimeout)
	defer cancel()
	summary, err := m.reconcile(attemptCtx)
	m.observe(summary, err)
}

func (m *billingReconcileMonitor) observe(summary billing.ReconciliationSummary, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := "healthy"
	if err != nil {
		result = "error"
	} else if !summary.Healthy() {
		result = "difference"
	}
	m.meter.RecordBillingReconciliation(result, summary.AccountBalances, summary.HeldBalances,
		summary.FundingReversals, summary.Operations, summary.EconomicObjects,
		summary.ExternalCash.TotalDifferences(), summary.UsageCapture.TotalDifferences())
	if result != "healthy" {
		if m.gate != nil {
			m.gate.ClearReady(handlers.PrereqBillingReconciliation)
		}
		if m.log != nil {
			m.log.Error("billing ledger reconciliation failed closed", "result", result, "error", err,
				"account_balance_differences", summary.AccountBalances,
				"held_balance_differences", summary.HeldBalances,
				"funding_reversal_differences", summary.FundingReversals,
				"operation_ledger_differences", summary.Operations,
				"economic_object_differences", summary.EconomicObjects,
				"external_cash_provider_only", summary.ExternalCash.ProviderOnly,
				"external_cash_ledger_only", summary.ExternalCash.LedgerOnly,
				"external_cash_identity_differences", summary.ExternalCash.Identity,
				"external_cash_amount_differences", summary.ExternalCash.Amount,
				"usage_hold_missing", summary.UsageCapture.HoldMissing,
				"usage_missing", summary.UsageCapture.UsageMissing,
				"usage_association_differences", summary.UsageCapture.Association,
				"usage_non_authoritative", summary.UsageCapture.NonAuthoritative,
				"usage_legacy_non_authoritative", summary.UsageCapture.LegacyNonAuthoritative,
				"usage_capture_amount_differences", summary.UsageCapture.CaptureAmount,
				"usage_unsettled", summary.UsageCapture.Unsettled)
		}
		return
	}
	m.lastSuccess = m.now()
	m.everSucceeded = true
	m.stale = false
	if m.gate != nil {
		m.gate.SetReady(handlers.PrereqBillingReconciliation)
	}
}

func (m *billingReconcileMonitor) checkStaleness() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.everSucceeded || m.stale || m.now().Sub(m.lastSuccess) <= m.staleThreshold {
		return
	}
	m.stale = true
	if m.gate != nil {
		m.gate.ClearReady(handlers.PrereqBillingReconciliation)
	}
	m.meter.MarkBillingReconciliationStale()
	if m.log != nil {
		m.log.Error("billing ledger reconciliation stale; paid admission closed",
			"last_success", m.lastSuccess, "stale_threshold", m.staleThreshold)
	}
}

func (m *billingReconcileMonitor) run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runOnce(ctx)
		}
	}
}

func (m *billingReconcileMonitor) runWatchdog(ctx context.Context) {
	cadence := watchdogCadence(m.interval, m.attemptTimeout)
	ticker := time.NewTicker(cadence)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkStaleness()
		}
	}
}

func startBillingReconciliation(ctx context.Context, store *billing.Store, gate *handlers.PaidGate, meter *cost.Meter, log *slog.Logger, cashProvider billing.ExternalCashProvider, usageSource billing.UsageReceiptSource) {
	interval := envDuration("YSCALE_BILLING_RECONCILE_INTERVAL", time.Minute)
	if interval <= 0 {
		interval = time.Minute
	}
	if gate != nil {
		meter.EnableBillingReconciliation()
	}
	monitor := newBillingReconcileMonitor(store, gate, meter, log, interval, cashProvider, usageSource)
	monitor.runOnce(ctx) // boot proof completes before the HTTP server starts
	go monitor.run(ctx)
	go monitor.runWatchdog(ctx)
	if log != nil {
		log.Info("billing ledger reconciliation worker started", "interval", interval,
			"attempt_timeout", billingReconcileAttemptTimeout, "stale_threshold", monitor.staleThreshold)
	}
}

type durableUsageReceiptSource struct {
	store *state.Store
}

// UsageReceiptSnapshot returns the complete durable receipt snapshot. It takes
// no limit on purpose: managed reconciliation must never compare a truncated
// workload view against the full billing hold set, and terminal receipts are
// retained, so any total limit would eventually close paid admission on row
// count alone. The state store pages internally with bounded queries.
func (s durableUsageReceiptSource) UsageReceiptSnapshot(ctx context.Context) ([]billing.UsageReceipt, error) {
	if s.store == nil {
		return nil, fmt.Errorf("durable workload state is unavailable")
	}
	receipts, durable, err := s.store.BillingUsageReceiptSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	if !durable {
		return nil, fmt.Errorf("durable workload state is unavailable")
	}
	out := make([]billing.UsageReceipt, 0, len(receipts))
	for _, receipt := range receipts {
		out = append(out, billing.UsageReceipt{
			CustomerID: receipt.CustomerID, WorkloadID: receipt.WorkloadID,
			WorkloadRef: receipt.WorkloadRef, HoldID: receipt.HoldID,
			ReservedMicroUSD: receipt.ReservedMicroUSD, ManualAttention: receipt.ManualAttention,
			AuthoritativeUsageRequired: receipt.AuthoritativeUsageRequired,
			BurstID:                    receipt.BurstID, CostPresent: receipt.CostPresent,
			CostBurstID: receipt.CostBurstID, Backend: receipt.Backend,
			Authoritative: receipt.Basis == state.WorkloadCostBasisRateRuntimeToProviderDelete,
			EstimatedUSD:  receipt.EstimatedUSD, HourlyUSD: receipt.HourlyUSD,
		})
	}
	return out, nil
}
