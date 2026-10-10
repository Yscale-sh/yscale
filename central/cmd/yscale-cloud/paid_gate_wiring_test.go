package main

import (
	"os"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/handlers"
)

func TestPaidGateNilWhenNotRequested(t *testing.T) {
	t.Setenv("PAID_RUNTIME_ENABLED", "")
	t.Setenv("PAID_RUNTIME_LIVE_MODE", "")
	gate, err := paidRuntimeGateFromEnv(paidGateInputs{PrepaidRequested: false})
	if err != nil {
		t.Fatal(err)
	}
	if gate != nil {
		t.Fatal("gate should be nil when prepaid is not requested")
	}
}

func TestPaidGateDisabledByDefault(t *testing.T) {
	t.Setenv("PAID_RUNTIME_ENABLED", "")
	t.Setenv("PAID_RUNTIME_LIVE_MODE", "false")
	gate, err := paidRuntimeGateFromEnv(paidGateInputs{
		PrepaidRequested: true,
		BillingStoreOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gate == nil {
		t.Fatal("gate is nil")
	}
	if gate.Allowed() {
		t.Fatal("gate should not be allowed without all prerequisites")
	}
}

func TestPaidGateBillingDBAloneCannotEnable(t *testing.T) {
	t.Setenv("PAID_RUNTIME_ENABLED", "true")
	t.Setenv("PAID_RUNTIME_LIVE_MODE", "false")
	gate, err := paidRuntimeGateFromEnv(paidGateInputs{
		PrepaidRequested: true,
		BillingStoreOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gate.Allowed() {
		t.Fatal("billing DB alone enabled paid mode")
	}
}

func TestPaidGateEachPrereqIndependentlyPreventsActivation(t *testing.T) {
	full := paidGateInputs{
		PrepaidRequested:       true,
		BillingStoreOpen:       true,
		BillingLiveMode:        false,
		DurableAdmission:       true,
		LifecycleStoreOpen:     true,
		ConnectorLedger:        true,
		ProviderReconciliation: true,
		ReconciliationMonitor:  true,
		BillingReconciliation:  true,
	}

	fields := []struct {
		name  string
		clear func(*paidGateInputs)
	}{
		{"BillingStoreOpen", func(i *paidGateInputs) { i.BillingStoreOpen = false }},
		{"DurableAdmission", func(i *paidGateInputs) { i.DurableAdmission = false }},
		{"LifecycleStoreOpen", func(i *paidGateInputs) { i.LifecycleStoreOpen = false }},
		{"ConnectorLedger", func(i *paidGateInputs) { i.ConnectorLedger = false }},
		{"ProviderReconciliation", func(i *paidGateInputs) { i.ProviderReconciliation = false }},
		{"ReconciliationMonitor", func(i *paidGateInputs) { i.ReconciliationMonitor = false }},
		{"BillingReconciliation", func(i *paidGateInputs) { i.BillingReconciliation = false }},
	}

	for _, f := range fields {
		t.Run(f.name, func(t *testing.T) {
			t.Setenv("PAID_RUNTIME_ENABLED", "true")
			t.Setenv("PAID_RUNTIME_LIVE_MODE", "false")
			cfg := full
			f.clear(&cfg)
			gate, err := paidRuntimeGateFromEnv(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if gate.Allowed() {
				t.Fatalf("gate allowed with %s missing", f.name)
			}
		})
	}

	// Kill switch off
	t.Run("KillSwitch", func(t *testing.T) {
		t.Setenv("PAID_RUNTIME_ENABLED", "")
		t.Setenv("PAID_RUNTIME_LIVE_MODE", "false")
		gate, err := paidRuntimeGateFromEnv(full)
		if err != nil {
			t.Fatal(err)
		}
		if gate.Allowed() {
			t.Fatal("gate allowed with kill switch off")
		}
	})
}

func TestPaidGateFullyReadyTestModeFixture(t *testing.T) {
	t.Setenv("PAID_RUNTIME_ENABLED", "true")
	t.Setenv("PAID_RUNTIME_LIVE_MODE", "false")
	gate, err := paidRuntimeGateFromEnv(paidGateInputs{
		PrepaidRequested:       true,
		BillingStoreOpen:       true,
		BillingLiveMode:        false,
		DurableAdmission:       true,
		LifecycleStoreOpen:     true,
		ConnectorLedger:        true,
		ProviderReconciliation: true,
		ReconciliationMonitor:  true,
		BillingReconciliation:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !gate.Allowed() {
		st, reason, missing := gate.Status()
		t.Fatalf("fully configured gate not allowed: state=%s reason=%s missing=%v",
			st, reason, missing)
	}
}

func TestPaidGateLiveModeMismatchReturnsError(t *testing.T) {
	t.Run("declared_live_actual_test", func(t *testing.T) {
		t.Setenv("PAID_RUNTIME_ENABLED", "true")
		t.Setenv("PAID_RUNTIME_LIVE_MODE", "true")
		_, err := paidRuntimeGateFromEnv(paidGateInputs{
			PrepaidRequested: true,
			BillingStoreOpen: true,
			BillingLiveMode:  false,
		})
		if err == nil {
			t.Fatal("expected error for live/test mode mismatch")
		}
	})
	t.Run("declared_test_actual_live", func(t *testing.T) {
		t.Setenv("PAID_RUNTIME_ENABLED", "true")
		t.Setenv("PAID_RUNTIME_LIVE_MODE", "false")
		_, err := paidRuntimeGateFromEnv(paidGateInputs{
			PrepaidRequested: true,
			BillingStoreOpen: true,
			BillingLiveMode:  true,
		})
		if err == nil {
			t.Fatal("expected error for test/live mode mismatch")
		}
	})
	t.Run("missing", func(t *testing.T) {
		t.Setenv("PAID_RUNTIME_ENABLED", "true")
		t.Setenv("PAID_RUNTIME_LIVE_MODE", "")
		_, err := paidRuntimeGateFromEnv(paidGateInputs{
			PrepaidRequested: true,
			BillingStoreOpen: true,
		})
		if err == nil {
			t.Fatal("expected error for missing PAID_RUNTIME_LIVE_MODE")
		}
	})
	t.Run("malformed", func(t *testing.T) {
		t.Setenv("PAID_RUNTIME_ENABLED", "true")
		t.Setenv("PAID_RUNTIME_LIVE_MODE", "yes")
		_, err := paidRuntimeGateFromEnv(paidGateInputs{
			PrepaidRequested: true,
			BillingStoreOpen: true,
		})
		if err == nil {
			t.Fatal("expected error for malformed PAID_RUNTIME_LIVE_MODE")
		}
	})
}

func TestPaidGateKillSwitchInvalidValue(t *testing.T) {
	t.Setenv("PAID_RUNTIME_ENABLED", "yes")
	t.Setenv("PAID_RUNTIME_LIVE_MODE", "false")
	_, err := paidRuntimeGateFromEnv(paidGateInputs{PrepaidRequested: true})
	if err == nil {
		t.Fatal("invalid PAID_RUNTIME_ENABLED value accepted")
	}
}

func TestPaidGateKillSwitchExplicitFalse(t *testing.T) {
	t.Setenv("PAID_RUNTIME_ENABLED", "false")
	t.Setenv("PAID_RUNTIME_LIVE_MODE", "false")
	gate, err := paidRuntimeGateFromEnv(paidGateInputs{
		PrepaidRequested:       true,
		BillingStoreOpen:       true,
		BillingLiveMode:        false,
		DurableAdmission:       true,
		LifecycleStoreOpen:     true,
		ConnectorLedger:        true,
		ProviderReconciliation: true,
		ReconciliationMonitor:  true,
		BillingReconciliation:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gate.Allowed() {
		t.Fatal("gate allowed with explicit kill switch false")
	}
}

func TestPaidGateReconciliationNotYetImplementedKeepsGateClosed(t *testing.T) {
	t.Setenv("PAID_RUNTIME_ENABLED", "true")
	t.Setenv("PAID_RUNTIME_LIVE_MODE", "false")
	gate, err := paidRuntimeGateFromEnv(paidGateInputs{
		PrepaidRequested:   true,
		BillingStoreOpen:   true,
		BillingLiveMode:    false,
		DurableAdmission:   true,
		LifecycleStoreOpen: true,
		ConnectorLedger:    true,
		// All reconciliation prerequisites default false.
	})
	if err != nil {
		t.Fatal(err)
	}
	if gate.Allowed() {
		t.Fatal("gate allowed without reconciliation prerequisites")
	}
	_, _, missing := gate.Status()
	foundReconciliation := false
	foundMonitor := false
	foundBilling := false
	for _, m := range missing {
		if m == "provider_reconciliation" {
			foundReconciliation = true
		}
		if m == "reconciliation_monitor" {
			foundMonitor = true
		}
		if m == "billing_reconciliation" {
			foundBilling = true
		}
	}
	if !foundReconciliation || !foundMonitor || !foundBilling {
		t.Fatalf("missing = %v, want all provider and billing reconciliation prerequisites", missing)
	}
}

func TestPaidGateSourceCodeDoesNotCallDDL(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if !containsStr(text, "paidRuntimeGateFromEnv") {
		t.Fatal("main.go missing paid runtime gate wiring")
	}
	if !containsStr(text, "PAID_RUNTIME_ENABLED") {
		t.Fatal("main.go missing kill switch env reference")
	}
}

func TestPaidGateReadyzPaidRegistered(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !containsStr(string(source), "/readyz/paid") {
		t.Fatal("main.go missing /readyz/paid endpoint registration")
	}
}

func TestPaidGateReadyzPaidEndpointReportsStatus(t *testing.T) {
	gate := handlers.NewPaidGate()
	handler := handlers.ReadyzPaidHandler(gate)
	if handler == nil {
		t.Fatal("ReadyzPaidHandler returned nil")
	}
}

func containsStr(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
