package main

import (
	"os"
	"strings"
	"testing"
)

func TestBillingRuntimeIsExplicitSeparateAndDoesNotRunDDL(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{"BILLING_DATABASE_URL", "BILLING_LIVE_MODE", "billing.OpenStore"} {
		if !strings.Contains(text, required) {
			t.Fatalf("billing runtime wiring missing %q", required)
		}
	}
	if strings.Contains(text, "billing.EnsureSchema") {
		t.Fatal("production runtime calls billing.EnsureSchema")
	}
}

func TestBillingConfigFailsClosedWhenLiveModeHasNoDatabase(t *testing.T) {
	t.Setenv("BILLING_DATABASE_URL", "")
	t.Setenv("BILLING_LIVE_MODE", "true")
	if _, _, _, err := billingConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "requires BILLING_DATABASE_URL") {
		t.Fatalf("billing config error = %v, want missing database refusal", err)
	}
}

func TestBillingConfigExplicitEnableAndDisabledDefaults(t *testing.T) {
	t.Setenv("BILLING_DATABASE_URL", "")
	t.Setenv("BILLING_LIVE_MODE", "")
	if _, _, enabled, err := billingConfigFromEnv(); err != nil || enabled {
		t.Fatalf("unset billing = enabled %v err %v", enabled, err)
	}
	t.Setenv("BILLING_DATABASE_URL", "postgres://billing.example/db")
	t.Setenv("BILLING_LIVE_MODE", "true")
	dsn, live, enabled, err := billingConfigFromEnv()
	if err != nil || !enabled || !live || dsn != "postgres://billing.example/db" {
		t.Fatalf("enabled billing = (%q,%v,%v,%v)", dsn, live, enabled, err)
	}
}

func TestPrepaidBurstBillingConfigIsExplicitAndFailsClosed(t *testing.T) {
	t.Setenv("BILLING_ENFORCE_PREPAID_BURSTS", "")
	if enabled, err := prepaidBurstBillingFromEnv(false); err != nil || enabled {
		t.Fatalf("unset prepaid enforcement = %v, %v", enabled, err)
	}
	t.Setenv("BILLING_ENFORCE_PREPAID_BURSTS", "true")
	if _, err := prepaidBurstBillingFromEnv(false); err == nil {
		t.Fatal("prepaid enforcement accepted missing billing store")
	}
	if enabled, err := prepaidBurstBillingFromEnv(true); err != nil || !enabled {
		t.Fatalf("configured prepaid enforcement = %v, %v", enabled, err)
	}
	t.Setenv("BILLING_ENFORCE_PREPAID_BURSTS", "yes")
	if _, err := prepaidBurstBillingFromEnv(true); err == nil {
		t.Fatal("invalid prepaid enforcement value accepted")
	}
}
