// yscale:proprietary

package billingcanary

import (
	"errors"
	"strings"
	"testing"
)

func baseEnv() map[string]string {
	return map[string]string{
		"BILLING_TEST_DATABASE_URL":                 "postgres://user@localhost/example_billing_test?sslmode=disable",
		"BILLING_TEST_ALLOW_DESTRUCTIVE":            AllowDestructiveSentinel,
		"BILLING_TEST_EXPECT_DATABASE":              "example_billing_test",
		"STRIPE_SECRET_KEY":                         "sk_test_deadbeef",
		"STRIPE_WEBHOOK_SECRET":                     "whsec_abc",
		"STRIPE_ACCOUNT_ID":                         "acct_123",
		"BILLING_CHECKOUT_SUCCESS_URL":              "https://example.test/ok",
		"BILLING_CHECKOUT_CANCEL_URL":               "https://example.test/cancel",
		"YSCALE_STRIPE_CANARY_ALLOW_SHARED_ACCOUNT": AllowSharedAccountSentinel,
		"YSCALE_STRIPE_CANARY_EVIDENCE_PATH":        "/tmp/canary-evidence.json",
	}
}

func envFunc(m map[string]string) func(string) string {
	return func(key string) string { return m[key] }
}

func TestLoadConfigAcceptsFullTestModeEnvironment(t *testing.T) {
	cfg, err := LoadConfig(envFunc(baseEnv()))
	if err != nil {
		t.Fatalf("LoadConfig = %v", err)
	}
	if cfg.LiveScenarioEnabled {
		t.Errorf("live flow must default to disabled")
	}
	if cfg.CheckoutAmountMicroUSD == 0 || cfg.CheckoutAmountMicroUSD < defaultMinMicroUSD {
		t.Errorf("default checkout amount = %d, want >= min", cfg.CheckoutAmountMicroUSD)
	}
	if cfg.EvidencePath != "/tmp/canary-evidence.json" {
		t.Errorf("evidence path = %q", cfg.EvidencePath)
	}
}

func TestLoadConfigRejectsMissingOrWrongSentinels(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{"missing dsn", func(m map[string]string) { delete(m, "BILLING_TEST_DATABASE_URL") }},
		{"missing destructive sentinel", func(m map[string]string) { m["BILLING_TEST_ALLOW_DESTRUCTIVE"] = "yes" }},
		{"wrong destructive sentinel", func(m map[string]string) { m["BILLING_TEST_ALLOW_DESTRUCTIVE"] = "I_understand" }},
		{"missing expect database", func(m map[string]string) { delete(m, "BILLING_TEST_EXPECT_DATABASE") }},
		{"expect database wrong suffix", func(m map[string]string) { m["BILLING_TEST_EXPECT_DATABASE"] = "example_prod" }},
		{"live secret key", func(m map[string]string) { m["STRIPE_SECRET_KEY"] = "sk_live_xxx" }},
		{"missing webhook secret", func(m map[string]string) { m["STRIPE_WEBHOOK_SECRET"] = "not-a-secret" }},
		{"missing account id", func(m map[string]string) { m["STRIPE_ACCOUNT_ID"] = "" }},
		{"success url not https", func(m map[string]string) { m["BILLING_CHECKOUT_SUCCESS_URL"] = "http://oops" }},
		{"cancel url with userinfo", func(m map[string]string) { m["BILLING_CHECKOUT_CANCEL_URL"] = "https://user@example.test/x" }},
		{"missing shared account sentinel", func(m map[string]string) { delete(m, "YSCALE_STRIPE_CANARY_ALLOW_SHARED_ACCOUNT") }},
		{"relative evidence path", func(m map[string]string) { m["YSCALE_STRIPE_CANARY_EVIDENCE_PATH"] = "evidence.json" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := baseEnv()
			tc.mutate(env)
			_, err := LoadConfig(envFunc(env))
			if !errors.Is(err, ErrGuardRejected) {
				t.Fatalf("LoadConfig(%s) err = %v, want ErrGuardRejected", tc.name, err)
			}
		})
	}
}

func TestLoadConfigLiveFlowRequiresControlDirectory(t *testing.T) {
	env := baseEnv()
	env["YSCALE_STRIPE_CANARY_LIVE"] = AllowLiveScenarioSentinel
	if _, err := LoadConfig(envFunc(env)); !errors.Is(err, ErrGuardRejected) {
		t.Fatalf("LoadConfig without control dir err = %v", err)
	}
	env["YSCALE_STRIPE_CANARY_CONTROL_DIR"] = "relative/path"
	if _, err := LoadConfig(envFunc(env)); !errors.Is(err, ErrGuardRejected) {
		t.Fatalf("LoadConfig relative control dir err = %v", err)
	}
	env["YSCALE_STRIPE_CANARY_CONTROL_DIR"] = "/tmp/canary-control"
	if _, err := LoadConfig(envFunc(env)); !errors.Is(err, ErrGuardRejected) {
		t.Fatalf("LoadConfig without webhook addr err = %v", err)
	}
	env["YSCALE_STRIPE_CANARY_WEBHOOK_ADDR"] = "0.0.0.0:9000"
	if _, err := LoadConfig(envFunc(env)); !errors.Is(err, ErrGuardRejected) {
		t.Fatalf("LoadConfig non-loopback addr err = %v", err)
	}
	env["YSCALE_STRIPE_CANARY_WEBHOOK_ADDR"] = "127.0.0.1:38234"
	cfg, err := LoadConfig(envFunc(env))
	if err != nil {
		t.Fatalf("LoadConfig live-enabled err = %v", err)
	}
	if !cfg.LiveScenarioEnabled {
		t.Fatalf("LiveScenarioEnabled = false, want true")
	}
	if cfg.ControlDir != "/tmp/canary-control" {
		t.Errorf("control dir = %q", cfg.ControlDir)
	}
	if cfg.WebhookListenAddr != "127.0.0.1:38234" {
		t.Errorf("webhook addr = %q", cfg.WebhookListenAddr)
	}
}

func TestLoadConfigMessageNeverEchoesSecretPrefixes(t *testing.T) {
	env := baseEnv()
	env["STRIPE_SECRET_KEY"] = "sk_live_leaky_prefix_should_never_appear"
	_, err := LoadConfig(envFunc(env))
	if err == nil {
		t.Fatal("LoadConfig accepted a live secret key")
	}
	if strings.Contains(err.Error(), "sk_live_") {
		t.Fatalf("error message echoes secret prefix: %v", err)
	}
}
