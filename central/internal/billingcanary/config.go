// yscale:proprietary

package billingcanary

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Required opt-in and safety sentinels. Callers must supply the exact literal
// values; a typo or a truncation must NOT be interpreted as a request.
const (
	// AllowDestructiveSentinel gates the schema drop the canary performs on the
	// isolated billing test database. Mirrors the existing integration-test
	// wording so operators cannot conflate a canary run with a production DB.
	AllowDestructiveSentinel = "I_UNDERSTAND_DROP_BILLING_SCHEMA"
	// AllowSharedAccountSentinel is the exact literal that opts in to running
	// against a Stripe test account that is not exclusive to this canary run.
	AllowSharedAccountSentinel = "I_UNDERSTAND"
	// AllowLiveScenarioSentinel gates the single live browser-driven flow. The
	// deterministic scenarios run without it; the live scenario requires it AND
	// a caller-supplied control directory.
	AllowLiveScenarioSentinel = "I_UNDERSTAND"
	// TestDatabaseSuffix is the required suffix on the Postgres database name.
	// The canary refuses any database that does not end with it.
	TestDatabaseSuffix = "_billing_test"
	// MaxEvidenceBytes bounds the redacted evidence payload written per run.
	MaxEvidenceBytes = 1 << 20
)

// ErrGuardRejected wraps every guard failure so callers can distinguish an
// intentional refusal from an unrelated runtime error. The wrapped message is
// safe to surface to an operator; it must never contain secret material.
var ErrGuardRejected = errors.New("billingcanary: guard rejected")

// Config is the resolved, validated canary invocation. All fields are the
// product of an explicit environment opt-in; no default enables a scenario.
type Config struct {
	DatabaseDSN            string
	ExpectDatabaseName     string
	StripeSecretKey        string
	StripeWebhookSecret    string
	StripeAccountID        string
	SuccessURL             string
	CancelURL              string
	MinMicroUSD            int64
	MaxMicroUSD            int64
	CheckoutAmountMicroUSD int64
	EvidencePath           string
	ControlDir             string
	LiveScenarioEnabled    bool
	LiveTimeoutSeconds     int
	// WebhookListenAddr binds the test's HTTP server to a fixed loopback
	// address so the wrapper's Stripe CLI listener knows where to forward
	// signed events. Required for live scenarios; empty in the deterministic
	// mode lets httptest pick a random port.
	WebhookListenAddr string
}

// Defaults used only when the operator does not override them. The checkout
// window is pinned at exactly the smallest whole-cent grant the canary ever
// makes, so a compromised or misconfigured wrapper cannot enlarge the amount.
// The live browser wait is 10 minutes: a human has to complete the hosted
// checkout by hand and the timeout must not race a legitimately slow tester.
const (
	defaultMinMicroUSD            int64 = 5_000_000
	defaultMaxMicroUSD            int64 = 5_000_000
	defaultCheckoutAmountMicroUSD int64 = 5_000_000
	defaultLiveTimeoutSeconds           = 600
)

// LoadConfig builds a Config from the caller-supplied environment lookup and
// verifies every guard the canary asserts BEFORE any Postgres or Stripe call.
// getenv is passed in (not os.Getenv called directly) so the guards are
// testable without process mutation, and so the wrapper script can inject a
// process-only environment that is never persisted.
//
// Any failure returns ErrGuardRejected wrapped with a redacted reason.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		return Config{}, fmt.Errorf("%w: no environment lookup provided", ErrGuardRejected)
	}
	dsn := strings.TrimSpace(getenv("BILLING_TEST_DATABASE_URL"))
	if dsn == "" {
		return Config{}, fmt.Errorf("%w: BILLING_TEST_DATABASE_URL is required", ErrGuardRejected)
	}
	if getenv("BILLING_TEST_ALLOW_DESTRUCTIVE") != AllowDestructiveSentinel {
		return Config{}, fmt.Errorf("%w: BILLING_TEST_ALLOW_DESTRUCTIVE must be exactly %q", ErrGuardRejected, AllowDestructiveSentinel)
	}
	expected := strings.TrimSpace(getenv("BILLING_TEST_EXPECT_DATABASE"))
	if expected == "" {
		return Config{}, fmt.Errorf("%w: BILLING_TEST_EXPECT_DATABASE is required", ErrGuardRejected)
	}
	if !strings.HasSuffix(expected, TestDatabaseSuffix) {
		return Config{}, fmt.Errorf("%w: BILLING_TEST_EXPECT_DATABASE must end with %q", ErrGuardRejected, TestDatabaseSuffix)
	}

	secretKey := strings.TrimSpace(getenv("STRIPE_SECRET_KEY"))
	if !strings.HasPrefix(secretKey, "sk_test_") && !strings.HasPrefix(secretKey, "rk_test_") {
		return Config{}, fmt.Errorf("%w: STRIPE_SECRET_KEY must be a Stripe test-mode key", ErrGuardRejected)
	}
	webhookSecret := strings.TrimSpace(getenv("STRIPE_WEBHOOK_SECRET"))
	if !strings.HasPrefix(webhookSecret, "whsec_") {
		return Config{}, fmt.Errorf("%w: STRIPE_WEBHOOK_SECRET must be a whsec_ signing secret", ErrGuardRejected)
	}
	accountID := strings.TrimSpace(getenv("STRIPE_ACCOUNT_ID"))
	if !strings.HasPrefix(accountID, "acct_") {
		return Config{}, fmt.Errorf("%w: STRIPE_ACCOUNT_ID must be a Stripe acct_ identifier", ErrGuardRejected)
	}
	successURL := strings.TrimSpace(getenv("BILLING_CHECKOUT_SUCCESS_URL"))
	cancelURL := strings.TrimSpace(getenv("BILLING_CHECKOUT_CANCEL_URL"))
	for label, raw := range map[string]string{"success URL": successURL, "cancel URL": cancelURL} {
		if err := validateHTTPS(raw); err != nil {
			return Config{}, fmt.Errorf("%w: %s: %v", ErrGuardRejected, label, err)
		}
	}
	if getenv("YSCALE_STRIPE_CANARY_ALLOW_SHARED_ACCOUNT") != AllowSharedAccountSentinel {
		return Config{}, fmt.Errorf("%w: shared Stripe test accounts require YSCALE_STRIPE_CANARY_ALLOW_SHARED_ACCOUNT=%s", ErrGuardRejected, AllowSharedAccountSentinel)
	}

	evidencePath := strings.TrimSpace(getenv("YSCALE_STRIPE_CANARY_EVIDENCE_PATH"))
	if evidencePath == "" {
		return Config{}, fmt.Errorf("%w: YSCALE_STRIPE_CANARY_EVIDENCE_PATH must be a caller-provided temp path", ErrGuardRejected)
	}
	if !filepath.IsAbs(evidencePath) {
		return Config{}, fmt.Errorf("%w: YSCALE_STRIPE_CANARY_EVIDENCE_PATH must be absolute", ErrGuardRejected)
	}

	cfg := Config{
		DatabaseDSN: dsn, ExpectDatabaseName: expected,
		StripeSecretKey: secretKey, StripeWebhookSecret: webhookSecret, StripeAccountID: accountID,
		SuccessURL: successURL, CancelURL: cancelURL,
		MinMicroUSD: defaultMinMicroUSD, MaxMicroUSD: defaultMaxMicroUSD,
		CheckoutAmountMicroUSD: defaultCheckoutAmountMicroUSD,
		EvidencePath:           evidencePath,
		LiveTimeoutSeconds:     defaultLiveTimeoutSeconds,
	}

	if getenv("YSCALE_STRIPE_CANARY_LIVE") == AllowLiveScenarioSentinel {
		cfg.LiveScenarioEnabled = true
		cfg.ControlDir = strings.TrimSpace(getenv("YSCALE_STRIPE_CANARY_CONTROL_DIR"))
		if cfg.ControlDir == "" || !filepath.IsAbs(cfg.ControlDir) {
			return Config{}, fmt.Errorf("%w: live flow requires an absolute YSCALE_STRIPE_CANARY_CONTROL_DIR", ErrGuardRejected)
		}
		cfg.WebhookListenAddr = strings.TrimSpace(getenv("YSCALE_STRIPE_CANARY_WEBHOOK_ADDR"))
		if cfg.WebhookListenAddr == "" {
			return Config{}, fmt.Errorf("%w: live flow requires YSCALE_STRIPE_CANARY_WEBHOOK_ADDR", ErrGuardRejected)
		}
		if !strings.HasPrefix(cfg.WebhookListenAddr, "127.0.0.1:") && !strings.HasPrefix(cfg.WebhookListenAddr, "localhost:") {
			return Config{}, fmt.Errorf("%w: webhook listen addr must bind loopback only", ErrGuardRejected)
		}
	}
	return cfg, nil
}

func validateHTTPS(raw string) error {
	if raw == "" {
		return errors.New("required")
	}
	if len(raw) > 2048 {
		return errors.New("too long")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return errors.New("must be bounded HTTPS without userinfo")
	}
	return nil
}

// VerifyDatabase confirms the connected pool addresses the exact database name
// the operator declared and that the name still ends with the safety suffix.
// It is the last guard before the canary drops the billing schema.
func VerifyDatabase(ctx context.Context, pool *pgxpool.Pool, expected string) error {
	if pool == nil {
		return fmt.Errorf("%w: nil database pool", ErrGuardRejected)
	}
	if !strings.HasSuffix(expected, TestDatabaseSuffix) {
		return fmt.Errorf("%w: expected database %q does not end with %q", ErrGuardRejected, expected, TestDatabaseSuffix)
	}
	var actual string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&actual); err != nil {
		return fmt.Errorf("%w: cannot read current_database(): %v", ErrGuardRejected, err)
	}
	if actual != expected {
		return fmt.Errorf("%w: connected database %q does not match declared %q", ErrGuardRejected, actual, expected)
	}
	return nil
}
