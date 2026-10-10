package billing

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

type grantRecorder struct {
	statements []string
}

func (r *grantRecorder) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	r.statements = append(r.statements, sql)
	return pgconn.NewCommandTag("GRANT"), nil
}

func TestValidateRuntimeRoleRejectsSQLAndSurprisingIdentifiers(t *testing.T) {
	for _, role := range []string{"billing_runtime", "b", strings.Repeat("a", 63)} {
		if err := ValidateRuntimeRole(role); err != nil {
			t.Errorf("valid role %q: %v", role, err)
		}
	}
	for _, role := range []string{"", "Billing", "billing-runtime", "billing runtime", `billing_runtime";GRANT ALL TO PUBLIC;--`, strings.Repeat("a", 64)} {
		if err := ValidateRuntimeRole(role); err == nil {
			t.Errorf("unsafe role %q accepted", role)
		}
	}
}

func TestRuntimeGrantSetIsExactAndContainsNoDDLOrPublicGrant(t *testing.T) {
	recorder := &grantRecorder{}
	if err := grantRuntimePrivileges(context.Background(), recorder, "yscale_billing", "billing_runtime"); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`GRANT CONNECT ON DATABASE "yscale_billing" TO "billing_runtime"`,
		`GRANT USAGE ON SCHEMA billing TO "billing_runtime"`,
		`GRANT USAGE ON ALL SEQUENCES IN SCHEMA billing TO "billing_runtime"`,
		`GRANT SELECT, INSERT, UPDATE ON TABLE "billing"."environment", "billing"."accounts", "billing"."operations", "billing"."funding_sources", "billing"."credit_reversals", "billing"."credit_disputes", "billing"."holds", "billing"."ledger", "billing"."checkout_sessions", "billing"."webhook_inbox" TO "billing_runtime"`,
	}
	if strings.Join(recorder.statements, "\n") != strings.Join(want, "\n") {
		t.Fatalf("grants:\n%s\nwant:\n%s", strings.Join(recorder.statements, "\n"), strings.Join(want, "\n"))
	}
	upper := strings.ToUpper(strings.Join(recorder.statements, "\n"))
	for _, forbidden := range []string{"GRANT CREATE", "ALL PRIVILEGES", " TO PUBLIC", "ALTER ", "DROP ", "CREATE "} {
		if strings.Contains(upper, forbidden) {
			t.Fatalf("runtime grants contain %q: %s", forbidden, upper)
		}
	}
}

func TestRuntimeGrantTablesCoverSchema(t *testing.T) {
	matches := regexp.MustCompile(`(?m)^CREATE TABLE IF NOT EXISTS billing\.([a-z_][a-z0-9_]*) \(`).
		FindAllStringSubmatch(billingSchema, -1)
	want := make([]string, 0, len(matches))
	for _, match := range matches {
		want = append(want, match[1])
	}
	got := slices.Clone(runtimeBillingTables)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("runtime grant tables = %v, schema tables = %v", got, want)
	}
}

func TestEnvironmentSentinelMismatch(t *testing.T) {
	if err := validateEnvironmentMode(false, false); err != nil {
		t.Fatalf("matching test sentinel: %v", err)
	}
	if err := validateEnvironmentMode(true, true); err != nil {
		t.Fatalf("matching live sentinel: %v", err)
	}
	if err := validateEnvironmentMode(false, true); !errors.Is(err, ErrEnvironmentMismatch) {
		t.Fatalf("test to live mismatch = %v", err)
	}
	if err := validateEnvironmentMode(true, false); !errors.Is(err, ErrEnvironmentMismatch) {
		t.Fatalf("live to test mismatch = %v", err)
	}
}
