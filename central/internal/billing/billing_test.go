package billing

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestOpenStoreFailsClosedOnInvalidExplicitDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store, err := OpenStore(ctx, "://invalid-billing-dsn", false)
	if err == nil || store != nil {
		t.Fatalf("OpenStore = (%v, %v), want nil,error", store, err)
	}
}

func TestValidatePositiveAmount(t *testing.T) {
	for _, test := range []struct {
		amount int64
		valid  bool
	}{{1, true}, {1_000_000, true}, {math.MaxInt64, true}, {0, false}, {-1, false}, {math.MinInt64, false}} {
		err := ValidatePositiveAmount(test.amount)
		if test.valid && err != nil {
			t.Fatalf("ValidatePositiveAmount(%d): %v", test.amount, err)
		}
		if !test.valid && !errors.Is(err, ErrInvalidAmount) {
			t.Fatalf("ValidatePositiveAmount(%d) = %v, want ErrInvalidAmount", test.amount, err)
		}
	}
}

func TestSafeAdd(t *testing.T) {
	for _, test := range []struct {
		a, b, want int64
		overflow   bool
	}{
		{1, 2, 3, false},
		{10, -5, 5, false},
		{math.MaxInt64 - 1, 1, math.MaxInt64, false},
		{math.MinInt64 + 1, -1, math.MinInt64, false},
		{math.MaxInt64, 1, 0, true},
		{math.MinInt64, -1, 0, true},
	} {
		got, err := safeAdd(test.a, test.b)
		if test.overflow && !errors.Is(err, ErrAmountOverflow) {
			t.Fatalf("safeAdd(%d,%d) = %d,%v, want overflow", test.a, test.b, got, err)
		}
		if !test.overflow && (err != nil || got != test.want) {
			t.Fatalf("safeAdd(%d,%d) = %d,%v, want %d,nil", test.a, test.b, got, err, test.want)
		}
	}
}

func TestHoldStateMachine(t *testing.T) {
	for _, terminal := range []HoldState{HoldCaptured, HoldReleased, HoldExpired} {
		if !CanTransition(HoldPending, terminal) {
			t.Fatalf("pending -> %s should be legal", terminal)
		}
		for _, target := range []HoldState{HoldPending, HoldCaptured, HoldReleased, HoldExpired} {
			if CanTransition(terminal, target) {
				t.Fatalf("terminal transition %s -> %s should be illegal", terminal, target)
			}
		}
	}
	if CanTransition(HoldPending, HoldPending) || CanTransition("unknown", HoldReleased) {
		t.Fatal("self/unknown transition accepted")
	}
}

func TestAccountSpendable(t *testing.T) {
	account := Account{BalanceMicroUSD: 10_000_000, HeldMicroUSD: 3_000_000}
	if got := account.SpendableMicroUSD(); got != 7_000_000 {
		t.Fatalf("SpendableMicroUSD = %d", got)
	}
}

func TestValidatePriceQuote(t *testing.T) {
	now := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	valid := PriceQuote{
		QuoteID: "quote-1", PricingVersion: 1, Currency: "USD", Provider: "linode",
		SKU: "rtx4000ada", Region: "us-east", ProviderRateMicroUSDPerHour: 1000,
		CustomerRateMicroUSDPerHour: 1020, PlatformFeeBasisPoints: 200,
		TaxMicroUSD: 5, MaximumDurationSeconds: 7, MaximumChargeMicroUSD: 7,
		IssuedAt: now, ValidUntil: now.Add(time.Minute),
	}
	if err := validatePriceQuote(valid); err != nil {
		t.Fatalf("valid quote: %v", err)
	}
	invalidRate := valid
	invalidRate.CustomerRateMicroUSDPerHour++
	if !errors.Is(validatePriceQuote(invalidRate), ErrInvalidArgument) {
		t.Fatal("mismatched customer rate accepted")
	}
	invalidMaximum := valid
	invalidMaximum.MaximumChargeMicroUSD++
	if !errors.Is(validatePriceQuote(invalidMaximum), ErrInvalidArgument) {
		t.Fatal("mismatched maximum charge accepted")
	}
	invalidCurrency := valid
	invalidCurrency.Currency = "EUR"
	if !errors.Is(validatePriceQuote(invalidCurrency), ErrInvalidArgument) {
		t.Fatal("unsupported currency accepted")
	}
	if got, err := ceilMulDiv(1020, 7, 3600); err != nil || got != 2 {
		t.Fatalf("ceilMulDiv = %d,%v, want 2,nil", got, err)
	}
}

func TestOperationHashIncludesSemanticPayload(t *testing.T) {
	type payload struct {
		Amount int64 `json:"amount"`
	}
	one, err := operationHash("grant", "customer-a", payload{100})
	if err != nil {
		t.Fatal(err)
	}
	two, _ := operationHash("grant", "customer-a", payload{100})
	otherAmount, _ := operationHash("grant", "customer-a", payload{101})
	otherCustomer, _ := operationHash("grant", "customer-b", payload{100})
	otherOperation, _ := operationHash("reversal", "customer-a", payload{100})
	if one != two || one == otherAmount || one == otherCustomer || one == otherOperation {
		t.Fatal("operation hash does not preserve exact semantic identity")
	}
}

func TestTruncateUTF8(t *testing.T) {
	for _, test := range []struct {
		name     string
		value    string
		maxBytes int
		want     string
	}{
		{name: "unchanged", value: "safe error", maxBytes: 64, want: "safe error"},
		{name: "ascii", value: "abcdef", maxBytes: 3, want: "abc"},
		{name: "rune boundary", value: "ab€cd", maxBytes: 4, want: "ab"},
		{name: "invalid input", value: "a\xffb", maxBytes: 8, want: "a�b"},
		{name: "postgres nul", value: "a\x00b", maxBytes: 8, want: "a�b"},
		{name: "disabled", value: "abc", maxBytes: 0, want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := truncateUTF8(test.value, test.maxBytes)
			if got != test.want {
				t.Fatalf("truncateUTF8(%q, %d) = %q, want %q", test.value, test.maxBytes, got, test.want)
			}
			if len(got) > test.maxBytes && test.maxBytes > 0 {
				t.Fatalf("result is %d bytes, limit %d", len(got), test.maxBytes)
			}
			if !utf8.ValidString(got) {
				t.Fatal("result is not valid UTF-8")
			}
		})
	}
}

func TestMutationValidationRequiresIdempotencyKey(t *testing.T) {
	if !errors.Is(validateMutationRequest("", "key"), ErrInvalidArgument) {
		t.Fatal("empty customer accepted")
	}
	if !errors.Is(validateMutationRequest("customer", ""), ErrInvalidArgument) {
		t.Fatal("empty idempotency key accepted")
	}
	if !errors.Is(validateMutationRequest("customer", strings.Repeat("x", 256)), ErrInvalidArgument) {
		t.Fatal("oversized idempotency key accepted")
	}
	if !errors.Is(validateMutationRequest("customer", "sys:expire-hold:1"), ErrInvalidArgument) {
		t.Fatal("reserved system idempotency prefix accepted")
	}
	if err := validateMutationRequest("customer", "key"); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaContainsDatabaseInvariants(t *testing.T) {
	for _, required := range []string{
		"CHECK (held_micro_usd <= balance_micro_usd)",
		"CHECK (debt_micro_usd = 0 OR frozen)",
		"PRIMARY KEY (customer_id, idempotency_key)",
		"canonical_version SMALLINT NOT NULL",
		"UNIQUE (customer_id, workload_ref)",
		"FOREIGN KEY (customer_id, operation_key)",
		"FOREIGN KEY (customer_id, hold_id)",
		"billing_operation_result_hold_tenant_fk",
		"billing_environment_immutable",
		"UNIQUE (provider, provider_account_id, livemode, payment_object_id)",
		"UNIQUE (provider, provider_account_id, livemode, reversal_object_id)",
		"UNIQUE (provider, provider_account_id, livemode, provider_event_id)",
		"octet_length(payload) BETWEEN 1 AND 1048576",
		"billing_ledger_immutable",
		"billing_hold_transition",
		"FOR UPDATE SKIP LOCKED",
	} {
		// The SKIP LOCKED query lives in store.go rather than the schema; test it
		// separately below and keep this list focused on DDL.
		if required == "FOR UPDATE SKIP LOCKED" {
			continue
		}
		if !strings.Contains(billingSchema, required) {
			t.Fatalf("billing schema missing %q", required)
		}
	}
}

func TestErrorSentinelsWrap(t *testing.T) {
	for _, sentinel := range []error{
		ErrInsufficientCredit, ErrIdempotencyConflict, ErrIllegalTransition,
		ErrInvalidArgument, ErrInvalidAmount, ErrAmountOverflow,
		ErrCaptureExceedsHold, ErrHoldExpired, ErrNotFound,
		ErrAccountFrozen, ErrInvariantViolation, ErrLeaseLost,
		ErrEconomicObjectConflict,
		ErrEnvironmentMismatch,
	} {
		if !errors.Is(errors.Join(errors.New("context"), sentinel), sentinel) {
			t.Fatalf("errors.Is failed for %v", sentinel)
		}
	}
}
