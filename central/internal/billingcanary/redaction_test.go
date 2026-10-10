// yscale:proprietary

package billingcanary

import (
	"errors"
	"strings"
	"testing"
)

func TestSanitiseAcceptsSafeLiterals(t *testing.T) {
	safe := []string{
		"",
		"grant applied",
		"settled exactly once",
		"attempts=1",
		"webhook rejected",
		// Ordinary words that happen to contain a prefix substring must not be
		// rejected. The current false-positive candidate: "store_error"
		// contains "re_", and both "reset" and "reversal_applied" contain the
		// same three letters at different positions.
		"store_error",
		"reset counters",
		"reversal_applied",
		"customer refund settled",
		"balance zeroed",
	}
	for _, value := range safe {
		if err := Sanitise(value); err != nil {
			t.Errorf("Sanitise(%q) rejected safe literal: %v", value, err)
		}
	}
}

func TestSanitiseRejectsProviderAndSecretPrefixes(t *testing.T) {
	forbidden := []string{
		"pi_1MZzAB", "ch_abcdef", "re_deadbeef", "dp_zzz", "cs_test_123", "cus_1234",
		"sk_test_secret", "sk_live_secret", "rk_live_secret", "pk_test_x", "whsec_abc",
		"acct_XYZ", "evt_123", "co_local", "cust_yscale_prod", "ysk_leaky_token",
	}
	for _, value := range forbidden {
		if err := Sanitise(value); !errors.Is(err, ErrRedactionRejected) {
			t.Errorf("Sanitise(%q) accepted forbidden prefix: %v", value, err)
		}
	}
}

func TestSanitiseRejectsForbiddenSubstrings(t *testing.T) {
	notes := []string{
		"yscale_customer_id=cust_tenant",
		"Stripe-Signature: t=1,v1=xxx",
		"Authorization: Bearer whsec_...",
		"leaked payment_intent=pi_1",
	}
	for _, value := range notes {
		if err := Sanitise(value); !errors.Is(err, ErrRedactionRejected) {
			t.Errorf("Sanitise(%q) accepted forbidden substring: %v", value, err)
		}
	}
}

func TestSafeNoteReplacesRejectedValues(t *testing.T) {
	if got := SafeNote("checkout OK"); got != "checkout OK" {
		t.Errorf("SafeNote passthrough = %q", got)
	}
	if got := SafeNote("pi_leak"); got != "REDACTED" {
		t.Errorf("SafeNote redacted = %q", got)
	}
}

// TestSanitiseRejectsProviderIDsAnywhereInNote guards against a note that
// wraps a provider identifier in prose. A previous implementation only
// checked the value's leading characters and let mid-sentence IDs leak.
func TestSanitiseRejectsProviderIDsAnywhereInNote(t *testing.T) {
	buried := []string{
		"the intent was pi_1abcdef",
		"grant recorded, cs_test_1abc followed",
		"refund for ch_deadbeef succeeded",
		"customer cus_1abc removed",
		"secret sk_test_zzz should never appear",
	}
	for _, value := range buried {
		if err := Sanitise(value); !errors.Is(err, ErrRedactionRejected) {
			t.Errorf("Sanitise(%q) accepted an embedded provider identifier: %v", value, err)
		}
	}
}

func TestSanitiseRejectsOversizedValue(t *testing.T) {
	err := Sanitise(strings.Repeat("a", 513))
	if !errors.Is(err, ErrRedactionRejected) {
		t.Errorf("Sanitise oversize err = %v", err)
	}
}
