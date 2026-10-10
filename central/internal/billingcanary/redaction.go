// yscale:proprietary

package billingcanary

import (
	"errors"
	"fmt"
	"strings"
)

// forbiddenPrefixes rejects any secret- or provider-identifier fragment the
// canary MUST NOT export in evidence. The redactor screens both scenario names
// and safe-note strings against this list before the evidence is serialised.
//
// New prefixes are added here rather than at each call site so the rule stays
// in one place and grows with the provider surface.
var forbiddenPrefixes = []string{
	// Stripe object identifiers of any kind.
	"pi_", "ch_", "re_", "dp_", "cs_", "cus_", "in_", "sub_",
	"evt_", "seti_", "src_", "txn_", "iv_", "prod_", "price_",
	// Stripe credential shapes.
	"sk_test_", "sk_live_", "rk_test_", "rk_live_", "pk_test_", "pk_live_",
	"whsec_", "acct_",
	// Yscale internal identifiers that would let evidence be cross-referenced.
	"co_", "cust_", "tenant_", "acc_", "ysk_", "yscale_dev_",
}

// forbiddenSubstrings catches the same categories when they appear inside a
// longer note. Anchored substrings only — never bare human words that could
// occur in an ordinary sentence.
var forbiddenSubstrings = []string{
	"yscale_customer_id=", "yscale_checkout_id=",
	"client_reference_id=", "payment_intent=",
	"Authorization:", "Stripe-Signature:", "whsec_",
}

// ErrRedactionRejected marks a value the canary refuses to record.
var ErrRedactionRejected = errors.New("billingcanary: redaction rejected")

// Sanitise returns nil when the value is safe to appear in evidence exactly as
// written. It refuses any known secret or provider-ID shape; the caller MUST
// replace a rejected value with a bounded, describable literal before adding
// it to a SafeEvidence field.
//
// A provider-identifier prefix is a bug ANYWHERE in the value, not just at
// position zero — a note like "the intent was pi_1abc" must be redacted the
// same as one written as "pi_1abc alone". Prefix matching walks every
// identifier-shaped token in the value, so ordinary words that happen to
// contain a prefix substring (e.g. "store_error" contains "re_") are not
// falsely rejected.
func Sanitise(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	if len(trimmed) > 512 {
		return fmt.Errorf("%w: value exceeds bounded 512-byte evidence field", ErrRedactionRejected)
	}
	lower := strings.ToLower(trimmed)
	for _, token := range identifierTokens(lower) {
		for _, prefix := range forbiddenPrefixes {
			if len(token) > len(prefix) && strings.HasPrefix(token, prefix) {
				return fmt.Errorf("%w: value contains forbidden identifier prefix", ErrRedactionRejected)
			}
		}
	}
	for _, needle := range forbiddenSubstrings {
		if strings.Contains(lower, strings.ToLower(needle)) {
			return fmt.Errorf("%w: value contains forbidden substring", ErrRedactionRejected)
		}
	}
	return nil
}

// identifierTokens splits value on any rune that is not part of a Stripe- or
// Yscale-style identifier. Provider IDs are drawn from [A-Za-z0-9_]; anything
// outside that class separates one candidate identifier from the next.
func identifierTokens(value string) []string {
	tokens := make([]string, 0, 4)
	start := -1
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			if start == -1 {
				start = i
			}
			continue
		}
		if start != -1 {
			tokens = append(tokens, value[start:i])
			start = -1
		}
	}
	if start != -1 {
		tokens = append(tokens, value[start:])
	}
	return tokens
}

// SafeNote returns the value when Sanitise accepts it, or a bounded literal
// describing why it was refused. It is the canary's only path from a
// short-lived detail into evidence, and it is intentionally lossy on refusal
// so a copy-paste can never smuggle a provider identifier through.
func SafeNote(value string) string {
	if err := Sanitise(value); err != nil {
		return "REDACTED"
	}
	return strings.TrimSpace(value)
}
