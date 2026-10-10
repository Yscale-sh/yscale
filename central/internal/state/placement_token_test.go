package state

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

const testPlacementKey = "placement-launch-signing-key-for-tests-0123456789" // gitleaks:allow -- deterministic test fixture

func testSigner(t *testing.T) *PlacementTokenSigner {
	t.Helper()
	s, err := NewPlacementTokenSigner(testPlacementKey)
	if err != nil {
		t.Fatalf("NewPlacementTokenSigner: %v", err)
	}
	return s
}

func testClaims() PlacementLaunchClaims {
	issued := time.Unix(1_760_000_000, 0).UTC()
	return PlacementLaunchClaims{
		Version:   PlacementReceiptVersion,
		Tenant:    "cust_alpha",
		QuoteID:   "quote_alpha_1",
		Digest:    strings.Repeat("ab", 32),
		IssuedAt:  issued,
		ExpiresAt: issued.Add(5 * time.Minute),
	}
}

// A key that cannot carry HMAC-SHA256's security is refused outright. There is
// no stretching or hashing-up: a deployment that believes it authenticates
// previews and does not is worse than one that knows it cannot.
func TestPlacementSignerRefusesAWeakKey(t *testing.T) {
	for _, tc := range []struct{ name, secret string }{
		{"empty", ""},
		{"whitespace only", "        \t\n   "},
		{"short", "hunter2"},
		{"one byte under the minimum", strings.Repeat("k", MinPlacementSigningKeyBytes-1)},
		{"padded to length with one character", strings.Repeat("a", 64)},
		{"a placeholder padded to length", strings.Repeat("changeme", 8)},
		{"long enough only before trimming", "  " + strings.Repeat("k", MinPlacementSigningKeyBytes-4) + "  "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signer, err := NewPlacementTokenSigner(tc.secret)
			if err == nil {
				t.Fatal("a weak key produced a usable signer")
			}
			if !errors.Is(err, ErrPlacementSigningKey) {
				t.Fatalf("error = %v, want ErrPlacementSigningKey", err)
			}
			if signer != nil {
				t.Fatal("a refused key still returned a signer")
			}
			if strings.Contains(err.Error(), tc.secret) && tc.secret != "" {
				t.Fatalf("the refusal echoes the key: %v", err)
			}
		})
	}
	// The shortest key that is actually admitted, so the boundary is pinned in
	// both directions rather than only on the failing side.
	if _, err := NewPlacementTokenSigner("abcdefgh" + strings.Repeat("z", MinPlacementSigningKeyBytes-8)); err != nil {
		t.Fatalf("a key at the minimum length with real variety was refused: %v", err)
	}
}

// A nil signer is the deployment with no key. It must answer, not panic, so a
// fail-closed caller has something to fail closed on.
func TestNilPlacementSignerFailsClosed(t *testing.T) {
	var signer *PlacementTokenSigner
	if _, err := signer.Sign(testClaims()); !errors.Is(err, ErrPlacementSigningKey) {
		t.Fatalf("Sign on a nil signer = %v, want ErrPlacementSigningKey", err)
	}
	if _, err := signer.Verify("yspl1.aaaa.bbbb"); !errors.Is(err, ErrPlacementSigningKey) {
		t.Fatalf("Verify on a nil signer = %v, want ErrPlacementSigningKey", err)
	}
}

// Round trip: every claim central signed comes back exactly, at the second
// resolution the wire format carries.
func TestPlacementTokenRoundTrips(t *testing.T) {
	signer := testSigner(t)
	want := testClaims()

	token, err := signer.Sign(want)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if !strings.HasPrefix(token, "yspl1.") {
		t.Fatalf("token %q does not carry the envelope version", token)
	}
	// The token is a credential, not a disclosure: nothing about the key may be
	// recoverable from it, and the only readable content is what the customer
	// was already shown.
	if strings.Contains(token, testPlacementKey) {
		t.Fatal("the token contains the signing key")
	}

	got, err := signer.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.Version != want.Version || got.Tenant != want.Tenant ||
		got.QuoteID != want.QuoteID || got.Digest != want.Digest ||
		!got.IssuedAt.Equal(want.IssuedAt) || !got.ExpiresAt.Equal(want.ExpiresAt) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}

	// Signing is deterministic, so one issuance has one token — there is no
	// nonce for a client to vary and no second spelling of the same credential.
	again, err := signer.Sign(want)
	if err != nil || again != token {
		t.Fatalf("Sign is not deterministic: %q vs %q (%v)", again, token, err)
	}
}

// Claims that could not describe a real issuance are refused at SIGNING time.
// Sign and Verify share one gate, so this is also the set Verify refuses.
func TestPlacementSignerRefusesUnusableClaims(t *testing.T) {
	base := testClaims()
	for _, tc := range []struct {
		name  string
		mutfn func(*PlacementLaunchClaims)
	}{
		{"no tenant", func(c *PlacementLaunchClaims) { c.Tenant = "" }},
		{"no quote id", func(c *PlacementLaunchClaims) { c.QuoteID = "" }},
		{"no digest", func(c *PlacementLaunchClaims) { c.Digest = "" }},
		{"a digest that is not one", func(c *PlacementLaunchClaims) { c.Digest = "not-a-digest" }},
		{"a digest in the wrong case", func(c *PlacementLaunchClaims) { c.Digest = strings.ToUpper(base.Digest) }},
		{"a short digest", func(c *PlacementLaunchClaims) { c.Digest = base.Digest[:63] }},
		{"an unknown receipt version", func(c *PlacementLaunchClaims) { c.Version = PlacementReceiptVersion + 1 }},
		{"no receipt version", func(c *PlacementLaunchClaims) { c.Version = 0 }},
		{"no issued-at", func(c *PlacementLaunchClaims) { c.IssuedAt = time.Time{} }},
		{"no expiry", func(c *PlacementLaunchClaims) { c.ExpiresAt = time.Time{} }},
		{"an inverted window", func(c *PlacementLaunchClaims) { c.ExpiresAt = c.IssuedAt.Add(-time.Minute) }},
		{"a zero-length window", func(c *PlacementLaunchClaims) { c.ExpiresAt = c.IssuedAt }},
		{"a tenant carrying the field delimiter", func(c *PlacementLaunchClaims) {
			c.Tenant = "cust_alpha\x1fquote_alpha_1"
		}},
		{"a quote id carrying the field delimiter", func(c *PlacementLaunchClaims) { c.QuoteID = "a\x1fb" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutfn(&c)
			token, err := testSigner(t).Sign(c)
			if err == nil {
				t.Fatalf("signed unusable claims into %q", token)
			}
			if !errors.Is(err, ErrInvalidPlacementToken) {
				t.Fatalf("error = %v, want ErrInvalidPlacementToken", err)
			}
		})
	}
}

// Tampering with any part of the envelope invalidates it. This is the property
// the whole credential rests on: the expiry is inside the signature, so re-timing
// a preview is forging one.
func TestPlacementTokenRefusesTampering(t *testing.T) {
	signer := testSigner(t)
	token, err := signer.Sign(testClaims())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	parts := strings.Split(token, ".")

	// A payload re-timed to a fresh window, presented with the original MAC.
	reTimed := testClaims()
	reTimed.IssuedAt = time.Now().UTC()
	reTimed.ExpiresAt = reTimed.IssuedAt.Add(5 * time.Minute)
	reTimedPayload, err := reTimed.canonical()
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}

	for _, tc := range []struct{ name, token string }{
		{"a re-timed payload under the original signature",
			parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(reTimedPayload)) + "." + parts[2]},
		{"a flipped signature byte", parts[0] + "." + parts[1] + "." + flipDecodedByte(t, parts[2])},
		{"a flipped payload byte", parts[0] + "." + flipDecodedByte(t, parts[1]) + "." + parts[2]},
		{"no signature", parts[0] + "." + parts[1]},
		{"an empty signature", parts[0] + "." + parts[1] + "."},
		{"no payload", parts[0] + ".." + parts[2]},
		{"a truncated signature", parts[0] + "." + parts[1] + "." + parts[2][:20]},
		{"a fourth segment", token + "." + parts[2]},
		{"another envelope version", "yspl2." + parts[1] + "." + parts[2]},
		{"no envelope version", parts[1] + "." + parts[2]},
		{"payload that is not base64", parts[0] + ".!!!!." + parts[2]},
		{"signature that is not base64", parts[0] + "." + parts[1] + ".!!!!"},
		{"empty", ""},
		{"not a token at all", "hello"},
		{"oversized", parts[0] + "." + strings.Repeat("A", 2048) + "." + parts[2]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := signer.Verify(tc.token); !errors.Is(err, ErrInvalidPlacementToken) {
				t.Fatalf("Verify(%q) = %v, want ErrInvalidPlacementToken", tc.token, err)
			}
		})
	}
}

// A token is bound to the key that signed it, so one deployment's credential is
// not another's — and rotating the key retires every outstanding preview.
func TestPlacementTokenIsBoundToItsKey(t *testing.T) {
	token, err := testSigner(t).Sign(testClaims())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	other, err := NewPlacementTokenSigner("a-different-operator-secret-entirely-98765")
	if err != nil {
		t.Fatalf("NewPlacementTokenSigner: %v", err)
	}
	if _, err := other.Verify(token); !errors.Is(err, ErrInvalidPlacementToken) {
		t.Fatalf("another deployment's key verified the token: %v", err)
	}
}

// One issuance has exactly one token. A payload that decodes to the same claims
// but is spelled differently carries no valid MAC, and even under the right key
// the re-encoding check refuses it — so there is no family of equivalent
// credentials to probe with.
func TestPlacementTokenRefusesANonCanonicalPayload(t *testing.T) {
	signer := testSigner(t)
	c := testClaims()
	payload, err := c.canonical()
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	// A zero-padded version reads back as the same integer but is not the
	// spelling canonical() produces. Signed with the REAL key, so the only thing
	// that can refuse it is the canonical re-encoding check.
	padded := strings.Replace(payload, "1\x1f", "01\x1f", 1)
	forged := "yspl1." + base64.RawURLEncoding.EncodeToString([]byte(padded)) + "." +
		base64.RawURLEncoding.EncodeToString(signer.mac(padded))
	if _, err := signer.Verify(forged); !errors.Is(err, ErrInvalidPlacementToken) {
		t.Fatalf("a non-canonical payload verified: %v", err)
	}
}

// Verify authenticates; it does not decide expiry. The caller owns that,
// because an expired-but-genuine preview and a forged one are different answers
// to the customer.
func TestPlacementTokenVerifyDoesNotDecideExpiry(t *testing.T) {
	signer := testSigner(t)
	c := testClaims()
	c.IssuedAt = time.Now().UTC().Add(-2 * time.Hour)
	c.ExpiresAt = c.IssuedAt.Add(5 * time.Minute)
	token, err := signer.Sign(c)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	got, err := signer.Verify(token)
	if err != nil {
		t.Fatalf("Verify refused a genuine expired token: %v", err)
	}
	if !got.ExpiresAt.Before(time.Now().UTC()) {
		t.Fatalf("expiry = %s, want the past so the caller can refuse it", got.ExpiresAt)
	}
}

// The launch MAC and the account binding are two keyed/unkeyed digests over
// placement material. Their domains differ, so neither can ever be replayed as
// the other.
func TestPlacementLaunchDomainIsSeparateFromTheAccountBinding(t *testing.T) {
	if placementLaunchTokenDomain == placementAccountBindingDomain {
		t.Fatal("the launch token and the account binding share a domain")
	}
}

// flipDecodedByte flips one bit of the DECODED segment and re-encodes it, so
// the tampered token is always a well-formed envelope carrying different bytes.
// Editing the base64 text directly would not do: in a segment whose length is
// not a multiple of three the final character carries padding bits, so two
// different characters can decode identically.
func flipDecodedByte(t *testing.T, segment string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil || len(raw) == 0 {
		t.Fatalf("decode %q: %v", segment, err)
	}
	raw[len(raw)/2] ^= 0x01
	return base64.RawURLEncoding.EncodeToString(raw)
}
