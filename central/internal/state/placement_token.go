package state

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// A placement launch token is the ONE credential a placement preview hands out
// and a launch presents back. It is central's own signature over the issuance
// it produced: which decision, for which tenant, from which quote, and until
// when.
//
// It is stateless BY CONSTRUCTION, and that is the whole design constraint. A
// preview is a read — it takes no claim, writes no row and leaves nothing
// behind — so there is no issuance record for a launch to be checked against.
// The alternative to a signature is trusting the client's copy of the issuance,
// and a client-supplied window is not a window central issued: a digest stays
// the same value for as long as the decision holds, so a stale one paired with
// any plausible near-future timestamp was indistinguishable from a fresh
// preview. Putting the expiry INSIDE the signature is what makes re-timing a
// stale preview require forging one.
//
// The key is operator-supplied and stable, not process-local: a token minted by
// one replica is presented to another, and a restart must not invalidate the
// previews customers are holding.
const (
	// placementLaunchTokenDomain separates this MAC from every other digest
	// central computes over placement material — notably
	// placementAccountBindingDomain — so no value derived for one purpose can
	// ever be replayed as the other.
	placementLaunchTokenDomain = "yscale.placement.launch.v1" // gitleaks:allow -- public MAC domain separator

	// placementLaunchTokenPrefix versions the token ENVELOPE (how the fields are
	// framed on the wire), independently of PlacementReceiptVersion, which
	// versions the decision shape they describe. It is covered by the MAC, so a
	// future envelope cannot be re-framed as this one.
	placementLaunchTokenPrefix = "yspl1"

	// placementLaunchTokenSep frames the signed fields. It is a byte no id,
	// digest or decimal central produces can contain, and canonical() refuses to
	// sign a claim that contains one anyway — a delimiter a field can carry is a
	// delimiter a field can forge.
	placementLaunchTokenSep = "\x1f"

	// placementLaunchTokenFields is the exact field count of a v1 payload.
	placementLaunchTokenFields = 6

	// MinPlacementSigningKeyBytes is the shortest operator-supplied secret this
	// build will sign with. HMAC-SHA256's security rests entirely on the key, so
	// a short one is not a weaker deployment — it is an unauthenticated one that
	// looks authenticated.
	MinPlacementSigningKeyBytes = 32

	// minPlacementSigningKeyDistinct rejects the placeholder secret an operator
	// pads to length ("aaaa…", "changemechangeme…"). Any key with real entropy
	// clears it easily: 32 random bytes carry ~30 distinct values, and even a
	// hex-alphabet key carries 16.
	minPlacementSigningKeyDistinct = 8

	// maxPlacementLaunchTokenBytes bounds what Verify will even decode. A v1
	// token is a couple of hundred bytes; anything larger is refused before any
	// allocation rather than after.
	maxPlacementLaunchTokenBytes = 1024
)

// ErrPlacementSigningKey marks a key this build will not sign or verify with,
// and a signer that is absent entirely. Both are the same operator-facing
// answer: this deployment cannot authenticate a preview.
var ErrPlacementSigningKey = errors.New("placement launch signing key unusable")

// ErrInvalidPlacementToken marks a token — or a set of claims — that cannot be
// honored. Every reason collapses to this one error deliberately: a forged
// signature, a truncated envelope, a token from another deployment and a token
// this build's receipt version cannot describe must be indistinguishable to
// whoever presented it.
var ErrInvalidPlacementToken = errors.New("invalid placement launch token")

// PlacementLaunchClaims is the authenticated tuple: everything a launch needs
// to know a preview is genuine, and nothing central holds only for itself.
//
// There is no cloud account id and no AccountBinding here, and there must never
// be: the binding is already covered by Digest, and a token is a value that
// travels through a customer's client, their logs and their CI.
type PlacementLaunchClaims struct {
	// Version is the receipt SHAPE the digest describes — PlacementReceiptVersion
	// at issuance. A token naming a shape this build does not produce is refused
	// rather than reinterpreted.
	Version int
	// Tenant is the account the preview was issued to. It is what makes a token
	// non-transferable: a launch compares it against its own authenticated
	// caller, so one tenant's preview cannot be replayed on another's submission.
	Tenant string
	// QuoteID is the issuance identity — WHICH preview this is, out of the many
	// that share a digest while the decision holds.
	QuoteID string
	// Digest is the decision the preview showed. A launch recomputes the decision
	// and compares it against this, so the token authenticates which decision was
	// promised without ever being the decision itself.
	Digest string
	// IssuedAt and ExpiresAt are the issuance WINDOW, at second resolution.
	// ExpiresAt is the field this whole type exists for.
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// PlacementTokenSigner mints and authenticates launch tokens under one
// operator-supplied key.
//
// The zero value is unusable on purpose — NewPlacementTokenSigner is the only
// way to hold one, so a key that never passed the strength gate cannot reach a
// signature. A nil *PlacementTokenSigner is the "signing is unavailable"
// deployment and answers every call with ErrPlacementSigningKey rather than
// panicking, so a fail-closed caller has something to fail closed on.
type PlacementTokenSigner struct {
	key []byte
}

// NewPlacementTokenSigner validates the operator's secret and returns a signer
// bound to a private copy of it. It fails rather than weakening: there is no
// stretching, padding or hashing-up of a short key, because a deployment that
// silently accepts a four-character secret is one that believes it authenticates
// previews and does not.
func NewPlacementTokenSigner(secret string) (*PlacementTokenSigner, error) {
	key := []byte(strings.TrimSpace(secret))
	if len(key) < MinPlacementSigningKeyBytes {
		// The length is the operator's own input and safe to state; the value
		// never appears, here or anywhere else.
		return nil, fmt.Errorf("%w: %d bytes is under the %d-byte minimum",
			ErrPlacementSigningKey, len(key), MinPlacementSigningKeyBytes)
	}
	distinct := make(map[byte]struct{}, len(key))
	for _, b := range key {
		distinct[b] = struct{}{}
	}
	if len(distinct) < minPlacementSigningKeyDistinct {
		return nil, fmt.Errorf("%w: only %d distinct byte values, which is a padded placeholder rather than a secret",
			ErrPlacementSigningKey, len(distinct))
	}
	return &PlacementTokenSigner{key: append([]byte(nil), key...)}, nil
}

// mac is the domain-separated HMAC over one canonical payload. The domain and
// the envelope version are inside the MAC, not merely beside it, so a payload
// can never be lifted into another envelope or another keyed construction.
func (s *PlacementTokenSigner) mac(payload string) []byte {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte(placementLaunchTokenDomain + placementLaunchTokenSep +
		placementLaunchTokenPrefix + placementLaunchTokenSep + payload))
	return m.Sum(nil)
}

// Sign issues the opaque token for one preview. The claims pass the SAME
// canonical gate Verify applies, so this cannot mint anything Verify would
// refuse — and cannot mint a token with an empty tenant, an unbounded window or
// a digest that is not one.
func (s *PlacementTokenSigner) Sign(c PlacementLaunchClaims) (string, error) {
	if s == nil {
		return "", fmt.Errorf("%w: no signing key is configured", ErrPlacementSigningKey)
	}
	payload, err := c.canonical()
	if err != nil {
		return "", err
	}
	return placementLaunchTokenPrefix + "." +
		base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." +
		base64.RawURLEncoding.EncodeToString(s.mac(payload)), nil
}

// Verify authenticates a presented token and returns the claims central itself
// put in it.
//
// It deliberately does NOT decide expiry: the caller owns that, because an
// expired-but-genuine preview and a forged one are different answers to the
// customer even though both refuse the launch.
//
// The signature is checked BEFORE the payload is parsed, and with
// hmac.Equal — a byte-by-byte comparison would leak, over enough attempts, how
// much of a guessed MAC was right.
func (s *PlacementTokenSigner) Verify(token string) (PlacementLaunchClaims, error) {
	if s == nil {
		return PlacementLaunchClaims{}, fmt.Errorf("%w: no signing key is configured", ErrPlacementSigningKey)
	}
	if token == "" || len(token) > maxPlacementLaunchTokenBytes {
		return PlacementLaunchClaims{}, fmt.Errorf("%w: envelope", ErrInvalidPlacementToken)
	}
	prefix, rest, ok := strings.Cut(token, ".")
	if !ok || prefix != placementLaunchTokenPrefix {
		return PlacementLaunchClaims{}, fmt.Errorf("%w: envelope", ErrInvalidPlacementToken)
	}
	encoded, signature, ok := strings.Cut(rest, ".")
	if !ok || strings.Contains(signature, ".") {
		return PlacementLaunchClaims{}, fmt.Errorf("%w: envelope", ErrInvalidPlacementToken)
	}
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return PlacementLaunchClaims{}, fmt.Errorf("%w: envelope", ErrInvalidPlacementToken)
	}
	sig, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil || len(sig) != sha256.Size {
		return PlacementLaunchClaims{}, fmt.Errorf("%w: signature", ErrInvalidPlacementToken)
	}
	if !hmac.Equal(sig, s.mac(string(payload))) {
		return PlacementLaunchClaims{}, fmt.Errorf("%w: signature", ErrInvalidPlacementToken)
	}
	return parsePlacementLaunchPayload(string(payload))
}

// canonical renders the claims as the one byte sequence that may be signed for
// them, and IS the invariant gate both directions pass through. Every rule that
// makes a token meaningful lives here exactly once, so Sign can never produce
// something Verify would reject and Verify can never accept something Sign
// could not have produced.
func (c PlacementLaunchClaims) canonical() (string, error) {
	// The version is pinned rather than merely recorded: this build describes
	// one receipt shape, and honoring a token that names another would be
	// interpreting a decision it cannot recompute.
	if c.Version != PlacementReceiptVersion {
		return "", fmt.Errorf("%w: receipt version %d", ErrInvalidPlacementToken, c.Version)
	}
	tenant, err := placementTokenField("tenant", c.Tenant)
	if err != nil {
		return "", err
	}
	quoteID, err := placementTokenField("quote id", c.QuoteID)
	if err != nil {
		return "", err
	}
	if !isPlacementDigest(c.Digest) {
		return "", fmt.Errorf("%w: digest", ErrInvalidPlacementToken)
	}
	// A zero or inverted window is not a window. Both would otherwise sign
	// cleanly and then be read as "expired forever" or "valid forever" depending
	// on who compared them.
	if c.IssuedAt.IsZero() || c.ExpiresAt.IsZero() || !c.ExpiresAt.After(c.IssuedAt) {
		return "", fmt.Errorf("%w: issuance window", ErrInvalidPlacementToken)
	}
	return strings.Join([]string{
		strconv.Itoa(c.Version),
		tenant,
		quoteID,
		c.Digest,
		strconv.FormatInt(c.IssuedAt.Unix(), 10),
		strconv.FormatInt(c.ExpiresAt.Unix(), 10),
	}, placementLaunchTokenSep), nil
}

// parsePlacementLaunchPayload reads an ALREADY-AUTHENTICATED payload back into
// claims. Re-encoding and requiring the result to be byte-identical is what
// makes the encoding canonical: a payload with a padded integer or a stray
// field is refused even though it carries a valid MAC, so there is exactly one
// token per issuance rather than a family of equivalent ones.
func parsePlacementLaunchPayload(payload string) (PlacementLaunchClaims, error) {
	fields := strings.Split(payload, placementLaunchTokenSep)
	if len(fields) != placementLaunchTokenFields {
		return PlacementLaunchClaims{}, fmt.Errorf("%w: payload", ErrInvalidPlacementToken)
	}
	version, versionErr := strconv.Atoi(fields[0])
	issued, issuedErr := strconv.ParseInt(fields[4], 10, 64)
	expires, expiresErr := strconv.ParseInt(fields[5], 10, 64)
	if versionErr != nil || issuedErr != nil || expiresErr != nil {
		return PlacementLaunchClaims{}, fmt.Errorf("%w: payload", ErrInvalidPlacementToken)
	}
	c := PlacementLaunchClaims{
		Version:   version,
		Tenant:    fields[1],
		QuoteID:   fields[2],
		Digest:    fields[3],
		IssuedAt:  time.Unix(issued, 0).UTC(),
		ExpiresAt: time.Unix(expires, 0).UTC(),
	}
	round, err := c.canonical()
	if err != nil {
		return PlacementLaunchClaims{}, err
	}
	if round != payload {
		return PlacementLaunchClaims{}, fmt.Errorf("%w: non-canonical payload", ErrInvalidPlacementToken)
	}
	return c, nil
}

// placementTokenField admits one free-form id: non-empty, and unable to carry
// the delimiter that frames it.
func placementTokenField(name, value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("%w: %s is empty", ErrInvalidPlacementToken, name)
	}
	if strings.Contains(value, placementLaunchTokenSep) {
		return "", fmt.Errorf("%w: %s contains the field delimiter", ErrInvalidPlacementToken, name)
	}
	return value, nil
}

// isPlacementDigest reports whether s has the exact shape ComputeDigest
// produces: 64 lowercase hex characters. Uppercase is refused rather than
// folded, so one decision has one spelling.
func isPlacementDigest(s string) bool {
	if len(s) != 2*sha256.Size {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
