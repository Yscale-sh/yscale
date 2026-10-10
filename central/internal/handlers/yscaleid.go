// yscale:proprietary

package handlers

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Identity is the verified external identity behind a human's access token.
// Subject is the only field anything is keyed on; the rest is cached profile.
type Identity struct {
	Subject       string
	Email         string
	EmailVerified bool
	Name          string
}

// IdentityResolver validates a human's Bearer ACCESS TOKEN and returns the
// identity behind it. This is a different credential from Customer.Token: that
// one is a long-lived cluster secret a machine presents, and it must never
// resolve to a human here (nor a human's token to a cluster).
//
// The interface exists so handlers can be tested without an identity provider;
// YscaleID below is the only production implementation.
type IdentityResolver interface {
	Resolve(ctx context.Context, accessToken string) (Identity, error)
}

var (
	// ErrUnauthenticated means the token was rejected — absent, malformed, or
	// refused by the issuer. It maps to 401.
	ErrUnauthenticated = errors.New("unauthenticated")
	// ErrIdentityUnavailable means the issuer could not be consulted, so the
	// token is neither accepted nor rejected. It maps to 503: fail closed, but
	// tell the caller it is worth retrying.
	ErrIdentityUnavailable = errors.New("identity provider unavailable")
	// ErrRateLimited means this central declined to consult the issuer because
	// its own outbound budget is spent. Like ErrIdentityUnavailable it decides
	// nothing about the credential, but it maps to 429 + Retry-After: the fault
	// is load, and the caller can retry in a moment.
	ErrRateLimited = errors.New("identity resolution rate limited")
)

// YscaleID resolves access tokens against Yscale ID's OIDC userinfo endpoint.
// Introspection is delegated on purpose: central holds no signing key and no
// password, so it can only ever learn who a token belongs to by asking.
//
// Every resolve is a call out to Yscale ID, and /v1/account is reachable with
// no valid credential at all — so without a bound, anyone can point traffic at
// central and have it amplified one-for-one onto the identity provider. Three
// bounds hold that down, and all three are needed:
//
//   - a global CONCURRENCY semaphore, which caps the goroutines a slow issuer
//     can pin here;
//   - a PER-CREDENTIAL rate bucket, so one credential in a hot loop throttles
//     itself and nobody else;
//   - a global RATE bucket, which is the only one that actually caps aggregate
//     requests per second onto the issuer.
//
// The global rate bucket is the one with a real cost, and it is not optional.
// Per-credential budgets bound each caller and nothing in total: an attacker
// rotating junk tokens gets a fresh full bucket per token, so the aggregate is
// unbounded, and the semaphore caps only how many are in flight at once — at
// 8 slots and a fast issuer that is still hundreds of requests a second.
//
// Which bucket is asked first depends on whether the credential is already
// tracked, and both orders are load-bearing. A KNOWN credential is checked
// against its own bucket first, so one in a hot loop is refused without
// spending from the shared budget it would otherwise drain on its way to being
// rejected. An UNSEEN one takes the global token first and is only written into
// the tracker if it gets one — a junk-token flood is then shed without leaving
// a trace in the LRU, so it cannot evict the entry of the credential it is
// meant to be laundering, or of a real human.
//
// The residual is unchanged: a flood of distinct tokens still drains the shared
// budget and signed-in humans are shed with it (429). Capping what we can do to
// the identity provider is worth that; see docs/internals/tenant-isolation.md.
//
// Nothing waits: all three shed to ErrRateLimited (429 + Retry-After at the
// edge), because queueing a foreground request behind a queue we would also
// have to bound just moves the problem.
//
// Nothing here caches a resolution either. A rejected credential must never be
// remembered, and a positive cache would mean holding a token digest keyed to a
// live session — bounded outbound load is what the amplification actually
// needs.
type YscaleID struct {
	// BaseURL is the in-cluster Yscale ID base (YSCALE_ID_INTERNAL_URL); the
	// userinfo path is appended.
	BaseURL string
	// Client is optional; nil uses a shared client with the timeout below.
	Client *http.Client
	// Timeout bounds one userinfo call. Zero uses defaultUserinfoTimeout.
	Timeout time.Duration
	// MaxInflight caps concurrent userinfo calls across every caller. Zero uses
	// defaultMaxInflight.
	MaxInflight int
	// ResolvesPerSecond and Burst shape ONE credential's token bucket: the
	// sustained rate and how much of a spike passes before that credential is
	// shed. Zero uses the defaults below.
	ResolvesPerSecond float64
	Burst             int
	// GlobalResolvesPerSecond and GlobalBurst shape the aggregate bucket every
	// credential draws from — the cap on what this central can do to Yscale ID
	// in total. Zero uses the defaults below.
	GlobalResolvesPerSecond float64
	GlobalBurst             int
	// MaxTrackedCredentials caps how many per-credential buckets are held at
	// once, so the tracker cannot grow with the number of distinct tokens thrown
	// at the route. Zero uses defaultMaxTrackedCredentials.
	MaxTrackedCredentials int

	limiterOnce sync.Once
	inflight    chan struct{}

	globalMu sync.Mutex
	global   rateBucket

	// creds is a fixed-capacity LRU: a map to the list element, a list in
	// recency order. Every path — hit, insert, eviction — is O(1), because the
	// traffic that fills this tracker is precisely the token-rotation flood it
	// exists to survive, and a scan per miss would make the attack pay for
	// itself. Keyed by digest, never by token: the tracker outlives any one
	// request, and telling two callers apart does not require holding their
	// credentials.
	credMu  sync.Mutex
	creds   map[[sha256.Size]byte]*list.Element
	credLRU *list.List // front = most recently seen; Value is *credEntry
}

// credEntry is one credential's bucket plus the key it is filed under, so an
// eviction from the back of the list can unfile itself without a lookup.
type credEntry struct {
	digest [sha256.Size]byte
	bucket rateBucket
}

// rateBucket is a token bucket that refills lazily, on the call that reads it.
// There is no timer and no goroutine per bucket: an idle bucket costs nothing
// and a busy one is refilled from the clock the moment it is asked.
type rateBucket struct {
	tokens   float64
	refilled time.Time
}

// spend takes one token, refilling for the elapsed time first. false means the
// bucket is empty and nothing was taken.
func (b *rateBucket) spend(now time.Time, rate, burst float64) bool {
	if b.refilled.IsZero() {
		b.tokens, b.refilled = burst, now
	}
	b.tokens = math.Min(burst, b.tokens+now.Sub(b.refilled).Seconds()*rate)
	b.refilled = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

const (
	// defaultUserinfoTimeout bounds a userinfo call so a hung identity provider
	// cannot pin a central request goroutine open.
	defaultUserinfoTimeout = 5 * time.Second
	// maxUserinfoBytes caps the response we will read. userinfo payloads are a
	// few hundred bytes; the cap stops a hostile or broken issuer from feeding
	// central an unbounded body.
	maxUserinfoBytes = 64 << 10
	// The outbound defaults. Deliberately small: a dashboard polls its own
	// account, so a handful of resolves a second per credential is ordinary
	// traffic, and anything far above that is someone else's load test. The
	// global pair caps the aggregate independently, and sits above the
	// per-credential one — enough headroom for the humans a single central
	// serves at once, low enough that central can never become an interesting
	// amplifier.
	//
	// The per-credential pair is what a single valid session is allowed, and it
	// has not moved since it was introduced. Adding the aggregate bucket must
	// not quietly tighten one user's existing allowance: the new bound is on
	// everyone together, not on anyone in particular.
	defaultMaxInflight             = 8
	defaultResolvesPerSecond       = 10
	defaultResolveBurst            = 20
	defaultGlobalResolvesPerSecond = 20
	defaultGlobalBurst             = 40
	// defaultMaxTrackedCredentials bounds the LRU. Sized well above the number
	// of humans one central serves; past that the least-recently-seen entry is
	// dropped, which under a junk-token flood is a junk token, and in the worst
	// case is a real credential that simply starts over with a full bucket.
	defaultMaxTrackedCredentials = 4096
)

var defaultIdentityClient = &http.Client{Timeout: defaultUserinfoTimeout}

// userinfo is the subset of the OIDC userinfo response central consumes.
// Unknown claims are ignored — nothing else is stored, so nothing else can leak
// into an account response.
type userinfo struct {
	Sub           string `json:"sub"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	Name          string `json:"name"`
}

// Resolve exchanges an access token for its identity. Every failure path denies
// access: a non-200 status, an undecodable body, and an empty `sub` are all
// rejections, never a partially-trusted result.
func (y *YscaleID) Resolve(ctx context.Context, accessToken string) (Identity, error) {
	if strings.TrimSpace(accessToken) == "" {
		return Identity{}, ErrUnauthenticated
	}
	if y.BaseURL == "" {
		return Identity{}, fmt.Errorf("%w: no base URL configured", ErrIdentityUnavailable)
	}

	// All three bounds are taken before the token is used for anything, so a
	// shed request costs no upstream call and reveals nothing about the
	// credential. The order depends on whether this credential is already
	// tracked, and both orders exist for a reason — see allowKnownCredential and
	// trackCredential.
	y.initLimits()
	select {
	case y.inflight <- struct{}{}:
		defer func() { <-y.inflight }()
	default:
		return Identity{}, fmt.Errorf("%w: %d calls already in flight", ErrRateLimited, cap(y.inflight))
	}
	digest := sha256.Sum256([]byte(accessToken))
	if known, allowed := y.allowKnownCredential(digest); known {
		if !allowed {
			return Identity{}, fmt.Errorf("%w: this credential's outbound budget is spent", ErrRateLimited)
		}
		if !y.allowGlobal() {
			return Identity{}, fmt.Errorf("%w: central's outbound identity budget is spent", ErrRateLimited)
		}
	} else {
		if !y.allowGlobal() {
			return Identity{}, fmt.Errorf("%w: central's outbound identity budget is spent", ErrRateLimited)
		}
		if !y.trackCredential(digest) {
			return Identity{}, fmt.Errorf("%w: this credential's outbound budget is spent", ErrRateLimited)
		}
	}

	timeout := y.Timeout
	if timeout <= 0 {
		timeout = defaultUserinfoTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := strings.TrimSuffix(y.BaseURL, "/") + "/userinfo"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Identity{}, fmt.Errorf("%w: build request: %v", ErrIdentityUnavailable, err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	client := y.Client
	if client == nil {
		client = defaultIdentityClient
	}
	resp, err := client.Do(req)
	if err != nil {
		// The token is not rejected — we never got an answer. Surfacing this as
		// 401 would tell a user with a perfectly good session to sign in again.
		return Identity{}, fmt.Errorf("%w: %v", ErrIdentityUnavailable, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxUserinfoBytes))
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return Identity{}, ErrUnauthenticated
	default:
		return Identity{}, fmt.Errorf("%w: userinfo status %d", ErrIdentityUnavailable, resp.StatusCode)
	}

	var info userinfo
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxUserinfoBytes)).Decode(&info); err != nil {
		return Identity{}, fmt.Errorf("%w: decode userinfo: %v", ErrUnauthenticated, err)
	}
	if strings.TrimSpace(info.Sub) == "" {
		// A 200 with no subject identifies nobody. Accepting it would key an
		// account on the empty string — one shared account for every such token.
		return Identity{}, fmt.Errorf("%w: userinfo carried no subject", ErrUnauthenticated)
	}
	return Identity{
		Subject:       strings.TrimSpace(info.Sub),
		Email:         info.Email,
		EmailVerified: info.EmailVerified,
		Name:          info.Name,
	}, nil
}

// initLimits sizes the three bounds on first use, so a zero-value YscaleID (the
// production wiring sets only BaseURL) is limited rather than unlimited.
func (y *YscaleID) initLimits() {
	y.limiterOnce.Do(func() {
		inflight := y.MaxInflight
		if inflight <= 0 {
			inflight = defaultMaxInflight
		}
		y.inflight = make(chan struct{}, inflight)
		y.creds = make(map[[sha256.Size]byte]*list.Element)
		y.credLRU = list.New()
	})
}

func (y *YscaleID) burst() float64 {
	if y.Burst > 0 {
		return float64(y.Burst)
	}
	return defaultResolveBurst
}

func (y *YscaleID) rate() float64 {
	if y.ResolvesPerSecond > 0 {
		return y.ResolvesPerSecond
	}
	return defaultResolvesPerSecond
}

func (y *YscaleID) globalBurst() float64 {
	if y.GlobalBurst > 0 {
		return float64(y.GlobalBurst)
	}
	return defaultGlobalBurst
}

func (y *YscaleID) globalRate() float64 {
	if y.GlobalResolvesPerSecond > 0 {
		return y.GlobalResolvesPerSecond
	}
	return defaultGlobalResolvesPerSecond
}

func (y *YscaleID) maxTracked() int {
	if y.MaxTrackedCredentials > 0 {
		return y.MaxTrackedCredentials
	}
	return defaultMaxTrackedCredentials
}

// allowGlobal takes one token from the aggregate budget. false means this
// central has already asked Yscale ID as often as it is willing to, whoever is
// asking.
func (y *YscaleID) allowGlobal() bool {
	y.globalMu.Lock()
	defer y.globalMu.Unlock()
	return y.global.spend(time.Now(), y.globalRate(), y.globalBurst())
}

// allowKnownCredential is the lookup half: for a credential already in the
// tracker it takes one token from that credential's own bucket, and it does
// nothing at all for one that isn't. known=false means the caller has to decide
// whether this digest is worth tracking; known=true with allowed=false means
// THIS credential is over its sustained rate, and no other credential's budget
// — nor the global one — was touched on the way to refusing it.
//
// Refusing a known-abusive credential BEFORE the shared budget is the whole
// point of the split. A credential in a hot loop that spent a global token per
// rejection would throttle every signed-in human alongside itself, which is the
// per-credential bucket paying for the thing it exists to prevent.
//
// O(1): a hit moves one list element to the front. Nothing iterates, because
// the traffic that fills this tracker is precisely the token-rotation flood it
// exists to survive.
func (y *YscaleID) allowKnownCredential(digest [sha256.Size]byte) (known, allowed bool) {
	y.credMu.Lock()
	defer y.credMu.Unlock()
	el, ok := y.creds[digest]
	if !ok {
		return false, false
	}
	y.credLRU.MoveToFront(el)
	return true, el.Value.(*credEntry).bucket.spend(time.Now(), y.rate(), y.burst())
}

// trackCredential files an unseen digest and takes its first token. It runs
// only AFTER the global budget has already accepted the request, and that
// ordering is what keeps the LRU honest: a flood of fresh tokens is shed by the
// aggregate bucket without ever being written down, so it cannot evict the
// entries of the credentials being abused — or of real humans — and an attacker
// cannot launder an exhausted credential into a fresh bucket by churning junk
// through the tracker until its own entry falls off the back.
//
// A miss at capacity drops exactly one entry, the least recently seen. The
// re-check under the lock covers two requests racing the same new digest: the
// loser spends from the bucket the winner just filed rather than replacing it.
func (y *YscaleID) trackCredential(digest [sha256.Size]byte) bool {
	y.credMu.Lock()
	defer y.credMu.Unlock()
	now := time.Now()
	if el, ok := y.creds[digest]; ok {
		y.credLRU.MoveToFront(el)
		return el.Value.(*credEntry).bucket.spend(now, y.rate(), y.burst())
	}
	if y.credLRU.Len() >= y.maxTracked() {
		oldest := y.credLRU.Back()
		y.credLRU.Remove(oldest)
		delete(y.creds, oldest.Value.(*credEntry).digest)
	}
	entry := &credEntry{digest: digest}
	y.creds[digest] = y.credLRU.PushFront(entry)
	return entry.bucket.spend(now, y.rate(), y.burst())
}
