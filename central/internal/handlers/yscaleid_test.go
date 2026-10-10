// yscale:proprietary

package handlers

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// userinfoServer stands in for Yscale ID. It records the Authorization header
// it saw so a test can prove the caller's token — and only that token — is
// forwarded, and counts calls so a test can prove how much load actually
// reached the issuer.
type userinfoServer struct {
	*httptest.Server
	gotAuth string
	gotPath string
	calls   atomic.Int64
}

func newUserinfoServer(t *testing.T, status int, body string) *userinfoServer {
	t.Helper()
	s := &userinfoServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.calls.Add(1)
		s.gotAuth = r.Header.Get("Authorization")
		s.gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}

func TestYscaleIDResolveAcceptsVerifiedUserinfo(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK,
		`{"sub":"sub-1","email":"human@acme.com","email_verified":true,"name":"A Human","aud":"ignored"}`)
	resolver := &YscaleID{BaseURL: srv.URL}

	id, err := resolver.Resolve(context.Background(), "access-token-1")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if id.Subject != "sub-1" || id.Email != "human@acme.com" || !id.EmailVerified || id.Name != "A Human" {
		t.Fatalf("identity = %+v", id)
	}
	if srv.gotAuth != "Bearer access-token-1" {
		t.Errorf("Authorization forwarded = %q", srv.gotAuth)
	}
	if srv.gotPath != "/userinfo" {
		t.Errorf("userinfo path = %q, want /userinfo", srv.gotPath)
	}
}

// A trailing slash on the configured base URL must not produce //userinfo.
func TestYscaleIDResolveTrimsBaseURLSlash(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK, `{"sub":"sub-1"}`)
	resolver := &YscaleID{BaseURL: srv.URL + "/"}
	if _, err := resolver.Resolve(context.Background(), "tok"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if srv.gotPath != "/userinfo" {
		t.Errorf("userinfo path = %q, want /userinfo", srv.gotPath)
	}
}

// Everything that isn't a clean, subject-bearing 200 denies access. The split
// matters: a refused token is 401-shaped (sign in again), an unreachable issuer
// is unavailable-shaped (retry) — never silently accepted.
func TestYscaleIDResolveFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr error
	}{
		{"issuer rejects the token", http.StatusUnauthorized, `{"error":"invalid_token"}`, ErrUnauthenticated},
		{"issuer forbids the token", http.StatusForbidden, ``, ErrUnauthenticated},
		{"200 with no subject", http.StatusOK, `{"email":"human@acme.com"}`, ErrUnauthenticated},
		{"200 with blank subject", http.StatusOK, `{"sub":"   "}`, ErrUnauthenticated},
		{"200 with a broken body", http.StatusOK, `not json`, ErrUnauthenticated},
		{"issuer errors", http.StatusInternalServerError, ``, ErrIdentityUnavailable},
		{"issuer rate-limits", http.StatusTooManyRequests, ``, ErrIdentityUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newUserinfoServer(t, tc.status, tc.body)
			resolver := &YscaleID{BaseURL: srv.URL}
			id, err := resolver.Resolve(context.Background(), "tok")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if id.Subject != "" {
				t.Errorf("a rejected token yielded an identity: %+v", id)
			}
		})
	}
}

// An empty token is refused without ever calling the issuer — nothing to check,
// and an unauthenticated probe should not become upstream traffic.
func TestYscaleIDResolveRejectsEmptyTokenWithoutCallingIssuer(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK, `{"sub":"sub-1"}`)
	resolver := &YscaleID{BaseURL: srv.URL}
	for _, token := range []string{"", "   "} {
		if _, err := resolver.Resolve(context.Background(), token); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("Resolve(%q) err = %v, want ErrUnauthenticated", token, err)
		}
	}
	if srv.gotAuth != "" {
		t.Errorf("empty token still reached the issuer (auth=%q)", srv.gotAuth)
	}
}

// An unconfigured resolver never guesses a URL.
func TestYscaleIDResolveWithoutBaseURL(t *testing.T) {
	resolver := &YscaleID{}
	if _, err := resolver.Resolve(context.Background(), "tok"); !errors.Is(err, ErrIdentityUnavailable) {
		t.Fatalf("err = %v, want ErrIdentityUnavailable", err)
	}
}

// A hung issuer must not pin the request goroutine: the resolver's own timeout
// bounds the call even when the caller's context has none.
func TestYscaleIDResolveTimesOut(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked
	}))
	defer srv.Close()
	defer close(blocked)

	resolver := &YscaleID{BaseURL: srv.URL, Timeout: 50 * time.Millisecond}
	done := make(chan error, 1)
	go func() {
		_, err := resolver.Resolve(context.Background(), "tok")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrIdentityUnavailable) {
			t.Fatalf("err = %v, want ErrIdentityUnavailable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Resolve did not honor its timeout against a hung issuer")
	}
}

// A cancelled caller context aborts the userinfo call rather than outliving the
// request it belongs to.
func TestYscaleIDResolveHonorsCallerContext(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked
	}))
	defer srv.Close()
	defer close(blocked)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	resolver := &YscaleID{BaseURL: srv.URL, Timeout: 10 * time.Second}
	if _, err := resolver.Resolve(ctx, "tok"); !errors.Is(err, ErrIdentityUnavailable) {
		t.Fatalf("err = %v, want ErrIdentityUnavailable", err)
	}
}

// A hostile or broken issuer cannot feed central an unbounded body: the read is
// capped, so an oversized response fails to decode instead of being buffered.
func TestYscaleIDResolveCapsResponseSize(t *testing.T) {
	// Valid JSON whose padding claim runs well past the cap; truncation at the
	// limit makes it undecodable, which is a denial.
	body := fmt.Sprintf(`{"sub":"sub-1","pad":"%s"}`, strings.Repeat("x", maxUserinfoBytes+1024))
	srv := newUserinfoServer(t, http.StatusOK, body)
	resolver := &YscaleID{BaseURL: srv.URL}
	if _, err := resolver.Resolve(context.Background(), "tok"); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("err = %v, want ErrUnauthenticated (capped read)", err)
	}
}

// One credential in a hot loop throttles itself and nobody else. Without the
// per-credential bucket a single caller would spend the shared budget everyone
// draws from.
func TestResolveRateLimitsPerCredential(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK, `{"sub":"sub-1"}`)
	// Burst 1 per credential with no meaningful refill, and global headroom, so
	// only the per-credential bound can be what sheds.
	resolver := &YscaleID{
		BaseURL: srv.URL, Burst: 1, ResolvesPerSecond: 0.001,
		GlobalBurst: 100, GlobalResolvesPerSecond: 1000,
	}

	if _, err := resolver.Resolve(context.Background(), "tok_a"); err != nil {
		t.Fatalf("first call for tok_a: %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), "tok_a"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("repeat call for tok_a = %v, want ErrRateLimited", err)
	}
	// A different credential has its own budget — the exhausted one does not
	// starve it, and did not spend the global one on its way to being refused.
	if _, err := resolver.Resolve(context.Background(), "tok_b"); err != nil {
		t.Fatalf("tok_b was starved by tok_a's spent budget: %v", err)
	}
}

// The global bucket is the only bound that caps AGGREGATE requests onto the
// identity provider. Per-credential budgets bound each caller and nothing in
// total: rotating junk tokens buys a fresh full bucket per token, so without
// this an attacker's amplification is limited only by how fast they can send.
func TestResolveBoundsAggregateOutboundRate(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK, `{"sub":"sub-1"}`)
	// Per-credential headroom and a global burst of 5 with no meaningful
	// refill, so only the global bound can be what sheds.
	resolver := &YscaleID{
		BaseURL: srv.URL, Burst: 100, ResolvesPerSecond: 1000,
		GlobalBurst: 5, GlobalResolvesPerSecond: 0.001,
	}

	allowed := 0
	for i := range 50 {
		_, err := resolver.Resolve(context.Background(), fmt.Sprintf("junk-%d", i))
		switch {
		case err == nil:
			allowed++
		case !errors.Is(err, ErrRateLimited):
			t.Fatalf("junk-%d = %v, want nil or ErrRateLimited", i, err)
		}
	}
	if allowed != 5 {
		t.Fatalf("upstream calls from 50 distinct junk tokens = %d, want the 5 the global budget allows — "+
			"a per-credential bucket alone bounds nobody in aggregate", allowed)
	}
	if got := srv.calls.Load(); got != 5 {
		t.Fatalf("userinfo calls = %d, want 5", got)
	}
	// The shed decides nothing about a credential: it is load, not a rejection.
	if _, err := resolver.Resolve(context.Background(), "tok_real"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("call under a spent global budget = %v, want ErrRateLimited", err)
	}
}

// Distinct credentials inside the global allowance all get through: the global
// bucket is a ceiling on total load, not a per-caller quota of one.
func TestResolveAdmitsDistinctCredentialsWithinGlobalBudget(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK, `{"sub":"sub-1"}`)
	resolver := &YscaleID{
		BaseURL: srv.URL, Burst: 1, ResolvesPerSecond: 0.001,
		GlobalBurst: 10, GlobalResolvesPerSecond: 0.001,
	}
	for i := range 10 {
		if _, err := resolver.Resolve(context.Background(), fmt.Sprintf("human-%d", i)); err != nil {
			t.Fatalf("human-%d was shed inside the global allowance: %v", i, err)
		}
	}
}

// The semaphore bounds how many goroutines a slow issuer can pin here at once.
// It is a concurrency bound, not a rate one — which is exactly why it does not
// replace the global bucket.
func TestResolveBoundsConcurrency(t *testing.T) {
	release := make(chan struct{})
	var inflight atomic.Int32
	var peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := inflight.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		<-release
		inflight.Add(-1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"sub":"sub-1"}`))
	}))
	t.Cleanup(srv.Close)

	resolver := &YscaleID{
		BaseURL: srv.URL, MaxInflight: 2,
		Burst: 100, ResolvesPerSecond: 1000, GlobalBurst: 100, GlobalResolvesPerSecond: 1000,
	}
	var wg sync.WaitGroup
	shed := make(chan error, 8)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := resolver.Resolve(context.Background(), fmt.Sprintf("tok-%d", i)); err != nil {
				shed <- err
			}
		}()
	}
	// Let the two that won slots finish; the rest must already have been shed
	// rather than queued behind them.
	for inflight.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()
	close(shed)

	if got := peak.Load(); got > 2 {
		t.Fatalf("peak concurrent userinfo calls = %d, want at most the 2 configured", got)
	}
	for err := range shed {
		if !errors.Is(err, ErrRateLimited) {
			t.Fatalf("shed error = %v, want ErrRateLimited", err)
		}
	}
}

// The credential tracker cannot grow with the number of distinct tokens
// presented, and it must not pay a scan to stay bounded — the flood that fills
// it is the same traffic a per-miss scan would charge for. Both halves are
// checked here: a fixed ceiling, and eviction that takes exactly the
// least-recently-seen entry.
func TestResolveBoundsTrackedCredentials(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK, `{"sub":"sub-1"}`)
	resolver := &YscaleID{
		BaseURL: srv.URL, Burst: 1, ResolvesPerSecond: 0.001,
		GlobalBurst: 1000, GlobalResolvesPerSecond: 10000, MaxTrackedCredentials: 8,
	}

	for i := range 200 {
		if _, err := resolver.Resolve(context.Background(), fmt.Sprintf("junk-%d", i)); err != nil {
			t.Fatalf("junk-%d: %v", i, err)
		}
		if got := resolver.trackedCredentials(); got > 8 {
			t.Fatalf("tracked credentials = %d after %d tokens, want at most the 8 configured", got, i+1)
		}
	}
	if got := resolver.trackedCredentials(); got != 8 {
		t.Fatalf("tracked credentials = %d, want the tracker held at its 8-entry ceiling", got)
	}

	// Eviction is by recency, not by scanning for a victim: the last 8 tokens
	// seen are the 8 still filed, and the one before them is gone.
	for i := 192; i < 200; i++ {
		if !resolver.tracksDigest(sha256.Sum256([]byte(fmt.Sprintf("junk-%d", i)))) {
			t.Errorf("junk-%d was evicted though it is one of the 8 most recent", i)
		}
	}
	if resolver.tracksDigest(sha256.Sum256([]byte("junk-191"))) {
		t.Error("junk-191 is still tracked; the tracker is not evicting least-recently-seen first")
	}
}

// Nothing keyed on a credential may hold the credential. The buckets are keyed
// by SHA-256 digest, so the raw token is not reachable from the limiter even
// with a heap dump.
func TestResolveKeysBucketsByDigestNotToken(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK, `{"sub":"sub-1"}`)
	resolver := &YscaleID{BaseURL: srv.URL}
	if _, err := resolver.Resolve(context.Background(), "tok_secret"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !resolver.tracksDigest(sha256.Sum256([]byte("tok_secret"))) {
		t.Fatal("no bucket under the token's digest")
	}
	resolver.credMu.Lock()
	defer resolver.credMu.Unlock()
	for k := range resolver.creds {
		if strings.Contains(string(k[:]), "tok_secret") {
			t.Error("a bucket key contains the raw token")
		}
	}
}

// trackedCredentials and tracksDigest read the LRU under its lock, so a test
// can assert the tracker's size and contents without reaching into it.
func (y *YscaleID) trackedCredentials() int {
	y.credMu.Lock()
	defer y.credMu.Unlock()
	if y.credLRU == nil {
		return 0
	}
	if n := y.credLRU.Len(); n != len(y.creds) {
		panic(fmt.Sprintf("credential LRU and index disagree: list %d, map %d", n, len(y.creds)))
	}
	return len(y.creds)
}

func (y *YscaleID) tracksDigest(digest [sha256.Size]byte) bool {
	y.credMu.Lock()
	defer y.credMu.Unlock()
	_, ok := y.creds[digest]
	return ok
}

// A credential that is over its own rate is refused WITHOUT spending from the
// shared budget. Charging the aggregate bucket for a rejection would let one
// caller in a hot loop shed every signed-in human alongside itself — the
// per-credential bucket paying for the thing it exists to prevent.
func TestResolveRejectedCredentialDoesNotSpendGlobalBudget(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK, `{"sub":"sub-1"}`)
	// One token per credential, two in the aggregate, neither refilling: the
	// second call for tok_abuse can only be refused by its own bucket, and what
	// it does to the global one is then visible in tok_real's answer.
	resolver := &YscaleID{
		BaseURL: srv.URL, Burst: 1, ResolvesPerSecond: 0.001,
		GlobalBurst: 2, GlobalResolvesPerSecond: 0.001,
	}

	if _, err := resolver.Resolve(context.Background(), "tok_abuse"); err != nil {
		t.Fatalf("first call for tok_abuse: %v", err)
	}
	for i := range 5 {
		if _, err := resolver.Resolve(context.Background(), "tok_abuse"); !errors.Is(err, ErrRateLimited) {
			t.Fatalf("repeat %d for tok_abuse = %v, want ErrRateLimited", i, err)
		}
	}
	if _, err := resolver.Resolve(context.Background(), "tok_real"); err != nil {
		t.Fatalf("a signed-in human was shed by someone else's refused calls: %v", err)
	}
	if got := srv.calls.Load(); got != 2 {
		t.Fatalf("userinfo calls = %d, want 2 — a refused credential must not reach the issuer", got)
	}
}

// A digest the global budget refuses is never written down. Tracking it would
// make the tracker a lever: an attacker who cannot get an upstream call out of
// central could still churn junk through the LRU, evicting the entries of the
// credentials being throttled — including their own — and hand them a fresh
// full bucket.
func TestResolveDoesNotTrackGloballyShedCredentials(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK, `{"sub":"sub-1"}`)
	resolver := &YscaleID{
		BaseURL: srv.URL, Burst: 10, ResolvesPerSecond: 1000,
		GlobalBurst: 1, GlobalResolvesPerSecond: 0.001, MaxTrackedCredentials: 4,
	}

	if _, err := resolver.Resolve(context.Background(), "tok_human"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	for i := range 100 {
		junk := fmt.Sprintf("junk-%d", i)
		if _, err := resolver.Resolve(context.Background(), junk); !errors.Is(err, ErrRateLimited) {
			t.Fatalf("%s = %v, want ErrRateLimited from the spent global budget", junk, err)
		}
		if resolver.tracksDigest(sha256.Sum256([]byte(junk))) {
			t.Fatalf("%s was filed in the tracker though its call was never made", junk)
		}
	}
	if got := resolver.trackedCredentials(); got != 1 {
		t.Fatalf("tracked credentials = %d, want only the one that actually got through", got)
	}
	if !resolver.tracksDigest(sha256.Sum256([]byte("tok_human"))) {
		t.Fatal("the real credential was evicted by tokens that never reached the issuer")
	}
}

// The laundering move this ordering closes: spend a credential's budget, then
// churn junk until its own entry falls off the back of the LRU, and come back
// with a full bucket. Junk only enters the tracker if the aggregate budget paid
// for it, so a tracker sized above that budget cannot be churned through.
func TestResolveKeepsAnAbusiveCredentialThroughJunkChurn(t *testing.T) {
	srv := newUserinfoServer(t, http.StatusOK, `{"sub":"sub-1"}`)
	resolver := &YscaleID{
		BaseURL: srv.URL, Burst: 1, ResolvesPerSecond: 0.001,
		GlobalBurst: 4, GlobalResolvesPerSecond: 0.001, MaxTrackedCredentials: 8,
	}

	if _, err := resolver.Resolve(context.Background(), "tok_abuse"); err != nil {
		t.Fatalf("first call for tok_abuse: %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), "tok_abuse"); !errors.Is(err, ErrRateLimited) {
		t.Fatal("tok_abuse was not throttled by its own bucket")
	}
	for i := range 100 {
		if _, err := resolver.Resolve(context.Background(), fmt.Sprintf("junk-%d", i)); err != nil &&
			!errors.Is(err, ErrRateLimited) {
			t.Fatalf("junk-%d = %v", i, err)
		}
	}
	if !resolver.tracksDigest(sha256.Sum256([]byte("tok_abuse"))) {
		t.Fatal("the abusive credential was evicted by junk churn; its next call gets a fresh bucket")
	}
	if _, err := resolver.Resolve(context.Background(), "tok_abuse"); !errors.Is(err, ErrRateLimited) {
		t.Fatal("tok_abuse came back with a full bucket after churning junk through the tracker")
	}
	// Only what the aggregate budget paid for was ever filed: the one real
	// credential plus the 3 junk tokens its remaining tokens covered.
	if got := resolver.trackedCredentials(); got != 4 {
		t.Fatalf("tracked credentials = %d, want 4 — the tracker took entries the global budget never allowed", got)
	}
}

// The per-credential defaults are what ONE valid session is allowed, and adding
// an aggregate bucket must not quietly tighten them: a dashboard that polled
// happily before is not something the new bound is aimed at. The aggregate pair
// is separate and sits above them.
func TestIdentityLimiterDefaults(t *testing.T) {
	y := &YscaleID{}
	if y.rate() != 10 || y.burst() != 20 {
		t.Errorf("per-credential defaults = %v/s burst %v, want 10/s burst 20", y.rate(), y.burst())
	}
	if y.globalRate() != 20 || y.globalBurst() != 40 {
		t.Errorf("aggregate defaults = %v/s burst %v, want 20/s burst 40", y.globalRate(), y.globalBurst())
	}
	// And an explicit setting still wins over each default independently.
	tuned := &YscaleID{ResolvesPerSecond: 1, Burst: 2, GlobalResolvesPerSecond: 3, GlobalBurst: 4}
	if tuned.rate() != 1 || tuned.burst() != 2 || tuned.globalRate() != 3 || tuned.globalBurst() != 4 {
		t.Errorf("configured limits = %v/%v and %v/%v, want 1/2 and 3/4",
			tuned.rate(), tuned.burst(), tuned.globalRate(), tuned.globalBurst())
	}
}
