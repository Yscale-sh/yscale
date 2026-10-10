// yscale:proprietary

package meshpolicy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/central/internal/tailscale"
)

// errLostReceipt is the failure a central that dies — or whose durable write is
// refused — between the ACL POST and the record of it leaves behind. The rule
// exists on the tailnet; nothing in this process learned it.
var errLostReceipt = errors.New("receipt lost after the ACL POST")

// liveTailnet is a stand-in for the tailnet ACL API that actually KEEPS what is
// written to it. The canned-document server in package tailscale is enough to
// assert what one write carries; the defect here is about what a SEQUENCE of
// writes leaves behind, so the document has to survive between them.
type liveTailnet struct {
	mu         sync.Mutex
	doc        string
	postStatus int
	getStatus  int
	posts      int
	gets       int
	rev        int
}

func (l *liveTailnet) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/oauth/token"):
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"tok","expires_in":3600}`)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/acl"):
		l.mu.Lock()
		doc, rev, status := l.doc, l.rev, l.getStatus
		l.gets++
		l.mu.Unlock()
		if status != 0 && status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, "the tailnet API is down")
			return
		}
		w.Header().Set("Etag", `W/"v`+string(rune('0'+rev%10))+`"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, doc)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/acl"):
		body, _ := io.ReadAll(r.Body)
		l.mu.Lock()
		defer l.mu.Unlock()
		l.posts++
		if l.postStatus != 0 && l.postStatus != http.StatusOK {
			w.WriteHeader(l.postStatus)
			_, _ = io.WriteString(w, "the tailnet API is refusing writes")
			return
		}
		// The write LANDS here, which is the whole point: whether central lives
		// to hear about it is a separate question.
		l.doc = string(body)
		l.rev++
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// refuseWrites makes the API reject POSTs with status, or accept them again at 0.
func (l *liveTailnet) refuseWrites(status int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.postStatus = status
}

// refuseReads takes the whole ACL API down, or brings it back at 0. A recovery
// that cannot GET cannot see which of its proven unions is live, so it is the
// failure that comes BEFORE any selection — the one nothing may be spent on.
func (l *liveTailnet) refuseReads(status int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.getStatus = status
}

func (l *liveTailnet) postCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.posts
}

func (l *liveTailnet) getCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.gets
}

// rules decodes the document's acls as generic JSON, so an assertion sees what
// is really on the tailnet rather than this client's view of it.
func (l *liveTailnet) rules(t *testing.T) []map[string]any {
	t.Helper()
	l.mu.Lock()
	doc := l.doc
	l.mu.Unlock()
	var parsed struct {
		ACLs []map[string]any `json:"acls"`
	}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatalf("decode tailnet policy %s: %v", doc, err)
	}
	return parsed.ACLs
}

// srcsFor names the Src of every rule on the document carrying dst, in order.
func (l *liveTailnet) srcsFor(t *testing.T, dst string) [][]string {
	t.Helper()
	var out [][]string
	for _, rule := range l.rules(t) {
		dsts, _ := rule["dst"].([]any)
		if len(dsts) != 1 || dsts[0] != dst {
			continue
		}
		srcs, _ := rule["src"].([]any)
		set := make([]string, 0, len(srcs))
		for _, s := range srcs {
			str, _ := s.(string)
			set = append(set, str)
		}
		out = append(out, set)
	}
	return out
}

// operatorRule is a rule an operator authored with exactly central's action,
// proto and destination. Central never claimed its Src, so no write central
// makes may touch it — that is the whole reason ownership is proven by Src and
// not by destination.
var operatorRule = []string{"10.99.0.0/16"}

// lossyTailnet forwards to the REAL client and can then report the failure a
// central that died between the POST and its receipt leaves behind. The write
// has landed on the far side; this process just never got to record it.
type lossyTailnet struct {
	inner *tailscale.Client

	mu   sync.Mutex
	lose bool
}

func (l *lossyTailnet) ReplaceManagedKubeletACL(ctx context.Context, dst string, owned [][]string, src []string) error {
	err := l.inner.ReplaceManagedKubeletACL(ctx, dst, owned, src)
	l.mu.Lock()
	defer l.mu.Unlock()
	if err == nil && l.lose {
		return errLostReceipt
	}
	return err
}

// CollapseManagedKubeletACL loses its receipt the same way: the collapse really
// happened on the far side — the document is on the union it returned — and this
// process never got to record it.
func (l *lossyTailnet) CollapseManagedKubeletACL(ctx context.Context, dst string, pushed []string, claims [][]string) ([]string, error) {
	landed, err := l.inner.CollapseManagedKubeletACL(ctx, dst, pushed, claims)
	l.mu.Lock()
	defer l.mu.Unlock()
	if err == nil && l.lose {
		return nil, errLostReceipt
	}
	return landed, err
}

func (l *lossyTailnet) loseNextReceipt(lose bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lose = lose
}

// newLiveSharedReconciler wires a reconciler to a REAL tailscale client pointed
// at a tailnet that keeps what is written to it, seeded with an operator rule
// that merely looks like central's. Options are applied last, so a test that
// needs a worker which cannot run behind its back can lengthen the debounce.
func newLiveSharedReconciler(t *testing.T, opts ...Option) (*Reconciler, *state.Store, *liveTailnet, *lossyTailnet) {
	t.Helper()
	tn := &liveTailnet{doc: `{
	  "ssh": [{"action": "accept", "src": ["autogroup:member"], "dst": ["autogroup:self"], "users": ["root"]}],
	  "acls": [
	    {"action": "accept", "proto": "tcp", "src": ["10.99.0.0/16"], "dst": ["` + kubeletACLDst + `"]}
	  ]
	}`}
	srv := httptest.NewServer(tn)
	t.Cleanup(srv.Close)
	c, err := tailscale.New(tailscale.Config{ClientID: "id", ClientSecret: "secret", Tailnet: "acme.example", APIBase: srv.URL})
	if err != nil {
		t.Fatalf("tailscale.New: %v", err)
	}
	lossy := &lossyTailnet{inner: c}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := state.New()
	r := New(ctx, s, slog.New(slog.NewTextHandler(io.Discard, nil)),
		append([]Option{
			WithDebounce(time.Millisecond), WithBackoff(2 * time.Millisecond), WithMaxBackoff(20 * time.Millisecond),
		}, opts...)...)
	r.shared = lossy
	return r, s, tn, lossy
}

// managedSrcs is every Src on the tailnet's managed destination EXCEPT the
// operator's — i.e. what central left behind.
func managedSrcs(t *testing.T, tn *liveTailnet) [][]string {
	t.Helper()
	var out [][]string
	for _, src := range tn.srcsFor(t, kubeletACLDst) {
		if slices.Equal(src, operatorRule) {
			continue
		}
		out = append(out, src)
	}
	return out
}

// The audit defect, end to end. U1 is converged; the U2 write LANDS and its
// receipt is lost; the desired union then moves to U3 before the retry runs.
//
// With the recorded union as the only ownership proof, the retry arrived
// carrying U1 and U3 against a document holding U2 — matching neither — so the
// U2 rule survived every write after it and granted :10250 to CIDRs no tenant
// advertises, for as long as that tailnet existed. The write-ahead claim is what
// makes U2 still nameable at that point.
func TestSharedTailnetLostReceiptThenDesiredMovesLeavesExactlyTheLatest(t *testing.T) {
	r, s, tn, lossy := newLiveSharedReconciler(t)
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "a"})

	// U1 converges normally.
	u1 := []string{"10.42.0.0/16"}
	reportRoutes(t, s, "cust_a", "a-1", u1)
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("U1 reconcile: %v", err)
	}
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], u1) {
		t.Fatalf("after U1 the tailnet carries %v, want exactly %v", got, u1)
	}

	// U2's POST lands; the receipt does not.
	u2 := []string{"10.42.0.0/16", "10.96.0.0/12"}
	lossy.loseNextReceipt(true)
	reportRoutes(t, s, "cust_a", "a-2", []string{"10.96.0.0/12"})
	if err := pushShared(t, r, false); !errors.Is(err, errLostReceipt) {
		t.Fatalf("U2 reconcile err = %v, want the lost receipt", err)
	}
	lossy.loseNextReceipt(false)
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], u2) {
		t.Fatalf("the U2 write did not land on the tailnet: %v", got)
	}
	if got := s.SharedTailnetRouteIntent().Pushed; !slices.Equal(got, u1) {
		t.Fatalf("pushed union = %v, want it still at U1 — the receipt was lost", got)
	}
	// U2 is claimed, which is the only reason a later write can still name it.
	if !containsUnionIn(s.SharedTailnetRouteIntent().Proof(), u2) {
		t.Fatalf("proof = %v, want it to name the union whose receipt was lost %v",
			s.SharedTailnetRouteIntent().Proof(), u2)
	}

	// Desired moves AGAIN before the retry: this is the sequence that used to
	// orphan U2 permanently.
	u3 := []string{"10.43.0.0/16"}
	reportRoutes(t, s, "cust_a", "a-1", []string{"10.43.0.0/16"})
	reportRoutes(t, s, "cust_a", "a-2", []string{"10.43.0.0/16"})
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("U3 retry: %v", err)
	}

	got := managedSrcs(t, tn)
	if len(got) != 1 || !slices.Equal(got[0], u3) {
		t.Fatalf("the tailnet carries %v, want exactly one central rule with %v", got, u3)
	}
	for _, src := range got {
		if slices.Equal(src, u1) || slices.Equal(src, u2) {
			t.Fatalf("an orphaned central rule survived: %v", src)
		}
	}
	// The operator's identically-shaped rule was never claimed, so nothing here
	// was ever entitled to touch it.
	if !slices.ContainsFunc(tn.srcsFor(t, kubeletACLDst), func(s []string) bool { return slices.Equal(s, operatorRule) }) {
		t.Fatalf("the operator's rule was deleted: %v", tn.srcsFor(t, kubeletACLDst))
	}
	// Converged, so the proof collapses to the one rule that is really there.
	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 {
		t.Fatalf("claims = %v, want them collapsed once the write completed", intent.Claims)
	}
	if proof := intent.Proof(); len(proof) != 1 || !slices.Equal(proof[0], u3) {
		t.Fatalf("proof = %v, want exactly the converged rule %v", proof, u3)
	}
}

// A refused ACL POST retains the claim — the request may have been applied and
// the response lost — and the retry converges with it. The proof stays BOUNDED
// while it does: one entry per distinct union, not one per attempt.
func TestSharedTailnetFailedPostRetainsBoundedProofThenCollapses(t *testing.T) {
	r, s, tn, _ := newLiveSharedReconciler(t)
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "a"})
	reportRoutes(t, s, "cust_a", "a-1", []string{"10.42.0.0/16"})

	tn.refuseWrites(http.StatusInternalServerError)
	for i := 0; i < 3; i++ {
		if err := pushShared(t, r, false); err == nil {
			t.Fatalf("attempt %d: expected the refused POST to be an error", i)
		}
	}
	if got := tn.postCount(); got != 3 {
		t.Fatalf("POSTs = %d, want one per attempt", got)
	}
	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 1 || !slices.Equal(intent.Claims[0], []string{"10.42.0.0/16"}) {
		t.Fatalf("claims = %v, want exactly one entry for the one union attempted", intent.Claims)
	}
	if len(intent.Pushed) != 0 {
		t.Fatalf("pushed = %v, want it unmoved by a failed write", intent.Pushed)
	}

	tn.refuseWrites(0)
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("retry after the API recovered: %v", err)
	}
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], []string{"10.42.0.0/16"}) {
		t.Fatalf("the tailnet carries %v, want the one converged rule", got)
	}
	intent = s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 {
		t.Fatalf("claims = %v, want them collapsed by the successful write", intent.Claims)
	}
	if !slices.Equal(intent.Pushed, []string{"10.42.0.0/16"}) {
		t.Fatalf("pushed = %v, want the union that landed", intent.Pushed)
	}
}

// The proof set is bounded, and reaching that bound never makes room by
// forgetting. Evicting the oldest claim was the wrong trade: that union is in the
// set because its POST may have landed, so giving it up leaves a rule central may
// have written that no later replace can match — granting :10250 to CIDRs no
// tenant advertises for as long as the tailnet exists.
//
// So the DESIRED union is never written at the cap: it cannot be claimed, and an
// unclaimed POST is exactly that orphan. The one call the cap does issue is the
// COLLAPSE, which needs no claim because it only ever leaves behind a union the
// document is already carrying and only ever removes rules the proof set covers
// (cleanSharedTailnetProof). Here the API is refusing writes, so even that fails
// — and every claim survives it.
func TestSharedTailnetProofCapNeverWritesTheUnclaimedUnion(t *testing.T) {
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "a"})
	// Every POST refused and the desired union moving each time: the only way the
	// proof set fills at all.
	m.fail(errBoom)

	var claims [][]string
	capped := false
	for i := 0; i < 4*maxProofFillAttempts && !capped; i++ {
		desired := []string{fmt.Sprintf("10.%d.0.0/16", 100+i)}
		reportRoutes(t, s, "cust_a", "a-1", desired)
		before, posts := s.SharedTailnetRouteIntent().Claims, m.count()
		err := pushShared(t, r, false)
		if err == nil {
			t.Fatalf("attempt %d: a refused ACL write reported success", i)
		}
		claims = s.SharedTailnetRouteIntent().Claims
		if len(claims) > len(before) {
			continue // still filling
		}
		capped = true
		if containsUnion(claims, desired) {
			t.Fatalf("the refused union %v was claimed anyway; the cap is what makes it unwritable", desired)
		}
		// One call, and it is the COLLAPSE: no union is handed to it to install,
		// because which one the document can honestly be left on is a question only
		// the document answers. What it is handed is the whole proof set, as
		// evidence of every rule it may have to remove.
		if got := m.count(); got != posts+1 {
			t.Fatalf("ACL calls %d -> %d at the cap, want exactly the one collapse", posts, got)
		}
		cleanup := m.last()
		if !cleanup.collapse {
			t.Fatalf("the cap issued a plain replace carrying %v; only a collapse may write without a claim", cleanup.src)
		}
		if slices.Equal(cleanup.src, desired) {
			t.Fatalf("the desired union %v was POSTed without a claim; that rule could never be taken back", desired)
		}
		for _, union := range before {
			if !cleanup.proves(union) {
				t.Fatalf("collapse owned = %v, want every claim %v as evidence — it may have to remove that rule", cleanup.owned, union)
			}
		}
		// The cleanup itself was refused, so nothing may be given up for it.
		for _, union := range before {
			if !containsUnion(claims, union) {
				t.Fatalf("claim %v was dropped to admit a new one; the rule it may name is now unprovable", union)
			}
		}
		if got := s.SharedTailnetRouteIntent().Pushed; len(got) != 0 {
			t.Fatalf("pushed = %v, want it unmoved by a cleanup that failed", got)
		}
	}
	if !capped {
		t.Fatal("the proof set never reached its cap")
	}

	// Every union claimed on the way here is still evidence: the write that
	// finally lands carries the whole set, which is what a rule any of those POSTs
	// may have installed is matched and taken back by.
	m.fail(nil)
	reportRoutes(t, s, "cust_a", "a-1", claims[0])
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("convergence on an already-claimed union: %v", err)
	}
	landed := m.last()
	for _, union := range claims {
		if !landed.proves(union) {
			t.Fatalf("the write carried %v as proof set; %v is missing, so a rule carrying it could never be taken back", landed.owned, union)
		}
	}
	if got := s.SharedTailnetRouteIntent().Claims; len(got) != 0 {
		t.Fatalf("claims = %v, want the completed write to collapse them", got)
	}
}

// maxProofFillAttempts bounds the fill loop above. state's cap is unexported, so
// the test finds it by growing the set until it stops — this only stops a bug
// there from hanging the test.
const maxProofFillAttempts = 16

// containsUnion reports whether set already holds union.
func containsUnion(set [][]string, union []string) bool {
	return slices.ContainsFunc(set, func(u []string) bool { return slices.Equal(u, union) })
}

// An operator rule with central's exact action, proto and destination but a Src
// central never claimed survives every write central makes — including the
// removal of central's own rule, where an empty desired union means the write
// exists only to delete.
func TestSharedTailnetUnclaimedOperatorRuleSurvivesTheRemoval(t *testing.T) {
	r, s, tn, _ := newLiveSharedReconciler(t)
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "a"})
	reportRoutes(t, s, "cust_a", "a-1", []string{"10.42.0.0/16"})
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("seed reconcile: %v", err)
	}

	if err := s.RevokeCustomer("cust_a"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("removal reconcile: %v", err)
	}

	srcs := tn.srcsFor(t, kubeletACLDst)
	if len(srcs) != 1 || !slices.Equal(srcs[0], operatorRule) {
		t.Fatalf("the managed destination carries %v, want the operator's rule alone", srcs)
	}
	intent := s.SharedTailnetRouteIntent()
	if len(intent.Proof()) != 0 {
		t.Fatalf("proof = %v, want it empty once the rule is removed", intent.Proof())
	}
}

// The sharpest form of a lost receipt: the desired union comes BACK to what the
// store already records as pushed. Desired and pushed agree, so the gate says
// converged and every write stops — while the document is still carrying the
// rule the lost write installed. An outstanding claim is what makes that gate
// refuse to fire, and one more write is what takes the orphan back.
func TestSharedTailnetLostReceiptThenDesiredReturnsStillReclaimsTheRule(t *testing.T) {
	r, s, tn, lossy := newLiveSharedReconciler(t)
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "a"})

	u1 := []string{"10.42.0.0/16"}
	reportRoutes(t, s, "cust_a", "a-1", u1)
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("U1 reconcile: %v", err)
	}

	lossy.loseNextReceipt(true)
	reportRoutes(t, s, "cust_a", "a-2", []string{"10.96.0.0/12"})
	if err := pushShared(t, r, false); !errors.Is(err, errLostReceipt) {
		t.Fatalf("U2 reconcile err = %v, want the lost receipt", err)
	}
	lossy.loseNextReceipt(false)

	// Back to U1 — which the store already calls pushed.
	reportRoutes(t, s, "cust_a", "a-2", u1)
	intent := s.SharedTailnetRouteIntent()
	if !slices.Equal(intent.Desired, intent.Pushed) {
		t.Fatalf("desired %v / pushed %v, want the gate's converged shape", intent.Desired, intent.Pushed)
	}
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("reconcile after the union returned: %v", err)
	}

	got := managedSrcs(t, tn)
	if len(got) != 1 || !slices.Equal(got[0], u1) {
		t.Fatalf("the tailnet carries %v, want exactly the one rule %v — the orphan was never taken back", got, u1)
	}
	if len(s.SharedTailnetRouteIntent().Claims) != 0 {
		t.Fatalf("claims = %v, want them collapsed", s.SharedTailnetRouteIntent().Claims)
	}
}

// Two shared tenants advertising the SAME CIDRs share one rule, so the second
// one's routes are already granted the moment it reports them: the cross-tenant
// union has not moved, and there is no ACL write for its bookkeeping to ride on.
//
// Its own convergence state still has to advance. Gated on a write that will
// never come, the second tenant's pushed union stayed empty for good — so every
// change re-asked the shared worker for nothing, and its deleted clusters'
// withdrawal tombstones, which may only drop once the shared document has caught
// up, could never be dropped at all.
func TestSharedTailnetIdenticalCIDRsConvergeBothTenantsWithOneWrite(t *testing.T) {
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "a"})
	s.AddCustomer(&state.Customer{ID: "cust_b", Token: "b"})

	// A converges the shared document first.
	shared := []string{"10.42.0.0/16"}
	reportRoutes(t, s, "cust_a", "a-1", shared)
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("cust_a reconcile: %v", err)
	}
	if m.count() != 1 {
		t.Fatalf("ACL writes = %d, want 1", m.count())
	}

	// B then reports the SAME CIDRs, and separately owes a withdrawal tombstone
	// from a cluster it deleted.
	owner := tenantOwner(t, s, "cust_b")
	reportRoutes(t, s, "cust_b", "b-1", shared)
	reportRoutes(t, s, "cust_b", "b-2", shared)
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_b", "b-2", shared); err != nil {
		t.Fatalf("seed cust_b's tombstone: %v", err)
	}
	deleteCluster(t, s, "cust_b", owner, "b-2")
	if got := intentOf(s, "cust_b").PushedUnion; len(got) != 0 {
		t.Fatalf("cust_b PushedGatewayRoutes = %v, want it stale before the reconcile", got)
	}

	r.Enqueue("cust_b", "b-1", false)
	r.EnqueueClusterRemoval("cust_b", "b-2")
	waitFor(t, func() bool { return slices.Equal(intentOf(s, "cust_b").PushedUnion, shared) },
		"cust_b's pushed union to catch up to the document that already carries its CIDRs")
	waitFor(t, func() bool {
		_, ok := intentOf(s, "cust_b").PushedByCluster["b-2"]
		return !ok
	}, "cust_b's withdrawal tombstone to drop")

	if got := m.count(); got != 1 {
		t.Fatalf("ACL writes = %d, want 1 — the union never moved, so there was nothing to write", got)
	}
	if got := intentOf(s, "cust_a").PushedUnion; !slices.Equal(got, shared) {
		t.Fatalf("cust_a PushedGatewayRoutes = %v, want %v", got, shared)
	}

	// And with both converged, a further reconcile of either asks for nothing.
	if err := r.Reconcile(t.Context(), "cust_b", []string{"b-1"}, false); err != nil {
		t.Fatalf("settled cust_b reconcile: %v", err)
	}
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("settled shared reconcile: %v", err)
	}
	if got := m.count(); got != 1 {
		t.Fatalf("ACL writes after both tenants settled = %d, want 1", got)
	}
}

// containsUnionIn is the assertion form of the proof-set membership the replace
// makes: does this proof name that union.
func containsUnionIn(proof [][]string, want []string) bool {
	return slices.ContainsFunc(proof, func(u []string) bool { return slices.Equal(u, want) })
}
