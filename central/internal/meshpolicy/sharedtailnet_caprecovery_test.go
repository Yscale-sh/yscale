// yscale:proprietary

package meshpolicy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// fillProofCapWithLostReceipts drives the shared document into exactly the state
// the recovery exists for: a proof set standing at its cap, a stale central rule
// on the tailnet that nothing durable names, and one more desired union that
// cannot be claimed.
//
// Every push here LANDS on the tailnet and loses its receipt, which is the only
// way a real fleet fills the set — the claim stays because that POST may have
// been applied, and the pushed union does not move because central never heard
// that it was. The document therefore ends up carrying the LAST union written,
// while the recorded pushed union is still whatever converged before all this.
//
// The cap is unexported in state, so it is found rather than assumed: the loop
// claims each union itself and stops on the first one the store refuses, which it
// returns. That probe IS the claim — the push behind it re-claims the same union
// idempotently — so the loop fills the set rather than merely measuring it. The
// lossy client is made whole on the way out, so a caller decides for itself what
// fails next.
func fillProofCapWithLostReceipts(t *testing.T, r *Reconciler, s *state.Store, lossy *lossyTailnet) []string {
	t.Helper()
	for i := 0; i < 4*maxProofFillAttempts; i++ {
		desired := []string{fmt.Sprintf("10.%d.0.0/16", 110+i)}
		reportRoutes(t, s, "cust_a", "a-1", desired)
		if _, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent()); errors.Is(err, state.ErrSharedTailnetClaimsFull) {
			lossy.loseNextReceipt(false)
			return desired
		}
		lossy.loseNextReceipt(true)
		if err := pushShared(t, r, false); !errors.Is(err, errLostReceipt) {
			t.Fatalf("fill attempt %d: err = %v, want the lost receipt", i, err)
		}
	}
	t.Fatal("the proof set never reached its cap")
	return nil
}

// writeRefuser is the half of an ACL API stand-in this fill drives: writes off
// while the claims accumulate, back on for whatever the caller does next.
type writeRefuser interface{ refuseWrites(status int) }

// fillProofCapWithRefusedWrites reaches the same cap the other way a real fleet
// does: an ACL API refusing writes while the tenants keep changing their routes.
// Every union is claimed durably — the claim is written BEFORE the POST — and
// none of them lands, so the document is left carrying whatever converged before
// the outage and the NEWEST claim is one the tailnet has never seen.
//
// That is the shape the recovery has to write its way out of. The lost-receipt
// fill above leaves the opposite one, where the document already carries the
// union the cleanup would install and the replace stops at the GET.
//
// Writes are accepted again on the way out, so a caller decides for itself what
// fails next. It returns the union that could not be claimed and the full proof
// set, newest last.
//
// The cap is found by CLAIMING before each pass rather than by watching the claim
// count across one: a pass that reaches the cap runs the recovery, and the
// recovery is entitled to retire claims the document is proven not to be carrying
// — which is exactly this set — so the count moves either way. That probe is not a
// read: it writes the claim, and the refused push behind it re-claims the same
// union idempotently.
func fillProofCapWithRefusedWrites(t *testing.T, r *Reconciler, s *state.Store, tn writeRefuser) (desired []string, claims [][]string) {
	t.Helper()
	tn.refuseWrites(http.StatusInternalServerError)
	for i := 0; i < 4*maxProofFillAttempts; i++ {
		desired = []string{fmt.Sprintf("10.%d.0.0/16", 150+i)}
		reportRoutes(t, s, "cust_a", "a-1", desired)
		if _, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent()); errors.Is(err, state.ErrSharedTailnetClaimsFull) {
			tn.refuseWrites(0)
			return desired, claims
		}
		if err := pushShared(t, r, false); err == nil {
			t.Fatalf("fill attempt %d: a refused ACL write reported success", i)
		}
		claims = s.SharedTailnetRouteIntent().Claims
	}
	t.Fatal("the proof set never reached its cap")
	return nil, nil
}

// seedConvergedUnion converges one union normally, so the recovery has a real
// durable pushed union to clean back to rather than an empty one.
func seedConvergedUnion(t *testing.T, r *Reconciler, s *state.Store, union []string) {
	t.Helper()
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "a"})
	reportRoutes(t, s, "cust_a", "a-1", union)
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("seed convergence on %v: %v", union, err)
	}
}

// sequencedTailnet records every ACL write the reconciler issues together with
// the DURABLE state the store held at the moment it went out.
//
// The recovery's guarantee is an ORDER — the cleanup, then the collapse of the
// claims onto what it installed, then the re-claim of the desired union, then the
// desired write — and half of that order is durable state that is gone by the
// time the pass returns. A transcript taken inside the client is where it is
// visible without racing anything.
type sequencedTailnet struct {
	inner tailnetEnsurer
	store *state.Store

	mu    sync.Mutex
	calls []sharedWrite
}

// sharedWrite is one ACL replace: what it carried, and what central could prove
// when it issued it. For the collapse, src is the union it SELECTED and returned
// — the document's answer, not the caller's.
type sharedWrite struct {
	src    []string
	owned  [][]string
	pushed []string
	claims [][]string
}

func (s *sequencedTailnet) ReplaceManagedKubeletACL(ctx context.Context, dst string, owned [][]string, src []string) error {
	intent := s.store.SharedTailnetRouteIntent()
	s.mu.Lock()
	s.calls = append(s.calls, sharedWrite{
		src: slices.Clone(src), owned: cloneUnions(owned), pushed: intent.Pushed, claims: intent.Claims,
	})
	s.mu.Unlock()
	return s.inner.ReplaceManagedKubeletACL(ctx, dst, owned, src)
}

// CollapseManagedKubeletACL is recorded AFTER it returns, because the union it
// installed is the thing under test and only the return value names it. The
// durable state is read first all the same: what the collapse was entitled to
// remove is what central could prove when it went out.
func (s *sequencedTailnet) CollapseManagedKubeletACL(ctx context.Context, dst string, pushed []string, claims [][]string) ([]string, error) {
	intent := s.store.SharedTailnetRouteIntent()
	landed, err := s.inner.CollapseManagedKubeletACL(ctx, dst, pushed, claims)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.calls = append(s.calls, sharedWrite{
		src:    slices.Clone(landed),
		owned:  cloneUnions(append([][]string{pushed}, claims...)),
		pushed: intent.Pushed,
		claims: intent.Claims,
	})
	s.mu.Unlock()
	return landed, nil
}

func (s *sequencedTailnet) writes() []sharedWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls)
}

// reset drops the transcript so far. The recorder is wired before the wedge is
// built — assigning it later would be a write to a field a worker goroutine may
// already be reading — so the setup's own writes are cleared here instead.
func (s *sequencedTailnet) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = nil
}

// assertRecoverySequence checks the whole transcript of a recovery pass: the
// cleanup write first, installing a union the proof set already named and
// carrying every proof as evidence, with nothing collapsed yet; the desired write
// second, issued only once that collapse was DURABLE and the desired union
// re-claimed against it.
func assertRecoverySequence(t *testing.T, writes []sharedWrite, full state.SharedTailnetIntent, target, desired []string) {
	t.Helper()
	if len(writes) != 2 {
		t.Fatalf("the recovery made %d writes, want the cleanup and then the desired one", len(writes))
	}
	cleanup, final := writes[0], writes[1]
	if !slices.Equal(cleanup.src, target) {
		t.Fatalf("the cleanup wrote %v, want the newest proven union %v", cleanup.src, target)
	}
	for _, union := range full.Proof() {
		if !containsUnionIn(cleanup.owned, union) {
			t.Fatalf("the cleanup's proof = %v, want it to name %v — it may have to remove that rule", cleanup.owned, union)
		}
	}
	if !slices.Equal(cleanup.pushed, full.Pushed) || len(cleanup.claims) != len(full.Claims) {
		t.Fatalf("the cleanup went out with pushed = %v and %d claims, want the full set intact: nothing may be collapsed before the write that earns it",
			cleanup.pushed, len(cleanup.claims))
	}
	if !slices.Equal(final.src, desired) {
		t.Fatalf("the second write carried %v, want the desired union %v", final.src, desired)
	}
	if !slices.Equal(final.pushed, target) {
		t.Fatalf("the desired write went out with pushed = %v, want the collapse onto %v recorded durably before it", final.pushed, target)
	}
	if len(final.claims) != 1 || !slices.Equal(final.claims[0], desired) {
		t.Fatalf("the desired write went out with claims = %v, want exactly its own re-claim of %v — an unclaimed POST could never be taken back",
			final.claims, desired)
	}
	if !containsUnionIn(final.owned, target) || !containsUnionIn(final.owned, desired) {
		t.Fatalf("the desired write's proof = %v, want the cleanup's union %v and its own claim %v", final.owned, target, desired)
	}
}

// The cap with the ACL API down OUTRIGHT — reads refused, not just writes — so
// the recovery cannot even see which of its proven unions the document is
// carrying. That is the failure that comes before any selection, and nothing may
// be spent on it: every claim stays, the pushed union does not move, and nothing
// reports a convergence that has not happened.
func TestSharedTailnetProofCapRecoveryKeepsEveryProofWhileTheAPIIsDown(t *testing.T) {
	r, s, tn, _ := newLiveSharedReconciler(t)
	u1 := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, u1)
	ninth, _ := fillProofCapWithRefusedWrites(t, r, s, tn)

	full := s.SharedTailnetRouteIntent()
	stale := managedSrcs(t, tn)
	if len(stale) != 1 || !slices.Equal(stale[0], u1) {
		t.Fatalf("the tailnet carries %v, want the union that converged before the outage %v", stale, u1)
	}

	tn.refuseWrites(http.StatusInternalServerError)
	tn.refuseReads(http.StatusInternalServerError)
	posts, gets := tn.postCount(), tn.getCount()
	for i := 0; i < 3; i++ {
		if err := pushShared(t, r, false); err == nil {
			t.Fatalf("attempt %d: a recovery against a dead API reported success", i)
		}
	}
	// The cleanup is ATTEMPTED every pass — a wedge that stopped trying would
	// never leave the cap once the API came back — and it never gets far enough
	// to write anything.
	if got := tn.getCount(); got != gets+3 {
		t.Fatalf("GETs %d -> %d over three attempts, want one refused collapse read each", gets, got)
	}
	if got := tn.postCount(); got != posts {
		t.Fatalf("POSTs %d -> %d; a collapse that could not read the document may not write one", posts, got)
	}

	after := s.SharedTailnetRouteIntent()
	if len(after.Claims) != len(full.Claims) {
		t.Fatalf("claims %d -> %d; a cleanup that never landed may not spend one", len(full.Claims), len(after.Claims))
	}
	for _, union := range full.Claims {
		if !containsUnion(after.Claims, union) {
			t.Fatalf("claim %v was dropped by a failed recovery; the rule it may name is now unprovable", union)
		}
	}
	if !slices.Equal(after.Pushed, u1) {
		t.Fatalf("pushed = %v, want it still at the union that really converged %v", after.Pushed, u1)
	}
	if r.sharedTailnetConverged() {
		t.Fatal("a wedged tailnet reported convergence; the ninth union is not on the document")
	}
	if containsUnionIn(after.Proof(), ninth) {
		t.Fatalf("proof = %v, want the unclaimable ninth union %v absent — nothing wrote it", after.Proof(), ninth)
	}
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], stale[0]) {
		t.Fatalf("the tailnet carries %v, want it untouched by refused writes (was %v)", got, stale)
	}
}

// The cap with the ACL API back, on a set filled by LOST RECEIPTS. The document
// already carries the newest claim — that is what a lost receipt leaves — so the
// cleanup back to it stops at the GET, and that GET is the evidence the collapse
// is made on: it proves no rule matching any other proof is on the document. The
// claims collapse against the union the tailnet is proven to hold, the desired
// union is claimed against that, and the one write that follows takes the stale
// rule with it. The document is left carrying exactly what the tenants asked for
// and nothing else of central's.
func TestSharedTailnetProofCapRecoveryConvergesTheDesiredUnion(t *testing.T) {
	r, s, tn, lossy := newLiveSharedReconciler(t)
	u1 := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, u1)
	ninth := fillProofCapWithLostReceipts(t, r, s, lossy)
	full := s.SharedTailnetRouteIntent()
	stale := managedSrcs(t, tn)

	posts := tn.postCount()
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("recovery pass: %v", err)
	}
	// One write: the cleanup had nothing to change, and the desired write removes
	// the stale rule on the strength of the proof set it carries. A second POST
	// here would be central re-writing a document that already said what it wanted.
	if got := tn.postCount(); got != posts+1 {
		t.Fatalf("POSTs %d -> %d, want the desired write alone — the cleanup's union was already on the document", posts, got)
	}

	got := managedSrcs(t, tn)
	if len(got) != 1 || !slices.Equal(got[0], ninth) {
		t.Fatalf("the tailnet carries %v, want exactly one central rule with the desired union %v", got, ninth)
	}
	for _, union := range append([][]string{u1, stale[0]}, full.Claims...) {
		if slices.Equal(union, ninth) {
			continue
		}
		if containsUnion(got, union) {
			t.Fatalf("a stale central rule survived the recovery: %v", union)
		}
	}
	// The operator's identically-shaped rule was never claimed, so neither write
	// was entitled to touch it.
	if !slices.ContainsFunc(tn.srcsFor(t, kubeletACLDst), func(s []string) bool { return slices.Equal(s, operatorRule) }) {
		t.Fatalf("the operator's rule was deleted: %v", tn.srcsFor(t, kubeletACLDst))
	}

	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 {
		t.Fatalf("claims = %v, want them collapsed by the recovery and the write after it", intent.Claims)
	}
	if !slices.Equal(intent.Pushed, ninth) {
		t.Fatalf("pushed = %v, want the desired union that landed %v", intent.Pushed, ninth)
	}
	if !r.sharedTailnetConverged() {
		t.Fatal("the recovery finished without reporting convergence")
	}
	// And the cap is unblocked: a fresh distinct union claims normally again.
	reportRoutes(t, s, "cust_a", "a-1", []string{"10.250.0.0/16"})
	if _, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent()); err != nil {
		t.Fatalf("claim after the recovery: %v, want the cap unblocked", err)
	}
}

// The cleanup lands and the process never records it — the same lost-receipt
// failure the claims exist for, now on the recovery's own write. Nothing may be
// given up for it: the claims still cover the rule that write left, and the retry
// repeats the cleanup harmlessly, because the union it selects is the one the
// document is already carrying.
func TestSharedTailnetProofCapRecoverySurvivesALostCleanupReceipt(t *testing.T) {
	r, s, tn, lossy := newLiveSharedReconciler(t)
	u1 := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, u1)
	ninth, _ := fillProofCapWithRefusedWrites(t, r, s, tn)
	// The document never took any of the claims — every POST after the seed was
	// refused — so the only union the collapse can select is the recorded one.
	target := u1
	full := s.SharedTailnetRouteIntent()

	// The cleanup returns; its receipt does not.
	lossy.loseNextReceipt(true)
	if err := pushShared(t, r, false); !errors.Is(err, errLostReceipt) {
		t.Fatalf("cleanup pass err = %v, want the lost receipt", err)
	}
	lossy.loseNextReceipt(false)

	// The document is on the cleanup's target — that selection really happened —
	// and the claims that cover it are all still here, because nothing proved they
	// were spent.
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], target) {
		t.Fatalf("the tailnet carries %v, want the cleanup's union %v", got, target)
	}
	after := s.SharedTailnetRouteIntent()
	if len(after.Claims) != len(full.Claims) {
		t.Fatalf("claims %d -> %d; the collapse was never recorded, so none may be dropped", len(full.Claims), len(after.Claims))
	}
	for _, union := range full.Claims {
		if !containsUnion(after.Claims, union) {
			t.Fatalf("claim %v was dropped without a recorded collapse", union)
		}
	}
	if !slices.Equal(after.Pushed, u1) {
		t.Fatalf("pushed = %v, want it unmoved by the cleanup %v", after.Pushed, u1)
	}
	if r.sharedTailnetConverged() {
		t.Fatal("a tailnet still carrying the cleanup's union reported the desired one converged")
	}

	// The retry repeats the cleanup against a document that already carries its
	// union, which is why repeating it is safe — it stops at the GET — and then
	// finishes the job.
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("retry after the lost cleanup receipt: %v", err)
	}
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], ninth) {
		t.Fatalf("the tailnet carries %v, want the desired union %v", got, ninth)
	}
	if got := s.SharedTailnetRouteIntent(); len(got.Claims) != 0 || !slices.Equal(got.Pushed, ninth) {
		t.Fatalf("claims = %v / pushed = %v, want the set collapsed onto %v", got.Claims, got.Pushed, ninth)
	}
}

// A restart in the middle of the wedge. The claims are durable and the desired
// union is recomputed from live tenants, so nothing about the recovery needs the
// process that filled the set — and nothing needs an operator either. The boot
// replay is the only thing that runs, on the real worker, with the real ladder.
func TestSharedTailnetProofCapRecoveryResumesAfterARestart(t *testing.T) {
	r, s, tn, lossy := newLiveSharedReconciler(t)
	u1 := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, u1)
	ninth := fillProofCapWithLostReceipts(t, r, s, lossy)
	if len(s.SharedTailnetRouteIntent().Claims) == 0 {
		t.Fatal("the fill left no claims to resume from")
	}

	// The restart: a new reconciler over the durable record the old one left. Only
	// the replay is called — no operator step, no hand-driven push.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	booted := New(ctx, s, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithDebounce(time.Millisecond), WithBackoff(2*time.Millisecond), WithMaxBackoff(20*time.Millisecond))
	booted.shared = lossy
	if err := booted.ReplaySharedTailnet(ctx); err != nil {
		t.Fatalf("ReplaySharedTailnet: %v", err)
	}

	waitFor(t, func() bool {
		intent := s.SharedTailnetRouteIntent()
		return len(intent.Claims) == 0 && slices.Equal(intent.Pushed, ninth)
	}, "the boot replay to unwedge the proof set and converge the desired union")
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], ninth) {
		t.Fatalf("the tailnet carries %v, want exactly the desired union %v", got, ninth)
	}
	if !slices.ContainsFunc(tn.srcsFor(t, kubeletACLDst), func(s []string) bool { return slices.Equal(s, operatorRule) }) {
		t.Fatalf("the operator's rule was deleted: %v", tn.srcsFor(t, kubeletACLDst))
	}
}

// The recovery ends the churn rather than joining it. A wedged tailnet re-asked
// on every tenant change never wrote again; once it converges, the shared worker
// has nothing left to do — and the tenant bookkeeping the wedge was holding, the
// withdrawal tombstone included, is released only by the write that put the
// DESIRED union on the document.
func TestSharedTailnetProofCapRecoverySettlesTheWorkerAndTheTombstones(t *testing.T) {
	r, s, tn, lossy := newLiveSharedReconciler(t)
	u1 := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, u1)
	owner := tenantOwner(t, s, "cust_a")
	ninth := fillProofCapWithLostReceipts(t, r, s, lossy)

	// A cluster of this tenant is deleted while the document is wedged, leaving
	// the durable tombstone the withdrawal is driven from.
	reportRoutes(t, s, "cust_a", "a-2", ninth)
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_a", "a-2", ninth); err != nil {
		t.Fatalf("seed the tombstone: %v", err)
	}
	deleteCluster(t, s, "cust_a", owner, "a-2")

	withdraw := request{policy: true, clusters: []clusterRequest{{id: "a-2", withdraw: true}}}
	tn.refuseWrites(http.StatusInternalServerError)
	out := r.reconcile(context.Background(), "cust_a", withdraw)
	if out.err != nil {
		t.Fatalf("reconcile while wedged = %v, want the withdrawal held rather than failed", out.err)
	}
	if !slices.Equal(out.failedClusters, []string{"a-2"}) {
		t.Fatalf("requeued clusters = %v, want the held tombstone", out.failedClusters)
	}
	if _, ok := intentOf(s, "cust_a").PushedByCluster["a-2"]; !ok {
		t.Fatal("the tombstone was dropped while the shared document still granted its CIDRs")
	}

	// The API comes back and the shared worker runs the recovery on its own.
	tn.refuseWrites(0)
	r.EnqueueSharedTailnet(false)
	waitFor(t, func() bool {
		intent := s.SharedTailnetRouteIntent()
		return len(intent.Claims) == 0 && slices.Equal(intent.Desired, intent.Pushed)
	}, "the shared worker to converge the desired union")

	// Settled: a converged document is a gate the worker stops at, so the next
	// pass writes nothing at all.
	posts := tn.postCount()
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("post-convergence pass: %v", err)
	}
	if got := tn.postCount(); got != posts {
		t.Fatalf("POSTs %d -> %d after convergence; the worker is still churning", posts, got)
	}

	// And only NOW may the tombstone go: the same request that was held completes.
	out = r.reconcile(context.Background(), "cust_a", withdraw)
	if out.err != nil {
		t.Fatalf("reconcile after the recovery: %v", out.err)
	}
	if _, ok := intentOf(s, "cust_a").PushedByCluster["a-2"]; ok {
		t.Fatal("the tombstone survived a converged shared document")
	}
}

// The COMPLETE recovery, in order, with nothing left to timing. The set filled
// against an API that refused every write, so none of those claims ever reached
// the document: the collapse selects the one union that is really on it — the
// recorded pushed one — carrying every claim as evidence, the set collapses
// durably onto exactly that, the desired union is re-claimed against the
// collapse, and only then is it written. After it the worker has nothing left to
// do, and the tenant bookkeeping the wedge was holding — the withdrawal tombstone
// included — is finally released.
//
// Every step is observed where it happens: the transcript inside the ACL client
// carries the durable state each write went out with, so the ORDER is asserted
// rather than inferred from the end state.
func TestSharedTailnetProofCapRecoveryRunsTheCompleteSequence(t *testing.T) {
	// The debounce is an hour: the withdrawal below re-asks the shared worker, and
	// a worker that ran that request on its own ladder would be writing the
	// document alongside the pass this test is asserting.
	r, s, tn, lossy := newLiveSharedReconciler(t, WithDebounce(time.Hour))
	seq := &sequencedTailnet{inner: lossy, store: s}
	r.shared = seq
	u1 := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, u1)
	owner := tenantOwner(t, s, "cust_a")
	ninth, _ := fillProofCapWithRefusedWrites(t, r, s, tn)
	target := u1

	// A cluster of this tenant is deleted while the document is wedged, leaving the
	// durable tombstone the withdrawal is driven from.
	reportRoutes(t, s, "cust_a", "a-2", ninth)
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_a", "a-2", ninth); err != nil {
		t.Fatalf("seed the tombstone: %v", err)
	}
	deleteCluster(t, s, "cust_a", owner, "a-2")

	withdraw := request{policy: true, clusters: []clusterRequest{{id: "a-2", withdraw: true}}}
	tn.refuseWrites(http.StatusInternalServerError)
	out := r.reconcile(context.Background(), "cust_a", withdraw)
	if out.err != nil {
		t.Fatalf("reconcile while wedged = %v, want the withdrawal held rather than failed", out.err)
	}
	if _, ok := intentOf(s, "cust_a").PushedByCluster["a-2"]; !ok {
		t.Fatal("the tombstone was dropped while the shared document still granted its CIDRs")
	}

	// The API comes back, and ONE shared pass is the whole recovery.
	tn.refuseWrites(0)
	seq.reset()
	full := s.SharedTailnetRouteIntent()
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("recovery pass: %v", err)
	}
	assertRecoverySequence(t, seq.writes(), full, target, ninth)

	// Durably complete: the proof set collapsed onto the union that landed, and the
	// document carries that rule and nothing else of central's.
	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 || !slices.Equal(intent.Pushed, ninth) {
		t.Fatalf("claims = %v / pushed = %v, want the set collapsed onto the desired union %v", intent.Claims, intent.Pushed, ninth)
	}
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], ninth) {
		t.Fatalf("the tailnet carries %v, want exactly the desired union %v", got, ninth)
	}
	for _, union := range append([][]string{u1}, full.Claims...) {
		if slices.Equal(union, ninth) {
			continue
		}
		if containsUnionIn(managedSrcs(t, tn), union) {
			t.Fatalf("a stale central rule survived the recovery: %v", union)
		}
	}
	if !slices.ContainsFunc(tn.srcsFor(t, kubeletACLDst), func(s []string) bool { return slices.Equal(s, operatorRule) }) {
		t.Fatalf("the operator's rule was deleted: %v", tn.srcsFor(t, kubeletACLDst))
	}
	if !r.sharedTailnetConverged() {
		t.Fatal("the recovery finished without reporting convergence")
	}

	// The churn stops with it: a converged document is a gate the worker halts at,
	// so the next pass writes nothing at all.
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("post-convergence pass: %v", err)
	}
	if got := len(seq.writes()); got != 2 {
		t.Fatalf("the pass after convergence brought the write count to %d, want the worker settled at 2", got)
	}

	// And only NOW may the tombstone go: the same request that was held completes.
	out = r.reconcile(context.Background(), "cust_a", withdraw)
	if out.err != nil {
		t.Fatalf("reconcile after the recovery: %v", out.err)
	}
	if _, ok := intentOf(s, "cust_a").PushedByCluster["a-2"]; ok {
		t.Fatal("the tombstone survived a converged shared document")
	}
	// The cap is unblocked too: a fresh distinct union claims normally again.
	reportRoutes(t, s, "cust_a", "a-1", []string{"10.251.0.0/16"})
	if _, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent()); err != nil {
		t.Fatalf("claim after the recovery: %v, want the cap unblocked", err)
	}
}

// The same complete sequence, resumed from the FULL persisted claim set by a
// process that did not fill it. The claims and the pushed union are the durable
// record, and the desired union is recomputed from the live tenants — so a
// restart mid-wedge owes exactly the same two writes in the same order, with no
// operator step and nothing carried over in memory.
func TestSharedTailnetProofCapRecoveryRunsTheCompleteSequenceAfterARestart(t *testing.T) {
	r, s, tn, lossy := newLiveSharedReconciler(t)
	u1 := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, u1)
	ninth, claims := fillProofCapWithRefusedWrites(t, r, s, tn)
	target := u1

	// The restart: a new reconciler over the durable record the old one left.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	seq := &sequencedTailnet{inner: lossy, store: s}
	booted := New(ctx, s, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithDebounce(time.Millisecond), WithBackoff(2*time.Millisecond), WithMaxBackoff(20*time.Millisecond))
	booted.shared = seq

	full := s.SharedTailnetRouteIntent()
	if len(full.Claims) != len(claims) {
		t.Fatalf("the booted process reads %d claims, want the %d the wedge left durable", len(full.Claims), len(claims))
	}
	// force is what the boot replay enqueues; draining that pass synchronously is
	// the same work the shared worker would do, without the wait.
	if err := pushShared(t, booted, true); err != nil {
		t.Fatalf("boot recovery pass: %v", err)
	}
	assertRecoverySequence(t, seq.writes(), full, target, ninth)

	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 || !slices.Equal(intent.Pushed, ninth) {
		t.Fatalf("claims = %v / pushed = %v, want the set collapsed onto the desired union %v", intent.Claims, intent.Pushed, ninth)
	}
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], ninth) {
		t.Fatalf("the tailnet carries %v, want exactly the desired union %v", got, ninth)
	}
	if !slices.ContainsFunc(tn.srcsFor(t, kubeletACLDst), func(s []string) bool { return slices.Equal(s, operatorRule) }) {
		t.Fatalf("the operator's rule was deleted: %v", tn.srcsFor(t, kubeletACLDst))
	}
	if !booted.sharedTailnetConverged() {
		t.Fatal("the boot recovery finished without reporting convergence")
	}
	if err := pushShared(t, booted, false); err != nil {
		t.Fatalf("post-convergence pass: %v", err)
	}
	if got := len(seq.writes()); got != 2 {
		t.Fatalf("the pass after convergence brought the write count to %d, want the booted worker settled at 2", got)
	}
}

// The window between the recovery's two writes — the one thing it cannot make
// atomic. The desired union may not go out first (it is the union that could not
// be claimed, and an unclaimed POST is the orphan the whole scheme prevents), so
// the document carries the collapse's target until the second write lands. Here
// that second write fails, which is exactly when the CHOICE of target is worth
// something: the shared fleet keeps the kubelet grant the document was already
// making, rather than losing it to an empty document or gaining one from a claim
// that never landed. A later pass then converges it, with no cleanup left to do.
func TestSharedTailnetProofCapRecoveryHoldsAProvenGrantWhenTheDesiredWriteFails(t *testing.T) {
	r, s, tn, _ := newLiveSharedReconciler(t)
	u1 := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, u1)
	ninth, _ := fillProofCapWithRefusedWrites(t, r, s, tn)
	target := u1

	// The collapse selects what the document already carries, so it needs no write
	// of its own; the desired write right behind it is refused.
	tn.refuseWrites(http.StatusInternalServerError)
	posts := tn.postCount()
	if err := pushShared(t, r, false); err == nil {
		t.Fatal("a refused desired write reported success")
	}
	if got := tn.postCount(); got != posts+1 {
		t.Fatalf("POSTs %d -> %d, want the refused desired write alone — the collapse's union was already on the document", posts, got)
	}

	// What the tailnet is left granting is the union it was already granting, and
	// not nothing.
	got := managedSrcs(t, tn)
	if len(got) != 1 || !slices.Equal(got[0], target) {
		t.Fatalf("the tailnet carries %v, want the proven collapse target %v held across the window", got, target)
	}
	intent := s.SharedTailnetRouteIntent()
	if !slices.Equal(intent.Pushed, target) {
		t.Fatalf("pushed = %v, want the cleanup's union %v recorded", intent.Pushed, target)
	}
	if len(intent.Claims) != 1 || !slices.Equal(intent.Claims[0], ninth) {
		t.Fatalf("claims = %v, want exactly the desired union's own claim — the retry needs it to take back a POST that may have landed", intent.Claims)
	}
	if r.sharedTailnetConverged() {
		t.Fatal("a document carrying the cleanup target reported the desired union converged")
	}

	// The wedge itself is over: the retry is an ordinary write, no cleanup needed.
	tn.refuseWrites(0)
	posts = tn.postCount()
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("retry after the API recovered: %v", err)
	}
	if got := tn.postCount(); got != posts+1 {
		t.Fatalf("POSTs %d -> %d on the retry, want the desired write alone — the proof set is no longer full", posts, got)
	}
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], ninth) {
		t.Fatalf("the tailnet carries %v, want the desired union %v", got, ninth)
	}
	if intent := s.SharedTailnetRouteIntent(); len(intent.Claims) != 0 || !slices.Equal(intent.Pushed, ninth) {
		t.Fatalf("claims = %v / pushed = %v, want the set collapsed onto %v", intent.Claims, intent.Pushed, ninth)
	}
	if !slices.ContainsFunc(tn.srcsFor(t, kubeletACLDst), func(s []string) bool { return slices.Equal(s, operatorRule) }) {
		t.Fatalf("the operator's rule was deleted: %v", tn.srcsFor(t, kubeletACLDst))
	}
}

// A cleanup that needs no write at all. The set filled through LOST RECEIPTS, so
// the document already carries the newest claim, and the replace stops at the GET
// when the rule it would install is the only central rule there. That GET is
// EVIDENCE and not a shortcut — it proves no rule matching any other proof is on
// the document, which is the whole of what those claims were being kept for — so
// the set may collapse onto it even while the API is refusing writes. The desired
// union, which still cannot be written, is claimed and left outstanding.
func TestSharedTailnetProofCapRecoveryCollapsesOnAProvenDocumentWithoutAWrite(t *testing.T) {
	r, s, tn, lossy := newLiveSharedReconciler(t)
	u1 := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, u1)
	ninth := fillProofCapWithLostReceipts(t, r, s, lossy)
	full := s.SharedTailnetRouteIntent()
	landed := full.Claims[len(full.Claims)-1]
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], landed) {
		t.Fatalf("the tailnet carries %v, want the last union whose receipt was lost %v", got, landed)
	}

	tn.refuseWrites(http.StatusInternalServerError)
	posts := tn.postCount()
	if err := pushShared(t, r, false); err == nil {
		t.Fatal("a refused desired write reported success")
	}
	if got := tn.postCount(); got != posts+1 {
		t.Fatalf("POSTs %d -> %d, want the desired write alone — the cleanup's union was already on the document", posts, got)
	}

	intent := s.SharedTailnetRouteIntent()
	if !slices.Equal(intent.Pushed, landed) {
		t.Fatalf("pushed = %v, want the union the GET proved the document carries %v", intent.Pushed, landed)
	}
	if len(intent.Claims) != 1 || !slices.Equal(intent.Claims[0], ninth) {
		t.Fatalf("claims = %v, want the collapse onto the proven union plus the desired union's own claim", intent.Claims)
	}
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], landed) {
		t.Fatalf("the tailnet carries %v, want it untouched by a recovery that wrote nothing", got)
	}
	if r.sharedTailnetConverged() {
		t.Fatal("a document that does not carry the desired union reported convergence")
	}

	// And the wedge is gone: with the API back, one ordinary write converges.
	tn.refuseWrites(0)
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("retry after the API recovered: %v", err)
	}
	if got := managedSrcs(t, tn); len(got) != 1 || !slices.Equal(got[0], ninth) {
		t.Fatalf("the tailnet carries %v, want the desired union %v", got, ninth)
	}
	if got := s.SharedTailnetRouteIntent(); len(got.Claims) != 0 || !slices.Equal(got.Pushed, ninth) {
		t.Fatalf("claims = %v / pushed = %v, want the set collapsed onto %v", got.Claims, got.Pushed, ninth)
	}
}

// A full proof set does not stand between the tenants and a WITHDRAWAL. With
// nothing left to grant, the desired union is empty — which is already provable,
// so it needs no claim, takes the cap's refusal out of the picture entirely, and
// the one write removes every rule the proof set names.
func TestSharedTailnetProofCapWithdrawsWhenTheTenantsAskForNothing(t *testing.T) {
	r, s, tn, _ := newLiveSharedReconciler(t)
	seedConvergedUnion(t, r, s, []string{"10.42.0.0/16"})
	if _, claims := fillProofCapWithRefusedWrites(t, r, s, tn); len(claims) == 0 {
		t.Fatal("the fill left no claims to withdraw against")
	}

	// The tenant is revoked: its access is gone, so it contributes nothing.
	if err := s.RevokeCustomer("cust_a"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	posts := tn.postCount()
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("withdrawal at the cap: %v", err)
	}
	if got := tn.postCount(); got != posts+1 {
		t.Fatalf("POSTs %d -> %d, want the withdrawal alone — an empty union is already proven, so there is no cleanup to make room for it", posts, got)
	}

	if got := managedSrcs(t, tn); len(got) != 0 {
		t.Fatalf("the tailnet still carries %v, want every rule central can prove it wrote gone", got)
	}
	if !slices.ContainsFunc(tn.srcsFor(t, kubeletACLDst), func(s []string) bool { return slices.Equal(s, operatorRule) }) {
		t.Fatalf("the operator's rule was deleted: %v", tn.srcsFor(t, kubeletACLDst))
	}
	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 || len(intent.Pushed) != 0 || len(intent.Proof()) != 0 {
		t.Fatalf("claims = %v / pushed = %v, want nothing left to prove once the rule is gone", intent.Claims, intent.Pushed)
	}
	if !r.sharedTailnetConverged() {
		t.Fatal("the withdrawal finished without reporting convergence")
	}
}

// The recovery's fail-safe choice, asserted end to end: a claim whose POST was
// never accepted names CIDRs the tailnet has never granted, and the recovery may
// not be the thing that grants them. The set here filled against an API refusing
// every write, so EVERY claim in it is in exactly that position — and the union
// the collapse leaves on the document is the one central has a completed write
// for, which is also the only one that is really there.
//
// This is the whole defect the live selection exists for. Choosing the newest
// claim from the durable record — the shape that reads most like "as close to
// what the tenants want as the evidence allows" — would have opened :10250 to a
// union the document never carried, for the length of the recovery window.
func TestSharedTailnetProofCapRecoveryNeverInstallsAClaimTheDocumentLacks(t *testing.T) {
	r, s, tn, _ := newLiveSharedReconciler(t)
	u1 := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, u1)
	_, claims := fillProofCapWithRefusedWrites(t, r, s, tn)
	if len(claims) < 2 {
		t.Fatalf("the fill left %d claims, want a set with a newest one to prefer", len(claims))
	}

	// Only the collapse runs: the desired write behind it is refused, so what the
	// document is left carrying is the collapse's choice and nothing else.
	tn.refuseWrites(http.StatusInternalServerError)
	if err := pushShared(t, r, false); err == nil {
		t.Fatal("a refused desired write reported success")
	}

	got := managedSrcs(t, tn)
	if len(got) != 1 || !slices.Equal(got[0], u1) {
		t.Fatalf("the tailnet carries %v, want the one union a completed write put there %v", got, u1)
	}
	for _, claim := range claims {
		if containsUnionIn(got, claim) {
			t.Fatalf("the recovery installed %v, a claim whose write the API refused — it was never on this document", claim)
		}
	}
	if intent := s.SharedTailnetRouteIntent(); !slices.Equal(intent.Pushed, u1) {
		t.Fatalf("pushed = %v, want the union the collapse actually landed %v", intent.Pushed, u1)
	}
}
