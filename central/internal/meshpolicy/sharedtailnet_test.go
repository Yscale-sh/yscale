// yscale:proprietary

package meshpolicy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/factory/client"
)

// errBoom is a sentinel shared-tailnet push failure used to exercise the
// no-advance-on-failure contract.
var errBoom = errors.New("boom")

// tailnetEnsureCall records one ReplaceManagedKubeletACL invocation against the
// shared tailnet client. It lives here (not in the external _test package)
// because the seam it exercises — tailnetEnsurer — is unexported, and the field
// it sets (Reconciler.shared) is unexported.
type tailnetEnsureCall struct {
	dst string
	// owned is the ownership PROOF SET the write carried: every union central
	// could still have standing on the shared document.
	owned [][]string
	src   []string
	// collapse marks the cap recovery's own write, whose src is the union it
	// selected and returned rather than one the caller handed it.
	collapse bool
}

// proves reports whether this write carried union as evidence — i.e. whether a
// rule on the document carrying that Src would have been recognised as central's
// and replaced.
func (c tailnetEnsureCall) proves(union []string) bool {
	return slices.ContainsFunc(c.owned, func(u []string) bool { return slices.Equal(u, union) })
}

// mockTailnetEnsurer is written from the shared worker's goroutine and read from
// the test's, so every accessor takes the lock — a race here would be the test
// harness's, not the reconciler's.
type mockTailnetEnsurer struct {
	mu    sync.Mutex
	calls []tailnetEnsureCall
	err   error
}

func (m *mockTailnetEnsurer) ReplaceManagedKubeletACL(_ context.Context, dst string, owned [][]string, src []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, tailnetEnsureCall{dst: dst, owned: cloneUnions(owned), src: slices.Clone(src)})
	return m.err
}

// CollapseManagedKubeletACL stands in for the recovery's own write. The real one
// selects its target from the DOCUMENT; a mock that holds no document can only
// answer with the union it was told central last completed a write for, which is
// the same preference the live selection applies first.
func (m *mockTailnetEnsurer) CollapseManagedKubeletACL(_ context.Context, dst string, pushed []string, claims [][]string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, tailnetEnsureCall{
		dst: dst, owned: cloneUnions(append([][]string{pushed}, claims...)), src: slices.Clone(pushed), collapse: true,
	})
	if m.err != nil {
		return nil, m.err
	}
	return slices.Clone(pushed), nil
}

// cloneUnions deep-copies a proof set so a later write cannot edit a recorded
// call out from under an assertion.
func cloneUnions(in [][]string) [][]string {
	out := make([][]string, 0, len(in))
	for _, u := range in {
		out = append(out, slices.Clone(u))
	}
	return out
}

func (m *mockTailnetEnsurer) fail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

func (m *mockTailnetEnsurer) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockTailnetEnsurer) at(i int) tailnetEnsureCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[i]
}

// last is the most recent replace, or a zero call when none has been made.
func (m *mockTailnetEnsurer) last() tailnetEnsureCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return tailnetEnsureCall{}
	}
	return m.calls[len(m.calls)-1]
}

// lastSrc is the source set the most recent replace carried.
func (m *mockTailnetEnsurer) lastSrc() []string { return m.last().src }

func newSharedReconciler(t *testing.T, m *mockTailnetEnsurer, opts ...Option) (*Reconciler, *state.Store) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := state.New()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Default Headscale factory; unused on the shared path.
	r := New(ctx, s, log, append([]Option{
		WithDebounce(time.Millisecond),
		WithBackoff(2 * time.Millisecond),
		WithMaxBackoff(20 * time.Millisecond),
	}, opts...)...)
	r.shared = m // wire the shared tailnet ensurer
	return r, s
}

// pushShared drives one shared-tailnet reconcile synchronously, the way the
// shared worker would drain it. Tests that are about WHAT is written use this;
// the ones about how the work gets there go through Enqueue and wait.
func pushShared(t *testing.T, r *Reconciler, force bool) error {
	t.Helper()
	return r.reconcileSharedTailnet(context.Background(), request{policy: true, policyForce: force}).err
}

// #38: a SaaS customer (no per-customer mesh box) with the shared tailnet client
// wired must get the kubelet-transparency ACL pushed — the fix for
// `kubectl logs`/`exec`/`port-forward` to burst pods timing out on the default
// shared tailnet. This is the path Reconcile used to no-op entirely.
//
// The assertion locks the scope of the write: it names the ONE destination
// central manages (the burst/agent tag on :10250 exclusively) and carries the
// desired CIDRs as its source, so nothing else on that multi-tenant document is
// in the blast radius.
func TestReconcileSharedTailnetKubeletACL(t *testing.T) {
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_saas", Token: "saas"})
	reportRoutes(t, s, "cust_saas", "prod-1", []string{"10.42.0.0/16", "10.96.0.0/12"})

	if err := pushShared(t, r, true); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if m.count() != 1 {
		t.Fatalf("ReplaceManagedKubeletACL calls = %d, want 1", m.count())
	}
	call := m.at(0)

	wantSrc := []string{"10.42.0.0/16", "10.96.0.0/12"}
	if !slices.Equal(call.src, wantSrc) {
		t.Errorf("src = %v, want %v (exact validated desired CIDRs)", call.src, wantSrc)
	}
	// On a FIRST write the only thing central can prove is the union it is about
	// to install — the write-ahead claim it just took. Nothing else may be in the
	// proof set, or an operator's rule would be in the blast radius.
	if len(call.owned) != 1 || !call.proves(wantSrc) {
		t.Errorf("owned = %v, want exactly the claimed union %v — central has written nothing else to prove", call.owned, wantSrc)
	}
	if call.dst != handlers.AgentTailnetTag+":10250" {
		t.Fatalf("dst = %q, want the burst/agent tag on kubelet port ONLY", call.dst)
	}
	// Regression guards mirroring TestBuildPolicyKubeletACLSrcDst: the shared
	// path must never broaden the rule to all ports, the bootstrap port, or the
	// gateway tag.
	if strings.HasSuffix(call.dst, ":*") {
		t.Errorf("dst %q opens ALL ports; kubelet rule must pin :10250", call.dst)
	}
	if strings.HasSuffix(call.dst, ":8080") {
		t.Errorf("dst %q reaches the bootstrap port; kubelet rule must pin :10250", call.dst)
	}
	if strings.HasPrefix(call.dst, handlers.GatewayTailnetTag) {
		t.Errorf("dst %q targets the gateway tag; kubelet rule must target the agent tag only", call.dst)
	}

	// PushedGatewayRoutes advances on success so the gate suppresses an
	// identical re-reconcile (same contract as the mesh-box path).
	if got := intentOf(s, "cust_saas").PushedUnion; !slices.Equal(got, wantSrc) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v", got, wantSrc)
	}

	// Gate: an identical non-forced reconcile must NOT push again.
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("re-reconcile: %v", err)
	}
	if m.count() != 1 {
		t.Fatalf("ReplaceManagedKubeletACL calls after identical = %d, want 1 (gate)", m.count())
	}

	// And the NEXT write carries what the last one landed, which is the only
	// thing that lets it recognise its own rule on a shared document.
	reportRoutes(t, s, "cust_saas", "prod-1", []string{"10.42.0.0/16"})
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("reconcile after the shrink: %v", err)
	}
	if !m.last().proves(wantSrc) {
		t.Fatalf("owned = %v, want it to name the union the previous write landed %v", m.last().owned, wantSrc)
	}
}

// The shared tailnet's kubelet rule has ONE Src for every tenant on it, so the
// set central writes has to be the cross-tenant union — recomputed on every
// reconcile, from the store rather than from the customer that triggered it.
// Reconciling tenant A with A's routes alone would revoke B's :10250 grant, and
// on the next B event revoke A's back: the flap the additive merge was hiding.
func TestReconcileSharedTailnetUnionSpansEveryTenant(t *testing.T) {
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "a"})
	s.AddCustomer(&state.Customer{ID: "cust_b", Token: "b"})
	reportRoutes(t, s, "cust_a", "a-1", []string{"10.42.0.0/16"})
	reportRoutes(t, s, "cust_b", "b-1", []string{"10.96.0.0/12"})

	// A dedicated Headscale tenant owns a whole policy namespace of its own and
	// must contribute nothing here — its CIDRs on the shared tailnet would be a
	// grant nobody asked for.
	s.AddCustomer(&state.Customer{ID: "cust_box", Token: "box"})
	if err := s.SetCustomerMesh("cust_box", &state.MeshEndpoint{
		Provider: "headscale", LoginServer: "https://hs.example", APIKey: "k", User: "u",
	}); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}
	reportRoutes(t, s, "cust_box", "box-1", []string{"192.168.7.0/24"})

	if err := pushShared(t, r, true); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	want := []string{"10.42.0.0/16", "10.96.0.0/12"}
	if got := m.lastSrc(); !slices.Equal(got, want) {
		t.Fatalf("src = %v, want the cross-tenant union %v", got, want)
	}
	if slices.Contains(m.lastSrc(), "192.168.7.0/24") {
		t.Fatal("a dedicated Headscale tenant's routes reached the shared tailnet union")
	}

	// cust_b's last cluster is deleted. The write that follows must take B's
	// CIDRs off and leave A's exactly where they were.
	owner := tenantOwner(t, s, "cust_b")
	deleteCluster(t, s, "cust_b", owner, "b-1")
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("reconcile after the delete: %v", err)
	}
	if got := m.lastSrc(); !slices.Equal(got, []string{"10.42.0.0/16"}) {
		t.Fatalf("src after cust_b's delete = %v, want cust_a's routes alone", got)
	}
	if !m.last().proves(want) {
		t.Fatalf("owned = %v, want it to name the union standing on the tailnet %v", m.last().owned, want)
	}

	// And A shrinking is a revocation too, not a rule that lingers.
	reportRoutes(t, s, "cust_a", "a-1", []string{"10.43.0.0/16"})
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("reconcile after the shrink: %v", err)
	}
	if got := m.lastSrc(); !slices.Equal(got, []string{"10.43.0.0/16"}) {
		t.Fatalf("src after cust_a shrank = %v, want only the CIDR it still reports", got)
	}
}

// Revoking a tenant is a revocation of its ACL grant too: an ACL is access, and
// a tenant that can no longer reach central must not keep :10250 open to CIDRs
// it advertised. It must not wait on another tenant changing later, either —
// nothing else is going to ask on a revoked tenant's behalf.
func TestReconcileSharedTailnetRevokedTenantLosesItsGrant(t *testing.T) {
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "a"})
	s.AddCustomer(&state.Customer{ID: "cust_b", Token: "b"})
	reportRoutes(t, s, "cust_a", "a-1", []string{"10.42.0.0/16"})
	reportRoutes(t, s, "cust_b", "b-1", []string{"10.96.0.0/12"})
	if err := pushShared(t, r, true); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	landed := []string{"10.42.0.0/16", "10.96.0.0/12"}
	if got := m.lastSrc(); !slices.Equal(got, landed) {
		t.Fatalf("src = %v, want %v", got, landed)
	}

	if err := s.RevokeCustomer("cust_b"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	// No force: the revoke moved the desired union, which IS the work.
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("reconcile after the revoke: %v", err)
	}
	if got := m.lastSrc(); !slices.Equal(got, []string{"10.42.0.0/16"}) {
		t.Fatalf("src after cust_b was revoked = %v, want the surviving tenant's routes alone", got)
	}
	if !m.last().proves(landed) {
		t.Fatalf("owned = %v, want it to name the union standing on the tailnet %v — without it the rule cannot be replaced", m.last().owned, landed)
	}
}

// The LAST shared tenant going away has to remove central's rule outright. The
// tenant's row is gone by then, which is exactly why the record of what central
// wrote cannot live on it: without a global one there would be nothing left to
// prove the rule still granting that tenant's CIDRs is central's to take back.
func TestReconcileSharedTailnetLastTenantRemovalWithdrawsTheRule(t *testing.T) {
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_only", Token: "only"})
	reportRoutes(t, s, "cust_only", "prod-1", []string{"10.42.0.0/16"})
	if err := pushShared(t, r, true); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := s.RevokeCustomer("cust_only"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := s.DeleteRevokedCustomer("cust_only"); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("reconcile after the tenant was deleted: %v", err)
	}
	last := m.last()
	if len(last.src) != 0 {
		t.Fatalf("src = %v, want empty — the rule has to go, not shrink", last.src)
	}
	if !last.proves([]string{"10.42.0.0/16"}) {
		t.Fatalf("owned = %v, want the deleted tenant's union — the only proof the rule is central's", last.owned)
	}
	// Converged: nothing left to write.
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("re-reconcile: %v", err)
	}
	if m.count() != 2 {
		t.Fatalf("calls = %d, want 2 (the write and the removal)", m.count())
	}
}

// Push-failure on the shared path must NOT advance the pushed union, so the
// worker retries (same self-heal contract as the mesh-box path).
func TestReconcileSharedTailnetPushFailureDoesNotAdvance(t *testing.T) {
	m := &mockTailnetEnsurer{}
	m.fail(errBoom)
	r, s := newSharedReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_saas", Token: "saas"})
	reportRoutes(t, s, "cust_saas", "prod-1", []string{"10.42.0.0/16"})
	if err := pushShared(t, r, true); err == nil {
		t.Fatalf("expected reconcile error on shared-tailnet push failure")
	}
	if m.count() != 1 {
		t.Fatalf("ReplaceManagedKubeletACL calls = %d, want 1", m.count())
	}
	if got := intentOf(s, "cust_saas").PushedUnion; len(got) != 0 {
		t.Fatalf("PushedGatewayRoutes = %v, want empty (failure must not advance)", got)
	}

	// The failure leaves the work due, and the retry recomputes the union from
	// the store rather than replaying the set the failed attempt carried — which
	// is what makes a concurrent tenant change converge instead of flap.
	m.fail(nil)
	reportRoutes(t, s, "cust_saas", "prod-2", []string{"10.96.0.0/12"})
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("retry reconcile: %v", err)
	}
	want := []string{"10.42.0.0/16", "10.96.0.0/12"}
	if got := m.lastSrc(); !slices.Equal(got, want) {
		t.Fatalf("retry src = %v, want the union recomputed at retry time %v", got, want)
	}
	// The failed attempt's claim SURVIVES: a POST that errored may still have
	// landed, so the union it carried stays provable until a write completes.
	if !m.last().proves([]string{"10.42.0.0/16"}) {
		t.Fatalf("owned = %v, want it to still name the failed attempt's union — that rule may be on the document", m.last().owned)
	}
	if !m.last().proves(want) {
		t.Fatalf("owned = %v, want it to name the union this attempt claims %v", m.last().owned, want)
	}
	if got := intentOf(s, "cust_saas").PushedUnion; !slices.Equal(got, want) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v once the retry landed", got, want)
	}
}

// An empty union is the case a merge could not express: the LAST shared tenant's
// routes going away has to REMOVE central's kubelet rule, so the write must be
// issued rather than skipped. Skipping it is how a CIDR that is no longer any
// tenant's gateway kept its :10250 grant forever.
func TestReconcileSharedTailnetEmptyUnionStillWrites(t *testing.T) {
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_saas", Token: "saas"})

	// No gateway routes at all: a forced reconcile still calls, with no source.
	if err := pushShared(t, r, true); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if m.count() != 1 {
		t.Fatalf("ReplaceManagedKubeletACL calls = %d, want 1 — an empty union is a removal", m.count())
	}
	if got := m.at(0).src; len(got) != 0 {
		t.Fatalf("src = %v, want empty", got)
	}
	if m.at(0).dst == "" {
		t.Fatal("an empty-union write must still name the destination it manages")
	}

	// And the same after routes existed and then went away entirely.
	owner := tenantOwner(t, s, "cust_saas")
	reportRoutes(t, s, "cust_saas", "prod-1", []string{"10.42.0.0/16"})
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("reconcile with routes: %v", err)
	}
	deleteCluster(t, s, "cust_saas", owner, "prod-1")
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("reconcile after the delete: %v", err)
	}
	if m.count() != 3 {
		t.Fatalf("calls = %d, want 3 (empty, populated, emptied again)", m.count())
	}
	if got := m.lastSrc(); len(got) != 0 {
		t.Fatalf("src after the tenant's last cluster was deleted = %v, want empty", got)
	}
}

// refusingBox satisfies PolicyEnsurer and counts every way a mesh box can be
// reached, including being constructed at all.
type refusingBox struct {
	built     int
	ensures   int
	finds     int
	approvals int
}

func (b *refusingBox) EnsurePolicy(context.Context, map[string][]string, map[string][]string, []client.PolicyACL) error {
	b.ensures++
	return nil
}

func (b *refusingBox) FindDeviceByHostname(context.Context, string) (string, error) {
	b.finds++
	return "", nil
}

func (b *refusingBox) ApproveNodeRoutes(context.Context, string, []string) error {
	b.approvals++
	return nil
}

// Node-level route approval is a per-customer-box operation and has no meaning
// on the shared Tailscale SaaS tailnet, whose nodes and route approvals are
// operator-managed out of band. The reconcile is handed a concrete cluster and
// forced, so a gateway hostname is fully in hand — the absence of any node call
// is the shared path's doing, not a can't-name-the-gateway skip.
func TestReconcileSharedTailnetSkipsNodeApproval(t *testing.T) {
	box := &refusingBox{}
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m, WithEnsurerFactory(func(*state.MeshEndpoint) PolicyEnsurer {
		box.built++
		return box
	}))

	s.AddCustomer(&state.Customer{ID: "cust_saas", Token: "saas"})
	want := []string{"10.42.0.0/16"}
	reportRoutes(t, s, "cust_saas", "prod-1", want)

	// The tenant's own reconcile hands the shared document to the shared worker
	// rather than writing it, so the ACL lands asynchronously.
	if err := r.Reconcile(context.Background(), "cust_saas", []string{"prod-1"}, true); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	waitFor(t, func() bool { return m.count() == 1 }, "the shared-tailnet ACL to be written once")
	if box.built != 0 || box.ensures != 0 || box.finds != 0 || box.approvals != 0 {
		t.Fatalf("shared-tailnet customer reached a mesh box: %+v", box)
	}
	waitFor(t, func() bool { return slices.Equal(intentOf(s, "cust_saas").PushedUnion, want) },
		"the tenant's pushed union to advance once the ACL landed")
}

// The shared document is ONE document for every box-less tenant, so N connectors
// reporting must not become N identical rewrites of it. Their work coalesces on
// the one shared worker, which recomputes the union once.
func TestReconcileSharedTailnetForcedReconcilesCoalesce(t *testing.T) {
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m, WithDebounce(20*time.Millisecond))
	var want []string
	for _, id := range []string{"cust_1", "cust_2", "cust_3", "cust_4"} {
		s.AddCustomer(&state.Customer{ID: id, Token: id})
	}
	for i, id := range []string{"cust_1", "cust_2", "cust_3", "cust_4"} {
		cidr := []string{[]string{"10.1.0.0/16", "10.2.0.0/16", "10.3.0.0/16", "10.4.0.0/16"}[i]}
		reportRoutes(t, s, id, id+"-1", cidr)
		want = append(want, cidr[0])
	}
	// Every tenant forces its own reconcile, the way a fleet-wide reconnect does.
	for _, id := range []string{"cust_1", "cust_2", "cust_3", "cust_4"} {
		r.Enqueue(id, id+"-1", true)
	}

	waitFor(t, func() bool { return slices.Equal(m.lastSrc(), want) },
		"the shared ACL to carry every tenant's routes")
	if got := m.count(); got != 1 {
		t.Fatalf("ACL writes = %d, want 1 — four connectors must not rewrite one document four times", got)
	}
}

// A shared-tailnet tenant has no per-customer gateway NODE, so a delete's
// pushed-by-cluster tombstone has nothing to withdraw from. It still has to go:
// the entry has no reader left, and keeping it grows the map by one dead entry
// per cluster the tenant ever deletes. It is dropped once the union that
// replaced it has converged, and never through a mesh-box method.
func TestReconcileSharedTailnetDropsDeletedClusterTombstone(t *testing.T) {
	box := &refusingBox{}
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m, WithEnsurerFactory(func(*state.MeshEndpoint) PolicyEnsurer {
		box.built++
		return box
	}))

	s.AddCustomer(&state.Customer{ID: "cust_saas", Token: "saas"})
	owner := tenantOwner(t, s, "cust_saas")
	reportRoutes(t, s, "cust_saas", "prod-1", []string{"10.42.0.0/16"})
	// The node half of this tenant's convergence state — left from when it had
	// a per-customer box, which is the only way this path ever acquires one.
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_saas", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed pushed entry: %v", err)
	}
	if err := pushShared(t, r, true); err != nil {
		t.Fatalf("reconcile before the delete: %v", err)
	}

	deleteCluster(t, s, "cust_saas", owner, "prod-1")
	r.EnqueueClusterRemoval("cust_saas", "prod-1")

	waitFor(t, func() bool {
		_, ok := intentOf(s, "cust_saas").PushedByCluster["prod-1"]
		return !ok
	}, "the shared-tailnet tenant's dead tombstone to be dropped")

	// The union it converged to is the empty one the delete left, and no mesh
	// box was built, let alone called.
	if got := intentOf(s, "cust_saas").PushedUnion; len(got) != 0 {
		t.Fatalf("PushedGatewayRoutes = %v, want empty after the only cluster was deleted", got)
	}
	if box.built != 0 || box.ensures != 0 || box.finds != 0 || box.approvals != 0 {
		t.Fatalf("shared-tailnet withdrawal reached a mesh box: %+v", box)
	}
	if got := m.lastSrc(); len(got) != 0 {
		t.Fatalf("shared ACL src = %v, want empty once the tenant's last cluster went", got)
	}
}

// A tombstone is the durable record that one of this tenant's clusters had
// routes in play. Discarding it while the shared document still grants them
// would report a convergence that has not happened, so the drop waits for the
// ACL — and the wait is a requeue, not a failure.
func TestReconcileSharedTailnetHoldsTombstonesUntilTheACLConverges(t *testing.T) {
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_saas", Token: "saas"})
	owner := tenantOwner(t, s, "cust_saas")
	reportRoutes(t, s, "cust_saas", "prod-1", []string{"10.42.0.0/16"})
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_saas", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed pushed entry: %v", err)
	}
	// The document really does grant this tenant's CIDRs before the delete, and
	// the write that would take them back is failing.
	if err := pushShared(t, r, true); err != nil {
		t.Fatalf("seed shared push: %v", err)
	}
	m.fail(errBoom)
	deleteCluster(t, s, "cust_saas", owner, "prod-1")

	out := r.reconcile(context.Background(), "cust_saas",
		request{policy: true, clusters: []clusterRequest{{id: "prod-1", withdraw: true}}})
	if out.err != nil {
		t.Fatalf("reconcile = %v, want the withdrawal held rather than failed", out.err)
	}
	if !slices.Equal(out.failedClusters, []string{"prod-1"}) {
		t.Fatalf("requeued clusters = %v, want the held tombstone", out.failedClusters)
	}
	if _, ok := intentOf(s, "cust_saas").PushedByCluster["prod-1"]; !ok {
		t.Fatal("the tombstone was dropped while the shared ACL still granted its CIDRs")
	}

	// Once the document catches up the same request completes the bookkeeping.
	m.fail(nil)
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("shared push: %v", err)
	}
	out = r.reconcile(context.Background(), "cust_saas",
		request{policy: true, clusters: []clusterRequest{{id: "prod-1", withdraw: true}}})
	if out.err != nil {
		t.Fatalf("reconcile after the ACL landed: %v", out.err)
	}
	if _, ok := intentOf(s, "cust_saas").PushedByCluster["prod-1"]; ok {
		t.Fatal("the tombstone survived a converged shared document")
	}
}

// A restart is the one moment nothing is left to ask: the tenant whose routes
// the shared document still grants was offboarded, or its live-only connector's
// intent was reaped at load, and neither is coming back to enqueue anything. The
// startup replay is what closes that, for the document and for the per-tenant
// tombstones a boot reap leaves behind.
func TestReplaySharedTailnetConvergesWhatARestartWouldSwallow(t *testing.T) {
	m := &mockTailnetEnsurer{}
	r, s := newSharedReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "a"})
	s.AddCustomer(&state.Customer{ID: "cust_gone", Token: "gone"})
	reportRoutes(t, s, "cust_a", "a-1", []string{"10.42.0.0/16"})
	reportRoutes(t, s, "cust_gone", "gone-1", []string{"10.96.0.0/12"})
	if err := pushShared(t, r, true); err != nil {
		t.Fatalf("seed reconcile: %v", err)
	}
	landed := []string{"10.42.0.0/16", "10.96.0.0/12"}
	if got := m.lastSrc(); !slices.Equal(got, landed) {
		t.Fatalf("seeded src = %v, want %v", got, landed)
	}

	// While central was down: the tenant went away entirely, and cust_a's own
	// cluster delete left a durable withdrawal tombstone nothing else replays.
	if err := s.RevokeCustomer("cust_gone"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := s.DeleteRevokedCustomer("cust_gone"); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}
	owner := tenantOwner(t, s, "cust_a")
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_a", "a-2", []string{"10.7.0.0/16"}); err != nil {
		t.Fatalf("seed tombstone: %v", err)
	}
	deleteCluster(t, s, "cust_a", owner, "a-1")

	// A fresh reconciler over the same durable state — the restart.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	booted := New(ctx, s, log,
		WithDebounce(time.Millisecond), WithBackoff(2*time.Millisecond), WithMaxBackoff(20*time.Millisecond))
	booted.shared = m
	if err := booted.ReplaySharedTailnet(ctx); err != nil {
		t.Fatalf("ReplaySharedTailnet: %v", err)
	}

	waitFor(t, func() bool { return len(m.lastSrc()) == 0 }, "the shared ACL to be emptied by the boot replay")
	if !m.last().proves(landed) {
		t.Fatalf("owned = %v, want it to name the union standing on the tailnet %v", m.last().owned, landed)
	}
	waitFor(t, func() bool {
		_, ok := intentOf(s, "cust_a").PushedByCluster["a-2"]
		return !ok
	}, "the boot replay to clear the shared tenant's tombstone")
}

// With no shared client configured — the OSS build, or TS_OAUTH_* unset — every
// shared-tailnet entry point stays the no-op it has always been. Nothing is
// enqueued, so a tenant's bookkeeping cannot end up waiting on a document
// nothing in this build writes.
func TestSharedTailnetPathIsANoOpWithoutAClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := state.New()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := New(ctx, s, log, WithDebounce(time.Millisecond))
	s.AddCustomer(&state.Customer{ID: "cust_saas", Token: "saas"})
	owner := tenantOwner(t, s, "cust_saas")
	reportRoutes(t, s, "cust_saas", "prod-1", []string{"10.42.0.0/16"})
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_saas", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed pushed entry: %v", err)
	}
	deleteCluster(t, s, "cust_saas", owner, "prod-1")

	r.EnqueueSharedTailnet(true)
	if err := r.ReplaySharedTailnet(ctx); err != nil {
		t.Fatalf("ReplaySharedTailnet: %v", err)
	}
	r.mu.Lock()
	_, spawned := r.workers[sharedTailnetKey]
	r.mu.Unlock()
	if spawned {
		t.Fatal("a shared-tailnet worker was started with no client to write with")
	}
	// The whole path stays the legacy no-op: nothing written, nothing recorded,
	// and the tenant's own state left exactly where the store put it.
	if err := r.Reconcile(context.Background(), "cust_saas", nil, true); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := intentOf(s, "cust_saas").PushedUnion; len(got) != 0 {
		t.Fatalf("PushedGatewayRoutes = %v, want untouched with no shared client", got)
	}
	if _, ok := intentOf(s, "cust_saas").PushedByCluster["prod-1"]; !ok {
		t.Fatal("a build that writes no shared document still tore down the tenant's record of one")
	}
}
