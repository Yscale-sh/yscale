package state

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"
)

// sharedTenant seeds one box-less tenant contributing routes to the shared
// tailnet, with a persister watching, so the durable side of every claim and
// receipt is visible.
func sharedTenant(t *testing.T, routes []string) (*Store, *gatewayRouteSpyPersister) {
	t.Helper()
	s := New()
	claimCluster(t, s, "cust_test", "prod-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", routes); err != nil {
		t.Fatalf("seed routes: %v", err)
	}
	p := &gatewayRouteSpyPersister{}
	s.persist = p
	return s, p
}

// The write-ahead claim has to be DURABLE before the POST, because the rule a
// POST installs outlives the process that made it. A restart between the two
// reads the claim back and can still name — and therefore still take back — the
// rule it may have written.
func TestClaimSharedTailnetPushSurvivesARestart(t *testing.T) {
	s, p := sharedTenant(t, []string{"10.42.0.0/16"})

	intent := s.SharedTailnetRouteIntent()
	claimed, err := s.ClaimSharedTailnetPush(intent)
	if err != nil {
		t.Fatalf("ClaimSharedTailnetPush: %v", err)
	}
	if !containsUnion(claimed.Claims, []string{"10.42.0.0/16"}) {
		t.Fatalf("claims = %v, want the union about to be written", claimed.Claims)
	}
	row := p.meshState()
	if row == nil {
		t.Fatal("the claim was in memory only; a restart would find nothing to prove the rule with")
	}
	if len(row.SharedKubeletRoutes) != 0 {
		t.Fatalf("SharedKubeletRoutes = %v, want it unmoved — nothing has been written yet", row.SharedKubeletRoutes)
	}
	if len(row.SharedKubeletClaims) != 1 || !slices.Equal(row.SharedKubeletClaims[0], []string{"10.42.0.0/16"}) {
		t.Fatalf("SharedKubeletClaims = %v, want exactly the claimed union", row.SharedKubeletClaims)
	}

	// The restart: the POST landed, the receipt never did, and this process is
	// gone. A fresh store over the same rows still proves that rule is central's.
	booted := emptyStore()
	booted.applySnapshot(&snapshot{MeshState: row})
	proof := booted.SharedTailnetRouteIntent().Proof()
	if len(proof) != 1 || !slices.Equal(proof[0], []string{"10.42.0.0/16"}) {
		t.Fatalf("proof after the restart = %v, want the in-flight claim", proof)
	}
}

// Recording a completed write COLLAPSES the proof: the document provably carries
// exactly the recorded union, so every claim covering the way there is spent.
// Collapsing in the SAME durable row as the new pushed union is what keeps a
// crash from splitting the two.
func TestRecordSharedTailnetPushedCollapsesTheProof(t *testing.T) {
	s, p := sharedTenant(t, []string{"10.42.0.0/16"})

	claimed, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent())
	if err != nil {
		t.Fatalf("ClaimSharedTailnetPush: %v", err)
	}
	if err := s.RecordSharedTailnetPushed(claimed); err != nil {
		t.Fatalf("RecordSharedTailnetPushed: %v", err)
	}

	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 {
		t.Fatalf("claims = %v, want them spent by the completed write", intent.Claims)
	}
	if proof := intent.Proof(); len(proof) != 1 || !slices.Equal(proof[0], []string{"10.42.0.0/16"}) {
		t.Fatalf("proof = %v, want exactly the converged rule", proof)
	}
	row := p.meshState()
	if len(row.SharedKubeletClaims) != 0 {
		t.Fatalf("durable claims = %v, want the collapse to be durable too", row.SharedKubeletClaims)
	}
	if !slices.Equal(row.SharedKubeletRoutes, []string{"10.42.0.0/16"}) {
		t.Fatalf("SharedKubeletRoutes = %v, want the union that landed", row.SharedKubeletRoutes)
	}

	// A REMOVAL collapses to nothing at all: with no rule left there is nothing
	// to prove, and an empty proof is what stops the next write deleting on
	// evidence it does not have.
	if err := s.RevokeCustomer("cust_test"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	removal := s.SharedTailnetRouteIntent()
	claimed, err = s.ClaimSharedTailnetPush(removal)
	if err != nil {
		t.Fatalf("claim for the removal: %v", err)
	}
	if err := s.RecordSharedTailnetPushed(claimed); err != nil {
		t.Fatalf("record the removal: %v", err)
	}
	if proof := s.SharedTailnetRouteIntent().Proof(); len(proof) != 0 {
		t.Fatalf("proof = %v, want it empty once the rule is gone", proof)
	}
}

// A claim is only retired by a write that completes, so a failing ACL API and a
// fleet that keeps changing its routes could grow the proof set without end — in
// a row every boot reads back. It is capped, and one attempt per union: a retry
// ladder against the same desired union costs one durable write, not one each.
func TestClaimSharedTailnetPushIsBoundedAndIdempotent(t *testing.T) {
	s, p := sharedTenant(t, []string{"10.42.0.0/16"})

	intent := s.SharedTailnetRouteIntent()
	for i := 0; i < 3; i++ {
		if _, err := s.ClaimSharedTailnetPush(intent); err != nil {
			t.Fatalf("re-claim %d: %v", i, err)
		}
	}
	if got := s.SharedTailnetRouteIntent().Claims; len(got) != 1 {
		t.Fatalf("claims after three attempts at one union = %v, want one", got)
	}
	if writes := p.meshState(); len(writes.SharedKubeletClaims) != 1 {
		t.Fatalf("durable claims = %v, want one", writes.SharedKubeletClaims)
	}

	// Every attempt fails and the desired union keeps moving: the set grows to the
	// cap and stops, with every union it took on the way still in it.
	newest := fillSharedTailnetClaims(t, s)
	claims := s.SharedTailnetRouteIntent().Claims
	if len(claims) != maxSharedTailnetClaims {
		t.Fatalf("claims = %d entries, want the cap of %d", len(claims), maxSharedTailnetClaims)
	}
	if !slices.Equal(claims[len(claims)-1], newest) {
		t.Fatalf("newest claim = %v, want the union of the last attempt %v", claims[len(claims)-1], newest)
	}
	if !containsUnion(claims, []string{"10.42.0.0/16"}) {
		t.Fatal("the cap gave up the oldest claim; a rule carrying it could no longer be proven central's")
	}
}

// fillSharedTailnetClaims moves the desired union to a fresh set and claims it,
// over and over, until the proof set stands exactly at its cap — the way a fleet
// whose routes keep changing against an ACL API that keeps refusing gets there.
// It returns the last union claimed.
func fillSharedTailnetClaims(t *testing.T, s *Store) []string {
	t.Helper()
	var newest []string
	for i := 0; len(s.SharedTailnetRouteIntent().Claims) < maxSharedTailnetClaims; i++ {
		routes := []string{fmt.Sprintf("10.%d.0.0/16", 100+i)}
		if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", routes); err != nil {
			t.Fatalf("move the desired union: %v", err)
		}
		moved := s.SharedTailnetRouteIntent()
		if _, err := s.ClaimSharedTailnetPush(moved); err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		newest = moved.Desired
	}
	return newest
}

// The cap must never buy room by forgetting. A union is in the proof set because
// its POST may have landed, so evicting the oldest to admit a new one leaves a
// rule central may have written with no candidate Src that matches it: no later
// replace can take it back, and it grants :10250 to CIDRs no tenant advertises
// for as long as the tailnet exists. At the cap the NEW claim is refused instead
// — nothing durable moves, nothing is written — and every union already claimed
// is still there for the cleanup that will need it.
func TestClaimSharedTailnetPushRefusesANewUnionAtTheCap(t *testing.T) {
	s, p := sharedTenant(t, []string{"10.42.0.0/16"})
	fillSharedTailnetClaims(t, s)
	full := s.SharedTailnetRouteIntent().Claims
	if len(full) != maxSharedTailnetClaims {
		t.Fatalf("claims = %d entries, want the cap of %d", len(full), maxSharedTailnetClaims)
	}
	oldest := slices.Clone(full[0])
	before := p.meshState()

	overflow := []string{"10.200.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", overflow); err != nil {
		t.Fatalf("move the desired union past the cap: %v", err)
	}
	claimed, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent())
	if !errors.Is(err, ErrSharedTailnetClaimsFull) {
		t.Fatalf("claim at the cap err = %v, want ErrSharedTailnetClaimsFull", err)
	}
	if got := p.meshState(); !reflect.DeepEqual(got, before) {
		t.Fatalf("mesh-state row moved: %v -> %v; a refused claim must not give up proof", before, got)
	}
	if containsUnion(claimed.Claims, overflow) {
		t.Fatalf("returned claims %v name the refused union; the caller must not write on it", claimed.Claims)
	}

	// Every earlier claim is still in hand, in memory, durably, and in the proof a
	// write would carry — the refusal cost an attempt, not evidence.
	for _, view := range [][][]string{claimed.Claims, s.SharedTailnetRouteIntent().Claims, before.SharedKubeletClaims} {
		if len(view) != maxSharedTailnetClaims {
			t.Fatalf("claims = %v, want all %d retained", view, maxSharedTailnetClaims)
		}
		for _, union := range full {
			if !containsUnion(view, union) {
				t.Fatalf("claim %v was dropped to make room; the rule it may name is now unprovable", union)
			}
		}
	}
	if proof := s.SharedTailnetRouteIntent().Proof(); !containsUnion(proof, oldest) {
		t.Fatalf("proof = %v, want the oldest claim %v still deletable", proof, oldest)
	}

	// The way out. Reuse of a union the set already holds stays idempotent at the
	// cap, so a convergence can still be made — and the write that completes
	// collapses the whole set, which is what admits new claims again.
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", oldest); err != nil {
		t.Fatalf("converge on an already-claimed union: %v", err)
	}
	reused, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent())
	if err != nil {
		t.Fatalf("re-claiming a union the set already holds at the cap: %v", err)
	}
	if err := s.RecordSharedTailnetPushed(reused); err != nil {
		t.Fatalf("RecordSharedTailnetPushed: %v", err)
	}
	if got := s.SharedTailnetRouteIntent().Claims; len(got) != 0 {
		t.Fatalf("claims = %v, want the completed write to collapse them", got)
	}
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", overflow); err != nil {
		t.Fatalf("move the desired union after the collapse: %v", err)
	}
	if _, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent()); err != nil {
		t.Fatalf("claim after the collapse: %v, want the cap unblocked", err)
	}
}

// A refused durable claim means NO ACL write: the caller is told, and nothing in
// the store moves. An unclaimed POST is exactly the orphan the claim exists to
// prevent, so failing closed here is the whole contract.
func TestClaimSharedTailnetPushIsFailClosed(t *testing.T) {
	s, _ := sharedTenant(t, []string{"10.42.0.0/16"})
	s.persist = &refusingMeshStatePersister{err: errors.New("postgres is down")}

	if _, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent()); err == nil {
		t.Fatal("a refused claim reported success; the write it guards would be unprovable")
	}
	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 {
		t.Fatalf("claims = %v, want nothing recorded that no restart could read back", intent.Claims)
	}
	if len(intent.Proof()) != 0 {
		t.Fatalf("proof = %v, want it unchanged", intent.Proof())
	}
}

// A mesh-state row written before claims existed has no claims member at all.
// It has to read back as a central that had written receipts only — which is
// exactly no outstanding claims — rather than as a decode failure or a proof set
// with a nil entry in it.
func TestApplySnapshotAcceptsAMeshStateRowWithoutClaims(t *testing.T) {
	s := emptyStore()
	s.applySnapshot(&snapshot{MeshState: &meshState{SharedKubeletRoutes: []string{"10.42.0.0/16"}}})

	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 {
		t.Fatalf("claims = %v, want none from a row that predates them", intent.Claims)
	}
	if proof := intent.Proof(); len(proof) != 1 || !slices.Equal(proof[0], []string{"10.42.0.0/16"}) {
		t.Fatalf("proof = %v, want the recorded union alone", proof)
	}
}

// The proof set is what a rule is deleted on, so an empty union may never be in
// it: every rule shaped like central's would match, including an operator's. A
// duplicate claim is not in it either — the replace would just look twice.
func TestSharedTailnetProofDropsEmptyAndDuplicateUnions(t *testing.T) {
	intent := SharedTailnetIntent{
		Pushed: []string{"10.42.0.0/16"},
		Claims: [][]string{nil, {"10.42.0.0/16"}, {"10.43.0.0/16"}, {"10.43.0.0/16"}},
	}
	want := [][]string{{"10.42.0.0/16"}, {"10.43.0.0/16"}}
	if got := intent.Proof(); !reflect.DeepEqual(got, want) {
		t.Fatalf("proof = %v, want %v", got, want)
	}
	if got := (SharedTailnetIntent{}).Proof(); len(got) != 0 {
		t.Fatalf("proof of an empty intent = %v, want nothing to prove", got)
	}
}

// Two tenants advertising the SAME CIDRs converge the shared document on the
// first one's write. The second then has no ACL work at all — and its own pushed
// bookkeeping used to be skipped along with the write, leaving it permanently
// stale and re-asking forever.
func TestAdvanceSharedTailnetTenantsCatchesUpATenantWithNoWriteToRideOn(t *testing.T) {
	s, p := sharedTenant(t, []string{"10.42.0.0/16"})
	s.AddCustomer(&Customer{ID: "cust_second", Token: "tok_second"})
	claimCluster(t, s, "cust_second", "prod-2")
	if _, err := s.SetCustomerGatewayRoutes("cust_second", "prod-2", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("the second tenant reports the same CIDRs: %v", err)
	}

	// The first tenant's write converges the cross-tenant union outright.
	claimed, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent())
	if err != nil {
		t.Fatalf("ClaimSharedTailnetPush: %v", err)
	}
	if err := s.RecordSharedTailnetPushed(claimed); err != nil {
		t.Fatalf("RecordSharedTailnetPushed: %v", err)
	}

	// A tenant that joins after that convergence has nothing to trigger a write.
	s.AddCustomer(&Customer{ID: "cust_third", Token: "tok_third"})
	claimCluster(t, s, "cust_third", "prod-3")
	if _, err := s.SetCustomerGatewayRoutes("cust_third", "prod-3", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("the third tenant reports the same CIDRs: %v", err)
	}
	intent := s.SharedTailnetRouteIntent()
	if !slices.Equal(intent.Desired, intent.Pushed) {
		t.Fatalf("desired %v / pushed %v, want them already equal", intent.Desired, intent.Pushed)
	}

	before := p.meshState()
	s.AdvanceSharedTailnetTenants(intent)
	for _, id := range []string{"cust_test", "cust_second", "cust_third"} {
		c, err := s.CustomerByID(id)
		if err != nil {
			t.Fatalf("CustomerByID %s: %v", id, err)
		}
		if !slices.Equal(c.PushedGatewayRoutes, []string{"10.42.0.0/16"}) {
			t.Fatalf("%s PushedGatewayRoutes = %v, want the union the shared document already carries", id, c.PushedGatewayRoutes)
		}
	}
	if got := p.meshState(); !reflect.DeepEqual(got, before) {
		t.Fatalf("mesh-state row moved: %v -> %v; the bookkeeping catch-up must not claim a write", before, got)
	}
}

// The read-locked pre-pass and the locked authoritative pass must decide the same
// thing. The stored half is canonical and the locked pass canonicalizes before
// comparing, so a pre-pass reading the caller's raw contribution called an
// already-converged tenant stale on spelling alone — taking the customer write
// lock on every enqueue of an idle fleet for a set the authoritative comparison
// then found unchanged.
func TestAdvanceSharedTailnetTenantsPrePassMatchesTheLockedComparison(t *testing.T) {
	s, p := sharedTenant(t, []string{"10.42.0.0/16", "10.96.0.0/12"})

	claimed, err := s.ClaimSharedTailnetPush(s.SharedTailnetRouteIntent())
	if err != nil {
		t.Fatalf("ClaimSharedTailnetPush: %v", err)
	}
	if err := s.RecordSharedTailnetPushed(claimed); err != nil {
		t.Fatalf("RecordSharedTailnetPushed: %v", err)
	}

	// The same contribution, spelled the way a caller may hand it over: unsorted,
	// duplicated, and with a host-bit form routecidr normalises.
	intent := s.SharedTailnetRouteIntent()
	intent.ByCustomer["cust_test"] = []string{"10.96.0.0/12", "10.42.0.1/16", "10.42.0.0/16"}

	// Skipping the customer write lock IS the fast path, so holding that lock is
	// how a test sees which comparison ran: one that agrees returns without it,
	// one that does not blocks here.
	before := len(p.customers)
	s.custMu.Lock()
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		s.AdvanceSharedTailnetTenants(intent)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		s.custMu.Unlock()
		<-returned
		t.Fatal("the pre-pass took the customer write lock for a contribution the locked comparison finds unchanged")
	}
	s.custMu.Unlock()

	if got := len(p.customers); got != before {
		t.Fatalf("customer rows written = %d, want none", got-before)
	}
	c, err := s.CustomerByID("cust_test")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if !slices.Equal(c.PushedGatewayRoutes, []string{"10.42.0.0/16", "10.96.0.0/12"}) {
		t.Fatalf("PushedGatewayRoutes = %v, want the canonical union untouched", c.PushedGatewayRoutes)
	}
}
