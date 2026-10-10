// yscale:proprietary

package meshpolicy

import (
	"context"
	"encoding/json"
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

// etagTailnet is the tailnet ACL API with its OPTIMISTIC CONCURRENCY intact:
// every GET hands out the Etag of the version it served and keeps that version,
// and a POST whose If-Match does not name the CURRENT one is a 412 rather than a
// write. liveTailnet keeps a document; this keeps the versions, which is what a
// test about re-reading after a lost race has to be able to name.
//
// Every /acl request is recorded with the DURABLE state the store held when it
// arrived. Half of what the two-phase write guarantees is an ORDER between an
// HTTP call and a durable row — nothing advances until the write that earns it
// returns — and by the time a pass ends, the intermediate rows are gone. Reading
// the store inside the handler is where that order is visible without racing the
// reconciler for it.
type etagTailnet struct {
	store *state.Store

	mu         sync.Mutex
	doc        string
	rev        int
	postStatus int
	versions   map[string]string
	reqs       []aclRequest
	edit       func(doc string) string
}

// aclRequest is one /acl call as the API saw it, with the durable shared-tailnet
// state that was current when it landed.
type aclRequest struct {
	method  string
	status  int
	served  string // GET: the Etag the version came back under.
	ifMatch string // POST: the version this write was made against.
	body    string // POST: the document it tried to install.
	intent  state.SharedTailnetIntent
}

func (e *etagTailnet) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/oauth/token"):
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"tok","expires_in":3600}`)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/acl"):
		// Snapshotted before the lock: the store is the reconciler's, and nothing
		// here may hold this server's lock across a call into it.
		intent := e.store.SharedTailnetRouteIntent()
		e.mu.Lock()
		doc, etag := e.doc, e.etagLocked()
		e.versions[etag] = doc
		e.reqs = append(e.reqs, aclRequest{
			method: http.MethodGet, status: http.StatusOK, served: etag, body: doc, intent: intent,
		})
		e.mu.Unlock()
		w.Header().Set("Etag", etag)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, doc)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/acl"):
		body, _ := io.ReadAll(r.Body)
		intent := e.store.SharedTailnetRouteIntent()
		e.mu.Lock()
		defer e.mu.Unlock()
		req := aclRequest{
			method: http.MethodPost, ifMatch: r.Header.Get("If-Match"), body: string(body), intent: intent,
		}
		if e.postStatus != 0 && e.postStatus != http.StatusOK {
			req.status = e.postStatus
			e.reqs = append(e.reqs, req)
			w.WriteHeader(e.postStatus)
			_, _ = io.WriteString(w, "the tailnet API is refusing writes")
			return
		}
		// The other writer lands in the window between central's GET and this
		// POST, which is the only way a 412 is ever produced for real.
		if e.edit != nil {
			e.doc, e.edit = e.edit(e.doc), nil
			e.rev++
		}
		if req.ifMatch != e.etagLocked() {
			req.status = http.StatusPreconditionFailed
			e.reqs = append(e.reqs, req)
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, "the policy moved under this write")
			return
		}
		e.doc = string(body)
		e.rev++
		req.status = http.StatusOK
		e.reqs = append(e.reqs, req)
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// etagLocked names the current version. Monotonic, so no two versions of one
// document can be confused for each other. Callers hold e.mu.
func (e *etagTailnet) etagLocked() string { return fmt.Sprintf(`W/"v%d"`, e.rev) }

// refuseWrites makes the API reject POSTs with status, or accept them again at 0.
func (e *etagTailnet) refuseWrites(status int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.postStatus = status
}

// editBeforeNextPost arms ONE concurrent edit, applied just before the next POST
// is admitted — the operator saving the policy from a UI while central is between
// its GET and its write.
func (e *etagTailnet) editBeforeNextPost(edit func(doc string) string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.edit = edit
}

// setDoc replaces the whole document, for seeding a state central's own writes
// converge away from and so cannot produce.
func (e *etagTailnet) setDoc(doc string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.doc = doc
	e.rev++
}

// document is what the tailnet is carrying now.
func (e *etagTailnet) document() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.doc
}

// version returns the document served under etag, so an assertion can ask what
// the write that named it was really selecting against.
func (e *etagTailnet) version(etag string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	doc, ok := e.versions[etag]
	return doc, ok
}

// requests is the /acl transcript, oldest first.
func (e *etagTailnet) requests() []aclRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.reqs)
}

// resetRequests starts a fresh transcript, so a test asserts the pass it is about
// rather than everything its seeding did.
func (e *etagTailnet) resetRequests() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reqs = nil
}

// operatorDst is a destination central does not manage at all. A write scoped to
// the kubelet rule has no business touching it.
const operatorDst = "tag:yscale-jump:22"

// sharedDoc renders a tailnet policy carrying the operator's own top-level
// members and rules, plus one `accept tcp <union> -> kubeletACLDst` per central
// union given. Two of those are what a document with duplicate central-owned
// grants looks like.
func sharedDoc(t *testing.T, central [][]string) string {
	t.Helper()
	acls := []map[string]any{
		{"action": "accept", "proto": "tcp", "src": operatorRule, "dst": []string{kubeletACLDst}},
		{"action": "accept", "proto": "tcp", "src": []string{"10.98.0.0/16"}, "dst": []string{operatorDst}},
	}
	for _, union := range central {
		acls = append(acls, map[string]any{
			"action": "accept", "proto": "tcp", "src": union, "dst": []string{kubeletACLDst},
		})
	}
	raw, err := json.Marshal(map[string]any{
		"acls":      acls,
		"ssh":       []map[string]any{{"action": "accept", "src": []string{"autogroup:member"}, "dst": []string{"autogroup:self"}, "users": []string{"root"}}},
		"nodeAttrs": []map[string]any{{"target": []string{"autogroup:member"}, "attr": []string{"funnel"}}},
		"tagOwners": map[string]any{"tag:yscale-burst": []string{"autogroup:admin"}},
	})
	if err != nil {
		t.Fatalf("encode the seeded policy: %v", err)
	}
	return string(raw)
}

// srcsOn names the Src of every rule in doc carrying dst, in document order.
func srcsOn(t *testing.T, doc, dst string) [][]string {
	t.Helper()
	var parsed struct {
		ACLs []struct {
			Src []string `json:"src"`
			Dst []string `json:"dst"`
		} `json:"acls"`
	}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatalf("decode tailnet policy %s: %v", doc, err)
	}
	var out [][]string
	for _, rule := range parsed.ACLs {
		if len(rule.Dst) == 1 && rule.Dst[0] == dst {
			out = append(out, rule.Src)
		}
	}
	return out
}

// centralSrcs is every Src on the kubelet destination EXCEPT the operator's —
// i.e. what central left behind on that version of the document.
func centralSrcs(t *testing.T, doc string) [][]string {
	t.Helper()
	var out [][]string
	for _, src := range srcsOn(t, doc, kubeletACLDst) {
		if !slices.Equal(src, operatorRule) {
			out = append(out, src)
		}
	}
	return out
}

// assertOperatorPolicyIntact checks the parts of the document central has no
// claim on: the rule an operator wrote for central's own destination, the rule
// for a destination central does not manage, and the top-level members this
// client does not model at all.
func assertOperatorPolicyIntact(t *testing.T, doc string) {
	t.Helper()
	if got := srcsOn(t, doc, kubeletACLDst); !slices.ContainsFunc(got, func(s []string) bool { return slices.Equal(s, operatorRule) }) {
		t.Fatalf("rules on %s = %v, want the operator's own %v among them", kubeletACLDst, got, operatorRule)
	}
	if got := srcsOn(t, doc, operatorDst); len(got) != 1 {
		t.Fatalf("rules on %s = %v, want the operator's untouched", operatorDst, got)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal([]byte(doc), &members); err != nil {
		t.Fatalf("decode tailnet policy: %v", err)
	}
	for _, key := range []string{"ssh", "nodeAttrs", "tagOwners"} {
		if len(members[key]) == 0 {
			t.Fatalf("top-level %q is gone; a kubelet-ACL write dropped a member this client does not model", key)
		}
	}
}

// wedgedWithDuplicateGrants builds the state the collapse has to write its way out
// of, on a REAL client against a REAL ACL API: one converged pushed union, a proof
// set standing at its cap, a ninth desired union that cannot be claimed, a deleted
// cluster's withdrawal tombstone — and a document carrying THREE rules central can
// prove are its own.
//
// Those duplicates are seeded, because central's own writes converge them away: an
// exact replace leaves exactly one. They are what an older additive central left
// behind, or an operator duplicating a rule they saw, and the replace's promise
// about them is explicit — a tailnet that somehow holds two rules central owns
// converges back to one. Nothing else here is seeded: the pushed union is
// converged through the real write path, and the claims accumulate the way a
// tailnet refusing writes accumulates them.
//
// It returns the reconciler and its store, the API, the converged pushed union,
// the full claim set (oldest first), and the desired union the cap refused.
func wedgedWithDuplicateGrants(t *testing.T) (*Reconciler, *state.Store, *etagTailnet, []string, [][]string, []string) {
	t.Helper()
	s := state.New()
	tn := &etagTailnet{store: s, doc: sharedDoc(t, nil), versions: map[string]string{}}
	srv := httptest.NewServer(tn)
	t.Cleanup(srv.Close)
	c, err := tailscale.New(tailscale.Config{
		ClientID: "id", ClientSecret: "secret", Tailnet: "acme.example", APIBase: srv.URL,
	})
	if err != nil {
		t.Fatalf("tailscale.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// The debounce is an hour: the withdrawal below re-asks the shared worker, and
	// a worker running that on its own ladder would be writing the document
	// alongside the pass under test.
	r := New(ctx, s, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithDebounce(time.Hour), WithBackoff(2*time.Millisecond), WithMaxBackoff(20*time.Millisecond))
	r.shared = c

	pushed := []string{"10.42.0.0/16"}
	seedConvergedUnion(t, r, s, pushed)
	owner := tenantOwner(t, s, "cust_a")
	ninth, claims := fillProofCapWithRefusedWrites(t, r, s, tn)

	// A cluster of this tenant is deleted while the document is wedged, leaving the
	// durable tombstone the withdrawal is driven from.
	reportRoutes(t, s, "cust_a", "a-2", ninth)
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_a", "a-2", ninth); err != nil {
		t.Fatalf("seed the tombstone: %v", err)
	}
	deleteCluster(t, s, "cust_a", owner, "a-2")

	if len(claims) < 6 {
		t.Fatalf("the fill left %d claims, want a set deep enough to seed duplicates from", len(claims))
	}
	tn.setDoc(sharedDoc(t, [][]string{pushed, claims[2], claims[5]}))
	return r, s, tn, pushed, claims, ninth
}

// requireACLTranscript checks the shape of one pass's /acl traffic — method and
// status in order — and hands the transcript back for the assertions that read
// what each request carried.
func requireACLTranscript(t *testing.T, tn *etagTailnet, want []aclRequest) []aclRequest {
	t.Helper()
	got := tn.requests()
	if len(got) != len(want) {
		t.Fatalf("the pass made %d ACL calls, want %d: %s", len(got), len(want), describeACLs(got))
	}
	for i, w := range want {
		if got[i].method != w.method || got[i].status != w.status {
			t.Fatalf("ACL call %d was %s -> %d, want %s -> %d: %s",
				i, got[i].method, got[i].status, w.method, w.status, describeACLs(got))
		}
	}
	return got
}

func describeACLs(reqs []aclRequest) string {
	var b strings.Builder
	for i, r := range reqs {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s->%d", r.method, r.status)
	}
	return b.String()
}

// The proof-cap recovery end to end against the REAL ACL client and a REAL ACL
// API, on the document shape the collapse exists for: three rules central can
// prove it owns, one of them the recorded pushed union.
//
// The client's own unit tests prove that write in isolation; what was untested is
// the SEAM. Every earlier reconciler test either mocked the client — which hands
// back a target no tailnet ever carried — or reached the cap on a document holding
// ONE central rule, where the collapse selects it, finds the document already says
// so, and stops at the GET without writing. The duplicate grants are what make the
// collapse POST, and the POST is where the guarantees live: the target is chosen
// from the version it replaces, the duplicates go, the claims are retired onto
// exactly what landed, and only then is the desired union claimed and written.
//
// The transcript is the evidence, and it is taken inside the API: each request
// carries the durable state that was current when it arrived, so the ORDER between
// the two writes and the rows between them is asserted rather than inferred from
// where it all ended up.
func TestSharedTailnetCollapsePostsAwayDuplicateGrantsOnALiveTailnet(t *testing.T) {
	r, s, tn, pushed, claims, ninth := wedgedWithDuplicateGrants(t)
	full := s.SharedTailnetRouteIntent()
	if len(full.Claims) != len(claims) || !slices.Equal(full.Pushed, pushed) {
		t.Fatalf("the wedge left pushed = %v with %d claims, want %v and %d", full.Pushed, len(full.Claims), pushed, len(claims))
	}
	if got := centralSrcs(t, tn.document()); len(got) != 3 {
		t.Fatalf("the document carries %v, want three central-owned copies for the collapse to reduce", got)
	}

	tn.resetRequests()
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("recovery pass: %v", err)
	}
	reqs := requireACLTranscript(t, tn, []aclRequest{
		{method: http.MethodGet, status: http.StatusOK},
		{method: http.MethodPost, status: http.StatusOK},
		{method: http.MethodGet, status: http.StatusOK},
		{method: http.MethodPost, status: http.StatusOK},
	})
	collapseGet, collapsePost, desiredPost := reqs[0], reqs[1], reqs[3]

	// The collapse WROTE, and what it wrote is one central rule where three were.
	if got := centralSrcs(t, collapsePost.body); len(got) != 1 || !slices.Equal(got[0], pushed) {
		t.Fatalf("the collapse installed %v, want exactly the recorded pushed union %v", got, pushed)
	}
	assertOperatorPolicyIntact(t, collapsePost.body)

	// It selected from the DOCUMENT, in the exact version it then replaced: the
	// Etag its write named is the one the GET handed out, and that version really
	// carried the union it installed. A target taken from the durable record alone
	// could name a union the tailnet has never had on it.
	if collapsePost.ifMatch != collapseGet.served {
		t.Fatalf("the collapse wrote against %q, want the version it selected from %q", collapsePost.ifMatch, collapseGet.served)
	}
	version, ok := tn.version(collapsePost.ifMatch)
	if !ok {
		t.Fatalf("no recorded document for %q", collapsePost.ifMatch)
	}
	replaced := centralSrcs(t, version)
	if len(replaced) != 3 {
		t.Fatalf("the version the collapse replaced carried %v, want the three central-owned copies", replaced)
	}
	if !containsUnionIn(replaced, pushed) {
		t.Fatalf("the version the collapse replaced carried %v, want the union it installed %v", replaced, pushed)
	}
	for _, union := range [][]string{claims[2], claims[5]} {
		if containsUnionIn(centralSrcs(t, collapsePost.body), union) {
			t.Fatalf("the collapse kept a duplicate proven copy %v", union)
		}
	}

	// Nothing durable moved before that write returned: it went out with the whole
	// proof set intact, because every rule it may remove is covered by one of them.
	if len(collapsePost.intent.Claims) != len(claims) || !slices.Equal(collapsePost.intent.Pushed, pushed) {
		t.Fatalf("the collapse went out with pushed = %v and %d claims, want the full set of %d intact",
			collapsePost.intent.Pushed, len(collapsePost.intent.Claims), len(claims))
	}

	// And the row between the two writes is the recovery's whole point: the claims
	// collapsed onto the union that LANDED, and the desired one is claimed against
	// that before its POST goes out — an unclaimed write could never be taken back.
	if !slices.Equal(desiredPost.intent.Pushed, pushed) {
		t.Fatalf("the desired write went out with pushed = %v, want the collapse onto %v recorded first", desiredPost.intent.Pushed, pushed)
	}
	if len(desiredPost.intent.Claims) != 1 || !slices.Equal(desiredPost.intent.Claims[0], ninth) {
		t.Fatalf("the desired write went out with claims = %v, want exactly its own re-claim of %v", desiredPost.intent.Claims, ninth)
	}
	if got := centralSrcs(t, desiredPost.body); len(got) != 1 || !slices.Equal(got[0], ninth) {
		t.Fatalf("the desired write installed %v, want the union the tenants are asking for %v", got, ninth)
	}

	// The tailnet ends on exactly the desired union, with nothing of central's
	// beside it and nothing of the operator's missing.
	doc := tn.document()
	if got := centralSrcs(t, doc); len(got) != 1 || !slices.Equal(got[0], ninth) {
		t.Fatalf("the tailnet carries %v, want exactly the desired union %v", got, ninth)
	}
	for _, union := range append([][]string{pushed}, claims...) {
		if !slices.Equal(union, ninth) && containsUnionIn(centralSrcs(t, doc), union) {
			t.Fatalf("a stale central rule survived the recovery: %v", union)
		}
	}
	assertOperatorPolicyIntact(t, doc)

	// Durably complete, and the tenant bookkeeping with it.
	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 || !slices.Equal(intent.Pushed, ninth) {
		t.Fatalf("claims = %v / pushed = %v, want the set collapsed onto the desired union %v", intent.Claims, intent.Pushed, ninth)
	}
	if got := intentOf(s, "cust_a").PushedUnion; !slices.Equal(got, ninth) {
		t.Fatalf("the tenant's pushed union = %v, want the shared document's %v", got, ninth)
	}
	if !r.sharedTailnetConverged() {
		t.Fatal("the recovery finished without reporting convergence")
	}
	// Only now may the tombstone go, and the request that drops it is the real one.
	withdraw := request{policy: true, clusters: []clusterRequest{{id: "a-2", withdraw: true}}}
	if out := r.reconcile(context.Background(), "cust_a", withdraw); out.err != nil {
		t.Fatalf("reconcile after the recovery: %v", out.err)
	}
	if _, ok := intentOf(s, "cust_a").PushedByCluster["a-2"]; ok {
		t.Fatal("the tombstone survived a converged shared document")
	}
}

// A 412 on the collapse throws the SELECTION away with the document it was made
// against, and nothing durable may move until a write actually lands.
//
// The other writer's edit here removes the copy the first pass chose, which is the
// case that matters: installing that choice on the retry would GRANT :10250 to
// CIDRs the document no longer carries, on the strength of a read that is no
// longer true. The retry has to re-read, re-select from what is really there, and
// only then write — with the whole proof set still intact behind it, because the
// rule it may have to remove is covered by one of those claims and by nothing else.
func TestSharedTailnetCollapsePreconditionFailureReselectsBeforeAnythingAdvances(t *testing.T) {
	r, s, tn, pushed, claims, ninth := wedgedWithDuplicateGrants(t)
	survivor := claims[5]

	// The operator saves the policy in the window between central's GET and its
	// POST, taking the copy central was about to keep with it.
	tn.editBeforeNextPost(func(string) string {
		return sharedDoc(t, [][]string{claims[2], survivor})
	})
	tn.resetRequests()
	if err := pushShared(t, r, false); err != nil {
		t.Fatalf("recovery pass across the conflict: %v", err)
	}
	reqs := requireACLTranscript(t, tn, []aclRequest{
		{method: http.MethodGet, status: http.StatusOK},
		{method: http.MethodPost, status: http.StatusPreconditionFailed},
		{method: http.MethodGet, status: http.StatusOK},
		{method: http.MethodPost, status: http.StatusOK},
		{method: http.MethodGet, status: http.StatusOK},
		{method: http.MethodPost, status: http.StatusOK},
	})
	refused, refetch, retry, desiredPost := reqs[1], reqs[2], reqs[3], reqs[5]

	// The refused write carried the stale choice; the retry carries the one the
	// FRESH version can prove, and it read that version after the 412 rather than
	// reusing the document it already had.
	if got := centralSrcs(t, refused.body); len(got) != 1 || !slices.Equal(got[0], pushed) {
		t.Fatalf("the refused write carried %v, want the union the first version made provable %v", got, pushed)
	}
	if refetch.served == refused.ifMatch {
		t.Fatalf("the retry re-read version %q, want the one the other writer left behind", refetch.served)
	}
	if retry.ifMatch != refetch.served {
		t.Fatalf("the retry wrote against %q, want the version it just re-read %q", retry.ifMatch, refetch.served)
	}
	if got := centralSrcs(t, retry.body); len(got) != 1 || !slices.Equal(got[0], survivor) {
		t.Fatalf("the retry installed %v, want the newest union the re-read document still carries %v", got, survivor)
	}
	version, ok := tn.version(retry.ifMatch)
	if !ok {
		t.Fatalf("no recorded document for %q", retry.ifMatch)
	}
	if containsUnionIn(centralSrcs(t, version), pushed) {
		t.Fatalf("the re-read version still carried %v; the conflict this asserts never happened", pushed)
	}
	if !containsUnionIn(centralSrcs(t, version), survivor) {
		t.Fatalf("the re-selected union %v was not on the version the retry wrote against", survivor)
	}

	// NOTHING advanced across the conflict. The 412, the re-read and the write that
	// finally landed all saw the same durable row: the full proof set, and the
	// pushed union the wedge left.
	for _, req := range []aclRequest{refused, refetch, retry} {
		if len(req.intent.Claims) != len(claims) || !slices.Equal(req.intent.Pushed, pushed) {
			t.Fatalf("%s saw pushed = %v with %d claims, want the wedge's %v and %d — a 412 may advance nothing",
				req.method, req.intent.Pushed, len(req.intent.Claims), pushed, len(claims))
		}
	}
	// Only the write that RETURNED moves the record, and it records what landed.
	if !slices.Equal(desiredPost.intent.Pushed, survivor) {
		t.Fatalf("the desired write went out with pushed = %v, want the union the collapse actually landed %v", desiredPost.intent.Pushed, survivor)
	}
	if len(desiredPost.intent.Claims) != 1 || !slices.Equal(desiredPost.intent.Claims[0], ninth) {
		t.Fatalf("the desired write went out with claims = %v, want exactly its own re-claim of %v", desiredPost.intent.Claims, ninth)
	}

	doc := tn.document()
	if got := centralSrcs(t, doc); len(got) != 1 || !slices.Equal(got[0], ninth) {
		t.Fatalf("the tailnet carries %v, want exactly the desired union %v", got, ninth)
	}
	assertOperatorPolicyIntact(t, doc)
	intent := s.SharedTailnetRouteIntent()
	if len(intent.Claims) != 0 || !slices.Equal(intent.Pushed, ninth) {
		t.Fatalf("claims = %v / pushed = %v, want the set collapsed onto the desired union %v", intent.Claims, intent.Pushed, ninth)
	}
	if !r.sharedTailnetConverged() {
		t.Fatal("the pass finished without reporting convergence")
	}
}
