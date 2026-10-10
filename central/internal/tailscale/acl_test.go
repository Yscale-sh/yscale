package tailscale

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// managedDst is the destination central's own kubelet rule names. It is the
// whole identity of that rule on a shared document, so these tests use one
// constant for it and assert on rules that do and do not carry it.
const managedDst = "tag:yscale:10250"

// aclServer is a stand-in for the tailnet ACL API: it answers the OAuth token
// exchange, serves a canned policy document with an Etag, and records exactly
// what was POSTed back. Recording the RAW body is the point — the contract under
// test is what survives a write, and a body decoded through this client's own
// types could not tell a preserved field from a re-synthesized one.
type aclServer struct {
	doc        string
	etag       string
	getStatus  int
	postStatus int

	mu      sync.Mutex
	posted  []byte
	ifMatch string
	posts   int
	gets    int
}

func (a *aclServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/oauth/token"):
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"tok","expires_in":3600}`)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/acl"):
		a.mu.Lock()
		a.gets++
		a.mu.Unlock()
		if a.getStatus != 0 && a.getStatus != http.StatusOK {
			w.WriteHeader(a.getStatus)
			return
		}
		if a.etag != "" {
			w.Header().Set("Etag", a.etag)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, a.doc)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/acl"):
		body, _ := io.ReadAll(r.Body)
		a.mu.Lock()
		a.posted = body
		a.ifMatch = r.Header.Get("If-Match")
		a.posts++
		a.mu.Unlock()
		if a.postStatus != 0 && a.postStatus != http.StatusOK {
			w.WriteHeader(a.postStatus)
			_, _ = io.WriteString(w, "precondition failed")
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (a *aclServer) writes() (body []byte, ifMatch string, posts int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.posted, a.ifMatch, a.posts
}

func newACLClient(t *testing.T, srv http.Handler) *Client {
	t.Helper()
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	c, err := New(Config{ClientID: "id", ClientSecret: "secret", Tailnet: "acme.example", APIBase: ts.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// tenantPolicy is a realistic shared-tailnet document: operator content this
// client does not model at all (groups, hosts, ssh, tests, nodeAttrs), another
// party's tag owners and route approvers, another party's ACL rule carrying a
// field this client has no struct member for, an OPERATOR-authored rule that
// happens to have exactly central's action, proto and destination, and central's
// own kubelet rule — the one carrying managedPrev.
const tenantPolicy = `{
  "groups": {"group:ops": ["alice@example.com"]},
  "hosts": {"jump": "100.64.0.1"},
  "tagOwners": {"tag:other": ["ops@example.com"]},
  "autoApprovers": {"routes": {"10.7.0.0/16": ["tag:other"]}},
  "ssh": [{"action": "accept", "src": ["autogroup:member"], "dst": ["autogroup:self"], "users": ["root"]}],
  "tests": [{"src": "alice@example.com", "accept": ["jump:22"]}],
  "nodeAttrs": [{"target": ["autogroup:member"], "attr": ["funnel"]}],
  "acls": [
    {"action": "accept", "proto": "tcp", "src": ["10.1.0.0/16"], "dst": ["tag:yscale:10250"]},
    {"action": "accept", "src": ["group:ops"], "dst": ["tag:other:22"], "srcPosture": ["posture:latest"]},
    {"action": "accept", "proto": "tcp", "src": ["10.2.0.0/16", "10.3.0.0/16"], "dst": ["tag:yscale:10250"]}
  ]
}`

// managedPrev is the Src central recorded after the write that put its rule on
// tenantPolicy — the whole of its claim to that rule. The FIRST rule in the
// document is shaped identically and is not central's; the difference between
// them is this value and nothing else.
var managedPrev = []string{"10.2.0.0/16", "10.3.0.0/16"}

// managedProof is that same claim in the shape the replace takes it: the
// ownership PROOF SET, which on a tailnet whose last write completed holds
// exactly the recorded union and nothing else.
var managedProof = [][]string{managedPrev}

// decodeDoc reads a policy body into generic JSON so the assertions compare
// STRUCTURE, not this client's view of it. Anything the client dropped on the
// way through shows up as a missing key.
func decodeDoc(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode policy %s: %v", raw, err)
	}
	return doc
}

func aclsOf(t *testing.T, doc map[string]any) []map[string]any {
	t.Helper()
	raw, ok := doc["acls"]
	if !ok {
		t.Fatal("policy has no acls member")
	}
	list, ok := raw.([]any)
	if !ok {
		t.Fatalf("acls = %T, want a JSON array", raw)
	}
	out := make([]map[string]any, 0, len(list))
	for i, entry := range list {
		rule, ok := entry.(map[string]any)
		if !ok {
			t.Fatalf("acls[%d] = %T, want a JSON object", i, entry)
		}
		out = append(out, rule)
	}
	return out
}

// The write central makes to a SHARED policy document may touch exactly one
// thing: the kubelet rule it can PROVE it wrote. Everything else — the
// operator's groups, hosts, SSH rules, tests and node attributes, another
// party's tag owners, route approvers and ACL rules, including fields this
// client has no struct member for, and an operator rule that merely LOOKS like
// central's — has to come back out the far side unchanged. The old struct round
// trip silently dropped every key it did not model, which on this document is
// the operator's SSH policy.
func TestReplaceManagedKubeletACLPreservesUnrelatedPolicy(t *testing.T) {
	srv := &aclServer{doc: tenantPolicy, etag: `W/"v7"`}
	c := newACLClient(t, srv)

	if err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, managedProof, []string{"10.9.0.0/16"}); err != nil {
		t.Fatalf("ReplaceManagedKubeletACL: %v", err)
	}
	body, ifMatch, posts := srv.writes()
	if posts != 1 {
		t.Fatalf("POSTs = %d, want 1", posts)
	}
	// Optimistic concurrency: the write carries the Etag the read observed, so a
	// document that moved underneath it is a 412 rather than a clobber.
	if ifMatch != `W/"v7"` {
		t.Fatalf("If-Match = %q, want the Etag the GET returned", ifMatch)
	}

	before, after := decodeDoc(t, []byte(tenantPolicy)), decodeDoc(t, body)
	for _, key := range []string{"groups", "hosts", "tagOwners", "autoApprovers", "ssh", "tests", "nodeAttrs"} {
		if _, ok := after[key]; !ok {
			t.Fatalf("%q was dropped from the written policy", key)
		}
		if !reflect.DeepEqual(before[key], after[key]) {
			t.Errorf("%q changed:\n before %v\n after  %v", key, before[key], after[key])
		}
	}

	// Three rules still: the operator's lookalike, the unrelated rule, and
	// central's own replaced IN PLACE.
	rules, want := aclsOf(t, after), aclsOf(t, before)
	if len(rules) != 3 {
		t.Fatalf("acls = %d rules, want 3 (two preserved, one replaced): %v", len(rules), rules)
	}
	if !reflect.DeepEqual(rules[0], want[0]) {
		t.Fatalf("the operator's accept/tcp/%s rule was rewritten:\n before %v\n after  %v", managedDst, want[0], rules[0])
	}
	if !reflect.DeepEqual(rules[1], want[1]) {
		t.Fatalf("the unrelated rule changed:\n before %v\n after  %v", want[1], rules[1])
	}
	managed := rules[2]
	if got := managed["dst"]; !reflect.DeepEqual(got, []any{managedDst}) {
		t.Fatalf("acls[2] dst = %v, want the managed destination in the replaced rule's slot", got)
	}
	if got := managed["src"]; !reflect.DeepEqual(got, []any{"10.9.0.0/16"}) {
		t.Errorf("managed src = %v, want exactly the requested union", got)
	}
	if managed["action"] != "accept" || managed["proto"] != "tcp" {
		t.Errorf("managed rule = %v, want accept/tcp", managed)
	}
}

// An empty union is a REMOVAL, and the removal is a real write. The additive
// merge could not express it at all: with no rule to add it had nothing to say,
// so a CIDR that stopped being any tenant's gateway kept its :10250 grant. Only
// the rule central can prove it wrote goes; the operator's lookalike stays.
func TestReplaceManagedKubeletACLEmptySrcRemovesTheRule(t *testing.T) {
	srv := &aclServer{doc: tenantPolicy, etag: `W/"v7"`}
	c := newACLClient(t, srv)

	if err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, managedProof, nil); err != nil {
		t.Fatalf("ReplaceManagedKubeletACL: %v", err)
	}
	body, _, posts := srv.writes()
	if posts != 1 {
		t.Fatalf("POSTs = %d, want 1 — an empty union must still be written", posts)
	}
	rules := aclsOf(t, decodeDoc(t, body))
	if len(rules) != 2 {
		t.Fatalf("acls = %v, want the operator's two rules left", rules)
	}
	for _, rule := range rules {
		if reflect.DeepEqual(rule["src"], []any{"10.2.0.0/16", "10.3.0.0/16"}) {
			t.Fatal("the managed rule survived an empty union")
		}
	}
	if got := rules[0]["src"]; !reflect.DeepEqual(got, []any{"10.1.0.0/16"}) {
		t.Fatalf("acls[0] src = %v, want the operator's lookalike untouched", got)
	}
	// The operator's content is still untouched on the removal path.
	if _, ok := decodeDoc(t, body)["ssh"]; !ok {
		t.Fatal("removing the managed rule dropped the operator's ssh policy")
	}
}

// Without a recorded Src central has no claim to ANY rule on the document, so
// the first adoption of a tailnet adds its rule and deletes nothing — even
// though a rule with the same action, proto and destination is sitting right
// there. The alternative is deleting an operator's rule on the strength of a
// destination central does not have exclusive use of.
func TestReplaceManagedKubeletACLFirstAdoptionDeletesNothing(t *testing.T) {
	srv := &aclServer{doc: tenantPolicy, etag: `W/"v7"`}
	c := newACLClient(t, srv)

	if err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, nil, []string{"10.9.0.0/16"}); err != nil {
		t.Fatalf("ReplaceManagedKubeletACL: %v", err)
	}
	body, _, posts := srv.writes()
	if posts != 1 {
		t.Fatalf("POSTs = %d, want 1", posts)
	}
	rules, want := aclsOf(t, decodeDoc(t, body)), aclsOf(t, decodeDoc(t, []byte(tenantPolicy)))
	if len(rules) != 4 {
		t.Fatalf("acls = %d rules, want 4 (all three kept plus the appended one): %v", len(rules), rules)
	}
	for i := range want {
		if !reflect.DeepEqual(rules[i], want[i]) {
			t.Errorf("acls[%d] changed with nothing to prove ownership:\n before %v\n after  %v", i, want[i], rules[i])
		}
	}
	if got := rules[3]["src"]; !reflect.DeepEqual(got, []any{"10.9.0.0/16"}) {
		t.Fatalf("appended rule src = %v, want the requested union", got)
	}
}

// The gap between the two writes this converges: the ACL POST lands and the
// record of what it wrote does not. The retry arrives with a stale prev against
// a rule carrying exactly the Src it just installed — which it recognises, so it
// replaces that rule with itself instead of appending a second copy. Nothing
// changes, so nothing is POSTed, and the caller is free to record.
func TestReplaceManagedKubeletACLRecoversFromALostRecord(t *testing.T) {
	srv := &aclServer{doc: tenantPolicy, etag: `W/"v7"`}
	c := newACLClient(t, srv)

	// prev is a union two writes old; the document carries what actually landed.
	stale := []string{"10.8.0.0/16"}
	if err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, [][]string{stale}, managedPrev); err != nil {
		t.Fatalf("ReplaceManagedKubeletACL: %v", err)
	}
	if _, _, posts := srv.writes(); posts != 0 {
		t.Fatalf("POSTs = %d, want 0 — the rule already says what it should", posts)
	}
}

// A forced re-assert of a converged tailnet reads the document and stops. An
// unconditional write would burn the Etag — turning a concurrent operator edit
// into a 412 for no reason — every time a central booted.
func TestReplaceManagedKubeletACLUnchangedWriteIsNotPosted(t *testing.T) {
	srv := &aclServer{doc: tenantPolicy, etag: `W/"v7"`}
	c := newACLClient(t, srv)

	if err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, managedProof, managedPrev); err != nil {
		t.Fatalf("ReplaceManagedKubeletACL: %v", err)
	}
	if _, _, posts := srv.writes(); posts != 0 {
		t.Fatalf("POSTs = %d, want 0 for an already-converged document", posts)
	}
}

// A tailnet with no policy yet answers 404. That seeds an empty document and a
// write with no If-Match — the same first-write path EnsurePolicy has always
// taken.
func TestReplaceManagedKubeletACLSeedsAnEmptyPolicy(t *testing.T) {
	srv := &aclServer{getStatus: http.StatusNotFound}
	c := newACLClient(t, srv)

	if err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, nil, []string{"10.9.0.0/16"}); err != nil {
		t.Fatalf("ReplaceManagedKubeletACL: %v", err)
	}
	body, ifMatch, posts := srv.writes()
	if posts != 1 {
		t.Fatalf("POSTs = %d, want 1", posts)
	}
	if ifMatch != "" {
		t.Fatalf("If-Match = %q, want none when the GET had no Etag", ifMatch)
	}
	rules := aclsOf(t, decodeDoc(t, body))
	if len(rules) != 1 || !reflect.DeepEqual(rules[0]["src"], []any{"10.9.0.0/16"}) {
		t.Fatalf("seeded acls = %v, want the one managed rule", rules)
	}
}

// No policy on the tailnet AND nothing to grant is not a policy to create. A
// write here would author `{"acls":[]}` on a tailnet whose ACL is entirely
// operator-managed — which is a deny-all, not a no-op.
func TestReplaceManagedKubeletACLMissingPolicyWithEmptyUnionWritesNothing(t *testing.T) {
	srv := &aclServer{getStatus: http.StatusNotFound}
	c := newACLClient(t, srv)

	if err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, nil, nil); err != nil {
		t.Fatalf("ReplaceManagedKubeletACL: %v", err)
	}
	if _, _, posts := srv.writes(); posts != 0 {
		t.Fatalf("POSTs = %d, want 0 — an absent policy plus an empty union is a no-op", posts)
	}
}

// A concurrent editor wins the Etag and this write is refused. That has to
// surface as an ERROR: it is what puts the work back on the caller's retry
// ladder, and the retry re-reads both the document and the union — which is how
// two tenants changing at once converge instead of taking turns clobbering. The
// unrelated edit the other writer made is still on the document afterwards,
// because this write never landed.
func TestReplaceManagedKubeletACLConflictIsRetryable(t *testing.T) {
	srv := &aclServer{doc: tenantPolicy, etag: `W/"v7"`, postStatus: http.StatusPreconditionFailed}
	c := newACLClient(t, srv)

	err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, managedProof, []string{"10.9.0.0/16"})
	if err == nil {
		t.Fatal("a refused write reported success; the caller would never retry it")
	}
	if !strings.Contains(err.Error(), "412") {
		t.Fatalf("error = %v, want the precondition status in it", err)
	}
	body, _, posts := srv.writes()
	if posts != 1 {
		t.Fatalf("POSTs = %d, want 1 — the client must not retry internally", posts)
	}
	// Even the refused body carried every unrelated entry: a 412 that had been
	// accepted must not have been a document that dropped the operator's policy.
	before, attempted := decodeDoc(t, []byte(tenantPolicy)), decodeDoc(t, body)
	for _, key := range []string{"groups", "hosts", "tagOwners", "autoApprovers", "ssh", "tests", "nodeAttrs"} {
		if !reflect.DeepEqual(before[key], attempted[key]) {
			t.Errorf("%q would have been changed by the refused write", key)
		}
	}
}

// A rule this client cannot decode is not a rule it may guess about: it could be
// the managed one in a shape the API changed, or someone else's that a write
// would drop. Refusing the whole write leaves the document exactly as it was.
func TestReplaceManagedKubeletACLFailsClosedOnAnUnreadableRule(t *testing.T) {
	srv := &aclServer{doc: `{"acls": ["this is not a rule object"]}`, etag: `W/"v1"`}
	c := newACLClient(t, srv)

	if err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, managedProof, []string{"10.9.0.0/16"}); err == nil {
		t.Fatal("a policy with an undecodable rule was written anyway")
	}
	if _, _, posts := srv.writes(); posts != 0 {
		t.Fatalf("POSTs = %d, want 0 — nothing may be written on an unreadable document", posts)
	}
}

// An empty destination would make every rule on the document look unmanaged (or,
// worse, match one). It is refused before a single request goes out.
func TestReplaceManagedKubeletACLRequiresADestination(t *testing.T) {
	srv := &aclServer{doc: tenantPolicy}
	c := newACLClient(t, srv)

	if err := c.ReplaceManagedKubeletACL(context.Background(), "  ", managedProof, []string{"10.9.0.0/16"}); err == nil {
		t.Fatal("a write with no destination was accepted")
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.gets != 0 || srv.posts != 0 {
		t.Fatalf("requests made = %d GET / %d POST, want none", srv.gets, srv.posts)
	}
}

// The rule identity is the (action, proto, dst) triple AND the recorded Src. A
// rule on the same tag at a DIFFERENT port, on a different tag at :10250, on a
// different proto, on more than one destination, or simply with a Src central
// never wrote, belongs to someone else and is never touched.
func TestReplaceManagedACLRuleIdentityIsTheDestinationAndTheRecordedSrc(t *testing.T) {
	doc := aclDoc{"acls": json.RawMessage(`[
		{"action":"accept","proto":"tcp","src":["10.2.0.0/16"],"dst":["tag:yscale:22"]},
		{"action":"accept","proto":"tcp","src":["10.2.0.0/16"],"dst":["tag:other:10250"]},
		{"action":"accept","proto":"udp","src":["10.2.0.0/16"],"dst":["tag:yscale:10250"]},
		{"action":"accept","proto":"tcp","src":["10.2.0.0/16"],"dst":["tag:yscale:10250","tag:other:10250"]},
		{"action":"accept","proto":"tcp","src":["10.4.0.0/16"],"dst":["tag:yscale:10250"]},
		{"action":"accept","proto":"tcp","src":["10.2.0.0/16"],"dst":["tag:yscale:10250"]}
	]`)}

	next, changed, err := replaceManagedACLRule(doc, managedDst, [][]string{{"10.2.0.0/16"}}, []string{"10.9.0.0/16"})
	if err != nil {
		t.Fatalf("replaceManagedACLRule: %v", err)
	}
	if !changed {
		t.Fatal("the replacement was reported as no change")
	}
	rules := aclsOf(t, decodeDoc(t, mustMarshal(t, next)))
	if len(rules) != 6 {
		t.Fatalf("acls = %d rules, want 6 (five untouched plus the one replacement): %v", len(rules), rules)
	}
	// Only the last rule matched on BOTH halves of the identity, so the
	// replacement lands in ITS slot and the five near-misses keep their order.
	if got := rules[5]["src"]; !reflect.DeepEqual(got, []any{"10.9.0.0/16"}) {
		t.Fatalf("acls[5] src = %v, want the replacement in the matched rule's slot", got)
	}
	for i, want := range []string{"10.2.0.0/16", "10.2.0.0/16", "10.2.0.0/16", "10.2.0.0/16", "10.4.0.0/16"} {
		if got := rules[i]["src"]; !reflect.DeepEqual(got, []any{want}) {
			t.Errorf("acls[%d] src = %v, want the untouched %v", i, got, want)
		}
	}
}

// A document that has never carried an acls member must come back with one, and
// an emptied list must be [] rather than null — the API rejects the latter.
func TestReplaceManagedACLRuleAlwaysWritesAnACLList(t *testing.T) {
	seeded, changed, err := replaceManagedACLRule(aclDoc{}, managedDst, nil, []string{"10.9.0.0/16"})
	if err != nil {
		t.Fatalf("replaceManagedACLRule: %v", err)
	}
	if !changed {
		t.Fatal("seeding a rule onto an empty document was reported as no change")
	}
	if rules := aclsOf(t, decodeDoc(t, mustMarshal(t, seeded))); len(rules) != 1 {
		t.Fatalf("acls = %v, want the one managed rule", rules)
	}
	emptied, changed, err := replaceManagedACLRule(
		aclDoc{"acls": json.RawMessage(`[{"action":"accept","proto":"tcp","src":["10.9.0.0/16"],"dst":["tag:yscale:10250"]}]`)},
		managedDst, [][]string{{"10.9.0.0/16"}}, nil)
	if err != nil {
		t.Fatalf("replaceManagedACLRule removing the last rule: %v", err)
	}
	if !changed {
		t.Fatal("removing the managed rule was reported as no change")
	}
	if got := string(emptied["acls"]); got != "[]" {
		t.Fatalf("acls = %s, want []", got)
	}
	nulled, _, err := replaceManagedACLRule(aclDoc{"acls": json.RawMessage(`null`)}, managedDst, nil, []string{"10.9.0.0/16"})
	if err != nil {
		t.Fatalf("replaceManagedACLRule over a null acls: %v", err)
	}
	if rules := aclsOf(t, decodeDoc(t, mustMarshal(t, nulled))); len(rules) != 1 {
		t.Fatalf("acls = %v, want the one managed rule", rules)
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// retryACLServer refuses the first POST with a 412 and serves a document that
// moved in the meantime — an operator editing the same policy while central
// writes. It is the only way to observe the whole read-modify-write, which is
// what actually has to preserve the other writer's edit.
type retryACLServer struct {
	aclServer
	next string // the document served from the second GET on
}

func (a *retryACLServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	if a.posts == 1 {
		a.doc, a.etag, a.postStatus = a.next, `W/"v8"`, 0
	}
	a.mu.Unlock()
	a.aclServer.ServeHTTP(w, r)
}

// A 412 is not a lost write: the caller retries the whole read-modify-write, and
// the second pass reads the document the OTHER writer landed. Their edit is
// still there afterwards, and central's rule is replaced rather than duplicated
// — the retry recognises it by the Src the refused attempt never got to change.
func TestReplaceManagedKubeletACLRetryPreservesAConcurrentEdit(t *testing.T) {
	edited := strings.Replace(tenantPolicy,
		`"hosts": {"jump": "100.64.0.1"}`,
		`"hosts": {"jump": "100.64.0.1", "bastion": "100.64.0.9"}`, 1)
	if edited == tenantPolicy {
		t.Fatal("the fixture did not take the concurrent edit")
	}
	srv := &retryACLServer{
		aclServer: aclServer{doc: tenantPolicy, etag: `W/"v7"`, postStatus: http.StatusPreconditionFailed},
		next:      edited,
	}
	c := newACLClient(t, srv)

	if err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, managedProof, []string{"10.9.0.0/16"}); err == nil {
		t.Fatal("the conflicting write reported success")
	}
	// The retry the caller's ladder makes.
	if err := c.ReplaceManagedKubeletACL(context.Background(), managedDst, managedProof, []string{"10.9.0.0/16"}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	body, ifMatch, posts := srv.writes()
	if posts != 2 {
		t.Fatalf("POSTs = %d, want 2 (the refused one and the retry)", posts)
	}
	if ifMatch != `W/"v8"` {
		t.Fatalf("If-Match = %q, want the Etag of the document the retry read", ifMatch)
	}
	after := decodeDoc(t, body)
	hosts, _ := after["hosts"].(map[string]any)
	if hosts["bastion"] != "100.64.0.9" {
		t.Fatalf("hosts = %v, want the concurrent editor's addition preserved", after["hosts"])
	}
	rules := aclsOf(t, after)
	if len(rules) != 3 {
		t.Fatalf("acls = %d rules, want 3 — the retry must replace central's rule, not add another: %v", len(rules), rules)
	}
	if got := rules[2]["src"]; !reflect.DeepEqual(got, []any{"10.9.0.0/16"}) {
		t.Fatalf("acls[2] src = %v, want the replacement in central's own slot", got)
	}
	if got := rules[0]["src"]; !reflect.DeepEqual(got, []any{"10.1.0.0/16"}) {
		t.Fatalf("acls[0] src = %v, want the operator's lookalike still untouched", got)
	}
}

// --- the cap recovery's collapse ---

// collapseDoc builds a policy document carrying the operator's lookalike rule
// first — same action, proto and destination as central's, a Src central never
// claimed — followed by one central-shaped rule per union given. It is the shape
// every collapse assertion is made against: what survives, and what is left.
func collapseDoc(srcs ...[]string) string {
	rules := []string{`{"action":"accept","proto":"tcp","src":["10.1.0.0/16"],"dst":["` + managedDst + `"]}`}
	for _, src := range srcs {
		quoted := make([]string, 0, len(src))
		for _, cidr := range src {
			quoted = append(quoted, `"`+cidr+`"`)
		}
		rules = append(rules, `{"action":"accept","proto":"tcp","src":[`+strings.Join(quoted, ",")+`],"dst":["`+managedDst+`"]}`)
	}
	return `{"hosts": {"jump": "100.64.0.1"}, "acls": [` + strings.Join(rules, ",") + `]}`
}

// operatorSrc is the Src of that lookalike rule. Nothing central writes may take
// it, on any path.
var operatorSrc = []any{"10.1.0.0/16"}

// srcsOf names the Src of every rule on a written document, in order.
func srcsOf(t *testing.T, raw []byte) []any {
	t.Helper()
	var out []any
	for _, rule := range aclsOf(t, decodeDoc(t, raw)) {
		out = append(out, rule["src"])
	}
	return out
}

// A claim is durable BEFORE its POST, so the newest one can name a union the
// tailnet has never carried — a whole ACL API outage fills the set with exactly
// those. The collapse must therefore pick from what the DOCUMENT says, not from
// what the record claims: here that is the pushed union, and the absent claims are
// neither selected nor written anywhere.
func TestCollapseManagedKubeletACLNeverInstallsAUnionTheDocumentLacks(t *testing.T) {
	pushed := []string{"10.2.0.0/16"}
	absent, newest := []string{"10.30.0.0/16"}, []string{"10.31.0.0/16"}
	srv := &aclServer{doc: collapseDoc(pushed), etag: `W/"v7"`}
	c := newACLClient(t, srv)

	landed, err := c.CollapseManagedKubeletACL(context.Background(), managedDst, pushed, [][]string{absent, newest})
	if err != nil {
		t.Fatalf("CollapseManagedKubeletACL: %v", err)
	}
	if !reflect.DeepEqual(landed, pushed) {
		t.Fatalf("landed = %v, want the one union the document carries %v", landed, pushed)
	}
	if _, _, posts := srv.writes(); posts != 0 {
		t.Fatalf("POSTs = %d, want 0 — the document already says exactly what was selected", posts)
	}
}

// The recorded PUSHED union wins over a claim even when both are on the document.
// It is the only one central has a completed write for, and the collapse's job is
// to leave ONE rule behind: the extra proven copy goes, the operator's lookalike
// stays, and the claim that was never live is nowhere in the result.
func TestCollapseManagedKubeletACLPrefersTheLivePushedUnion(t *testing.T) {
	pushed := []string{"10.2.0.0/16"}
	live, absent := []string{"10.30.0.0/16"}, []string{"10.31.0.0/16"}
	srv := &aclServer{doc: collapseDoc(live, pushed), etag: `W/"v7"`}
	c := newACLClient(t, srv)

	landed, err := c.CollapseManagedKubeletACL(context.Background(), managedDst, pushed, [][]string{live, absent})
	if err != nil {
		t.Fatalf("CollapseManagedKubeletACL: %v", err)
	}
	if !reflect.DeepEqual(landed, pushed) {
		t.Fatalf("landed = %v, want the recorded union %v over a merely claimed one", landed, pushed)
	}
	body, ifMatch, posts := srv.writes()
	if posts != 1 || ifMatch != `W/"v7"` {
		t.Fatalf("POSTs = %d with If-Match %q, want one Etag-guarded write", posts, ifMatch)
	}
	if got, want := srcsOf(t, body), []any{operatorSrc, []any{"10.2.0.0/16"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acls = %v, want the operator's rule and exactly one central copy %v", got, want)
	}
}

// With the pushed union gone from the document — a lost receipt replaced it — the
// newest claim that is ACTUALLY there wins, and the older live copy goes with the
// rest. A newer claim the document does not carry is not a candidate at all.
func TestCollapseManagedKubeletACLTakesTheNewestClaimTheDocumentCarries(t *testing.T) {
	pushed := []string{"10.2.0.0/16"}
	older, newer, absent := []string{"10.30.0.0/16"}, []string{"10.31.0.0/16"}, []string{"10.32.0.0/16"}
	srv := &aclServer{doc: collapseDoc(older, newer), etag: `W/"v7"`}
	c := newACLClient(t, srv)

	landed, err := c.CollapseManagedKubeletACL(context.Background(), managedDst, pushed, [][]string{older, newer, absent})
	if err != nil {
		t.Fatalf("CollapseManagedKubeletACL: %v", err)
	}
	if !reflect.DeepEqual(landed, newer) {
		t.Fatalf("landed = %v, want the newest claim the document carries %v", landed, newer)
	}
	body, _, posts := srv.writes()
	if posts != 1 {
		t.Fatalf("POSTs = %d, want the one collapse", posts)
	}
	if got, want := srcsOf(t, body), []any{operatorSrc, []any{"10.31.0.0/16"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acls = %v, want the older proven copy collapsed into %v", got, want)
	}
}

// Nothing central can prove is on the document leaves NOTHING to install. The
// collapse writes no grant at all — inventing one from the record is the re-grant
// this selection exists to prevent — and the operator's identically shaped rule,
// which was never claimed, is not central's to take.
func TestCollapseManagedKubeletACLInstallsNothingWhenNoProvenRuleIsLive(t *testing.T) {
	pushed := []string{"10.2.0.0/16"}
	claims := [][]string{{"10.30.0.0/16"}, {"10.31.0.0/16"}}
	srv := &aclServer{doc: collapseDoc(), etag: `W/"v7"`}
	c := newACLClient(t, srv)

	landed, err := c.CollapseManagedKubeletACL(context.Background(), managedDst, pushed, claims)
	if err != nil {
		t.Fatalf("CollapseManagedKubeletACL: %v", err)
	}
	if len(landed) != 0 {
		t.Fatalf("landed = %v, want an empty target — no proven rule is on this document", landed)
	}
	if _, _, posts := srv.writes(); posts != 0 {
		t.Fatalf("POSTs = %d, want 0 — there is nothing of central's here to remove", posts)
	}
}

// Every proven copy the document holds is collapsed into ONE, and the one left is
// a union that was already there. Three copies in, one out.
func TestCollapseManagedKubeletACLCollapsesEveryProvenCopy(t *testing.T) {
	pushed := []string{"10.2.0.0/16"}
	c1, c2 := []string{"10.30.0.0/16"}, []string{"10.31.0.0/16"}
	srv := &aclServer{doc: collapseDoc(c1, pushed, c2), etag: `W/"v7"`}
	c := newACLClient(t, srv)

	landed, err := c.CollapseManagedKubeletACL(context.Background(), managedDst, pushed, [][]string{c1, c2})
	if err != nil {
		t.Fatalf("CollapseManagedKubeletACL: %v", err)
	}
	if !reflect.DeepEqual(landed, pushed) {
		t.Fatalf("landed = %v, want the recorded union %v", landed, pushed)
	}
	body, _, posts := srv.writes()
	if posts != 1 {
		t.Fatalf("POSTs = %d, want the one collapse", posts)
	}
	if got, want := srcsOf(t, body), []any{operatorSrc, []any{"10.2.0.0/16"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acls = %v, want three central copies collapsed into %v", got, want)
	}
}

// The selection and the write share ONE Etag, and a 412 throws the selection away
// with the document it was made against. The first pass would have left the
// newest live claim; by the time the retry reads, another writer has replaced
// that rule with the pushed union — so installing the first pass's choice would
// GRANT CIDRs the document no longer carries. The retry re-selects, and the other
// writer's unrelated edit survives.
func TestCollapseManagedKubeletACLReselectsAfterAConflict(t *testing.T) {
	pushed := []string{"10.2.0.0/16"}
	stale, other := []string{"10.30.0.0/16"}, []string{"10.31.0.0/16"}
	edited := strings.Replace(collapseDoc(other, pushed),
		`"hosts": {"jump": "100.64.0.1"}`,
		`"hosts": {"jump": "100.64.0.1", "bastion": "100.64.0.9"}`, 1)
	srv := &retryACLServer{
		aclServer: aclServer{doc: collapseDoc(other, stale), etag: `W/"v7"`, postStatus: http.StatusPreconditionFailed},
		next:      edited,
	}
	c := newACLClient(t, srv)

	landed, err := c.CollapseManagedKubeletACL(context.Background(), managedDst, pushed, [][]string{other, stale})
	if err != nil {
		t.Fatalf("CollapseManagedKubeletACL: %v", err)
	}
	if !reflect.DeepEqual(landed, pushed) {
		t.Fatalf("landed = %v, want the union the RETRY's document carries %v", landed, pushed)
	}
	body, ifMatch, posts := srv.writes()
	if posts != 2 {
		t.Fatalf("POSTs = %d, want the refused one and the re-selected retry", posts)
	}
	if ifMatch != `W/"v8"` {
		t.Fatalf("If-Match = %q, want the Etag of the document the retry selected against", ifMatch)
	}
	after := decodeDoc(t, body)
	if hosts, _ := after["hosts"].(map[string]any); hosts["bastion"] != "100.64.0.9" {
		t.Fatalf("hosts = %v, want the concurrent editor's addition preserved", after["hosts"])
	}
	if got, want := srcsOf(t, body), []any{operatorSrc, []any{"10.2.0.0/16"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acls = %v, want the re-selected union %v and nothing the first pass chose", got, want)
	}
}
