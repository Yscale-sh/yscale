package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
	"gopkg.in/yaml.v3"
)

// failingDecider fails every Plan the way the decider does when scheduling
// falls over after it may already have reached a backend — the ambiguous case
// the claim must NOT let a retry re-enter.
type failingDecider struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (d *failingDecider) Plan(context.Context, *workload.Workload, PlanOptions) (*Plan, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	if d.err != nil {
		return nil, d.err
	}
	return nil, errors.New("no capacity in any region")
}

func (d *failingDecider) planCalls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func (d *fakeDecider) planCalls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// idemFixture is one tenant with a connected connector, ready to submit.
type idemFixture struct {
	store *state.Store
	cust  *state.Customer
	h     *Workloads
}

func newIdemFixture(t *testing.T, dec Decider, namespaces ...string) *idemFixture {
	t.Helper()
	store := state.New()
	cust := &state.Customer{ID: "cust_idem", Token: "tok_idem", WorkloadNamespaces: namespaces}
	store.AddCustomer(cust)
	store.AddAgent(&state.Agent{
		ID: "agent_idem", CustomerID: cust.ID, ClusterID: "cluster_idem",
		Send: make(chan protocol.Envelope, 256),
	})
	return &idemFixture{
		store: store,
		cust:  cust,
		h: &Workloads{
			Store:   store,
			Decider: dec,
			Reaper:  &fakeReaper{},
			Log:     quietLog(),
		},
	}
}

const idemSpec = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: idem-test
spec:
  image: busybox
  size: small`

// submitOpts is one submission's deviation from the fixture's default: a
// different key, a different spec, a named cluster, or a cluster credential
// instead of a human.
type submitOpts struct {
	key       string
	spec      string
	cluster   bool
	clusterID string
	// The launch-template pair the console sends. templateVersion is a string
	// rather than an int so a test can send a malformed one, and either may be
	// set alone to exercise half a reference.
	templateID      string
	templateVersion string
	// The submission path a connector reports. A string rather than a
	// protocol.SubmissionOrigin so a test can send a value the closed set does
	// not contain, which is the whole point of central validating it.
	origin string
}

func (f *idemFixture) submit(o submitOpts) *httptest.ResponseRecorder {
	return submitTo(f.h, f.cust, o)
}

// submitTo is one submission against a handler, shaped the way the surface that
// made it would: a human with a submitter in the context, or a cluster
// credential with none.
func submitTo(h *Workloads, cust *state.Customer, o submitOpts) *httptest.ResponseRecorder {
	spec := o.spec
	if spec == "" {
		spec = idemSpec
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(spec))
	ctx := context.WithValue(context.Background(), ctxCustomer, cust)
	if !o.cluster {
		// What the human surface injects: HandleCreateWorkload resolves the
		// account and its role before handing the request to Create.
		ctx = withSubmitter(ctx, submitter{
			Actor: state.HumanActor("acct_idem", cust.ID),
			Role:  state.RoleOwner,
		})
	}
	if o.key != "" {
		req.Header.Set(idempotencyHeader, o.key)
	}
	if o.clusterID != "" {
		req.Header.Set(clusterIDHeader, o.clusterID)
	}
	if o.templateID != "" {
		req.Header.Set(templateIDHeader, o.templateID)
	}
	if o.templateVersion != "" {
		req.Header.Set(templateVersionHeader, o.templateVersion)
	}
	if o.origin != "" {
		req.Header.Set(protocol.WorkloadOriginHeader, o.origin)
	}
	rec := httptest.NewRecorder()
	h.Create(rec, req.WithContext(ctx))
	return rec
}

// A human's client is the one that retries: a browser that gave up, a proxy
// that replayed the POST, a double click. Without a key there is nothing to
// deduplicate on, so the submission is refused before anything can provision.
func TestCreate_HumanSubmitRequiresIdempotencyKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  string
	}{
		{name: "missing"},
		{name: "too short", key: "abc"},
		{name: "with a space", key: "key with space"},
		{name: "with a control character", key: "key\nwith-newline"},
		{name: "non-ascii", key: "clé-de-retry-très-longue"},
		{name: "too long", key: strings.Repeat("k", maxIdempotencyKeyBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dec := &fakeDecider{}
			f := newIdemFixture(t, dec)
			rec := f.submit(submitOpts{key: tc.key})

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			if got := dec.planCalls(); got != 0 {
				t.Fatalf("Plan calls = %d, want 0 — a refused key must not reach a provider", got)
			}
			if tc.key != "" && strings.Contains(rec.Body.String(), tc.key) {
				t.Error("the refusal echoed the key back; it must not reach a body or a log")
			}
		})
	}
}

// The cluster-token API shipped before this contract. A connector that sends no
// key keeps working exactly as it did — once, with no claim — because breaking
// every deployed connector to close a browser-retry hole would be the larger
// outage.
func TestCreate_ClusterTokenWithoutKeyStillWorks(t *testing.T) {
	dec := &fakeDecider{}
	f := newIdemFixture(t, dec)

	rec := f.submit(submitOpts{cluster: true})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	if got := dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls = %d, want 1", got)
	}
	// Unkeyed means unprotected, which is the pre-existing contract: a second
	// identical submission is a second workload, exactly as before.
	if rec2 := f.submit(submitOpts{cluster: true}); rec2.Code != http.StatusAccepted {
		t.Fatalf("second unkeyed submit = %d, want 202", rec2.Code)
	}
	if got := dec.planCalls(); got != 2 {
		t.Fatalf("Plan calls after two unkeyed submits = %d, want 2 (behaviour unchanged)", got)
	}
}

// The core replay: the same key and the same request gets the same answer back,
// byte for byte, and buys exactly one node.
func TestCreate_KeyedReplayIsByteIdentical(t *testing.T) {
	dec := &fakeDecider{}
	f := newIdemFixture(t, dec)

	first := f.submit(submitOpts{key: "retry-token-0001"})
	if first.Code != http.StatusAccepted {
		t.Fatalf("first submit = %d, want 202: %s", first.Code, first.Body)
	}

	second := f.submit(submitOpts{key: "retry-token-0001"})
	if second.Code != first.Code {
		t.Fatalf("replay status = %d, want %d", second.Code, first.Code)
	}
	if !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) {
		t.Fatalf("replay body = %q, want the original %q", second.Body, first.Body)
	}
	if got := second.Header().Get("Idempotency-Replayed"); got != "true" {
		t.Errorf("Idempotency-Replayed = %q, want \"true\"", got)
	}
	if got := dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls = %d, want 1 — the replay provisioned a second node", got)
	}
	if got := len(f.store.BurstsForCustomer(f.cust.ID)); got != 1 {
		t.Fatalf("bursts = %d, want 1", got)
	}
}

// The race the whole slice exists for: N arrivals of one keyed request, one
// provider call. Run under -race.
func TestCreate_ConcurrentKeyedSubmitsProvisionOnce(t *testing.T) {
	dec := &fakeDecider{delay: 20 * time.Millisecond}
	f := newIdemFixture(t, dec)

	const n = 8
	start := make(chan struct{})
	codes := make([]int, n)
	bodies := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			rec := f.submit(submitOpts{key: "concurrent-retry-01"})
			codes[i] = rec.Code
			bodies[i] = rec.Body.String()
		}(i)
	}
	close(start)
	wg.Wait()

	if got := dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls = %d, want exactly 1 — every extra one is a paid node", got)
	}
	if got := len(f.store.BurstsForCustomer(f.cust.ID)); got != 1 {
		t.Fatalf("bursts = %d, want 1", got)
	}

	// Every caller gets one of two honest answers: the submission's own result,
	// or "it is running, here is its id". Which one depends purely on timing.
	var accepted, waiting int
	var winner string
	for i, code := range codes {
		switch code {
		case http.StatusAccepted:
			accepted++
			if winner == "" {
				winner = bodies[i]
			} else if bodies[i] != winner {
				t.Errorf("two 202s disagreed:\n%s\n%s", winner, bodies[i])
			}
		case http.StatusServiceUnavailable:
			waiting++
			if !strings.Contains(bodies[i], `"status":"in_progress"`) {
				t.Errorf("503 body = %s, want the in-progress answer", bodies[i])
			}
			if !strings.Contains(bodies[i], `"id":"wl_`) {
				t.Errorf("in-progress answer carries no workload id: %s", bodies[i])
			}
		default:
			t.Errorf("unexpected status %d: %s", code, bodies[i])
		}
	}
	if accepted < 1 {
		t.Fatalf("no caller was answered; %d waited", waiting)
	}
}

// A key that already means one workload cannot be made to mean another. The
// refusal lands before the admission reservation and before any provider call,
// so the conflicting request costs nothing.
func TestCreate_SameKeyDifferentSpecConflicts(t *testing.T) {
	dec := &fakeDecider{}
	f := newIdemFixture(t, dec)

	if rec := f.submit(submitOpts{key: "reused-key-0001"}); rec.Code != http.StatusAccepted {
		t.Fatalf("first submit = %d, want 202: %s", rec.Code, rec.Body)
	}

	other := strings.Replace(idemSpec, "image: busybox", "image: nginx", 1)
	rec := f.submit(submitOpts{key: "reused-key-0001", spec: other})
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body)
	}
	if got := dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls = %d, want 1 — the conflicting request reached a provider", got)
	}
	if got := len(f.store.BurstsForCustomer(f.cust.ID)); got != 1 {
		t.Fatalf("bursts = %d, want 1", got)
	}
}

// The key binds to the CANONICAL request — the spec as central re-marshals it
// with the namespace already pinned — not to the bytes that arrived. So a
// submission that names the namespace it would have been defaulted into is the
// same request, and replays. That is deliberate: it is the same document, the
// same Job, in the same namespace, so provisioning a second node for it would
// be the duplicate this slice exists to stop.
func TestCreate_NamespaceEquivalentSpecsReplay(t *testing.T) {
	dec := &fakeDecider{}
	f := newIdemFixture(t, dec, "team-a", "team-b")

	// Unqualified: pins to the tenant's first authorized namespace, team-a.
	first := f.submit(submitOpts{key: "namespace-equiv-1"})
	if first.Code != http.StatusAccepted {
		t.Fatalf("unqualified submit = %d, want 202: %s", first.Code, first.Body)
	}

	qualified := strings.Replace(idemSpec, "  name: idem-test", "  name: idem-test\n  namespace: team-a", 1)
	second := f.submit(submitOpts{key: "namespace-equiv-1", spec: qualified})
	if second.Code != http.StatusAccepted {
		t.Fatalf("explicitly-team-a submit = %d, want the replayed 202: %s", second.Code, second.Body)
	}
	if !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) {
		t.Fatalf("namespace-equivalent submit was not replayed:\n%q\n%q", second.Body, first.Body)
	}
	if got := dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls = %d, want 1", got)
	}

	// A DIFFERENT authorized namespace is a different canonical request, and is
	// refused rather than replayed — it would run the Job somewhere else.
	elsewhere := strings.Replace(idemSpec, "  name: idem-test", "  name: idem-test\n  namespace: team-b", 1)
	if rec := f.submit(submitOpts{key: "namespace-equiv-1", spec: elsewhere}); rec.Code != http.StatusConflict {
		t.Fatalf("other-namespace submit = %d, want 409: %s", rec.Code, rec.Body)
	}
	if got := dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls = %d, want 1", got)
	}
}

// A Plan that failed may have failed AFTER a backend handed it a machine. The
// refusal is therefore this key's terminal answer: the retry replays it, and
// does not re-enter Plan on the guess that nothing was created.
func TestCreate_TerminalFailureReplaysWithoutAnotherPlan(t *testing.T) {
	dec := &failingDecider{}
	f := newIdemFixture(t, dec)

	first := f.submit(submitOpts{key: "doomed-key-00001"})
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("first submit = %d, want 503: %s", first.Code, first.Body)
	}
	if !strings.Contains(first.Body.String(), `"status":"failed"`) {
		t.Fatalf("body = %s, want a failed answer", first.Body)
	}

	second := f.submit(submitOpts{key: "doomed-key-00001"})
	if second.Code != first.Code || !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) {
		t.Fatalf("replay = %d %q, want %d %q", second.Code, second.Body, first.Code, first.Body)
	}
	if got := dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls = %d, want 1 — an ambiguous failure was retried into a second node", got)
	}
}

// A claim whose owner is gone — the process died mid-submission — must never be
// taken over, however stale. The retry is told to wait and handed the workload
// id to poll; nothing re-enters Plan.
func TestCreate_AbandonedClaimRefusesToRerunPlan(t *testing.T) {
	dec := &fakeDecider{}
	f := newIdemFixture(t, dec)

	// The claim the dead process would have left behind: same scope, same
	// canonical request, still in progress.
	by := submitter{Actor: state.HumanActor("acct_idem", f.cust.ID), Role: state.RoleOwner}
	var wl workload.Workload
	if err := yamlUnmarshalForTest(t, idemSpec, &wl); err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	wl.Metadata.Namespace = "default"
	specYAML := marshalSpecForTest(t, &wl)
	orphan := &state.IdempotencyClaim{
		ID:          idempotencyScopeID(f.cust.ID, by, "", "abandoned-key-001"),
		CustomerID:  f.cust.ID,
		Actor:       by.Actor,
		KeyDigest:   idempotencyKeyDigest("abandoned-key-001"),
		RequestHash: canonicalRequestHash(specYAML, "", nil),
		WorkloadID:  "wl_orphaned",
	}
	if _, won, err := f.store.ClaimIdempotent(context.Background(), orphan); err != nil || !won {
		t.Fatalf("seeding the orphaned claim = won %v, err %v", won, err)
	}

	rec := f.submit(submitOpts{key: "abandoned-key-001"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Error("no Retry-After on the in-progress answer")
	}
	if !strings.Contains(rec.Body.String(), `"id":"wl_orphaned"`) {
		t.Fatalf("body = %s, want the stable workload id of the claim being waited on", rec.Body)
	}
	if got := dec.planCalls(); got != 0 {
		t.Fatalf("Plan calls = %d, want 0 — an ambiguous claim was taken over", got)
	}
}

func TestCanonicalRequestHashSeparatesCreateFromRetry(t *testing.T) {
	spec := []byte("kind: Workload\n")
	if canonicalRequestHash(spec, "", nil) == canonicalRequestHash(spec, "wl_source", nil) {
		t.Fatal("plain create and retry of the same spec share an idempotency request hash")
	}
}

// The template reference is part of what a key is bound to, so the same spec
// launched under two different references cannot replay one as the other — the
// answer would carry a provenance stamp the second submission never asked for.
// A submission that names no template hashes exactly as it always has, so no
// claim already in flight changes meaning when this ships.
func TestCanonicalRequestHashSeparatesTemplateReferences(t *testing.T) {
	spec := []byte("kind: Workload\n")
	plain := canonicalRequestHash(spec, "", nil)
	v1 := canonicalRequestHash(spec, "", &state.TemplateRef{ID: "container-job", Version: 1, CatalogRevision: "rev_a"})
	v2 := canonicalRequestHash(spec, "", &state.TemplateRef{ID: "container-job", Version: 2, CatalogRevision: "rev_a"})
	other := canonicalRequestHash(spec, "", &state.TemplateRef{ID: "node-capacity", Version: 1, CatalogRevision: "rev_a"})
	moved := canonicalRequestHash(spec, "", &state.TemplateRef{ID: "container-job", Version: 1, CatalogRevision: "rev_b"})

	sum := sha256.Sum256(spec)
	if plain != hex.EncodeToString(sum[:]) {
		t.Fatal("an untemplated submission no longer hashes as the bare canonical spec")
	}
	for name, got := range map[string]string{"v1": v1, "v2": v2, "other id": other, "moved catalog": moved} {
		if got == plain {
			t.Errorf("%s: templated submission shares the untemplated hash", name)
		}
	}
	if v1 == v2 || v1 == other || v1 == moved {
		t.Fatalf("distinct references share a request hash: v1=%s v2=%s other=%s moved=%s", v1, v2, other, moved)
	}
}

// A refusal that provisions nothing must not spend the key. The tenant's client
// keeps sending the one it picked, and it has to be allowed to succeed once the
// condition clears.
func TestCreate_PreProviderRefusalsFreeTheKey(t *testing.T) {
	t.Run("no connector", func(t *testing.T) {
		dec := &fakeDecider{}
		store := state.New()
		cust := &state.Customer{ID: "cust_noagent", Token: "tok_noagent"}
		store.AddCustomer(cust)
		f := &idemFixture{store: store, cust: cust, h: &Workloads{
			Store: store, Decider: dec, Reaper: &fakeReaper{}, Log: quietLog(),
		}}

		if rec := f.submit(submitOpts{key: "no-connector-key1"}); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503: %s", rec.Code, rec.Body)
		}
		// Connector comes up; the SAME key must now be able to win.
		store.AddAgent(&state.Agent{
			ID: "agent_late", CustomerID: cust.ID, ClusterID: "cluster_late",
			Send: make(chan protocol.Envelope, 256),
		})
		rec := f.submit(submitOpts{key: "no-connector-key1"})
		if rec.Code != http.StatusAccepted {
			t.Fatalf("retry after the connector connected = %d, want 202: %s", rec.Code, rec.Body)
		}
		if got := dec.planCalls(); got != 1 {
			t.Fatalf("Plan calls = %d, want 1", got)
		}
	})

	t.Run("tenant at its burst ceiling", func(t *testing.T) {
		dec := &fakeDecider{}
		f := newIdemFixture(t, dec)
		f.cust.MaxConcurrentBursts = 1

		if rec := f.submit(submitOpts{key: "ceiling-key-00001"}); rec.Code != http.StatusAccepted {
			t.Fatalf("first submit = %d, want 202: %s", rec.Code, rec.Body)
		}
		second := f.submit(submitOpts{key: "ceiling-key-00002"})
		if second.Code != http.StatusTooManyRequests {
			t.Fatalf("at-ceiling submit = %d, want 429: %s", second.Code, second.Body)
		}
		// Capacity frees up; the refused key is not spent.
		for _, b := range f.store.BurstsForCustomer(f.cust.ID) {
			f.store.DeleteBurst(b.ID)
		}
		if rec := f.submit(submitOpts{key: "ceiling-key-00002"}); rec.Code != http.StatusAccepted {
			t.Fatalf("retry after capacity freed = %d, want 202: %s", rec.Code, rec.Body)
		}
		if got := dec.planCalls(); got != 2 {
			t.Fatalf("Plan calls = %d, want 2 (one per distinct submission)", got)
		}
	})
}

// A key is scoped to the principal that used it, so one tenant's client cannot
// replay — or block — another's by picking the same string. A scoped connector
// credential's cluster binding is part of that principal: every connector on a
// multi-cluster tenant submits as the same cluster actor, so the binding is the
// only thing separating siblings.
func TestIdempotencyScopeSeparatesPrincipals(t *testing.T) {
	const key = "shared-key-00001"
	human := submitter{Actor: state.HumanActor("acct_a", "cust_1"), Role: state.RoleOwner}
	other := submitter{Actor: state.HumanActor("acct_b", "cust_1"), Role: state.RoleOwner}
	cluster := submitter{Actor: state.ClusterActor("cust_1")}

	ids := map[string]string{
		"tenant 1 / human a":        idempotencyScopeID("cust_1", human, "", key),
		"tenant 2 / human a":        idempotencyScopeID("cust_2", human, "", key),
		"tenant 1 / human b":        idempotencyScopeID("cust_1", other, "", key),
		"tenant 1 / legacy token":   idempotencyScopeID("cust_1", cluster, "", key),
		"tenant 1 / bound east":     idempotencyScopeID("cust_1", cluster, "cl-east", key),
		"tenant 1 / bound west":     idempotencyScopeID("cust_1", cluster, "cl-west", key),
		"tenant 2 / bound east":     idempotencyScopeID("cust_2", cluster, "cl-east", key),
		"tenant 1 / east other key": idempotencyScopeID("cust_1", cluster, "cl-east", "shared-key-00002"),
		"tenant 1 / other key":      idempotencyScopeID("cust_1", human, "", "shared-key-00002"),
	}
	seen := make(map[string]string, len(ids))
	for name, id := range ids {
		if prev, dup := seen[id]; dup {
			t.Errorf("%s and %s share a claim id", prev, name)
		}
		seen[id] = name
	}
	// Same inputs, same id — that is what makes a retry find its own claim, and
	// a credential's binding is fixed for as long as it authenticates.
	if idempotencyScopeID("cust_1", human, "", key) != ids["tenant 1 / human a"] {
		t.Error("the scope id is not stable across calls")
	}
	if idempotencyScopeID("cust_1", cluster, "cl-east", key) != ids["tenant 1 / bound east"] {
		t.Error("the bound scope id is not stable across calls")
	}
	// Length-prefixed fields: no regrouping of the same characters collides.
	if idempotencyScopeID("cust_1a", human, "", key) == idempotencyScopeID("cust_1", human, "", "a"+key) {
		t.Error("scope fields are not unambiguously framed")
	}
	if idempotencyScopeID("cust_1", cluster, "cl-east", key) == idempotencyScopeID("cust_1", cluster, "cl", "-east"+key) {
		t.Error("the cluster binding is not unambiguously framed against the key")
	}
}

// The unbound scopes are byte-for-byte what they were before the binding
// joined the digest. A human's and a legacy tenant token's claims are the ones
// that may be in flight when this ships, and a scope that shifted under them
// would re-run Plan on a key whose first attempt is still provisioning.
func TestIdempotencyScopeIsUnchangedWithoutABinding(t *testing.T) {
	const key = "legacy-key-00001"
	for name, by := range map[string]submitter{
		"human":        {Actor: state.HumanActor("acct_a", "cust_1"), Role: state.RoleOwner},
		"legacy token": {Actor: state.ClusterActor("cust_1")},
	} {
		h := sha256.New()
		hashField(h, "cust_1")
		hashField(h, by.Actor.Kind)
		hashField(h, by.Actor.AccountID)
		hashField(h, by.Actor.CustomerID)
		hashField(h, key)
		want := "idem_" + hex.EncodeToString(h.Sum(nil))
		if got := idempotencyScopeID("cust_1", by, "", key); got != want {
			t.Errorf("%s scope id = %s, want the pre-binding digest %s", name, got, want)
		}
	}
}

// The answer stays bounded even when a scheduling error carries a pathological
// upstream message. These are also the exact bytes the first caller receives,
// so a later replay cannot differ from the original response.
func TestRenderCreateResponseBoundsTheStoredBody(t *testing.T) {
	huge := CreateWorkloadResponse{
		ID:      "wl_1",
		Status:  "failed",
		Message: strings.Repeat("upstream said no. ", state.MaxIdempotencyResponseBytes),
	}
	body, err := renderCreateResponse(huge)
	if err != nil {
		t.Fatalf("renderCreateResponse: %v", err)
	}
	if len(body) > state.MaxIdempotencyResponseBytes {
		t.Fatalf("stored body = %d bytes, over the %d-byte bound", len(body), state.MaxIdempotencyResponseBytes)
	}

	// An ordinary answer is stored verbatim — byte-identical to what writeJSON
	// writes, which is what makes a replay a replay.
	ordinary := CreateWorkloadResponse{ID: "wl_1", Status: "provisioning", Backend: "linode"}
	body, err = renderCreateResponse(ordinary)
	if err != nil {
		t.Fatalf("renderCreateResponse: %v", err)
	}
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusAccepted, ordinary)
	if !bytes.Equal(body, rec.Body.Bytes()) {
		t.Fatalf("stored %q, but writeJSON writes %q", body, rec.Body)
	}
}

func TestCreate_BoundedFailureIsIdenticalOnFirstAnswerAndReplay(t *testing.T) {
	dec := &failingDecider{err: errors.New(strings.Repeat("provider detail ", state.MaxIdempotencyResponseBytes))}
	f := newIdemFixture(t, dec)

	first := f.submit(submitOpts{key: "repeat-repeat-repeat"})
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("first submit = %d, want 503: %s", first.Code, first.Body)
	}
	if first.Body.Len() > state.MaxIdempotencyResponseBytes {
		t.Fatalf("first answer = %d bytes, over the %d-byte replay bound", first.Body.Len(), state.MaxIdempotencyResponseBytes)
	}

	replay := f.submit(submitOpts{key: "repeat-repeat-repeat"})
	if replay.Code != first.Code || !bytes.Equal(replay.Body.Bytes(), first.Body.Bytes()) {
		t.Fatalf("replay = %d %q, want first answer %d %q", replay.Code, replay.Body, first.Code, first.Body)
	}
	if got := dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls = %d, want 1", got)
	}
}

func yamlUnmarshalForTest(t *testing.T, in string, out *workload.Workload) error {
	t.Helper()
	return yaml.Unmarshal([]byte(in), out)
}

func marshalSpecForTest(t *testing.T, wl *workload.Workload) []byte {
	t.Helper()
	b, err := yaml.Marshal(wl)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	return b
}
