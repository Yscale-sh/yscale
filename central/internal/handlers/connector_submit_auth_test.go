package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// connectorSubmitFixture is one tenant running TWO connectors — the situation the
// binding exists for, because it is the only one where "which cluster" has a
// wrong answer — plus a neighbouring tenant's connector that must stay
// unreachable. The credentials are real: minted by the same store call the
// tenant cluster registration route makes, never hand-written.
type connectorSubmitFixture struct {
	store                *state.Store
	mux                  *http.ServeMux
	dec                  *fakeDecider
	reaper               *fakeReaper
	ownerID              string
	eastToken, westToken string
	east, west, foreign  *state.Agent
}

const submitSpec = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: connector-submit
spec:
  image: busybox
  size: small`

// otherSubmitSpec is a DIFFERENT request under whatever key carries it — what
// separates a replay from a conflict.
const otherSubmitSpec = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: connector-submit-other
spec:
  image: busybox
  size: small`

func submitWorld(t *testing.T) *connectorSubmitFixture {
	t.Helper()
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_ms", Token: "tok_ms", Plan: "pro"})
	s.AddCustomer(&state.Customer{ID: "cust_other", Token: "tok_other", Plan: "pro"})

	owner, err := s.UpsertAccount("https://id.test", "ms-owner", state.AccountProfile{Email: "o@acme.com", EmailVerified: true})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s.AddTenantMembership(owner.ID, "cust_ms", state.RoleOwner); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}
	register := func(clusterID, name string) string {
		t.Helper()
		_, token, _, err := s.RegisterTenantCluster("cust_ms", owner.ID, clusterID, name, state.HumanActor(owner.ID, "cust_ms"))
		if err != nil {
			t.Fatalf("RegisterTenantCluster %s: %v", clusterID, err)
		}
		return token
	}
	eastToken := register("cl-east", "East")
	westToken := register("cl-west", "West")

	dec, reaper := &fakeDecider{}, &fakeReaper{}
	wls := &Workloads{Store: s, Decider: dec, Reaper: reaper, Log: quietLog()}
	// main.go's workload wiring: submit and cancel are connector-aware, reading
	// a workload back is not.
	mux := http.NewServeMux()
	mux.Handle("POST /v1/workloads", ConnectorSubmitAuth(s, http.HandlerFunc(wls.Create)))
	mux.Handle("GET /v1/workloads/{id}", Auth(s, http.HandlerFunc(wls.Get)))
	mux.Handle("DELETE /v1/workloads/{id}", ConnectorWorkloadAuth(s, http.HandlerFunc(wls.Cancel)))

	return &connectorSubmitFixture{
		store: s, mux: mux, dec: dec, reaper: reaper, ownerID: owner.ID,
		eastToken: eastToken, westToken: westToken,
		east:    connectConnector(s, "agent_east", "cust_ms", "cl-east"),
		west:    connectConnector(s, "agent_west", "cust_ms", "cl-west"),
		foreign: connectConnector(s, "agent_foreign", "cust_other", "cl-foreign"),
	}
}

// connectConnector registers one connector socket, which is what makes a
// registered cluster a dispatch target.
func connectConnector(s *state.Store, id, customerID, clusterID string) *state.Agent {
	a := &state.Agent{
		ID: id, CustomerID: customerID, ClusterID: clusterID,
		ConnectedAt: time.Now().UTC(),
		Send:        make(chan protocol.Envelope, 8),
	}
	s.AddAgent(a)
	return a
}

// submit drives a create through the real middleware. clusterHeader is what the
// CALLER sends, so "" is the headerless connector submit the binding answers.
func (fx *connectorSubmitFixture) submit(t *testing.T, token, clusterHeader string) *httptest.ResponseRecorder {
	t.Helper()
	return fx.submitKeyed(t, token, clusterHeader, "", submitSpec)
}

// submitKeyed is submit carrying an Idempotency-Key, which is what turns a
// submission into a durable claim. spec is explicit because the key binds to
// ONE request, and telling a replay from a conflict needs both.
func (fx *connectorSubmitFixture) submitKeyed(t *testing.T, token, clusterHeader, key, spec string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(spec))
	req.Header.Set("Authorization", "Bearer "+token)
	if clusterHeader != "" {
		req.Header.Set(clusterIDHeader, clusterHeader)
	}
	if key != "" {
		req.Header.Set(idempotencyHeader, key)
	}
	rec := httptest.NewRecorder()
	fx.mux.ServeHTTP(rec, req)
	return rec
}

func (fx *connectorSubmitFixture) cancel(t *testing.T, token, workloadID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, "/v1/workloads/"+workloadID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	fx.mux.ServeHTTP(rec, req)
	return rec
}

// dispatched drains one connector's queue and reports whether a submission
// reached it. The whole queue, because one submission is an announce plus a
// CreateJob: what this asserts is WHICH connector was dispatched to, and a
// leftover envelope would otherwise be read as the next submission's.
func dispatched(a *state.Agent) bool {
	seen := false
	for {
		select {
		case <-a.Send:
			seen = true
		default:
			return seen
		}
	}
}

func createdWorkloadID(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var res CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode create response: %v (body %q)", err, rec.Body.String())
	}
	if res.ID == "" {
		t.Fatalf("create response carries no workload id: %s", rec.Body)
	}
	return res.ID
}

// The connector submits its own cluster's work WITHOUT naming a cluster: the
// credential's binding is the routing answer. On a two-connector tenant this is
// the case that used to be unanswerable — the same headerless submit under the
// tenant token is a 409 asking for X-Cluster-ID (asserted below).
func TestConnectorSubmitAuthRoutesHeaderlessSubmitToItsBoundCluster(t *testing.T) {
	fx := submitWorld(t)

	rec := fx.submit(t, fx.eastToken, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("headerless scoped submit = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !dispatched(fx.east) {
		t.Fatal("the bound cluster's connector received no announce")
	}
	if dispatched(fx.west) {
		t.Fatal("ISOLATION BREACH: the submission was dispatched to a sibling cluster")
	}
	if dispatched(fx.foreign) {
		t.Fatal("ISOLATION BREACH: the submission reached another tenant's connector")
	}

	bursts := fx.store.BurstsForCustomer("cust_ms")
	if len(bursts) != 1 || bursts[0].ClusterID != "cl-east" || bursts[0].AgentID != fx.east.ID {
		t.Fatalf("burst = %+v, want one booked on cl-east/%s", bursts, fx.east.ID)
	}
	wl, err := fx.store.GetWorkload(createdWorkloadID(t, rec))
	if err != nil || wl.ClusterID != "cl-east" {
		t.Fatalf("workload cluster = %+v err=%v, want cl-east", wl, err)
	}

	// The other credential is bound to the other cluster, and answers the same
	// way — the binding is per-credential, not a single-connector fallback.
	if rec := fx.submit(t, fx.westToken, ""); rec.Code != http.StatusAccepted {
		t.Fatalf("headerless submit on the west credential = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !dispatched(fx.west) {
		t.Fatal("the west credential's submission did not reach the west connector")
	}
	if dispatched(fx.east) {
		t.Fatal("ISOLATION BREACH: the west credential's submission reached east")
	}
}

// A scoped credential naming a SIBLING cluster is refused outright, and refused
// early: no provisioning decision is made, no announce is queued, and no
// workload or burst is recorded. The header is caller-supplied, so this is the
// check that stops a compromised connector aiming paid work at the tenant's
// other cluster.
func TestConnectorSubmitAuthRefusesASiblingClusterHeader(t *testing.T) {
	fx := submitWorld(t)

	for _, header := range []string{"cl-west", "cl-foreign", "cl-nope"} {
		rec := fx.submit(t, fx.eastToken, header)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("scoped submit naming %q = %d, want 403: %s", header, rec.Code, rec.Body)
		}
	}
	if fx.dec.planCalls() != 0 {
		t.Fatalf("Plan calls = %d, want 0 — a refused header reached the decider", fx.dec.planCalls())
	}
	if dispatched(fx.east) || dispatched(fx.west) || dispatched(fx.foreign) {
		t.Fatal("a refused submission was dispatched to a connector")
	}
	if bursts := fx.store.BurstsForCustomer("cust_ms"); len(bursts) != 0 {
		t.Fatalf("bursts = %+v, want none", bursts)
	}

	// Its OWN cluster in the header is the same request, spelled out, and is
	// accepted — the refusal above is the mismatch, not the header.
	if rec := fx.submit(t, fx.eastToken, "cl-east"); rec.Code != http.StatusAccepted {
		t.Fatalf("scoped submit naming its own cluster = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !dispatched(fx.east) {
		t.Fatal("the bound cluster's connector received no announce")
	}
}

// Tenant cluster policy decides where a TENANT's unpinned work lands. It must
// never move a scoped credential's submission: this connector is submitting the
// work of the cluster it runs in, and an ordered policy that prefers the
// sibling would otherwise bill that cluster for a pod this one is holding.
func TestConnectorSubmitAuthIsNotReroutedByTenantPolicy(t *testing.T) {
	fx := submitWorld(t)
	if _, _, _, err := fx.store.SetTenantClusterPolicy("cust_ms", fx.ownerID, state.ClusterPolicy{
		Auto:  state.ClusterPolicyAutoOrdered,
		Allow: []string{"cl-west", "cl-east"},
	}, state.HumanActor(fx.ownerID, "cust_ms")); err != nil {
		t.Fatalf("SetTenantClusterPolicy: %v", err)
	}

	// The tenant token, headerless, is what the policy is for: automode picks
	// the first allowed cluster, west.
	if rec := fx.submit(t, "tok_ms", ""); rec.Code != http.StatusAccepted {
		t.Fatalf("legacy automode submit = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !dispatched(fx.west) {
		t.Fatal("automode did not place the tenant's submission on the first allowed cluster")
	}

	// The east credential, headerless, under the same policy: still east.
	if rec := fx.submit(t, fx.eastToken, ""); rec.Code != http.StatusAccepted {
		t.Fatalf("scoped submit under an ordered policy = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !dispatched(fx.east) {
		t.Fatal("the bound cluster's connector received no announce")
	}
	if dispatched(fx.west) {
		t.Fatal("ISOLATION BREACH: tenant policy routed a bound credential to a sibling cluster")
	}
}

// The tenant token's submit is untouched: still routed by header and tenant
// state, still ambiguous with two connectors and no header, still refused
// another tenant's cluster.
func TestConnectorSubmitAuthKeepsLegacyTenantSubmitUnchanged(t *testing.T) {
	fx := submitWorld(t)

	rec := fx.submit(t, "tok_ms", "")
	if rec.Code != http.StatusConflict {
		t.Fatalf("headerless tenant-token submit with two connectors = %d, want 409: %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), clusterIDHeader) {
		t.Fatalf("the refusal does not name the header that fixes it: %s", rec.Body)
	}
	if fx.dec.planCalls() != 0 {
		t.Fatalf("Plan calls = %d, want 0", fx.dec.planCalls())
	}

	if rec := fx.submit(t, "tok_ms", "cl-west"); rec.Code != http.StatusAccepted {
		t.Fatalf("tenant-token submit naming a cluster = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !dispatched(fx.west) {
		t.Fatal("the named cluster's connector received no announce")
	}
	if dispatched(fx.east) {
		t.Fatal("the tenant's other connector was dispatched to")
	}

	if rec := fx.submit(t, "tok_ms", "cl-foreign"); rec.Code != http.StatusForbidden {
		t.Fatalf("tenant-token submit naming another tenant's cluster = %d, want 403: %s", rec.Code, rec.Body)
	}
	if dispatched(fx.foreign) {
		t.Fatal("ISOLATION BREACH: a submission reached another tenant's connector")
	}

	// Nothing about the tenant token is a connector credential: it is bound to
	// no cluster, so a workload it created stays cancellable by it.
	created := fx.submit(t, "tok_ms", "cl-east")
	if created.Code != http.StatusAccepted {
		t.Fatalf("tenant-token submit = %d, want 202: %s", created.Code, created.Body)
	}
	id := createdWorkloadID(t, created)
	if rec := fx.cancel(t, "tok_ms", id); rec.Code != http.StatusOK {
		t.Fatalf("tenant-token cancel = %d, want 200: %s", rec.Code, rec.Body)
	}
	wl, err := fx.store.GetWorkload(id)
	if err != nil || wl.Status != "cancelled" {
		t.Fatalf("workload after tenant-token cancel = %+v err=%v, want cancelled", wl, err)
	}
}

// Submit and cancel close over the same binding: the connector cancels what it
// submitted, and the sibling credential cannot — the workload it did not run is
// the same 404 as one that never existed.
func TestConnectorSubmitAuthAndCancelShareTheBinding(t *testing.T) {
	fx := submitWorld(t)

	rec := fx.submit(t, fx.eastToken, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("scoped submit = %d, want 202: %s", rec.Code, rec.Body)
	}
	id := createdWorkloadID(t, rec)
	submitted, err := fx.store.GetWorkload(id)
	if err != nil {
		t.Fatalf("GetWorkload %s: %v", id, err)
	}
	burstID := submitted.BurstID

	if rec := fx.cancel(t, fx.westToken, id); rec.Code != http.StatusNotFound {
		t.Fatalf("sibling credential cancelling east's workload = %d, want 404: %s", rec.Code, rec.Body)
	}
	if wl, _ := fx.store.GetWorkload(id); wl.Status == "cancelled" {
		t.Fatal("ISOLATION BREACH: a sibling cluster's credential cancelled this workload")
	}
	if rec := fx.cancel(t, "tok_other", id); rec.Code != http.StatusNotFound {
		t.Fatalf("another tenant's token cancelling this workload = %d, want 404: %s", rec.Code, rec.Body)
	}
	if fx.reaper.reaped(burstID) {
		t.Fatal("a refused cancel tore down the burst anyway")
	}

	if rec := fx.cancel(t, fx.eastToken, id); rec.Code != http.StatusOK {
		t.Fatalf("bound credential cancelling its own workload = %d, want 200: %s", rec.Code, rec.Body)
	}
	wl, err := fx.store.GetWorkload(id)
	if err != nil || wl.Status != "cancelled" {
		t.Fatalf("workload after scoped cancel = %+v err=%v, want cancelled", wl, err)
	}
	if !fx.reaper.reaped(burstID) {
		t.Fatalf("scoped cancel did not reap burst %q", burstID)
	}
}

// The isolation the claim scope exists for. Two connectors pick their
// Idempotency-Keys alone, knowing nothing of each other, so on a multi-cluster
// tenant they WILL collide — every connector submits as the same cluster actor,
// and before the binding joined the scope that collision replayed east's answer
// at west, or refused it 409 for a key west had never used.
func TestConnectorSubmitIdempotencyIsScopedToTheBoundCluster(t *testing.T) {
	fx := submitWorld(t)
	const key = "connector-retry-01"

	east := fx.submitKeyed(t, fx.eastToken, "", key, submitSpec)
	if east.Code != http.StatusAccepted {
		t.Fatalf("east keyed submit = %d, want 202: %s", east.Code, east.Body)
	}
	dispatched(fx.east)

	// Same key, same spec, sibling credential: its own submission, not east's
	// answer handed back.
	west := fx.submitKeyed(t, fx.westToken, "", key, submitSpec)
	if west.Code != http.StatusAccepted {
		t.Fatalf("sibling submit under the same key = %d, want 202: %s", west.Code, west.Body)
	}
	if west.Header().Get("Idempotency-Replayed") != "" {
		t.Fatal("ISOLATION BREACH: the sibling was handed east's stored answer as a replay")
	}
	eastID, westID := createdWorkloadID(t, east), createdWorkloadID(t, west)
	if eastID == westID {
		t.Fatalf("ISOLATION BREACH: both clusters were given the same workload id %s", eastID)
	}
	if !dispatched(fx.west) {
		t.Fatal("the sibling's submission never reached its own connector")
	}
	if dispatched(fx.east) {
		t.Fatal("ISOLATION BREACH: the sibling's submission was dispatched to east")
	}
	if fx.dec.planCalls() != 2 {
		t.Fatalf("Plan calls = %d, want 2 — each cluster's own submission", fx.dec.planCalls())
	}
	for id, want := range map[string]string{eastID: "cl-east", westID: "cl-west"} {
		wl, err := fx.store.GetWorkload(id)
		if err != nil || wl.ClusterID != want {
			t.Fatalf("workload %s = %+v err=%v, want cluster %s", id, wl, err, want)
		}
	}
}

// The other half of the collision: a DIFFERENT request under a key a sibling
// happens to hold. A 409 here would be the leak — west learning that somebody
// else on the tenant is using "connector-retry-01" for something west cannot
// see.
func TestConnectorSubmitIdempotencyConflictDoesNotCrossClusters(t *testing.T) {
	fx := submitWorld(t)
	const key = "connector-retry-02"

	if rec := fx.submitKeyed(t, fx.eastToken, "", key, submitSpec); rec.Code != http.StatusAccepted {
		t.Fatalf("east keyed submit = %d, want 202: %s", rec.Code, rec.Body)
	}

	west := fx.submitKeyed(t, fx.westToken, "", key, otherSubmitSpec)
	if west.Code != http.StatusAccepted {
		t.Fatalf("sibling submit of a different spec under the same key = %d, want 202: %s", west.Code, west.Body)
	}
	if !dispatched(fx.west) {
		t.Fatal("the sibling's submission never reached its own connector")
	}

	// Within ONE credential the conflict still stands: the same key really was
	// spent on a different request, and answering it with the first one's
	// response would run the wrong workload under the right id.
	conflict := fx.submitKeyed(t, fx.eastToken, "", key, otherSubmitSpec)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("same credential, same key, different spec = %d, want 409: %s", conflict.Code, conflict.Body)
	}
	if fx.dec.planCalls() != 2 {
		t.Fatalf("Plan calls = %d, want 2 — the conflict reached a provider", fx.dec.planCalls())
	}
}

// Narrowing the scope must not cost the connector its own replay: the retry
// that arrives because the connector gave up on a slow response still gets the
// first answer back rather than a second paid node.
func TestConnectorSubmitIdempotencyReplaysOnTheSameCredential(t *testing.T) {
	fx := submitWorld(t)
	const key = "connector-retry-03"

	first := fx.submitKeyed(t, fx.eastToken, "", key, submitSpec)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first submit = %d, want 202: %s", first.Code, first.Body)
	}
	dispatched(fx.east)

	replay := fx.submitKeyed(t, fx.eastToken, "", key, submitSpec)
	if replay.Code != first.Code {
		t.Fatalf("replay status = %d, want %d: %s", replay.Code, first.Code, replay.Body)
	}
	if replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Error("the retry was not marked as a replay")
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %q, want the original %q", replay.Body, first.Body)
	}
	if fx.dec.planCalls() != 1 {
		t.Fatalf("Plan calls = %d, want 1 — the retry provisioned a second node", fx.dec.planCalls())
	}
	if dispatched(fx.east) {
		t.Fatal("a replay was dispatched to the connector a second time")
	}
	if bursts := fx.store.BurstsForCustomer("cust_ms"); len(bursts) != 1 {
		t.Fatalf("bursts = %d, want 1", len(bursts))
	}

	// Rotation replaces the credential, not the binding, so the connector's
	// in-flight key still finds its own claim across a rotation.
	_, rotated, _, err := fx.store.RotateTenantClusterCredential("cust_ms", fx.ownerID, "cl-east", state.HumanActor(fx.ownerID, "cust_ms"))
	if err != nil {
		t.Fatalf("RotateTenantClusterCredential: %v", err)
	}
	fx.east = connectConnector(fx.store, "agent_east_rotated", "cust_ms", "cl-east")
	if rec := fx.submitKeyed(t, rotated, "", key, submitSpec); rec.Body.String() != first.Body.String() {
		t.Fatalf("replay after rotation = %d %q, want the original %q", rec.Code, rec.Body, first.Body)
	}
	if fx.dec.planCalls() != 1 {
		t.Fatalf("Plan calls after rotation = %d, want 1", fx.dec.planCalls())
	}
}

// The legacy tenant token is bound to no cluster, so nothing about its claim
// scope moves: its own retry replays its own answer, and a different request
// under the same key is still its 409. What it no longer does is share a scope
// with a scoped credential — which is the fix, not a change to this path.
func TestConnectorSubmitIdempotencyLeavesTheLegacyTokenUnchanged(t *testing.T) {
	fx := submitWorld(t)
	const key = "tenant-retry-0001"

	first := fx.submitKeyed(t, "tok_ms", "cl-east", key, submitSpec)
	if first.Code != http.StatusAccepted {
		t.Fatalf("tenant-token keyed submit = %d, want 202: %s", first.Code, first.Body)
	}
	dispatched(fx.east)

	replay := fx.submitKeyed(t, "tok_ms", "cl-east", key, submitSpec)
	if replay.Header().Get("Idempotency-Replayed") != "true" || replay.Body.String() != first.Body.String() {
		t.Fatalf("tenant-token replay = %d %q, want the original %q", replay.Code, replay.Body, first.Body)
	}
	if rec := fx.submitKeyed(t, "tok_ms", "cl-east", key, otherSubmitSpec); rec.Code != http.StatusConflict {
		t.Fatalf("tenant-token key reused for a different request = %d, want 409: %s", rec.Code, rec.Body)
	}

	// The header the tenant token routes by is caller-chosen, so it stays OUT of
	// the scope: the same key naming the other cluster is the same spent claim,
	// replayed, exactly as before.
	if rec := fx.submitKeyed(t, "tok_ms", "cl-west", key, submitSpec); rec.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("tenant-token replay under a different X-Cluster-ID = %d %q, want the original answer replayed", rec.Code, rec.Body)
	}
	if fx.dec.planCalls() != 1 {
		t.Fatalf("Plan calls = %d, want 1", fx.dec.planCalls())
	}
	if dispatched(fx.west) {
		t.Fatal("a replayed tenant-token submission was dispatched to a second cluster")
	}
}

// Rotation is the operator's revocation: the credential the console just handed
// back submits and cancels on the same binding, and the one it replaced
// authenticates nothing.
func TestConnectorSubmitAuthFollowsCredentialRotation(t *testing.T) {
	fx := submitWorld(t)

	_, rotated, _, err := fx.store.RotateTenantClusterCredential("cust_ms", fx.ownerID, "cl-east", state.HumanActor(fx.ownerID, "cust_ms"))
	if err != nil {
		t.Fatalf("RotateTenantClusterCredential: %v", err)
	}

	if rec := fx.submit(t, fx.eastToken, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("rotated-away credential = %d, want 401: %s", rec.Code, rec.Body)
	}
	if fx.dec.planCalls() != 0 {
		t.Fatalf("Plan calls = %d, want 0 — a dead credential reached the decider", fx.dec.planCalls())
	}

	// Rotation evicts the socket the old credential held, so the connector comes
	// back with the new one before it has anywhere to submit to.
	fx.east = connectConnector(fx.store, "agent_east_rotated", "cust_ms", "cl-east")

	rec := fx.submit(t, rotated, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("rotated credential submit = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !dispatched(fx.east) {
		t.Fatal("the rotated credential's submission did not reach its bound cluster")
	}
	if dispatched(fx.west) {
		t.Fatal("ISOLATION BREACH: the rotated credential reached a sibling cluster")
	}
	if cancelled := fx.cancel(t, rotated, createdWorkloadID(t, rec)); cancelled.Code != http.StatusOK {
		t.Fatalf("rotated credential cancel = %d, want 200: %s", cancelled.Code, cancelled.Body)
	}
}
