package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// twoClusterFixture is one tenant running two connectors — the situation every
// "which cluster" answer below exists for — plus a second tenant whose cluster
// the first must never be able to aim work at.
type twoClusterFixture struct {
	*idemFixture
	east, west, foreign *state.Agent
	dec                 *fakeDecider
}

func newTwoClusterFixture(t *testing.T) *twoClusterFixture {
	t.Helper()
	dec := &fakeDecider{}
	store := state.New()
	cust := &state.Customer{ID: "cust_multi", Token: "tok_multi"}
	store.AddCustomer(cust)
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "tok_other"})

	newAgent := func(id, customerID, clusterID string) *state.Agent {
		a := &state.Agent{
			ID: id, CustomerID: customerID, ClusterID: clusterID,
			ConnectedAt: time.Now().UTC(),
			Send:        make(chan protocol.Envelope, 8),
		}
		store.AddAgent(a)
		return a
	}
	return &twoClusterFixture{
		idemFixture: &idemFixture{store: store, cust: cust, h: &Workloads{
			Store: store, Decider: dec, Reaper: &fakeReaper{}, Log: quietLog(),
		}},
		east:    newAgent("agent_east", cust.ID, "cluster_east"),
		west:    newAgent("agent_west", cust.ID, "cluster_west"),
		foreign: newAgent("agent_foreign", "cust_other", "cluster_foreign"),
		dec:     dec,
	}
}

// drained reports whether an announce reached this agent's queue.
func announced(t *testing.T, a *state.Agent) bool {
	t.Helper()
	select {
	case env := <-a.Send:
		if env.Type != protocol.TypeBurstAnnounce {
			t.Fatalf("unexpected envelope on %s: %s", a.ID, env.Type)
		}
		return true
	default:
		return false
	}
}

// A named cluster routes exactly there: the commands go to that connector's
// queue and the burst records the cluster it was booked on, so the teardown can
// find its way back.
func TestCreate_ExplicitClusterRoutesThere(t *testing.T) {
	f := newTwoClusterFixture(t)

	rec := f.submit(submitOpts{key: "explicit-cluster-1", clusterID: "cluster_west"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !announced(t, f.west) {
		t.Fatal("the named cluster's connector received no announce")
	}
	if announced(t, f.east) {
		t.Fatal("the tenant's OTHER cluster was dispatched to")
	}
	bursts := f.store.BurstsForCustomer(f.cust.ID)
	if len(bursts) != 1 {
		t.Fatalf("bursts = %d, want 1", len(bursts))
	}
	if bursts[0].ClusterID != "cluster_west" {
		t.Fatalf("burst cluster = %q, want cluster_west", bursts[0].ClusterID)
	}
	if bursts[0].AgentID != f.west.ID {
		t.Fatalf("burst agent = %q, want %q", bursts[0].AgentID, f.west.ID)
	}
	workload, err := f.store.WorkloadByBurst(bursts[0].ID)
	if err != nil {
		t.Fatalf("workload by burst: %v", err)
	}
	if workload == nil || workload.ClusterID != "cluster_west" {
		t.Fatalf("workload cluster = %+v, want cluster_west", workload)
	}
}

// Naming another tenant's cluster is refused outright — the header is
// caller-supplied, and a tenant that could aim work at a cluster it does not
// own is the isolation break the check exists for.
func TestCreate_ForeignClusterIsForbidden(t *testing.T) {
	f := newTwoClusterFixture(t)

	rec := f.submit(submitOpts{key: "foreign-cluster-1", clusterID: "cluster_foreign"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %s", rec.Code, rec.Body)
	}
	if announced(t, f.foreign) {
		t.Fatal("a submission was dispatched to another tenant's connector")
	}
	if f.dec.planCalls() != 0 {
		t.Fatalf("Plan calls = %d, want 0 — a cross-tenant submit reached a provider", f.dec.planCalls())
	}
	// Nothing was provisioned, so the key is unspent: the tenant's client keeps
	// sending it, and a corrected submission under it must be able to win.
	if rec := f.submit(submitOpts{key: "foreign-cluster-1", clusterID: "cluster_east"}); rec.Code != http.StatusAccepted {
		t.Fatalf("corrected retry under the same key = %d, want 202: %s", rec.Code, rec.Body)
	}
}

// A cluster of this tenant's that is not connected is 409, not a fallback to
// the other one: the fix is that connector coming back, and running the job
// somewhere else is not the same request.
func TestCreate_NamedUnavailableClusterConflicts(t *testing.T) {
	f := newTwoClusterFixture(t)

	rec := f.submit(submitOpts{key: "absent-cluster-1", clusterID: "cluster_south"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body)
	}
	if announced(t, f.east) || announced(t, f.west) {
		t.Fatal("an unavailable named cluster fell back to a connected one")
	}
	if f.dec.planCalls() != 0 {
		t.Fatalf("Plan calls = %d, want 0", f.dec.planCalls())
	}
	// The key is unspent — the connector reconnecting is exactly the retry this
	// answer is asking for.
	f.store.AddAgent(&state.Agent{
		ID: "agent_south", CustomerID: f.cust.ID, ClusterID: "cluster_south",
		ConnectedAt: time.Now().UTC(), Send: make(chan protocol.Envelope, 8),
	})
	if rec := f.submit(submitOpts{key: "absent-cluster-1", clusterID: "cluster_south"}); rec.Code != http.StatusAccepted {
		t.Fatalf("retry after the connector connected = %d, want 202: %s", rec.Code, rec.Body)
	}
}

// With no header and two clusters there is no answer to give, and picking one
// bills the wrong cluster. The caller is asked for the header.
func TestCreate_NoHeaderWithTwoClustersConflicts(t *testing.T) {
	f := newTwoClusterFixture(t)

	rec := f.submit(submitOpts{key: "ambiguous-cluster1"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), clusterIDHeader) {
		t.Fatalf("the refusal does not name the header that fixes it: %s", rec.Body)
	}
	if announced(t, f.east) || announced(t, f.west) {
		t.Fatal("an ambiguous submission was dispatched anyway")
	}
	if f.dec.planCalls() != 0 {
		t.Fatalf("Plan calls = %d, want 0", f.dec.planCalls())
	}
	// Unspent: the same key with the header must win.
	if rec := f.submit(submitOpts{key: "ambiguous-cluster1", clusterID: "cluster_east"}); rec.Code != http.StatusAccepted {
		t.Fatalf("retry naming a cluster = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !announced(t, f.east) {
		t.Fatal("the named cluster received no announce")
	}
}

// The single-connector tenant — every deployed one today — keeps submitting
// with no header, and its burst still records where it landed.
func TestCreate_SingleClusterFallbackUnchanged(t *testing.T) {
	dec := &fakeDecider{}
	f := newIdemFixture(t, dec)

	rec := f.submit(submitOpts{key: "single-cluster-key"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	bursts := f.store.BurstsForCustomer(f.cust.ID)
	if len(bursts) != 1 || bursts[0].ClusterID != "cluster_idem" {
		t.Fatalf("burst cluster = %+v, want cluster_idem recorded", bursts)
	}
}

// A tenant with a second socket for the SAME cluster is not ambiguous: that is
// one connector that reconnected, and the submission goes to the live socket.
func TestCreate_DuplicateSocketsAreOneCluster(t *testing.T) {
	dec := &fakeDecider{}
	f := newIdemFixture(t, dec)
	fresh := &state.Agent{
		ID: "agent_idem2", CustomerID: f.cust.ID, ClusterID: "cluster_idem",
		ConnectedAt: time.Now().UTC().Add(time.Minute),
		Send:        make(chan protocol.Envelope, 8),
	}
	f.store.AddAgent(fresh)

	rec := f.submit(submitOpts{key: "reconnected-key-1"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !announced(t, fresh) {
		t.Fatal("the submission did not go to the newest socket")
	}
}

func TestCreate_ClusterPolicyOrderedAutoRoutesByAllowOrderAndRecordsPlacement(t *testing.T) {
	f := newTwoClusterFixture(t)
	f.cust.ClusterPolicy = &state.ClusterPolicy{
		Allow: []string{"cluster_west", "cluster_east"},
		Auto:  state.ClusterPolicyAutoOrdered,
	}

	rec := f.submit(submitOpts{key: "policy-ordered-1"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !announced(t, f.west) {
		t.Fatal("ordered automode did not dispatch to the first allowed cluster")
	}
	if announced(t, f.east) {
		t.Fatal("ordered automode dispatched to the wrong cluster")
	}
	var res CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Placement == nil || res.Placement.Mode != state.ClusterPlacementModeAuto || res.Placement.GrantedClusterID != "cluster_west" {
		t.Fatalf("response placement = %+v", res.Placement)
	}
	wl, err := f.store.WorkloadByBurst(res.BurstID)
	if err != nil {
		t.Fatalf("workload by burst: %v", err)
	}
	if wl.ClusterID != "cluster_west" || wl.Placement == nil || wl.Placement.GrantedClusterID != "cluster_west" || wl.Placement.Mode != state.ClusterPlacementModeAuto {
		t.Fatalf("stored placement = workload cluster %q placement %+v", wl.ClusterID, wl.Placement)
	}

	replay := f.submit(submitOpts{key: "policy-ordered-1"})
	if replay.Code != http.StatusAccepted {
		t.Fatalf("idempotent replay = %d, want 202: %s", replay.Code, replay.Body)
	}
	var replayed CreateWorkloadResponse
	if err := json.Unmarshal(replay.Body.Bytes(), &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.ID != res.ID || replayed.Placement == nil || replayed.Placement.GrantedClusterID != "cluster_west" {
		t.Fatalf("replayed response lost placement: first=%+v replay=%+v", res, replayed)
	}
	if f.dec.planCalls() != 1 {
		t.Fatalf("idempotent replay planned %d times, want 1", f.dec.planCalls())
	}
}

func TestCreate_ClusterPolicyRequirePinAndEligibilityMatrix(t *testing.T) {
	t.Run("require pin conflicts when multiple eligible clusters are connected", func(t *testing.T) {
		f := newTwoClusterFixture(t)
		f.cust.ClusterPolicy = &state.ClusterPolicy{Auto: state.ClusterPolicyAutoRequirePin}
		rec := f.submit(submitOpts{key: "policy-require-pin-1"})
		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body)
		}
		if f.dec.planCalls() != 0 {
			t.Fatalf("Plan calls = %d, want 0", f.dec.planCalls())
		}
	})

	t.Run("require pin auto-routes when exactly one connected cluster is eligible", func(t *testing.T) {
		f := newTwoClusterFixture(t)
		f.cust.ClusterPolicy = &state.ClusterPolicy{Deny: []string{"cluster_west"}}
		rec := f.submit(submitOpts{key: "policy-one-eligible-1"})
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
		}
		if !announced(t, f.east) || announced(t, f.west) {
			t.Fatal("submission did not route to the only eligible connected cluster")
		}
	})

	t.Run("zero connected eligible clusters conflicts but no connectors stays unavailable", func(t *testing.T) {
		f := newTwoClusterFixture(t)
		f.cust.ClusterPolicy = &state.ClusterPolicy{Deny: []string{"cluster_east", "cluster_west"}}
		rec := f.submit(submitOpts{key: "policy-zero-eligible-1"})
		if rec.Code != http.StatusConflict {
			t.Fatalf("zero eligible status = %d, want 409: %s", rec.Code, rec.Body)
		}

		dec := &fakeDecider{}
		store := state.New()
		cust := &state.Customer{ID: "cust_empty", Token: "tok_empty", ClusterPolicy: &state.ClusterPolicy{Auto: state.ClusterPolicyAutoOrdered}}
		store.AddCustomer(cust)
		h := &Workloads{Store: store, Decider: dec, Reaper: &fakeReaper{}, Log: quietLog()}
		rec = (&idemFixture{store: store, cust: cust, h: h}).submit(submitOpts{key: "policy-no-connectors-1"})
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("no connectors status = %d, want 503: %s", rec.Code, rec.Body)
		}
	})
}

func TestCreate_ClusterPolicyExplicitDenialDisconnectionAndReregistration(t *testing.T) {
	t.Run("explicit denied cluster is 403 and releases idempotency key", func(t *testing.T) {
		f := newTwoClusterFixture(t)
		f.cust.ClusterPolicy = &state.ClusterPolicy{Deny: []string{"cluster_west"}}
		rec := f.submit(submitOpts{key: "policy-denied-1", clusterID: "cluster_west"})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("denied status = %d, want 403: %s", rec.Code, rec.Body)
		}
		if announced(t, f.west) || f.dec.planCalls() != 0 {
			t.Fatal("denied cluster reached dispatch or planning")
		}
		f.cust.ClusterPolicy = &state.ClusterPolicy{Allow: []string{"cluster_east"}}
		if rec := f.submit(submitOpts{key: "policy-denied-1", clusterID: "cluster_east"}); rec.Code != http.StatusAccepted {
			t.Fatalf("retry with allowed cluster under same key = %d, want 202: %s", rec.Code, rec.Body)
		}
	})

	t.Run("explicit allowed but disconnected cluster is 409 until it reconnects", func(t *testing.T) {
		f := newTwoClusterFixture(t)
		f.cust.ClusterPolicy = &state.ClusterPolicy{Allow: []string{"cluster_south"}, Auto: state.ClusterPolicyAutoOrdered}
		rec := f.submit(submitOpts{key: "policy-disconnected-1", clusterID: "cluster_south"})
		if rec.Code != http.StatusConflict {
			t.Fatalf("disconnected allowed status = %d, want 409: %s", rec.Code, rec.Body)
		}
		if f.dec.planCalls() != 0 {
			t.Fatalf("Plan calls = %d, want 0", f.dec.planCalls())
		}
		south := &state.Agent{
			ID: "agent_south", CustomerID: f.cust.ID, ClusterID: "cluster_south",
			ConnectedAt: time.Now().UTC(), Send: make(chan protocol.Envelope, 8),
		}
		f.store.AddAgent(south)
		rec = f.submit(submitOpts{key: "policy-disconnected-1", clusterID: "cluster_south"})
		if rec.Code != http.StatusAccepted {
			t.Fatalf("retry after reconnect = %d, want 202: %s", rec.Code, rec.Body)
		}
		if !announced(t, south) {
			t.Fatal("re-registered allowed cluster did not receive dispatch")
		}
		var res CreateWorkloadResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatal(err)
		}
		if res.Placement == nil || res.Placement.Mode != state.ClusterPlacementModePinned ||
			res.Placement.RequestedClusterID != "cluster_south" || res.Placement.GrantedClusterID != "cluster_south" {
			t.Fatalf("pinned placement = %+v", res.Placement)
		}
	})
}

// drainBurstNode follows the burst's own cluster. Node names are unique only
// within a cluster, so a drain sent to the tenant's other cluster is at best a
// no-op and at worst a delete of an unrelated node.
func TestDrainBurstNode_RoutesByBurstCluster(t *testing.T) {
	f := newTwoClusterFixture(t)
	burst := &state.Burst{
		ID: "burst_w", CustomerID: f.cust.ID, ClusterID: "cluster_west",
		AgentID: "agent_gone", NodeName: "ys-burst-w",
	}

	done := make(chan error, 1)
	go func() {
		done <- drainBurstNode(context.Background(), f.store, nil, quietLog(), burst, "complete")
	}()

	var env protocol.Envelope
	select {
	case env = <-f.west.Send:
	case <-time.After(2 * time.Second):
		t.Fatal("the burst's own cluster received no drain")
	}
	if env.Type != protocol.TypeDrainNode {
		t.Fatalf("envelope = %s, want drain_node", env.Type)
	}
	if announced(t, f.east) {
		t.Fatal("the drain went to the tenant's other cluster")
	}
	if !f.west.DeliverCommandAck(protocol.CommandAck{CommandID: env.ID, Success: true}) {
		t.Fatal("drain acknowledgement had no waiter")
	}
	if err := <-done; err != nil {
		t.Fatalf("drain: %v", err)
	}
}

// A burst booked before the cluster was recorded still drains — through the
// single-connector fallback — and still refuses to guess when the tenant has
// two clusters connected.
func TestDrainBurstNode_LegacyBurstFallsBackAndRefusesAmbiguity(t *testing.T) {
	t.Run("one cluster falls back", func(t *testing.T) {
		store := state.New()
		agent := &state.Agent{
			ID: "agent_only", CustomerID: "cust_legacy", ClusterID: "cluster_only",
			ConnectedAt: time.Now().UTC(), Send: make(chan protocol.Envelope, 8),
		}
		store.AddAgent(agent)
		burst := &state.Burst{ID: "burst_legacy", CustomerID: "cust_legacy", NodeName: "ys-burst-legacy"}

		done := make(chan error, 1)
		go func() {
			done <- drainBurstNode(context.Background(), store, nil, quietLog(), burst, "complete")
		}()
		var env protocol.Envelope
		select {
		case env = <-agent.Send:
		case <-time.After(2 * time.Second):
			t.Fatal("a legacy burst got no drain from the tenant's only connector")
		}
		if !agent.DeliverCommandAck(protocol.CommandAck{CommandID: env.ID, Success: true}) {
			t.Fatal("drain acknowledgement had no waiter")
		}
		if err := <-done; err != nil {
			t.Fatalf("drain: %v", err)
		}
	})

	t.Run("two clusters refuse", func(t *testing.T) {
		f := newTwoClusterFixture(t)
		burst := &state.Burst{ID: "burst_legacy", CustomerID: f.cust.ID, NodeName: "ys-burst-legacy"}

		err := drainBurstNode(context.Background(), f.store, nil, quietLog(), burst, "complete")
		if err == nil {
			t.Fatal("a legacy burst on a two-cluster tenant must not pick a cluster")
		}
		if announced(t, f.east) || announced(t, f.west) {
			t.Fatal("a drain was enqueued despite the ambiguity")
		}
	})
}

// fakeMeshProvider is a coordination server that mints without talking to one.
// The hostname it is asked to sweep is recorded, because that hostname is
// derived from the cluster id the handler resolved — it is how these tests see
// WHICH cluster the mint decided it was for.
type fakeMeshProvider struct{ swept []string }

func (p *fakeMeshProvider) MintAuthKey(context.Context, []string, time.Duration) (string, error) {
	return "tskey-auth-fake", nil
}

func (p *fakeMeshProvider) MintAuthKeyEphemeral(context.Context, []string, time.Duration, bool) (string, error) {
	return "tskey-auth-fake", nil
}

func (p *fakeMeshProvider) FindDeviceByHostname(_ context.Context, hostname string) (string, error) {
	p.swept = append(p.swept, hostname)
	return "", nil
}

func (p *fakeMeshProvider) DeleteDevice(context.Context, string) error { return nil }
func (p *fakeMeshProvider) LoginServer() string                        { return "https://box.example" }

// mintFixture is a tenant with two connected clusters and a mint that will
// succeed once a cluster has been resolved.
func newMintFixture(t *testing.T) (*AgentAuth, *state.Customer, *fakeMeshProvider) {
	t.Helper()
	store := state.New()
	cust := &state.Customer{ID: "cust_mint", Token: "tok_mint", Mesh: &state.MeshEndpoint{
		Provider: "box", LoginServer: "https://box.example", APIKey: "k", User: "acme",
	}}
	store.AddCustomer(cust)
	for _, cl := range []string{"cluster_a", "cluster_b"} {
		store.AddAgent(&state.Agent{
			ID: "agent_" + cl, CustomerID: cust.ID, ClusterID: cl,
			ConnectedAt: time.Now().UTC(),
			Send:        make(chan protocol.Envelope, 1),
		})
	}
	store.AddAgent(&state.Agent{ID: "agent_x", CustomerID: "cust_other", ClusterID: "cluster_x"})
	prov := &fakeMeshProvider{}
	return &AgentAuth{
		Store:       store,
		Log:         quietLog(),
		BoxProvider: func(string, string, string) mesh.Provider { return prov },
	}, cust, prov
}

func callMint(h *AgentAuth, cust *state.Customer, clusterID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/agent/ts-auth-key", strings.NewReader("{}"))
	if clusterID != "" {
		req.Header.Set(clusterIDHeader, clusterID)
	}
	rec := httptest.NewRecorder()
	h.MintKey(rec, req.WithContext(context.WithValue(context.Background(), ctxCustomer, cust)))
	return rec
}

// The ts-auth-key mint's fallback tightens the same way: with two clusters
// connected and no header there is no hostname to sweep, and guessing one
// deletes a running cluster's tailnet device.
func TestMintKey_NoHeaderWithTwoClustersIsRefused(t *testing.T) {
	h, cust, prov := newMintFixture(t)

	rec := callMint(h, cust, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), clusterIDHeader) {
		t.Fatalf("the refusal does not name the header that fixes it: %s", rec.Body)
	}
	if len(prov.swept) != 0 {
		t.Fatalf("a device sweep ran for a guessed cluster: %v", prov.swept)
	}
}

// One cluster and no header is the deployed path, and it is unchanged.
func TestMintKey_NoHeaderWithOneClusterStillResolves(t *testing.T) {
	h, cust, prov := newMintFixture(t)
	h.Store.RemoveAgent("agent_cluster_b")

	if rec := callMint(h, cust, ""); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body)
	}
	if len(prov.swept) != 1 || prov.swept[0] != "yscale-agent-cluster_a" {
		t.Fatalf("swept = %v, want the tenant's only cluster", prov.swept)
	}
}

// Naming a cluster is unaffected by the ambiguity guard, which is about the
// FALLBACK only: an owned cluster mints for exactly that cluster, and another
// tenant's is refused as before.
func TestMintKey_ExplicitClusterOwnershipUnchanged(t *testing.T) {
	h, cust, prov := newMintFixture(t)

	if rec := callMint(h, cust, "cluster_b"); rec.Code != http.StatusOK {
		t.Fatalf("naming an owned cluster = %d, want 200: %s", rec.Code, rec.Body)
	}
	if len(prov.swept) != 1 || prov.swept[0] != "yscale-agent-cluster_b" {
		t.Fatalf("swept = %v, want the named cluster's hostname", prov.swept)
	}
	if rec := callMint(h, cust, "cluster_x"); rec.Code != http.StatusForbidden {
		t.Fatalf("naming another tenant's cluster = %d, want 403: %s", rec.Code, rec.Body)
	}
}

// A REGISTERED cluster that is offline is a different answer from an unknown
// one: still 409, but naming the real problem — the connector is down, and the
// fix is bringing it back, not doubting the id. An id another tenant holds
// durably is 403 with no socket live anywhere, exactly as it is with one.
func TestCreate_RegisteredOfflineClusterIsConflictNotUnknown(t *testing.T) {
	f := newTwoClusterFixture(t)
	now := time.Now().UTC()
	if _, _, err := f.store.ClaimAgentCluster("cust_multi", "cluster_offline", false, now); err != nil {
		t.Fatalf("claim cluster_offline: %v", err)
	}
	if _, _, err := f.store.ClaimAgentCluster("cust_other", "cluster_theirs", false, now); err != nil {
		t.Fatalf("claim cluster_theirs: %v", err)
	}

	// Our own registered-but-offline cluster: a clear conflict that says
	// "registered", never "unknown cluster".
	rec := f.submit(submitOpts{key: "registered-offline-1", clusterID: "cluster_offline"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("registered offline pin = %d, want 409: %s", rec.Code, rec.Body)
	}
	if body := rec.Body.String(); !strings.Contains(body, "registered") {
		t.Fatalf("refusal does not say the cluster is registered: %s", body)
	}
	if f.dec.planCalls() != 0 {
		t.Fatalf("Plan calls = %d, want 0 — an offline pin reached a provider", f.dec.planCalls())
	}

	// Another tenant's registered cluster, with NO connector live on their
	// side: denied the same way it is when one is connected.
	rec = f.submit(submitOpts{key: "registered-foreign-1", clusterID: "cluster_theirs"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("foreign registered offline pin = %d, want 403: %s", rec.Code, rec.Body)
	}
	// A genuinely unknown id keeps the unregistered answer.
	rec = f.submit(submitOpts{key: "unknown-cluster-1", clusterID: "cluster_nowhere"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("unknown pin = %d, want 409: %s", rec.Code, rec.Body)
	}
	if body := rec.Body.String(); strings.Contains(body, "registered but") {
		t.Fatalf("unknown cluster answered as registered: %s", body)
	}
}

// A scoped connector credential's mint is pinned to its bound cluster: no
// header needed, no single-connector guessing, and the ambiguity guard never
// fires — the binding IS the answer.
func TestMintKey_ScopedCredentialBindingResolvesCluster(t *testing.T) {
	h, cust, prov := newMintFixture(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/agent/ts-auth-key", strings.NewReader("{}"))
	ctx := context.WithValue(context.Background(), ctxCustomer, cust)
	ctx = context.WithValue(ctx, ctxAgentCluster, "cluster_b")
	rec := httptest.NewRecorder()
	h.MintKey(rec, req.WithContext(ctx))
	if rec.Code != http.StatusOK {
		t.Fatalf("bound mint = %d, want 200: %s", rec.Code, rec.Body)
	}
	if len(prov.swept) != 1 || prov.swept[0] != "yscale-agent-cluster_b" {
		t.Fatalf("swept = %v, want the BOUND cluster's hostname with two clusters connected", prov.swept)
	}
}
