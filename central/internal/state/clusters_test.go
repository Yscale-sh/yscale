package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

var clusterEpoch = time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC)

// registerCluster is the happy-path shorthand: alice (an owner unless the test
// says otherwise) registers one cluster and the test gets the row and the
// once-revealed credential back.
func registerCluster(t *testing.T, s *Store, customerID, callerID, clusterID, name string) (TenantCluster, string) {
	t.Helper()
	row, token, _, err := s.RegisterTenantCluster(customerID, callerID, clusterID, name, HumanActor(callerID, customerID))
	if err != nil {
		t.Fatalf("RegisterTenantCluster %s/%s: %v", customerID, clusterID, err)
	}
	return row, token
}

// The registration contract: owner/admin may mint, the credential comes back
// exactly once and is stored only as a hash, and the credential authenticates
// as the tenant bound to exactly its cluster.
func TestRegisterTenantClusterMintsScopedCredential(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "adm": RoleAdmin, "mem": RoleMember, "view": RoleViewer,
	}, "stranger")

	row, token, role, err := s.RegisterTenantCluster("cust_r", ids["alice"], "cl-east", "  Build farm east  ", HumanActor(ids["alice"], "cust_r"))
	if err != nil {
		t.Fatalf("owner register: %v", err)
	}
	if role != RoleOwner || row.ClusterID != "cl-east" || row.Name != "Build farm east" ||
		row.Source != ClusterSourceRegistered || row.State != ClusterStateNeverConnected {
		t.Fatalf("registered row = %+v role=%q", row, role)
	}
	if token == "" || !strings.HasPrefix(token, "yscale_cluster_") {
		t.Fatalf("credential = %q, want a yscale_cluster_ token", token)
	}

	// Stored as a hash and only as a hash: the row carries the SHA-256, never
	// the plaintext, and the plaintext is not recoverable from the store.
	c, err := s.CustomerByID("cust_r")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if len(c.RegisteredClusters) != 1 {
		t.Fatalf("registry = %+v, want 1 row", c.RegisteredClusters)
	}
	stored := c.RegisteredClusters[0]
	if stored.CredentialHash != HashClusterCredential(token) {
		t.Fatalf("stored hash does not match the revealed credential's digest")
	}
	if strings.Contains(stored.CredentialHash, token) || stored.CredentialHash == token {
		t.Fatal("plaintext credential leaked into durable state")
	}

	// The credential authenticates THIS tenant, bound to THIS cluster — and
	// nothing else does: a guess and the tenant token both miss.
	cust, boundCluster, err := s.AuthClusterCredential(token)
	if err != nil || cust.ID != "cust_r" || boundCluster != "cl-east" {
		t.Fatalf("AuthClusterCredential = (%v,%q,%v), want cust_r/cl-east", cust, boundCluster, err)
	}
	if _, _, err := s.AuthClusterCredential("yscale_cluster_" + strings.Repeat("0", 48)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("guessed credential err = %v, want ErrNotFound", err)
	}
	if _, _, err := s.AuthClusterCredential("tok_cust_r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant token on the credential lookup err = %v, want ErrNotFound", err)
	}

	// The id is owned durably — no connector has ever been live.
	if owner, ok := s.CustomerForClusterID("cl-east"); !ok || owner != "cust_r" {
		t.Fatalf("CustomerForClusterID = (%q,%v), want (cust_r,true) with no socket", owner, ok)
	}

	// An admin may register; member and viewer may not; a non-member and an
	// unknown tenant are the same ErrNotFound the rest of the surface answers.
	if _, _, gotRole, err := s.RegisterTenantCluster("cust_r", ids["adm"], "", "West", HumanActor(ids["adm"], "cust_r")); err != nil || gotRole != RoleAdmin {
		t.Fatalf("admin register = (%q,%v), want accepted", gotRole, err)
	}
	for _, sub := range []string{"mem", "view"} {
		if _, _, _, err := s.RegisterTenantCluster("cust_r", ids[sub], "", "Nope", HumanActor(ids[sub], "cust_r")); !errors.Is(err, ErrNotAuthorized) {
			t.Errorf("%s register err = %v, want ErrNotAuthorized", sub, err)
		}
	}
	if _, _, _, err := s.RegisterTenantCluster("cust_r", ids["stranger"], "", "Nope", HumanActor(ids["stranger"], "cust_r")); !errors.Is(err, ErrNotFound) {
		t.Errorf("non-member register err = %v, want ErrNotFound", err)
	}
	if _, _, _, err := s.RegisterTenantCluster("cust_nope", ids["alice"], "", "Nope", HumanActor(ids["alice"], "cust_nope")); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown tenant register err = %v, want ErrNotFound", err)
	}

	// A generated id is valid under the same grammar a supplied one must pass.
	generated, _ := registerCluster(t, s, "cust_r", ids["alice"], "", "Generated")
	if !ValidClusterID(generated.ClusterID) || !strings.HasPrefix(generated.ClusterID, "cluster-") {
		t.Fatalf("generated cluster id %q", generated.ClusterID)
	}

	// Invalid input is refused before anything is minted.
	if _, _, _, err := s.RegisterTenantCluster("cust_r", ids["alice"], "bad id!", "X", HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrInvalidCluster) {
		t.Errorf("invalid id err = %v, want ErrInvalidCluster", err)
	}
	if _, _, _, err := s.RegisterTenantCluster("cust_r", ids["alice"], "", "   ", HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrInvalidCluster) {
		t.Errorf("blank name err = %v, want ErrInvalidCluster", err)
	}
	if _, _, _, err := s.RegisterTenantCluster("cust_r", ids["alice"], "", "evil\x00name", HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrInvalidCluster) {
		t.Errorf("control-character name err = %v, want ErrInvalidCluster", err)
	}
}

// Cluster ids are GLOBAL: a second tenant cannot register an id the first one
// holds — with no connector live on either side — and one tenant cannot
// register the same id twice. The registry is bounded at MaxRegisteredClusters.
func TestRegisterTenantClusterEnforcesGlobalUniquenessAndBound(t *testing.T) {
	s, _, ids := manageStore(t, "cust_a", map[string]string{"alice": RoleOwner})
	s.AddCustomer(&Customer{ID: "cust_b", Token: "tok_b", Plan: "pro"})
	bob, err := s.UpsertAccount(humanIssuer, "bob", AccountProfile{Email: "bob@acme.com", EmailVerified: true})
	if err != nil {
		t.Fatalf("UpsertAccount bob: %v", err)
	}
	if _, err := s.AddTenantMembership(bob.ID, "cust_b", RoleOwner); err != nil {
		t.Fatalf("AddTenantMembership bob: %v", err)
	}

	registerCluster(t, s, "cust_a", ids["alice"], "cl-shared", "A's cluster")
	if _, _, _, err := s.RegisterTenantCluster("cust_b", bob.ID, "cl-shared", "B's cluster", HumanActor(bob.ID, "cust_b")); !errors.Is(err, ErrClusterExists) {
		t.Fatalf("cross-tenant duplicate register err = %v, want ErrClusterExists", err)
	}
	if _, _, _, err := s.RegisterTenantCluster("cust_a", ids["alice"], "cl-shared", "Again", HumanActor(ids["alice"], "cust_a")); !errors.Is(err, ErrClusterExists) {
		t.Fatalf("same-tenant duplicate register err = %v, want ErrClusterExists", err)
	}

	// A live socket owns its id too, registry row or not: the legacy cluster
	// another tenant is running must not be registerable out from under it.
	connect(s, "agent_live", "cust_b", "cl-live", 0)
	if _, _, _, err := s.RegisterTenantCluster("cust_a", ids["alice"], "cl-live", "Steal", HumanActor(ids["alice"], "cust_a")); !errors.Is(err, ErrClusterExists) {
		t.Fatalf("live-socket duplicate register err = %v, want ErrClusterExists", err)
	}

	for i := 1; i < MaxRegisteredClusters; i++ {
		registerCluster(t, s, "cust_a", ids["alice"], fmt.Sprintf("cl-fill-%02d", i), fmt.Sprintf("Fill %d", i))
	}
	if _, _, _, err := s.RegisterTenantCluster("cust_a", ids["alice"], "cl-overflow", "Too many", HumanActor(ids["alice"], "cust_a")); !errors.Is(err, ErrClusterLimit) {
		t.Fatalf("registration past the bound err = %v, want ErrClusterLimit", err)
	}
}

// Rotation replaces the credential: the new one works, the OLD ONE STOPS, and
// only owner/admin may rotate. Deletion invalidates the credential entirely,
// frees the id, and refuses while the tenant's own policy still names the
// cluster.
func TestRotateAndDeleteClusterCredential(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "view": RoleViewer,
	})
	_, oldToken := registerCluster(t, s, "cust_r", ids["alice"], "cl-rot", "Rotating")

	if _, _, _, err := s.RotateTenantClusterCredential("cust_r", ids["view"], "cl-rot", HumanActor(ids["view"], "cust_r")); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("viewer rotate err = %v, want ErrNotAuthorized", err)
	}
	if _, _, _, err := s.RotateTenantClusterCredential("cust_r", ids["alice"], "cl-gone", HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrClusterNotFound) {
		t.Fatalf("rotate unknown cluster err = %v, want ErrClusterNotFound", err)
	}

	_, newToken, _, err := s.RotateTenantClusterCredential("cust_r", ids["alice"], "cl-rot", HumanActor(ids["alice"], "cust_r"))
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if newToken == oldToken {
		t.Fatal("rotation returned the same credential")
	}
	if _, _, err := s.AuthClusterCredential(oldToken); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old credential still authenticates after rotation: %v", err)
	}
	if _, bound, err := s.AuthClusterCredential(newToken); err != nil || bound != "cl-rot" {
		t.Fatalf("new credential = (%q,%v), want cl-rot", bound, err)
	}

	// A cluster the policy names cannot be deleted out from under the policy.
	if _, _, _, err := s.SetTenantClusterPolicy("cust_r", ids["alice"], ClusterPolicy{Deny: []string{"cl-rot"}}, HumanActor(ids["alice"], "cust_r")); err != nil {
		t.Fatalf("set policy: %v", err)
	}
	if _, err := s.DeleteTenantCluster("cust_r", ids["alice"], "cl-rot", HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrClusterNamedByPolicy) {
		t.Fatalf("delete of policy-named cluster err = %v, want ErrClusterNamedByPolicy", err)
	}
	if _, _, _, err := s.SetTenantClusterPolicy("cust_r", ids["alice"], ClusterPolicy{}, HumanActor(ids["alice"], "cust_r")); err != nil {
		t.Fatalf("clear policy: %v", err)
	}
	if _, err := s.DeleteTenantCluster("cust_r", ids["view"], "cl-rot", HumanActor(ids["view"], "cust_r")); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("viewer delete err = %v, want ErrNotAuthorized", err)
	}
	if _, err := s.DeleteTenantCluster("cust_r", ids["alice"], "cl-rot", HumanActor(ids["alice"], "cust_r")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.DeleteTenantCluster("cust_r", ids["alice"], "cl-rot", HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrClusterNotFound) {
		t.Fatalf("second delete err = %v, want ErrClusterNotFound", err)
	}
	// The credential died with the row and the id is free again.
	if _, _, err := s.AuthClusterCredential(newToken); !errors.Is(err, ErrNotFound) {
		t.Fatalf("credential survived its cluster's deletion: %v", err)
	}
	if _, ok := s.CustomerForClusterID("cl-rot"); ok {
		t.Fatal("deleted cluster id still owned")
	}
}

// agentsGauge scrapes yscale_agents_connected off the meter's own endpoint —
// the only public read the gauge has — so eviction tests can prove the count
// is exact, not just plausible.
func agentsGauge(t *testing.T, m *cost.Meter) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, "yscale_agents_connected "); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				t.Fatalf("parse yscale_agents_connected %q: %v", v, err)
			}
			return f
		}
	}
	t.Fatal("yscale_agents_connected not found in metrics")
	return 0
}

// Rotation is the recovery path for a LEAKED credential, so the socket the
// old credential holds open must die with it: the live agent leaves routing
// in the same store mutation that publishes the new hash, its pumps are
// signalled, enqueue refuses — and the stream cleanup's later RemoveAgent is
// a harmless no-op that cannot double-decrement the gauge.
func TestRotateEvictsLiveConnector(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	s.Cost = cost.NewMeter()
	registerCluster(t, s, "cust_r", ids["alice"], "cl-rot", "Rotating")
	a := connect(s, "agent_rot", "cust_r", "cl-rot", 0)
	if got := agentsGauge(t, s.Cost); got != 1 {
		t.Fatalf("gauge after connect = %v, want 1", got)
	}

	if _, _, _, err := s.RotateTenantClusterCredential("cust_r", ids["alice"], "cl-rot", HumanActor(ids["alice"], "cust_r")); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if _, err := s.AgentForCluster("cust_r", "cl-rot"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rotated cluster still routable: %v", err)
	}
	if agents := s.AgentsForCustomer("cust_r"); len(agents) != 0 {
		t.Fatalf("evicted agent still listed: %v", agentIDs(agents))
	}
	select {
	case <-a.Evicted():
	default:
		t.Fatal("rotation did not signal the connector's pumps")
	}
	if err := a.Enqueue(protocol.Envelope{}); err == nil {
		t.Fatal("enqueue to an evicted agent succeeded")
	}
	// The dead socket's cleanup still runs: RemoveAgent and a second Evict
	// must both be no-ops, and the gauge must land on exactly zero.
	s.RemoveAgent(a.ID)
	a.Evict()
	if got := agentsGauge(t, s.Cost); got != 0 {
		t.Fatalf("gauge after eviction + cleanup = %v, want exactly 0", got)
	}
	// A reconnect with the NEW credential is a fresh, routable agent.
	connect(s, "agent_rot2", "cust_r", "cl-rot", 1)
	if got, err := s.AgentForCluster("cust_r", "cl-rot"); err != nil || got.ID != "agent_rot2" {
		t.Fatalf("reconnect after rotation = (%v,%v), want agent_rot2", got, err)
	}
}

// Delete: the moment the store mutation returns, the registry row is gone AND
// the live socket is out of routing — the fleet list and AgentForCluster
// agree, and the disconnect stamping that trails the dead socket cannot
// resurrect the row.
func TestDeleteEvictsLiveConnectorAndFleetAgrees(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	registerCluster(t, s, "cust_r", ids["alice"], "cl-del", "Doomed")
	a := connect(s, "agent_del", "cust_r", "cl-del", 0)

	if _, err := s.DeleteTenantCluster("cust_r", ids["alice"], "cl-del", HumanActor(ids["alice"], "cust_r")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	rows, err := s.TenantClustersFor("cust_r", ids["alice"])
	if err != nil || len(rows) != 0 {
		t.Fatalf("fleet after delete = (%+v,%v), want empty", rows, err)
	}
	if _, err := s.AgentForCluster("cust_r", "cl-del"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted cluster still routable: %v", err)
	}
	if owner, ok := s.CustomerForClusterID("cl-del"); ok {
		t.Fatalf("deleted cluster id still owned by %q", owner)
	}
	select {
	case <-a.Evicted():
	default:
		t.Fatal("delete did not signal the connector's pumps")
	}
	if err := a.Enqueue(protocol.Envelope{}); err == nil {
		t.Fatal("enqueue to a deleted cluster's agent succeeded")
	}
	// The trailing stream cleanup: no panic, no resurrection.
	s.RemoveAgent(a.ID)
	s.MarkClusterDisconnected("cust_r", "cl-del", clusterEpoch.Add(time.Hour), clusterEpoch.Add(59*time.Minute))
	if rows, _ := s.TenantClustersFor("cust_r", ids["alice"]); len(rows) != 0 {
		t.Fatalf("disconnect stamping resurrected the deleted row: %+v", rows)
	}
}

// The Hello bind: a legacy tenant-token connector claims its self-announced
// cluster into the registry on first connection; a scoped credential requires
// its row; and a cluster id held by another tenant is refused with NO
// connector live on the other side.
func TestClaimAgentCluster(t *testing.T) {
	s, spy, _ := manageStore(t, "cust_a", map[string]string{"alice": RoleOwner})
	s.AddCustomer(&Customer{ID: "cust_b", Token: "tok_b", Plan: "pro"})

	claimed, _, err := s.ClaimAgentCluster("cust_a", "cl-legacy", false, clusterEpoch)
	if err != nil || !claimed {
		t.Fatalf("first legacy claim = (%v,%v), want (true,nil)", claimed, err)
	}
	c, _ := s.CustomerByID("cust_a")
	_, row := registeredClusterLocked(c, "cl-legacy")
	if row == nil || row.Source != ClusterSourceClaimed || row.CredentialHash != "" {
		t.Fatalf("claimed row = %+v, want source=claimed with no credential", row)
	}
	if row.FirstConnectedAt == nil || !row.FirstConnectedAt.Equal(clusterEpoch) ||
		row.LastConnectedAt == nil || !row.LastConnectedAt.Equal(clusterEpoch) {
		t.Fatalf("claim did not stamp connect metadata: %+v", row)
	}

	// A reconnect is not a second claim, and it advances only LastConnectedAt.
	later := clusterEpoch.Add(time.Hour)
	claimed, _, err = s.ClaimAgentCluster("cust_a", "cl-legacy", false, later)
	if err != nil || claimed {
		t.Fatalf("reconnect claim = (%v,%v), want (false,nil)", claimed, err)
	}
	c, _ = s.CustomerByID("cust_a")
	_, row = registeredClusterLocked(c, "cl-legacy")
	if !row.FirstConnectedAt.Equal(clusterEpoch) || !row.LastConnectedAt.Equal(later) {
		t.Fatalf("reconnect stamps = %+v", row)
	}

	// Cross-tenant, and offline: cust_a's claim is durable, so cust_b is
	// refused with no socket held anywhere.
	if _, _, err := s.ClaimAgentCluster("cust_b", "cl-legacy", false, later); !errors.Is(err, ErrClusterOwnedElsewhere) {
		t.Fatalf("cross-tenant claim err = %v, want ErrClusterOwnedElsewhere", err)
	}

	// A scoped credential's bind requires the registered row — a cluster
	// deleted after the request authenticated is a refusal, not a fresh claim.
	if _, _, err := s.ClaimAgentCluster("cust_b", "cl-unregistered", true, later); !errors.Is(err, ErrClusterNotFound) {
		t.Fatalf("scoped bind of unregistered cluster err = %v, want ErrClusterNotFound", err)
	}
	// A LEGACY hello with a pre-grammar id is not refused — it degrades to the
	// old live-only mode (no row; see TestClaimAgentClusterLegacyInvalidIDStaysEphemeral)
	// — while the scoped path holds the grammar strictly.
	if claimed, _, err := s.ClaimAgentCluster("cust_a", "not a cluster id!", false, later); err != nil || claimed {
		t.Fatalf("legacy invalid hello = (%v,%v), want ephemeral (false,nil)", claimed, err)
	}
	if _, _, err := s.ClaimAgentCluster("cust_a", "not a cluster id!", true, later); !errors.Is(err, ErrInvalidCluster) {
		t.Fatalf("scoped invalid hello err = %v, want ErrInvalidCluster", err)
	}

	// The claim wrote through: a restart still knows the owner.
	reloaded := emptyStore()
	reloaded.applySnapshot(spy.recordedSnapshot(t))
	if owner, ok := reloaded.CustomerForClusterID("cl-legacy"); !ok || owner != "cust_a" {
		t.Fatalf("claim not durable across restart: (%q,%v)", owner, ok)
	}
}

// A legacy connector whose pre-registry cluster id predates the ValidClusterID
// grammar keeps its old contract: it connects live-only, claims no row, and is
// still protected cross-tenant while its socket lives. Only the scoped path
// treats the grammar as a hard refusal.
func TestClaimAgentClusterLegacyInvalidIDStaysEphemeral(t *testing.T) {
	s, _, _ := manageStore(t, "cust_a", map[string]string{"alice": RoleOwner})
	s.AddCustomer(&Customer{ID: "cust_b", Token: "tok_b", Plan: "pro"})

	const legacyID = "Legacy_Cluster/01" // '/' is outside the grammar
	claimed, _, err := s.ClaimAgentCluster("cust_a", legacyID, false, clusterEpoch)
	if err != nil || claimed {
		t.Fatalf("legacy claim of pre-grammar id = (%v,%v), want ephemeral (false,nil)", claimed, err)
	}
	c, _ := s.CustomerByID("cust_a")
	if len(c.RegisteredClusters) != 0 {
		t.Fatalf("pre-grammar id was written into the registry: %+v", c.RegisteredClusters)
	}
	// While a socket is live the id is owned exactly as before the registry.
	connect(s, "agent_legacy", "cust_a", legacyID, 0)
	if _, _, err := s.ClaimAgentCluster("cust_b", legacyID, false, clusterEpoch); !errors.Is(err, ErrClusterOwnedElsewhere) {
		t.Fatalf("cross-tenant claim of a live legacy id err = %v, want ErrClusterOwnedElsewhere", err)
	}
	// Scoped credentials remain strict, and the tenant checks still hold.
	if _, _, err := s.ClaimAgentCluster("cust_a", legacyID, true, clusterEpoch); !errors.Is(err, ErrInvalidCluster) {
		t.Fatalf("scoped claim of invalid id err = %v, want ErrInvalidCluster", err)
	}
	if _, _, err := s.ClaimAgentCluster("cust_nope", legacyID, false, clusterEpoch); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown tenant's legacy claim err = %v, want ErrNotFound", err)
	}
}

// At MaxRegisteredClusters a fresh legacy claim sheds the oldest CLAIMED row
// that is safe to drop — not live here, no minted credential, not named by
// policy — and NEVER an API-registered row. The pruned id is free again.
func TestClaimAgentClusterPrunesOldestSafeClaimedRowAtCapacity(t *testing.T) {
	s, _, ids := manageStore(t, "cust_a", map[string]string{"alice": RoleOwner})
	alice := HumanActor(ids["alice"], "cust_a")

	// The five protected shapes: API-registered; claimed-then-rotated (its
	// credential would die with the row); claimed but live; claimed but named
	// by policy — and cl-old, the oldest claim that is none of those.
	registerCluster(t, s, "cust_a", ids["alice"], "cl-api", "API registered")
	if _, _, err := s.ClaimAgentCluster("cust_a", "cl-upg", false, clusterEpoch.Add(-time.Hour)); err != nil {
		t.Fatalf("claim cl-upg: %v", err)
	}
	if _, _, _, err := s.RotateTenantClusterCredential("cust_a", ids["alice"], "cl-upg", alice); err != nil {
		t.Fatalf("upgrade cl-upg: %v", err)
	}
	if _, _, err := s.ClaimAgentCluster("cust_a", "cl-old", false, clusterEpoch); err != nil {
		t.Fatalf("claim cl-old: %v", err)
	}
	if _, _, err := s.ClaimAgentCluster("cust_a", "cl-live", false, clusterEpoch.Add(time.Minute)); err != nil {
		t.Fatalf("claim cl-live: %v", err)
	}
	connect(s, "agent_live", "cust_a", "cl-live", 0)
	if _, _, err := s.ClaimAgentCluster("cust_a", "cl-pinned", false, clusterEpoch.Add(2*time.Minute)); err != nil {
		t.Fatalf("claim cl-pinned: %v", err)
	}
	if _, _, _, err := s.SetTenantClusterPolicy("cust_a", ids["alice"], ClusterPolicy{Allow: []string{"cl-pinned"}}, alice); err != nil {
		t.Fatalf("set policy: %v", err)
	}
	for i := 5; i < MaxRegisteredClusters; i++ {
		if _, _, err := s.ClaimAgentCluster("cust_a", fmt.Sprintf("cl-fill-%02d", i), false, clusterEpoch.Add(time.Hour+time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("claim fill %d: %v", i, err)
		}
	}

	claimed, _, err := s.ClaimAgentCluster("cust_a", "cl-new", false, clusterEpoch.Add(2*time.Hour))
	if err != nil || !claimed {
		t.Fatalf("claim at capacity = (%v,%v), want a pruned slot and (true,nil)", claimed, err)
	}
	c, _ := s.CustomerByID("cust_a")
	if len(c.RegisteredClusters) != MaxRegisteredClusters {
		t.Fatalf("registry size = %d, want %d", len(c.RegisteredClusters), MaxRegisteredClusters)
	}
	if _, row := registeredClusterLocked(c, "cl-old"); row != nil {
		t.Fatal("oldest safe claimed row survived; something else was pruned")
	}
	for _, keep := range []string{"cl-api", "cl-upg", "cl-live", "cl-pinned", "cl-new"} {
		if _, row := registeredClusterLocked(c, keep); row == nil {
			t.Fatalf("%s was pruned; only the oldest safe claimed row may be", keep)
		}
	}
	if owner, ok := s.CustomerForClusterID("cl-old"); ok {
		t.Fatalf("pruned id still owned by %q", owner)
	}
}

// When nothing is safe to prune — every row API-registered — the limit stays a
// strict answer on the tenant API but NOT on the connector path: the legacy
// connector falls back to live-only instead of the socket being refused.
func TestClaimAgentClusterCapacityFallbackStaysEphemeral(t *testing.T) {
	s, _, ids := manageStore(t, "cust_a", map[string]string{"alice": RoleOwner})
	for i := 0; i < MaxRegisteredClusters; i++ {
		registerCluster(t, s, "cust_a", ids["alice"], fmt.Sprintf("cl-reg-%02d", i), fmt.Sprintf("Reg %d", i))
	}

	claimed, _, err := s.ClaimAgentCluster("cust_a", "cl-extra", false, clusterEpoch)
	if err != nil || claimed {
		t.Fatalf("legacy claim over a full registry = (%v,%v), want ephemeral (false,nil)", claimed, err)
	}
	c, _ := s.CustomerByID("cust_a")
	if len(c.RegisteredClusters) != MaxRegisteredClusters {
		t.Fatalf("registry size = %d after fallback, want untouched %d", len(c.RegisteredClusters), MaxRegisteredClusters)
	}
	if _, row := registeredClusterLocked(c, "cl-extra"); row != nil {
		t.Fatal("ephemeral fallback wrote a registry row")
	}
	// The connector is live-only routable; the strict surfaces stay strict.
	connect(s, "agent_extra", "cust_a", "cl-extra", 0)
	if _, err := s.AgentForCluster("cust_a", "cl-extra"); err != nil {
		t.Fatalf("ephemeral connector not routable: %v", err)
	}
	if _, _, _, err := s.RegisterTenantCluster("cust_a", ids["alice"], "cl-more", "Nope", HumanActor(ids["alice"], "cust_a")); !errors.Is(err, ErrClusterLimit) {
		t.Fatalf("API register past the bound err = %v, want ErrClusterLimit", err)
	}
}

// Disconnect stamps land on the row; the tenant list derives the stable states
// from registry + live sockets; and a heartbeat is never a durable write.
func TestClusterLifecycleStatesAndDisconnectStamps(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner, "view": RoleViewer})

	registerCluster(t, s, "cust_r", ids["alice"], "cl-idle", "Never connected")
	if _, _, err := s.ClaimAgentCluster("cust_r", "cl-up", false, clusterEpoch); err != nil {
		t.Fatalf("claim cl-up: %v", err)
	}
	a := connect(s, "agent_up", "cust_r", "cl-up", 0)
	if _, _, err := s.ClaimAgentCluster("cust_r", "cl-down", false, clusterEpoch); err != nil {
		t.Fatalf("claim cl-down: %v", err)
	}
	observed := clusterEpoch.Add(30 * time.Minute)
	dropped := clusterEpoch.Add(31 * time.Minute)
	s.MarkClusterDisconnected("cust_r", "cl-down", dropped, observed)
	// Unknown cluster and unknown customer are silent no-ops: a delete racing
	// a disconnect must not resurrect a row.
	s.MarkClusterDisconnected("cust_r", "cl-never-here", dropped, observed)
	s.MarkClusterDisconnected("cust_nope", "cl-down", dropped, observed)

	rows, err := s.TenantClustersFor("cust_r", ids["view"])
	if err != nil {
		t.Fatalf("TenantClustersFor: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %+v, want 3", rows)
	}
	byID := make(map[string]TenantCluster, len(rows))
	for _, r := range rows {
		byID[r.ClusterID] = r
	}
	if got := byID["cl-idle"]; got.State != ClusterStateNeverConnected || got.Connections != 0 {
		t.Errorf("cl-idle = %+v, want never_connected", got)
	}
	if got := byID["cl-up"]; got.State != ClusterStateConnectedHere || got.Connections != 1 || !got.ConnectedAt.Equal(a.ConnectedAt) {
		t.Errorf("cl-up = %+v, want connected_here with the live socket's facts", got)
	}
	down := byID["cl-down"]
	if down.State != ClusterStateDisconnected {
		t.Errorf("cl-down state = %q, want disconnected — a registered cluster stays in the fleet offline", down.State)
	}
	if down.LastDisconnectedAt == nil || !down.LastDisconnectedAt.Equal(dropped) ||
		down.LastObservedAt == nil || !down.LastObservedAt.Equal(observed) {
		t.Errorf("cl-down stamps = %+v", down)
	}
}

// The registry survives a restart through the customer JSONB path: rows,
// hashes and stamps round-trip, both derived indexes are rebuilt, and a
// revoked tenant's credential no longer authenticates while its id stays held.
func TestClusterRegistryPersistsAcrossRestart(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	s.AddCustomer(&Customer{ID: "cust_gone", Token: "tok_gone", Plan: "pro"})

	_, token := registerCluster(t, s, "cust_r", ids["alice"], "cl-durable", "Durable")
	if _, _, err := s.ClaimAgentCluster("cust_gone", "cl-revoked", false, clusterEpoch); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.RevokeCustomer("cust_gone"); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	reloaded := emptyStore()
	reloaded.applySnapshot(spy.recordedSnapshot(t))

	cust, bound, err := reloaded.AuthClusterCredential(token)
	if err != nil || cust.ID != "cust_r" || bound != "cl-durable" {
		t.Fatalf("credential after restart = (%v,%q,%v)", cust, bound, err)
	}
	if owner, ok := reloaded.CustomerForClusterID("cl-durable"); !ok || owner != "cust_r" {
		t.Fatalf("owner index after restart = (%q,%v)", owner, ok)
	}
	// The revoked tenant's id stays HELD — a half-finished offboard must not
	// free it — but nothing of the tenant authenticates.
	if owner, ok := reloaded.CustomerForClusterID("cl-revoked"); !ok || owner != "cust_gone" {
		t.Fatalf("revoked tenant's cluster id freed: (%q,%v)", owner, ok)
	}
	if _, _, err := reloaded.ClaimAgentCluster("cust_r", "cl-revoked", false, clusterEpoch); !errors.Is(err, ErrClusterOwnedElsewhere) {
		t.Fatalf("claiming a revoked tenant's id err = %v, want ErrClusterOwnedElsewhere", err)
	}

	// The JSONB encoding itself: every field a row carries must round-trip.
	now := clusterEpoch
	in := &Customer{ID: "c", Token: "t", RegisteredClusters: []*RegisteredCluster{{
		ClusterID: "cl-x", Name: "X", CredentialHash: HashClusterCredential("secret"),
		Source: ClusterSourceRegistered, RegisteredAt: now,
		FirstConnectedAt: &now, LastConnectedAt: &now, LastDisconnectedAt: &now, LastObservedAt: &now,
	}}}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Customer
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := out.RegisteredClusters[0]
	if got.ClusterID != "cl-x" || got.Name != "X" || got.CredentialHash != HashClusterCredential("secret") ||
		got.Source != ClusterSourceRegistered || !got.RegisteredAt.Equal(now) ||
		got.FirstConnectedAt == nil || got.LastDisconnectedAt == nil || got.LastObservedAt == nil {
		t.Fatalf("registered cluster lost fields in round-trip: %+v", got)
	}
}

// The env-seed path re-adds customers from config on every boot, knowing
// nothing about clusters registered at runtime. The registry — and the
// credential hashes inside it — must be carried over like the namespace set,
// or every registered connector stops authenticating at the next restart.
func TestIndexCustomerCarriesRegisteredClustersOver(t *testing.T) {
	s, _, ids := manageStore(t, "cust_seed", map[string]string{"alice": RoleOwner})
	_, token := registerCluster(t, s, "cust_seed", ids["alice"], "cl-kept", "Kept")

	// Boot re-seed: same id, fresh record, no registry.
	s.AddCustomer(&Customer{ID: "cust_seed", Token: "tok_cust_seed", Plan: "pro"})

	if _, bound, err := s.AuthClusterCredential(token); err != nil || bound != "cl-kept" {
		t.Fatalf("credential after re-seed = (%q,%v), want cl-kept", bound, err)
	}
	if owner, ok := s.CustomerForClusterID("cl-kept"); !ok || owner != "cust_seed" {
		t.Fatalf("owner after re-seed = (%q,%v)", owner, ok)
	}

	// TenantClustersFor hands back copies: mutating the answer must not touch
	// the stored registry.
	rows, err := s.TenantClustersFor("cust_seed", ids["alice"])
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows = (%+v,%v)", rows, err)
	}
	rows[0].Name = "vandalized"
	if fresh, _ := s.TenantClustersFor("cust_seed", ids["alice"]); fresh[0].Name != "Kept" {
		t.Fatalf("stored registry mutated through a returned row: %+v", fresh[0])
	}
}

// The registry writes are journaled with safe metadata only: the action, the
// cluster id and the actor's role — never the credential, its hash, or the
// tenant-authored name.
func TestClusterRegistryAudits(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner})

	_, token := registerCluster(t, s, "cust_r", ids["alice"], "cl-aud", "Secret Name")
	_, rotated, _, err := s.RotateTenantClusterCredential("cust_r", ids["alice"], "cl-aud", HumanActor(ids["alice"], "cust_r"))
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if _, err := s.DeleteTenantCluster("cust_r", ids["alice"], "cl-aud", HumanActor(ids["alice"], "cust_r")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	for action, reason := range map[string]string{
		ActionClusterRegister: ReasonClusterRegistered,
		ActionClusterRotate:   ReasonClusterCredentialRotated,
		ActionClusterDelete:   ReasonClusterDeleted,
	} {
		rows := spy.eventsWith(action)
		if len(rows) != 1 {
			t.Fatalf("%s rows = %d, want 1", action, len(rows))
		}
		ev := rows[0]
		if ev.TargetKind != TargetCluster || ev.TargetID != "cl-aud" ||
			ev.Outcome != OutcomeAccepted || ev.Detail.Reason != reason || ev.Detail.Role != RoleOwner {
			t.Errorf("%s row = %+v", action, ev)
		}
		raw, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("marshal audit row: %v", err)
		}
		for _, secret := range []string{token, rotated, HashClusterCredential(token), HashClusterCredential(rotated), "Secret Name"} {
			if strings.Contains(string(raw), secret) {
				t.Fatalf("%s audit row leaked a secret or tenant-authored text: %s", action, raw)
			}
		}
	}
}
