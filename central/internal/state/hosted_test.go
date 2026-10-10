package state

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
)

// assignHosted is the happy-path shorthand: the operator assigns one hosted
// cluster and the test gets the row and the once-revealed credential back.
func assignHosted(t *testing.T, s *Store, customerID, clusterID, name, namespace string) (TenantCluster, string) {
	t.Helper()
	row, token, err := s.AssignHostedCluster(customerID, clusterID, name, namespace, OperatorActor())
	if err != nil {
		t.Fatalf("AssignHostedCluster %s/%s: %v", customerID, clusterID, err)
	}
	return row, token
}

// The assignment contract: a hosted row in the TENANT'S OWN registry whose
// credential authenticates as that tenant bound to exactly its virtual
// cluster id, plus the reserved namespace granted into the tenant's
// authorized set without moving where an unqualified submission lands.
func TestAssignHostedClusterMintsTenantBoundCredential(t *testing.T) {
	s, _ := storeWithSpy("cust_a")

	row, token := assignHosted(t, s, "cust_a", "", "", "")
	if row.Source != ClusterSourceHosted || row.State != ClusterStateNeverConnected {
		t.Fatalf("hosted row = %+v", row)
	}
	if !strings.HasPrefix(row.ClusterID, "hosted-") || !ValidClusterID(row.ClusterID) {
		t.Fatalf("generated hosted cluster id %q", row.ClusterID)
	}
	if row.HostedNamespace != "ys-cust-a" {
		t.Fatalf("derived namespace = %q, want ys-cust-a", row.HostedNamespace)
	}
	if token == "" || !strings.HasPrefix(token, "yscale_cluster_") {
		t.Fatalf("credential = %q, want a yscale_cluster_ token", token)
	}

	// The credential binds to THIS tenant and THIS virtual cluster id — the
	// exact seam ConnectorAuth and the agent Hello bind resolve through.
	cust, bound, err := s.AuthClusterCredential(token)
	if err != nil || cust.ID != "cust_a" || bound != row.ClusterID {
		t.Fatalf("AuthClusterCredential = (%v,%q,%v), want cust_a/%s", cust, bound, err, row.ClusterID)
	}
	if owner, ok := s.CustomerForClusterID(row.ClusterID); !ok || owner != "cust_a" {
		t.Fatalf("CustomerForClusterID = (%q,%v), want (cust_a,true)", owner, ok)
	}

	// Stored as a hash only, on the tenant's own record, with the namespace
	// authorization published in the same mutation. The fail-closed default
	// is materialized FIRST: an unqualified submission still lands in
	// "default", never in the shared cluster's namespace.
	c, err := s.CustomerByID("cust_a")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if len(c.RegisteredClusters) != 1 || c.RegisteredClusters[0].CredentialHash != HashClusterCredential(token) {
		t.Fatalf("registry = %+v", c.RegisteredClusters)
	}
	if !slices.Equal(c.WorkloadNamespaces, []string{"default", "ys-cust-a"}) {
		t.Fatalf("authorized namespaces = %v, want [default ys-cust-a]", c.WorkloadNamespaces)
	}

	// A tenant with a configured set keeps its first entry first.
	s.AddCustomer(&Customer{ID: "cust_b", Token: "tok_b", Plan: "pro", WorkloadNamespaces: []string{"team-a", "team-b"}})
	rowB, _ := assignHosted(t, s, "cust_b", "hosted-b", "B capacity", "ys-b")
	if rowB.HostedNamespace != "ys-b" || rowB.Name != "B capacity" {
		t.Fatalf("supplied assignment = %+v", rowB)
	}
	cb, _ := s.CustomerByID("cust_b")
	if !slices.Equal(cb.WorkloadNamespaces, []string{"team-a", "team-b", "ys-b"}) {
		t.Fatalf("cust_b namespaces = %v, want the hosted one appended", cb.WorkloadNamespaces)
	}
}

// The fail-closed refusals: two tenants cannot hold one hosted namespace or
// one cluster id, one tenant cannot hold two assignments, and a missing
// tenant or an input outside the closed grammars refuses before anything is
// written.
func TestAssignHostedClusterFailsClosed(t *testing.T) {
	s, _ := storeWithSpy("cust_a", "cust_b")
	assignHosted(t, s, "cust_a", "hosted-a", "", "ys-shared")

	// Namespace uniqueness is GLOBAL across the shared physical cluster.
	if _, _, err := s.AssignHostedCluster("cust_b", "hosted-b", "", "ys-shared", OperatorActor()); !errors.Is(err, ErrHostedNamespaceTaken) {
		t.Fatalf("namespace collision err = %v, want ErrHostedNamespaceTaken", err)
	}
	// Cluster ids are global exactly as registered ones are.
	if _, _, err := s.AssignHostedCluster("cust_b", "hosted-a", "", "", OperatorActor()); !errors.Is(err, ErrClusterExists) {
		t.Fatalf("cluster id collision err = %v, want ErrClusterExists", err)
	}
	// One assignment per tenant.
	if _, _, err := s.AssignHostedCluster("cust_a", "hosted-a2", "", "ys-a2", OperatorActor()); !errors.Is(err, ErrHostedClusterExists) {
		t.Fatalf("duplicate assignment err = %v, want ErrHostedClusterExists", err)
	}
	// The refusals changed nothing: cust_b can still take free values.
	if _, _, err := s.AssignHostedCluster("cust_b", "hosted-b", "", "ys-b", OperatorActor()); err != nil {
		t.Fatalf("assign after refusals: %v", err)
	}

	if _, _, err := s.AssignHostedCluster("cust_nope", "", "", "", OperatorActor()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown tenant err = %v, want ErrNotFound", err)
	}
	for _, bad := range []string{"Hosted_X", "-lead", "x!", strings.Repeat("a", 22)} {
		if _, _, err := s.AssignHostedCluster("cust_a", bad, "", "", OperatorActor()); !errors.Is(err, ErrInvalidCluster) {
			t.Errorf("cluster id %q err = %v, want ErrInvalidCluster", bad, err)
		}
	}
	for _, bad := range []string{"Team-A", "bad ns", "a,b", strings.Repeat("a", 64), "-x"} {
		if _, _, err := s.AssignHostedCluster("cust_a", "", "", bad, OperatorActor()); !errors.Is(err, ErrInvalidHostedNamespace) {
			t.Errorf("namespace %q err = %v, want ErrInvalidHostedNamespace", bad, err)
		}
	}
	// Platform-owned names are refused even though they are valid labels:
	// "default" is every unconfigured tenant's fail-closed namespace, and the
	// rest belong to Kubernetes or to the platform itself.
	for _, reserved := range []string{"default", "kube-system", "kube-public", "kube-node-lease", "yscale", "yscale-system"} {
		if _, _, err := s.AssignHostedCluster("cust_a", "", "", reserved, OperatorActor()); !errors.Is(err, ErrInvalidHostedNamespace) {
			t.Errorf("reserved namespace %q err = %v, want ErrInvalidHostedNamespace", reserved, err)
		}
	}
	if _, _, err := s.AssignHostedCluster("cust_a", "", "evil\x00name", "ys-x", OperatorActor()); !errors.Is(err, ErrInvalidCluster) {
		t.Errorf("control-character name err = %v, want ErrInvalidCluster", err)
	}
}

func TestHostedClusterRequiresScopedCredentialAndPreservesExistingNamespace(t *testing.T) {
	s, _ := storeWithSpy("cust_a")
	s.mu.Lock()
	s.customers["cust_a"].WorkloadNamespaces = []string{"team-a"}
	s.mu.Unlock()

	row, token := assignHosted(t, s, "cust_a", "hosted-a", "", "team-a")
	if row.HostedNamespace != "team-a" {
		t.Fatalf("hosted namespace = %q", row.HostedNamespace)
	}
	if !s.HostedClusterRequiresScopedCredential("cust_a", "hosted-a") {
		t.Fatal("hosted row did not require its scoped credential")
	}
	if _, _, err := s.ClaimAgentCluster("cust_a", "hosted-a", false, clusterEpoch); !errors.Is(err, ErrClusterPlatformManaged) {
		t.Fatalf("legacy tenant-token claim err = %v, want ErrClusterPlatformManaged", err)
	}
	if _, _, err := s.ClaimAgentCluster("cust_a", "hosted-a", true, clusterEpoch); err != nil {
		t.Fatalf("scoped credential claim: %v", err)
	}
	if _, _, err := s.AuthClusterCredential(token); err != nil {
		t.Fatalf("hosted credential after claim: %v", err)
	}
	if _, err := s.DeleteHostedCluster("cust_a", "hosted-a", OperatorActor()); err != nil {
		t.Fatalf("delete hosted assignment: %v", err)
	}
	c, err := s.CustomerByID("cust_a")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.WorkloadNamespaces, []string{"team-a"}) {
		t.Fatalf("delete revoked pre-existing namespace grant: %v", c.WorkloadNamespaces)
	}
}

// The tenant may LIST and ROUTE to its hosted row through the existing
// surface, but the tenant mutation paths refuse it with the stable
// platform-managed conflict — an owner is not the row's manager.
func TestHostedClusterRefusesTenantMutation(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	row, token := assignHosted(t, s, "cust_r", "hosted-r", "", "")

	rows, err := s.TenantClustersFor("cust_r", ids["alice"])
	if err != nil || len(rows) != 1 {
		t.Fatalf("TenantClustersFor = (%+v,%v)", rows, err)
	}
	if rows[0].Source != ClusterSourceHosted || rows[0].HostedNamespace != row.HostedNamespace {
		t.Fatalf("tenant view of the hosted row = %+v", rows[0])
	}

	if _, _, _, err := s.RotateTenantClusterCredential("cust_r", ids["alice"], "hosted-r", HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrClusterPlatformManaged) {
		t.Fatalf("tenant rotate err = %v, want ErrClusterPlatformManaged", err)
	}
	if _, err := s.DeleteTenantCluster("cust_r", ids["alice"], "hosted-r", HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrClusterPlatformManaged) {
		t.Fatalf("tenant delete err = %v, want ErrClusterPlatformManaged", err)
	}

	// The refusals mutated nothing: the credential still authenticates and
	// the row is still owned.
	if _, bound, err := s.AuthClusterCredential(token); err != nil || bound != "hosted-r" {
		t.Fatalf("credential after refused mutations = (%q,%v)", bound, err)
	}
	if owner, ok := s.CustomerForClusterID("hosted-r"); !ok || owner != "cust_r" {
		t.Fatalf("owner after refused mutations = (%q,%v)", owner, ok)
	}
}

// The operator lifecycle: rotation replaces the credential and evicts the old
// one's socket; delete releases the row, the id, the namespace reservation
// and the tenant's namespace grant — and only ever sees hosted rows, so a
// tenant-owned cluster cannot be touched through the hosted routes.
func TestHostedClusterOperatorLifecycle(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	s.AddCustomer(&Customer{ID: "cust_b", Token: "tok_b", Plan: "pro"})
	registerCluster(t, s, "cust_r", ids["alice"], "cl-own", "Tenant's own")
	_, oldToken := assignHosted(t, s, "cust_r", "hosted-r", "", "ys-r")

	// The hosted routes answer ErrClusterNotFound for anything that is not a
	// hosted row — the tenant's registered cluster included.
	if _, _, err := s.RotateHostedClusterCredential("cust_r", "cl-own", OperatorActor()); !errors.Is(err, ErrClusterNotFound) {
		t.Fatalf("hosted rotate of a registered row err = %v, want ErrClusterNotFound", err)
	}
	if _, err := s.DeleteHostedCluster("cust_r", "cl-own", OperatorActor()); !errors.Is(err, ErrClusterNotFound) {
		t.Fatalf("hosted delete of a registered row err = %v, want ErrClusterNotFound", err)
	}

	a := connect(s, "agent_h", "cust_r", "hosted-r", 0)
	_, newToken, err := s.RotateHostedClusterCredential("cust_r", "hosted-r", OperatorActor())
	if err != nil {
		t.Fatalf("operator rotate: %v", err)
	}
	if newToken == oldToken {
		t.Fatal("rotation returned the same credential")
	}
	if _, _, err := s.AuthClusterCredential(oldToken); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old credential still authenticates: %v", err)
	}
	if _, bound, err := s.AuthClusterCredential(newToken); err != nil || bound != "hosted-r" {
		t.Fatalf("new credential = (%q,%v), want hosted-r", bound, err)
	}
	select {
	case <-a.Evicted():
	default:
		t.Fatal("operator rotation did not evict the old credential's socket")
	}
	s.RemoveAgent(a.ID)

	// A hosted cluster the tenant's policy names is refused, exactly as the
	// tenant delete would be: the operator does not move tenant-owned
	// boundaries.
	if _, _, _, err := s.SetTenantClusterPolicy("cust_r", ids["alice"], ClusterPolicy{Allow: []string{"hosted-r"}, Auto: ClusterPolicyAutoRequirePin}, HumanActor(ids["alice"], "cust_r")); err != nil {
		t.Fatalf("set policy: %v", err)
	}
	if _, err := s.DeleteHostedCluster("cust_r", "hosted-r", OperatorActor()); !errors.Is(err, ErrClusterNamedByPolicy) {
		t.Fatalf("delete of policy-named hosted cluster err = %v, want ErrClusterNamedByPolicy", err)
	}
	if _, _, _, err := s.SetTenantClusterPolicy("cust_r", ids["alice"], ClusterPolicy{}, HumanActor(ids["alice"], "cust_r")); err != nil {
		t.Fatalf("clear policy: %v", err)
	}

	namespace, err := s.DeleteHostedCluster("cust_r", "hosted-r", OperatorActor())
	if err != nil || namespace != "ys-r" {
		t.Fatalf("operator delete = (%q,%v), want ys-r", namespace, err)
	}
	if _, _, err := s.AuthClusterCredential(newToken); !errors.Is(err, ErrNotFound) {
		t.Fatalf("credential survived unassignment: %v", err)
	}
	if _, ok := s.CustomerForClusterID("hosted-r"); ok {
		t.Fatal("unassigned hosted cluster id still owned")
	}
	c, _ := s.CustomerByID("cust_r")
	if slices.Contains(c.WorkloadNamespaces, "ys-r") {
		t.Fatalf("namespace grant survived unassignment: %v", c.WorkloadNamespaces)
	}
	if _, err := s.DeleteHostedCluster("cust_r", "hosted-r", OperatorActor()); !errors.Is(err, ErrClusterNotFound) {
		t.Fatalf("second delete err = %v, want ErrClusterNotFound", err)
	}

	// The freed namespace and id are reservable again — by another tenant.
	if _, _, err := s.AssignHostedCluster("cust_b", "hosted-r", "", "ys-r", OperatorActor()); err != nil {
		t.Fatalf("reassign freed values to cust_b: %v", err)
	}
}

// A refused durable write publishes NEITHER the cluster nor the namespace
// ownership: no owner index entry, no credential, no namespace reservation,
// no widened authorization set — SetCustomerWorkloadNamespaces' rollback
// contract, held across both grants the assignment makes.
func TestAssignHostedClusterFailedPersistencePublishesNothing(t *testing.T) {
	s := emptyStore()
	fail := &nsFailPersister{err: errors.New("postgres down")}
	s.persist = fail
	s.AddCustomer(&Customer{ID: "cust_a", Token: "tok_a", Plan: "pro"})
	s.AddCustomer(&Customer{ID: "cust_b", Token: "tok_b", Plan: "pro"})

	if _, _, err := s.AssignHostedCluster("cust_a", "hosted-x", "", "ys-x", OperatorActor()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("assign with a refusing backend err = %v, want ErrPersistence", err)
	}
	if owner, ok := s.CustomerForClusterID("hosted-x"); ok {
		t.Fatalf("unpersisted cluster id published to %q", owner)
	}
	c, err := s.CustomerByID("cust_a")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.RegisteredClusters) != 0 || len(c.WorkloadNamespaces) != 0 {
		t.Fatalf("unpersisted assignment published in memory: clusters=%+v namespaces=%v",
			c.RegisteredClusters, c.WorkloadNamespaces)
	}

	// Neither ownership stuck: with the backend healthy again, ANOTHER tenant
	// can take the exact id and namespace the failed assignment named.
	fail.err = nil
	if _, _, err := s.AssignHostedCluster("cust_b", "hosted-x", "", "ys-x", OperatorActor()); err != nil {
		t.Fatalf("assign after failed attempt: %v", err)
	}
}

// The assignment survives a restart through the real load path: credential,
// owner index and the one-owner namespace reservation are all rebuilt from
// the customer document, so the refusals hold with nothing in memory.
func TestHostedClusterPersistsAcrossRestart(t *testing.T) {
	s, spy := storeWithSpy("cust_a", "cust_b")
	row, token := assignHosted(t, s, "cust_a", "hosted-a", "", "ys-a")

	reloaded := emptyStore()
	reloaded.applySnapshot(spy.recordedSnapshot(t))

	cust, bound, err := reloaded.AuthClusterCredential(token)
	if err != nil || cust.ID != "cust_a" || bound != "hosted-a" {
		t.Fatalf("credential after restart = (%v,%q,%v)", cust, bound, err)
	}
	if owner, ok := reloaded.CustomerForClusterID("hosted-a"); !ok || owner != "cust_a" {
		t.Fatalf("owner index after restart = (%q,%v)", owner, ok)
	}
	if _, _, err := reloaded.AssignHostedCluster("cust_b", "hosted-b", "", "ys-a", OperatorActor()); !errors.Is(err, ErrHostedNamespaceTaken) {
		t.Fatalf("namespace reservation after restart err = %v, want ErrHostedNamespaceTaken", err)
	}
	rc, err := reloaded.CustomerByID("cust_a")
	if err != nil {
		t.Fatal(err)
	}
	if got := rc.RegisteredClusters[0]; got.Source != ClusterSourceHosted || got.HostedNamespace != row.HostedNamespace {
		t.Fatalf("hosted row lost fields in the JSON round-trip: %+v", got)
	}
	if !slices.Equal(rc.WorkloadNamespaces, []string{"default", "ys-a"}) {
		t.Fatalf("authorized namespaces after restart = %v", rc.WorkloadNamespaces)
	}
}

// The hosted writes are journaled as OPERATOR actions with safe metadata: the
// action, the cluster id, the reserved namespace on the boundary moves — and
// never a credential, a hash, or the display name.
func TestHostedClusterAudits(t *testing.T) {
	s, spy := storeWithSpy("cust_a")
	_, token := assignHosted(t, s, "cust_a", "hosted-a", "Secret Name", "ys-a")
	_, rotated, err := s.RotateHostedClusterCredential("cust_a", "hosted-a", OperatorActor())
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if _, err := s.DeleteHostedCluster("cust_a", "hosted-a", OperatorActor()); err != nil {
		t.Fatalf("delete: %v", err)
	}

	wantNamespaces := map[string]bool{
		ActionHostedClusterAssign: true, ActionHostedClusterDelete: true,
		ActionHostedClusterRotate: false,
	}
	for action, reason := range map[string]string{
		ActionHostedClusterAssign: ReasonHostedClusterAssigned,
		ActionHostedClusterRotate: ReasonHostedClusterCredentialRotated,
		ActionHostedClusterDelete: ReasonHostedClusterDeleted,
	} {
		rows := spy.eventsWith(action)
		if len(rows) != 1 {
			t.Fatalf("%s rows = %d, want 1", action, len(rows))
		}
		ev := rows[0]
		if ev.TargetKind != TargetCluster || ev.TargetID != "hosted-a" ||
			ev.Outcome != OutcomeAccepted || ev.Detail.Reason != reason || ev.Actor.Kind != ActorOperator {
			t.Errorf("%s row = %+v", action, ev)
		}
		if got := slices.Contains(ev.Detail.Namespaces, "ys-a"); got != wantNamespaces[action] {
			t.Errorf("%s namespaces = %v, want recorded=%v", action, ev.Detail.Namespaces, wantNamespaces[action])
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
