package handlers

import (
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// Store.CustomerForClusterID resolves the owner of a connected agent's cluster.
func TestCustomerForClusterID(t *testing.T) {
	store := state.New()
	store.AddAgent(&state.Agent{ID: "ag_a", CustomerID: "cust_a", ClusterID: "cl_aaa"})
	store.AddAgent(&state.Agent{ID: "ag_b", CustomerID: "cust_b", ClusterID: "cl_bbb"})

	if owner, ok := store.CustomerForClusterID("cl_aaa"); !ok || owner != "cust_a" {
		t.Fatalf("cl_aaa -> (%q,%v), want (cust_a,true)", owner, ok)
	}
	if _, ok := store.CustomerForClusterID("cl_unknown"); ok {
		t.Fatalf("unknown cluster should be unowned")
	}
}

// clusterOwner: a connected agent is authoritative; the in-memory first-seen map
// covers the pre-WS window; and a clusterID owned by another tenant is rejected.
func TestClusterOwner_CrossTenantRejected(t *testing.T) {
	store := state.New()
	h := &AgentAuth{Store: store}

	// Pre-WS: no agent yet. First customer to mint claims the clusterID.
	if _, known := h.clusterOwner("cl_x"); known {
		t.Fatal("cl_x should be unknown before first use")
	}
	h.clusterSeen.LoadOrStore("cl_x", "cust_a")
	if owner, known := h.clusterOwner("cl_x"); !known || owner != "cust_a" {
		t.Fatalf("after first-seen, cl_x -> (%q,%v), want (cust_a,true)", owner, known)
	}

	// A different tenant naming cl_x is now detectable as a conflict.
	if owner, known := h.clusterOwner("cl_x"); !known || owner == "cust_b" {
		t.Fatalf("cl_x must remain owned by cust_a, got %q", owner)
	}

	// A connected agent is authoritative and overrides even an absent first-seen.
	store.AddAgent(&state.Agent{ID: "ag_c", CustomerID: "cust_c", ClusterID: "cl_y"})
	if owner, known := h.clusterOwner("cl_y"); !known || owner != "cust_c" {
		t.Fatalf("cl_y -> (%q,%v), want (cust_c,true) from connected agent", owner, known)
	}
}
