package state

import (
	"testing"
	"time"
)

// The operator inventory is the standing hosted grants and nothing else: rows
// a TENANT registered are invisible, a revoked tenant's assignment is gone the
// moment the grant is revoked, and the order is (tenant id, cluster id) so two
// reads of the same state render identically.
func TestHostedClusterAssignmentsSafeAndDeterministic(t *testing.T) {
	registeredAt := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	revokedAt := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	store := New()
	for _, c := range []*Customer{
		{ID: "cust_b", Token: "ysk_secret_b", Email: "b@acme.com", Plan: "pro", Name: "Bravo",
			RegisteredClusters: []*RegisteredCluster{
				{ClusterID: "hosted-b", Name: "Yscale hosted capacity", Source: ClusterSourceHosted,
					HostedNamespace: "ys-cust-b", CredentialHash: "sha256-secret-b", RegisteredAt: registeredAt},
			}},
		{ID: "cust_a", Token: "ysk_secret_a", Email: "a@acme.com", Plan: "free",
			RegisteredClusters: []*RegisteredCluster{
				// A tenant's OWN cluster is not the operator's inventory.
				{ClusterID: "cl-own", Name: "Their cluster", Source: ClusterSourceRegistered,
					CredentialHash: "sha256-secret-own", RegisteredAt: registeredAt},
				{ClusterID: "hosted-a", Source: ClusterSourceHosted,
					HostedNamespace: "ys-cust-a", CredentialHash: "sha256-secret-a", RegisteredAt: registeredAt},
			}},
		{ID: "cust_revoked", Token: "ysk_secret_rev", Plan: "pro", RevokedAt: &revokedAt,
			RegisteredClusters: []*RegisteredCluster{
				{ClusterID: "hosted-rev", Source: ClusterSourceHosted,
					HostedNamespace: "ys-cust-rev", CredentialHash: "sha256-secret-rev", RegisteredAt: registeredAt},
			}},
		{ID: "cust_none", Token: "ysk_secret_none", Plan: "pro"},
	} {
		store.AddCustomer(c)
	}

	rows := store.HostedClusterAssignments()
	if len(rows) != 2 {
		t.Fatalf("assignments = %+v, want exactly the two live hosted rows", rows)
	}
	if rows[0].TenantID != "cust_a" || rows[0].Cluster.ClusterID != "hosted-a" {
		t.Fatalf("first row = %+v, want cust_a/hosted-a (tenant id ordering)", rows[0])
	}
	if rows[1].TenantID != "cust_b" || rows[1].Cluster.ClusterID != "hosted-b" || rows[1].Name != "Bravo" {
		t.Fatalf("second row = %+v, want cust_b/hosted-b named Bravo", rows[1])
	}
	if rows[0].Plan != "free" || rows[1].Plan != "pro" {
		t.Fatalf("plans = %q, %q, want the tenants' own", rows[0].Plan, rows[1].Plan)
	}
	if rows[0].Name != "" {
		t.Errorf("unnamed tenant name = %q, want empty (a real value, not an error)", rows[0].Name)
	}
	for _, row := range rows {
		if row.TenantID == "cust_revoked" {
			t.Fatalf("revoked tenant is in the inventory: %+v", row)
		}
		if row.Cluster.Source != ClusterSourceHosted {
			t.Fatalf("non-hosted row in the inventory: %+v", row)
		}
		if row.Cluster.HostedNamespace == "" {
			t.Errorf("row %+v lost the reserved namespace", row)
		}
	}

	// Deterministic: a second read of unchanged state is the same slice.
	again := store.HostedClusterAssignments()
	for i := range rows {
		if again[i].TenantID != rows[i].TenantID || again[i].Cluster.ClusterID != rows[i].Cluster.ClusterID {
			t.Fatalf("read %d differs between calls: %+v vs %+v", i, again[i], rows[i])
		}
	}
}

// The rows are a copy-out: mutating what the caller was handed — including the
// timestamp POINTERS the registry row carries — changes nothing durable, so an
// operator console can hold and edit the response without reaching into state.
func TestHostedClusterAssignmentsCannotMutateStore(t *testing.T) {
	registeredAt := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	connectedAt := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	store := New()
	store.AddCustomer(&Customer{ID: "cust_m", Token: "ysk_m", Plan: "pro", Name: "Mutate",
		RegisteredClusters: []*RegisteredCluster{
			{ClusterID: "hosted-m", Name: "Yscale hosted capacity", Source: ClusterSourceHosted,
				HostedNamespace: "ys-cust-m", CredentialHash: "sha256-secret-m",
				RegisteredAt: registeredAt, FirstConnectedAt: &connectedAt, LastConnectedAt: &connectedAt},
		}})

	rows := store.HostedClusterAssignments()
	if len(rows) != 1 {
		t.Fatalf("assignments = %+v, want one", rows)
	}
	rows[0].TenantID = "cust_attacker"
	rows[0].Name = "clobbered"
	rows[0].Plan = "enterprise"
	rows[0].Cluster.ClusterID = "hosted-attacker"
	rows[0].Cluster.Name = "clobbered"
	rows[0].Cluster.HostedNamespace = "kube-system"
	if rows[0].Cluster.FirstConnectedAt != nil {
		*rows[0].Cluster.FirstConnectedAt = time.Unix(0, 0).UTC()
	}
	if rows[0].Cluster.LastConnectedAt != nil {
		*rows[0].Cluster.LastConnectedAt = time.Unix(0, 0).UTC()
	}

	cust, err := store.CustomerByID("cust_m")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if cust.Name != "Mutate" || cust.Plan != "pro" {
		t.Fatalf("customer mutated through the inventory: %+v", cust)
	}
	if len(cust.RegisteredClusters) != 1 {
		t.Fatalf("registry mutated through the inventory: %+v", cust.RegisteredClusters)
	}
	row := cust.RegisteredClusters[0]
	if row.ClusterID != "hosted-m" || row.Name != "Yscale hosted capacity" || row.HostedNamespace != "ys-cust-m" {
		t.Fatalf("registry row mutated through the inventory: %+v", row)
	}
	if row.CredentialHash != "sha256-secret-m" {
		t.Fatalf("credential hash mutated through the inventory: %+v", row)
	}
	if row.FirstConnectedAt == nil || !row.FirstConnectedAt.Equal(connectedAt) {
		t.Fatalf("first_connected_at mutated through a shared pointer: %v", row.FirstConnectedAt)
	}
	if row.LastConnectedAt == nil || !row.LastConnectedAt.Equal(connectedAt) {
		t.Fatalf("last_connected_at mutated through a shared pointer: %v", row.LastConnectedAt)
	}

	// A second read is unaffected by the first caller's edits.
	fresh := store.HostedClusterAssignments()
	if len(fresh) != 1 || fresh[0].TenantID != "cust_m" || fresh[0].Cluster.ClusterID != "hosted-m" {
		t.Fatalf("second read = %+v, want the untouched assignment", fresh)
	}
}

// No assignments anywhere is an empty slice, not nil: the route renders a JSON
// array either way.
func TestHostedClusterAssignmentsEmptyNotNil(t *testing.T) {
	store := New()
	store.AddCustomer(&Customer{ID: "cust_empty", Token: "ysk_empty", Plan: "pro"})
	rows := store.HostedClusterAssignments()
	if rows == nil || len(rows) != 0 {
		t.Fatalf("assignments = %+v, want a non-nil empty slice", rows)
	}
}
