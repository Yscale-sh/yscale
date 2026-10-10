// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// namespaceAccounts wires the one signed-in human "alice" over a store the
// caller has already seeded, so each test below differs only in the tenant
// configuration it is asking about.
func namespaceAccounts(t *testing.T, store *state.Store) *Accounts {
	t.Helper()
	return &Accounts{
		Store:    store,
		Resolver: idProvider(t, map[string]string{"human_alice": "alice"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}
}

// A console picks the namespace to submit into from this field, and central
// pins an unqualified submission to the FIRST authorized one. Order is
// therefore part of the answer, not a detail: sort it and the console offers a
// different default than the one the submission gate will apply.
func TestAccountTenantsCarryConfiguredNamespacesInOrder(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro",
		MaxConcurrentBursts: 5, MaxHourlyUSD: 10,
		WorkloadNamespaces: []string{"ml-team-b", "jobs", "ml-team-a"}})
	alice, err := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := store.AddTenantMembership(alice.ID, "cust_alice", state.RoleOwner); err != nil {
		t.Fatalf("membership: %v", err)
	}

	rec := getAccount(namespaceAccounts(t, store), "human_alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	res := decodeAccount(t, rec)
	if len(res.Tenants) != 1 {
		t.Fatalf("tenants = %+v, want exactly cust_alice", res.Tenants)
	}
	want := []string{"ml-team-b", "jobs", "ml-team-a"}
	if got := res.Tenants[0].WorkloadNamespaces; !slices.Equal(got, want) {
		t.Errorf("workload_namespaces = %v, want %v (configured order)", got, want)
	}
	// The rest of the summary is untouched by the new field.
	if res.Tenants[0].CustomerID != "cust_alice" || res.Tenants[0].Role != state.RoleOwner ||
		res.Tenants[0].Plan != "pro" || res.Tenants[0].Limits.MaxConcurrentBursts != 5 {
		t.Errorf("tenant summary = %+v", res.Tenants[0])
	}
}

// A tenant provisioned before namespaces existed has none stored, but its
// submissions are still judged — against the fail-closed set. The field has to
// report that effective set. null or [] would tell a console the tenant may
// submit nowhere, so it would offer no namespace at all for a tenant that can
// in fact submit to "default".
func TestAccountTenantsReportTheEffectiveNamespaceDefault(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_legacy", Token: "ysk_legacy", Plan: "free"})
	alice, err := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := store.AddTenantMembership(alice.ID, "cust_legacy", state.RoleOwner); err != nil {
		t.Fatalf("membership: %v", err)
	}

	rec := getAccount(namespaceAccounts(t, store), "human_alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	res := decodeAccount(t, rec)
	if len(res.Tenants) != 1 {
		t.Fatalf("tenants = %+v, want exactly cust_legacy", res.Tenants)
	}
	if got := res.Tenants[0].WorkloadNamespaces; !slices.Equal(got, []string{"default"}) {
		t.Errorf("workload_namespaces = %v, want [default]", got)
	}
	// Decoding hides the difference between [], null and a missing key, and a
	// console reading raw JSON sees all three differently.
	if body := rec.Body.String(); !strings.Contains(body, `"workload_namespaces":["default"]`) {
		t.Errorf("body does not render the effective set as a list: %s", body)
	}
}

// The field must not become a way to see a tenant the response otherwise
// refuses to name. A revoked tenant and a tenant this human never joined are
// both absent entirely — namespaces included.
func TestAccountNamespacesNeverSurfaceForeignOrRevokedTenants(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro",
		WorkloadNamespaces: []string{"alice-ns"}})
	store.AddCustomer(&state.Customer{ID: "cust_gone", Token: "ysk_gone", Plan: "pro",
		WorkloadNamespaces: []string{"revoked-ns"}})
	store.AddCustomer(&state.Customer{ID: "cust_bob", Token: "ysk_bob", Plan: "pro",
		WorkloadNamespaces: []string{"bob-ns"}})

	alice, err := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	for _, id := range []string{"cust_alice", "cust_gone"} {
		if _, err := store.AddTenantMembership(alice.ID, id, state.RoleOwner); err != nil {
			t.Fatalf("membership on %s: %v", id, err)
		}
	}
	if err := store.RevokeCustomer("cust_gone"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}

	rec := getAccount(namespaceAccounts(t, store), "human_alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	res := decodeAccount(t, rec)
	if len(res.Tenants) != 1 || res.Tenants[0].CustomerID != "cust_alice" {
		t.Fatalf("tenants = %+v, want exactly cust_alice", res.Tenants)
	}
	if got := res.Tenants[0].WorkloadNamespaces; !slices.Equal(got, []string{"alice-ns"}) {
		t.Errorf("workload_namespaces = %v, want [alice-ns]", got)
	}
	body := rec.Body.String()
	for _, forbidden := range []string{"revoked-ns", "bob-ns", "cust_gone", "cust_bob"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("account response leaked %q: %s", forbidden, body)
		}
	}
}

// The response is a value a caller owns. Writing through the slice it got back
// must not reach the store, or one handler rewriting its own copy would change
// what every later submission is authorized against.
func TestAccountNamespaceSliceIsDetachedFromTheStore(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro",
		WorkloadNamespaces: []string{"jobs", "batch"}})
	alice, err := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := store.AddTenantMembership(alice.ID, "cust_alice", state.RoleOwner); err != nil {
		t.Fatalf("membership: %v", err)
	}
	accounts := namespaceAccounts(t, store)

	summaries, err := accounts.tenantsFor(context.Background(), alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 {
		t.Fatalf("tenantsFor = %+v, want exactly cust_alice", summaries)
	}
	summaries[0].WorkloadNamespaces[0] = "kube-system"
	summaries[0].WorkloadNamespaces = append(summaries[0].WorkloadNamespaces, "smuggled")

	cust, err := store.CustomerByID("cust_alice")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if !slices.Equal(cust.WorkloadNamespaces, []string{"jobs", "batch"}) {
		t.Fatalf("store namespaces = %v, want [jobs batch] — the response mutated store state",
			cust.WorkloadNamespaces)
	}
	// And the next caller still sees the tenant's real set.
	again, err := accounts.tenantsFor(context.Background(), alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || !slices.Equal(again[0].WorkloadNamespaces, []string{"jobs", "batch"}) {
		t.Errorf("second read = %+v, want [jobs batch]", again)
	}
}

// The fail-closed default is minted per response for the same reason: a shared
// backing array would let one tenant's summary rewrite another's.
func TestAccountNamespaceDefaultIsNotSharedAcrossTenants(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_one", Token: "ysk_one", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_two", Token: "ysk_two", Plan: "pro"})
	alice, err := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	for _, id := range []string{"cust_one", "cust_two"} {
		if _, err := store.AddTenantMembership(alice.ID, id, state.RoleOwner); err != nil {
			t.Fatalf("membership on %s: %v", id, err)
		}
	}
	accounts := namespaceAccounts(t, store)

	summaries, err := accounts.tenantsFor(context.Background(), alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 2 {
		t.Fatalf("tenantsFor = %+v, want two tenants", summaries)
	}
	summaries[0].WorkloadNamespaces[0] = "kube-system"
	if got := summaries[1].WorkloadNamespaces; !slices.Equal(got, []string{"default"}) {
		t.Errorf("second tenant namespaces = %v, want [default]", got)
	}
}

// A field the console cannot decode is a field it will not use. This pins the
// wire name and shape independently of the Go struct.
func TestAccountNamespacesRenderUnderTheDocumentedKey(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro",
		WorkloadNamespaces: []string{"jobs", "batch"}})
	alice, err := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := store.AddTenantMembership(alice.ID, "cust_alice", state.RoleOwner); err != nil {
		t.Fatalf("membership: %v", err)
	}

	rec := getAccount(namespaceAccounts(t, store), "human_alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var wire struct {
		Tenants []map[string]json.RawMessage `json:"tenants"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &wire); err != nil {
		t.Fatalf("decode: %v (body %q)", err, rec.Body.String())
	}
	if len(wire.Tenants) != 1 {
		t.Fatalf("tenants = %d, want 1", len(wire.Tenants))
	}
	raw, ok := wire.Tenants[0]["workload_namespaces"]
	if !ok {
		t.Fatalf("no workload_namespaces key in %v", wire.Tenants[0])
	}
	if string(raw) != `["jobs","batch"]` {
		t.Errorf("workload_namespaces = %s, want [\"jobs\",\"batch\"]", raw)
	}
	// The fields the console already depends on are still there.
	for _, key := range []string{"customer_id", "role", "plan", "limits"} {
		if _, ok := wire.Tenants[0][key]; !ok {
			t.Errorf("summary lost %q: %v", key, wire.Tenants[0])
		}
	}
}
