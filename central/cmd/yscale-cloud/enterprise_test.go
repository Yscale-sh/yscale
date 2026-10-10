// yscale:proprietary

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/decider"
	"github.com/yscale-sh/yscale/central/internal/factoryclient"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
)

type pollerFactoryFake struct {
	fabric factoryclient.Fabric
	err    error
}

func TestStripeCheckoutActivationIsExplicitTestModeAndComplete(t *testing.T) {
	for _, key := range []string{
		"BILLING_CHECKOUT_ENABLED", "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "STRIPE_ACCOUNT_ID",
		"BILLING_CHECKOUT_SUCCESS_URL", "BILLING_CHECKOUT_CANCEL_URL",
		"BILLING_CHECKOUT_MIN_MICRO_USD", "BILLING_CHECKOUT_MAX_MICRO_USD",
	} {
		t.Setenv(key, "")
	}
	if gateway, minimum, maximum, err := stripeCheckoutFromEnv(billing.NewStore(nil, false)); err != nil || gateway != nil || minimum != 0 || maximum != 0 {
		t.Fatalf("disabled checkout = gateway=%v min=%d max=%d err=%v", gateway, minimum, maximum, err)
	}
	t.Setenv("BILLING_CHECKOUT_ENABLED", "true")
	if _, _, _, err := stripeCheckoutFromEnv(nil); err == nil {
		t.Fatal("checkout enabled without billing store")
	}
	store := billing.NewStore(nil, false)
	if _, _, _, err := stripeCheckoutConfigFromEnv(false); err == nil {
		t.Fatal("checkout enabled with incomplete Stripe configuration")
	}
	t.Setenv("STRIPE_SECRET_KEY", "sk_test_safe")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_test_safe")
	t.Setenv("STRIPE_ACCOUNT_ID", "acct_test")
	t.Setenv("BILLING_CHECKOUT_SUCCESS_URL", "https://app.example.test/billing/success")
	t.Setenv("BILLING_CHECKOUT_CANCEL_URL", "https://app.example.test/billing/cancel")
	if _, _, _, err := stripeCheckoutFromEnv(store); err == nil {
		t.Fatal("checkout activated against an unverified schema")
	}
	gateway, minimum, maximum, err := stripeCheckoutConfigFromEnv(false)
	if err != nil || gateway == nil || minimum != defaultStripeCheckoutMinMicroUSD || maximum != defaultStripeCheckoutMaxMicroUSD {
		t.Fatalf("configured checkout = gateway=%v min=%d max=%d err=%v", gateway, minimum, maximum, err)
	}
	if _, _, _, err := stripeCheckoutConfigFromEnv(true); err == nil {
		t.Fatal("unproven Stripe livemode activated")
	}
	t.Setenv("BILLING_CHECKOUT_MIN_MICRO_USD", "5000001")
	if _, _, _, err := stripeCheckoutConfigFromEnv(false); err == nil {
		t.Fatal("fractional-cent checkout bound accepted")
	}
}

func TestStripeCashReconciliationActivationIsIndependentAndExplicit(t *testing.T) {
	for _, key := range []string{
		"BILLING_STRIPE_RECONCILIATION_ENABLED", "STRIPE_RECONCILIATION_SECRET_KEY",
		"STRIPE_RECONCILIATION_ACCOUNT_ID",
	} {
		t.Setenv(key, "")
	}
	testStore := billing.NewStore(nil, false)
	if provider, err := stripeCashProviderFromEnv(testStore, nil); err != nil || provider != nil {
		t.Fatalf("disabled cash reconciliation = provider=%v err=%v", provider, err)
	}

	checkout, err := billing.NewStripeCashProvider(billing.StripeCashConfig{
		SecretKey: "sk_test_safe", AccountID: "acct_checkout", LiveMode: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if provider, err := stripeCashProviderFromEnv(testStore, checkout); err != nil || provider != checkout {
		t.Fatalf("checkout cash provider = %v err=%v, want existing gateway", provider, err)
	}

	t.Setenv("BILLING_STRIPE_RECONCILIATION_ENABLED", "true")
	if _, err := stripeCashProviderFromEnv(nil, nil); err == nil {
		t.Fatal("cash reconciliation activated without billing store")
	}
	if _, err := stripeCashProviderFromEnv(testStore, nil); err == nil {
		t.Fatal("cash reconciliation activated with incomplete Stripe configuration")
	}
	t.Setenv("STRIPE_RECONCILIATION_SECRET_KEY", "sk_test_safe")
	t.Setenv("STRIPE_RECONCILIATION_ACCOUNT_ID", "acct_test")
	if provider, err := stripeCashProviderFromEnv(testStore, nil); err != nil || provider == nil {
		t.Fatalf("configured cash reconciliation = provider=%v err=%v", provider, err)
	}
	if _, err := stripeCashProviderFromEnv(billing.NewStore(nil, true), nil); err == nil {
		t.Fatal("unproven Stripe reconciliation livemode activated")
	}
}

func TestStripeWebhookRouteIsAbsentWithoutCheckoutActivation(t *testing.T) {
	for _, key := range []string{
		"BILLING_CHECKOUT_ENABLED", "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "STRIPE_ACCOUNT_ID",
		"BILLING_CHECKOUT_SUCCESS_URL", "BILLING_CHECKOUT_CANCEL_URL",
		"BILLING_CHECKOUT_MIN_MICRO_USD", "BILLING_CHECKOUT_MAX_MICRO_USD",
	} {
		t.Setenv(key, "")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	disabled := http.NewServeMux()
	registerAccountRoutes(disabled, state.New(), nil, handlers.NoopReconciler{}, log, billing.NewStore(nil, false))
	req := httptest.NewRequest(http.MethodPost, "/v1/billing/webhooks/stripe", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	disabled.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("disabled webhook route = %d", rec.Code)
	}

}

func (f pollerFactoryFake) EnsureFabric(context.Context, string, string) error { return nil }
func (f pollerFactoryFake) GetFabric(context.Context, string) (factoryclient.Fabric, error) {
	return f.fabric, f.err
}
func (f pollerFactoryFake) DeleteFabric(context.Context, string) error { return nil }

// A ready fabric stops the provisioning signal (Provisioning → false) and
// attaches a keyless factory mesh: central holds no box credential, so every
// mesh call for the tenant routes through the factory.
func TestFabricPollerReadyAttachesKeylessFactoryMesh(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_factory", Token: "tok_factory", Plan: "pro"})
	poller := newFabricPoller(store, pollerFactoryFake{fabric: factoryclient.Fabric{
		Status: "ready", LoginServer: "https://box.example", User: "acme", BackendID: "linode-9",
	}}, slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	var reconciled []string
	poller.attach = func(_ context.Context, customerID string) error {
		reconciled = append(reconciled, customerID)
		return nil
	}
	poller.Track("cust_factory")
	if !poller.Provisioning("cust_factory") {
		t.Fatal("tracked fabric should report provisioning before poll")
	}
	poller.poll(context.Background())

	customer, err := store.CustomerByID("cust_factory")
	if err != nil {
		t.Fatal(err)
	}
	want := state.MeshEndpoint{Provider: state.MeshProviderFactory, LoginServer: "https://box.example", User: "acme", BackendID: "linode-9"}
	if customer.Mesh == nil || *customer.Mesh != want {
		t.Fatalf("attached mesh = %+v, want %+v", customer.Mesh, want)
	}
	if poller.Provisioning("cust_factory") {
		t.Fatal("ready fabric remained in flight — provisioning signal should clear")
	}
	if len(reconciled) != 1 || reconciled[0] != "cust_factory" {
		t.Fatalf("attach reconciles = %v, want one durable attach reconcile", reconciled)
	}
}

func TestFabricPollerDoesNotAttachAnInsecureEndpoint(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_factory", Token: "tok_factory", Plan: "pro"})
	poller := newFabricPoller(store, pollerFactoryFake{fabric: factoryclient.Fabric{
		Status: "ready", LoginServer: "http://box.example", User: "acme",
	}}, slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	poller.Track("cust_factory")
	poller.poll(context.Background())
	customer, _ := store.CustomerByID("cust_factory")
	if customer.Mesh != nil {
		t.Fatalf("attached an insecure endpoint: %+v", customer.Mesh)
	}
	if !poller.Provisioning("cust_factory") {
		t.Fatal("unattached tenant should stay tracked for a retry")
	}
}

// A just-requested fabric can be absent at the factory for a moment; it must
// stay tracked through the grace window, while a startup-discovered tenant
// with no fabric drops out on the first poll.
func TestFabricPollerAbsentGraceOnlyForRequestedTenants(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_new", Token: "tok_new", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_old", Token: "tok_old", Plan: "pro"})
	poller := newFabricPoller(store, pollerFactoryFake{err: factoryclient.ErrFabricAbsent},
		slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	poller.Track("cust_new")
	poller.discover("cust_old")
	poller.poll(context.Background())
	if !poller.Provisioning("cust_new") {
		t.Fatal("requested tenant dropped on a transient absent")
	}
	if poller.Provisioning("cust_old") {
		t.Fatal("discovered tenant with no fabric should drop out")
	}
}

// A delayed ready poll must not overwrite a mesh attached meanwhile (for
// example an env-attached box).
func TestFabricPollerNeverOverwritesAnExistingMesh(t *testing.T) {
	store := state.New()
	envMesh := &state.MeshEndpoint{Provider: "headscale", LoginServer: "https://env.example", APIKey: "k", User: "env"}
	store.AddCustomer(&state.Customer{ID: "cust_env", Token: "tok_env", Plan: "pro"})
	poller := newFabricPoller(store, pollerFactoryFake{fabric: factoryclient.Fabric{
		Status: "ready", LoginServer: "https://box.example", User: "acme",
	}}, slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	poller.Track("cust_env")
	if err := store.SetCustomerMesh("cust_env", envMesh); err != nil {
		t.Fatal(err)
	}
	poller.poll(context.Background())
	customer, _ := store.CustomerByID("cust_env")
	if customer.Mesh == nil || customer.Mesh.LoginServer != "https://env.example" {
		t.Fatalf("mesh = %+v, want the env attachment kept", customer.Mesh)
	}
}

// A restart empties the in-flight set; Start re-tracks tenants without a mesh.
func TestFabricPollerStartRediscoversUnattachedTenants(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_waiting", Token: "tok_waiting", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_attached", Token: "tok_attached", Plan: "pro",
		Mesh: &state.MeshEndpoint{Provider: state.MeshProviderFactory, LoginServer: "https://b.example", User: "u"}})
	poller := newFabricPoller(store, pollerFactoryFake{err: factoryclient.ErrFabricProvisioning},
		slog.New(slog.NewTextHandler(io.Discard, nil)), time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller.Start(ctx)
	if !poller.Provisioning("cust_waiting") {
		t.Fatal("unattached tenant was not re-tracked at start")
	}
	if poller.Provisioning("cust_attached") {
		t.Fatal("tenant with a mesh must not be tracked")
	}
}

// A still-provisioning fabric keeps the typed-503 signal live across a tick.
func TestFabricPollerKeepsSignalWhileProvisioning(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_factory", Token: "tok_factory", Plan: "pro"})
	poller := newFabricPoller(store, pollerFactoryFake{err: factoryclient.ErrFabricProvisioning},
		slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	poller.Track("cust_factory")
	poller.poll(context.Background())
	if !poller.Provisioning("cust_factory") {
		t.Fatal("provisioning fabric should keep reporting provisioning")
	}
}

func TestAttachMeshBoxesRejectsSharedTailscale(t *testing.T) {
	dec := decider.New(decider.Config{
		TSOAuthClientID:     "client-id",
		TSOAuthClientSecret: "client-secret",
		TSTailnet:           "example.com",
	})

	want := "managed-mesh release must not have TS_OAUTH_* set"
	assertPanicsWith(t, want, func() {
		attachMeshBoxes(state.New(), slog.New(slog.NewTextHandler(io.Discard, nil)),
			handlers.NoopReconciler{}, dec, &handlers.AgentAuth{}, nil)
	})
}

func TestAttachMeshBoxesRejectsUnexpectedReconciler(t *testing.T) {
	assertPanicsWith(t, "expected *meshpolicy.Reconciler", func() {
		attachMeshBoxes(state.New(), slog.New(slog.NewTextHandler(io.Discard, nil)),
			handlers.NoopReconciler{}, decider.New(decider.Config{}), &handlers.AgentAuth{}, nil)
	})
}

func assertPanicsWith(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		got := recover()
		if got == nil {
			t.Fatalf("did not panic; want panic containing %q", want)
		}
		if !strings.Contains(got.(string), want) {
			t.Fatalf("panic = %q, want substring %q", got, want)
		}
	}()
	fn()
}

// The human tenant surface is one hook and one credential: every route on it
// appears together when Yscale ID is configured, and none of it exists when it
// is not. This drives the real registration rather than a hand-written mux, so
// a route added to the handler and forgotten here fails.
func TestAccountRoutesRegisterTheHumanTenantSurface(t *testing.T) {
	routes := []struct{ method, path string }{
		{http.MethodGet, "/v1/account"},
		{http.MethodGet, "/v1/tenants/cust_x/members"},
		{http.MethodPost, "/v1/tenants/cust_x/members"},
		{http.MethodPatch, "/v1/tenants/cust_x/members/acct_y"},
		{http.MethodDelete, "/v1/tenants/cust_x/members/acct_y"},
		{http.MethodPost, "/v1/tenants/cust_x/placement-preview"},
		{http.MethodGet, "/v1/tenants/cust_x/usage"},
		{http.MethodGet, "/v1/tenants/cust_x/hosted-capacity"},
		{http.MethodPost, "/v1/tenants/cust_x/hosted-capacity"},
		{http.MethodGet, "/v1/tenants/cust_x/clusters"},
		{http.MethodPost, "/v1/tenants/cust_x/clusters"},
		{http.MethodPost, "/v1/tenants/cust_x/clusters/cl_y/credential"},
		{http.MethodDelete, "/v1/tenants/cust_x/clusters/cl_y"},
		{http.MethodGet, "/v1/tenants/cust_x/gitops/sources"},
		{http.MethodPut, "/v1/tenants/cust_x/gitops/sources"},
		{http.MethodGet, "/v1/tenants/cust_x/catalog-publishers"},
		{http.MethodPost, "/v1/tenants/cust_x/catalog-publishers"},
		{http.MethodPost, "/v1/tenants/cust_x/catalog-publishers/pub_y/credential"},
		{http.MethodDelete, "/v1/tenants/cust_x/catalog-publishers/pub_y"},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Configured: the routes are there, and answer the credential. The resolver
	// address is never dialed — a request with no Bearer token is refused before
	// the identity provider is asked anything.
	t.Setenv("YSCALE_ID_ISSUER", "https://id.yscale.sh")
	t.Setenv("YSCALE_ID_INTERNAL_URL", "http://127.0.0.1:1")
	configured := http.NewServeMux()
	registerAccountRoutes(configured, state.New(), nil, handlers.NoopReconciler{}, log)
	for _, route := range routes {
		rec := httptest.NewRecorder()
		configured.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`)))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s on a configured deployment = %d, want 401 (an unregistered route would 404)",
				route.method, route.path, rec.Code)
		}
	}
	// Unconfigured: the same requests 404, exactly as an unset YSCALE_ADMIN_TOKEN
	// disables the operator routes. Multi-tenancy is not half-on.
	t.Setenv("YSCALE_ID_INTERNAL_URL", "")
	disabled := http.NewServeMux()
	registerAccountRoutes(disabled, state.New(), nil, handlers.NoopReconciler{}, log)
	for _, route := range routes {
		rec := httptest.NewRecorder()
		disabled.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`)))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s without Yscale ID = %d, want 404", route.method, route.path, rec.Code)
		}
	}
}

func TestCatalogAutomationRoutesRegisterAtExactPaths(t *testing.T) {
	t.Setenv("YSCALE_ID_INTERNAL_URL", "")
	mux := http.NewServeMux()
	registerAccountRoutes(mux, state.New(), nil, handlers.NoopReconciler{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, "/v1/automation/tenants/cust_x/templates", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s exact automation route = %d, want 401 from dedicated auth", method, rec.Code)
		}
	}
	for _, path := range []string{"/v1/automation/tenant/cust_x/templates", "/v1/tenants/cust_x/automation/templates"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("inexact automation path %s = %d, want 404", path, rec.Code)
		}
	}
}

func TestBillingRoutesAreAbsentWithoutExplicitBillingDatabase(t *testing.T) {
	t.Setenv("YSCALE_ID_ISSUER", "https://id.yscale.sh")
	t.Setenv("YSCALE_ID_INTERNAL_URL", "http://127.0.0.1:1")
	mux := http.NewServeMux()
	registerAccountRoutes(mux, state.New(), nil, handlers.NoopReconciler{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/v1/tenants/cust_x/billing"},
		{http.MethodGet, "/v1/tenants/cust_x/billing/statement?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z"},
		{http.MethodGet, "/v1/tenants/cust_x/billing/statement.csv?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z"},
		{http.MethodPost, "/v1/operator/tenants/cust_x/billing/service-credits"},
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s %s without billing DB = %d, want 404", route.method, route.path, rec.Code)
		}
	}
}

func TestBillingRoutesRegisterAtExactHumanAndOperatorPaths(t *testing.T) {
	t.Setenv("YSCALE_ID_ISSUER", "https://id.yscale.sh")
	t.Setenv("YSCALE_ID_INTERNAL_URL", "http://127.0.0.1:1")
	t.Setenv("YSCALE_OPERATOR_SUBJECTS", "operator-subject")
	mux := http.NewServeMux()
	registerAccountRoutes(mux, state.New(), nil, handlers.NoopReconciler{},
		slog.New(slog.NewTextHandler(io.Discard, nil)), billing.NewStore(nil, false))
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/v1/tenants/cust_x/billing"},
		{http.MethodGet, "/v1/tenants/cust_x/billing/statement?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z"},
		{http.MethodGet, "/v1/tenants/cust_x/billing/statement.csv?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z"},
		{http.MethodPost, "/v1/operator/tenants/cust_x/billing/service-credits"},
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s configured = %d, want 401 from human auth", route.method, route.path, rec.Code)
		}
	}
}

// The hosted-capacity operator surface rides the SAME credential and the same
// hook as the rest of the admin routes: the queue read and all three
// mutators appear together behind YSCALE_ADMIN_TOKEN, and none of them exists
// when it is unset. This drives the real registration, so a handler added and
// forgotten here fails.
func TestAdminRoutesRegisterHostedClusterSurface(t *testing.T) {
	routes := []struct{ method, path string }{
		{http.MethodGet, "/v1/admin/hosted-capacity/requests"},
		{http.MethodGet, "/v1/admin/hosted-clusters"},
		{http.MethodPost, "/v1/admin/tenants/cust_x/credential"},
		{http.MethodPost, "/v1/admin/tenants/cust_x/hosted-clusters"},
		{http.MethodPost, "/v1/admin/tenants/cust_x/hosted-clusters/hosted-y/credential"},
		{http.MethodDelete, "/v1/admin/tenants/cust_x/hosted-clusters/hosted-y"},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Configured: the routes exist and answer the operator credential — a
	// request with no Bearer token is refused before any handler runs.
	t.Setenv("YSCALE_ADMIN_TOKEN", "adm_test_token")
	configured := http.NewServeMux()
	registerAdminRoutes(configured, state.New(), nil, handlers.NoopReconciler{}, log)
	for _, route := range routes {
		rec := httptest.NewRecorder()
		configured.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`)))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s with an admin token set = %d, want 401 (an unregistered route would 404)",
				route.method, route.path, rec.Code)
		}
	}
	authorized := httptest.NewRequest(http.MethodGet, "/v1/admin/hosted-clusters", nil)
	authorized.Header.Set("Authorization", "Bearer adm_test_token")
	authorizedRec := httptest.NewRecorder()
	configured.ServeHTTP(authorizedRec, authorized)
	if authorizedRec.Code != http.StatusOK || strings.TrimSpace(authorizedRec.Body.String()) != `{"clusters":[]}` {
		t.Fatalf("authorized admin inventory = %d %q, want exact empty safe envelope", authorizedRec.Code, authorizedRec.Body.String())
	}

	// Unconfigured: the same requests 404, like every other admin route.
	t.Setenv("YSCALE_ADMIN_TOKEN", "")
	disabled := http.NewServeMux()
	registerAdminRoutes(disabled, state.New(), nil, handlers.NoopReconciler{}, log)
	for _, route := range routes {
		rec := httptest.NewRecorder()
		disabled.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`)))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s without YSCALE_ADMIN_TOKEN = %d, want 404", route.method, route.path, rec.Code)
		}
	}
}

// Connector-command recovery is registered on BOTH enterprise recovery
// credentials and on neither tenant middleware: the static admin family and
// the browser-operator console. Each appears and disappears with its own
// configuration, exactly like the hosted-cluster routes beside it.
func TestConnectorCommandRecoveryRoutesAreOperatorOnly(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	adminRoutes := []struct{ method, path string }{
		{http.MethodGet, "/v1/admin/tenants/cust_x/connector-commands"},
		{http.MethodPost, "/v1/admin/tenants/cust_x/connector-commands/ccmd_y/requeue"},
	}
	operatorRoutes := []struct{ method, path string }{
		{http.MethodGet, "/v1/operator/tenants/cust_x/connector-commands"},
		{http.MethodPost, "/v1/operator/tenants/cust_x/connector-commands/ccmd_y/requeue"},
	}

	// Admin family configured: the routes exist and demand the operator token.
	t.Setenv("YSCALE_ADMIN_TOKEN", "adm_test_token")
	configured := http.NewServeMux()
	registerAdminRoutes(configured, state.New(), nil, handlers.NoopReconciler{}, log)
	for _, route := range adminRoutes {
		rec := httptest.NewRecorder()
		configured.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`)))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s with an admin token set = %d, want 401 (an unregistered route would 404)",
				route.method, route.path, rec.Code)
		}
	}

	// Unset: gone with the rest of the admin family.
	t.Setenv("YSCALE_ADMIN_TOKEN", "")
	disabled := http.NewServeMux()
	registerAdminRoutes(disabled, state.New(), nil, handlers.NoopReconciler{}, log)
	for _, route := range adminRoutes {
		rec := httptest.NewRecorder()
		disabled.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`)))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s without YSCALE_ADMIN_TOKEN = %d, want 404", route.method, route.path, rec.Code)
		}
	}

	// Operator console configured: same pair, human credential, independent of
	// the admin token above (still unset here).
	t.Setenv("YSCALE_ID_ISSUER", "https://id.yscale.sh")
	t.Setenv("YSCALE_ID_INTERNAL_URL", "http://127.0.0.1:1")
	t.Setenv("YSCALE_OPERATOR_SUBJECTS", "op_alice")
	console := http.NewServeMux()
	registerAccountRoutes(console, state.New(), nil, handlers.NoopReconciler{}, log)
	for _, route := range operatorRoutes {
		rec := httptest.NewRecorder()
		console.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`)))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s with the console configured = %d, want 401", route.method, route.path, rec.Code)
		}
	}

	// A blank allowlist takes the console's pair away with the rest of it.
	t.Setenv("YSCALE_OPERATOR_SUBJECTS", "")
	unseated := http.NewServeMux()
	registerAccountRoutes(unseated, state.New(), nil, handlers.NoopReconciler{}, log)
	for _, route := range operatorRoutes {
		rec := httptest.NewRecorder()
		unseated.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`)))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s without an operator allowlist = %d, want 404", route.method, route.path, rec.Code)
		}
	}
}

// Self-service signup is a second opt-in on top of the identity config: only
// the exact value YSCALE_SELF_SERVICE_TENANTS=true enables the route, and any
// other spelling leaves it indistinguishable from one that was never
// registered — even on a deployment where the rest of the human surface is up.
func TestSelfServiceTenantRouteIsExactEnvOptIn(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Setenv("YSCALE_ID_ISSUER", "https://id.yscale.sh")
	t.Setenv("YSCALE_ID_INTERNAL_URL", "http://127.0.0.1:1")

	for _, value := range []string{"", "TRUE", "1", " true"} {
		t.Run("disabled_"+value, func(t *testing.T) {
			t.Setenv("YSCALE_SELF_SERVICE_TENANTS", value)
			mux := http.NewServeMux()
			registerAccountRoutes(mux, state.New(), nil, handlers.NoopReconciler{}, log)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/account/tenants", strings.NewReader(`{"name":"Acme"}`)))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 for %q", rec.Code, value)
			}
		})
	}

	t.Setenv("YSCALE_SELF_SERVICE_TENANTS", "true")
	mux := http.NewServeMux()
	registerAccountRoutes(mux, state.New(), nil, handlers.NoopReconciler{}, log)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/account/tenants", strings.NewReader(`{"name":"Acme"}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("enabled route status = %d, want 401 before dialing identity provider", rec.Code)
	}
}

// The browser-operator surface is a third credential on its own switch: the
// routes exist only when the identity provider, the issuer AND a non-blank
// YSCALE_OPERATOR_SUBJECTS allowlist are all configured — each missing piece
// leaves them indistinguishable from unregistered ones (404), exactly as an
// unset YSCALE_ADMIN_TOKEN disables the admin family. Configured, they answer
// the human credential (401 before the identity provider is asked anything).
// This drives the real registration, so a route added to the handler and
// forgotten here fails.
func TestOperatorRoutesNeedIdentityAndAllowlist(t *testing.T) {
	routes := []struct{ method, path string }{
		{http.MethodGet, "/v1/operator/tenants"},
		{http.MethodPatch, "/v1/operator/tenants/cust_x/limits"},
		{http.MethodGet, "/v1/operator/hosted-capacity/requests"},
		{http.MethodGet, "/v1/operator/hosted-clusters"},
		{http.MethodPost, "/v1/operator/tenants/cust_x/hosted-clusters"},
		{http.MethodPost, "/v1/operator/tenants/cust_x/hosted-clusters/hosted-x/credential"},
		{http.MethodDelete, "/v1/operator/tenants/cust_x/hosted-clusters/hosted-x"},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	configured := func(t *testing.T) {
		t.Helper()
		t.Setenv("YSCALE_ID_ISSUER", "https://id.yscale.sh")
		t.Setenv("YSCALE_ID_INTERNAL_URL", "http://127.0.0.1:1")
		t.Setenv("YSCALE_OPERATOR_SUBJECTS", "op_alice")
	}

	// Configured: both routes answer the human credential.
	t.Run("configured", func(t *testing.T) {
		configured(t)
		mux := http.NewServeMux()
		registerAccountRoutes(mux, state.New(), nil, handlers.NoopReconciler{}, log)
		for _, route := range routes {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`)))
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("%s %s configured = %d, want 401 (an unregistered route would 404)",
					route.method, route.path, rec.Code)
			}
		}
	})

	// Each missing piece alone leaves the whole family absent — including an
	// allowlist that is present but blank.
	for name, clear := range map[string]func(t *testing.T){
		"no resolver":    func(t *testing.T) { configured(t); t.Setenv("YSCALE_ID_INTERNAL_URL", "") },
		"no issuer":      func(t *testing.T) { configured(t); t.Setenv("YSCALE_ID_ISSUER", "") },
		"no subjects":    func(t *testing.T) { configured(t); t.Setenv("YSCALE_OPERATOR_SUBJECTS", "") },
		"blank subjects": func(t *testing.T) { configured(t); t.Setenv("YSCALE_OPERATOR_SUBJECTS", " , ,, ") },
	} {
		t.Run(name, func(t *testing.T) {
			clear(t)
			mux := http.NewServeMux()
			registerAccountRoutes(mux, state.New(), nil, handlers.NoopReconciler{}, log)
			for _, route := range routes {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`)))
				if rec.Code != http.StatusNotFound {
					t.Errorf("%s %s with %s = %d, want 404", route.method, route.path, name, rec.Code)
				}
			}
		})
	}
}

// The operator console never weakens the static admin family: with the
// operator surface fully configured, the admin routes still appear and
// disappear with YSCALE_ADMIN_TOKEN alone — and an operator token in the
// browser's place buys nothing on /v1/admin/*.
func TestOperatorSurfaceLeavesAdminAuthContractAlone(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Setenv("YSCALE_ID_ISSUER", "https://id.yscale.sh")
	t.Setenv("YSCALE_ID_INTERNAL_URL", "http://127.0.0.1:1")
	t.Setenv("YSCALE_OPERATOR_SUBJECTS", "op_alice")

	// Admin token set: the admin route still demands IT, refusing a request
	// that carries no credential at all.
	t.Setenv("YSCALE_ADMIN_TOKEN", "adm_static")
	mux := http.NewServeMux()
	registerAdminRoutes(mux, state.New(), nil, handlers.NoopReconciler{}, log)
	adminPaths := []struct{ method, path string }{
		{http.MethodGet, "/v1/admin/hosted-capacity/requests"},
		{http.MethodGet, "/v1/admin/hosted-clusters"},
		{http.MethodPost, "/v1/admin/tenants/cust_x/hosted-clusters"},
	}
	for _, route := range adminPaths {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without the admin token = %d, want 401", route.method, route.path, rec.Code)
		}
	}

	// Admin token unset: the admin family is gone while the operator family on
	// the SAME deployment stays up behind the human credential.
	t.Setenv("YSCALE_ADMIN_TOKEN", "")
	adminMux := http.NewServeMux()
	registerAdminRoutes(adminMux, state.New(), nil, handlers.NoopReconciler{}, log)
	accountMux := http.NewServeMux()
	registerAccountRoutes(accountMux, state.New(), nil, handlers.NoopReconciler{}, log)
	for _, route := range adminPaths {
		rec := httptest.NewRecorder()
		adminMux.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s without YSCALE_ADMIN_TOKEN = %d, want 404", route.method, route.path, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	accountMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/operator/hosted-capacity/requests", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("operator route without YSCALE_ADMIN_TOKEN = %d, want 401 (independent of the admin family)", rec.Code)
	}
}

// operatorIDProvider is a stand-in Yscale ID for the wiring test: it resolves
// the fixed tokens and refuses everything else, exactly as /userinfo would —
// which is how the static admin token proves useless as a human credential.
func operatorIDProvider(t *testing.T, tokens map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		sub, ok := tokens[token]
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"sub": sub, "email_verified": true})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// The wired operator console end to end against a real userinfo endpoint: a
// seated, existing human fulfills the pending queue and assigns hosted
// capacity; an unseated human and the static admin token are refused; a
// seated subject with no account is refused too — the route never mints one.
func TestOperatorRouteWiringEndToEnd(t *testing.T) {
	const issuer = "https://id.yscale.sh"
	idp := operatorIDProvider(t, map[string]string{
		"tok_alice": "op_alice",
		"tok_bob":   "op_bob",
		"tok_ghost": "op_ghost",
	})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Setenv("YSCALE_ID_ISSUER", issuer)
	t.Setenv("YSCALE_ID_INTERNAL_URL", idp.URL)
	t.Setenv("YSCALE_OPERATOR_SUBJECTS", "op_alice, op_ghost")

	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_e2e", Token: "ysk_e2e", Plan: "pro", Name: "EndToEnd"})
	requester, err := store.UpsertAccount(issuer, "req_human", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := store.AddTenantMembership(requester.ID, "cust_e2e", state.RoleOwner); err != nil {
		t.Fatalf("membership: %v", err)
	}
	if _, _, err := store.RequestTenantHostedCapacity("cust_e2e", requester.ID, state.HumanActor(requester.ID, "cust_e2e")); err != nil {
		t.Fatalf("request hosted capacity: %v", err)
	}
	// The seated human's account exists from a prior sign-in; op_bob exists
	// but holds no seat; op_ghost holds a seat but has no account. Only the
	// first may act.
	if _, err := store.UpsertAccount(issuer, "op_alice", state.AccountProfile{}); err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	mux := http.NewServeMux()
	registerAccountRoutes(mux, store, nil, handlers.NoopReconciler{}, log)

	call := func(method, path, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	if rec := call(http.MethodGet, "/v1/operator/hosted-capacity/requests", "tok_alice"); rec.Code != http.StatusOK {
		t.Fatalf("seated operator listing = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	rec := call(http.MethodPost, "/v1/operator/tenants/cust_e2e/hosted-clusters", "tok_alice")
	if rec.Code != http.StatusCreated {
		t.Fatalf("seated operator assigning = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}
	var res struct {
		TenantID       string `json:"tenant_id"`
		ConnectorToken string `json:"connector_token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode assignment: %v", err)
	}
	if res.TenantID != "cust_e2e" || res.ConnectorToken == "" {
		t.Fatalf("assignment = %+v", res)
	}
	// The assignment is durably the tenant's, and the fulfilled request left
	// the queue this console exists to drain.
	cust, err := store.CustomerByID("cust_e2e")
	if err != nil {
		t.Fatal(err)
	}
	if len(cust.RegisteredClusters) != 1 || cust.RegisteredClusters[0].Source != state.ClusterSourceHosted {
		t.Fatalf("hosted row = %+v", cust.RegisteredClusters)
	}
	if pending := store.PendingHostedCapacityRequests(); len(pending) != 0 {
		t.Fatalf("pending after assign = %+v, want empty", pending)
	}
	// The signed-in human's account is the durable handle the audit row rode;
	// the issuer and subject appear nowhere in the response.
	if strings.Contains(rec.Body.String(), "op_alice") || strings.Contains(rec.Body.String(), issuer) {
		t.Fatalf("assignment response leaks identity: %q", rec.Body.String())
	}

	// The rest of the lifecycle is wired to the same credential: the standing
	// inventory shows the grant, rotation reveals a fresh credential once, and
	// the delete releases the assignment out of both the registry and the
	// inventory. The inventory carries the tenant's safe metadata and never the
	// customer token or a credential.
	inv := call(http.MethodGet, "/v1/operator/hosted-clusters", "tok_alice")
	if inv.Code != http.StatusOK {
		t.Fatalf("inventory = %d, want 200 (body %q)", inv.Code, inv.Body.String())
	}
	var inventory struct {
		Clusters []struct {
			TenantID string `json:"tenant_id"`
			Name     string `json:"name"`
			Plan     string `json:"plan"`
			Cluster  struct {
				ClusterID       string `json:"cluster_id"`
				HostedNamespace string `json:"hosted_namespace"`
			} `json:"cluster"`
		} `json:"clusters"`
	}
	if err := json.Unmarshal(inv.Body.Bytes(), &inventory); err != nil {
		t.Fatalf("decode inventory: %v (body %q)", err, inv.Body.String())
	}
	if len(inventory.Clusters) != 1 || inventory.Clusters[0].TenantID != "cust_e2e" ||
		inventory.Clusters[0].Name != "EndToEnd" || inventory.Clusters[0].Plan != "pro" {
		t.Fatalf("inventory = %+v", inventory.Clusters)
	}
	hostedID := inventory.Clusters[0].Cluster.ClusterID
	if hostedID == "" || inventory.Clusters[0].Cluster.HostedNamespace == "" {
		t.Fatalf("inventory row lost the cluster: %+v", inventory.Clusters[0])
	}
	if strings.Contains(inv.Body.String(), "ysk_e2e") || strings.Contains(inv.Body.String(), res.ConnectorToken) {
		t.Fatalf("inventory leaks credential material: %q", inv.Body.String())
	}

	rot := call(http.MethodPost, "/v1/operator/tenants/cust_e2e/hosted-clusters/"+hostedID+"/credential", "tok_alice")
	if rot.Code != http.StatusOK {
		t.Fatalf("operator rotate = %d, want 200 (body %q)", rot.Code, rot.Body.String())
	}
	var rotated struct {
		ConnectorToken string `json:"connector_token"`
	}
	if err := json.Unmarshal(rot.Body.Bytes(), &rotated); err != nil {
		t.Fatalf("decode rotation: %v", err)
	}
	if rotated.ConnectorToken == "" || rotated.ConnectorToken == res.ConnectorToken {
		t.Fatal("operator rotation did not mint a fresh credential")
	}
	if _, _, err := store.AuthClusterCredential(res.ConnectorToken); err == nil {
		t.Fatal("old credential still authenticates after the wired rotation")
	}

	if rec := call(http.MethodDelete, "/v1/operator/tenants/cust_e2e/hosted-clusters/"+hostedID, "tok_alice"); rec.Code != http.StatusOK {
		t.Fatalf("operator delete = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	cust, err = store.CustomerByID("cust_e2e")
	if err != nil {
		t.Fatal(err)
	}
	if len(cust.RegisteredClusters) != 0 {
		t.Fatalf("hosted row survived the wired delete: %+v", cust.RegisteredClusters)
	}
	inv = call(http.MethodGet, "/v1/operator/hosted-clusters", "tok_alice")
	if got := strings.TrimSpace(inv.Body.String()); got != `{"clusters":[]}` {
		t.Fatalf("inventory after delete = %s, want empty", got)
	}

	if rec := call(http.MethodGet, "/v1/operator/hosted-capacity/requests", "tok_bob"); rec.Code != http.StatusForbidden {
		t.Fatalf("unseated human = %d, want 403 (body %q)", rec.Code, rec.Body.String())
	}
	if rec := call(http.MethodGet, "/v1/operator/hosted-capacity/requests", "tok_ghost"); rec.Code != http.StatusForbidden {
		t.Fatalf("seated subject without account = %d, want 403 (body %q)", rec.Code, rec.Body.String())
	}
	if rec := call(http.MethodGet, "/v1/operator/hosted-capacity/requests", "adm_static"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("static admin token as human credential = %d, want 401", rec.Code)
	}
}
