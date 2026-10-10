// yscale:proprietary

package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/decider"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestDefaultRouteCompositionProvisionsTenantFabric(t *testing.T) {
	requested := make(chan string, 1)
	factory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer factory-fixture" {
			t.Error("factory bearer missing")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodPost {
			requested <- r.URL.Path
			w.WriteHeader(http.StatusAccepted)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer factory.Close()
	t.Setenv("FACTORY_URL", factory.URL)
	t.Setenv("FACTORY_BEARER_TOKEN", "factory-fixture")
	t.Setenv("YSCALE_ADMIN_TOKEN", "operator-fixture")
	t.Setenv("YSCALE_ID_INTERNAL_URL", "")
	ctx, cancel := context.WithCancel(context.Background())
	previousContext, previousFactory, previousPoller := backgroundWorkersContext, enterpriseFactory, enterprisePoller
	backgroundWorkersContext = ctx
	t.Cleanup(func() {
		cancel()
		backgroundWorkersContext, enterpriseFactory, enterprisePoller = previousContext, previousFactory, previousPoller
	})
	store := state.New()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reconciler := newPolicyReconciler(store, log)
	mux := http.NewServeMux()
	registerControlPlaneRoutes(mux, store, &handlers.Workloads{Store: store, Log: log}, reconciler,
		decider.New(decider.Config{}), &handlers.AgentAuth{}, nil, log)
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/tenants", strings.NewReader(`{"id":"cust_mesh_order"}`))
	req.Header.Set("Authorization", "Bearer operator-fixture")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("provision status=%d", w.Code)
	}
	select {
	case got := <-requested:
		if got != "/v1/tenants/cust_mesh_order/fabric" {
			t.Fatalf("factory request=%s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tenant handler did not capture the factory initialized by the default build")
	}
}

func TestManagedMeshStartupConfiguration(t *testing.T) {
	for _, tc := range []struct{ name, endpoint, token, oauthID, oauthSecret, want string }{
		{name: "local diagnostic configuration"},
		{name: "operator factory", endpoint: "http://factory.internal:8080", token: "fixture"},
		{name: "TLS factory", endpoint: "https://factory.example.invalid", token: "fixture"},
		{name: "missing token", endpoint: "http://factory.internal:8080", want: "configured together"},
		{name: "blank token", endpoint: "http://factory.internal:8080", token: " ", want: "configured together"},
		{name: "missing URL", token: "fixture", want: "configured together"},
		{name: "relative URL", endpoint: "factory.internal", token: "fixture", want: "absolute http(s)"},
		{name: "wrong scheme", endpoint: "ftp://factory.internal", token: "fixture", want: "absolute http(s)"},
		{name: "URL credentials", endpoint: "https://user:must-not-leak@factory.internal", token: "fixture", want: "without credentials"},
		{name: "URL query", endpoint: "https://factory.internal?secret=must-not-leak", token: "fixture", want: "without credentials"},
		{name: "old OAuth ID only", oauthID: "must-not-leak", want: "TS_OAUTH_CLIENT_ID is not supported"},
		{name: "old OAuth secret only", oauthSecret: "must-not-leak", want: "TS_OAUTH_CLIENT_SECRET is not supported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FACTORY_URL", tc.endpoint)
			t.Setenv("FACTORY_BEARER_TOKEN", tc.token)
			t.Setenv("TS_OAUTH_CLIENT_ID", tc.oauthID)
			t.Setenv("TS_OAUTH_CLIENT_SECRET", tc.oauthSecret)
			// Exercise the hook installed by the ordinary, untagged full build.
			err := validateMeshConfig()
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
			if err != nil && strings.Contains(err.Error(), "must-not-leak") {
				t.Fatal("error leaked credential")
			}
		})
	}
}
