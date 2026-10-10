package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

// routeMiddleware maps every mux.Handle pattern in package main to the
// handlers.<Middleware> wrapping it. Routes registered without one (health,
// metrics, the account surface) are absent — the auth boundary this test is
// about is the middleware, so a route that names none has none to assert.
//
// Read from the AST rather than from a live mux because main() is not
// decomposable: the wiring only exists inside it, and the wiring IS the
// security property. A route that quietly moves middleware is exactly the
// regression worth failing a build over.
func routeMiddleware(t *testing.T) map[string]string {
	t.Helper()
	packages, err := parser.ParseDir(token.NewFileSet(), ".", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]string{}
	for _, file := range packages["main"].Files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			handle, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || handle.Sel.Name != "Handle" || len(call.Args) != 2 {
				return true
			}
			if receiver, ok := handle.X.(*ast.Ident); !ok || receiver.Name != "mux" {
				return true
			}
			literal, ok := call.Args[0].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			pattern, err := strconv.Unquote(literal.Value)
			if err != nil {
				return true
			}
			wrapper, ok := call.Args[1].(*ast.CallExpr)
			if !ok {
				return true
			}
			middleware, ok := wrapper.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := middleware.X.(*ast.Ident); !ok || pkg.Name != "handlers" {
				return true
			}
			routes[pattern] = middleware.Sel.Name
			return true
		})
	}
	if len(routes) == 0 {
		t.Fatal("found no handlers-wrapped routes — the AST walk stopped matching main's wiring")
	}
	return routes
}

// A connector credential lives inside a customer's cluster, so the routes that
// accept one are a closed set: the two agent routes, and the workload routes a
// connector drives its OWN cluster's work through — submit, status read, the
// two lifecycle callbacks, and cancel. Anything else appearing here means a
// credential that cannot be revoked by a human just gained reach.
func TestConnectorCredentialRoutesAreExactlyTheAgentSurface(t *testing.T) {
	routes := routeMiddleware(t)

	connectorAware := map[string]string{
		"GET /v1/agent/auth-check":         "ConnectorAuth",
		"GET /v1/agent/stream":             "ConnectorAuth",
		"GET /v1/workloads/{id}":           "ConnectorWorkloadAuth",
		"POST /v1/agent/ts-auth-key":       "ConnectorAuth",
		"POST /v1/workloads":               "ConnectorSubmitAuth",
		"POST /v1/workloads/{id}/started":  "ConnectorWorkloadAuth",
		"POST /v1/workloads/{id}/complete": "ConnectorWorkloadAuth",
		"DELETE /v1/workloads/{id}":        "ConnectorWorkloadAuth",
	}
	for pattern, want := range connectorAware {
		got, ok := routes[pattern]
		if !ok {
			t.Errorf("%s is not wired through any handlers middleware, want handlers.%s", pattern, want)
			continue
		}
		if got != want {
			t.Errorf("%s wired through handlers.%s, want handlers.%s", pattern, got, want)
		}
	}
	for pattern, middleware := range routes {
		switch middleware {
		case "ConnectorAuth", "ConnectorSubmitAuth", "ConnectorWorkloadAuth":
		default:
			continue
		}
		if _, ok := connectorAware[pattern]; !ok {
			t.Errorf("%s accepts connector credentials (handlers.%s) but is not a connector route", pattern, middleware)
		}
	}
}

// The read, money and operator surfaces stay on the tenant/human middlewares.
// Auth resolves tenant tokens ONLY, so a connector credential presented to any
// of these authenticates nothing.
func TestTenantAndAdminRoutesStayHumanOnly(t *testing.T) {
	routes := routeMiddleware(t)

	tenantOnly := []string{
		"GET /v1/spend",
		"GET /v1/price/gpu",
		"GET /v1/bursts",
		"GET /v1/connector-commands",
	}
	for _, pattern := range tenantOnly {
		got, ok := routes[pattern]
		if !ok {
			t.Errorf("%s lost its auth middleware entirely", pattern)
			continue
		}
		if got != "Auth" {
			t.Errorf("%s wired through handlers.%s, want handlers.Auth", pattern, got)
		}
	}

	for pattern, middleware := range routes {
		if _, path, found := strings.Cut(pattern, " "); found && strings.HasPrefix(path, "/v1/admin/") && middleware != "AdminAuth" {
			t.Errorf("admin route %s wired through handlers.%s, want handlers.AdminAuth", pattern, middleware)
		}
	}
}

// The connector-command surface a TENANT credential can reach is exactly one
// read. On the OSS and connector path a tenant token IS the cluster's own
// credential, so a mutation here would let a compromised connector re-drive the
// drains and announces central is trying to send it — and journal it as a
// cluster. Requeue lives on the enterprise operator surfaces
// (/v1/admin/tenants/... and /v1/operator/tenants/..., enterprise.go), which
// this build registers elsewhere and this test deliberately does not accept
// here.
func TestTenantConnectorCommandSurfaceIsReadOnly(t *testing.T) {
	routes := routeMiddleware(t)

	got, ok := routes["GET /v1/connector-commands"]
	if !ok {
		t.Fatal("GET /v1/connector-commands lost its auth middleware entirely")
	}
	if got != "Auth" {
		t.Errorf("GET /v1/connector-commands wired through handlers.%s, want handlers.Auth", got)
	}
	for pattern := range routes {
		method, path, found := strings.Cut(pattern, " ")
		if !found || !strings.HasPrefix(path, "/v1/connector-commands") {
			continue
		}
		if method != http.MethodGet {
			t.Errorf("%s is a tenant-reachable connector-command mutation; requeue belongs on the operator surfaces", pattern)
		}
	}
}
