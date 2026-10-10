package tenantadversarial

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestCentralTelemetryPostureIsOperatorOnly is a SOURCE-CONTRACT posture check
// (not a fabricated HTTP denial): central system metrics/logs are operator
// surfaces, so the wiring in central/cmd/yscale-cloud must keep the operator
// /metrics mount at the root AND must never register a tenant-facing metrics
// or system-log route. Tenant-scoped workload logs are a separate authenticated
// data-plane surface covered by the retained workload-log isolation seam.
// Prefer this AST walk over grep — it reads exactly what the mux sees.
func TestCentralTelemetryPostureIsOperatorOnly(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, "central", "cmd", "yscale-cloud")

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", dir, err)
	}

	var patterns []string
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || len(call.Args) < 1 {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if sel.Sel.Name != "Handle" && sel.Sel.Name != "HandleFunc" {
					return true
				}
				recv, ok := sel.X.(*ast.Ident)
				if !ok || recv.Name != "mux" {
					return true
				}
				lit, ok := call.Args[0].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				p, err := strconv.Unquote(lit.Value)
				if err != nil {
					return true
				}
				patterns = append(patterns, p)
				return true
			})
		}
	}

	if len(patterns) == 0 {
		t.Fatalf("no mux.Handle / mux.HandleFunc patterns found under %s — the parser wiring drifted", dir)
	}

	// The operator /metrics mount MUST survive.
	const operatorMetrics = "GET /metrics"
	seen := false
	for _, p := range patterns {
		if p == operatorMetrics {
			seen = true
			break
		}
	}
	if !seen {
		t.Errorf("operator %q mount is gone; central telemetry lost its Prometheus scrape", operatorMetrics)
	}

	for _, p := range patterns {
		route := p
		if i := strings.Index(route, " "); i >= 0 {
			route = route[i+1:]
		}
		lower := strings.ToLower(route)

		// Per-workload logs are authenticated data-plane egress, not central
		// telemetry; their cross-tenant denial is an explicit evidence seam.
		if strings.HasSuffix(route, "/workloads/{id}/logs") {
			continue
		}

		// Any /metrics route other than the operator root mount is a leak.
		if strings.HasSuffix(lower, "/metrics") && route != "/metrics" {
			t.Errorf("tenant-facing metrics route appeared: %q", p)
		}
		// A top-level or tenant-scoped system-log route is a leak.
		if route == "/logs" || route == "/v1/logs" ||
			(strings.Contains(route, "/tenants/") && strings.HasSuffix(route, "/logs")) {
			t.Errorf("system-log route appeared: %q", p)
		}
	}
}
