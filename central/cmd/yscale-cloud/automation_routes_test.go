// yscale:proprietary

package main

import "testing"

// Automation catalog routes must use the catalog-publisher middleware so only a
// pinned publisher token may update or read them.
func TestAutomationCatalogRoutesUseCatalogPublisherAuth(t *testing.T) {
	routes := routeMiddleware(t)
	automationCatalogRoutes := map[string]string{
		"GET /v1/automation/tenants/{tenant_id}/templates": "CatalogPublisherAuth",
		"PUT /v1/automation/tenants/{tenant_id}/templates": "CatalogPublisherAuth",
	}
	for pattern, want := range automationCatalogRoutes {
		got, ok := routes[pattern]
		if !ok {
			t.Fatalf("%s is not wired through any handlers middleware, want handlers.%s", pattern, want)
		}
		if got != want {
			t.Errorf("%s wired through handlers.%s, want handlers.%s", pattern, got, want)
		}
	}
}
