// yscale:proprietary

package state

import "testing"

func TestSetAutomationTenantTemplateCatalogIfRevisionNoopAndAuditOnChange(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_auto", map[string]string{"owner": RoleOwner})
	publisher, _, _, err := s.CreateCatalogPublisher("cust_auto", ids["owner"], "ci")
	if err != nil {
		t.Fatalf("create publisher = %v", err)
	}
	view, err := s.AutomationTenantTemplateCatalogFor("cust_auto", publisher.ID)
	if err != nil {
		t.Fatal(err)
	}
	current := WorkloadTemplateCatalog{Templates: append([]WorkloadTemplate(nil), view.Catalog.Templates...)}
	written, err := s.SetAutomationTenantTemplateCatalogIfRevision("cust_auto", publisher.ID, view.Revision, current)
	if err != nil || !written.Changed {
		t.Fatalf("automation first write = (%+v, %v)", written, err)
	}
	if got := len(spy.eventsWith(ActionTemplateCatalogSet)); got != 1 {
		t.Fatalf("automation first write wrote %d audit rows, want 1", got)
	}
	if _, err := s.SetAutomationTenantTemplateCatalogIfRevision("cust_auto", publisher.ID, written.Revision, current); err != nil {
		t.Fatalf("automation no-op write = %v", err)
	}
	if got := len(spy.eventsWith(ActionTemplateCatalogSet)); got != 1 {
		t.Fatalf("automation no-op write wrote %d audit rows, want 1", got)
	}

	changed := WorkloadTemplateCatalog{Templates: append([]WorkloadTemplate(nil), view.Catalog.Templates...)}
	changed.Templates[0].Title = "publisher-changed"
	written, err = s.SetAutomationTenantTemplateCatalogIfRevision("cust_auto", publisher.ID, written.Revision, changed)
	if err != nil || !written.Changed {
		t.Fatalf("automation change = (%+v, %v)", written, err)
	}
	events := spy.eventsWith(ActionTemplateCatalogSet)
	if len(events) != 2 {
		t.Fatalf("automation change wrote %d audit rows, want 2", len(events))
	}
	ev := events[0]
	if ev.Actor.Kind != ActorCatalogPublisher || ev.Actor.PublisherID != publisher.ID || ev.Actor.CustomerID != "cust_auto" {
		t.Fatalf("automation actor = %+v", ev.Actor)
	}

	reloaded := emptyStore()
	reloaded.applySnapshot(spy.recordedSnapshot(t))
	reloadedView, err := reloaded.AutomationTenantTemplateCatalogFor("cust_auto", publisher.ID)
	if err != nil {
		t.Fatalf("reloaded automation catalog: %v", err)
	}
	if template := reloadedView.Catalog.Template(changed.Templates[0].ID); template == nil || template.Title != "publisher-changed" {
		t.Fatalf("reloaded automation catalog = (%+v)", reloadedView)
	}
}
