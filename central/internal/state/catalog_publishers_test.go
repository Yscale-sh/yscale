// yscale:proprietary

package state

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// This wrapper is proprietary: the public export deliberately replaces it
// with ErrNotFound. Keep its durable-auth coverage here, not in shared tests.
func TestCatalogPublisherWrapperUsesDurableAuthority(t *testing.T) {
	for _, wantErr := range []error{nil, ErrNotFound, errors.New("synthetic read failure")} {
		s := New()
		called := false
		s.persist = credentialTestPersister{read: func(_ context.Context, digest string, kind credentialKind) (*Customer, string, error) {
			called = true
			if kind != publisherCredential || digest != HashCatalogPublisherCredential("synthetic-publisher") {
				t.Fatal("publisher wrapper changed credential kind or verifier")
			}
			if wantErr != nil {
				return nil, "", wantErr
			}
			return &Customer{ID: "synthetic-durable-customer"}, "synthetic-publisher-id", nil
		}}
		customer, publisher, err := s.AuthCatalogPublisher("synthetic-publisher")
		if !called || !errors.Is(err, wantErr) {
			t.Fatalf("publisher wrapper bypassed durable authority: called=%v err=%v", called, err)
		}
		if wantErr == nil && (customer == nil || customer.ID != "synthetic-durable-customer" || publisher != "synthetic-publisher-id") {
			t.Fatal("publisher wrapper lost its durable principal binding")
		}
	}
}

func TestCatalogPublisherLifecyclePersistenceAndAutomationAudit(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_pub", map[string]string{"owner": RoleOwner, "member": RoleMember})
	if _, _, _, err := s.CreateCatalogPublisher("cust_pub", ids["member"], "denied"); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("member create error = %v, want not authorized", err)
	}
	publisher, token, _, err := s.CreateCatalogPublisher("cust_pub", ids["owner"], "gitops-prod")
	if err != nil || token == "" || publisher.ID == "" {
		t.Fatalf("create = (%+v, %q, %v)", publisher, token, err)
	}
	if customer, publisherID, err := s.AuthCatalogPublisher(token); err != nil || customer.ID != "cust_pub" || publisherID != publisher.ID {
		t.Fatalf("auth = (%v, %q, %v)", customer, publisherID, err)
	}
	persisted, _ := json.Marshal(spy.recordedSnapshot(t).Customers[0])
	if strings.Contains(string(persisted), token) || !strings.Contains(string(persisted), HashCatalogPublisherCredential(token)) {
		t.Fatalf("persisted publisher secret shape is unsafe: %s", persisted)
	}

	current, err := s.AutomationTenantTemplateCatalogFor("cust_pub", publisher.ID)
	if err != nil {
		t.Fatal(err)
	}
	written, err := s.SetAutomationTenantTemplateCatalogIfRevision("cust_pub", publisher.ID, current.Revision, WorkloadTemplateCatalog{Templates: []WorkloadTemplate{}})
	if err != nil || !written.Changed {
		t.Fatalf("automation write = (%+v, %v)", written, err)
	}
	events := spy.eventsWith(ActionTemplateCatalogSet)
	if len(events) != 1 || events[0].Actor.Kind != ActorCatalogPublisher || events[0].Actor.PublisherID != publisher.ID || events[0].Actor.CustomerID != "cust_pub" {
		t.Fatalf("automation audit actor = %+v", events)
	}
	auditJSON, _ := json.Marshal(events[0])
	if strings.Contains(string(auditJSON), token) || strings.Contains(string(auditJSON), HashCatalogPublisherCredential(token)) {
		t.Fatalf("audit leaked credential material: %s", auditJSON)
	}

	_, rotated, _, err := s.RotateCatalogPublisherCredential("cust_pub", ids["owner"], publisher.ID)
	if err != nil || rotated == token {
		t.Fatalf("rotate = (%q, %v)", rotated, err)
	}
	if _, _, err := s.AuthCatalogPublisher(token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old token after rotate = %v, want not found", err)
	}
	restarted := emptyStore()
	restarted.applySnapshot(spy.recordedSnapshot(t))
	if customer, gotID, err := restarted.AuthCatalogPublisher(rotated); err != nil || customer.ID != "cust_pub" || gotID != publisher.ID {
		t.Fatalf("restarted auth = (%v, %q, %v)", customer, gotID, err)
	}
	if _, err := s.DeleteCatalogPublisher("cust_pub", ids["owner"], publisher.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AuthCatalogPublisher(rotated); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rotated token after revoke = %v, want not found", err)
	}

	for _, action := range []string{ActionCatalogPublisherCreate, ActionCatalogPublisherRotate, ActionCatalogPublisherDelete} {
		events := spy.eventsWith(action)
		if len(events) != 1 {
			t.Fatalf("%s audit rows = %d, want 1", action, len(events))
		}
		ev := events[0]
		if ev.Actor.Kind != ActorHuman || ev.Actor.AccountID != ids["owner"] || ev.Actor.CustomerID != "cust_pub" ||
			ev.TargetKind != TargetCatalogPublisher || ev.TargetID != publisher.ID || ev.Detail.Role != RoleOwner {
			t.Fatalf("%s audit event = %+v", action, ev)
		}
		raw, _ := json.Marshal(ev)
		for _, forbidden := range []string{token, rotated, HashCatalogPublisherCredential(token), HashCatalogPublisherCredential(rotated), "gitops-prod"} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatalf("%s audit leaked %q: %s", action, forbidden, raw)
			}
		}
	}
}

func TestCatalogPublisherCreatePersistenceFailureWritesNoAuditOrState(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_pub_fail", map[string]string{"owner": RoleOwner})
	before := len(spy.events())
	spy.appendErr = errors.New("postgres down")
	if _, _, _, err := s.CreateCatalogPublisher("cust_pub_fail", ids["owner"], "must-not-land"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("create error = %v, want persistence", err)
	}
	if len(spy.events()) != before {
		t.Fatalf("failed create appended %d audit rows", len(spy.events())-before)
	}
	spy.appendErr = nil
	rows, _, err := s.CatalogPublishersFor("cust_pub_fail", ids["owner"])
	if err != nil || len(rows) != 0 {
		t.Fatalf("failed create published rows = %+v err=%v", rows, err)
	}

	publisher, token, _, err := s.CreateCatalogPublisher("cust_pub_fail", ids["owner"], "stable")
	if err != nil {
		t.Fatal(err)
	}
	spy.appendErr = errors.New("postgres down")
	if _, _, _, err := s.RotateCatalogPublisherCredential("cust_pub_fail", ids["owner"], publisher.ID); !errors.Is(err, ErrPersistence) {
		t.Fatalf("rotate error = %v, want persistence", err)
	}
	if got := len(spy.eventsWith(ActionCatalogPublisherRotate)); got != 0 {
		t.Fatalf("failed rotate wrote %d audit rows", got)
	}
	if _, gotID, err := s.AuthCatalogPublisher(token); err != nil || gotID != publisher.ID {
		t.Fatalf("failed rotate revoked old token = (%q, %v)", gotID, err)
	}
	if _, err := s.DeleteCatalogPublisher("cust_pub_fail", ids["owner"], publisher.ID); !errors.Is(err, ErrPersistence) {
		t.Fatalf("delete error = %v, want persistence", err)
	}
	if got := len(spy.eventsWith(ActionCatalogPublisherDelete)); got != 0 {
		t.Fatalf("failed delete wrote %d audit rows", got)
	}
	if _, gotID, err := s.AuthCatalogPublisher(token); err != nil || gotID != publisher.ID {
		t.Fatalf("failed delete revoked token = (%q, %v)", gotID, err)
	}
}
