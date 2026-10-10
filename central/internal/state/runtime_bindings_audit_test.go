// yscale:proprietary

package state

import (
	"errors"
	"testing"
	"time"
)

func TestRuntimeBindingCustodyAndAuditAreAtomic(t *testing.T) {
	s, spy, ids := auditFixture(t, "cust_rt", map[string]string{"owner": RoleOwner, "member": RoleMember})
	now := time.Now().UTC()
	row := RuntimeBinding{ID: "rtb_1", Key: "API_TOKEN", Name: "API", CredentialCiphertext: "v1.ciphertext", Revision: 1, CreatedAt: now, UpdatedAt: now}
	if _, _, err := s.SetRuntimeBinding("cust_rt", ids["member"], row); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("member set error = %v, want ErrNotAuthorized", err)
	}
	before := len(spy.events())
	summary, role, err := s.SetRuntimeBinding("cust_rt", ids["owner"], row)
	if err != nil || role != RoleOwner || summary.Key != "API_TOKEN" {
		t.Fatalf("owner set = %+v role=%q err=%v", summary, role, err)
	}
	events := spy.eventsWith(ActionRuntimeBindingSet)
	if len(events) != 1 || events[0].TargetID != "rtb_1" || events[0].Detail.Reason != ReasonRuntimeBindingCreated || events[0].Detail.Role != RoleOwner {
		t.Fatalf("runtime binding audit = %+v", events)
	}
	stored, err := s.RuntimeBindingRows("cust_rt")
	if err != nil || len(stored) != 1 || stored[0].CredentialCiphertext != "v1.ciphertext" {
		t.Fatalf("stored runtime binding = %+v err=%v", stored, err)
	}
	stored[0].CredentialCiphertext = "mutated"
	again, _ := s.RuntimeBindingRows("cust_rt")
	if again[0].CredentialCiphertext != "v1.ciphertext" {
		t.Fatal("runtime binding rows were not deep-copied")
	}
	beforeReseed, _ := s.CustomerByID("cust_rt")
	s.AddCustomer(&Customer{ID: "cust_rt", Token: beforeReseed.Token, Plan: beforeReseed.Plan})
	afterReseed, _ := s.RuntimeBindingRows("cust_rt")
	reseededCustomer, _ := s.CustomerByID("cust_rt")
	if len(afterReseed) != 1 || afterReseed[0].CredentialCiphertext != "v1.ciphertext" || reseededCustomer.RuntimeBindingsRevision != 1 {
		t.Fatalf("env re-seed dropped runtime custody: rows=%+v revision=%d", afterReseed, reseededCustomer.RuntimeBindingsRevision)
	}

	spy.appendErr = errors.New("postgres down")
	row.CredentialCiphertext = "v1.next"
	row.Revision = 2
	row.UpdatedAt = now.Add(time.Second)
	if _, _, err := s.SetRuntimeBinding("cust_rt", ids["owner"], row); !errors.Is(err, ErrPersistence) {
		t.Fatalf("refused rotation error = %v, want ErrPersistence", err)
	}
	after, _ := s.RuntimeBindingRows("cust_rt")
	if after[0].CredentialCiphertext != "v1.ciphertext" || after[0].Revision != 1 {
		t.Fatalf("refused audit still changed custody: %+v", after[0])
	}
	if got := len(spy.events()); got != before+1 {
		t.Fatalf("refused append recorded %d new rows", got-before-1)
	}
}
