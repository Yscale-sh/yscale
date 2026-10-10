package state

import "testing"

// DeleteCustomer removes the record and both indexes (by id and by token), so a
// stale token can't authenticate an offboarded tenant.
func TestDeleteCustomer(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cust_x", Token: "tok_x", Plan: "pro"})

	s.DeleteCustomer("cust_x")

	if _, err := s.CustomerByID("cust_x"); err == nil {
		t.Error("customer should be gone after DeleteCustomer")
	}
	if _, err := s.AuthCustomer("tok_x"); err == nil {
		t.Error("offboarded token must no longer authenticate")
	}
	// Deleting a missing customer is a no-op (idempotent).
	s.DeleteCustomer("cust_x")
}

// BurstsForCustomer returns only that customer's bursts — offboard reaps by it.
func TestBurstsForCustomer(t *testing.T) {
	s := New()
	s.PutBurst(&Burst{ID: "b1", CustomerID: "cust_a"})
	s.PutBurst(&Burst{ID: "b2", CustomerID: "cust_a"})
	s.PutBurst(&Burst{ID: "b3", CustomerID: "cust_b"})

	got := s.BurstsForCustomer("cust_a")
	if len(got) != 2 {
		t.Fatalf("BurstsForCustomer(cust_a) = %d bursts, want 2", len(got))
	}
	for _, b := range got {
		if b.CustomerID != "cust_a" {
			t.Errorf("got a burst for the wrong customer: %+v", b)
		}
	}
	if n := len(s.BurstsForCustomer("cust_none")); n != 0 {
		t.Errorf("unknown customer should have 0 bursts, got %d", n)
	}
}
