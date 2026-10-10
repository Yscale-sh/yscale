package state

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestUSDToMicroUSD(t *testing.T) {
	if got := USDToMicroUSD(1.0); got != 1_000_000 {
		t.Errorf("USDToMicroUSD(1.0) = %d, want 1000000", got)
	}
	if got := USDToMicroUSD(0.62); got != 620_000 {
		t.Errorf("USDToMicroUSD(0.62) = %d, want 620000", got)
	}
	if got := USDToMicroUSD(0); got != 0 {
		t.Errorf("USDToMicroUSD(0) = %d, want 0", got)
	}
	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1.0} {
		if got := USDToMicroUSD(bad); got >= 0 {
			t.Errorf("USDToMicroUSD(%v) = %d, want negative", bad, got)
		}
	}
}

func TestValidateCandidateMicroUSD(t *testing.T) {
	if err := ValidateCandidateMicroUSD(0); err != nil {
		t.Errorf("zero should be valid: %v", err)
	}
	if err := ValidateCandidateMicroUSD(1_000_000); err != nil {
		t.Errorf("positive should be valid: %v", err)
	}
	if err := ValidateCandidateMicroUSD(-1); err == nil {
		t.Error("negative should be invalid")
	}
	if err := ValidateCandidateMicroUSD(-1); !errors.Is(err, ErrAdmissionInvalidRate) {
		t.Errorf("negative should wrap ErrAdmissionInvalidRate, got: %v", err)
	}
}

func TestReserveAdmission_EmptyIdentifiers(t *testing.T) {
	s := New()
	ctx := context.Background()

	if _, err := s.ReserveAdmission(ctx, "", "wl_1", 0); err == nil {
		t.Error("empty customerID should fail")
	}
	if _, err := s.ReserveAdmission(ctx, "cust_test", "", 0); err == nil {
		t.Error("empty workloadID should fail")
	}
}

func TestReserveAdmission_UnknownCustomer(t *testing.T) {
	s := New()
	ctx := context.Background()

	if _, err := s.ReserveAdmission(ctx, "nonexistent", "wl_1", 0); err == nil {
		t.Error("unknown customer should fail")
	}
}

func TestReserveAdmission_ExactAtCap(t *testing.T) {
	s := New()
	cust := &Customer{ID: "cust_exact", Token: "tex", MaxHourlyUSD: 10}
	s.AddCustomer(cust)
	ctx := context.Background()

	// $5 + $5 = $10 = cap — must admit
	_, err := s.ReserveAdmission(ctx, "cust_exact", "wl_1", 5_000_000)
	if err != nil {
		t.Fatalf("first $5: %v", err)
	}
	_, err = s.ReserveAdmission(ctx, "cust_exact", "wl_2", 5_000_000)
	if err != nil {
		t.Fatalf("$5+$5=$10=cap: %v", err)
	}

	// $10 + $0.01 > $10
	_, err = s.ReserveAdmission(ctx, "cust_exact", "wl_3", 10_000)
	if err == nil {
		t.Fatal("over cap should reject")
	}
}

func TestReserveAdmission_ReservationCountTracking(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cust_cnt", Token: "tc"})
	ctx := context.Background()

	if n := s.AllAdmissionReservationCount(); n != 0 {
		t.Fatalf("initial count = %d, want 0", n)
	}

	id1, _ := s.ReserveAdmission(ctx, "cust_cnt", "wl_1", 0)
	id2, _ := s.ReserveAdmission(ctx, "cust_cnt", "wl_2", 0)

	if n := s.AllAdmissionReservationCount(); n != 2 {
		t.Fatalf("after 2 reserves = %d, want 2", n)
	}
	if n := s.AdmissionReservationCount("cust_cnt"); n != 2 {
		t.Fatalf("customer count = %d, want 2", n)
	}

	s.ReleaseAdmission(ctx, id1)
	if n := s.AllAdmissionReservationCount(); n != 1 {
		t.Fatalf("after 1 release = %d, want 1", n)
	}

	s.ReleaseAdmission(ctx, id2)
	if n := s.AllAdmissionReservationCount(); n != 0 {
		t.Fatalf("after release = %d, want 0", n)
	}
}

func TestReserveAdmission_ExpiredSameWorkloadIsReplaced(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cust_ttl", Token: "ttl", MaxConcurrentBursts: 1})
	ctx := context.Background()

	oldID, err := s.ReserveAdmission(ctx, "cust_ttl", "wl_1", 1_000_000)
	if err != nil {
		t.Fatalf("initial reserve: %v", err)
	}
	s.admissionReservations[oldID].CreatedAt = time.Now().Add(-AdmissionReservationTTL - time.Second)

	newID, err := s.ReserveAdmission(ctx, "cust_ttl", "wl_1", 1_000_000)
	if err != nil {
		t.Fatalf("reserve after expiry: %v", err)
	}
	if newID == oldID {
		t.Fatalf("expired reservation was replayed: %q", newID)
	}
}
