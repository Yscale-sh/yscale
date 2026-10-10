package handlers

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sync"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

type releaseContextStore struct {
	releaseContextErr  error
	releaseHasDeadline bool
}

func TestRetryAdmissionFailsClosedWithoutPreparation(t *testing.T) {
	s := state.New()
	c := &state.Customer{ID: "retry_tenant", Plan: "pro"}
	s.AddCustomer(c)
	ctx := withRetryOfWorkload(context.Background(), "retry_source")
	id, status, _ := newAdmissionGate(s).reserve(ctx, c, "retry_new", 1)
	if status != http.StatusServiceUnavailable || id != "" || s.AllAdmissionReservationCount() != 0 {
		t.Fatalf("unprepared retry admitted: %s status=%d", id, status)
	}
}

func (*releaseContextStore) ReserveAdmission(context.Context, string, string, int64) (string, error) {
	return "rsv_test", nil
}

func (s *releaseContextStore) ReleaseAdmission(ctx context.Context, _ string) error {
	s.releaseContextErr = ctx.Err()
	_, s.releaseHasDeadline = ctx.Deadline()
	return nil
}

func (*releaseContextStore) AllAdmissionReservationCount() int { return 0 }

func TestAdmissionGate_ReleaseDetachesCanceledRequestContext(t *testing.T) {
	store := &releaseContextStore{}
	gate := newAdmissionGate(store)
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := gate.release(requestCtx, "rsv_test"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if store.releaseContextErr != nil {
		t.Fatalf("release inherited request cancellation: %v", store.releaseContextErr)
	}
	if !store.releaseHasDeadline {
		t.Fatal("release cleanup context is not bounded")
	}
}

func TestMemoryAdmissionStore_ReserveAndRelease(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t", MaxConcurrentBursts: 5, MaxHourlyUSD: 10})
	ctx := context.Background()

	id, err := s.ReserveAdmission(ctx, "cust_1", "wl_1", 2_000_000)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty reservation id")
	}

	if err := s.ReleaseAdmission(ctx, id); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := s.ReleaseAdmission(ctx, id); err != nil {
		t.Fatalf("double release: %v", err)
	}
}

func TestMemoryAdmissionStore_SlotLimit(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t", MaxConcurrentBursts: 2})
	ctx := context.Background()

	// Two running bursts already; one more should fail
	s.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_1", HourlyUSD: 1.0})
	s.PutBurst(&state.Burst{ID: "b2", CustomerID: "cust_1", HourlyUSD: 1.0})
	_, err := s.ReserveAdmission(ctx, "cust_1", "wl_1", 1_000_000)
	if err == nil {
		t.Fatal("expected slot limit rejection")
	}

	// Remove one burst, then reserve should work
	s.DeleteBurst("b2")
	id, err := s.ReserveAdmission(ctx, "cust_1", "wl_2", 1_000_000)
	if err != nil {
		t.Fatalf("reserve with room: %v", err)
	}

	// 1 running + 1 reserved + 1 candidate = 3 > 2
	_, err = s.ReserveAdmission(ctx, "cust_1", "wl_3", 1_000_000)
	if err == nil {
		t.Fatal("expected slot limit with reservation")
	}

	// Release first, then it should succeed
	s.ReleaseAdmission(ctx, id)
	_, err = s.ReserveAdmission(ctx, "cust_1", "wl_3", 1_000_000)
	if err != nil {
		t.Fatalf("reserve after release: %v", err)
	}
}

func TestMemoryAdmissionStore_RateLimit(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t", MaxHourlyUSD: 10})
	ctx := context.Background()

	// Running $8/hr + candidate $3/hr = $11/hr > $10/hr cap
	s.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_1", HourlyUSD: 8.0})
	_, err := s.ReserveAdmission(ctx, "cust_1", "wl_1", 3_000_000)
	if err == nil {
		t.Fatal("expected rate limit rejection")
	}

	// Running $8/hr + candidate $2/hr = $10/hr = cap (exactly at limit is OK)
	_, err = s.ReserveAdmission(ctx, "cust_1", "wl_2", 2_000_000)
	if err != nil {
		t.Fatalf("exactly at cap should admit: %v", err)
	}
}

func TestMemoryAdmissionStore_CurrentPlusCandidateRateRejection(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t", MaxConcurrentBursts: 10, MaxHourlyUSD: 5})
	ctx := context.Background()

	_, err := s.ReserveAdmission(ctx, "cust_1", "wl_1", 3_000_000)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}

	// 0 running + $3 reserved + $3 candidate = $6 > $5 cap
	_, err = s.ReserveAdmission(ctx, "cust_1", "wl_2", 3_000_000)
	if err == nil {
		t.Fatal("expected rate limit with reserved + candidate exceeding cap")
	}

	// $2 candidate: 0 + $3 + $2 = $5 = cap, should admit
	_, err = s.ReserveAdmission(ctx, "cust_1", "wl_3", 2_000_000)
	if err != nil {
		t.Fatalf("$3 reserved + $2 candidate should admit: %v", err)
	}
}

func TestMemoryAdmissionStore_Idempotent(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t", MaxConcurrentBursts: 1})
	ctx := context.Background()

	id1, err := s.ReserveAdmission(ctx, "cust_1", "wl_1", 2_000_000)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}

	id2, err := s.ReserveAdmission(ctx, "cust_1", "wl_1", 2_000_000)
	if err != nil {
		t.Fatalf("idempotent reserve: %v", err)
	}
	if id1 != id2 {
		t.Errorf("idempotent reserve returned different id: %q vs %q", id1, id2)
	}

	_, err = s.ReserveAdmission(ctx, "cust_1", "wl_1", 3_000_000)
	if err == nil {
		t.Fatal("conflicting replay should fail")
	}
}

func TestMemoryAdmissionStore_IdempotentDoesNotDoubleCount(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t", MaxConcurrentBursts: 1})
	ctx := context.Background()

	_, err := s.ReserveAdmission(ctx, "cust_1", "wl_1", 2_000_000)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}

	_, err = s.ReserveAdmission(ctx, "cust_1", "wl_1", 2_000_000)
	if err != nil {
		t.Fatalf("idempotent replay must not double-count: %v", err)
	}
}

func TestMemoryAdmissionStore_ZeroLimitsUnlimited(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t"})
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		wlID := fmt.Sprintf("wl_%d", i)
		_, err := s.ReserveAdmission(ctx, "cust_1", wlID, 999_000_000)
		if err != nil {
			t.Fatalf("unlimited should always admit: %v (i=%d)", err, i)
		}
	}
}

func TestMemoryAdmissionStore_InvalidRate(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t"})
	ctx := context.Background()

	for _, micro := range []int64{-1, -1_000_000} {
		_, err := s.ReserveAdmission(ctx, "cust_1", "wl_1", micro)
		if err == nil {
			t.Errorf("expected rejection for micro=%d", micro)
		}
	}

	for _, rate := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1.0} {
		micro := state.USDToMicroUSD(rate)
		if micro >= 0 {
			t.Errorf("USDToMicroUSD(%v) = %d, want negative sentinel", rate, micro)
		}
	}
}

func TestMemoryAdmissionStore_TenantIsolation(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t1", MaxConcurrentBursts: 1})
	s.AddCustomer(&state.Customer{ID: "cust_2", Token: "t2", MaxConcurrentBursts: 1})
	ctx := context.Background()

	_, err := s.ReserveAdmission(ctx, "cust_1", "wl_1", 1_000_000)
	if err != nil {
		t.Fatalf("cust_1 reserve: %v", err)
	}

	_, err = s.ReserveAdmission(ctx, "cust_2", "wl_1", 1_000_000)
	if err != nil {
		t.Fatalf("cust_2 reserve should succeed: %v", err)
	}
}

func TestMemoryAdmissionStore_ReleaseAfterBurstPersist(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t", MaxConcurrentBursts: 1})
	ctx := context.Background()

	id, err := s.ReserveAdmission(ctx, "cust_1", "wl_1", 1_000_000)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	_, err = s.ReserveAdmission(ctx, "cust_1", "wl_2", 1_000_000)
	if err == nil {
		t.Fatal("expected slot rejection")
	}

	if err := s.ReleaseAdmission(ctx, id); err != nil {
		t.Fatalf("release: %v", err)
	}

	_, err = s.ReserveAdmission(ctx, "cust_1", "wl_2", 1_000_000)
	if err != nil {
		t.Fatalf("after release, should admit: %v", err)
	}
}

func TestMemoryAdmissionStore_ConcurrentReserve(t *testing.T) {
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_1", Token: "t", MaxConcurrentBursts: 1})
	ctx := context.Background()

	const n = 20
	results := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, results[i] = s.ReserveAdmission(ctx, "cust_1", fmt.Sprintf("wl_%d", i), 1_000_000)
		}(i)
	}
	close(start)
	wg.Wait()

	admitted := 0
	for _, err := range results {
		if err == nil {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("exactly 1 should win with max_concurrent_bursts=1, got %d", admitted)
	}
}

func TestAdmissionGate_Reserve_Integration(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_test", Token: "t", MaxConcurrentBursts: 2, MaxHourlyUSD: 10}
	store.AddCustomer(cust)

	gate := newAdmissionGate(store)
	ctx := context.Background()

	// First reservation
	id1, status, msg := gate.reserve(ctx, cust, "wl_1", 3.0)
	if status != 0 {
		t.Fatalf("first reserve: status=%d msg=%q", status, msg)
	}

	// Second reservation
	id2, status, msg := gate.reserve(ctx, cust, "wl_2", 3.0)
	if status != 0 {
		t.Fatalf("second reserve: status=%d msg=%q", status, msg)
	}

	// Third should fail on slot limit (0 running + 2 reserved + 1 candidate = 3 > 2)
	_, status, _ = gate.reserve(ctx, cust, "wl_3", 1.0)
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", status)
	}

	// Release one, third should succeed
	gate.release(ctx, id1)
	id3, status, msg := gate.reserve(ctx, cust, "wl_3", 1.0)
	if status != 0 {
		t.Fatalf("third after release: status=%d msg=%q", status, msg)
	}

	gate.release(ctx, id2)
	gate.release(ctx, id3)

	// Isolated rate cap check: separate store with ample slot cap
	t.Run("rate cap isolation", func(t *testing.T) {
		rateStore := state.New()
		rateCust := &state.Customer{ID: "cust_rate", Token: "tr", MaxConcurrentBursts: 10, MaxHourlyUSD: 10}
		rateStore.AddCustomer(rateCust)
		rateGate := newAdmissionGate(rateStore)

		r1, status, msg := rateGate.reserve(ctx, rateCust, "wl_r1", 4.0)
		if status != 0 {
			t.Fatalf("rate reserve 1: status=%d msg=%q", status, msg)
		}

		// $4 reserved + $7 candidate = $11 > $10
		_, status, _ = rateGate.reserve(ctx, rateCust, "wl_r2", 7.0)
		if status != http.StatusPaymentRequired {
			t.Fatalf("expected 402 for rate cap, got %d", status)
		}

		rateGate.release(ctx, r1)
	})
}

func TestAdmissionGate_NoProviderCallOnRefusal(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_test", Token: "t", MaxHourlyUSD: 5.0}
	store.AddCustomer(cust)

	gate := newAdmissionGate(store)
	ctx := context.Background()

	_, status, _ := gate.reserve(ctx, cust, "wl_1", 5.0)
	if status != 0 {
		t.Fatalf("first reserve should pass: status=%d", status)
	}

	_, status, _ = gate.reserve(ctx, cust, "wl_2", 0.01)
	if status != http.StatusPaymentRequired {
		t.Fatalf("expected 402, got %d", status)
	}
}

// TestAdmissionGate_PersistedBurstVisibility proves that a burst persisted in
// the store is counted by admission decisions.
func TestAdmissionGate_PersistedBurstVisibility(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_vis", Token: "tv", MaxConcurrentBursts: 2}
	store.AddCustomer(cust)

	store.PutBurst(&state.Burst{ID: "b_other", CustomerID: "cust_vis", HourlyUSD: 1.0})

	gate := newAdmissionGate(store)
	ctx := context.Background()

	// 1 running + 1 candidate = 2 = cap — should admit
	id1, status, msg := gate.reserve(ctx, cust, "wl_1", 1.0)
	if status != 0 {
		t.Fatalf("first reserve with running burst: status=%d msg=%q", status, msg)
	}

	// 1 running + 1 reserved + 1 candidate = 3 > 2 — should fail
	_, status, _ = gate.reserve(ctx, cust, "wl_2", 1.0)
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected 429 with running + reserved, got %d", status)
	}

	gate.release(ctx, id1)
}
