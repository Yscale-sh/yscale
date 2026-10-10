//go:build integration

package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestPostgresAdmissionReservationInvariants(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()

	newStore := func(t *testing.T) *Store {
		t.Helper()
		s, err := NewPostgres(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}

	reset := func(t *testing.T, s *Store) {
		t.Helper()
		pp := s.persist.(*pgPersister)
		if _, err := pp.pool.Exec(ctx, `DELETE FROM admission_reservations`); err != nil {
			t.Fatal(err)
		}
		for _, b := range s.ListBursts() {
			s.DeleteBurst(b.ID)
		}
	}

	ensureCust := func(s *Store, id, token string, maxBursts int, maxHourlyUSD float64) *Customer {
		cust := &Customer{ID: id, Token: token, MaxConcurrentBursts: maxBursts, MaxHourlyUSD: maxHourlyUSD}
		if _, err := s.CustomerByID(id); err != nil {
			s.AddCustomer(cust)
		}
		return cust
	}

	t.Run("reserve and release", func(t *testing.T) {
		s := newStore(t)
		reset(t, s)
		ensureCust(s, "cust_adm1", "tok_adm1", 5, 10)

		id, err := s.ReserveAdmission(ctx, "cust_adm1", "wl_1", 2_000_000)
		if err != nil {
			t.Fatalf("reserve: %v", err)
		}
		if id == "" {
			t.Fatal("expected non-empty id")
		}
		if err := s.ReleaseAdmission(ctx, id); err != nil {
			t.Fatalf("release: %v", err)
		}
		if err := s.ReleaseAdmission(ctx, id); err != nil {
			t.Fatalf("double release: %v", err)
		}
	})

	t.Run("slot limit across two store instances", func(t *testing.T) {
		s1 := newStore(t)
		reset(t, s1)
		ensureCust(s1, "cust_adm2", "tok_adm2", 2, 0)
		s2 := newStore(t)

		_, err := s1.ReserveAdmission(ctx, "cust_adm2", "wl_1", 1_000_000)
		if err != nil {
			t.Fatalf("s1 reserve: %v", err)
		}

		_, err = s2.ReserveAdmission(ctx, "cust_adm2", "wl_2", 1_000_000)
		if err != nil {
			t.Fatalf("s2 reserve: %v", err)
		}

		_, err = s1.ReserveAdmission(ctx, "cust_adm2", "wl_3", 1_000_000)
		if err == nil {
			t.Fatal("expected slot limit rejection across instances")
		}
	})

	t.Run("persisted running burst visibility across instances", func(t *testing.T) {
		s1 := newStore(t)
		reset(t, s1)
		ensureCust(s1, "cust_adm3", "tok_adm3", 2, 0)

		s1.PutBurst(&Burst{ID: "b_cross", CustomerID: "cust_adm3", HourlyUSD: 1.0})

		s2 := newStore(t)

		_, err := s2.ReserveAdmission(ctx, "cust_adm3", "wl_1", 1_000_000)
		if err != nil {
			t.Fatalf("s2 reserve with running burst: %v", err)
		}

		// 1 running + 1 reserved + 1 = 3 > 2
		_, err = s2.ReserveAdmission(ctx, "cust_adm3", "wl_2", 1_000_000)
		if err == nil {
			t.Fatal("expected slot limit with running burst from s1 + reservation")
		}
	})

	t.Run("rate limit across instances", func(t *testing.T) {
		s1 := newStore(t)
		reset(t, s1)
		ensureCust(s1, "cust_adm4", "tok_adm4", 0, 10)

		_, err := s1.ReserveAdmission(ctx, "cust_adm4", "wl_1", 6_000_000)
		if err != nil {
			t.Fatalf("s1 reserve: %v", err)
		}

		s2 := newStore(t)

		// $6 + $5 = $11 > $10
		_, err = s2.ReserveAdmission(ctx, "cust_adm4", "wl_2", 5_000_000)
		if err == nil {
			t.Fatal("expected rate limit rejection across instances")
		}

		// $6 + $4 = $10 = cap
		_, err = s2.ReserveAdmission(ctx, "cust_adm4", "wl_3", 4_000_000)
		if err != nil {
			t.Fatalf("$6+$4=$10 should admit: %v", err)
		}
	})

	t.Run("idempotent replay", func(t *testing.T) {
		s := newStore(t)
		reset(t, s)
		ensureCust(s, "cust_adm5", "tok_adm5", 1, 0)

		id1, err := s.ReserveAdmission(ctx, "cust_adm5", "wl_1", 2_000_000)
		if err != nil {
			t.Fatalf("first: %v", err)
		}

		id2, err := s.ReserveAdmission(ctx, "cust_adm5", "wl_1", 2_000_000)
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		if id1 != id2 {
			t.Errorf("idempotent replay returned different id: %q vs %q", id1, id2)
		}

		_, err = s.ReserveAdmission(ctx, "cust_adm5", "wl_1", 3_000_000)
		if err == nil {
			t.Fatal("conflicting replay should fail")
		}
	})

	t.Run("concurrent single slot", func(t *testing.T) {
		s := newStore(t)
		reset(t, s)
		ensureCust(s, "cust_adm6", "tok_adm6", 1, 0)

		const n = 10
		results := make([]error, n)
		var wg sync.WaitGroup
		start := make(chan struct{})

		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				si := newStore(t)
				<-start
				_, results[i] = si.ReserveAdmission(ctx, "cust_adm6", fmt.Sprintf("wl_%d", i), 1_000_000)
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
			t.Fatalf("exactly 1 should win with max=1, got %d", admitted)
		}
	})

	t.Run("concurrent hourly cap", func(t *testing.T) {
		s := newStore(t)
		reset(t, s)
		ensureCust(s, "cust_adm_rate_race", "tok_adm_rate_race", 0, 10)

		start := make(chan struct{})
		results := make(chan error, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				si := newStore(t)
				<-start
				_, err := si.ReserveAdmission(ctx, "cust_adm_rate_race", fmt.Sprintf("wl_rate_%d", i), 6_000_000)
				results <- err
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)

		var admitted, rejected int
		for err := range results {
			switch {
			case err == nil:
				admitted++
			case errors.Is(err, ErrAdmissionRateLimitReached):
				rejected++
			default:
				t.Fatalf("unexpected reservation result: %v", err)
			}
		}
		if admitted != 1 || rejected != 1 {
			t.Fatalf("admitted=%d rejected=%d, want 1/1", admitted, rejected)
		}
	})

	t.Run("release makes slot available", func(t *testing.T) {
		s := newStore(t)
		reset(t, s)
		ensureCust(s, "cust_adm7", "tok_adm7", 1, 0)

		id, err := s.ReserveAdmission(ctx, "cust_adm7", "wl_1", 1_000_000)
		if err != nil {
			t.Fatalf("first: %v", err)
		}

		_, err = s.ReserveAdmission(ctx, "cust_adm7", "wl_2", 1_000_000)
		if err == nil {
			t.Fatal("should fail: slot full")
		}

		s.ReleaseAdmission(ctx, id)

		_, err = s.ReserveAdmission(ctx, "cust_adm7", "wl_2", 1_000_000)
		if err != nil {
			t.Fatalf("after release should succeed: %v", err)
		}
	})

	t.Run("TTL expiry allows same workload reuse", func(t *testing.T) {
		s := newStore(t)
		reset(t, s)
		ensureCust(s, "cust_adm8", "tok_adm8", 1, 0)

		_, err := s.ReserveAdmission(ctx, "cust_adm8", "wl_1", 1_000_000)
		if err != nil {
			t.Fatalf("first: %v", err)
		}

		// Manually expire it
		pp := s.persist.(*pgPersister)
		if _, err := pp.pool.Exec(ctx, `
			UPDATE admission_reservations
			SET created_at = now() - interval '15 minutes'
			WHERE customer_id = 'cust_adm8'`); err != nil {
			t.Fatal(err)
		}

		// Should now be able to reserve the same workload again
		_, err = s.ReserveAdmission(ctx, "cust_adm8", "wl_1", 1_000_000)
		if err != nil {
			t.Fatalf("after TTL expiry, same workload should reserve: %v", err)
		}
	})

	t.Run("restart visibility", func(t *testing.T) {
		s1 := newStore(t)
		reset(t, s1)
		ensureCust(s1, "cust_adm9", "tok_adm9", 1, 0)

		_, err := s1.ReserveAdmission(ctx, "cust_adm9", "wl_1", 1_000_000)
		if err != nil {
			t.Fatalf("reserve: %v", err)
		}

		s2 := newStore(t)
		_, err = s2.ReserveAdmission(ctx, "cust_adm9", "wl_2", 1_000_000)
		if err == nil {
			t.Fatal("new store instance should see existing reservation")
		}
	})

	t.Run("negative rejected", func(t *testing.T) {
		s := newStore(t)
		reset(t, s)
		ensureCust(s, "cust_adm10", "tok_adm10", 0, 0)

		_, err := s.ReserveAdmission(ctx, "cust_adm10", "wl_neg", -1)
		if err == nil {
			t.Error("expected rejection for negative rate")
		}
	})

	t.Run("tenant isolation", func(t *testing.T) {
		s := newStore(t)
		reset(t, s)
		ensureCust(s, "cust_iso1", "tok_iso1", 1, 0)
		ensureCust(s, "cust_iso2", "tok_iso2", 1, 0)

		_, err := s.ReserveAdmission(ctx, "cust_iso1", "wl_1", 1_000_000)
		if err != nil {
			t.Fatalf("cust_iso1 reserve: %v", err)
		}

		_, err = s.ReserveAdmission(ctx, "cust_iso2", "wl_1", 1_000_000)
		if err != nil {
			t.Fatalf("cust_iso2 should be isolated: %v", err)
		}
	})

	t.Run("exact at cap", func(t *testing.T) {
		s := newStore(t)
		reset(t, s)
		ensureCust(s, "cust_exact", "tok_exact", 0, 10)

		// $5 + $5 = $10 = cap — should admit
		_, err := s.ReserveAdmission(ctx, "cust_exact", "wl_1", 5_000_000)
		if err != nil {
			t.Fatalf("first $5: %v", err)
		}
		_, err = s.ReserveAdmission(ctx, "cust_exact", "wl_2", 5_000_000)
		if err != nil {
			t.Fatalf("$5+$5=$10=cap should admit: %v", err)
		}

		// $10 + $0.01 > $10
		_, err = s.ReserveAdmission(ctx, "cust_exact", "wl_3", 10_000)
		if err == nil {
			t.Fatal("over cap should reject")
		}
	})

	t.Run("empty identifiers rejected", func(t *testing.T) {
		s := newStore(t)
		reset(t, s)

		_, err := s.ReserveAdmission(ctx, "", "wl_1", 0)
		if err == nil {
			t.Error("empty customer should reject")
		}

		_, err = s.ReserveAdmission(ctx, "cust_1", "", 0)
		if err == nil {
			t.Error("empty workload should reject")
		}
	})
}
