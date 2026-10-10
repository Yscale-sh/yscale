package state

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"
)

const AdmissionReservationTTL = 10 * time.Minute

var (
	ErrAdmissionSlotLimitReached = errors.New("concurrent burst limit reached")
	ErrAdmissionRateLimitReached = errors.New("hourly spend cap reached")
	ErrAdmissionInvalidRate      = errors.New("candidate hourly rate is invalid")
	ErrAdmissionConflictingRate  = errors.New("reservation exists with different rate")
)

// AdmissionReservation is a durable record of a burst that has passed admission
// but is not yet visible as a running burst in the store.
type AdmissionReservation struct {
	ID             string
	CustomerID     string
	WorkloadID     string
	HourlyMicroUSD int64
	CreatedAt      time.Time
}

// ValidateCandidateMicroUSD rejects negative values. Zero is valid (unknown rate
// with concurrency-only cap).
func ValidateCandidateMicroUSD(microUSD int64) error {
	if microUSD < 0 {
		return fmt.Errorf("%w: %d", ErrAdmissionInvalidRate, microUSD)
	}
	return nil
}

// USDToMicroUSD converts a float64 USD value to integer micro-USD. Returns -1
// for NaN, Inf, or negative values so the caller's validation catches them.
func USDToMicroUSD(usd float64) int64 {
	if math.IsNaN(usd) || math.IsInf(usd, 0) || usd < 0 {
		return -1
	}
	return int64(math.Round(usd * 1_000_000))
}

func newAdmissionID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("state: crypto/rand read failed: " + err.Error())
	}
	return "rsv_" + hex.EncodeToString(b[:])
}

// ReserveAdmission atomically checks tenant limits against running bursts +
// existing reservations + the candidate, and if admitted, records a new
// reservation. The Postgres path reads customer limits, live bursts, and
// reservations from the database in one advisory-locked transaction. The
// in-memory path uses the store's working set under its lock.
func (s *Store) ReserveAdmission(ctx context.Context, customerID, workloadID string, candidateMicroUSD int64) (string, error) {
	if err := ValidateCandidateMicroUSD(candidateMicroUSD); err != nil {
		return "", err
	}
	if customerID == "" || workloadID == "" {
		return "", fmt.Errorf("%w: empty customer or workload id", ErrAdmissionInvalidRate)
	}
	if durable, ok := s.persist.(admissionPersister); ok {
		id, err := durable.reserveAdmission(ctx, customerID, workloadID, candidateMicroUSD)
		if err != nil && !errors.Is(err, ErrAdmissionSlotLimitReached) &&
			!errors.Is(err, ErrAdmissionRateLimitReached) && !errors.Is(err, ErrAdmissionConflictingRate) {
			s.recordPersistenceFailure("admission", "reserve", err)
		}
		return id, err
	}
	return s.reserveAdmissionInMemory(customerID, workloadID, candidateMicroUSD)
}

func (s *Store) reserveAdmissionInMemory(customerID, workloadID string, candidateMicroUSD int64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reserveAdmissionLocked(customerID, workloadID, candidateMicroUSD)
}

func (s *Store) reserveAdmissionLocked(customerID, workloadID string, candidateMicroUSD int64) (string, error) {
	cust := s.customers[customerID]
	if cust == nil {
		return "", fmt.Errorf("customer not found: %s", customerID)
	}

	now := time.Now().UTC()

	var reservedSlots int
	var reservedMicroUSD int64
	for id, r := range s.admissionReservations {
		if now.Sub(r.CreatedAt) > AdmissionReservationTTL {
			delete(s.admissionReservations, id)
			continue
		}
		if r.CustomerID != customerID {
			continue
		}
		if r.WorkloadID == workloadID {
			if r.HourlyMicroUSD != candidateMicroUSD {
				return "", ErrAdmissionConflictingRate
			}
			return r.ID, nil
		}
		reservedSlots++
		reservedMicroUSD += r.HourlyMicroUSD
	}

	var runningBursts int
	var runningMicroUSD int64
	for _, b := range s.bursts {
		if b.CustomerID != customerID {
			continue
		}
		runningBursts++
		runningMicroUSD += USDToMicroUSD(b.HourlyUSD)
	}

	maxBursts := cust.MaxConcurrentBursts
	maxHourlyMicroUSD := USDToMicroUSD(cust.MaxHourlyUSD)

	totalSlots := runningBursts + reservedSlots + 1
	totalMicroUSD := runningMicroUSD + reservedMicroUSD + candidateMicroUSD

	if maxBursts > 0 && totalSlots > maxBursts {
		return "", ErrAdmissionSlotLimitReached
	}
	if cust.MaxHourlyUSD > 0 && totalMicroUSD > maxHourlyMicroUSD {
		return "", ErrAdmissionRateLimitReached
	}

	id := newAdmissionID()
	s.admissionReservations[id] = &AdmissionReservation{
		ID:             id,
		CustomerID:     customerID,
		WorkloadID:     workloadID,
		HourlyMicroUSD: candidateMicroUSD,
		CreatedAt:      now,
	}
	return id, nil
}

// ReleaseAdmission drops a reservation. Called once the burst is persisted or
// the provision fails. Idempotent.
func (s *Store) ReleaseAdmission(ctx context.Context, id string) error {
	if durable, ok := s.persist.(admissionPersister); ok {
		if err := durable.releaseAdmission(ctx, id); err != nil {
			s.recordPersistenceFailure("admission", "release", err)
			slog.Error("release admission reservation", "id", id, "error", err)
			return err
		}
		return nil
	}
	s.mu.Lock()
	delete(s.admissionReservations, id)
	s.mu.Unlock()
	return nil
}

// AdmissionReservationCount returns the number of reservations for a
// customer. Used by tests for leak detection.
func (s *Store) AdmissionReservationCount(customerID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, r := range s.admissionReservations {
		if customerID == "" || r.CustomerID == customerID {
			n++
		}
	}
	return n
}

// AllAdmissionReservationCount returns the total reservation count
// across all tenants.
func (s *Store) AllAdmissionReservationCount() int {
	return s.AdmissionReservationCount("")
}
