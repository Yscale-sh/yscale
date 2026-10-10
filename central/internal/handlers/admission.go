package handlers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// admissionGate wraps a state.Store and provides the HTTP-level admission
// decision used by Create. The store's ReserveAdmission method handles
// both in-memory and Postgres-backed admission atomically — reading
// customer limits, live bursts, and existing reservations in one
// advisory-locked transaction (Postgres) or under the store lock (in-memory).
type admissionStore interface {
	ReserveAdmission(context.Context, string, string, int64) (string, error)
	ReleaseAdmission(context.Context, string) error
	AllAdmissionReservationCount() int
}

type admissionGate struct {
	store admissionStore
}

type retryAdmissionKey struct{}

type retryAdmission struct {
	store interface {
		ReserveWorkloadRetry(context.Context, *state.WorkloadRetryApproval, string, int64) (string, *state.AuditEvent, error)
	}
	approval *state.WorkloadRetryApproval
}

func newAdmissionGate(store admissionStore) *admissionGate {
	return &admissionGate{store: store}
}

// reserve runs admission for one new burst and, if admitted, records a durable
// reservation. candidateHourlyUSD is the quoted rate (0 if unknown/concurrency-only).
//
// Returns (reservationID, 0, "") on admit, or ("", status, msg) when the tenant
// is at a ceiling.
func (ag *admissionGate) reserve(ctx context.Context, cust *state.Customer, workloadID string, candidateHourlyUSD float64) (string, int, string) {
	if err := validateCandidateRate(candidateHourlyUSD); err != nil {
		return "", http.StatusBadRequest, "invalid candidate rate"
	}

	candidateMicroUSD := state.USDToMicroUSD(candidateHourlyUSD)
	if candidateMicroUSD < 0 {
		return "", http.StatusBadRequest, "invalid candidate rate"
	}

	var id string
	var err error
	if retryOfWorkload(ctx) != "" {
		retry, ok := ctx.Value(retryAdmissionKey{}).(retryAdmission)
		if !ok || retry.store == nil || retry.approval == nil {
			return "", http.StatusServiceUnavailable, "retry preparation unavailable"
		}
		id, _, err = retry.store.ReserveWorkloadRetry(ctx, retry.approval, workloadID, candidateMicroUSD)
	} else {
		id, err = ag.store.ReserveAdmission(ctx, cust.ID, workloadID, candidateMicroUSD)
	}
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return "", http.StatusNotFound, "not found"
		}
		if errors.Is(err, state.ErrWorkloadRetryForbidden) {
			return "", http.StatusForbidden, "forbidden: retrying this workload requires owner or admin, or the member who submitted it"
		}
		if errors.Is(err, state.ErrWorkloadRetryChanged) {
			return "", http.StatusConflict, "retry source or tenant policy changed; retry the request"
		}
		if errors.Is(err, state.ErrWorkloadRetryNotTerminal) {
			return "", http.StatusConflict, "source workload must be terminal before it can be retried"
		}
		if errors.Is(err, state.ErrAdmissionSlotLimitReached) {
			return "", http.StatusTooManyRequests, fmt.Sprintf(
				"concurrent burst limit reached (max %d) — let some finish first",
				cust.MaxConcurrentBursts)
		}
		if errors.Is(err, state.ErrAdmissionRateLimitReached) {
			return "", http.StatusPaymentRequired, fmt.Sprintf(
				"hourly spend cap reached (cap $%.2f/hr) — existing bursts must finish first",
				cust.MaxHourlyUSD)
		}
		if errors.Is(err, state.ErrAdmissionInvalidRate) || errors.Is(err, state.ErrAdmissionConflictingRate) {
			return "", http.StatusBadRequest, "invalid candidate rate"
		}
		return "", http.StatusServiceUnavailable, "admission check failed"
	}
	return id, 0, ""
}

// release drops a reservation. Called once the burst is persisted (store now
// counts it) or the provision fails.
func (ag *admissionGate) release(ctx context.Context, id string) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := ag.store.ReleaseAdmission(cleanupCtx, id); err != nil {
		// Logged inside state.Store; conservative double-count until TTL.
		return err
	}
	return nil
}

// reservationCount returns the number of active reservations for leak detection
// in tests.
func (ag *admissionGate) reservationCount() int {
	return ag.store.AllAdmissionReservationCount()
}

func validateCandidateRate(rate float64) error {
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate < 0 {
		return fmt.Errorf("invalid candidate rate: %v", rate)
	}
	return nil
}
