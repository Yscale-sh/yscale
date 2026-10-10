package billing

import (
	"errors"
	"fmt"
)

// Stable errors let handlers fail closed without parsing database messages.
var (
	ErrInsufficientCredit     = errors.New("billing: insufficient credit")
	ErrIdempotencyConflict    = errors.New("billing: idempotency key conflict")
	ErrIllegalTransition      = errors.New("billing: illegal state transition")
	ErrInvalidArgument        = errors.New("billing: invalid argument")
	ErrInvalidAmount          = errors.New("billing: invalid amount")
	ErrAmountOverflow         = errors.New("billing: amount overflow")
	ErrCaptureExceedsHold     = errors.New("billing: capture exceeds hold")
	ErrHoldExpired            = errors.New("billing: hold expired")
	ErrNotFound               = errors.New("billing: not found")
	ErrAccountFrozen          = errors.New("billing: account frozen")
	ErrInvariantViolation     = errors.New("billing: invariant violation")
	ErrLeaseLost              = errors.New("billing: webhook lease lost")
	ErrEconomicObjectConflict = errors.New("billing: economic object conflict")
	ErrEnvironmentMismatch    = errors.New("billing: test/live environment mismatch")
	ErrWebhookRejected        = errors.New("billing: webhook rejected")
	ErrStatementTooLarge      = errors.New("billing: statement has too many entries")
)

func ValidatePositiveAmount(microUSD int64) error {
	if microUSD <= 0 {
		return fmt.Errorf("%w: got %d micro-USD", ErrInvalidAmount, microUSD)
	}
	return nil
}

func safeAdd(a, b int64) (int64, error) {
	if b > 0 && a > maxInt64-b {
		return 0, fmt.Errorf("%w: %d + %d", ErrAmountOverflow, a, b)
	}
	if b < 0 && a < minInt64-b {
		return 0, fmt.Errorf("%w: %d + %d", ErrAmountOverflow, a, b)
	}
	return a + b, nil
}

const (
	maxInt64 int64 = 1<<63 - 1
	minInt64 int64 = -(1 << 63)
)
