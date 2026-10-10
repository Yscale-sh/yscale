package lifecycle

import (
	"errors"
	"fmt"
)

var (
	ErrIdempotencyConflict = errors.New("lifecycle: idempotency key conflict")
	ErrIdentityConflict    = errors.New("lifecycle: identity conflict")
	ErrInvalidArgument     = errors.New("lifecycle: invalid argument")
	ErrInvariantViolation  = errors.New("lifecycle: invariant violation")
	ErrLeaseLost           = errors.New("lifecycle: lease lost")
	ErrNotFound            = errors.New("lifecycle: not found")
)

func validateIdentifier(label, value string) error {
	if value == "" || len(value) > maxIdentifierBytes {
		return fmt.Errorf("%w: %s must be 1..255 bytes", ErrInvalidArgument, label)
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '.' || r == ':' || r == '/':
		default:
			return fmt.Errorf("%w: %s contains unsupported characters", ErrInvalidArgument, label)
		}
	}
	return nil
}
