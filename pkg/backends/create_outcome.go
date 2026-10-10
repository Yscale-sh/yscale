package backends

import (
	"errors"
	"net/http"
)

// Create-outcome classification is the provider-neutral answer to the one
// question central asks a failed CreateNode: "did the provider prove that no
// billable node exists?" The answer is a single boolean, derived here so every
// adapter records it in the same shape and central never has to know which
// provider it was talking to.
//
// The default is AMBIGUOUS. Any non-nil CreateNode error that reaches central
// without an explicit marker is treated as if the machine may exist: the
// tenant's credit hold is retained, the cloud-account lease is extended for
// manual attention, and the durable settlement records the create as
// ambiguous rather than clean-failed. An adapter must OPT IN to the proven
// classification by wrapping the error with MarkCreateProvenZeroResource, and
// only for failures where the provider itself has proven that no resource
// exists.
//
// The ambiguous marker exists too, but only for adapters or callers that need
// to say "this was CERTAINLY ambiguous even if a later wrapper looks proven".
// It is retained for compatibility with Linode's original API and for wrappers
// that need to escalate a proven marker back to ambiguous. Escalation is
// one-way: MarkCreateProvenZeroResource never overrides an existing ambiguous
// marker, so a proven wrapper cannot silently downgrade an outcome an earlier
// layer already flagged as uncertain.

// createOutcomeProvenZeroResource is the explicit marker for a CreateNode
// failure the adapter has proven left no billable resource behind. Wrap only
// pre-machine-dispatch validation, provider 4xx / modeled client rejection
// that proves the create was refused, or terminal-failed operations the
// provider reports as Done.
type createOutcomeProvenZeroResource struct{ err error }

func (e *createOutcomeProvenZeroResource) Error() string { return e.err.Error() }
func (e *createOutcomeProvenZeroResource) Unwrap() error { return e.err }

// createOutcomeAmbiguous is the explicit marker for a CreateNode failure the
// adapter cannot prove one way or the other. Wrap transport errors, decode
// failures on an accepted response, empty-ID responses, nonterminal polling
// errors, caller cancellation after dispatch, and any cleanup path whose
// success cannot be verified.
type createOutcomeAmbiguous struct{ err error }

func (e *createOutcomeAmbiguous) Error() string { return e.err.Error() }
func (e *createOutcomeAmbiguous) Unwrap() error { return e.err }

// MarkCreateProvenZeroResource wraps a CreateNode error with the explicit
// proof that no billable resource exists. It NEVER overrides an existing
// ambiguous marker: if an earlier layer already flagged the outcome as
// uncertain, that uncertainty stands and the returned error still classifies
// ambiguous.
func MarkCreateProvenZeroResource(err error) error {
	if err == nil {
		return nil
	}
	var amb *createOutcomeAmbiguous
	if errors.As(err, &amb) {
		return err
	}
	var already *createOutcomeProvenZeroResource
	if errors.As(err, &already) {
		return err
	}
	return &createOutcomeProvenZeroResource{err: err}
}

// MarkCreateAmbiguous wraps a CreateNode error with the explicit uncertain
// classification. An ambiguous marker always wins over a proven one, so
// wrapping is safe even when the underlying error may already carry a
// proven-zero-resource marker from an inner layer.
func MarkCreateAmbiguous(err error) error {
	if err == nil {
		return nil
	}
	var already *createOutcomeAmbiguous
	if errors.As(err, &already) {
		return err
	}
	return &createOutcomeAmbiguous{err: err}
}

// CreateOutcomeAmbiguous is the single classifier central uses to derive the
// one ambiguous boolean that drives durable settlement, admission release, and
// cloud-account lease behavior for a failed CreateNode.
//
// Any non-nil error that carries no explicit proven-zero-resource marker is
// treated as ambiguous. An explicit ambiguous marker wins over an explicit
// proven marker in the same chain — that is what lets an outer layer escalate
// a provider's "refused" into "may hold a resource" without the escalation
// being silently undone.
func CreateOutcomeAmbiguous(err error) bool {
	if err == nil {
		return false
	}
	var amb *createOutcomeAmbiguous
	if errors.As(err, &amb) {
		return true
	}
	var proven *createOutcomeProvenZeroResource
	if errors.As(err, &proven) {
		return false
	}
	return true
}

// HTTPCreateResponseProvesNoResource reports whether a provider's HTTP
// response is a terminal client refusal rather than an uncertain outcome.
// Conflict may mean the deterministically named resource already exists;
// timeout, too-early, and rate-limit responses do not prove the request was
// never accepted.
func HTTPCreateResponseProvesNoResource(statusCode int) bool {
	if statusCode < http.StatusBadRequest || statusCode >= http.StatusInternalServerError {
		return false
	}
	switch statusCode {
	case http.StatusRequestTimeout, http.StatusConflict, http.StatusTooEarly, http.StatusTooManyRequests:
		return false
	default:
		return true
	}
}
