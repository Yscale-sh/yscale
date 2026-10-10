package backends

import (
	"errors"
	"fmt"
	"testing"
)

// The default is ambiguous. An unclassified non-nil CreateNode error must be
// treated as if the machine may exist, or every unknown adapter failure would
// silently refund a create that landed.
func TestCreateOutcomeAmbiguousDefaultsToTrue(t *testing.T) {
	if !CreateOutcomeAmbiguous(errors.New("anything")) {
		t.Fatal("an unmarked CreateNode error must classify as ambiguous by default")
	}
	if CreateOutcomeAmbiguous(nil) {
		t.Fatal("a nil error must not classify as ambiguous")
	}
}

// A proven-zero-resource marker flips the classification to clean-failed. The
// marker rides through wrapping so an adapter's post-error formatting cannot
// lose it.
func TestMarkCreateProvenZeroResourceIsCleanFailed(t *testing.T) {
	err := MarkCreateProvenZeroResource(errors.New("400 bad request"))
	if CreateOutcomeAmbiguous(err) {
		t.Fatalf("a proven-zero-resource marker must not classify as ambiguous: %v", err)
	}
	wrapped := fmt.Errorf("creating burst: %w", err)
	if CreateOutcomeAmbiguous(wrapped) {
		t.Fatalf("the proven marker must survive wrapping: %v", wrapped)
	}
	if MarkCreateProvenZeroResource(nil) != nil {
		t.Fatal("nil in, nil out")
	}
}

// An explicit ambiguous marker overrides an inner proven marker: escalation
// is one-way, so a caller who knows the outcome is uncertain can raise the
// classification even when the adapter thought it had proof.
func TestMarkCreateAmbiguousWinsOverProven(t *testing.T) {
	inner := MarkCreateProvenZeroResource(errors.New("400 bad request"))
	escalated := MarkCreateAmbiguous(inner)
	if !CreateOutcomeAmbiguous(escalated) {
		t.Fatal("an ambiguous marker must override a proven-zero-resource one")
	}
	// Also survives wrapping.
	wrapped := fmt.Errorf("post-create cleanup uncertain: %w", escalated)
	if !CreateOutcomeAmbiguous(wrapped) {
		t.Fatalf("ambiguous marker must survive wrapping: %v", wrapped)
	}
}

// The other direction is refused: a proven wrapper around an already-ambiguous
// error must not silently downgrade the outcome. If a layer said "may hold a
// resource", every later layer has to keep that answer or a settlement that
// nothing verified would release the tenant's hold.
func TestMarkCreateProvenZeroResourceCannotOverrideAmbiguous(t *testing.T) {
	inner := MarkCreateAmbiguous(errors.New("connection lost after request write"))
	demoted := MarkCreateProvenZeroResource(inner)
	if !CreateOutcomeAmbiguous(demoted) {
		t.Fatal("a proven-zero-resource wrapper must not override an ambiguous marker")
	}
}

// Wrapping the same marker twice is a no-op: the classification is idempotent
// so a helper that always marks its return value doesn't stack layers on a
// caller that already marked its input.
func TestMarkersAreIdempotent(t *testing.T) {
	proven := MarkCreateProvenZeroResource(errors.New("boom"))
	if MarkCreateProvenZeroResource(proven) != proven {
		t.Fatal("re-marking a proven-zero-resource error must return the same instance")
	}
	amb := MarkCreateAmbiguous(errors.New("boom"))
	if MarkCreateAmbiguous(amb) != amb {
		t.Fatal("re-marking an ambiguous error must return the same instance")
	}
}

func TestHTTPCreateResponseProvesNoResource(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   bool
	}{
		{status: 400, want: true},
		{status: 401, want: true},
		{status: 403, want: true},
		{status: 404, want: true},
		{status: 408, want: false},
		{status: 409, want: false},
		{status: 425, want: false},
		{status: 429, want: false},
		{status: 500, want: false},
	} {
		if got := HTTPCreateResponseProvesNoResource(tc.status); got != tc.want {
			t.Errorf("status %d: got %v, want %v", tc.status, got, tc.want)
		}
	}
}
