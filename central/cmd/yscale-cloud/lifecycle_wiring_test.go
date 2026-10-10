package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type fakeDeleteTracker struct {
	ids map[string]bool
	err error
}

func (f *fakeDeleteTracker) NonterminalProviderResourceIDs(context.Context) (map[string]bool, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.ids, nil
}

func trackingStore(t *testing.T) *state.Store {
	t.Helper()
	store := state.New()
	if err := store.PutBurst(&state.Burst{
		ID: "burst_live", CustomerID: state.DevCustomerID, Backend: "linode", BackendID: "linode_live",
	}); err != nil {
		t.Fatal(err)
	}
	return store
}

// A burst whose provider delete is queued, leased, retrying or waiting on an
// operator is NOT a leak: the live burst row is retired the moment the delete
// terminalizes, and between the two the delete worker may be inside the
// provider call for it. Sweeping without those IDs destroys the very resource
// the worker is deleting — and records nothing about having done so.
func TestOrphanTrackingIncludesNonterminalProviderDeletes(t *testing.T) {
	tracked, ok := trackedBackendIDs(
		context.Background(),
		trackingStore(t),
		&fakeDeleteTracker{ids: map[string]bool{"linode_deleting": true}},
		quietLog(),
	)
	if !ok {
		t.Fatal("the tracked set could not be built")
	}
	for _, id := range []string{"linode_live", "linode_deleting"} {
		if !tracked[id] {
			t.Errorf("%s is missing from the tracked set; the sweep would destroy it", id)
		}
	}
}

// Same fail-closed rule the durable burst read already has, for a stronger
// reason: the rows that could not be read are exactly the ones mid-teardown.
func TestOrphanTrackingRefusesToSweepWhenInFlightDeletesAreUnreadable(t *testing.T) {
	if _, ok := trackedBackendIDs(
		context.Background(),
		trackingStore(t),
		&fakeDeleteTracker{err: errors.New("lifecycle database unavailable")},
		quietLog(),
	); ok {
		t.Fatal("the sweep was allowed to run on a set that omits in-flight deletes")
	}
}

// Lifecycle disabled is the unchanged pre-existing behaviour.
func TestOrphanTrackingIsUnchangedWithoutLifecycle(t *testing.T) {
	tracked, ok := trackedBackendIDs(context.Background(), trackingStore(t), nil, quietLog())
	if !ok || !tracked["linode_live"] || len(tracked) != 1 {
		t.Fatalf("tracked = %v ok = %v, want exactly the live burst", tracked, ok)
	}
}

func TestLegacySweepRunsOnlyWithoutLifecycleReconciliation(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{
		"if lifecycleStore != nil {",
		"dec.ReconcileProviders(reconcileCtx",
		"} else if tracked, ok := trackedBackendIDs(reconcileCtx, store, nil, log); ok {",
		"legacy orphan sweep worker started",
		"trackedBackendIDs(sweepCtx, store, nil, log)",
		"dec.SweepOrphans(sweepCtx, tracked)",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("legacy/lifecycle orphan path wiring missing %q", required)
		}
	}
}

// The runtime/migration split: this process verifies the migrated schema and
// holds no DDL. A runtime role that could reshape the delete state machine
// could also reshape the record that says a paid resource still exists.
func TestLifecycleRuntimeIsExplicitAndDoesNotRunDDL(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{
		"LIFECYCLE_DATABASE_URL",
		"lifecycle.OpenStore",
		"handlers.ProviderDeleteWorker",
		"GET /v1/admin/provider-deletes/manual-attention",
		"POST /v1/admin/provider-deletes/retry",
		"handlers.AdminAuth",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("lifecycle runtime wiring missing %q", required)
		}
	}
	for _, forbidden := range []string{"lifecycle.EnsureSchema", "lifecycle.EnsureProviderDeleteSchema"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("production runtime calls %q", forbidden)
		}
	}
	// Create and delete are two halves of ONE lifecycle aggregate: admission
	// writes the burst row the authoritative delete later projects onto. One
	// verified store, one pool, one runtime identity.
	if got := strings.Count(text, "lifecycle.OpenStore"); got != 1 {
		t.Fatalf("lifecycle.OpenStore is called %d times; the admission path must reuse the one verified store", got)
	}
}

// Authoritative admission is an explicit opt-in that must be BACKED by the
// migrated, least-privilege lifecycle database. Silently downgrading to the
// inline path would give an operator who asked for durable intent a create that
// has none; silently accepting DATABASE_URL would run the authoritative machine
// on the state store's credential, which is not the role OpenStore audits.
func TestLifecycleAdmissionRequiresTheOpenedLifecycleStore(t *testing.T) {
	t.Setenv("LIFECYCLE_AUTHORITATIVE_ADMISSION", "true")
	t.Setenv("DATABASE_URL", "postgres://state/db")

	if _, err := lifecycleAdmissionFromEnv(false); err == nil ||
		!strings.Contains(err.Error(), "requires LIFECYCLE_DATABASE_URL") {
		t.Fatalf("admission without an open lifecycle store = %v, want a startup configuration error", err)
	}
	if !strings.Contains(mustAdmissionError(t, false), "DATABASE_URL is not a substitute") {
		t.Fatal("the refusal does not say why the state store's credential is not accepted")
	}

	enabled, err := lifecycleAdmissionFromEnv(true)
	if err != nil || !enabled {
		t.Fatalf("admission with the opened store = %v (err %v), want enabled", enabled, err)
	}
}

func TestLifecycleAdmissionGateIsExplicit(t *testing.T) {
	for value, want := range map[string]bool{"": false, "false": false} {
		t.Setenv("LIFECYCLE_AUTHORITATIVE_ADMISSION", value)
		// Unset or off never fails, and never enables, even with a store open.
		enabled, err := lifecycleAdmissionFromEnv(true)
		if err != nil || enabled != want {
			t.Fatalf("%q = %v (err %v), want %v", value, enabled, err, want)
		}
	}
	t.Setenv("LIFECYCLE_AUTHORITATIVE_ADMISSION", "yes")
	if _, err := lifecycleAdmissionFromEnv(true); err == nil {
		t.Fatal("an unrecognised gate value was accepted")
	}
}

func mustAdmissionError(t *testing.T, storeOpen bool) string {
	t.Helper()
	_, err := lifecycleAdmissionFromEnv(storeOpen)
	if err == nil {
		t.Fatal("expected a configuration error")
	}
	return err.Error()
}
