package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// cancelReq builds a DELETE /v1/workloads/{id} authenticated as the given
// customer — the same shape the Auth middleware produces for the agent DELETE
// that fires when the customer removes the Workload CR.
func cancelReq(cust *state.Customer, id string) *http.Request {
	r := httptest.NewRequest(http.MethodDelete, "/v1/workloads/"+id, nil)
	r.SetPathValue("id", id)
	return r.WithContext(context.WithValue(r.Context(), ctxCustomer, cust))
}

// cancelStore mirrors completeStore: one running workload behind one live
// burst, which is exactly the state a cancel arrives into during the issue #93
// race — the burst row is still present because reap has only just enqueued
// its authoritative delete.
func cancelStore(t *testing.T) (*state.Store, *Workloads, *state.Customer, *fakeReaper) {
	t.Helper()
	store := state.New()
	cust, err := store.CustomerByID(state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	store.PutWorkload(&state.Workload{ID: "wl1", CustomerID: cust.ID, Status: "running", BurstID: "b1"})
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: cust.ID, CreatedAt: time.Now()})
	reaper := &fakeReaper{}
	return store, &Workloads{Store: store, Reaper: reaper, Log: quietLog()}, cust, reaper
}

// Issue #93: agent DELETE racing a completed Job. Complete already terminalised
// the workload as succeeded (with its receipt) and enqueued the reap; the burst
// row is still present. A moment later the agent sees the customer's Workload
// CR delete and calls Cancel. Cancel must NOT overwrite the terminal record:
// the succeeded status, its finished_at, and the outcome that explains it all
// stand. The reap still runs — it is idempotent.
func TestCancelAfterSucceededPreservesTerminalRecord(t *testing.T) {
	store, h, cust, reaper := cancelStore(t)
	completedAt := time.Now().Add(-time.Second).UTC()
	outcome := &state.WorkloadOutcome{
		Compute: state.WorkloadComputeOutcome{Result: state.WorkloadResultSucceeded},
	}
	if !store.FinishWorkloadWithOutcome("wl1", "succeeded", completedAt, false, outcome) {
		t.Fatal("seed: FinishWorkloadWithOutcome should apply to a running workload")
	}

	rec := httptest.NewRecorder()
	h.Cancel(rec, cancelReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Cancel = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}

	wl, err := store.GetWorkload("wl1")
	if err != nil {
		t.Fatal(err)
	}
	if wl.Status != "succeeded" {
		t.Errorf("status = %q, want the earlier observation's succeeded", wl.Status)
	}
	if wl.FinishedAt == nil || !wl.FinishedAt.Equal(completedAt) {
		t.Errorf("finished_at = %v, want the original %v; a late cancel moved the terminal timestamp",
			wl.FinishedAt, completedAt)
	}
	if wl.Outcome == nil || wl.Outcome.Compute.Result != state.WorkloadResultSucceeded {
		t.Errorf("outcome = %+v, want the receipt that explained succeeded", wl.Outcome)
	}

	// The reap is idempotent and still runs — the whole point of the split is
	// that a late cancel does not skip cleanup on an already-terminal workload.
	if !reaper.reaped("b1") {
		t.Error("reap did not run; the burst was left live behind a terminal workload")
	}
	if _, err := store.GetBurst("b1"); err == nil {
		t.Error("burst record survived a reap that returned success")
	}
}

// Same rule for a failed run: the cancel that races a Complete("failed") must
// keep failed, its finished_at, and its receipt.
func TestCancelAfterFailedPreservesTerminalRecord(t *testing.T) {
	store, h, cust, reaper := cancelStore(t)
	completedAt := time.Now().Add(-2 * time.Second).UTC()
	outcome := &state.WorkloadOutcome{
		Compute: state.WorkloadComputeOutcome{Result: state.WorkloadResultFailed, Reason: "exit code 137"},
	}
	if !store.FinishWorkloadWithOutcome("wl1", "failed", completedAt, false, outcome) {
		t.Fatal("seed: FinishWorkloadWithOutcome should apply to a running workload")
	}

	rec := httptest.NewRecorder()
	h.Cancel(rec, cancelReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Cancel = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}

	wl, err := store.GetWorkload("wl1")
	if err != nil {
		t.Fatal(err)
	}
	if wl.Status != "failed" {
		t.Errorf("status = %q, want the earlier observation's failed", wl.Status)
	}
	if wl.FinishedAt == nil || !wl.FinishedAt.Equal(completedAt) {
		t.Errorf("finished_at = %v, want the original %v", wl.FinishedAt, completedAt)
	}
	if wl.Outcome == nil || wl.Outcome.Compute.Result != state.WorkloadResultFailed ||
		wl.Outcome.Compute.Reason != "exit code 137" {
		t.Errorf("outcome = %+v, want the failure receipt intact", wl.Outcome)
	}

	if !reaper.reaped("b1") {
		t.Error("reap did not run behind a terminal-failed workload")
	}
}

// The preservation guard must not stop cancel from terminalising a workload
// that never finished: a running (or provisioning) workload still becomes
// cancelled and the burst is still reaped.
func TestCancelStillTerminalisesARunningWorkload(t *testing.T) {
	store, h, cust, reaper := cancelStore(t)

	rec := httptest.NewRecorder()
	h.Cancel(rec, cancelReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Cancel = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}

	wl, err := store.GetWorkload("wl1")
	if err != nil {
		t.Fatal(err)
	}
	if wl.Status != "cancelled" {
		t.Errorf("status = %q, want cancelled", wl.Status)
	}
	if wl.FinishedAt == nil {
		t.Error("finished_at = nil, want a stamped terminal time")
	}
	if !reaper.reaped("b1") {
		t.Error("cancel of a running workload did not reap its burst")
	}
}

// A repeat cancel after the burst is already reaped is the existing "already
// gone" branch — the terminal record still stands, and the response is the
// idempotent already_reaped answer. Pinned so the two branches keep the same
// invariant.
func TestCancelAfterSucceededIsIdempotentWhenBurstAlreadyGone(t *testing.T) {
	store, h, cust, _ := cancelStore(t)
	completedAt := time.Now().Add(-time.Second).UTC()
	outcome := &state.WorkloadOutcome{
		Compute: state.WorkloadComputeOutcome{Result: state.WorkloadResultSucceeded},
	}
	store.FinishWorkloadWithOutcome("wl1", "succeeded", completedAt, false, outcome)
	if err := store.DeleteBurst("b1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordBurstReap(context.Background(), "b1", cust.ID); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	h.Cancel(rec, cancelReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Cancel = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, "already_reaped") {
		t.Fatalf("body = %q, want already_reaped", body)
	}

	wl, _ := store.GetWorkload("wl1")
	if wl.Status != "succeeded" || wl.FinishedAt == nil || !wl.FinishedAt.Equal(completedAt) {
		t.Errorf("workload = %+v, want succeeded + original finished_at", wl)
	}
	if wl.Outcome == nil || wl.Outcome.Compute.Result != state.WorkloadResultSucceeded {
		t.Errorf("outcome = %+v, want the receipt intact", wl.Outcome)
	}
}
