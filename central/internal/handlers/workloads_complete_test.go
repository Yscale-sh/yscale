package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func completeReq(cust *state.Customer, id, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/workloads/"+id+"/complete", strings.NewReader(body))
	r.SetPathValue("id", id)
	return r.WithContext(context.WithValue(r.Context(), ctxCustomer, cust))
}

// controlReasonBody renders a completion body whose compute or artifact reason
// carries one control character, marshalled the way a real client would send it.
// A RAW control byte never survives the JSON decoder, so the encoder's \u escape
// is the only route one has into a stored reason — which is exactly why the byte
// cap alone does not bound what lands in the record.
func controlReasonBody(field string, r rune) string {
	note := "exit code 137" + string(r)
	req := CompleteRequest{Phase: "Succeeded", Outcome: &CompleteOutcome{
		Compute: &CompleteComputeOutcome{Result: state.WorkloadResultSucceeded},
	}}
	if field == "artifacts" {
		// A succeeded phase with a failed export is allowed, so the reason is the
		// only thing left for the handler to refuse.
		req.Outcome.Artifacts = &CompleteArtifactOutcome{Result: state.WorkloadResultFailed, Reason: note}
	} else {
		req.Outcome.Compute.Reason = note
	}
	b, err := json.Marshal(req)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestCompleteRejectsInvalidPayloadWithoutReaping(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "empty", body: "", want: http.StatusBadRequest},
		{name: "malformed", body: `{`, want: http.StatusBadRequest},
		{name: "missing phase", body: `{}`, want: http.StatusBadRequest},
		{name: "unknown phase", body: `{"phase":"Cancelled"}`, want: http.StatusBadRequest},
		{name: "wrong case", body: `{"phase":"succeeded"}`, want: http.StatusBadRequest},
		{name: "unknown field", body: `{"phase":"Succeeded","trusted":true}`, want: http.StatusBadRequest},
		{name: "trailing JSON", body: `{"phase":"Succeeded"}{}`, want: http.StatusBadRequest},
		{name: "oversized", body: `{"phase":"Succeeded","padding":"` + strings.Repeat("x", 1<<16) + `"}`, want: http.StatusRequestEntityTooLarge},

		// A receipt is an observation central publishes as its own record, so
		// every part of it is checked before anything is written or reaped.
		{name: "outcome without compute", body: `{"phase":"Succeeded","outcome":{}}`, want: http.StatusBadRequest},
		{name: "null compute", body: `{"phase":"Succeeded","outcome":{"compute":null}}`, want: http.StatusBadRequest},
		{name: "empty compute result", body: `{"phase":"Succeeded","outcome":{"compute":{"result":""}}}`, want: http.StatusBadRequest},
		{name: "unknown compute result", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"Succeeded"}}}`, want: http.StatusBadRequest},
		{name: "unknown artifact result", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded"},"artifacts":{"result":"partial"}}}`, want: http.StatusBadRequest},
		{name: "compute skipped", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"skipped"}}}`, want: http.StatusBadRequest},

		// Phase and receipt must not contradict each other.
		{name: "succeeded phase with failed compute", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"failed"}}}`, want: http.StatusBadRequest},
		{name: "failed phase with nothing failed", body: `{"phase":"Failed","outcome":{"compute":{"result":"succeeded"}}}`, want: http.StatusBadRequest},
		{name: "failed phase with succeeded export", body: `{"phase":"Failed","outcome":{"compute":{"result":"succeeded"},"artifacts":{"result":"succeeded"}}}`, want: http.StatusBadRequest},
		{name: "failed phase with skipped export", body: `{"phase":"Failed","outcome":{"compute":{"result":"succeeded"},"artifacts":{"result":"skipped"}}}`, want: http.StatusBadRequest},

		// Counts are counts.
		{name: "negative object count", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded"},"artifacts":{"result":"succeeded","objects_uploaded":-1}}}`, want: http.StatusBadRequest},
		{name: "negative byte count", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded"},"artifacts":{"result":"succeeded","bytes_uploaded":-1}}}`, want: http.StatusBadRequest},
		{name: "skipped export that uploaded", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded"},"artifacts":{"result":"skipped","objects_uploaded":2}}}`, want: http.StatusBadRequest},

		// Bounded strings: the 64 KiB body cap is not a bound on what is stored.
		{name: "oversized compute reason", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded","reason":"` + strings.Repeat("x", maxCompleteReasonBytes+1) + `"}}}`, want: http.StatusBadRequest},
		{name: "oversized artifact reason", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded"},"artifacts":{"result":"failed","reason":"` + strings.Repeat("x", maxCompleteReasonBytes+1) + `"}}}`, want: http.StatusBadRequest},

		// A reason is stored for the life of the tenant and rendered straight
		// back, so it is held to the stance a tenant name is held to: text, not
		// a control stream aimed at whatever reads the record later.
		{name: "NUL in compute reason", body: controlReasonBody("compute", 0x00), want: http.StatusBadRequest},
		{name: "newline in compute reason", body: controlReasonBody("compute", '\n'), want: http.StatusBadRequest},
		{name: "ANSI escape in compute reason", body: controlReasonBody("compute", 0x1b), want: http.StatusBadRequest},
		{name: "DEL in compute reason", body: controlReasonBody("compute", 0x7f), want: http.StatusBadRequest},
		{name: "NUL in artifact reason", body: controlReasonBody("artifacts", 0x00), want: http.StatusBadRequest},
		{name: "carriage return in artifact reason", body: controlReasonBody("artifacts", '\r'), want: http.StatusBadRequest},
		{name: "ANSI escape in artifact reason", body: controlReasonBody("artifacts", 0x1b), want: http.StatusBadRequest},

		// Unknown fields are refused at every level of the receipt, not just the
		// top of the body — a destination or a credential smuggled into a nested
		// object must not be stored because nobody declared the field.
		{name: "unknown field in outcome", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded"},"bucket":"s3://x"}}`, want: http.StatusBadRequest},
		{name: "unknown field in compute", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded","exit_code":0}}}`, want: http.StatusBadRequest},
		{name: "unknown field in artifacts", body: `{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded"},"artifacts":{"result":"succeeded","endpoint":"https://x"}}}`, want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := state.New()
			cust, err := store.CustomerByID(state.DevCustomerID)
			if err != nil {
				t.Fatal(err)
			}
			store.PutWorkload(&state.Workload{ID: "wl1", CustomerID: cust.ID, Status: "running", BurstID: "b1"})
			store.PutBurst(&state.Burst{ID: "b1", CustomerID: cust.ID, CreatedAt: time.Now()})
			reaper := &fakeReaper{}
			h := &Workloads{Store: store, Reaper: reaper, Log: quietLog()}

			rec := httptest.NewRecorder()
			h.Complete(rec, completeReq(cust, "wl1", tt.body))
			if rec.Code != tt.want {
				t.Fatalf("Complete status = %d, want %d; body=%q", rec.Code, tt.want, rec.Body.String())
			}
			if reaper.teardownCount("b1") != 0 {
				t.Fatal("invalid completion must not reap the burst")
			}
			if _, err := store.GetBurst("b1"); err != nil {
				t.Fatal("invalid completion removed the burst")
			}
			wl, err := store.GetWorkload("wl1")
			if err != nil || wl.Status != "running" || wl.FinishedAt != nil {
				t.Fatalf("invalid completion mutated workload: workload=%+v err=%v", wl, err)
			}
		})
	}
}

// The NUL cases above are only meaningful if the NUL really does reach the
// handler, so pin the wire form: the encoder writes it as the six-character
// escape, and the decoder hands the raw rune to the validator on the far side.
func TestControlReasonBodyCarriesEscapedNUL(t *testing.T) {
	escaped := `\u` + "0000"
	body := controlReasonBody("compute", 0x00)
	if !strings.Contains(body, escaped) {
		t.Fatalf("body = %q, want the NUL carried as %s", body, escaped)
	}
	var back CompleteRequest
	if err := json.Unmarshal([]byte(body), &back); err != nil {
		t.Fatalf("the escaped body must still decode: %v", err)
	}
	if back.Outcome == nil || back.Outcome.Compute == nil {
		t.Fatalf("decoded body = %+v, want the compute receipt", back)
	}
	if !strings.ContainsRune(back.Outcome.Compute.Reason, 0x00) {
		t.Fatalf("decoded reason = %q, want it to carry the NUL", back.Outcome.Compute.Reason)
	}
}

// encoding/json coerces every unpaired surrogate and malformed byte in a string
// to U+FFFD, so invalid UTF-8 cannot reach the field through a request body.
// The check is exercised on the validator itself — the same seam any non-HTTP
// caller reaches — and the byte cap has to keep behaving as a byte cap beside it.
func TestCompleteReasonRejectsInvalidUTF8AndKeepsByteCap(t *testing.T) {
	malformed := "container exited \xff\xfe"
	if _, err := completeReason("outcome.compute.reason", malformed); err == nil {
		t.Error("invalid UTF-8 reason accepted")
	}
	if _, err := workloadOutcome("Succeeded", &CompleteOutcome{
		Compute: &CompleteComputeOutcome{Result: state.WorkloadResultSucceeded, Reason: malformed},
	}); err == nil {
		t.Error("workloadOutcome accepted an invalid-UTF-8 compute reason")
	}
	if _, err := workloadOutcome("Succeeded", &CompleteOutcome{
		Compute:   &CompleteComputeOutcome{Result: state.WorkloadResultSucceeded},
		Artifacts: &CompleteArtifactOutcome{Result: state.WorkloadResultFailed, Reason: malformed},
	}); err == nil {
		t.Error("workloadOutcome accepted an invalid-UTF-8 artifact reason")
	}

	// The stance is malformed bytes and control characters, not non-ASCII: a
	// legitimate note survives unchanged.
	text := "conteneur arrêté — code 137 ✓"
	if got, err := completeReason("outcome.compute.reason", text); err != nil || got != text {
		t.Errorf("completeReason(%q) = %q, %v; want it accepted unchanged", text, got, err)
	}
	if _, err := completeReason("outcome.compute.reason", strings.Repeat("x", maxCompleteReasonBytes)); err != nil {
		t.Errorf("a reason exactly at the cap was rejected: %v", err)
	}
	if _, err := completeReason("outcome.compute.reason", strings.Repeat("x", maxCompleteReasonBytes+1)); err == nil {
		t.Error("a reason one byte over the cap was accepted")
	}
}

// completeStore builds a store + handler holding one running workload behind
// one live burst — the state every completion report arrives into.
func completeStore(t *testing.T) (*state.Store, *Workloads, *state.Customer) {
	t.Helper()
	store := state.New()
	cust, err := store.CustomerByID(state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	store.PutWorkload(&state.Workload{ID: "wl1", CustomerID: cust.ID, Status: "running", BurstID: "b1"})
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: cust.ID, CreatedAt: time.Now()})
	return store, &Workloads{Store: store, Reaper: &fakeReaper{}, Log: quietLog()}, cust
}

func mustComplete(t *testing.T, h *Workloads, cust *state.Customer, body string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.Complete(rec, completeReq(cust, "wl1", body))
	if rec.Code != http.StatusOK {
		t.Fatalf("Complete status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
}

// The legacy body is every connector that predates the receipt, and it must
// keep working unchanged — including storing no outcome. A fabricated
// "succeeded because the phase says succeeded" would be central inventing an
// observation the agent never made.
func TestCompleteLegacyBodyStoresNoOutcome(t *testing.T) {
	for _, phase := range []string{"Succeeded", "Failed"} {
		t.Run(phase, func(t *testing.T) {
			store, h, cust := completeStore(t)
			mustComplete(t, h, cust, `{"phase":"`+phase+`"}`)

			wl, err := store.GetWorkload("wl1")
			if err != nil {
				t.Fatal(err)
			}
			if wl.Outcome != nil {
				t.Errorf("outcome = %+v, want nil for a legacy body", wl.Outcome)
			}
			wantStatus := "succeeded"
			if phase == "Failed" {
				wantStatus = "failed"
			}
			if wl.Status != wantStatus || wl.FinishedAt == nil {
				t.Errorf("status=%q finishedAt=%v, want %q + set", wl.Status, wl.FinishedAt, wantStatus)
			}
			if _, err := store.GetBurst("b1"); err == nil {
				t.Error("a legacy completion should still reap the burst")
			}
		})
	}
}

// The narrow receipt: the agent observed the Job's result and nothing else,
// because the workload configured no artifact export.
func TestCompleteStoresComputeOnlyOutcome(t *testing.T) {
	store, h, cust := completeStore(t)
	mustComplete(t, h, cust, `{"phase":"Failed","outcome":{"compute":{"result":"failed","reason":"exit code 137"}}}`)

	wl, err := store.GetWorkload("wl1")
	if err != nil {
		t.Fatal(err)
	}
	if wl.Status != "failed" {
		t.Fatalf("status = %q, want failed", wl.Status)
	}
	if wl.Outcome == nil || wl.Outcome.Compute.Result != state.WorkloadResultFailed ||
		wl.Outcome.Compute.Reason != "exit code 137" {
		t.Fatalf("outcome = %+v, want the reported compute failure", wl.Outcome)
	}
	if wl.Outcome.Artifacts != nil {
		t.Errorf("artifacts = %+v, want nil: no export was reported", wl.Outcome.Artifacts)
	}
}

// The case the receipt exists for: the Job did its work and the outputs never
// reached the customer's bucket. Both halves are recorded, and neither is
// collapsed into the other.
func TestCompleteStoresComputeSuccessWithArtifactFailure(t *testing.T) {
	store, h, cust := completeStore(t)
	mustComplete(t, h, cust, `{"phase":"Failed","outcome":{
		"compute":{"result":"succeeded"},
		"artifacts":{"result":"failed","reason":"destination rejected 2 objects","objects_uploaded":3,"bytes_uploaded":4096}
	}}`)

	wl, err := store.GetWorkload("wl1")
	if err != nil {
		t.Fatal(err)
	}
	if wl.Status != "failed" {
		t.Fatalf("status = %q, want the phase the agent reported", wl.Status)
	}
	if wl.Outcome == nil || wl.Outcome.Compute.Result != state.WorkloadResultSucceeded {
		t.Fatalf("compute = %+v, want succeeded beside a failed run", wl.Outcome)
	}
	a := wl.Outcome.Artifacts
	if a == nil || a.Result != state.WorkloadResultFailed || a.Reason != "destination rejected 2 objects" {
		t.Fatalf("artifacts = %+v, want the reported export failure", a)
	}
	if a.ObjectsUploaded == nil || *a.ObjectsUploaded != 3 || a.BytesUploaded == nil || *a.BytesUploaded != 4096 {
		t.Fatalf("counts = %v / %v, want 3 objects / 4096 bytes", a.ObjectsUploaded, a.BytesUploaded)
	}
}

// A succeeded phase may carry a failed export: whether a lost upload fails the
// run is the agent's call, and refusing the combination would force it to
// misreport one half to record the other.
func TestCompleteAllowsSucceededPhaseWithFailedExport(t *testing.T) {
	store, h, cust := completeStore(t)
	mustComplete(t, h, cust, `{"phase":"Succeeded","outcome":{
		"compute":{"result":"succeeded"},
		"artifacts":{"result":"failed","reason":"upload timed out"}
	}}`)

	wl, _ := store.GetWorkload("wl1")
	if wl.Status != "succeeded" {
		t.Fatalf("status = %q, want succeeded", wl.Status)
	}
	if wl.Outcome.Artifacts == nil || wl.Outcome.Artifacts.Result != state.WorkloadResultFailed {
		t.Fatalf("artifacts = %+v, want the failed export recorded on a succeeded run", wl.Outcome.Artifacts)
	}
	if wl.Outcome.Artifacts.ObjectsUploaded != nil || wl.Outcome.Artifacts.BytesUploaded != nil {
		t.Errorf("counts = %v / %v, want nil: the agent counted nothing",
			wl.Outcome.Artifacts.ObjectsUploaded, wl.Outcome.Artifacts.BytesUploaded)
	}
}

// An export that succeeded and had nothing to upload is a real observation: the
// zero must survive as a count, not decay into "not counted".
func TestCompleteStoresCountedZero(t *testing.T) {
	store, h, cust := completeStore(t)
	mustComplete(t, h, cust, `{"phase":"Succeeded","outcome":{
		"compute":{"result":"succeeded"},
		"artifacts":{"result":"succeeded","objects_uploaded":0,"bytes_uploaded":0}
	}}`)

	wl, _ := store.GetWorkload("wl1")
	a := wl.Outcome.Artifacts
	if a.ObjectsUploaded == nil || *a.ObjectsUploaded != 0 || a.BytesUploaded == nil || *a.BytesUploaded != 0 {
		t.Fatalf("counts = %v / %v, want a stored zero", a.ObjectsUploaded, a.BytesUploaded)
	}
}

// The agent re-delivers on watch resync. A replay lands after the burst is
// already reaped and must not rewrite the first observation's receipt.
func TestCompleteReplayAfterReapKeepsStoredOutcome(t *testing.T) {
	store, h, cust := completeStore(t)
	mustComplete(t, h, cust, `{"phase":"Succeeded","outcome":{
		"compute":{"result":"succeeded"},
		"artifacts":{"result":"succeeded","objects_uploaded":7}
	}}`)

	rec := httptest.NewRecorder()
	h.Complete(rec, completeReq(cust, "wl1", `{"phase":"Failed","outcome":{"compute":{"result":"failed","reason":"replayed"}}}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("replay status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "already_reaped") {
		t.Fatalf("replay body = %q, want already_reaped", rec.Body.String())
	}

	wl, _ := store.GetWorkload("wl1")
	if wl.Status != "succeeded" {
		t.Errorf("status = %q, want the first observation's succeeded", wl.Status)
	}
	if wl.Outcome == nil || wl.Outcome.Compute.Result != state.WorkloadResultSucceeded ||
		wl.Outcome.Compute.Reason != "" {
		t.Fatalf("outcome = %+v, want the first receipt unchanged", wl.Outcome)
	}
	if wl.Outcome.Artifacts == nil || *wl.Outcome.Artifacts.ObjectsUploaded != 7 {
		t.Fatalf("artifacts = %+v, want the first receipt's counts", wl.Outcome.Artifacts)
	}
}

// The receipt is what the tenant read surfaces render, so a stored one has to
// reach them and an absent one must not become an empty object.
func TestStoredOutcomeResponseRendersBothResults(t *testing.T) {
	if got := storedOutcomeResponse(nil); got != nil {
		t.Fatalf("nil receipt rendered as %+v, want nil", got)
	}
	uploaded := int64(3)
	got := storedOutcomeResponse(&state.WorkloadOutcome{
		Compute: state.WorkloadComputeOutcome{Result: state.WorkloadResultSucceeded},
		Artifacts: &state.WorkloadArtifactOutcome{
			Result: state.WorkloadResultFailed, Reason: "timed out", ObjectsUploaded: &uploaded,
		},
	})
	if got.Compute.Result != state.WorkloadResultSucceeded {
		t.Errorf("compute result = %q, want succeeded", got.Compute.Result)
	}
	if got.Artifacts == nil || got.Artifacts.Result != state.WorkloadResultFailed ||
		got.Artifacts.ObjectsUploaded == nil || *got.Artifacts.ObjectsUploaded != 3 {
		t.Fatalf("artifacts = %+v, want the failed export with its count", got.Artifacts)
	}
	if got.Artifacts.BytesUploaded != nil {
		t.Errorf("bytes = %v, want nil rather than a fabricated zero", got.Artifacts.BytesUploaded)
	}
}

// A pending-pod-triggered NodeOnly burst labels its owning batch/v1 Job with
// this workload ID. When CompletionWatcher reports that Job's terminal phase,
// Complete must use the normal reap path just like a yscale-created Job.
func TestCompleteReapsNodeOnlyBurstFromJob(t *testing.T) {
	store := state.New()
	cust, err := store.CustomerByID(state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	store.PutWorkload(&state.Workload{ID: "wl_nodeonly_job", CustomerID: cust.ID, Status: "running", BurstID: "b_nodeonly_job"})
	store.PutBurst(&state.Burst{
		ID: "b_nodeonly_job", CustomerID: cust.ID, CreatedAt: time.Now(), NodeOnly: true,
	})
	reaper := &fakeReaper{}
	h := &Workloads{Store: store, Reaper: reaper, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.Complete(rec, completeReq(cust, "wl_nodeonly_job", `{"phase":"Succeeded"}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("Complete status = %d, want %d; body=%q", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !reaper.reaped("b_nodeonly_job") {
		t.Fatal("terminal Job completion should reap its NodeOnly burst")
	}
	if _, err := store.GetBurst("b_nodeonly_job"); err == nil {
		t.Fatal("NodeOnly burst should be deleted after terminal Job completion")
	}
}
