package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// storedOrigin reads back the submission origin central actually persisted for
// an accepted submission, through the id the caller was handed.
func storedOrigin(t *testing.T, f *idemFixture, rec *httptest.ResponseRecorder) protocol.SubmissionOrigin {
	t.Helper()
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	var body CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	wl, err := f.store.GetWorkload(body.ID)
	if err != nil {
		t.Fatalf("get workload %s: %v", body.ID, err)
	}
	return wl.SubmissionOrigin
}

// No header on a cluster-token request is ambiguous during rolling upgrades:
// it may be a CLI, customer automation, or an old connector. Keep it unlabelled
// rather than inventing API provenance.
func TestCreate_OriginUnknownWithoutClusterHeader(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})

	got := storedOrigin(t, f, f.submit(submitOpts{key: "origin-default-1", cluster: true}))
	if got != "" {
		t.Fatalf("stored origin = %q, want empty", got)
	}
}

// The two values a connector may claim are stored exactly as reported. They
// describe which connector path submitted, and central does not translate them.
func TestCreate_StoresConnectorReportedOrigin(t *testing.T) {
	for _, want := range []protocol.SubmissionOrigin{protocol.OriginPendingPod, protocol.OriginWorkloadCR} {
		t.Run(string(want), func(t *testing.T) {
			f := newIdemFixture(t, &fakeDecider{})

			rec := f.submit(submitOpts{key: "origin-" + string(want), cluster: true, origin: string(want)})
			if got := storedOrigin(t, f, rec); got != want {
				t.Fatalf("stored origin = %q, want %q", got, want)
			}
		})
	}
}

// A value outside the closed set is refused BEFORE anything happens that costs
// the tenant something: no admission decision, no reserved slot, no provider
// call, and nothing written — including the idempotency claim, so the client
// retrying under the same key with a corrected header still wins.
func TestCreate_RejectsUnknownOriginBeforeSideEffects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		origin string
	}{
		{"unrecognised path", "keda"},
		// "api" is what central concludes from silence, never something a caller
		// reports about itself.
		{"the derived default", string(protocol.OriginAPI)},
		{"wrong case", "Pending-Pod"},
		{"padded", " pending-pod"},
		{"oversized", strings.Repeat("p", maxWorkloadOriginBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dec := &fakeDecider{}
			f := newIdemFixture(t, dec)

			rec := f.submit(submitOpts{key: "origin-refused-1", cluster: true, origin: tc.origin})
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
			}
			assertNoSubmitSideEffects(t, f, dec)

			// The key is unspent: this is one client fixing a header, not a
			// second submission.
			corrected := f.submit(submitOpts{
				key: "origin-refused-1", cluster: true, origin: string(protocol.OriginPendingPod),
			})
			if got := storedOrigin(t, f, corrected); got != protocol.OriginPendingPod {
				t.Fatalf("corrected retry stored origin = %q, want %q", got, protocol.OriginPendingPod)
			}
		})
	}
}

// Two headers name two paths, which is no answer at all. Refused on the same
// terms as an unknown one.
func TestCreate_RejectsRepeatedOriginHeader(t *testing.T) {
	dec := &fakeDecider{}
	f := newIdemFixture(t, dec)

	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(idemSpec))
	req.Header.Set(idempotencyHeader, "origin-repeated-1")
	req.Header.Add(protocol.WorkloadOriginHeader, string(protocol.OriginPendingPod))
	req.Header.Add(protocol.WorkloadOriginHeader, string(protocol.OriginWorkloadCR))
	rec := httptest.NewRecorder()
	f.h.Create(rec, req.WithContext(context.WithValue(context.Background(), ctxCustomer, f.cust)))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
	}
	assertNoSubmitSideEffects(t, f, dec)
}

// The header is a CONNECTOR reporting which of its controllers fired. A human
// surface reaches Create with a person in the request context, and a person
// cannot report that — so the claim is refused rather than believed. Without it
// the one field a console shows for "this burst came up on its own" would be
// the one field a browser can write.
func TestCreate_RefusesOriginFromHumanSubmission(t *testing.T) {
	dec := &fakeDecider{}
	f := newIdemFixture(t, dec)

	rec := f.submit(submitOpts{key: "origin-human-1", origin: string(protocol.OriginPendingPod)})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
	}
	assertNoSubmitSideEffects(t, f, dec)

	// The same human submitting without the header is untouched, and records the
	// API path.
	got := storedOrigin(t, f, f.submit(submitOpts{key: "origin-human-1"}))
	if got != protocol.OriginAPI {
		t.Fatalf("human submission origin = %q, want %q", got, protocol.OriginAPI)
	}
}

// assertNoSubmitSideEffects holds a refusal to costing the tenant nothing: the
// four things Create does after this check, in the order it does them.
func assertNoSubmitSideEffects(t *testing.T, f *idemFixture, dec *fakeDecider) {
	t.Helper()
	if reserved := f.h.adm().reservationCount(); reserved != 0 {
		t.Errorf("admission reservations = %d, want 0", reserved)
	}
	if got := dec.planCalls(); got != 0 {
		t.Errorf("Plan calls = %d, want 0 — a refused submission reached a provider", got)
	}
	if got := len(f.store.BurstsForCustomer(f.cust.ID)); got != 0 {
		t.Errorf("bursts = %d, want 0", got)
	}
	if got := len(f.store.WorkloadsForCustomer(f.cust.ID, 10)); got != 0 {
		t.Errorf("workloads = %d, want 0", got)
	}
}

// A legacy record — every workload written before the field existed — stays
// readable and reports nothing. Central cannot learn after the fact which
// controller asked, and "api" there would be an answer nobody gave.
func TestGetWorkload_OmitsOriginOnLegacyRecord(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	f.store.PutWorkload(&state.Workload{
		ID: "wl_legacy_origin", CustomerID: f.cust.ID, Status: "succeeded", BurstID: "burst_legacy",
	})

	rec := getWorkload(t, f, "wl_legacy_origin")
	if _, present := rec["submission_origin"]; present {
		t.Fatalf("legacy record reported an origin: %v", rec["submission_origin"])
	}
}

// A stored origin is rendered verbatim on the single-workload read the console
// and the CLI share.
func TestGetWorkload_ExposesStoredOrigin(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	f.store.PutWorkload(&state.Workload{
		ID: "wl_origin_get", CustomerID: f.cust.ID, Status: "running", BurstID: "burst_origin_get",
		SubmissionOrigin: protocol.OriginPendingPod,
	})

	rec := getWorkload(t, f, "wl_origin_get")
	if got := rec["submission_origin"]; got != string(protocol.OriginPendingPod) {
		t.Fatalf("submission_origin = %v, want %q", got, protocol.OriginPendingPod)
	}
}

func getWorkload(t *testing.T, f *idemFixture, id string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/workloads/"+id, nil)
	req.SetPathValue("id", id)
	rec := httptest.NewRecorder()
	f.h.Get(rec, req.WithContext(context.WithValue(context.Background(), ctxCustomer, f.cust)))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200: %s", rec.Code, rec.Body)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode workload: %v", err)
	}
	return body
}
