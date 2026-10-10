package agent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"unicode"
	"unicode/utf8"
)

// centralReasonCapBytes mirrors maxCompleteReasonBytes in central's completion
// handler: a reason above it is a 400, which would strand the burst.
const centralReasonCapBytes = 512

// completeBody captures exactly what the agent posted, so the encoding is
// pinned rather than inferred. Central decodes this body with unknown fields
// rejected at every object level.
func completeBody(t *testing.T, phase string, outcome *WorkloadOutcome, status int) string {
	t.Helper()
	var body, path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		body, path = string(raw), r.URL.Path
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content type = %q, want application/json", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("authorization = %q, want the bearer token", got)
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()

	err := NewCentralHTTPClient(srv.URL, "tok", "cluster_a").
		ReportWorkloadDone(context.Background(), "wl_1", phase, outcome)
	if status < 300 && err != nil {
		t.Fatalf("ReportWorkloadDone: %v", err)
	}
	if path != "/v1/workloads/wl_1/complete" {
		t.Fatalf("path = %q, want the completion endpoint", path)
	}
	return body
}

// The legacy body: an agent with nothing it can honestly report sends the phase
// alone, and central stores no receipt for it.
func TestReportWorkloadDoneOmitsAbsentOutcome(t *testing.T) {
	if got := completeBody(t, "Succeeded", nil, http.StatusOK); got != `{"phase":"Succeeded"}` {
		t.Errorf("body = %s, want the phase-only body", got)
	}
}

func TestReportWorkloadDoneEncodesComputeOnlyOutcome(t *testing.T) {
	got := completeBody(t, "Failed", &WorkloadOutcome{
		Compute: ComputeOutcome{Result: OutcomeResultFailed, Reason: "the job exhausted its retry budget"},
	}, http.StatusOK)

	want := `{"phase":"Failed","outcome":{"compute":{"result":"failed",` +
		`"reason":"the job exhausted its retry budget"}}}`
	if got != want {
		t.Errorf("body  = %s\nwant = %s", got, want)
	}
}

// The full shape: a compute that worked, an export that did not, and counts
// that are present because they were counted — including zeros.
func TestReportWorkloadDoneEncodesArtifactOutcome(t *testing.T) {
	got := completeBody(t, "Failed", &WorkloadOutcome{
		Compute: ComputeOutcome{Result: OutcomeResultSucceeded},
		Artifacts: &ArtifactOutcome{
			Result:          OutcomeResultFailed,
			Reason:          artifactFailureReasons[artifactFailureUpload],
			ObjectsUploaded: int64Ref(0),
			BytesUploaded:   int64Ref(0),
		},
	}, http.StatusOK)

	want := `{"phase":"Failed","outcome":{"compute":{"result":"succeeded"},` +
		`"artifacts":{"result":"failed","reason":"an artifact upload to the object store failed",` +
		`"objects_uploaded":0,"bytes_uploaded":0}}}`
	if got != want {
		t.Errorf("body  = %s\nwant = %s", got, want)
	}
}

// A skipped export carries no counts at all: central rejects a positive count
// on a skipped export, and a zero would claim an upload that never ran.
func TestReportWorkloadDoneEncodesSkippedArtifacts(t *testing.T) {
	got := completeBody(t, "Failed", &WorkloadOutcome{
		Compute:   ComputeOutcome{Result: OutcomeResultFailed},
		Artifacts: &ArtifactOutcome{Result: OutcomeResultSkipped, Reason: skippedArtifactReason},
	}, http.StatusOK)

	want := `{"phase":"Failed","outcome":{"compute":{"result":"failed"},` +
		`"artifacts":{"result":"skipped","reason":"` + skippedArtifactReason + `"}}}`
	if got != want {
		t.Errorf("body  = %s\nwant = %s", got, want)
	}
}

// A receipt central refuses must surface as an error, not as a silent success:
// the watcher leaves the workload unreported so the resync retries it, and a
// burst that is still billing is not written off as reported.
func TestReportWorkloadDoneSurfacesCentralRejection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "outcome.compute.result must be succeeded or failed", http.StatusBadRequest)
	}))
	defer srv.Close()

	err := NewCentralHTTPClient(srv.URL, "tok", "cluster_a").ReportWorkloadDone(
		context.Background(), "wl_1", "Succeeded",
		&WorkloadOutcome{Compute: ComputeOutcome{Result: OutcomeResultSucceeded}})
	if err == nil {
		t.Fatal("a rejected receipt was reported as delivered")
	}
	if !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "outcome.compute.result") {
		t.Errorf("error = %v, want central's status and reason", err)
	}
}

// Central answers a completion with 200 or 202 depending on the reap path; both
// mean delivered.
func TestReportWorkloadDoneAcceptsAccepted(t *testing.T) {
	completeBody(t, "Succeeded", &WorkloadOutcome{
		Compute: ComputeOutcome{Result: OutcomeResultSucceeded},
	}, http.StatusAccepted)
}

// An unreachable central is an error the watcher retries, not a delivery.
func TestReportWorkloadDoneSurfacesTransportFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close() // nothing is listening

	err := NewCentralHTTPClient(srv.URL, "tok", "cluster_a").ReportWorkloadDone(
		context.Background(), "wl_1", "Succeeded",
		&WorkloadOutcome{Compute: ComputeOutcome{Result: OutcomeResultSucceeded}})
	if err == nil {
		t.Fatal("an undelivered report was treated as delivered")
	}
}

// completeAttempts posts one completion against a server that answers the Nth
// request with statuses[N], and returns every body it received, in order. A
// request past the end of statuses is still recorded and answered 200, so a
// test that expects no retry fails on the extra body rather than passing
// because the server happened to refuse it too.
func completeAttempts(t *testing.T, phase string, outcome *WorkloadOutcome, statuses ...int) ([]string, error) {
	t.Helper()
	bodies, _, err := completeAttemptsLogged(t, phase, outcome, statuses...)
	return bodies, err
}

// completeAttemptsLogged is completeAttempts plus everything the client logged
// while it ran, so a test can assert on the log the way an operator reads it.
func completeAttemptsLogged(t *testing.T, phase string, outcome *WorkloadOutcome, statuses ...int) ([]string, string, error) {
	t.Helper()
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		mu.Lock()
		attempt := len(bodies)
		bodies = append(bodies, string(raw))
		mu.Unlock()

		status := http.StatusOK
		if attempt < len(statuses) {
			status = statuses[attempt]
		}
		if status != http.StatusOK {
			http.Error(w, `json: unknown field "outcome"`, status)
			return
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()

	var logged bytes.Buffer
	client := NewCentralHTTPClient(srv.URL, "tok", "cluster_a")
	client.Log = slog.New(slog.NewTextHandler(&logged, nil))
	err := client.ReportWorkloadDone(context.Background(), "wl_1", phase, outcome)
	mu.Lock()
	defer mu.Unlock()
	return slices.Clone(bodies), logged.String(), err
}

// A connector that upgraded ahead of its central posts a receipt central has
// never heard of, and central's strict decoder answers 400. The run is still
// reportable: the one retry carries the phase alone, which is the body central
// has always accepted, and the completion lands.
func TestReportWorkloadDoneFallsBackToThePhaseOnlyBodyOn400(t *testing.T) {
	bodies, err := completeAttempts(t, "Failed", &WorkloadOutcome{
		Compute: ComputeOutcome{Result: OutcomeResultSucceeded},
		Artifacts: &ArtifactOutcome{
			Result: OutcomeResultFailed,
			Reason: artifactFailureReasons[artifactFailureUpload],
		},
	}, http.StatusBadRequest, http.StatusOK)
	if err != nil {
		t.Fatalf("the phase-only retry was not treated as delivered: %v", err)
	}

	want := []string{
		`{"phase":"Failed","outcome":{"compute":{"result":"succeeded"},` +
			`"artifacts":{"result":"failed","reason":"an artifact upload to the object store failed"}}}`,
		`{"phase":"Failed"}`,
	}
	if !slices.Equal(bodies, want) {
		t.Errorf("bodies = %q\nwant   = %q", bodies, want)
	}
}

// Exactly one retry. A central that refuses the phase-only body too is
// refusing the completion on its merits, and the error has to carry both
// answers or the operator sees a 400 with no sign of which body earned it.
func TestReportWorkloadDoneRetriesTheLegacyBodyExactlyOnce(t *testing.T) {
	bodies, err := completeAttempts(t, "Succeeded", &WorkloadOutcome{
		Compute: ComputeOutcome{Result: OutcomeResultSucceeded},
	}, http.StatusBadRequest, http.StatusBadRequest)
	if err == nil {
		t.Fatal("a refused completion was reported as delivered")
	}
	if len(bodies) != 2 {
		t.Fatalf("attempts = %d, want 2 (the receipt and one phase-only retry): %q", len(bodies), bodies)
	}
	if bodies[1] != `{"phase":"Succeeded"}` {
		t.Errorf("retry body = %s, want the phase-only body", bodies[1])
	}
	if !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "retry") {
		t.Errorf("error = %v, want central's status and the retry it made", err)
	}
}

// The fallback exists for one thing: a central too old to decode the receipt.
// Any other status is central refusing the completion itself, and re-posting a
// thinner body would only lose the receipt on the way to the same refusal.
func TestReportWorkloadDoneDoesNotFallBackOnOtherStatuses(t *testing.T) {
	for _, status := range []int{
		http.StatusUnauthorized, http.StatusNotFound, http.StatusConflict,
		http.StatusRequestEntityTooLarge, http.StatusInternalServerError,
	} {
		bodies, err := completeAttempts(t, "Succeeded", &WorkloadOutcome{
			Compute: ComputeOutcome{Result: OutcomeResultSucceeded},
		}, status)
		if err == nil {
			t.Errorf("status %d was reported as delivered", status)
		}
		if len(bodies) != 1 {
			t.Errorf("status %d produced %d attempts, want 1: %q", status, len(bodies), bodies)
			continue
		}
		if !strings.Contains(bodies[0], `"outcome"`) {
			t.Errorf("status %d: first body = %s, want the receipt", status, bodies[0])
		}
	}
}

// A phase-only completion is already the legacy body, so a 400 on it is
// central rejecting the phase — the same request again would get the same
// answer.
func TestReportWorkloadDoneDoesNotRetryAPhaseOnlyBody(t *testing.T) {
	bodies, err := completeAttempts(t, "Succeeded", nil, http.StatusBadRequest)
	if err == nil {
		t.Fatal("a refused completion was reported as delivered")
	}
	if len(bodies) != 1 {
		t.Fatalf("attempts = %d, want 1: %q", len(bodies), bodies)
	}
	if bodies[0] != `{"phase":"Succeeded"}` {
		t.Errorf("body = %s, want the phase-only body", bodies[0])
	}
}

// The fallback is the one path that reports a run and drops its receipt, and
// this warning is the only record that it happened. It says so once, names the
// workload and the status central answered, and carries nothing else: the body
// central refused, the outcome inside it, what central said about it and the
// endpoint it went to are customer data or an operator's credential surface,
// and a log line outlives the run it describes.
func TestReportWorkloadDoneWarnsOnlyWhenTheFallbackLands(t *testing.T) {
	receipt := &WorkloadOutcome{
		Compute: ComputeOutcome{Result: OutcomeResultSucceeded},
		Artifacts: &ArtifactOutcome{
			Result: OutcomeResultFailed,
			Reason: artifactFailureReasons[artifactFailureUpload],
		},
	}

	t.Run("the phase-only retry landed", func(t *testing.T) {
		bodies, logged, err := completeAttemptsLogged(t, "Failed", receipt,
			http.StatusBadRequest, http.StatusOK)
		if err != nil {
			t.Fatalf("the phase-only retry was not treated as delivered: %v", err)
		}
		if len(bodies) != 2 {
			t.Fatalf("attempts = %d, want 2: %q", len(bodies), bodies)
		}
		if got := strings.Count(logged, "level=WARN"); got != 1 {
			t.Fatalf("the fallback logged %d warnings, want exactly 1:\n%s", got, logged)
		}
		for _, want := range []string{"phase only", "workload=wl_1", "status=400"} {
			if !strings.Contains(logged, want) {
				t.Errorf("the fallback warning is missing %q:\n%s", want, logged)
			}
		}
		// In order: the body central refused, the receipt inside it, the note it
		// carried, central's own answer, the endpoint it went to and the
		// credential that reached it, and any request or response body at all.
		forbidden := []string{
			bodies[0],
			`"compute"`,
			`"artifacts"`,
			"objects_uploaded",
			artifactFailureReasons[artifactFailureUpload],
			"unknown field",
			"http",
			"tok",
			"{",
		}
		for _, leak := range forbidden {
			if strings.Contains(logged, leak) {
				t.Errorf("the fallback warning leaked %q:\n%s", leak, logged)
			}
		}
	})

	// Nothing was downgraded on any of these, so there is nothing to warn about:
	// either the receipt is on the record, or the completion failed outright and
	// the error the caller gets carries it.
	for _, quiet := range []struct {
		name     string
		outcome  *WorkloadOutcome
		statuses []int
	}{
		{"the receipt was accepted", receipt, []int{http.StatusOK}},
		{"the retry was refused too", receipt, []int{http.StatusBadRequest, http.StatusBadRequest}},
		{"a status that is not 400", receipt, []int{http.StatusInternalServerError}},
		{"a phase-only body was refused", nil, []int{http.StatusBadRequest}},
	} {
		t.Run(quiet.name, func(t *testing.T) {
			_, logged, _ := completeAttemptsLogged(t, "Failed", quiet.outcome, quiet.statuses...)
			if logged != "" {
				t.Errorf("no receipt was downgraded, but the client logged:\n%s", logged)
			}
		})
	}
}

// completeTransport answers each request from statuses in order, and counts
// the response bodies that were closed.
type completeTransport struct {
	statuses []int
	err      error
	calls    int
	closed   int
}

func (ct *completeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ct.calls++
	if ct.err != nil {
		return nil, ct.err
	}
	status := http.StatusOK
	if ct.calls-1 < len(ct.statuses) {
		status = ct.statuses[ct.calls-1]
	}
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Request:    req,
		Body:       &countingCloser{Reader: strings.NewReader("invalid completion payload"), closed: &ct.closed},
	}, nil
}

type countingCloser struct {
	io.Reader
	closed *int
}

func (c *countingCloser) Close() error {
	*c.closed++
	return nil
}

func newTransportClient(t *testing.T, rt *completeTransport) *CentralHTTPClient {
	t.Helper()
	c := NewCentralHTTPClient("http://central.invalid", "tok", "cluster_a")
	c.HTTP = &http.Client{Transport: rt}
	return c
}

// A transport failure never reached a decoder, so it says nothing about the
// body's shape. Spending the retry on it would double the requests every
// unreachable central sees, and the watcher already retries the whole report.
func TestReportWorkloadDoneDoesNotFallBackOnTransportFailure(t *testing.T) {
	rt := &completeTransport{err: errors.New("connection refused")}
	err := newTransportClient(t, rt).ReportWorkloadDone(context.Background(), "wl_1", "Succeeded",
		&WorkloadOutcome{Compute: ComputeOutcome{Result: OutcomeResultSucceeded}})
	if err == nil {
		t.Fatal("an undelivered report was treated as delivered")
	}
	if rt.calls != 1 {
		t.Errorf("attempts = %d, want 1", rt.calls)
	}
}

// Both attempts run through one client, so a body left open holds its
// connection out of the pool for the life of the agent.
func TestReportWorkloadDoneClosesEveryResponseBody(t *testing.T) {
	rt := &completeTransport{statuses: []int{http.StatusBadRequest, http.StatusOK}}
	if err := newTransportClient(t, rt).ReportWorkloadDone(context.Background(), "wl_1", "Succeeded",
		&WorkloadOutcome{Compute: ComputeOutcome{Result: OutcomeResultSucceeded}}); err != nil {
		t.Fatalf("ReportWorkloadDone: %v", err)
	}
	if rt.calls != 2 {
		t.Fatalf("attempts = %d, want 2", rt.calls)
	}
	if rt.closed != 2 {
		t.Errorf("closed %d of %d response bodies", rt.closed, rt.calls)
	}
}

// Every note the agent can put on a receipt comes from these tables, and the
// tables are the whole reason no destination, credential, or log line can reach
// a record that outlives the run. Anything that reads like one is a bug here.
func TestOutcomeReasonsAreSafe(t *testing.T) {
	reasons := []string{genericArtifactFailureReason, missingArtifactSummaryReason, skippedArtifactReason}
	for _, reason := range artifactFailureReasons {
		reasons = append(reasons, reason)
	}
	for _, reason := range computeFailureReasons {
		reasons = append(reasons, reason)
	}

	for _, reason := range reasons {
		if reason == "" {
			t.Error("a reason table holds an empty phrase")
			continue
		}
		if len(reason) > centralReasonCapBytes {
			t.Errorf("reason %q is %d bytes, over central's cap", reason, len(reason))
		}
		if !utf8.ValidString(reason) {
			t.Errorf("reason %q is not valid UTF-8", reason)
		}
		for _, r := range reason {
			if unicode.IsControl(r) {
				t.Errorf("reason %q carries a control character", reason)
				break
			}
		}
		// A URL, a path, a key/value pair or a host would all have to arrive
		// through one of these.
		for _, marker := range []string{"://", "@", "/", "\\", "=", ":"} {
			if strings.Contains(reason, marker) {
				t.Errorf("reason %q contains %q — it is not a plain phrase", reason, marker)
			}
		}
	}
}

// artifactFailureReason is what stands between an unrecognised category token
// and a stored note. Unknown tokens get the generic phrase; they are never
// echoed.
func TestArtifactFailureReasonNeverEchoesUnknownTokens(t *testing.T) {
	for _, token := range []string{"", "unknown", "https://signed.example/put?sig=abc", "s3://customer-bucket"} {
		if got := artifactFailureReason(token); got != genericArtifactFailureReason {
			t.Errorf("artifactFailureReason(%q) = %q, want the generic phrase", token, got)
		}
	}
	if got := artifactFailureReason(artifactFailureSigner); got != artifactFailureReasons[artifactFailureSigner] {
		t.Errorf("known category = %q, want its own phrase", got)
	}
}
