package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const fakeLinodeToken = "linode-token-do-not-log-me"

// linodeStub serves canned /linode/instances pages and records what the
// audit actually asked for. Recording is mutex-guarded: the handler runs
// on the server's goroutine and the assertions run on the test's, so
// -race would otherwise flag every observation.
type linodeStub struct {
	pages  map[string]string // page number → JSON body
	status int               // non-zero → serve this status with a chatty body

	mu        sync.Mutex
	authSeen  []string
	pagesSeen []string
	srv       *httptest.Server
}

func (s *linodeStub) observe(auth, page string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.authSeen = append(s.authSeen, auth)
	s.pagesSeen = append(s.pagesSeen, page)
}

func (s *linodeStub) seen() (auth, pages []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.authSeen...), append([]string(nil), s.pagesSeen...)
}

func newLinodeStub(t *testing.T, s *linodeStub) *linodeInventory {
	t.Helper()
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		page := req.URL.Query().Get("page")
		s.observe(req.Header.Get("Authorization"), page)

		if s.status != 0 {
			w.WriteHeader(s.status)
			// Deliberately chatty: a real Linode error body echoes request
			// context back, and the audit must not forward any of it.
			fmt.Fprintf(w, `{"errors":[{"reason":"Invalid Token: %s","field":"authorization"}]}`, fakeLinodeToken)
			return
		}
		body, ok := s.pages[page]
		if !ok {
			t.Errorf("unexpected page request %q", page)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(s.srv.Close)

	inv := newLinodeInventory(fakeLinodeToken)
	inv.baseURL = s.srv.URL
	return inv
}

// TestLinodeInventoryPaginatesAndMatchesExactTags is the core evidence
// contract: an instance counts only when it carries BOTH yscale-burst and
// the FULL burst ID, and the whole account is walked, not just page one.
func TestLinodeInventoryPaginatesAndMatchesExactTags(t *testing.T) {
	stub := &linodeStub{pages: map[string]string{
		"1": `{"page":1,"pages":2,"results":5,"data":[
			{"id":1,"tags":["yscale-burst","burst_abc123def456"]},
			{"id":2,"tags":["yscale-burst"]},
			{"id":3,"tags":["burst_abc123def456"]}
		]}`,
		// id 4 is a prefix near-miss; id 5 lives on page two, which a
		// non-paginating audit would report as absent.
		"2": `{"page":2,"pages":2,"results":5,"data":[
			{"id":4,"tags":["yscale-burst","burst_abc123def456789"]},
			{"id":5,"tags":["other-owner","yscale-burst","burst_abc123def456"]}
		]}`,
	}}
	inv := newLinodeStub(t, stub)

	got, err := inv.CountBurstInstances(context.Background(), testBurstID)
	if err != nil {
		t.Fatalf("audit failed: %v", err)
	}
	if got.Count != 2 {
		t.Fatalf("want 2 owned instances, got %d (ids %v)", got.Count, got.IDs)
	}
	if strings.Join(got.IDs, ",") != "1,5" {
		t.Fatalf("want ids [1 5], got %v — id 2 lacks the burst tag, id 3 lacks the owner tag, id 4 is only a prefix match", got.IDs)
	}
	auth, pages := stub.seen()
	if len(pages) != 2 || pages[0] != "1" || pages[1] != "2" {
		t.Fatalf("want both pages walked, saw %v", pages)
	}
	for _, a := range auth {
		if a != "Bearer "+fakeLinodeToken {
			t.Fatalf("token not sent as a bearer header, got %q", a)
		}
	}
}

func TestLinodeInventoryReportsCleanAccount(t *testing.T) {
	stub := &linodeStub{pages: map[string]string{
		"1": `{"page":1,"pages":1,"results":1,"data":[{"id":7,"tags":["yscale-burst","burst_ffffffffffff"]}]}`,
	}}
	inv := newLinodeStub(t, stub)

	got, err := inv.CountBurstInstances(context.Background(), testBurstID)
	if err != nil {
		t.Fatalf("audit failed: %v", err)
	}
	if got.Count != 0 || len(got.IDs) != 0 {
		t.Fatalf("another burst's instance was counted against this case: %+v", got)
	}
}

// TestLinodeInventoryRedactsAPIErrors pins that a failing audit never
// forwards the API's response body — Linode error payloads echo request
// context, and this string lands in a report.
func TestLinodeInventoryRedactsAPIErrors(t *testing.T) {
	stub := &linodeStub{status: http.StatusUnauthorized}
	inv := newLinodeStub(t, stub)

	_, err := inv.CountBurstInstances(context.Background(), testBurstID)
	if err == nil {
		t.Fatal("a 401 must fail the audit, not report absence")
	}
	msg := err.Error()
	if strings.Contains(msg, fakeLinodeToken) {
		t.Fatalf("error leaked the API token: %q", msg)
	}
	for _, leak := range []string{"Invalid Token", "authorization", "errors"} {
		if strings.Contains(msg, leak) {
			t.Fatalf("error forwarded the response body (%q): %q", leak, msg)
		}
	}
	if !strings.Contains(msg, "401") {
		t.Fatalf("error should keep the status code for triage, got %q", msg)
	}
}

// TestLinodeInventoryRefusesTruncatedListing: hitting the pagination cap
// means we did not read the whole account, which is not evidence of
// absence.
func TestLinodeInventoryRefusesTruncatedListing(t *testing.T) {
	stub := &linodeStub{pages: map[string]string{
		"1": `{"page":1,"pages":9999,"data":[]}`,
		"2": `{"page":2,"pages":9999,"data":[]}`,
	}}
	inv := newLinodeStub(t, stub)
	inv.maxPages = 2

	got, err := inv.CountBurstInstances(context.Background(), testBurstID)
	if err == nil {
		t.Fatalf("a truncated listing reported absence: %+v", got)
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("error should say why it refused, got %q", err)
	}
}

func TestLinodeInventoryRefusesEmptyBurstID(t *testing.T) {
	inv := newLinodeStub(t, &linodeStub{pages: map[string]string{"1": `{"page":1,"pages":1,"data":[]}`}})

	if _, err := inv.CountBurstInstances(context.Background(), ""); err == nil {
		t.Fatal("an audit with no ownership tag to match must fail, not report absence")
	}
}

func TestLinodeInventoryHonoursContextCancellation(t *testing.T) {
	inv := newLinodeStub(t, &linodeStub{pages: map[string]string{"1": `{"page":1,"pages":1,"data":[]}`}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := inv.CountBurstInstances(ctx, testBurstID); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestLinodeInventoryRejectsMissingPaginationMetadata(t *testing.T) {
	stub := &linodeStub{pages: map[string]string{
		"1": `{"data":[],"page":0,"pages":0}`,
	}}
	inv := newLinodeStub(t, stub)

	_, err := inv.CountBurstInstances(context.Background(), testBurstID)
	if err == nil {
		t.Fatal("missing/zero pagination metadata should fail")
	}
	if !strings.Contains(err.Error(), "invalid pagination") {
		t.Fatalf("error should mention invalid pagination, got %q", err.Error())
	}
}

func TestLinodeInventoryRejectsRegressedPaginationMetadata(t *testing.T) {
	stub := &linodeStub{pages: map[string]string{
		"1": `{"page":1,"pages":2,"data":[]}`,
		"2": `{"page":1,"pages":1,"data":[]}`,
	}}
	inv := newLinodeStub(t, stub)

	_, err := inv.CountBurstInstances(context.Background(), testBurstID)
	if err == nil {
		t.Fatal("a regressed page envelope should fail instead of proving absence")
	}
	if !strings.Contains(err.Error(), "invalid pagination") {
		t.Fatalf("error should mention invalid pagination, got %q", err.Error())
	}
}

func TestLinodeInventoryNameMatchesTheBackendValue(t *testing.T) {
	if got := newLinodeInventory("t").Name(); got != backendLinode {
		t.Fatalf("inventory name %q will never match a Workload's status.backend %q", got, backendLinode)
	}
}

// TestRunPassesOnCleanLinodeAbsence is the whole seam end to end against a
// real HTTP inventory: burst node observed then gone, account audited and
// empty, case passes.
func TestRunPassesOnCleanLinodeAbsence(t *testing.T) {
	stub := &linodeStub{pages: map[string]string{
		"1": `{"page":1,"pages":1,"results":1,"data":[{"id":42,"tags":["yscale-burst","burst_someoneelse"]}]}`,
	}}
	inv := newLinodeStub(t, stub)

	// baseline empty → burst node up → reaped.
	k8s := fakeK8sWithNodeScript(
		nodeStep{},
		nodeStep{names: []string{testBurstNodeName}},
		nodeStep{},
	)

	r := testRunner(k8s, dynServingWorkload("Succeeded", backendLinode, testBurstID), inv, 2*time.Second)
	res := r.Run(context.Background(), Case{Name: "linode-gpu-rtx6000", RawYAML: []byte(testCaseYAML)})

	if res.Phase != "Succeeded" {
		t.Fatalf("want Succeeded, got %q (%s)", res.Phase, res.errorText())
	}
	if !res.Reaped {
		t.Fatalf("clean Linode absence should pass reap, got %q", res.ReapErr)
	}
	if res.errorText() != "" {
		t.Fatalf("a passing case should report no error, got %q", res.errorText())
	}
	if _, pages := stub.seen(); len(pages) == 0 {
		t.Fatal("the account was never audited")
	}
}
