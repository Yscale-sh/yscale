// yscale:proprietary

package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// The three shapes the default catalog publishes, so a test can submit one that
// matches a template and one that does not.
const (
	templateContainerSpec = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: tmpl-container
spec:
  image: busybox
  size: small`

	templateGPUSpec = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: tmpl-gpu
spec:
  image: busybox
  size: large
  gpu:
    kind: rtx4000ada
    count: 1`

	templateNodeOnlySpec = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: tmpl-node
spec:
  nodeOnly: true
  command: ["sh", "-c"]
  args: ["echo", "node"]
  size: medium`
)

// templateFixture is one tenant with a connected connector and a catalog, wired
// to a journal that records what the submit path decided — the refusals matter
// as much as the acceptances here, and a refusal leaves nothing else behind.
type templateFixture struct {
	store   *state.Store
	cust    *state.Customer
	journal *failingJournal
	dec     *fakeDecider
	h       *Workloads
}

// newTemplateFixture wires a tenant with the given catalog. nil is the tenant
// that has never published one, which inherits the server defaults.
func newTemplateFixture(t *testing.T, catalog *state.WorkloadTemplateCatalog) *templateFixture {
	t.Helper()
	store := state.New()
	cust := &state.Customer{ID: "cust_tmpl", Token: "tok_tmpl", TemplateCatalog: catalog}
	store.AddCustomer(cust)
	store.AddAgent(&state.Agent{
		ID: "agent_tmpl", CustomerID: cust.ID, ClusterID: "cluster_tmpl",
		Send: make(chan protocol.Envelope, 256),
	})
	journal := &failingJournal{Store: store}
	dec := &fakeDecider{}
	return &templateFixture{
		store: store, cust: cust, journal: journal, dec: dec,
		h: &Workloads{Store: store, Decider: dec, Reaper: &fakeReaper{}, Log: quietLog(), Journal: journal},
	}
}

func (f *templateFixture) submit(o submitOpts) *httptest.ResponseRecorder {
	return submitTo(f.h, f.cust, o)
}

func (f *templateFixture) workload(t *testing.T, rec *httptest.ResponseRecorder) *state.Workload {
	t.Helper()
	var resp CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode create response: %v (body %q)", err, rec.Body)
	}
	wl, err := f.store.GetWorkload(resp.ID)
	if err != nil {
		t.Fatalf("stored workload %q: %v", resp.ID, err)
	}
	return wl
}

// A submission that names a template it really does come from is stamped with
// that exact entry — id, version, and the catalog revision it was verified
// against — and the accepted journal row says the same.
func TestCreate_TemplateReferenceStampsExactProvenance(t *testing.T) {
	f := newTemplateFixture(t, nil)
	revision := f.cust.TemplateCatalog.Revision()

	rec := f.submit(submitOpts{key: "template-key-0001", templateID: "container-job", templateVersion: "3"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	wl := f.workload(t, rec)
	want := state.TemplateRef{ID: "container-job", Version: 3, CatalogRevision: revision}
	if wl.TemplateRef == nil || *wl.TemplateRef != want {
		t.Fatalf("stamped template = %+v, want %+v", wl.TemplateRef, want)
	}

	rows := f.journal.recorded(state.ActionWorkloadSubmit)
	if len(rows) != 1 || rows[0].Outcome != state.OutcomeAccepted {
		t.Fatalf("submit audit rows = %+v", rows)
	}
	detail := rows[0].Detail
	if detail.TemplateID != "container-job" || detail.TemplateVersion != 3 || detail.TemplateCatalogRevision != revision {
		t.Fatalf("accepted audit detail = %+v", detail)
	}
	// The journal carries the reference and nothing the catalog says about it.
	raw, err := json.Marshal(rows[0])
	if err != nil {
		t.Fatalf("marshal audit row: %v", err)
	}
	for _, leak := range []string{"busybox", "Generic container job", "Run-once job"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("audit row leaked template text %q: %s", leak, raw)
		}
	}

	// The same provenance is what a co-member reads back off both workload
	// views, and it is not re-derived from the spec's shape.
	if got := f.workload(t, rec).TemplateRef; got == nil || *got != want {
		t.Fatalf("stored provenance drifted: %+v", got)
	}
}

// Every reference central cannot honour is refused BEFORE the provider is
// reached, with a bounded message and a denied submit row an operator can read.
func TestCreate_BadTemplateReferencesAreRefusedAndJournaled(t *testing.T) {
	published := &state.WorkloadTemplateCatalog{Templates: []state.WorkloadTemplate{
		{
			ID: "container-job", Version: 5, Title: "Container job", Kind: "Run-once job",
			Defaults: state.WorkloadTemplateDefaults{
				Name: "container-job", Image: "busybox:1", Size: "small", Mode: state.WorkloadTemplateModeCPU,
			},
		},
		{
			ID: "trainer", Version: 3, Title: "Trainer", Kind: "Run-once GPU job",
			Defaults: state.WorkloadTemplateDefaults{
				Name: "trainer", Image: "busybox:1", Size: "large", Mode: state.WorkloadTemplateModeGPU,
			},
		},
		{
			ID: "bare-node", Version: 1, Title: "Bare node", Kind: "Capacity request", NodeOnly: true,
			Defaults: state.WorkloadTemplateDefaults{
				Name: "bare-node", Size: "medium", Mode: state.WorkloadTemplateModeCPU,
			},
		},
		{
			ID: "retired", Version: 9, Title: "Retired", Kind: "Run-once job", Disabled: true,
			Defaults: state.WorkloadTemplateDefaults{
				Name: "retired", Image: "busybox:1", Size: "small", Mode: state.WorkloadTemplateModeCPU,
			},
		},
	}}

	for _, tc := range []struct {
		name       string
		opts       submitOpts
		wantStatus int
		wantReason string
	}{
		{
			name:       "unknown template",
			opts:       submitOpts{templateID: "never-published", templateVersion: "1"},
			wantStatus: http.StatusConflict, wantReason: state.ReasonTemplateNotOffered,
		},
		{
			name:       "retired template",
			opts:       submitOpts{templateID: "retired", templateVersion: "9"},
			wantStatus: http.StatusConflict, wantReason: state.ReasonTemplateNotOffered,
		},
		{
			name:       "stale version",
			opts:       submitOpts{templateID: "container-job", templateVersion: "4"},
			wantStatus: http.StatusConflict, wantReason: state.ReasonTemplateVersionStale,
		},
		{
			name:       "container submitted against node-only capacity",
			opts:       submitOpts{templateID: "bare-node", templateVersion: "1"},
			wantStatus: http.StatusBadRequest, wantReason: state.ReasonTemplateShapeMismatch,
		},
		{
			name:       "node-only template cannot accept command or args",
			opts:       submitOpts{spec: templateNodeOnlySpec, templateID: "bare-node", templateVersion: "1"},
			wantStatus: http.StatusBadRequest, wantReason: state.ReasonTemplateShapeMismatch,
		},
		{
			name:       "node-only submitted against a container job",
			opts:       submitOpts{spec: templateNodeOnlySpec, templateID: "container-job", templateVersion: "5"},
			wantStatus: http.StatusBadRequest, wantReason: state.ReasonTemplateShapeMismatch,
		},
		{
			name:       "no GPU submitted against a GPU template",
			opts:       submitOpts{templateID: "trainer", templateVersion: "3"},
			wantStatus: http.StatusBadRequest, wantReason: state.ReasonTemplateShapeMismatch,
		},
		{
			name:       "GPU submitted against a CPU template",
			opts:       submitOpts{spec: templateGPUSpec, templateID: "container-job", templateVersion: "5"},
			wantStatus: http.StatusBadRequest, wantReason: state.ReasonTemplateShapeMismatch,
		},
		{
			name:       "id with no version",
			opts:       submitOpts{templateID: "container-job"},
			wantStatus: http.StatusBadRequest, wantReason: state.ReasonTemplateReferenceInvalid,
		},
		{
			name:       "version with no id",
			opts:       submitOpts{templateVersion: "5"},
			wantStatus: http.StatusBadRequest, wantReason: state.ReasonTemplateReferenceInvalid,
		},
		{
			name:       "non-numeric version",
			opts:       submitOpts{templateID: "container-job", templateVersion: "latest"},
			wantStatus: http.StatusBadRequest, wantReason: state.ReasonTemplateReferenceInvalid,
		},
		{
			name:       "zero version",
			opts:       submitOpts{templateID: "container-job", templateVersion: "0"},
			wantStatus: http.StatusBadRequest, wantReason: state.ReasonTemplateReferenceInvalid,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTemplateFixture(t, published)
			opts := tc.opts
			opts.key = "template-refusal-0001"
			rec := f.submit(opts)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body)
			}
			if got := f.dec.planCalls(); got != 0 {
				t.Fatalf("Plan calls = %d, want 0 — a refused reference must not reach a provider", got)
			}
			if got := len(f.store.BurstsForCustomer(f.cust.ID)); got != 0 {
				t.Fatalf("bursts after a refused reference = %d, want 0", got)
			}
			rows := f.journal.recorded(state.ActionWorkloadSubmit)
			if len(rows) != 1 || rows[0].Outcome != state.OutcomeDenied {
				t.Fatalf("submit audit rows = %+v, want one denial", rows)
			}
			if got := rows[0].Detail.Reason; got != tc.wantReason {
				t.Fatalf("denial reason = %q, want %q", got, tc.wantReason)
			}
			if rows[0].Detail.Rule != workloadTemplateRule || rows[0].Detail.RuleVersion != workloadTemplateRuleVersion {
				t.Fatalf("denial rule = %q/%q", rows[0].Detail.Rule, rows[0].Detail.RuleVersion)
			}
			// The refusal has to be useful without being a place to put
			// tenant-authored text or an unbounded echo of the request.
			if body := rec.Body.String(); len(body) > 512 {
				t.Fatalf("refusal body is %d bytes; it is meant to be bounded: %s", len(body), body)
			}
		})
	}
}

// A reference central could not even parse must not put submitter input into a
// durable row: the id is held to the catalog's grammar first, and anything else
// is recorded as the redaction.
func TestCreate_UnparseableTemplateIDIsRedactedInTheJournal(t *testing.T) {
	f := newTemplateFixture(t, nil)
	rec := f.submit(submitOpts{
		key:        "template-redact-0001",
		templateID: "Not A Template ID",
		// A version that will not parse, so the reference is refused on shape
		// before the id is ever looked up.
		templateVersion: "two",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
	}
	rows := f.journal.recorded(state.ActionWorkloadSubmit)
	if len(rows) != 1 || rows[0].Detail.TemplateID != state.NamespaceRedacted {
		t.Fatalf("journalled template id = %+v, want the redaction", rows)
	}
	if strings.Contains(rec.Body.String(), "Not A Template ID") {
		t.Fatalf("the refusal echoed the submitted id back: %s", rec.Body)
	}
}

// Everything that submitted before the catalog existed still submits, and is
// stamped with no template rather than with a guess made from the spec's shape.
func TestCreate_SubmissionsWithoutATemplateAreUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts submitOpts
	}{
		{name: "human raw API", opts: submitOpts{key: "no-template-0001"}},
		{name: "cluster credential", opts: submitOpts{cluster: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTemplateFixture(t, nil)
			rec := f.submit(tc.opts)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
			}
			if wl := f.workload(t, rec); wl.TemplateRef != nil {
				t.Fatalf("untemplated submission was stamped with %+v", wl.TemplateRef)
			}
			rows := f.journal.recorded(state.ActionWorkloadSubmit)
			if len(rows) != 1 || rows[0].Detail.TemplateID != "" || rows[0].Detail.TemplateVersion != 0 {
				t.Fatalf("untemplated submit audit = %+v", rows)
			}
		})
	}
}

// A tenant that publishes its own catalog is judged against it and not against
// central's: an id only the defaults carry is not offered here.
func TestCreate_TenantCatalogReplacesTheServerDefaults(t *testing.T) {
	published := &state.WorkloadTemplateCatalog{Templates: []state.WorkloadTemplate{{
		ID: "house-job", Version: 11, Title: "House job", Kind: "Run-once job",
		Defaults: state.WorkloadTemplateDefaults{
			Name: "house-job", Image: "busybox:1", Size: "small", Mode: state.WorkloadTemplateModeCPU,
		},
	}}}
	f := newTemplateFixture(t, published)

	if rec := f.submit(submitOpts{key: "house-key-000001", templateID: "container-job", templateVersion: "3"}); rec.Code != http.StatusConflict {
		t.Fatalf("default template on a tenant that publishes its own = %d, want 409: %s", rec.Code, rec.Body)
	}
	rec := f.submit(submitOpts{key: "house-key-000002", templateID: "house-job", templateVersion: "11"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("published template = %d, want 202: %s", rec.Code, rec.Body)
	}
	wl := f.workload(t, rec)
	want := state.TemplateRef{ID: "house-job", Version: 11, CatalogRevision: published.Revision()}
	if wl.TemplateRef == nil || *wl.TemplateRef != want {
		t.Fatalf("stamped template = %+v, want %+v", wl.TemplateRef, want)
	}
}

// The reference is part of what an Idempotency-Key is bound to. The same YAML
// under a different template is a different submission, and answering it with
// the first one's response would hand back a provenance stamp it never asked
// for.
func TestCreate_IdempotencyKeyIsScopedToTheTemplateReference(t *testing.T) {
	published := &state.WorkloadTemplateCatalog{Templates: []state.WorkloadTemplate{
		{
			ID: "first", Version: 1, Title: "First", Kind: "Run-once job",
			Defaults: state.WorkloadTemplateDefaults{
				Name: "first", Image: "busybox:1", Size: "small", Mode: state.WorkloadTemplateModeCPU,
			},
		},
		{
			ID: "second", Version: 1, Title: "Second", Kind: "Run-once job",
			Defaults: state.WorkloadTemplateDefaults{
				Name: "second", Image: "busybox:1", Size: "small", Mode: state.WorkloadTemplateModeCPU,
			},
		},
	}}
	f := newTemplateFixture(t, published)

	first := f.submit(submitOpts{key: "scoped-key-000001", templateID: "first", templateVersion: "1"})
	if first.Code != http.StatusAccepted {
		t.Fatalf("first submit = %d, want 202: %s", first.Code, first.Body)
	}
	// Same key, same spec, different template: a conflict rather than a replay.
	other := f.submit(submitOpts{key: "scoped-key-000001", templateID: "second", templateVersion: "1"})
	if other.Code != http.StatusConflict {
		t.Fatalf("same key under another template = %d, want 409: %s", other.Code, other.Body)
	}
	if got := f.dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls = %d, want 1 — the second reference must not provision", got)
	}
	// And the honest retry of the FIRST submission still replays it.
	replay := f.submit(submitOpts{key: "scoped-key-000001", templateID: "first", templateVersion: "1"})
	if replay.Code != http.StatusAccepted || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay = %d %s, want the original answer %s", replay.Code, replay.Body, first.Body)
	}
	if got := f.dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls after the replay = %d, want 1", got)
	}
}

// An untemplated submission's key binds exactly as it always did, so a claim
// already in flight when this ships keeps meaning what it meant.
func TestCreate_UntemplatedKeyStillReplays(t *testing.T) {
	f := newTemplateFixture(t, nil)
	first := f.submit(submitOpts{key: "plain-key-000001"})
	if first.Code != http.StatusAccepted {
		t.Fatalf("first submit = %d, want 202: %s", first.Code, first.Body)
	}
	second := f.submit(submitOpts{key: "plain-key-000001"})
	if second.Code != first.Code || second.Body.String() != first.Body.String() {
		t.Fatalf("replay = %d %s, want %d %s", second.Code, second.Body, first.Code, first.Body)
	}
	if got := f.dec.planCalls(); got != 1 {
		t.Fatalf("Plan calls = %d, want 1", got)
	}
}
