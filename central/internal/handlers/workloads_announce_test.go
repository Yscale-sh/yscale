package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
	"gopkg.in/yaml.v3"
)

// nsRecordingDecider is fakeDecider plus the namespace the spec carried
// when Plan saw it. Plan renders the Job into that namespace, so it has
// to already be the authorized one, not the submitted one.
type nsRecordingDecider struct {
	fakeDecider
	mu        sync.Mutex
	planNS    string
	planCalls int
}

func (d *nsRecordingDecider) Plan(ctx context.Context, wl *workload.Workload, opts PlanOptions) (*Plan, error) {
	d.mu.Lock()
	d.planNS = wl.Metadata.Namespace
	d.planCalls++
	d.mu.Unlock()
	return d.fakeDecider.Plan(ctx, wl, opts)
}

// nsFixture wires a tenant authorized for the given namespaces (nil =
// none configured, i.e. the fail-closed set) and a connected agent.
func nsFixture(t *testing.T, namespaces []string) (*Workloads, *state.Customer, *state.Agent, *nsRecordingDecider) {
	t.Helper()
	store := state.New()
	cust := &state.Customer{
		ID: "cust_ns", Token: "t", MaxConcurrentBursts: 1,
		WorkloadNamespaces: namespaces,
	}
	store.AddCustomer(cust)
	agent := &state.Agent{
		ID: "agent_ns", CustomerID: cust.ID, ClusterID: "cluster_ns",
		Send: make(chan protocol.Envelope, 16),
	}
	store.AddAgent(agent)
	decider := &nsRecordingDecider{}
	h := &Workloads{Store: store, Decider: decider, Reaper: &fakeReaper{}, Log: quietLog()}
	return h, cust, agent, decider
}

// The agent will only sign storage URLs against the namespace central
// puts on the announce, so the announce has to carry one — and it must
// be the same one the Job lands in and the same one Plan rendered.
func TestCreate_BurstAnnounceCarriesAuthorizedNamespace(t *testing.T) {
	cases := []struct {
		name      string
		allowed   []string
		submitted string
		want      string
	}{
		{name: "authorized namespace", allowed: []string{"team-a", "team-b"}, submitted: "team-a", want: "team-a"},
		{name: "second authorized namespace", allowed: []string{"team-a", "team-b"}, submitted: "team-b", want: "team-b"},
		{name: "unqualified lands in the tenant's first", allowed: []string{"team-a", "team-b"}, submitted: "", want: "team-a"},
		{name: "unconfigured tenant defaults", allowed: nil, submitted: "", want: "default"},
		{name: "unconfigured tenant may still say default", allowed: nil, submitted: "default", want: "default"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, cust, agent, decider := nsFixture(t, tc.allowed)

			rec := httptest.NewRecorder()
			h.Create(rec, newCreateReqNS(cust, tc.submitted))
			if rec.Code != http.StatusAccepted && rec.Code != http.StatusOK {
				t.Fatalf("create returned %d: %s", rec.Code, rec.Body.String())
			}

			announce, createJob := drainCommands(t, agent)
			if announce.Namespace != tc.want {
				t.Errorf("burst_announce namespace = %q, want %q", announce.Namespace, tc.want)
			}
			// The three must not drift: the Job would land in one
			// namespace and its bucket credentials be read from another.
			if createJob.Namespace != tc.want {
				t.Errorf("create_job namespace = %q, want %q", createJob.Namespace, tc.want)
			}
			if decider.planNS != tc.want {
				t.Errorf("Plan saw namespace %q, want %q", decider.planNS, tc.want)
			}
		})
	}
}

// The submitted namespace decides which namespace the connector reads
// bucket-credentials Secrets from. An unauthorized one is refused
// before anything is planned, provisioned, or announced.
func TestCreate_RejectsUnauthorizedNamespace(t *testing.T) {
	for _, tc := range []struct {
		name, submitted string
		allowed         []string
	}{
		{name: "another team's namespace", submitted: "team-b", allowed: []string{"team-a"}},
		{name: "kube-system", submitted: "kube-system", allowed: []string{"team-a"}},
		{name: "unconfigured tenant is default-only", submitted: "team-a", allowed: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, cust, agent, decider := nsFixture(t, tc.allowed)

			rec := httptest.NewRecorder()
			h.Create(rec, newCreateReqNS(cust, tc.submitted))

			if rec.Code != http.StatusForbidden {
				t.Fatalf("create returned %d, want 403: %s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "not authorized") {
				t.Errorf("rejection should say what is wrong, got %s", rec.Body.String())
			}
			if decider.planCalls != 0 {
				t.Error("planned a burst for an unauthorized namespace")
			}
			select {
			case env := <-agent.Send:
				t.Fatalf("enqueued %s for an unauthorized namespace", env.Type)
			default:
			}
			if bursts := h.Store.BurstsForCustomer(cust.ID); len(bursts) != 0 {
				t.Errorf("recorded %d bursts for a rejected submission", len(bursts))
			}
		})
	}
}

// What central stores is what central acted on. Storing the submitted bytes
// made the record disagree with reality for every submission that did not name
// its own authorized namespace — the Job, the announce and the connector's
// Secret read all used the pinned one, and only the console showed the other.
func TestCreate_StoresTheCanonicalSpecNotTheSubmittedBytes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		submitted string
		want      string
	}{
		{name: "unqualified is pinned to the tenant's first", submitted: "", want: "team-a"},
		{name: "authorized namespace is kept", submitted: "team-b", want: "team-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, cust, _, _ := nsFixture(t, []string{"team-a", "team-b"})

			rec := httptest.NewRecorder()
			h.Create(rec, newRichCreateReqNS(cust, tc.submitted))
			if rec.Code != http.StatusAccepted {
				t.Fatalf("create returned %d: %s", rec.Code, rec.Body.String())
			}

			records := h.Store.WorkloadsForCustomer(cust.ID, 10)
			if len(records) != 1 {
				t.Fatalf("stored %d workloads, want 1", len(records))
			}
			var stored workload.Workload
			if err := yaml.Unmarshal(records[0].SpecYAML, &stored); err != nil {
				t.Fatalf("stored spec does not parse: %v\n%s", err, records[0].SpecYAML)
			}
			if stored.Metadata.Namespace != tc.want {
				t.Errorf("stored namespace = %q, want %q", stored.Metadata.Namespace, tc.want)
			}
			// The pin is the only edit. Everything else the submitter sent is
			// still there, or the record is no longer the workload.
			if stored.Metadata.Name != "rich-job" || stored.Spec.Image != "busybox:latest" ||
				stored.Spec.Size != "small" || stored.Spec.Replicas != 3 ||
				stored.Metadata.Labels["team"] != "a" {
				t.Errorf("stored spec lost fields: %+v", stored)
			}
			if stored.Spec.Budget == nil || stored.Spec.Budget.MaxUSD != 2.5 ||
				stored.Spec.Budget.Deadline != 5*time.Minute {
				t.Errorf("stored budget = %+v", stored.Spec.Budget)
			}
			if len(stored.Spec.Env) != 1 || stored.Spec.Env[0].Name != "MODE" {
				t.Errorf("stored env = %+v", stored.Spec.Env)
			}
		})
	}
}

func newRichCreateReqNS(cust *state.Customer, namespace string) *http.Request {
	ns := ""
	if namespace != "" {
		ns = fmt.Sprintf("\n  namespace: %s", namespace)
	}
	body := fmt.Sprintf(`apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: rich-job%s
  labels:
    team: a
spec:
  image: busybox:latest
  size: small
  replicas: 3
  budget:
    maxUSD: 2.5
    deadline: 5m
  env:
    - name: MODE
      value: batch`, ns)
	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(body))
	return req.WithContext(context.WithValue(context.Background(), ctxCustomer, cust))
}

func newCreateReqNS(cust *state.Customer, namespace string) *http.Request {
	ns := ""
	if namespace != "" {
		ns = fmt.Sprintf("\n  namespace: %s", namespace)
	}
	body := fmt.Sprintf(`apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: announce-test%s
spec:
  image: busybox
  size: small`, ns)
	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(body))
	return req.WithContext(context.WithValue(context.Background(), ctxCustomer, cust))
}

// drainCommands pulls the burst_announce and create_job the Create
// path enqueued for the agent.
func drainCommands(t *testing.T, agent *state.Agent) (protocol.BurstAnnounce, protocol.CreateJob) {
	t.Helper()
	var announce protocol.BurstAnnounce
	var createJob protocol.CreateJob
	var sawAnnounce, sawJob bool
	for {
		select {
		case env := <-agent.Send:
			switch env.Type {
			case protocol.TypeBurstAnnounce:
				if err := json.Unmarshal(env.Body, &announce); err != nil {
					t.Fatalf("decode burst_announce: %v", err)
				}
				sawAnnounce = true
			case protocol.TypeCreateJob:
				if err := json.Unmarshal(env.Body, &createJob); err != nil {
					t.Fatalf("decode create_job: %v", err)
				}
				sawJob = true
			}
		default:
			if !sawAnnounce {
				t.Fatal("no burst_announce enqueued")
			}
			if !sawJob {
				t.Fatal("no create_job enqueued")
			}
			return announce, createJob
		}
	}
}
