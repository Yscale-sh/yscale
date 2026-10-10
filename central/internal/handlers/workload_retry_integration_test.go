//go:build integration

// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
)

type retryAdmissionDecider struct {
	fakeDecider
	beforeQuote   func()
	beforePlan    func(PlanOptions)
	image, suffix string
}

func (d *retryAdmissionDecider) Quote(ctx context.Context, w *workload.Workload, opts PlanOptions) (*BurstQuote, error) {
	if d.beforeQuote != nil {
		d.beforeQuote()
	}
	return d.fakeDecider.Quote(ctx, w, opts)
}

func (d *retryAdmissionDecider) Plan(ctx context.Context, w *workload.Workload, opts PlanOptions) (*Plan, error) {
	if d.beforePlan != nil {
		d.beforePlan(opts)
	}
	d.image = w.Spec.Image
	p, err := d.fakeDecider.Plan(ctx, w, opts)
	if p != nil {
		p.BurstID += d.suffix
		p.BackendID = "synthetic-" + p.BurstID
	}
	return p, err
}

func (d *retryAdmissionDecider) PlanQuoted(ctx context.Context, w *workload.Workload, opts PlanOptions, _ *BurstQuote) (*Plan, error) {
	return d.Plan(ctx, w, opts)
}

func TestPostgresWorkloadRetryCurrentSourceAndAdmission(t *testing.T) {
	for _, change := range []string{"uncached", "submitter-changed", "spec-changed", "role-at-quote", "source-at-quote", "policy-at-quote", "revoked-at-quote"} {
		t.Run(change, func(t *testing.T) {
			f := newHumanAuthorizationFixture(t)
			customer, err := f.a.CustomerByID(f.tenant)
			if err != nil {
				t.Fatal(err)
			}
			customer.Plan, customer.MaxHourlyUSD = "pro", 10
			f.a.AddCustomer(customer)
			if _, err := f.a.SetCustomerWorkloadNamespaces(f.tenant, []string{"jobs"}, state.OperatorActor()); err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.a.SetTenantMemberRole(f.tenant, f.owner, f.member, state.RoleMember, state.HumanActor(f.owner, f.tenant)); err != nil {
				t.Fatal(err)
			}
			finished := time.Now().UTC().Add(-time.Minute)
			actor := state.HumanActor(f.member, f.tenant)
			source := &state.Workload{ID: "wl_retry_source_" + f.tenant, CustomerID: f.tenant, ClusterID: "cl-retry", Status: "succeeded", FinishedAt: &finished, SubmittedBy: &actor, SpecYAML: []byte(tenantWorkloadYAML)}
			if err := f.a.PutWorkloadDurable(source); err != nil {
				t.Fatal(err)
			}
			b := f.b
			if change != "uncached" {
				b = coreReadStore(t)
			}
			b.AddAgent(&state.Agent{ID: "agent-" + f.tenant, CustomerID: f.tenant, ClusterID: "cl-retry", Send: make(chan protocol.Envelope, 256)})
			d := &retryAdmissionDecider{suffix: f.tenant}
			d.beforePlan = func(opts PlanOptions) {
				page, err := f.a.TenantAuditFor(context.Background(), f.tenant, f.owner, state.AuditQuery{})
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, event := range page.Events {
					if event.Action == state.ActionWorkloadRetry && event.Outcome == state.OutcomeAccepted && event.TargetID == source.ID && event.Detail.RetryWorkloadID == opts.WorkloadID {
						found = true
					}
				}
				if !found {
					t.Error("provider plan ran before the matching retry audit was durably visible on another replica")
				}
			}
			f.api.Store = b
			f.api.Workloads = &Workloads{Store: b, Decider: d, Reaper: &fakeReaper{}, Log: quietLog()}
			want := http.StatusAccepted
			switch change {
			case "submitter-changed":
				other := state.HumanActor(f.owner, f.tenant)
				source.SubmittedBy = &other
				if err := f.a.PutWorkloadDurable(source); err != nil {
					t.Fatal(err)
				}
				want = http.StatusForbidden
			case "spec-changed":
				source.SpecYAML = []byte(strings.ReplaceAll(tenantWorkloadYAML, "busybox", "synthetic-new-image"))
				if err := f.a.PutWorkloadDurable(source); err != nil {
					t.Fatal(err)
				}
			case "role-at-quote":
				d.beforeQuote = func() {
					if _, _, err := f.a.SetTenantMemberRole(f.tenant, f.owner, f.member, state.RoleViewer, state.HumanActor(f.owner, f.tenant)); err != nil {
						t.Fatal(err)
					}
				}
				want = http.StatusForbidden
			case "source-at-quote":
				d.beforeQuote = func() {
					source.SpecYAML = []byte(strings.ReplaceAll(tenantWorkloadYAML, "busybox", "synthetic-new-image"))
					if err := f.a.PutWorkloadDurable(source); err != nil {
						t.Fatal(err)
					}
				}
				want = http.StatusConflict
			case "policy-at-quote":
				d.beforeQuote = func() {
					if _, err := f.a.SetCustomerWorkloadNamespaces(f.tenant, []string{"different"}, state.OperatorActor()); err != nil {
						t.Fatal(err)
					}
				}
				want = http.StatusConflict
			case "revoked-at-quote":
				d.beforeQuote = func() {
					if err := f.a.RevokeCustomer(f.tenant); err != nil {
						t.Fatal(err)
					}
				}
				want = http.StatusNotFound
			}
			var billingEvents []string
			if want != http.StatusAccepted {
				f.api.Workloads.EnforcePrepaidBilling = true
				f.api.Workloads.Billing = &orderedBilling{events: &billingEvents}
			}
			path := "/v1/tenants/" + f.tenant + "/workloads/" + source.ID + "/retry"
			response := callTenantWorkloadWithKey(f.api, http.MethodPost, path, "synthetic-human-session", "", "retry-current-source", "")
			if want == http.StatusAccepted && response.Code == want {
				replay := callTenantWorkloadWithKey(f.api, http.MethodPost, path, "synthetic-human-session", "", "retry-current-source", "")
				if replay.Code != want || replay.Header().Get("Idempotency-Replayed") != "true" || d.calls != 1 {
					t.Errorf("retry replay status=%d replay=%s plans=%d", replay.Code, replay.Header().Get("Idempotency-Replayed"), d.calls)
				}
				page, err := f.a.TenantAuditFor(context.Background(), f.tenant, f.owner, state.AuditQuery{})
				if err != nil {
					t.Fatal(err)
				}
				accepted := 0
				for _, event := range page.Events {
					if event.Action == state.ActionWorkloadRetry && event.Outcome == state.OutcomeAccepted {
						accepted++
					}
				}
				if accepted != 1 {
					t.Errorf("HTTP replay emitted %d accepted retry decisions, want one", accepted)
				}
			}
			// Clean synthetic bookings even when a regression wrongly provisions.
			for _, burst := range b.ListBursts() {
				if burst.CustomerID == f.tenant {
					if err := b.DeleteBurst(burst.ID); err != nil {
						t.Error(err)
					}
				}
			}
			if response.Code != want {
				t.Errorf("retry after %s=%d, want %d", change, response.Code, want)
			}
			if want != http.StatusAccepted && d.calls != 0 {
				t.Errorf("unauthorized/stale retry reached provider plan %d times", d.calls)
			}
			if len(billingEvents) != 0 {
				t.Errorf("refused retry reached credit reservation: %v", billingEvents)
			}
			if change == "spec-changed" && !strings.Contains(d.image, "synthetic-new-image") {
				t.Error("retry executed stale source YAML")
			}
			if want == http.StatusAccepted && response.Code == want {
				var reply CreateWorkloadResponse
				if err := json.Unmarshal(response.Body.Bytes(), &reply); err != nil {
					t.Fatal(err)
				}
				current, err := b.WorkloadSnapshotForCustomer(context.Background(), f.tenant, reply.ID)
				if err != nil || current.Workload.RetryOfWorkloadID != source.ID {
					t.Fatalf("retry lost durable provenance: %v", err)
				}
			}
		})
	}
}
