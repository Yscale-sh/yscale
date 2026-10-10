package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
	"gopkg.in/yaml.v3"
)

// The submission a KEDA/PendingPodWatcher scale-up makes: bare capacity, no
// budget, no caller in a position to declare one.
const nodeOnlyBareSpec = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: nodeonly-bare
spec:
  nodeOnly: true
  size: small`

// nodeOnlyFixture is one tenant with one connector whose occupancy capability
// the test chooses, and a handler carrying the configured fallback.
type nodeOnlyFixture struct {
	store *state.Store
	cust  *state.Customer
	h     *Workloads
}

func newNodeOnlyFixture(t *testing.T, authoritative bool, fallback time.Duration) *nodeOnlyFixture {
	t.Helper()
	store := state.New()
	cust := &state.Customer{ID: "cust_nodeonly", Token: "tok_nodeonly"}
	store.AddCustomer(cust)
	store.AddAgent(&state.Agent{
		ID: "agent_nodeonly", CustomerID: cust.ID, ClusterID: "cluster_nodeonly",
		AuthoritativeOccupancy: authoritative,
		Send:                   make(chan protocol.Envelope, 256),
	})
	return &nodeOnlyFixture{
		store: store,
		cust:  cust,
		h: &Workloads{
			Store: store, Decider: &fakeDecider{}, Reaper: &fakeReaper{}, Log: quietLog(),
			NodeOnlyMaxLifetime: fallback,
		},
	}
}

// submit posts one nodeOnly workload and returns the accepted record plus the
// burst booked for it.
func (f *nodeOnlyFixture) submit(t *testing.T, spec, key string) (*httptest.ResponseRecorder, *state.Workload, *state.Burst) {
	t.Helper()
	rec := submitTo(f.h, f.cust, submitOpts{spec: spec, key: key})
	if rec.Code != http.StatusAccepted {
		return rec, nil, nil
	}
	var resp CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode create response: %v (body %q)", err, rec.Body)
	}
	wl, err := f.store.GetWorkload(resp.ID)
	if err != nil {
		t.Fatalf("stored workload %q: %v", resp.ID, err)
	}
	b, err := f.store.GetBurst(resp.BurstID)
	if err != nil {
		t.Fatalf("stored burst %q: %v", resp.BurstID, err)
	}
	return rec, wl, b
}

// storedBudget reads the budget off the spec central actually recorded, which is
// what every API read of this workload serves back.
func storedBudget(t *testing.T, wl *state.Workload) *workload.Budget {
	t.Helper()
	var parsed workload.Workload
	if err := yaml.Unmarshal(wl.SpecYAML, &parsed); err != nil {
		t.Fatalf("parse stored spec: %v", err)
	}
	return parsed.Spec.Budget
}

// The gap this closes. A nodeOnly burst is ended by a signal only the customer's
// cluster sends, and a connector without cluster-wide pod visibility can never
// send it — so the watchdog's silence ceiling has nothing to measure and the
// burst runs until someone reads the invoice. Admission gives it the same finite
// bound instead, written onto the spec so the limit is visible rather than
// applied invisibly later.
//
// It needs no caller change, which is the point: the PendingPodWatcher submission
// this covers declares no budget and has nobody to ask for one.
func TestCreate_UnobservableNodeOnlyGetsTheConfiguredDeadline(t *testing.T) {
	f := newNodeOnlyFixture(t, false, 6*time.Hour)

	rec, wl, b := f.submit(t, nodeOnlyBareSpec, "nodeonly-default-0001")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	budget := storedBudget(t, wl)
	if budget == nil || budget.Deadline != 6*time.Hour {
		t.Fatalf("stored budget = %+v, want a 6h deadline the submitter can read back", budget)
	}
	if budget.MaxUSD != 0 {
		t.Errorf("stored maxUSD = %v, want none invented", budget.MaxUSD)
	}
	// The watchdog enforces the burst, not the spec, so the deadline has to have
	// reached it.
	if b.Deadline != 6*time.Hour {
		t.Errorf("burst deadline = %s, want 6h", b.Deadline)
	}
	// Nothing promised an observation, so the silence ceiling must stay off this
	// burst: the deadline above is its whole bound.
	if b.OccupancyObservationExpected {
		t.Error("a burst on a connector that cannot observe occupancy was recorded as expecting observations")
	}
}

// An explicit budget is the submitter's own bound and is left exactly as sent.
// Either half is enough — a deadline and a spend cap are both terminal for the
// burst — and neither is shortened, lengthened, or joined by an invented second
// bound.
func TestCreate_ExplicitBudgetSurvivesTheNodeOnlyFallback(t *testing.T) {
	const withDeadline = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: nodeonly-deadline
spec:
  nodeOnly: true
  size: small
  budget:
    deadline: 48h`

	const withMaxUSD = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: nodeonly-spend
spec:
  nodeOnly: true
  size: small
  budget:
    maxUSD: 12.5`

	t.Run("a longer declared deadline is not shortened", func(t *testing.T) {
		f := newNodeOnlyFixture(t, false, 6*time.Hour)
		rec, wl, b := f.submit(t, withDeadline, "nodeonly-explicit-0001")
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
		}
		if budget := storedBudget(t, wl); budget == nil || budget.Deadline != 48*time.Hour {
			t.Fatalf("stored budget = %+v, want the declared 48h untouched", budget)
		}
		if b.Deadline != 48*time.Hour {
			t.Errorf("burst deadline = %s, want the declared 48h", b.Deadline)
		}
	})

	t.Run("a spend cap alone is a budget", func(t *testing.T) {
		f := newNodeOnlyFixture(t, false, 6*time.Hour)
		rec, wl, b := f.submit(t, withMaxUSD, "nodeonly-explicit-0002")
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
		}
		budget := storedBudget(t, wl)
		if budget == nil || budget.MaxUSD != 12.5 {
			t.Fatalf("stored budget = %+v, want the declared $12.50 untouched", budget)
		}
		if budget.Deadline != 0 {
			t.Errorf("stored deadline = %s, want none added on top of a declared spend cap", budget.Deadline)
		}
		if b.Deadline != 0 || b.MaxUSD != 12.5 {
			t.Errorf("burst budget = deadline %s / $%v, want the declared spend cap alone", b.Deadline, b.MaxUSD)
		}
	})
}

// A connector that CAN observe occupancy gets no deadline at all, and that is the
// whole reason the capability exists: capacity behind a Deployment meant to stay
// up must not be destroyed at six hours. What it gets instead is the promise
// recorded on the burst, so the watchdog can read its silence later.
func TestCreate_AuthoritativeConnectorGetsNoInjectedDeadline(t *testing.T) {
	f := newNodeOnlyFixture(t, true, 6*time.Hour)

	rec, wl, b := f.submit(t, nodeOnlyBareSpec, "nodeonly-auth-0001")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	if budget := storedBudget(t, wl); budget != nil && (budget.Deadline > 0 || budget.MaxUSD > 0) {
		t.Fatalf("stored budget = %+v, want none: this connector can end the burst itself", budget)
	}
	if b.Deadline != 0 || b.MaxUSD != 0 {
		t.Errorf("burst budget = deadline %s / $%v, want unbounded by admission", b.Deadline, b.MaxUSD)
	}
	if !b.OccupancyObservationExpected {
		t.Error("the connector's authoritative capability was not recorded on the burst; the watchdog cannot read its silence")
	}
}

// A managed Job burst is unaffected either way. Central owns its completion, it
// never depends on occupancy, and it must not be handed a nodeOnly deadline or
// recorded as expecting observations.
func TestCreate_ManagedJobBurstIsUntouchedByTheNodeOnlyFallback(t *testing.T) {
	for _, tc := range []struct {
		name          string
		key           string
		authoritative bool
	}{
		{name: "unobservable connector", key: "managed-unobservable-0001", authoritative: false},
		{name: "authoritative connector", key: "managed-authoritative-0001", authoritative: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNodeOnlyFixture(t, tc.authoritative, 6*time.Hour)
			rec, wl, b := f.submit(t, `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: managed-job
spec:
  image: busybox
  size: small`, tc.key)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
			}
			if budget := storedBudget(t, wl); budget != nil && (budget.Deadline > 0 || budget.MaxUSD > 0) {
				t.Fatalf("stored budget = %+v, want none on a managed job", budget)
			}
			if b.Deadline != 0 {
				t.Errorf("burst deadline = %s, want none on a managed job", b.Deadline)
			}
			if b.OccupancyObservationExpected {
				t.Error("a managed job burst was recorded as expecting occupancy observations")
			}
		})
	}
}

// An operator who disables the fallback has not asked for unlimited capacity,
// they have asked for no automatic bound. The submission is refused rather than
// admitted: with the fallback off, an unobservable nodeOnly burst with no
// declared budget has nothing at all that can end it.
//
// The refusal is a 400 — it is answered by declaring a budget — and it provisions
// nothing.
func TestCreate_DisabledFallbackRejectsAnUnboundableNodeOnlySubmission(t *testing.T) {
	f := newNodeOnlyFixture(t, false, 0)

	rec := submitTo(f.h, f.cust, submitOpts{spec: nodeOnlyBareSpec, key: "nodeonly-disabled-0001"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
	}
	var resp CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "rejected" {
		t.Errorf("status = %q, want rejected", resp.Status)
	}
	if n := len(f.store.ListBursts()); n != 0 {
		t.Fatalf("bursts = %d, want nothing provisioned by a refused submission", n)
	}
	if n := f.h.adm().reservationCount(); n != 0 {
		t.Errorf("admission reservation leaked on a refused submission: %d remain", n)
	}

	// The same submission with a budget of its own is admitted: the refusal is
	// about the missing bound, not about nodeOnly.
	budgeted, _, b := f.submit(t, `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: nodeonly-budgeted
spec:
  nodeOnly: true
  size: small
  budget:
    deadline: 2h`, "nodeonly-disabled-0002")
	if budgeted.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", budgeted.Code, budgeted.Body)
	}
	if b.Deadline != 2*time.Hour {
		t.Errorf("burst deadline = %s, want the declared 2h", b.Deadline)
	}
}

// A disabled fallback still admits what it can bound. An authoritative connector
// ends its own bursts, so switching the fallback off must not refuse them.
func TestCreate_DisabledFallbackStillAdmitsAnObservableNodeOnlySubmission(t *testing.T) {
	f := newNodeOnlyFixture(t, true, 0)

	rec, _, b := f.submit(t, nodeOnlyBareSpec, "nodeonly-disabled-auth-0001")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	if !b.OccupancyObservationExpected {
		t.Error("the connector's capability was not recorded on the burst")
	}
}
