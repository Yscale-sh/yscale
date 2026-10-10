package handlers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// seedNodeOnlyBurst puts the burst an idle report is allowed to act on: this
// tenant's, booked in the cluster the fixture's connector authenticated for,
// carrying the node name central assigned, and nodeOnly — capacity behind pods
// central never created, which is the only kind with no completion path of its
// own.
func (f *nodePhaseFixture) seedNodeOnlyBurst(t *testing.T) {
	t.Helper()
	err := f.store.PutBurst(&state.Burst{
		ID:         nodePhaseBurstID,
		CustomerID: state.DevCustomerID,
		ClusterID:  "c1",
		Backend:    "linode",
		BackendID:  "vm-1",
		NodeName:   nodePhaseNodeName,
		NodeOnly:   true,
		Status:     state.BurstStatusRunning,
		HourlyUSD:  0.20,
		CreatedAt:  time.Now().Add(-30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("seed nodeOnly burst: %v", err)
	}
}

// reportIdle sends the teardown request an idle burst node produces, naming the
// node central itself assigned.
func (f *nodePhaseFixture) reportIdle(t *testing.T) {
	t.Helper()
	f.report(t, protocol.NodeEvent{
		BurstID:  nodePhaseBurstID,
		NodeName: nodePhaseNodeName,
		Phase:    protocol.NodePhaseIdle,
		Reason:   "burst node carried no schedulable pods for the idle grace period",
	})
}

// The path the whole feature exists for: KEDA scaled its Deployment to zero, the
// connector watched the node empty out, and central takes the capacity back
// through the same single-winner reap every other teardown goes through.
func TestAgentStreamIdleReapsANodeOnlyBurstAndCancelsItsWorkload(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()

	f.reportIdle(t)
	if cmd := f.awaitPreflight(t); cmd.NodeName != nodePhaseNodeName {
		t.Fatalf("preflight = %+v, want it to name the node central assigned", cmd)
	}
	if out := f.awaitReap(t); !out.reaped || out.burstID != nodePhaseBurstID {
		t.Fatalf("reap outcome = %+v, want burst_abc123 reaped", out)
	}

	if _, err := f.store.GetBurst(nodePhaseBurstID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("burst survived the teardown: %v", err)
	}
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 1 {
		t.Fatalf("teardowns = %d, want exactly 1", n)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil {
		t.Fatalf("workload: %v", err)
	}
	// Cancelled, not failed: nothing went wrong, the capacity was spare.
	if wl.Status != "cancelled" || wl.FinishedAt == nil {
		t.Fatalf("workload = %+v, want cancelled and finished", wl)
	}
	if ack := f.awaitAck(t); ack.BurstID != nodePhaseBurstID || ack.Phase != protocol.NodePhaseIdle {
		t.Fatalf("receipt = %+v, want burst_abc123 Idle", ack)
	}
	// The ack is the LAST thing, and it is behind the durable receipt: the
	// connector may only drop a request once teardown is proven, never on the
	// strength of central having tried.
	recorded, err := f.store.BurstReapRecorded(context.Background(), nodePhaseBurstID, state.DevCustomerID)
	if err != nil || !recorded {
		t.Fatalf("teardown receipt at ack time: recorded=%v err=%v, want the receipt written first", recorded, err)
	}
	f.drain(t)
	// The cordon stays. It is what keeps work off the node between the
	// connector's answer and the provider's delete, and this is the one path where
	// nothing is left stranded by it — the node itself is going away.
	f.requireNoRelease(t)
}

// A failed provider delete restores the burst with ReapPending, so the watchdog
// will retry even if no connector or HTTP caller asks again. The preflight's
// cordon must stay during that window: releasing it would let a pod land on a
// node central has durably scheduled for deletion.
func TestAgentStreamIdleKeepsItsCordonWhenTheTeardownIsPendingRetry(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()
	f.reaper.setErr(errors.New("provider delete failed"))

	f.reportIdle(t)
	f.awaitPreflight(t)

	if out := f.awaitReap(t); out.reaped {
		t.Fatalf("reap outcome = %+v, want a teardown that failed", out)
	}
	f.drain(t)
	f.requireNoRelease(t)
	f.requireNoAck(t)
	if b, err := f.store.GetBurst(nodePhaseBurstID); err != nil || !b.ReapPending {
		t.Fatalf("pending teardown burst = %+v, err=%v; want reap-pending marker", b, err)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil || wl.FinishedAt != nil {
		t.Fatalf("workload = %+v err=%v, want it untouched by a teardown that did not happen", wl, err)
	}
}

// lostClaimLifecycle is another replica winning the race from inside this call's
// own reap: by the time ReapBurst answers, the burst is claimed and its teardown
// receipt is written — by somebody else.
type lostClaimLifecycle struct {
	store *state.Store
}

func (l *lostClaimLifecycle) ReapBurst(ctx context.Context, burstID, _ string) ReapOutcome {
	if _, won, err := l.store.ClaimBurst(burstID); err != nil || !won {
		return ReapOutcomeNotOwned
	}
	if _, err := l.store.RecordBurstReap(ctx, burstID, state.DevCustomerID); err != nil {
		return ReapOutcomeUnknown
	}
	// This call did not tear it down; the winner did.
	return ReapOutcomeNotOwned
}

// A lost claim is NOT a stranded cordon, and the durable receipt is what tells
// them apart. Another winner holds the provider record, so the node is on its way
// out and the cordon is the only thing keeping work off it in the meantime.
// Uncordoning here would reopen the window the preflight exists to shut, on a
// node about to be destroyed.
//
// The receipt is also what releases the connector, so this is the ordering in
// one: proof first, acknowledgement second, and no give-back at all.
func TestAgentStreamIdleKeepsTheCordonWhenAnotherWinnerHoldsTheTeardown(t *testing.T) {
	f := newNodePhaseFixtureWith(t, func(inner BurstLifecycle) BurstLifecycle {
		return &lostClaimLifecycle{store: inner.(*Workloads).Store}
	})
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()

	f.reportIdle(t)
	f.awaitPreflight(t)
	if out := f.awaitReap(t); out.reaped {
		t.Fatalf("reap outcome = %+v, want this call to have lost the claim", out)
	}
	if ack := f.awaitAck(t); ack.BurstID != nodePhaseBurstID || ack.Phase != protocol.NodePhaseIdle {
		t.Fatalf("receipt = %+v, want the request acknowledged on the winner's proof", ack)
	}
	f.drain(t)
	f.requireNoRelease(t)
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 — the receipt is the winner's, not a second teardown", n)
	}
}

// unknownClaimLifecycle is the claim whose outcome nobody knows: the DELETE ...
// RETURNING may have committed before the connection dropped, so this call can
// say neither that it owns the burst nor that it does not.
type unknownClaimLifecycle struct{}

func (unknownClaimLifecycle) ReapBurst(context.Context, string, string) ReapOutcome {
	return ReapOutcomeUnknown
}

// An unknown claim outcome is not a failed teardown. The row may be gone with
// another worker's provider delete already in flight behind it, and uncordoning
// on that guess puts pods onto a node being destroyed — the exact window the
// preflight exists to shut. So the cordon stays, and so does the request: nothing
// here proves a teardown, so nothing here may retire the connector's retry.
func TestAgentStreamIdleKeepsTheCordonWhenTheClaimOutcomeIsUnknown(t *testing.T) {
	f := newNodePhaseFixtureWith(t, func(BurstLifecycle) BurstLifecycle {
		return unknownClaimLifecycle{}
	})
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()

	f.reportIdle(t)
	f.awaitPreflight(t)
	if out := f.awaitReap(t); out.reaped {
		t.Fatalf("reap outcome = %+v, want a claim this call could not resolve", out)
	}
	f.drain(t)

	f.requireNoRelease(t)
	f.requireNoAck(t)
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 — a claim of unknown outcome tears nothing down", n)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil || wl.FinishedAt != nil {
		t.Fatalf("workload = %+v err=%v, want it untouched by a teardown that may not be this call's", wl, err)
	}
}

// inFlightWinnerLifecycle is another replica mid-teardown: it has taken the row,
// so this call loses the claim, and it has NOT written its receipt yet, because
// its provider delete is still running.
type inFlightWinnerLifecycle struct {
	store *state.Store
}

func (l *inFlightWinnerLifecycle) ReapBurst(_ context.Context, burstID, _ string) ReapOutcome {
	// The winner's claim, taken from under this call. No receipt: the delete it
	// is running has not come back yet.
	l.store.ClaimBurst(burstID)
	return ReapOutcomeNotOwned
}

// The stranded-cordon fix's dangerous half. A lost claim with no readable receipt
// used to look exactly like a failed teardown — both leave this call holding a
// cordon and no proof — and the cordon was handed back. But the winner's provider
// delete is in flight, so that uncordon reopens a node that is about to stop
// existing, and the scheduler will happily bind pods to it.
//
// Absence of a receipt is not evidence the node is still there. Only this call's
// own confirmed delete failure is, and that is not what happened here.
func TestAgentStreamIdleKeepsTheCordonWhenAnotherWinnersTeardownIsStillInFlight(t *testing.T) {
	f := newNodePhaseFixtureWith(t, func(inner BurstLifecycle) BurstLifecycle {
		return &inFlightWinnerLifecycle{store: inner.(*Workloads).Store}
	})
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()

	f.reportIdle(t)
	f.awaitPreflight(t)
	if out := f.awaitReap(t); out.reaped {
		t.Fatalf("reap outcome = %+v, want this call to have lost the claim", out)
	}
	f.drain(t)

	// The node is on its way out under somebody else's delete: cordon stays.
	f.requireNoRelease(t)
	// And nothing proves that delete finished, so the request stays outstanding.
	f.requireNoAck(t)
	recorded, err := f.store.BurstReapRecorded(context.Background(), nodePhaseBurstID, state.DevCustomerID)
	if err != nil || recorded {
		t.Fatalf("teardown receipt: recorded=%v err=%v, want the in-flight winner to have written none yet", recorded, err)
	}
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 — the delete is the winner's, not a second one", n)
	}
}

// receiptPendingLifecycle is the post-delete ambiguity: the claim was won, the
// provider really did destroy the node, and only the cost/receipt write failed —
// so the burst is back in the store to retry that write, with no receipt behind
// it. The re-queued row looks exactly like a failed teardown's and is not one.
type receiptPendingLifecycle struct {
	inner *Workloads
}

func (l *receiptPendingLifecycle) ReapBurst(ctx context.Context, burstID, _ string) ReapOutcome {
	b, won, err := l.inner.Store.ClaimBurst(burstID)
	if err != nil || !won {
		return ReapOutcomeNotOwned
	}
	if err := l.inner.Reaper.Teardown(ctx, b); err != nil {
		return ReapOutcomeRequeued
	}
	// The node is gone. The receipt is what would not persist, so the record goes
	// back for the retry that writes it.
	if err := l.inner.Store.PutBurst(b); err != nil {
		return ReapOutcomeUnknown
	}
	return ReapOutcomeReceiptPending
}

// The other way the old bool got it wrong, and the worse one. The provider delete
// SUCCEEDED; only its receipt did not persist. The burst is back in the store and
// there is no receipt to read, which is bit-for-bit the state a failed teardown
// leaves — so the cordon was released and the scheduler was invited to put pods
// on a VM that no longer exists.
//
// The outcome is what tells them apart, and it says the delete happened. Cordon
// stays, and the request stays outstanding too: the acknowledgement rule is
// unchanged and still wants the durable receipt, which is precisely what is
// missing here.
func TestAgentStreamIdleKeepsTheCordonWhenTheReceiptFailsAfterASuccessfulDelete(t *testing.T) {
	f := newNodePhaseFixtureWith(t, func(inner BurstLifecycle) BurstLifecycle {
		return &receiptPendingLifecycle{inner: inner.(*Workloads)}
	})
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()

	f.reportIdle(t)
	f.awaitPreflight(t)
	if out := f.awaitReap(t); out.reaped {
		t.Fatalf("reap outcome = %+v, want a delete whose receipt did not persist", out)
	}
	f.drain(t)

	f.requireNoRelease(t)
	f.requireNoAck(t)
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 1 {
		t.Fatalf("teardowns = %d, want exactly 1 — the provider delete did happen", n)
	}
	// The re-queued row is the receipt retry, not a live node. Reading it as one
	// is the mistake this test exists to keep out.
	if _, err := f.store.GetBurst(nodePhaseBurstID); err != nil {
		t.Fatalf("the burst was not re-queued to retry its receipt: %v", err)
	}
	recorded, err := f.store.BurstReapRecorded(context.Background(), nodePhaseBurstID, state.DevCustomerID)
	if err != nil || recorded {
		t.Fatalf("teardown receipt: recorded=%v err=%v, want none — the write is what failed", recorded, err)
	}
}

// A preflight that answered NO cordoned nothing — it took its own cordon back
// before answering — so there is nothing for central to release, and sending a
// release anyway would name a version this request does not own.
func TestAgentStreamIdleReleasesNothingWhenThePreflightRefused(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()
	f.nodeTookWork.Store(true)

	f.reportIdle(t)
	f.awaitPreflight(t)
	if out := f.awaitReap(t); out.reaped {
		t.Fatalf("reap outcome = %+v, want the teardown refused by the preflight", out)
	}
	f.drain(t)

	f.requireNoRelease(t)
	f.requireNoAck(t)
}

// Idle is a request, not health. A burst that carried "Idle" as its recorded
// phase would be showing a customer a status for a node that is Ready and
// answering — and would leave that status behind on any burst whose teardown was
// then refused or lost.
func TestAgentStreamIdleIsNeverRecordedAsANodePhase(t *testing.T) {
	gate := &gatedLifecycle{started: make(chan string, 1), release: make(chan struct{})}
	f := newNodePhaseFixtureWith(t, func(inner BurstLifecycle) BurstLifecycle {
		gate.inner = inner
		return gate
	})
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()

	f.reportIdle(t)
	select {
	case <-gate.started:
	case <-time.After(3 * time.Second):
		t.Fatal("the teardown an authorised idle request owes never started")
	}

	// The crash point, exactly as for a removal: nothing durable may have been
	// written before the teardown was secured.
	b := f.burst(t)
	if b.NodePhase != "" {
		t.Fatalf("node_phase = %q; Idle is an action request and must never be persisted as health", b.NodePhase)
	}
	if b.Status != state.BurstStatusRunning {
		t.Fatalf("status = %q, want it untouched before the teardown was secured", b.Status)
	}
	f.requireNoAck(t)

	close(gate.release)
	if out := f.awaitReap(t); !out.reaped {
		t.Fatalf("reap outcome = %+v, want the teardown to complete", out)
	}
	f.awaitAck(t)
}

// THE RACE. The connector's idle report describes what it saw on its last sweep;
// the scheduler is free to bind a pod into the gap between that sweep and this
// claim, and central holds nothing that can see it. Every check in handleIdleNode
// is against central's own record and passes anyway.
//
// So the last thing before the irreversible claim is a question put to the
// cluster that owns the node: cordon it, then look again. A no here means no
// claim, no provider teardown, no workload finish, and — just as important — no
// acknowledgement, so the request stays with the connector rather than being
// quietly closed.
func TestAgentStreamIdleDoesNotTearDownANodeThatTookWorkAfterTheReport(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()

	// A pod landed after the connector's last idle sweep. Nothing central holds
	// records that; only the preflight finds it.
	f.nodeTookWork.Store(true)

	f.reportIdle(t)
	f.awaitPreflight(t)
	if out := f.awaitReap(t); out.reaped {
		t.Fatalf("reap outcome = %+v, want the teardown refused by the preflight", out)
	}
	f.drain(t)

	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 — the node took work after the idle report", n)
	}
	if _, err := f.store.GetBurst(nodePhaseBurstID); err != nil {
		t.Fatalf("the burst was claimed despite a refused preflight: %v", err)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil || wl.FinishedAt != nil {
		t.Fatalf("workload = %+v err=%v, want it untouched by a teardown that did not happen", wl, err)
	}
	// Not acknowledged: the request is still the connector's, and its next sweep
	// either re-asserts it or withdraws it.
	f.requireNoAck(t)
}

// A managed Job burst has a completion path of its own, and an idle node under
// one means the Job has not started or is already over — not that the customer
// wants their run destroyed. Central's record decides this, never the connector.
//
// It is ACKNOWLEDGED rather than dropped: the connector holds an unacknowledged
// terminal report forever, so silently ignoring one would leave it re-sending a
// request central will never honour for as long as the node exists.
func TestAgentStreamIdleOnAManagedJobBurstIsAnAcknowledgedNoOp(t *testing.T) {
	f := newNodePhaseFixture(t)
	// seedBurst is the managed shape: no NodeOnly flag.
	f.seedBurst(t)
	f.seedWorkload()

	f.reportIdle(t)
	if ack := f.awaitAck(t); ack.BurstID != nodePhaseBurstID || ack.Phase != protocol.NodePhaseIdle {
		t.Fatalf("receipt = %+v, want the idle request acknowledged as a no-op", ack)
	}
	f.drain(t)

	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 — central owns a managed job burst's lifecycle", n)
	}
	if len(f.reaps) != 0 {
		t.Fatal("an idle report on a managed job burst reached the teardown path")
	}
	// Not cordoned either. The no-op answer is a receipt, not a half-teardown, and
	// cordoning here would take a node out of service for a Job that may be about
	// to start on it.
	f.requireNoPreflight(t)
	b := f.burst(t)
	if b.NodePhase != "" || b.Status != state.BurstStatusProvisioning {
		t.Fatalf("burst mutated by an idle report it does not apply to: %+v", b)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil || wl.FinishedAt != nil {
		t.Fatalf("workload = %+v err=%v, want it untouched", wl, err)
	}
}

// Everything an idle report can name that central must refuse. A Removed that
// central gets wrong destroys a Node object Kubernetes had already lost; an Idle
// that central gets wrong destroys a node under traffic — so each of these tears
// nothing down AND acknowledges nothing, leaving the request outstanding rather
// than quietly closing it.
func TestAgentStreamIdleRefusesEventsItCannotAuthorise(t *testing.T) {
	tests := []struct {
		name  string
		seed  func(t *testing.T, f *nodePhaseFixture)
		event protocol.NodeEvent
	}{
		{
			// Another tenant's burst is indistinguishable from one that does not
			// exist, and both are indistinguishable from an id that was never
			// minted. That is the point: a connector able to tell them apart would
			// have a burst-id oracle across the fleet.
			name: "another tenant's burst",
			seed: func(t *testing.T, f *nodePhaseFixture) {
				f.store.AddCustomer(&state.Customer{ID: "cust_other", Token: "tok_other", Plan: "pro"})
				if err := f.store.PutBurst(&state.Burst{
					ID: "burst_other", CustomerID: "cust_other", ClusterID: "c1",
					Backend: "linode", BackendID: "vm-other", NodeName: "ys-burst-other",
					NodeOnly: true, Status: state.BurstStatusRunning, CreatedAt: time.Now(),
				}); err != nil {
					t.Fatalf("seed foreign burst: %v", err)
				}
			},
			event: protocol.NodeEvent{
				BurstID: "burst_other", NodeName: "ys-burst-other", Phase: protocol.NodePhaseIdle,
			},
		},
		{
			name: "an id central never minted",
			seed: func(t *testing.T, f *nodePhaseFixture) { f.seedNodeOnlyBurst(t) },
			event: protocol.NodeEvent{
				BurstID: "burst_nosuch", NodeName: "ys-burst-nosuch", Phase: protocol.NodePhaseIdle,
			},
		},
		{
			// The legacy shape, and it gets the Removed path's answer: refused.
			// A record that cannot say where central booked the capacity is
			// unverifiable, and the connectivity central happens to see now is not
			// evidence about a booking made earlier — a tenant with several
			// clusters on one token would have this connector destroying a node in
			// whichever of them the burst actually landed.
			name: "a burst with no recorded cluster",
			seed: func(t *testing.T, f *nodePhaseFixture) {
				f.seedNodeOnlyBurst(t)
				b := f.burst(t)
				legacy := *b
				legacy.ClusterID = ""
				if err := f.store.PutBurst(&legacy); err != nil {
					t.Fatalf("clear the burst's cluster: %v", err)
				}
			},
			event: protocol.NodeEvent{
				BurstID: nodePhaseBurstID, NodeName: nodePhaseNodeName, Phase: protocol.NodePhaseIdle,
			},
		},
		{
			// A tenant may connect several clusters on one token. Without this
			// check any of them could ask for the teardown of capacity running in
			// another.
			name: "a burst booked in a different cluster",
			seed: func(t *testing.T, f *nodePhaseFixture) {
				f.seedNodeOnlyBurst(t)
				b := f.burst(t)
				moved := *b
				moved.ClusterID = "c2"
				if err := f.store.PutBurst(&moved); err != nil {
					t.Fatalf("move burst to another cluster: %v", err)
				}
			},
			event: protocol.NodeEvent{
				BurstID: nodePhaseBurstID, NodeName: nodePhaseNodeName, Phase: protocol.NodePhaseIdle,
			},
		},
		{
			// The connector derives the burst id FROM the node name, so requiring
			// the pair central minted closes that loop: a burst id cannot be
			// attached to whatever node the event happens to name.
			name: "a node this burst does not have",
			seed: func(t *testing.T, f *nodePhaseFixture) { f.seedNodeOnlyBurst(t) },
			event: protocol.NodeEvent{
				BurstID: nodePhaseBurstID, NodeName: "ys-burst-someoneelse", Phase: protocol.NodePhaseIdle,
			},
		},
		{
			// An unverifiable claim is not a weaker claim here. A burst with no
			// recorded node name cannot satisfy the check and is refused.
			name: "a burst with no recorded node name",
			seed: func(t *testing.T, f *nodePhaseFixture) {
				f.seedNodeOnlyBurst(t)
				b := f.burst(t)
				anon := *b
				anon.NodeName = ""
				if err := f.store.PutBurst(&anon); err != nil {
					t.Fatalf("clear the burst's node name: %v", err)
				}
			},
			event: protocol.NodeEvent{
				BurstID: nodePhaseBurstID, NodeName: nodePhaseNodeName, Phase: protocol.NodePhaseIdle,
			},
		},
		{
			name: "no node name at all",
			seed: func(t *testing.T, f *nodePhaseFixture) { f.seedNodeOnlyBurst(t) },
			event: protocol.NodeEvent{
				BurstID: nodePhaseBurstID, Phase: protocol.NodePhaseIdle,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newNodePhaseFixture(t)
			tc.seed(t, f)
			f.seedWorkload()

			f.report(t, tc.event)
			// drain is the barrier: frames are handled in order, so a connection
			// that has finished disconnecting has finished everything queued
			// behind it. Asserting before it would pass on a frame central had
			// simply not read yet.
			f.drain(t)
			f.requireNoAck(t)
			f.requireNoPreflight(t)

			if n := f.reaper.teardownCount(tc.event.BurstID); n != 0 {
				t.Fatalf("teardowns = %d, want 0", n)
			}
			if len(f.reaps) != 0 {
				t.Fatal("an unauthorised idle request reached the teardown path")
			}
			wl, err := f.store.GetWorkload(nodePhaseWorkload)
			if err != nil || wl.FinishedAt != nil {
				t.Fatalf("workload = %+v err=%v, want it untouched", wl, err)
			}
		})
	}
}

// failingRecordReader is a durable backend that cannot answer the read an idle
// request is authorised against.
type failingRecordReader struct {
	mu    sync.Mutex
	calls int
}

func (r *failingRecordReader) BurstForNodeTeardown(context.Context, string, string) (*state.Burst, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return nil, false, errors.New("postgres is down")
}

func (r *failingRecordReader) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// An unreadable burst is UNKNOWN, not "not yours" and not "not nodeOnly".
// Central tears nothing down on it — the claim is irreversible — and
// acknowledges nothing either, so the request stays with the connector and is
// retried once the backend is back.
func TestAgentStreamIdleStaysOutstandingWhenTheBurstCannotBeRead(t *testing.T) {
	reader := &failingRecordReader{}
	f := newNodePhaseFixture(t, withBurstNodeRecordReader(reader))
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()

	f.reportIdle(t)
	f.drain(t)

	if reader.count() == 0 {
		t.Fatal("the authorisation read was never attempted")
	}
	f.requireNoAck(t)
	f.requireNoPreflight(t)
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 when the burst is unreadable", n)
	}
	if len(f.reaps) != 0 {
		t.Fatal("an idle request central could not verify reached the teardown path")
	}
	b := f.burst(t)
	if b.NodePhase != "" || b.Status != state.BurstStatusRunning {
		t.Fatalf("burst mutated on an unverifiable idle request: %+v", b)
	}
}

// A teardown that failed re-queues the burst for a later reap. The node is still
// up and still billing, so the connector must keep its request: acknowledging
// here would retire the very signal that retries it.
func TestAgentStreamIdleIsNotAcknowledgedWhenTeardownFails(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()
	f.reaper.setErr(errors.New("provider delete failed"))

	f.reportIdle(t)
	if out := f.awaitReap(t); out.reaped {
		t.Fatalf("reap outcome = %+v, want a teardown that failed", out)
	}
	f.drain(t)

	f.requireNoAck(t)
	if _, err := f.store.GetBurst(nodePhaseBurstID); err != nil {
		t.Fatalf("the burst was not re-queued after a failed teardown: %v", err)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil || wl.FinishedAt != nil {
		t.Fatalf("workload = %+v err=%v, want it untouched by a teardown that did not happen", wl, err)
	}
}

// Row absence is not a teardown receipt. A DELETE ... RETURNING claim can have an
// unknown client outcome after the database committed: the row is gone, but
// nobody holds the provider record and no teardown was queued. Acknowledging on
// absence alone would retire the connector's retry over a burst still running.
func TestAgentStreamIdleIsNotAcknowledgedOnAnAbsentBurstWithoutAReceipt(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()
	if _, won, err := f.store.ClaimBurst(nodePhaseBurstID); err != nil || !won {
		t.Fatalf("stage the ambiguous missing row: won=%v err=%v", won, err)
	}

	f.reportIdle(t)
	f.drain(t)
	f.requireNoAck(t)
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 without a claim result", n)
	}
}

// The other half: the durable receipt IS proof, and it is what releases a
// connector whose request lost the claim race to another winner. No second
// teardown happens — the receipt says one already did.
func TestAgentStreamIdleIsAcknowledgedOnceTheReceiptProvesTeardown(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()
	if _, won, err := f.store.ClaimBurst(nodePhaseBurstID); err != nil || !won {
		t.Fatalf("stage the claimed burst: won=%v err=%v", won, err)
	}
	if _, err := f.store.RecordBurstReap(context.Background(), nodePhaseBurstID, state.DevCustomerID); err != nil {
		t.Fatalf("record the teardown receipt: %v", err)
	}

	f.reportIdle(t)
	if ack := f.awaitAck(t); ack.BurstID != nodePhaseBurstID || ack.Phase != protocol.NodePhaseIdle {
		t.Fatalf("receipt = %+v, want the idle request acknowledged once teardown is proven", ack)
	}
	f.drain(t)
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 — the receipt is proof, not a second teardown", n)
	}
}

// The races that all end at the same claim. A repeated idle request, a removal
// arriving behind one, and the workload's own completion converge on a single
// teardown and a single cost accrual, because every one of them goes through
// ClaimBurst rather than through a path of its own.
func TestAgentStreamIdleRemovalAndCompletionRacesStayIdempotent(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedNodeOnlyBurst(t)
	f.seedWorkload()

	// The completion landed first: status and receipt written, burst not yet
	// claimed. This is the window the paths overlap in.
	outcome := &state.WorkloadOutcome{
		Compute: state.WorkloadComputeOutcome{Result: state.WorkloadResultSucceeded},
	}
	if !f.store.FinishWorkloadWithOutcome(nodePhaseWorkload, "succeeded", time.Now().UTC(), false, outcome) {
		t.Fatal("could not stage the completed workload")
	}

	f.reportIdle(t)
	if out := f.awaitReap(t); !out.reaped {
		t.Fatalf("reap outcome = %+v, want the first idle request to win the claim", out)
	}
	f.awaitAck(t)

	// A repeated request and a removal behind it. The burst is gone, so both find
	// the receipt the first teardown wrote and are acknowledged without a second
	// teardown.
	f.reportIdle(t)
	if ack := f.awaitAck(t); ack.Phase != protocol.NodePhaseIdle {
		t.Fatalf("repeated idle receipt = %+v, want Idle", ack)
	}
	f.report(t, protocol.NodeEvent{
		BurstID: nodePhaseBurstID, NodeName: nodePhaseNodeName, Phase: protocol.NodePhaseRemoved,
	})
	if ack := f.awaitAck(t); ack.Phase != protocol.NodePhaseRemoved {
		t.Fatalf("removal receipt = %+v, want Removed", ack)
	}
	f.drain(t)

	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 1 {
		t.Fatalf("teardowns = %d, want exactly 1 across every racing path", n)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil {
		t.Fatalf("workload: %v", err)
	}
	// onlyIfUnfinished: the run's own reported result outranks the teardown.
	if wl.Status != "succeeded" {
		t.Fatalf("status = %q, want succeeded — an idle teardown must not overwrite a reported outcome", wl.Status)
	}
	if wl.Outcome == nil || wl.Outcome.Compute.Result != state.WorkloadResultSucceeded {
		t.Fatalf("outcome receipt = %+v, want the reported success intact", wl.Outcome)
	}
}
