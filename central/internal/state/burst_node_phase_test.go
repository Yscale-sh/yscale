package state

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

// Every burst written before connectors reported node lifecycle is still in the
// table. It has to decode without inventing a phase, and re-marshal without
// claiming one — the durable UPDATE keys off these field names, so a record
// that grew empty ones would be indistinguishable from a real report of "".
func TestBurstNodePhaseIsLegacyJSONCompatible(t *testing.T) {
	var legacy Burst
	stored := `{"ID":"burst_old","CustomerID":"cust_a","Backend":"linode","Status":"provisioning"}`
	if err := json.Unmarshal([]byte(stored), &legacy); err != nil {
		t.Fatalf("a stored legacy burst no longer decodes: %v", err)
	}
	if legacy.NodePhase != "" || legacy.NodePhaseReason != "" || legacy.NodePhaseAt != nil {
		t.Fatalf("legacy record decoded with an invented phase: %+v", legacy)
	}
	// Same rule for the occupancy stamp, and it matters more: an absent one has
	// to mean "no connector has ever proved it can see this node's pods", which
	// is what puts the burst's silence ceiling back on its creation time.
	if legacy.OccupancyObservedAt != nil {
		t.Fatalf("legacy record decoded with an invented occupancy observation: %+v", legacy)
	}
	raw, err := json.Marshal(&legacy)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "NodePhase") || strings.Contains(string(raw), "OccupancyObservedAt") {
		t.Fatalf("re-marshalled legacy record carries phase fields: %s", raw)
	}
}

func nodePhaseUpdate(phase string) BurstNodePhaseUpdate {
	now := time.Now().UTC()
	return BurstNodePhaseUpdate{
		BurstID:     "b1",
		CustomerID:  "cust_a",
		Phase:       phase,
		ObservedAt:  now,
		ReceiptTime: now,
	}
}

func seedPhaseBurst(s *Store) *Burst {
	b := &Burst{ID: "b1", CustomerID: "cust_a", Backend: "linode", Status: BurstStatusProvisioning}
	s.bursts[b.ID] = b
	return b
}

// The happy path, and the only one that may move a status: the burst exists and
// belongs to the tenant whose connector reported.
func TestUpdateBurstNodePhaseAppliesAndMapsStatus(t *testing.T) {
	tests := []struct {
		phase      string
		wantStatus string
	}{
		{phase: protocol.NodePhaseJoining, wantStatus: BurstStatusJoining},
		{phase: protocol.NodePhaseReady, wantStatus: BurstStatusRunning},
		{phase: protocol.NodePhaseNotReady, wantStatus: BurstStatusDegraded},
		{phase: protocol.NodePhaseRemoved, wantStatus: BurstStatusRemoved},
	}
	for _, tc := range tests {
		t.Run(tc.phase, func(t *testing.T) {
			s := emptyStore()
			seedPhaseBurst(s)

			applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(tc.phase))
			if err != nil || !applied {
				t.Fatalf("update = applied:%v err:%v, want applied:true err:nil", applied, err)
			}
			b, err := s.GetBurst("b1")
			if err != nil {
				t.Fatalf("GetBurst: %v", err)
			}
			if b.NodePhase != tc.phase || b.Status != tc.wantStatus {
				t.Fatalf("burst = phase:%q status:%q, want %q/%q", b.NodePhase, b.Status, tc.phase, tc.wantStatus)
			}
			if b.NodePhaseAt == nil {
				t.Fatal("no observation time recorded; a phase with no time cannot be read as current")
			}
		})
	}
}

// A repeat of the SAME phase is not a no-op: NodePhaseAt is what a dashboard
// reads as "last heard from", so a re-stated phase has to move it forward rather
// than be swallowed as unchanged. It is NOT what the nodeOnly backstop measures
// — see the occupancy tests below.
func TestUpdateBurstNodePhaseRefreshesTheObservationTimeOnARepeat(t *testing.T) {
	s := emptyStore()
	seedPhaseBurst(s)

	first := nodePhaseUpdate(protocol.NodePhaseReady)
	first.ObservedAt = time.Now().Add(-time.Hour).UTC()
	if applied, err := s.UpdateBurstNodePhase(context.Background(), first); err != nil || !applied {
		t.Fatalf("first update = applied:%v err:%v", applied, err)
	}

	repeat := nodePhaseUpdate(protocol.NodePhaseReady)
	applied, err := s.UpdateBurstNodePhase(context.Background(), repeat)
	if err != nil || !applied {
		t.Fatalf("repeat of an unchanged phase = applied:%v err:%v, want it to apply", applied, err)
	}
	b, err := s.GetBurst("b1")
	if err != nil {
		t.Fatalf("GetBurst: %v", err)
	}
	if b.NodePhaseAt == nil || !b.NodePhaseAt.After(first.ObservedAt) {
		t.Fatalf("observation time = %v, want it moved forward past %v by the repeat",
			b.NodePhaseAt, first.ObservedAt)
	}
}

// The stamp the nodeOnly backstop measures from moves ONLY on the explicit
// claim, and it moves on both store paths.
//
// This is the defect the field exists to fix. Node health is reported by
// connectors that could never end a nodeOnly burst — one scoped to a namespace,
// one that has lost cluster-wide pod LIST — so any health report renewing the
// ceiling meant capacity nothing was watching looked permanently observed. The
// repeats matter as much as the transitions: an unchanged Ready re-stated every
// few minutes is exactly the traffic that used to hold it open.
func TestUpdateBurstNodePhaseStampsOccupancyOnlyOnTheExplicitClaim(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "in-memory"
		if durable {
			name = "durable"
		}
		t.Run(name, func(t *testing.T) {
			s, p := storeWith(durable, nil, nil)
			seedPhaseBurst(s)
			if p != nil {
				p.phaseRows = map[string]*Burst{"b1": {ID: "b1", CustomerID: "cust_a", Status: BurstStatusProvisioning}}
			}
			read := func(t *testing.T) *Burst {
				t.Helper()
				b, err := s.GetBurst("b1")
				if err != nil {
					t.Fatalf("GetBurst: %v", err)
				}
				return b
			}

			// A plain health report, and then the SAME phase again. Neither is a
			// claim about pod visibility, so neither may stamp.
			for i := 0; i < 2; i++ {
				if applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseReady)); err != nil || !applied {
					t.Fatalf("health report %d = applied:%v err:%v", i, applied, err)
				}
				if got := read(t); got.OccupancyObservedAt != nil {
					t.Fatalf("a health report stamped the occupancy observation (%v); the nodeOnly ceiling would never fire for a connector that cannot see pods",
						got.OccupancyObservedAt)
				}
			}

			observed := nodePhaseUpdate(protocol.NodePhaseReady)
			observed.OccupancyObserved = true
			observed.ObservedAt = time.Now().UTC()
			if applied, err := s.UpdateBurstNodePhase(context.Background(), observed); err != nil || !applied {
				t.Fatalf("observation = applied:%v err:%v", applied, err)
			}
			first := read(t).OccupancyObservedAt
			if first == nil {
				t.Fatal("the explicit occupancy claim did not stamp anything")
			}

			// And a health report AFTER it leaves the stamp exactly where it was: a
			// fresh phase must never postpone the backstop.
			later := nodePhaseUpdate(protocol.NodePhaseNotReady)
			later.ObservedAt = first.Add(time.Hour)
			if applied, err := s.UpdateBurstNodePhase(context.Background(), later); err != nil || !applied {
				t.Fatalf("later health report = applied:%v err:%v", applied, err)
			}
			got := read(t)
			if got.OccupancyObservedAt == nil || !got.OccupancyObservedAt.Equal(*first) {
				t.Fatalf("occupancy stamp = %v, want it untouched at %v by a health-only report",
					got.OccupancyObservedAt, first)
			}
			if got.NodePhaseAt == nil || !got.NodePhaseAt.After(*first) {
				t.Fatalf("NodePhaseAt = %v, want the health report to still move the status timestamp", got.NodePhaseAt)
			}
		})
	}
}

// The record is replaced, not edited. GetBurst, ListBursts and BurstsForCustomer
// all hand out the live pointer and their callers read it without the lock, so a
// mutation in place is a data race — and one that shows a dashboard a
// half-applied phase.
func TestUpdateBurstNodePhaseReplacesRatherThanMutates(t *testing.T) {
	s := emptyStore()
	held := seedPhaseBurst(s)

	if applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseReady)); err != nil || !applied {
		t.Fatalf("update = applied:%v err:%v", applied, err)
	}
	if held.NodePhase != "" || held.Status != BurstStatusProvisioning {
		t.Fatalf("the record a reader already held was edited underneath it: %+v", held)
	}
}

// Everything the update must refuse. Each is "this event changes nothing", and
// the caller may not tell them apart — a connector able to distinguish an
// unknown burst from another tenant's would have an id oracle across the fleet.
func TestUpdateBurstNodePhaseRefusals(t *testing.T) {
	tests := []struct {
		name   string
		seed   func(s *Store)
		update BurstNodePhaseUpdate
	}{
		{
			name:   "no such burst",
			seed:   func(*Store) {},
			update: nodePhaseUpdate(protocol.NodePhaseReady),
		},
		{
			name:   "another tenant's burst",
			seed:   func(s *Store) { seedPhaseBurst(s) },
			update: BurstNodePhaseUpdate{BurstID: "b1", CustomerID: "cust_b", Phase: protocol.NodePhaseRemoved},
		},
		{
			name:   "a phase outside the closed set",
			seed:   func(s *Store) { seedPhaseBurst(s) },
			update: BurstNodePhaseUpdate{BurstID: "b1", CustomerID: "cust_a", Phase: "Terminated"},
		},
		{
			name:   "no burst id",
			seed:   func(s *Store) { seedPhaseBurst(s) },
			update: BurstNodePhaseUpdate{CustomerID: "cust_a", Phase: protocol.NodePhaseReady},
		},
		{
			name:   "no tenant",
			seed:   func(s *Store) { seedPhaseBurst(s) },
			update: BurstNodePhaseUpdate{BurstID: "b1", Phase: protocol.NodePhaseReady},
		},
		{
			// Idle is a valid wire phase but not node health: it is a request to
			// tear a node down, made about a node that is Ready and answering. A
			// burst carrying it as its recorded phase would be showing a customer a
			// status for a node that is fine, and would keep showing it if the
			// teardown were then refused or lost.
			name:   "an idle teardown request, which is not health",
			seed:   func(s *Store) { seedPhaseBurst(s) },
			update: nodePhaseUpdate(protocol.NodePhaseIdle),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := emptyStore()
			tc.seed(s)

			applied, err := s.UpdateBurstNodePhase(context.Background(), tc.update)
			if err != nil {
				t.Fatalf("a refusal is not an error: %v", err)
			}
			if applied {
				t.Fatal("update applied; a refused event must change nothing")
			}
			if b, gerr := s.GetBurst("b1"); gerr == nil {
				if b.NodePhase != "" || b.Status != BurstStatusProvisioning {
					t.Fatalf("burst mutated by a refused update: %+v", b)
				}
			}
		})
	}
}

// Removed is terminal. A late Ready arriving behind it — a reconnect flush, a
// redelivered frame — must not put a destroyed node back on a dashboard as
// running, and a repeated Removed must not re-apply.
func TestUpdateBurstNodePhaseTerminalCannotRegress(t *testing.T) {
	s := emptyStore()
	seedPhaseBurst(s)

	if applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseRemoved)); err != nil || !applied {
		t.Fatalf("first Removed = applied:%v err:%v", applied, err)
	}
	for _, phase := range []string{protocol.NodePhaseReady, protocol.NodePhaseNotReady, protocol.NodePhaseRemoved} {
		applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(phase))
		if err != nil {
			t.Fatalf("%s after Removed: %v", phase, err)
		}
		if applied {
			t.Fatalf("%s applied after Removed; the terminal phase is not terminal", phase)
		}
	}
	b, err := s.GetBurst("b1")
	if err != nil {
		t.Fatalf("GetBurst: %v", err)
	}
	if b.NodePhase != protocol.NodePhaseRemoved || b.Status != BurstStatusRemoved {
		t.Fatalf("burst = %+v, want it still Removed", b)
	}
}

// The race this design exists for. ClaimBurst has already removed the record —
// the reaper won it, the VM is being destroyed — and a phase update landing
// behind it must not put the row back. A resurrected burst is a VM the watchdog
// re-bills and tries to tear down again, forever.
func TestUpdateBurstNodePhaseCannotResurrectAClaimedBurst(t *testing.T) {
	s := emptyStore()
	seedPhaseBurst(s)

	if _, won, err := s.ClaimBurst("b1"); !won || err != nil {
		t.Fatalf("ClaimBurst = won:%v err:%v", won, err)
	}

	applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseReady))
	if err != nil {
		t.Fatalf("update after the claim: %v", err)
	}
	if applied {
		t.Fatal("a phase applied to a burst that had already been claimed")
	}
	if _, err := s.GetBurst("b1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetBurst after the update = %v, want ErrNotFound — the claimed row came back", err)
	}
}

// With a durable backend the DATABASE decides, exactly as it does for the claim.
// This replica's map may not hold a burst another central created, and cannot
// see a claim another central just won, so a refusal from the row is final and a
// local copy may not override it.
func TestUpdateBurstNodePhaseDurableDecides(t *testing.T) {
	t.Run("durable refusal is not overridden by memory", func(t *testing.T) {
		s := emptyStore()
		seedPhaseBurst(s)
		// No row: the statement matched nothing — claimed, foreign, or terminal.
		s.persist = &burstFailPersister{phaseRows: map[string]*Burst{}}

		applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseRemoved))
		if err != nil || applied {
			t.Fatalf("update = applied:%v err:%v, want applied:false err:nil", applied, err)
		}
		b, gerr := s.GetBurst("b1")
		if gerr != nil {
			t.Fatalf("GetBurst: %v", gerr)
		}
		if b.NodePhase != "" {
			t.Fatalf("memory applied a phase the database refused: %+v", b)
		}
	})

	t.Run("memory follows the row the database returns", func(t *testing.T) {
		s := emptyStore()
		seedPhaseBurst(s)
		s.persist = &burstFailPersister{phaseRows: map[string]*Burst{
			"b1": {ID: "b1", CustomerID: "cust_a", Backend: pgBackend},
		}}

		applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseReady))
		if err != nil || !applied {
			t.Fatalf("update = applied:%v err:%v, want applied:true err:nil", applied, err)
		}
		b, err := s.GetBurst("b1")
		if err != nil {
			t.Fatalf("GetBurst: %v", err)
		}
		if b.Backend != pgBackend {
			t.Fatalf("in-memory copy = %q, want the durable record %q", b.Backend, pgBackend)
		}
		if b.NodePhase != protocol.NodePhaseReady || b.Status != BurstStatusRunning {
			t.Fatalf("burst = phase:%q status:%q, want Ready/running", b.NodePhase, b.Status)
		}
	})

	t.Run("a burst this replica never held is not inserted", func(t *testing.T) {
		// Another central created the burst, so the row exists and the update
		// applies — but re-creating it here would resurrect a burst this
		// replica's own claim may have just removed.
		s := emptyStore()
		s.persist = &burstFailPersister{phaseRows: map[string]*Burst{
			"b1": {ID: "b1", CustomerID: "cust_a", Backend: pgBackend},
		}}

		applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseReady))
		if err != nil || !applied {
			t.Fatalf("update = applied:%v err:%v, want applied:true err:nil", applied, err)
		}
		if _, err := s.GetBurst("b1"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetBurst = %v, want ErrNotFound — a durable-only burst was inserted locally", err)
		}
	})
}

// The ownership check gates an irreversible teardown, so its three answers have
// to stay three: yours, not yours, and unknown. Collapsing the third into the
// second would tear a node down on a database that could not be reached.
func TestBurstOwnedBy(t *testing.T) {
	t.Run("in memory", func(t *testing.T) {
		s := emptyStore()
		seedPhaseBurst(s)

		owned, err := s.BurstOwnedBy(context.Background(), "b1", "cust_a")
		if err != nil || !owned {
			t.Fatalf("owner check = %v err:%v, want the tenant's own burst owned", owned, err)
		}
		// The two refusals are one answer on purpose: a caller that could tell
		// them apart would be a burst-id oracle across the fleet.
		if owned, err := s.BurstOwnedBy(context.Background(), "b1", "cust_b"); err != nil || owned {
			t.Fatalf("owner check for another tenant = %v err:%v, want false", owned, err)
		}
		if owned, err := s.BurstOwnedBy(context.Background(), "b_nosuch", "cust_a"); err != nil || owned {
			t.Fatalf("owner check for an unknown id = %v err:%v, want false", owned, err)
		}
	})

	t.Run("the check does not mutate", func(t *testing.T) {
		s := emptyStore()
		b := seedPhaseBurst(s)
		if _, err := s.BurstOwnedBy(context.Background(), "b1", "cust_a"); err != nil {
			t.Fatalf("owner check: %v", err)
		}
		if b.NodePhase != "" || b.Status != BurstStatusProvisioning || b.NodePhaseAt != nil {
			t.Fatalf("the ownership check wrote to the burst: %+v", b)
		}
	})

	t.Run("durable decides", func(t *testing.T) {
		// This replica's map is empty; the row belongs to the tenant. A memory
		// answer would refuse teardown for a burst another central admitted.
		s := emptyStore()
		s.persist = &burstFailPersister{phaseRows: map[string]*Burst{
			"b1": {ID: "b1", CustomerID: "cust_a", Backend: pgBackend},
		}}

		owned, err := s.BurstOwnedBy(context.Background(), "b1", "cust_a")
		if err != nil || !owned {
			t.Fatalf("owner check = %v err:%v, want the durable row to decide", owned, err)
		}
		if owned, err := s.BurstOwnedBy(context.Background(), "b1", "cust_b"); err != nil || owned {
			t.Fatalf("owner check for another tenant = %v err:%v, want false", owned, err)
		}
	})

	t.Run("a failure is unknown, not a refusal", func(t *testing.T) {
		s := emptyStore()
		seedPhaseBurst(s)
		failure := errors.New("postgres is down")
		s.persist = &burstFailPersister{ownerErr: failure}

		owned, err := s.BurstOwnedBy(context.Background(), "b1", "cust_a")
		if !errors.Is(err, failure) {
			t.Fatalf("err = %v, want the durable failure", err)
		}
		if owned {
			t.Fatal("a check that failed reported ownership")
		}
	})
}

// BurstForNodeTeardown answers the same three ways BurstOwnedBy does — yours,
// not yours, unknown — and hands back the record an idle teardown is authorised
// against. The fields it carries are the ones the request may not supply:
// nodeOnly, the cluster the burst was booked in, and the node name central
// assigned.
func TestBurstForNodeTeardown(t *testing.T) {
	t.Run("in memory", func(t *testing.T) {
		s := emptyStore()
		b := seedPhaseBurst(s)
		b.ClusterID = "c1"
		b.NodeName = "ys-burst-b1"
		b.NodeOnly = true

		got, found, err := s.BurstForNodeTeardown(context.Background(), "b1", "cust_a")
		if err != nil || !found {
			t.Fatalf("read = found:%v err:%v, want the tenant's own burst", found, err)
		}
		if !got.NodeOnly || got.ClusterID != "c1" || got.NodeName != "ys-burst-b1" {
			t.Fatalf("record = %+v, want the fields an idle request is checked against", got)
		}
		// The two refusals are one answer, exactly as they are for BurstOwnedBy.
		if _, found, err := s.BurstForNodeTeardown(context.Background(), "b1", "cust_b"); err != nil || found {
			t.Fatalf("read for another tenant = found:%v err:%v, want false", found, err)
		}
		if _, found, err := s.BurstForNodeTeardown(context.Background(), "b_nosuch", "cust_a"); err != nil || found {
			t.Fatalf("read for an unknown id = found:%v err:%v, want false", found, err)
		}
	})

	t.Run("the read hands back a copy", func(t *testing.T) {
		// GetBurst hands out the live pointer and its callers read it unlocked, so
		// a record handed to an authorisation path must not be one a concurrent
		// phase update is replacing underneath it.
		s := emptyStore()
		b := seedPhaseBurst(s)
		got, _, err := s.BurstForNodeTeardown(context.Background(), "b1", "cust_a")
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if got == b {
			t.Fatal("the read handed back the live record rather than a copy")
		}
		got.NodeOnly = true
		if b.NodeOnly {
			t.Fatal("mutating the returned record changed the stored burst")
		}
	})

	t.Run("durable decides", func(t *testing.T) {
		// This replica's map is empty; the row belongs to the tenant. A memory
		// answer would refuse teardown for a burst another central admitted.
		s := emptyStore()
		s.persist = &burstFailPersister{phaseRows: map[string]*Burst{
			"b1": {ID: "b1", CustomerID: "cust_a", Backend: pgBackend, NodeOnly: true, NodeName: "ys-burst-b1"},
		}}

		got, found, err := s.BurstForNodeTeardown(context.Background(), "b1", "cust_a")
		if err != nil || !found || !got.NodeOnly {
			t.Fatalf("read = %+v found:%v err:%v, want the durable row to decide", got, found, err)
		}
		if _, found, err := s.BurstForNodeTeardown(context.Background(), "b1", "cust_b"); err != nil || found {
			t.Fatalf("read for another tenant = found:%v err:%v, want false", found, err)
		}
	})

	t.Run("a failure is unknown, not a refusal", func(t *testing.T) {
		s := emptyStore()
		seedPhaseBurst(s)
		failure := errors.New("postgres is down")
		s.persist = &burstFailPersister{ownerErr: failure}

		got, found, err := s.BurstForNodeTeardown(context.Background(), "b1", "cust_a")
		if !errors.Is(err, failure) {
			t.Fatalf("err = %v, want the durable failure", err)
		}
		if found || got != nil {
			t.Fatalf("a read that failed reported a burst: %+v found:%v", got, found)
		}
	})
}

func TestBurstReapRecordedRequiresTenantScopedReceipt(t *testing.T) {
	s := emptyStore()

	recorded, err := s.BurstReapRecorded(context.Background(), "b1", "cust_a")
	if err != nil || recorded {
		t.Fatalf("missing receipt = %v err:%v, want false/nil", recorded, err)
	}

	if written, err := s.RecordBurstReap(context.Background(), "b1", "cust_a"); err != nil || !written {
		t.Fatalf("RecordBurstReap = %v err:%v, want true/nil", written, err)
	}
	recorded, err = s.BurstReapRecorded(context.Background(), "b1", "cust_a")
	if err != nil || !recorded {
		t.Fatalf("matching receipt = %v err:%v, want true/nil", recorded, err)
	}
	if recorded, err = s.BurstReapRecorded(context.Background(), "b1", "cust_b"); err != nil || recorded {
		t.Fatalf("another tenant's receipt = %v err:%v, want false/nil", recorded, err)
	}
	if recorded, err = s.BurstReapRecorded(context.Background(), "b2", "cust_a"); err != nil || recorded {
		t.Fatalf("another burst's receipt = %v err:%v, want false/nil", recorded, err)
	}
}

func TestBurstReapRecordedUsesDurableTenantScopedReceipt(t *testing.T) {
	s := emptyStore()
	p := &burstFailPersister{}
	s.persist = p

	if recorded, err := s.RecordBurstReap(context.Background(), "b1", "cust_a"); err != nil || !recorded {
		t.Fatalf("RecordBurstReap = %v err:%v, want true/nil", recorded, err)
	}
	if recorded, err := s.BurstReapRecorded(context.Background(), "b1", "cust_a"); err != nil || !recorded {
		t.Fatalf("matching durable receipt = %v err:%v, want true/nil", recorded, err)
	}
	if recorded, err := s.BurstReapRecorded(context.Background(), "b1", "cust_b"); err != nil || recorded {
		t.Fatalf("foreign durable receipt = %v err:%v, want false/nil", recorded, err)
	}
}

// --- NodeObservation on the permanent Workload record ---

func seedPhaseWorkload(s *Store) *Workload {
	w := &Workload{ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "provisioning"}
	s.workloads[w.ID] = w
	return w
}

func seedPhaseBurstWithNode(s *Store) *Burst {
	b := seedPhaseBurst(s)
	b.NodeName = "ys-burst-b1"
	return b
}

// The workload's NodeObservation must be stamped atomically with the burst phase.
func TestUpdateBurstNodePhaseStampsWorkloadObservation(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	seedPhaseWorkload(s)

	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.Reason = "KubeletReady"
	applied, err := s.UpdateBurstNodePhase(context.Background(), u)
	if err != nil || !applied {
		t.Fatalf("update = applied:%v err:%v", applied, err)
	}
	wl, err := s.GetWorkload("wl1")
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if wl.NodeObservation == nil {
		t.Fatal("workload has no NodeObservation after a successful phase update")
	}
	obs := wl.NodeObservation
	if obs.NodeName != "ys-burst-b1" {
		t.Fatalf("NodeName = %q, want the burst's trusted name", obs.NodeName)
	}
	if obs.Phase != protocol.NodePhaseReady {
		t.Fatalf("Phase = %q, want %q", obs.Phase, protocol.NodePhaseReady)
	}
	if obs.Reason != "KubeletReady" {
		t.Fatalf("Reason = %q, want the bounded reason", obs.Reason)
	}
	if obs.ObservedAt.IsZero() {
		t.Fatal("ObservedAt is zero")
	}
}

// A subsequent phase update replaces the workload observation entirely.
func TestUpdateBurstNodePhaseReplacesWorkloadObservation(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	seedPhaseWorkload(s)

	first := nodePhaseUpdate(protocol.NodePhaseReady)
	first.Reason = "KubeletReady"
	first.ObservedAt = time.Now().Add(-time.Hour).UTC()
	if applied, err := s.UpdateBurstNodePhase(context.Background(), first); err != nil || !applied {
		t.Fatalf("first = applied:%v err:%v", applied, err)
	}

	second := nodePhaseUpdate(protocol.NodePhaseNotReady)
	second.Reason = "KubeletNotReady"
	second.ObservedAt = time.Now().UTC()
	if applied, err := s.UpdateBurstNodePhase(context.Background(), second); err != nil || !applied {
		t.Fatalf("second = applied:%v err:%v", applied, err)
	}

	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation.Phase != protocol.NodePhaseNotReady {
		t.Fatalf("Phase = %q, want the second observation", wl.NodeObservation.Phase)
	}
	if wl.NodeObservation.Reason != "KubeletNotReady" {
		t.Fatalf("Reason = %q, want the second observation's reason", wl.NodeObservation.Reason)
	}
	if !wl.NodeObservation.ObservedAt.After(first.ObservedAt) {
		t.Fatal("ObservedAt did not advance past the first observation")
	}
}

// A refused burst update must not touch the workload.
func TestUpdateBurstNodePhaseRefusalLeavesWorkloadUntouched(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	seedPhaseWorkload(s)

	applied, _ := s.UpdateBurstNodePhase(context.Background(),
		BurstNodePhaseUpdate{BurstID: "b1", CustomerID: "cust_b", Phase: protocol.NodePhaseReady})
	if applied {
		t.Fatal("update applied for wrong tenant")
	}
	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation != nil {
		t.Fatal("a refused update stamped the workload anyway")
	}
}

// A burst with no workload (nodeOnly) silently skips the workload stamp.
func TestUpdateBurstNodePhaseNoWorkloadIsNotAnError(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	// No workload seeded

	applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseReady))
	if err != nil || !applied {
		t.Fatalf("update = applied:%v err:%v", applied, err)
	}
}

// The workload copy-on-write discipline: a reader holding a workload pointer
// must not see its NodeObservation mutated by a later phase update.
func TestUpdateBurstNodePhaseWorkloadCopyIsolation(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	held := seedPhaseWorkload(s)

	if applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseReady)); err != nil || !applied {
		t.Fatalf("update = applied:%v err:%v", applied, err)
	}
	if held.NodeObservation != nil {
		t.Fatal("the workload pointer a reader held was mutated in place")
	}
}

// The workload observation survives durable round-trip: a legacy record without
// the field decodes cleanly, and a record carrying one marshals the snapshot.
func TestNodeObservationIsLegacyJSONCompatible(t *testing.T) {
	legacy := `{"ID":"wl_old","CustomerID":"cust_a","Status":"succeeded"}`
	var wl Workload
	if err := json.Unmarshal([]byte(legacy), &wl); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if wl.NodeObservation != nil {
		t.Fatal("legacy record decoded with an invented observation")
	}
	raw, _ := json.Marshal(&wl)
	if strings.Contains(string(raw), "NodeObservation") {
		t.Fatalf("re-marshalled legacy record carries an observation: %s", raw)
	}

	wl.NodeObservation = &NodeObservation{
		NodeName: "ys-burst-b1", Phase: "Ready", Reason: "KubeletReady",
		ObservedAt: time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC),
	}
	raw, _ = json.Marshal(&wl)
	if !strings.Contains(string(raw), "NodeObservation") {
		t.Fatalf("observation did not marshal: %s", raw)
	}
	var wl2 Workload
	if err := json.Unmarshal(raw, &wl2); err != nil {
		t.Fatalf("round-trip decode: %v", err)
	}
	if wl2.NodeObservation == nil || wl2.NodeObservation.Phase != "Ready" {
		t.Fatalf("round-trip observation = %+v, want the original", wl2.NodeObservation)
	}
}

// With a durable backend the workload observation follows the burst update.
func TestUpdateBurstNodePhaseDurableStampsWorkloadObservation(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	seedPhaseWorkload(s)
	s.persist = &burstFailPersister{phaseRows: map[string]*Burst{
		"b1": {ID: "b1", CustomerID: "cust_a", Backend: pgBackend, NodeName: "ys-burst-b1"},
	}}

	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.Reason = "KubeletReady"
	applied, err := s.UpdateBurstNodePhase(context.Background(), u)
	if err != nil || !applied {
		t.Fatalf("update = applied:%v err:%v", applied, err)
	}
	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation == nil {
		t.Fatal("durable path did not stamp the workload observation")
	}
	if wl.NodeObservation.NodeName != "ys-burst-b1" || wl.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("observation = %+v, want the durable burst's fields", wl.NodeObservation)
	}
}

// The observation is tenant-scoped: only the workload matching both the burst
// and the tenant receives the stamp. A workload with the same burst but a
// different tenant is untouched.
func TestUpdateBurstNodePhaseWorkloadObservationIsTenantScoped(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	seedPhaseWorkload(s)
	// Second workload for a different tenant sharing the same burst id pattern
	s.workloads["wl_foreign"] = &Workload{ID: "wl_foreign", CustomerID: "cust_b", BurstID: "b1", Status: "provisioning"}

	if applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseReady)); err != nil || !applied {
		t.Fatalf("update = applied:%v err:%v", applied, err)
	}
	foreign, _ := s.GetWorkload("wl_foreign")
	if foreign.NodeObservation != nil {
		t.Fatal("another tenant's workload received the observation")
	}
	own, _ := s.GetWorkload("wl1")
	if own.NodeObservation == nil {
		t.Fatal("the tenant's own workload did not receive the observation")
	}
}

// A stale event whose source timestamp predates the stored observation must not
// regress either the burst or the workload. This is the in-memory ordering
// guard that prevents delayed or replayed NodeEvents from overwriting newer
// snapshots.
func TestUpdateBurstNodePhaseRejectsStaleInMemory(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	seedPhaseWorkload(s)

	newer := time.Now().UTC()
	first := nodePhaseUpdate(protocol.NodePhaseReady)
	first.Reason = "KubeletReady"
	first.ObservedAt = newer
	if applied, err := s.UpdateBurstNodePhase(context.Background(), first); err != nil || !applied {
		t.Fatalf("first = applied:%v err:%v", applied, err)
	}

	stale := nodePhaseUpdate(protocol.NodePhaseNotReady)
	stale.Reason = "KubeletNotReady"
	stale.ObservedAt = newer.Add(-time.Hour)
	applied, err := s.UpdateBurstNodePhase(context.Background(), stale)
	if err != nil {
		t.Fatalf("stale err = %v", err)
	}
	if applied {
		t.Fatal("a stale event was applied")
	}

	b, _ := s.GetBurst("b1")
	if b.NodePhase != protocol.NodePhaseReady {
		t.Fatalf("burst phase = %q, want Ready (stale NotReady must not regress)", b.NodePhase)
	}
	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation == nil || wl.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("workload observation = %+v, want Ready (stale must not regress)", wl.NodeObservation)
	}
}

// Health ordering and occupancy visibility are independent facts. A delayed
// health phase must not regress NodePhase or the Workload snapshot, but a newer
// successful pod-list observation still renews the nodeOnly silence ceiling.
func TestUpdateBurstNodePhaseStaleHealthAdvancesFreshOccupancyInMemory(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	seedPhaseWorkload(s)

	base := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	ready := nodePhaseUpdate(protocol.NodePhaseReady)
	ready.Reason = "KubeletReady"
	ready.ObservedAt = base.Add(2 * time.Minute)
	if applied, err := s.UpdateBurstNodePhase(context.Background(), ready); err != nil || !applied {
		t.Fatalf("ready = applied:%v err:%v", applied, err)
	}

	occupancy := nodePhaseUpdate(protocol.NodePhaseNotReady)
	occupancy.Reason = "delayed health"
	occupancy.ObservedAt = base.Add(time.Minute)
	occupancy.ReceiptTime = base.Add(3 * time.Minute)
	occupancy.OccupancyObserved = true
	if applied, err := s.UpdateBurstNodePhase(context.Background(), occupancy); err != nil || !applied {
		t.Fatalf("occupancy = applied:%v err:%v", applied, err)
	}

	b, _ := s.GetBurst("b1")
	if b.NodePhase != protocol.NodePhaseReady || b.NodePhaseAt == nil || !b.NodePhaseAt.Equal(ready.ObservedAt) {
		t.Fatalf("burst health regressed: %+v", b)
	}
	if b.OccupancyObservedAt == nil || !b.OccupancyObservedAt.Equal(occupancy.ReceiptTime) {
		t.Fatalf("occupancy time = %v, want receipt time %v", b.OccupancyObservedAt, occupancy.ReceiptTime)
	}
	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation == nil || wl.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("workload observation regressed: %+v", wl.NodeObservation)
	}
}

func TestUpdateBurstNodePhaseEqualHealthAdvancesOccupancyWithoutRestampingWorkload(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	seedPhaseWorkload(s)

	at := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	ready := nodePhaseUpdate(protocol.NodePhaseReady)
	ready.Reason = "KubeletReady"
	ready.ObservedAt = at
	ready.ReceiptTime = at
	if applied, err := s.UpdateBurstNodePhase(context.Background(), ready); err != nil || !applied {
		t.Fatalf("ready = applied:%v err:%v", applied, err)
	}

	replay := ready
	replay.Reason = "must not replace the phase observation"
	replay.OccupancyObserved = true
	if applied, err := s.UpdateBurstNodePhase(context.Background(), replay); err != nil || !applied {
		t.Fatalf("occupancy replay = applied:%v err:%v", applied, err)
	}

	b, _ := s.GetBurst("b1")
	if b.OccupancyObservedAt == nil || !b.OccupancyObservedAt.Equal(at) {
		t.Fatalf("occupancy time = %v, want %v", b.OccupancyObservedAt, at)
	}
	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation == nil || wl.NodeObservation.Reason != "KubeletReady" {
		t.Fatalf("equal phase replay restamped workload: %+v", wl.NodeObservation)
	}
}

// A stale event must not regress the workload observation even when the burst
// phase itself is allowed through (different codepath). This tests the workload
// guard independently: seed an observation, then attempt to stamp an older one.
func TestStampWorkloadNodeObservationRejectsStale(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	seedPhaseWorkload(s)

	newer := time.Now().UTC()
	first := nodePhaseUpdate(protocol.NodePhaseReady)
	first.Reason = "KubeletReady"
	first.ObservedAt = newer
	s.UpdateBurstNodePhase(context.Background(), first)

	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation == nil {
		t.Fatal("precondition: workload must have an observation")
	}

	s.mu.Lock()
	s.stampWorkloadNodeObservation(BurstNodePhaseUpdate{
		BurstID:    "b1",
		CustomerID: "cust_a",
		Phase:      protocol.NodePhaseNotReady,
		Reason:     "KubeletNotReady",
		ObservedAt: newer.Add(-time.Minute),
	}, "ys-burst-b1")
	s.mu.Unlock()

	wl, _ = s.GetWorkload("wl1")
	if wl.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("observation phase = %q, want Ready (stale stamp must not regress)", wl.NodeObservation.Phase)
	}
}

// PutWorkload must not erase a stored NodeObservation when the incoming record
// has none (the caller read the workload before the observation was stamped).
func TestPutWorkloadPreservesNodeObservation(t *testing.T) {
	s := emptyStore()
	base := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	s.PutWorkload(&Workload{
		ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "running",
		NodeObservation: &NodeObservation{
			NodeName: "ys-burst-b1", Phase: "Ready", Reason: "KubeletReady",
			ObservedAt: base,
		},
	})

	s.PutWorkload(&Workload{
		ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "succeeded",
	})

	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation == nil {
		t.Fatal("PutWorkload erased the stored NodeObservation")
	}
	if wl.NodeObservation.Phase != "Ready" {
		t.Fatalf("observation = %+v, want the original", wl.NodeObservation)
	}
	if wl.Status != "succeeded" {
		t.Fatalf("status = %q, want the incoming update", wl.Status)
	}
}

// PutWorkload must not regress a stored NodeObservation to an older one.
func TestPutWorkloadDoesNotRegressNodeObservation(t *testing.T) {
	s := emptyStore()
	base := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	s.PutWorkload(&Workload{
		ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "running",
		NodeObservation: &NodeObservation{
			NodeName: "ys-burst-b1", Phase: "NotReady", Reason: "KubeletNotReady",
			ObservedAt: base.Add(time.Hour),
		},
	})

	s.PutWorkload(&Workload{
		ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "running",
		NodeObservation: &NodeObservation{
			NodeName: "ys-burst-b1", Phase: "Ready", Reason: "KubeletReady",
			ObservedAt: base,
		},
	})

	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation.Phase != "NotReady" {
		t.Fatalf("observation phase = %q, want NotReady (older Ready must not regress)", wl.NodeObservation.Phase)
	}
}

// PutWorkload accepts a newer NodeObservation over an older one.
func TestPutWorkloadAcceptsNewerNodeObservation(t *testing.T) {
	s := emptyStore()
	base := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	s.PutWorkload(&Workload{
		ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "running",
		NodeObservation: &NodeObservation{
			NodeName: "ys-burst-b1", Phase: "Ready", Reason: "KubeletReady",
			ObservedAt: base,
		},
	})

	s.PutWorkload(&Workload{
		ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "running",
		NodeObservation: &NodeObservation{
			NodeName: "ys-burst-b1", Phase: "NotReady", Reason: "KubeletNotReady",
			ObservedAt: base.Add(time.Hour),
		},
	})

	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation.Phase != "NotReady" {
		t.Fatalf("observation phase = %q, want NotReady (newer observation must win)", wl.NodeObservation.Phase)
	}
}

// PutWorkloadDurable must not erase or regress a stored NodeObservation.
func TestPutWorkloadDurablePreservesNodeObservation(t *testing.T) {
	s := emptyStore()
	base := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	s.PutWorkload(&Workload{
		ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "running",
		NodeObservation: &NodeObservation{
			NodeName: "ys-burst-b1", Phase: "Ready", Reason: "KubeletReady",
			ObservedAt: base,
		},
	})

	err := s.PutWorkloadDurable(&Workload{
		ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "succeeded",
	})
	if err != nil {
		t.Fatalf("PutWorkloadDurable: %v", err)
	}

	wl, _ := s.GetWorkload("wl1")
	if wl.NodeObservation == nil || wl.NodeObservation.Phase != "Ready" {
		t.Fatalf("PutWorkloadDurable erased the stored NodeObservation: %+v", wl.NodeObservation)
	}
}

// A durable write that failed is reported, never swallowed and never applied
// locally. The caller fail-closes on it: the teardown a Removed triggers is
// irreversible, and the durable row is what every other replica reads to know
// it happened.
func TestUpdateBurstNodePhaseReportsDurableFailure(t *testing.T) {
	s := emptyStore()
	seedPhaseBurst(s)
	failure := errors.New("postgres is down")
	s.persist = &burstFailPersister{phaseErr: failure}

	applied, err := s.UpdateBurstNodePhase(context.Background(), nodePhaseUpdate(protocol.NodePhaseRemoved))
	if !errors.Is(err, failure) {
		t.Fatalf("err = %v, want the durable failure", err)
	}
	if applied {
		t.Fatal("a write that failed reported as applied")
	}
	b, gerr := s.GetBurst("b1")
	if gerr != nil {
		t.Fatalf("GetBurst: %v", gerr)
	}
	if b.NodePhase != "" || b.Status != BurstStatusProvisioning {
		t.Fatalf("memory applied a phase the database refused: %+v", b)
	}
}

// --- Guarded real-PostgreSQL integration tests ---

func pgStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	s, err := NewPostgres(context.Background(), dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// The atomic burst + workload update writes both in one transaction and both
// survive a process restart (new Store from the same DSN).
func TestPostgresNodeObservationAtomicAndRestart(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_np", Token: "tok_np", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_np", CustomerID: "cust_np", Backend: "linode", BackendID: "np1",
		NodeName: "ys-burst-np", Status: BurstStatusProvisioning, CreatedAt: now,
	})
	s.PutWorkload(&Workload{
		ID: "wl_np", CustomerID: "cust_np", BurstID: "burst_np",
		Status: "provisioning", CreatedAt: now,
	})

	observed := now.Add(time.Minute)
	applied, err := s.UpdateBurstNodePhase(context.Background(), BurstNodePhaseUpdate{
		BurstID: "burst_np", CustomerID: "cust_np",
		Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
		ObservedAt: observed,
	})
	if err != nil || !applied {
		t.Fatalf("update = applied:%v err:%v", applied, err)
	}

	wl, err := s.GetWorkload("wl_np")
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if wl.NodeObservation == nil || wl.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("observation = %+v, want Ready", wl.NodeObservation)
	}
	s.Close()

	s2, err := NewPostgres(context.Background(), os.Getenv("YSCALE_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer s2.Close()

	wl2, err := s2.GetWorkload("wl_np")
	if err != nil {
		t.Fatalf("reload workload: %v", err)
	}
	if wl2.NodeObservation == nil || wl2.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("observation after restart = %+v, want Ready", wl2.NodeObservation)
	}
	if wl2.NodeObservation.NodeName != "ys-burst-np" {
		t.Fatalf("NodeName = %q after restart", wl2.NodeObservation.NodeName)
	}
}

// The observation survives burst retirement: ClaimBurst deletes the burst row
// but the workload's NodeObservation persists.
func TestPostgresNodeObservationSurvivesBurstRetirement(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_ret", Token: "tok_ret", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_ret", CustomerID: "cust_ret", Backend: "linode", BackendID: "ret1",
		NodeName: "ys-burst-ret", Status: BurstStatusProvisioning, CreatedAt: now,
	})
	s.PutWorkload(&Workload{
		ID: "wl_ret", CustomerID: "cust_ret", BurstID: "burst_ret",
		Status: "running", CreatedAt: now,
	})

	s.UpdateBurstNodePhase(context.Background(), BurstNodePhaseUpdate{
		BurstID: "burst_ret", CustomerID: "cust_ret",
		Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
		ObservedAt: now.Add(time.Minute),
	})

	s.ClaimBurst("burst_ret")

	wl, _ := s.GetWorkload("wl_ret")
	if wl.NodeObservation == nil || wl.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("observation after claim = %+v, want Ready to survive retirement", wl.NodeObservation)
	}
}

// A stale event is rejected by the durable path: the burst row and workload
// observation are both unchanged.
func TestPostgresNodeObservationRejectsStale(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_stale", Token: "tok_stale", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_stale", CustomerID: "cust_stale", Backend: "linode", BackendID: "st1",
		NodeName: "ys-burst-stale", Status: BurstStatusProvisioning, CreatedAt: now,
	})
	s.PutWorkload(&Workload{
		ID: "wl_stale", CustomerID: "cust_stale", BurstID: "burst_stale",
		Status: "running", CreatedAt: now,
	})

	newer := now.Add(2 * time.Minute)
	s.UpdateBurstNodePhase(context.Background(), BurstNodePhaseUpdate{
		BurstID: "burst_stale", CustomerID: "cust_stale",
		Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
		ObservedAt: newer,
	})

	applied, err := s.UpdateBurstNodePhase(context.Background(), BurstNodePhaseUpdate{
		BurstID: "burst_stale", CustomerID: "cust_stale",
		Phase: protocol.NodePhaseNotReady, Reason: "KubeletNotReady",
		ObservedAt: now,
	})
	if err != nil {
		t.Fatalf("stale err = %v", err)
	}
	if applied {
		t.Fatal("stale event was applied against the database")
	}

	b, _ := s.GetBurst("burst_stale")
	if b.NodePhase != protocol.NodePhaseReady {
		t.Fatalf("burst phase = %q, want Ready (stale must not regress)", b.NodePhase)
	}
	wl, _ := s.GetWorkload("wl_stale")
	if wl.NodeObservation == nil || wl.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("workload observation = %+v, want Ready", wl.NodeObservation)
	}
}

func TestPostgresStaleHealthAdvancesFreshOccupancy(t *testing.T) {
	s := pgStore(t)
	base := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_occ", Token: "tok_occ", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_occ", CustomerID: "cust_occ", Backend: "linode", BackendID: "occ1",
		NodeName: "ys-burst-occ", Status: BurstStatusProvisioning, CreatedAt: base,
	})
	s.PutWorkload(&Workload{
		ID: "wl_occ", CustomerID: "cust_occ", BurstID: "burst_occ",
		Status: "running", CreatedAt: base,
	})

	readyAt := base.Add(2 * time.Minute)
	if applied, err := s.UpdateBurstNodePhase(context.Background(), BurstNodePhaseUpdate{
		BurstID: "burst_occ", CustomerID: "cust_occ",
		Phase: protocol.NodePhaseReady, Reason: "KubeletReady", ObservedAt: readyAt,
	}); err != nil || !applied {
		t.Fatalf("ready = applied:%v err:%v", applied, err)
	}

	occupancyAt := base.Add(time.Minute)
	if applied, err := s.UpdateBurstNodePhase(context.Background(), BurstNodePhaseUpdate{
		BurstID: "burst_occ", CustomerID: "cust_occ",
		Phase: protocol.NodePhaseNotReady, Reason: "delayed health", ObservedAt: occupancyAt,
		OccupancyObserved: true,
	}); err != nil || !applied {
		t.Fatalf("occupancy = applied:%v err:%v", applied, err)
	}

	b, _ := s.GetBurst("burst_occ")
	if b.NodePhase != protocol.NodePhaseReady || b.NodePhaseAt == nil || !b.NodePhaseAt.Equal(readyAt) {
		t.Fatalf("burst health regressed: %+v", b)
	}
	if b.OccupancyObservedAt == nil || !b.OccupancyObservedAt.Equal(occupancyAt) {
		t.Fatalf("occupancy time = %v, want %v", b.OccupancyObservedAt, occupancyAt)
	}
	wl, _ := s.GetWorkload("wl_occ")
	if wl.NodeObservation == nil || wl.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("workload observation regressed: %+v", wl.NodeObservation)
	}
}

func TestPostgresEqualHealthAdvancesOccupancyWithoutRestampingWorkload(t *testing.T) {
	s := pgStore(t)
	base := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_eq", Token: "tok_eq", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_eq", CustomerID: "cust_eq", Backend: "linode", BackendID: "eq1",
		NodeName: "ys-burst-eq", Status: BurstStatusProvisioning, CreatedAt: base,
	})
	s.PutWorkload(&Workload{
		ID: "wl_eq", CustomerID: "cust_eq", BurstID: "burst_eq",
		Status: "running", CreatedAt: base,
	})

	at := base.Add(time.Minute)
	ready := BurstNodePhaseUpdate{
		BurstID: "burst_eq", CustomerID: "cust_eq",
		Phase: protocol.NodePhaseReady, Reason: "KubeletReady", ObservedAt: at,
	}
	if applied, err := s.UpdateBurstNodePhase(context.Background(), ready); err != nil || !applied {
		t.Fatalf("ready = applied:%v err:%v", applied, err)
	}

	replay := ready
	replay.Reason = "must not replace the phase observation"
	replay.OccupancyObserved = true
	if applied, err := s.UpdateBurstNodePhase(context.Background(), replay); err != nil || !applied {
		t.Fatalf("occupancy replay = applied:%v err:%v", applied, err)
	}

	b, _ := s.GetBurst("burst_eq")
	if b.OccupancyObservedAt == nil || !b.OccupancyObservedAt.Equal(at) {
		t.Fatalf("occupancy time = %v, want %v", b.OccupancyObservedAt, at)
	}
	wl, _ := s.GetWorkload("wl_eq")
	if wl.NodeObservation == nil || wl.NodeObservation.Reason != "KubeletReady" {
		t.Fatalf("equal phase replay restamped workload: %+v", wl.NodeObservation)
	}
}

// Legacy JSON without the NodeObservation field decodes cleanly against a real
// database and does not invent an observation on reload.
func TestPostgresNodeObservationLegacyJSON(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_leg", Token: "tok_leg", Plan: "pro"})
	s.PutWorkload(&Workload{
		ID: "wl_leg", CustomerID: "cust_leg", BurstID: "burst_leg",
		Status: "succeeded", CreatedAt: now,
	})
	s.Close()

	s2, err := NewPostgres(context.Background(), os.Getenv("YSCALE_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer s2.Close()

	wl, _ := s2.GetWorkload("wl_leg")
	if wl.NodeObservation != nil {
		t.Fatalf("legacy record invented an observation after reload: %+v", wl.NodeObservation)
	}
}

// A whole-workload upsert must not erase or regress the stored NodeObservation
// in the database.
func TestPostgresUpsertWorkloadPreservesNodeObservation(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_up", Token: "tok_up", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_up", CustomerID: "cust_up", Backend: "linode", BackendID: "up1",
		NodeName: "ys-burst-up", Status: BurstStatusProvisioning, CreatedAt: now,
	})
	s.PutWorkload(&Workload{
		ID: "wl_up", CustomerID: "cust_up", BurstID: "burst_up",
		Status: "running", CreatedAt: now,
	})

	observed := now.Add(time.Minute)
	s.UpdateBurstNodePhase(context.Background(), BurstNodePhaseUpdate{
		BurstID: "burst_up", CustomerID: "cust_up",
		Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
		ObservedAt: observed,
	})

	s.PutWorkload(&Workload{
		ID: "wl_up", CustomerID: "cust_up", BurstID: "burst_up",
		Status: "succeeded", CreatedAt: now,
	})

	wl, _ := s.GetWorkload("wl_up")
	if wl.NodeObservation == nil || wl.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("upsert erased observation in memory: %+v", wl.NodeObservation)
	}

	s.Close()
	s2, err := NewPostgres(context.Background(), os.Getenv("YSCALE_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer s2.Close()

	wl2, _ := s2.GetWorkload("wl_up")
	if wl2.NodeObservation == nil || wl2.NodeObservation.Phase != protocol.NodePhaseReady {
		t.Fatalf("upsert erased observation in database: %+v", wl2.NodeObservation)
	}
	if wl2.Status != "succeeded" {
		t.Fatalf("upsert lost the status update: %q", wl2.Status)
	}
}

// --- Source-vs-legacy ordering and reconnect replay regressions ---

// The concrete bug this ordering contract fixes. A connector disconnects while
// holding a cached Ready (Kubernetes LastTransitionTime T=100). It reconnects at
// T=200 and replays the cached Ready. Before the fix, central rewrote T=100 to
// receipt time T=200, and a subsequent real NotReady at T=199 (source clock
// slightly behind) was rejected as stale. With stable source timestamps, the
// replayed Ready keeps T=100, and the real NotReady at T=199 wins.
func TestCachedReplayThenNewTransition(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "in-memory"
		if durable {
			name = "durable"
		}
		t.Run(name, func(t *testing.T) {
			s, p := storeWith(durable, nil, nil)
			seedPhaseBurstWithNode(s)
			seedPhaseWorkload(s)
			if p != nil {
				p.phaseRows = map[string]*Burst{"b1": {ID: "b1", CustomerID: "cust_a", Status: BurstStatusProvisioning, NodeName: "ys-burst-b1"}}
			}

			k8sReadyTime := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

			// First: connector sees Ready, sends it with the K8s timestamp.
			first := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a",
				Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
				ObservedAt: k8sReadyTime, SourceTimestamped: true,
				ReceiptTime: k8sReadyTime.Add(time.Second),
			}
			if applied, err := s.UpdateBurstNodePhase(context.Background(), first); err != nil || !applied {
				t.Fatalf("first Ready = applied:%v err:%v", applied, err)
			}

			// Connector disconnects, reconnects, replays the cached Ready.
			// Same K8s timestamp because it is the same event.
			replay := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a",
				Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
				ObservedAt: k8sReadyTime, SourceTimestamped: true,
				ReceiptTime: k8sReadyTime.Add(100 * time.Second),
			}
			applied, err := s.UpdateBurstNodePhase(context.Background(), replay)
			if err != nil {
				t.Fatalf("replay err = %v", err)
			}
			if applied {
				t.Fatal("replayed event with same timestamp was applied; it should be rejected as not-newer")
			}

			// Real NotReady arrives with a LATER K8s timestamp.
			k8sNotReadyTime := k8sReadyTime.Add(99 * time.Second)
			real := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a",
				Phase: protocol.NodePhaseNotReady, Reason: "KubeletNotReady",
				ObservedAt: k8sNotReadyTime, SourceTimestamped: true,
				ReceiptTime: k8sReadyTime.Add(101 * time.Second),
			}
			if applied, err := s.UpdateBurstNodePhase(context.Background(), real); err != nil || !applied {
				t.Fatalf("real NotReady = applied:%v err:%v — the whole point is that this must win", applied, err)
			}

			b, _ := s.GetBurst("b1")
			if b.NodePhase != protocol.NodePhaseNotReady {
				t.Fatalf("burst phase = %q, want NotReady — the real transition must not be suppressed by the replay", b.NodePhase)
			}
			wl, _ := s.GetWorkload("wl1")
			if wl.NodeObservation == nil || wl.NodeObservation.Phase != protocol.NodePhaseNotReady {
				t.Fatalf("workload observation = %+v, want NotReady", wl.NodeObservation)
			}
		})
	}
}

// The first source-timestamped event from an upgraded connector must take
// precedence over a legacy (receipt-time) record, even if the source timestamp
// is numerically earlier — because it is more trustworthy.
func TestSourceTimestampUpgradesLegacyRecord(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "in-memory"
		if durable {
			name = "durable"
		}
		t.Run(name, func(t *testing.T) {
			s, p := storeWith(durable, nil, nil)
			seedPhaseBurstWithNode(s)
			seedPhaseWorkload(s)
			if p != nil {
				p.phaseRows = map[string]*Burst{"b1": {ID: "b1", CustomerID: "cust_a", Status: BurstStatusProvisioning, NodeName: "ys-burst-b1"}}
			}

			receiptTime := time.Date(2026, 8, 20, 12, 5, 0, 0, time.UTC)

			// Legacy connector (no source timestamp) reports Ready at receipt time.
			legacy := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a",
				Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
				ObservedAt: receiptTime, SourceTimestamped: false,
				ReceiptTime: receiptTime,
			}
			if applied, err := s.UpdateBurstNodePhase(context.Background(), legacy); err != nil || !applied {
				t.Fatalf("legacy = applied:%v err:%v", applied, err)
			}

			// Upgraded connector sends NotReady with K8s source timestamp that is
			// numerically BEFORE the receipt time. Must still win because source
			// timestamps are authoritative over receipt-time fallbacks.
			k8sTime := time.Date(2026, 8, 20, 12, 3, 0, 0, time.UTC)
			upgrade := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a",
				Phase: protocol.NodePhaseNotReady, Reason: "KubeletNotReady",
				ObservedAt: k8sTime, SourceTimestamped: true,
				ReceiptTime: receiptTime.Add(time.Second),
			}
			if applied, err := s.UpdateBurstNodePhase(context.Background(), upgrade); err != nil || !applied {
				t.Fatalf("source-timestamped upgrade = applied:%v err:%v — source must always beat legacy", applied, err)
			}

			b, _ := s.GetBurst("b1")
			if b.NodePhase != protocol.NodePhaseNotReady {
				t.Fatalf("phase = %q, want NotReady — source timestamp must upgrade legacy", b.NodePhase)
			}
			if !b.NodePhaseSourceTimestamped {
				t.Fatal("NodePhaseSourceTimestamped not set after source-timestamped event")
			}
			wl, _ := s.GetWorkload("wl1")
			if wl.NodeObservation == nil || wl.NodeObservation.Phase != protocol.NodePhaseNotReady {
				t.Fatalf("workload observation = %+v, want NotReady", wl.NodeObservation)
			}
		})
	}
}

// A legacy event must never regress a source-timestamped record.
func TestLegacyEventCannotRegressSourceOrderedState(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "in-memory"
		if durable {
			name = "durable"
		}
		t.Run(name, func(t *testing.T) {
			s, p := storeWith(durable, nil, nil)
			seedPhaseBurstWithNode(s)
			seedPhaseWorkload(s)
			if p != nil {
				p.phaseRows = map[string]*Burst{"b1": {ID: "b1", CustomerID: "cust_a", Status: BurstStatusProvisioning, NodeName: "ys-burst-b1"}}
			}

			k8sTime := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

			// Source-timestamped NotReady.
			src := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a",
				Phase: protocol.NodePhaseNotReady, Reason: "KubeletNotReady",
				ObservedAt: k8sTime, SourceTimestamped: true,
				ReceiptTime: k8sTime.Add(time.Second),
			}
			if applied, err := s.UpdateBurstNodePhase(context.Background(), src); err != nil || !applied {
				t.Fatalf("source = applied:%v err:%v", applied, err)
			}

			// Legacy event with a LATER receipt time tries to report Ready.
			laterReceipt := k8sTime.Add(10 * time.Minute)
			legacyReady := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a",
				Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
				ObservedAt: laterReceipt, SourceTimestamped: false,
				ReceiptTime: laterReceipt,
			}
			applied, err := s.UpdateBurstNodePhase(context.Background(), legacyReady)
			if err != nil {
				t.Fatalf("legacy err = %v", err)
			}
			if applied {
				t.Fatal("legacy event was applied over source-timestamped state — it must never regress it")
			}

			b, _ := s.GetBurst("b1")
			if b.NodePhase != protocol.NodePhaseNotReady {
				t.Fatalf("phase = %q, want NotReady — legacy must not regress source", b.NodePhase)
			}
			wl, _ := s.GetWorkload("wl1")
			if wl.NodeObservation == nil || wl.NodeObservation.Phase != protocol.NodePhaseNotReady {
				t.Fatalf("workload observation = %+v, want NotReady", wl.NodeObservation)
			}
		})
	}
}

// OccupancyObservedAt uses receipt time, not the source timestamp. The client
// strips OccupancyObserved from reconnect-cached events, so its arrival is
// always fresh and receipt time is the correct clock.
func TestOccupancyUsesReceiptTimeNotSourceTimestamp(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)

	k8sTime := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	receipt := k8sTime.Add(5 * time.Minute)

	u := BurstNodePhaseUpdate{
		BurstID: "b1", CustomerID: "cust_a",
		Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
		ObservedAt: k8sTime, SourceTimestamped: true,
		ReceiptTime: receipt, OccupancyObserved: true,
	}
	if applied, err := s.UpdateBurstNodePhase(context.Background(), u); err != nil || !applied {
		t.Fatalf("update = applied:%v err:%v", applied, err)
	}

	b, _ := s.GetBurst("b1")
	if b.NodePhaseAt == nil || !b.NodePhaseAt.Equal(k8sTime) {
		t.Fatalf("NodePhaseAt = %v, want source timestamp %v", b.NodePhaseAt, k8sTime)
	}
	if b.OccupancyObservedAt == nil || !b.OccupancyObservedAt.Equal(receipt) {
		t.Fatalf("OccupancyObservedAt = %v, want receipt time %v", b.OccupancyObservedAt, receipt)
	}
}

// An occupancy-only refresh against a burst whose phase already equals the
// event must NOT synthesize a workload NodeObservation that was never durably
// stamped. Before the fix, the Store caller inferred phaseApplied by comparing
// the returned burst's phase/time with the update's values. An occupancy-only
// update whose phase happened to match tripped the inference and stamped a
// non-durable observation into local memory.
func TestOccupancyOnlyRefreshCannotSynthesizeWorkloadObservation(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "in-memory"
		if durable {
			name = "durable"
		}
		t.Run(name, func(t *testing.T) {
			s, p := storeWith(durable, nil, nil)
			seedPhaseBurstWithNode(s)
			seedPhaseWorkload(s)
			if p != nil {
				p.phaseRows = map[string]*Burst{"b1": {
					ID: "b1", CustomerID: "cust_a",
					Status: BurstStatusProvisioning, NodeName: "ys-burst-b1",
				}}
			}

			at := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

			// First: apply Ready so the burst has a phase and time.
			ready := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a",
				Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
				ObservedAt: at, ReceiptTime: at,
			}
			if applied, err := s.UpdateBurstNodePhase(context.Background(), ready); err != nil || !applied {
				t.Fatalf("ready = applied:%v err:%v", applied, err)
			}
			wl, _ := s.GetWorkload("wl1")
			if wl.NodeObservation == nil {
				t.Fatal("precondition: workload must have an observation after a phase update")
			}

			// Reset the workload observation so we can detect a false stamp.
			s.mu.Lock()
			wcp := *s.workloads["wl1"]
			wcp.NodeObservation = nil
			s.workloads["wl1"] = &wcp
			s.mu.Unlock()

			// Now send the SAME phase + time but with OccupancyObserved=true and
			// a newer receipt time. The phase is not fresh (same timestamp), but
			// occupancy is. The workload must NOT be stamped.
			occupancy := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a",
				Phase: protocol.NodePhaseReady, Reason: "should not appear",
				ObservedAt: at, ReceiptTime: at.Add(time.Second),
				OccupancyObserved: true,
			}
			applied, err := s.UpdateBurstNodePhase(context.Background(), occupancy)
			if err != nil {
				t.Fatalf("occupancy err = %v", err)
			}
			if !applied {
				t.Fatal("occupancy update was not applied")
			}

			b, _ := s.GetBurst("b1")
			if b.OccupancyObservedAt == nil {
				t.Fatal("occupancy stamp missing")
			}

			wl, _ = s.GetWorkload("wl1")
			if wl.NodeObservation != nil {
				t.Fatalf("occupancy-only refresh synthesized a workload observation: %+v", wl.NodeObservation)
			}
		})
	}
}

func TestUpdateBurstNodePhaseStampsGPUObservation(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	gpuAt := time.Now().UTC()
	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.ClusterID = "cluster-a"
	u.GPUAllocatable = true
	u.GPUAllocatableAt = &gpuAt
	applied, err := s.UpdateBurstNodePhase(context.Background(), u)
	if err != nil || !applied {
		t.Fatalf("update = applied:%v err:%v", applied, err)
	}
	wl, _ := s.GetWorkload("wl1")
	if wl.GPUObservation == nil {
		t.Fatal("workload has no GPUObservation after GPU-bearing node event")
	}
	if !wl.GPUObservation.AllocatableAt.Equal(gpuAt) {
		t.Fatalf("AllocatableAt = %v, want %v", wl.GPUObservation.AllocatableAt, gpuAt)
	}
}

func TestGPUObservationWrittenOnceNeverOverwritten(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	first := time.Now().Add(-time.Hour).UTC()
	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.ClusterID = "cluster-a"
	u.GPUAllocatable = true
	u.GPUAllocatableAt = &first
	s.UpdateBurstNodePhase(context.Background(), u)

	second := time.Now().UTC()
	u2 := nodePhaseUpdate(protocol.NodePhaseReady)
	u2.ClusterID = "cluster-a"
	u2.GPUAllocatable = true
	u2.GPUAllocatableAt = &second
	u2.ObservedAt = second
	s.UpdateBurstNodePhase(context.Background(), u2)

	wl, _ := s.GetWorkload("wl1")
	if !wl.GPUObservation.AllocatableAt.Equal(first) {
		t.Fatalf("GPUObservation was overwritten: got %v, want original %v",
			wl.GPUObservation.AllocatableAt, first)
	}
}

func TestCPUOnlyBurstNeverGetsGPUObservation(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	seedPhaseWorkload(s)

	u := nodePhaseUpdate(protocol.NodePhaseReady)
	s.UpdateBurstNodePhase(context.Background(), u)

	wl, _ := s.GetWorkload("wl1")
	if wl.GPUObservation != nil {
		t.Fatal("CPU-only burst produced a GPUObservation")
	}
}

func TestStampWorkloadPodObservation(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	b.NodeName = "burst-node"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	ok, err := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "train-pod", "burst-node", at)
	if err != nil || !ok {
		t.Fatalf("StampWorkloadPodObservation = ok:%v err:%v", ok, err)
	}

	wl, _ := s.GetWorkload("wl1")
	if wl.PodObservation == nil {
		t.Fatal("PodObservation = nil after stamping")
	}
	if wl.PodObservation.PodName != "train-pod" {
		t.Errorf("PodName = %q, want %q", wl.PodObservation.PodName, "train-pod")
	}
	if wl.PodObservation.NodeName != "burst-node" {
		t.Errorf("NodeName = %q, want %q", wl.PodObservation.NodeName, "burst-node")
	}
	if !wl.PodObservation.ScheduledAt.Equal(at) {
		t.Errorf("ScheduledAt = %v, want %v", wl.PodObservation.ScheduledAt, at)
	}
}

func TestStampWorkloadPodObservationWriteOnce(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	b.NodeName = "node-1"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	first := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "pod-1", "node-1", first)

	second := time.Date(2026, 8, 21, 11, 0, 0, 0, time.UTC)
	s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "pod-2", "node-2", second)

	wl, _ := s.GetWorkload("wl1")
	if wl.PodObservation.PodName != "pod-1" {
		t.Fatalf("PodObservation overwritten: PodName = %q, want original %q",
			wl.PodObservation.PodName, "pod-1")
	}
}

func TestStampWorkloadPodObservationTenantIsolation(t *testing.T) {
	s := emptyStore()
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	ok, _ := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_other", "cluster-a", "pod", "node",
		time.Now().UTC())
	if ok {
		t.Fatal("pod observation stamped for wrong tenant")
	}

	wl, _ := s.GetWorkload("wl1")
	if wl.PodObservation != nil {
		t.Fatal("wrong-tenant stamp created a PodObservation")
	}
}

func TestStampWorkloadPodObservationUnknownWorkload(t *testing.T) {
	s := emptyStore()
	ok, _ := s.StampWorkloadPodObservation(context.Background(), "wl_nonexistent", "cust_a", "cluster-a", "pod", "node",
		time.Now().UTC())
	if ok {
		t.Fatal("pod observation stamped for unknown workload")
	}
}

func TestPodObservationSurvivesPutWorkload(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	b.NodeName = "burst-node"
	wk := seedPhaseWorkload(s)
	wk.ClusterID = "cluster-a"

	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "train-pod", "burst-node", at)

	w, _ := s.GetWorkload("wl1")
	w.Status = "running"
	w.PodObservation = nil
	s.PutWorkload(w)

	wl, _ := s.GetWorkload("wl1")
	if wl.PodObservation == nil {
		t.Fatal("PodObservation lost during PutWorkload")
	}
}

func TestGPUObservationSurvivesPutWorkload(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	wk := seedPhaseWorkload(s)
	wk.ClusterID = "cluster-a"

	gpuAt := time.Now().UTC()
	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.ClusterID = "cluster-a"
	u.GPUAllocatable = true
	u.GPUAllocatableAt = &gpuAt
	s.UpdateBurstNodePhase(context.Background(), u)

	w, _ := s.GetWorkload("wl1")
	w.Status = "running"
	w.GPUObservation = nil
	s.PutWorkload(w)

	wl, _ := s.GetWorkload("wl1")
	if wl.GPUObservation == nil {
		t.Fatal("GPUObservation lost during PutWorkload")
	}
}

func TestWholeWorkloadWritesPreserveWriteOnceObservations(t *testing.T) {
	putters := []struct {
		name string
		put  func(*Store, *Workload) error
	}{
		{
			name: "PutWorkload",
			put: func(s *Store, w *Workload) error {
				s.PutWorkload(w)
				return nil
			},
		},
		{
			name: "PutWorkloadDurable",
			put: func(s *Store, w *Workload) error {
				return s.PutWorkloadDurable(w)
			},
		},
	}
	storedAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	incomingAt := storedAt.Add(time.Hour)

	for _, tc := range putters {
		t.Run(tc.name, func(t *testing.T) {
			s := emptyStore()
			s.PutWorkload(&Workload{
				ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "running",
				PodObservation: &PodObservation{
					PodName: "first-pod", NodeName: "first-node", ScheduledAt: storedAt,
				},
				GPUObservation: &GPUObservation{AllocatableAt: storedAt},
			})

			err := tc.put(s, &Workload{
				ID: "wl1", CustomerID: "cust_a", BurstID: "b1", Status: "succeeded",
				PodObservation: &PodObservation{
					PodName: "replacement-pod", NodeName: "replacement-node", ScheduledAt: incomingAt,
				},
				GPUObservation: &GPUObservation{AllocatableAt: incomingAt},
			})
			if err != nil {
				t.Fatalf("whole-workload write: %v", err)
			}

			wl, err := s.GetWorkload("wl1")
			if err != nil {
				t.Fatalf("GetWorkload: %v", err)
			}
			if wl.PodObservation == nil || wl.PodObservation.PodName != "first-pod" ||
				wl.PodObservation.NodeName != "first-node" || !wl.PodObservation.ScheduledAt.Equal(storedAt) {
				t.Fatalf("write-once PodObservation replaced: %+v", wl.PodObservation)
			}
			if wl.GPUObservation == nil || !wl.GPUObservation.AllocatableAt.Equal(storedAt) {
				t.Fatalf("write-once GPUObservation replaced: %+v", wl.GPUObservation)
			}
			if wl.Status != "succeeded" {
				t.Fatalf("status = %q, want incoming lifecycle update", wl.Status)
			}
		})
	}
}

func TestWorkloadUpsertStatementPreservesConnectorObservations(t *testing.T) {
	stmt := upsertWorkloadStmt(tblWorkloads)
	for _, want := range []string{
		`COALESCE(` + tblWorkloads + `.data->'PodObservation', 'null'::jsonb) <> 'null'::jsonb`,
		`jsonb_build_object('PodObservation', ` + tblWorkloads + `.data->'PodObservation')`,
		`COALESCE(` + tblWorkloads + `.data->'GPUObservation', 'null'::jsonb) <> 'null'::jsonb`,
		`jsonb_build_object('GPUObservation', ` + tblWorkloads + `.data->'GPUObservation')`,
		`COALESCE(` + tblWorkloads + `.data->'SchedulingObservation', 'null'::jsonb) <> 'null'::jsonb`,
		`data->'SchedulingObservation'->>'State' = 'Scheduled'`,
		`EXCLUDED.data->'SchedulingObservation'->>'State' = 'Scheduled'`,
		`EXCLUDED.data->'SchedulingObservation'->>'ObservedAt'`,
		`EXCLUDED.data->'SchedulingObservation'->>'PodName'`,
		`'SchedulingObservation', ` + tblWorkloads + `.data->'SchedulingObservation'`,
	} {
		if !strings.Contains(stmt, want) {
			t.Fatalf("workload upsert does not preserve connector observation rule %q: %s", want, stmt)
		}
	}
}

func TestSchedulingObservationFreshRules(t *testing.T) {
	base := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	storedWaiting := &SchedulingObservation{State: "Waiting", ObservedAt: base, PodName: "pod-1"}
	storedScheduled := &SchedulingObservation{State: "Scheduled", ObservedAt: base, PodName: "pod-1"}

	for _, tc := range []struct {
		name     string
		stored   *SchedulingObservation
		incoming *SchedulingObservation
		fresh    bool
	}{
		{name: "first observation", incoming: storedWaiting, fresh: true},
		{name: "missing incoming", stored: storedWaiting},
		{name: "scheduled is terminal", stored: storedScheduled, incoming: &SchedulingObservation{State: "Waiting", ObservedAt: base.Add(time.Hour), PodName: "pod-1"}},
		{name: "incoming scheduled wins regardless of timestamp", stored: storedWaiting, incoming: &SchedulingObservation{State: "Scheduled", ObservedAt: base.Add(-time.Hour), PodName: "pod-1"}, fresh: true},
		{name: "newer waiting wins", stored: storedWaiting, incoming: &SchedulingObservation{State: "Waiting", ObservedAt: base.Add(time.Second), PodName: "pod-1"}, fresh: true},
		{name: "older waiting loses", stored: storedWaiting, incoming: &SchedulingObservation{State: "Waiting", ObservedAt: base.Add(-time.Second), PodName: "pod-1"}},
		{name: "same pod at equal timestamp loses", stored: storedWaiting, incoming: &SchedulingObservation{State: "Waiting", ObservedAt: base, PodName: "pod-1"}},
		{name: "replacement pod at equal timestamp wins", stored: storedWaiting, incoming: &SchedulingObservation{State: "Waiting", ObservedAt: base, PodName: "pod-2"}, fresh: true},
		{name: "empty replacement pod does not win", stored: storedWaiting, incoming: &SchedulingObservation{State: "Waiting", ObservedAt: base}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := schedulingObservationFresh(tc.stored, tc.incoming); got != tc.fresh {
				t.Fatalf("schedulingObservationFresh() = %v, want %v", got, tc.fresh)
			}
		})
	}
}

func TestGPUObservationClusterIsolation(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	gpuAt := time.Now().UTC()
	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.ClusterID = "cluster-b"
	u.GPUAllocatable = true
	u.GPUAllocatableAt = &gpuAt
	s.UpdateBurstNodePhase(context.Background(), u)

	wl, _ := s.GetWorkload("wl1")
	if wl.GPUObservation != nil {
		t.Fatal("GPU observation stamped by wrong cluster's connector")
	}
}

func TestPodObservationClusterIsolation(t *testing.T) {
	s := emptyStore()
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	ok, _ := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-b", "pod", "node",
		time.Now().UTC())
	if ok {
		t.Fatal("pod observation stamped by wrong cluster's connector")
	}
	wl, _ := s.GetWorkload("wl1")
	if wl.PodObservation != nil {
		t.Fatal("PodObservation created by wrong cluster")
	}
}

func TestNodePhaseClusterIsolation(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurst(s)
	b.ClusterID = "cluster-a"

	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.ClusterID = "cluster-b"
	applied, err := s.UpdateBurstNodePhase(context.Background(), u)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if applied {
		t.Fatal("node phase applied by wrong cluster's connector")
	}
}

func TestGPUObservationWriteOnce(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	first := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.ClusterID = "cluster-a"
	u.GPUAllocatable = true
	u.GPUAllocatableAt = &first
	s.UpdateBurstNodePhase(context.Background(), u)

	second := time.Date(2026, 8, 21, 11, 0, 0, 0, time.UTC)
	u2 := nodePhaseUpdate(protocol.NodePhaseReady)
	u2.ClusterID = "cluster-a"
	u2.GPUAllocatable = true
	u2.GPUAllocatableAt = &second
	u2.ObservedAt = second
	s.UpdateBurstNodePhase(context.Background(), u2)

	wl, _ := s.GetWorkload("wl1")
	if wl.GPUObservation == nil {
		t.Fatal("GPUObservation nil")
	}
	if !wl.GPUObservation.AllocatableAt.Equal(first) {
		t.Fatalf("GPUObservation overwritten: AllocatableAt = %v, want original %v",
			wl.GPUObservation.AllocatableAt, first)
	}
}

// podObsFailPersister wraps burstFailPersister and overrides stampWorkloadPodObservation
// and getWorkloadPodObservation to inject failures.
type podObsFailPersister struct {
	burstFailPersister
	err     error
	rows    int64
	getObs  *PodObservation
	getOK   bool
	getErr  error
	getSeen int
}

func (p *podObsFailPersister) stampWorkloadPodObservation(context.Context, string, string, string, string, string, time.Time) (int64, error) {
	return p.rows, p.err
}
func (p *podObsFailPersister) getWorkloadPodObservation(context.Context, string, string, string, string) (*PodObservation, bool, error) {
	p.getSeen++
	return p.getObs, p.getOK, p.getErr
}

// --- Fix 2: Pod observation fail-closed ---

func TestStampWorkloadPodObservationFailClosed(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	b.NodeName = "node"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"
	failure := errors.New("postgres is down")
	s.persist = &podObsFailPersister{err: failure}

	ok, err := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "pod", "node", time.Now().UTC())
	if !errors.Is(err, failure) {
		t.Fatalf("err = %v, want the durable failure", err)
	}
	if ok {
		t.Fatal("persist failure reported success")
	}
	wl, _ := s.GetWorkload("wl1")
	if wl.PodObservation != nil {
		t.Fatal("memory mutated despite persist failure — must be absent so retry can succeed")
	}
}

func TestStampWorkloadPodObservationDurableRefusal(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	b.NodeName = "node"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"
	s.persist = &podObsFailPersister{rows: 0}

	ok, err := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "pod", "node", time.Now().UTC())
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if ok {
		t.Fatal("durable refusal (0 rows) reported success")
	}
	wl, _ := s.GetWorkload("wl1")
	if wl.PodObservation != nil {
		t.Fatal("memory mutated despite durable refusal")
	}
}

// --- Fix 3: GPU observation independent of node phase freshness ---

func TestGPUObservationAcceptedWhenPhaseFreshnessIsFalse(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "in-memory"
		if durable {
			name = "durable"
		}
		t.Run(name, func(t *testing.T) {
			s, p := storeWith(durable, nil, nil)
			b := seedPhaseBurstWithNode(s)
			b.ClusterID = "cluster-a"
			w := seedPhaseWorkload(s)
			w.ClusterID = "cluster-a"
			if p != nil {
				p.phaseRows = map[string]*Burst{"b1": {
					ID: "b1", CustomerID: "cust_a", Status: BurstStatusProvisioning,
					NodeName: "ys-burst-b1", ClusterID: "cluster-a",
				}}
			}

			at := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
			ready := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a", ClusterID: "cluster-a",
				Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
				ObservedAt: at, ReceiptTime: at,
			}
			if applied, err := s.UpdateBurstNodePhase(context.Background(), ready); err != nil || !applied {
				t.Fatalf("ready = applied:%v err:%v", applied, err)
			}

			gpuAt := at.Add(time.Second)
			gpuUpdate := BurstNodePhaseUpdate{
				BurstID: "b1", CustomerID: "cust_a", ClusterID: "cluster-a",
				Phase: protocol.NodePhaseReady, Reason: "same-phase",
				ObservedAt:     at,
				GPUAllocatable: true, GPUAllocatableAt: &gpuAt,
			}
			applied, err := s.UpdateBurstNodePhase(context.Background(), gpuUpdate)
			if err != nil {
				t.Fatalf("gpu update err = %v", err)
			}
			if !applied {
				t.Fatal("GPU observation was not applied when phase freshness was false — GPU must be independent")
			}

			wl, _ := s.GetWorkload("wl1")
			if wl.GPUObservation == nil {
				t.Fatal("workload has no GPUObservation")
			}
			if !wl.GPUObservation.AllocatableAt.Equal(gpuAt) {
				t.Fatalf("AllocatableAt = %v, want %v", wl.GPUObservation.AllocatableAt, gpuAt)
			}
		})
	}
}

// --- Fix 4: Exact non-empty cluster identity for GPU/Pod observations ---

func TestGPUObservationRejectsEmptyClusterID(t *testing.T) {
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	gpuAt := time.Now().UTC()
	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.GPUAllocatable = true
	u.GPUAllocatableAt = &gpuAt
	s.UpdateBurstNodePhase(context.Background(), u)

	wl, _ := s.GetWorkload("wl1")
	if wl.GPUObservation != nil {
		t.Fatal("GPU observation stamped with empty cluster ID")
	}
}

func TestPodObservationRejectsEmptyClusterID(t *testing.T) {
	s := emptyStore()
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	ok, _ := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "", "pod", "node", time.Now().UTC())
	if ok {
		t.Fatal("pod observation stamped with empty agent cluster ID")
	}
}

func TestPodObservationRejectsEmptyWorkloadClusterID(t *testing.T) {
	s := emptyStore()
	seedPhaseWorkload(s)

	ok, _ := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "pod", "node", time.Now().UTC())
	if ok {
		t.Fatal("pod observation stamped with empty workload cluster ID")
	}
}

func TestGPUObservationRejectsWrongCluster(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	gpuAt := time.Now().UTC()
	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.ClusterID = "cluster-wrong"
	u.GPUAllocatable = true
	u.GPUAllocatableAt = &gpuAt
	s.UpdateBurstNodePhase(context.Background(), u)

	wl, _ := s.GetWorkload("wl1")
	if wl.GPUObservation != nil {
		t.Fatal("GPU observation stamped by wrong cluster")
	}
}

func TestPodObservationRejectsWrongCluster(t *testing.T) {
	s := emptyStore()
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	ok, _ := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-wrong", "pod", "node", time.Now().UTC())
	if ok {
		t.Fatal("pod observation stamped by wrong cluster")
	}
}

// --- Correction A: exact burst identity ---

func TestGPUObservationRejectsLegacyBurstEmptyClusterID(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "" // legacy burst
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	gpuAt := time.Now().UTC()
	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.ClusterID = "cluster-a"
	u.GPUAllocatable = true
	u.GPUAllocatableAt = &gpuAt
	s.UpdateBurstNodePhase(context.Background(), u)

	wl, _ := s.GetWorkload("wl1")
	if wl.GPUObservation != nil {
		t.Fatal("GPU observation stamped through a legacy burst with empty ClusterID")
	}
}

func TestGPUObservationRejectsMismatchedBurstClusterID(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-b"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	gpuAt := time.Now().UTC()
	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.ClusterID = "cluster-a"
	u.GPUAllocatable = true
	u.GPUAllocatableAt = &gpuAt
	s.UpdateBurstNodePhase(context.Background(), u)

	wl, _ := s.GetWorkload("wl1")
	if wl.GPUObservation != nil {
		t.Fatal("GPU observation stamped when burst ClusterID mismatches agent ClusterID")
	}
}

func TestGPUObservationDurableRejectsLegacyBurstClusterID(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = ""
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"
	s.persist = &burstFailPersister{phaseRows: map[string]*Burst{
		"b1": {ID: "b1", CustomerID: "cust_a", Status: BurstStatusProvisioning,
			NodeName: "ys-burst-b1", ClusterID: ""},
	}}

	gpuAt := time.Now().UTC()
	u := nodePhaseUpdate(protocol.NodePhaseReady)
	u.ClusterID = "cluster-a"
	u.GPUAllocatable = true
	u.GPUAllocatableAt = &gpuAt
	s.UpdateBurstNodePhase(context.Background(), u)

	wl, _ := s.GetWorkload("wl1")
	if wl.GPUObservation != nil {
		t.Fatal("durable path: GPU observation stamped through legacy burst with empty ClusterID")
	}
}

func TestPodObservationRejectsNoBurst(t *testing.T) {
	s := emptyStore()
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"
	w.BurstID = "b_nonexistent"

	ok, _ := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "pod", "node", time.Now().UTC())
	if ok {
		t.Fatal("pod observation stamped when workload's burst does not exist")
	}
}

func TestPodObservationRejectsBurstEmptyClusterID(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = ""
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	ok, _ := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "pod", "ys-burst-b1", time.Now().UTC())
	if ok {
		t.Fatal("pod observation stamped when burst has empty ClusterID")
	}
}

func TestPodObservationRejectsBurstWrongClusterID(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-b"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	ok, _ := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "pod", "ys-burst-b1", time.Now().UTC())
	if ok {
		t.Fatal("pod observation stamped when burst has mismatched ClusterID")
	}
}

func TestPodObservationRejectsWrongNodeName(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	ok, _ := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "pod", "wrong-node", time.Now().UTC())
	if ok {
		t.Fatal("pod observation stamped when nodeName does not match burst NodeName")
	}
}

func TestPodObservationAcceptsExactBurstIdentity(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	ok, err := s.StampWorkloadPodObservation(context.Background(), "wl1", "cust_a", "cluster-a", "pod", "ys-burst-b1", time.Now().UTC())
	if err != nil || !ok {
		t.Fatalf("pod observation with exact burst identity failed: ok=%v err=%v", ok, err)
	}
}

// --- Outcome enum + cross-replica retirement ---

func TestPodObservationOutcomeAppliedIn(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	outcome, obs, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl1", "cust_a", "cluster-a", "pod-1", "ys-burst-b1", at)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if outcome != PodObservationApplied {
		t.Fatalf("outcome = %v, want Applied", outcome)
	}
	if obs == nil || obs.PodName != "pod-1" || obs.NodeName != "ys-burst-b1" || !obs.ScheduledAt.Equal(at) {
		t.Fatalf("echoed observation = %+v, want the exact identity persisted", obs)
	}
	if !outcome.Acknowledgeable() {
		t.Fatal("Applied must be Acknowledgeable")
	}
}

func TestPodObservationOutcomeResolvedOnSecondAttempt(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	if _, _, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl1", "cust_a", "cluster-a", "pod-1", "ys-burst-b1", at); err != nil {
		t.Fatalf("first stamp: %v", err)
	}
	outcome, obs, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl1", "cust_a", "cluster-a", "pod-1", "ys-burst-b1", at)
	if err != nil {
		t.Fatalf("second stamp: %v", err)
	}
	if outcome != PodObservationResolved {
		t.Fatalf("second outcome = %v, want Resolved", outcome)
	}
	if obs == nil || obs.PodName != "pod-1" {
		t.Fatalf("Resolved observation = %+v, want the durable identity", obs)
	}
	if !outcome.Acknowledgeable() {
		t.Fatal("Resolved must be Acknowledgeable")
	}
}

func TestPodObservationOutcomeRejectedNotAcknowledgeable(t *testing.T) {
	if PodObservationRejected.Acknowledgeable() {
		t.Fatal("Rejected must NOT be Acknowledgeable — the connector's retry must stand")
	}
}

// TestPodObservationDurableRefusalReturnsRejected covers the persistent-store
// case where the SQL UPDATE affects 0 rows AND the durable re-read shows the
// workload row is gone. The stale in-memory entry (workload still present)
// MUST NOT return success — the durable store is the authority.
func TestPodObservationDurableRefusalReturnsRejected(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"
	// Persister rejects both the write and the durable re-read (0 rows, no such
	// workload). Simulates a workload retired between the replica's in-memory
	// snapshot and this call, or a persister-authoritative wrong-owner.
	s.persist = &podObsFailPersister{rows: 0, getObs: nil, getOK: false}

	outcome, obs, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl1", "cust_a", "cluster-a", "pod-1", "ys-burst-b1", time.Now().UTC())
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if outcome != PodObservationRejected {
		t.Fatalf("outcome = %v, want Rejected — durable re-read said the workload is not there", outcome)
	}
	if obs != nil {
		t.Fatalf("obs = %+v, want nil for Rejected", obs)
	}
	// Neither should this fabricate an in-memory PodObservation from stale state.
	wl, _ := s.GetWorkload("wl1")
	if wl.PodObservation != nil {
		t.Fatal("Rejected outcome must not mutate memory into a phantom observation")
	}
}

// TestPodObservationDurableResolvedRepopulatesMemory covers the cross-replica
// retirement replay case: replica A applied the observation and its durable
// row survives, but replica B never had it in memory. A retry lands on B; its
// UPDATE affects 0 rows (already stamped by A), and the durable re-read
// returns the observation. The outcome must be Resolved and echo the durable
// identity so the connector's cache-key match is against what central holds.
func TestPodObservationDurableResolvedRepopulatesMemory(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"
	durableAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	durable := &PodObservation{PodName: "pod-1", NodeName: "ys-burst-b1", ScheduledAt: durableAt}
	s.persist = &podObsFailPersister{rows: 0, getObs: durable, getOK: true}

	outcome, obs, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl1", "cust_a", "cluster-a", "pod-1", "ys-burst-b1", durableAt)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if outcome != PodObservationResolved {
		t.Fatalf("outcome = %v, want Resolved from the durable re-read", outcome)
	}
	if obs == nil || obs.PodName != "pod-1" || !obs.ScheduledAt.Equal(durableAt) {
		t.Fatalf("echoed observation = %+v, want the durable identity", obs)
	}
	// The in-memory row of this replica is now backfilled with the durable
	// truth, so a subsequent memory read returns the same identity that any
	// other replica would see.
	wl, _ := s.GetWorkload("wl1")
	if wl.PodObservation == nil || wl.PodObservation.PodName != "pod-1" {
		t.Fatalf("memory not backfilled from durable read: %+v", wl.PodObservation)
	}
}

// In-memory-only replay must not return Acknowledgeable from a stale memory
// hit that has no durable proof at all — but for the in-memory backend, memory
// IS the durable state, so an existing observation is legitimately Resolved.
// This test asserts that once an observation is applied, the second call is
// Resolved (and the ACK will echo the FIRST identity).
func TestPodObservationInMemoryResolvedWriteOnce(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"

	first := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	if _, _, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl1", "cust_a", "cluster-a", "pod-1", "ys-burst-b1", first); err != nil {
		t.Fatalf("first stamp: %v", err)
	}
	// Second call with a DIFFERENT identity must not overwrite (write-once) and
	// must echo the FIRST observation so the ACK matches what is durably held.
	second := time.Date(2026, 8, 21, 11, 0, 0, 0, time.UTC)
	outcome, obs, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl1", "cust_a", "cluster-a", "pod-different", "ys-burst-b1", second)
	if err != nil {
		t.Fatalf("second stamp: %v", err)
	}
	if outcome != PodObservationResolved {
		t.Fatalf("outcome = %v, want Resolved (write-once existing observation)", outcome)
	}
	if obs == nil || obs.PodName != "pod-1" || !obs.ScheduledAt.Equal(first) {
		t.Fatalf("echoed observation = %+v, want the ORIGINAL identity (write-once)", obs)
	}
}

// TestPodObservationRejectedForRetiredMemoryEntry covers the exact stale-in-
// memory retirement scenario: the workload has been retired but the durable
// re-read is authoritative. If the persister's re-read says "no such row"
// (existence check bundled into the persister return), Rejected wins. This
// pairs with TestPodObservationDurableRefusalReturnsRejected — the emphasis
// here is on there being no ACK, and no phantom in-memory PodObservation.
func TestPodObservationInMemoryRejectedForUnknownWorkload(t *testing.T) {
	s := emptyStore()
	outcome, obs, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl_nonexistent", "cust_a", "cluster-a", "pod-1", "node-1", time.Now().UTC())
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if outcome != PodObservationRejected {
		t.Fatalf("outcome = %v, want Rejected", outcome)
	}
	if obs != nil {
		t.Fatalf("obs = %+v, want nil", obs)
	}
}

func TestPodObservationPersistFailureIsError(t *testing.T) {
	s := emptyStore()
	b := seedPhaseBurstWithNode(s)
	b.ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"
	failure := errors.New("postgres is down")
	s.persist = &podObsFailPersister{err: failure}

	outcome, _, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl1", "cust_a", "cluster-a", "pod-1", "ys-burst-b1", time.Now().UTC())
	if !errors.Is(err, failure) {
		t.Fatalf("err = %v, want the durable failure", err)
	}
	if outcome != PodObservationRejected {
		t.Fatalf("outcome = %v, want Rejected on error", outcome)
	}
}

func TestPostgresPodObservationRequiresBurstIdentity(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_pod", Token: "tok_pod", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_pod", CustomerID: "cust_pod", Backend: "linode", BackendID: "pod1",
		NodeName: "ys-burst-pod", ClusterID: "cluster-a",
		Status: BurstStatusProvisioning, CreatedAt: now,
	})
	s.PutWorkload(&Workload{
		ID: "wl_pod", CustomerID: "cust_pod", BurstID: "burst_pod",
		ClusterID: "cluster-a", Status: "provisioning", CreatedAt: now,
	})

	ok, err := s.StampWorkloadPodObservation(context.Background(),
		"wl_pod", "cust_pod", "cluster-a", "train-pod", "wrong-node", now)
	if err != nil {
		t.Fatalf("wrong node err = %v", err)
	}
	if ok {
		t.Fatal("pod observation stamped with wrong nodeName against PostgreSQL")
	}

	ok, err = s.StampWorkloadPodObservation(context.Background(),
		"wl_pod", "cust_pod", "cluster-a", "train-pod", "ys-burst-pod", now)
	if err != nil || !ok {
		t.Fatalf("exact match = ok:%v err:%v, want success", ok, err)
	}
	wl, _ := s.GetWorkload("wl_pod")
	if wl.PodObservation == nil || wl.PodObservation.NodeName != "ys-burst-pod" {
		t.Fatalf("PodObservation = %+v, want ys-burst-pod", wl.PodObservation)
	}
}

// TestPostgresPodObservationCrossReplicaReplay covers the durability defect
// the outcome enum was introduced to close. Replica A stamps and its
// in-memory row is warm. Replica B never had it in memory. A retry lands on
// B; its UPDATE affects 0 rows because the row is already stamped, but the
// durable re-read returns the observation with the SAME identity. B returns
// Resolved and the ACK carries the exact durable identity.
func TestPostgresPodObservationCrossReplicaReplay(t *testing.T) {
	sA := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	sA.AddCustomer(&Customer{ID: "cust_repl", Token: "tok_repl", Plan: "pro"})
	sA.PutBurst(&Burst{
		ID: "burst_repl", CustomerID: "cust_repl", Backend: "linode", BackendID: "repl1",
		NodeName: "ys-burst-repl", ClusterID: "cluster-a",
		Status: BurstStatusProvisioning, CreatedAt: now,
	})
	sA.PutWorkload(&Workload{
		ID: "wl_repl", CustomerID: "cust_repl", BurstID: "burst_repl",
		ClusterID: "cluster-a", Status: "provisioning", CreatedAt: now,
	})

	scheduledAt := now.Add(time.Minute)
	outcomeA, obsA, err := sA.StampWorkloadPodObservationOutcome(context.Background(),
		"wl_repl", "cust_repl", "cluster-a", "train-pod-A", "ys-burst-repl", scheduledAt)
	if err != nil || outcomeA != PodObservationApplied {
		t.Fatalf("replica A stamp = %v (%+v) err=%v, want Applied", outcomeA, obsA, err)
	}

	// Replica B: fresh Store from the same DSN — did not receive the write in
	// memory, only through the durable store.
	sB, err := NewPostgres(context.Background(), os.Getenv("YSCALE_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("replica B connect: %v", err)
	}
	defer sB.Close()

	outcomeB, obsB, err := sB.StampWorkloadPodObservationOutcome(context.Background(),
		"wl_repl", "cust_repl", "cluster-a", "train-pod-A", "ys-burst-repl", scheduledAt)
	if err != nil {
		t.Fatalf("replica B stamp err = %v", err)
	}
	if outcomeB != PodObservationResolved {
		t.Fatalf("replica B outcome = %v, want Resolved from the durable re-read", outcomeB)
	}
	if obsB == nil || obsB.PodName != "train-pod-A" || !obsB.ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("replica B echoed observation = %+v, want the durable identity", obsB)
	}
	if !outcomeB.Acknowledgeable() {
		t.Fatal("Resolved must be Acknowledgeable")
	}
}

// TestPostgresPodObservationRetiredRowReturnsRejected covers the stale-in-
// memory phantom this outcome enum forbids. The workload's PodObservation
// was stamped, then the BURST row is retired durably — the workload row AND
// its PodObservation are left intact on purpose, because that is the shape
// that used to fool a workload-only re-read into returning Resolved. A retry
// lands on a replica whose memory still has the observation. The persister's
// UPDATE hits 0 rows (write-once refuses the second stamp), and the durable
// re-read joins against the (now-gone) burst identity and reports no live
// match — which MUST become Rejected. No ACK.
func TestPostgresPodObservationRetiredRowReturnsRejected(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_ret2", Token: "tok_ret2", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_ret2", CustomerID: "cust_ret2", Backend: "linode", BackendID: "ret2",
		NodeName: "ys-burst-ret2", ClusterID: "cluster-a",
		Status: BurstStatusProvisioning, CreatedAt: now,
	})
	s.PutWorkload(&Workload{
		ID: "wl_ret2", CustomerID: "cust_ret2", BurstID: "burst_ret2",
		ClusterID: "cluster-a", Status: "provisioning", CreatedAt: now,
	})

	scheduledAt := now.Add(time.Minute)
	if _, _, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl_ret2", "cust_ret2", "cluster-a", "train-pod", "ys-burst-ret2", scheduledAt); err != nil {
		t.Fatalf("initial stamp: %v", err)
	}

	// Retire the BURST durably. The workload row and its PodObservation are
	// deliberately left intact — this is the exact shape a workload-only
	// re-read would misread as Resolved. Only the burst-identity join can
	// tell that the observation no longer names a live node.
	p, ok := s.persist.(*pgPersister)
	if !ok {
		t.Fatalf("expected pgPersister, got %T", s.persist)
	}
	if _, err := p.pool.Exec(context.Background(), "DELETE FROM "+tblBursts+" WHERE id = $1", "burst_ret2"); err != nil {
		t.Fatalf("retire burst: %v", err)
	}
	// Sanity: the durable workload row and its PodObservation ARE still there.
	var durablePodName string
	if err := p.pool.QueryRow(context.Background(),
		"SELECT data->'PodObservation'->>'PodName' FROM "+tblWorkloads+" WHERE id = $1",
		"wl_ret2").Scan(&durablePodName); err != nil {
		t.Fatalf("test setup: durable workload+PodObservation should still be present: %v", err)
	}
	if durablePodName != "train-pod" {
		t.Fatalf("test setup: durable PodObservation = %q, want train-pod", durablePodName)
	}
	// In-memory row is intentionally NOT cleared — that is the stale phantom.
	if wl, _ := s.GetWorkload("wl_ret2"); wl == nil || wl.PodObservation == nil {
		t.Fatal("test setup: in-memory PodObservation should still be present")
	}

	outcome, obs, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl_ret2", "cust_ret2", "cluster-a", "train-pod", "ys-burst-ret2", scheduledAt)
	if err != nil {
		t.Fatalf("retry err = %v", err)
	}
	if outcome != PodObservationRejected {
		t.Fatalf("outcome = %v, want Rejected — burst retired, no live identity to ACK against", outcome)
	}
	if obs != nil {
		t.Fatalf("obs = %+v, want nil for Rejected", obs)
	}
	if outcome.Acknowledgeable() {
		t.Fatal("Rejected must not be Acknowledgeable — a stale replica must not ACK a retired burst's observation")
	}
}

func TestPostgresGetWorkloadPodObservation(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_get", Token: "tok_get", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_get", CustomerID: "cust_get", Backend: "linode", BackendID: "get1",
		NodeName: "ys-burst-get", ClusterID: "cluster-a",
		Status: BurstStatusProvisioning, CreatedAt: now,
	})
	s.PutWorkload(&Workload{
		ID: "wl_get", CustomerID: "cust_get", BurstID: "burst_get",
		ClusterID: "cluster-a", Status: "provisioning", CreatedAt: now,
	})

	p, ok := s.persist.(*pgPersister)
	if !ok {
		t.Fatalf("expected pgPersister, got %T", s.persist)
	}

	// Row exists, burst identity matches, no observation yet.
	obs, exists, err := p.getWorkloadPodObservation(context.Background(), "wl_get", "cust_get", "cluster-a", "ys-burst-get")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !exists {
		t.Fatal("row missing")
	}
	if obs != nil {
		t.Fatalf("obs = %+v before stamp, want nil", obs)
	}

	// After stamp, observation is present.
	at := now.Add(time.Minute)
	if _, _, err := s.StampWorkloadPodObservationOutcome(context.Background(),
		"wl_get", "cust_get", "cluster-a", "train-pod", "ys-burst-get", at); err != nil {
		t.Fatalf("stamp: %v", err)
	}
	obs, exists, err = p.getWorkloadPodObservation(context.Background(), "wl_get", "cust_get", "cluster-a", "ys-burst-get")
	if err != nil || !exists {
		t.Fatalf("read after stamp: exists=%v err=%v", exists, err)
	}
	if obs == nil || obs.PodName != "train-pod" || obs.NodeName != "ys-burst-get" || !obs.ScheduledAt.Equal(at) {
		t.Fatalf("obs = %+v, want the exact stamped identity", obs)
	}

	// Wrong tenant is indistinguishable from no such row.
	_, existsWrongTenant, err := p.getWorkloadPodObservation(context.Background(), "wl_get", "cust_other", "cluster-a", "ys-burst-get")
	if err != nil {
		t.Fatalf("read wrong tenant err = %v", err)
	}
	if existsWrongTenant {
		t.Fatal("wrong tenant leaked existence")
	}

	// Wrong cluster is indistinguishable from no such row.
	_, existsWrongCluster, err := p.getWorkloadPodObservation(context.Background(), "wl_get", "cust_get", "cluster-b", "ys-burst-get")
	if err != nil {
		t.Fatalf("read wrong cluster err = %v", err)
	}
	if existsWrongCluster {
		t.Fatal("wrong cluster leaked existence")
	}

	// Wrong burst NodeName is indistinguishable from no such row — the
	// burst-identity join is what stops a stale replica from ACKing an
	// observation whose burst has since been retired or renamed.
	_, existsWrongNode, err := p.getWorkloadPodObservation(context.Background(), "wl_get", "cust_get", "cluster-a", "ys-burst-different")
	if err != nil {
		t.Fatalf("read wrong node err = %v", err)
	}
	if existsWrongNode {
		t.Fatal("wrong burst NodeName leaked existence")
	}

	// Empty cluster refuses without querying (guard against legacy callers).
	_, existsEmpty, err := p.getWorkloadPodObservation(context.Background(), "wl_get", "cust_get", "", "ys-burst-get")
	if err != nil || existsEmpty {
		t.Fatalf("empty cluster ID: exists=%v err=%v, want false/nil", existsEmpty, err)
	}

	// Empty nodeName refuses without querying — the join would be vacuous.
	_, existsEmptyNode, err := p.getWorkloadPodObservation(context.Background(), "wl_get", "cust_get", "cluster-a", "")
	if err != nil || existsEmptyNode {
		t.Fatalf("empty nodeName: exists=%v err=%v, want false/nil", existsEmptyNode, err)
	}
}

func TestPostgresGPUObservationRejectsLegacyBurstClusterID(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_gpu", Token: "tok_gpu", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_gpu", CustomerID: "cust_gpu", Backend: "linode", BackendID: "gpu1",
		NodeName: "ys-burst-gpu", ClusterID: "",
		Status: BurstStatusProvisioning, CreatedAt: now,
	})
	s.PutWorkload(&Workload{
		ID: "wl_gpu", CustomerID: "cust_gpu", BurstID: "burst_gpu",
		ClusterID: "cluster-a", Status: "provisioning", CreatedAt: now,
	})

	gpuAt := now.Add(time.Minute)
	applied, err := s.UpdateBurstNodePhase(context.Background(), BurstNodePhaseUpdate{
		BurstID: "burst_gpu", CustomerID: "cust_gpu", ClusterID: "cluster-a",
		Phase: protocol.NodePhaseReady, Reason: "KubeletReady",
		ObservedAt:     now.Add(time.Minute),
		GPUAllocatable: true, GPUAllocatableAt: &gpuAt,
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !applied {
		t.Fatal("phase update not applied")
	}

	wl, _ := s.GetWorkload("wl_gpu")
	if wl.GPUObservation != nil {
		t.Fatal("GPU observation stamped through legacy burst with empty ClusterID in PostgreSQL")
	}
}
