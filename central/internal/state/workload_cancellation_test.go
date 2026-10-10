package state

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

func TestWorkloadCancellationMemoryAndFailures(t *testing.T) {
	for _, scenario := range []string{"accepted", "cancelled-context", "unsupported-durable", "foreign-burst", "wrong-credential"} {
		t.Run(scenario, func(t *testing.T) {
			s := emptyStore()
			s.AddCustomer(&Customer{ID: "cancel_tenant", Token: "synthetic-cancel"})
			w := &Workload{ID: "cancel_workload", CustomerID: "cancel_tenant", BurstID: "cancel_burst", Status: "running"}
			s.PutWorkload(w)
			b := &Burst{ID: w.BurstID, CustomerID: w.CustomerID, BackendID: "synthetic-resource"}
			if err := s.PutBurst(b); err != nil {
				t.Fatal(err)
			}
			principal := WorkloadCancelPrincipal{CredentialHash: HashCustomerToken("synthetic-cancel")}
			ctx := context.Background()
			var want error
			switch scenario {
			case "cancelled-context":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "unsupported-durable":
				s.persist = &accountSpyPersister{}
				want = ErrPersistence
			case "foreign-burst":
				b.CustomerID = "foreign"
				want = ErrPersistence
			case "wrong-credential":
				principal.CredentialHash = HashCustomerToken("wrong-synthetic")
				want = ErrNotFound
			}
			result, err := s.RequestWorkloadCancellation(ctx, w.CustomerID, w.ID, principal)
			if !errors.Is(err, want) {
				t.Fatalf("cancel error=%v, want %v", err, want)
			}
			current, _ := s.GetWorkload(w.ID)
			booking, _ := s.GetBurst(b.ID)
			if want != nil {
				if current.Status != "running" || booking.ReapPending {
					t.Fatal("failed cancel published state")
				}
				return
			}
			if !result.Decision.Allowed || !result.applied || current.Status != "cancelled" || !booking.ReapPending || b.ReapPending {
				t.Fatal("cancellation did not publish detached pending state")
			}
			finished := *current.FinishedAt
			result.Workload.Status = "corrupted"
			result.Burst.BackendID = "corrupted"
			again, err := s.RequestWorkloadCancellation(ctx, w.CustomerID, w.ID, principal)
			if err != nil || again.applied || again.Workload.Status != "cancelled" || !again.Workload.FinishedAt.Equal(finished) || again.Burst.BackendID != "synthetic-resource" {
				t.Fatalf("repeat or detached result failed: %v", err)
			}
			stale := cloneWorkload(again.Workload)
			stale.BurstID = "different"
			if err := s.PutWorkloadDurable(stale); !errors.Is(err, ErrPersistence) {
				t.Fatalf("terminal rebind=%v", err)
			}
			ev := submitEvent(w.CustomerID, w.ID, ClusterActor(w.CustomerID))
			if err := s.SubmitWorkload(stale, ev); !errors.Is(err, ErrPersistence) {
				t.Fatalf("terminal resubmit rebind=%v", err)
			}
			command := testConnectorCommand(t, w.CustomerID, "cluster", protocol.TypeCreateJob, w.ID, w.ID, stale.BurstID, "")
			if err := s.SubmitWorkloadWithConnectorCommands(ctx, stale, ev, []ConnectorCommand{command}); !errors.Is(err, ErrPersistence) {
				t.Fatalf("terminal command admission rebind=%v", err)
			}
			if len(s.connectorCommands) != 0 {
				t.Fatal("rejected rebind published a connector command")
			}
		})
	}
}

func TestPendingBurstPreservesSettlementAndCleanup(t *testing.T) {
	stored := &Burst{ID: "burst", CustomerID: "tenant", BackendID: "synthetic", ReapPending: true, ReapPendingStatus: "cancelled", CreatedAt: time.Now().UTC(), HourlyUSD: 1.25,
		NodeName: "original-node", TSHostname: "original-mesh", PodCIDR: "10.244.1.0/24", Billing: &WorkloadBilling{HoldID: 1, WorkloadRef: "original", ManualAttention: true}, TerminalCost: &WorkloadCost{}}
	stale := cloneBurstSnapshot(stored)
	stale.ReapPending, stale.ReapPendingStatus = false, ""
	stale.CreatedAt, stale.HourlyUSD = time.Now().Add(time.Hour), 99
	stale.Billing, stale.TerminalCost = nil, nil
	stale.NodeName, stale.TSHostname, stale.PodCIDR = "wrong-node", "wrong-mesh", "wrong-pod-cidr"
	if err := preservePendingBurst(stale, stored, nil); err != nil {
		t.Fatal(err)
	}
	if !stale.ReapPending || stale.ReapPendingStatus != "cancelled" || !stale.CreatedAt.Equal(stored.CreatedAt) || stale.HourlyUSD != stored.HourlyUSD || stale.Billing == nil || *stale.Billing != *stored.Billing || stale.TerminalCost == nil || stale.NodeName != stored.NodeName || stale.TSHostname != stored.TSHostname || stale.PodCIDR != stored.PodCIDR {
		t.Fatal("pending cleanup or settlement identity was overwritten")
	}
	stored.Billing.ManualAttention = false
	stale.Billing.ManualAttention = true
	if err := preservePendingBurst(stale, stored, nil); err != nil || !stale.Billing.ManualAttention {
		t.Fatalf("new billing manual-attention flag was lost: %v", err)
	}
}
