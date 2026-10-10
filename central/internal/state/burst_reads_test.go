package state

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type burstReadPersister struct {
	listOnlyPersister
	read func(context.Context, string) (*Burst, error)
}

func (p *burstReadPersister) readBurstSnapshot(ctx context.Context, id string) (*Burst, error) {
	return p.read(ctx, id)
}

func TestBurstSnapshotContextFailsClosed(t *testing.T) {
	for _, scenario := range []string{"unsupported", "missing", "failure", "nil", "wrong-id", "no-customer", "cancelled", "current"} {
		t.Run(scenario, func(t *testing.T) {
			s := emptyStore()
			s.bursts["burst"] = &Burst{ID: "burst", CustomerID: "customer", BackendID: "cached"}
			calls := 0
			s.persist = &burstReadPersister{read: func(ctx context.Context, id string) (*Burst, error) {
				calls++
				if id != "burst" {
					t.Fatal("reader received a different lookup key")
				}
				switch scenario {
				case "missing":
					return nil, ErrNotFound
				case "failure":
					return s.bursts[id], errors.New("read failed with partial data")
				case "nil":
					return nil, nil
				case "wrong-id":
					return &Burst{ID: "different", CustomerID: "customer"}, nil
				case "no-customer":
					return &Burst{ID: id}, nil
				default:
					return &Burst{ID: id, CustomerID: "customer", BackendID: "current"}, nil
				}
			}}
			ctx := context.Background()
			wantErr := ErrPersistence
			switch scenario {
			case "unsupported":
				s.persist = &listOnlyPersister{}
			case "missing":
				wantErr = ErrNotFound
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				wantErr = context.Canceled
			case "current":
				wantErr = nil
			}
			got, err := s.BurstSnapshotContext(ctx, "burst")
			if !errors.Is(err, wantErr) || (wantErr != nil && got != nil) {
				t.Fatalf("snapshot=%v, error=%v; want %v", got, err, wantErr)
			}
			if wantErr == nil && got.BackendID != "current" {
				t.Error("cache overrode the durable record")
			}
			if s.bursts["burst"].BackendID != "cached" {
				t.Error("read replaced the mutation cache")
			}
			if scenario == "cancelled" && calls != 0 {
				t.Error("cancelled request reached persistence")
			}
		})
	}
}

func TestBurstSnapshotContextDetachedInMemory(t *testing.T) {
	s := emptyStore()
	at := time.Now().UTC()
	original := &Burst{ID: "burst", CustomerID: "customer", BackendID: "original",
		Billing: &WorkloadBilling{HoldID: 101}, TerminalCost: &WorkloadCost{BurstID: "burst"},
		LastHeartbeatAt: &at, NodePhaseAt: &at, OccupancyObservedAt: &at}
	s.bursts[original.ID] = original
	before := cloneBurstSnapshot(original)
	read, err := s.BurstSnapshotContext(context.Background(), original.ID)
	if err != nil || !reflect.DeepEqual(read, before) {
		t.Fatalf("snapshot differs: %v", err)
	}
	read.BackendID = "changed"
	read.Billing.HoldID = 202
	read.TerminalCost.BurstID = "changed"
	*read.LastHeartbeatAt = at.Add(time.Hour)
	*read.NodePhaseAt = at.Add(2 * time.Hour)
	*read.OccupancyObservedAt = at.Add(3 * time.Hour)
	if !reflect.DeepEqual(original, before) {
		t.Error("snapshot shares mutable fields with the stored booking")
	}
	for _, id := range []string{"", "missing"} {
		if got, err := s.BurstSnapshotContext(context.Background(), id); got != nil || !errors.Is(err, ErrNotFound) {
			t.Errorf("absent snapshot=%v, error=%v", got, err)
		}
	}
}
