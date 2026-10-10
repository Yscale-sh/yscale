package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Mirrors the durable row selector for recording backends, never the Store's
// local map. Callers hold their backend lock through mutation/publication.
func transitionTestRow(rows map[string]*Workload, req workloadTransition) (*Workload, error) {
	var current *Workload
	for _, w := range rows {
		if (req.WorkloadID != "" && w.ID == req.WorkloadID) || (req.BurstID != "" && w.BurstID == req.BurstID) {
			if current != nil {
				return nil, errors.New("ambiguous workload transition binding")
			}
			current = cloneWorkload(w)
		}
	}
	if current == nil || !req.Target.matches(current) {
		return nil, ErrNotFound
	}
	return current, nil
}

func (p *workloadPersister) transitionWorkload(ctx context.Context, req workloadTransition) (*Workload, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if p.lookupErr != nil {
		return nil, false, p.lookupErr
	}
	w, err := transitionTestRow(p.rows, req)
	if err != nil {
		return nil, false, err
	}
	applied := applyWorkloadTransition(w, req)
	if applied {
		p.upserts = append(p.upserts, cloneWorkload(w))
		if p.upsertErr != nil {
			return nil, false, p.upsertErr
		}
		p.rows[w.BurstID] = cloneWorkload(w)
	}
	return w, applied, nil
}

func TestWorkloadTransitionTargetAndCancellation(t *testing.T) {
	s := emptyStore()
	s.PutWorkload(&Workload{ID: "w", CustomerID: "c", ClusterID: "cluster", BurstID: "b", Status: "provisioning"})
	target := WorkloadTransitionTarget{WorkloadID: "w", CustomerID: "c", ClusterID: "cluster", BurstID: "b"}
	for _, field := range []string{"customer", "cluster", "burst", "workload"} {
		wrong := target
		switch field {
		case "customer":
			wrong.CustomerID = "other"
		case "cluster":
			wrong.ClusterID = "other"
		case "burst":
			wrong.BurstID = "other"
		case "workload":
			wrong.WorkloadID = "other"
		}
		if _, applied, err := s.FinishWorkloadContext(context.Background(), wrong, "cancelled", time.Now(), true, nil); !errors.Is(err, ErrNotFound) || applied {
			t.Errorf("%s mismatch = applied %v, err %v", field, applied, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, applied, err := s.StartWorkloadContext(ctx, target, time.Now()); !errors.Is(err, context.Canceled) || applied {
		t.Fatalf("cancelled request = applied %v, err %v", applied, err)
	}
	w, applied, err := s.StartWorkloadContext(context.Background(), target, time.Now())
	if err != nil || !applied || w.Status != "running" {
		t.Fatalf("start = applied %v, err %v", applied, err)
	}
	*w.StartedAt = time.Time{}
	stored, _ := s.GetWorkload("w")
	if stored.StartedAt.IsZero() {
		t.Fatal("transition result aliases stored timestamp")
	}
}

func TestWorkloadTransitionMissingDurableOperationFailsClosed(t *testing.T) {
	s := emptyStore()
	s.PutWorkload(&Workload{ID: "w", CustomerID: "c", Status: "provisioning"})
	// This legacy backend deliberately lacks workloadTransitionWriter. Its
	// successful whole-record writer is not an atomic-transition fallback.
	s.persist = &idemPersister{}
	target := WorkloadTransitionTarget{WorkloadID: "w", CustomerID: "c"}
	if _, applied, err := s.FinishWorkloadContext(context.Background(), target, "cancelled", time.Now(), true, nil); applied || !errors.Is(err, ErrPersistence) {
		t.Fatalf("missing durable operation = applied %v, err %v", applied, err)
	}
	w, _ := s.GetWorkload("w")
	if w.Status != "provisioning" || w.FinishedAt != nil {
		t.Fatal("unsupported durable operation used the local cache")
	}
}
