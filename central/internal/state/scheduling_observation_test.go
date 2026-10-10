package state

import (
	"context"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

func schedulingTestStore(t *testing.T) *Store {
	t.Helper()
	s := emptyStore()
	seedPhaseBurstWithNode(s)
	s.bursts["b1"].ClusterID = "cluster-a"
	w := seedPhaseWorkload(s)
	w.ClusterID = "cluster-a"
	return s
}

func TestSchedulingObservationLifecycleIsMonotonic(t *testing.T) {
	s := schedulingTestStore(t)
	ctx := context.Background()
	firstAt := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	newerAt := firstAt.Add(time.Minute)

	result, err := s.StampWorkloadSchedulingObservation(ctx, "wl1", "cust_a", "cluster-a",
		protocol.SchedulingStateWaiting, protocol.SchedulingReasonInsufficientGPU,
		protocol.SchedulingReasonMessage(protocol.SchedulingReasonInsufficientGPU), "pod-a", firstAt)
	if err != nil || result != ObservationApplied {
		t.Fatalf("first Waiting = %v, %v", result, err)
	}
	result, err = s.StampWorkloadSchedulingObservation(ctx, "wl1", "cust_a", "cluster-a",
		protocol.SchedulingStateWaiting, protocol.SchedulingReasonInsufficientGPU,
		protocol.SchedulingReasonMessage(protocol.SchedulingReasonInsufficientGPU), "pod-a", firstAt)
	if err != nil || result != ObservationResolved {
		t.Fatalf("Waiting replay = %v, %v", result, err)
	}
	result, err = s.StampWorkloadSchedulingObservation(ctx, "wl1", "cust_a", "cluster-a",
		protocol.SchedulingStateWaiting, protocol.SchedulingReasonInsufficientCPU,
		protocol.SchedulingReasonMessage(protocol.SchedulingReasonInsufficientCPU), "pod-a", newerAt)
	if err != nil || result != ObservationApplied {
		t.Fatalf("newer Waiting = %v, %v", result, err)
	}

	w, _ := s.GetWorkload("wl1")
	w.PodObservation = &PodObservation{PodName: "pod-a", NodeName: "ys-burst-b1", ScheduledAt: newerAt}
	s.PutWorkload(w)
	result, err = s.StampWorkloadSchedulingObservation(ctx, "wl1", "cust_a", "cluster-a",
		protocol.SchedulingStateScheduled, "", "", "pod-a", newerAt)
	if err != nil || result != ObservationApplied {
		t.Fatalf("Scheduled = %v, %v", result, err)
	}
	result, _ = s.StampWorkloadSchedulingObservation(ctx, "wl1", "cust_a", "cluster-a",
		protocol.SchedulingStateWaiting, protocol.SchedulingReasonInsufficientGPU, "ignored", "pod-a", newerAt.Add(time.Minute))
	if result != ObservationRejected {
		t.Fatalf("Waiting after Scheduled = %v, want rejected", result)
	}
	got, _ := s.GetWorkload("wl1")
	if got.SchedulingObservation == nil || got.SchedulingObservation.State != protocol.SchedulingStateScheduled {
		t.Fatalf("terminal scheduling state = %+v", got.SchedulingObservation)
	}
}

func TestSchedulingObservationRejectsUnboundIdentity(t *testing.T) {
	s := schedulingTestStore(t)
	at := time.Now().UTC()
	cases := map[string][3]string{
		"wrong tenant":  {"other", "cluster-a", protocol.SchedulingReasonInsufficientGPU},
		"wrong cluster": {"cust_a", "cluster-b", protocol.SchedulingReasonInsufficientGPU},
		"empty cluster": {"cust_a", "", protocol.SchedulingReasonInsufficientGPU},
		"bad reason":    {"cust_a", "cluster-a", "RawSchedulerText"},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			result, err := s.StampWorkloadSchedulingObservation(context.Background(), "wl1", values[0], values[1],
				protocol.SchedulingStateWaiting, values[2], "message", "pod-a", at)
			if err != nil || result != ObservationRejected {
				t.Fatalf("result = %v, err = %v", result, err)
			}
		})
	}
}
