package agent

import (
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

func TestPodEventAckIdentityMatching(t *testing.T) {
	oldAt := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	newAt := oldAt.Add(time.Minute)
	explicit := protocol.PodEvent{
		WorkloadID:           "wl1",
		PodName:              "pod-a",
		SchedulingState:      protocol.SchedulingStateWaiting,
		SchedulingObservedAt: &newAt,
	}
	tests := []struct {
		name string
		ack  protocol.PodEventAck
		want bool
	}{
		{"exact", protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-a", SchedulingState: protocol.SchedulingStateWaiting, SchedulingObservedAt: &newAt}, true},
		{"old version", protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-a", SchedulingState: protocol.SchedulingStateWaiting, SchedulingObservedAt: &oldAt}, false},
		{"wrong pod", protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-b", SchedulingState: protocol.SchedulingStateWaiting, SchedulingObservedAt: &newAt}, false},
		{"wrong state", protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-a", SchedulingState: protocol.SchedulingStateScheduled, SchedulingObservedAt: &newAt}, false},
		{"legacy ack", protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-a"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podEventAckMatchesCached(tt.ack, explicit); got != tt.want {
				t.Fatalf("match = %v, want %v", got, tt.want)
			}
		})
	}

	legacy := protocol.PodEvent{WorkloadID: "wl1", PodName: "pod-a", ScheduledAt: &newAt}
	if !podEventAckMatchesCached(protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-a", ScheduledAt: &newAt}, legacy) {
		t.Fatal("exact legacy positive ACK should match")
	}
	if podEventAckMatchesCached(protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-a", ScheduledAt: &oldAt}, legacy) {
		t.Fatal("stale legacy positive ACK must not match")
	}
}

func TestSchedulerClassificationDoesNotExposeRawMessage(t *testing.T) {
	raw := "0/3 nodes are available: 3 Insufficient nvidia.com/gpu; tenant-secret=do-not-copy"
	reason := classifySchedulerMessage("Unschedulable", raw)
	if reason != protocol.SchedulingReasonInsufficientGPU {
		t.Fatalf("reason = %q", reason)
	}
	if message := protocol.SchedulingReasonMessage(reason); message == raw {
		t.Fatal("raw scheduler message crossed the protocol boundary")
	}
}
