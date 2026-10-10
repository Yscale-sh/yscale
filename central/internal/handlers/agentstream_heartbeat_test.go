package handlers

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

func mustHeartbeatBody(t *testing.T, hb protocol.Heartbeat) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(hb)
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	return raw
}

func TestAgentStreamHeartbeatStoresObservedZeroValuesAndClearsStaleInventory(t *testing.T) {
	stream := &AgentStream{Log: quietLog()}
	agent := &state.Agent{ID: "agent_1"}
	observedAt := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)

	stream.handleHeartbeat(agent, mustHeartbeatBody(t, protocol.Heartbeat{
		NodeInventoryObserved: true,
		PodInventoryObserved:  true,
	}), observedAt)
	got, ok := agent.ClusterInventory()
	if !ok {
		t.Fatal("observed zero-value heartbeat did not store inventory")
	}
	if !got.NodeInventoryObserved || !got.PodInventoryObserved ||
		!got.NodeInventoryObservedAt.Equal(observedAt) || !got.PodInventoryObservedAt.Equal(observedAt) ||
		got.NodeCount != 0 || got.BurstCount != 0 || got.PendingPods != 0 {
		t.Fatalf("inventory = %+v, want observed zero snapshot at %v", got, observedAt)
	}

	stream.handleHeartbeat(agent, mustHeartbeatBody(t, protocol.Heartbeat{}), observedAt.Add(time.Minute))
	if got, ok := agent.ClusterInventory(); ok {
		t.Fatalf("unobserved heartbeat left stale inventory: %+v", got)
	}
}

func TestAgentStreamHeartbeatRejectsInvalidObservedInventory(t *testing.T) {
	stream := &AgentStream{Log: quietLog()}
	agent := &state.Agent{ID: "agent_1"}
	stale := state.ClusterInventorySnapshot{
		ObservedAt:              time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC),
		NodeInventoryObserved:   true,
		NodeInventoryObservedAt: time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC),
		PodInventoryObserved:    true,
		PodInventoryObservedAt:  time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC),
		NodeCount:               3,
		BurstCount:              1,
		PendingPods:             2,
	}
	agent.ApplyClusterInventory(state.ClusterInventoryUpdate{
		NodeValid: true, NodeObserved: true, NodeObservedAt: stale.NodeInventoryObservedAt,
		NodeCount: stale.NodeCount, BurstCount: stale.BurstCount,
		PodValid: true, PodObserved: true, PodObservedAt: stale.PodInventoryObservedAt,
		PendingPods: stale.PendingPods,
	})

	for _, hb := range []protocol.Heartbeat{
		{NodeInventoryObserved: true, NodeCount: -1, PodInventoryObserved: true, PendingPods: 2},
		{NodeInventoryObserved: true, NodeCount: 1, BurstCount: 2, PodInventoryObserved: true, PendingPods: 2},
		{NodeInventoryObserved: true, NodeCount: maxHeartbeatInventoryCount + 1, PodInventoryObserved: true, PendingPods: 2},
		{NodeInventoryObserved: true, NodeCount: 3, BurstCount: 1, PodInventoryObserved: true, PendingPods: -1},
		{NodeInventoryObserved: true, NodeCount: 3, BurstCount: 1, PodInventoryObserved: true, PendingPods: maxHeartbeatInventoryCount + 1},
	} {
		hb.NodeInventoryObservedAt = heartbeatTimePointer(stale.NodeInventoryObservedAt)
		hb.PodInventoryObservedAt = heartbeatTimePointer(stale.PodInventoryObservedAt)
		stream.handleHeartbeat(agent, mustHeartbeatBody(t, hb), stale.ObservedAt.Add(time.Minute))
		got, ok := agent.ClusterInventory()
		if !ok {
			t.Fatalf("invalid heartbeat %+v cleared the prior inventory", hb)
		}
		if got != stale {
			t.Fatalf("invalid heartbeat %+v changed inventory to %+v, want %+v", hb, got, stale)
		}
	}
}

func TestAgentStreamHeartbeatValidatesAndAppliesScopesIndependently(t *testing.T) {
	stream := &AgentStream{Log: quietLog()}
	agent := &state.Agent{ID: "agent_1"}
	start := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	agent.ApplyClusterInventory(state.ClusterInventoryUpdate{
		NodeValid: true, NodeObserved: true, NodeObservedAt: start,
		PodValid: true, PodObserved: true, PodObservedAt: start,
		NodeCount: 3, BurstCount: 1, PendingPods: 2,
	})

	stream.handleHeartbeat(agent, mustHeartbeatBody(t, protocol.Heartbeat{
		NodeInventoryObserved: true, NodeCount: -1,
		PodInventoryObserved: true, PendingPods: 7,
	}), start.Add(time.Minute))
	got, ok := agent.ClusterInventory()
	if !ok || got.NodeCount != 3 || got.BurstCount != 1 || got.PendingPods != 7 {
		t.Fatalf("invalid node scope blocked valid pod update: %+v, ok=%v", got, ok)
	}

	stream.handleHeartbeat(agent, mustHeartbeatBody(t, protocol.Heartbeat{
		NodeInventoryObserved: true, NodeCount: 5, BurstCount: 2,
		PodInventoryObserved: true, PendingPods: -1,
	}), start.Add(2*time.Minute))
	got, ok = agent.ClusterInventory()
	if !ok || got.NodeCount != 5 || got.BurstCount != 2 || got.PendingPods != 7 {
		t.Fatalf("invalid pod scope blocked valid node update: %+v, ok=%v", got, ok)
	}
}

func TestAgentStreamHeartbeatUpdatesAndClearsScopesIndependently(t *testing.T) {
	stream := &AgentStream{Log: quietLog()}
	agent := &state.Agent{ID: "agent_1"}
	nodeAt := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	podAt := nodeAt.Add(time.Minute)

	stream.handleHeartbeat(agent, mustHeartbeatBody(t, protocol.Heartbeat{
		NodeInventoryObserved:   true,
		NodeInventoryObservedAt: heartbeatTimePointer(nodeAt),
		NodeCount:               4,
		BurstCount:              1,
		PodInventoryObserved:    true,
		PodInventoryObservedAt:  heartbeatTimePointer(podAt),
		PendingPods:             2,
	}), podAt)

	stream.handleHeartbeat(agent, mustHeartbeatBody(t, protocol.Heartbeat{
		NodeInventoryObserved:   true,
		NodeInventoryObservedAt: heartbeatTimePointer(nodeAt.Add(2 * time.Minute)),
		NodeCount:               0,
		BurstCount:              0,
	}), podAt.Add(2*time.Minute))

	got, ok := agent.ClusterInventory()
	if !ok {
		t.Fatal("partial heartbeat cleared all inventory")
	}
	if !got.NodeInventoryObserved || got.NodeCount != 0 || got.BurstCount != 0 {
		t.Fatalf("node inventory = %+v, want observed zero node scope", got)
	}
	if got.PodInventoryObserved {
		t.Fatalf("pod inventory remained observed after explicit unobserved scope: %+v", got)
	}
}

func heartbeatTimePointer(value time.Time) *time.Time {
	return &value
}

func TestAgentStreamHeartbeatLegacyObservedInventoryStillWorks(t *testing.T) {
	stream := &AgentStream{Log: quietLog()}
	agent := &state.Agent{ID: "agent_1"}
	observedAt := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)

	stream.handleHeartbeat(agent, mustHeartbeatBody(t, protocol.Heartbeat{
		InventoryObserved: true,
		NodeCount:         3,
		BurstCount:        1,
		PendingPods:       2,
	}), observedAt)
	got, ok := agent.ClusterInventory()
	if !ok {
		t.Fatal("legacy observed heartbeat did not store inventory")
	}
	if !got.NodeInventoryObserved || !got.PodInventoryObserved ||
		got.NodeCount != 3 || got.BurstCount != 1 || got.PendingPods != 2 {
		t.Fatalf("legacy inventory = %+v, want both scopes observed", got)
	}
}

func TestAgentStreamHeartbeatClampsFutureObservationTimes(t *testing.T) {
	stream := &AgentStream{Log: quietLog()}
	agent := &state.Agent{ID: "agent_clock"}
	receivedAt := time.Now().UTC()
	future := receivedAt.Add(24 * time.Hour)
	stream.handleHeartbeat(agent, mustHeartbeatBody(t, protocol.Heartbeat{
		NodeInventoryObserved: true, NodeInventoryObservedAt: heartbeatTimePointer(future), NodeCount: 1,
		PodInventoryObserved: true, PodInventoryObservedAt: heartbeatTimePointer(future), PendingPods: 0,
	}), receivedAt)
	got, ok := agent.ClusterInventory()
	if !ok || !got.NodeInventoryObservedAt.Equal(receivedAt) || !got.PodInventoryObservedAt.Equal(receivedAt) {
		t.Fatalf("future connector timestamps were not clamped to receipt time: %+v", got)
	}
}

func TestAgentReadLimitAccommodatesMaximumWorkloadLogAck(t *testing.T) {
	if wsAgentReadLimit <= maxTenantLogResultBytes {
		t.Fatalf("agent read limit = %d, must exceed max log result %d for envelope overhead", wsAgentReadLimit, maxTenantLogResultBytes)
	}
}
