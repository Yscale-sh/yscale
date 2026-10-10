package handlers

import (
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

func seedGPUAgent(s *state.Store) *state.Agent {
	agent := &state.Agent{
		ID:         "agent_gpu",
		CustomerID: "cust_a",
		ClusterID:  "cluster_1",
	}
	s.AddAgent(agent)
	return agent
}

func seedGPUBurstInStore(t *testing.T, s *state.Store, burstID, customerID, clusterID, nodeName string) {
	t.Helper()
	if err := s.PutBurst(&state.Burst{
		ID:         burstID,
		CustomerID: customerID,
		ClusterID:  clusterID,
		NodeName:   nodeName,
		Backend:    "linode",
		Status:     state.BurstStatusRunning,
		CreatedAt:  time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
}

func gpuSample(burstID, nodeName string, util float64, observedAt time.Time) protocol.GPUTelemetrySample {
	return protocol.GPUTelemetrySample{
		BurstID:     burstID,
		NodeName:    nodeName,
		Utilization: util,
		ObservedAt:  observedAt,
	}
}

func TestApplyGPUTelemetryHappyPath(t *testing.T) {
	s := state.New()
	seedGPUBurstInStore(t, s, "b1", "cust_a", "cluster_1", "ys-burst-b1")
	stream := &AgentStream{Store: s, Log: quietLog()}
	agent := seedGPUAgent(s)
	now := time.Now().UTC()

	stream.applyGPUTelemetry(agent, []protocol.GPUTelemetrySample{
		gpuSample("b1", "ys-burst-b1", 75, now),
	}, now)

	b, err := s.GetBurst("b1")
	if err != nil {
		t.Fatal(err)
	}
	if b.GPUUtilPercent != 75 {
		t.Fatalf("got %f, want 75", b.GPUUtilPercent)
	}
}

func TestApplyGPUTelemetryZeroUtilization(t *testing.T) {
	s := state.New()
	seedGPUBurstInStore(t, s, "b1", "cust_a", "cluster_1", "ys-burst-b1")
	stream := &AgentStream{Store: s, Log: quietLog()}
	agent := seedGPUAgent(s)
	now := time.Now().UTC()

	stream.applyGPUTelemetry(agent, []protocol.GPUTelemetrySample{
		gpuSample("b1", "ys-burst-b1", 0, now),
	}, now)

	b, _ := s.GetBurst("b1")
	if b.GPUUtilPercent != 0 || b.LastHeartbeatAt == nil {
		t.Fatalf("0%% should be stored with a timestamp: util=%f hb=%v", b.GPUUtilPercent, b.LastHeartbeatAt)
	}
}

func TestApplyGPUTelemetrySkipsMalformedSamples(t *testing.T) {
	s := state.New()
	seedGPUBurstInStore(t, s, "b1", "cust_a", "cluster_1", "ys-burst-b1")
	stream := &AgentStream{Store: s, Log: quietLog()}
	agent := seedGPUAgent(s)
	now := time.Now().UTC()

	stream.applyGPUTelemetry(agent, []protocol.GPUTelemetrySample{
		{BurstID: "", NodeName: "ys-burst-b1", Utilization: 50, ObservedAt: now},
		{BurstID: "b1", NodeName: "", Utilization: 50, ObservedAt: now},
		{BurstID: "b1", NodeName: "ys-burst-b1", Utilization: -1, ObservedAt: now},
		{BurstID: "b1", NodeName: "ys-burst-b1", Utilization: 101, ObservedAt: now},
	}, now)

	b, _ := s.GetBurst("b1")
	if b.LastHeartbeatAt != nil {
		t.Fatal("malformed samples should not have been applied")
	}
}

func TestApplyGPUTelemetrySkipsFutureSamples(t *testing.T) {
	s := state.New()
	seedGPUBurstInStore(t, s, "b1", "cust_a", "cluster_1", "ys-burst-b1")
	stream := &AgentStream{Store: s, Log: quietLog()}
	agent := seedGPUAgent(s)
	now := time.Now().UTC()

	stream.applyGPUTelemetry(agent, []protocol.GPUTelemetrySample{
		gpuSample("b1", "ys-burst-b1", 50, now.Add(2*time.Minute)),
	}, now)

	b, _ := s.GetBurst("b1")
	if b.LastHeartbeatAt != nil {
		t.Fatal("future sample should be rejected")
	}
}

func TestApplyGPUTelemetrySkipsStaleSamples(t *testing.T) {
	s := state.New()
	seedGPUBurstInStore(t, s, "b1", "cust_a", "cluster_1", "ys-burst-b1")
	stream := &AgentStream{Store: s, Log: quietLog()}
	agent := seedGPUAgent(s)
	now := time.Now().UTC()

	stream.applyGPUTelemetry(agent, []protocol.GPUTelemetrySample{
		gpuSample("b1", "ys-burst-b1", 50, now.Add(-6*time.Minute)),
	}, now)

	b, _ := s.GetBurst("b1")
	if b.LastHeartbeatAt != nil {
		t.Fatal("stale sample should be rejected")
	}
}

func TestApplyGPUTelemetryTruncatesOverLimit(t *testing.T) {
	s := state.New()
	stream := &AgentStream{Store: s, Log: quietLog()}
	agent := seedGPUAgent(s)
	now := time.Now().UTC()

	samples := make([]protocol.GPUTelemetrySample, protocol.MaxGPUTelemetrySamples+5)
	for i := range samples {
		samples[i] = gpuSample("b1", "ys-burst-b1", 50, now)
	}
	// should not panic; truncation is the observable effect
	stream.applyGPUTelemetry(agent, samples, now)
}

func TestApplyGPUTelemetryIsolatesTenants(t *testing.T) {
	s := state.New()
	seedGPUBurstInStore(t, s, "b1", "cust_a", "cluster_1", "ys-burst-b1")
	seedGPUBurstInStore(t, s, "b2", "cust_b", "cluster_2", "ys-burst-b2")

	stream := &AgentStream{Store: s, Log: quietLog()}
	agentA := &state.Agent{ID: "agent_a", CustomerID: "cust_a", ClusterID: "cluster_1"}
	s.AddAgent(agentA)

	now := time.Now().UTC()
	stream.applyGPUTelemetry(agentA, []protocol.GPUTelemetrySample{
		gpuSample("b2", "ys-burst-b2", 99, now),
	}, now)

	b2, _ := s.GetBurst("b2")
	if b2.LastHeartbeatAt != nil {
		t.Fatal("agent_a must not write to cust_b's burst")
	}
}

func TestApplyGPUTelemetryNilStoreNoPanic(t *testing.T) {
	stream := &AgentStream{Store: nil, Log: quietLog()}
	agent := &state.Agent{ID: "a", CustomerID: "c", ClusterID: "cl"}
	now := time.Now().UTC()
	stream.applyGPUTelemetry(agent, []protocol.GPUTelemetrySample{
		gpuSample("b1", "ys-burst-b1", 50, now),
	}, now)
}

func TestApplyGPUTelemetryEmptySamplesNoop(t *testing.T) {
	stream := &AgentStream{Store: state.New(), Log: quietLog()}
	agent := &state.Agent{ID: "a", CustomerID: "c", ClusterID: "cl"}
	stream.applyGPUTelemetry(agent, nil, time.Now())
	stream.applyGPUTelemetry(agent, []protocol.GPUTelemetrySample{}, time.Now())
}

func TestHeartbeatCarriesGPUTelemetryToApply(t *testing.T) {
	s := state.New()
	seedGPUBurstInStore(t, s, "b1", "cust_a", "cluster_1", "ys-burst-b1")
	stream := &AgentStream{Store: s, Log: quietLog()}
	agent := seedGPUAgent(s)
	now := time.Now().UTC()

	hb := protocol.Heartbeat{
		GPUTelemetry: []protocol.GPUTelemetrySample{
			gpuSample("b1", "ys-burst-b1", 42, now),
		},
	}
	stream.handleHeartbeat(agent, mustHeartbeatBody(t, hb), now)

	b, _ := s.GetBurst("b1")
	if b.GPUUtilPercent != 42 {
		t.Fatalf("heartbeat GPU telemetry not applied: got %f", b.GPUUtilPercent)
	}
}
