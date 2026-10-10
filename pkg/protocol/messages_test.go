package protocol

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestCommandAckWithoutResultKeepsLegacyWireShape(t *testing.T) {
	body, err := json.Marshal(CommandAck{CommandID: "cmd_1", Success: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "result") {
		t.Fatalf("legacy acknowledgement unexpectedly gained a result field: %s", body)
	}
}

func TestCommandAckCarriesStructuredWorkloadLogs(t *testing.T) {
	result, err := json.Marshal(WorkloadLogs{Streams: []WorkloadLogStream{{
		Pod: "job-abc", Container: "main", Output: "ready\n",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(CommandAck{CommandID: "cmd_2", Success: true, Result: result})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"result":{"observed_at"`) || !strings.Contains(string(body), `"output":"ready\n"`) {
		t.Fatalf("result was not embedded as structured JSON: %s", body)
	}
}

func TestValidateSyncRuntimeBindings(t *testing.T) {
	valid := SyncRuntimeBindings{Namespace: "team-a", Revision: 1, Bindings: map[string]string{"API_TOKEN": "secret"}}
	if err := ValidateSyncRuntimeBindings(valid); err != nil {
		t.Fatalf("valid sync command: %v", err)
	}
	cases := []SyncRuntimeBindings{
		{Namespace: "Team-A", Revision: 1, Bindings: map[string]string{"API_TOKEN": "secret"}},
		{Namespace: "team-a", Revision: -1, Bindings: map[string]string{"API_TOKEN": "secret"}},
		{Namespace: "team-a", Revision: 1, Bindings: map[string]string{"bad": "secret"}},
		{Namespace: "team-a", Revision: 1, Bindings: map[string]string{"API_TOKEN": ""}},
		{Namespace: "team-a", Revision: 1, Bindings: map[string]string{"API_TOKEN": "bad\x00value"}},
	}
	for _, tc := range cases {
		if err := ValidateSyncRuntimeBindings(tc); err == nil {
			t.Fatalf("invalid sync command passed: %+v", tc)
		}
	}
	tooMany := SyncRuntimeBindings{Namespace: "team-a", Revision: 1, Bindings: map[string]string{}}
	for i := 0; i <= MaxRuntimeBindings; i++ {
		tooMany.Bindings[fmt.Sprintf("KEY_%02d", i)] = "secret"
	}
	if err := ValidateSyncRuntimeBindings(tooMany); err == nil {
		t.Fatal("too many bindings passed")
	}
}

// A connector too old to know the occupancy capability must decode as making no
// claim, and a connector that makes none must not put one on the wire. Central
// admits a nodeOnly burst with a finite deadline on that answer, so an absent
// field being read as anything but false would hand unbounded capacity to every
// connector deployed before this field existed.
func TestHelloOccupancyCapabilityIsAbsentUnlessClaimed(t *testing.T) {
	var legacy Hello
	if err := json.Unmarshal([]byte(`{"agent_version":"yscale-agent/v0","cluster_id":"c1"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.AuthoritativeOccupancy {
		t.Fatal("a hello with no occupancy field decoded as claiming the capability")
	}

	quiet, err := json.Marshal(Hello{ClusterID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(quiet), "authoritative_occupancy") {
		t.Fatalf("a connector claiming nothing put a claim on the wire: %s", quiet)
	}

	claiming, err := json.Marshal(Hello{ClusterID: "c1", AuthoritativeOccupancy: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(claiming), `"authoritative_occupancy":true`) {
		t.Fatalf("the capability did not reach the wire: %s", claiming)
	}
}

func TestHeartbeatInventoryObservationFlagControlsCountMeaning(t *testing.T) {
	var legacy Heartbeat
	if err := json.Unmarshal([]byte(`{"node_count":0,"burst_count":0,"pending_pods":0}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.InventoryObserved {
		t.Fatal("a heartbeat without inventory_observed decoded as observed")
	}
	if legacy.NodeInventoryObserved || legacy.PodInventoryObserved {
		t.Fatalf("legacy heartbeat decoded per-scope flags as observed: %+v", legacy)
	}

	observed, err := json.Marshal(Heartbeat{
		InventoryObserved:     true,
		NodeInventoryObserved: true,
		PodInventoryObserved:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"inventory_observed":true`,
		`"node_inventory_observed":true`,
		`"pod_inventory_observed":true`,
		`"node_count":0`,
		`"burst_count":0`,
		`"pending_pods":0`,
	} {
		if !strings.Contains(string(observed), want) {
			t.Fatalf("observed zero heartbeat missing %s: %s", want, observed)
		}
	}

	unobserved, err := json.Marshal(Heartbeat{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(unobserved), `"inventory_observed":false`) {
		t.Fatalf("unobserved heartbeat did not carry the explicit false flag: %s", unobserved)
	}
	if strings.Contains(string(unobserved), "inventory_observed_at") {
		t.Fatalf("unobserved heartbeat carried a zero observation timestamp: %s", unobserved)
	}
}

// The three questions asked of a phase, and the fact that they are three
// different questions. Idle is the phase that separates them: it is a valid
// thing for a connector to send, it is terminal, and it must never be written
// down as node health — a burst carrying "Idle" as its status would be
// describing a node that is Ready and answering.
func TestHeartbeatGPUTelemetryIsOmitEmptyBackwardsCompatible(t *testing.T) {
	var legacy Heartbeat
	if err := json.Unmarshal([]byte(`{"inventory_observed":false,"node_count":0,"burst_count":0,"pending_pods":0}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if len(legacy.GPUTelemetry) != 0 {
		t.Fatal("legacy heartbeat decoded with gpu telemetry")
	}
	raw, err := json.Marshal(Heartbeat{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "gpu_telemetry") {
		t.Fatalf("empty heartbeat carries gpu_telemetry: %s", raw)
	}
}

func TestValidGPUTelemetrySample(t *testing.T) {
	now := func() GPUTelemetrySample {
		return GPUTelemetrySample{
			BurstID: "burst_abc", NodeName: "ys-burst-abc",
			Utilization: 50, ObservedAt: mustParseTime("2026-08-22T12:00:00Z"),
		}
	}
	if !ValidGPUTelemetrySample(now()) {
		t.Fatal("valid sample rejected")
	}
	cases := []struct {
		name string
		mod  func(*GPUTelemetrySample)
	}{
		{"empty burst_id", func(s *GPUTelemetrySample) { s.BurstID = "" }},
		{"empty node_name", func(s *GPUTelemetrySample) { s.NodeName = "" }},
		{"negative", func(s *GPUTelemetrySample) { s.Utilization = -1 }},
		{"over 100", func(s *GPUTelemetrySample) { s.Utilization = 100.001 }},
		{"NaN", func(s *GPUTelemetrySample) { s.Utilization = math.NaN() }},
		{"Inf", func(s *GPUTelemetrySample) { s.Utilization = math.Inf(1) }},
		{"zero time", func(s *GPUTelemetrySample) { s.ObservedAt = time.Time{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := now()
			tc.mod(&s)
			if ValidGPUTelemetrySample(s) {
				t.Fatalf("should have rejected: %+v", s)
			}
		})
	}
	// 0% and 100% are valid
	for _, v := range []float64{0, 100} {
		s := now()
		s.Utilization = v
		if !ValidGPUTelemetrySample(s) {
			t.Fatalf("%f%% should be valid", v)
		}
	}
}

func mustParseTime(s string) (t time.Time) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestNodePhaseClassification(t *testing.T) {
	tests := []struct {
		phase                        string
		valid, terminal, persistable bool
	}{
		{phase: NodePhaseJoining, valid: true, terminal: false, persistable: true},
		{phase: NodePhaseReady, valid: true, terminal: false, persistable: true},
		{phase: NodePhaseNotReady, valid: true, terminal: false, persistable: true},
		{phase: NodePhaseRemoved, valid: true, terminal: true, persistable: true},
		{phase: NodePhaseIdle, valid: true, terminal: true, persistable: false},
		// A newer agent or a corrupt frame. Neither is a reason to guess.
		{phase: "Terminated", valid: false, terminal: false, persistable: false},
		{phase: "", valid: false, terminal: false, persistable: false},
		// Wire values are case-sensitive: a phase central half-recognises is one
		// it would act on for the wrong reason.
		{phase: "idle", valid: false, terminal: false, persistable: false},
	}
	for _, tc := range tests {
		t.Run(tc.phase, func(t *testing.T) {
			if got := ValidNodePhase(tc.phase); got != tc.valid {
				t.Errorf("ValidNodePhase(%q) = %v, want %v", tc.phase, got, tc.valid)
			}
			if got := TerminalNodePhase(tc.phase); got != tc.terminal {
				t.Errorf("TerminalNodePhase(%q) = %v, want %v", tc.phase, got, tc.terminal)
			}
			if got := PersistableNodePhase(tc.phase); got != tc.persistable {
				t.Errorf("PersistableNodePhase(%q) = %v, want %v", tc.phase, got, tc.persistable)
			}
		})
	}
}
