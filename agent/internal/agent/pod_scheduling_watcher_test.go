package agent

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

type recordingPodReporter struct {
	mu     sync.Mutex
	events []protocol.PodEvent
}

func (r *recordingPodReporter) ReportPodEvent(ev protocol.PodEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func scheduledPod(name, ns, wlID, nodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{labelWorkloadID: wlID},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.PodScheduled,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)),
			}},
		},
	}
}

func TestPodSchedulingWatcherReportsScheduled(t *testing.T) {
	reporter := &recordingPodReporter{}
	w := NewPodSchedulingWatcher(nil, reporter, nil)

	pod := scheduledPod("train-abc", "ml", "wl_abc", "ys-burst-deadbeef")
	w.observe(pod)

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 1 {
		t.Fatalf("events = %d, want 1", len(reporter.events))
	}
	ev := reporter.events[0]
	if ev.WorkloadID != "wl_abc" {
		t.Errorf("WorkloadID = %q, want %q", ev.WorkloadID, "wl_abc")
	}
	if ev.PodName != "train-abc" {
		t.Errorf("PodName = %q, want %q", ev.PodName, "train-abc")
	}
	if ev.NodeName != "ys-burst-deadbeef" {
		t.Errorf("NodeName = %q, want %q", ev.NodeName, "ys-burst-deadbeef")
	}
	if !ev.Scheduled {
		t.Error("Scheduled = false, want true")
	}
	if ev.ScheduledAt == nil {
		t.Fatal("ScheduledAt = nil")
	}
	if ev.SchedulingState != protocol.SchedulingStateScheduled || ev.SchedulingObservedAt == nil {
		t.Fatalf("scheduling identity = %q/%v, want Scheduled with source timestamp", ev.SchedulingState, ev.SchedulingObservedAt)
	}
}

func TestPodSchedulingWatcherDeduplicates(t *testing.T) {
	reporter := &recordingPodReporter{}
	w := NewPodSchedulingWatcher(nil, reporter, nil)

	pod := scheduledPod("train-abc", "ml", "wl_abc", "ys-burst-deadbeef")
	w.observe(pod)
	w.observe(pod)

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 1 {
		t.Fatalf("events = %d, want 1 (dedup)", len(reporter.events))
	}
}

func TestPodSchedulingWatcherReportsUnscheduledPod(t *testing.T) {
	reporter := &recordingPodReporter{}
	w := NewPodSchedulingWatcher(nil, reporter, nil)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pending-pod",
			Namespace: "ml",
			Labels:    map[string]string{labelWorkloadID: "wl_pending"},
		},
		Spec: corev1.PodSpec{},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: "Unschedulable",
			}},
		},
	}
	w.observe(pod)

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 1 {
		t.Fatalf("events = %d, want one Waiting observation", len(reporter.events))
	}
	ev := reporter.events[0]
	if ev.SchedulingState != protocol.SchedulingStateWaiting || ev.SchedulingReason != protocol.SchedulingReasonUnschedulable {
		t.Fatalf("scheduling observation = %q/%q, want Waiting/Unschedulable", ev.SchedulingState, ev.SchedulingReason)
	}
}

func TestPodSchedulingWatcherIgnoresNoNodeName(t *testing.T) {
	reporter := &recordingPodReporter{}
	w := NewPodSchedulingWatcher(nil, reporter, nil)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "no-node",
			Namespace: "ml",
			Labels:    map[string]string{labelWorkloadID: "wl_nonode"},
		},
		Spec: corev1.PodSpec{},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	w.observe(pod)

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 0 {
		t.Fatalf("events = %d, want 0 (no node name)", len(reporter.events))
	}
}

func TestPodSchedulingWatcherIgnoresNoWorkloadLabel(t *testing.T) {
	reporter := &recordingPodReporter{}
	w := NewPodSchedulingWatcher(nil, reporter, nil)

	pod := scheduledPod("unlabeled", "ml", "", "burst-node")
	w.observe(pod)

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 0 {
		t.Fatalf("events = %d, want 0 (no workload label)", len(reporter.events))
	}
}

func TestClientPodEventCaching(t *testing.T) {
	cli := &Client{log: slog.Default()}
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	cli.ReportPodEvent(protocol.PodEvent{
		WorkloadID:  "wl1",
		Scheduled:   true,
		PodName:     "pod-1",
		NodeName:    "burst-node",
		ScheduledAt: &at,
	})
	cli.mu.Lock()
	if len(cli.podEvents) != 1 {
		t.Fatalf("podEvents cache = %d, want 1", len(cli.podEvents))
	}
	ev := cli.podEvents["wl1"]
	cli.mu.Unlock()
	if ev.PodName != "pod-1" || ev.NodeName != "burst-node" {
		t.Fatalf("cached event = %+v, want pod-1/burst-node", ev)
	}
}

func TestClientPodEventFlushOnReconnect(t *testing.T) {
	cli := &Client{log: slog.Default()}
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	cli.podEvents = map[string]protocol.PodEvent{
		"wl1": {WorkloadID: "wl1", Scheduled: true, PodName: "pod-1", ScheduledAt: &at},
	}

	send := make(chan protocol.Envelope, 8)
	cli.mu.Lock()
	cli.send = send
	cli.mu.Unlock()

	cli.flushPodEvents()

	select {
	case env := <-send:
		if env.Type != protocol.TypePodEvent {
			t.Fatalf("flushed type = %q, want %q", env.Type, protocol.TypePodEvent)
		}
	default:
		t.Fatal("no pod event flushed on reconnect")
	}
}

func TestClientPodEventCacheBounded(t *testing.T) {
	cli := &Client{log: slog.Default()}
	for i := 0; i < maxPodEventCache+10; i++ {
		cli.ReportPodEvent(protocol.PodEvent{
			WorkloadID: "wl_" + time.Now().String() + string(rune(i)),
		})
	}
	cli.mu.Lock()
	n := len(cli.podEvents)
	cli.mu.Unlock()
	if n > maxPodEventCache {
		t.Fatalf("podEvents cache = %d, want <= %d", n, maxPodEventCache)
	}
}

func TestClientPodEventACKClearing(t *testing.T) {
	cli := &Client{log: slog.Default()}
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	cli.ReportPodEvent(protocol.PodEvent{
		WorkloadID: "wl1", Scheduled: true, PodName: "pod-1", ScheduledAt: &at,
	})
	cli.ReportPodEvent(protocol.PodEvent{
		WorkloadID: "wl2", Scheduled: true, PodName: "pod-2", ScheduledAt: &at,
	})

	// Versioned ACK: must carry the exact identity to clear the cached event.
	cli.resolvePodEvent(protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-1", ScheduledAt: &at})

	cli.mu.Lock()
	_, wl1Held := cli.podEvents["wl1"]
	_, wl2Held := cli.podEvents["wl2"]
	cli.mu.Unlock()
	if wl1Held {
		t.Fatal("ACK did not clear wl1 from the cache")
	}
	if !wl2Held {
		t.Fatal("ACK for wl1 cleared unrelated wl2")
	}
}

func TestClientPodEventACKMismatchedPodNameLeavesCache(t *testing.T) {
	cli := &Client{log: slog.Default()}
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	cli.ReportPodEvent(protocol.PodEvent{
		WorkloadID: "wl1", Scheduled: true, PodName: "pod-1", ScheduledAt: &at,
	})

	cli.resolvePodEvent(protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-2", ScheduledAt: &at})

	cli.mu.Lock()
	_, held := cli.podEvents["wl1"]
	cli.mu.Unlock()
	if !held {
		t.Fatal("ACK with wrong pod identity cleared a cached versioned event")
	}
}

func TestClientPodEventACKMismatchedScheduledAtLeavesCache(t *testing.T) {
	cli := &Client{log: slog.Default()}
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	other := time.Date(2026, 8, 21, 11, 0, 0, 0, time.UTC)
	cli.ReportPodEvent(protocol.PodEvent{
		WorkloadID: "wl1", Scheduled: true, PodName: "pod-1", ScheduledAt: &at,
	})

	cli.resolvePodEvent(protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-1", ScheduledAt: &other})

	cli.mu.Lock()
	_, held := cli.podEvents["wl1"]
	cli.mu.Unlock()
	if !held {
		t.Fatal("ACK with wrong ScheduledAt cleared a cached versioned event")
	}
}

func TestClientPodEventLegacyACKCannotClearVersionedCache(t *testing.T) {
	cli := &Client{log: slog.Default()}
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	cli.ReportPodEvent(protocol.PodEvent{
		WorkloadID: "wl1", Scheduled: true, PodName: "pod-1", ScheduledAt: &at,
	})

	// Legacy ACK: no PodName, no ScheduledAt. Must not clear a cached
	// versioned event — a legacy receipt is not authority over a fresher
	// observation the connector has already sent with full identity.
	cli.resolvePodEvent(protocol.PodEventAck{WorkloadID: "wl1"})

	cli.mu.Lock()
	_, held := cli.podEvents["wl1"]
	cli.mu.Unlock()
	if !held {
		t.Fatal("legacy ACK cleared a cached versioned event; would retire an unacknowledged observation")
	}
}

func TestClientPodEventLegacyACKClearsLegacyCache(t *testing.T) {
	// A legacy cached event carries no ScheduledAt. A legacy ACK clears it —
	// the retry contract for connectors that predate the versioned wire is
	// preserved.
	cli := &Client{log: slog.Default()}
	cli.ReportPodEvent(protocol.PodEvent{
		WorkloadID: "wl1", Scheduled: true, PodName: "pod-1",
	})

	cli.resolvePodEvent(protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-1"})

	cli.mu.Lock()
	_, held := cli.podEvents["wl1"]
	cli.mu.Unlock()
	if held {
		t.Fatal("legacy ACK with matching PodName did not clear the legacy cached event")
	}
}

func TestClientPodEventUTCNormalizationMatches(t *testing.T) {
	cli := &Client{log: slog.Default()}
	// Cache in a non-UTC location; ACK arrives in UTC. UTC-normalised
	// comparison must accept them as equal.
	loc := time.FixedZone("test-zone", 3600)
	cachedAt := time.Date(2026, 8, 21, 11, 0, 0, 0, loc)
	ackAt := cachedAt.UTC()
	cli.ReportPodEvent(protocol.PodEvent{
		WorkloadID: "wl1", Scheduled: true, PodName: "pod-1", ScheduledAt: &cachedAt,
	})

	cli.resolvePodEvent(protocol.PodEventAck{WorkloadID: "wl1", PodName: "pod-1", ScheduledAt: &ackAt})

	cli.mu.Lock()
	_, held := cli.podEvents["wl1"]
	cli.mu.Unlock()
	if held {
		t.Fatal("UTC-normalised ACK failed to clear cache with equivalent timestamp in another zone")
	}
}

func TestClientPodEventACKForUnknownIsIgnored(t *testing.T) {
	cli := &Client{log: slog.Default()}
	cli.resolvePodEvent(protocol.PodEventAck{WorkloadID: "wl_unknown"})
}

func TestClientPodEventACKEmptyIsIgnored(t *testing.T) {
	cli := &Client{log: slog.Default()}
	cli.resolvePodEvent(protocol.PodEventAck{})
}

func TestClientPodEventRetainedAfterLostACK(t *testing.T) {
	cli := &Client{log: slog.Default()}
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	cli.ReportPodEvent(protocol.PodEvent{
		WorkloadID: "wl1", Scheduled: true, PodName: "pod-1", ScheduledAt: &at,
	})

	send := make(chan protocol.Envelope, 8)
	cli.mu.Lock()
	cli.send = send
	cli.mu.Unlock()

	cli.flushPodEvents()

	select {
	case env := <-send:
		if env.Type != protocol.TypePodEvent {
			t.Fatalf("flushed type = %q, want %q", env.Type, protocol.TypePodEvent)
		}
	default:
		t.Fatal("no pod event flushed — retained events must re-send until ACK")
	}

	cli.mu.Lock()
	if len(cli.podEvents) != 1 {
		t.Fatalf("cache = %d, want 1 — event must remain cached until ACK", len(cli.podEvents))
	}
	cli.mu.Unlock()
}
