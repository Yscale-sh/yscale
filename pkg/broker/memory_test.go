package broker

import (
	"context"
	"testing"
	"time"
)

func TestMemoryPublishConsumeAck(t *testing.T) {
	ctx := context.Background()
	b := NewMemory(time.Hour) // no auto-redelivery; exercise new-message delivery
	const stream, group, consumer = "s", "g", "c"

	mustPublish(t, b, stream, "A")
	mustPublish(t, b, stream, "B")

	m1 := mustConsume(t, b, stream, group, consumer)
	if string(m1.Payload) != "A" || m1.Deliveries != 1 {
		t.Fatalf("first consume = %q d=%d, want A d=1", m1.Payload, m1.Deliveries)
	}
	if err := b.Ack(ctx, stream, group, m1.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	m2 := mustConsume(t, b, stream, group, consumer)
	if string(m2.Payload) != "B" || m2.Deliveries != 1 {
		t.Fatalf("second consume = %q d=%d, want B d=1", m2.Payload, m2.Deliveries)
	}
	mustAck(t, b, stream, group, m2.ID)

	// Nothing left.
	if m, _ := b.Consume(ctx, stream, group, consumer); m != nil {
		t.Fatalf("expected empty, got %q", m.Payload)
	}
}

// An un-Ack'd message must be redelivered (with an incremented delivery count)
// — this is the durable-retry property the teardown worker relies on. Acking
// then stops redelivery.
func TestMemoryRedeliversUnacked(t *testing.T) {
	ctx := context.Background()
	b := NewMemory(0) // 0 = eligible for redelivery immediately
	const stream, group, consumer = "s", "g", "c"

	mustPublish(t, b, stream, "job")

	m1 := mustConsume(t, b, stream, group, consumer)
	if m1.Deliveries != 1 {
		t.Fatalf("first delivery count = %d, want 1", m1.Deliveries)
	}
	// Simulate processing failure: do NOT ack.
	m2 := mustConsume(t, b, stream, group, consumer)
	if string(m2.Payload) != "job" || m2.Deliveries != 2 {
		t.Fatalf("redelivery = %q d=%d, want job d=2", m2.Payload, m2.Deliveries)
	}
	// Now ack — no further redelivery.
	mustAck(t, b, stream, group, m2.ID)
	if m, _ := b.Consume(ctx, stream, group, consumer); m != nil {
		t.Fatalf("acked message redelivered: %q", m.Payload)
	}
}

// Two groups consume the same stream independently (the fan-out the OSS/
// enterprise seam needs: e.g. a teardown worker and an audit consumer).
func TestMemoryIndependentGroups(t *testing.T) {
	b := NewMemory(time.Hour)
	const stream = "s"
	mustPublish(t, b, stream, "A")

	g1 := mustConsume(t, b, stream, "g1", "c")
	g2 := mustConsume(t, b, stream, "g2", "c")
	if string(g1.Payload) != "A" || string(g2.Payload) != "A" {
		t.Fatalf("each group should see A independently: g1=%q g2=%q", g1.Payload, g2.Payload)
	}
}

// Publish copies the payload so a caller reusing its buffer can't corrupt a
// queued message.
func TestMemoryPayloadIsolation(t *testing.T) {
	b := NewMemory(time.Hour)
	buf := []byte("orig")
	if err := b.Publish(context.Background(), "s", buf); err != nil {
		t.Fatalf("publish: %v", err)
	}
	copy(buf, "XXXX")
	m := mustConsume(t, b, "s", "g", "c")
	if string(m.Payload) != "orig" {
		t.Fatalf("payload not isolated from caller buffer: got %q", m.Payload)
	}
}

func mustPublish(t *testing.T, b Broker, stream, payload string) {
	t.Helper()
	if err := b.Publish(context.Background(), stream, []byte(payload)); err != nil {
		t.Fatalf("publish %q: %v", payload, err)
	}
}

func mustConsume(t *testing.T, b Broker, stream, group, consumer string) *Message {
	t.Helper()
	m, err := b.Consume(context.Background(), stream, group, consumer)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if m == nil {
		t.Fatal("consume returned nil, expected a message")
	}
	return m
}

func mustAck(t *testing.T, b Broker, stream, group, id string) {
	t.Helper()
	if err := b.Ack(context.Background(), stream, group, id); err != nil {
		t.Fatalf("ack: %v", err)
	}
}
