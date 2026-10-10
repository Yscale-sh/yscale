// Package broker is a durable, at-least-once message stream — the seam along
// which the control plane decouples into separately-deployable pieces (e.g. a
// teardown enqueuer in central and a teardown worker that can run as its own
// process; OSS core vs enterprise add-ons). Components depend on the Broker
// interface, never a concrete implementation.
//
// Two implementations:
//   - Memory (memory.go): in-process, for tests and OSS-local single-binary runs.
//   - Redis Streams (redis.go): durable + multi-process, for production.
//
// Delivery semantics are at-least-once with redelivery: a consumed message that
// is not Ack'd within the retry window is delivered again (so a worker that
// crashes or a job that fails is retried, surviving a restart). Consumers must
// therefore be idempotent — which the teardown path already is (DeleteNode is a
// no-op when the node is already gone).
package broker

import "context"

// Message is one delivered stream item. ID is the broker-assigned id used to
// Ack; Payload is the opaque job body (typically JSON). Deliveries is the
// number of times this message has been delivered (1 on first delivery), so a
// consumer can drop a poison job after N attempts instead of retrying forever.
type Message struct {
	ID         string
	Payload    []byte
	Deliveries int64
}

// Broker is the durable stream contract.
type Broker interface {
	// Publish appends payload to the named stream.
	Publish(ctx context.Context, stream string, payload []byte) error

	// Consume returns the next message for (stream, group, consumer): either a
	// new message, or one previously delivered to the group but not Ack'd
	// within the retry window (redelivery). Returns (nil, nil) when nothing is
	// ready within the poll window. The consumer group is created on first use.
	Consume(ctx context.Context, stream, group, consumer string) (*Message, error)

	// Ack marks a delivered message done so it is not redelivered.
	Ack(ctx context.Context, stream, group, msgID string) error

	// Close releases resources (connections, goroutines).
	Close() error
}
