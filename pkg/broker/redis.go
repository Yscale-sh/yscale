package broker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis is a Broker backed by Redis Streams + consumer groups. Durable
// (messages survive restarts) and multi-process (separate workers can share a
// group), which is what makes it the production seam between decoupled
// components. Implements the same at-least-once + redelivery contract as Memory.
type Redis struct {
	rdb        *redis.Client
	retryAfter time.Duration // min idle before an un-Ack'd message is reclaimed
	pollBlock  time.Duration // how long Consume blocks waiting for a new message
}

const payloadField = "p"

// StreamStats is the bounded operational view exported for a Redis stream.
// It intentionally contains only aggregate values: stream and consumer-group
// names are fixed by the caller, so no tenant or message IDs reach metrics.
type StreamStats struct {
	Length      int64
	Pending     int64
	MemoryBytes int64
}

// AckedHistoryTrimmer is an optional Broker capability. Call it only after a
// successful Ack: retention housekeeping must not weaken delivery semantics.
type AckedHistoryTrimmer interface {
	TrimAckedHistory(context.Context, string) error
}

// NewRedis connects to Redis at url (redis://[:pass@]host:port[/db]). retryAfter
// is how long a delivered-but-un-Ack'd message must be idle before it is
// reclaimed and redelivered (production: a few seconds to minutes; tests: small).
func NewRedis(ctx context.Context, url string, retryAfter time.Duration) (*Redis, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("broker: parse redis url: %w", err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("broker: ping redis: %w", err)
	}
	return &Redis{rdb: rdb, retryAfter: retryAfter, pollBlock: 2 * time.Second}, nil
}

func (b *Redis) Publish(ctx context.Context, stream string, payload []byte) error {
	return b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{payloadField: payload},
	}).Err()
}

// ensureGroup creates the consumer group starting at "0" so messages published
// before any consumer existed are still delivered (enqueue-before-worker-start
// is normal for a job queue). A pre-existing group surfaces BUSYGROUP, which is
// the expected idempotent no-op.
func (b *Redis) ensureGroup(ctx context.Context, stream, group string) error {
	err := b.rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("broker: create group %s/%s: %w", stream, group, err)
	}
	return nil
}

func (b *Redis) Consume(ctx context.Context, stream, group, consumer string) (*Message, error) {
	if err := b.ensureGroup(ctx, stream, group); err != nil {
		return nil, err
	}

	// 1. Reclaim the oldest message that has been pending (delivered, not
	//    Ack'd) longer than retryAfter — a failed job or a crashed worker's
	//    in-flight message. This is the durable-retry / restart-survival path.
	msgs, _, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  b.retryAfter,
		Start:    "0-0",
		Count:    1,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("broker: autoclaim %s/%s: %w", stream, group, err)
	}
	if len(msgs) > 0 {
		return b.toMessage(ctx, stream, group, msgs[0]), nil
	}

	// 2. Otherwise read a brand-new message, blocking up to pollBlock.
	res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    b.pollBlock,
	}).Result()
	if err == redis.Nil {
		return nil, nil // nothing ready in the poll window
	}
	if err != nil {
		// A blocking read cancelled via ctx is not an error worth surfacing.
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, fmt.Errorf("broker: readgroup %s/%s: %w", stream, group, err)
	}
	if len(res) == 0 || len(res[0].Messages) == 0 {
		return nil, nil
	}
	return b.toMessage(ctx, stream, group, res[0].Messages[0]), nil
}

func (b *Redis) Ack(ctx context.Context, stream, group, msgID string) error {
	return b.rdb.XAck(ctx, stream, group, msgID).Err()
}

// TrimAckedHistory removes only entries older than every registered consumer
// group's safe replay boundary. Pending work pins the boundary so XAUTOCLAIM
// can recover it; a lagging group pins it at its last-delivered ID. These IDs
// only advance, so concurrent reads and acknowledgements can make this trim
// conservative but cannot make it delete recoverable work.
//
// Consumer groups are forward consumers, not archival readers. A group added
// after compaction begins can consume retained/future entries only.
func (b *Redis) TrimAckedHistory(ctx context.Context, stream string) error {
	groups, err := b.rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return fmt.Errorf("broker: inspect groups for %s: %w", stream, err)
	}
	minID := ""
	for _, group := range groups {
		boundary := group.LastDeliveredID
		if group.Pending > 0 {
			pending, err := b.rdb.XPending(ctx, stream, group.Name).Result()
			if err != nil {
				return fmt.Errorf("broker: inspect pending %s/%s: %w", stream, group.Name, err)
			}
			if pending.Count > 0 {
				boundary = pending.Lower
			}
		}
		// A group that has never delivered anything must retain the complete
		// stream. This is the only safe interpretation of its 0-0 cursor.
		if boundary == "" || boundary == "0-0" {
			return nil
		}
		if minID == "" {
			minID = boundary
			continue
		}
		less, err := streamIDLess(boundary, minID)
		if err != nil {
			return fmt.Errorf("broker: compare retention boundary for %s: %w", stream, err)
		}
		if less {
			minID = boundary
		}
	}
	if minID == "" {
		return nil
	}
	if err := b.rdb.XTrimMinID(ctx, stream, minID).Err(); err != nil {
		return fmt.Errorf("broker: trim acknowledged history for %s: %w", stream, err)
	}
	return nil
}

func streamIDLess(a, b string) (bool, error) {
	parse := func(id string) ([2]uint64, error) {
		parts := strings.SplitN(id, "-", 2)
		if len(parts) != 2 {
			return [2]uint64{}, fmt.Errorf("invalid stream id %q", id)
		}
		ms, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return [2]uint64{}, fmt.Errorf("invalid stream id %q: %w", id, err)
		}
		seq, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return [2]uint64{}, fmt.Errorf("invalid stream id %q: %w", id, err)
		}
		return [2]uint64{ms, seq}, nil
	}
	left, err := parse(a)
	if err != nil {
		return false, err
	}
	right, err := parse(b)
	if err != nil {
		return false, err
	}
	return left[0] < right[0] || (left[0] == right[0] && left[1] < right[1]), nil
}

// StreamStats reads the values needed to size and alert on a durable stream.
// A missing consumer group is an error rather than a healthy zero: without the
// group, pending work cannot be observed and a zero would hide that failure.
func (b *Redis) StreamStats(ctx context.Context, stream, group string) (StreamStats, error) {
	pipe := b.rdb.Pipeline()
	length := pipe.XLen(ctx, stream)
	pending := pipe.XPending(ctx, stream, group)
	memory := pipe.MemoryUsage(ctx, stream)
	if _, err := pipe.Exec(ctx); err != nil {
		return StreamStats{}, fmt.Errorf("broker: stream stats %s/%s: %w", stream, group, err)
	}
	return StreamStats{
		Length:      length.Val(),
		Pending:     pending.Val().Count,
		MemoryBytes: memory.Val(),
	}, nil
}

func (b *Redis) Close() error { return b.rdb.Close() }

// toMessage decodes a Redis stream entry and fills in the accurate delivery
// count from the pending-entries list (XAutoClaim/XReadGroup don't return it).
// A lookup miss falls back to 1, which is correct for a first delivery.
func (b *Redis) toMessage(ctx context.Context, stream, group string, m redis.XMessage) *Message {
	var payload []byte
	switch v := m.Values[payloadField].(type) {
	case string:
		payload = []byte(v)
	case []byte:
		payload = v
	}
	deliveries := int64(1)
	if ext, err := b.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream, Group: group, Start: m.ID, End: m.ID, Count: 1,
	}).Result(); err == nil && len(ext) == 1 {
		deliveries = ext[0].RetryCount
	}
	return &Message{ID: m.ID, Payload: payload, Deliveries: deliveries}
}
