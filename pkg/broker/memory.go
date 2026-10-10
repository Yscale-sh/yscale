package broker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Memory is an in-process Broker for tests and OSS-local single-binary runs.
// It mirrors the Redis Streams contract: per-stream ordered messages, per-group
// consumption offsets, and redelivery of un-Ack'd messages once they are older
// than retryAfter. Safe for concurrent use.
type Memory struct {
	mu         sync.Mutex
	streams    map[string]*memStream
	retryAfter time.Duration
	now        func() time.Time
}

// NewMemory builds an in-memory broker. retryAfter is how long a delivered but
// un-Ack'd message waits before it is eligible for redelivery (0 = redeliver on
// the next Consume — useful in tests; production uses a few seconds).
func NewMemory(retryAfter time.Duration) *Memory {
	return &Memory{
		streams:    map[string]*memStream{},
		retryAfter: retryAfter,
		now:        time.Now,
	}
}

type memMsg struct {
	id      string
	seq     int64
	payload []byte
}

type memPending struct {
	msg         *memMsg
	deliveredAt time.Time
	deliveries  int64
}

type memGroup struct {
	nextNew int                    // index into stream.msgs of the next never-delivered message
	pending map[string]*memPending // delivered-not-acked, by msg id
}

type memStream struct {
	seq    int64
	msgs   []*memMsg
	groups map[string]*memGroup
}

func (s *memStream) group(name string) *memGroup {
	g := s.groups[name]
	if g == nil {
		g = &memGroup{pending: map[string]*memPending{}}
		s.groups[name] = g
	}
	return g
}

func (b *Memory) Publish(_ context.Context, stream string, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.streams[stream]
	if st == nil {
		st = &memStream{groups: map[string]*memGroup{}}
		b.streams[stream] = st
	}
	st.seq++
	// Copy the payload so a caller mutating its slice can't corrupt the queued message.
	cp := append([]byte(nil), payload...)
	st.msgs = append(st.msgs, &memMsg{id: fmt.Sprintf("%d-0", st.seq), seq: st.seq, payload: cp})
	return nil
}

func (b *Memory) Consume(_ context.Context, stream, group, _ string) (*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.streams[stream]
	if st == nil {
		return nil, nil
	}
	g := st.group(group)
	now := b.now()

	// 1. Redeliver the oldest un-Ack'd message past the retry window.
	var oldest *memPending
	for _, p := range g.pending {
		if now.Sub(p.deliveredAt) >= b.retryAfter {
			if oldest == nil || p.msg.seq < oldest.msg.seq {
				oldest = p
			}
		}
	}
	if oldest != nil {
		oldest.deliveredAt = now
		oldest.deliveries++
		return &Message{ID: oldest.msg.id, Payload: oldest.msg.payload, Deliveries: oldest.deliveries}, nil
	}

	// 2. Deliver the next never-delivered message.
	if g.nextNew < len(st.msgs) {
		m := st.msgs[g.nextNew]
		g.nextNew++
		g.pending[m.id] = &memPending{msg: m, deliveredAt: now, deliveries: 1}
		return &Message{ID: m.id, Payload: m.payload, Deliveries: 1}, nil
	}
	return nil, nil
}

func (b *Memory) Ack(_ context.Context, stream, group, msgID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st := b.streams[stream]; st != nil {
		delete(st.group(group).pending, msgID)
	}
	return nil
}

func (b *Memory) Close() error { return nil }
