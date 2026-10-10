package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/broker"
)

type deadLetterTestBroker struct {
	publishedStream string
	published       []byte
	publishErr      error
	ackedID         string
}

func (b *deadLetterTestBroker) Publish(_ context.Context, stream string, payload []byte) error {
	if b.publishErr != nil {
		return b.publishErr
	}
	b.publishedStream = stream
	b.published = append([]byte(nil), payload...)
	return nil
}

func (*deadLetterTestBroker) Consume(context.Context, string, string, string) (*broker.Message, error) {
	return nil, nil
}

func (b *deadLetterTestBroker) Ack(_ context.Context, _, _ string, msgID string) error {
	b.ackedID = msgID
	return nil
}

func (*deadLetterTestBroker) Close() error { return nil }

type deadLetterFailingReaper struct{ err error }

func (r deadLetterFailingReaper) Teardown(context.Context, *state.Burst) error { return r.err }

func deadLetterTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func deadLetterTestMessage(t *testing.T, deliveries int64) *broker.Message {
	t.Helper()
	payload, err := json.Marshal(teardownJob{
		Burst: state.Burst{
			ID:         "burst_deadletter_test",
			CustomerID: "cust_test",
			Backend:    "linode",
			BackendID:  "123456",
		},
		Reason: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return &broker.Message{ID: "42-0", Payload: payload, Deliveries: deliveries}
}

func TestTeardownWorkerPersistsDeadLetterBeforeAck(t *testing.T) {
	b := &deadLetterTestBroker{}
	providerErr := errors.New("provider delete refused")
	w := &TeardownWorker{
		Broker: b,
		Reaper: deadLetterFailingReaper{err: providerErr},
		Log:    deadLetterTestLogger(),
	}

	w.process(context.Background(), deadLetterTestMessage(t, teardownMaxAttempts))

	if b.publishedStream != teardownDeadLetterStream {
		t.Fatalf("published stream = %q, want %q", b.publishedStream, teardownDeadLetterStream)
	}
	if b.ackedID != "42-0" {
		t.Fatalf("acked message = %q, want source message", b.ackedID)
	}
	var got teardownDeadLetter
	if err := json.Unmarshal(b.published, &got); err != nil {
		t.Fatalf("decode dead letter: %v", err)
	}
	if got.SourceMessageID != "42-0" || got.Job.Burst.ID != "burst_deadletter_test" {
		t.Fatalf("dead letter lost recovery identity: %+v", got)
	}
	if got.Deliveries != teardownMaxAttempts || got.Error != providerErr.Error() || got.FailedAt.IsZero() {
		t.Fatalf("dead letter missing failure evidence: %+v", got)
	}
}

func TestTeardownWorkerLeavesSourcePendingWhenDeadLetterPublishFails(t *testing.T) {
	b := &deadLetterTestBroker{publishErr: errors.New("redis unavailable")}
	w := &TeardownWorker{
		Broker: b,
		Reaper: deadLetterFailingReaper{err: errors.New("provider delete refused")},
		Log:    deadLetterTestLogger(),
	}

	w.process(context.Background(), deadLetterTestMessage(t, teardownMaxAttempts))

	if b.ackedID != "" {
		t.Fatalf("source message was acked despite dead-letter failure: %q", b.ackedID)
	}
}
