package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

type logResultHandler struct{}

func (logResultHandler) OnBurstAnnounce(context.Context, protocol.BurstAnnounce) error { return nil }
func (logResultHandler) OnCreateJob(context.Context, protocol.CreateJob) error         { return nil }
func (logResultHandler) OnDeleteJob(context.Context, protocol.DeleteJob) error         { return nil }
func (logResultHandler) OnDrainNode(context.Context, protocol.DrainNode) error         { return nil }
func (logResultHandler) OnPrepareIdleTeardown(context.Context, protocol.PrepareIdleTeardown) (protocol.IdleTeardownPreflight, error) {
	return protocol.IdleTeardownPreflight{}, nil
}
func (logResultHandler) OnReleaseIdleTeardown(context.Context, protocol.ReleaseIdleTeardown) error {
	return nil
}
func (logResultHandler) OnFetchWorkloadLogs(_ context.Context, cmd protocol.FetchWorkloadLogs) (protocol.WorkloadLogs, error) {
	return protocol.WorkloadLogs{
		ObservedAt: time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC),
		Streams:    []protocol.WorkloadLogStream{{Pod: "job-abc", Container: "main", Output: cmd.WorkloadID + "\n"}},
	}, nil
}
func (logResultHandler) OnSyncRuntimeBindings(context.Context, protocol.SyncRuntimeBindings) error {
	return nil
}

func TestClientReturnsStructuredWorkloadLogResultInAck(t *testing.T) {
	ackCh := make(chan protocol.CommandAck, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var hello protocol.Envelope
		if err := conn.ReadJSON(&hello); err != nil {
			return
		}
		body, _ := json.Marshal(protocol.FetchWorkloadLogs{WorkloadID: "wl_logs", Namespace: "jobs", TailLines: 20})
		if err := conn.WriteJSON(protocol.Envelope{
			APIVersion: protocol.APIVersion, Type: protocol.TypeFetchWorkloadLogs,
			ID: "cmd_logs", Timestamp: time.Now().UTC(), Body: body,
		}); err != nil {
			return
		}
		for {
			var ackEnv protocol.Envelope
			if err := conn.ReadJSON(&ackEnv); err != nil {
				return
			}
			if ackEnv.Type != protocol.TypeCommandAck {
				continue
			}
			var ack protocol.CommandAck
			if json.Unmarshal(ackEnv.Body, &ack) == nil {
				ackCh <- ack
			}
			return
		}
	}))
	defer server.Close()

	client, err := New(Config{
		Endpoint: server.URL, Token: "cluster-token", ClusterID: "cluster-1",
		HeartbeatInterval: time.Minute,
	}, logResultHandler{}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.runOnce(ctx) }()

	select {
	case ack := <-ackCh:
		if !ack.Success || ack.CommandID != "cmd_logs" || len(ack.Result) == 0 {
			t.Fatalf("ack = %+v", ack)
		}
		var result protocol.WorkloadLogs
		if err := json.Unmarshal(ack.Result, &result); err != nil || len(result.Streams) != 1 || result.Streams[0].Output != "wl_logs\n" {
			t.Fatalf("result = %+v err=%v", result, err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for log acknowledgement")
	}
	cancel()
	<-done
}
