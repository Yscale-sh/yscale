package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

type enqueueCall struct {
	customerID string
	clusterID  string
	force      bool
	// removal marks a call that came in through EnqueueClusterRemoval — the
	// lifecycle delete seam, which no agent event may ever reach.
	removal bool
	// shared marks a call that came in through EnqueueSharedTailnet, the
	// cross-tenant seam that names no customer and no cluster.
	shared bool
}

// recordingReconciler is a queue, not a direct push: it only records Enqueue
// calls and NEVER calls EnsurePolicy. Driving the handler through it proves the
// read goroutine enqueues and returns without an inline PUT (#7).
type recordingReconciler struct {
	mu    sync.Mutex
	calls []enqueueCall
}

func (r *recordingReconciler) Enqueue(customerID, clusterID string, force bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, enqueueCall{customerID: customerID, clusterID: clusterID, force: force})
}

func (r *recordingReconciler) EnqueueClusterRemoval(customerID, clusterID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, enqueueCall{customerID: customerID, clusterID: clusterID, removal: true})
}

func (r *recordingReconciler) EnqueueSharedTailnet(force bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, enqueueCall{shared: true, force: force})
}

func (r *recordingReconciler) snapshot() []enqueueCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]enqueueCall(nil), r.calls...)
}

// reset drops what the setup enqueued, so a test asserting on what one boundary
// asked for is not reading the connect-time re-assert that seeded it.
func (r *recordingReconciler) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

func mustEnvelope(t *testing.T, typ protocol.MessageType, body any) protocol.Envelope {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       typ,
		Timestamp:  time.Now().UTC(),
		Body:       raw,
	}
}

func TestAgentStreamClusterRoutesEnqueuesNoInlinePush(t *testing.T) {
	s := state.New() // seeds cust_test
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	rec := &recordingReconciler{}
	stream := NewAgentStream(s, log, rec)

	srv := httptest.NewServer(Auth(s, stream))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	hdr := http.Header{"Authorization": {"Bearer " + state.DefaultDevToken}}
	conn, _, err := websocket.DefaultDialer.Dial(url, hdr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Handshake.
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeHello, protocol.Hello{ClusterID: "c1"})); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	// Connect-time force enqueue should land, carrying the Hello cluster: the
	// reconciler converges THAT gateway, and must never have to guess it.
	waitForCalls(t, rec, 1)
	if got := rec.snapshot()[0]; got.customerID != "cust_test" || got.clusterID != "c1" || !got.force {
		t.Fatalf("connect enqueue = %+v, want {cust_test c1 true}", got)
	}

	// Report a valid route set: handler validates, stores, enqueues (force=false).
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
		Routes: []string{"10.42.0.0/16", "10.96.0.0/12"},
	})); err != nil {
		t.Fatalf("write routes: %v", err)
	}
	waitForCalls(t, rec, 2)
	if got := rec.snapshot()[1]; got.customerID != "cust_test" || got.clusterID != "c1" || got.force {
		t.Fatalf("report enqueue = %+v, want {cust_test c1 false}", got)
	}

	// Stored on the customer (tenancy from the bearer token).
	cust, err := s.CustomerByID("cust_test")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if len(cust.GatewayRoutes) != 2 {
		t.Fatalf("stored GatewayRoutes = %v, want 2 entries", cust.GatewayRoutes)
	}
	// Scoped to the cluster the CONNECTION was admitted under, never to a
	// cluster named in the message body: the report can only ever replace this
	// gateway's own set.
	if got := cust.GatewayRoutesByCluster["c1"]; len(got) != 2 {
		t.Fatalf("stored GatewayRoutesByCluster[c1] = %v, want the reported 2 entries", got)
	}
	if len(cust.GatewayRoutesByCluster) != 1 {
		t.Fatalf("route intent written for %d clusters, want only c1", len(cust.GatewayRoutesByCluster))
	}

	// An invalid report (public CIDR) is rejected fail-closed: no new enqueue,
	// prior stored set intact.
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
		Routes: []string{"8.8.8.0/24"},
	})); err != nil {
		t.Fatalf("write bad routes: %v", err)
	}
	// Close the socket behind the bad report. The read goroutine handles frames
	// IN ORDER and only reaches the close after the frame ahead of it, so an
	// agent that is gone from the store is proof the bad report was fully
	// processed — the assertion below is then a fact rather than a sleep long
	// enough to hope. Disconnect enqueues nothing, so the count is untouched.
	conn.Close()
	waitFor(t, "the stream to drain the invalid report and disconnect", func() bool {
		_, err := s.AgentForCluster("cust_test", "c1")
		return errors.Is(err, state.ErrNotFound)
	})

	if n := len(rec.snapshot()); n != 2 {
		t.Fatalf("enqueue count after invalid report = %d, want 2", n)
	}
	cust, _ = s.CustomerByID("cust_test")
	if len(cust.GatewayRoutes) != 2 {
		t.Fatalf("GatewayRoutes mutated by invalid report = %v", cust.GatewayRoutes)
	}
}

func waitForCalls(t *testing.T, rec *recordingReconciler, n int) {
	t.Helper()
	waitFor(t, fmt.Sprintf("%d enqueue calls", n), func() bool { return len(rec.snapshot()) >= n })
}
