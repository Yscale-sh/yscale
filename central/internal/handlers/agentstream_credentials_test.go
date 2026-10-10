package handlers

import (
	"context"
	"errors"
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

func credentialStreamServer(t *testing.T, store *state.Store, poll time.Duration, opts ...AgentStreamOption) *httptest.Server {
	srv, _ := credentialStreamServerWithCompletion(t, store, poll, opts...)
	return srv
}

func credentialStreamServerWithCompletion(t *testing.T, store *state.Store, poll time.Duration, opts ...AgentStreamOption) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	opts = append(opts, func(h *AgentStream) { h.credentialPoll = poll })
	stream := NewAgentStream(store, quietLog(), nil, opts...)
	var active sync.WaitGroup
	completed := make(chan struct{}, 32)
	srv := httptest.NewServer(ConnectorAuth(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active.Add(1)
		defer active.Done()
		stream.ServeHTTP(w, r)
		select {
		case completed <- struct{}{}:
		default:
		}
	})))
	t.Cleanup(func() {
		srv.Close()
		active.Wait()
		stream.leaseMu.Lock()
		defer stream.leaseMu.Unlock()
		for key, timer := range stream.leases {
			timer.Stop()
			delete(stream.leases, key)
		}
	})
	return srv, completed
}

func expectCredentialClose(t *testing.T, conn *websocket.Conn, code int) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var env protocol.Envelope
	err := conn.ReadJSON(&env)
	if !websocket.IsCloseError(err, code) {
		t.Fatalf("stream received type=%q err=%v; want close %d without message delivery", env.Type, err, code)
	}
}

func TestAgentStreamTenantRotationRefusesQueuedCommand(t *testing.T) {
	s := state.New()
	// A long idle interval proves the queued envelope itself revalidates.
	srv := credentialStreamServer(t, s, time.Hour)
	conn, err := dialAgent(t, srv, state.DefaultDevToken, "cl-stream-rotation")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitFor(t, "stream registration", func() bool {
		_, err := s.AgentForCluster(state.DevCustomerID, "cl-stream-rotation")
		return err == nil
	})
	agent, _ := s.AgentForCluster(state.DevCustomerID, "cl-stream-rotation")
	if _, err := s.RotateCustomerCredential(state.DevCustomerID, state.OperatorActor()); err != nil {
		t.Fatal(err)
	}
	if err := agent.Enqueue(mustEnvelope(t, protocol.TypeDrainNode, protocol.DrainNode{NodeName: "synthetic-never-executed-node"})); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var env protocol.Envelope
	err = conn.ReadJSON(&env)
	if !websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
		t.Fatalf("old credential stream received type=%q err=%v; want policy close without command delivery", env.Type, err)
	}
}

func TestAgentStreamTenantRotationRefusesInboundRoutes(t *testing.T) {
	s := state.New()
	srv := credentialStreamServer(t, s, time.Hour)
	conn, err := dialAgent(t, srv, state.DefaultDevToken, "cl-inbound-rotation")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitFor(t, "stream registration", func() bool {
		_, err := s.AgentForCluster(state.DevCustomerID, "cl-inbound-rotation")
		return err == nil
	})
	if _, err := s.RotateCustomerCredential(state.DevCustomerID, state.OperatorActor()); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{Routes: []string{"10.84.0.0/24"}})); err != nil {
		t.Fatal(err)
	}
	expectCredentialClose(t, conn, websocket.ClosePolicyViolation)
	intent, err := s.CustomerGatewayRouteIntent(state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	if len(intent.Union) != 0 {
		t.Error("revoked stream changed gateway policy")
	}
	if _, err := s.AgentForCluster(state.DevCustomerID, "cl-inbound-rotation"); err == nil {
		t.Error("refused stream remains routable")
	}
}

func TestAgentStreamTenantRevocationClosesIdleSocket(t *testing.T) {
	s := state.New()
	// Exercise the production default, not a shortened test ticker.
	srv := credentialStreamServer(t, s, 0)
	conn, err := dialAgent(t, srv, state.DefaultDevToken, "cl-idle-revoke")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitFor(t, "stream registration", func() bool { _, err := s.AgentForCluster(state.DevCustomerID, "cl-idle-revoke"); return err == nil })
	if err := s.RevokeCustomer(state.DevCustomerID); err != nil {
		t.Fatal(err)
	}
	expectCredentialClose(t, conn, websocket.ClosePolicyViolation)
	if _, err := s.AgentForCluster(state.DevCustomerID, "cl-idle-revoke"); err == nil {
		t.Error("idle revoked stream remains routable")
	}
}

func TestAgentStreamTenantRotationRefusesPongLiveness(t *testing.T) {
	s := state.New()
	srv, completed := credentialStreamServerWithCompletion(t, s, time.Hour)
	conn, err := dialAgent(t, srv, state.DefaultDevToken, "cl-pong-rotation")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitFor(t, "stream registration", func() bool {
		_, err := s.AgentForCluster(state.DevCustomerID, "cl-pong-rotation")
		return err == nil
	})
	agent, _ := s.AgentForCluster(state.DevCustomerID, "cl-pong-rotation")
	lastSeen := agent.SeenAt()
	if _, err := s.RotateCustomerCredential(state.DevCustomerID, state.OperatorActor()); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	expectCredentialClose(t, conn, websocket.ClosePolicyViolation)
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("revoked pong did not complete stream cleanup")
	}
	if !agent.SeenAt().Equal(lastSeen) {
		t.Error("revoked pong refreshed agent liveness")
	}
}

func TestAgentStreamWriterFailureCancelsPendingWork(t *testing.T) {
	s := state.New()
	h := NewAgentStream(s, quietLog(), nil)
	result := make(chan error, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := h.upgrader.Upgrade(w, r, nil)
		if err != nil {
			result <- err
			return
		}
		defer conn.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		credential := &agentStreamCredential{token: state.DefaultDevToken, customerID: state.DevCustomerID, cancel: cancel}
		agent := &state.Agent{CustomerID: state.DevCustomerID, ClusterID: "cl-write-failure", Send: make(chan protocol.Envelope, 1)}
		agent.Send <- protocol.Envelope{APIVersion: protocol.APIVersion, Type: protocol.TypeDrainNode, ID: "synthetic-write-failure"}
		// Force an actual socket write error, independently of credential
		// revocation or reader exit. A reader may be waiting on ledger I/O.
		_ = conn.Close()
		h.writePump(ctx, conn, agent, make(chan struct{}), credential)
		result <- ctx.Err()
	}))
	defer srv.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("writer exit left pending stream work uncancelled: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not exit after socket failure")
	}
}

func TestAgentStreamCredentialCannotChangePrincipalKindOrBinding(t *testing.T) {
	t.Run("scoped-never-falls-back-to-tenant", func(t *testing.T) {
		fx := registryStore(t)
		credential := &agentStreamCredential{token: fx.token, customerID: "cust_a", clusterID: "cl-a"}
		if _, err := fx.store.DeleteTenantCluster("cust_a", fx.aliceID, "cl-a", state.HumanActor(fx.aliceID, "cust_a")); err != nil {
			t.Fatal(err)
		}
		fx.store.AddCustomer(&state.Customer{ID: "cust_a", Token: fx.token})
		if _, err := fx.store.AuthCustomer(fx.token); err != nil {
			t.Fatal("fixture did not make the old bearer a tenant credential")
		}
		if err := credential.check(context.Background(), fx.store); !errors.Is(err, state.ErrNotFound) {
			t.Fatalf("scoped session changed credential kind: %v", err)
		}
	})
	t.Run("scoped-cluster-cannot-change", func(t *testing.T) {
		fx := registryStore(t)
		credential := &agentStreamCredential{token: fx.token, customerID: "cust_a", clusterID: "cl-a"}
		fx.store.AddCustomer(&state.Customer{ID: "cust_a", RegisteredClusters: []*state.RegisteredCluster{{ClusterID: "cl-rebound", CredentialHash: state.HashClusterCredential(fx.token)}}})
		if err := credential.check(context.Background(), fx.store); !errors.Is(err, state.ErrNotFound) {
			t.Fatalf("scoped session changed cluster: %v", err)
		}
	})
	t.Run("tenant-cannot-change", func(t *testing.T) {
		s := state.New()
		credential := &agentStreamCredential{token: state.DefaultDevToken, customerID: state.DevCustomerID}
		s.AddCustomer(&state.Customer{ID: "synthetic-new-owner", Token: state.DefaultDevToken})
		if err := credential.check(context.Background(), s); !errors.Is(err, state.ErrNotFound) {
			t.Fatalf("session changed tenant: %v", err)
		}
	})
}

func TestAgentStreamRotationDuringHelloRefusesRegistration(t *testing.T) {
	s := state.New()
	srv := bindServer(t, s)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), http.Header{"Authorization": {"Bearer " + state.DefaultDevToken}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := s.RotateCustomerCredential(state.DevCustomerID, state.OperatorActor()); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeHello, protocol.Hello{ClusterID: "cl-stale-hello"})); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err = conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
		t.Errorf("stale Hello = %v; want policy close", err)
	}
	if _, err := s.AgentForCluster(state.DevCustomerID, "cl-stale-hello"); err == nil {
		t.Error("credential rotated during Hello still registered a routable agent")
	}
}
