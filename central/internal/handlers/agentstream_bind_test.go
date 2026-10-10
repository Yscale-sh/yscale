package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// bindServer serves the agent stream behind ConnectorAuth, exactly as main.go
// wires it.
func bindServer(t *testing.T, s *state.Store) *httptest.Server {
	t.Helper()
	stream := NewAgentStream(s, quietLog(), nil)
	srv := httptest.NewServer(ConnectorAuth(s, stream))
	t.Cleanup(srv.Close)
	return srv
}

func dialAgent(t *testing.T, srv *httptest.Server, token, clusterID string) (*websocket.Conn, error) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	hdr := http.Header{"Authorization": {"Bearer " + token}}
	conn, _, err := websocket.DefaultDialer.Dial(url, hdr)
	if err != nil {
		return nil, err
	}
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeHello, protocol.Hello{ClusterID: clusterID})); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// The capability has to reach the live socket's record, because admission is
// what reads it: a nodeOnly submission routed to this connector is bounded by a
// deadline or by the connector's own idle teardown depending on this one field,
// and it is decided while the socket is up. A connector that claims nothing —
// including every one deployed before the field existed — must land as false.
func TestAgentStreamRecordsTheConnectorsOccupancyCapability(t *testing.T) {
	for _, tt := range []struct {
		name    string
		cluster string
		hello   protocol.Hello
		want    bool
	}{
		{
			name:    "a connector that claims it",
			cluster: "cl-authoritative",
			hello:   protocol.Hello{ClusterID: "cl-authoritative", AuthoritativeOccupancy: true},
			want:    true,
		},
		{
			name:    "a connector that claims nothing",
			cluster: "cl-quiet",
			hello:   protocol.Hello{ClusterID: "cl-quiet"},
			want:    false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := state.New()
			srv := bindServer(t, store)
			url := "ws" + strings.TrimPrefix(srv.URL, "http")
			hdr := http.Header{"Authorization": {"Bearer " + state.DefaultDevToken}}
			conn, _, err := websocket.DefaultDialer.Dial(url, hdr)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()
			if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeHello, tt.hello)); err != nil {
				t.Fatalf("hello: %v", err)
			}
			waitFor(t, "the connector to register", func() bool {
				_, err := store.AgentForCluster(state.DevCustomerID, tt.cluster)
				return err == nil
			})
			a, err := store.AgentForCluster(state.DevCustomerID, tt.cluster)
			if err != nil {
				t.Fatalf("agent: %v", err)
			}
			if a.AuthoritativeOccupancy != tt.want {
				t.Fatalf("AuthoritativeOccupancy = %v, want %v", a.AuthoritativeOccupancy, tt.want)
			}
		})
	}
}

// waitFor polls until cond is true or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func fleetRow(s *state.Store, customerID, accountID, clusterID string) *state.TenantCluster {
	rows, err := s.TenantClustersFor(customerID, accountID)
	if err != nil {
		return nil
	}
	for i := range rows {
		rc := &rows[i]
		if rc.ClusterID == clusterID {
			return rc
		}
	}
	return nil
}

// A legacy tenant-token connector keeps working unchanged AND its
// self-announced cluster is claimed into the durable registry before the
// socket registers — so the fleet becomes durable without a redeploy, and the
// disconnect stamps land when the socket closes.
func TestAgentStreamLegacyTokenClaimsClusterAndStampsLifecycle(t *testing.T) {
	s := state.New()
	viewer, err := s.UpsertAccount("https://id.test", "dev-viewer", state.AccountProfile{Email: "viewer@dev.test", EmailVerified: true})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s.AddTenantMembership(viewer.ID, state.DevCustomerID, state.RoleViewer); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}
	srv := bindServer(t, s)

	conn, err := dialAgent(t, srv, state.DefaultDevToken, "cl-legacy")
	if err != nil {
		t.Fatalf("legacy connect: %v", err)
	}
	waitFor(t, "agent to register", func() bool {
		_, err := s.AgentForCluster(state.DevCustomerID, "cl-legacy")
		return err == nil
	})
	row := fleetRow(s, state.DevCustomerID, viewer.ID, "cl-legacy")
	if row == nil || row.Source != state.ClusterSourceClaimed {
		t.Fatalf("claimed registry row = %+v, want source=claimed", row)
	}
	if row.FirstConnectedAt == nil || row.LastConnectedAt == nil {
		t.Fatalf("connect stamps missing: %+v", row)
	}
	if owner, ok := s.CustomerForClusterID("cl-legacy"); !ok || owner != state.DevCustomerID {
		t.Fatalf("claimed owner = (%q,%v)", owner, ok)
	}

	conn.Close()
	waitFor(t, "disconnect stamps", func() bool {
		row := fleetRow(s, state.DevCustomerID, viewer.ID, "cl-legacy")
		return row != nil && row.LastDisconnectedAt != nil
	})
	// The cluster is still in the fleet after the socket died: registered
	// disconnected, not forgotten.
	if row := fleetRow(s, state.DevCustomerID, viewer.ID, "cl-legacy"); row == nil {
		t.Fatal("disconnected cluster fell out of the registry")
	}
}

// A cluster-scoped credential may only announce the cluster it is bound to.
// The right Hello connects and stamps the registered row; a Hello naming any
// other cluster is closed before AddAgent, so no socket ever registers.
func TestAgentStreamScopedCredentialEnforcesHelloBinding(t *testing.T) {
	fx := registryStore(t)
	s, token := fx.store, fx.token
	srv := bindServer(t, s)

	// Wrong cluster: the connection is closed with a policy violation and no
	// agent appears.
	conn, err := dialAgent(t, srv, token, "cl-elsewhere")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("mismatched hello was not closed")
	}
	conn.Close()
	if _, err := s.AgentForCluster("cust_a", "cl-elsewhere"); err == nil {
		t.Fatal("a mismatched hello registered an agent")
	}

	// Matching cluster: connected, and the registered row gains its connect
	// stamps.
	conn, err = dialAgent(t, srv, token, "cl-a")
	if err != nil {
		t.Fatalf("scoped connect: %v", err)
	}
	defer conn.Close()
	waitFor(t, "scoped agent to register", func() bool {
		_, err := s.AgentForCluster("cust_a", "cl-a")
		return err == nil
	})
	row := fleetRow(s, "cust_a", fx.aliceID, "cl-a")
	if row == nil || row.Source != state.ClusterSourceRegistered || row.FirstConnectedAt == nil {
		t.Fatalf("registered row after scoped connect = %+v", row)
	}
}

// readUntilClosed drains frames until the server tears the connection down,
// failing if it stays open past the deadline.
func readUntilClosed(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			if strings.Contains(err.Error(), "timeout") {
				t.Fatal("server left the evicted socket open")
			}
			return
		}
	}
}

// Rotating a cluster's credential revokes its OPEN socket on this replica,
// not just the next reconnect: by the time the rotation returns the agent is
// unroutable, and the websocket itself is then torn down. The old credential
// cannot dial back in.
func TestAgentStreamRotateEvictsLiveSocket(t *testing.T) {
	fx := registryStore(t)
	s := fx.store
	srv := bindServer(t, s)

	conn, err := dialAgent(t, srv, fx.token, "cl-a")
	if err != nil {
		t.Fatalf("scoped connect: %v", err)
	}
	defer conn.Close()
	waitFor(t, "agent to register", func() bool {
		_, err := s.AgentForCluster("cust_a", "cl-a")
		return err == nil
	})

	if _, _, _, err := s.RotateTenantClusterCredential("cust_a", fx.aliceID, "cl-a", state.HumanActor(fx.aliceID, "cust_a")); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	// Synchronous: no waitFor — the mutation returning IS the guarantee.
	if _, err := s.AgentForCluster("cust_a", "cl-a"); err == nil {
		t.Fatal("rotated cluster still routable on the old socket")
	}
	readUntilClosed(t, conn)
	if _, err := dialAgent(t, srv, fx.token, "cl-a"); err == nil {
		t.Fatal("old credential still dials after rotation")
	}
}

// Deleting a cluster with a live connector: the registry row, the routing
// entry and the socket all go together, so the fleet list and the dispatch
// path can never disagree about a deleted cluster.
func TestAgentStreamDeleteEvictsSocketAndFleetAgrees(t *testing.T) {
	fx := registryStore(t)
	s := fx.store
	srv := bindServer(t, s)

	conn, err := dialAgent(t, srv, fx.token, "cl-a")
	if err != nil {
		t.Fatalf("scoped connect: %v", err)
	}
	defer conn.Close()
	waitFor(t, "agent to register", func() bool {
		_, err := s.AgentForCluster("cust_a", "cl-a")
		return err == nil
	})

	if _, err := s.DeleteTenantCluster("cust_a", fx.aliceID, "cl-a", state.HumanActor(fx.aliceID, "cust_a")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if row := fleetRow(s, "cust_a", fx.aliceID, "cl-a"); row != nil {
		t.Fatalf("fleet still lists the deleted cluster: %+v", row)
	}
	if _, err := s.AgentForCluster("cust_a", "cl-a"); err == nil {
		t.Fatal("deleted cluster still routable")
	}
	readUntilClosed(t, conn)
	// The dead socket's cleanup runs MarkClusterDisconnected; give it a beat
	// and confirm it did not resurrect the deleted row.
	waitFor(t, "stream cleanup", func() bool {
		return len(s.AgentsForCustomer("cust_a")) == 0
	})
	if row := fleetRow(s, "cust_a", fx.aliceID, "cl-a"); row != nil {
		t.Fatalf("stream cleanup resurrected the deleted row: %+v", row)
	}
}

// A legacy tenant-token connector with a pre-registry cluster id OUTSIDE the
// ValidClusterID grammar still connects — live-only, exactly the pre-registry
// contract — instead of the registry bricking a running fleet on upgrade.
func TestAgentStreamLegacyInvalidClusterIDConnectsEphemerally(t *testing.T) {
	fx := registryStore(t)
	s := fx.store
	srv := bindServer(t, s)

	const legacyID = "Legacy_Cluster/01" // '/' is outside the grammar
	conn, err := dialAgent(t, srv, "tok_b", legacyID)
	if err != nil {
		t.Fatalf("legacy connect: %v", err)
	}
	defer conn.Close()
	waitFor(t, "legacy agent to register", func() bool {
		_, err := s.AgentForCluster("cust_b", legacyID)
		return err == nil
	})
	if row := fleetRow(s, "cust_b", fx.bobID, legacyID); row != nil {
		t.Fatalf("pre-grammar id was claimed into the registry: %+v", row)
	}
	if owner, ok := s.CustomerForClusterID(legacyID); !ok || owner != "cust_b" {
		t.Fatalf("live-only owner = (%q,%v), want (cust_b,true)", owner, ok)
	}
}

// A legacy token cannot claim a cluster id another tenant holds durably, even
// with no connector live on the owner's side: the socket is closed before
// AddAgent and the registry is untouched.
func TestAgentStreamRefusesCrossTenantHello(t *testing.T) {
	fx := registryStore(t) // cl-a is registered to cust_a; no socket is live
	s := fx.store
	srv := bindServer(t, s)

	conn, err := dialAgent(t, srv, "tok_b", "cl-a")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("cross-tenant hello was not closed")
	}
	conn.Close()
	if agents := s.AgentsForCustomer("cust_b"); len(agents) != 0 {
		t.Fatalf("cross-tenant hello registered an agent: %v", agents)
	}
	if owner, _ := s.CustomerForClusterID("cl-a"); owner != "cust_a" {
		t.Fatalf("cl-a owner = %q, want cust_a", owner)
	}
	if row := fleetRow(s, "cust_b", fx.bobID, "cl-a"); row != nil {
		t.Fatalf("cross-tenant hello claimed a row: %+v", row)
	}
}
