package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/central/internal/state"
)

const defaultStreamCredentialPoll = time.Second

// The original bearer and principal stay bound to this socket. Revalidation
// does not use the admission Customer pointer (mutable in memory stores), and
// a failed scoped lookup never falls back to a tenant credential. The bearer
// stays in request-lifetime memory only, never Agent, persistence or diagnostics.
type agentStreamCredential struct {
	token      string
	customerID string
	clusterID  string
	cancel     context.CancelFunc
	closeOnce  sync.Once
}

func newAgentStreamCredential(r *http.Request, customerID string) (*agentStreamCredential, error) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return nil, state.ErrNotFound
	}
	return &agentStreamCredential{token: token, customerID: customerID, clusterID: AgentClusterFromContext(r.Context())}, nil
}

func (c *agentStreamCredential) check(ctx context.Context, store *state.Store) error {
	if store == nil {
		return state.ErrPersistence
	}
	var customer *state.Customer
	var err error
	if c.clusterID != "" {
		var clusterID string
		customer, clusterID, err = store.AuthClusterCredentialContext(ctx, c.token)
		if err == nil && clusterID != c.clusterID {
			return state.ErrNotFound
		}
	} else {
		customer, err = store.AuthCustomerContext(ctx, c.token)
	}
	if err != nil {
		return err
	}
	if customer == nil || customer.ID != c.customerID {
		return state.ErrNotFound
	}
	return nil
}

func (h *AgentStream) requireStreamCredential(ctx context.Context, conn *websocket.Conn, agent *state.Agent, credential *agentStreamCredential) bool {
	if ctx.Err() != nil {
		return false
	}
	err := credential.check(ctx, h.Store)
	if agent != nil {
		select {
		case <-agent.Evicted():
			err = state.ErrNotFound
		default:
		}
	}
	if err == nil {
		return true
	}
	h.closeCredentialStream(conn, agent, credential, err)
	return false
}

func (h *AgentStream) closeCredentialStream(conn *websocket.Conn, agent *state.Agent, credential *agentStreamCredential, err error) {
	credential.closeOnce.Do(func() {
		code, reason := websocket.CloseTryAgainLater, "credential store unavailable"
		clusterID := credential.clusterID
		if errors.Is(err, state.ErrNotFound) {
			code, reason = websocket.ClosePolicyViolation, "connector credential revoked"
		}
		// Stop routing/enqueues before waiting on any socket I/O. The normal
		// disconnect path will still settle outstanding command leases.
		if agent != nil {
			clusterID = agent.ClusterID
			h.Store.RemoveAgent(agent.ID)
		}
		h.Log.Warn("agent session authorization refused", "customer", credential.customerID,
			"cluster", clusterID, "reason", reason)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(time.Second))
		_ = conn.Close()
		if credential.cancel != nil {
			credential.cancel()
		}
	})
}

func (h *AgentStream) streamCredentialPoll() time.Duration {
	if h.credentialPoll > 0 {
		return h.credentialPoll
	}
	return defaultStreamCredentialPoll
}
