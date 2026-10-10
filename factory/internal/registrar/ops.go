// Package registrar contains factory-side credential handoff implementations.
package registrar

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/factory/internal/boxes"
)

const defaultTTL = 10 * time.Minute

// Option configures an OpsRegistrar.
type Option func(*OpsRegistrar)

// WithTTL changes the lifetime of a handoff token.
func WithTTL(ttl time.Duration) Option { return func(r *OpsRegistrar) { r.ttl = ttl } }

type pendingHandoff struct {
	tenantID string
	expires  time.Time
	wait     chan struct{}
}

type receivedHandoff struct {
	tenantID    string
	loginServer string
	apiKey      string
}

// OpsRegistrar accepts single-use Headscale admin-key handoffs over the
// factory's private ops tailnet. Keys are never persisted or logged.
type OpsRegistrar struct {
	mu       sync.Mutex
	ttl      time.Duration
	now      func() time.Time
	pending  map[string]pendingHandoff
	received map[string]receivedHandoff
}

// NewOpsRegistrar constructs an in-memory ops-tailnet handoff registrar.
func NewOpsRegistrar(opts ...Option) *OpsRegistrar {
	r := &OpsRegistrar{
		ttl:      defaultTTL,
		now:      time.Now,
		pending:  make(map[string]pendingHandoff),
		received: make(map[string]receivedHandoff),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// NewRegistration mints a cryptographically random, single-use handoff token.
func (r *OpsRegistrar) NewRegistration(ctx context.Context, tenantID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	token := hex.EncodeToString(raw[:])
	r.mu.Lock()
	r.pending[token] = pendingHandoff{
		tenantID: tenantID,
		expires:  r.now().Add(r.ttl),
		wait:     make(chan struct{}),
	}
	r.mu.Unlock()
	return token, nil
}

// AwaitKey waits for one matching handoff and returns its transient admin key.
func (r *OpsRegistrar) AwaitKey(ctx context.Context, token, expectedLoginServer, tenantID string) (string, error) {
	r.mu.Lock()
	if received, ok := r.received[token]; ok {
		delete(r.pending, token)
		delete(r.received, token)
		r.mu.Unlock()
		return validateReceived(received, expectedLoginServer, tenantID)
	}
	pending, ok := r.pending[token]
	if !ok {
		r.mu.Unlock()
		return "", errors.New("ops handoff is unavailable")
	}
	if !pending.expires.After(r.now()) {
		delete(r.pending, token)
		r.mu.Unlock()
		return "", errors.New("ops handoff has expired")
	}
	wait := pending.wait
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		r.mu.Lock()
		delete(r.pending, token)
		delete(r.received, token)
		r.mu.Unlock()
		return "", ctx.Err()
	case <-wait:
	}

	r.mu.Lock()
	received, ok := r.received[token]
	delete(r.pending, token)
	delete(r.received, token)
	r.mu.Unlock()
	if !ok {
		return "", errors.New("ops handoff is unavailable")
	}
	return validateReceived(received, expectedLoginServer, tenantID)
}

func validateReceived(received receivedHandoff, expectedLoginServer, tenantID string) (string, error) {
	if received.tenantID != tenantID {
		return "", errors.New("ops handoff tenant mismatch")
	}
	if received.loginServer != expectedLoginServer {
		return "", errors.New("ops handoff login server mismatch")
	}
	return received.apiKey, nil
}

// Handler serves the ops-tailnet-only key handoff endpoint.
func (r *OpsRegistrar) Handler() http.Handler {
	return http.HandlerFunc(r.handleRegister)
}

type handoffRequest struct {
	Token       string `json:"token"`
	TenantID    string `json:"tenant_id"`
	LoginServer string `json:"login_server"`
	APIKey      string `json:"api_key"`
}

func (r *OpsRegistrar) handleRegister(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost || req.URL.Path != "/ops/register" {
		http.NotFound(w, req)
		return
	}
	defer req.Body.Close()
	var handoff handoffRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, req.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&handoff); err != nil || handoff.Token == "" || handoff.TenantID == "" || handoff.LoginServer == "" || handoff.APIKey == "" {
		http.Error(w, "invalid handoff", http.StatusBadRequest)
		return
	}

	r.mu.Lock()
	pending, ok := r.pending[handoff.Token]
	if !ok || !pending.expires.After(r.now()) || pending.tenantID != handoff.TenantID {
		if ok && !pending.expires.After(r.now()) {
			delete(r.pending, handoff.Token)
		}
		r.mu.Unlock()
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	delete(r.pending, handoff.Token)
	r.received[handoff.Token] = receivedHandoff{
		tenantID:    handoff.TenantID,
		loginServer: handoff.LoginServer,
		apiKey:      handoff.APIKey,
	}
	close(pending.wait)
	r.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

var _ boxes.Registrar = (*OpsRegistrar)(nil)
