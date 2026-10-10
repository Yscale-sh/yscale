package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/factory/internal/boxes"
	"github.com/yscale-sh/yscale/factory/internal/store"
)

// FabricView is the only fabric record exposed over HTTP. It intentionally has
// no API-key field, independent of the store's encrypted internal field.
type FabricView struct {
	Status              string `json:"status"`
	LoginServer         string `json:"login_server"`
	User                string `json:"user"`
	BackendID           string `json:"backend_id"`
	CreatedAt           string `json:"created_at"`
	LastValidatedAt     string `json:"last_validated_at"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
}

// provisionSyncWait is how long POST waits for provisioning before answering
// 202: long enough to surface fast configuration failures and fake workers,
// far shorter than a caller's request budget.
var provisionSyncWait = 2 * time.Second

// provisionBudget bounds one background provision: create, boot and the ops
// key handoff (whose registration lives 10 minutes).
const provisionBudget = 20 * time.Minute

// inflight keeps one provision per tenant in this process (the factory runs
// as a single replica), so a retried POST cannot create a second box.
type inflight struct {
	mu      sync.Mutex
	tenants map[string]bool
}

func (f *inflight) begin(tenantID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tenants[tenantID] {
		return false
	}
	f.tenants[tenantID] = true
	return true
}

func (f *inflight) end(tenantID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.tenants, tenantID)
}

// NewHandler returns the factory lifecycle HTTP API. Authentication is a
// deploy-time bearer-token seam until mTLS/auth is wired by the deployment.
func NewHandler(s store.Store, worker boxes.Worker, bearerToken string) http.Handler {
	return newHandler(s, worker, bearerToken, defaultMeshBox)
}

func newHandler(s store.Store, worker boxes.Worker, bearerToken string, newBox meshBoxFactory) http.Handler {
	running := &inflight{tenants: map[string]bool{}}
	want := []byte("Bearer " + bearerToken)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"status": "unauthorized"})
			return
		}

		if meshTenant, rest, ok := meshPath(r.URL.Path); ok {
			serveMesh(w, r, s, newBox, meshTenant, rest)
			return
		}

		tenantID, ok := fabricTenantID(r.URL.Path)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"status": "not_found"})
			return
		}

		switch r.Method {
		case http.MethodPost:
			idempotencyKey := r.Header.Get("Idempotency-Key")
			if idempotencyKey == "" {
				idempotencyKey = "tenant:" + tenantID
			}
			finished := false
			if running.begin(tenantID) {
				done := make(chan error, 1)
				go func() {
					defer running.end(tenantID)
					// Provisioning outlives this request: callers use a short
					// budget, and cancelling after the box exists would strand it.
					ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), provisionBudget)
					defer cancel()
					_, err := worker.Provision(ctx, tenantID, idempotencyKey)
					if err != nil {
						// The response stays generic; the operator log carries the cause.
						log.Printf("factory: provision tenant %s: %v", tenantID, err)
					}
					done <- err
				}()
				select {
				case err := <-done:
					if err != nil {
						writeJSON(w, http.StatusInternalServerError, map[string]string{"status": "provision_failed"})
						return
					}
					finished = true
				case <-time.After(provisionSyncWait):
				}
			}
			box, err := s.GetFabric(tenantID)
			if err != nil {
				if finished {
					writeJSON(w, http.StatusInternalServerError, map[string]string{"status": "provision_failed"})
					return
				}
				writeJSON(w, http.StatusAccepted, map[string]string{"status": store.StatusProvisioning})
				return
			}
			writeJSON(w, http.StatusAccepted, fabricView(box))
		case http.MethodGet:
			box, err := s.GetFabric(tenantID)
			if err != nil {
				writeJSON(w, http.StatusNotFound, map[string]string{"status": "absent"})
				return
			}
			if box.Status != store.StatusReady {
				writeJSON(w, http.StatusConflict, map[string]string{"status": box.Status})
				return
			}
			writeJSON(w, http.StatusOK, fabricView(box))
		case http.MethodDelete:
			box, err := s.GetFabric(tenantID)
			if err != nil {
				writeJSON(w, http.StatusNotFound, map[string]string{"status": "absent"})
				return
			}
			if _, err := worker.Deprovision(r.Context(), tenantID, box.LoginServer); err != nil {
				log.Printf("factory: deprovision tenant %s: %v", tenantID, err)
				writeJSON(w, http.StatusInternalServerError, map[string]string{"status": "deprovision_failed"})
				return
			}
			writeJSON(w, http.StatusAccepted, map[string]string{"status": store.StatusDecommissioning})
		default:
			w.Header().Set("Allow", "POST, GET, DELETE")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"status": "method_not_allowed"})
		}
	})
}

func fabricTenantID(path string) (string, bool) {
	const prefix = "/v1/tenants/"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, "/fabric") {
		return "", false
	}
	tenantID := strings.TrimSuffix(strings.TrimPrefix(path, prefix), "/fabric")
	if tenantID == "" || strings.Contains(tenantID, "/") {
		return "", false
	}
	return tenantID, true
}

func fabricView(box store.Box) FabricView {
	return FabricView{
		Status:              box.Status,
		LoginServer:         box.LoginServer,
		User:                box.HSUser,
		BackendID:           box.BackendID,
		CreatedAt:           box.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		LastValidatedAt:     box.LastValidatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		ConsecutiveFailures: box.ConsecutiveFailures,
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
