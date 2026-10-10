package handlers_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/decider"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
)

type provisioningDecider struct{}

func (provisioningDecider) Plan(context.Context, *workload.Workload, handlers.PlanOptions) (*handlers.Plan, error) {
	return nil, decider.ErrFabricProvisioning
}

func TestCreateMapsFabricProvisioningToRetryable503(t *testing.T) {
	store := state.New()
	customer, err := store.AuthCustomer(state.DefaultDevToken)
	if err != nil {
		t.Fatal(err)
	}
	store.AddAgent(&state.Agent{
		ID: "agent_fabric", CustomerID: customer.ID, ClusterID: "cluster_fabric",
		Send: make(chan protocol.Envelope, 1),
	})
	h := &handlers.Workloads{
		Store: store, Decider: provisioningDecider{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(`apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: fabric-test
spec:
  image: busybox
  size: small`))
	req.Header.Set("Authorization", "Bearer "+state.DefaultDevToken)
	rec := httptest.NewRecorder()
	handlers.Auth(store, http.HandlerFunc(h.Create)).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "15" {
		t.Fatalf("Retry-After = %q, want 15", rec.Header().Get("Retry-After"))
	}
	var response struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "provisioning" || response.Reason != "fabric_provisioning" {
		t.Fatalf("response = %+v", response)
	}
}
