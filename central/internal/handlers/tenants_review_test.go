package handlers

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestHandleProvisionRejectsMalformedJSONBeforeSideEffects(t *testing.T) {
	for _, body := range []string{
		`{"ID":"cust_invalid","MaxConcurrentBursts":"five"}`,
		`{"ID":"cust_invalid",`,
		`{"ID":"cust_invalid","workload_namespaces":[1]}`,
		`[]`,
	} {
		t.Run(body, func(t *testing.T) {
			store := state.New()
			before, _ := store.ListOperatorTenants("", 100)
			tn := newTenants(store)
			factory := &lifecycleFactoryFake{ensures: make(chan ensureCall, 1)}
			tn.Factory = factory
			tracked := false
			tn.TrackFabric = func(string) { tracked = true }
			rec := httptest.NewRecorder()
			tn.HandleProvision(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/tenants", strings.NewReader(body)))
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
			customers, _ := store.ListOperatorTenants("", 100)
			if len(customers) != len(before) || tracked {
				t.Fatalf("invalid JSON created %d tenants; fabric tracked = %v", len(customers)-len(before), tracked)
			}
		})
	}
}

func TestHandleProvisionStillAcceptsOptionalBody(t *testing.T) {
	for _, body := range []string{"", " \n\t", "{}"} {
		t.Run(body, func(t *testing.T) {
			tn := newTenants(state.New())
			rec := httptest.NewRecorder()
			tn.HandleProvision(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/tenants", strings.NewReader(body)))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
		})
	}
}

func TestOffboardReportsHostedConnectorLeftovers(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		name := "active"
		if revoked {
			name = "revoked_retry"
		}
		t.Run(name, func(t *testing.T) {
			store := state.New()
			store.AddCustomer(&state.Customer{ID: "cust_hosted", Token: "tok", Plan: "pro"})
			for _, id := range []string{"hosted-one"} {
				if _, _, err := store.AssignHostedCluster("cust_hosted", id, id, "ns-"+id, state.OperatorActor()); err != nil {
					t.Fatal(err)
				}
			}
			if revoked {
				if err := store.RevokeCustomer("cust_hosted"); err != nil {
					t.Fatal(err)
				}
			}
			var logs bytes.Buffer
			tn := newTenants(store)
			tn.Log = slog.New(slog.NewTextHandler(&logs, nil))
			report, err := tn.Offboard(context.Background(), "cust_hosted")
			if err != nil {
				t.Fatal(err)
			}
			if report.Clean || !report.CustomerRevoked || !report.CustomerRecordDeleted {
				t.Fatalf("hosted releases must be reported as manual residue: %+v", report)
			}
			if len(report.Warnings) != 1 {
				t.Fatalf("warnings = %v, want one per hosted release", report.Warnings)
			}
			warnings := strings.Join(report.Warnings, " ")
			for _, id := range []string{"hosted-one"} {
				for _, want := range []string{"helm uninstall yscale-agent-" + id + " --namespace yscale-system", "ns-" + id} {
					if !strings.Contains(warnings, want) || !strings.Contains(logs.String(), want) {
						t.Errorf("response and log must name leftover %q", want)
					}
				}
			}
		})
	}
}
