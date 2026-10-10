package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
)

type fakeProviderDeleteReconciler struct {
	records []lifecycle.ProviderDeleteRecord
	err     error
	limit   int
	retry   providerDeleteRetryRequest
	actor   string
}

func (f *fakeProviderDeleteReconciler) ListProviderDeleteManualAttention(_ context.Context, limit int) ([]lifecycle.ProviderDeleteRecord, error) {
	f.limit = limit
	return f.records, f.err
}

func (f *fakeProviderDeleteReconciler) RetryProviderDelete(_ context.Context, customerID, clusterID, burstID, actor, traceID string) (lifecycle.ProviderDeleteRecord, error) {
	f.retry = providerDeleteRetryRequest{CustomerID: customerID, ClusterID: clusterID, BurstID: burstID, TraceID: traceID}
	f.actor = actor
	if f.err != nil {
		return lifecycle.ProviderDeleteRecord{}, f.err
	}
	return f.records[0], nil
}

func manualAttentionRecord() lifecycle.ProviderDeleteRecord {
	return lifecycle.ProviderDeleteRecord{
		ID: 17, CustomerID: "cust_1", ClusterID: "cluster_1", WorkloadID: "wl_1", BurstID: "burst_1",
		Provider: "linode", Region: "us-east", CloudAccountID: "account_1", SKU: "g1-gpu-rtx6000",
		ProviderResourceID: "123456", Reason: "workload failed", State: lifecycle.ProviderDeleteManualAttention,
		Generation: 2, Attempts: 8, LastError: "provider unavailable", RequestedAt: time.Unix(100, 0).UTC(),
		UpdatedAt: time.Unix(200, 0).UTC(), Payload: []byte(`{"secret":"worker-only"}`), LeaseToken: "worker-lease",
	}
}

func TestProviderDeleteAdminListsBoundedRedactedManualAttention(t *testing.T) {
	store := &fakeProviderDeleteReconciler{records: []lifecycle.ProviderDeleteRecord{manualAttentionRecord()}}
	h := &ProviderDeleteAdmin{Deletes: store}
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/provider-deletes/manual-attention?limit=7", nil)
	rec := httptest.NewRecorder()

	h.ListManualAttention(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if store.limit != 7 {
		t.Fatalf("limit = %d, want 7", store.limit)
	}
	body := rec.Body.String()
	for _, forbidden := range []string{"worker-only", "worker-lease", "payload", "lease_token"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("response leaked %q: %s", forbidden, body)
		}
	}
	if !strings.Contains(body, `"state":"manual_attention"`) || !strings.Contains(body, `"burst_id":"burst_1"`) {
		t.Fatalf("response omitted reconciliation identity: %s", body)
	}
}

func TestProviderDeleteAdminRetryRequeuesAndWakesWorker(t *testing.T) {
	record := manualAttentionRecord()
	record.State = lifecycle.ProviderDeleteQueued
	store := &fakeProviderDeleteReconciler{records: []lifecycle.ProviderDeleteRecord{record}}
	wake := make(chan struct{}, 1)
	h := &ProviderDeleteAdmin{Deletes: store, Wake: wake}
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/provider-deletes/retry",
		strings.NewReader(`{"customer_id":"cust_1","cluster_id":"cluster_1","burst_id":"burst_1","trace_id":"trace_1"}`))
	rec := httptest.NewRecorder()

	h.RetryManualAttention(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %q)", rec.Code, rec.Body.String())
	}
	want := providerDeleteRetryRequest{CustomerID: "cust_1", ClusterID: "cluster_1", BurstID: "burst_1", TraceID: "trace_1"}
	if store.retry != want || store.actor != "operator" {
		t.Fatalf("retry = %+v actor=%q, want %+v actor=operator", store.retry, store.actor, want)
	}
	select {
	case <-wake:
	default:
		t.Fatal("successful operator retry did not wake the delete worker")
	}
	var response providerDeleteAdminRecord
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.State != lifecycle.ProviderDeleteQueued || response.BurstID != "burst_1" {
		t.Fatalf("response = %+v, want queued burst_1", response)
	}
}

func TestProviderDeleteAdminMapsReconciliationErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code int
	}{
		{name: "invalid", err: lifecycle.ErrInvalidArgument, code: http.StatusBadRequest},
		{name: "missing", err: lifecycle.ErrNotFound, code: http.StatusNotFound},
		{name: "terminal", err: lifecycle.ErrInvariantViolation, code: http.StatusConflict},
		{name: "unavailable", err: errors.New("database unavailable"), code: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeProviderDeleteReconciler{err: test.err}
			h := &ProviderDeleteAdmin{Deletes: store}
			req := httptest.NewRequest(http.MethodPost, "/v1/admin/provider-deletes/retry",
				strings.NewReader(`{"customer_id":"cust_1","cluster_id":"cluster_1","burst_id":"burst_1"}`))
			rec := httptest.NewRecorder()
			h.RetryManualAttention(rec, req)
			if rec.Code != test.code {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, test.code, rec.Body.String())
			}
		})
	}
}

func TestProviderDeleteAdminRoutesRequireAdminToken(t *testing.T) {
	h := &ProviderDeleteAdmin{Deletes: &fakeProviderDeleteReconciler{records: []lifecycle.ProviderDeleteRecord{manualAttentionRecord()}}}
	protected := AdminAuth("admin-secret", http.HandlerFunc(h.ListManualAttention))
	for _, test := range []struct {
		name   string
		header string
		code   int
	}{
		{name: "missing", code: http.StatusUnauthorized},
		{name: "wrong", header: "Bearer wrong", code: http.StatusUnauthorized},
		{name: "valid", header: "Bearer admin-secret", code: http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/admin/provider-deletes/manual-attention", nil)
			req.Header.Set("Authorization", test.header)
			rec := httptest.NewRecorder()
			protected.ServeHTTP(rec, req)
			if rec.Code != test.code {
				t.Fatalf("status = %d, want %d", rec.Code, test.code)
			}
		})
	}
}
