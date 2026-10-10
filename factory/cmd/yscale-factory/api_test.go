package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yscale-sh/yscale/factory/internal/boxes"
	"github.com/yscale-sh/yscale/factory/internal/store"
)

func TestFabricLifecycleAPIIsIdempotentAndRedacted(t *testing.T) {
	s, err := store.NewWithKEK(bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	worker := boxes.NewFakeWorker(s)
	worker.SetProvisionHook(func(context.Context, store.Box) error { return nil })
	h := NewHandler(s, worker, "test-token")

	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-a/fabric", nil)
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Idempotency-Key", "create-1")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	if rec := post(); rec.Code != http.StatusAccepted {
		t.Fatalf("first POST status = %d, want 202", rec.Code)
	}
	if rec := post(); rec.Code != http.StatusAccepted {
		t.Fatalf("duplicate POST status = %d, want 202", rec.Code)
	}
	if got := len(s.ListBoxes()); got != 1 {
		t.Fatalf("boxes after duplicate POST = %d, want 1", got)
	}
	if got := len(s.ListJobs("tenant-a")); got != 1 {
		t.Fatalf("jobs after duplicate POST = %d, want 1", got)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/tenant-a/fabric", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict || !bytes.Contains(rec.Body.Bytes(), []byte(`"provisioning"`)) {
		t.Fatalf("GET while provisioning = %d %s, want 409 provisioning", rec.Code, rec.Body.String())
	}

	if err := worker.AdvanceProvision(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/tenants/tenant-a/fabric", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET while ready status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if _, ok := response["api_key"]; ok {
		t.Fatal("fabric response exposed api_key")
	}
}

func TestDeleteFabricEnqueuesDrain(t *testing.T) {
	s, err := store.NewWithKEK(bytes.Repeat([]byte{2}, 32))
	if err != nil {
		t.Fatal(err)
	}
	worker := boxes.NewFakeWorker(s)
	h := NewHandler(s, worker, "test-token")

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-a/fabric", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodDelete, "/v1/tenants/tenant-a/fabric", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("DELETE status = %d, body = %s", rec.Code, rec.Body.String())
	}
	jobs := s.ListJobs("tenant-a")
	if len(jobs) != 2 || jobs[1].Kind != "deprovision" || jobs[1].Status != store.JobPending {
		t.Fatalf("jobs after DELETE = %#v, want pending deprovision", jobs)
	}
}
