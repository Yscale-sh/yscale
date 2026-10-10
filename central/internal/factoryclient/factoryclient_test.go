// yscale:proprietary

package factoryclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestEnsureFabricSendsBearerAndIdempotencyKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer factory-token" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Idempotency-Key") != "cust_a" {
			t.Errorf("Idempotency-Key = %q", r.Header.Get("Idempotency-Key"))
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tenants/cust_a/fabric" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	if err := New(server.URL, "factory-token").EnsureFabric(context.Background(), "cust_a", "cust_a"); err != nil {
		t.Fatalf("EnsureFabric: %v", err)
	}
}

func TestEnsureFabricConflictIsProvisioning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	err := New(server.URL, "token").EnsureFabric(context.Background(), "cust_a", "cust_a")
	if !errors.Is(err, ErrFabricProvisioning) {
		t.Fatalf("EnsureFabric error = %v, want ErrFabricProvisioning", err)
	}
}

func TestGetFabricDecodesPublicFieldsOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ready","login_server":"https://box.example","user":"acme","backend_id":"linode-9","consecutive_failures":2,"api_key":"must-not-decode"}`))
	}))
	defer server.Close()

	fabric, err := New(server.URL, "token").GetFabric(context.Background(), "cust_a")
	if err != nil {
		t.Fatalf("GetFabric: %v", err)
	}
	if fabric.Status != "ready" || fabric.LoginServer != "https://box.example" || fabric.User != "acme" || fabric.BackendID != "linode-9" || fabric.ConsecutiveFailures != 2 {
		t.Fatalf("Fabric = %+v", fabric)
	}
	if _, ok := reflect.TypeOf(Fabric{}).FieldByName("APIKey"); ok {
		t.Fatal("Fabric must not carry an APIKey")
	}
}

func TestDeleteFabricNotFoundIsIdempotent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %s, want DELETE", r.Method)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	if err := New(server.URL, "token").DeleteFabric(context.Background(), "cust_a"); err != nil {
		t.Fatalf("DeleteFabric: %v", err)
	}
}

func TestShortContextBeatsClientBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := New(server.URL, "token").EnsureFabric(ctx, "cust_a", "cust_a")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("EnsureFabric error = %v, want deadline exceeded", err)
	}
}
