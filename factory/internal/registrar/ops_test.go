package registrar

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	testTenant = "tenant-a"
	testLogin  = "https://box.example"
	testKey    = "headscale-admin-key"
)

func postHandoff(t *testing.T, r *OpsRegistrar, body handoffRequest) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	r.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/ops/register", strings.NewReader(string(payload))))
	return recorder
}

func TestOpsRegistrarHandoff(t *testing.T) {
	r := NewOpsRegistrar()
	token, err := r.NewRegistration(context.Background(), testTenant)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan struct {
		key string
		err error
	}, 1)
	go func() {
		key, err := r.AwaitKey(context.Background(), token, testLogin, testTenant)
		result <- struct {
			key string
			err error
		}{key, err}
	}()
	if got := postHandoff(t, r, handoffRequest{Token: token, TenantID: testTenant, LoginServer: testLogin, APIKey: testKey}).Code; got != http.StatusNoContent {
		t.Fatalf("POST status = %d, want %d", got, http.StatusNoContent)
	}
	got := <-result
	if got.err != nil || got.key != testKey {
		t.Fatalf("AwaitKey() = %q, %v", got.key, got.err)
	}
}

func TestOpsRegistrarRejectsUnknownExpiredAndReusedTokens(t *testing.T) {
	r := NewOpsRegistrar()
	unknown := postHandoff(t, r, handoffRequest{Token: "unknown", TenantID: testTenant, LoginServer: testLogin, APIKey: testKey})
	if unknown.Code != http.StatusUnauthorized || strings.Contains(unknown.Body.String(), testKey) {
		t.Fatalf("unknown handoff = %d, %q", unknown.Code, unknown.Body.String())
	}
	if len(r.received) != 0 {
		t.Fatal("unknown handoff stored a key")
	}
	mismatchToken, err := r.NewRegistration(context.Background(), testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if got := postHandoff(t, r, handoffRequest{Token: mismatchToken, TenantID: "other-tenant", LoginServer: testLogin, APIKey: testKey}).Code; got != http.StatusUnauthorized {
		t.Fatalf("tenant mismatch status = %d, want %d", got, http.StatusUnauthorized)
	}
	if len(r.received) != 0 {
		t.Fatal("tenant-mismatched handoff stored a key")
	}
	expired := NewOpsRegistrar(WithTTL(-time.Second))
	token, err := expired.NewRegistration(context.Background(), testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if got := postHandoff(t, expired, handoffRequest{Token: token, TenantID: testTenant, LoginServer: testLogin, APIKey: testKey}).Code; got != http.StatusUnauthorized {
		t.Fatalf("expired handoff status = %d, want %d", got, http.StatusUnauthorized)
	}
	if len(expired.received) != 0 {
		t.Fatal("expired handoff stored a key")
	}
	token, err = r.NewRegistration(context.Background(), testTenant)
	if err != nil {
		t.Fatal(err)
	}
	request := handoffRequest{Token: token, TenantID: testTenant, LoginServer: testLogin, APIKey: testKey}
	if got := postHandoff(t, r, request).Code; got != http.StatusNoContent {
		t.Fatalf("first handoff status = %d", got)
	}
	if got := postHandoff(t, r, request).Code; got != http.StatusUnauthorized {
		t.Fatalf("reused handoff status = %d, want %d", got, http.StatusUnauthorized)
	}
	if _, err := r.AwaitKey(context.Background(), token, testLogin, testTenant); err != nil {
		t.Fatal(err)
	}
}

func TestOpsRegistrarRejectsMismatchedHandoffAndDoesNotExposeKey(t *testing.T) {
	for _, test := range []struct {
		name     string
		login    string
		tenant   string
		contains string
	}{
		{name: "login server", login: "https://other.example", tenant: testTenant, contains: "login server mismatch"},
		{name: "tenant", login: testLogin, tenant: "tenant-b", contains: "tenant mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := NewOpsRegistrar()
			token, err := r.NewRegistration(context.Background(), testTenant)
			if err != nil {
				t.Fatal(err)
			}
			if got := postHandoff(t, r, handoffRequest{Token: token, TenantID: testTenant, LoginServer: test.login, APIKey: testKey}).Code; got != http.StatusNoContent {
				t.Fatalf("POST status = %d", got)
			}
			_, err = r.AwaitKey(context.Background(), token, testLogin, test.tenant)
			if err == nil || !strings.Contains(err.Error(), test.contains) || strings.Contains(err.Error(), testKey) {
				t.Fatalf("AwaitKey error = %v", err)
			}
		})
	}
}

func TestOpsRegistrarAwaitKeyTimeout(t *testing.T) {
	r := NewOpsRegistrar()
	token, err := r.NewRegistration(context.Background(), testTenant)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := r.AwaitKey(ctx, token, testLogin, testTenant); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AwaitKey error = %v, want deadline exceeded", err)
	}
}

func TestOpsRegistrarConcurrentHandoffsDoNotCross(t *testing.T) {
	r := NewOpsRegistrar()
	tokenA, err := r.NewRegistration(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tokenB, err := r.NewRegistration(context.Background(), "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		key string
		err error
	}
	resultA := make(chan result, 1)
	resultB := make(chan result, 1)
	go func() {
		key, err := r.AwaitKey(context.Background(), tokenA, "https://a.example", "tenant-a")
		resultA <- result{key, err}
	}()
	go func() {
		key, err := r.AwaitKey(context.Background(), tokenB, "https://b.example", "tenant-b")
		resultB <- result{key, err}
	}()
	statuses := make(chan int, 2)
	post := func(request handoffRequest) {
		payload, _ := json.Marshal(request)
		recorder := httptest.NewRecorder()
		r.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/ops/register", strings.NewReader(string(payload))))
		statuses <- recorder.Code
	}
	go func() {
		post(handoffRequest{Token: tokenB, TenantID: "tenant-b", LoginServer: "https://b.example", APIKey: "key-b"})
	}()
	go func() {
		post(handoffRequest{Token: tokenA, TenantID: "tenant-a", LoginServer: "https://a.example", APIKey: "key-a"})
	}()
	for range 2 {
		if got := <-statuses; got != http.StatusNoContent {
			t.Fatalf("handoff status = %d", got)
		}
	}
	if got := <-resultA; got.err != nil || got.key != "key-a" {
		t.Fatalf("A AwaitKey() = %q, %v", got.key, got.err)
	}
	if got := <-resultB; got.err != nil || got.key != "key-b" {
		t.Fatalf("B AwaitKey() = %q, %v", got.key, got.err)
	}
}
