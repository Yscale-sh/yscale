package hsclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLoginServerFor(t *testing.T) {
	cases := map[string]string{
		"172-1-2-3.ip.linodeusercontent.com": "https://172-1-2-3.ip.linodeusercontent.com",
		"https://box.example.com":            "https://box.example.com",
		"https://box.example.com/":           "https://box.example.com",
		"http://127.0.0.1:8080":              "http://127.0.0.1:8080",
		"  box.example.com  ":                "https://box.example.com",
	}
	for in, want := range cases {
		if got := loginServerFor(in); got != want {
			t.Errorf("loginServerFor(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRegisterHeadscale_RequiresFields(t *testing.T) {
	_, _, _, _, err := RegisterHeadscale(context.Background(), BoxInfo{Hostname: "h", APIKey: "", User: "u"})
	if err == nil {
		t.Fatal("expected error when APIKey is empty")
	}
}

func TestRegisterHeadscale_EnsuresUserAndReturnsTuple(t *testing.T) {
	var sawUserCreate bool
	var sawAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/user" {
			sawUserCreate = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	// srv.URL is http://127.0.0.1:port — the loopback carve-out in the
	// Headscale client permits the API key over http for tests.
	login, apiKey, user, backendID, err := RegisterHeadscale(context.Background(), BoxInfo{
		Hostname:  srv.URL,
		APIKey:    "hs-apikey-xyz",
		User:      "acme",
		BackendID: "linode-123",
	})
	if err != nil {
		t.Fatalf("RegisterHeadscale: %v", err)
	}
	if !sawUserCreate {
		t.Error("expected EnsureUser to POST /api/v1/user")
	}
	if sawAuth != "Bearer hs-apikey-xyz" {
		t.Errorf("API key not sent as bearer: got %q", sawAuth)
	}
	if login != srv.URL {
		t.Errorf("login = %q, want %q", login, srv.URL)
	}
	if apiKey != "hs-apikey-xyz" || user != "acme" || backendID != "linode-123" {
		t.Errorf("tuple mismatch: apiKey=%q user=%q backendID=%q", apiKey, user, backendID)
	}
}
