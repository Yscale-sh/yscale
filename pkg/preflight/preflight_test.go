package preflight

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/pkg/config"
)

func caBase64FromTLSServer(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	cert := srv.Certificate()
	if cert == nil {
		t.Fatal("server has no certificate")
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	return base64.StdEncoding.EncodeToString(pemBytes)
}

func TestVerifyAPIServerK3sModeIsNoop(t *testing.T) {
	cfg := &config.Config{
		Join: config.JoinConfig{Mode: config.JoinModeK3s},
	}
	if err := VerifyAPIServer(context.Background(), cfg); err != nil {
		t.Fatalf("k3s mode should be no-op, got %v", err)
	}
}

func TestVerifyAPIServerEmptyFieldsError(t *testing.T) {
	cfg := &config.Config{
		Join: config.JoinConfig{Mode: config.JoinModeKubelet},
	}
	err := VerifyAPIServer(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "apiServer") {
		t.Fatalf("expected apiServer error, got %v", err)
	}

	cfg.Join.APIServer = "https://example.com"
	err = VerifyAPIServer(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "clusterCA") {
		t.Fatalf("expected clusterCA error, got %v", err)
	}
}

func TestVerifyAPIServerBadBase64(t *testing.T) {
	cfg := &config.Config{
		Join: config.JoinConfig{
			Mode:      config.JoinModeKubelet,
			APIServer: "https://example.com",
			ClusterCA: "not!valid!base64!",
		},
	}
	err := VerifyAPIServer(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "base64") {
		t.Fatalf("expected base64 decode error, got %v", err)
	}
}

func TestVerifyAPIServerInvalidPEM(t *testing.T) {
	cfg := &config.Config{
		Join: config.JoinConfig{
			Mode:      config.JoinModeKubelet,
			APIServer: "https://example.com",
			ClusterCA: base64.StdEncoding.EncodeToString([]byte("not a PEM cert")),
		},
	}
	err := VerifyAPIServer(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "PEM") {
		t.Fatalf("expected PEM error, got %v", err)
	}
}

func TestVerifyAPIServerHappyPath(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/version" {
			t.Errorf("expected /version, got %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"major":"1","minor":"31","gitVersion":"v1.31.0"}`))
	}))
	defer srv.Close()

	cfg := &config.Config{
		Join: config.JoinConfig{
			Mode:      config.JoinModeKubelet,
			APIServer: srv.URL,
			ClusterCA: caBase64FromTLSServer(t, srv),
		},
	}
	if err := VerifyAPIServer(context.Background(), cfg); err != nil {
		t.Fatalf("preflight should pass against valid TLS server, got %v", err)
	}
}

func TestVerifyAPIServerAcceptsAuthRequiredCodes(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			cfg := &config.Config{
				Join: config.JoinConfig{
					Mode:      config.JoinModeKubelet,
					APIServer: srv.URL,
					ClusterCA: caBase64FromTLSServer(t, srv),
				},
			}
			if err := VerifyAPIServer(context.Background(), cfg); err != nil {
				t.Fatalf("status %d should be ok (TLS validated), got %v", status, err)
			}
		})
	}
}

func TestVerifyAPIServerRejectsServerErrors(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := &config.Config{
		Join: config.JoinConfig{
			Mode:      config.JoinModeKubelet,
			APIServer: srv.URL,
			ClusterCA: caBase64FromTLSServer(t, srv),
		},
	}
	err := VerifyAPIServer(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected 500 error, got %v", err)
	}
}

func TestVerifyAPIServerUnreachableFails(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	caB64 := caBase64FromTLSServer(t, srv)
	srv.Close() // close immediately so the URL is now unreachable

	cfg := &config.Config{
		Join: config.JoinConfig{
			Mode:      config.JoinModeKubelet,
			APIServer: srv.URL,
			ClusterCA: caB64,
		},
	}
	err := VerifyAPIServer(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error reaching closed server")
	}
	if !strings.Contains(err.Error(), "reaching api server") {
		t.Fatalf("error should be from the dial, got %v", err)
	}
}
