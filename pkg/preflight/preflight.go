// Package preflight runs early validation against the configured cluster
// before the controller starts its reconcile loop.
//
// The most important check is VerifyAPIServer: dial the configured
// join.apiServer URL using join.clusterCA, fail loud if either is wrong.
// Without this, a typo in the API endpoint or a stale CA leaves the
// controller running happily while every burst node silently fails to
// join — a debugging nightmare.
package preflight

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/pkg/config"
)

// VerifyAPIServer ensures the configured Join.APIServer is reachable and
// presents a TLS certificate signed by the configured Join.ClusterCA.
//
// Only meaningful when join.mode = kubelet. K3s mode points burst nodes at
// a separate K3s server URL whose verification happens on the burst node
// itself when the k3s agent connects, so we skip it here.
//
// Returns nil if the API server responds (any HTTP status counts — we only
// care that the TLS handshake validated against the supplied CA).
func VerifyAPIServer(ctx context.Context, cfg *config.Config) error {
	if cfg.Join.Mode != config.JoinModeKubelet {
		return nil
	}
	if cfg.Join.APIServer == "" {
		return fmt.Errorf("join.apiServer is empty")
	}
	if cfg.Join.ClusterCA == "" {
		return fmt.Errorf("join.clusterCA is empty")
	}

	caBytes, err := base64.StdEncoding.DecodeString(cfg.Join.ClusterCA)
	if err != nil {
		return fmt.Errorf("decoding join.clusterCA as base64: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return fmt.Errorf("join.clusterCA is not a valid PEM certificate (after base64 decode)")
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS12,
			},
		},
	}

	versionURL := strings.TrimRight(cfg.Join.APIServer, "/") + "/version"
	req, err := http.NewRequestWithContext(ctx, "GET", versionURL, nil)
	if err != nil {
		return fmt.Errorf("building preflight request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("reaching api server %s: %w (verify join.apiServer URL and join.clusterCA — see 'yscale init')", cfg.Join.APIServer, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	// /version is unauthenticated on most clusters, but some lock it down.
	// 401/403 still proves TLS handshake against this CA succeeded, which
	// is all we need.
	switch {
	case resp.StatusCode == 200,
		resp.StatusCode == 401,
		resp.StatusCode == 403,
		resp.StatusCode == 404: // some clusters disable /version entirely
		return nil
	default:
		return fmt.Errorf("api server %s returned unexpected status %d on /version", cfg.Join.APIServer, resp.StatusCode)
	}
}
