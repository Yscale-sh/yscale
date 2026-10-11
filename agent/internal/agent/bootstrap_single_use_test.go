package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type retryBootstrapIssuer struct{ attempts int }

func (i *retryBootstrapIssuer) Issue(context.Context, string) (string, error) {
	i.attempts++
	if i.attempts == 1 {
		return "", errors.New("temporary failure")
	}
	return "token", nil
}

func TestBootstrapSingleUse(t *testing.T) {
	for _, customIssuer := range []bool{false, true} {
		name := "secret"
		if customIssuer {
			name = "issuer"
		}
		t.Run(name, func(t *testing.T) {
			kube := fake.NewSimpleClientset()
			attempts, ciliumMints := 0, 0
			kube.PrependReactor("create", "secrets", func(ktesting.Action) (bool, runtime.Object, error) {
				attempts++
				if attempts == 1 {
					return true, nil, errors.New("temporary failure")
				}
				return false, nil, nil
			})
			kube.PrependReactor("create", "serviceaccounts", func(ktesting.Action) (bool, runtime.Object, error) {
				ciliumMints++
				return true, &authenticationv1.TokenRequest{Status: authenticationv1.TokenRequestStatus{Token: "cilium-token"}}, nil
			})
			s := &BootstrapServer{K8s: kube, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), APIServer: "https://api", grants: map[string]*burstGrant{}, nodes: map[string]string{}}
			issuer := &retryBootstrapIssuer{}
			if customIssuer {
				s.TokenIssuer = issuer
			}
			s.AuthorizeBurst("burst_aaaa", "full", "workload", nil)
			request := func() int {
				rec := httptest.NewRecorder()
				s.handleBootstrap(rec, httptest.NewRequest(http.MethodPost, "/bootstrap-kubeconfig", strings.NewReader(`{"burst_id":"burst_aaaa","node_name":"ys-burst-aaaa"}`)))
				return rec.Code
			}
			if got := request(); got < 500 {
				t.Fatalf("failed mint status = %d", got)
			}
			if got := request(); got != http.StatusOK {
				t.Fatalf("retry status = %d", got)
			}
			if got := request(); got != http.StatusForbidden {
				t.Errorf("replay status = %d, want 403", got)
			}
			s.AuthorizeBurst("burst_aaaa", "full", "workload", nil)
			if got := request(); got != http.StatusForbidden {
				t.Errorf("re-announced replay status = %d, want 403", got)
			}
			if ciliumMints != 1 {
				t.Errorf("Cilium mints = %d, want 1", ciliumMints)
			}
			if customIssuer && issuer.attempts != 2 {
				t.Errorf("issuer attempts = %d, want 2", issuer.attempts)
			}
			if !customIssuer && attempts != 2 {
				t.Errorf("secret attempts = %d, want 2", attempts)
			}
			if ns, _, ok := s.authorizedStorage("burst_aaaa"); !ok || ns != "workload" {
				t.Error("single use revoked storage access")
			}
			s.ForgetNode("ys-burst-aaaa")
			if _, _, ok := s.authorizedStorage("burst_aaaa"); ok {
				t.Error("ForgetNode failed")
			}
		})
	}
}

func TestBootstrapConcurrentSingleUse(t *testing.T) {
	kube := fake.NewSimpleClientset()
	s := &BootstrapServer{K8s: kube, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), APIServer: "https://api", grants: map[string]*burstGrant{}, nodes: map[string]string{}}
	s.AuthorizeBurst("burst_aaaa", "full", "default", nil)
	var wg sync.WaitGroup
	results := make(chan int, 16)
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			s.handleBootstrap(rec, httptest.NewRequest(http.MethodPost, "/bootstrap-kubeconfig", strings.NewReader(`{"burst_id":"burst_aaaa","node_name":"ys-burst-aaaa"}`)))
			results <- rec.Code
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for code := range results {
		if code == http.StatusOK {
			successes++
		} else if code != http.StatusForbidden && code != http.StatusServiceUnavailable {
			t.Errorf("unexpected status %d", code)
		}
	}
	if successes != 1 {
		t.Fatalf("successful mints = %d, want 1", successes)
	}
}
