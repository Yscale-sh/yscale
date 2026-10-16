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
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
			var bodies []string
			request := func() int {
				rec := httptest.NewRecorder()
				s.handleBootstrap(rec, httptest.NewRequest(http.MethodPost, "/bootstrap-kubeconfig", strings.NewReader(`{"burst_id":"burst_aaaa","node_name":"ys-burst-aaaa"}`)))
				if rec.Code == http.StatusOK {
					bodies = append(bodies, rec.Body.String())
				}
				return rec.Code
			}
			if got := request(); got < 500 {
				t.Fatalf("failed mint status = %d", got)
			}
			if got := request(); got != http.StatusOK {
				t.Fatalf("retry status = %d", got)
			}
			// A client retry before the node registers replays the minted body verbatim.
			if got := request(); got != http.StatusOK {
				t.Fatalf("pre-registration retry status = %d, want 200 replay", got)
			}
			if len(bodies) != 2 || bodies[0] != bodies[1] {
				t.Fatalf("replay body differs from the minted body")
			}
			// Once the Node exists, the grant locks: pods could now run on it.
			if _, err := kube.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ys-burst-aaaa"}}, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if got := request(); got != http.StatusForbidden {
				t.Errorf("post-registration replay status = %d, want 403", got)
			}
			s.AuthorizeBurst("burst_aaaa", "full", "workload", nil)
			if got := request(); got != http.StatusForbidden {
				t.Errorf("re-announced replay status = %d, want 403", got)
			}
			// An unregistered node past the replay window is refused too.
			s.mu.Lock()
			s.grants["burst_aaaa"].mintedAt = time.Now().Add(-bootstrapReplayWindow - time.Minute)
			s.mu.Unlock()
			if err := kube.CoreV1().Nodes().Delete(context.Background(), "ys-burst-aaaa", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if got := request(); got != http.StatusForbidden {
				t.Errorf("expired replay status = %d, want 403", got)
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
	var mu sync.Mutex
	mints := 0
	kube.PrependReactor("create", "secrets", func(ktesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		mints++
		mu.Unlock()
		return false, nil, nil
	})
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
	for code := range results {
		if code != http.StatusOK && code != http.StatusForbidden && code != http.StatusServiceUnavailable {
			t.Errorf("unexpected status %d", code)
		}
	}
	// Concurrent requests may be served the minted body (replay) but only one mint happens.
	if mints != 1 {
		t.Fatalf("bootstrap-token mints = %d, want 1", mints)
	}
}
