package hostedcontroller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientPinsAdminContract(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.EscapedPath())
		if r.Header.Get("Authorization") != "Bearer adm-secret" || r.Header.Get("Accept") != "application/json" {
			t.Errorf("headers = %v", r.Header)
		}
		if r.Method == http.MethodPost && r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type = %q", r.Header.Get("Content-Type"))
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/admin/hosted-clusters":
			fmt.Fprintf(w, `{"clusters":[{"tenant_id":"cust-a","plan":"pro","cluster":{"cluster_id":"hosted-a","source":"hosted","state":"connected","hosted_namespace":"ys-a","registered_at":%q}}]}`, now)
		case "/v1/admin/hosted-capacity/requests":
			fmt.Fprintf(w, `{"requests":[{"tenant_id":"cust-b","plan":"pro","requested_at":%q}]}`, now)
		case "/v1/admin/tenants/cust-b/hosted-clusters":
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, credentialJSON("cust-b", "hosted-b", "ys-b", now, "one-time"))
		case "/v1/admin/tenants/cust-b/hosted-clusters/hosted-b/credential":
			fmt.Fprint(w, credentialJSON("cust-b", "hosted-b", "ys-b", now, "rotated"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c, err := NewClient(srv.URL, "adm-secret", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.ListInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ListRequests(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Assign(context.Background(), "cust-b"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Rotate(context.Background(), "cust-b", "hosted-b"); err != nil {
		t.Fatal(err)
	}
	want := []string{"GET /v1/admin/hosted-clusters", "GET /v1/admin/hosted-capacity/requests", "POST /v1/admin/tenants/cust-b/hosted-clusters", "POST /v1/admin/tenants/cust-b/hosted-clusters/hosted-b/credential"}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
}

func credentialJSON(tenant, cluster, namespace, now, token string) string {
	return fmt.Sprintf(`{"tenant_id":%q,"cluster":{"cluster_id":%q,"source":"hosted","state":"connected","hosted_namespace":%q,"registered_at":%q},"connector_token":%q,"helm_release":"yscale-agent-%s","connector_rbac_set":"x","helm_command":"x"}`, tenant, cluster, namespace, now, token, cluster)
}

func TestClientRefusesRedirectOversizeUnknownAndRedactsToken(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		token   string
	}{
		{"redirect", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/elsewhere", http.StatusFound) }, "adm"},
		{"oversize", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"clusters":"` + strings.Repeat("x", maxResponseBytes) + `"}`))
		}, "adm"},
		{"unknown", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"clusters":[],"extra":true}`)) }, "adm"},
		{"token", func(w http.ResponseWriter, r *http.Request) { panic("server should not be reached") }, "http://bad token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			c, err := NewClient(srv.URL, tc.token, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.ListInventory(context.Background())
			if err == nil {
				t.Fatal("expected refusal")
			}
			if strings.Contains(err.Error(), tc.token) {
				t.Fatalf("error leaked token: %v", err)
			}
		})
	}
}
