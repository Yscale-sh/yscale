package factoryclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/factory/client"
)

func TestMeshRPCsSpeakTheFactoryProtocol(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fyk" {
			t.Errorf("missing bearer on %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get(MeshLoginServerHeader) != "https://box.example" {
			t.Errorf("%s %s not pinned to the recorded box: %q", r.Method, r.URL.Path, r.Header.Get(MeshLoginServerHeader))
		}
		seen = append(seen, r.Method+" "+r.URL.RequestURI())
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tenants/acme/mesh/keys":
			var body struct {
				Tags          []string `json:"tags"`
				ExpirySeconds int64    `json:"expiry_seconds"`
				Ephemeral     bool     `json:"ephemeral"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ExpirySeconds != 1800 || !body.Ephemeral || body.Tags[0] != "tag:yscale" {
				t.Errorf("mint body = %+v, %v", body, err)
			}
			_, _ = w.Write([]byte(`{"auth_key":"hskey-1","login_server":"https://box.example"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants/acme/mesh/devices":
			_, _ = w.Write([]byte(`{"id":"42"}`))
		case r.Method == http.MethodPut && r.URL.Path == "/v1/tenants/acme/mesh/policy":
			var body struct {
				ExtraACLs []client.PolicyACL `json:"extra_acls"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.ExtraACLs) != 1 || body.ExtraACLs[0].Proto != "tcp" {
				t.Errorf("policy body = %+v, %v", body, err)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()

	m := NewTenantMesh(New(srv.URL, "fyk"), "acme", "https://box.example")
	ctx := context.Background()
	if key, err := m.MintAuthKey(ctx, []string{"tag:yscale"}, 30*time.Minute); err != nil || key != "hskey-1" {
		t.Fatalf("MintAuthKey = %q, %v", key, err)
	}
	if id, err := m.FindDeviceByHostname(ctx, "ys-burst 1"); err != nil || id != "42" {
		t.Fatalf("FindDeviceByHostname = %q, %v", id, err)
	}
	if err := m.DeleteDevice(ctx, "42"); err != nil {
		t.Fatal(err)
	}
	if err := m.ApproveNodeRoutes(ctx, "42", []string{"10.42.0.0/16"}); err != nil {
		t.Fatal(err)
	}
	if err := m.EnsurePolicy(ctx, nil, nil, []client.PolicyACL{{Action: "accept", Proto: "tcp"}}); err != nil {
		t.Fatal(err)
	}
	if m.LoginServer() != "https://box.example" {
		t.Fatalf("LoginServer = %q", m.LoginServer())
	}
	want := []string{
		"POST /v1/tenants/acme/mesh/keys",
		"GET /v1/tenants/acme/mesh/devices?hostname=ys-burst+1",
		"DELETE /v1/tenants/acme/mesh/devices/42",
		"POST /v1/tenants/acme/mesh/devices/42/routes",
		"PUT /v1/tenants/acme/mesh/policy",
	}
	if strings.Join(seen, "|") != strings.Join(want, "|") {
		t.Fatalf("requests = %v\nwant %v", seen, want)
	}
}

func TestMeshRPCErrorsAreTypedAndBodyFree(t *testing.T) {
	status := http.StatusConflict
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"status":"provisioning","leak":"box-admin-key"}`))
	}))
	defer srv.Close()
	m := NewTenantMesh(New(srv.URL, "fyk"), "acme", "https://box.example")

	if _, err := m.MintAuthKey(context.Background(), []string{"tag:yscale"}, time.Minute); !errors.Is(err, ErrFabricProvisioning) {
		t.Fatalf("409 = %v, want ErrFabricProvisioning", err)
	}
	status = http.StatusBadGateway
	_, err := m.MintAuthKey(context.Background(), []string{"tag:yscale"}, time.Minute)
	if err == nil || strings.Contains(err.Error(), "box-admin-key") {
		t.Fatalf("502 = %v, want a body-free error", err)
	}
}

// A key from a box other than the recorded one would join a coordinator
// central's teardown no longer resolves, so it is refused.
func TestTenantMeshRefusesAKeyFromAnotherBox(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"auth_key":"hskey-1","login_server":"https://replacement.example"}`))
	}))
	defer srv.Close()
	m := NewTenantMesh(New(srv.URL, "fyk"), "acme", "https://box.example")
	if key, err := m.MintAuthKey(context.Background(), []string{"tag:yscale"}, time.Minute); err == nil || key != "" {
		t.Fatalf("MintAuthKey = %q, %v; want refusal", key, err)
	}
}
