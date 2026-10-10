package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/factory/internal/boxes"
	"github.com/yscale-sh/yscale/factory/internal/hsclient"
	"github.com/yscale-sh/yscale/factory/internal/store"
)

type fakeMeshBox struct {
	gotLogin, gotKey, gotUser string
	calls                     []string
	mintTags                  []string
	mintExpiry                time.Duration
	mintEphemeral             bool
	policyOwners              map[string][]string
	policyACLs                []hsclient.PolicyACL
	fail                      error
}

func (f *fakeMeshBox) MintAuthKeyEphemeral(_ context.Context, tags []string, expiry time.Duration, ephemeral bool) (string, error) {
	f.calls = append(f.calls, "mint")
	f.mintTags, f.mintExpiry, f.mintEphemeral = tags, expiry, ephemeral
	return "hskey-auth-test", f.fail
}
func (f *fakeMeshBox) FindDeviceByHostname(_ context.Context, hostname string) (string, error) {
	f.calls = append(f.calls, "find:"+hostname)
	if hostname == "ghost" {
		return "", f.fail
	}
	return "42", f.fail
}
func (f *fakeMeshBox) DeleteDevice(_ context.Context, id string) error {
	f.calls = append(f.calls, "delete:"+id)
	return f.fail
}
func (f *fakeMeshBox) ApproveNodeRoutes(_ context.Context, id string, routes []string) error {
	f.calls = append(f.calls, "approve:"+id+":"+strings.Join(routes, ","))
	return f.fail
}
func (f *fakeMeshBox) EnsurePolicy(_ context.Context, owners, _ map[string][]string, acls []hsclient.PolicyACL) error {
	f.calls = append(f.calls, "policy")
	f.policyOwners, f.policyACLs = owners, acls
	return f.fail
}

// meshFixture stages a tenant fabric the way the Linode worker does and
// returns a handler whose box constructor records what it was handed.
func meshFixture(t *testing.T, status string) (http.Handler, *fakeMeshBox) {
	t.Helper()
	s, err := store.NewWithKEK(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	_, staged, err := s.EnsureFabric("tenant-a", "k1")
	if err != nil {
		t.Fatal(err)
	}
	login := testBoxLogin
	if err := s.PromoteFabric(staged.LoginServer, store.Box{
		TenantID: "tenant-a", LoginServer: login, HSUser: "tenant-a", Backend: "linode", BackendID: "9",
	}, "box-admin-key"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetBoxStatus(login, status); err != nil {
		t.Fatal(err)
	}
	fake := &fakeMeshBox{}
	h := newHandler(s, boxes.NewFakeWorker(s), "test-token", func(l, k, u string) meshBox {
		fake.gotLogin, fake.gotKey, fake.gotUser = l, k, u
		return fake
	})
	return h, fake
}

const testBoxLogin = "https://203-0-113-9.ip.linodeusercontent.com"

func meshDo(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	return meshDoAt(h, testBoxLogin, method, path, body)
}

func meshDoAt(h http.Handler, login, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	if login != "" {
		req.Header.Set(meshLoginServerHeader, login)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestMeshMintUsesTheBoxCredentialInsideTheFactory(t *testing.T) {
	h, fake := meshFixture(t, store.StatusReady)
	rec := meshDo(h, http.MethodPost, "/v1/tenants/tenant-a/mesh/keys",
		`{"tags":["tag:yscale-burst"],"expiry_seconds":1800,"ephemeral":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("mint status = %d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["auth_key"] != "hskey-auth-test" || got["login_server"] != fake.gotLogin {
		t.Fatalf("mint response = %v", got)
	}
	if fake.gotKey != "box-admin-key" || fake.gotUser != "tenant-a" {
		t.Fatalf("box client built with key=%q user=%q", fake.gotKey, fake.gotUser)
	}
	if strings.Contains(rec.Body.String(), "box-admin-key") {
		t.Fatal("box admin key leaked in a response")
	}
	if fake.mintExpiry != 30*time.Minute || !fake.mintEphemeral || len(fake.mintTags) != 1 {
		t.Fatalf("mint args tags=%v expiry=%v ephemeral=%v", fake.mintTags, fake.mintExpiry, fake.mintEphemeral)
	}
}

func TestMeshDeviceRoutesAndPolicy(t *testing.T) {
	h, fake := meshFixture(t, store.StatusReady)
	rec := meshDo(h, http.MethodGet, "/v1/tenants/tenant-a/mesh/devices?hostname=ys-burst-1", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"id":"42"`) {
		t.Fatalf("find = %d %s", rec.Code, rec.Body.String())
	}
	if rec := meshDo(h, http.MethodDelete, "/v1/tenants/tenant-a/mesh/devices/42", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d", rec.Code)
	}
	if rec := meshDo(h, http.MethodPost, "/v1/tenants/tenant-a/mesh/devices/42/routes",
		`{"routes":["10.42.0.0/16","10.0.0.0/24"]}`); rec.Code != http.StatusNoContent {
		t.Fatalf("approve = %d", rec.Code)
	}
	if rec := meshDo(h, http.MethodPut, "/v1/tenants/tenant-a/mesh/policy",
		`{"tag_owners":{"tag:yscale":["tenant-a@"]},"route_approvers":{},"extra_acls":[{"Action":"accept","Proto":"tcp","Src":["10.42.0.0/16"],"Dst":["tag:yscale-burst:10250"]}]}`); rec.Code != http.StatusNoContent {
		t.Fatalf("policy = %d", rec.Code)
	}
	want := []string{"find:ys-burst-1", "delete:42", "approve:42:10.42.0.0/16,10.0.0.0/24", "policy"}
	if strings.Join(fake.calls, "|") != strings.Join(want, "|") {
		t.Fatalf("calls = %v, want %v", fake.calls, want)
	}
	if len(fake.policyACLs) != 1 || fake.policyACLs[0].Dst[0] != "tag:yscale-burst:10250" || len(fake.policyOwners["tag:yscale"]) != 1 {
		t.Fatalf("policy decoded owners=%v acls=%+v", fake.policyOwners, fake.policyACLs)
	}
}

func TestMeshRejectsInvalidRequestsBeforeTouchingTheBox(t *testing.T) {
	h, fake := meshFixture(t, store.StatusReady)
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/tenants/tenant-a/mesh/keys", `{"tags":[],"expiry_seconds":1800}`},
		{http.MethodPost, "/v1/tenants/tenant-a/mesh/keys", `{"tags":["yscale"],"expiry_seconds":1800}`},
		{http.MethodPost, "/v1/tenants/tenant-a/mesh/keys", `{"tags":["tag:yscale"],"expiry_seconds":5}`},
		{http.MethodPost, "/v1/tenants/tenant-a/mesh/keys", `{"tags":["tag:yscale"],"expiry_seconds":1800,"admin":true}`},
		{http.MethodDelete, "/v1/tenants/tenant-a/mesh/devices/1;drop", ""},
		{http.MethodPost, "/v1/tenants/tenant-a/mesh/devices/42/routes", `{"routes":["not-a-cidr"]}`},
		{http.MethodGet, "/v1/tenants/tenant-a/mesh/devices", ""},
	} {
		if rec := meshDo(h, tc.method, tc.path, tc.body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s %s %s = %d, want 400", tc.method, tc.path, tc.body, rec.Code)
		}
	}
	if len(fake.calls) != 0 {
		t.Fatalf("invalid requests reached the box: %v", fake.calls)
	}
}

func TestMeshRequiresAReadyFabricAndAuth(t *testing.T) {
	h, fake := meshFixture(t, store.StatusProvisioning)
	if rec := meshDo(h, http.MethodPost, "/v1/tenants/tenant-a/mesh/keys",
		`{"tags":["tag:yscale"],"expiry_seconds":1800}`); rec.Code != http.StatusConflict {
		t.Fatalf("provisioning fabric mint = %d, want 409", rec.Code)
	}
	if rec := meshDo(h, http.MethodPost, "/v1/tenants/other/mesh/keys",
		`{"tags":["tag:yscale"],"expiry_seconds":1800}`); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown tenant mint = %d, want 404", rec.Code)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-a/mesh/keys", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad bearer = %d, want 401", rec.Code)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("box touched: %v", fake.calls)
	}
}

func TestMeshBoxFailureIsGeneric(t *testing.T) {
	h, fake := meshFixture(t, store.StatusReady)
	fake.fail = errors.New("headscale said: secret detail")
	rec := meshDo(h, http.MethodPost, "/v1/tenants/tenant-a/mesh/keys", `{"tags":["tag:yscale"],"expiry_seconds":1800}`)
	if rec.Code != http.StatusBadGateway || strings.Contains(rec.Body.String(), "secret detail") {
		t.Fatalf("box failure = %d %s", rec.Code, rec.Body.String())
	}
}

// Every RPC is pinned to the box central expects: another login server (for
// example a replacement box) or a missing pin never reaches a box.
func TestMeshRPCsArePinnedToTheExpectedBox(t *testing.T) {
	h, fake := meshFixture(t, store.StatusReady)
	if rec := meshDoAt(h, "https://replacement.example", http.MethodDelete, "/v1/tenants/tenant-a/mesh/devices/42", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("wrong box delete = %d, want 404", rec.Code)
	}
	if rec := meshDoAt(h, "", http.MethodPost, "/v1/tenants/tenant-a/mesh/keys", `{"tags":["tag:yscale"],"expiry_seconds":1800}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unpinned mint = %d, want 400", rec.Code)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("box touched: %v", fake.calls)
	}
}

// A draining box still serves cleanup (find/delete) but no new keys or policy.
func TestMeshDrainingBoxServesCleanupOnly(t *testing.T) {
	h, fake := meshFixture(t, store.StatusDecommissioning)
	if rec := meshDo(h, http.MethodDelete, "/v1/tenants/tenant-a/mesh/devices/42", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("draining delete = %d, want 204", rec.Code)
	}
	if rec := meshDo(h, http.MethodPost, "/v1/tenants/tenant-a/mesh/keys", `{"tags":["tag:yscale"],"expiry_seconds":1800}`); rec.Code != http.StatusConflict {
		t.Fatalf("draining mint = %d, want 409", rec.Code)
	}
	if strings.Join(fake.calls, "|") != "delete:42" {
		t.Fatalf("calls = %v", fake.calls)
	}
}
