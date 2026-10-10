package hsclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMintAuthKeyEphemeral(t *testing.T) {
	cases := []struct {
		name      string
		respBody  string
		wantKey   string
		ephemeral bool
	}{
		{
			name:      "nested preAuthKey shape",
			respBody:  `{"preAuthKey":{"key":"hskey-auth-abc123"}}`,
			wantKey:   "hskey-auth-abc123",
			ephemeral: true,
		},
		{
			name:      "flat key fallback",
			respBody:  `{"key":"hskey-auth-flat999"}`,
			wantKey:   "hskey-auth-flat999",
			ephemeral: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotMethod, gotAuth string
			var gotReq preAuthKeyRequest

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				// Headscale 0.26: mint resolves username -> numeric id first.
				if r.URL.Path == "/api/v1/user" {
					_, _ = io.WriteString(w, `{"users":[{"id":"42","name":"tenant-acme"}]}`)
					return
				}
				gotPath = r.URL.Path
				gotMethod = r.Method
				gotAuth = r.Header.Get("Authorization")
				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &gotReq)
				_, _ = io.WriteString(w, tc.respBody)
			}))
			defer srv.Close()

			h := NewHeadscale(srv.URL, "api-key-xyz", "tenant-acme")
			key, err := h.MintAuthKeyEphemeral(context.Background(), []string{"tag:burst"}, time.Hour, tc.ephemeral)
			if err != nil {
				t.Fatalf("MintAuthKeyEphemeral: %v", err)
			}
			if key != tc.wantKey {
				t.Errorf("key = %q, want %q", key, tc.wantKey)
			}
			if gotMethod != http.MethodPost {
				t.Errorf("method = %q, want POST", gotMethod)
			}
			if gotPath != "/api/v1/preauthkey" {
				t.Errorf("path = %q, want /api/v1/preauthkey", gotPath)
			}
			if gotAuth != "Bearer api-key-xyz" {
				t.Errorf("auth = %q, want Bearer api-key-xyz", gotAuth)
			}
			// The preauthkey body must carry the NUMERIC user id resolved from
			// /api/v1/user ("42"), not the username — headscale 0.26's "user"
			// field is a uint64 and rejects the name.
			if gotReq.User != "42" {
				t.Errorf("body.user = %q, want 42 (numeric id, not name)", gotReq.User)
			}
			if !gotReq.Reusable {
				t.Errorf("body.reusable = false, want true")
			}
			if gotReq.Ephemeral != tc.ephemeral {
				t.Errorf("body.ephemeral = %v, want %v", gotReq.Ephemeral, tc.ephemeral)
			}
			if len(gotReq.ACLTags) != 1 || gotReq.ACLTags[0] != "tag:burst" {
				t.Errorf("body.aclTags = %v, want [tag:burst]", gotReq.ACLTags)
			}
			if _, err := time.Parse(time.RFC3339, gotReq.Expiration); err != nil {
				t.Errorf("body.expiration %q not RFC3339: %v", gotReq.Expiration, err)
			}
		})
	}
}

func TestMintAuthKeyDefaultsEphemeral(t *testing.T) {
	var gotReq preAuthKeyRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/user" {
			_, _ = io.WriteString(w, `{"users":[{"id":"5","name":"u"}]}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotReq)
		_, _ = io.WriteString(w, `{"preAuthKey":{"key":"k"}}`)
	}))
	defer srv.Close()

	h := NewHeadscale(srv.URL, "k", "u")
	if _, err := h.MintAuthKey(context.Background(), nil, time.Hour); err != nil {
		t.Fatalf("MintAuthKey: %v", err)
	}
	if !gotReq.Ephemeral {
		t.Errorf("MintAuthKey should mint ephemeral=true, got false")
	}
	if gotReq.User != "5" {
		t.Errorf("body.user = %q, want 5 (numeric id)", gotReq.User)
	}
}

func TestFindDeviceByHostname(t *testing.T) {
	cases := []struct {
		name     string
		respBody string
		query    string
		wantID   string
	}{
		{
			name:     "match on givenName",
			respBody: `{"nodes":[{"id":"7","name":"raw.example.com","givenName":"burst-1"}]}`,
			query:    "burst-1",
			wantID:   "7",
		},
		{
			name:     "match on name",
			respBody: `{"nodes":[{"id":"9","name":"burst-2","givenName":"munged-99"}]}`,
			query:    "burst-2",
			wantID:   "9",
		},
		{
			name:     "no match returns empty",
			respBody: `{"nodes":[{"id":"3","name":"other","givenName":"other"}]}`,
			query:    "burst-1",
			wantID:   "",
		},
		{
			name:     "empty list returns empty",
			respBody: `{"nodes":[]}`,
			query:    "burst-1",
			wantID:   "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotUser, gotMethod string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotUser = r.URL.Query().Get("user")
				gotMethod = r.Method
				_, _ = io.WriteString(w, tc.respBody)
			}))
			defer srv.Close()

			h := NewHeadscale(srv.URL, "k", "tenant-acme")
			id, err := h.FindDeviceByHostname(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("FindDeviceByHostname: %v", err)
			}
			if id != tc.wantID {
				t.Errorf("id = %q, want %q", id, tc.wantID)
			}
			if gotMethod != http.MethodGet {
				t.Errorf("method = %q, want GET", gotMethod)
			}
			if gotPath != "/api/v1/node" {
				t.Errorf("path = %q, want /api/v1/node", gotPath)
			}
			if gotUser != "tenant-acme" {
				t.Errorf("query user = %q, want tenant-acme", gotUser)
			}
		})
	}
}

func TestDeleteDevice(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{name: "200 success", status: http.StatusOK, wantErr: false},
		{name: "404 treated as success", status: http.StatusNotFound, wantErr: false},
		{name: "500 is error", status: http.StatusInternalServerError, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotMethod string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotMethod = r.Method
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			h := NewHeadscale(srv.URL, "k", "u")
			err := h.DeleteDevice(context.Background(), "42")
			if tc.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if gotMethod != http.MethodDelete {
				t.Errorf("method = %q, want DELETE", gotMethod)
			}
			if gotPath != "/api/v1/node/42" {
				t.Errorf("path = %q, want /api/v1/node/42", gotPath)
			}
		})
	}
}

func TestEnsureUser(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{name: "200 created", status: http.StatusOK, wantErr: false},
		{name: "400 already exists", status: http.StatusBadRequest, wantErr: false},
		{name: "409 already exists", status: http.StatusConflict, wantErr: false},
		{name: "500 is error", status: http.StatusInternalServerError, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotMethod string
			var gotReq userRequest
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotMethod = r.Method
				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &gotReq)
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			h := NewHeadscale(srv.URL, "k", "tenant-acme")
			err := h.EnsureUser(context.Background())
			if tc.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if gotMethod != http.MethodPost {
				t.Errorf("method = %q, want POST", gotMethod)
			}
			if gotPath != "/api/v1/user" {
				t.Errorf("path = %q, want /api/v1/user", gotPath)
			}
			if !tc.wantErr && gotReq.Name != "tenant-acme" {
				t.Errorf("body.name = %q, want tenant-acme", gotReq.Name)
			}
		})
	}
}

func TestEnsurePolicy(t *testing.T) {
	var gotMethod, gotPath, gotAuth string
	var gotReq policySetRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotReq)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	h := NewHeadscale(srv.URL, "api-key-xyz", "tenant-acme")
	err := h.EnsurePolicy(context.Background(),
		map[string][]string{
			"tag:yscale":         {"tenant-acme@", "tenant-acme@"},
			"tag:yscale-gateway": {"tenant-acme@"},
		},
		map[string][]string{
			"10.42.0.0/16":  {"tag:yscale-gateway"},
			"10.244.0.0/16": {"tag:yscale"},
		},
		[]PolicyACL{
			// Kubelet-transparency rule: unsorted Src to prove marshal-time
			// cleaning, single Dst pinned to the kubelet port.
			{Action: "accept", Proto: "tcp", Src: []string{"10.42.0.0/16", "10.0.0.0/24"}, Dst: []string{"tag:yscale:10250"}},
			// Empty-Src rule must be dropped, not marshaled as an invalid ACL.
			{Action: "accept", Src: nil, Dst: []string{"tag:yscale:10250"}},
		})
	if err != nil {
		t.Fatalf("EnsurePolicy: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if gotPath != "/api/v1/policy" {
		t.Errorf("path = %q, want /api/v1/policy", gotPath)
	}
	if gotAuth != "Bearer api-key-xyz" {
		t.Errorf("auth = %q, want Bearer api-key-xyz", gotAuth)
	}
	if gotReq.Policy == "" {
		t.Fatal("body.policy is empty; Headscale 0.26 expects policy text in this field")
	}

	var policy struct {
		TagOwners     map[string][]string `json:"tagOwners"`
		AutoApprovers struct {
			Routes map[string][]string `json:"routes"`
		} `json:"autoApprovers"`
		ACLs []headscalePolicyACL `json:"acls"`
	}
	if err := json.Unmarshal([]byte(gotReq.Policy), &policy); err != nil {
		t.Fatalf("body.policy is not JSON policy text: %v", err)
	}
	if got := policy.TagOwners["tag:yscale"]; len(got) != 1 || got[0] != "tenant-acme@" {
		t.Errorf("tag owner = %v, want [tenant-acme@]", got)
	}
	if got := policy.AutoApprovers.Routes["10.42.0.0/16"]; len(got) != 1 || got[0] != "tag:yscale-gateway" {
		t.Errorf("gateway route approver = %v, want [tag:yscale-gateway]", got)
	}
	if got := policy.AutoApprovers.Routes["10.244.0.0/16"]; len(got) != 1 || got[0] != "tag:yscale" {
		t.Errorf("burst route approver = %v, want [tag:yscale]", got)
	}
	// Mesh rule, route-distribution rule, and the ONE kubelet rule; the
	// empty-Src extra ACL is dropped, not marshaled.
	if len(policy.ACLs) != 3 {
		t.Fatalf("acls len = %d, want 3 (mesh, route-dist, kubelet)", len(policy.ACLs))
	}
	var rawPolicy struct {
		ACLs []map[string]json.RawMessage `json:"acls"`
	}
	if err := json.Unmarshal([]byte(gotReq.Policy), &rawPolicy); err != nil {
		t.Fatalf("body.policy is not JSON policy text: %v", err)
	}
	for i, name := range []string{"mesh", "route-distribution"} {
		if _, ok := rawPolicy.ACLs[i]["proto"]; ok {
			t.Errorf("%s ACL unexpectedly marshaled a proto field", name)
		}
	}
	if got := policy.ACLs[1].Dst; len(got) != 2 || got[0] != "10.244.0.0/16:*" || got[1] != "10.42.0.0/16:*" {
		t.Errorf("route ACL dst = %v, want sorted route CIDR destinations", got)
	}
	// Kubelet ACL: Src cleaned+sorted, Dst pinned to the burst/agent tag on
	// :10250 only (never a wildcard port, never the gateway tag).
	kube := policy.ACLs[2]
	if kube.Action != "accept" {
		t.Errorf("kubelet ACL action = %q, want accept", kube.Action)
	}
	if kube.Proto != "tcp" {
		t.Errorf("kubelet ACL proto = %q, want tcp", kube.Proto)
	}
	if len(kube.Src) != 2 || kube.Src[0] != "10.0.0.0/24" || kube.Src[1] != "10.42.0.0/16" {
		t.Errorf("kubelet ACL src = %v, want sorted [10.0.0.0/24 10.42.0.0/16]", kube.Src)
	}
	if len(kube.Dst) != 1 || kube.Dst[0] != "tag:yscale:10250" {
		t.Errorf("kubelet ACL dst = %v, want [tag:yscale:10250] only", kube.Dst)
	}
}

// The raw wire shape of the node-level approval: POST to the node's
// approve_routes path, bearer auth, and a body whose "routes" is the exact set
// the caller asked for after trimming, de-duplication and sorting.
func TestApproveNodeRoutes(t *testing.T) {
	cases := []struct {
		name     string
		nodeID   string
		routes   []string
		wantPath string
		wantBody string
	}{
		{
			name:     "exact set",
			nodeID:   "7",
			routes:   []string{"10.42.0.0/16", "10.96.0.0/12"},
			wantPath: "/api/v1/node/7/approve_routes",
			wantBody: `{"routes":["10.42.0.0/16","10.96.0.0/12"]}`,
		},
		{
			name:     "trimmed, de-duped and sorted",
			nodeID:   "12",
			routes:   []string{" 10.96.0.0/12 ", "10.42.0.0/16", "", "10.96.0.0/12"},
			wantPath: "/api/v1/node/12/approve_routes",
			wantBody: `{"routes":["10.42.0.0/16","10.96.0.0/12"]}`,
		},
		{
			// Withdrawing the last approval must send an empty ARRAY. A null
			// here would leave the node's stale approvals standing. This is a
			// client-layer contract only: central's reconciler never asks for
			// it, because an empty stored route set means "nothing reported
			// yet", not "withdraw everything".
			name:     "empty set clears approvals",
			nodeID:   "7",
			routes:   nil,
			wantPath: "/api/v1/node/7/approve_routes",
			wantBody: `{"routes":[]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotMethod, gotPath, gotAuth, gotBody string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotPath, gotAuth = r.Method, r.URL.Path, r.Header.Get("Authorization")
				body, _ := io.ReadAll(r.Body)
				gotBody = string(body)
				_, _ = io.WriteString(w, `{}`)
			}))
			defer srv.Close()

			h := NewHeadscale(srv.URL, "api-key-xyz", "tenant-acme")
			if err := h.ApproveNodeRoutes(context.Background(), tc.nodeID, tc.routes); err != nil {
				t.Fatalf("ApproveNodeRoutes: %v", err)
			}
			if gotMethod != http.MethodPost {
				t.Errorf("method = %q, want POST", gotMethod)
			}
			if gotPath != tc.wantPath {
				t.Errorf("path = %q, want %q", gotPath, tc.wantPath)
			}
			if gotAuth != "Bearer api-key-xyz" {
				t.Errorf("authorization header = %q, want bearer api key", gotAuth)
			}
			if gotBody != tc.wantBody {
				t.Errorf("body = %s, want %s", gotBody, tc.wantBody)
			}
		})
	}
}

// A node id is never interpolated raw into the path, and an empty one is
// refused before any request goes out — approving "all nodes" is not a thing
// a typo should be able to ask for.
func TestApproveNodeRoutesRejectsEmptyNodeID(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	h := NewHeadscale(srv.URL, "k", "u")
	if err := h.ApproveNodeRoutes(context.Background(), "  ", []string{"10.42.0.0/16"}); err == nil {
		t.Fatalf("expected an error for an empty node id")
	}
	if requests != 0 {
		t.Fatalf("requests = %d, want 0 (nothing sent for an empty node id)", requests)
	}
}

// A non-2xx from the box is an error the reconciler must see, not a silent
// "approved".
func TestApproveNodeRoutesPropagatesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"message":"node not found"}`)
	}))
	defer srv.Close()

	h := NewHeadscale(srv.URL, "k", "u")
	err := h.ApproveNodeRoutes(context.Background(), "7", []string{"10.42.0.0/16"})
	if err == nil {
		t.Fatalf("expected an error on a 500 response")
	}
	if !isHTTPStatus(err, http.StatusInternalServerError) {
		t.Fatalf("err = %v, want an *httpError carrying 500", err)
	}
}

func TestLoginServerTrimsTrailingSlash(t *testing.T) {
	h := NewHeadscale("https://hs.example.com/", "k", "u")
	if got := h.LoginServer(); got != "https://hs.example.com" {
		t.Errorf("LoginServer = %q, want https://hs.example.com", got)
	}
}

func TestNewHeadscaleTrimsMultipleSlashes(t *testing.T) {
	h := NewHeadscale("https://hs.example.com///", "k", "u")
	if !strings.HasSuffix(h.LoginServer(), ".com") {
		t.Errorf("LoginServer = %q, want trailing slashes trimmed", h.LoginServer())
	}
}
