package boxes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testLinodeToken = "test-linode-token"

func assertLinodeHeaders(t *testing.T, r *http.Request) {
	t.Helper()
	if got, want := r.Header.Get("Authorization"), "Bearer "+testLinodeToken; got != want {
		t.Fatalf("Authorization = %q, want %q", got, want)
	}
	if got := r.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestLinodeHTTPEnsureFirewallReusesMatchingLabel(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		assertLinodeHeaders(t, r)
		if r.Method != http.MethodGet || r.URL.Path != "/networking/firewalls" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"data":[{"id":42,"label":"yscale-headscale-fw"}]}`))
	}))
	defer server.Close()

	id, err := NewLinodeHTTP(testLinodeToken, WithBaseURL(server.URL)).EnsureFirewall(context.Background(), "yscale-headscale-fw", []string{"yscale-headscale"})
	if err != nil || id != "42" {
		t.Fatalf("EnsureFirewall() = %q, %v; want 42, nil", id, err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
}

func TestLinodeHTTPEnsureFirewallCreatesDefaultDropRules(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertLinodeHeaders(t, r)
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"data":[]}`))
		case http.MethodPost:
			var body struct {
				Rules struct {
					InboundPolicy string `json:"inbound_policy"`
					Inbound       []struct {
						Ports string `json:"ports"`
					} `json:"inbound"`
				} `json:"rules"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Rules.InboundPolicy != "DROP" || len(body.Rules.Inbound) != 4 {
				t.Fatalf("unexpected firewall rules: %+v", body.Rules)
			}
			if got := []string{body.Rules.Inbound[0].Ports, body.Rules.Inbound[1].Ports, body.Rules.Inbound[2].Ports, body.Rules.Inbound[3].Ports}; strings.Join(got, ",") != "443,80,3478,41641,22" {
				t.Fatalf("rule ports = %v", got)
			}
			_, _ = w.Write([]byte(`{"id":43,"label":"yscale-headscale-fw"}`))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	id, err := NewLinodeHTTP(testLinodeToken, WithBaseURL(server.URL)).EnsureFirewall(context.Background(), "yscale-headscale-fw", []string{"yscale-headscale"})
	if err != nil || id != "43" {
		t.Fatalf("EnsureFirewall() = %q, %v; want 43, nil", id, err)
	}
}

func TestLinodeHTTPCreateInstance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertLinodeHeaders(t, r)
		if r.Method != http.MethodPost || r.URL.Path != "/linode/instances" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			Tags       []string        `json:"tags"`
			FirewallID json.RawMessage `json:"firewall_id"`
			Metadata   struct {
				UserData string `json:"user_data"`
			} `json:"metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if !hasExactTag(body.Tags, "yscale-headscale") || body.Metadata.UserData == "" {
			t.Fatalf("create body missing tag or user_data: %+v", body)
		}
		// Linode rejects a quoted firewall_id ("Must be of type Integer").
		if string(body.FirewallID) != "43" {
			t.Fatalf("firewall_id = %s, want the JSON integer 43", body.FirewallID)
		}
		_, _ = w.Write([]byte(`{"id":44,"status":"provisioning","ipv4":["203.0.113.1"],"tags":["yscale-headscale"],"label":"headscale-acme"}`))
	}))
	defer server.Close()

	instance, err := NewLinodeHTTP(testLinodeToken, WithBaseURL(server.URL)).CreateInstance(context.Background(), InstanceSpec{
		Label: "headscale-acme", Region: "us-ord", Type: "g6-nanode-1", Image: "linode/debian12",
		RootPass: "root-password", UserDataB64: "dXNlci1kYXRh", FirewallID: "43", Tags: []string{"yscale-headscale"},
	})
	if err != nil || instance.ID != "44" || instance.Status != "provisioning" || instance.Label != "headscale-acme" || len(instance.IPv4) != 1 {
		t.Fatalf("CreateInstance() = %+v, %v", instance, err)
	}
}

func TestLinodeHTTPGetInstance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertLinodeHeaders(t, r)
		if r.Method != http.MethodGet || r.URL.Path != "/linode/instances/45" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"id":45,"status":"running","ipv4":["203.0.113.2"]}`))
	}))
	defer server.Close()

	instance, err := NewLinodeHTTP(testLinodeToken, WithBaseURL(server.URL)).GetInstance(context.Background(), "45")
	if err != nil || instance.ID != "45" || instance.Status != "running" || len(instance.IPv4) != 1 || instance.IPv4[0] != "203.0.113.2" {
		t.Fatalf("GetInstance() = %+v, %v", instance, err)
	}
}

func TestLinodeHTTPListInstancesByTagPagesAndFilters(t *testing.T) {
	pages := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertLinodeHeaders(t, r)
		if r.Method != http.MethodGet || r.URL.Path != "/linode/instances" || r.URL.Query().Get("page_size") != "500" {
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		page := r.URL.Query().Get("page")
		pages = append(pages, page)
		switch page {
		case "1":
			_, _ = w.Write([]byte(`{"pages":2,"data":[{"id":46,"tags":["yscale-headscale"],"label":"first"}]}`))
		case "2":
			_, _ = w.Write([]byte(`{"pages":2,"data":[{"id":47,"tags":["yscale-burst"],"label":"excluded"},{"id":48,"tags":["yscale-headscale"],"label":"second"}]}`))
		default:
			t.Fatalf("unexpected page %q", page)
		}
	}))
	defer server.Close()

	instances, err := NewLinodeHTTP(testLinodeToken, WithBaseURL(server.URL)).ListInstancesByTag(context.Background(), "yscale-headscale")
	if err != nil || len(instances) != 2 || instances[0].ID != "46" || instances[1].ID != "48" {
		t.Fatalf("ListInstancesByTag() = %+v, %v", instances, err)
	}
	if strings.Join(pages, ",") != "1,2" {
		t.Fatalf("fetched pages = %v, want [1 2]", pages)
	}
}

func TestLinodeHTTPDeleteInstanceAcceptsSuccessAndNotFound(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assertLinodeHeaders(t, r)
				if r.Method != http.MethodDelete || r.URL.Path != "/linode/instances/49" {
					t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				w.WriteHeader(status)
			}))
			defer server.Close()

			if err := NewLinodeHTTP(testLinodeToken, WithBaseURL(server.URL)).DeleteInstance(context.Background(), "49"); err != nil {
				t.Fatalf("DeleteInstance() error = %v", err)
			}
		})
	}
}

func TestLinodeHTTPCreateInstanceErrorIsSanitized(t *testing.T) {
	const rootPass = "test-root-password"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertLinodeHeaders(t, r)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"test-linode-token test-root-password never include this body"}`))
	}))
	defer server.Close()

	_, err := NewLinodeHTTP(testLinodeToken, WithBaseURL(server.URL)).CreateInstance(context.Background(), InstanceSpec{
		Label: "headscale-acme", Region: "us-ord", Type: "g6-nanode-1", Image: "linode/debian12",
		RootPass: rootPass, UserDataB64: "dXNlci1kYXRh", Tags: []string{"yscale-headscale"},
	})
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("error = %v, want HTTP status", err)
	}
	for _, secret := range []string{testLinodeToken, rootPass, "never include this body"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error = %v, must not expose %q", err, secret)
		}
	}
}

func TestLinodeHTTPErrorIncludesOnlyStructuredReasons(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"errors":[{"field":"firewall_id","reason":"Must be of type Integer"}],"echo":"test-root-password"}`))
	}))
	defer server.Close()

	_, err := NewLinodeHTTP(testLinodeToken, WithBaseURL(server.URL)).CreateInstance(context.Background(), InstanceSpec{
		Label: "headscale-acme", Region: "us-ord", Type: "g6-nanode-1", Image: "linode/debian12",
		RootPass: "test-root-password", UserDataB64: "dXNlci1kYXRh", Tags: []string{"yscale-headscale"},
	})
	if err == nil || !strings.Contains(err.Error(), "firewall_id: Must be of type Integer") {
		t.Fatalf("error = %v, want Linode's field reason", err)
	}
	if strings.Contains(err.Error(), "test-root-password") {
		t.Fatalf("error = %v, must not expose other body content", err)
	}
}
