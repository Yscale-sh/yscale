package linode

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
)

func TestValidateAccount(t *testing.T) {
	b := NewWithConfig("tenant-token", Config{Region: "us-ord"})
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		if req.Header.Get("Authorization") != "Bearer tenant-token" {
			t.Fatal("missing tenant authorization")
		}
		switch req.URL.Path {
		case "/v4/account":
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"uuid":"provider-uuid"}`))}, nil
		case "/v4/regions/us-ord":
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"id":"us-ord","capabilities":["Metadata"]}`))}, nil
		default:
			return nil, errors.New("unexpected request")
		}
	}}
	identity, err := b.ValidateAccount(context.Background())
	if err != nil || identity != "provider-uuid" {
		t.Fatalf("ValidateAccount = %q, %v", identity, err)
	}
}

func TestValidateAccountSanitizesProviderBody(t *testing.T) {
	b := NewWithConfig("bad-token", Config{Region: "us-ord"})
	b.client.Transport = &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(strings.NewReader(`secret provider diagnostic`))}, nil
	}}
	_, err := b.ValidateAccount(context.Background())
	if err == nil || strings.Contains(err.Error(), "secret provider diagnostic") {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestTenantGPUWithoutImageFailsBeforeAPI(t *testing.T) {
	b := NewWithConfig("token", Config{Region: "us-ord"})
	calls := 0
	b.client.Transport = &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("must not call") }}
	_, err := b.CreateNode(context.Background(), &backends.NodeSpec{Resources: backends.ResourceRequirements{GPU: &backends.GPUSpec{Kind: "rtx4000ada", Count: 1}}})
	if err == nil || !strings.Contains(err.Error(), "require a configured GPU image") {
		t.Fatalf("error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("API calls = %d", calls)
	}
}

func TestNewWithConfigUsesOnlyAccountImages(t *testing.T) {
	t.Setenv("YSCALE_LINODE_IMAGE_CPU", "private/platform-cpu")
	t.Setenv("YSCALE_LINODE_IMAGE_GPU", "private/platform-gpu")
	b := NewWithConfig("tenant", Config{Region: "us-ord", CPUImage: "private/tenant-cpu"})
	if b.cpuImage != "private/tenant-cpu" || b.gpuImage != "" {
		t.Fatalf("account images = cpu %q gpu %q", b.cpuImage, b.gpuImage)
	}
}
