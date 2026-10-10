package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type priceRoundTripper func(*http.Request) (*http.Response, error)

func (f priceRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPriceUsesOperatorEndpoint(t *testing.T) {
	for _, tc := range []struct{ name, env, flag, want string }{
		{name: "local default", want: "http://127.0.0.1:8443/v1/price/gpu"},
		{name: "operator env", env: "https://central.example.invalid", want: "https://central.example.invalid/v1/price/gpu"},
		{name: "flag wins", env: "https://ignored.example.invalid", flag: "https://chosen.example.invalid", want: "https://chosen.example.invalid/v1/price/gpu"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("YSCALE_ENDPOINT", tc.env)
			previous := http.DefaultTransport
			t.Cleanup(func() { http.DefaultTransport = previous })
			called := false
			http.DefaultTransport = priceRoundTripper(func(r *http.Request) (*http.Response, error) {
				called = true
				if r.URL.String() != tc.want {
					t.Errorf("URL=%s, want %s", r.URL, tc.want)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"gpus":[]}`)), Header: make(http.Header)}, nil
			})
			args := []string{"-token=fixture", "-json"}
			if tc.flag != "" {
				args = append(args, "-endpoint="+tc.flag)
			}
			if err := runPrice(append(args, "gpu")); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("price command did not use the HTTP client")
			}
		})
	}
}
