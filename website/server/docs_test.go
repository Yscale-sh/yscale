package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestReleaseDocsStaticRoutes(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"index.html": "app", "docs/index.html": "docs", "docs/getting-started.html": "guide",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	a := &app{staticDir: dir}
	srv := httptest.NewServer(http.HandlerFunc(a.handleStatic))
	defer srv.Close()
	for _, tc := range []struct {
		path string
		code int
		body string
	}{
		{"/docs", 200, "docs"}, {"/docs/", 200, "docs"},
		{"/docs/index.html", 200, "docs"}, {"/docs/getting-started.html", 200, "guide"},
		{"/docs/missing.html", 404, "404 page not found\n"}, {"/account", 200, "app"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			res, err := srv.Client().Get(srv.URL + tc.path)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			if res.StatusCode != tc.code || string(body) != tc.body {
				t.Fatalf("status=%d body=%q", res.StatusCode, body)
			}
		})
	}
}
