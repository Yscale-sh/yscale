package agent

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
)

// Join the real signer (authorization, Secret lookup, SDK pagination and
// presigning) to ToJob's real download command. Only Kubernetes Secret storage
// and the S3 HTTP service are fixtures; this is not provider qualification.
func TestCacheSignerToInit(t *testing.T) {
	for _, failDownload := range []bool{false, true} {
		name := "download"
		if failDownload {
			name = "download-failure"
		}
		t.Run(name, func(t *testing.T) {
			for _, tool := range []string{"sh", "curl", "jq"} {
				if _, err := exec.LookPath(tool); err != nil {
					t.Fatalf("cache execution tests require %s: %v", tool, err)
				}
			}
			var lists, downloads atomic.Int32
			objects := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") == "2" {
					lists.Add(1)
					if r.URL.Path != "/models" || r.URL.Query().Get("prefix") != "weights/" || !strings.Contains(r.Header.Get("Authorization"), "Credential=team-a-key/") {
						t.Error("signer listed the wrong bucket, prefix or credential identity")
						http.Error(w, "wrong listing scope", http.StatusForbidden)
						return
					}
					page := listingPage{Contents: []listingObject{listedObject("weights/nested/model.bin", 13)}}
					if r.URL.Query().Get("continuation-token") == "" {
						page = listingPage{IsTruncated: true, NextContinuationToken: "page-2", Contents: []listingObject{listedObject("weights/", 0), listedObject("weights/nested/", 0)}}
					}
					w.Header().Set("Content-Type", "application/xml")
					_ = xml.NewEncoder(w).Encode(page)
					return
				}
				downloads.Add(1)
				q := r.URL.Query()
				if r.Method != http.MethodGet || r.URL.Path != "/models/weights/nested/model.bin" || q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" || !strings.HasPrefix(q.Get("X-Amz-Credential"), "team-a-key/") || len(q.Get("X-Amz-Signature")) != 64 || q.Get("X-Amz-Expires") != "3600" {
					t.Error("cache did not use the scoped SDK-presigned GET URL")
					http.Error(w, "wrong signed request", http.StatusForbidden)
					return
				}
				if failDownload {
					http.Error(w, "fixture-only-sensitive-upstream-body", http.StatusForbidden)
					return
				}
				_, _ = w.Write([]byte("model weights"))
			}))
			defer objects.Close()
			src := protocol.BucketRef{Bucket: "models", Prefix: "weights", Endpoint: objects.URL, Region: "us-east-1", CredentialsSecret: "r2-creds"}
			signer := signerFor(t, "team-a", []protocol.StorageBinding{cacheBinding(src)}, credsSecret("team-a", "r2-creds", "team-a-key"))
			target := filepath.Join(t.TempDir(), "cache")
			job, err := workload.ToJob(&workload.Workload{
				APIVersion: workload.APIVersion, Kind: workload.Kind,
				Metadata: workload.Metadata{Name: "cache-signer", Namespace: "team-a"},
				Spec: workload.Spec{Image: "trainer", Size: "small", Storage: &workload.Storage{Cache: []workload.CacheSpec{{
					Name: "models", Target: target,
					Source: workload.BucketRef{Bucket: src.Bucket, Prefix: src.Prefix, Endpoint: src.Endpoint, Region: src.Region, CredentialsSecret: src.CredentialsSecret},
				}}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			bin := t.TempDir()
			if err := os.WriteFile(filepath.Join(bin, "apk"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			init := job.Spec.Template.Spec.InitContainers[0]
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, init.Command[0], init.Command[1:]...)
			cmd.Env = []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "TMPDIR=" + t.TempDir(), "NO_PROXY=*", "BURST_ID=burst_x", "BOOTSTRAP_ENDPOINT=" + signer}
			for _, env := range init.Env {
				if env.ValueFrom == nil {
					cmd.Env = append(cmd.Env, env.Name+"="+env.Value)
				}
			}
			output, runErr := cmd.CombinedOutput()
			if ctx.Err() != nil || lists.Load() != 2 || downloads.Load() != 1 {
				t.Fatalf("pipeline did not complete: %v, lists=%d, downloads=%d", ctx.Err(), lists.Load(), downloads.Load())
			}
			if strings.Contains(string(output), "secret-for-") || strings.Contains(string(output), "X-Amz-Signature") || strings.Contains(string(output), "fixture-only-sensitive") {
				t.Fatal("init logged credentials, a signed URL or an upstream body")
			}
			cached, readErr := os.ReadFile(filepath.Join(target, "nested/model.bin"))
			if failDownload {
				if runErr == nil || !os.IsNotExist(readErr) || !strings.Contains(string(output), "cache init failed: object download") {
					t.Fatalf("failed download did not fail closed: run=%v, file=%v, output=%q", runErr, readErr, output)
				}
			} else if runErr != nil || readErr != nil || string(cached) != "model weights" {
				t.Fatalf("signer-to-init download failed: run=%v, file=%v, output=%q", runErr, readErr, output)
			}
		})
	}
}
