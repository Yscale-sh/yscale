package release

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var binaries = map[string]string{
	"yscale":                   "./cmd/yscale",
	"yscale-cloud":             "./central/cmd/yscale-cloud",
	"yscale-agent":             "./agent/cmd/yscale-agent",
	"yscale-factory":           "./factory/cmd/yscale-factory",
	"yscale-lifecycle-migrate": "./central/cmd/yscale-lifecycle-migrate",
}

func TestDefaultBuildIncludesManagedMesh(t *testing.T) {
	cmd := exec.Command("make", "-n", "build")
	cmd.Dir = "../.."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make: %v: %s", err, out)
	}
	for name, pkg := range binaries {
		if !strings.Contains(string(out), pkg) {
			t.Errorf("default build omits %s (%s)", name, pkg)
		}
	}
}

func TestReleaseContainsFullStackAndFactoryAssets(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	// The private monorepo carries website/ as a submodule CI does not check
	// out; packaging needs every inventory file, so the public tree covers it.
	if _, err := os.Stat(filepath.Join(root, "website", "package.json")); err != nil {
		t.Skip("website/ is not checked out (submodule); covered by the public tree")
	}
	output := t.TempDir()
	// Exercise the real packager and Makefile without cross-compiling in this
	// contract test. Native builds test the actual Go programs separately.
	fakeGo := filepath.Join(output, "go-fixture")
	stub := "#!/bin/sh\nset -eu\nwhile [ $# -gt 0 ]; do\n  if [ \"$1\" = -o ]; then shift; printf 'build fixture\\n' > \"$1\"; chmod +x \"$1\"; fi\n  shift\ndone\n"
	if err := os.WriteFile(fakeGo, []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "scripts/package-release.sh", "v0.0.0-test", output)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GO="+fakeGo, "GOOS=linux", "GOARCH=amd64")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("package: %v: %s", err, out)
	}
	name := "yscale-v0.0.0-test-linux-amd64"
	f, err := os.Open(filepath.Join(output, name+".tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	entries := map[string]bool{}
	reader := tar.NewReader(gz)
	for {
		h, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries[strings.TrimPrefix(h.Name, name+"/")] = true
	}
	for binary := range binaries {
		if !entries["bin/"+binary] {
			t.Errorf("archive missing %s", binary)
		}
	}
	for entry := range entries {
		for _, forbidden := range []string{"docs/evidence/", "docs/internals/", "docs/design/", "docs/product/", ".git/"} {
			if strings.HasPrefix(entry, forbidden) {
				t.Errorf("archive contains private material: %s", entry)
			}
		}
		if strings.HasSuffix(entry, ".bak") {
			t.Errorf("archive contains backup: %s", entry)
		}
	}
	for _, file := range []string{"deploy/headscale/cloud-init.sh", "deploy/headscale/acls.hujson", "deploy/helm/yscale-agent/Chart.yaml", ".env.cloud.example", ".env.factory.example", "docs/self-hosted-mesh.md", "LICENSE"} {
		if !entries[file] {
			t.Errorf("archive missing %s", file)
		}
	}
	check := exec.Command("sha256sum", "-c", name+".tar.gz.sha256")
	check.Dir = output
	if out, err := check.CombinedOutput(); err != nil {
		t.Fatalf("checksum: %v: %s", err, out)
	}
}

// Rendered connector install commands and current operator docs must use the
// chart shipped in the archive, not a maintainer-operated OCI registry.

func TestFailedPackagingDoesNotPublishOrReplaceArchive(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, existing := range []bool{false, true} {
		name := "new"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			output, tools := t.TempDir(), t.TempDir()
			archive := filepath.Join(output, "yscale-v0.0.0-test-linux-amd64.tar.gz")
			for _, file := range []string{archive, archive + ".sha256"} {
				if existing {
					if err := os.WriteFile(file, []byte("previous complete release"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Only compression fails; assets still come from the real exporter.
			for tool, body := range map[string]string{
				"make": "#!/bin/sh\nexit 0\n",
				"tar":  "#!/bin/sh\nprintf 'partial archive' > \"$2\"\nexit 29\n",
			} {
				if err := os.WriteFile(filepath.Join(tools, tool), []byte(body), 0700); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("bash", "scripts/package-release.sh", "v0.0.0-test", output)
			cmd.Dir = root
			cmd.Env = append(os.Environ(), "PATH="+tools+string(os.PathListSeparator)+os.Getenv("PATH"), "GOOS=linux", "GOARCH=amd64")
			if out, err := cmd.CombinedOutput(); err == nil {
				t.Fatalf("failed compression reported success: %s", out)
			}
			for _, file := range []string{archive, archive + ".sha256"} {
				body, err := os.ReadFile(file)
				if existing {
					if err != nil || string(body) != "previous complete release" {
						t.Fatalf("failed packaging replaced an existing artifact: %s", file)
					}
				} else if !os.IsNotExist(err) {
					t.Fatalf("failed packaging published an incomplete artifact: %s", file)
				}
			}
			if stages, _ := filepath.Glob(filepath.Join(output, ".yscale-release.*")); len(stages) != 0 {
				t.Fatal("failed packaging left a staging directory")
			}
		})
	}
}

func TestInstallCommandsUseShippedChart(t *testing.T) {
	files := []string{"../../docs/self-hosted-mesh.md", "../../docs/platform.md"}
	handlers, err := filepath.Glob("../../central/internal/handlers/*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range handlers {
		if !strings.HasSuffix(f, "_test.go") {
			files = append(files, f)
		}
	}
	for _, f := range files {
		contents, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(contents), "oci://ghcr.io/jakenesler/charts") {
			t.Errorf("%s references the maintainer chart registry", f)
		}
	}
}

func TestManagedMeshEnvironmentAndImage(t *testing.T) {
	contents, err := os.ReadFile("../../.env.cloud.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		if strings.HasPrefix(line, "TS_OAUTH_") || strings.HasPrefix(line, "TS_TAILNET=") {
			t.Errorf("default central example configures shared Tailscale: %s", strings.SplitN(line, "=", 2)[0])
		}
	}
	for _, key := range []string{"FACTORY_URL=", "FACTORY_BEARER_TOKEN=", "YSCALE_CENTRAL_ENDPOINT="} {
		if !strings.Contains(string(contents), "\n"+key) {
			t.Errorf("missing %s", key)
		}
	}
	dockerfile, err := os.ReadFile("../../factory/build/Dockerfile.factory")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dockerfile), "COPY deploy/headscale/") {
		t.Error("factory image must include its provisioning templates")
	}
	ignore, err := os.ReadFile("../../.dockerignore")
	if err != nil {
		t.Fatal(err)
	}
	for _, asset := range []string{"cloud-init.sh", "acls.hujson"} {
		if !strings.Contains(string(ignore), "!deploy/headscale/"+asset) {
			t.Errorf("factory asset excluded from Docker context: %s", asset)
		}
	}
}
