package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func fixture(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	var names []string
	for name, body := range files {
		write(t, root, name, body)
		names = append(names, name)
	}
	names = append(names, inventory)
	slices.Sort(names)
	write(t, root, inventory, strings.Join(names, "\n")+"\n")
	return root
}

func write(t *testing.T, root, name, body string) {
	t.Helper()
	file := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestSourceAndAssetsKeepBusinessLogicAndOmitPrivateFiles(t *testing.T) {
	files := map[string]string{
		"central/internal/state/state.go":              "package state // yscale:" + "proprietary\n",
		"factory/cmd/main.go":                          "package main\n",
		"website/src/pages/Account.jsx":                "export const Account = () => null;\n",
		"docs/getting-started.md":                      "# Public guide\n",
		"deploy/helm/yscale-agent/templates/NOTES.txt": "Functional Helm output\n",
		"internal/launch/testdata/synthetic-proof.txt": "Synthetic fixture\n",
		".env.factory.example":                         "FACTORY_BEARER_TOKEN=\n",
	}
	root := fixture(t, files)
	for _, name := range []string{".git/config", ".env.factory", "docs/evidence/receipt.json", "docs/internals/secret.md", "docs/design/business-plan.md", "central/private-notes.md", "deploy/operator.key", "deploy/old.yaml.bak"} {
		write(t, root, name, "DO NOT PUBLISH\n")
	}
	plan, err := reviewedFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"source", "assets"} {
		t.Run(mode, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "new-export")
			if err := export(root, out, mode, plan); err != nil {
				t.Fatal(err)
			}
			for name, body := range files {
				got, err := os.ReadFile(filepath.Join(out, name))
				if mode == "assets" && !assets(name) {
					if !os.IsNotExist(err) {
						t.Errorf("unexpected asset %s: %v", name, err)
					}
					continue
				}
				if err != nil || string(got) != body {
					t.Errorf("changed or missing implementation %s: %v", name, err)
				}
			}
			if _, err := os.Stat(filepath.Join(out, "docs/evidence")); !os.IsNotExist(err) {
				t.Fatal("private evidence exported")
			}
			if err := export(root, out, mode, plan); err == nil {
				t.Fatal("must not overwrite existing destination")
			}
		})
	}
}

func TestUnreviewedImplementationCannotBeSilentlyStripped(t *testing.T) {
	root := fixture(t, map[string]string{"central/main.go": "package central\n"})
	write(t, root, "factory/new-business-feature.go", "package factory\n")
	if _, err := reviewedFiles(root); err == nil || !strings.Contains(err.Error(), "implementation missing") {
		t.Fatalf("got %v", err)
	}
}

func TestPrivateNamesAreCaseInsensitive(t *testing.T) {
	for _, name := range []string{
		".env", "scripts/.ENV", "central/.EnV.production",
		"docs/EVIDENCE/receipt.json", "Docs/Internals/private.md",
		"website/Deploy.dev.yaml", "linode_burst_known_issues.md",
		"scripts/PRIVATE-HISTORY-SECRET-SCAN.SH",
		".github/workflows/PRIVATE-HISTORY-SECRET-SCAN.YAML",
		"PUBLIC_RELEASE.md", "scripts/OSS-EXCLUDE.txt", "scripts/oss-gates.sh",
		"scripts/oss-overlay/central/x.go.overlay", "test/oss-gates/ossgates_test.go",
		".github/workflows/ci-oss.yaml", ".github/workflows/oss-export-check.yaml",
	} {
		t.Run(name, func(t *testing.T) {
			root := fixture(t, map[string]string{name: "OPERATOR_ONLY=fixture\n"})
			if _, err := reviewedFiles(root); err == nil || !strings.Contains(err.Error(), "unsafe inventory path") {
				t.Fatalf("existing private file %s must be rejected, got %v", name, err)
			}
		})
	}
	for _, name := range []string{".env.factory.example", "scripts/.ENV.EXAMPLE", ".env.Factory.Example"} {
		t.Run(name, func(t *testing.T) {
			root := fixture(t, map[string]string{name: "FACTORY_BEARER_TOKEN=\n"})
			if _, err := reviewedFiles(root); err != nil {
				t.Fatalf("reviewed environment template %s rejected: %v", name, err)
			}
		})
	}
}

func TestInvalidOrPrivateInventoryFailsClosed(t *testing.T) {
	for _, name := range []string{"../outside", "/absolute", "docs/../private", ".git/config", ".env", "docs/evidence/receipt.json", "private.key", "website/deploy.yaml"} {
		t.Run(name, func(t *testing.T) {
			root := fixture(t, nil)
			write(t, root, inventory, inventory+"\n"+name+"\n")
			if _, err := reviewedFiles(root); err == nil {
				t.Fatal("unsafe inventory accepted")
			}
		})
	}
	for _, marker := range []string{"PRIVATE / INTERNAL", "Do not publish", "Editorial verification notes"} {
		root := fixture(t, map[string]string{"docs/guide.md": marker})
		if _, err := reviewedFiles(root); err == nil {
			t.Fatal("private document accepted")
		}
	}
	for _, entries := range []string{"", inventory + "\n" + inventory + "\n", inventory + "\ndocs/missing.md\n"} {
		root := fixture(t, nil)
		write(t, root, inventory, entries)
		if _, err := reviewedFiles(root); err == nil {
			t.Fatal("invalid inventory accepted")
		}
	}
}

func TestSymlinksCannotImportPrivateContent(t *testing.T) {
	for _, directory := range []bool{false, true} {
		root := fixture(t, nil)
		outside := t.TempDir()
		write(t, outside, "private.md", "private data")
		name, target := "link.md", filepath.Join(outside, "private.md")
		if directory {
			name, target = "linked", outside
		}
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
		if directory {
			name += "/private.md"
		}
		write(t, root, inventory, inventory+"\n"+name+"\n")
		if _, err := reviewedFiles(root); err == nil {
			t.Fatal("symlink accepted")
		}
	}
}

// requireWebsite skips checks that need website/ itself. The public tree
// carries it inline; the private monorepo carries it as a submodule that CI
// does not check out, and the public tree's CI covers it.
func requireWebsite(t *testing.T, root string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(root, "website", "package.json")); err != nil {
		t.Skip("website/ is not checked out (submodule); covered by the public tree")
	}
}

func TestCandidateInventoryRetainsFullImplementation(t *testing.T) {
	requireWebsite(t, "../..")
	files, err := reviewedFiles("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"central/internal/state/state.go", "factory/cmd/yscale-factory/main.go", "website/src/pages/Account.jsx", "website/src/pages/Console.jsx", "deploy/helm/yscale-agent/templates/NOTES.txt", "internal/launch/testdata/synthetic-proof.txt"} {
		if !slices.Contains(files, required) {
			t.Errorf("required implementation omitted: %s", required)
		}
	}
}

func TestPublicationRefusesDraftLicense(t *testing.T) {
	for _, body := range []string{"", "DRAFT terms", "Not yet in effect", "pending legal review"} {
		root := t.TempDir()
		write(t, root, "LICENSE", body)
		write(t, root, "COMMERCIAL.md", "Example final text for this test only.")
		if err := publicationLicense(root); err == nil {
			t.Fatal("draft license accepted for publication")
		}
	}
	root := t.TempDir()
	write(t, root, "LICENSE", "Example final text for this test only.")
	if err := publicationLicense(root); err == nil {
		t.Fatal("missing commercial terms accepted")
	}
	write(t, root, "COMMERCIAL.md", "DRAFT")
	if err := publicationLicense(root); err == nil {
		t.Fatal("draft commercial terms accepted")
	}
	write(t, root, "COMMERCIAL.md", "Example final text for this test only.")
	if err := publicationLicense(root); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseCandidateHasFinalConsistentLicense(t *testing.T) {
	root := "../.."
	requireWebsite(t, root)
	if err := publicationLicense(root); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"LICENSE", "COMMERCIAL.md"} {
		canonical, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		website, err := os.ReadFile(filepath.Join(root, "website/content/release", name))
		if err != nil {
			t.Fatal(err)
		}
		if string(canonical) != string(website) {
			t.Errorf("website %s differs from the release terms", name)
		}
	}
}
