// release-export copies only explicitly reviewed files. It never strips or
// rewrites implementation, follows symlinks, copies Git history, or overwrites
// an existing destination. The same inventory drives source and native assets.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

const inventory = "scripts/release-files.txt"

func main() {
	root := flag.String("root", ".", "source root")
	output := flag.String("output", "", "new destination directory (must not exist)")
	mode := flag.String("mode", "source", "source or assets")
	publication := flag.Bool("publication", false, "reject a missing or draft license before publication")
	flag.Parse()
	files, err := reviewedFiles(*root)
	if err == nil && *publication {
		err = publicationLicense(*root)
	}
	if err == nil && *mode != "source" && *mode != "assets" {
		err = errors.New("mode must be source or assets")
	}
	if err == nil && *output != "" {
		err = export(*root, *output, *mode, files)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "release-export:", err)
		os.Exit(1)
	}
	fmt.Printf("Reviewed inventory: %d files; mode=%s; output=%s\n", len(files), *mode, *output)
}

func publicationLicense(root string) error {
	for _, name := range []string{"LICENSE", "COMMERCIAL.md"} {
		body, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		text := strings.ToUpper(strings.TrimSpace(string(body)))
		if text == "" || strings.Contains(text, "DRAFT") || strings.Contains(text, "NOT YET IN EFFECT") || strings.Contains(text, "PENDING LEGAL REVIEW") {
			return fmt.Errorf("%s is empty or still a draft; owner-approved license text is required before publication", name)
		}
	}
	return nil
}

// Never let an inventory entry override a privacy exclusion.
func privatePath(name string) bool {
	name = strings.ToLower(name)
	for _, part := range strings.Split(name, "/") {
		switch part {
		case ".git", ".gitmodules", ".gitleaksignore", ".cartogopher", "node_modules", "dist", "bin", ".claude", ".codex", ".mission", "notes", "scratch":
			return true
		}
		if strings.HasPrefix(part, ".env") && !strings.HasSuffix(part, ".example") {
			return true
		}
	}
	for _, prefix := range []string{"docs/evidence/", "docs/internals/", "docs/design/", "docs/product/", "docs/architecture/", "security/", "website/docs/", "website/brochure/"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	for _, suffix := range []string{".pem", ".key", ".p12", ".pfx", ".sqlite", ".db", ".log", ".jsonl", ".bak"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	if legacyOpenCore(name) {
		return true
	}
	return strings.HasPrefix(name, "website/deploy") || name == "linode_burst_known_issues.md" || name == "public_release.md" ||
		name == "scripts/private-history-secret-scan.sh" || name == ".github/workflows/private-history-secret-scan.yaml"
}

// legacyOpenCore matches the retired split-edition export tooling. It only
// serves the private repository; its notes describe private paths and past
// release decisions, so it never ships. name is already lower-cased.
func legacyOpenCore(name string) bool {
	switch name {
	case "scripts/oss-exclude.txt", "scripts/oss-manifest.txt", "scripts/oss-gates.sh", "scripts/release-oss.sh",
		"scripts/sync-oss.sh", ".github/workflows/ci-oss.yaml", ".github/workflows/oss-export-check.yaml":
		return true
	}
	return strings.HasPrefix(name, "scripts/oss-overlay/") || strings.HasPrefix(name, "test/oss-gates/")
}

func document(name string) bool {
	ext := strings.ToLower(path.Ext(name))
	return ext == ".md" || ext == ".txt"
}

func implementation(name string) bool {
	if privatePath(name) || document(name) || strings.HasPrefix(name, "docs/") || strings.HasPrefix(name, "website/content/") {
		return false
	}
	for _, prefix := range []string{"agent/", "burst/", "central/", "cmd/", "deploy/", "examples/", "factory/", "internal/", "kube-bench/", "pkg/", "pricefeed/", "scripts/", "test/", "website/src/", "website/server/", "website/scripts/", "website/public/"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func assets(name string) bool {
	return strings.HasPrefix(name, "docs/") || strings.HasPrefix(name, "deploy/") || strings.HasPrefix(name, "examples/") ||
		!strings.Contains(name, "/") && (document(name) || strings.HasSuffix(name, ".example") || name == "LICENSE" || name == "go.mod")
}

func reviewedFiles(root string) ([]string, error) {
	r, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	f, err := r.Open(inventory)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var files []string
	seen := map[string]bool{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		name := scanner.Text()
		if name == "" || strings.HasPrefix(name, "#") {
			continue
		}
		if !fs.ValidPath(name) || strings.ContainsAny(name, "\\\r\t") || strings.TrimSpace(name) != name || privatePath(name) {
			return nil, fmt.Errorf("unsafe inventory path %q", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate inventory path %q", name)
		}
		for current := name; current != "."; current = path.Dir(current) {
			info, err := r.Lstat(current)
			if err != nil {
				return nil, fmt.Errorf("inventory path %s: %w", name, err)
			}
			if info.Mode()&os.ModeSymlink != 0 || current == name && !info.Mode().IsRegular() {
				return nil, fmt.Errorf("inventory path %s is not a regular file through real directories", name)
			}
		}
		if document(name) {
			body, err := r.ReadFile(name)
			if err != nil {
				return nil, err
			}
			for _, marker := range []string{"PRIVATE / INTERNAL", "EDITORIAL VERIFICATION NOTES"} {
				if strings.Contains(strings.ToUpper(string(body)), marker) {
					return nil, fmt.Errorf("private document marker in %s", name)
				}
			}
			for _, line := range strings.Split(strings.ToUpper(string(body)), "\n") {
				if strings.HasPrefix(strings.TrimLeft(line, ">* \t"), "DO NOT PUBLISH") {
					return nil, fmt.Errorf("private document notice in %s", name)
				}
			}
		}
		seen[name] = true
		files = append(files, name)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(files) == 0 || !seen[inventory] {
		return nil, errors.New("inventory must contain itself and cannot be empty")
	}
	// A new implementation file cannot silently disappear from the full-source
	// release. New documents, on the other hand, remain private until reviewed.
	err = fs.WalkDir(r.FS(), ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if privatePath(name + "/") {
				return fs.SkipDir
			}
			return nil
		}
		if implementation(name) && !seen[name] {
			return fmt.Errorf("implementation missing from full-source inventory: %s", name)
		}
		return nil
	})
	slices.Sort(files)
	return files, err
}

func export(root, output, mode string, files []string) error {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	absOut, err := filepath.Abs(output)
	if err != nil {
		return err
	}
	if absOut == absRoot {
		return errors.New("destination cannot be the source root")
	}
	// mkdir (not MkdirAll) refuses any existing target, including a symlink.
	if err := os.Mkdir(absOut, 0755); err != nil {
		return fmt.Errorf("create new export destination: %w", err)
	}
	in, err := os.OpenRoot(absRoot)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenRoot(absOut)
	if err != nil {
		return err
	}
	defer out.Close()
	for _, name := range files {
		if mode == "assets" && !assets(name) {
			continue
		}
		if err := out.MkdirAll(path.Dir(name), 0755); err != nil {
			return err
		}
		if err := copyFile(in, out, name); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(in, out *os.Root, name string) error {
	src, err := in.Open(name)
	if err != nil {
		return err
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return err
	}
	dst, err := out.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm()&0755)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(dst, src)
	return errors.Join(copyErr, dst.Close())
}
