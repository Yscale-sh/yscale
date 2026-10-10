package launch

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitIdentityRequiresActualMatchingTrees(t *testing.T) {
	repo := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		base := []string{"-C", repo, "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false",
			"-c", "user.name=Launch Test", "-c", "user.email=launch-test@example.invalid"}
		out, err := exec.Command("git", append(base, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("fixture git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "--quiet")
	git("commit", "--allow-empty", "-m", "first")
	first := git("rev-parse", "HEAD")
	firstTree := git("rev-parse", "HEAD^{tree}")
	if got := verifyGitIdentity(repo, Release{Commit: first, TestedHead: first, Tree: firstTree}); len(got) != 0 {
		t.Fatalf("matching local commits: %v", got)
	}
	if err := os.WriteFile(filepath.Join(repo, "fixture.txt"), []byte("different tree\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "fixture.txt")
	git("commit", "-m", "second")
	second := git("rev-parse", "HEAD")
	secondTree := git("rev-parse", "HEAD^{tree}")
	git("tag", "-a", "fixture-tag", "-m", "annotated tag")
	tag := git("rev-parse", "fixture-tag")
	for name, release := range map[string]Release{
		"stale tested tree":   {Commit: second, TestedHead: first, Tree: secondTree},
		"wrong declared tree": {Commit: second, TestedHead: second, Tree: firstTree},
		"nonexistent commit":  {Commit: strings.Repeat("a", 40), TestedHead: first, Tree: firstTree},
		"tree is not commit":  {Commit: firstTree, TestedHead: first, Tree: firstTree},
		"tag is not commit":   {Commit: tag, TestedHead: second, Tree: secondTree},
	} {
		t.Run(name, func(t *testing.T) {
			if got := verifyGitIdentity(repo, release); len(got) == 0 {
				t.Fatal("unverifiable identity passed")
			}
		})
	}
}
