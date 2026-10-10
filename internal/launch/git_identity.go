package launch

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// verifyGitIdentity checks local objects, not remote availability or deployment.
// A plausible-looking SHA is not proof that the tested and released trees match.
func verifyGitIdentity(root string, release Release) []string {
	var failures []string
	for _, identity := range []struct{ name, commit string }{
		{"release.commit", release.Commit},
		{"release.tested_head", release.TestedHead},
	} {
		if !commit40Pattern.MatchString(identity.commit) || !commit40Pattern.MatchString(release.Tree) {
			failures = append(failures, "Git identity: malformed release identity cannot be verified")
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		objectType, typeErr := exec.CommandContext(ctx, "git", "--no-replace-objects", "-C", root,
			"cat-file", "-t", identity.commit).Output()
		if typeErr != nil || strings.TrimSpace(string(objectType)) != "commit" {
			cancel()
			failures = append(failures, fmt.Sprintf("Git identity: %s %s is not a readable local commit; fetch the exact source before certification", identity.name, identity.commit))
			continue
		}
		out, err := exec.CommandContext(ctx, "git", "--no-replace-objects", "-C", root,
			"rev-parse", "--verify", identity.commit+"^{commit}^{tree}").Output()
		cancel()
		if err != nil {
			failures = append(failures, fmt.Sprintf("Git identity: %s %s is not a readable local commit; fetch the exact source before certification", identity.name, identity.commit))
			continue
		}
		if tree := strings.TrimSpace(string(out)); !strings.EqualFold(tree, release.Tree) {
			failures = append(failures, fmt.Sprintf("Git identity: %s resolves to tree %s, not declared release.tree %s", identity.name, tree, release.Tree))
		}
	}
	return failures
}
