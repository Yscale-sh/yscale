package evidence

import (
	"fmt"
	"runtime/debug"
)

// ResolveCommitSHA returns the full Git object ID from Go build info.
// The resolver is injectable for testing; nil uses debug.ReadBuildInfo.
func ResolveCommitSHA(resolver func() (*debug.BuildInfo, bool)) (string, error) {
	if resolver == nil {
		resolver = debug.ReadBuildInfo
	}
	info, ok := resolver()
	if !ok || info == nil {
		return "", fmt.Errorf("Go build info unavailable")
	}
	var revision string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				return "", fmt.Errorf("binary built from dirty tree (vcs.modified=true); evidence requires a clean commit")
			}
		}
	}
	if revision == "" {
		return "", fmt.Errorf("vcs.revision not found in build info")
	}
	if !commitSHA.MatchString(revision) {
		return "", fmt.Errorf("vcs.revision %q is not a full Git object ID", revision)
	}
	return revision, nil
}
