package evidence

import (
	"runtime/debug"
	"strings"
	"testing"
)

func TestResolveCommitSHA_FullSHA(t *testing.T) {
	sha := strings.Repeat("a", 40)
	resolver := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: sha},
			},
		}, true
	}
	got, err := ResolveCommitSHA(resolver)
	if err != nil {
		t.Fatal(err)
	}
	if got != sha {
		t.Fatalf("got %q, want %q", got, sha)
	}
}

func TestResolveCommitSHA_SHA256(t *testing.T) {
	sha := strings.Repeat("b", 64)
	resolver := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: sha},
			},
		}, true
	}
	got, err := ResolveCommitSHA(resolver)
	if err != nil {
		t.Fatal(err)
	}
	if got != sha {
		t.Fatalf("got %q, want %q", got, sha)
	}
}

func TestResolveCommitSHA_ShortSHAFails(t *testing.T) {
	resolver := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: "abc123"},
			},
		}, true
	}
	_, err := ResolveCommitSHA(resolver)
	if err == nil {
		t.Fatal("short SHA should fail")
	}
	if !strings.Contains(err.Error(), "full Git object ID") {
		t.Fatalf("error should mention full ID, got %q", err.Error())
	}
}

func TestResolveCommitSHA_MissingRevision(t *testing.T) {
	resolver := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Settings: []debug.BuildSetting{
				{Key: "vcs", Value: "git"},
			},
		}, true
	}
	_, err := ResolveCommitSHA(resolver)
	if err == nil {
		t.Fatal("missing vcs.revision should fail")
	}
}

func TestResolveCommitSHA_NoBuildInfo(t *testing.T) {
	resolver := func() (*debug.BuildInfo, bool) {
		return nil, false
	}
	_, err := ResolveCommitSHA(resolver)
	if err == nil {
		t.Fatal("unavailable build info should fail")
	}
}

func TestResolveCommitSHA_DirtyBuildRejected(t *testing.T) {
	sha := strings.Repeat("a", 40)
	resolver := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: sha},
				{Key: "vcs.modified", Value: "true"},
			},
		}, true
	}
	_, err := ResolveCommitSHA(resolver)
	if err == nil {
		t.Fatal("dirty build should be rejected")
	}
	if !strings.Contains(err.Error(), "dirty") {
		t.Fatalf("error should mention dirty tree, got %q", err.Error())
	}
}

func TestResolveCommitSHA_CleanBuildAccepted(t *testing.T) {
	sha := strings.Repeat("c", 40)
	resolver := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: sha},
				{Key: "vcs.modified", Value: "false"},
			},
		}, true
	}
	got, err := ResolveCommitSHA(resolver)
	if err != nil {
		t.Fatal(err)
	}
	if got != sha {
		t.Fatalf("got %q, want %q", got, sha)
	}
}

func TestResolveCommitSHA_AbsentModifiedSettingAccepted(t *testing.T) {
	sha := strings.Repeat("d", 40)
	resolver := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: sha},
			},
		}, true
	}
	got, err := ResolveCommitSHA(resolver)
	if err != nil {
		t.Fatal(err)
	}
	if got != sha {
		t.Fatalf("got %q, want %q", got, sha)
	}
}
