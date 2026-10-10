package state

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
)

func gitOpsSource(id string) GitOpsSource {
	return GitOpsSource{
		ID:         id,
		Name:       "Platform " + id,
		Reconciler: GitOpsReconcilerFlux,
		RepoURL:    "https://github.example.invalid/acme/platform",
		Path:       "clusters/prod",
		Ref:        "main",
		ClusterID:  "cl-prod",
	}
}

// The repository URL is the one field a caller could smuggle a secret through,
// so the grammar is asserted on the forms Flux and Argo actually take and on
// every shape that would carry a credential out of this record.
func TestValidateGitOpsSourcesRepoURLGrammar(t *testing.T) {
	accepted := []string{
		"https://github.example.invalid/acme/platform",
		"https://github.example.invalid/acme/platform.git",
		"https://git.example.invalid:8443/acme/platform.git",
		"ssh://git@github.example.invalid/acme/platform.git",
		"ssh://git@git.example.invalid:2222/acme/platform.git",
		"git@github.example.invalid:acme/platform.git",
		"github.example.invalid:acme/platform.git",
	}
	for _, raw := range accepted {
		src := gitOpsSource("repo")
		src.RepoURL = raw
		if _, err := ValidateGitOpsSources([]GitOpsSource{src}); err != nil {
			t.Errorf("ordinary repo url %q refused: %v", raw, err)
		}
	}

	refused := map[string]string{
		"https basic auth":     "https://alice:token@github.example.invalid/acme/platform",
		"https bare token":     "https://ghp_deadbeef@github.example.invalid/acme/platform",
		"ssh password":         "ssh://git:hunter2@github.example.invalid/acme/platform.git",
		"scp-like password":    "git:hunter2@github.example.invalid:acme/platform.git",
		"query string":         "https://github.example.invalid/acme/platform?token=ghp_deadbeef",
		"fragment":             "https://github.example.invalid/acme/platform#token",
		"plaintext http":       "http://github.example.invalid/acme/platform",
		"git protocol":         "git://github.example.invalid/acme/platform.git",
		"local file":           "file:///etc/shadow",
		"no host":              "https:///acme/platform",
		"control character":    "https://github.example.invalid/acme/pl\natform",
		"not a repository url": "just some text",
		"empty":                "",
	}
	for name, raw := range refused {
		src := gitOpsSource("repo")
		src.RepoURL = raw
		if _, err := ValidateGitOpsSources([]GitOpsSource{src}); !errors.Is(err, ErrInvalidGitOpsSources) {
			t.Errorf("%s (%q) accepted, want ErrInvalidGitOpsSources (got %v)", name, raw, err)
		}
	}
}

// A source is a coordinate INTO a repository. A path that can climb out of one
// is a coordinate into whatever the reconciler's working tree sits next to.
func TestValidateGitOpsSourcesPathIsRepositoryRelative(t *testing.T) {
	for _, path := range []string{"", "clusters/prod", "apps/team-a/overlays/prod", "a.b/c-d_e"} {
		src := gitOpsSource("path")
		src.Path = path
		if _, err := ValidateGitOpsSources([]GitOpsSource{src}); err != nil {
			t.Errorf("repository-relative path %q refused: %v", path, err)
		}
	}
	for _, path := range []string{"/etc/flux", "../../etc", "clusters/../../etc", "./clusters", "clusters//prod", `clusters\prod`, "clusters/prod/"} {
		src := gitOpsSource("path")
		src.Path = path
		if _, err := ValidateGitOpsSources([]GitOpsSource{src}); !errors.Is(err, ErrInvalidGitOpsSources) {
			t.Errorf("path %q accepted, want ErrInvalidGitOpsSources (got %v)", path, err)
		}
	}
}

func TestValidateGitOpsSourcesFieldGrammarAndBounds(t *testing.T) {
	mutate := func(f func(*GitOpsSource)) []GitOpsSource {
		src := gitOpsSource("ok")
		f(&src)
		return []GitOpsSource{src}
	}
	cases := map[string][]GitOpsSource{
		"empty id":              mutate(func(s *GitOpsSource) { s.ID = "" }),
		"uppercase id":          mutate(func(s *GitOpsSource) { s.ID = "Prod" }),
		"missing name":          mutate(func(s *GitOpsSource) { s.Name = "" }),
		"oversized name":        mutate(func(s *GitOpsSource) { s.Name = strings.Repeat("n", maxGitOpsNameBytes+1) }),
		"control in name":       mutate(func(s *GitOpsSource) { s.Name = "line\none" }),
		"unknown reconciler":    mutate(func(s *GitOpsSource) { s.Reconciler = "kustomize" }),
		"empty reconciler":      mutate(func(s *GitOpsSource) { s.Reconciler = "" }),
		"oversized repo url":    mutate(func(s *GitOpsSource) { s.RepoURL = "https://h.invalid/" + strings.Repeat("p", maxGitOpsRepoURLBytes) }),
		"oversized path":        mutate(func(s *GitOpsSource) { s.Path = strings.Repeat("p", maxGitOpsPathBytes+1) }),
		"missing ref":           mutate(func(s *GitOpsSource) { s.Ref = "" }),
		"oversized ref":         mutate(func(s *GitOpsSource) { s.Ref = strings.Repeat("r", maxGitOpsRefBytes+1) }),
		"traversing ref":        mutate(func(s *GitOpsSource) { s.Ref = "refs/../heads/main" }),
		"whitespace ref":        mutate(func(s *GitOpsSource) { s.Ref = "main branch" }),
		"missing cluster id":    mutate(func(s *GitOpsSource) { s.ClusterID = "" }),
		"invalid cluster id":    mutate(func(s *GitOpsSource) { s.ClusterID = "cl prod/../x" }),
		"duplicate id":          {gitOpsSource("dup"), gitOpsSource("dup")},
		"more than the ceiling": make([]GitOpsSource, maxTenantGitOpsSources+1),
	}
	for name, sources := range cases {
		if _, err := ValidateGitOpsSources(sources); !errors.Is(err, ErrInvalidGitOpsSources) {
			t.Errorf("%s accepted, want ErrInvalidGitOpsSources (got %v)", name, err)
		}
	}

	// A registry exactly at the ceiling is fine — the bound is on what a caller
	// may exceed, not on what a tenant may use.
	full := make([]GitOpsSource, 0, maxTenantGitOpsSources)
	for i := range maxTenantGitOpsSources {
		full = append(full, gitOpsSource(string(rune('a'+i%26))+string(rune('a'+i/26))))
	}
	if _, err := ValidateGitOpsSources(full); err != nil {
		t.Fatalf("a registry at the ceiling was refused: %v", err)
	}
	// Both reconcilers are real answers.
	argo := gitOpsSource("argo")
	argo.Reconciler = GitOpsReconcilerArgo
	if _, err := ValidateGitOpsSources([]GitOpsSource{argo}); err != nil {
		t.Fatalf("argo source refused: %v", err)
	}
}

func TestValidateGitOpsSourcesUsesGitRefRules(t *testing.T) {
	for _, ref := range []string{"main", "release/v1.2.3+build", "refs/tags/v2", "8c879ac"} {
		src := gitOpsSource("ref")
		src.Ref = ref
		if _, err := ValidateGitOpsSources([]GitOpsSource{src}); err != nil {
			t.Errorf("valid git ref %q refused: %v", ref, err)
		}
	}
	for _, ref := range []string{"@", "main/", "/main", "foo//bar", "foo.lock", ".hidden", "a/.hidden", "refs/heads/main.", "refs/heads/ma..in", "refs/heads/main@{1}", "main branch", `main\\branch`} {
		src := gitOpsSource("ref")
		src.Ref = ref
		if _, err := ValidateGitOpsSources([]GitOpsSource{src}); !errors.Is(err, ErrInvalidGitOpsSources) {
			t.Errorf("invalid git ref %q accepted: %v", ref, err)
		}
	}
}

// A registry is a set keyed by id, not an ordered document: two managers who
// agree on the content must not disagree about the revision, or every write
// after the first is a phantom conflict.
func TestGitOpsSourcesNormalizeToADeterministicRevision(t *testing.T) {
	forward := []GitOpsSource{gitOpsSource("alpha"), gitOpsSource("beta"), gitOpsSource("gamma")}
	reversed := []GitOpsSource{gitOpsSource("gamma"), gitOpsSource("beta"), gitOpsSource("alpha")}

	a, err := ValidateGitOpsSources(forward)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ValidateGitOpsSources(reversed)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a, b) {
		t.Fatalf("submission order survived normalization: %+v vs %+v", a, b)
	}
	if a[0].ID != "alpha" || a[2].ID != "gamma" {
		t.Fatalf("normalized registry is not sorted by id: %+v", a)
	}
	if gitOpsSourcesRevision(a) != gitOpsSourcesRevision(b) {
		t.Fatal("the same registry hashed to two revisions")
	}

	// Nil and empty are the same registry, because they are: there is no
	// server-owned default to inherit here.
	if gitOpsSourcesRevision(nil) != gitOpsSourcesRevision([]GitOpsSource{}) {
		t.Fatal("an empty registry and an absent one hashed differently")
	}
	// Any content change moves it, and reverting the change moves it back.
	changed := slices.Clone(a)
	changed[0].Ref = "release"
	if gitOpsSourcesRevision(changed) == gitOpsSourcesRevision(a) {
		t.Fatal("a changed ref did not move the revision")
	}
	changed[0].Ref = "main"
	if gitOpsSourcesRevision(changed) != gitOpsSourcesRevision(a) {
		t.Fatal("a reverted registry did not get its revision back")
	}
	if !strings.HasPrefix(gitOpsSourcesRevision(a), gitOpsRevisionPrefix) {
		t.Fatalf("revision %q is not prefixed", gitOpsSourcesRevision(a))
	}
}

func TestSetTenantGitOpsSourcesPersistsReloadsAndRefusesNonManagers(t *testing.T) {
	roles := map[string]string{
		"alice": RoleOwner, "adm": RoleAdmin, "mem": RoleMember, "view": RoleViewer,
	}
	s, spy, ids := manageStore(t, "cust_git", roles)
	by := HumanActor(ids["alice"], "cust_git")
	sources := []GitOpsSource{gitOpsSource("platform")}

	// Every member reads the registry, viewers included: it is what says how the
	// tenant deploys, and a seat that cannot see it cannot tell what is running.
	for sub, role := range roles {
		view, err := s.TenantGitOpsSourcesFor("cust_git", ids[sub])
		if err != nil {
			t.Fatalf("%s read sources: %v", sub, err)
		}
		if len(view.Sources) != 0 || view.Revision == "" || view.Role != role {
			t.Fatalf("%s: empty registry view = %+v", sub, view)
		}
	}

	written, err := s.SetTenantGitOpsSources("cust_git", ids["alice"], sources, by)
	if err != nil || !written.Changed || written.Role != RoleOwner {
		t.Fatalf("owner set sources = (%+v, %v)", written, err)
	}
	if !slices.Equal(written.Sources, sources) {
		t.Fatalf("stored registry = %+v", written.Sources)
	}

	events := spy.eventsWith(ActionGitOpsSourcesSet)
	if len(events) != 1 {
		t.Fatalf("gitops audit rows = %d, want 1", len(events))
	}
	row := events[0]
	if row.Actor.AccountID != ids["alice"] || row.Detail.Reason != ReasonGitOpsSourcesUpdated ||
		row.Detail.Rule != gitOpsSourcesRule || row.Detail.GitOpsSourcesRevision != written.Revision {
		t.Fatalf("audit row = %+v", row)
	}
	if len(row.Detail.GitOpsSources) != 1 ||
		row.Detail.GitOpsSources[0] != (AuditGitOpsSourceRef{ID: "platform", Reconciler: GitOpsReconcilerFlux}) {
		t.Fatalf("audit detail = %+v, want ids and reconcilers only", row.Detail.GitOpsSources)
	}
	// The journal carries ids and reconciler kinds and NOTHING else. A
	// repository URL, a ref and a path name systems outside this one, and an
	// audit row is the last place one belongs.
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("marshal audit row: %v", err)
	}
	for _, leak := range []string{"github.example.invalid", "clusters/prod", "Platform platform", "main", "cl-prod"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("audit row leaked source text %q: %s", leak, raw)
		}
	}

	reloaded := emptyStore()
	reloaded.applySnapshot(spy.recordedSnapshot(t))
	loaded, err := reloaded.TenantGitOpsSourcesFor("cust_git", ids["alice"])
	if err != nil || !slices.Equal(loaded.Sources, sources) || loaded.Revision != written.Revision {
		t.Fatalf("reloaded registry = (%+v, %v)", loaded, err)
	}

	// An identical replacement is not a change: it reconciles the durable row
	// without putting an edit nobody made into the evidence.
	again, err := s.SetTenantGitOpsSources("cust_git", ids["adm"], sources, HumanActor(ids["adm"], "cust_git"))
	if err != nil || again.Changed {
		t.Fatalf("identical replacement = (%+v, %v), want a silent reconcile", again, err)
	}
	if _, ok := spy.customers["cust_git"]; !ok {
		t.Fatal("an unchanged registry did not reconcile the durable customer row")
	}
	stored := spy.customerRow(t, "cust_git")
	delete(spy.customers, "cust_git")
	if _, err := s.SetTenantGitOpsSources("cust_git", ids["adm"], sources, HumanActor(ids["adm"], "cust_git")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unchanged registry recreated a deleted tenant: %v", err)
	}
	if _, exists := spy.customers["cust_git"]; exists {
		t.Fatal("refused registry write recreated the durable customer")
	}
	// Restore only the test fixture before exercising the remaining role matrix.
	if err := spy.upsertCustomer(stored); err != nil {
		t.Fatal(err)
	}
	if rows := spy.eventsWith(ActionGitOpsSourcesSet); len(rows) != 1 {
		t.Fatalf("unchanged replacement wrote audit rows: %d", len(rows))
	}

	for _, sub := range []string{"mem", "view"} {
		if _, err := s.SetTenantGitOpsSources("cust_git", ids[sub], nil, HumanActor(ids[sub], "cust_git")); !errors.Is(err, ErrNotAuthorized) {
			t.Errorf("%s set sources err = %v, want ErrNotAuthorized", sub, err)
		}
	}

	// A refused durable write publishes nothing: nothing may read a registry the
	// database still refuses.
	spy.appendErr = errPersist
	if _, err := s.SetTenantGitOpsSources("cust_git", ids["alice"], nil, by); !errors.Is(err, ErrPersistence) {
		t.Fatalf("persist failure err = %v, want ErrPersistence", err)
	}
	spy.appendErr = nil
	after, err := s.TenantGitOpsSourcesFor("cust_git", ids["alice"])
	if err != nil || !slices.Equal(after.Sources, sources) {
		t.Fatalf("failed write published a registry: (%+v, %v)", after, err)
	}
}

func TestSetTenantGitOpsSourcesRefusesStaleRevision(t *testing.T) {
	s, _, ids := manageStore(t, "cust_git", map[string]string{"alice": RoleOwner, "adm": RoleAdmin})
	loaded, err := s.TenantGitOpsSourcesFor("cust_git", ids["alice"])
	if err != nil {
		t.Fatal(err)
	}
	mine := []GitOpsSource{gitOpsSource("platform")}
	written, err := s.SetTenantGitOpsSourcesIfRevision(
		"cust_git", ids["alice"], loaded.Revision, mine, HumanActor(ids["alice"], "cust_git"),
	)
	if err != nil {
		t.Fatalf("first replacement: %v", err)
	}

	stale := []GitOpsSource{gitOpsSource("apps")}
	conflict, err := s.SetTenantGitOpsSourcesIfRevision(
		"cust_git", ids["adm"], loaded.Revision, stale, HumanActor(ids["adm"], "cust_git"),
	)
	if !errors.Is(err, ErrGitOpsSourcesConflict) {
		t.Fatalf("stale replacement err = %v, want ErrGitOpsSourcesConflict", err)
	}
	// The conflict carries what is actually stored, so the loser can show it.
	if conflict.Revision != written.Revision || !slices.Equal(conflict.Sources, mine) || conflict.Role != RoleAdmin {
		t.Fatalf("conflict view = %+v, want the current registry", conflict)
	}
	after, err := s.TenantGitOpsSourcesFor("cust_git", ids["adm"])
	if err != nil || !slices.Equal(after.Sources, mine) {
		t.Fatalf("stale write changed the registry: (%+v, %v)", after, err)
	}
	// A missing revision is refused rather than treated as unconditional: the
	// sentinel is an internal path, not something a caller can spell.
	if _, err := s.SetTenantGitOpsSourcesIfRevision("cust_git", ids["alice"], "", stale, HumanActor(ids["alice"], "cust_git")); !errors.Is(err, ErrInvalidGitOpsSources) {
		t.Fatalf("empty revision err = %v, want ErrInvalidGitOpsSources", err)
	}
}

// The registry is tenant-scoped state, and the answer for a tenant that is not
// yours is the answer for one that does not exist — so neither route can be used
// to enumerate tenant ids or read another tenant's repositories.
func TestTenantGitOpsSourcesAreNotReadableAcrossTenants(t *testing.T) {
	s, _, ids := manageStore(t, "cust_alpha", map[string]string{"alice": RoleOwner}, "stranger")
	s.AddCustomer(&Customer{ID: "cust_beta", Token: "tok_beta", Plan: "pro"})

	for _, tc := range []struct{ name, customer, caller string }{
		{"another tenant", "cust_beta", ids["alice"]},
		{"unknown tenant", "cust_nope", ids["alice"]},
		{"non-member", "cust_alpha", ids["stranger"]},
	} {
		if _, err := s.TenantGitOpsSourcesFor(tc.customer, tc.caller); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s read err = %v, want ErrNotFound", tc.name, err)
		}
		if _, err := s.SetTenantGitOpsSources(tc.customer, tc.caller, nil, HumanActor(tc.caller, tc.customer)); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s write err = %v, want ErrNotFound", tc.name, err)
		}
	}
}

// The env-seed path re-adds its customers from config on every boot, knowing
// nothing about a registry an owner published later. A blind overwrite would
// silently drop a tenant's sources at the next restart.
func TestGitOpsSourcesSurviveEnvSeedRefresh(t *testing.T) {
	s := New()
	registered := []GitOpsSource{gitOpsSource("platform")}
	s.AddCustomer(&Customer{ID: "cust_seed", Token: "tok_seed", Plan: "pro", GitOpsSources: registered})
	s.AddCustomer(&Customer{ID: "cust_seed", Token: "tok_seed", Plan: "pro"})

	got, err := s.CustomerByID("cust_seed")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if !slices.Equal(got.GitOpsSources, registered) {
		t.Fatalf("registry was not preserved across a seed refresh: %+v", got.GitOpsSources)
	}
}

// The customer document is persisted as JSON, so the registry has to survive the
// round trip the durable backend makes — including the revision, which is what a
// replacement is checked against after a restart.
func TestGitOpsSourcesJSONRoundTrip(t *testing.T) {
	for name, sources := range map[string][]GitOpsSource{
		"absent":     nil,
		"empty":      {},
		"registered": {gitOpsSource("platform"), gitOpsSource("apps")},
	} {
		var out Customer
		mustRoundTrip(t, &Customer{ID: "c", Token: "t", GitOpsSources: sources}, &out)
		if len(out.GitOpsSources) != len(sources) {
			t.Fatalf("%s: sources lost in round-trip: %+v", name, out.GitOpsSources)
		}
		for i := range sources {
			if out.GitOpsSources[i] != sources[i] {
				t.Fatalf("%s: source %d changed across a round trip: %+v", name, i, out.GitOpsSources[i])
			}
		}
		if gitOpsSourcesRevision(out.GitOpsSources) != gitOpsSourcesRevision(sources) {
			t.Fatalf("%s: revision moved across a round trip", name)
		}
	}
}
