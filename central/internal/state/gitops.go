package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

var (
	// ErrInvalidGitOpsSources rejects a tenant GitOps registry outside the closed
	// source grammar.
	ErrInvalidGitOpsSources = errors.New("invalid gitops sources")
	// ErrGitOpsSourcesConflict rejects a whole-registry replacement made from a
	// stale view, so two managers editing the same tenant cannot silently
	// overwrite each other's entries.
	ErrGitOpsSourcesConflict = errors.New("gitops sources changed since they were read")
)

const (
	// The two reconcilers a source may name. A closed set rather than free text:
	// the id is what a later slice would dispatch on, and a value nothing can
	// reconcile is a source that looks configured and never runs.
	GitOpsReconcilerFlux = "flux"
	GitOpsReconcilerArgo = "argo"

	gitOpsSourcesRule        = "tenant.gitops_sources"
	gitOpsSourcesRuleVersion = "v1"

	// The registry is durable tenant state rendered by a browser and echoed in
	// refusals, so every string is bounded here rather than wherever it is
	// displayed. The count is generous against the handful of repositories a
	// tenant reconciles and small enough that a whole registry fits in one
	// bounded body.
	maxTenantGitOpsSources = 32
	maxGitOpsNameBytes     = 120
	maxGitOpsRepoURLBytes  = 400
	maxGitOpsPathBytes     = 256
	maxGitOpsRefBytes      = 128

	gitOpsRevisionPrefix               = "gitops_"
	unconditionalGitOpsSourcesRevision = "\x00"
)

var (
	// Source ids are referenced by a browser client and echoed in audit rows, so
	// they are the same narrow DNS-1123-label shape the template catalog uses.
	gitOpsSourceIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$`)
	// The scp-like SSH form Flux and Argo both accept — git@host:org/repo.git.
	// The username charset excludes ':' by construction, which is what keeps a
	// password out of the form that has no scheme to parse.
	gitOpsSCPPattern = regexp.MustCompile(`^(?:[A-Za-z0-9._-]+@)?[A-Za-z0-9.-]+:[A-Za-z0-9._~/-]+$`)
)

// GitOpsSource is one Git repository a tenant reconciles into one of its
// clusters.
//
// CONFIGURATION ONLY. There is no credential, token, or secret-reference field,
// and that is the point rather than an omission: this record is readable by
// every member of the tenant, echoed in a browser response, and persisted in the
// customer document. Whatever the reconciler authenticates with lives in the
// cluster, where the reconciler runs — central holds the coordinates and nothing
// else.
type GitOpsSource struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Reconciler is "flux" or "argo": which controller in the target cluster
	// owns this source.
	Reconciler string `json:"reconciler"`
	// RepoURL is an https:// or ssh:// (or scp-like) repository URL carrying no
	// embedded credential.
	RepoURL string `json:"repo_url"`
	// Path is repository-relative; empty means the repository root.
	Path string `json:"path"`
	// Ref is the branch, tag, or commit the source tracks. Named Ref rather than
	// "revision" because the registry has a revision of its own, and a field
	// that could be mistaken for it is a stale-write check nobody can reason
	// about.
	Ref string `json:"ref"`
	// ClusterID names which of the tenant's registered clusters reconciles this
	// source. Validated for shape only — a cluster may be registered after its
	// sources are, and a registry that refused forward references would make the
	// two writes order-dependent.
	ClusterID string `json:"cluster_id"`
}

// TenantGitOpsSourcesView is one answer about a tenant's registry: the stored
// sources, the revision a replacement must carry back, and the caller's role.
type TenantGitOpsSourcesView struct {
	Sources  []GitOpsSource
	Revision string
	Role     string
	Changed  bool
}

// ValidateGitOpsSources checks a replacement registry against the closed grammar
// and returns the copy that will be stored.
//
// The stored copy is sorted by id, which is what makes the revision deterministic
// for callers that submit the same set in a different order: a registry is a set
// of sources keyed by id, not an ordered document, so two managers agreeing on
// the content should not disagree about the revision.
func ValidateGitOpsSources(in []GitOpsSource) ([]GitOpsSource, error) {
	if len(in) > maxTenantGitOpsSources {
		return nil, fmt.Errorf("%w: registry may contain at most %d sources", ErrInvalidGitOpsSources, maxTenantGitOpsSources)
	}
	out := make([]GitOpsSource, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, src := range in {
		if !gitOpsSourceIDPattern.MatchString(src.ID) {
			return nil, fmt.Errorf("%w: invalid source id %q", ErrInvalidGitOpsSources, src.ID)
		}
		if seen[src.ID] {
			return nil, fmt.Errorf("%w: duplicate source id %q", ErrInvalidGitOpsSources, src.ID)
		}
		seen[src.ID] = true
		if err := boundedGitOpsText(src.ID, "name", src.Name, 1, maxGitOpsNameBytes); err != nil {
			return nil, err
		}
		if src.Reconciler != GitOpsReconcilerFlux && src.Reconciler != GitOpsReconcilerArgo {
			return nil, fmt.Errorf("%w: source %q reconciler must be %q or %q", ErrInvalidGitOpsSources, src.ID, GitOpsReconcilerFlux, GitOpsReconcilerArgo)
		}
		if err := boundedGitOpsText(src.ID, "repo_url", src.RepoURL, 1, maxGitOpsRepoURLBytes); err != nil {
			return nil, err
		}
		if err := validateGitOpsRepoURL(src.ID, src.RepoURL); err != nil {
			return nil, err
		}
		if err := boundedGitOpsText(src.ID, "path", src.Path, 0, maxGitOpsPathBytes); err != nil {
			return nil, err
		}
		if err := validateGitOpsPath(src.ID, src.Path); err != nil {
			return nil, err
		}
		if err := boundedGitOpsText(src.ID, "ref", src.Ref, 1, maxGitOpsRefBytes); err != nil {
			return nil, err
		}
		if !validGitOpsRef(src.Ref) {
			return nil, fmt.Errorf("%w: source %q ref %q is not a branch, tag or commit", ErrInvalidGitOpsSources, src.ID, src.Ref)
		}
		if !ValidClusterID(src.ClusterID) {
			return nil, fmt.Errorf("%w: source %q names an invalid cluster id %q", ErrInvalidGitOpsSources, src.ID, src.ClusterID)
		}
		out = append(out, src)
	}
	slices.SortFunc(out, func(a, b GitOpsSource) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

// validGitOpsRef applies git-check-ref-format's rules to the branch, tag, or
// commit ref a reconciler tracks. It intentionally accepts ordinary punctuation
// such as '+' while rejecting values Git itself cannot resolve or store.
func validGitOpsRef(ref string) bool {
	if ref == "" || ref == "@" || strings.HasPrefix(ref, "/") || strings.HasSuffix(ref, "/") ||
		strings.HasSuffix(ref, ".") || strings.Contains(ref, "//") || strings.Contains(ref, "..") ||
		strings.Contains(ref, "@{") || strings.ContainsAny(ref, " ~^:?*[\\") {
		return false
	}
	for _, r := range ref {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	for _, component := range strings.Split(ref, "/") {
		if strings.HasPrefix(component, ".") || strings.HasSuffix(component, ".lock") {
			return false
		}
	}
	return true
}

// validateGitOpsRepoURL accepts the repository forms Flux and Argo actually take
// and refuses the one thing this record must never carry: a credential.
//
// For https that means NO userinfo at all, not merely no password — a bare
// "https://<token>@host/org/repo" is exactly how a forge personal access token
// is smuggled into a URL. ssh keeps a username, because "git@" is the form and
// carries no secret, but never a password. Query strings and fragments are
// refused in every form: neither reconciler needs one, and both are places a
// secret ends up.
func validateGitOpsRepoURL(id, raw string) error {
	if strings.ContainsAny(raw, " \t?#") {
		return fmt.Errorf("%w: source %q repo_url may not carry whitespace, a query string or a fragment", ErrInvalidGitOpsSources, id)
	}
	if !strings.Contains(raw, "://") {
		if !gitOpsSCPPattern.MatchString(raw) {
			return fmt.Errorf("%w: source %q repo_url %q is not an https:// or ssh repository url", ErrInvalidGitOpsSources, id, raw)
		}
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: source %q repo_url is not a url", ErrInvalidGitOpsSources, id)
	}
	if u.Scheme != "https" && u.Scheme != "ssh" {
		return fmt.Errorf("%w: source %q repo_url scheme must be https or ssh", ErrInvalidGitOpsSources, id)
	}
	if u.Host == "" || u.Opaque != "" {
		return fmt.Errorf("%w: source %q repo_url must name a host", ErrInvalidGitOpsSources, id)
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return fmt.Errorf("%w: source %q repo_url may not carry a query string or fragment", ErrInvalidGitOpsSources, id)
	}
	if u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword || u.Scheme == "https" {
			return fmt.Errorf("%w: source %q repo_url may not embed credentials", ErrInvalidGitOpsSources, id)
		}
		if u.User.Username() == "" {
			return fmt.Errorf("%w: source %q repo_url may not embed credentials", ErrInvalidGitOpsSources, id)
		}
	}
	return nil
}

// validateGitOpsPath refuses anything that is not a clean repository-relative
// path. Absolute and parent-traversing paths are the ones that matter: a source
// is a coordinate INTO a repository, and a path that can climb out of it is a
// coordinate into whatever the reconciler's working tree sits next to.
func validateGitOpsPath(id, path string) error {
	if path == "" {
		return nil
	}
	if strings.HasPrefix(path, "/") || strings.Contains(path, `\`) {
		return fmt.Errorf("%w: source %q path must be repository-relative", ErrInvalidGitOpsSources, id)
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("%w: source %q path %q must be a clean repository-relative path", ErrInvalidGitOpsSources, id, path)
		}
	}
	return nil
}

// boundedGitOpsText refuses registry text that is missing where it is required,
// over its byte bound, or carrying anything but printable single-line content.
// Bounded on bytes for boundedTemplateText's reason: the bound exists so the
// registry cannot become a log sink, and bytes are what that is measured in.
func boundedGitOpsText(id, field, value string, min, max int) error {
	if len(value) < min {
		return fmt.Errorf("%w: source %q %s is required", ErrInvalidGitOpsSources, id, field)
	}
	if len(value) > max {
		return fmt.Errorf("%w: source %q %s may be at most %d bytes", ErrInvalidGitOpsSources, id, field, max)
	}
	if strings.ContainsFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return fmt.Errorf("%w: source %q %s must be printable text on one line", ErrInvalidGitOpsSources, id, field)
	}
	return nil
}

// gitOpsSourcesRevision names the exact registry a replacement was written
// against.
//
// A digest of the normalized entries rather than a counter, for the template
// catalog's reasons: it needs no column, every replica computes the same answer
// from the record alone, and a tenant that reverts an edit gets back the
// revision it had before. A tenant with no sources and one that removed its last
// source are the same registry and hash the same, because they are.
func gitOpsSourcesRevision(sources []GitOpsSource) string {
	if sources == nil {
		sources = []GitOpsSource{}
	}
	raw, err := json.Marshal(sources)
	if err != nil {
		// GitOpsSource holds nothing encoding/json can refuse, so this is
		// unreachable; a fixed revision keeps it from becoming a nil stamp if it
		// ever is reached.
		return gitOpsRevisionPrefix + strings.Repeat("0", 16)
	}
	sum := sha256.Sum256(raw)
	return gitOpsRevisionPrefix + hex.EncodeToString(sum[:8])
}

func copyGitOpsSources(sources []GitOpsSource) []GitOpsSource {
	if sources == nil {
		return nil
	}
	return slices.Clone(sources)
}

// auditGitOpsEntries reduces a registry to what the journal may hold: one id and
// reconciler per source, in stored order. Repository URLs, refs, paths and names
// are tenant-authored strings naming systems outside this one, and an audit row
// is the last place that should be able to carry one — so there is no field here
// to put them in.
func auditGitOpsEntries(sources []GitOpsSource) []AuditGitOpsSourceRef {
	out := make([]AuditGitOpsSourceRef, 0, len(sources))
	for _, src := range sources {
		out = append(out, AuditGitOpsSourceRef{ID: src.ID, Reconciler: src.Reconciler})
	}
	return out
}

// TenantGitOpsSourcesFor returns the registry visible to any member of a live
// tenant, viewers included: the sources are what the tenant's clusters
// reconcile, and a seat that cannot see them cannot tell what is deployed.
func (s *Store) TenantGitOpsSourcesFor(customerID, callerAccountID string) (TenantGitOpsSourcesView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return TenantGitOpsSourcesView{}, err
	}
	c := s.customers[customerID]
	return TenantGitOpsSourcesView{
		Sources:  copyGitOpsSources(c.GitOpsSources),
		Revision: gitOpsSourcesRevision(c.GitOpsSources),
		Role:     caller.Role,
	}, nil
}

// SetTenantGitOpsSources replaces a tenant's registry unconditionally.
// Owner/admin may write it; every other role gets ErrNotAuthorized.
func (s *Store) SetTenantGitOpsSources(customerID, callerAccountID string, sources []GitOpsSource, by Actor) (TenantGitOpsSourcesView, error) {
	return s.SetTenantGitOpsSourcesIfRevision(customerID, callerAccountID, unconditionalGitOpsSourcesRevision, sources, by)
}

// SetTenantGitOpsSourcesIfRevision is SetTenantGitOpsSources with an optimistic
// concurrency check performed under the same lock as authorization and the
// snapshot. The human HTTP surface always supplies the revision it read;
// SetTenantGitOpsSources is the explicit unconditional internal path.
//
// SetTenantTemplateCatalogIfRevision's shape, and for its reasons: the durable
// write lands before the in-memory slice moves, so nothing reads a registry the
// database may still refuse; a behaviorally-identical replacement is still
// written durably but produces no audit row, so a record whose best-effort boot
// write never landed can be reconciled without a journal entry for a change that
// did not happen.
func (s *Store) SetTenantGitOpsSourcesIfRevision(customerID, callerAccountID, expectedRevision string, sources []GitOpsSource, by Actor) (TenantGitOpsSourcesView, error) {
	normalized, err := ValidateGitOpsSources(sources)
	if err != nil {
		return TenantGitOpsSourcesView{}, err
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return TenantGitOpsSourcesView{}, err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		role := caller.Role
		s.mu.Unlock()
		return TenantGitOpsSourcesView{Role: role}, fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, role)
	}
	c := s.customers[customerID]
	if expectedRevision != unconditionalGitOpsSourcesRevision && (expectedRevision == "" || len(expectedRevision) > 64) {
		role := caller.Role
		s.mu.Unlock()
		return TenantGitOpsSourcesView{Role: role}, fmt.Errorf("%w: sources_revision is required", ErrInvalidGitOpsSources)
	}
	if expectedRevision != unconditionalGitOpsSourcesRevision && expectedRevision != gitOpsSourcesRevision(c.GitOpsSources) {
		view := TenantGitOpsSourcesView{
			Sources:  copyGitOpsSources(c.GitOpsSources),
			Revision: gitOpsSourcesRevision(c.GitOpsSources),
			Role:     caller.Role,
		}
		s.mu.Unlock()
		return view, ErrGitOpsSourcesConflict
	}
	unchanged := slices.Equal(c.GitOpsSources, normalized)
	snapshot := *c
	snapshot.GitOpsSources = copyGitOpsSources(normalized)
	role := caller.Role
	s.mu.Unlock()

	view := TenantGitOpsSourcesView{
		Sources:  copyGitOpsSources(normalized),
		Revision: gitOpsSourcesRevision(normalized),
		Role:     role,
		Changed:  !unchanged,
	}
	var ev *AuditEvent
	if !unchanged {
		ev = NewAuditEvent(AuditEvent{
			CustomerID: customerID,
			Actor:      by,
			Action:     ActionGitOpsSourcesSet,
			Outcome:    OutcomeAccepted,
			TargetKind: TargetTenant,
			TargetID:   customerID,
			Detail: AuditDetail{
				Reason:                ReasonGitOpsSourcesUpdated,
				Role:                  role,
				Rule:                  gitOpsSourcesRule,
				RuleVersion:           gitOpsSourcesRuleVersion,
				GitOpsSourcesRevision: view.Revision,
				GitOpsSources:         auditGitOpsEntries(normalized),
			},
		})
	}
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return TenantGitOpsSourcesView{Role: role}, fmt.Errorf("%w: persist gitops sources %s: %w", ErrPersistence, customerID, err)
	}
	if unchanged {
		return view, nil
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.GitOpsSources = copyGitOpsSources(normalized)
	}
	s.mu.Unlock()
	return view, nil
}
