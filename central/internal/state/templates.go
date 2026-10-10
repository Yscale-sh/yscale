package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/yscale-sh/yscale/pkg/workload"
)

var (
	// ErrInvalidTemplateCatalog rejects a tenant launch catalog outside the closed
	// catalog grammar.
	ErrInvalidTemplateCatalog = errors.New("invalid template catalog")
	// ErrTemplateCatalogConflict rejects a whole-catalog replacement made from a
	// stale view. Without it, two managers can publish different documents under
	// the same per-template version and make provenance ambiguous.
	ErrTemplateCatalogConflict = errors.New("template catalog changed since it was read")
)

const (
	// DefaultWorkloadTemplateVersion is the version every server-owned template
	// carries. It moves when a default's shape changes, and it moves for all of
	// them together: a version is what a submission pins itself to, so a tenant
	// that reviewed a template must be able to tell that the thing behind the id
	// is still what they reviewed.
	DefaultWorkloadTemplateVersion = 3

	// The two execution modes a template's defaults may open the launch form in.
	// Not a placement decision — the spec decides that — but the catalog has to
	// say which one it means, because a GPU template whose submission carries no
	// GPU is a different workload from the one the tenant published.
	WorkloadTemplateModeCPU = "cpu"
	WorkloadTemplateModeGPU = "gpu"

	templateCatalogRule        = "tenant.workload_templates"
	templateCatalogRuleVersion = "v1"

	// The catalog is durable tenant state rendered by a browser and echoed in
	// refusals, so every string in it is bounded here rather than wherever it is
	// displayed. The counts are generous against the three templates the console
	// ships and small enough that a whole catalog fits in one bounded body.
	maxTenantTemplates          = 32
	maxTemplateTitleBytes       = 120
	maxTemplateKindBytes        = 64
	maxTemplateDescBytes        = 400
	maxTemplateMarkBytes        = 8
	maxTemplateImageBytes       = 512
	maxTemplateRecipeItems      = 16
	maxTemplateRecipeTokenBytes = 256
	maxTemplateRecipeBytes      = 2048
	maxTemplateEnvEncodedBytes  = 16 * 1024
	maxTemplateVersion          = 1 << 20

	templateRevisionPrefix               = "rev_"
	unconditionalTemplateCatalogRevision = "\x00"
)

var (
	// Template ids are referenced in a request header, so they are the narrow
	// DNS-1123-label shape the console already uses for the three defaults.
	templateIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$`)
	// The default workload name has to survive into metadata.name, so it is the
	// same shape the launch form validates before it renders YAML.
	templateNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,61}[a-z0-9])?$`)
	// A GPU kind is checked for shape and not against a catalogue: the backends
	// own which kinds they can place, and a second copy of that list here would
	// be the copy that goes stale. A default the provider cannot place is
	// refused at admission, exactly as a hand-written one is.
	templateGPUKindPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`)
)

// WorkloadTemplateDefaults is what a template opens the launch form on. It is
// editable form state, not an immutable spec: central re-validates the submitted
// document either way, and the template provenance says where a launch began,
// not that the submitter left every default unchanged.
type WorkloadTemplateDefaults struct {
	Name    string            `json:"name"`
	Image   string            `json:"image"`
	Size    string            `json:"size"`
	Mode    string            `json:"mode"`
	Command []string          `json:"command,omitempty"`
	Args    []string          `json:"args,omitempty"`
	Env     []workload.EnvVar `json:"env,omitempty" yaml:"env,omitempty"`
	// GPUKind is only meaningful under mode "gpu"; empty means the launch form's
	// own cheapest-GPU default.
	GPUKind string `json:"gpuKind,omitempty"`
}

// WorkloadTemplate is one launch template a tenant offers its members.
//
// ID and Version together are what a submission references, and neither is
// central's to change: an id whose shape moved without its version moving is a
// tenant launching something other than what they published.
type WorkloadTemplate struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
	// Mark is the two-or-three character badge the console renders in the
	// template gallery.
	Mark        string `json:"mark,omitempty"`
	Title       string `json:"title"`
	Kind        string `json:"kind"`
	Description string `json:"description,omitempty"`
	// NodeOnly publishes bare capacity: the submission may carry no image, and
	// the connector's own controller owns whatever runs on the node.
	NodeOnly bool `json:"nodeOnly,omitempty"`
	// Disabled retires an entry without deleting it, so the history of what a
	// tenant used to offer stays readable while new submissions cannot name it.
	// The submit path treats disabled and absent as one answer.
	Disabled bool                     `json:"disabled,omitempty"`
	Defaults WorkloadTemplateDefaults `json:"defaults"`
}

// WorkloadTemplateCatalog is a tenant's whole published set.
//
// A POINTER to this is what a Customer holds, and the pointer is the contract:
// nil is "this tenant has never published a catalog and inherits the server's",
// while a non-nil catalog with no entries is "this tenant publishes nothing",
// which is a different and deliberate answer. A bare slice cannot hold that
// distinction — nil and empty both marshal away — so the pointer is what makes
// the two durable.
type WorkloadTemplateCatalog struct {
	Templates []WorkloadTemplate `json:"templates"`
}

// defaultWorkloadTemplates is the server-owned catalog: the three launch
// templates the console has always rendered from its own module, now answered
// by central so a tenant that has never published one still has a catalog its
// submissions can reference.
//
// A fresh copy per call, because the caller hands it to a tenant that may edit
// it — a shared backing array would let one tenant's edit rewrite the defaults
// every other tenant reads.
func defaultWorkloadTemplates() []WorkloadTemplate {
	return []WorkloadTemplate{
		{
			ID:          "container-job",
			Version:     DefaultWorkloadTemplateVersion,
			Mark:        "01",
			Title:       "Generic container job",
			Kind:        "Run-once job",
			Description: "Run an OCI image to completion on fresh CPU or GPU capacity.",
			Defaults: WorkloadTemplateDefaults{
				Name:  "container-job",
				Image: "docker.io/library/busybox:1.36",
				Size:  "small",
				Mode:  WorkloadTemplateModeCPU,
			},
		},
		{
			ID:          "pytorch-training",
			Version:     DefaultWorkloadTemplateVersion,
			Mark:        "PT",
			Title:       "PyTorch training job",
			Kind:        "Run-once GPU job",
			Description: "Launch a PyTorch image with an explicit GPU shape and budget ceiling.",
			Defaults: WorkloadTemplateDefaults{
				Name:    "pytorch-train",
				Image:   "docker.io/pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
				Size:    "large",
				Mode:    WorkloadTemplateModeGPU,
				GPUKind: "rtx4000ada",
				// Deterministic, bounded training recipe: no network fetches and a
				// fixed loop count. The escaped newlines stay printable catalog data
				// while Python's exec turns them into the loop body at runtime.
				Command: []string{"python"},
				Args:    []string{"-c", `import torch; d="cuda"; w=torch.tensor(0.,device=d); b=torch.tensor(0.,device=d); x=torch.arange(4.,device=d); y=2*x+1; exec("for _ in range(3):\n e=w*x+b-y\n w-=.1*(e*x).mean()\n b-=.1*e.mean()"); print(float(w),float(b))`},
			},
		},
		{
			ID:          "node-capacity",
			Version:     DefaultWorkloadTemplateVersion,
			Mark:        "N+",
			Title:       "Node-only capacity",
			Kind:        "Capacity request",
			Description: "Provision a burst node for a controller that already owns the workload.",
			NodeOnly:    true,
			Defaults: WorkloadTemplateDefaults{
				Name:  "node-capacity",
				Image: "",
				Size:  "medium",
				Mode:  WorkloadTemplateModeCPU,
			},
		},
	}
}

// DefaultWorkloadTemplateCatalog is the server-owned catalog as a value, for
// callers seeding or comparing against it.
func DefaultWorkloadTemplateCatalog() WorkloadTemplateCatalog {
	return WorkloadTemplateCatalog{Templates: defaultWorkloadTemplates()}
}

// Effective materializes the catalog a submission is judged against. Safe on a
// nil receiver, which is the whole point: the stored nil stays nil so a tenant
// that never published one keeps tracking the server defaults as they move.
func (c *WorkloadTemplateCatalog) Effective() WorkloadTemplateCatalog {
	if c == nil {
		return DefaultWorkloadTemplateCatalog()
	}
	return *copyTemplateCatalog(c)
}

// Custom reports whether the tenant published this catalog rather than
// inheriting the server's.
func (c *WorkloadTemplateCatalog) Custom() bool { return c != nil }

// Revision names the exact catalog a submission was verified against.
//
// A digest of the effective entries rather than a counter, and deliberately: it
// needs no column, every replica computes the same answer from the record
// alone, and a tenant that reverts an edit gets back the revision it had
// before — which is the honest answer, the catalog being the same catalog.
func (c *WorkloadTemplateCatalog) Revision() string {
	effective := c.Effective()
	raw, err := json.Marshal(effective.Templates)
	if err != nil {
		// WorkloadTemplate holds nothing encoding/json can refuse, so this is
		// unreachable; a fixed revision keeps it from becoming a nil provenance
		// stamp if it ever is reached.
		return templateRevisionPrefix + strings.Repeat("0", 16)
	}
	sum := sha256.Sum256(raw)
	return templateRevisionPrefix + hex.EncodeToString(sum[:8])
}

// Template returns the ENABLED entry with this id, or nil.
//
// Disabled and absent are one answer on purpose: a retired template is not
// offered, and a caller that could tell the two apart could enumerate what a
// tenant used to run by probing ids.
func (c *WorkloadTemplateCatalog) Template(id string) *WorkloadTemplate {
	effective := c.Effective()
	for i := range effective.Templates {
		if effective.Templates[i].ID == id && !effective.Templates[i].Disabled {
			return &effective.Templates[i]
		}
	}
	return nil
}

// TemplateRef is the catalog entry a workload was launched from, stamped once
// at submission. The revision travels with the id and version because the two
// alone do not identify a catalog: a tenant may republish an id at the same
// version, and provenance that could not tell those apart would attribute a run
// to a template it never came from.
type TemplateRef struct {
	ID              string `json:"id"`
	Version         int    `json:"version"`
	CatalogRevision string `json:"catalog_revision"`
}

// ValidateWorkloadTemplateCatalog checks a replacement catalog against the
// closed grammar and returns the copy that will be stored.
//
// It never normalizes a catalog back to nil, even one identical to the server
// defaults, and that is the difference from the cluster policy: a tenant that
// publishes today's defaults is pinning them, and collapsing that to "inherit"
// would silently move their catalog the next time central's own changed.
func ValidateWorkloadTemplateCatalog(in WorkloadTemplateCatalog) (*WorkloadTemplateCatalog, error) {
	if len(in.Templates) > maxTenantTemplates {
		return nil, fmt.Errorf("%w: catalog may contain at most %d templates", ErrInvalidTemplateCatalog, maxTenantTemplates)
	}
	out := WorkloadTemplateCatalog{Templates: make([]WorkloadTemplate, 0, len(in.Templates))}
	seen := make(map[string]bool, len(in.Templates))
	for _, t := range in.Templates {
		if !templateIDPattern.MatchString(t.ID) {
			return nil, fmt.Errorf("%w: invalid template id %q", ErrInvalidTemplateCatalog, t.ID)
		}
		if seen[t.ID] {
			return nil, fmt.Errorf("%w: duplicate template id %q", ErrInvalidTemplateCatalog, t.ID)
		}
		seen[t.ID] = true
		if t.Version < 1 || t.Version > maxTemplateVersion {
			return nil, fmt.Errorf("%w: template %q version must be 1..%d", ErrInvalidTemplateCatalog, t.ID, maxTemplateVersion)
		}
		if err := boundedTemplateText(t.ID, "title", t.Title, 1, maxTemplateTitleBytes); err != nil {
			return nil, err
		}
		if err := boundedTemplateText(t.ID, "kind", t.Kind, 1, maxTemplateKindBytes); err != nil {
			return nil, err
		}
		if err := boundedTemplateText(t.ID, "description", t.Description, 0, maxTemplateDescBytes); err != nil {
			return nil, err
		}
		if err := boundedTemplateText(t.ID, "mark", t.Mark, 0, maxTemplateMarkBytes); err != nil {
			return nil, err
		}
		if err := validateTemplateDefaults(t); err != nil {
			return nil, err
		}
		out.Templates = append(out.Templates, cloneWorkloadTemplate(t))
	}
	return &out, nil
}

// validateTemplateDefaults holds the one rule the catalog shares with the
// submit path: a template's execution class — bare capacity or a container —
// is decided by nodeOnly, and the defaults have to agree with it. A node-only
// template carrying an image publishes a launch central would refuse.
func validateTemplateDefaults(t WorkloadTemplate) error {
	d := t.Defaults
	if !templateNamePattern.MatchString(d.Name) {
		return fmt.Errorf("%w: template %q default name must be a lowercase DNS name of at most 63 characters", ErrInvalidTemplateCatalog, t.ID)
	}
	if _, ok := workload.LookupSize(d.Size); !ok {
		return fmt.Errorf("%w: template %q default size %q is not a supported size", ErrInvalidTemplateCatalog, t.ID, d.Size)
	}
	if d.Mode != WorkloadTemplateModeCPU && d.Mode != WorkloadTemplateModeGPU {
		return fmt.Errorf("%w: template %q default mode must be %q or %q", ErrInvalidTemplateCatalog, t.ID, WorkloadTemplateModeCPU, WorkloadTemplateModeGPU)
	}
	if d.GPUKind != "" {
		if d.Mode != WorkloadTemplateModeGPU {
			return fmt.Errorf("%w: template %q declares a gpu kind under mode %q", ErrInvalidTemplateCatalog, t.ID, d.Mode)
		}
		if !templateGPUKindPattern.MatchString(d.GPUKind) {
			return fmt.Errorf("%w: template %q default gpu kind %q is not a gpu kind", ErrInvalidTemplateCatalog, t.ID, d.GPUKind)
		}
	}
	if t.NodeOnly {
		if d.Image != "" {
			return fmt.Errorf("%w: template %q is node-only and may not carry a default image", ErrInvalidTemplateCatalog, t.ID)
		}
		if len(d.Command) > 0 {
			return fmt.Errorf("%w: template %q is node-only and may not carry a default command", ErrInvalidTemplateCatalog, t.ID)
		}
		if len(d.Args) > 0 {
			return fmt.Errorf("%w: template %q is node-only and may not carry default args", ErrInvalidTemplateCatalog, t.ID)
		}
		if len(d.Env) > 0 {
			return fmt.Errorf("%w: template %q is node-only and may not carry default env", ErrInvalidTemplateCatalog, t.ID)
		}
		return nil
	}
	if err := validateTemplateEnv(t.ID, d.Env); err != nil {
		return err
	}
	if err := validateTemplateRecipeTokens(t.ID, "default command", d.Command, false); err != nil {
		return err
	}
	// Empty args are intentionally rejected here, even though Kubernetes accepts
	// them, because this path only needs a deterministic, conservative catalog
	// surface and explicit empty tokens are easy to misuse as placeholder data.
	if err := validateTemplateRecipeTokens(t.ID, "default args", d.Args, false); err != nil {
		return err
	}
	return boundedTemplateText(t.ID, "default image", d.Image, 1, maxTemplateImageBytes)
}

func validateTemplateEnv(id string, env []workload.EnvVar) error {
	for i, entry := range env {
		if entry.ValueFrom == nil || entry.Value != "" {
			return fmt.Errorf("%w: template %q default env[%d] must use valueFrom and may not carry a literal value", ErrInvalidTemplateCatalog, id, i)
		}
	}
	if err := workload.ValidateEnv(env); err != nil {
		return fmt.Errorf("%w: template %q default env is invalid: %v", ErrInvalidTemplateCatalog, id, err)
	}
	encoded, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("%w: template %q default env cannot be encoded: %v", ErrInvalidTemplateCatalog, id, err)
	}
	if len(encoded) > maxTemplateEnvEncodedBytes {
		return fmt.Errorf("%w: template %q default env may encode to at most %d bytes", ErrInvalidTemplateCatalog, id, maxTemplateEnvEncodedBytes)
	}
	return nil
}

// boundedTemplateText refuses catalog text that is missing where it is
// required, over its byte bound, or carrying anything but printable single-line
// content. Bounded on bytes rather than runes: the bound exists so a catalog
// cannot become a log sink or a durable record nobody can read back, and bytes
// are what both of those are measured in.
func boundedTemplateText(id, field, value string, min, max int) error {
	if len(value) < min {
		return fmt.Errorf("%w: template %q %s is required", ErrInvalidTemplateCatalog, id, field)
	}
	if len(value) > max {
		return fmt.Errorf("%w: template %q %s may be at most %d bytes", ErrInvalidTemplateCatalog, id, field, max)
	}
	if strings.ContainsFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return fmt.Errorf("%w: template %q %s must be printable text on one line", ErrInvalidTemplateCatalog, id, field)
	}
	return nil
}

func cloneWorkloadTemplateDefaults(d WorkloadTemplateDefaults) WorkloadTemplateDefaults {
	if len(d.Command) > 0 {
		d.Command = slices.Clone(d.Command)
	}
	if len(d.Args) > 0 {
		d.Args = slices.Clone(d.Args)
	}
	if d.Env != nil {
		d.Env = workload.CloneEnv(d.Env)
	}
	return d
}

func cloneWorkloadTemplate(t WorkloadTemplate) WorkloadTemplate {
	t.Defaults = cloneWorkloadTemplateDefaults(t.Defaults)
	return t
}

func validateTemplateRecipeTokens(id, field string, tokens []string, allowEmpty bool) error {
	if len(tokens) > maxTemplateRecipeItems {
		return fmt.Errorf("%w: template %q %s may have at most %d tokens", ErrInvalidTemplateCatalog, id, field, maxTemplateRecipeItems)
	}

	var bytes int
	for _, token := range tokens {
		if token == "" && !allowEmpty {
			return fmt.Errorf("%w: template %q %s must not contain empty tokens", ErrInvalidTemplateCatalog, id, field)
		}
		if len(token) > maxTemplateRecipeTokenBytes {
			return fmt.Errorf("%w: template %q %s token may be at most %d bytes", ErrInvalidTemplateCatalog, id, field, maxTemplateRecipeTokenBytes)
		}
		if strings.ContainsFunc(token, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
			return fmt.Errorf("%w: template %q %s token must contain printable characters", ErrInvalidTemplateCatalog, id, field)
		}
		bytes += len(token)
		if bytes > maxTemplateRecipeBytes {
			return fmt.Errorf("%w: template %q %s may be at most %d bytes total", ErrInvalidTemplateCatalog, id, field, maxTemplateRecipeBytes)
		}
	}
	return nil
}

func sameTemplateCatalog(a, b *WorkloadTemplateCatalog) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if len(a.Templates) != len(b.Templates) {
		return false
	}
	for i := range a.Templates {
		if !sameTemplate(a.Templates[i], b.Templates[i]) {
			return false
		}
	}
	return true
}

func sameTemplate(a, b WorkloadTemplate) bool {
	return a.ID == b.ID &&
		a.Version == b.Version &&
		a.Mark == b.Mark &&
		a.Title == b.Title &&
		a.Kind == b.Kind &&
		a.Description == b.Description &&
		a.NodeOnly == b.NodeOnly &&
		a.Disabled == b.Disabled &&
		a.Defaults.Name == b.Defaults.Name &&
		a.Defaults.Image == b.Defaults.Image &&
		a.Defaults.Size == b.Defaults.Size &&
		a.Defaults.Mode == b.Defaults.Mode &&
		a.Defaults.GPUKind == b.Defaults.GPUKind &&
		slices.Equal(a.Defaults.Command, b.Defaults.Command) &&
		slices.Equal(a.Defaults.Args, b.Defaults.Args) &&
		slices.EqualFunc(a.Defaults.Env, b.Defaults.Env, sameTemplateEnvVar)
}

func sameTemplateEnvVar(a, b workload.EnvVar) bool {
	if a.Name != b.Name || a.Value != b.Value || (a.ValueFrom == nil) != (b.ValueFrom == nil) {
		return false
	}
	if a.ValueFrom == nil {
		return true
	}
	return sameTemplateKeyRef(a.ValueFrom.SecretKeyRef, b.ValueFrom.SecretKeyRef) &&
		sameTemplateKeyRef(a.ValueFrom.ConfigMapKeyRef, b.ValueFrom.ConfigMapKeyRef)
}

func sameTemplateKeyRef(a, b *workload.KeyRef) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func copyTemplateCatalog(c *WorkloadTemplateCatalog) *WorkloadTemplateCatalog {
	if c == nil {
		return nil
	}
	out := make([]WorkloadTemplate, 0, len(c.Templates))
	for _, t := range c.Templates {
		out = append(out, cloneWorkloadTemplate(t))
	}
	return &WorkloadTemplateCatalog{Templates: out}
}

// auditTemplateEntries reduces a catalog to what the journal may hold: one id
// and version per entry, in published order. Titles, descriptions, marks and
// images are tenant-authored text, and an audit row is the last place that
// should be able to carry one — so there is no field here to put them in.
func auditTemplateEntries(c *WorkloadTemplateCatalog) []AuditTemplateRef {
	effective := c.Effective()
	out := make([]AuditTemplateRef, 0, len(effective.Templates))
	for _, t := range effective.Templates {
		out = append(out, AuditTemplateRef{ID: t.ID, Version: t.Version})
	}
	return out
}

// TenantTemplateCatalogView is one answer about a tenant's launch catalog: the
// effective entries, the revision a submission is verified against, the
// caller's role, and whether the tenant owns the catalog or inherits it.
type TenantTemplateCatalogView struct {
	Catalog  WorkloadTemplateCatalog
	Revision string
	Role     string
	Custom   bool
	Changed  bool
}

// TenantTemplateCatalogFor returns the catalog visible to any member of a live
// tenant, viewers included: a seat that cannot read the templates cannot launch
// from one, and the ids are what its own submissions have to name.
//
// A tenant that has never published a catalog reads the server defaults rather
// than an empty list, because the defaults are what its submissions are
// actually judged against.
func (s *Store) TenantTemplateCatalogFor(customerID, callerAccountID string) (TenantTemplateCatalogView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return TenantTemplateCatalogView{}, err
	}
	c := s.customers[customerID]
	return TenantTemplateCatalogView{
		Catalog:  c.TemplateCatalog.Effective(),
		Revision: c.TemplateCatalog.Revision(),
		Role:     caller.Role,
		Custom:   c.TemplateCatalog.Custom(),
	}, nil
}

// AutomationTenantTemplateCatalogFor is the catalog-publisher read seam. It
// authorizes only a server-minted publisher id already pinned to customerID;
// unlike the human seam it has no membership role to broaden into.
func (s *Store) AutomationTenantTemplateCatalogFor(customerID, publisherID string) (TenantTemplateCatalogView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.customers[customerID]
	if c == nil || c.Revoked() || !hasCatalogPublisher(c, publisherID) {
		return TenantTemplateCatalogView{}, ErrNotFound
	}
	return TenantTemplateCatalogView{
		Catalog:  c.TemplateCatalog.Effective(),
		Revision: c.TemplateCatalog.Revision(),
		Custom:   c.TemplateCatalog.Custom(),
	}, nil
}

// SetTenantTemplateCatalog replaces a tenant's published catalog. Owner/admin
// may write it; every other role gets ErrNotAuthorized.
//
// SetTenantClusterPolicy's shape, and for its reasons: the durable write lands
// before the in-memory pointer moves, so no submission is judged against a
// catalog the database may still refuse; a behaviorally-identical replacement
// is still written durably but produces no audit row, so a record whose
// best-effort boot write never landed can be reconciled without a journal entry
// for a change that did not happen.
func (s *Store) SetTenantTemplateCatalog(customerID, callerAccountID string, catalog WorkloadTemplateCatalog, by Actor) (TenantTemplateCatalogView, error) {
	return s.SetTenantTemplateCatalogIfRevision(customerID, callerAccountID, unconditionalTemplateCatalogRevision, catalog, by)
}

// SetTenantTemplateCatalogIfRevision is SetTenantTemplateCatalog with an
// optimistic concurrency check performed under the same lock as authorization
// and the snapshot. The human HTTP surface always supplies the revision it
// read; SetTenantTemplateCatalog is the explicit unconditional internal path.
func (s *Store) SetTenantTemplateCatalogIfRevision(customerID, callerAccountID, expectedRevision string, catalog WorkloadTemplateCatalog, by Actor) (TenantTemplateCatalogView, error) {
	normalized, err := ValidateWorkloadTemplateCatalog(catalog)
	if err != nil {
		return TenantTemplateCatalogView{}, err
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return TenantTemplateCatalogView{}, err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		role := caller.Role
		s.mu.Unlock()
		return TenantTemplateCatalogView{Role: role}, fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, role)
	}
	c := s.customers[customerID]
	if expectedRevision != unconditionalTemplateCatalogRevision && (expectedRevision == "" || len(expectedRevision) > 64) {
		role := caller.Role
		s.mu.Unlock()
		return TenantTemplateCatalogView{Role: role}, fmt.Errorf("%w: catalog_revision is required", ErrInvalidTemplateCatalog)
	}
	if expectedRevision != unconditionalTemplateCatalogRevision && expectedRevision != c.TemplateCatalog.Revision() {
		view := TenantTemplateCatalogView{
			Catalog:  c.TemplateCatalog.Effective(),
			Revision: c.TemplateCatalog.Revision(),
			Role:     caller.Role,
			Custom:   c.TemplateCatalog.Custom(),
		}
		s.mu.Unlock()
		return view, ErrTemplateCatalogConflict
	}
	unchanged := sameTemplateCatalog(c.TemplateCatalog, normalized)
	snapshot := *c
	snapshot.TemplateCatalog = copyTemplateCatalog(normalized)
	role := caller.Role
	s.mu.Unlock()

	view := TenantTemplateCatalogView{
		Catalog:  normalized.Effective(),
		Revision: normalized.Revision(),
		Role:     role,
		Custom:   true,
		Changed:  !unchanged,
	}
	var ev *AuditEvent
	if !unchanged {
		ev = NewAuditEvent(AuditEvent{
			CustomerID: customerID,
			Actor:      by,
			Action:     ActionTemplateCatalogSet,
			Outcome:    OutcomeAccepted,
			TargetKind: TargetTenant,
			TargetID:   customerID,
			Detail: AuditDetail{
				Reason:                  ReasonTemplateCatalogUpdated,
				Role:                    role,
				Rule:                    templateCatalogRule,
				RuleVersion:             templateCatalogRuleVersion,
				TemplateCatalogRevision: view.Revision,
				TemplateCatalog:         auditTemplateEntries(normalized),
			},
		})
	}
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return TenantTemplateCatalogView{Role: role}, fmt.Errorf("%w: persist template catalog %s: %w", ErrPersistence, customerID, err)
	}
	if unchanged {
		return view, nil
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.TemplateCatalog = copyTemplateCatalog(normalized)
	}
	s.mu.Unlock()
	return view, nil
}

// SetAutomationTenantTemplateCatalogIfRevision is the publisher-only write
// seam. It preserves the human route's validation, optimistic concurrency,
// no-op, persistence and audit behavior without manufacturing a human role.
func (s *Store) SetAutomationTenantTemplateCatalogIfRevision(customerID, publisherID, expectedRevision string, catalog WorkloadTemplateCatalog) (TenantTemplateCatalogView, error) {
	normalized, err := ValidateWorkloadTemplateCatalog(catalog)
	if err != nil {
		return TenantTemplateCatalogView{}, err
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c := s.customers[customerID]
	if c == nil || c.Revoked() || !hasCatalogPublisher(c, publisherID) {
		s.mu.Unlock()
		return TenantTemplateCatalogView{}, ErrNotFound
	}
	if expectedRevision == "" || len(expectedRevision) > 64 {
		s.mu.Unlock()
		return TenantTemplateCatalogView{}, fmt.Errorf("%w: catalog_revision is required", ErrInvalidTemplateCatalog)
	}
	if expectedRevision != c.TemplateCatalog.Revision() {
		view := TenantTemplateCatalogView{
			Catalog: c.TemplateCatalog.Effective(), Revision: c.TemplateCatalog.Revision(), Custom: c.TemplateCatalog.Custom(),
		}
		s.mu.Unlock()
		return view, ErrTemplateCatalogConflict
	}
	unchanged := sameTemplateCatalog(c.TemplateCatalog, normalized)
	snapshot := *c
	snapshot.TemplateCatalog = copyTemplateCatalog(normalized)
	s.mu.Unlock()

	view := TenantTemplateCatalogView{
		Catalog: normalized.Effective(), Revision: normalized.Revision(), Custom: true, Changed: !unchanged,
	}
	var ev *AuditEvent
	if !unchanged {
		ev = NewAuditEvent(AuditEvent{
			CustomerID: customerID,
			Actor:      CatalogPublisherActor(customerID, publisherID),
			Action:     ActionTemplateCatalogSet,
			Outcome:    OutcomeAccepted,
			TargetKind: TargetTenant,
			TargetID:   customerID,
			Detail: AuditDetail{
				Reason: ReasonTemplateCatalogUpdated, Rule: templateCatalogRule, RuleVersion: templateCatalogRuleVersion,
				TemplateCatalogRevision: view.Revision, TemplateCatalog: auditTemplateEntries(normalized),
			},
		})
	}
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return TenantTemplateCatalogView{}, fmt.Errorf("%w: persist template catalog %s: %w", ErrPersistence, customerID, err)
	}
	if unchanged {
		return view, nil
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() && hasCatalogPublisher(live, publisherID) {
		live.TemplateCatalog = copyTemplateCatalog(normalized)
	}
	s.mu.Unlock()
	return view, nil
}

func hasCatalogPublisher(c *Customer, publisherID string) bool {
	for _, p := range c.CatalogPublishers {
		if p != nil && p.ID == publisherID {
			return true
		}
	}
	return false
}
