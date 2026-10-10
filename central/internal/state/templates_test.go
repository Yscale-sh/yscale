package state

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// The server defaults are what every tenant that has never published a catalog
// launches from, so they have to satisfy the same grammar a tenant's own
// replacement does — a default central would refuse from a tenant is a launch
// nobody can make.
func TestDefaultTemplateCatalogIsValidAndMatchesTheConsole(t *testing.T) {
	catalog := DefaultWorkloadTemplateCatalog()
	if _, err := ValidateWorkloadTemplateCatalog(catalog); err != nil {
		t.Fatalf("server default catalog fails its own grammar: %v", err)
	}

	want := map[string]WorkloadTemplate{
		"container-job": {
			ID: "container-job", Version: DefaultWorkloadTemplateVersion, Mark: "01",
			Title: "Generic container job", Kind: "Run-once job",
			Defaults: WorkloadTemplateDefaults{
				Name: "container-job", Image: "docker.io/library/busybox:1.36",
				Size: "small", Mode: WorkloadTemplateModeCPU,
			},
		},
		"pytorch-training": {
			ID: "pytorch-training", Version: DefaultWorkloadTemplateVersion, Mark: "PT",
			Title: "PyTorch training job", Kind: "Run-once GPU job",
			Defaults: WorkloadTemplateDefaults{
				Name: "pytorch-train", Image: "docker.io/pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
				Size: "large", Mode: WorkloadTemplateModeGPU, GPUKind: "rtx4000ada",
				Command: []string{"python"},
				Args:    []string{"-c", `import torch; d="cuda"; w=torch.tensor(0.,device=d); b=torch.tensor(0.,device=d); x=torch.arange(4.,device=d); y=2*x+1; exec("for _ in range(3):\n e=w*x+b-y\n w-=.1*(e*x).mean()\n b-=.1*e.mean()"); print(float(w),float(b))`},
			},
		},
		"node-capacity": {
			ID: "node-capacity", Version: DefaultWorkloadTemplateVersion, Mark: "N+",
			Title: "Node-only capacity", Kind: "Capacity request", NodeOnly: true,
			Defaults: WorkloadTemplateDefaults{
				Name: "node-capacity", Image: "", Size: "medium", Mode: WorkloadTemplateModeCPU,
			},
		},
	}
	if len(catalog.Templates) != len(want) {
		t.Fatalf("default templates = %d, want %d", len(catalog.Templates), len(want))
	}
	for _, got := range catalog.Templates {
		expected, ok := want[got.ID]
		if !ok {
			t.Fatalf("unexpected default template %q", got.ID)
		}
		expected.Description = got.Description // prose is not pinned; shape is
		if !reflect.DeepEqual(got, expected) {
			t.Errorf("default template %q = %+v, want %+v", got.ID, got, expected)
		}
		if got.Description == "" {
			t.Errorf("default template %q has no description", got.ID)
		}
	}

	// A caller may edit what it is handed, so the defaults must not be one
	// shared array every tenant reads.
	first := DefaultWorkloadTemplateCatalog()
	first.Templates[0].Title = "rewritten"
	if DefaultWorkloadTemplateCatalog().Templates[0].Title == "rewritten" {
		t.Fatal("the default catalog is shared: one caller's edit rewrote it for everybody")
	}
}

func TestTemplateCatalogValidation(t *testing.T) {
	valid := WorkloadTemplateCatalog{Templates: []WorkloadTemplate{
		{
			ID: "team-trainer", Version: 7, Title: "Team trainer", Kind: "Run-once GPU job",
			Description: "The shape our team runs.", Mark: "TT",
			Defaults: WorkloadTemplateDefaults{
				Name: "team-trainer", Image: "ghcr.io/acme/trainer:1", Size: "large",
				Mode: WorkloadTemplateModeGPU, GPUKind: "h100",
				Command: []string{"sh", "-c"},
				Args:    []string{"echo", "training"},
			},
		},
		{
			ID: "bare-node", Version: 1, Title: "Bare node", Kind: "Capacity request", NodeOnly: true,
			Defaults: WorkloadTemplateDefaults{Name: "bare-node", Size: "medium", Mode: WorkloadTemplateModeCPU},
		},
	}}
	stored, err := ValidateWorkloadTemplateCatalog(valid)
	if err != nil {
		t.Fatalf("valid catalog rejected: %v", err)
	}
	if stored == nil || !reflect.DeepEqual(stored.Templates, valid.Templates) {
		t.Fatalf("stored catalog = %+v", stored)
	}
	stored.Templates[0].Defaults.Command[0] = "python"
	stored.Templates[0].Defaults.Args[0] = "tampered"
	if valid.Templates[0].Defaults.Command[0] != "sh" || valid.Templates[0].Defaults.Args[0] != "echo" {
		t.Fatal("catalog validation output aliases mutable command/args from caller input")
	}
	snapshot := copyTemplateCatalog(&valid)
	snapshot.Templates[0].Defaults.Command[0] = "snapshot"
	snapshot.Templates[0].Defaults.Args[0] = "snapshot"
	if valid.Templates[0].Defaults.Command[0] != "sh" || valid.Templates[0].Defaults.Args[0] != "echo" {
		t.Fatal("catalog copy aliases mutable command/args from caller input")
	}

	// An empty catalog is a tenant that publishes nothing, which is a different
	// and deliberate answer from never having published one. It must survive as
	// an explicit catalog rather than collapsing back to "inherit".
	empty, err := ValidateWorkloadTemplateCatalog(WorkloadTemplateCatalog{})
	if err != nil || empty == nil || len(empty.Templates) != 0 {
		t.Fatalf("empty catalog = (%+v, %v), want an explicit empty catalog", empty, err)
	}
	// Nor does publishing today's defaults mean inheriting them: a tenant that
	// pins the catalog must keep it when central's own moves.
	pinned, err := ValidateWorkloadTemplateCatalog(DefaultWorkloadTemplateCatalog())
	if err != nil || pinned == nil || !pinned.Custom() {
		t.Fatalf("catalog equal to the defaults = (%+v, %v), want a tenant-owned catalog", pinned, err)
	}

	base := func(mutate func(*WorkloadTemplate)) WorkloadTemplateCatalog {
		t := WorkloadTemplate{
			ID: "ok", Version: 1, Title: "Ok", Kind: "Run-once job",
			Defaults: WorkloadTemplateDefaults{Name: "ok", Image: "busybox:1", Size: "small", Mode: WorkloadTemplateModeCPU},
		}
		mutate(&t)
		return WorkloadTemplateCatalog{Templates: []WorkloadTemplate{t}}
	}
	tests := map[string]WorkloadTemplateCatalog{
		"empty id":                base(func(t *WorkloadTemplate) { t.ID = "" }),
		"upper-case id":           base(func(t *WorkloadTemplate) { t.ID = "Container-Job" }),
		"id with a space":         base(func(t *WorkloadTemplate) { t.ID = "container job" }),
		"over-long id":            base(func(t *WorkloadTemplate) { t.ID = strings.Repeat("a", 65) }),
		"zero version":            base(func(t *WorkloadTemplate) { t.Version = 0 }),
		"negative version":        base(func(t *WorkloadTemplate) { t.Version = -1 }),
		"absurd version":          base(func(t *WorkloadTemplate) { t.Version = 1 << 30 }),
		"no title":                base(func(t *WorkloadTemplate) { t.Title = "" }),
		"over-long title":         base(func(t *WorkloadTemplate) { t.Title = strings.Repeat("t", 121) }),
		"newline in title":        base(func(t *WorkloadTemplate) { t.Title = "one\ntwo" }),
		"no kind":                 base(func(t *WorkloadTemplate) { t.Kind = "" }),
		"over-long description":   base(func(t *WorkloadTemplate) { t.Description = strings.Repeat("d", 401) }),
		"over-long mark":          base(func(t *WorkloadTemplate) { t.Mark = "123456789" }),
		"bad default name":        base(func(t *WorkloadTemplate) { t.Defaults.Name = "Not A Name" }),
		"unsupported size":        base(func(t *WorkloadTemplate) { t.Defaults.Size = "enormous" }),
		"unsupported mode":        base(func(t *WorkloadTemplate) { t.Defaults.Mode = "tpu" }),
		"gpu kind without gpu":    base(func(t *WorkloadTemplate) { t.Defaults.GPUKind = "h100" }),
		"bad gpu kind":            base(func(t *WorkloadTemplate) { t.Defaults.Mode, t.Defaults.GPUKind = WorkloadTemplateModeGPU, "H 100" }),
		"container with no image": base(func(t *WorkloadTemplate) { t.Defaults.Image = "" }),
		"over-long image":         base(func(t *WorkloadTemplate) { t.Defaults.Image = strings.Repeat("i", 513) }),
		"command item overflow": base(func(t *WorkloadTemplate) {
			t.Defaults.Command = make([]string, maxTemplateRecipeItems+1)
			for i := range t.Defaults.Command {
				t.Defaults.Command[i] = "echo"
			}
		}),
		"command token too long": base(func(t *WorkloadTemplate) {
			t.Defaults.Command = []string{strings.Repeat("c", maxTemplateRecipeTokenBytes+1)}
		}),
		"command token control char": base(func(t *WorkloadTemplate) {
			t.Defaults.Command = []string{"sh\000"}
		}),
		"command empty token": base(func(t *WorkloadTemplate) {
			t.Defaults.Command = []string{""}
		}),
		"args empty token": base(func(t *WorkloadTemplate) {
			t.Defaults.Args = []string{""}
		}),
		"args token too long": base(func(t *WorkloadTemplate) {
			t.Defaults.Args = []string{strings.Repeat("a", maxTemplateRecipeTokenBytes+1)}
		}),
		"args aggregate too long": base(func(t *WorkloadTemplate) {
			payload := strings.Repeat("a", maxTemplateRecipeTokenBytes)
			for i := 0; i < 10; i++ {
				t.Defaults.Args = append(t.Defaults.Args, payload)
			}
		}),
		"node-only with an image": base(func(t *WorkloadTemplate) { t.NodeOnly = true }),
		"node-only with command": base(func(t *WorkloadTemplate) {
			t.NodeOnly = true
			t.Defaults.Command = []string{"sh", "-c"}
		}),
		"node-only with args": base(func(t *WorkloadTemplate) {
			t.NodeOnly = true
			t.Defaults.Args = []string{"ok"}
		}),
	}
	validRecipe := WorkloadTemplateCatalog{Templates: []WorkloadTemplate{{
		ID: "cmd-args", Version: 1, Title: "Command args", Kind: "Run-once job", Mark: "A1",
		Defaults: WorkloadTemplateDefaults{
			Name: "cmd-args", Image: "busybox:1.36", Size: "small", Mode: WorkloadTemplateModeCPU,
			Command: []string{"sh", "-c"},
			Args:    []string{"echo", "ok"},
		},
	}}}
	if _, err := ValidateWorkloadTemplateCatalog(validRecipe); err != nil {
		t.Fatalf("valid command recipe rejected: %v", err)
	}
	tests["duplicate ids"] = WorkloadTemplateCatalog{Templates: []WorkloadTemplate{
		base(func(*WorkloadTemplate) {}).Templates[0],
		base(func(t *WorkloadTemplate) { t.Version = 2 }).Templates[0],
	}}
	tooMany := WorkloadTemplateCatalog{Templates: make([]WorkloadTemplate, 0, maxTenantTemplates+1)}
	for i := 0; i <= maxTenantTemplates; i++ {
		entry := base(func(*WorkloadTemplate) {}).Templates[0]
		entry.ID = "t" + string(rune('a'+i%26)) + strings.Repeat("z", i/26)
		tooMany.Templates = append(tooMany.Templates, entry)
	}
	tests["over the catalog bound"] = tooMany

	for name, catalog := range tests {
		if _, err := ValidateWorkloadTemplateCatalog(catalog); !errors.Is(err, ErrInvalidTemplateCatalog) {
			t.Errorf("%s: err = %v, want ErrInvalidTemplateCatalog", name, err)
		}
	}
}

// The revision names the exact catalog a submission was verified against, so it
// has to move with the content and only with the content — a counter that moved
// on an identical replacement would make two identical catalogs look different
// to every stamp that carries it.
func TestTemplateCatalogRevisionTracksContent(t *testing.T) {
	var inherited *WorkloadTemplateCatalog
	defaults := DefaultWorkloadTemplateCatalog()
	if inherited.Revision() != defaults.Revision() {
		t.Fatal("an inherited catalog does not carry the default catalog's revision")
	}
	if inherited.Custom() || !defaults.Custom() {
		t.Fatalf("custom = (%v, %v), want inherit then tenant-owned", inherited.Custom(), defaults.Custom())
	}

	bumped := DefaultWorkloadTemplateCatalog()
	bumped.Templates[0].Version = DefaultWorkloadTemplateVersion + 1
	if bumped.Revision() == defaults.Revision() {
		t.Fatal("a version bump did not move the catalog revision")
	}
	reverted := DefaultWorkloadTemplateCatalog()
	if reverted.Revision() != defaults.Revision() {
		t.Fatal("reverting a catalog did not restore its revision")
	}
	if !strings.HasPrefix(defaults.Revision(), templateRevisionPrefix) {
		t.Fatalf("revision = %q, want the %q prefix", defaults.Revision(), templateRevisionPrefix)
	}
}

// A retired entry is not offered, and it is indistinguishable from one that was
// never there: a caller able to tell the two apart could enumerate what a
// tenant used to run by probing ids.
func TestTemplateLookupHidesDisabledAndUnknownAlike(t *testing.T) {
	catalog := WorkloadTemplateCatalog{Templates: []WorkloadTemplate{
		{ID: "live", Version: 1, Title: "Live", Kind: "Run-once job",
			Defaults: WorkloadTemplateDefaults{Name: "live", Image: "busybox:1", Size: "small", Mode: WorkloadTemplateModeCPU}},
		{ID: "retired", Version: 4, Title: "Retired", Kind: "Run-once job", Disabled: true,
			Defaults: WorkloadTemplateDefaults{Name: "retired", Image: "busybox:1", Size: "small", Mode: WorkloadTemplateModeCPU}},
	}}
	if entry := catalog.Template("live"); entry == nil || entry.Version != 1 {
		t.Fatalf("live template = %+v", entry)
	}
	if entry := catalog.Template("retired"); entry != nil {
		t.Fatalf("disabled template resolved: %+v", entry)
	}
	if entry := catalog.Template("never-existed"); entry != nil {
		t.Fatalf("unknown template resolved: %+v", entry)
	}
	// A disabled entry is still PUBLISHED, so a manager can see what they
	// retired rather than having to remember it.
	if len(catalog.Effective().Templates) != 2 {
		t.Fatal("a disabled entry disappeared from the catalog a manager reads")
	}
}

func TestSetTenantTemplateCatalogPersistsReloadsAndRefusesNonManagers(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_tmpl", map[string]string{
		"alice": RoleOwner, "adm": RoleAdmin, "mem": RoleMember, "view": RoleViewer,
	})
	by := HumanActor(ids["alice"], "cust_tmpl")
	catalog := WorkloadTemplateCatalog{Templates: []WorkloadTemplate{{
		ID: "team-trainer", Version: 7, Title: "Team trainer", Kind: "Run-once GPU job",
		Description: "What our team runs.", Mark: "TT",
		Defaults: WorkloadTemplateDefaults{
			Name: "team-trainer", Image: "ghcr.io/acme/trainer:1",
			Size: "large", Mode: WorkloadTemplateModeGPU, GPUKind: "h100",
		},
	}}}

	// Every member reads the catalog; a tenant that has published none reads
	// the defaults, because that is what its submissions are judged against.
	for _, sub := range []string{"alice", "adm", "mem", "view"} {
		view, err := s.TenantTemplateCatalogFor("cust_tmpl", ids[sub])
		if err != nil {
			t.Fatalf("%s read catalog: %v", sub, err)
		}
		if view.Custom || len(view.Catalog.Templates) != 3 || view.Revision == "" {
			t.Fatalf("%s: default view = %+v", sub, view)
		}
	}

	written, err := s.SetTenantTemplateCatalog("cust_tmpl", ids["alice"], catalog, by)
	if err != nil || !written.Changed || written.Role != RoleOwner || !written.Custom {
		t.Fatalf("owner set catalog = (%+v, %v)", written, err)
	}
	if !reflect.DeepEqual(written.Catalog.Templates, catalog.Templates) {
		t.Fatalf("stored catalog = %+v", written.Catalog)
	}

	events := spy.eventsWith(ActionTemplateCatalogSet)
	if len(events) != 1 {
		t.Fatalf("template catalog audit rows = %d, want 1", len(events))
	}
	row := events[0]
	if row.Actor.AccountID != ids["alice"] || row.Detail.Reason != ReasonTemplateCatalogUpdated ||
		row.Detail.Rule != templateCatalogRule || row.Detail.TemplateCatalogRevision != written.Revision {
		t.Fatalf("audit row = %+v", row)
	}
	if len(row.Detail.TemplateCatalog) != 1 ||
		row.Detail.TemplateCatalog[0] != (AuditTemplateRef{ID: "team-trainer", Version: 7}) {
		t.Fatalf("audit catalog detail = %+v, want ids and versions only", row.Detail.TemplateCatalog)
	}
	// The journal carries ids and versions and NOTHING else. A template's
	// image, title and prose are tenant-authored strings, and an audit row is
	// the last place one belongs.
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("marshal audit row: %v", err)
	}
	for _, leak := range []string{"ghcr.io/acme/trainer:1", "Team trainer", "What our team runs.", "h100", "TT"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("audit row leaked template text %q: %s", leak, raw)
		}
	}

	reloaded := emptyStore()
	reloaded.applySnapshot(spy.recordedSnapshot(t))
	loaded, err := reloaded.TenantTemplateCatalogFor("cust_tmpl", ids["alice"])
	if err != nil || !loaded.Custom || !reflect.DeepEqual(loaded.Catalog.Templates, catalog.Templates) {
		t.Fatalf("reloaded catalog = (%+v, %v)", loaded, err)
	}
	if loaded.Revision != written.Revision {
		t.Fatalf("reloaded revision = %q, want %q", loaded.Revision, written.Revision)
	}

	// An identical replacement is not a change: it reconciles the durable row
	// without putting an edit nobody made into the evidence.
	again, err := s.SetTenantTemplateCatalog("cust_tmpl", ids["adm"], catalog, HumanActor(ids["adm"], "cust_tmpl"))
	if err != nil || again.Changed {
		t.Fatalf("identical replacement = (%+v, %v), want a silent reconcile", again, err)
	}
	if _, ok := spy.customers["cust_tmpl"]; !ok {
		t.Fatal("an unchanged catalog did not reconcile the durable customer row")
	}
	stored := spy.customerRow(t, "cust_tmpl")
	delete(spy.customers, "cust_tmpl")
	if _, err := s.SetTenantTemplateCatalog("cust_tmpl", ids["adm"], catalog, HumanActor(ids["adm"], "cust_tmpl")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unchanged catalog recreated a deleted tenant: %v", err)
	}
	if _, exists := spy.customers["cust_tmpl"]; exists {
		t.Fatal("refused catalog write recreated the durable customer")
	}
	// Restore only the test fixture before exercising the remaining role matrix.
	if err := spy.upsertCustomer(stored); err != nil {
		t.Fatal(err)
	}
	if rows := spy.eventsWith(ActionTemplateCatalogSet); len(rows) != 1 {
		t.Fatalf("unchanged replacement wrote audit rows: %d", len(rows))
	}

	for _, sub := range []string{"mem", "view"} {
		if _, err := s.SetTenantTemplateCatalog("cust_tmpl", ids[sub], WorkloadTemplateCatalog{}, HumanActor(ids[sub], "cust_tmpl")); !errors.Is(err, ErrNotAuthorized) {
			t.Errorf("%s set catalog err = %v, want ErrNotAuthorized", sub, err)
		}
	}

	// A refused durable write publishes nothing: a submission must never be
	// judged against a catalog the database still refuses.
	spy.appendErr = errPersist
	if _, err := s.SetTenantTemplateCatalog("cust_tmpl", ids["alice"], WorkloadTemplateCatalog{}, by); !errors.Is(err, ErrPersistence) {
		t.Fatalf("persist failure err = %v, want ErrPersistence", err)
	}
	spy.appendErr = nil
	after, err := s.TenantTemplateCatalogFor("cust_tmpl", ids["alice"])
	if err != nil || !reflect.DeepEqual(after.Catalog.Templates, catalog.Templates) {
		t.Fatalf("failed write published a catalog: (%+v, %v)", after, err)
	}
}

func TestSetTenantTemplateCatalogRefusesStaleRevision(t *testing.T) {
	s, _, ids := manageStore(t, "cust_tmpl", map[string]string{
		"alice": RoleOwner, "adm": RoleAdmin,
	})
	loaded, err := s.TenantTemplateCatalogFor("cust_tmpl", ids["alice"])
	if err != nil {
		t.Fatal(err)
	}
	catalog := DefaultWorkloadTemplateCatalog()
	catalog.Templates[0].Title = "Alice's approved container"
	written, err := s.SetTenantTemplateCatalogIfRevision(
		"cust_tmpl", ids["alice"], loaded.Revision, catalog, HumanActor(ids["alice"], "cust_tmpl"),
	)
	if err != nil {
		t.Fatalf("first replacement: %v", err)
	}
	stale := DefaultWorkloadTemplateCatalog()
	stale.Templates[0].Title = "Admin's stale container"
	if _, err := s.SetTenantTemplateCatalogIfRevision(
		"cust_tmpl", ids["adm"], loaded.Revision, stale, HumanActor(ids["adm"], "cust_tmpl"),
	); !errors.Is(err, ErrTemplateCatalogConflict) {
		t.Fatalf("stale replacement err = %v, want ErrTemplateCatalogConflict", err)
	}
	after, err := s.TenantTemplateCatalogFor("cust_tmpl", ids["adm"])
	if err != nil || after.Revision != written.Revision || after.Catalog.Templates[0].Title != "Alice's approved container" {
		t.Fatalf("stale write changed catalog: (%+v, %v)", after, err)
	}
}

// The catalog is tenant-scoped state, and the answer for a tenant that is not
// yours is the answer for one that does not exist — so neither route can be
// used to enumerate tenant ids or read another tenant's templates.
func TestTenantTemplateCatalogIsNotReadableAcrossTenants(t *testing.T) {
	s, _, ids := manageStore(t, "cust_alpha", map[string]string{"alice": RoleOwner}, "stranger")
	s.AddCustomer(&Customer{ID: "cust_beta", Token: "tok_beta", Plan: "pro"})

	for _, tc := range []struct{ name, customer, caller string }{
		{"another tenant", "cust_beta", ids["alice"]},
		{"unknown tenant", "cust_nope", ids["alice"]},
		{"non-member", "cust_alpha", ids["stranger"]},
	} {
		if _, err := s.TenantTemplateCatalogFor(tc.customer, tc.caller); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s read err = %v, want ErrNotFound", tc.name, err)
		}
		if _, err := s.SetTenantTemplateCatalog(tc.customer, tc.caller, WorkloadTemplateCatalog{}, HumanActor(tc.caller, tc.customer)); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s write err = %v, want ErrNotFound", tc.name, err)
		}
	}
}

// The env-seed path re-adds its customers from config on every boot, knowing
// nothing about a catalog an owner published later. A blind overwrite would
// silently revert a tenant's templates at the next restart.
func TestTemplateCatalogSurvivesEnvSeedRefresh(t *testing.T) {
	s := New()
	published := &WorkloadTemplateCatalog{Templates: []WorkloadTemplate{{
		ID: "only-ours", Version: 3, Title: "Only ours", Kind: "Run-once job",
		Defaults: WorkloadTemplateDefaults{Name: "only-ours", Image: "busybox:1", Size: "small", Mode: WorkloadTemplateModeCPU},
	}}}
	s.AddCustomer(&Customer{ID: "cust_seed", Token: "tok_seed", Plan: "pro", TemplateCatalog: published})
	s.AddCustomer(&Customer{ID: "cust_seed", Token: "tok_seed", Plan: "pro"})

	got, err := s.CustomerByID("cust_seed")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if got.TemplateCatalog == nil || !reflect.DeepEqual(got.TemplateCatalog.Templates, published.Templates) {
		t.Fatalf("catalog was not preserved across a seed refresh: %+v", got.TemplateCatalog)
	}
}

// The customer document is persisted as JSON, so the catalog has to survive the
// round trip the durable backend makes — including the nil that means "inherit"
// and the empty catalog that means "publish nothing", which are different
// answers a lossy encoding would flatten into one.
func TestTemplateCatalogJSONRoundTrip(t *testing.T) {
	for name, catalog := range map[string]*WorkloadTemplateCatalog{
		"inherited": nil,
		"empty":     {},
		"published": {Templates: []WorkloadTemplate{{
			ID: "team-trainer", Version: 7, Title: "Team trainer", Kind: "Run-once GPU job", Mark: "TT",
			Defaults: WorkloadTemplateDefaults{
				Name: "team-trainer", Image: "ghcr.io/acme/trainer:1",
				Size: "large", Mode: WorkloadTemplateModeGPU, GPUKind: "h100",
			},
		}}},
	} {
		var out Customer
		mustRoundTrip(t, &Customer{ID: "c", Token: "t", TemplateCatalog: catalog}, &out)
		if (out.TemplateCatalog == nil) != (catalog == nil) {
			t.Fatalf("%s: inherit/publish distinction lost: %+v", name, out.TemplateCatalog)
		}
		if catalog != nil && !reflect.DeepEqual(out.TemplateCatalog.Templates, catalog.Templates) {
			t.Fatalf("%s: templates lost in round-trip: %+v", name, out.TemplateCatalog)
		}
		if out.TemplateCatalog.Revision() != catalog.Revision() {
			t.Fatalf("%s: revision moved across a round trip", name)
		}
	}

	var legacyDefaults = WorkloadTemplateCatalog{Templates: []WorkloadTemplate{{
		ID: "legacy-bare-recipe", Version: 2, Title: "Legacy bare recipe", Kind: "Run-once job",
		Defaults: WorkloadTemplateDefaults{
			Name: "legacy-bare-recipe", Image: "busybox:1", Size: "small", Mode: WorkloadTemplateModeCPU,
		},
	}}}
	var legacyOut Customer
	mustRoundTrip(t, &Customer{ID: "c", Token: "t", TemplateCatalog: &legacyDefaults}, &legacyOut)
	if raw, err := json.Marshal(legacyOut.TemplateCatalog.Templates[0].Defaults); err != nil {
		t.Fatalf("marshal legacy defaults: %v", err)
	} else if got := string(raw); strings.Contains(got, "\"command\"") || strings.Contains(got, "\"args\"") {
		t.Fatalf("legacy default recipe was serialized with command/args: %q", got)
	}

	// The provenance stamp rides the workload document the same way.
	var wl Workload
	mustRoundTrip(t, &Workload{
		ID: "wl_1", CustomerID: "c", Status: "provisioning",
		TemplateRef: &TemplateRef{ID: "container-job", Version: 2, CatalogRevision: "rev_0123456789abcdef"},
	}, &wl)
	if wl.TemplateRef == nil || *wl.TemplateRef != (TemplateRef{ID: "container-job", Version: 2, CatalogRevision: "rev_0123456789abcdef"}) {
		t.Fatalf("workload template ref lost in round-trip: %+v", wl.TemplateRef)
	}
}
