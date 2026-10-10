package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/workload"
)

type templateRefContextKey struct{}

func withTemplateRef(ctx context.Context, ref *state.TemplateRef) context.Context {
	if ref == nil {
		return ctx
	}
	copyRef := *ref
	return context.WithValue(ctx, templateRefContextKey{}, &copyRef)
}

func storedTemplateRef(ctx context.Context) *state.TemplateRef {
	ref, _ := ctx.Value(templateRefContextKey{}).(*state.TemplateRef)
	if ref == nil {
		return nil
	}
	copyRef := *ref
	return &copyRef
}

// The launch-template reference a submission may carry.
//
// Both headers or neither, and that is the contract rather than a convenience:
// an id with no version names no particular template and a version with no id
// names nothing at all, so half a reference is a client bug rather than
// something central picks an answer for. A submission with neither is every
// raw-API and connector submission there has ever been, and stays exactly that.
const (
	templateIDHeader      = "X-Template-ID"
	templateVersionHeader = "X-Template-Version"

	// maxTemplateHeaderBytes bounds what is parsed at all. A template id is a
	// short label and a version is a small integer; anything longer is not a
	// reference central could resolve, and refusing it early keeps it out of the
	// journal and out of the refusal body.
	maxTemplateHeaderBytes = 128
)

// workloadTemplateRule names the policy verifyWorkloadTemplate applies, and the
// version stamps which shape of it decided a given submission — the pair
// workloadNamespaceRule is, for the same reason. Bump the version when the
// rule's inputs or effect change, not when a tenant edits their catalog: the
// catalog is data the rule reads, and the journal records that separately.
const (
	workloadTemplateRule        = "tenant.workload_templates"
	workloadTemplateRuleVersion = "v1"
)

// templateReference reads the reference off a submission. present=false means
// no template was named, which is not a refusal: it is the unchanged path every
// cluster-token and raw-API submission takes.
func templateReference(r *http.Request) (id string, version int, present bool, err error) {
	ids, versions := r.Header.Values(templateIDHeader), r.Header.Values(templateVersionHeader)
	if len(ids) == 0 && len(versions) == 0 {
		return "", 0, false, nil
	}
	if len(ids) != 1 || len(versions) != 1 {
		if len(ids) == 1 {
			id = ids[0]
		}
		return id, 0, false, fmt.Errorf("%s and %s must be sent together, exactly once each", templateIDHeader, templateVersionHeader)
	}
	if len(ids[0]) > maxTemplateHeaderBytes || len(versions[0]) > maxTemplateHeaderBytes {
		return ids[0], 0, false, fmt.Errorf("%s and %s must be short header values", templateIDHeader, templateVersionHeader)
	}
	parsed, convErr := strconv.Atoi(versions[0])
	if convErr != nil || parsed < 1 {
		return ids[0], 0, false, fmt.Errorf("%s must be a positive integer", templateVersionHeader)
	}
	return ids[0], parsed, true, nil
}

// templateRefusal is one refused reference: the closed reason the journal
// records, the status the caller gets, and the message that tells them what to
// do about it. The message is built from ids, versions and fixed text — never
// from the catalog's own prose or a template's image, which are tenant-authored
// strings and have no business in an error body.
type templateRefusal struct {
	reason  string
	status  int
	message string
}

// verifyWorkloadTemplate resolves a reference against the tenant's own catalog
// and returns the provenance stamp when the submission really is what the
// referenced entry publishes.
//
// What it checks is the EXECUTION CLASS — bare capacity or a container, with a
// GPU or without — and not the rest of the document. Command and args are
// editable defaults just like image and size; the reference records which
// catalog entry opened the form, not that every default remained unchanged.
// Those execution classes are what a member
// picking a template off the gallery is relying on: what gets provisioned and
// what runs on it. Everything else is the submitter's own choice and is
// validated on its own terms, so pinning it here would only make a template a
// second, weaker copy of spec validation.
//
// A stale version is refused rather than resolved to whatever the id points at
// today. That is the whole point of versioning the catalog: an owner who
// retires a template must not find yesterday's console still launching it.
func verifyWorkloadTemplate(cust *state.Customer, id string, version int, wl *workload.Workload) (*state.TemplateRef, *templateRefusal) {
	catalog := cust.TemplateCatalog.Effective()
	entry := catalog.Template(id)
	if entry == nil {
		return nil, &templateRefusal{
			reason: state.ReasonTemplateNotOffered,
			status: http.StatusConflict,
			message: fmt.Sprintf("template %q is not offered by this tenant's catalog; reload the catalog and launch again",
				state.SafeTemplateID(id)),
		}
	}
	if entry.Version != version {
		return nil, &templateRefusal{
			reason: state.ReasonTemplateVersionStale,
			status: http.StatusConflict,
			message: fmt.Sprintf("template %q is published at version %d, not %d; reload the catalog and launch again",
				entry.ID, entry.Version, version),
		}
	}
	if entry.NodeOnly != wl.Spec.NodeOnly {
		return nil, templateShapeMismatch(entry.ID,
			fmt.Sprintf("publishes %s; the submitted workload requests %s",
				templateCapacityClass(entry.NodeOnly), templateCapacityClass(wl.Spec.NodeOnly)))
	}
	if entry.NodeOnly && wl.Spec.Image != "" {
		return nil, templateShapeMismatch(entry.ID, "publishes node-only capacity; the submitted workload carries a container image")
	}
	if entry.NodeOnly && (len(wl.Spec.Command) > 0 || len(wl.Spec.Args) > 0) {
		return nil, templateShapeMismatch(entry.ID, "publishes node-only capacity; the submitted workload carries command arguments")
	}
	if wantsGPU := entry.Defaults.Mode == state.WorkloadTemplateModeGPU; wantsGPU != (wl.Spec.GPU != nil) {
		return nil, templateShapeMismatch(entry.ID,
			fmt.Sprintf("publishes a %s launch; the submitted workload is a %s launch",
				templateModeClass(wantsGPU), templateModeClass(wl.Spec.GPU != nil)))
	}
	return &state.TemplateRef{
		ID:      entry.ID,
		Version: entry.Version,
		// The catalog this decision was made against, not merely the entry: a
		// tenant may republish an id at the same version, and provenance that
		// could not tell those apart would credit a run to a catalog it never
		// came from.
		CatalogRevision: cust.TemplateCatalog.Revision(),
	}, nil
}

func templateShapeMismatch(id, detail string) *templateRefusal {
	return &templateRefusal{
		reason:  state.ReasonTemplateShapeMismatch,
		status:  http.StatusBadRequest,
		message: fmt.Sprintf("template %q %s", id, detail),
	}
}

func templateCapacityClass(nodeOnly bool) string {
	if nodeOnly {
		return "node-only capacity"
	}
	return "a container job"
}

func templateModeClass(gpu bool) string {
	if gpu {
		return "GPU"
	}
	return "CPU"
}
