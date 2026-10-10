// Package inject implements the "yscale inject" transform: it reads a
// multi-document Kubernetes YAML stream and, for every supported Pod
// template that already carries the yscale.sh/burst-template annotation,
// adds the burst-node nodeSelector and toleration required for the pod to
// schedule on a yscale-provisioned burst node.
//
// The transform is a pure text pipeline. It never contacts the cluster and
// never mutates cluster state — the caller pipes its stdout into
// `kubectl apply -f -` (or whatever GitOps machinery is in play).
//
// Design points:
//
//   - The burst-template annotation is authored by the workload owner. This
//     package never invents it; documents without it pass through unchanged.
//   - Unknown kinds pass through unchanged: the tool is a supported-tooling
//     surface, not a universal munger.
//   - Injection is fail-closed on a conflicting yscale.sh/burst-node
//     nodeSelector value. A stray "false" or a typo does not silently become
//     "true".
//   - Existing compatible tolerations are respected. An Exists or Equal true
//     toleration with a compatible effect (empty or NoSchedule) is treated
//     as sufficient.
//   - The burst-template YAML is validated as a nodeOnly workload spec via
//     the existing pkg/workload validator — we do not maintain a second
//     schema. A malformed template fails the whole transform (fail closed).
//   - The transform is deterministic and idempotent: running it twice on
//     the same input produces the same output as running it once.
package inject

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/yscale-sh/yscale/pkg/workload"
)

// Transform reads a multi-document YAML stream from r and writes the
// transformed stream to w. See the package doc for the injection rules.
//
// Returns an error only on a fatal condition (unreadable input, malformed
// YAML, invalid burst-template annotation, or a conflicting burst-node
// nodeSelector). On success the transformed stream is complete on w.
func Transform(r io.Reader, w io.Writer) error {
	if r == nil {
		return fmt.Errorf("nil input reader")
	}
	if w == nil {
		return fmt.Errorf("nil output writer")
	}
	dec := yaml.NewDecoder(r)
	var output bytes.Buffer
	enc := yaml.NewEncoder(&output)
	enc.SetIndent(2)

	docIndex := 0
	for {
		var doc yaml.Node
		err := dec.Decode(&doc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("parse yaml document %d: %w", docIndex, err)
		}
		docIndex++
		if isEmptyDoc(&doc) {
			// Empty separators contain no Kubernetes object to preserve.
			continue
		}
		if err := transformDoc(&doc); err != nil {
			return fmt.Errorf("document %d: %w", docIndex, err)
		}
		if err := enc.Encode(&doc); err != nil {
			return fmt.Errorf("encode document %d: %w", docIndex, err)
		}
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("finish yaml stream: %w", err)
	}
	if _, err := io.Copy(w, &output); err != nil {
		return fmt.Errorf("write yaml stream: %w", err)
	}
	return nil
}

// isEmptyDoc reports whether the decoded document carries no content.
func isEmptyDoc(n *yaml.Node) bool {
	if n == nil {
		return true
	}
	if n.Kind == 0 {
		return true
	}
	if n.Kind == yaml.DocumentNode && len(n.Content) == 0 {
		return true
	}
	return false
}

func transformDoc(doc *yaml.Node) error {
	sites, known := siteFor(doc)
	if !known {
		return nil
	}
	for _, s := range sites {
		if err := s.apply(); err != nil {
			return err
		}
	}
	return nil
}

// site names one Pod-template location inside a document. It carries the
// mapping nodes we need to read (metadata for the burst-template annotation)
// and the mapping node we inject into (nodeSelector + tolerations).
type site struct {
	// name identifies this site in error messages (e.g. "spec.template" or
	// "spec.templates[2]"). Not user-visible unless we return an error.
	name string

	// container is the mapping holding the Pod template's metadata + spec.
	// For Argo templates, it is the template mapping itself.
	container *yaml.Node

	// inject is the mapping where nodeSelector and tolerations live. For
	// most kinds this is container.spec; for Argo templates it is the
	// template mapping itself (nodeSelector/tolerations are top-level on
	// an Argo Template).
	inject *yaml.Node
}

func (s site) apply() error {
	metaMap := mapping(mapGet(s.container, "metadata"))
	if metaMap == nil {
		return nil
	}
	annsMap := mapping(mapGet(metaMap, "annotations"))
	if annsMap == nil {
		return nil
	}
	tplNode := mapGet(annsMap, workload.AnnotationBurstTemplate)
	if tplNode == nil {
		return nil
	}
	if tplNode.Kind != yaml.ScalarNode || tplNode.Tag != "!!str" {
		return fmt.Errorf("%s: %s annotation must be a YAML string",
			s.name, workload.AnnotationBurstTemplate)
	}
	tplYAML := tplNode.Value
	if err := validateBurstTemplate(tplYAML); err != nil {
		return fmt.Errorf("%s: invalid %s annotation: %w",
			s.name, workload.AnnotationBurstTemplate, err)
	}
	if err := ensureBurstSelector(s.inject); err != nil {
		return fmt.Errorf("%s: %w", s.name, err)
	}
	if err := ensureBurstToleration(s.inject); err != nil {
		return fmt.Errorf("%s: %w", s.name, err)
	}
	return nil
}

// siteFor returns the Pod-template sites this document exposes, and a
// boolean marking the document's kind as "supported". Unknown or missing
// kinds are reported as (nil, false) so the caller passes them through
// unchanged.
func siteFor(doc *yaml.Node) ([]site, bool) {
	root := mapping(doc)
	if root == nil {
		return nil, false
	}
	apiVersion := scalarStr(mapGet(root, "apiVersion"))
	kind := scalarStr(mapGet(root, "kind"))
	if apiVersion == "" || kind == "" {
		return nil, false
	}

	switch {
	case apiVersion == "v1" && kind == "Pod":
		spec := mapping(mapGet(root, "spec"))
		if spec == nil {
			return nil, true
		}
		return []site{{name: kind, container: root, inject: spec}}, true

	case apiVersion == "apps/v1" &&
		(kind == "Deployment" || kind == "StatefulSet" ||
			kind == "DaemonSet" || kind == "ReplicaSet"):
		return templateAt(root, "spec.template", "spec", "template"), true

	case apiVersion == "batch/v1" && kind == "Job":
		return templateAt(root, "spec.template", "spec", "template"), true

	case apiVersion == "batch/v1" && kind == "CronJob":
		return templateAt(root, "spec.jobTemplate.spec.template",
			"spec", "jobTemplate", "spec", "template"), true

	case groupIs(apiVersion, "keda.sh") && kind == "ScaledJob":
		return templateAt(root, "spec.jobTargetRef.template",
			"spec", "jobTargetRef", "template"), true

	case groupIs(apiVersion, "argoproj.io") && kind == "Workflow":
		return argoTemplates(root), true

	default:
		return nil, false
	}
}

// templateAt returns a single site rooted at the mapping reached by the
// given path. If the path or the terminal .spec are missing, nothing is
// returned — a template without a spec has no pod-level fields to touch.
func templateAt(root *yaml.Node, name string, path ...string) []site {
	tpl := mapping(mapGetPath(root, path...))
	if tpl == nil {
		return nil
	}
	spec := mapping(mapGet(tpl, "spec"))
	if spec == nil {
		return nil
	}
	return []site{{name: name, container: tpl, inject: spec}}
}

// argoTemplates enumerates spec.templates[] on an Argo Workflow. For each
// element the annotation (if any) sits at .metadata.annotations, and the
// nodeSelector/tolerations live at the template's top level.
func argoTemplates(root *yaml.Node) []site {
	templates := mapGetPath(root, "spec", "templates")
	if templates == nil || templates.Kind != yaml.SequenceNode {
		return nil
	}
	sites := make([]site, 0, len(templates.Content))
	for i, tpl := range templates.Content {
		if tpl.Kind != yaml.MappingNode {
			continue
		}
		sites = append(sites, site{
			name:      fmt.Sprintf("spec.templates[%d]", i),
			container: tpl,
			inject:    tpl,
		})
	}
	return sites
}

// validateBurstTemplate parses the annotation as a workload.Spec and
// validates it against the existing nodeOnly workload contract, so the
// injector never emits a manifest whose declared burst shape would be
// rejected downstream. A synthetic Name and NodeOnly=true match how the
// agent wraps the same template before submission — we reuse that shape
// rather than build a second schema.
func validateBurstTemplate(s string) error {
	var spec workload.Spec
	if err := yaml.Unmarshal([]byte(s), &spec); err != nil {
		return fmt.Errorf("parse yaml: %w", err)
	}
	wl := workload.Workload{
		APIVersion: workload.APIVersion,
		Kind:       workload.Kind,
		Metadata:   workload.Metadata{Name: "yscale-inject-validate"},
		Spec:       spec,
	}
	wl.Spec.NodeOnly = true
	return workload.Validate(&wl)
}

// ensureBurstSelector adds yscale.sh/burst-node="true" to the mapping's
// nodeSelector. If nodeSelector already carries a different value for the
// key, the transform fails closed rather than overwriting the operator's
// intent.
func ensureBurstSelector(specMap *yaml.Node) error {
	ns, err := mapEnsureMapping(specMap, "nodeSelector")
	if err != nil {
		return err
	}
	existing := mapGet(ns, workload.LabelBurstNode)
	if existing == nil {
		setScalarString(ns, workload.LabelBurstNode, "true")
		return nil
	}
	if existing.Kind == yaml.ScalarNode && existing.Tag == "!!str" && existing.Value == "true" {
		return nil
	}
	got := ""
	if existing.Kind == yaml.ScalarNode {
		got = existing.Value
	}
	return fmt.Errorf(
		"nodeSelector %s already set to %q; refusing to overwrite (expected %q or absent)",
		workload.LabelBurstNode, got, "true")
}

// ensureBurstToleration appends the burst-node toleration unless an
// existing entry already covers it (Exists or Equal-true against
// NoSchedule / any effect).
func ensureBurstToleration(specMap *yaml.Node) error {
	tolerations, err := mapEnsureSequence(specMap, "tolerations")
	if err != nil {
		return err
	}
	for _, t := range tolerations.Content {
		if tolerationSatisfies(t) {
			return nil
		}
	}
	tolerations.Content = append(tolerations.Content, newBurstTolerationNode())
	return nil
}

// tolerationSatisfies reports whether the given toleration mapping already
// permits scheduling onto a NoSchedule taint on yscale.sh/burst-node.
func tolerationSatisfies(t *yaml.Node) bool {
	if t == nil || t.Kind != yaml.MappingNode {
		return false
	}
	if scalarStr(mapGet(t, "key")) != workload.LabelBurstNode {
		return false
	}
	effect := scalarStr(mapGet(t, "effect"))
	if effect != "" && effect != "NoSchedule" {
		return false
	}
	operator := scalarStr(mapGet(t, "operator"))
	switch operator {
	case "Exists":
		return true
	case "", "Equal":
		return scalarStr(mapGet(t, "value")) == "true"
	default:
		return false
	}
}

// newBurstTolerationNode returns a yaml.Node for the canonical toleration
// {key: yscale.sh/burst-node, operator: Exists, effect: NoSchedule}.
func newBurstTolerationNode() *yaml.Node {
	n := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	setScalarString(n, "key", workload.LabelBurstNode)
	setScalarString(n, "operator", "Exists")
	setScalarString(n, "effect", "NoSchedule")
	return n
}

// --- small yaml.Node helpers -------------------------------------------------
//
// These deliberately walk yaml.Node trees rather than round-tripping through
// typed Go structs. Round-tripping would drop unknown fields, reorder keys,
// and reformat scalars — behaviour that fights the "preserve unrelated
// fields/documents" contract.

// mapping returns the underlying MappingNode inside a DocumentNode, or the
// node itself if it is already a MappingNode. Any other kind returns nil.
func mapping(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	if n.Kind == yaml.DocumentNode {
		if len(n.Content) == 0 {
			return nil
		}
		return mapping(n.Content[0])
	}
	if n.Kind == yaml.MappingNode {
		return n
	}
	return nil
}

// mapGet returns the value node for key in mapping m, or nil.
func mapGet(m *yaml.Node, key string) *yaml.Node {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Kind == yaml.ScalarNode && m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// mapGetPath walks a dotted path of mapping keys. A missing or non-mapping
// intermediate node stops the walk and returns nil.
func mapGetPath(root *yaml.Node, path ...string) *yaml.Node {
	cur := mapping(root)
	for i, key := range path {
		if cur == nil {
			return nil
		}
		v := mapGet(cur, key)
		if v == nil {
			return nil
		}
		if i == len(path)-1 {
			return v
		}
		cur = mapping(v)
	}
	return cur
}

// mapEnsureMapping returns m[key] as a mapping, creating an empty one if
// absent.
func mapEnsureMapping(m *yaml.Node, key string) (*yaml.Node, error) {
	if v := mapGet(m, key); v != nil {
		if v.Kind == yaml.MappingNode {
			return v, nil
		}
		if v.Kind == yaml.ScalarNode && v.Tag == "!!null" {
			v.Kind = yaml.MappingNode
			v.Tag = "!!map"
			v.Value = ""
			v.Content = nil
			return v, nil
		}
		return nil, fmt.Errorf("%s must be a mapping", key)
	}
	keyNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
	valNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	m.Content = append(m.Content, keyNode, valNode)
	return valNode, nil
}

// mapEnsureSequence returns m[key] as a sequence, creating an empty one if
// absent.
func mapEnsureSequence(m *yaml.Node, key string) (*yaml.Node, error) {
	if v := mapGet(m, key); v != nil {
		if v.Kind == yaml.SequenceNode {
			return v, nil
		}
		if v.Kind == yaml.ScalarNode && v.Tag == "!!null" {
			v.Kind = yaml.SequenceNode
			v.Tag = "!!seq"
			v.Value = ""
			v.Content = nil
			return v, nil
		}
		return nil, fmt.Errorf("%s must be a sequence", key)
	}
	keyNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
	valNode := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	m.Content = append(m.Content, keyNode, valNode)
	return valNode, nil
}

// setScalarString sets m[key] = value, appending or replacing as needed.
// Values are quoted so labels like "true" round-trip as a string.
func setScalarString(m *yaml.Node, key, value string) {
	valNode := &yaml.Node{
		Kind:  yaml.ScalarNode,
		Tag:   "!!str",
		Value: value,
		Style: yaml.DoubleQuotedStyle,
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Kind == yaml.ScalarNode && m.Content[i].Value == key {
			m.Content[i+1] = valNode
			return
		}
	}
	m.Content = append(m.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
		valNode)
}

// scalarStr returns the string value of a scalar node, or "" for anything
// else. Booleans and numbers keep their source token in Value, so this is
// safe for the boolean-ish label strings Kubernetes uses.
func scalarStr(n *yaml.Node) string {
	if n == nil || n.Kind != yaml.ScalarNode {
		return ""
	}
	return n.Value
}

// groupIs reports whether apiVersion belongs to the given API group. The
// group is the part before "/" (or the whole string if no slash).
func groupIs(apiVersion, group string) bool {
	i := strings.Index(apiVersion, "/")
	return i > 0 && i < len(apiVersion)-1 && apiVersion[:i] == group
}
