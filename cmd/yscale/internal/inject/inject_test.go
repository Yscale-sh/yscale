package inject

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const validTemplate = `size: medium
gpu: {kind: l4, count: 1}
budget: {maxUSD: 5, deadline: 30m}
`

func transform(t *testing.T, input string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	err := Transform(strings.NewReader(input), &output)
	return output.String(), err
}

func documents(t *testing.T, stream string) []*yaml.Node {
	t.Helper()
	decoder := yaml.NewDecoder(strings.NewReader(stream))
	var result []*yaml.Node
	for {
		var document yaml.Node
		if err := decoder.Decode(&document); err != nil {
			if errors.Is(err, io.EOF) {
				return result
			}
			t.Fatalf("decode output: %v", err)
		}
		if root := mapping(&document); root != nil {
			result = append(result, root)
		}
	}
}

func assertInjected(t *testing.T, root *yaml.Node, specPath ...string) {
	t.Helper()
	spec := mapping(mapGetPath(root, specPath...))
	if got := scalarStr(mapGet(mapping(mapGet(spec, "nodeSelector")), "yscale.sh/burst-node")); got != "true" {
		t.Fatalf("burst selector = %q, want true", got)
	}
	tolerations := mapGet(spec, "tolerations")
	if tolerations == nil || tolerations.Kind != yaml.SequenceNode {
		t.Fatal("burst toleration was not injected")
	}
	for _, toleration := range tolerations.Content {
		if tolerationSatisfies(toleration) {
			return
		}
	}
	t.Fatal("no compatible burst toleration found")
}

func deployment(annotation, scheduling string) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata: {name: inference}
spec:
  selector: {matchLabels: {app: inference}}
  template:
    metadata:
      labels: {app: inference}
      annotations:
%s
    spec:
%s
      containers:
        - name: main
          image: example.invalid/inference
          resources:
            limits: {nvidia.com/gpu: 1}
`, annotation, scheduling)
}

func validAnnotation() string {
	return "        yscale.sh/burst-template: |\n" + indent(validTemplate, "          ")
}

func indent(value, prefix string) string {
	return prefix + strings.ReplaceAll(strings.TrimSuffix(value, "\n"), "\n", "\n"+prefix)
}

func TestDeploymentInjectionPreservesWorkloadFields(t *testing.T) {
	output, err := transform(t, deployment(validAnnotation(), ""))
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	docs := documents(t, output)
	assertInjected(t, docs[0], "spec", "template", "spec")
	for _, want := range []string{"example.invalid/inference", "nvidia.com/gpu"} {
		if !strings.Contains(output, want) {
			t.Fatalf("output dropped %q", want)
		}
	}
}

func TestArgoInjectsOnlyAnnotatedTemplate(t *testing.T) {
	input := `apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: {name: pipeline}
spec:
  templates:
    - name: cpu
      container: {image: busybox}
    - name: gpu
      metadata:
        annotations:
          yscale.sh/burst-template: |
            size: medium
      container: {image: example.invalid/gpu}
`
	output, err := transform(t, input)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	templates := mapGetPath(documents(t, output)[0], "spec", "templates")
	if mapGet(templates.Content[0], "nodeSelector") != nil {
		t.Fatal("unannotated Argo template was changed")
	}
	assertInjected(t, templates.Content[1])
}

func TestKEDAScaledJobInjection(t *testing.T) {
	input := `apiVersion: keda.sh/v1alpha1
kind: ScaledJob
metadata: {name: workers}
spec:
  jobTargetRef:
    template:
      metadata:
        annotations:
          yscale.sh/burst-template: |
            size: medium
      spec:
        containers: [{name: worker, image: example.invalid/worker}]
  triggers: [{type: cron, metadata: {desiredReplicas: "1"}}]
`
	output, err := transform(t, input)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	assertInjected(t, documents(t, output)[0], "spec", "jobTargetRef", "template", "spec")
	if !strings.Contains(output, "desiredReplicas") {
		t.Fatal("KEDA trigger was dropped")
	}
}

func TestBuiltInTemplatePaths(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
		path     []string
	}{
		{
			name: "pod",
			manifest: `apiVersion: v1
kind: Pod
metadata:
  name: gpu
  annotations:
    yscale.sh/burst-template: |
      size: medium
spec: {containers: [{name: gpu, image: example.invalid/gpu}]}
`,
			path: []string{"spec"},
		},
		{
			name: "cronjob",
			manifest: `apiVersion: batch/v1
kind: CronJob
metadata: {name: gpu}
spec:
  schedule: "0 * * * *"
  jobTemplate:
    spec:
      template:
        metadata:
          annotations:
            yscale.sh/burst-template: |
              size: medium
        spec: {containers: [{name: gpu, image: example.invalid/gpu}], restartPolicy: Never}
`,
			path: []string{"spec", "jobTemplate", "spec", "template", "spec"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			output, err := transform(t, tc.manifest)
			if err != nil {
				t.Fatalf("Transform: %v", err)
			}
			assertInjected(t, documents(t, output)[0], tc.path...)
		})
	}
}

func TestMultiDocumentUnknownAndUnannotatedPassThrough(t *testing.T) {
	input := `apiVersion: v1
kind: ConfigMap
metadata: {name: settings}
data: {color: blue}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: plain}
spec:
  template:
    metadata: {labels: {app: plain}}
    spec: {containers: [{name: plain, image: nginx}]}
---
apiVersion: example.invalid/v1
kind: Custom
metadata: {name: custom}
spec: {answer: 42}
`
	output, err := transform(t, input)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	docs := documents(t, output)
	if len(docs) != 3 || scalarStr(mapGetPath(docs[0], "data", "color")) != "blue" || scalarStr(mapGetPath(docs[2], "spec", "answer")) != "42" {
		t.Fatalf("passthrough documents changed: %s", output)
	}
	if mapGetPath(docs[1], "spec", "template", "spec", "nodeSelector") != nil {
		t.Fatal("unannotated template was changed")
	}
}

func TestInvalidInputsFailWithoutPartialOutput(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		scheduling string
	}{
		{name: "empty annotation", annotation: `        yscale.sh/burst-template: ""`},
		{name: "non-string annotation", annotation: `        yscale.sh/burst-template: {size: medium}`},
		{name: "invalid template", annotation: "        yscale.sh/burst-template: |\n          gpu: [broken"},
		{name: "selector conflict", annotation: validAnnotation(), scheduling: "      nodeSelector:\n        yscale.sh/burst-node: \"false\""},
		{name: "boolean selector", annotation: validAnnotation(), scheduling: "      nodeSelector:\n        yscale.sh/burst-node: true"},
		{name: "selector wrong shape", annotation: validAnnotation(), scheduling: "      nodeSelector: []"},
		{name: "tolerations wrong shape", annotation: validAnnotation(), scheduling: "      tolerations: {}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n---\n" + deployment(tc.annotation, tc.scheduling)
			var output bytes.Buffer
			if err := Transform(strings.NewReader(input), &output); err == nil {
				t.Fatal("expected fail-closed error")
			}
			if output.Len() != 0 {
				t.Fatalf("failed transform wrote partial output: %q", output.String())
			}
		})
	}
}

func TestCompatibleSchedulingContractIsIdempotent(t *testing.T) {
	tests := []struct {
		name       string
		toleration string
	}{
		{name: "exists", toleration: "      - {key: yscale.sh/burst-node, operator: Exists, effect: NoSchedule}"},
		{name: "equal true", toleration: "      - {key: yscale.sh/burst-node, operator: Equal, value: \"true\", effect: NoSchedule}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheduling := "      nodeSelector:\n        yscale.sh/burst-node: \"true\"\n      tolerations:\n" + tc.toleration
			once, err := transform(t, deployment(validAnnotation(), scheduling))
			if err != nil {
				t.Fatalf("first transform: %v", err)
			}
			twice, err := transform(t, once)
			if err != nil {
				t.Fatalf("second transform: %v", err)
			}
			if once != twice {
				t.Fatal("transform is not idempotent")
			}
			spec := mapping(mapGetPath(documents(t, once)[0], "spec", "template", "spec"))
			if len(mapGet(spec, "tolerations").Content) != 1 {
				t.Fatal("compatible toleration was duplicated")
			}
		})
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestStreamErrors(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name string
		r    io.Reader
		w    io.Writer
	}{
		{name: "nil reader", r: nil, w: io.Discard},
		{name: "nil writer", r: strings.NewReader(""), w: nil},
		{name: "read", r: failingReader{boom}, w: io.Discard},
		{name: "write", r: strings.NewReader("apiVersion: v1\nkind: ConfigMap\n"), w: failingWriter{boom}},
		{name: "yaml", r: strings.NewReader("apiVersion: ["), w: io.Discard},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := Transform(tc.r, tc.w); err == nil {
				t.Fatal("expected stream error")
			}
		})
	}
}
