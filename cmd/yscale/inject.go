package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/yscale-sh/yscale/cmd/yscale/internal/inject"
)

// runInject implements `yscale inject -f FILE`. It reads a multi-document
// Kubernetes YAML stream from FILE (or stdin when FILE is "-") and writes a
// transformed stream to stdout. For every supported Pod template that
// already carries the yscale.sh/burst-template annotation, the transform
// adds the yscale.sh/burst-node="true" nodeSelector and a matching
// toleration if missing. It never contacts the cluster; the operator pipes
// the output into `kubectl apply -f -` (or a GitOps commit) themselves.
func runInject(args []string) error {
	fs := flag.NewFlagSet("inject", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var filename string
	fs.StringVar(&filename, "f", "", "path to a Kubernetes YAML file, or '-' for stdin (required)")
	fs.StringVar(&filename, "filename", "", "alias for -f")

	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `yscale inject -f FILE          read a Kubernetes YAML file
yscale inject --filename FILE  (alias for -f)
yscale inject -f -             read from stdin

For every supported Pod template that already carries the
`+`yscale.sh/burst-template`+` annotation, injects the required burst
scheduling constraints and writes the transformed multi-document stream to
stdout. The tool never contacts the cluster.

Injected fields (only when absent):

  nodeSelector:
    yscale.sh/burst-node: "true"
  tolerations:
    - {key: yscale.sh/burst-node, operator: Exists, effect: NoSchedule}

Supported kinds:
  v1                 Pod
  apps/v1            Deployment, StatefulSet, DaemonSet, ReplicaSet
  batch/v1           Job, CronJob
  keda.sh/*          ScaledJob        (spec.jobTargetRef.template)
  argoproj.io/*      Workflow         (each spec.templates[])

Unknown kinds and documents without the annotation pass through unchanged.
Existing compatible nodeSelector and tolerations are preserved. A
conflicting yscale.sh/burst-node selector value is refused (fail closed).
An invalid burst-template annotation fails the whole transform.

Typical use:

  yscale inject -f controller.yaml | kubectl apply -f -

Flags:
`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if filename == "" {
		fs.Usage()
		return fmt.Errorf("-f is required")
	}

	var in io.Reader
	if filename == "-" {
		in = os.Stdin
	} else {
		f, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("open %s: %w", filename, err)
		}
		defer f.Close()
		in = f
	}
	return inject.Transform(in, os.Stdout)
}
