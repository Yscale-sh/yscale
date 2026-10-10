package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/yscale-sh/yscale/pkg/workload"
)

// runApply implements `yscale apply -f workload.yaml`. It reads a
// Workload YAML, translates it to a Kubernetes Job, and applies it
// against the user's cluster. The yscale controller running in that
// cluster sees the resulting Pending pod and provisions a burst node
// on the configured backend (Fly.io / Linode / AWS) so the workload runs.
//
// Usage:
//
//	yscale apply -f workload.yaml
//	yscale apply -f workload.yaml --kubeconfig ~/.kube/staging
//	yscale apply -f - < workload.yaml          # stdin
func runApply(args []string) error {
	fs := flag.NewFlagSet("apply", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var (
		filename   string
		kubeconfig string
		ctxName    string
		dryRun     bool
		namespace  string
	)
	fs.StringVar(&filename, "f", "", "path to workload YAML, or '-' for stdin (required)")
	fs.StringVar(&filename, "filename", "", "alias for -f")
	fs.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path (default $KUBECONFIG, then ~/.kube/config)")
	fs.StringVar(&ctxName, "context", "", "kubeconfig context (default current-context)")
	fs.StringVar(&namespace, "namespace", "", "override the workload's namespace")
	fs.BoolVar(&dryRun, "dry-run", false, "print the translated Job and exit without applying")

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

	w, err := readWorkload(filename)
	if err != nil {
		return err
	}
	if namespace != "" {
		w.Metadata.Namespace = namespace
	}
	if w.Spec.NodeOnly {
		// `yscale apply` translates a Workload into a Job and creates it
		// directly. A nodeOnly workload wants bare burst capacity and NO
		// yscale Job — only central can honor that, via the Workload CR path.
		// Fail loud with guidance rather than creating a Job that contradicts
		// the request.
		return fmt.Errorf("nodeOnly workloads request bare burst capacity (no yscale Job); apply them as a Workload resource with `kubectl apply -f`, not `yscale apply` (which creates a Job directly and cannot express nodeOnly)")
	}
	if w.Spec.Storage != nil && len(w.Spec.Storage.Cache) > 0 {
		// Cache init containers read per-burst signing metadata (burst id +
		// bootstrap endpoint) that central injects into the Job during Plan.
		// `yscale apply` creates the Job directly and never runs Plan, so those
		// annotations would be empty and the signer request would fail. Direct
		// the user to the CR path instead of shipping a broken Job.
		return fmt.Errorf("spec.storage.cache workloads must be applied with `kubectl apply -f` — central injects the per-burst cache signing metadata that `yscale apply` (which creates a Job directly) cannot provide")
	}

	job, err := workload.ToJob(w)
	if err != nil {
		return fmt.Errorf("translating workload: %w", err)
	}

	if dryRun {
		out, err := yaml.Marshal(job)
		if err != nil {
			return fmt.Errorf("encoding dry-run output: %w", err)
		}
		_, _ = os.Stdout.Write(out)
		return nil
	}

	clientset, err := buildClientset(kubeconfig, ctxName)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	created, err := clientset.BatchV1().Jobs(job.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("job %s/%s already exists; delete it first or pick a different metadata.name", job.Namespace, job.Name)
		}
		return fmt.Errorf("creating job %s/%s: %w", job.Namespace, job.Name, err)
	}

	fmt.Fprintf(os.Stdout, "applied workload %s/%s\n", created.Namespace, created.Name)
	fmt.Fprintf(os.Stdout, "watch: kubectl -n %s get jobs %s -w\n", created.Namespace, created.Name)
	fmt.Fprintf(os.Stdout, "logs:  kubectl -n %s logs -l yscale.sh/workload=%s -f\n", created.Namespace, w.Metadata.Name)
	return nil
}

func readWorkload(path string) (*workload.Workload, error) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, fmt.Errorf("reading workload %s: %w", path, err)
	}

	var w workload.Workload
	if err := yaml.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("parsing workload YAML: %w", err)
	}
	return &w, nil
}

func buildClientset(kubeconfig, ctxName string) (kubernetes.Interface, error) {
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("locating home dir: %w", err)
		}
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}
	overrides := &clientcmd.ConfigOverrides{}
	if ctxName != "" {
		overrides.CurrentContext = ctxName
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("building rest config from %s: %w", kubeconfig, err)
	}
	return kubernetes.NewForConfig(cfg)
}
