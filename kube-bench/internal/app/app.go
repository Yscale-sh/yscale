package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/compare"
	"github.com/JakeNesler/yscale-kube-bench/internal/config"
	"github.com/JakeNesler/yscale-kube-bench/internal/kubectl"
	"github.com/JakeNesler/yscale-kube-bench/internal/report"
	"github.com/JakeNesler/yscale-kube-bench/internal/runner"
	"github.com/JakeNesler/yscale-kube-bench/internal/worker"
)

var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

type Streams struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

func Run(args []string, streams Streams) int {
	if streams.In == nil {
		streams.In = os.Stdin
	}
	if streams.Out == nil {
		streams.Out = os.Stdout
	}
	if streams.Err == nil {
		streams.Err = os.Stderr
	}
	if len(args) == 0 {
		printUsage(streams.Out)
		return 0
	}
	switch args[0] {
	case "run":
		return runBenchmark(args[1:], streams, false)
	case "inventory":
		return runBenchmark(args[1:], streams, true)
	case "compare":
		return runCompare(args[1:], streams)
	case "doctor":
		return runDoctor(args[1:], streams)
	case "config":
		return runConfig(args[1:], streams)
	case "worker":
		return worker.Run(args[1:], streams.Out, streams.Err)
	case "version", "--version", "-v":
		fmt.Fprintf(streams.Out, "yscale-kube-bench %s commit=%s built=%s\n", Version, Commit, Date)
		return 0
	case "help", "--help", "-h":
		printUsage(streams.Out)
		return 0
	default:
		fmt.Fprintf(streams.Err, "unknown command %q\n\n", args[0])
		printUsage(streams.Err)
		return 2
	}
}

func runBenchmark(args []string, streams Streams, inventoryOnly bool) int {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	configPath := flags.String("config", "", "JSON benchmark config")
	outputDirectory := flags.String("output-dir", "results", "report directory")
	kubeconfig := flags.String("kubeconfig", "", "kubeconfig path")
	currentContext := flags.String("context", "", "kubeconfig context")
	namespace := flags.String("namespace", "", "override benchmark namespace")
	image := flags.String("image", "", "override worker image")
	kubectlBinary := flags.String("kubectl", "kubectl", "kubectl executable")
	suites := flags.String("suite", "", "comma-separated suites or all")
	nodes := flags.String("node", "", "comma-separated node names")
	verbose := flags.Bool("verbose", false, "print kubectl commands")
	keep := optionalBool{}
	flags.Var(&keep, "keep", "keep benchmark resources after the run")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(streams.Err, err)
		return 2
	}
	if *namespace != "" {
		cfg.Spec.Namespace = *namespace
	}
	if *image != "" {
		cfg.Spec.Image = *image
	}
	if *nodes != "" {
		cfg.Spec.Nodes = splitCSV(*nodes)
	}
	if keep.set {
		cfg.Spec.KeepResources = keep.value
	}
	if inventoryOnly {
		if err := cfg.SetSuites([]string{"inventory"}); err != nil {
			fmt.Fprintln(streams.Err, err)
			return 2
		}
	} else if *suites != "" {
		if err := cfg.SetSuites(splitCSV(*suites)); err != nil {
			fmt.Fprintln(streams.Err, err)
			return 2
		}
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(streams.Err, err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	benchmark := runner.New(cfg, runner.Options{
		KubectlBinary: *kubectlBinary,
		Kubeconfig:    *kubeconfig,
		Context:       *currentContext,
		Verbose:       *verbose,
		LogWriter:     streams.Err,
		ToolVersion:   Version,
	})
	result, err := benchmark.Run(ctx)
	if err != nil {
		fmt.Fprintln(streams.Err, "benchmark failed:", err)
		return 1
	}
	paths, err := report.Write(*result, *outputDirectory)
	if err != nil {
		fmt.Fprintln(streams.Err, "write reports:", err)
		return 1
	}
	fmt.Fprint(streams.Out, report.ConsoleSummary(*result))
	fmt.Fprintf(streams.Out, "Reports:\n  %s\n  %s\n  %s\n", paths.JSON, paths.Markdown, paths.HTML)
	return 0
}

func runCompare(args []string, streams Streams) int {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	output := flags.String("output", "", "write Markdown comparison to a file")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 2 {
		fmt.Fprintln(streams.Err, "usage: yscale-kube-bench compare [--output file.md] BEFORE.json AFTER.json")
		return 2
	}
	before, err := compare.Load(flags.Arg(0))
	if err != nil {
		fmt.Fprintln(streams.Err, err)
		return 1
	}
	after, err := compare.Load(flags.Arg(1))
	if err != nil {
		fmt.Fprintln(streams.Err, err)
		return 1
	}
	markdown := compare.Markdown(before, after)
	if *output != "" {
		if err := os.MkdirAll(filepath.Dir(filepath.Clean(*output)), 0o755); err != nil && filepath.Dir(*output) != "." {
			fmt.Fprintln(streams.Err, err)
			return 1
		}
		if err := os.WriteFile(filepath.Clean(*output), []byte(markdown), 0o644); err != nil {
			fmt.Fprintln(streams.Err, err)
			return 1
		}
	}
	fmt.Fprint(streams.Out, markdown)
	return 0
}

func runDoctor(args []string, streams Streams) int {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	configPath := flags.String("config", "", "JSON benchmark config")
	kubeconfig := flags.String("kubeconfig", "", "kubeconfig path")
	currentContext := flags.String("context", "", "kubeconfig context")
	kubectlBinary := flags.String("kubectl", "kubectl", "kubectl executable")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(streams.Err, err)
		return 2
	}
	client := kubectl.New(*kubectlBinary, *kubeconfig, *currentContext, cfg.Spec.Namespace, false, streams.Err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Check(); err != nil {
		fmt.Fprintln(streams.Err, "FAIL kubectl/cluster:", err)
		return 1
	}
	version, _ := client.Version(ctx)
	nodes, err := client.ListNodes(ctx)
	if err != nil {
		fmt.Fprintln(streams.Err, "FAIL list nodes:", err)
		return 1
	}
	fmt.Fprintf(streams.Out, "OK Kubernetes %s via kubectl %s\n", version.ServerVersion.GitVersion, version.ClientVersion.GitVersion)
	fmt.Fprintf(streams.Out, "OK %d node objects visible\n", len(nodes))
	checks := []struct{ verb, resource string }{
		{"get", "nodes"}, {"get", "namespaces"},
		{"get", "pods"}, {"get", "pods/log"}, {"get", "events"}, {"get", "endpoints"},
		{"create", "pods"}, {"delete", "pods"}, {"create", "jobs.batch"}, {"delete", "jobs.batch"},
		{"create", "services"}, {"delete", "services"},
	}
	if _, namespaceErr := client.Run(ctx, nil, "get", "namespace", cfg.Spec.Namespace, "-o", "name"); namespaceErr == nil {
		fmt.Fprintf(streams.Out, "OK namespace %s exists\n", cfg.Spec.Namespace)
	} else {
		fmt.Fprintf(streams.Out, "INFO namespace %s is absent or not visible; create permission is required\n", cfg.Spec.Namespace)
		checks = append(checks, struct{ verb, resource string }{"create", "namespaces"})
	}
	failed := false
	for _, check := range checks {
		allowed, permissionErr := client.CanI(ctx, check.verb, check.resource)
		if permissionErr != nil {
			fmt.Fprintf(streams.Out, "FAIL can-i %s %s: %v\n", check.verb, check.resource, permissionErr)
			failed = true
			continue
		}
		status := "OK"
		if !allowed {
			status = "FAIL"
			failed = true
		}
		fmt.Fprintf(streams.Out, "%s can-i %s %s\n", status, check.verb, check.resource)
	}
	fmt.Fprintf(streams.Out, "Worker image: %s\n", cfg.Spec.Image)
	if failed {
		return 1
	}
	return 0
}

func runConfig(args []string, streams Streams) int {
	if len(args) == 0 || args[0] != "init" {
		fmt.Fprintln(streams.Err, "usage: yscale-kube-bench config init [--output config.json]")
		return 2
	}
	flags := flag.NewFlagSet("config init", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	output := flags.String("output", "", "write config to a file instead of stdout")
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}
	data, err := json.MarshalIndent(config.Default(), "", "  ")
	if err != nil {
		fmt.Fprintln(streams.Err, err)
		return 1
	}
	data = append(data, '\n')
	if *output == "" {
		_, _ = streams.Out.Write(data)
		return 0
	}
	directory := filepath.Dir(filepath.Clean(*output))
	if directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			fmt.Fprintln(streams.Err, err)
			return 1
		}
	}
	if err := os.WriteFile(filepath.Clean(*output), data, 0o644); err != nil {
		fmt.Fprintln(streams.Err, err)
		return 1
	}
	fmt.Fprintln(streams.Out, *output)
	return 0
}

type optionalBool struct {
	set   bool
	value bool
}

func (o *optionalBool) String() string {
	if !o.set {
		return ""
	}
	return fmt.Sprintf("%t", o.value)
}

func (o *optionalBool) Set(value string) error {
	parsed, err := parseBool(value)
	if err != nil {
		return err
	}
	o.set = true
	o.value = parsed
	return nil
}

func (o *optionalBool) IsBoolFlag() bool { return true }

func parseBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, errors.New("expected a boolean")
	}
}

func splitCSV(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}

func printUsage(writer io.Writer) {
	fmt.Fprint(writer, `yscale-kube-bench — reaction, efficiency, and throughput for heterogeneous Kubernetes clusters

Usage:
  yscale-kube-bench run [flags]
  yscale-kube-bench inventory [flags]
  yscale-kube-bench compare BEFORE.json AFTER.json
  yscale-kube-bench doctor [flags]
  yscale-kube-bench config init [--output config.json]
  yscale-kube-bench version

Run flags:
  --config FILE        JSON config; defaults are built in
  --output-dir DIR     JSON, Markdown, and HTML report directory
  --kubeconfig FILE    kubeconfig path
  --context NAME       kubeconfig context
  --namespace NAME     override benchmark namespace
  --image IMAGE        override in-cluster worker image
  --suite LIST         inventory,reaction,compute,network,dns,storage,transcode,all
  --node LIST          exact node names
  --keep               retain Jobs, Pods, and Services
  --verbose            print kubectl commands
`)
}
