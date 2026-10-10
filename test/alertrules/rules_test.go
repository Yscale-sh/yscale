// Package alertrules holds the guards for deploy/monitoring's PrometheusRule.
//
// The rules are YAML that nothing in the Go build compiles or type-checks, so a
// rule that quietly stops watching what it claims to watch is invisible until
// an incident. That is not hypothetical: BurstStuckProvisioning spent its whole
// life alerting on a p95 over COMPLETED burst starts, which by construction
// cannot see a burst that never starts — the exact failure it was named for.
//
// Two layers, both inside `go test ./...`:
//
//   - Semantic guards, which need nothing installed. They pin what each rule
//     reads and cross-check every yscale_ metric the rules reference against
//     the names central actually declares, so a rename or a typo fails here
//     instead of at 3am.
//   - promtool unit tests, which run the real evaluator over the real rules and
//     prove the firing behaviour. They skip when promtool is not on PATH, the
//     same way the helm chart guards in test/helmrender skip without helm.
package alertrules

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	monitoringDir = "../../deploy/monitoring"
	crdFile       = "yscale-reliability-alerts.yaml"
	unitTestFile  = "yscale-reliability-alerts.test.yaml"

	// meterDir holds the metric-name declarations the rules must agree with.
	meterDir = "../../central/internal/cost"
)

type alertRule struct {
	Alert       string            `yaml:"alert"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
}

// loadRules returns the alerting rules keyed by alert name.
func loadRules(t *testing.T) map[string]alertRule {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(monitoringDir, crdFile))
	if err != nil {
		t.Fatalf("read %s: %v", crdFile, err)
	}
	var doc struct {
		Spec struct {
			Groups []struct {
				Rules []alertRule `yaml:"rules"`
			} `yaml:"groups"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", crdFile, err)
	}
	rules := make(map[string]alertRule)
	for _, g := range doc.Spec.Groups {
		for _, r := range g.Rules {
			if r.Alert != "" {
				rules[r.Alert] = r
			}
		}
	}
	if len(rules) == 0 {
		t.Fatalf("%s declares no alerting rules", crdFile)
	}
	return rules
}

func mustRule(t *testing.T, rules map[string]alertRule, name string) alertRule {
	t.Helper()
	r, ok := rules[name]
	if !ok {
		t.Fatalf("alert %s is missing from %s", name, crdFile)
	}
	return r
}

// TestBurstStuckProvisioningReadsLiveEvidence is the regression guard for the
// defect this rule was rewritten to fix. A quantile over
// yscale_burst_provision_seconds only ever describes starts that FINISHED, so a
// burst that never starts leaves it healthy — or, once nothing completes at
// all, absent. The rule has to read the live provisioning view instead.
func TestBurstStuckProvisioningReadsLiveEvidence(t *testing.T) {
	r := mustRule(t, loadRules(t), "BurstStuckProvisioning")

	if !strings.Contains(r.Expr, "yscale_burst_provisioning_oldest_age_seconds") {
		t.Errorf("expr does not read the live provisioning age:\n%s", r.Expr)
	}
	if strings.Contains(r.Expr, "histogram_quantile") || strings.Contains(r.Expr, "yscale_burst_provision_seconds") {
		t.Errorf("expr is back on the completed-start histogram, which cannot see a "+
			"burst that never starts:\n%s", r.Expr)
	}
	if !strings.Contains(r.Expr, "360") {
		t.Errorf("expr no longer carries the 360-second stuck threshold:\n%s", r.Expr)
	}
	// Aggregated, so the threshold keeps meaning "the oldest stuck burst" no
	// matter how many replicas Prometheus scrapes.
	if !strings.Contains(r.Expr, "max(") {
		t.Errorf("expr is not aggregated across replicas; each central publishes the "+
			"same authoritative set, so an unaggregated comparison is per-target:\n%s", r.Expr)
	}
	d, err := time.ParseDuration(r.For)
	if err != nil || d <= 0 {
		t.Errorf("for = %q, want a positive duration so a transient slow boot cannot page", r.For)
	}
}

// TestProvisioningVisibilityLossIsAlerted covers the fail-closed half. Central
// omits the count and the age when it cannot read the authoritative burst view,
// so BurstStuckProvisioning goes quiet exactly when it is blind; something has
// to fire on the blindness itself or the silence reads as health.
func TestProvisioningVisibilityLossIsAlerted(t *testing.T) {
	r := mustRule(t, loadRules(t), "BurstProvisioningVisibilityLost")

	if !strings.Contains(r.Expr, "yscale_burst_provisioning_source_up") {
		t.Errorf("expr does not read the collector's read-health series:\n%s", r.Expr)
	}
	if d, err := time.ParseDuration(r.For); err != nil || d <= 0 {
		t.Errorf("for = %q, want a positive duration so one failed scrape cannot page", r.For)
	}
}

func TestStatePersistenceFailuresAlertImmediately(t *testing.T) {
	r := mustRule(t, loadRules(t), "StatePersistenceFailures")

	if !strings.Contains(r.Expr, "yscale_state_persistence_failures_total") || !strings.Contains(r.Expr, "increase(") {
		t.Errorf("expr does not alert from persistence-failure increments:\n%s", r.Expr)
	}
	if r.For != "0m" {
		t.Errorf("for = %q, want immediate alerting for an acknowledged durability fault", r.For)
	}
	if r.Labels["severity"] != "critical" {
		t.Errorf("severity = %q, want critical", r.Labels["severity"])
	}
}

// metricName matches a central-exported series name in a PromQL expression.
// Deliberately prefix-scoped: the synthetic-lifecycle rules read
// kube-state-metrics series, which central does not declare.
var metricName = regexp.MustCompile(`yscale_[a-z0-9_]+`)

// TestRuleMetricsAreDeclaredByCentral is what makes the semantic layer worth
// running on its own. An alert that names a series nothing exports never fires
// and never errors — it is silently dead, which is indistinguishable from a
// healthy system. Every yscale_ series the rules read must be declared in
// central/internal/cost.
func TestRuleMetricsAreDeclaredByCentral(t *testing.T) {
	entries, err := os.ReadDir(meterDir)
	if err != nil {
		t.Fatalf("read %s: %v", meterDir, err)
	}
	var declared strings.Builder
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Join(meterDir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		declared.Write(src)
	}
	meterSrc := declared.String()

	for name, r := range loadRules(t) {
		for _, series := range metricName.FindAllString(r.Expr, -1) {
			// _bucket / _sum / _count are the histogram's derived series; the
			// declared name is the family.
			base := series
			for _, suffix := range []string{"_bucket", "_sum", "_count"} {
				base = strings.TrimSuffix(base, suffix)
			}
			if !strings.Contains(meterSrc, `"`+base+`"`) {
				t.Errorf("alert %s reads %q, which %s does not declare — the rule would "+
					"never fire and never error", name, series, meterDir)
			}
		}
	}
}

// TestPromtoolRuleUnitTests runs the real Prometheus evaluator over the rules
// that actually ship. It extracts the PrometheusRule's .spec rather than
// asserting against a hand-kept copy, so the rules under test cannot drift from
// the rules in the cluster.
//
// Fixture parsing always runs, including on machines without promtool. Only
// evaluation may skip; an invalid fixture must fail the ordinary Go test gate.
func TestPromtoolRuleUnitTests(t *testing.T) {
	testRaw, err := os.ReadFile(filepath.Join(monitoringDir, unitTestFile))
	if err != nil {
		t.Fatalf("read %s: %v", unitTestFile, err)
	}
	var unit struct {
		RuleFiles []string `yaml:"rule_files"`
	}
	if err := yaml.Unmarshal(testRaw, &unit); err != nil {
		t.Fatalf("parse %s: %v", unitTestFile, err)
	}
	if len(unit.RuleFiles) != 1 {
		t.Fatalf("%s lists %d rule_files, want exactly 1 (the extracted spec)", unitTestFile, len(unit.RuleFiles))
	}
	bin, err := exec.LookPath("promtool")
	if err != nil {
		if os.Getenv("YSCALE_REQUIRE_PROMTOOL") == "1" {
			t.Fatal("promtool is required for CI rule evaluation but is not on PATH")
		}
		t.Skip("fixture YAML validated; promtool not installed, skipping rule evaluation")
	}

	crdRaw, err := os.ReadFile(filepath.Join(monitoringDir, crdFile))
	if err != nil {
		t.Fatalf("read %s: %v", crdFile, err)
	}
	var envelope struct {
		Spec yaml.Node `yaml:"spec"`
	}
	if err := yaml.Unmarshal(crdRaw, &envelope); err != nil {
		t.Fatalf("parse %s: %v", crdFile, err)
	}
	spec, err := yaml.Marshal(&envelope.Spec)
	if err != nil {
		t.Fatalf("re-marshal spec of %s: %v", crdFile, err)
	}

	dir := t.TempDir()
	rulesPath := filepath.Join(dir, unit.RuleFiles[0])
	if err := os.WriteFile(rulesPath, spec, 0o600); err != nil {
		t.Fatalf("write extracted rules: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, unitTestFile), testRaw, 0o600); err != nil {
		t.Fatalf("write unit tests: %v", err)
	}

	// Syntax first: a check failure and a test failure are different bugs.
	if out, err := run(bin, dir, "check", "rules", unit.RuleFiles[0]); err != nil {
		t.Fatalf("promtool check rules failed: %v\n%s", err, out)
	}
	if out, err := run(bin, dir, "test", "rules", unitTestFile); err != nil {
		t.Fatalf("promtool test rules failed: %v\n%s", err, out)
	}
}

// run executes promtool inside dir so rule_files resolve relative to the unit
// test file whichever way the installed promtool interprets them.
func run(bin, dir string, args ...string) (string, error) {
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	return string(out), err
}
