// Package helmrender holds render-time guards for the yscale-agent chart.
//
// These are the boundaries that only exist in template output: a Secret grant
// that must stay namespaced, a bind address that must never widen to a
// wildcard, and a `tailscale serve` forward that must track the bind address.
// Nothing in the Go build catches a template that stops emitting them, so they
// are asserted here, inside `go test ./...`, rather than in a script.
//
// Skipped when helm isn't on PATH.
package helmrender

import (
	"errors"
	"io"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const chartDir = "../../deploy/helm/yscale-agent"

// render runs `helm template` and fails the test if the chart doesn't render.
func render(t *testing.T, args ...string) string {
	t.Helper()
	out, err := helm(t, append([]string{"template", chartDir}, args...)...)
	if err != nil {
		t.Fatalf("helm template %v: %v\n%s", args, err, out)
	}
	return out
}

// renderErr runs `helm template` expecting it to fail, and returns the output.
func renderErr(t *testing.T, args ...string) string {
	t.Helper()
	out, err := helm(t, append([]string{"template", chartDir}, args...)...)
	if err == nil {
		t.Fatalf("helm template %v rendered, expected it to fail:\n%s", args, out)
	}
	return out
}

func helm(t *testing.T, args ...string) (string, error) {
	t.Helper()
	bin, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm not installed; skipping chart render checks")
	}
	out, err := exec.Command(bin, args...).CombinedOutput()
	return string(out), err
}

func mustContain(t *testing.T, haystack, needle, why string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("%s: rendered output is missing %q", why, needle)
	}
}

func mustNotContain(t *testing.T, haystack, needle, why string) {
	t.Helper()
	if strings.Contains(haystack, needle) {
		t.Errorf("%s: rendered output still contains %q", why, needle)
	}
}

// clusterRoleRules returns just the ClusterRole documents. Helm groups output
// by kind, so everything between a ClusterRole and the next kind is the
// cluster-scoped rule set.
//
// Takes t and asserts it found something: every use of this is a "must NOT
// contain secrets" check, which an extractor that silently returned "" would
// pass without looking at anything.
func clusterRoleRules(t *testing.T, rendered string) string {
	t.Helper()
	var b strings.Builder
	in := false
	for _, line := range strings.Split(rendered, "\n") {
		switch {
		case line == "kind: ClusterRole":
			in = true
		case strings.HasPrefix(line, "kind: ") && line != "kind: ClusterRole":
			in = false
		}
		if in {
			b.WriteString(line)
			b.WriteString("\n")
		}
	}
	out := b.String()
	// The agent's cluster-wide read of nodes is stable and unrelated to this
	// diff; if it isn't in the extract, the extract is wrong.
	if !strings.Contains(out, "resources: [nodes]") {
		t.Fatalf("ClusterRole extraction found no cluster-scoped rules; the negative assertions below would be meaningless:\n%s", out)
	}
	return out
}

// secretGrant is one rendered RBAC rule that names Secrets: where it applies
// and what it allows. Keyed by kind/namespace/name so a test can state the
// whole set the chart is allowed to produce.
type secretGrant struct {
	key   string // "Role/kube-system/name" — ClusterRoles carry an empty namespace
	verbs string // comma-joined, in the order the rule lists them
}

// secretGrants decodes every document `helm template` produced and returns
// every rule granting anything on Secrets.
//
// Parsed, not grepped. "No Secret access anywhere else" is a claim about
// rules, and a substring search cannot make it: it cannot tell which document
// a `verbs:` line belongs to, so a `list` added to the kube-system Role and a
// `list` added to a new cluster-wide one look identical to it.
func secretGrants(t *testing.T, rendered string) []secretGrant {
	t.Helper()
	type rbacDoc struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Name      string `yaml:"name"`
			Namespace string `yaml:"namespace"`
		} `yaml:"metadata"`
		Rules []struct {
			APIGroups []string `yaml:"apiGroups"`
			Resources []string `yaml:"resources"`
			Verbs     []string `yaml:"verbs"`
		} `yaml:"rules"`
	}
	var grants []secretGrant
	roles := 0
	dec := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var doc rbacDoc
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decoding rendered chart: %v", err)
		}
		if doc.Kind != "Role" && doc.Kind != "ClusterRole" {
			continue
		}
		roles++
		for _, rule := range doc.Rules {
			if !slices.Contains(rule.Resources, "secrets") {
				continue
			}
			grants = append(grants, secretGrant{
				key:   doc.Kind + "/" + doc.Metadata.Namespace + "/" + doc.Metadata.Name,
				verbs: strings.Join(rule.Verbs, ","),
			})
		}
	}
	// Every negative assertion below is "this rule is not in the set". A
	// decode that found no rules at all would pass all of them by finding
	// nothing anywhere.
	if roles == 0 {
		t.Fatalf("no Role/ClusterRole documents decoded; the assertions below would be meaningless")
	}
	return grants
}

// rbacVerbs decodes every rendered Role/ClusterRole and returns what each one
// grants, keyed by "Kind/namespace/name" and then by resource. Two rules naming
// the same resource accumulate into one entry.
//
// Parsed, not grepped, for the reason secretGrants is: "no ClusterRole grants
// this" is a claim about which document a rule sits in, and a substring search
// cannot tell a verb added to a namespaced Role from the same verb added to a
// cluster-wide one.
func rbacVerbs(t *testing.T, rendered string) map[string]map[string][]string {
	t.Helper()
	type rbacDoc struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Name      string `yaml:"name"`
			Namespace string `yaml:"namespace"`
		} `yaml:"metadata"`
		Rules []struct {
			APIGroups []string `yaml:"apiGroups"`
			Resources []string `yaml:"resources"`
			Verbs     []string `yaml:"verbs"`
		} `yaml:"rules"`
	}
	grants := map[string]map[string][]string{}
	dec := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var doc rbacDoc
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decoding rendered chart: %v", err)
		}
		if doc.Kind != "Role" && doc.Kind != "ClusterRole" {
			continue
		}
		key := doc.Kind + "/" + doc.Metadata.Namespace + "/" + doc.Metadata.Name
		if grants[key] == nil {
			grants[key] = map[string][]string{}
		}
		for _, rule := range doc.Rules {
			for _, resource := range rule.Resources {
				grants[key][resource] = append(grants[key][resource], rule.Verbs...)
			}
		}
	}
	if len(grants) == 0 {
		t.Fatalf("no Role/ClusterRole documents decoded; the assertions below would be meaningless")
	}
	return grants
}

// Every Secret grant the chart renders, in both RBAC scopes, listed exactly.
// The two below check the storage-credentials Role in isolation; this one is
// the audit that says nothing ELSE grants Secrets — no cluster-wide read, no
// `list`/`watch` on a namespaced Role, and nothing in the cilium scaffolding
// or any template added later.
func TestNoSecretGrantLeaksBeyondTheNamespacedGetRoles(t *testing.T) {
	for _, scope := range []string{"namespaced", "cluster"} {
		t.Run(scope, func(t *testing.T) {
			out := render(t, "--namespace", "yscale", "--set", "token=x", "--set", "tsAuthKey=y",
				"--set", "rbac.scope="+scope, "--set", "rbac.allowedNamespaces={team-a,team-b}")

			// The agent Creates a bootstrap-token Secret, Gets
			// storage-credentials Secrets (broad), and maintains the
			// runtime-bindings Secret (scoped to one resourceName).
			want := map[string][]string{
				"Role/kube-system/release-name-yscale-agent-bootstrap": {"create"},
				"Role/team-a/release-name-yscale-agent-storage-creds":  {"get", "get,update,patch"},
				"Role/team-b/release-name-yscale-agent-storage-creds":  {"get", "get,update,patch"},
			}
			seen := map[string]map[string]bool{}
			for _, g := range secretGrants(t, out) {
				allowed, ok := want[g.key]
				if !ok {
					t.Errorf("unexpected Secret grant %s: verbs [%s]", g.key, g.verbs)
					continue
				}
				if !slices.Contains(allowed, g.verbs) {
					t.Errorf("%s grants Secret verbs [%s], want one of %v", g.key, g.verbs, allowed)
				}
				if seen[g.key] == nil {
					seen[g.key] = map[string]bool{}
				}
				seen[g.key][g.verbs] = true
			}
			for key, verbs := range want {
				for _, v := range verbs {
					if !seen[key][v] {
						t.Errorf("Secret grant %s with verbs [%s] went missing from the render", key, v)
					}
				}
			}
		})
	}
}

// The connector reads bucket-credentials Secrets. That grant is what a
// cross-namespace signing request ultimately needs, so it has to be
// namespaced to exactly rbac.allowedNamespaces and carry `get` alone
// (SECURITY-REVIEW.md H2).
func TestSecretRBACIsNamespacedAndGetOnly(t *testing.T) {
	out := render(t, "--set", "rbac.allowedNamespaces={team-a,team-b}", "-s", "templates/rbac.yaml")

	for _, ns := range []string{"team-a", "team-b"} {
		mustContain(t, out, "  namespace: "+ns, "storage-creds Role for "+ns)
	}
	// Role + RoleBinding + roleRef name, per namespace.
	if got := strings.Count(out, "yscale-agent-storage-creds"); got != 6 {
		t.Errorf("storage-creds name references = %d, want 6 (Role+RoleBinding+roleRef per namespace)", got)
	}
	mustContain(t, out, "verbs: [get]", "the storage-creds Role must be get-only")
	mustNotContain(t, clusterRoleRules(t, out), "secrets", "no ClusterRole may grant Secret access")
}

// scope: cluster widens workload permissions on purpose. It must NOT widen the
// Secret grant — a cluster-wide Secret read is the exposure the namespace pin
// exists to remove.
func TestClusterScopeDoesNotWidenSecretRBAC(t *testing.T) {
	out := render(t, "--set", "rbac.scope=cluster", "--set", "rbac.allowedNamespaces={team-a}", "-s", "templates/rbac.yaml")

	mustNotContain(t, clusterRoleRules(t, out), "secrets", "scope=cluster granted cluster-wide Secret reads")
	mustContain(t, out, "  namespace: team-a", "scope=cluster dropped the namespaced storage-creds Role")
	// kube-system's bootstrap-token Secret Role is a separate, deliberately
	// narrow grant and must survive.
	mustContain(t, out, "yscale-agent-bootstrap", "kube-system bootstrap-token Role went missing")
}

// Draining a node evicts every non-DaemonSet pod off it and deletes the ones
// the Eviction API won't move. A connector pinned to one workload namespace
// does both there, so the namespaced Role has to carry them: without the grant
// the drain dies partway, and the pods it already cordoned never move.
func TestNamespacedWorkloadRoleGrantsTheDrainVerbs(t *testing.T) {
	out := render(t, "--set", "rbac.allowedNamespaces={ys-cust-hosted-demo}", "-s", "templates/rbac.yaml")

	role := rbacVerbs(t, out)["Role/ys-cust-hosted-demo/release-name-yscale-agent-workloads"]
	if role == nil {
		t.Fatalf("no namespaced workloads Role rendered for the workload namespace")
	}
	// list is what the drain enumerates the node's pods with; eviction is the
	// PDB-aware move, and delete is the fallback when a PDB still blocks at the
	// drain deadline.
	for resource, verb := range map[string]string{
		"pods":          "list",
		"pods/eviction": "create",
	} {
		if !slices.Contains(role[resource], verb) {
			t.Errorf("namespaced workloads Role grants %s %v, want %q among them", resource, role[resource], verb)
		}
	}
	if !slices.Contains(role["pods"], "delete") {
		t.Errorf("namespaced workloads Role grants pods %v, want delete among them", role["pods"])
	}
}

// A namespaced connector must not buy its scoped drain back at cluster scope.
func TestNamespacedScopeHasNoClusterWidePodDrainGrant(t *testing.T) {
	out := render(t, "--set", "rbac.scope=namespaced",
		"--set", "rbac.allowedNamespaces={ys-cust-hosted-demo}", "-s", "templates/rbac.yaml")
	for key, byResource := range rbacVerbs(t, out) {
		if !strings.HasPrefix(key, "ClusterRole/") {
			continue
		}
		if slices.Contains(byResource["pods"], "delete") || slices.Contains(byResource["pods/eviction"], "create") {
			t.Errorf("%s grants a cluster-wide pod drain operation", key)
		}
	}
	// Node and CSR access stays cluster-scoped because those objects are not
	// namespaced and teardown still removes the Node object.
	cluster := rbacVerbs(t, out)["ClusterRole//release-name-yscale-agent-cluster"]
	if !slices.Contains(cluster["nodes"], "delete") {
		t.Errorf("cluster role grants nodes %v, want delete among them", cluster["nodes"])
	}
	if !slices.Contains(cluster["certificatesigningrequests/approval"], "update") {
		t.Errorf("cluster role lost CSR approval: %v", cluster["certificatesigningrequests/approval"])
	}
}

// A BYOC cluster-scope connector keeps the historical all-namespace drain and
// therefore needs both fallback delete and PDB-aware eviction cluster-wide.
func TestClusterScopeWorkloadRoleGrantsTheDrainVerbs(t *testing.T) {
	out := render(t, "--set", "rbac.scope=cluster", "-s", "templates/rbac.yaml")
	role := rbacVerbs(t, out)["ClusterRole//release-name-yscale-agent-workloads"]
	if !slices.Contains(role["pods"], "delete") {
		t.Errorf("cluster workload role grants pods %v, want delete", role["pods"])
	}
	if !slices.Contains(role["pods/eviction"], "create") {
		t.Errorf("cluster workload role grants pods/eviction %v, want create", role["pods/eviction"])
	}
}

// The artifact uploader Job runs in the connector's own namespace
// (ArtifactNamespace / POD_NAMESPACE), and the completion watcher lists that
// Job's pods there to read the termination summary the receipt's counts come
// from. The release namespace is not required to be in rbac.allowedNamespaces,
// so the workloads Role does not reach it: without this grant a namespaced
// install loses the counts off every artifact receipt.
func TestArtifactRoleGrantsPodListInTheReleaseNamespace(t *testing.T) {
	for _, scope := range []string{"namespaced", "cluster"} {
		t.Run(scope, func(t *testing.T) {
			out := render(t, "--namespace", "yscale", "--set", "rbac.scope="+scope,
				"--set", "rbac.allowedNamespaces={team-a}", "-s", "templates/rbac.yaml")

			roles := rbacVerbs(t, out)
			role := roles["Role/yscale/release-name-yscale-agent-artifacts"]
			if role == nil {
				t.Fatalf("no artifacts Role rendered in the release namespace")
			}
			// Exactly `list`. Creating, patching, evicting or deleting the
			// uploader's pods is the Job controller's business, and a `get` or
			// `watch` here would be a widening nothing in the read path asks for.
			if got := role["pods"]; !slices.Equal(got, []string{"list"}) {
				t.Errorf("artifacts Role grants pods %v, want exactly [list]", got)
			}
			if got := role["jobs"]; !slices.Equal(got, []string{"get", "create"}) {
				t.Errorf("artifacts Role grants jobs %v, want [get create]", got)
			}
			// A Secret, a pod subresource, anything else: this Role needs the
			// uploader Job and its pods, and nothing has been added to it.
			for resource, verbs := range role {
				if resource != "jobs" && resource != "pods" {
					t.Errorf("artifacts Role grants %s %v, which it has no read path for", resource, verbs)
				}
			}
			if scope == "cluster" {
				return
			}
			// The grant belongs to one namespace. A namespaced install that
			// bought it cluster-wide could read every pod in the cluster to
			// learn the same thing.
			for key, byResource := range roles {
				if strings.HasPrefix(key, "ClusterRole/") && len(byResource["pods"]) > 0 {
					t.Errorf("%s grants pods %v cluster-wide", key, byResource["pods"])
				}
			}
		})
	}
}

func TestNamespacedWorkloadNamespaceMustBeAllowed(t *testing.T) {
	out := renderErr(t,
		"--set", "rbac.scope=namespaced",
		"--set", "rbac.allowedNamespaces={team-a}",
		"--set", "workloadNamespace=team-b",
		"-s", "templates/rbac.yaml")
	mustContain(t, out, `workloadNamespace="team-b" must be listed in rbac.allowedNamespaces`, "values drift must fail at render time")
}

// The idle grace is a value an operator tunes and a support engineer has to be
// able to read off a running pod, so it is rendered explicitly rather than left
// to the binary's own default — including the 0 that turns the watcher off.
func TestIdleTeardownGraceRendersOntoTheConnector(t *testing.T) {
	out := render(t, "-s", "templates/deployment.yaml")
	mustContain(t, out, "-idle-node-grace=10m", "the shipped default idle grace")

	tuned := render(t, "--set", "idleTeardown.grace=45m", "-s", "templates/deployment.yaml")
	mustContain(t, tuned, "-idle-node-grace=45m", "a tuned idle grace reaches the connector")

	// Disabling is a value, not an omission: a chart that stopped rendering the
	// flag would silently restore the binary's default.
	off := render(t, "--set", "idleTeardown.grace=0", "-s", "templates/deployment.yaml")
	mustContain(t, off, "-idle-node-grace=0", "idleTeardown.grace=0 must render explicitly")
}

// The occupancy interval is what holds central's nodeOnly ceiling open, so an
// operator has to be able to read the deployed value off a running pod — the 0
// that switches the observations off above all, since a burst nothing observes
// is bounded by its declared budget and nothing else.
func TestOccupancyObserveIntervalRendersOntoTheConnector(t *testing.T) {
	out := render(t, "-s", "templates/deployment.yaml")
	mustContain(t, out, "-occupancy-observe-interval=5m", "the shipped default occupancy interval")

	tuned := render(t, "--set", "occupancyObserveInterval=2m", "-s", "templates/deployment.yaml")
	mustContain(t, tuned, "-occupancy-observe-interval=2m", "a tuned occupancy interval reaches the connector")

	off := render(t, "--set", "occupancyObserveInterval=0", "-s", "templates/deployment.yaml")
	mustContain(t, off, "-occupancy-observe-interval=0", "occupancyObserveInterval=0 must render explicitly")
}

// Authoritative pod visibility is a CAPABILITY, and this chart is the only thing
// that knows whether it granted it. The connector cannot tell: an unset
// workloadNamespace says nobody scoped it, not that its Role can list pods
// cluster-wide — and the shipped RBAC scope is namespaced, so the honest answer
// for the default install is false.
//
// Getting this wrong is not cosmetic. A true here with no cluster-wide pod read
// behind it is a connector whose every sweep fails, sending central no occupancy
// observation at all while claiming the feature is live.
func TestAuthoritativePodVisibilityIsGrantedOnlyByClusterScope(t *testing.T) {
	// The shipped default: rbac.scope=namespaced.
	out := render(t, "-s", "templates/deployment.yaml")
	mustContain(t, out, "-authoritative-pod-visibility=false",
		"the namespaced default must state it has no authoritative pod visibility")
	mustNotContain(t, out, "-authoritative-pod-visibility=true",
		"the namespaced default must never claim authoritative pod visibility")

	// The one topology where the pod list IS the answer.
	cluster := render(t, "--set", "rbac.scope=cluster", "-s", "templates/deployment.yaml")
	mustContain(t, cluster, "-authoritative-pod-visibility=true",
		"a cluster-scoped connector with no workload namespace has the visibility")

	// Cluster RBAC does not help a connector pinned to one namespace: "idle" is a
	// claim about every namespace's pods, and this one watches one namespace's
	// Workload CRs.
	scoped := render(t,
		"--set", "rbac.scope=cluster",
		"--set", "workloadNamespace=team-a",
		"--set", "rbac.allowedNamespaces={team-a}",
		"-s", "templates/deployment.yaml")
	mustContain(t, scoped, "-authoritative-pod-visibility=false",
		"a cluster-scoped connector scoped to one workload namespace still cannot answer")
}

// The fix for the watchdog must not be quietly undone by widening RBAC instead.
// A namespaced install stays namespaced: its nodeOnly bursts are bounded by a
// declared budget, not by a cluster-wide Pod LIST added to make the idle watcher
// runnable. That grant is the whole exposure the namespaced scope exists to
// avoid — it reads every team's workloads on a shared cluster.
func TestNamespacedScopeGrantsNoClusterWidePodRead(t *testing.T) {
	out := render(t, "-s", "templates/rbac.yaml")
	for key, byResource := range rbacVerbs(t, out) {
		if !strings.HasPrefix(key, "ClusterRole/") {
			continue
		}
		for _, res := range []string{"pods", "pods/log"} {
			if slices.Contains(byResource[res], "list") {
				t.Errorf("%s grants a cluster-wide %s list; the namespaced default must not", key, res)
			}
		}
	}
}

// The agent is a Pod, so a wildcard bind publishes burst-token minting and
// bucket-URL signing on the customer's pod network (H3). The shipped default
// must not produce one, and must not leave the address to the binary's
// fallback either.
func TestShippedDefaultBindsLoopbackNotWildcard(t *testing.T) {
	out := render(t, "-s", "templates/deployment.yaml")

	mustContain(t, out, "-bootstrap-listen=127.0.0.1:8080", "default bind address")
	mustNotContain(t, out, "-bootstrap-listen=0.0.0.0", "default must not render a wildcard")
	mustNotContain(t, out, "-bootstrap-listen=:8080", "default must render an explicit address, not a bare port")
}

func TestListenModesRenderTheRightSpelling(t *testing.T) {
	tailnet := render(t, "--set", "bootstrap.listenMode=tailnet", "-s", "templates/deployment.yaml")
	mustContain(t, tailnet, "-bootstrap-listen=tailnet:8080", "tailnet mode asks for the tailnet address by name")

	// A wildcard is reachable, but only for an operator who writes one out.
	custom := render(t, "--set", "bootstrap.listenMode=custom", "--set", "bootstrap.listenAddress=0.0.0.0:8080", "-s", "templates/deployment.yaml")
	mustContain(t, custom, "-bootstrap-listen=0.0.0.0:8080", "custom mode honours listenAddress")

	out := renderErr(t, "--set", "bootstrap.listenMode=custom", "-s", "templates/deployment.yaml")
	mustContain(t, out, "requires bootstrap.listenAddress", "custom without an address must fail rendering")

	out = renderErr(t, "--set", "bootstrap.listenMode=everywhere", "-s", "templates/deployment.yaml")
	mustContain(t, out, "is not one of: loopback, tailnet, custom", "an unknown listenMode must fail rendering")
}

// The serve forward and the bind address are two halves of one path. Rendered
// from separate literals they drift on a port change and the endpoint goes
// quiet with nothing in the chart to show why.
func TestTailscaleServeForwardTracksTheBindAddress(t *testing.T) {
	serve := render(t, "--set", "bootstrap.port=9090", "-s", "templates/tailscale-serve.yaml")
	mustContain(t, serve, `"9090": {"HTTPS": false, "TCPForward": "127.0.0.1:9090"}`, "serve forward follows bootstrap.port")
	mustNotContain(t, serve, "8080", "serve config still carries a hardcoded port")

	deploy := render(t, "--set", "bootstrap.port=9090", "-s", "templates/deployment.yaml")
	mustContain(t, deploy, "-bootstrap-listen=127.0.0.1:9090", "bind address follows bootstrap.port")

	// In tailnet mode the agent binds tailscaled's own address, so forwarding
	// to loopback would point at nothing.
	tailnet := render(t, "--set", "bootstrap.listenMode=tailnet", "-s", "templates/tailscale-serve.yaml")
	mustNotContain(t, tailnet, "TCPForward", "tailnet mode must not forward to loopback")
}

func TestTailscaleAcceptRoutesIsExplicit(t *testing.T) {
	public := render(t, "-s", "templates/deployment.yaml")
	mustContain(t, public, "--advertise-tags=tag:yscale", "connector keeps its required device tag")
	mustNotContain(t, public, "--accept-routes", "public-central installs must not accept peer subnet routes")

	private := render(t, "--set", "tailscale.acceptRoutes=true", "-s", "templates/deployment.yaml")
	mustContain(t, private, "--advertise-tags=tag:yscale --accept-routes",
		"private-central installs must accept the route to central")
}

// The chart version has to move with the behaviour, or an operator upgrading
// gets a new bind address and new RBAC requirements under the same version.
func TestChartVersionCarriesTheUpgradeNote(t *testing.T) {
	out := render(t, "--set", "token=x", "--set", "tsAuthKey=y")
	mustContain(t, out, "helm.sh/chart: yscale-agent-0.2.4", "chart version was not bumped for the public image default")
}

// GKE bootstrap.authMode=gke renders the bind RBAC for
// system:node-bootstrapper and scopes GKE SA management to the release
// namespace, not kube-system.
func TestGKEBootstrapRendersBindRBACAndReleaseScopedSA(t *testing.T) {
	out := render(t, "--namespace", "yscale",
		"--set", "bootstrap.authMode=gke",
		"--set", "cloudProvider=gcp",
		"-s", "templates/rbac.yaml")

	// The bind verb on clusterroles scoped to system:node-bootstrapper.
	mustContain(t, out, "resourceNames: [system:node-bootstrapper]",
		"GKE ClusterRole must carry bind permission scoped to system:node-bootstrapper")
	mustContain(t, out, "verbs: [bind]",
		"GKE ClusterRole must grant the bind verb")

	// CRB management includes get for collision verification.
	mustContain(t, out, "verbs: [get, create, delete]",
		"GKE ClusterRole must grant get/create/delete on clusterrolebindings")

	// SA management is in the release namespace, not kube-system.
	roles := rbacVerbs(t, out)
	gkeSARole := roles["Role/yscale/release-name-yscale-agent-gke-sa"]
	if gkeSARole == nil {
		t.Fatal("GKE SA Role not rendered in release namespace")
	}
	if got := gkeSARole["serviceaccounts"]; !slices.Contains(got, "create") || !slices.Contains(got, "delete") || !slices.Contains(got, "get") {
		t.Errorf("GKE SA Role grants serviceaccounts %v, want [get create delete]", got)
	}
	if got := gkeSARole["serviceaccounts/token"]; !slices.Contains(got, "create") {
		t.Errorf("GKE SA Role grants serviceaccounts/token %v, want [create]", got)
	}

	// kube-system bootstrap Role must NOT grant SA/token in GKE mode.
	kubeBootstrap := roles["Role/kube-system/release-name-yscale-agent-bootstrap"]
	if kubeBootstrap == nil {
		t.Fatal("kube-system bootstrap Role not rendered")
	}
	if got := kubeBootstrap["serviceaccounts"]; len(got) > 0 {
		t.Errorf("kube-system bootstrap Role should not grant serviceaccounts in GKE mode, got %v", got)
	}
	if got := kubeBootstrap["serviceaccounts/token"]; len(got) > 0 {
		t.Errorf("kube-system bootstrap Role should not grant serviceaccounts/token in GKE mode, got %v", got)
	}
}

// Default mode (kubernetes-token) must not render GKE-specific RBAC at all.
func TestDefaultModeOmitsGKERBAC(t *testing.T) {
	out := render(t, "-s", "templates/rbac.yaml")

	mustNotContain(t, out, "gke-bootstrap",
		"default mode must not render the GKE bootstrap ClusterRole")
	mustNotContain(t, out, "gke-sa",
		"default mode must not render the GKE SA Role")
	mustNotContain(t, out, "verbs: [bind]",
		"default mode must not render the bind verb")
}

// GKE SA/token RBAC must not leak into kube-system: a token-create grant
// there covers every SA including kube-controller-manager.
func TestGKEModeNoSATokenInKubeSystem(t *testing.T) {
	out := render(t, "--namespace", "yscale",
		"--set", "bootstrap.authMode=gke",
		"--set", "cloudProvider=gcp",
		"-s", "templates/rbac.yaml")

	grants := secretGrants(t, out)
	for _, g := range grants {
		if strings.Contains(g.key, "kube-system") && strings.Contains(g.verbs, "token") {
			t.Errorf("kube-system still grants token access: %s verbs [%s]", g.key, g.verbs)
		}
	}

	roles := rbacVerbs(t, out)
	kubeBootstrap := roles["Role/kube-system/release-name-yscale-agent-bootstrap"]
	if kubeBootstrap != nil {
		for resource := range kubeBootstrap {
			if resource == "serviceaccounts/token" || resource == "serviceaccounts" {
				t.Errorf("kube-system bootstrap Role still grants %s in GKE mode", resource)
			}
		}
	}
}

// GKE must NOT pass a burst-provider-id-format (the burst omits
// --provider-id entirely because GKE admission rejects non-GCE IDs).
// EKS and LKE must still carry their prefixed formats.
func TestProviderIDFormatByCloudProvider(t *testing.T) {
	for _, tt := range []struct {
		name    string
		cloud   string
		present string // must appear when non-empty
		absent  string // must NOT appear when non-empty
	}{
		{
			name:   "GKE omits burst-provider-id-format",
			cloud:  "gcp",
			absent: "burst-provider-id-format",
		},
		{
			name:    "EKS carries aws prefix",
			cloud:   "aws",
			present: "burst-provider-id-format=aws://yscale-burst-{BURST_ID}",
		},
		{
			name:    "LKE carries linode prefix",
			cloud:   "linode",
			present: "burst-provider-id-format=linode://yscale-burst-{BURST_ID}",
		},
		{
			name:   "k3s omits burst-provider-id-format",
			cloud:  "k3s",
			absent: "burst-provider-id-format",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out := render(t,
				"--set", "cloudProvider="+tt.cloud,
				"-s", "templates/deployment.yaml")
			if tt.present != "" {
				mustContain(t, out, tt.present, tt.name)
			}
			if tt.absent != "" {
				mustNotContain(t, out, tt.absent, tt.name)
			}
		})
	}
}

// TestCRDConstrainsProjectedStatus pins the Workload CRD schema on the
// live GPU telemetry pair the agent projects from central: gpuUtilPercent
// is a number bounded to [0, 100] and lastHeartbeatAt is an RFC3339
// date-time. ProviderCreatedAt is also a central-owned RFC3339 timestamp.
//
// The additive receipt fields (computeResult/Reason and the four artifact
// fields) travel alongside the legacy status.outcome string — the two count
// fields carry a non-negative int64 bound so a signed negative can never be
// persisted, and the legacy `outcome` string is deliberately unconstrained.
func TestCRDConstrainsProjectedStatus(t *testing.T) {
	out := render(t, "-s", "templates/crd.yaml")
	type propType struct {
		Type    string   `yaml:"type"`
		Format  string   `yaml:"format"`
		Minimum *float64 `yaml:"minimum"`
		Maximum *float64 `yaml:"maximum"`
	}
	var doc struct {
		Kind string `yaml:"kind"`
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema struct {
						Properties struct {
							Status struct {
								Properties struct {
									GPU struct {
										Type       string `yaml:"type"`
										Properties struct {
											Product propType `yaml:"product"`
										} `yaml:"properties"`
									} `yaml:"gpu"`
									GpuUtilPercent          propType `yaml:"gpuUtilPercent"`
									LastHeartbeatAt         propType `yaml:"lastHeartbeatAt"`
									ProviderCreatedAt       propType `yaml:"providerCreatedAt"`
									ComputeResult           propType `yaml:"computeResult"`
									ComputeReason           propType `yaml:"computeReason"`
									ArtifactResult          propType `yaml:"artifactResult"`
									ArtifactReason          propType `yaml:"artifactReason"`
									ArtifactObjectsUploaded propType `yaml:"artifactObjectsUploaded"`
									ArtifactBytesUploaded   propType `yaml:"artifactBytesUploaded"`
								} `yaml:"properties"`
							} `yaml:"status"`
						} `yaml:"properties"`
					} `yaml:"openAPIV3Schema"`
				} `yaml:"schema"`
			} `yaml:"versions"`
		} `yaml:"spec"`
	}
	dec := yaml.NewDecoder(strings.NewReader(out))
	crds := 0
	for {
		d := doc
		err := dec.Decode(&d)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode CRD render: %v", err)
		}
		if d.Kind != "CustomResourceDefinition" {
			continue
		}
		crds++
		if len(d.Spec.Versions) == 0 {
			t.Fatal("CRD rendered with no versions; assertions would be meaningless")
		}
		props := d.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Status.Properties
		if props.GPU.Type != "object" || props.GPU.Properties.Product.Type != "string" {
			t.Errorf("gpu product schema = %+v, want gpu object with string product", props.GPU)
		}
		if props.GpuUtilPercent.Type != "number" || props.GpuUtilPercent.Format != "double" ||
			props.GpuUtilPercent.Minimum == nil || *props.GpuUtilPercent.Minimum != 0 ||
			props.GpuUtilPercent.Maximum == nil || *props.GpuUtilPercent.Maximum != 100 {
			t.Errorf("gpuUtilPercent schema = %+v, want {number/double [0,100]}", props.GpuUtilPercent)
		}
		if props.LastHeartbeatAt.Type != "string" || props.LastHeartbeatAt.Format != "date-time" {
			t.Errorf("lastHeartbeatAt schema = %+v, want {string/date-time}", props.LastHeartbeatAt)
		}
		if props.ProviderCreatedAt.Type != "string" || props.ProviderCreatedAt.Format != "date-time" {
			t.Errorf("providerCreatedAt schema = %+v, want {string/date-time}", props.ProviderCreatedAt)
		}
		for name, got := range map[string]propType{
			"computeResult":  props.ComputeResult,
			"computeReason":  props.ComputeReason,
			"artifactResult": props.ArtifactResult,
			"artifactReason": props.ArtifactReason,
		} {
			if got.Type != "string" {
				t.Errorf("%s schema = %+v, want {string}", name, got)
			}
		}
		for name, got := range map[string]propType{
			"artifactObjectsUploaded": props.ArtifactObjectsUploaded,
			"artifactBytesUploaded":   props.ArtifactBytesUploaded,
		} {
			if got.Type != "integer" || got.Format != "int64" ||
				got.Minimum == nil || *got.Minimum != 0 {
				t.Errorf("%s schema = %+v, want {integer/int64 minimum=0}", name, got)
			}
		}
	}
	if crds == 0 {
		t.Fatal("no CustomResourceDefinition document rendered; the schema assertion above found nothing to check")
	}
}

// cloud-provider flag is passed for every non-empty cloudProvider so the
// agent can plumb it to bursts in the bootstrap response.
func TestCloudProviderFlagPlumbed(t *testing.T) {
	for _, cloud := range []string{"gcp", "aws", "linode"} {
		t.Run(cloud, func(t *testing.T) {
			out := render(t,
				"--set", "cloudProvider="+cloud,
				"-s", "templates/deployment.yaml")
			mustContain(t, out, "-cloud-provider="+cloud,
				"cloud-provider flag must be passed for "+cloud)
		})
	}
}
