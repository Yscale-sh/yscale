package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/yscale-sh/yscale/agent/internal/agent"
)

func TestParseFlagsSelfHostedEndpoint(t *testing.T) {
	for _, tc := range []struct{ name, env, flag, want string }{
		{name: "local default", want: "http://127.0.0.1:8443"},
		{name: "operator env", env: "https://central.example.invalid", want: "https://central.example.invalid"},
		{name: "flag wins", env: "https://ignored.example.invalid", flag: "https://chosen.example.invalid", want: "https://chosen.example.invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("YSCALE_ENDPOINT", tc.env)
			args := []string{"-token=fixture", "-cluster-id=fixture"}
			if tc.flag != "" {
				args = append(args, "-endpoint="+tc.flag)
			}
			opts, err := parseFlags(args)
			if err != nil {
				t.Fatal(err)
			}
			if opts.endpoint != tc.want {
				t.Fatalf("endpoint=%q, want %q", opts.endpoint, tc.want)
			}
		})
	}
}

func TestParseFlagsGatewayEnabledDefaultAndOverrides(t *testing.T) {
	tests := []struct {
		name string
		env  string
		args []string
		want bool
	}{
		{name: "default true", want: true},
		{name: "env true", env: "true", want: true},
		{name: "env false", env: "false", want: false},
		{name: "env typo disables", env: "definitely", want: false},
		{name: "flag false wins over env true", env: "true", args: []string{"-gateway-enabled=false"}, want: false},
		{name: "flag true wins over env false", env: "false", args: []string{"-gateway-enabled"}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("YSCALE_GATEWAY_ENABLED", tt.env)
			args := []string{"-token=t", "-cluster-id=c"}
			args = append(args, tt.args...)
			opts, err := parseFlags(args)
			if err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			if got := opts.gatewayEnabled; got != tt.want {
				t.Fatalf("gatewayEnabled = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseFlagsEKSBootstrapRequiresCompleteAWSConfiguration(t *testing.T) {
	base := []string{"-token=t", "-cluster-id=c", "-cloud-provider=aws", "-bootstrap-auth-mode=eks"}
	for _, tt := range []struct {
		name string
		args []string
		ok   bool
	}{
		{name: "complete", args: []string{"-eks-cluster-name=c", "-eks-region=us-east-1", "-eks-bootstrap-role-arn=arn:aws:iam::123456789012:role/bootstrap"}, ok: true},
		{name: "missing cluster", args: []string{"-eks-region=us-east-1", "-eks-bootstrap-role-arn=arn:aws:iam::123456789012:role/bootstrap"}},
		{name: "missing region", args: []string{"-eks-cluster-name=c", "-eks-bootstrap-role-arn=arn:aws:iam::123456789012:role/bootstrap"}},
		{name: "missing role", args: []string{"-eks-cluster-name=c", "-eks-region=us-east-1"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseFlags(append(append([]string{}, base...), tt.args...))
			if tt.ok && err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			if !tt.ok && err == nil {
				t.Fatal("parseFlags accepted incomplete EKS bootstrap configuration")
			}
		})
	}
}

func TestParseFlagsGKEBootstrapValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		ok   bool
	}{
		{
			name: "gke with gcp cloud provider",
			args: []string{"-token=t", "-cluster-id=c", "-cloud-provider=gcp", "-bootstrap-auth-mode=gke"},
			ok:   true,
		},
		{
			name: "gke with non-gcp cloud provider",
			args: []string{"-token=t", "-cluster-id=c", "-cloud-provider=aws", "-bootstrap-auth-mode=gke"},
			ok:   false,
		},
		{
			name: "gke with disable-bootstrap",
			args: []string{"-token=t", "-cluster-id=c", "-cloud-provider=gcp", "-bootstrap-auth-mode=gke", "-disable-bootstrap"},
			ok:   false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseFlags(tt.args)
			if tt.ok && err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			if !tt.ok && err == nil {
				t.Fatal("parseFlags accepted invalid GKE bootstrap configuration")
			}
		})
	}
}

func TestParseFlagsGKEBootstrapRejectsNonGCPCluster(t *testing.T) {
	_, err := parseFlags([]string{
		"-token=t", "-cluster-id=c", "-cloud-provider=linode", "-bootstrap-auth-mode=gke",
	})
	if err == nil || !strings.Contains(err.Error(), "cloud-provider=gcp") {
		t.Fatalf("error = %v, want cloud-provider requirement", err)
	}
}

func TestParseFlagsEKSBootstrapRejectsNonAWSCluster(t *testing.T) {
	_, err := parseFlags([]string{
		"-token=t", "-cluster-id=c", "-cloud-provider=k3s", "-bootstrap-auth-mode=eks",
		"-eks-cluster-name=c", "-eks-region=us-east-1", "-eks-bootstrap-role-arn=arn:aws:iam::123456789012:role/bootstrap",
	})
	if err == nil || !strings.Contains(err.Error(), "cloud-provider=aws") {
		t.Fatalf("error = %v, want cloud-provider requirement", err)
	}
}

// -workload-namespace is the single value that pins this connector to one
// tenant: the CR reconciler, the watchers, and the node drain all read it.
// Unset has to stay empty rather than defaulting to a namespace, because empty
// is what every existing BYOC install runs with and it means "all namespaces".
func TestParseFlagsWorkloadNamespace(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{name: "unset stays empty", want: ""},
		{name: "flag pins the namespace", args: []string{"-workload-namespace=ys-cust-hosted-demo"}, want: "ys-cust-hosted-demo"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseFlags(append([]string{"-token=t", "-cluster-id=c"}, tt.args...))
			if err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			if got := opts.workloadNamespace; got != tt.want {
				t.Fatalf("workloadNamespace = %q, want %q", got, tt.want)
			}
		})
	}
}

// What this connector tells central it can do, and every way it can fail to be
// true. Central prices capacity against this claim: believing it, it admits a
// nodeOnly burst with no deadline and bounds it on the SILENCE of the
// observations instead. So the claim has to cover the whole chain that produces
// one — the RBAC, the scope, the watcher that sends it, and the interval that
// lets it — because a connector that is equipped but switched off holds that
// ceiling open forever on a burst nothing will ever end.
func TestAuthoritativeOccupancyNeedsEveryConditionThatProducesAnObservation(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want bool
	}{
		{name: "shipped default claims nothing", want: false},
		{
			name: "the grant alone, unscoped, is the claim",
			args: []string{"-authoritative-pod-visibility=true"},
			want: true,
		},
		{
			name: "the grant pinned to one namespace is not the claim",
			args: []string{"-authoritative-pod-visibility=true", "-workload-namespace=team-a"},
			want: false,
		},
		{
			name: "unscoped without the grant is not the claim",
			args: []string{"-authoritative-pod-visibility=false"},
			want: false,
		},
		{
			// The watcher is the only sender of either signal, and a zero grace
			// means startBackgroundServices never starts it. Equipped and silent is
			// the worst thing to claim: nothing ever reports, nothing ever expires.
			name: "a disabled idle grace is not the claim",
			args: []string{"-authoritative-pod-visibility=true", "-idle-node-grace=0"},
			want: false,
		},
		{
			name: "a negative idle grace is not the claim",
			args: []string{"-authoritative-pod-visibility=true", "-idle-node-grace=-1m"},
			want: false,
		},
		{
			// The watcher runs here and can still ask for a teardown — but
			// occupancyDue refuses every observation, so central's silence ceiling
			// has nothing to measure from and never expires the burst.
			name: "a disabled observation interval is not the claim",
			args: []string{"-authoritative-pod-visibility=true", "-occupancy-observe-interval=0"},
			want: false,
		},
		{
			name: "a negative observation interval is not the claim",
			args: []string{"-authoritative-pod-visibility=true", "-occupancy-observe-interval=-1m"},
			want: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseFlags(append([]string{"-token=t", "-cluster-id=c"}, tt.args...))
			if err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			if got := opts.authoritativeOccupancy(); got != tt.want {
				t.Fatalf("authoritativeOccupancy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSplitGatewayRoutes(t *testing.T) {
	got := splitGatewayRoutes(" 10.98.0.0/16,10.99.0.0/16\n10.100.0.0/16,, ")
	want := []string{"10.98.0.0/16", "10.99.0.0/16", "10.100.0.0/16"}
	if len(got) != len(want) {
		t.Fatalf("splitGatewayRoutes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("splitGatewayRoutes = %v, want %v", got, want)
		}
	}
}

func TestPublishGatewayRoutesWritesOverrideConfigMap(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "yscale")
	kube := fake.NewSimpleClientset()
	opts := &runOptions{
		gatewayRoutes: "10.98.0.0/16,10.99.0.0/16",
	}

	publishGatewayRoutes(context.Background(), kube, nil, opts, nil, discardLogger())

	cm, err := kube.CoreV1().ConfigMaps("yscale").Get(context.Background(), gatewayRoutesConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	if got, want := cm.Data[gatewayRoutesConfigMapKey], "10.98.0.0/16\n10.99.0.0/16"; got != want {
		t.Fatalf("routes = %q, want %q", got, want)
	}
}

// captureReporter records the routes handed to ReportClusterRoutes so a test
// can compare them byte-for-byte against the ConfigMap value.
type captureReporter struct {
	called bool
	routes []string
}

func (c *captureReporter) ReportClusterRoutes(routes []string) {
	c.called = true
	c.routes = append([]string(nil), routes...)
}

// Acceptance #9: the value ReportClusterRoutes sends equals the value written
// to the yscale-gateway-routes ConfigMap, byte-for-byte, including for an
// override-configured set (both masked from the SAME slice).
func TestPublishGatewayRoutesReportEqualsConfigMapOverrideMasked(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "yscale")
	kube := fake.NewSimpleClientset()
	// Deliberately UNMASKED, out-of-order overrides: the override path must be
	// masked + canonicalized just like the detected paths.
	opts := &runOptions{
		gatewayRoutes: "10.43.5.7/16,10.42.0.1/16",
	}

	rep := &captureReporter{}
	publishGatewayRoutes(context.Background(), kube, nil, opts, rep, discardLogger())

	cm, err := kube.CoreV1().ConfigMaps("yscale").Get(context.Background(), gatewayRoutesConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	cmValue := cm.Data[gatewayRoutesConfigMapKey]

	// Masked + sorted canonical form.
	if want := "10.42.0.0/16\n10.43.0.0/16"; cmValue != want {
		t.Fatalf("ConfigMap value = %q, want masked %q", cmValue, want)
	}
	if !rep.called {
		t.Fatalf("ReportClusterRoutes was not called after ConfigMap upsert")
	}
	if reported := strings.Join(rep.routes, "\n"); reported != cmValue {
		t.Fatalf("reported %q != ConfigMap %q (must be byte-for-byte equal)", reported, cmValue)
	}
}

func TestPublishGatewayRoutesLeavesConfigMapOnResolveError(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "yscale")
	kube := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gatewayRoutesConfigMapName,
			Namespace: "yscale",
		},
		Data: map[string]string{
			gatewayRoutesConfigMapKey: "keep",
		},
	})
	opts := &runOptions{
		cloudProvider: "unknown",
	}

	publishGatewayRoutes(context.Background(), kube, nil, opts, nil, discardLogger())

	cm, err := kube.CoreV1().ConfigMaps("yscale").Get(context.Background(), gatewayRoutesConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	if got := cm.Data[gatewayRoutesConfigMapKey]; got != "keep" {
		t.Fatalf("routes changed on resolve error: %q", got)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestSplitCommaListDeduplicatesAndTrimsNamespaces(t *testing.T) {
	got := splitCommaList(" default,team-a\nteam-a, team-b ")
	want := []string{"default", "team-a", "team-b"}
	if len(got) != len(want) {
		t.Fatalf("split list = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("split list = %q, want %q", got, want)
		}
	}
}

// A bootstrap listener that can't bind takes the agent with it. Nothing
// else in the process notices the endpoint is gone, so a connector that
// stayed up would keep accepting work it can never bring up.
func TestStartBackgroundServicesFatalOnBootstrapListenFailure(t *testing.T) {
	boot := &agent.BootstrapServer{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	opts := &runOptions{bootstrapListenAddr: "not-a-host-port"}

	ctx, fail := context.WithCancelCause(context.Background())
	defer fail(nil)
	startBackgroundServices(ctx, fail, opts, nil, nil, boot, nil, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("bootstrap listener failed but the agent kept running")
	}
	cause := context.Cause(ctx)
	if cause == nil || !strings.Contains(cause.Error(), "bootstrap server") {
		t.Fatalf("cancel cause = %v, want the bootstrap-server failure", cause)
	}
}
