package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func ifaces(entries ...hostIface) ifaceLister {
	return func() ([]hostIface, error) { return entries, nil }
}

func addrs(ss ...string) []netip.Addr {
	out := make([]netip.Addr, 0, len(ss))
	for _, s := range ss {
		out = append(out, netip.MustParseAddr(s))
	}
	return out
}

// podOnly is what the connector sees in the shipped chart: a pod IP and
// loopback, and — because tailscaled runs userspace — no tailnet address
// and no tailscale interface at all.
var podOnly = ifaces(
	hostIface{Name: "lo", Addrs: addrs("127.0.0.1", "::1")},
	hostIface{Name: "eth0", Addrs: addrs("10.42.3.7")},
)

// kernelMode is a tailscaled that owns a TUN and has been given its
// tailnet address.
var kernelMode = ifaces(
	hostIface{Name: "lo", Addrs: addrs("127.0.0.1")},
	hostIface{Name: "eth0", Addrs: addrs("10.42.3.7")},
	hostIface{Name: "tailscale0", Addrs: addrs("100.83.12.4", "fd7a:115c:a1e0::1")},
)

func TestResolveListenAddrPicksTailnetIPv4(t *testing.T) {
	for _, spec := range []string{":8080", "tailnet:8080"} {
		got, err := resolveListenAddr(spec, kernelMode)
		if err != nil {
			t.Fatalf("%s: %v", spec, err)
		}
		if got != "100.83.12.4:8080" {
			t.Errorf("%s resolved to %q, want the tailnet IPv4", spec, got)
		}
	}
}

// Some managed Kubernetes distributions hand pods addresses out of
// 100.64.0.0/10. A CGNAT address alone must not be mistaken for the
// tailnet, or the bind lands back on the pod network.
func TestResolveListenAddrIgnoresCGNATPodIP(t *testing.T) {
	cgnatPod := ifaces(hostIface{Name: "eth0", Addrs: addrs("100.72.4.9")})

	got, err := resolveListenAddr(":8080", cgnatPod)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "127.0.0.1:8080" {
		t.Errorf("addr = %q, want loopback rather than the CGNAT pod IP", got)
	}
	if _, err := resolveListenAddr("tailnet:8080", cgnatPod); !errors.Is(err, errNoTailnetAddr) {
		t.Errorf("tailnet:8080 err = %v, want errNoTailnetAddr", err)
	}
}

// The default spelling never widens the bind. With no tailnet address
// it settles on loopback — where the chart's `tailscale serve` forwards
// inbound tailnet TCP — and never on the pod IP or a wildcard.
func TestResolveListenAddrDefaultsToLoopbackNotWildcard(t *testing.T) {
	got, err := resolveListenAddr(":8080", podOnly)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "127.0.0.1:8080" {
		t.Fatalf("addr = %q, want 127.0.0.1:8080", got)
	}
	if isWildcardListen(got) {
		t.Error("default spelling produced a wildcard bind")
	}
}

// `tailnet:<port>` is an operator naming the tailnet address. There is
// nothing to fall back to, so it fails closed with an actionable error.
func TestResolveListenAddrTailnetSpecFailsClosed(t *testing.T) {
	_, err := resolveListenAddr("tailnet:8080", podOnly)
	if !errors.Is(err, errNoTailnetAddr) {
		t.Fatalf("err = %v, want errNoTailnetAddr", err)
	}
	if !strings.Contains(err.Error(), "-bootstrap-listen=:<port>") {
		t.Errorf("error should tell the operator what to set, got %q", err)
	}
}

// An explicit host is the operator's/test's call and binds verbatim —
// including loopback, which is what the shipped chart uses because its
// tailscale sidecar forwards inbound tailnet TCP there.
func TestResolveListenAddrHonorsExplicitHost(t *testing.T) {
	for _, spec := range []string{"127.0.0.1:8080", "100.83.12.4:9000", "0.0.0.0:8080"} {
		got, err := resolveListenAddr(spec, podOnly)
		if err != nil {
			t.Fatalf("%s: %v", spec, err)
		}
		if got != spec {
			t.Errorf("%s resolved to %q, want it verbatim", spec, got)
		}
	}
}

func TestResolveListenAddrRejectsMalformedSpec(t *testing.T) {
	for _, spec := range []string{"", "8080", "127.0.0.1"} {
		if _, err := resolveListenAddr(spec, podOnly); err == nil {
			t.Errorf("%q: expected an error", spec)
		}
	}
}

func TestIsWildcardListen(t *testing.T) {
	cases := map[string]bool{
		"0.0.0.0:8080":      true,
		"[::]:8080":         true,
		"127.0.0.1:8080":    false,
		"100.83.12.4:8080":  false,
		"yscale-agent:8080": false,
	}
	for addr, want := range cases {
		if got := isWildcardListen(addr); got != want {
			t.Errorf("isWildcardListen(%q) = %v, want %v", addr, got, want)
		}
	}
}

// A kernel-mode tailscaled can have made its TUN before it has an
// address. Settling on loopback there would strand a pod whose serve
// config doesn't forward, so the wait outlasts the gap.
func TestWaitForListenAddrWaitsForTailscaleToGetItsAddress(t *testing.T) {
	calls := 0
	lookup := func() ([]hostIface, error) {
		calls++
		if calls < 3 {
			return []hostIface{{Name: "tailscale0"}}, nil
		}
		return kernelMode()
	}
	got, err := waitForListenAddr(context.Background(), ":8080", lookup, time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got != "100.83.12.4:8080" {
		t.Errorf("addr = %q, want the tailnet IPv4 once tailscaled came up", got)
	}
}

// Userspace tailscaled never grows an interface, so there is nothing to
// wait for: the default spelling settles on loopback immediately rather
// than stalling the connector's only listener for the wait window.
func TestWaitForListenAddrDoesNotStallOnUserspaceTailscaled(t *testing.T) {
	start := time.Now()
	got, err := waitForListenAddr(context.Background(), ":8080", podOnly, time.Minute, time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got != "127.0.0.1:8080" {
		t.Errorf("addr = %q, want loopback", got)
	}
	if time.Since(start) > 5*time.Second {
		t.Error("waited for an address that was never coming")
	}
}

// A tailscaled that comes up and stays address-less gives up closed
// under the explicit tailnet spelling.
func TestWaitForListenAddrGivesUpClosedOnTailnetSpec(t *testing.T) {
	stuck := ifaces(hostIface{Name: "tailscale0"})
	_, err := waitForListenAddr(context.Background(), "tailnet:8080", stuck, 10*time.Millisecond, time.Millisecond)
	if !errors.Is(err, errNoTailnetAddr) {
		t.Fatalf("err = %v, want errNoTailnetAddr", err)
	}
}

// A malformed spec is fatal immediately; waiting on it would only
// delay a misconfiguration that will never resolve itself.
func TestWaitForListenAddrDoesNotRetryBadSpec(t *testing.T) {
	start := time.Now()
	if _, err := waitForListenAddr(context.Background(), "8080", podOnly, time.Minute, time.Second); err == nil {
		t.Fatal("expected an error")
	}
	if time.Since(start) > 5*time.Second {
		t.Error("waited on an unparseable spec instead of failing straight away")
	}
}

// Listen must not start a server it can't prove is tailnet-only.
func TestListenFailsClosedWithoutTailnetAddress(t *testing.T) {
	s := &BootstrapServer{
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		ifaceLookup: ifaces(hostIface{Name: "tailscale0"}),
		tailnetWait: 10 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.Listen(ctx, "tailnet:8080")
	if !errors.Is(err, errNoTailnetAddr) {
		t.Fatalf("err = %v, want errNoTailnetAddr", err)
	}
}

// The explicit override still works end-to-end: Listen binds the host
// it was given and serves there.
func TestListenHonorsExplicitOverride(t *testing.T) {
	s := &BootstrapServer{
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		ifaceLookup: podOnly,
		grants:      map[string]*burstGrant{},
		nodes:       map[string]string{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- s.Listen(ctx, "127.0.0.1:18080") }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.Dial("tcp", "127.0.0.1:18080")
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never came up on the explicit address: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Errorf("Listen returned %v, want context.Canceled", err)
	}
}

// The upgrade case: a manifest rendered by an older chart passes a bare
// :port. The new binary must still serve it — on loopback, where the
// chart's `tailscale serve` forwards — rather than refusing to listen.
func TestListenBarePortKeepsServingOnLoopback(t *testing.T) {
	s := &BootstrapServer{
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		ifaceLookup: podOnly,
		tailnetWait: 10 * time.Millisecond,
		grants:      map[string]*burstGrant{},
		nodes:       map[string]string{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- s.Listen(ctx, ":18081") }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.Dial("tcp", "127.0.0.1:18081")
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("bare :port lost its listener on upgrade: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Errorf("Listen returned %v, want context.Canceled", err)
	}
}
