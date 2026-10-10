package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"
)

// tailnetPrefix4 is the CGNAT range every Tailscale device's IPv4 is
// drawn from. An address outside it is never a tailnet address.
var tailnetPrefix4 = netip.MustParsePrefix("100.64.0.0/10")

// tailscaleIfacePrefixes are the interface names tailscaled gives its
// TUN device. The name is checked as well as the CGNAT range because
// some managed Kubernetes distributions allocate pod IPs out of
// 100.64.0.0/10 too — binding one of those would put the connector
// straight back on the customer's pod network.
var tailscaleIfacePrefixes = []string{"tailscale", "ts"}

// listenHostTailnet is the sentinel host that demands the tailnet
// address specifically: `-bootstrap-listen=tailnet:8080`. It is not a
// hostname the resolver ever tries to look up.
const listenHostTailnet = "tailnet"

// loopbackHost is where the connector binds when it has no tailnet
// address of its own. Inbound tailnet TCP still arrives — `tailscale
// serve` forwards it here from inside the pod — and nothing on the
// customer's pod network can dial it.
const loopbackHost = "127.0.0.1"

// tailnetWaitTimeout bounds how long Listen waits on tailscaled before
// settling. The tailscale sidecar is an ordinary container, not an init
// container, so on a cold pod start the connector reaches Listen before
// tailscaled has made its TUN.
const tailnetWaitTimeout = 90 * time.Second

// tailnetPollInterval is how often the wait re-enumerates interfaces.
const tailnetPollInterval = 2 * time.Second

// errNoTailnetAddr is what `tailnet:<port>` fails with once the wait
// window expires. That spelling is an operator asking for the tailnet
// address by name, so there is nothing to fall back to: the one
// fallback that would "just work" — all interfaces — is the exposure
// this resolution exists to remove.
var errNoTailnetAddr = errors.New(
	"no tailscale interface carrying a 100.64.0.0/10 address, and -bootstrap-listen names the tailnet explicitly. " +
		"With TS_USERSPACE=true (the shipped chart) tailscaled holds no interface address and inbound tailnet " +
		"traffic arrives on loopback via `tailscale serve` — use -bootstrap-listen=:<port> " +
		"(Helm: bootstrap.listenMode=loopback). Otherwise check that the tailscale sidecar came up")

// errTailnetPending means tailscaled is here but has not been given its
// address yet, so the answer may still change. Never returned to a
// caller of Listen — waitForListenAddr either outlasts it or settles.
var errTailnetPending = errors.New("tailscale interface has no 100.64.0.0/10 address yet")

// hostIface is the slice of a network interface the listen resolver
// reads. Declared separately from net.Interface so tests can hand the
// resolver a synthetic interface list.
type hostIface struct {
	Name  string
	Addrs []netip.Addr
}

// ifaceLister enumerates the machine's interfaces.
type ifaceLister func() ([]hostIface, error)

// hostInterfaces reports this machine's interfaces and their unicast
// IPs. The agent shares its Pod's network namespace with the tailscale
// sidecar, so a kernel-mode tailscaled's tailscale0 shows up here.
func hostInterfaces() ([]hostIface, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	out := make([]hostIface, 0, len(ifaces))
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue // interface disappeared mid-enumeration
		}
		hi := hostIface{Name: iface.Name}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			if ip, ok := netip.AddrFromSlice(ipNet.IP); ok {
				hi.Addrs = append(hi.Addrs, ip.Unmap())
			}
		}
		out = append(out, hi)
	}
	return out, nil
}

// tailnetIPv4 picks the tailnet IPv4 out of ifaces: a 100.64.0.0/10
// address on a tailscaled TUN. Reports false when tailscaled runs in
// userspace mode (no TUN, no address) or hasn't come up yet.
func tailnetIPv4(ifaces []hostIface) (netip.Addr, bool) {
	for _, iface := range ifaces {
		if !isTailscaleIface(iface.Name) {
			continue
		}
		for _, addr := range iface.Addrs {
			if addr.Is4() && tailnetPrefix4.Contains(addr) {
				return addr, true
			}
		}
	}
	return netip.Addr{}, false
}

// tailscaleIfacePresent reports whether a tailscaled TUN exists at all.
// It separates "userspace tailscaled, there will never be an address"
// from "kernel-mode tailscaled, mid-startup" — the first settles now,
// the second is worth waiting on.
func tailscaleIfacePresent(ifaces []hostIface) bool {
	for _, iface := range ifaces {
		if isTailscaleIface(iface.Name) {
			return true
		}
	}
	return false
}

func isTailscaleIface(name string) bool {
	for _, p := range tailscaleIfacePrefixes {
		if startsWith(name, p) {
			return true
		}
	}
	return false
}

// resolveListenAddr turns a -bootstrap-listen value into the host:port
// the connector's HTTP server binds. Three spellings, three meanings:
//
//   - `host:port` — bind that host verbatim. The override tests and
//     unusual operators rely on, and the only way to ask for a wildcard.
//   - `tailnet:port` — bind tailscaled's own 100.64.0.0/10 IPv4 and
//     nothing else. Fails closed when there isn't one.
//   - `:port` (the flag default) — bind the tailnet IPv4 when tailscaled
//     has one, loopback otherwise. Never all interfaces. Loopback is
//     the correct tailnet-only address under a userspace tailscaled,
//     where `tailscale serve` forwards inbound tailnet TCP there.
//
// errTailnetPending means tailscaled is up but address-less; the caller
// may retry. Every other error is final.
func resolveListenAddr(spec string, lookup ifaceLister) (string, error) {
	host, port, err := net.SplitHostPort(spec)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", spec, err)
	}
	if port == "" {
		return "", fmt.Errorf("parse %q: port required", spec)
	}
	if host != "" && host != listenHostTailnet {
		return spec, nil
	}
	ifaces, err := lookup()
	if err != nil {
		return "", fmt.Errorf("enumerate interfaces: %w", err)
	}
	if ip, ok := tailnetIPv4(ifaces); ok {
		return net.JoinHostPort(ip.String(), port), nil
	}
	if tailscaleIfacePresent(ifaces) {
		return "", errTailnetPending
	}
	return settleListenAddr(host, port)
}

// settleListenAddr is what a spec resolves to once waiting is over.
// Loopback for the default spelling, a hard error for `tailnet:port`.
func settleListenAddr(host, port string) (string, error) {
	if host == listenHostTailnet {
		return "", errNoTailnetAddr
	}
	return net.JoinHostPort(loopbackHost, port), nil
}

// waitForListenAddr resolves spec, retrying while tailscaled is up but
// has not been handed its address yet. Every other failure — an
// unparseable spec, an interface lookup error — returns immediately;
// retrying those would just delay a fatal misconfiguration.
func waitForListenAddr(ctx context.Context, spec string, lookup ifaceLister, timeout, poll time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		addr, err := resolveListenAddr(spec, lookup)
		if !errors.Is(err, errTailnetPending) {
			return addr, err
		}
		if !time.Now().Before(deadline) {
			host, port, splitErr := net.SplitHostPort(spec)
			if splitErr != nil {
				return "", err
			}
			return settleListenAddr(host, port)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(poll):
		}
	}
}

// fellBackToLoopback reports whether the default ":port" spelling
// settled on loopback because this pod has no tailnet address of its
// own. Worth saying out loud: reachability then rests on `tailscale
// serve` forwarding into the pod, not on the bind alone.
func fellBackToLoopback(spec, addr string) bool {
	specHost, _, err := net.SplitHostPort(spec)
	if err != nil || specHost != "" {
		return false
	}
	host, _, err := net.SplitHostPort(addr)
	return err == nil && host == loopbackHost
}

// isWildcardListen reports whether addr binds every interface, which
// on a Pod includes the cluster pod IP. Only reachable by an explicit
// operator override; Listen logs it loudly.
func isWildcardListen(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return ip.IsUnspecified()
}
