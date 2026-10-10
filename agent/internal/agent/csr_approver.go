package agent

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	certv1 "k8s.io/api/certificates/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"github.com/yscale-sh/yscale/pkg/backends"
)

// CSRApprover watches kubelet bootstrap CSRs and auto-approves those
// from the yscale bursts bootstrap group.
//
// Two CSR types matter:
//
//  1. The first CSR a kubelet files on bootstrap, signed by
//     `kubernetes.io/kube-apiserver-client-kubelet`, which gets it
//     a long-lived client cert. The bootstrap token authenticates
//     the request; the requesting username is
//     `system:bootstrap:<token-id>` and the group is
//     `system:bootstrappers:yscale:bursts`.
//
//  2. The serving-cert CSR a kubelet files when
//     --rotate-server-certificates is enabled, signed by
//     `kubernetes.io/kubelet-serving`. We approve those for the
//     same set of nodes once the first cert is in place.
//
// Both require the requested CN to match our burst-node prefix
// (default "system:node:ys-burst-"), but the group check differs by
// signer — see eligible().
//
// Approval is latency-critical: the kubelet cannot register until its
// client CSR is approved, and apiserver→kubelet (logs/exec) waits on
// the serving CSR — so every second a CSR sits pending is burst boot
// time. A raw watch nudges an immediate evaluation when a CSR appears;
// the poll ticker stays as the fallback/resync so a dropped watch only
// degrades latency, never correctness. No informer state needed.
type CSRApprover struct {
	K8s          kubernetes.Interface
	Log          *slog.Logger
	NodePrefix   string        // "system:node:ys-burst-"
	Group        string        // "system:bootstrappers:yscale:bursts"
	Interval     time.Duration // 0 → 5s default
	GKEMode      bool          // true when bootstrap-auth-mode=gke
	GKENamespace string        // connector's release namespace for GKE SA usernames
}

func NewCSRApprover(k8s kubernetes.Interface, log *slog.Logger) *CSRApprover {
	if log == nil {
		log = slog.Default()
	}
	return &CSRApprover{
		K8s:        k8s,
		Log:        log,
		NodePrefix: "system:node:" + backends.NodeNamePrefix,
		Group:      burstsBootstrapGroup,
		Interval:   5 * time.Second,
	}
}

// Run evaluates pending CSRs until ctx is canceled: immediately when
// the CSR watch reports activity, and every Interval as a fallback.
// Each evaluation lists pending CSRs and approves the eligible ones.
func (a *CSRApprover) Run(ctx context.Context) error {
	tick := a.Interval
	if tick <= 0 {
		tick = 5 * time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()

	// Buffered depth-1: coalesces bursts of events into one pending
	// nudge, and the watcher never blocks sending.
	nudge := make(chan struct{}, 1)
	go a.watchCSRs(ctx, nudge)

	a.Log.Info("csr approver running", "node_prefix", a.NodePrefix, "group", a.Group)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		case <-nudge:
		}
		if err := a.tick(ctx); err != nil {
			a.Log.Warn("csr approver tick failed", "error", err)
		}
	}
}

// watchCSRs holds a watch on CSRs and nudges the Run loop whenever one
// is added or updated, so a booting kubelet's CSR is approved in
// milliseconds instead of waiting out the poll interval. Re-establishes
// the watch when the server closes it; on watch errors it backs off and
// retries, with the Run loop's ticker covering the gap.
func (a *CSRApprover) watchCSRs(ctx context.Context, nudge chan<- struct{}) {
	for {
		w, err := a.K8s.CertificatesV1().CertificateSigningRequests().Watch(ctx, metav1.ListOptions{})
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}
		for ev := range w.ResultChan() {
			if ev.Type != watch.Added && ev.Type != watch.Modified {
				continue
			}
			select {
			case nudge <- struct{}{}:
			default:
			}
		}
		w.Stop()
		if ctx.Err() != nil {
			return
		}
	}
}

func (a *CSRApprover) tick(ctx context.Context) error {
	list, err := a.K8s.CertificatesV1().CertificateSigningRequests().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range list.Items {
		csr := &list.Items[i]
		if !a.eligible(csr) {
			continue
		}
		if alreadyDecided(csr) {
			continue
		}
		if err := a.approve(ctx, csr); err != nil {
			a.Log.Warn("approve failed", "csr", csr.Name, "error", err)
			continue
		}
		a.Log.Info("approved csr",
			"csr", csr.Name,
			"signer", csr.Spec.SignerName,
			"username", csr.Spec.Username)
	}
	return nil
}

// nodesGroup is the group every kubelet authenticates as once it
// holds a node client cert. Serving CSRs are filed under this group,
// not the bootstrap group.
const nodesGroup = "system:nodes"

// burstsBootstrapGroup is the shared extra group every burst's bootstrap
// token carries — the coarse "this is a yscale burst" gate.
const burstsBootstrapGroup = "system:bootstrappers:yscale:bursts"

// nodeBindGroupPrefix prefixes the PER-NODE extra group the agent mints
// into each burst's bootstrap token (auth-extra-groups). The full group is
// nodeBindGroupPrefix+<nodeName>. Because a bootstrap token's extra groups
// surface in the client CSR's Spec.Groups, the approver can require the
// requested node CN to carry its own bind group — so a burst's token can
// only ever earn a client cert for ITS OWN node, never a sibling's. Must
// start with "system:bootstrappers:" to be a valid bootstrap extra group.
const nodeBindGroupPrefix = "system:bootstrappers:yscale:nodes:"

// eligible decides whether a CSR is one yscale should auto-approve.
// We are intentionally conservative: only the two specific signer
// names, the requested CN must match our burst-node prefix, and the
// group must match the one expected for that signer.
//
// The group differs by signer because the two CSRs are filed with
// different credentials:
//   - The bootstrap client CSR is authenticated by the burst's
//     bootstrap token, so the requester is in the yscale bursts group.
//   - The serving CSR is filed later by the kubelet using the node
//     client cert it just earned, so the requester is
//     system:node:<name> in group system:nodes — NOT the bootstrap
//     group. We additionally require username == CN so a node can
//     only request a serving cert for its own identity.
func (a *CSRApprover) eligible(csr *certv1.CertificateSigningRequest) bool {
	parsed, err := parseCSRPEM(csr.Spec.Request)
	if err != nil {
		return false
	}
	if !strings.HasPrefix(parsed.Subject.CommonName, a.NodePrefix) {
		return false
	}

	switch csr.Spec.SignerName {
	case certv1.KubeAPIServerClientKubeletSignerName:
		nodeName := strings.TrimPrefix(parsed.Subject.CommonName, "system:node:")

		// GKE mode: the kubelet authenticates as the per-node SA
		// (system:serviceaccount:<namespace>:yscale-burst-bootstrap-<node>).
		// The username is deterministically derived from the node name, so
		// it binds the CSR to exactly this node with no shared SA.
		// Only checked when GKE mode is explicitly configured.
		if a.GKEMode && isGKEBootstrapUsername(csr.Spec.Username, a.GKENamespace) {
			return gkeUsernameMatchesNode(csr.Spec.Username, nodeName, a.GKENamespace)
		}

		// Standard bootstrap-token mode: coarse gate on bursts group +
		// per-node bind group.
		if !slices.Contains(csr.Spec.Groups, a.Group) {
			return false
		}
		return slices.Contains(csr.Spec.Groups, nodeBindGroupPrefix+nodeName)
	case certv1.KubeletServingSignerName:
		return slices.Contains(csr.Spec.Groups, nodesGroup) &&
			csr.Spec.Username == parsed.Subject.CommonName
	default:
		return false
	}
}

func (a *CSRApprover) approve(ctx context.Context, csr *certv1.CertificateSigningRequest) error {
	csr.Status.Conditions = append(csr.Status.Conditions, certv1.CertificateSigningRequestCondition{
		Type:           certv1.CertificateApproved,
		Status:         "True",
		Reason:         "YScaleAutoApproved",
		Message:        "Approved by yscale-agent CSR approver (yscale bursts bootstrap group)",
		LastUpdateTime: metav1.Now(),
	})
	_, err := a.K8s.CertificatesV1().CertificateSigningRequests().UpdateApproval(ctx, csr.Name, csr, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		// Someone else (the cluster's own approver, perhaps) raced us
		// to it. Not an error — the CSR is decided.
		return nil
	}
	return err
}

func alreadyDecided(csr *certv1.CertificateSigningRequest) bool {
	for _, c := range csr.Status.Conditions {
		switch c.Type {
		case certv1.CertificateApproved, certv1.CertificateDenied:
			return true
		}
	}
	return false
}

func parseCSRPEM(raw []byte) (*x509.CertificateRequest, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("no PEM block in CSR")
	}
	return x509.ParseCertificateRequest(block.Bytes)
}
