package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"

	"github.com/yscale-sh/yscale/internal/evidence"
)

const (
	accessProbeImage = "busybox:1.36.1@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
	accessProbePort  = 8080

	accessProbeLogMarker  = "yscaletest-access-log-ok"
	accessProbeExecToken  = "yscaletest-access-exec-ok"
	accessProbeHTTPMarker = "yscaletest-access-http-ok"

	defaultAccessProbeTimeout       = 90 * time.Second
	defaultAccessProbeReadyTimeout  = 45 * time.Second
	defaultAccessProbeOperationTime = 15 * time.Second
	defaultAccessProbeDeleteTimeout = 15 * time.Second
	defaultAccessProbePoll          = 500 * time.Millisecond
)

const labelAccessProbe = "yscale.sh/yscaletest-access-probe"

// AccessProbeRequest names only the exact run/case/node the prober may touch.
// The expected node is derived from the case's burstID by the Runner.
type AccessProbeRequest struct {
	Namespace    string
	RunID        string
	CaseName     string
	ExpectedNode string
}

// AccessProber is the narrow injectable seam for the paid exact-node access
// proof. A failure may return a bounded partial receipt alongside the error.
type AccessProber interface {
	Probe(context.Context, AccessProbeRequest) (*evidence.AccessProbe, error)
}

func shouldProbeAccess(evidenceEnabled bool, gpuCount int64) bool {
	return evidenceEnabled && gpuCount > 0
}

// AccessProbeError carries a closed stage identifier for artifact diagnostics
// while leaving the underlying operational error available only to the live
// CaseResult/report.
type AccessProbeError struct {
	Stage string
	Err   error
}

func (e *AccessProbeError) Error() string {
	if e == nil {
		return "access probe failed"
	}
	return fmt.Sprintf("%s: %v", e.Stage, e.Err)
}

func (e *AccessProbeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func accessProbeError(stage string, err error) error {
	return &AccessProbeError{Stage: stage, Err: err}
}

func accessProbeFailureStage(err error) string {
	var probeErr *AccessProbeError
	if errors.As(err, &probeErr) {
		switch probeErr.Stage {
		case "setup", "node", "create", "ready", "logs", "exec", "port_forward", "cleanup":
			return "access_probe." + probeErr.Stage
		}
	}
	return "access_probe.probe"
}

// maybeProbeAccess runs at most once per case, after checkNodeReadyDeadline has
// observed the exact burst node Ready and before waitForTerminal can return a
// terminal phase to the cleanup/reap path.
func (r *Runner) maybeProbeAccess(ctx context.Context, burstID string) error {
	if !r.accessProbeRequired || r.accessProbeAttempted || r.nodeReadyAt == nil {
		return nil
	}
	r.accessProbeAttempted = true

	expectedNode := burstNodeExpectedName(burstID)
	if expectedNode == "" {
		err := accessProbeError("setup", errors.New("burstID did not derive an exact node name"))
		r.accessProbeFailureStage = accessProbeFailureStage(err)
		return err
	}
	if r.AccessProber == nil {
		err := accessProbeError("setup", errors.New("access prober is not configured"))
		r.accessProbeFailureStage = accessProbeFailureStage(err)
		return err
	}

	receipt, err := r.AccessProber.Probe(ctx, AccessProbeRequest{
		Namespace:    r.Namespace,
		RunID:        r.RunID,
		CaseName:     r.currentCase,
		ExpectedNode: expectedNode,
	})
	if receipt != nil {
		clone := *receipt
		r.accessProbe = &clone
	}
	if err != nil {
		r.accessProbeFailureStage = accessProbeFailureStage(err)
		return err
	}
	if receipt == nil {
		err = accessProbeError("setup", errors.New("access prober returned no receipt"))
	} else if receipt.NodeName != expectedNode {
		err = accessProbeError("node", fmt.Errorf("observed node %q, want %q", receipt.NodeName, expectedNode))
	} else if receipt.ObservedAt.IsZero() || receipt.ObservedAt.Location() != time.UTC {
		err = accessProbeError("node", errors.New("observation timestamp is missing or not UTC"))
	} else if !receipt.Logs {
		err = accessProbeError("logs", errors.New("logs marker was not proven"))
	} else if !receipt.Exec {
		err = accessProbeError("exec", errors.New("exec token was not proven"))
	} else if !receipt.PortForward {
		err = accessProbeError("port_forward", errors.New("port-forward response was not proven"))
	}
	if err != nil {
		r.accessProbeFailureStage = accessProbeFailureStage(err)
	}
	return err
}

// KubernetesAccessProber exercises the real client-go pod logs and exec
// subresources plus the SPDY port-forward tunnel. Function fields are test-only
// overrides; nil selects the production implementation.
type KubernetesAccessProber struct {
	K8s        kubernetes.Interface
	RESTConfig *rest.Config

	ReadLogs    func(context.Context, string, string) ([]byte, error)
	Exec        func(context.Context, string, string) ([]byte, error)
	ForwardHTTP func(context.Context, string, string, int) ([]byte, error)

	Timeout       time.Duration
	ReadyTimeout  time.Duration
	OperationTime time.Duration
	DeleteTimeout time.Duration
	PollInterval  time.Duration
	Now           func() time.Time
}

func NewKubernetesAccessProber(k8s kubernetes.Interface, cfg *rest.Config) *KubernetesAccessProber {
	var copied *rest.Config
	if cfg != nil {
		copied = rest.CopyConfig(cfg)
	}
	return &KubernetesAccessProber{K8s: k8s, RESTConfig: copied}
}

func (p *KubernetesAccessProber) timeout() time.Duration {
	return orDuration(p.Timeout, defaultAccessProbeTimeout)
}

func (p *KubernetesAccessProber) readyTimeout() time.Duration {
	return orDuration(p.ReadyTimeout, defaultAccessProbeReadyTimeout)
}

func (p *KubernetesAccessProber) operationTime() time.Duration {
	return orDuration(p.OperationTime, defaultAccessProbeOperationTime)
}

func (p *KubernetesAccessProber) deleteTimeout() time.Duration {
	return orDuration(p.DeleteTimeout, defaultAccessProbeDeleteTimeout)
}

func (p *KubernetesAccessProber) pollInterval() time.Duration {
	return orDuration(p.PollInterval, defaultAccessProbePoll)
}

func (p *KubernetesAccessProber) now() time.Time {
	if p.Now != nil {
		return p.Now().UTC()
	}
	return time.Now().UTC()
}

func (p *KubernetesAccessProber) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, p.operationTime())
}

// Probe creates and later deletes one exact diagnostic Pod. Cleanup uses its
// exact name and a separate bounded context so cancellation cannot strand the
// probe pod; it never lists or sweeps pods.
func (p *KubernetesAccessProber) Probe(ctx context.Context, req AccessProbeRequest) (receipt *evidence.AccessProbe, retErr error) {
	if p == nil || p.K8s == nil || p.RESTConfig == nil {
		return nil, accessProbeError("setup", errors.New("Kubernetes client and REST config are required"))
	}
	if req.Namespace == "" || req.RunID == "" || req.CaseName == "" || req.ExpectedNode == "" {
		return nil, accessProbeError("setup", errors.New("namespace, run, case, and expected node are required"))
	}

	probeCtx, cancel := context.WithTimeout(ctx, p.timeout())
	defer cancel()
	podName := accessProbePodName(req.RunID, req.CaseName)

	opCtx, opCancel := p.operationContext(probeCtx)
	node, err := p.K8s.CoreV1().Nodes().Get(opCtx, req.ExpectedNode, metav1.GetOptions{})
	opCancel()
	if err != nil {
		return nil, accessProbeError("node", err)
	}
	receipt = &evidence.AccessProbe{NodeName: node.Name, ObservedAt: p.now()}
	if node.Name != req.ExpectedNode || !nodeIsReady(node) {
		return receipt, accessProbeError("node", fmt.Errorf("exact node %q is not observed Ready", req.ExpectedNode))
	}

	opCtx, opCancel = p.operationContext(probeCtx)
	created, err := p.K8s.CoreV1().Pods(req.Namespace).Create(opCtx, diagnosticAccessPod(podName, req), metav1.CreateOptions{})
	opCancel()
	if err != nil {
		return receipt, accessProbeError("create", err)
	}
	// Arm cleanup only after this call created the Pod. A create collision
	// must never delete a pre-existing object. When the apiserver assigned a
	// UID, include it as a precondition so an exact-name replacement cannot
	// be deleted either.
	defer func() {
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), p.deleteTimeout())
		defer deleteCancel()
		zero := int64(0)
		deleteOptions := metav1.DeleteOptions{GracePeriodSeconds: &zero}
		if created.UID != "" {
			uid := created.UID
			deleteOptions.Preconditions = &metav1.Preconditions{UID: &uid}
		}
		err := p.K8s.CoreV1().Pods(req.Namespace).Delete(deleteCtx, podName, deleteOptions)
		if err != nil && !apierrors.IsNotFound(err) && retErr == nil {
			retErr = accessProbeError("cleanup", err)
		}
	}()
	if created.Spec.NodeName != req.ExpectedNode {
		receipt.NodeName = created.Spec.NodeName
		return receipt, accessProbeError("node", fmt.Errorf("created pod pinned to %q, want %q", created.Spec.NodeName, req.ExpectedNode))
	}

	var observedPod *corev1.Pod
	err = wait.PollUntilContextTimeout(probeCtx, p.pollInterval(), p.readyTimeout(), true, func(waitCtx context.Context) (bool, error) {
		getCtx, getCancel := p.operationContext(waitCtx)
		defer getCancel()
		pod, getErr := p.K8s.CoreV1().Pods(req.Namespace).Get(getCtx, podName, metav1.GetOptions{})
		if getErr != nil {
			return false, getErr
		}
		observedPod = pod
		if pod.Spec.NodeName != req.ExpectedNode {
			return false, accessProbeError("node", fmt.Errorf("running pod observed on %q, want %q", pod.Spec.NodeName, req.ExpectedNode))
		}
		return pod.Status.Phase == corev1.PodRunning, nil
	})
	if err != nil {
		var probeErr *AccessProbeError
		if errors.As(err, &probeErr) {
			if observedPod != nil {
				receipt.NodeName = observedPod.Spec.NodeName
			}
			return receipt, err
		}
		return receipt, accessProbeError("ready", err)
	}

	opCtx, opCancel = p.operationContext(probeCtx)
	logBody, err := p.readLogs(opCtx, req.Namespace, podName)
	opCancel()
	if err != nil {
		return receipt, accessProbeError("logs", err)
	}
	if !bytes.Contains(logBody, []byte(accessProbeLogMarker)) {
		return receipt, accessProbeError("logs", errors.New("expected marker was absent"))
	}
	receipt.Logs = true

	opCtx, opCancel = p.operationContext(probeCtx)
	execBody, err := p.exec(opCtx, req.Namespace, podName)
	opCancel()
	if err != nil {
		return receipt, accessProbeError("exec", err)
	}
	if strings.TrimSpace(string(execBody)) != accessProbeExecToken {
		return receipt, accessProbeError("exec", errors.New("expected token was absent"))
	}
	receipt.Exec = true

	opCtx, opCancel = p.operationContext(probeCtx)
	httpBody, err := p.forwardHTTP(opCtx, req.Namespace, podName, accessProbePort)
	opCancel()
	if err != nil {
		return receipt, accessProbeError("port_forward", err)
	}
	if strings.TrimSpace(string(httpBody)) != accessProbeHTTPMarker {
		return receipt, accessProbeError("port_forward", errors.New("expected HTTP marker was absent"))
	}
	receipt.PortForward = true
	return receipt, nil
}

func accessProbePodName(runID, caseName string) string {
	sum := sha256.Sum256([]byte(runID + "\x00" + caseName))
	return "yscaletest-access-" + hex.EncodeToString(sum[:8])
}

func diagnosticAccessPod(name string, req AccessProbeRequest) *corev1.Pod {
	uid := int64(65532)
	group := int64(65532)
	activeDeadline := int64(120)
	terminationGrace := int64(0)
	automountToken := false
	nonRoot := true
	noPrivilegeEscalation := false
	readOnlyRoot := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: req.Namespace,
			Labels: map[string]string{
				labelRun:         req.RunID,
				labelCase:        req.CaseName,
				labelAccessProbe: "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:                      req.ExpectedNode,
			RestartPolicy:                 corev1.RestartPolicyNever,
			ActiveDeadlineSeconds:         &activeDeadline,
			TerminationGracePeriodSeconds: &terminationGrace,
			AutomountServiceAccountToken:  &automountToken,
			EnableServiceLinks:            &automountToken,
			Tolerations: []corev1.Toleration{{
				Operator: corev1.TolerationOpExists,
			}},
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: &nonRoot,
				RunAsUser:    &uid,
				RunAsGroup:   &group,
				FSGroup:      &group,
				SeccompProfile: &corev1.SeccompProfile{
					Type: corev1.SeccompProfileTypeRuntimeDefault,
				},
			},
			Containers: []corev1.Container{{
				Name:    "access",
				Image:   accessProbeImage,
				Command: []string{"sh", "-c"},
				Args: []string{fmt.Sprintf(
					"printf '%%s\\n' %s > /www/probe; printf '%%s\\n' %s; exec httpd -f -p %d -h /www",
					accessProbeHTTPMarker, accessProbeLogMarker, accessProbePort,
				)},
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: accessProbePort, Protocol: corev1.ProtocolTCP}},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &noPrivilegeEscalation,
					ReadOnlyRootFilesystem:   &readOnlyRoot,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "www", MountPath: "/www"}},
			}},
			Volumes: []corev1.Volume{{Name: "www", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
		},
	}
}

func (p *KubernetesAccessProber) readLogs(ctx context.Context, namespace, podName string) ([]byte, error) {
	if p.ReadLogs != nil {
		return p.ReadLogs(ctx, namespace, podName)
	}
	limitBytes := int64(4096)
	tailLines := int64(20)
	return p.K8s.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  "access",
		LimitBytes: &limitBytes,
		TailLines:  &tailLines,
	}).DoRaw(ctx)
}

type boundedBuffer struct {
	bytes.Buffer
	limit    int
	overflow bool
}

func (b *boundedBuffer) Write(payload []byte) (int, error) {
	original := len(payload)
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		b.overflow = b.overflow || original > 0
		return original, nil
	}
	if len(payload) > remaining {
		b.overflow = true
		payload = payload[:remaining]
	}
	_, _ = b.Buffer.Write(payload)
	return original, nil
}

func (p *KubernetesAccessProber) exec(ctx context.Context, namespace, podName string) ([]byte, error) {
	if p.Exec != nil {
		return p.Exec(ctx, namespace, podName)
	}
	req := p.K8s.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "access",
			Command:   []string{"sh", "-c", "printf '" + accessProbeExecToken + "\\n'"},
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(p.RESTConfig, http.MethodPost, req.URL())
	if err != nil {
		return nil, err
	}
	stdout := boundedBuffer{limit: 256}
	stderr := boundedBuffer{limit: 256}
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return nil, err
	}
	if stdout.overflow || stderr.overflow {
		return nil, errors.New("exec output exceeded bound")
	}
	return stdout.Bytes(), nil
}

func (p *KubernetesAccessProber) forwardHTTP(ctx context.Context, namespace, podName string, remotePort int) ([]byte, error) {
	if p.ForwardHTTP != nil {
		return p.ForwardHTTP(ctx, namespace, podName, remotePort)
	}
	roundTripper, upgrader, err := spdy.RoundTripperFor(p.RESTConfig)
	if err != nil {
		return nil, err
	}
	serverURL := p.K8s.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("portforward").URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: roundTripper}, http.MethodPost, serverURL)
	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	forwarder, err := portforward.NewOnAddresses(
		dialer,
		[]string{"127.0.0.1"},
		[]string{fmt.Sprintf("0:%d", remotePort)},
		stopCh,
		readyCh,
		io.Discard,
		io.Discard,
	)
	if err != nil {
		return nil, err
	}
	forwardErr := make(chan error, 1)
	go func() { forwardErr <- forwarder.ForwardPorts() }()
	defer close(stopCh)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-forwardErr:
		if err == nil {
			err = errors.New("port-forward stopped before becoming ready")
		}
		return nil, err
	case <-readyCh:
	}
	ports, err := forwarder.GetPorts()
	if err != nil {
		return nil, err
	}
	if len(ports) != 1 || ports[0].Local == 0 {
		return nil, errors.New("port-forward did not allocate one loopback port")
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://127.0.0.1:%d/probe", ports[0].Local), nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true},
		Timeout:   p.operationTime(),
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 257))
	if err != nil {
		return nil, err
	}
	if len(body) > 256 {
		return nil, errors.New("HTTP response exceeded bound")
	}
	return body, nil
}
