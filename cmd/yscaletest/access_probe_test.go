package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

func accessProbeRequestForTest() AccessProbeRequest {
	return AccessProbeRequest{
		Namespace:    "default",
		RunID:        "yt-access-test",
		CaseName:     "linode-gpu-rtx4000",
		ExpectedNode: testBurstNodeName,
	}
}

func readyAccessProbeNode() *corev1.Node {
	node := readyBurstNode()
	node.Name = testBurstNodeName
	return node
}

func TestKubernetesAccessProberBuildsExactPodAndDeletesExactName(t *testing.T) {
	k8s := fake.NewSimpleClientset(readyAccessProbeNode())
	var created *corev1.Pod
	var deletedName string
	var deleteOptions metav1.DeleteOptions
	k8s.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		created = action.(ktesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		created.UID = types.UID("probe-uid")
		created.Status.Phase = corev1.PodRunning
		return true, created.DeepCopy(), nil
	})
	k8s.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, created.DeepCopy(), nil
	})
	k8s.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(ktesting.DeleteAction)
		deletedName = deleteAction.GetName()
		deleteOptions = deleteAction.GetDeleteOptions()
		return true, nil, nil
	})

	prober := &KubernetesAccessProber{
		K8s:        k8s,
		RESTConfig: &rest.Config{Host: "https://127.0.0.1"},
		ReadLogs: func(context.Context, string, string) ([]byte, error) {
			return []byte("prefix\n" + accessProbeLogMarker + "\n"), nil
		},
		Exec: func(context.Context, string, string) ([]byte, error) {
			return []byte(accessProbeExecToken + "\n"), nil
		},
		ForwardHTTP: func(_ context.Context, _, _ string, port int) ([]byte, error) {
			if port != accessProbePort {
				t.Fatalf("remote port = %d, want %d", port, accessProbePort)
			}
			return []byte(accessProbeHTTPMarker + "\n"), nil
		},
		PollInterval:  time.Millisecond,
		ReadyTimeout:  time.Second,
		OperationTime: time.Second,
		DeleteTimeout: time.Second,
		Now: func() time.Time {
			return time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
		},
	}
	receipt, err := prober.Probe(context.Background(), accessProbeRequestForTest())
	if err != nil {
		t.Fatal(err)
	}
	if receipt == nil || receipt.NodeName != testBurstNodeName || !receipt.Logs || !receipt.Exec || !receipt.PortForward || receipt.ObservedAt.IsZero() {
		t.Fatalf("receipt = %+v", receipt)
	}
	wantName := accessProbePodName("yt-access-test", "linode-gpu-rtx4000")
	if created == nil || created.Name != wantName || deletedName != wantName {
		t.Fatalf("created=%v deleted=%q want exact pod %q", created != nil, deletedName, wantName)
	}
	if deleteOptions.Preconditions == nil || deleteOptions.Preconditions.UID == nil || *deleteOptions.Preconditions.UID != created.UID {
		t.Fatalf("delete UID precondition = %+v, want %q", deleteOptions.Preconditions, created.UID)
	}
	if deleteOptions.GracePeriodSeconds == nil || *deleteOptions.GracePeriodSeconds != 0 {
		t.Fatalf("delete grace period = %v, want 0", deleteOptions.GracePeriodSeconds)
	}
	if created.Spec.NodeName != testBurstNodeName {
		t.Fatalf("pod nodeName = %q, want %q", created.Spec.NodeName, testBurstNodeName)
	}
	if created.Labels[labelRun] != "yt-access-test" || created.Labels[labelCase] != "linode-gpu-rtx4000" || created.Labels[labelAccessProbe] != "true" {
		t.Fatalf("pod labels are not scoped to run/case: %+v", created.Labels)
	}
	if len(created.Spec.Tolerations) != 1 || created.Spec.Tolerations[0].Operator != corev1.TolerationOpExists {
		t.Fatalf("pod does not tolerate burst taints: %+v", created.Spec.Tolerations)
	}
	container := created.Spec.Containers[0]
	if container.Image != accessProbeImage || !strings.Contains(strings.Join(container.Args, " "), accessProbeHTTPMarker) || container.Ports[0].ContainerPort != accessProbePort {
		t.Fatalf("diagnostic container is not the pinned HTTP probe: %+v", container)
	}
	if container.SecurityContext == nil || container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("diagnostic container security context is not unprivileged: %+v", container.SecurityContext)
	}
}

func TestKubernetesAccessProberDeletesExactPodOnCancellation(t *testing.T) {
	k8s := fake.NewSimpleClientset(readyAccessProbeNode())
	var created *corev1.Pod
	var deletedName string
	k8s.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		created = action.(ktesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		created.UID = types.UID("cancelled-probe-uid")
		created.Status.Phase = corev1.PodRunning
		return true, created.DeepCopy(), nil
	})
	k8s.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, created.DeepCopy(), nil
	})
	k8s.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		deletedName = action.(ktesting.DeleteAction).GetName()
		return true, nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	prober := &KubernetesAccessProber{
		K8s:        k8s,
		RESTConfig: &rest.Config{Host: "https://127.0.0.1"},
		ReadLogs: func(logCtx context.Context, _, _ string) ([]byte, error) {
			cancel()
			<-logCtx.Done()
			return nil, logCtx.Err()
		},
		PollInterval:  time.Millisecond,
		ReadyTimeout:  time.Second,
		OperationTime: time.Second,
		DeleteTimeout: time.Second,
	}
	_, err := prober.Probe(ctx, accessProbeRequestForTest())
	if err == nil {
		t.Fatal("cancelled probe unexpectedly succeeded")
	}
	wantName := accessProbePodName("yt-access-test", "linode-gpu-rtx4000")
	if deletedName != wantName {
		t.Fatalf("cancel cleanup deleted %q, want exact pod %q", deletedName, wantName)
	}
}

func TestKubernetesAccessProberCreateCollisionDoesNotDeleteExistingPod(t *testing.T) {
	k8s := fake.NewSimpleClientset(readyAccessProbeNode())
	deletes := 0
	wantName := accessProbePodName("yt-access-test", "linode-gpu-rtx4000")
	k8s.PrependReactor("create", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "pods"}, wantName)
	})
	k8s.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		deletes++
		return true, nil, nil
	})
	prober := &KubernetesAccessProber{
		K8s:           k8s,
		RESTConfig:    &rest.Config{Host: "https://127.0.0.1"},
		OperationTime: time.Second,
		DeleteTimeout: time.Second,
	}
	_, err := prober.Probe(context.Background(), accessProbeRequestForTest())
	if err == nil {
		t.Fatal("create collision unexpectedly succeeded")
	}
	var probeErr *AccessProbeError
	if !errors.As(err, &probeErr) || probeErr.Stage != "create" {
		t.Fatalf("collision error = %v, want create stage", err)
	}
	if deletes != 0 {
		t.Fatalf("create collision deleted a pod %d times", deletes)
	}
}
