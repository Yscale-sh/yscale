package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// gpuTaintNode builds a burst node with the given gpu-not-ready taint state and
// nvidia.com/gpu allocatable quantity (the signal the NVIDIA device plugin is
// up). gpuAlloc="" omits the resource entirely (plugin not registered).
func gpuTaintNode(name string, tainted bool, gpuAlloc string) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if tainted {
		n.Spec.Taints = []corev1.Taint{{
			Key:    gpuNotReadyTaintKey,
			Value:  "true",
			Effect: corev1.TaintEffectNoSchedule,
		}}
	}
	if gpuAlloc != "" {
		n.Status.Allocatable = corev1.ResourceList{
			corev1.ResourceName("nvidia.com/gpu"): resource.MustParse(gpuAlloc),
		}
	}
	return n
}

// TestRemoveGpuNotReadyTaint covers the controller-side clear path (issue #39):
// once nvidia.com/gpu is allocatable the device-plugin taint is removed with the
// controller's own credentials; while it is not yet allocatable the taint is
// retained (so GPU pods keep waiting), and a node that never had it no-ops.
func TestRemoveGpuNotReadyTaint(t *testing.T) {
	for _, tt := range []struct {
		name        string
		node        *corev1.Node
		wantCleared bool // removeGpuNotReadyTaint return value
		wantTaint   bool // taint present on the node afterward
	}{
		{
			name:        "allocatable clears taint",
			node:        gpuTaintNode("n1", true, "1"),
			wantCleared: true,
			wantTaint:   false,
		},
		{
			name:        "not allocatable yet retains taint",
			node:        gpuTaintNode("n2", true, ""),
			wantCleared: false,
			wantTaint:   true,
		},
		{
			name:        "no taint (CPU node) is a no-op success",
			node:        gpuTaintNode("n3", false, "1"),
			wantCleared: true,
			wantTaint:   false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newPoolTestController(&poolTestBackend{})
			c.clientset = fake.NewSimpleClientset(tt.node)

			if got := c.removeGpuNotReadyTaint(context.Background(), tt.node.Name); got != tt.wantCleared {
				t.Fatalf("removeGpuNotReadyTaint = %v, want %v", got, tt.wantCleared)
			}

			after, err := c.clientset.CoreV1().Nodes().Get(context.Background(), tt.node.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get node after: %v", err)
			}
			if has := hasGpuNotReadyTaint(after); has != tt.wantTaint {
				t.Errorf("gpu-not-ready taint present after = %v, want %v", has, tt.wantTaint)
			}
		})
	}
}
