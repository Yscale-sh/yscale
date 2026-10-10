package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/yscale-sh/yscale/pkg/backends"
	v1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
)

const testClusterID = "cl_0123456789abcdef0123456789abcdef"

// burstNodeName returns a valid burst-prefixed node name.
func burstNodeName(suffix string) string {
	return backends.NodeNamePrefix + suffix
}

// makeNode constructs a Node for testing.
type makeNodeOpts struct {
	name              string
	ready             corev1.ConditionStatus // corev1.ConditionTrue/False/Unknown; "" → no condition
	lastTransition    time.Time
	creationTimestamp time.Time
}

func makeNode(o makeNodeOpts) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              o.name,
			CreationTimestamp: metav1.Time{Time: o.creationTimestamp},
		},
	}
	if strings.HasPrefix(o.name, backends.NodeNamePrefix) {
		n.Labels = map[string]string{v1.LabelClusterID: testClusterID}
	}
	if o.ready != "" {
		n.Status.Conditions = []corev1.NodeCondition{
			{
				Type:               corev1.NodeReady,
				Status:             o.ready,
				LastTransitionTime: metav1.Time{Time: o.lastTransition},
			},
		}
	}
	return n
}

// newTestGC builds a NodeGC with instant sweep / grace periods
// suitable for test control, wired to the provided fake k8s client.
func newTestGC(k8s *fake.Clientset, grace time.Duration) *NodeGC {
	h := NewRealHandler(k8s, nil, nil)
	gc := NewNodeGC(k8s, h, nil)
	gc.SweepInterval = time.Hour // irrelevant for direct sweep() calls
	gc.GracePeriod = grace
	return gc
}

// TestNodeGC_BurstNotReadyPastGrace: a burst-prefixed node that is
// NotReady longer than grace period → must be deleted.
func TestNodeGC_BurstNotReadyPastGrace(t *testing.T) {
	now := time.Now()
	grace := 15 * time.Minute
	node := makeNode(makeNodeOpts{
		name:              burstNodeName("abc123"),
		ready:             corev1.ConditionFalse,
		lastTransition:    now.Add(-grace - time.Minute), // 1 m past grace
		creationTimestamp: now.Add(-30 * time.Minute),
	})

	k8s := fake.NewSimpleClientset(node)
	gc := newTestGC(k8s, grace)
	gc.sweep(context.Background())

	_, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected node %q to be deleted, but it still exists", node.Name)
	}
}

// TestNodeGC_BurstNotReadyWithinGrace: NotReady but within grace → kept.
func TestNodeGC_BurstNotReadyWithinGrace(t *testing.T) {
	now := time.Now()
	grace := 15 * time.Minute
	node := makeNode(makeNodeOpts{
		name:              burstNodeName("def456"),
		ready:             corev1.ConditionFalse,
		lastTransition:    now.Add(-grace / 2), // well within grace
		creationTimestamp: now.Add(-10 * time.Minute),
	})

	k8s := fake.NewSimpleClientset(node)
	gc := newTestGC(k8s, grace)
	gc.sweep(context.Background())

	if _, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{}); err != nil {
		t.Errorf("expected node %q to be kept, but it was deleted: %v", node.Name, err)
	}
}

// TestNodeGC_BurstReady: a burst node that is Ready → kept.
func TestNodeGC_BurstReady(t *testing.T) {
	now := time.Now()
	grace := 15 * time.Minute
	node := makeNode(makeNodeOpts{
		name:              burstNodeName("ghi789"),
		ready:             corev1.ConditionTrue,
		lastTransition:    now.Add(-2 * grace), // long time ago but Ready
		creationTimestamp: now.Add(-60 * time.Minute),
	})

	k8s := fake.NewSimpleClientset(node)
	gc := newTestGC(k8s, grace)
	gc.sweep(context.Background())

	if _, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{}); err != nil {
		t.Errorf("expected ready node %q to be kept, but it was deleted: %v", node.Name, err)
	}
}

// TestNodeGC_NonBurstNotReadyPastGrace: a non-burst node that is
// NotReady past grace → must NOT be touched (we only GC burst nodes).
func TestNodeGC_NonBurstNotReadyPastGrace(t *testing.T) {
	now := time.Now()
	grace := 15 * time.Minute
	node := makeNode(makeNodeOpts{
		name:              "customer-worker-1", // no burst prefix
		ready:             corev1.ConditionFalse,
		lastTransition:    now.Add(-grace - time.Hour),
		creationTimestamp: now.Add(-2 * time.Hour),
	})

	k8s := fake.NewSimpleClientset(node)
	gc := newTestGC(k8s, grace)
	gc.sweep(context.Background())

	if _, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{}); err != nil {
		t.Errorf("expected non-burst node %q to be untouched, but it was deleted: %v", node.Name, err)
	}
}

// TestNodeGC_NoReadyConditionOlderThanGrace: a burst node with NO
// Ready condition at all, whose CreationTimestamp is older than grace
// → deleted (CreationTimestamp used as proxy for transition time).
func TestNodeGC_NoReadyConditionOlderThanGrace(t *testing.T) {
	now := time.Now()
	grace := 15 * time.Minute
	node := makeNode(makeNodeOpts{
		name:              burstNodeName("noready"),
		ready:             "", // no condition set
		creationTimestamp: now.Add(-grace - 5*time.Minute),
	})

	k8s := fake.NewSimpleClientset(node)
	gc := newTestGC(k8s, grace)
	gc.sweep(context.Background())

	_, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected no-condition node %q to be deleted, but it still exists", node.Name)
	}
}
