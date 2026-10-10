package kubectl

import "testing"

func TestBaseArgs(t *testing.T) {
	client := New("kubectl", "/tmp/kubeconfig", "lab", "bench", false, nil)
	args := client.baseArgs("get", "pods", "-o", "json")
	expected := []string{"--kubeconfig", "/tmp/kubeconfig", "--context", "lab", "--namespace", "bench", "get", "pods", "-o", "json"}
	if len(args) != len(expected) {
		t.Fatalf("unexpected args: %v", args)
	}
	for index := range expected {
		if args[index] != expected[index] {
			t.Fatalf("unexpected args: %v", args)
		}
	}
}

func TestNodeCommandOmitsNamespace(t *testing.T) {
	client := New("kubectl", "", "", "bench", false, nil)
	args := client.baseArgs("get", "nodes")
	for _, value := range args {
		if value == "--namespace" {
			t.Fatalf("node command should not include namespace: %v", args)
		}
	}
}
