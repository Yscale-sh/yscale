package kubectl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

type Client struct {
	Binary     string
	Kubeconfig string
	Context    string
	Namespace  string
	Verbose    bool
	LogWriter  io.Writer
}

func New(binary, kubeconfig, currentContext, namespace string, verbose bool, logWriter io.Writer) *Client {
	if binary == "" {
		binary = "kubectl"
	}
	if logWriter == nil {
		logWriter = io.Discard
	}
	return &Client{
		Binary:     binary,
		Kubeconfig: kubeconfig,
		Context:    currentContext,
		Namespace:  namespace,
		Verbose:    verbose,
		LogWriter:  logWriter,
	}
}

func (c *Client) Check() error {
	path, err := exec.LookPath(c.Binary)
	if err != nil {
		return fmt.Errorf("kubectl executable %q was not found: %w", c.Binary, err)
	}
	c.Binary = path
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var version Version
	if err := c.JSON(ctx, &version, "version", "-o", "json"); err != nil {
		return fmt.Errorf("connect to Kubernetes: %w", err)
	}
	if version.ServerVersion.GitVersion == "" {
		return errors.New("kubectl did not return a Kubernetes server version")
	}
	return nil
}

func (c *Client) Version(ctx context.Context) (Version, error) {
	var version Version
	err := c.JSON(ctx, &version, "version", "-o", "json")
	return version, err
}

func (c *Client) Run(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	base := c.baseArgs(args...)
	if c.Verbose {
		fmt.Fprintf(c.LogWriter, "+ %s %s\n", c.Binary, strings.Join(base, " "))
	}
	command := exec.CommandContext(ctx, c.Binary, base...)
	if stdin != nil {
		command.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return stdout.Bytes(), fmt.Errorf("kubectl %s: %s", strings.Join(args, " "), message)
	}
	return stdout.Bytes(), nil
}

func (c *Client) JSON(ctx context.Context, output any, args ...string) error {
	data, err := c.Run(ctx, nil, args...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, output); err != nil {
		return fmt.Errorf("decode kubectl JSON for %q: %w", strings.Join(args, " "), err)
	}
	return nil
}

func (c *Client) Create(ctx context.Context, object any, output any) error {
	data, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("encode Kubernetes object: %w", err)
	}
	result, err := c.Run(ctx, data, "create", "-f", "-", "-o", "json")
	if err != nil {
		return err
	}
	if output != nil {
		if err := json.Unmarshal(result, output); err != nil {
			return fmt.Errorf("decode created Kubernetes object: %w", err)
		}
	}
	return nil
}

func (c *Client) Apply(ctx context.Context, object any) error {
	data, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("encode Kubernetes object: %w", err)
	}
	_, err = c.Run(ctx, data, "apply", "-f", "-")
	return err
}

func (c *Client) Delete(ctx context.Context, resource, name string) error {
	_, err := c.Run(ctx, nil, "delete", resource, name, "--ignore-not-found=true", "--wait=false")
	return err
}

func (c *Client) DeleteByLabel(ctx context.Context, label string) error {
	resources := []string{"jobs", "pods", "services", "configmaps"}
	var problems []string
	for _, resource := range resources {
		if _, err := c.Run(ctx, nil, "delete", resource, "-l", label, "--ignore-not-found=true", "--wait=false"); err != nil {
			problems = append(problems, err.Error())
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func (c *Client) ListNodes(ctx context.Context) ([]Node, error) {
	var list NodeList
	if err := c.JSON(ctx, &list, "get", "nodes", "-o", "json"); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (c *Client) ListPods(ctx context.Context, selector string) ([]Pod, error) {
	var list PodList
	args := []string{"get", "pods", "-o", "json"}
	if selector != "" {
		args = append(args, "-l", selector)
	}
	if err := c.JSON(ctx, &list, args...); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (c *Client) GetPod(ctx context.Context, name string) (Pod, error) {
	var pod Pod
	err := c.JSON(ctx, &pod, "get", "pod", name, "-o", "json")
	return pod, err
}

func (c *Client) GetEndpoints(ctx context.Context, name string) (Endpoints, error) {
	var endpoints Endpoints
	err := c.JSON(ctx, &endpoints, "get", "endpoints", name, "-o", "json")
	return endpoints, err
}

func (c *Client) Logs(ctx context.Context, pod, container string) ([]byte, error) {
	args := []string{"logs", pod}
	if container != "" {
		args = append(args, "-c", container)
	}
	return c.Run(ctx, nil, args...)
}

func (c *Client) EventsFor(ctx context.Context, objectName string) ([]Event, error) {
	var list EventList
	selector := "involvedObject.name=" + objectName
	if err := c.JSON(ctx, &list, "get", "events", "--field-selector", selector, "-o", "json"); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (c *Client) EnsureNamespace(ctx context.Context) error {
	if c.Namespace == "" {
		return errors.New("namespace is empty")
	}
	if _, err := c.Run(ctx, nil, "get", "namespace", c.Namespace, "-o", "name"); err == nil {
		return nil
	}
	object := map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": c.Namespace,
			"labels": map[string]string{
				"app.kubernetes.io/managed-by": "yscale-kube-bench",
			},
		},
	}
	data, _ := json.Marshal(object)
	_, err := c.Run(ctx, data, "create", "-f", "-")
	return err
}

func (c *Client) CanI(ctx context.Context, verb, resource string) (bool, error) {
	output, err := c.Run(ctx, nil, "auth", "can-i", verb, resource)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(output)) == "yes", nil
}

func (c *Client) baseArgs(args ...string) []string {
	result := make([]string, 0, len(args)+6)
	if c.Kubeconfig != "" {
		result = append(result, "--kubeconfig", c.Kubeconfig)
	}
	if c.Context != "" {
		result = append(result, "--context", c.Context)
	}
	if c.Namespace != "" && commandUsesNamespace(args) {
		result = append(result, "--namespace", c.Namespace)
	}
	result = append(result, args...)
	return result
}

func commandUsesNamespace(args []string) bool {
	if len(args) == 0 {
		return false
	}
	for _, value := range args {
		if value == "namespace" || value == "namespaces" || value == "node" || value == "nodes" {
			return false
		}
	}
	return true
}

func WriteExecutable(path string, data []byte) error {
	if err := os.WriteFile(path, data, 0o755); err != nil {
		return err
	}
	return nil
}
