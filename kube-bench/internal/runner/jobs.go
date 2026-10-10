package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/kubectl"
)

type jobExecution struct {
	Pod  kubectl.Pod
	Logs []byte
}

func (r *Runner) runJob(ctx context.Context, request jobRequest) (jobExecution, error) {
	name := sanitizeName(request.Name)
	request.Name = name
	if request.TimeoutSeconds <= 0 {
		request.TimeoutSeconds = r.config.Spec.TimeoutSeconds
	}
	manifest := r.jobManifest(request)
	if err := r.client.Create(ctx, manifest, nil); err != nil {
		return jobExecution{}, fmt.Errorf("create job %s: %w", name, err)
	}
	if !r.config.Spec.KeepResources {
		defer func() {
			cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = r.client.Delete(cleanupContext, "job", name)
		}()
	}
	jobContext, cancel := context.WithTimeout(ctx, time.Duration(request.TimeoutSeconds)*time.Second)
	defer cancel()
	pod, err := r.waitForJobPod(jobContext, name)
	if err != nil {
		return jobExecution{}, err
	}
	logs, logErr := r.client.Logs(jobContext, pod.Metadata.Name, "bench")
	if logErr != nil {
		return jobExecution{Pod: pod}, fmt.Errorf("read logs for job %s: %w", name, logErr)
	}
	if pod.Status.Phase != "Succeeded" {
		return jobExecution{Pod: pod, Logs: logs}, fmt.Errorf("job %s pod %s ended in phase %s: %s", name, pod.Metadata.Name, pod.Status.Phase, podFailureMessage(pod))
	}
	return jobExecution{Pod: pod, Logs: logs}, nil
}

func (r *Runner) waitForJobPod(ctx context.Context, jobName string) (kubectl.Pod, error) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	selector := "job-name=" + jobName
	var lastPod kubectl.Pod
	for {
		pods, err := r.client.ListPods(ctx, selector)
		if err == nil && len(pods) > 0 {
			sort.Slice(pods, func(i, j int) bool {
				return pods[i].Metadata.CreationTimestamp.Before(pods[j].Metadata.CreationTimestamp)
			})
			lastPod = pods[len(pods)-1]
			switch lastPod.Status.Phase {
			case "Succeeded", "Failed":
				return lastPod, nil
			}
			if waiting := podWaitingFailure(lastPod); waiting != "" {
				return lastPod, fmt.Errorf("job %s cannot start: %s", jobName, waiting)
			}
		}
		select {
		case <-ctx.Done():
			message := ""
			if lastPod.Metadata.Name != "" {
				message = ": last pod state: " + podFailureMessage(lastPod)
			}
			return lastPod, fmt.Errorf("wait for job %s: %w%s", jobName, ctx.Err(), message)
		case <-ticker.C:
		}
	}
}

func (r *Runner) waitForPodReady(ctx context.Context, name string) (kubectl.Pod, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var last kubectl.Pod
	for {
		pod, err := r.client.GetPod(ctx, name)
		if err == nil {
			last = pod
			if _, ready := pod.Condition("Ready"); ready {
				return pod, nil
			}
			if pod.Status.Phase == "Failed" || pod.Status.Phase == "Succeeded" {
				return pod, fmt.Errorf("pod %s ended before becoming ready: %s", name, podFailureMessage(pod))
			}
			if waiting := podWaitingFailure(pod); waiting != "" {
				return pod, fmt.Errorf("pod %s cannot start: %s", name, waiting)
			}
		}
		select {
		case <-ctx.Done():
			return last, fmt.Errorf("wait for pod %s ready: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func decodeJobResult[T any](execution jobExecution) (T, error) {
	var result T
	data := strings.TrimSpace(string(execution.Logs))
	if data == "" {
		return result, fmt.Errorf("worker produced no JSON output")
	}
	lines := strings.Split(data, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		line := strings.TrimSpace(lines[index])
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		if err := json.Unmarshal([]byte(line), &result); err == nil {
			return result, nil
		}
	}
	if err := json.Unmarshal(execution.Logs, &result); err != nil {
		return result, fmt.Errorf("decode worker JSON: %w; output=%q", err, truncate(data, 500))
	}
	return result, nil
}

func podWaitingFailure(pod kubectl.Pod) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting == nil {
			continue
		}
		switch status.State.Waiting.Reason {
		case "ErrImagePull", "ImagePullBackOff", "CreateContainerConfigError", "CreateContainerError", "InvalidImageName", "RunContainerError":
			return status.State.Waiting.Reason + ": " + status.State.Waiting.Message
		}
	}
	return ""
}

func podFailureMessage(pod kubectl.Pod) string {
	var messages []string
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting != nil {
			messages = append(messages, status.Name+" waiting "+status.State.Waiting.Reason+": "+status.State.Waiting.Message)
		}
		if status.State.Terminated != nil {
			messages = append(messages, fmt.Sprintf("%s exited %d (%s): %s", status.Name, status.State.Terminated.ExitCode, status.State.Terminated.Reason, status.State.Terminated.Message))
		}
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Status == "False" && condition.Message != "" {
			messages = append(messages, condition.Type+": "+condition.Message)
		}
	}
	if len(messages) == 0 {
		return "no detailed failure message"
	}
	return strings.Join(messages, "; ")
}

func truncate(value string, size int) string {
	if len(value) <= size {
		return value
	}
	return value[:size] + "..."
}
