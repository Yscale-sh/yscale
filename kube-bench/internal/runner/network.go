package runner

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/kubectl"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/stats"
)

type networkServer struct {
	Node kubectl.Node
	Pod  kubectl.Pod
}

type networkTask struct {
	Source      string
	Destination string
	Path        string
	TCP         string
	UDP         string
}

func (r *Runner) runNetwork(ctx context.Context, nodes []kubectl.Node) *model.NetworkSuite {
	suite := &model.NetworkSuite{Status: "ok"}
	servers, serverErrors := r.startNetworkServers(ctx, nodes)
	for _, server := range servers {
		server := server
		if !r.config.Spec.KeepResources {
			defer func() {
				cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_ = r.client.Delete(cleanupContext, "pod", server.Pod.Metadata.Name)
			}()
		}
	}
	if len(servers) == 0 {
		suite.Status = "failed"
		suite.Error = "no network server pod became ready: " + joinErrors(serverErrors)
		return suite
	}

	serviceName := sanitizeName("network-" + r.runID)
	serviceReady := false
	if r.config.Spec.Network.ServicePath {
		if err := r.client.Create(ctx, r.networkServiceManifest(serviceName), nil); err != nil {
			serverErrors = append(serverErrors, "create service: "+err.Error())
		} else {
			if !r.config.Spec.KeepResources {
				defer func() {
					cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					_ = r.client.Delete(cleanupContext, "service", serviceName)
				}()
			}
			endpointContext, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := r.waitForServiceEndpoints(endpointContext, serviceName, len(servers)); err != nil {
				serverErrors = append(serverErrors, "service endpoints: "+err.Error())
			} else {
				serviceReady = true
			}
			cancel()
		}
	}

	tasks := buildNetworkTasks(servers, r.config.Spec.Network.Matrix, serviceReady, serviceName+"."+r.config.Spec.Namespace+".svc.cluster.local")
	suite.Results = r.runNetworkTasks(ctx, tasks)
	var same, cross, service, crossRTT []float64
	failed := 0
	partial := 0
	for _, result := range suite.Results {
		switch result.Status {
		case "failed":
			failed++
		case "partial":
			partial++
		}
		throughput := meanNonZero(result.TCPUploadMbps, result.TCPDownloadMbps)
		switch result.Path {
		case "same-node":
			if throughput > 0 {
				same = append(same, throughput)
			}
		case "cross-node":
			if throughput > 0 {
				cross = append(cross, throughput)
			}
			if result.TCPRTTMS.P99 > 0 {
				crossRTT = append(crossRTT, result.TCPRTTMS.P99)
			}
		case "cluster-ip":
			if throughput > 0 {
				service = append(service, throughput)
			}
		}
	}
	suite.Summary = model.NetworkSummary{
		SameNodeTCPMbps:  stats.Distribution(same, "Mbit/s"),
		CrossNodeTCPMbps: stats.Distribution(cross, "Mbit/s"),
		ServiceTCPMbps:   stats.Distribution(service, "Mbit/s"),
		CrossNodeRTTMS:   stats.Distribution(crossRTT, "ms"),
	}
	if len(serverErrors) > 0 {
		suite.Error = joinErrors(serverErrors)
	}
	if failed == len(suite.Results) && len(suite.Results) > 0 {
		suite.Status = "failed"
		if suite.Error != "" {
			suite.Error += "; "
		}
		suite.Error += "all network clients failed"
	} else if failed > 0 || partial > 0 || len(serverErrors) > 0 {
		suite.Status = "partial"
		if failed > 0 {
			if suite.Error != "" {
				suite.Error += "; "
			}
			suite.Error += fmt.Sprintf("%d of %d network clients failed", failed, len(suite.Results))
		}
		if partial > 0 {
			if suite.Error != "" {
				suite.Error += "; "
			}
			suite.Error += fmt.Sprintf("%d of %d network clients completed partially", partial, len(suite.Results))
		}
	}
	return suite
}

func (r *Runner) startNetworkServers(ctx context.Context, nodes []kubectl.Node) ([]networkServer, []string) {
	servers := make([]networkServer, 0, len(nodes))
	var problems []string
	for _, node := range nodes {
		name := sanitizeName("net-server-" + node.Metadata.Name + "-" + r.runID)
		var created kubectl.Pod
		if err := r.client.Create(ctx, r.serverPodManifest(name, node.Metadata.Name), &created); err != nil {
			problems = append(problems, node.Metadata.Name+": "+err.Error())
			continue
		}
		readyContext, cancel := context.WithTimeout(ctx, 2*time.Minute)
		ready, err := r.waitForPodReady(readyContext, name)
		cancel()
		if err != nil {
			problems = append(problems, node.Metadata.Name+": "+err.Error())
			if !r.config.Spec.KeepResources {
				_ = r.client.Delete(context.Background(), "pod", name)
			}
			continue
		}
		servers = append(servers, networkServer{Node: node, Pod: ready})
	}
	return servers, problems
}

func (r *Runner) waitForServiceEndpoints(ctx context.Context, serviceName string, expected int) error {
	if expected < 1 {
		expected = 1
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	lastCount := 0
	var lastErr error
	for {
		endpoints, err := r.client.GetEndpoints(ctx, serviceName)
		if err == nil {
			lastErr = nil
			lastCount = endpoints.ReadyAddressCount()
			if lastCount >= expected {
				return nil
			}
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("wait for service %s endpoints: %w; last query: %v", serviceName, ctx.Err(), lastErr)
			}
			return fmt.Errorf("wait for service %s endpoints: %w; ready addresses %d/%d", serviceName, ctx.Err(), lastCount, expected)
		case <-ticker.C:
		}
	}
}

func buildNetworkTasks(servers []networkServer, matrix string, serviceReady bool, serviceHost string) []networkTask {
	var tasks []networkTask
	for _, server := range servers {
		tasks = append(tasks, directNetworkTask(server.Node.Metadata.Name, server.Node.Metadata.Name, "same-node", server.Pod.Status.PodIP))
	}
	if len(servers) > 1 {
		if matrix == "full" {
			for _, source := range servers {
				for _, destination := range servers {
					if source.Node.Metadata.Name == destination.Node.Metadata.Name {
						continue
					}
					tasks = append(tasks, directNetworkTask(source.Node.Metadata.Name, destination.Node.Metadata.Name, "cross-node", destination.Pod.Status.PodIP))
				}
			}
		} else {
			for index, source := range servers {
				destination := servers[(index+1)%len(servers)]
				tasks = append(tasks, directNetworkTask(source.Node.Metadata.Name, destination.Node.Metadata.Name, "cross-node", destination.Pod.Status.PodIP))
			}
		}
	}
	if serviceReady {
		for _, source := range servers {
			tasks = append(tasks, networkTask{
				Source: source.Node.Metadata.Name,
				Path:   "cluster-ip",
				TCP:    net.JoinHostPort(serviceHost, "9090"),
				UDP:    net.JoinHostPort(serviceHost, "9091"),
			})
		}
	}
	return tasks
}

func directNetworkTask(source, destination, path, host string) networkTask {
	return networkTask{
		Source:      source,
		Destination: destination,
		Path:        path,
		TCP:         net.JoinHostPort(host, "9090"),
		UDP:         net.JoinHostPort(host, "9091"),
	}
}

func (r *Runner) runNetworkTasks(ctx context.Context, tasks []networkTask) []model.NetworkResult {
	results := make([]model.NetworkResult, len(tasks))
	parallelism := r.config.Spec.Network.Parallelism
	if parallelism < 1 {
		parallelism = 1
	}
	semaphore := make(chan struct{}, parallelism)
	var wait sync.WaitGroup
	for index, task := range tasks {
		index, task := index, task
		wait.Add(1)
		go func() {
			defer wait.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			results[index] = r.networkClient(ctx, task, index)
		}()
	}
	wait.Wait()
	return results
}

func (r *Runner) networkClient(ctx context.Context, task networkTask, index int) model.NetworkResult {
	request := jobRequest{
		Name:     fmt.Sprintf("net-client-%d-%s-%s", index, task.Source, r.runID),
		NodeName: task.Source,
		Args: []string{
			"worker", "net-client",
			"--source-node=" + task.Source,
			"--destination-node=" + task.Destination,
			"--path=" + task.Path,
			"--tcp-target=" + task.TCP,
			"--udp-target=" + task.UDP,
			fmt.Sprintf("--duration=%ds", r.config.Spec.Network.DurationSeconds),
			fmt.Sprintf("--rtt-samples=%d", r.config.Spec.Network.RTTSamples),
			fmt.Sprintf("--udp-samples=%d", r.config.Spec.Network.UDPSamples),
		},
		Environment: map[string]string{"NODE_NAME": task.Source},
	}
	execution, err := r.runJob(ctx, request)
	if err != nil {
		return model.NetworkResult{SourceNode: task.Source, DestinationNode: task.Destination, Path: task.Path, Target: task.TCP, Status: "failed", Error: err.Error()}
	}
	result, err := decodeJobResult[model.NetworkResult](execution)
	if err != nil {
		return model.NetworkResult{SourceNode: task.Source, DestinationNode: task.Destination, Path: task.Path, Target: task.TCP, Status: "failed", Error: err.Error()}
	}
	return result
}

func meanNonZero(values ...float64) float64 {
	var sum float64
	var count int
	for _, value := range values {
		if value > 0 {
			sum += value
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}
