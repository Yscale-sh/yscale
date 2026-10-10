package runner

import (
	"context"
	"fmt"
	"sync"

	"github.com/JakeNesler/yscale-kube-bench/internal/kubectl"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/stats"
)

type dnsTask struct {
	Node     string
	Name     string
	Protocol string
	Class    string
}

func (r *Runner) runDNS(ctx context.Context, nodes []kubectl.Node) *model.DNSSuite {
	suite := &model.DNSSuite{Status: "ok"}
	var tasks []dnsTask
	for _, node := range nodes {
		for _, target := range r.config.Spec.DNS.Targets {
			tasks = append(tasks, dnsTask{Node: node.Metadata.Name, Name: target.Name, Protocol: target.Protocol, Class: target.Class})
		}
	}
	suite.Results = make([]model.DNSResult, len(tasks))
	parallelism := r.config.Spec.Parallelism
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
			suite.Results[index] = r.dnsClient(ctx, task, index)
		}()
	}
	wait.Wait()
	var serviceP99, externalP99 []float64
	failed := 0
	partial := 0
	for _, result := range suite.Results {
		suite.Summary.FailureCount += result.Failures
		switch result.Status {
		case "failed":
			failed++
		case "partial":
			partial++
		}
		if result.LatencyMS.P99 <= 0 {
			continue
		}
		switch result.Class {
		case "service":
			serviceP99 = append(serviceP99, result.LatencyMS.P99)
		case "external":
			externalP99 = append(externalP99, result.LatencyMS.P99)
		}
	}
	suite.Summary.ServiceP99MS = stats.Distribution(serviceP99, "ms")
	suite.Summary.ExternalP99MS = stats.Distribution(externalP99, "ms")
	if failed == len(suite.Results) && len(suite.Results) > 0 {
		suite.Status = "failed"
		suite.Error = "all DNS measurements failed"
	} else if failed > 0 || partial > 0 {
		suite.Status = "partial"
		if failed > 0 {
			suite.Error = fmt.Sprintf("%d of %d DNS measurements failed", failed, len(suite.Results))
		}
		if partial > 0 {
			if suite.Error != "" {
				suite.Error += "; "
			}
			suite.Error += fmt.Sprintf("%d of %d DNS measurements completed partially", partial, len(suite.Results))
		}
	}
	return suite
}

func (r *Runner) dnsClient(ctx context.Context, task dnsTask, index int) model.DNSResult {
	request := jobRequest{
		Name:     fmt.Sprintf("dns-%d-%s-%s", index, task.Node, r.runID),
		NodeName: task.Node,
		Args: []string{
			"worker", "dns",
			"--node=" + task.Node,
			"--name=" + task.Name,
			"--protocol=" + task.Protocol,
			"--class=" + task.Class,
			fmt.Sprintf("--queries=%d", r.config.Spec.DNS.Queries),
			fmt.Sprintf("--concurrency=%d", r.config.Spec.DNS.Concurrency),
		},
		Environment: map[string]string{"NODE_NAME": task.Node},
	}
	execution, err := r.runJob(ctx, request)
	if err != nil {
		return model.DNSResult{Node: task.Node, Name: task.Name, Protocol: task.Protocol, Class: task.Class, Status: "failed", Error: err.Error()}
	}
	result, err := decodeJobResult[model.DNSResult](execution)
	if err != nil {
		return model.DNSResult{Node: task.Node, Name: task.Name, Protocol: task.Protocol, Class: task.Class, Status: "failed", Error: err.Error()}
	}
	return result
}
