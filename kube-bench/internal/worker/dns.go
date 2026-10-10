package worker

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/stats"
)

func runDNSCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("worker dns", flag.ContinueOnError)
	flags.SetOutput(stderr)
	node := flags.String("node", os.Getenv("NODE_NAME"), "node name")
	name := flags.String("name", "kubernetes.default.svc.cluster.local", "DNS name")
	protocol := flags.String("protocol", "udp", "udp or tcp")
	class := flags.String("class", "", "target class")
	queries := flags.Int("queries", 100, "query count")
	concurrency := flags.Int("concurrency", 4, "concurrency")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	result := benchmarkDNS(nodeName(*node), *name, *protocol, *class, *queries, *concurrency)
	return writeJSON(stdout, result)
}

func benchmarkDNS(node, name, protocol, class string, queries, concurrency int) model.DNSResult {
	result := model.DNSResult{Node: node, Name: name, Protocol: protocol, Class: class, Status: "ok"}
	if queries < 1 || concurrency < 1 {
		result.Status = "failed"
		result.Error = "queries and concurrency must be at least one"
		return result
	}
	if protocol != "udp" && protocol != "tcp" {
		result.Status = "failed"
		result.Error = fmt.Sprintf("unsupported DNS protocol %q", protocol)
		return result
	}
	resolver := clusterResolver(protocol)
	jobs := make(chan struct{})
	latencies := make(chan float64, queries)
	var failures atomic.Int64
	var wait sync.WaitGroup
	workers := concurrency
	if workers > queries {
		workers = queries
	}
	start := time.Now()
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range jobs {
				queryStart := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := resolver.LookupHost(ctx, name)
				cancel()
				elapsed := float64(time.Since(queryStart).Microseconds()) / 1000
				if err != nil {
					failures.Add(1)
					continue
				}
				latencies <- elapsed
			}
		}()
	}
	for index := 0; index < queries; index++ {
		jobs <- struct{}{}
	}
	close(jobs)
	wait.Wait()
	close(latencies)
	values := make([]float64, 0, queries)
	for value := range latencies {
		values = append(values, value)
	}
	wall := time.Since(start).Seconds()
	result.Failures = int(failures.Load())
	result.LatencyMS = stats.Distribution(values, "ms")
	if wall > 0 {
		result.QueriesPerSec = float64(len(values)) / wall
	}
	if len(values) == 0 {
		result.Status = "failed"
		result.Error = "all DNS queries failed"
	} else if result.Failures > 0 {
		result.Status = "partial"
		result.Error = fmt.Sprintf("%d of %d DNS queries failed", result.Failures, queries)
	}
	return result
}

func clusterResolver(protocol string) *net.Resolver {
	server := resolvConfServer()
	if server == "" {
		return net.DefaultResolver
	}
	if _, _, err := net.SplitHostPort(server); err != nil {
		server = net.JoinHostPort(server, "53")
	}
	dialer := net.Dialer{Timeout: 2 * time.Second}
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, protocol, server)
		},
	}
}

func resolvConfServer() string {
	file, err := os.Open("/etc/resolv.conf")
	if err != nil {
		return ""
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "nameserver" {
			return fields[1]
		}
	}
	return ""
}
