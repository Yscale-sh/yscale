package worker

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

var computeSink atomic.Uint64

func runComputeCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("worker compute", flag.ContinueOnError)
	flags.SetOutput(stderr)
	node := flags.String("node", os.Getenv("NODE_NAME"), "node name")
	duration := flags.Duration("duration", 5*time.Second, "duration per benchmark")
	mode := flags.String("mode", "isolated", "isolated or all")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *duration < 100*time.Millisecond {
		return writeJSON(stdout, failedCompute(nodeName(*node), "duration must be at least 100ms"))
	}
	result := benchmarkCompute(nodeName(*node), *duration, *mode)
	return writeJSON(stdout, result)
}

func benchmarkCompute(node string, duration time.Duration, mode string) model.NodeComputeResult {
	result := model.NodeComputeResult{
		Node:            node,
		Status:          "ok",
		LogicalCPUs:     runtime.NumCPU(),
		DurationSeconds: duration.Seconds(),
	}
	if mode != "isolated" && mode != "all" {
		result.Status = "failed"
		result.Error = fmt.Sprintf("unknown compute mode %q", mode)
		return result
	}
	startCPU := readCPUTimes()
	if mode == "isolated" {
		result.SingleIntegerOpsPerSec = benchmarkInteger(duration, 1)
		result.SingleSHA256MiBPerSec = benchmarkSHA256(duration, 1)
	}
	result.AllIntegerOpsPerSec = benchmarkInteger(duration, runtime.NumCPU())
	result.AllSHA256MiBPerSec = benchmarkSHA256(duration, runtime.NumCPU())
	result.MemoryCopyMiBPerSec = benchmarkMemoryCopy(duration)
	endCPU := readCPUTimes()
	result.StealPercentBefore = startCPU.stealPercent()
	result.StealPercentAfter = deltaStealPercent(startCPU, endCPU)
	return result
}

func benchmarkInteger(duration time.Duration, threads int) float64 {
	if threads < 1 {
		threads = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	var total atomic.Uint64
	var wait sync.WaitGroup
	start := time.Now()
	for workerID := 0; workerID < threads; workerID++ {
		wait.Add(1)
		go func(seed uint64) {
			defer wait.Done()
			value := seed*0x9e3779b97f4a7c15 + 0x6a09e667f3bcc909
			var operations uint64
			for {
				select {
				case <-ctx.Done():
					total.Add(operations)
					computeSink.Add(value)
					return
				default:
					for index := 0; index < 1024; index++ {
						value ^= value << 13
						value ^= value >> 7
						value ^= value << 17
						value = value*2862933555777941757 + 3037000493
					}
					operations += 4096
				}
			}
		}(uint64(workerID + 1))
	}
	wait.Wait()
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(total.Load()) / elapsed
}

func benchmarkSHA256(duration time.Duration, threads int) float64 {
	if threads < 1 {
		threads = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	var bytesProcessed atomic.Uint64
	var wait sync.WaitGroup
	start := time.Now()
	for workerID := 0; workerID < threads; workerID++ {
		wait.Add(1)
		go func(seed uint64) {
			defer wait.Done()
			buffer := make([]byte, 64*1024)
			binary.LittleEndian.PutUint64(buffer, seed)
			var local uint64
			var sink uint64
			for {
				select {
				case <-ctx.Done():
					bytesProcessed.Add(local)
					computeSink.Add(sink)
					return
				default:
					sum := sha256.Sum256(buffer)
					copy(buffer[:32], sum[:])
					local += uint64(len(buffer))
					sink ^= binary.LittleEndian.Uint64(sum[:8])
				}
			}
		}(uint64(workerID + 1))
	}
	wait.Wait()
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(bytesProcessed.Load()) / 1024 / 1024 / elapsed
}

func benchmarkMemoryCopy(duration time.Duration) float64 {
	threads := runtime.NumCPU()
	if threads > 8 {
		threads = 8
	}
	if threads < 1 {
		threads = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	var bytesCopied atomic.Uint64
	var wait sync.WaitGroup
	start := time.Now()
	for workerID := 0; workerID < threads; workerID++ {
		wait.Add(1)
		go func(seed byte) {
			defer wait.Done()
			source := make([]byte, 8*1024*1024)
			destination := make([]byte, len(source))
			for index := range source {
				source[index] = seed + byte(index)
			}
			var local uint64
			for {
				select {
				case <-ctx.Done():
					bytesCopied.Add(local)
					computeSink.Add(uint64(destination[len(destination)-1]))
					return
				default:
					copy(destination, source)
					copy(source, destination)
					local += uint64(len(source) * 2)
				}
			}
		}(byte(workerID))
	}
	wait.Wait()
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(bytesCopied.Load()) / 1024 / 1024 / elapsed
}

type cpuTimes struct {
	user, nice, system, idle, iowait, irq, softirq, steal uint64
}

func readCPUTimes() cpuTimes {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuTimes{}
	}
	line := strings.SplitN(string(data), "\n", 2)[0]
	fields := strings.Fields(line)
	if len(fields) < 9 || fields[0] != "cpu" {
		return cpuTimes{}
	}
	values := make([]uint64, 8)
	for index := range values {
		values[index], _ = strconv.ParseUint(fields[index+1], 10, 64)
	}
	return cpuTimes{user: values[0], nice: values[1], system: values[2], idle: values[3], iowait: values[4], irq: values[5], softirq: values[6], steal: values[7]}
}

func (c cpuTimes) total() uint64 {
	return c.user + c.nice + c.system + c.idle + c.iowait + c.irq + c.softirq + c.steal
}

func (c cpuTimes) stealPercent() float64 {
	if c.total() == 0 {
		return 0
	}
	return 100 * float64(c.steal) / float64(c.total())
}

func deltaStealPercent(before, after cpuTimes) float64 {
	totalBefore := before.total()
	totalAfter := after.total()
	if totalAfter <= totalBefore || after.steal < before.steal {
		return 0
	}
	return 100 * float64(after.steal-before.steal) / float64(totalAfter-totalBefore)
}
