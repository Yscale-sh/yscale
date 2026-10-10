package worker

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/sysinfo"
)

func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "worker subcommand is required")
		return 2
	}
	switch args[0] {
	case "inventory":
		return runInventory(args[1:], stdout, stderr)
	case "compute":
		return runComputeCommand(args[1:], stdout, stderr)
	case "dns":
		return runDNSCommand(args[1:], stdout, stderr)
	case "net-server":
		return runNetworkServerCommand(args[1:], stderr)
	case "net-client":
		return runNetworkClientCommand(args[1:], stdout, stderr)
	case "storage":
		return runStorageCommand(args[1:], stdout, stderr)
	case "transcode":
		return runTranscodeCommand(args[1:], stdout, stderr)
	case "ready-server":
		return runReadyServerCommand(args[1:], stderr)
	case "noop":
		return runNoop(args[1:], stderr)
	default:
		fmt.Fprintf(stderr, "unknown worker subcommand %q\n", args[0])
		return 2
	}
}

func runInventory(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("worker inventory", flag.ContinueOnError)
	flags.SetOutput(stderr)
	node := flags.String("node", os.Getenv("NODE_NAME"), "Kubernetes node name")
	hostRoot := flags.String("host-root", os.Getenv("YSCALE_HOST_ROOT"), "mounted host root")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	result := sysinfo.Collect(*hostRoot)
	result.NodeName = *node
	return writeJSON(stdout, result)
}

func writeJSON(writer io.Writer, value any) int {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return 1
	}
	return 0
}

func nodeName(value string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	if value = os.Getenv("NODE_NAME"); value != "" {
		return value
	}
	hostname, _ := os.Hostname()
	return hostname
}

func durationFlag(raw string, fallback time.Duration) time.Duration {
	if raw == "" {
		return fallback
	}
	if value, err := time.ParseDuration(raw); err == nil {
		return value
	}
	if seconds, err := strconv.Atoi(raw); err == nil {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}

func failedCompute(node, errMessage string) model.NodeComputeResult {
	return model.NodeComputeResult{Node: node, Status: "failed", Error: errMessage}
}
