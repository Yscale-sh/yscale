package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/scoring"
)

type Paths struct {
	JSON     string
	Markdown string
	HTML     string
}

func Write(result model.Result, directory string) (Paths, error) {
	if directory == "" {
		directory = "results"
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return Paths{}, fmt.Errorf("create report directory: %w", err)
	}
	base := filepath.Join(directory, result.RunID)
	paths := Paths{JSON: base + ".json", Markdown: base + ".md", HTML: base + ".html"}
	jsonData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return Paths{}, fmt.Errorf("encode JSON report: %w", err)
	}
	jsonData = append(jsonData, '\n')
	markdown := []byte(Markdown(result))
	htmlData, err := HTML(result)
	if err != nil {
		return Paths{}, err
	}
	files := []struct {
		path string
		data []byte
	}{
		{paths.JSON, jsonData},
		{paths.Markdown, markdown},
		{paths.HTML, htmlData},
		{filepath.Join(directory, "latest.json"), jsonData},
		{filepath.Join(directory, "latest.md"), markdown},
		{filepath.Join(directory, "latest.html"), htmlData},
	}
	for _, file := range files {
		if err := atomicWrite(file.path, file.data, 0o644); err != nil {
			return Paths{}, err
		}
	}
	return paths, nil
}

func Markdown(result model.Result) string {
	var out strings.Builder
	fmt.Fprintf(&out, "# Yscale Kubernetes Benchmark — `%s`\n\n", result.RunID)
	fmt.Fprintf(&out, "- **Started:** %s\n", result.StartedAt.Format(time.RFC3339))
	fmt.Fprintf(&out, "- **Duration:** %s\n", result.FinishedAt.Sub(result.StartedAt).Round(time.Millisecond))
	fmt.Fprintf(&out, "- **Cluster:** %d nodes, Kubernetes %s\n", result.Cluster.NodeCount, valueOr(result.Cluster.KubernetesServerVersion, "unknown"))
	fmt.Fprintf(&out, "- **Namespace:** `%s`\n\n", result.Cluster.Namespace)

	out.WriteString("## Scores\n\n")
	out.WriteString("| Overall | Reaction | Throughput | Efficiency | Version |\n")
	out.WriteString("|---:|---:|---:|---:|---|\n")
	fmt.Fprintf(&out, "| %s | %s | %s | %s | `%s` |\n\n", scoring.Format(result.Scores.Overall), scoring.Format(result.Scores.Reaction), scoring.Format(result.Scores.Throughput), scoring.Format(result.Scores.Efficiency), result.Scores.Version)

	out.WriteString("## Nodes\n\n")
	out.WriteString("| Node | Arch | Visible CPU | Physical host | Environment | vCPU | Memory | Price | Peak W |\n")
	out.WriteString("|---|---|---|---|---|---:|---:|---:|---:|\n")
	for _, node := range result.Nodes {
		physical := node.Effective.PhysicalHost.CPUModel.Value
		if node.Effective.PhysicalHost.Name.Value != "" {
			physical = node.Effective.PhysicalHost.Name.Value + valuePrefix(physical, " / ")
		}
		environment := node.Effective.Guest.Type.Value
		if platform := node.Effective.PhysicalHost.Platform.Value; platform != "" {
			environment += valuePrefix(platform, " / ")
		}
		fmt.Fprintf(&out, "| %s | %s | %s | %s | %s | %d | %s | %s | %s |\n",
			escapePipe(node.Name), escapePipe(node.Inventory.Architecture), escapePipe(node.Inventory.CPU.VisibleCPUModel),
			escapePipe(valueOr(physical, "unknown")), escapePipe(valueOr(environment, "unknown")), node.Effective.Guest.VCPUs.Value,
			formatBytes(node.Effective.Guest.MemoryBytes), formatMoney(node.Effective.Economics.PurchasePriceUSD), formatNumber(node.Effective.Economics.PeakWatts, 0))
	}
	out.WriteString("\n")

	if result.Reaction != nil {
		out.WriteString("## Reaction\n\n")
		out.WriteString("| Metric | p50 | p95 | p99 |\n|---|---:|---:|---:|\n")
		writeDistributionRow(&out, "Cached pod ready", result.Reaction.Summary.CachedReadyMS)
		writeDistributionRow(&out, "Pull-path pod ready", result.Reaction.Summary.PullReadyMS)
		writeDistributionRow(&out, "Cluster wave all ready", result.Reaction.Summary.ClusterAllReadyMS)
		out.WriteString("\n")
	}
	if result.Compute != nil {
		out.WriteString("## Compute\n\n")
		out.WriteString("| Node | Single integer ops/s | All-core integer ops/s | Single SHA-256 MiB/s | All-core SHA-256 MiB/s | Memory copy MiB/s | Steal % |\n")
		out.WriteString("|---|---:|---:|---:|---:|---:|---:|\n")
		for _, value := range result.Compute.Isolated {
			fmt.Fprintf(&out, "| %s | %s | %s | %s | %s | %s | %.2f |\n", escapePipe(value.Node), formatNumber(value.SingleIntegerOpsPerSec, 0), formatNumber(value.AllIntegerOpsPerSec, 0), formatNumber(value.SingleSHA256MiBPerSec, 1), formatNumber(value.AllSHA256MiBPerSec, 1), formatNumber(value.MemoryCopyMiBPerSec, 1), value.StealPercentAfter)
		}
		out.WriteString("\n| Cluster nodes | Aggregate integer ops/s | Expected sum | Scaling efficiency | Wall time |\n|---:|---:|---:|---:|---:|\n")
		for _, wave := range result.Compute.ClusterWaves {
			fmt.Fprintf(&out, "| %d | %s | %s | %.1f%% | %.2fs |\n", wave.NodeCount, formatNumber(wave.AggregateIntegerOpsPerSec, 0), formatNumber(wave.ExpectedIntegerOpsPerSec, 0), wave.ScalingEfficiency*100, wave.WallSeconds)
		}
		out.WriteString("\n")
	}
	if result.Network != nil {
		out.WriteString("## Network\n\n")
		out.WriteString("| Path | Source | Destination | Upload Mbit/s | Download Mbit/s | TCP p99 ms | UDP p99 ms | UDP loss |\n")
		out.WriteString("|---|---|---|---:|---:|---:|---:|---:|\n")
		for _, value := range result.Network.Results {
			fmt.Fprintf(&out, "| %s | %s | %s | %s | %s | %.3f | %.3f | %.2f%% |\n", escapePipe(value.Path), escapePipe(value.SourceNode), escapePipe(valueOr(value.DestinationNode, "service")), formatNumber(value.TCPUploadMbps, 1), formatNumber(value.TCPDownloadMbps, 1), value.TCPRTTMS.P99, value.UDPRTTMS.P99, value.UDPLossPercent)
		}
		out.WriteString("\n")
	}
	if result.DNS != nil {
		out.WriteString("## DNS\n\n")
		out.WriteString("| Node | Name | Protocol | p50 ms | p95 ms | p99 ms | QPS | Failures |\n")
		out.WriteString("|---|---|---|---:|---:|---:|---:|---:|\n")
		for _, value := range result.DNS.Results {
			fmt.Fprintf(&out, "| %s | `%s` | %s | %.3f | %.3f | %.3f | %.1f | %d |\n", escapePipe(value.Node), value.Name, value.Protocol, value.LatencyMS.P50, value.LatencyMS.P95, value.LatencyMS.P99, value.QueriesPerSec, value.Failures)
		}
		out.WriteString("\n")
	}
	if result.Storage != nil {
		out.WriteString("## Storage\n\n")
		out.WriteString("| Node | Target | Profile | Read MB/s | Write MB/s | Read IOPS | Write IOPS | Read p99 ms | Write/fsync p99 ms | Engine | Direct |\n")
		out.WriteString("|---|---|---|---:|---:|---:|---:|---:|---:|---|---|\n")
		for _, target := range result.Storage.Results {
			for _, value := range target.Profiles {
				writeP99 := value.WriteP99MS
				if value.FsyncP99MS > 0 {
					writeP99 = value.FsyncP99MS
				}
				fmt.Fprintf(&out, "| %s | %s | %s | %s | %s | %s | %s | %.3f | %.3f | %s | %t |\n", escapePipe(target.Node), escapePipe(target.Target), escapePipe(value.Name), formatNumber(value.ReadMBps, 1), formatNumber(value.WriteMBps, 1), formatNumber(value.ReadIOPS, 0), formatNumber(value.WriteIOPS, 0), value.ReadP99MS, writeP99, value.Engine, value.Direct)
			}
		}
		out.WriteString("\n")
	}
	if result.Transcode != nil {
		out.WriteString("## Transcoding\n\n")
		out.WriteString("| Node | Profile | Encoder | Hardware | Encode FPS | Realtime | Wall | Status |\n")
		out.WriteString("|---|---|---|---|---:|---:|---:|---|\n")
		for _, value := range result.Transcode.Results {
			fmt.Fprintf(&out, "| %s | %s | `%s` | %t | %.1f | %.2fx | %.2fs | %s |\n", escapePipe(value.Node), escapePipe(value.Profile), value.Encoder, value.Hardware, value.EncodeFPS, value.RealtimeFactor, value.WallSeconds, value.Status)
		}
		out.WriteString("\n")
	}
	if len(result.Warnings) > 0 {
		out.WriteString("## Warnings\n\n")
		for _, warning := range result.Warnings {
			fmt.Fprintf(&out, "- %s\n", warning)
		}
		out.WriteString("\n")
	}
	out.WriteString("## Reproducibility\n\nThe complete configuration, raw node inventory, raw benchmark records, score baselines, and tool version are preserved in the JSON report.\n")
	return out.String()
}

func HTML(result model.Result) ([]byte, error) {
	const document = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Yscale Kubernetes Benchmark {{.RunID}}</title>
<style>
:root{font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#111827;background:#f3f4f6}body{margin:0}main{max-width:1200px;margin:0 auto;padding:32px 20px 64px}.hero,.card{background:white;border:1px solid #e5e7eb;border-radius:14px;padding:22px;margin-bottom:18px;box-shadow:0 1px 2px rgba(0,0,0,.04)}h1{font-size:28px;margin:0 0 8px}h2{font-size:19px;margin:0 0 14px}.muted{color:#6b7280}.scores{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}.score{background:#f9fafb;border-radius:10px;padding:14px}.score strong{font-size:25px;display:block}table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:9px;border-bottom:1px solid #e5e7eb}th{color:#4b5563;font-weight:600}.scroll{overflow-x:auto}.status-ok{color:#047857}.status-failed{color:#b91c1c}.warning{padding:8px 0;border-bottom:1px solid #e5e7eb}@media(max-width:600px){main{padding:16px 10px}h1{font-size:22px}.hero,.card{padding:14px}}
</style></head><body><main>
<section class="hero"><h1>Yscale Kubernetes Benchmark</h1><div class="muted">{{.RunID}} · {{time .StartedAt}} · {{.Cluster.NodeCount}} nodes · {{.Cluster.KubernetesServerVersion}}</div></section>
<section class="card"><h2>Scores</h2><div class="scores"><div class="score"><span class="muted">Overall</span><strong>{{score .Scores.Overall}}</strong></div><div class="score"><span class="muted">Reaction</span><strong>{{score .Scores.Reaction}}</strong></div><div class="score"><span class="muted">Throughput</span><strong>{{score .Scores.Throughput}}</strong></div><div class="score"><span class="muted">Efficiency</span><strong>{{score .Scores.Efficiency}}</strong></div></div></section>
<section class="card"><h2>Nodes</h2><div class="scroll"><table><thead><tr><th>Node</th><th>Arch</th><th>Visible CPU</th><th>Physical host</th><th>Environment</th><th>vCPU</th><th>Memory</th></tr></thead><tbody>{{range .Nodes}}<tr><td>{{.Name}}</td><td>{{.Inventory.Architecture}}</td><td>{{.Inventory.CPU.VisibleCPUModel}}</td><td>{{.Effective.PhysicalHost.Name.Value}} {{.Effective.PhysicalHost.CPUModel.Value}}</td><td>{{.Effective.Guest.Type.Value}} / {{.Effective.PhysicalHost.Platform.Value}}</td><td>{{.Effective.Guest.VCPUs.Value}}</td><td>{{bytes .Effective.Guest.MemoryBytes}}</td></tr>{{end}}</tbody></table></div></section>
{{if .Reaction}}<section class="card"><h2>Reaction</h2><div class="scores"><div class="score"><span class="muted">Cached p95</span><strong>{{number .Reaction.Summary.CachedReadyMS.P95}} ms</strong></div><div class="score"><span class="muted">Pull path p95</span><strong>{{number .Reaction.Summary.PullReadyMS.P95}} ms</strong></div><div class="score"><span class="muted">Cluster all-ready</span><strong>{{number .Reaction.Summary.ClusterAllReadyMS.P50}} ms</strong></div></div></section>{{end}}
{{if .Compute}}<section class="card"><h2>Compute</h2><div class="scores"><div class="score"><span class="muted">Peak aggregate</span><strong>{{integer .Compute.PeakAggregate}}</strong></div><div class="score"><span class="muted">Scaling efficiency</span><strong>{{percent .Compute.ScalingEfficiency}}</strong></div></div><div class="scroll"><table><thead><tr><th>Node</th><th>Single ops/s</th><th>All-core ops/s</th><th>SHA MiB/s</th><th>Memory MiB/s</th></tr></thead><tbody>{{range .Compute.Isolated}}<tr><td>{{.Node}}</td><td>{{integer .SingleIntegerOpsPerSec}}</td><td>{{integer .AllIntegerOpsPerSec}}</td><td>{{number .AllSHA256MiBPerSec}}</td><td>{{number .MemoryCopyMiBPerSec}}</td></tr>{{end}}</tbody></table></div></section>{{end}}
{{if .Network}}<section class="card"><h2>Network</h2><div class="scores"><div class="score"><span class="muted">Cross-node median</span><strong>{{number .Network.Summary.CrossNodeTCPMbps.P50}} Mbit/s</strong></div><div class="score"><span class="muted">Cross-node RTT p95</span><strong>{{number .Network.Summary.CrossNodeRTTMS.P95}} ms</strong></div></div></section>{{end}}
{{if .DNS}}<section class="card"><h2>DNS</h2><div class="scores"><div class="score"><span class="muted">Service p99</span><strong>{{number .DNS.Summary.ServiceP99MS.P95}} ms</strong></div><div class="score"><span class="muted">Failures</span><strong>{{.DNS.Summary.FailureCount}}</strong></div></div></section>{{end}}
{{if .Storage}}<section class="card"><h2>Storage</h2><div class="scores"><div class="score"><span class="muted">Sequential read</span><strong>{{number .Storage.Summary.SequentialReadMBps.P50}} MB/s</strong></div><div class="score"><span class="muted">Random read</span><strong>{{integer .Storage.Summary.RandomReadIOPS.P50}} IOPS</strong></div><div class="score"><span class="muted">fsync p99</span><strong>{{number .Storage.Summary.FsyncP99MS.P95}} ms</strong></div></div></section>{{end}}
{{if .Transcode}}<section class="card"><h2>Transcoding</h2><div class="scores"><div class="score"><span class="muted">CPU median</span><strong>{{number .Transcode.Summary.CPURealtimeFactor.P50}}x</strong></div><div class="score"><span class="muted">GPU median</span><strong>{{number .Transcode.Summary.GPURealtimeFactor.P50}}x</strong></div></div></section>{{end}}
{{if .Warnings}}<section class="card"><h2>Warnings</h2>{{range .Warnings}}<div class="warning">{{.}}</div>{{end}}</section>{{end}}
</main></body></html>`
	functions := template.FuncMap{
		"time":    func(value time.Time) string { return value.Format(time.RFC3339) },
		"score":   scoring.Format,
		"bytes":   formatBytes,
		"number":  func(value float64) string { return formatNumber(value, 2) },
		"integer": func(value float64) string { return formatNumber(value, 0) },
		"percent": func(value float64) string { return fmt.Sprintf("%.1f%%", value*100) },
	}
	parsed, err := template.New("report").Funcs(functions).Parse(document)
	if err != nil {
		return nil, fmt.Errorf("parse HTML report template: %w", err)
	}
	var output bytes.Buffer
	if err := parsed.Execute(&output, result); err != nil {
		return nil, fmt.Errorf("render HTML report: %w", err)
	}
	return output.Bytes(), nil
}

func ConsoleSummary(result model.Result) string {
	var out strings.Builder
	fmt.Fprintf(&out, "Yscale Kubernetes Benchmark %s\n", result.RunID)
	fmt.Fprintf(&out, "Nodes: %d (%s)\n", result.Cluster.NodeCount, mapSummary(result.Cluster.Architectures))
	fmt.Fprintf(&out, "Scores: overall=%s reaction=%s throughput=%s efficiency=%s\n", scoring.Format(result.Scores.Overall), scoring.Format(result.Scores.Reaction), scoring.Format(result.Scores.Throughput), scoring.Format(result.Scores.Efficiency))
	if result.Reaction != nil {
		fmt.Fprintf(&out, "Reaction: cached ready p95 %.1f ms; cluster all-ready %.1f ms\n", result.Reaction.Summary.CachedReadyMS.P95, result.Reaction.Summary.ClusterAllReadyMS.P50)
	}
	if result.Compute != nil {
		fmt.Fprintf(&out, "Compute: peak %.0f integer ops/s; scaling %.1f%%\n", result.Compute.PeakAggregate, result.Compute.ScalingEfficiency*100)
	}
	if result.Network != nil {
		fmt.Fprintf(&out, "Network: cross-node median %.1f Mbit/s; p95-of-p99 RTT %.3f ms\n", result.Network.Summary.CrossNodeTCPMbps.P50, result.Network.Summary.CrossNodeRTTMS.P95)
	}
	if result.Storage != nil {
		fmt.Fprintf(&out, "Storage: sequential read median %.1f MB/s; random read median %.0f IOPS\n", result.Storage.Summary.SequentialReadMBps.P50, result.Storage.Summary.RandomReadIOPS.P50)
	}
	if result.Transcode != nil {
		fmt.Fprintf(&out, "Transcode: CPU median %.2fx; GPU median %.2fx realtime\n", result.Transcode.Summary.CPURealtimeFactor.P50, result.Transcode.Summary.GPURealtimeFactor.P50)
	}
	return out.String()
}

func writeDistributionRow(out *strings.Builder, name string, value model.Distribution) {
	fmt.Fprintf(out, "| %s | %.3f %s | %.3f %s | %.3f %s |\n", name, value.P50, value.Unit, value.P95, value.Unit, value.P99, value.Unit)
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, mode); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}

func formatBytes(value int64) string {
	if value <= 0 {
		return "n/a"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	amount := float64(value)
	index := 0
	for amount >= 1024 && index < len(units)-1 {
		amount /= 1024
		index++
	}
	return fmt.Sprintf("%.1f %s", amount, units[index])
}

func formatNumber(value float64, decimals int) string {
	if value == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.*f", decimals, value)
}

func formatMoney(value float64) string {
	if value <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("$%.0f", value)
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func valuePrefix(value, prefix string) string {
	if value == "" {
		return ""
	}
	return prefix + value
}

func escapePipe(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
}

func mapSummary(values map[string]int) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, values[key]))
	}
	return strings.Join(parts, ", ")
}
