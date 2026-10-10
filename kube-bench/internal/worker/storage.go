package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

type fioOutput struct {
	Jobs []struct {
		Read  fioDirection `json:"read"`
		Write fioDirection `json:"write"`
		Sync  fioSync      `json:"sync"`
	} `json:"jobs"`
}

type fioDirection struct {
	BWBytes float64    `json:"bw_bytes"`
	IOPS    float64    `json:"iops"`
	ClatNS  fioLatency `json:"clat_ns"`
	LatNS   fioLatency `json:"lat_ns"`
}

type fioSync struct {
	LatNS fioLatency `json:"lat_ns"`
}

type fioLatency struct {
	Percentile map[string]float64 `json:"percentile"`
}

type fioProfile struct {
	Name    string
	RW      string
	BS      string
	Depth   int
	Fsync   bool
	ReadMix int
}

func runStorageCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("worker storage", flag.ContinueOnError)
	flags.SetOutput(stderr)
	node := flags.String("node", os.Getenv("NODE_NAME"), "node name")
	target := flags.String("target", "ephemeral", "target name")
	kind := flags.String("kind", "emptyDir", "target kind")
	path := flags.String("path", "/bench/ephemeral", "target path")
	deviceHint := flags.String("device-hint", "", "device path description")
	duration := flags.Duration("duration", 5*time.Second, "duration per profile")
	sizeMiB := flags.Int("size-mib", 256, "test file size in MiB")
	engine := flags.String("engine", "io_uring", "preferred fio engine")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	result := benchmarkStorage(nodeName(*node), *target, *kind, *path, *deviceHint, *duration, *sizeMiB, *engine)
	return writeJSON(stdout, result)
}

func benchmarkStorage(node, target, kind, path, deviceHint string, duration time.Duration, sizeMiB int, preferredEngine string) model.StorageResult {
	result := model.StorageResult{
		Node:       node,
		Target:     target,
		Kind:       kind,
		Path:       path,
		DeviceHint: deviceHint,
		Status:     "ok",
	}
	fioPath, err := exec.LookPath("fio")
	if err != nil {
		result.Status = "skipped"
		result.Error = "fio is not installed in the worker image"
		return result
	}
	if sizeMiB < 16 {
		result.Status = "failed"
		result.Error = "size-mib must be at least 16"
		return result
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("create target directory: %v", err)
		return result
	}
	testFile := filepath.Join(path, ".yscale-kube-bench-fio.dat")
	defer os.Remove(testFile)

	profiles := []fioProfile{
		{Name: "sequential-write", RW: "write", BS: "1M", Depth: 32},
		{Name: "sequential-read", RW: "read", BS: "1M", Depth: 32},
		{Name: "random-read-4k", RW: "randread", BS: "4k", Depth: 64},
		{Name: "random-write-4k", RW: "randwrite", BS: "4k", Depth: 64},
		{Name: "mixed-70r-30w-4k", RW: "randrw", BS: "4k", Depth: 64, ReadMix: 70},
		{Name: "fsync-4k", RW: "write", BS: "4k", Depth: 1, Fsync: true},
	}
	for _, profile := range profiles {
		profileResult := runFIOProfile(fioPath, testFile, duration, sizeMiB, preferredEngine, profile)
		result.Profiles = append(result.Profiles, profileResult)
		if profileResult.Status != "ok" {
			if result.Status == "ok" {
				result.Status = "partial"
			}
			if result.Error != "" {
				result.Error += "; "
			}
			result.Error += profile.Name + ": " + profileResult.Error
		}
	}
	if len(result.Profiles) == 0 {
		result.Status = "failed"
		result.Error = "no fio profiles completed"
	}
	return result
}

func runFIOProfile(fioPath, filename string, duration time.Duration, sizeMiB int, preferredEngine string, profile fioProfile) model.FIOProfileResult {
	engines := uniqueStrings([]string{preferredEngine, "libaio", "psync"})
	var lastError string
	for _, engine := range engines {
		for _, direct := range []bool{true, false} {
			output, commandError := invokeFIO(fioPath, filename, duration, sizeMiB, engine, direct, profile)
			if commandError != nil {
				lastError = commandError.Error()
				continue
			}
			parsed, parseError := parseFIO(profile, engine, direct, output)
			if parseError != nil {
				lastError = parseError.Error()
				continue
			}
			return parsed
		}
	}
	return model.FIOProfileResult{Name: profile.Name, RW: profile.RW, Status: "failed", Error: lastError}
}

func invokeFIO(fioPath, filename string, duration time.Duration, sizeMiB int, engine string, direct bool, profile fioProfile) ([]byte, error) {
	seconds := int(duration.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	arguments := []string{
		"--output-format=json",
		"--name=" + profile.Name,
		"--filename=" + filename,
		"--rw=" + profile.RW,
		"--bs=" + profile.BS,
		"--iodepth=" + strconv.Itoa(profile.Depth),
		"--ioengine=" + engine,
		"--direct=" + boolInt(direct),
		"--size=" + strconv.Itoa(sizeMiB) + "M",
		"--runtime=" + strconv.Itoa(seconds),
		"--time_based=1",
		"--group_reporting=1",
		"--randrepeat=0",
		"--invalidate=1",
	}
	if profile.ReadMix > 0 {
		arguments = append(arguments, "--rwmixread="+strconv.Itoa(profile.ReadMix))
	}
	if profile.Fsync {
		arguments = append(arguments, "--fsync=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration+90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, fioPath, arguments...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return nil, fmt.Errorf("fio engine=%s direct=%t: %s", engine, direct, message)
	}
	return stdout.Bytes(), nil
}

func parseFIO(profile fioProfile, engine string, direct bool, data []byte) (model.FIOProfileResult, error) {
	var output fioOutput
	if err := json.Unmarshal(data, &output); err != nil {
		return model.FIOProfileResult{}, fmt.Errorf("decode fio JSON: %w", err)
	}
	if len(output.Jobs) == 0 {
		return model.FIOProfileResult{}, fmt.Errorf("fio returned no jobs")
	}
	job := output.Jobs[0]
	result := model.FIOProfileResult{
		Name:       profile.Name,
		RW:         profile.RW,
		Engine:     engine,
		Direct:     direct,
		ReadMBps:   job.Read.BWBytes / 1_000_000,
		WriteMBps:  job.Write.BWBytes / 1_000_000,
		ReadIOPS:   job.Read.IOPS,
		WriteIOPS:  job.Write.IOPS,
		ReadP50MS:  fioPercentile(job.Read, "50.000000") / 1_000_000,
		ReadP95MS:  fioPercentile(job.Read, "95.000000") / 1_000_000,
		ReadP99MS:  fioPercentile(job.Read, "99.000000") / 1_000_000,
		WriteP50MS: fioPercentile(job.Write, "50.000000") / 1_000_000,
		WriteP95MS: fioPercentile(job.Write, "95.000000") / 1_000_000,
		WriteP99MS: fioPercentile(job.Write, "99.000000") / 1_000_000,
		Status:     "ok",
	}
	if profile.Fsync {
		result.FsyncP99MS = percentileValue(job.Sync.LatNS.Percentile, "99.000000") / 1_000_000
		if result.FsyncP99MS == 0 {
			result.FsyncP99MS = result.WriteP99MS
		}
	}
	return result, nil
}

func fioPercentile(direction fioDirection, key string) float64 {
	if value := percentileValue(direction.ClatNS.Percentile, key); value != 0 {
		return value
	}
	return percentileValue(direction.LatNS.Percentile, key)
}

func percentileValue(values map[string]float64, key string) float64 {
	if values == nil {
		return 0
	}
	if value, exists := values[key]; exists {
		return value
	}
	trimmedKey := strings.TrimRight(strings.TrimRight(key, "0"), ".")
	for candidate, value := range values {
		trimmedCandidate := strings.TrimRight(strings.TrimRight(candidate, "0"), ".")
		if trimmedCandidate == trimmedKey {
			return value
		}
	}
	return 0
}

func boolInt(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
