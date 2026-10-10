package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/config"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

func runTranscodeCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("worker transcode", flag.ContinueOnError)
	flags.SetOutput(stderr)
	node := flags.String("node", os.Getenv("NODE_NAME"), "node name")
	profileJSON := flags.String("profile-json", os.Getenv("YSCALE_TRANSCODE_PROFILE"), "transcode profile JSON")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	var profile config.TranscodeProfile
	if err := json.Unmarshal([]byte(*profileJSON), &profile); err != nil {
		result := model.TranscodeResult{Node: nodeName(*node), Status: "failed", Error: "decode profile: " + err.Error()}
		return writeJSON(stdout, result)
	}
	result := benchmarkTranscode(nodeName(*node), profile)
	return writeJSON(stdout, result)
}

func benchmarkTranscode(node string, profile config.TranscodeProfile) model.TranscodeResult {
	result := model.TranscodeResult{
		Node:          node,
		Profile:       profile.Name,
		Encoder:       profile.Encoder,
		Width:         profile.Width,
		Height:        profile.Height,
		FPS:           profile.FPS,
		SourceSeconds: float64(profile.Seconds),
		Hardware:      profile.Hardware,
		Status:        "ok",
	}
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		result.Status = "skipped"
		result.Error = "ffmpeg is not installed in the worker image"
		return result
	}
	if !ffmpegHasEncoder(ffmpeg, profile.Encoder) {
		result.Status = "skipped"
		result.Error = fmt.Sprintf("ffmpeg encoder %q is not available", profile.Encoder)
		return result
	}
	filter := fmt.Sprintf("testsrc2=duration=%d:size=%dx%d:rate=%d", profile.Seconds, profile.Width, profile.Height, profile.FPS)
	arguments := []string{
		"-hide_banner",
		"-nostats",
		"-loglevel", "error",
		"-progress", "pipe:1",
		"-f", "lavfi",
		"-i", filter,
		"-an",
		"-c:v", profile.Encoder,
	}
	arguments = append(arguments, profile.Arguments...)
	arguments = append(arguments, "-f", "null", "-")
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(profile.Seconds)*20*time.Second+60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, ffmpeg, arguments...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	var errorBuffer bytes.Buffer
	command.Stderr = &errorBuffer
	start := time.Now()
	if err := command.Start(); err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	progress := map[string]string{}
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), "=")
		if found {
			progress[key] = value
		}
	}
	waitError := command.Wait()
	result.WallSeconds = time.Since(start).Seconds()
	result.Frames, _ = strconv.Atoi(progress["frame"])
	if result.Frames == 0 {
		result.Frames = profile.FPS * profile.Seconds
	}
	if result.WallSeconds > 0 {
		result.EncodeFPS = float64(result.Frames) / result.WallSeconds
		result.RealtimeFactor = float64(profile.Seconds) / result.WallSeconds
	}
	if waitError != nil {
		result.Status = "failed"
		result.Error = strings.TrimSpace(errorBuffer.String())
		if result.Error == "" {
			result.Error = waitError.Error()
		}
	}
	return result
}

func ffmpegHasEncoder(ffmpeg, encoder string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-encoders").CombinedOutput()
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == encoder {
			return true
		}
	}
	return false
}
