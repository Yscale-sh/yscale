package worker

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/stats"
)

const networkPayloadSize = 64

func runNetworkServerCommand(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("worker net-server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	tcpListen := flags.String("tcp-listen", ":9090", "TCP listen address")
	udpListen := flags.String("udp-listen", ":9091", "UDP listen address")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if err := serveNetwork(*tcpListen, *udpListen, stderr); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func runNetworkClientCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("worker net-client", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sourceNode := flags.String("source-node", os.Getenv("NODE_NAME"), "source node")
	destinationNode := flags.String("destination-node", "", "destination node")
	path := flags.String("path", "direct", "network path")
	tcpTarget := flags.String("tcp-target", "", "TCP target")
	udpTarget := flags.String("udp-target", "", "UDP target")
	duration := flags.Duration("duration", 5*time.Second, "throughput duration")
	rttSamples := flags.Int("rtt-samples", 200, "TCP RTT samples")
	udpSamples := flags.Int("udp-samples", 200, "UDP samples")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	result := benchmarkNetwork(nodeName(*sourceNode), *destinationNode, *path, *tcpTarget, *udpTarget, *duration, *rttSamples, *udpSamples)
	return writeJSON(stdout, result)
}

func serveNetwork(tcpAddress, udpAddress string, stderr io.Writer) error {
	tcpListener, err := net.Listen("tcp", tcpAddress)
	if err != nil {
		return fmt.Errorf("listen TCP: %w", err)
	}
	defer tcpListener.Close()
	udpPacket, err := net.ListenPacket("udp", udpAddress)
	if err != nil {
		return fmt.Errorf("listen UDP: %w", err)
	}
	defer udpPacket.Close()

	errorsChannel := make(chan error, 2)
	go func() { errorsChannel <- serveTCP(tcpListener) }()
	go func() { errorsChannel <- serveUDP(udpPacket) }()
	err = <-errorsChannel
	if err != nil {
		fmt.Fprintln(stderr, err)
	}
	return err
}

func serveTCP(listener net.Listener) error {
	for {
		connection, err := listener.Accept()
		if err != nil {
			return err
		}
		go handleTCPConnection(connection)
	}
}

func handleTCPConnection(connection net.Conn) {
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(10 * time.Minute))
	var mode [1]byte
	if _, err := io.ReadFull(connection, mode[:]); err != nil {
		return
	}
	switch mode[0] {
	case 'E':
		buffer := make([]byte, networkPayloadSize)
		for {
			if _, err := io.ReadFull(connection, buffer); err != nil {
				return
			}
			if _, err := connection.Write(buffer); err != nil {
				return
			}
		}
	case 'U':
		_, _ = io.Copy(io.Discard, connection)
	case 'D':
		var raw [8]byte
		if _, err := io.ReadFull(connection, raw[:]); err != nil {
			return
		}
		duration := time.Duration(binary.BigEndian.Uint64(raw[:])) * time.Millisecond
		if duration <= 0 || duration > 10*time.Minute {
			return
		}
		deadline := time.Now().Add(duration)
		buffer := make([]byte, 1024*1024)
		for time.Now().Before(deadline) {
			_ = connection.SetWriteDeadline(deadline.Add(time.Second))
			if _, err := connection.Write(buffer); err != nil {
				return
			}
		}
	}
}

func serveUDP(connection net.PacketConn) error {
	buffer := make([]byte, 2048)
	for {
		length, address, err := connection.ReadFrom(buffer)
		if err != nil {
			return err
		}
		if _, err := connection.WriteTo(buffer[:length], address); err != nil {
			return err
		}
	}
}

func benchmarkNetwork(sourceNode, destinationNode, path, tcpTarget, udpTarget string, duration time.Duration, rttSamples, udpSamples int) model.NetworkResult {
	result := model.NetworkResult{
		SourceNode:      sourceNode,
		DestinationNode: destinationNode,
		Path:            path,
		Target:          tcpTarget,
		Status:          "ok",
	}
	var problems []string
	if tcpTarget == "" {
		problems = append(problems, "TCP target is required")
	} else if err := waitForTCP(tcpTarget, 15*time.Second); err != nil {
		problems = append(problems, "TCP readiness: "+err.Error())
	} else {
		latencies, err := tcpRTT(tcpTarget, rttSamples)
		if err != nil {
			problems = append(problems, "TCP RTT: "+err.Error())
		} else {
			result.TCPRTTMS = stats.Distribution(latencies, "ms")
		}
		if throughput, err := tcpUpload(tcpTarget, duration); err != nil {
			problems = append(problems, "TCP upload: "+err.Error())
		} else {
			result.TCPUploadMbps = throughput
		}
		if throughput, err := tcpDownload(tcpTarget, duration); err != nil {
			problems = append(problems, "TCP download: "+err.Error())
		} else {
			result.TCPDownloadMbps = throughput
		}
	}
	if udpTarget != "" {
		latencies, loss, jitter, err := udpRTT(udpTarget, udpSamples)
		if err != nil {
			problems = append(problems, "UDP RTT: "+err.Error())
		} else {
			result.UDPRTTMS = stats.Distribution(latencies, "ms")
			result.UDPLossPercent = loss
			result.UDPJitterMS = jitter
		}
	}
	if len(problems) > 0 {
		result.Error = joinErrors(problems)
		if result.TCPUploadMbps == 0 && result.TCPDownloadMbps == 0 && result.TCPRTTMS.Count == 0 {
			result.Status = "failed"
		} else {
			result.Status = "partial"
		}
	}
	return result
}

func waitForTCP(address string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if lastErr == nil {
				return fmt.Errorf("connect to %s within %s", address, timeout)
			}
			return fmt.Errorf("connect to %s within %s: %w", address, timeout, lastErr)
		}
		attemptTimeout := remaining
		if attemptTimeout > 500*time.Millisecond {
			attemptTimeout = 500 * time.Millisecond
		}
		connection, err := net.DialTimeout("tcp", address, attemptTimeout)
		if err == nil {
			_ = connection.Close()
			return nil
		}
		lastErr = err
		delay := 100 * time.Millisecond
		if remaining < delay {
			delay = remaining
		}
		time.Sleep(delay)
	}
}

func tcpRTT(target string, samples int) ([]float64, error) {
	if samples < 1 {
		samples = 1
	}
	connection, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	if _, err := connection.Write([]byte{'E'}); err != nil {
		return nil, err
	}
	payload := make([]byte, networkPayloadSize)
	response := make([]byte, networkPayloadSize)
	values := make([]float64, 0, samples)
	for index := 0; index < samples; index++ {
		binary.BigEndian.PutUint64(payload, uint64(index))
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
		start := time.Now()
		if _, err := connection.Write(payload); err != nil {
			return values, err
		}
		if _, err := io.ReadFull(connection, response); err != nil {
			return values, err
		}
		values = append(values, float64(time.Since(start).Microseconds())/1000)
	}
	return values, nil
}

func tcpUpload(target string, duration time.Duration) (float64, error) {
	connection, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		return 0, err
	}
	defer connection.Close()
	if _, err := connection.Write([]byte{'U'}); err != nil {
		return 0, err
	}
	buffer := make([]byte, 1024*1024)
	deadline := time.Now().Add(duration)
	start := time.Now()
	var bytesWritten int64
	for time.Now().Before(deadline) {
		_ = connection.SetWriteDeadline(deadline.Add(time.Second))
		count, writeErr := connection.Write(buffer)
		bytesWritten += int64(count)
		if writeErr != nil {
			if isTimeout(writeErr) {
				break
			}
			return 0, writeErr
		}
	}
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return 0, nil
	}
	return float64(bytesWritten) * 8 / 1_000_000 / elapsed, nil
}

func tcpDownload(target string, duration time.Duration) (float64, error) {
	connection, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		return 0, err
	}
	defer connection.Close()
	request := make([]byte, 9)
	request[0] = 'D'
	binary.BigEndian.PutUint64(request[1:], uint64(duration.Milliseconds()))
	if _, err := connection.Write(request); err != nil {
		return 0, err
	}
	buffer := make([]byte, 1024*1024)
	deadline := time.Now().Add(duration + 2*time.Second)
	_ = connection.SetReadDeadline(deadline)
	start := time.Now()
	var bytesRead int64
	for {
		count, readErr := connection.Read(buffer)
		bytesRead += int64(count)
		if readErr != nil {
			if errors.Is(readErr, io.EOF) || isTimeout(readErr) {
				break
			}
			return 0, readErr
		}
	}
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return 0, nil
	}
	return float64(bytesRead) * 8 / 1_000_000 / elapsed, nil
}

func udpRTT(target string, samples int) ([]float64, float64, float64, error) {
	if samples < 1 {
		samples = 1
	}
	connection, err := net.DialTimeout("udp", target, 5*time.Second)
	if err != nil {
		return nil, 100, 0, err
	}
	defer connection.Close()
	payload := make([]byte, networkPayloadSize)
	response := make([]byte, networkPayloadSize)
	values := make([]float64, 0, samples)
	for index := 0; index < samples; index++ {
		binary.BigEndian.PutUint64(payload, uint64(index))
		_ = connection.SetDeadline(time.Now().Add(500 * time.Millisecond))
		start := time.Now()
		if _, err := connection.Write(payload); err != nil {
			continue
		}
		if _, err := io.ReadFull(connection, response); err != nil {
			continue
		}
		values = append(values, float64(time.Since(start).Microseconds())/1000)
	}
	loss := 100 * float64(samples-len(values)) / float64(samples)
	if len(values) == 0 {
		return values, loss, 0, errors.New("no UDP responses")
	}
	var differences []float64
	for index := 1; index < len(values); index++ {
		difference := values[index] - values[index-1]
		if difference < 0 {
			difference = -difference
		}
		differences = append(differences, difference)
	}
	sort.Float64s(differences)
	var jitter float64
	for _, value := range differences {
		jitter += value
	}
	if len(differences) > 0 {
		jitter /= float64(len(differences))
	}
	return values, loss, jitter, nil
}

func isTimeout(err error) bool {
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

func joinErrors(values []string) string {
	var result string
	for index, value := range values {
		if index > 0 {
			result += "; "
		}
		result += value
	}
	return result
}
