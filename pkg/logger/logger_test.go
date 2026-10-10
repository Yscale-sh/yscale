package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Verifies a log record is shipped to Loki with the right labels + line content.
func TestLokiShipping(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/loki/api/v1/push" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	log, closeFn := New(Options{Job: "test-job", Environment: "test-env", LokiURL: srv.URL})
	log.Info("hello", "k", "v")
	if err := closeFn(context.Background()); err != nil { // flushes the shipper
		t.Fatalf("close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) == 0 {
		t.Fatal("no Loki push received")
	}
	var payload struct {
		Streams []struct {
			Stream map[string]string `json:"stream"`
			Values [][2]string       `json:"values"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(bodies[0], &payload); err != nil {
		t.Fatalf("bad payload: %v", err)
	}
	if len(payload.Streams) == 0 || len(payload.Streams[0].Values) == 0 {
		t.Fatal("empty streams/values")
	}
	st := payload.Streams[0]
	if st.Stream["job"] != "test-job" || st.Stream["environment"] != "test-env" {
		t.Fatalf("bad labels: %v", st.Stream)
	}
	line := st.Values[0][1]
	if !strings.Contains(line, `"msg":"hello"`) || !strings.Contains(line, `"k":"v"`) {
		t.Fatalf("line missing fields: %s", line)
	}
}

func TestRedactsSensitiveAttrsBeforeEverySink(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	log, closeFn := New(Options{Job: "redaction", Environment: "test", LokiURL: srv.URL})
	log.With("api-key", "must-not-escape").Info("redact",
		"token", "must-not-escape",
		"credential_source", "configured out of band",
		"request", slog.GroupValue(
			slog.String("authorization", "must-not-escape"),
			slog.String("customer", "cust_test"),
		),
	)
	if err := closeFn(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) == 0 {
		t.Fatal("no Loki push received")
	}
	joined := string(bytes.Join(bodies, nil))
	if strings.Contains(joined, "must-not-escape") {
		t.Fatalf("sensitive value reached Loki: %s", joined)
	}
	for _, want := range []string{`\"api-key\":\"[REDACTED]\"`, `\"token\":\"[REDACTED]\"`, `\"authorization\":\"[REDACTED]\"`, `\"credential_source\":\"configured out of band\"`, `\"customer\":\"cust_test\"`} {
		if !strings.Contains(joined, want) {
			t.Errorf("redacted payload missing %s: %s", want, joined)
		}
	}
}

// Verifies logging concurrent with (and after) Close never panics — the shipper
// must drop late records, not send on a closed channel. Run under -race.
func TestCloseConcurrentWithWrites(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	log, closeFn := New(Options{Job: "race", Environment: "test", LokiURL: srv.URL})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				log.Info("spin", "j", j)
			}
		}()
	}
	if err := closeFn(context.Background()); err != nil { // races the writers above
		t.Fatalf("close: %v", err)
	}
	wg.Wait() // writes landing after Close must be silently dropped
}

func TestCloseFlushDoesNotInventEmptyRecords(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Streams []struct {
				Values [][2]string `json:"values"`
			} `json:"streams"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode Loki payload: %v", err)
		}
		mu.Lock()
		for _, stream := range payload.Streams {
			for _, value := range stream.Values {
				lines = append(lines, value[1])
			}
		}
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	log, closeFn := New(Options{Job: "flush", Environment: "test", LokiURL: srv.URL})
	for i := 0; i < 150; i++ {
		log.Info("queued", "sequence", i)
	}
	if err := closeFn(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(lines) != 150 {
		t.Fatalf("got %d Loki records after close, want 150", len(lines))
	}
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			t.Fatalf("record %d is empty", i)
		}
	}
}

// Verifies console-only mode (no LokiURL) doesn't error and the close is a no-op.
func TestConsoleOnly(t *testing.T) {
	log, closeFn := New(Options{Job: "x"})
	log.Info("no loki here")
	if err := closeFn(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}
