// Package logger provides a slog-based structured logger for yscale-sh services:
// structured JSON to stderr plus best-effort async shipping to Loki, mirroring
// the yscale-sh loki-logger pattern (see openprophet-saas/api/logger.js) but
// idiomatic Go built on log/slog — so existing slog.Info/Warn/Error calls keep
// working and gain Loki shipping by installing this as the default handler.
//
// Usage:
//
//	closeFn := logger.Setup(logger.Options{Job: "yscale"})
//	defer closeFn(context.Background())
//	slog.Info("server started", "addr", addr) // → stderr + Loki
//
// Configuration falls back to env: LOKI_URL (empty ⇒ console only) and
// ENVIRONMENT. The Loki shipper never blocks or panics the app: a full queue
// drops lines and transport errors are swallowed. Every sink is wrapped in a
// last-line-of-defense redactor so a mistakenly attached credential field is
// replaced before it reaches stderr or Loki.
package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Options configures a logger. Job is the Loki stream label and the only field
// most callers set; the rest fall back to env vars / sensible defaults.
type Options struct {
	Job         string     // Loki "job" label, e.g. "yscale" (default "app")
	Environment string     // default: $ENVIRONMENT, else Job+"-dev"
	LokiURL     string     // default: $LOKI_URL; empty ⇒ console only
	Level       slog.Level // default: slog.LevelInfo
	AddSource   bool       // include source file:line
}

// New builds an *slog.Logger and a close func that flushes the Loki shipper.
func New(opts Options) (*slog.Logger, func(context.Context) error) {
	if opts.Job == "" {
		opts.Job = "app"
	}
	environment := firstNonEmpty(opts.Environment, os.Getenv("ENVIRONMENT"), opts.Job+"-dev")
	lokiURL := firstNonEmpty(opts.LokiURL, os.Getenv("LOKI_URL"))

	hopts := &slog.HandlerOptions{Level: opts.Level, AddSource: opts.AddSource}
	handlers := []slog.Handler{redacting(slog.NewJSONHandler(os.Stderr, hopts))}

	noop := func(context.Context) error { return nil }
	if lokiURL == "" {
		return slog.New(fanout(handlers)), noop
	}

	shipper := newLokiShipper(lokiURL, opts.Job, environment)
	handlers = append(handlers, redacting(slog.NewJSONHandler(shipper, hopts)))
	return slog.New(fanout(handlers)), shipper.Close
}

// Setup builds the logger, installs it as slog's default, and returns the close
// func (call it on shutdown to flush in-flight Loki writes).
func Setup(opts Options) func(context.Context) error {
	l, closeFn := New(opts)
	slog.SetDefault(l)
	return closeFn
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// ── redaction: credentials never leave the process through structured attrs ─

const redactedValue = "[REDACTED]"

type redactingHandler struct {
	next slog.Handler
}

func redacting(next slog.Handler) slog.Handler { return redactingHandler{next: next} }

func (h redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h redactingHandler) Handle(ctx context.Context, record slog.Record) error {
	clean := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		clean.AddAttrs(redactAttr(attr))
		return true
	})
	return h.next.Handle(ctx, clean)
}

func (h redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, len(attrs))
	for i, attr := range attrs {
		clean[i] = redactAttr(attr)
	}
	return redactingHandler{next: h.next.WithAttrs(clean)}
}

func (h redactingHandler) WithGroup(name string) slog.Handler {
	return redactingHandler{next: h.next.WithGroup(name)}
}

func redactAttr(attr slog.Attr) slog.Attr {
	attr.Value = attr.Value.Resolve()
	if sensitiveLogKey(attr.Key) {
		return slog.String(attr.Key, redactedValue)
	}
	if attr.Value.Kind() != slog.KindGroup {
		return attr
	}
	children := attr.Value.Group()
	clean := make([]slog.Attr, len(children))
	for i, child := range children {
		clean[i] = redactAttr(child)
	}
	return slog.Group(attr.Key, attrsToAny(clean)...)
}

func attrsToAny(attrs []slog.Attr) []any {
	values := make([]any, len(attrs))
	for i := range attrs {
		values[i] = attrs[i]
	}
	return values
}

func sensitiveLogKey(key string) bool {
	normalized := strings.NewReplacer("-", "_", ".", "_").Replace(strings.ToLower(key))
	if normalized == "authorization" || normalized == "credentials" || normalized == "dsn" {
		return true
	}
	for _, fragment := range []string{
		"token", "password", "secret", "api_key", "apikey", "private_key",
	} {
		if strings.Contains(normalized, fragment) {
			return true
		}
	}
	return false
}

// ── fanout: deliver each record to several handlers (console + Loki) ──────────

type fanoutHandler []slog.Handler

func fanout(hs []slog.Handler) slog.Handler { return fanoutHandler(hs) }

func (f fanoutHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range f {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (f fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range f {
		if h.Enabled(ctx, r.Level) {
			_ = h.Handle(ctx, r.Clone()) // best-effort; one sink must not break others
		}
	}
	return nil
}

func (f fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make(fanoutHandler, len(f))
	for i, h := range f {
		out[i] = h.WithAttrs(attrs)
	}
	return out
}

func (f fanoutHandler) WithGroup(name string) slog.Handler {
	out := make(fanoutHandler, len(f))
	for i, h := range f {
		out[i] = h.WithGroup(name)
	}
	return out
}

// ── lokiShipper: an io.Writer that batches JSON log lines to Loki ─────────────

type lokiShipper struct {
	url string
	job string
	env string
	// mu+closed guard ch against send-on-closed-channel: service goroutines may
	// still log after Close runs during shutdown. Write holds RLock while
	// sending; Close takes the write lock to flip closed before close(ch).
	mu     sync.RWMutex
	closed bool
	ch     chan string
	wg     sync.WaitGroup
	once   sync.Once
}

func newLokiShipper(url, job, env string) *lokiShipper {
	s := &lokiShipper{
		url: strings.TrimRight(url, "/"),
		job: job,
		env: env,
		ch:  make(chan string, 1000),
	}
	s.wg.Add(1)
	go s.run()
	return s
}

// Write receives exactly one JSON record per call from slog's JSONHandler.
func (s *lokiShipper) Write(p []byte) (int, error) {
	line := string(bytes.TrimRight(p, "\n"))
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return len(p), nil // shipper stopped — drop; the stderr handler still has it
	}
	select {
	case s.ch <- line: // queued
	default: // queue full — drop rather than block the app
	}
	return len(p), nil
}

func (s *lokiShipper) run() {
	defer s.wg.Done()
	client := &http.Client{Timeout: 5 * time.Second}
	for line := range s.ch {
		batch := []string{line}
		// Opportunistically drain anything already buffered into one push.
	drain:
		for len(batch) < 100 {
			select {
			case more, ok := <-s.ch:
				if !ok {
					break drain
				}
				batch = append(batch, more)
			default:
				break drain
			}
		}
		s.push(client, batch)
	}
}

func (s *lokiShipper) push(client *http.Client, lines []string) {
	base := time.Now().UnixNano()
	values := make([][2]string, len(lines))
	for i, ln := range lines {
		values[i] = [2]string{strconv.FormatInt(base+int64(i), 10), ln} // unique, ordered ts
	}
	payload, err := json.Marshal(map[string]any{
		"streams": []map[string]any{{
			"stream": map[string]string{"job": s.job, "environment": s.env},
			"values": values,
		}},
	})
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, s.url+"/loki/api/v1/push", bytes.NewReader(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return // swallow: never break the app because Loki is down
	}
	_ = resp.Body.Close()
}

// Close stops the shipper and waits for in-flight writes to flush (bounded by ctx).
// Logging after Close is safe: records are dropped, never a panic.
func (s *lokiShipper) Close(ctx context.Context) error {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.ch)
		s.mu.Unlock()
	})
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
