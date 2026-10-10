package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sync"
	"time"
)

// Dashboard is the optional live web view. It runs an HTTP server
// that streams test events to a browser via Server-Sent Events. One
// shared broker per yscaletest invocation; cases write events to it
// from their goroutines, the server fan-outs to all connected
// browsers. Headless mode (no -watch) keeps the stdout-only path.
//
// Why a dashboard and not a TUI: the operator wants to launch a run
// and watch it on a second screen / browser tab while doing other
// work. SSH-into-laptop or tmux-pane workflows are clunky for that.
// Single-port HTTP + SSE has zero install friction.
type Dashboard struct {
	Port int

	mu          sync.RWMutex
	cases       map[string]*CaseState // by Case.Name (stable across the run)
	runID       string
	startedAt   time.Time
	subscribers map[chan []byte]struct{}
}

// CaseState is the per-case snapshot rendered in the dashboard.
// Each field is the most recent known value for that case; the
// dashboard surface diffs old vs new on the browser side.
type CaseState struct {
	Name       string    `json:"name"`
	Phase      string    `json:"phase"` // CR Status.Phase or framework error
	Backend    string    `json:"backend"`
	BurstID    string    `json:"burstID"`
	PodName    string    `json:"podName"`
	PodPhase   string    `json:"podPhase"`
	NodeName   string    `json:"nodeName"`
	Tail       []string  `json:"tail"` // last N log lines from the pod
	Note       string    `json:"note"` // most recent trace note
	StartedAt  time.Time `json:"startedAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
	FinishedAt time.Time `json:"finishedAt,omitempty"`
	Err        string    `json:"err,omitempty"`
}

// NewDashboard constructs and starts the dashboard. Returns nil if
// port is 0 (i.e. -watch not set); callers should nil-check before
// emitting.
func NewDashboard(port int, runID string) *Dashboard {
	if port == 0 {
		return nil
	}
	d := &Dashboard{
		Port:        port,
		runID:       runID,
		startedAt:   time.Now(),
		cases:       map[string]*CaseState{},
		subscribers: map[chan []byte]struct{}{},
	}
	go d.serve()
	return d
}

// CaseStart records a case has begun. Browser sees a new card appear.
func (d *Dashboard) CaseStart(name string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.cases[name] = &CaseState{
		Name:      name,
		Phase:     "Starting",
		StartedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	d.mu.Unlock()
	d.broadcast()
}

// UpdateCase applies fn to the case's state under the lock, then
// broadcasts. Centralizing the read-modify-write makes it impossible
// to lose events under concurrent updates.
func (d *Dashboard) UpdateCase(name string, fn func(*CaseState)) {
	if d == nil {
		return
	}
	d.mu.Lock()
	c, ok := d.cases[name]
	if !ok {
		c = &CaseState{Name: name, StartedAt: time.Now()}
		d.cases[name] = c
	}
	fn(c)
	c.UpdatedAt = time.Now()
	d.mu.Unlock()
	d.broadcast()
}

// AppendLog tacks one line onto the case's tail buffer (cap 30 lines).
// Cheap call site for streaming pod stdout — the existing fmt.Printf
// `pod ▸` path becomes a single AppendLog.
func (d *Dashboard) AppendLog(name, line string) {
	d.UpdateCase(name, func(c *CaseState) {
		c.Tail = append(c.Tail, line)
		if len(c.Tail) > 30 {
			c.Tail = c.Tail[len(c.Tail)-30:]
		}
	})
}

// serve runs the HTTP server. Three endpoints: GET / (HTML page),
// GET /events (SSE stream), GET /state (one-shot JSON snapshot used
// on initial page load + as a fallback if SSE breaks).
func (d *Dashboard) serve() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", d.handleIndex)
	mux.HandleFunc("/state", d.handleState)
	mux.HandleFunc("/events", d.handleEvents)
	addr := fmt.Sprintf(":%d", d.Port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	fmt.Printf("yscaletest: dashboard at http://localhost:%d\n", d.Port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("dashboard server: %v", err)
	}
}

func (d *Dashboard) handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = indexTemplate.Execute(w, struct {
		RunID string
	}{d.runID})
}

func (d *Dashboard) snapshot() snapshotPayload {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := snapshotPayload{
		RunID:     d.runID,
		StartedAt: d.startedAt,
		Now:       time.Now(),
		Cases:     make([]CaseState, 0, len(d.cases)),
	}
	for _, c := range d.cases {
		out.Cases = append(out.Cases, *c)
	}
	return out
}

type snapshotPayload struct {
	RunID     string      `json:"runID"`
	StartedAt time.Time   `json:"startedAt"`
	Now       time.Time   `json:"now"`
	Cases     []CaseState `json:"cases"`
}

func (d *Dashboard) handleState(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(d.snapshot())
}

// handleEvents serves an SSE stream. Each broadcast() call produces
// one message; the browser's EventSource consumes them and the JS
// re-renders the page. Plain SSE (one `data:` line per event,
// blank line to flush) — no library, no framework.
func (d *Dashboard) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	ch := make(chan []byte, 32)
	d.mu.Lock()
	d.subscribers[ch] = struct{}{}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.subscribers, ch)
		d.mu.Unlock()
	}()
	// Send initial state so the page renders fast on reconnect.
	if payload, err := json.Marshal(d.snapshot()); err == nil {
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case payload, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}

// broadcast pushes the current snapshot to all subscribers. Drops
// events for slow consumers rather than blocking the producer —
// the next broadcast carries fresh state anyway.
func (d *Dashboard) broadcast() {
	payload, err := json.Marshal(d.snapshot())
	if err != nil {
		return
	}
	d.mu.RLock()
	for ch := range d.subscribers {
		select {
		case ch <- payload:
		default:
			// Slow consumer — drop. They'll get fresh state on next event.
		}
	}
	d.mu.RUnlock()
}

// indexTemplate is the single-page HTML for the dashboard. Vanilla
// HTML + JS + CSS, no framework. Re-renders the whole page body on
// each SSE message — the case list is small (<10 entries) so cost
// is negligible.
var indexTemplate = template.Must(template.New("index").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>yscaletest · {{.RunID}}</title>
<style>
:root {
  --bg: #0d1117;
  --fg: #c9d1d9;
  --dim: #8b949e;
  --card: #161b22;
  --border: #30363d;
  --green: #3fb950;
  --red: #f85149;
  --yellow: #d29922;
  --blue: #58a6ff;
}
* { box-sizing: border-box; }
body {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, monospace;
  background: var(--bg);
  color: var(--fg);
  margin: 0;
  padding: 24px;
  font-size: 13px;
  line-height: 1.5;
}
header {
  display: flex;
  align-items: baseline;
  gap: 16px;
  margin-bottom: 24px;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--border);
}
header h1 { margin: 0; font-size: 18px; font-weight: 600; }
header .meta { color: var(--dim); }
header .totals { margin-left: auto; }
header .totals span { margin-left: 12px; padding: 2px 8px; border-radius: 3px; }
.pass { background: rgba(63,185,80,0.15); color: var(--green); }
.fail { background: rgba(248,81,73,0.15); color: var(--red); }
.run  { background: rgba(88,166,255,0.15); color: var(--blue); }
.case {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 12px 16px;
  margin-bottom: 12px;
}
.case-head { display: flex; align-items: center; gap: 12px; }
.case-name { font-weight: 600; }
.case-phase { padding: 2px 8px; border-radius: 3px; font-size: 12px; }
.case-phase.Succeeded { background: rgba(63,185,80,0.2); color: var(--green); }
.case-phase.Failed,
.case-phase.Cancelled,
.case-phase.Timeout,
.case-phase.Error { background: rgba(248,81,73,0.2); color: var(--red); }
.case-phase.Provisioning,
.case-phase.Running,
.case-phase.Starting { background: rgba(88,166,255,0.2); color: var(--blue); }
.case-meta { color: var(--dim); margin-left: auto; font-size: 12px; }
.case-fields { color: var(--dim); margin-top: 6px; font-size: 12px; }
.case-fields .k { color: var(--fg); }
.case-fields .err { color: var(--red); white-space: pre-wrap; }
.case-tail {
  background: #010409;
  border: 1px solid var(--border);
  border-radius: 4px;
  padding: 8px 12px;
  margin-top: 8px;
  font-size: 11px;
  max-height: 240px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-all;
}
.case-tail .line { color: var(--dim); }
.note { color: var(--yellow); }
</style>
</head>
<body>
<header>
  <h1>yscaletest</h1>
  <span class="meta" id="run-id">{{.RunID}}</span>
  <span class="meta" id="elapsed">…</span>
  <div class="totals" id="totals"></div>
</header>
<main id="cases"></main>
<script>
const fmtDuration = (ms) => {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  return m + ':' + String(s % 60).padStart(2, '0');
};
const esc = (s) => (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

let lastSnapshot = null;
function render(snap) {
  lastSnapshot = snap;
  const now = new Date(snap.now);
  const start = new Date(snap.startedAt);
  document.getElementById('elapsed').textContent = fmtDuration(now - start);
  const cases = snap.cases || [];
  let pass = 0, fail = 0, run = 0;
  for (const c of cases) {
    if (c.phase === 'Succeeded') pass++;
    else if (c.phase === 'Failed' || c.phase === 'Cancelled' || c.phase === 'Timeout' || c.phase === 'Error') fail++;
    else run++;
  }
  document.getElementById('totals').innerHTML =
    (run ? '<span class="run">' + run + ' running</span>' : '') +
    (pass ? '<span class="pass">' + pass + ' pass</span>' : '') +
    (fail ? '<span class="fail">' + fail + ' fail</span>' : '');

  cases.sort((a, b) => a.name.localeCompare(b.name));
  const html = cases.map(c => {
    const dur = c.finishedAt && c.finishedAt > '0001-01-01' ?
      (new Date(c.finishedAt) - new Date(c.startedAt)) :
      (now - new Date(c.startedAt));
    const tail = (c.tail || []).map(l => '<div class="line">▸ ' + esc(l) + '</div>').join('');
    return '<div class="case">' +
      '<div class="case-head">' +
        '<span class="case-name">' + esc(c.name) + '</span>' +
        '<span class="case-phase ' + esc(c.phase) + '">' + esc(c.phase || '…') + '</span>' +
        '<span class="case-meta">' + fmtDuration(dur) +
          (c.backend ? ' · ' + esc(c.backend) : '') +
          (c.burstID ? ' · ' + esc(c.burstID) : '') +
        '</span>' +
      '</div>' +
      '<div class="case-fields">' +
        (c.podName ? '<span class="k">pod:</span> ' + esc(c.podName) + ' ' + esc(c.podPhase || '') + '   ' : '') +
        (c.nodeName ? '<span class="k">node:</span> ' + esc(c.nodeName) + '   ' : '') +
        (c.note ? '<div class="note">' + esc(c.note) + '</div>' : '') +
        (c.err ? '<div class="err">' + esc(c.err) + '</div>' : '') +
      '</div>' +
      (tail ? '<div class="case-tail">' + tail + '</div>' : '') +
    '</div>';
  }).join('');
  document.getElementById('cases').innerHTML = html || '<div class="case"><em>(no cases yet)</em></div>';
}

const evt = new EventSource('/events');
evt.onmessage = (e) => {
  try { render(JSON.parse(e.data)); } catch {}
};
// Re-tick the elapsed counter once per second even without new events.
setInterval(() => {
  if (lastSnapshot) {
    lastSnapshot.now = new Date().toISOString();
    render(lastSnapshot);
  }
}, 1000);
</script>
</body>
</html>
`))
