// Package server exposes the price index over HTTP (a one-shot
// snapshot) and WebSocket (a snapshot followed by a live delta stream).
// The central decider is the intended WS subscriber.
package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/yscale-sh/yscale/pricefeed/internal/feed"
)

const wsWriteTimeout = 10 * time.Second

// Server serves the Feed.
type Server struct {
	Feed     *feed.Feed
	Log      *slog.Logger
	upgrader websocket.Upgrader
}

// New constructs a Server.
func New(f *feed.Feed, log *slog.Logger) *Server {
	return &Server{
		Feed: f,
		Log:  log,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
		},
	}
}

// Handler builds the mux: GET /v1/prices (snapshot),
// GET /v1/prices/stream (WS), GET /healthz. Wrapped in CORS so
// browsers (e.g. the yscale.sh site) can read the public price index.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/prices", s.handleSnapshot)
	mux.HandleFunc("GET /v1/prices/stream", s.handleStream)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	return cors(mux)
}

// cors allows cross-origin reads — the price index is public data, so
// any origin may fetch it.
func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// streamMessage is one WS frame: a snapshot on connect, then deltas.
type streamMessage struct {
	Type      string          `json:"type"` // "snapshot" | "delta"
	Offerings []feed.Offering `json:"offerings,omitempty"`
	Delta     *feed.Delta     `json:"delta,omitempty"`
}

func (s *Server) handleSnapshot(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"offerings": s.Feed.Snapshot()})
}

// handleStream upgrades to WebSocket, sends the current snapshot, then
// streams deltas until the client disconnects.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade writes its own error response
	}
	defer conn.Close()

	id, ch := s.Feed.Subscribe()
	defer s.Feed.Unsubscribe(id)

	if err := writeJSON(conn, streamMessage{Type: "snapshot", Offerings: s.Feed.Snapshot()}); err != nil {
		return
	}

	// A reader goroutine surfaces client disconnects (the protocol is
	// server→client only; any read error means the peer is gone).
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-done:
			return
		case d, ok := <-ch:
			if !ok {
				return
			}
			delta := d
			if err := writeJSON(conn, streamMessage{Type: "delta", Delta: &delta}); err != nil {
				return
			}
		}
	}
}

func writeJSON(conn *websocket.Conn, v any) error {
	_ = conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	return conn.WriteJSON(v)
}
