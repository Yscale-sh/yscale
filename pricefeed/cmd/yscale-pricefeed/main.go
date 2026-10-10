// Command yscale-pricefeed is the pricing-feed microservice. It polls
// each cloud provider for VM/GPU prices and regional availability,
// keeps a deduped in-memory index, and serves it over HTTP (a one-shot
// snapshot at GET /v1/prices) and WebSocket (snapshot + live deltas at
// GET /v1/prices/stream). The central decider is the intended
// subscriber — a live feed replaces the hand-curated price catalog.
//
// v0: skeleton. The provider Sources (pricefeed/internal/sources) are
// stubs returning feed.ErrNotImplemented; the aggregator, Feed, and WS
// server are real. Implement one provider's Fetch and it lights up.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yscale-sh/yscale/pkg/logger"
	"github.com/yscale-sh/yscale/pricefeed/internal/feed"
	"github.com/yscale-sh/yscale/pricefeed/internal/server"
	"github.com/yscale-sh/yscale/pricefeed/internal/sources"
)

func main() {
	var listenAddr string
	flag.StringVar(&listenAddr, "listen", ":8454", "address to listen on")
	flag.Parse()

	log, closeLog := logger.New(logger.Options{Job: "yscale-pricefeed"})
	slog.SetDefault(log)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = closeLog(ctx)
	}()

	f := feed.New()
	srcs := sources.All()
	agg := &feed.Aggregator{Feed: f, Sources: srcs, Log: log}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go agg.Run(ctx)

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           server.New(f, log).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shutdown)
	}()

	log.Info("yscale-pricefeed listening", "addr", listenAddr, "sources", len(srcs))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "error", err)
		os.Exit(1)
	}
}
