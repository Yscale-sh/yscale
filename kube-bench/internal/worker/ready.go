package worker

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

func runReadyServerCommand(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("worker ready-server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", ":8080", "listen address")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ready", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ready\n"))
	})
	server := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func runNoop(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("worker noop", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sleep := flags.Duration("sleep", 0, "sleep duration")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *sleep > 0 {
		time.Sleep(*sleep)
	}
	return 0
}
