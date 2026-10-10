// yscale-factory is the factory service entrypoint. It provisions and tears
// down per-tenant Headscale coordination boxes and is the sole custodian of
// their admin keys + the KEK — so it runs two listeners with deliberately
// different trust boundaries (see runProd).
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yscale-sh/yscale/factory/internal/boxes"
	"github.com/yscale-sh/yscale/factory/internal/registrar"
	"github.com/yscale-sh/yscale/factory/internal/store"
)

func main() {
	token := os.Getenv("FACTORY_BEARER_TOKEN")
	if token == "" {
		log.Fatal("FACTORY_BEARER_TOKEN is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if os.Getenv("FACTORY_DEV") == "1" {
		runDev(ctx, token)
		return
	}
	runProd(ctx, token)
}

// runDev is the local/CI path: in-memory store + fake worker, fabric API only.
// FACTORY_KEK is still required (store.New fails closed) so the crypto boundary
// is exercised locally exactly as in prod. It NEVER touches a cloud or a box.
func runDev(ctx context.Context, token string) {
	s, err := store.New()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("yscale-factory DEV MODE — in-memory store + fake worker (NOT production)")
	serve(ctx, envOr("FACTORY_LISTEN", ":8080"), NewHandler(s, boxes.NewFakeWorker(s), token))
}

// runProd wires the real factory: KEK-encrypted durable Postgres store, the
// Linode HTTP worker, and the ops-tailnet key-handoff registrar. It fails LOUD
// if any production credential is absent — a half-configured factory must never
// start and provision boxes it can't durably record or key.
func runProd(ctx context.Context, token string) {
	dsn := mustEnv("DATABASE_URL")                        // factory's own Postgres database
	linodeToken := mustEnv("LINODE_TOKEN")                // box-scoped: Linodes + Cloud Firewalls RW only
	opsAuthKey := mustEnv("FACTORY_OPS_AUTHKEY")          // ops-tailnet auth key baked into box user_data
	opsURL := mustEnv("FACTORY_OPS_URL")                  // factory ops URL the box POSTs its admin key to
	opsLoginServer := mustEnv("FACTORY_OPS_LOGIN_SERVER") // operator's ops Headscale, not tenant Headscale
	if err := boxes.ValidateOpsLoginServer(opsLoginServer); err != nil {
		log.Fatal(err)
	}
	// FACTORY_KEK is validated by store.NewFromEnv (fail-closed on absent/invalid).

	persister, err := store.NewPostgresPersister(ctx, dsn)
	if err != nil {
		log.Fatalf("factory: durable store: %v", err)
	}
	defer persister.Close()

	s, err := store.NewFromEnv()
	if err != nil {
		log.Fatalf("factory: store (FACTORY_KEK): %v", err)
	}
	if err := s.LoadFrom(ctx, persister); err != nil {
		log.Fatalf("factory: hydrate durable state: %v", err)
	}

	reg := registrar.NewOpsRegistrar()
	worker := boxes.NewLinodeWorker(s, boxes.NewLinodeHTTP(linodeToken), reg,
		boxes.WithOpsHandoff(opsAuthKey, opsURL), boxes.WithOpsLoginServer(opsLoginServer))

	// Two listeners, two trust boundaries:
	//  - fabric API (central-facing): bearer-authed lifecycle RPCs. The deploy
	//    exposes it ClusterIP-only, ingress restricted to central's pods.
	//  - ops handoff (box-facing): receives a box's freshly-minted admin key,
	//    authed by the single-use token + ops-tailnet membership. The deploy
	//    binds it behind the ops-tailnet interface + a NetworkPolicy so it is
	//    UNREACHABLE from central, bursts, or any customer mesh.
	fabricAddr := envOr("FACTORY_LISTEN", ":8080")
	opsAddr := envOr("FACTORY_OPS_LISTEN", ":8081")
	log.Printf("yscale-factory PROD — durable store + Linode worker; fabric API %s, ops handoff %s", fabricAddr, opsAddr)
	go serve(ctx, opsAddr, reg.Handler())
	serve(ctx, fabricAddr, NewHandler(s, worker, token))
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("factory: %s is required in production (set FACTORY_DEV=1 for the local in-memory dev mode)", key)
	}
	return v
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// serve runs one HTTP listener with graceful shutdown on ctx cancel. A bind or
// serve failure is fatal — both listeners are required for a correct factory.
func serve(ctx context.Context, addr string, h http.Handler) {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shutdown)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("factory: serve %s: %v", addr, err)
	}
}
