BINARY := yscale
TAG ?= latest
IMAGE_REGISTRY ?= ghcr.io/yscale-sh
PLATFORM ?= linux/amd64
GO ?= go
BIN_DIR ?= bin

.DEFAULT_GOAL := help

# `make` with no target should orient you, not fail. Lists the targets that
# work in a plain source checkout; the image/deploy targets below need
# provider credentials and a registry you can push to.
.PHONY: help
help:
	@echo "yscale — common targets"
	@echo
	@echo "  build            build the full self-hosted managed-mesh stack into bin/"
	@echo "  run              start your managed-mesh central (configure .env.cloud first)"
	@echo "  test             go test ./... (no cluster or cloud account needed)"
	@echo "  test-t1-contract deterministic T1 evidence/backend/crash contracts (race)"
	@echo "  test-lifecycle-integration"
	@echo "                   durable lifecycle fixtures; skipped without a guarded DSN"
	@echo "  vet              go vet ./..."
	@echo "  clean            remove bin/"
	@echo
	@echo "  agent-image-build   build the Cluster Connector image"
	@echo "  cloud-image-build   build central and its lifecycle migrator"
	@echo "  factory-image-build build the managed-mesh factory image"
	@echo "  burst-image-push    build + push the burst base and node images"
	@echo "  all-images-push     build + push connector, central, factory, burst base + node"
	@echo
	@echo "Build overrides: IMAGE_REGISTRY=$(IMAGE_REGISTRY), TAG=$(TAG), PLATFORM=$(PLATFORM)."
	@echo "Use one platform with these --load targets; BuildKit service Dockerfiles"
	@echo "also support multi-platform builds with docker buildx build --push."
	@echo "Burst images are amd64-only unless Dockerfile.kubelet.base is made TARGETARCH-aware."
	@echo "Per-image build overrides: FLY_AGENT_IMAGE, FLY_CLOUD_IMAGE, FLY_FACTORY_IMAGE,"
	@echo "FLY_BURST_IMAGE and FLY_BURST_BASE_IMAGE (historical names; any registry)."
	@echo "The default maintainer registry is private; publish to your own registry."
	@echo "YSCALE_BURST_IMAGE and YSCALE_FLYIO_BURST_IMAGE are central runtime settings,"
	@echo "as are YSCALE_LINODE_IMAGE_CPU/_GPU and YSCALE_AWS_AMI — see the README."
	@echo
	@echo "Run 'grep \"^[a-z-]*:\" Makefile' for the full target list."

# Publish every image to an operator-owned registry. The historical FLY_ names
# remain per-image overrides; they do not require Fly. For registry.fly.io,
# create each app in your own org and authenticate with `flyctl auth docker`.
# Kubernetes private registries also need namespace-local imagePullSecrets.
FLY_BURST_APP        ?= yscale-burst-image
FLY_BURST_IMAGE      ?= $(IMAGE_REGISTRY)/$(FLY_BURST_APP):$(TAG)
FLY_BURST_BASE_APP   ?= yscale-burst-base
FLY_BURST_BASE_IMAGE ?= $(IMAGE_REGISTRY)/$(FLY_BURST_BASE_APP):$(TAG)

FLY_CLOUD_APP   ?= yscale-cloud
FLY_CLOUD_IMAGE ?= $(IMAGE_REGISTRY)/$(FLY_CLOUD_APP):$(TAG)

FLY_AGENT_APP   ?= yscale-cluster-agent
FLY_AGENT_IMAGE ?= $(IMAGE_REGISTRY)/$(FLY_AGENT_APP):$(TAG)

FLY_FACTORY_APP   ?= yscale-factory
FLY_FACTORY_IMAGE ?= $(IMAGE_REGISTRY)/$(FLY_FACTORY_APP):$(TAG)

.PHONY: build run test test-t1-contract test-lifecycle-integration clean lint fmt \
        burst-base-image-build burst-base-image-push \
        burst-image-build burst-image-push \
        cloud-image-build cloud-image-push \
        agent-image-build agent-image-push \
        factory-image-build factory-image-push \
        bin/yscale-cloud-linux-amd64 bin/yscale-agent-linux-amd64 \
        all-images-push \
        yscaletest e2e sweep-leaks

build:
	mkdir -p "$(BIN_DIR)"
	$(GO) build -trimpath -o "$(BIN_DIR)/$(BINARY)" ./cmd/yscale
	$(GO) build -trimpath -o "$(BIN_DIR)/yscale-cloud" ./central/cmd/yscale-cloud
	$(GO) build -trimpath -o "$(BIN_DIR)/yscale-agent" ./agent/cmd/yscale-agent
	$(GO) build -trimpath -o "$(BIN_DIR)/yscale-factory" ./factory/cmd/yscale-factory
	$(GO) build -trimpath -o "$(BIN_DIR)/yscale-lifecycle-migrate" ./central/cmd/yscale-lifecycle-migrate

run: build
	./bin/yscale-cloud


test:
	go test ./...

# Fast deterministic contracts: no database, no cluster, no cloud account.
# Run under the race detector because the crash controller is entered
# concurrently by design.
test-t1-contract:
	go test -race ./internal/evidence ./internal/testkit ./central/internal/lifecycletest

# Durable crash-convergence fixtures against a real PostgreSQL database.
# This target is a no-op skip unless LIFECYCLE_TEST_DATABASE_URL is set, and
# the fixtures themselves refuse to run unless every existing guard agrees:
#
#   LIFECYCLE_TEST_DATABASE_URL       DSN of an isolated throwaway database
#   LIFECYCLE_TEST_ALLOW_DESTRUCTIVE  must be exactly DROP_LIFECYCLE_SCHEMA
#   LIFECYCLE_TEST_EXPECT_DATABASE    must equal current_database()
#
# and the database name must end in _lifecycle_test. The fixtures DROP and
# recreate the lifecycle schema, so never point this at a shared database.
# Nothing in this target relaxes those guards. Keep package execution serial:
# both guarded suites intentionally recreate the same lifecycle schema.
test-lifecycle-integration:
	go test -p=1 -tags=integration -count=1 \
	  ./central/internal/lifecycle ./central/internal/lifecycletest

# ---- end-to-end smoke tests ----
# Cycle one busybox workload through the chosen backend's smallest
# instance, verify pod runs to MARKER-OK, and verify no leaked Fly /
# Linode resources after yscale's natural reap. ~5-10 minutes per
# backend. KUBECONFIG must point at the customer cluster (the one
# running yscale-agent). See scripts/smoke-test.sh for the full
# contract.
smoke-test-fly: smoke-test-flyio  # alias
smoke-test-flyio:
	./scripts/smoke-test.sh flyio nano

smoke-test-linode:
	./scripts/smoke-test.sh linode nano

# Default smoke target — runs whichever backend yscale-cloud is
# currently configured for. Cheapest and quickest sanity check after
# any change to central, agent, burst image, or chart.
smoke-test: smoke-test-fly

# Run all backend smoke tests sequentially. Useful as a pre-release gate.
smoke-test-all: smoke-test-fly smoke-test-linode

clean:
	rm -rf bin/

# ---- burst-node base image (heavy downloads; rebuild rarely) ----
# Push this once, then `burst-image-push` cycles in seconds.
burst-base-image-build:
	@test "$(PLATFORM)" = linux/amd64 || { echo "Burst images require PLATFORM=linux/amd64; Dockerfile.kubelet.base is not TARGETARCH-aware." >&2; exit 1; }
	docker buildx build \
		--platform $(PLATFORM) \
		-t $(FLY_BURST_BASE_IMAGE) \
		-f burst/image/Dockerfile.kubelet.base \
		--load \
		.

burst-base-image-push: burst-base-image-build
	docker push $(FLY_BURST_BASE_IMAGE)

# ---- burst-node image (Fly machines pull this) ----
# Slim: FROM yscale-burst-base + COPY entrypoint. Iterations on the
# entrypoint are ~5s push.
burst-image-build:
	@test "$(PLATFORM)" = linux/amd64 || { echo "Burst images require PLATFORM=linux/amd64; Dockerfile.kubelet.base is not TARGETARCH-aware." >&2; exit 1; }
	docker buildx build \
		--platform $(PLATFORM) \
		--build-arg BASE_IMAGE=$(FLY_BURST_BASE_IMAGE) \
		-t $(FLY_BURST_IMAGE) \
		-f burst/image/Dockerfile.kubelet \
		--load \
		.

# Publish the base before building the overlay, including with parallel make:
# a container-backed Buildx builder cannot rely on Docker's local image store.
burst-image-push: burst-base-image-push
	$(MAKE) --no-print-directory burst-image-build
	docker push $(FLY_BURST_IMAGE)

# ---- yscale-cloud (central server, runs in customer cluster) ----
# BuildKit cross-compiles for PLATFORM and includes the lifecycle migrator.
# The explicit host-binary target remains available for local packaging.
bin/yscale-cloud-linux-amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
		-trimpath -ldflags="-s -w" \
		-o bin/yscale-cloud-linux-amd64 \
		./central/cmd/yscale-cloud

cloud-image-build:
	docker buildx build \
		--platform $(PLATFORM) \
		-t $(FLY_CLOUD_IMAGE) \
		-f central/build/Dockerfile.cloud.buildkit \
		--load \
		.

cloud-image-push: cloud-image-build
	docker push $(FLY_CLOUD_IMAGE)

# ---- yscale-cluster-agent (runs in customer cluster) ----
bin/yscale-agent-linux-amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
		-trimpath -ldflags="-s -w -X main.version=dev" \
		-o bin/yscale-agent-linux-amd64 \
		./agent/cmd/yscale-agent

agent-image-build:
	docker buildx build \
		--platform $(PLATFORM) \
		--build-arg VERSION=$(TAG) \
		-t $(FLY_AGENT_IMAGE) \
		-f agent/build/Dockerfile.agent.buildkit \
		--load \
		.

agent-image-push: agent-image-build
	docker push $(FLY_AGENT_IMAGE)

# ---- managed-mesh factory (includes coordination-box provisioning assets) ----
factory-image-build:
	docker buildx build \
		--platform $(PLATFORM) \
		-t $(FLY_FACTORY_IMAGE) \
		-f factory/build/Dockerfile.factory.buildkit \
		--load \
		.

factory-image-push: factory-image-build
	docker push $(FLY_FACTORY_IMAGE)

all-images-push: burst-base-image-push burst-image-push cloud-image-push agent-image-push factory-image-push

# ---- backend leak sweep ----
# Destroys ys-burst-* machines on Fly + Linode that were left over from
# killed test runs. Safe by construction: only touches resources matching the
# yscale prefix; never sweeps anything else.
# Override MIN_AGE_SEC=300 to keep machines newer than 5 min alive (if
# you're running a concurrent test that you don't want to clobber).
sweep-leaks:
	@test -f scripts/sweep-test-leaks.sh || { \
	  echo "sweep-leaks: scripts/sweep-test-leaks.sh is not included in this"; \
	  echo "             distribution (it drives the maintainer's test accounts)."; \
	  echo "             Burst nodes are named ys-burst-* and tagged yscale-burst;"; \
	  echo "             sweep them with your provider's CLI if a test run leaks."; \
	  exit 2; }
	@chmod +x scripts/sweep-test-leaks.sh
	@./scripts/sweep-test-leaks.sh

# ---- end-to-end test harness ----
# Builds the local yscaletest binary against the current source.
yscaletest:
	go build -o bin/yscaletest ./cmd/yscaletest

# One-shot end-to-end: build+push both deploy-side images, restart the
# cluster deployments so they pick up the new bits, then run yscaletest
# against test/cases/. Requires KUBECONFIG pointing at a cluster with
# yscale-agent + yscale-cloud already installed.
#
# Overrides:
#   E2E_FILTER=flyio-cpu-nano  # only one case
#   E2E_PARALLEL=4             # cases concurrency (default 4)
#   E2E_TIMEOUT=15m            # per-case timeout (default 15m)
#   E2E_NAMESPACE=default      # which ns to apply Workload CRs in
E2E_FILTER    ?=
E2E_PARALLEL  ?= 4
E2E_TIMEOUT   ?= 15m
E2E_NAMESPACE ?= default
# Live dashboard port. Set E2E_WATCH=0 to disable.
E2E_WATCH     ?= 7890

e2e: yscaletest
	@test -d test/cases || { \
	  echo "e2e: the full e2e case suite (test/cases/) is not included in this"; \
	  echo "     distribution. Apply examples/workloads/ (see the README quickstart)"; \
	  echo "     to run a live workload, or write your own cases for ./bin/yscaletest."; \
	  exit 2; }
	@chmod +x scripts/timed.sh scripts/sweep-test-leaks.sh scripts/wait-agent-connected.sh
	@E2E_START=$$(date +%s); \
	scripts/timed.sh "sweep-leaks"      $(MAKE) --no-print-directory sweep-leaks; \
	scripts/timed.sh "cloud-image-push" $(MAKE) --no-print-directory cloud-image-push; \
	scripts/timed.sh "agent-image-push" $(MAKE) --no-print-directory agent-image-push; \
	scripts/timed.sh "burst-image-push" $(MAKE) --no-print-directory burst-image-push; \
	echo "==> pre-flight: sweep test-tagged workload CRs from prior runs"; \
	kubectl -n $(E2E_NAMESPACE) delete wl -l yscale.sh/yscaletest-run --ignore-not-found --wait=false 2>/dev/null || true; \
	scripts/timed.sh "deploy-restart" sh -c '\
		kubectl -n yscale rollout restart deploy/yscale-cloud >/dev/null; \
		kubectl -n yscale rollout restart deploy/yscale-agent-yscale-agent >/dev/null; \
		kubectl -n yscale rollout status  deploy/yscale-cloud  --timeout=180s; \
		kubectl -n yscale rollout status  deploy/yscale-agent-yscale-agent --timeout=240s'; \
	scripts/timed.sh "preflight-ws"   scripts/wait-agent-connected.sh || (echo "FATAL: agent never reconnected — DNS, RBAC, or central down" >&2; exit 1); \
	scripts/timed.sh "preflight-http" scripts/wait-agent-connected.sh --verify-http || (echo "FATAL: agent pod cannot reach yscale-cloud — DNS likely broken" >&2; exit 1); \
	echo "==> running yscaletest (dashboard: http://localhost:$(E2E_WATCH))"; \
	scripts/timed.sh "yscaletest" ./bin/yscaletest \
		-dir test/cases \
		-namespace $(E2E_NAMESPACE) \
		-parallel $(E2E_PARALLEL) \
		-timeout $(E2E_TIMEOUT) \
		-watch $(E2E_WATCH) \
		$(if $(E2E_FILTER),-filter $(E2E_FILTER),); \
	E2E_END=$$(date +%s); \
	E2E_TOTAL=$$((E2E_END - E2E_START)); \
	printf '\n────────────────────────────────────────\n⏱  %-22s %02d:%02d\n' "TOTAL:" $$((E2E_TOTAL / 60)) $$((E2E_TOTAL % 60))

# À-la-carte e2e — convenience targets for common subsets. Each just
# sets E2E_FILTER and dispatches to the main e2e target, so all the
# build/push/preflight machinery is identical. Use `make e2e-fly` for a
# quick Fly-only smoke (~4 min, no GPU $); `make e2e-linode` when you
# specifically want to exercise the Linode path. Override E2E_FILTER
# directly for any combination not listed. The removed `lite`
# networking tier no longer has a filter target: central refuses
# `spec.networking.tier: lite`, so the previous `e2e-lite` target
# (which pointed at `flyio-lite.yaml`) has been dropped.
e2e-fly:
	$(MAKE) --no-print-directory e2e E2E_FILTER=flyio
e2e-linode:
	$(MAKE) --no-print-directory e2e E2E_FILTER=linode
e2e-full:
	$(MAKE) --no-print-directory e2e E2E_FILTER=full
.PHONY: e2e-fly e2e-linode e2e-full

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .
