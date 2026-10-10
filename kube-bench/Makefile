SHELL := /bin/bash

BINARY ?= yscale-kube-bench
IMAGE ?= ghcr.io/jakenesler/yscale-kube-bench
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X github.com/JakeNesler/yscale-kube-bench/internal/app.Version=$(VERSION) \
	-X github.com/JakeNesler/yscale-kube-bench/internal/app.Commit=$(COMMIT) \
	-X github.com/JakeNesler/yscale-kube-bench/internal/app.Date=$(BUILD_DATE)

.PHONY: all build test vet check clean image image-push dist install kind-smoke

all: check build

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/yscale-kube-bench

test:
	go test -race ./...

vet:
	go vet ./...

check: test vet

clean:
	rm -rf bin dist results

image:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t $(IMAGE):$(VERSION) \
		-t $(IMAGE):latest \
		.

image-push:
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t $(IMAGE):$(VERSION) \
		-t $(IMAGE):latest \
		--push \
		.

dist: check
	rm -rf dist
	mkdir -p dist
	for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do \
		os=$${target%/*}; arch=$${target#*/}; \
		name=$(BINARY)_$(VERSION)_$${os}_$${arch}; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -ldflags "$(LDFLAGS)" -o dist/$$name ./cmd/yscale-kube-bench; \
		tar -C dist -czf dist/$$name.tar.gz $$name; \
		rm dist/$$name; \
	done
	cd dist && sha256sum *.tar.gz > SHA256SUMS

install: build
	install -m 0755 bin/$(BINARY) /usr/local/bin/$(BINARY)

kind-smoke: build
	./scripts/kind-smoke.sh
