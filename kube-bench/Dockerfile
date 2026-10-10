# syntax=docker/dockerfile:1.7
FROM golang:1.23-bookworm AS build

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags "-s -w -X github.com/JakeNesler/yscale-kube-bench/internal/app.Version=${VERSION} -X github.com/JakeNesler/yscale-kube-bench/internal/app.Commit=${COMMIT} -X github.com/JakeNesler/yscale-kube-bench/internal/app.Date=${BUILD_DATE}" \
    -o /out/yscale-kube-bench \
    ./cmd/yscale-kube-bench

FROM debian:bookworm-slim

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       dnsutils \
       ffmpeg \
       fio \
       iproute2 \
       tini \
       util-linux \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/yscale-kube-bench /usr/local/bin/yscale-kube-bench

LABEL org.opencontainers.image.title="yscale-kube-bench" \
      org.opencontainers.image.description="Reaction, efficiency, and throughput benchmarks for heterogeneous Kubernetes clusters" \
      org.opencontainers.image.source="https://github.com/JakeNesler/yscale-kube-bench" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"

WORKDIR /bench
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/yscale-kube-bench"]
