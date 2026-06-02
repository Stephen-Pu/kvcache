# Phase H-5 — control-plane container image.
#
# Multi-stage Go build. Companion to Dockerfile.kvstore-node (Phase L-2)
# — the operator now points its CP StatefulSet at this image instead of
# reusing kvstore-node, which CrashLoopBackOff'd because the binaries
# share no flags.
#
# Build context is the repo root (not this directory) so the COPY
# expressions can reach src/. Tag convention: `kvcache/cp:e2e` for
# local kind workflows; release builds retag to
# `ghcr.io/stephen-pu/kvcache-cp:<version>`.
#
# Build (from the repo root):
#   docker build -f src/deploy/docker/Dockerfile.cp \
#       -t kvcache/cp:e2e .
#
# Load into kind:
#   kind load docker-image kvcache/cp:e2e
#
# Both flows are wired into the top-level Makefile (`make docker-image-cp`,
# `make e2e-operator-workload`).

# -----------------------------------------------------------------------
# Stage 1: build
# -----------------------------------------------------------------------
FROM golang:1.22-bookworm AS build

# Phase A-6: gRPC server uses generated pb stubs that are gitignored
# (see Makefile go-proto target). Install protoc + the go/go-grpc
# plugins so the build can regenerate them inside the image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends protobuf-compiler && \
    rm -rf /var/lib/apt/lists/* && \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
ENV PATH=/root/go/bin:$PATH

WORKDIR /work
# core/proto is the SOT for the .proto files — copy it alongside the
# control-plane tree so `make proto` can find them at ../core/proto/.
COPY src/core/proto/        src/core/proto/
COPY src/control-plane/     src/control-plane/

WORKDIR /work/src/control-plane
RUN make proto && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' \
        -o /out/cp ./cmd/cp

# -----------------------------------------------------------------------
# Stage 2: runtime
# -----------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive

# Only ca-certificates is needed at runtime — the static Go binary
# carries everything else.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/cp /usr/local/bin/cp

EXPOSE 7100

ENTRYPOINT ["/usr/local/bin/cp"]
