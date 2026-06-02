# Top-level Makefile — single entry point for local dev.
#
#   make build      # Configure + build everything (Debug, Ninja)
#   make test       # Build + run ctest
#   make go         # Build all Go components
#   make go-test    # Run all Go tests (excludes integration tag)
#   make go-it      # Run Go integration tests (embedded etcd)
#   make bench-strict # Phase S-7: fairness + priority regression gate.
#                       # Runs bench_fairness --strict and bench_priority
#                       # --strict; non-zero exit on regression.
#   make py-test    # Run Python E2E adapter tests
#   make py-test-vllm # Phase P-4: same as py-test, but installs vLLM
#                     # first so the skip-marked bridge tests run too.
#                     # Heavyweight install (~4–5 GB); set VLLM_VERSION
#                     # to pin a version (default: latest pip wheel).
#   make all        # build + test + go + go-test + py-test
#   make clean      # Remove build/ and Go artifacts
#
# Vars:
#   BUILD_DIR   default: build
#   BUILD_TYPE  default: Debug
#   CMAKE       default: cmake

BUILD_DIR    ?= build
BUILD_TYPE   ?= Debug
CMAKE        ?= cmake
GENERATOR    ?= Ninja
JOBS         ?= $(shell nproc 2>/dev/null || sysctl -n hw.ncpu)

.PHONY: help build configure compile test go go-proto go-test go-it py-test py-test-vllm bench-strict clean all e2e-operator docker-image docker-image-cp e2e-operator-workload

help:
	@grep -E '^# +' Makefile | head -20

configure:
	$(CMAKE) -S src -B $(BUILD_DIR) -G "$(GENERATOR)" -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)

compile:
	$(CMAKE) --build $(BUILD_DIR) -j $(JOBS)

build: configure compile

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure --timeout 60

go: go-proto
	cd src/control-plane && go build ./...
	cd src/operator      && go build ./...
	cd src/kvctl         && go build ./...

# Phase A-6 — Go proto stubs are gitignored; regenerate from .proto files.
# Pre-req: protoc-gen-go + protoc-gen-go-grpc on $PATH
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go-proto:
	$(MAKE) -C src/control-plane proto

go-test: go-proto
	cd src/control-plane && go test -short -count=1 ./...
	cd src/operator      && go vet ./...
	cd src/kvctl         && go vet ./...

go-it:
	cd src/control-plane && go test -tags=integration -count=1 -timeout 2m ./internal/membership/...

py-test: build
	@command -v pytest >/dev/null 2>&1 || { echo "pytest not installed; pip install cffi pytest"; exit 1; }
	@LIB=$$(ls $(BUILD_DIR)/core-abi/libkvcache.dylib $(BUILD_DIR)/core-abi/libkvcache.so 2>/dev/null | head -1); \
	 if [ -z "$$LIB" ]; then echo "libkvcache.{so,dylib} not found under $(BUILD_DIR)/core-abi/"; exit 1; fi; \
	 echo "Using $$LIB"; \
	 KVCACHE_LIB=$$PWD/$$LIB pytest src/adapters -v

# Phase P-4 — install vLLM and run the bridge tests against the real
# engine surface. The 4 skip-marked tests under
# src/adapters/vllm/tests/test_vllm_bridge.py flip to "executed" and
# verify the bridge against the actual KVConnectorBase / KVConnectorRole
# imports. Heavyweight install: torch + xformers + vllm itself runs
# ~4–5 GB on Linux/CUDA wheels.
#
# Use VLLM_VERSION to pin (e.g. ``make py-test-vllm VLLM_VERSION=0.6.2``).
py-test-vllm: build
	@command -v pytest >/dev/null 2>&1 || { echo "pytest not installed; pip install cffi pytest"; exit 1; }
	@if [ -n "$(VLLM_VERSION)" ]; then \
		echo "Installing vllm==$(VLLM_VERSION)"; \
		pip install --upgrade "vllm==$(VLLM_VERSION)"; \
	else \
		echo "Installing latest vllm"; \
		pip install --upgrade vllm; \
	fi
	@python -c "import vllm; print('vllm', vllm.__version__)"
	@LIB=$$(ls $(BUILD_DIR)/core-abi/libkvcache.dylib $(BUILD_DIR)/core-abi/libkvcache.so 2>/dev/null | head -1); \
	 if [ -z "$$LIB" ]; then echo "libkvcache.{so,dylib} not found under $(BUILD_DIR)/core-abi/"; exit 1; fi; \
	 echo "Using $$LIB"; \
	 KVCACHE_LIB=$$PWD/$$LIB pytest src/adapters -v

# Phase S-7 — scheduler fairness + P0-starvation regression gate.
# Both benches run for ~1.5–2s and exit non-zero if the relevant
# invariant slips (Jain index below 0.85, or P0 dispatched fewer
# than 5 ops while P2 saturators are full-tilt).
bench-strict: build
	$(CMAKE) --build $(BUILD_DIR) --target bench_fairness bench_priority -j $(JOBS)
	@echo "--- bench_fairness --strict ---"
	$(BUILD_DIR)/bench/bench_fairness --strict
	@echo "--- bench_priority --strict ---"
	$(BUILD_DIR)/bench/bench_priority --strict

all: test go go-test py-test

# Opt-in: kind cluster e2e for the operator. Spins up a real kind
# cluster, applies the CRDs, runs the operator in-process against the
# kind apiserver, asserts the Reconcile fan-out + cascade-delete
# behavior. NOT included in `make all` because it requires docker +
# kind on the host and takes ~45s end-to-end.
e2e-operator:
	@command -v kind     >/dev/null 2>&1 || { echo "kind not installed; brew install kind";     exit 1; }
	@command -v kubectl  >/dev/null 2>&1 || { echo "kubectl not installed; brew install kubectl"; exit 1; }
	@command -v docker   >/dev/null 2>&1 || { echo "docker not installed";                       exit 1; }
	bash src/operator/test/e2e/run.sh

# Phase L-2: build the kvstore-node container image from the
# multi-stage Dockerfile. Tag matches what the workload e2e expects.
KVSTORE_NODE_IMAGE ?= kvcache/kvstore-node:e2e

docker-image:
	@command -v docker >/dev/null 2>&1 || { echo "docker not installed"; exit 1; }
	docker build \
		-f src/deploy/docker/Dockerfile.kvstore-node \
		-t $(KVSTORE_NODE_IMAGE) \
		.

# Phase H-5: separate control-plane image — different binary, different
# image, different Dockerfile (Go multi-stage). The CP pods used to
# CrashLoopBackOff in the workload e2e because they were running the
# kvstore-node image with CP-shaped args.
CP_IMAGE ?= kvcache/cp:e2e

docker-image-cp:
	@command -v docker >/dev/null 2>&1 || { echo "docker not installed"; exit 1; }
	docker build \
		-f src/deploy/docker/Dockerfile.cp \
		-t $(CP_IMAGE) \
		.

# Phase L-2 + H-5: opt-in workload e2e — builds BOTH images, loads into
# kind, runs the operator e2e suite. Both kvstore-node and CP STSes
# now reach Ready (Phase H-5 fixed the CP pod's crash-loop).
e2e-operator-workload:
	@command -v kind     >/dev/null 2>&1 || { echo "kind not installed";    exit 1; }
	@command -v kubectl  >/dev/null 2>&1 || { echo "kubectl not installed"; exit 1; }
	@command -v docker   >/dev/null 2>&1 || { echo "docker not installed";  exit 1; }
	$(MAKE) docker-image    KVSTORE_NODE_IMAGE=$(KVSTORE_NODE_IMAGE)
	$(MAKE) docker-image-cp CP_IMAGE=$(CP_IMAGE)
	E2E_IMAGE=$(KVSTORE_NODE_IMAGE) E2E_CP_IMAGE=$(CP_IMAGE) \
		bash src/operator/test/e2e/run.sh

clean:
	rm -rf $(BUILD_DIR)
	cd src/control-plane && go clean ./... 2>/dev/null || true
	cd src/operator      && go clean ./... 2>/dev/null || true
	cd src/kvctl         && go clean ./... 2>/dev/null || true
