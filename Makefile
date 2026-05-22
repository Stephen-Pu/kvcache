# Top-level Makefile — single entry point for local dev.
#
#   make build      # Configure + build everything (Debug, Ninja)
#   make test       # Build + run ctest
#   make go         # Build all Go components
#   make go-test    # Run all Go tests (excludes integration tag)
#   make go-it      # Run Go integration tests (embedded etcd)
#   make py-test    # Run Python E2E adapter tests
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

.PHONY: help build configure compile test go go-test go-it py-test clean all e2e-operator docker-image e2e-operator-workload

help:
	@grep -E '^# +' Makefile | head -20

configure:
	$(CMAKE) -S src -B $(BUILD_DIR) -G "$(GENERATOR)" -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)

compile:
	$(CMAKE) --build $(BUILD_DIR) -j $(JOBS)

build: configure compile

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure --timeout 60

go:
	cd src/control-plane && go build ./...
	cd src/operator      && go build ./...
	cd src/kvctl         && go build ./...

go-test:
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

# Phase L-2: opt-in workload e2e — builds the image, loads into kind,
# runs the operator e2e suite with the workload-Ready test enabled.
e2e-operator-workload:
	@command -v kind     >/dev/null 2>&1 || { echo "kind not installed";    exit 1; }
	@command -v kubectl  >/dev/null 2>&1 || { echo "kubectl not installed"; exit 1; }
	@command -v docker   >/dev/null 2>&1 || { echo "docker not installed";  exit 1; }
	$(MAKE) docker-image KVSTORE_NODE_IMAGE=$(KVSTORE_NODE_IMAGE)
	E2E_IMAGE=$(KVSTORE_NODE_IMAGE) bash src/operator/test/e2e/run.sh

clean:
	rm -rf $(BUILD_DIR)
	cd src/control-plane && go clean ./... 2>/dev/null || true
	cd src/operator      && go clean ./... 2>/dev/null || true
	cd src/kvctl         && go clean ./... 2>/dev/null || true
