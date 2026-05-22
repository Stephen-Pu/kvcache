#!/usr/bin/env bash
# Spin up a kind cluster, apply the CRDs, run the operator e2e test
# in-process against the cluster, tear everything down.
#
# Idempotent: if a cluster with the same name already exists this script
# deletes it first.
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-kvcache-e2e}"
HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$HERE/../../../.." && pwd )"

cleanup() {
    echo "==> cleaning up kind cluster $CLUSTER_NAME"
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Recreate to avoid stale state from a previous run.
kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true

echo "==> creating kind cluster $CLUSTER_NAME"
kind create cluster --name "$CLUSTER_NAME" --config "$HERE/kind-config.yaml" --wait 90s

KUBECONFIG_PATH="$(mktemp)"
trap 'rm -f "$KUBECONFIG_PATH"; cleanup' EXIT
kind get kubeconfig --name "$CLUSTER_NAME" > "$KUBECONFIG_PATH"
export KUBECONFIG="$KUBECONFIG_PATH"

echo "==> applying CRDs"
kubectl apply -f "$REPO_ROOT/src/operator/config/crd/"

# Phase L-2: when E2E_IMAGE is set (typically by
# `make e2e-operator-workload`) load the image into the kind cluster so
# the StatefulSet pods can pull it locally. Without this the workload
# test would land in ImagePullBackOff against a registry that doesn't
# exist.
if [ -n "${E2E_IMAGE:-}" ]; then
    echo "==> loading $E2E_IMAGE into kind cluster $CLUSTER_NAME"
    kind load docker-image "$E2E_IMAGE" --name "$CLUSTER_NAME"
fi

echo "==> running e2e tests"
cd "$REPO_ROOT/src/operator"
# The workload test reads E2E_IMAGE itself; export it through so the go
# test process inherits the env.
E2E_IMAGE="${E2E_IMAGE:-}" go test -tags=e2e -count=1 -timeout=10m ./test/e2e/...
