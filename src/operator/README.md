# operator

Kubernetes operator for KV Cache. LLD §8.2.

## CRDs (MVP)

- `KVCacheCluster` — declares a cluster: node replica count, tier
  capacities, NIXL transport, etcd binding, Alluxio binding for the
  cold tier.
- `KVCacheTenant` — declares a tenant: namespace, 3D quota, priority
  class.

## What the reconciler actually does

Each `KVCacheCluster` drives a four-resource desired-state tree:

| Resource | Naming | Purpose |
|----------|--------|---------|
| `ServiceAccount` | `<cluster>-sa` | identity for `kvstore-node` pods |
| `ConfigMap` | `<cluster>-config` | cluster identity, NIXL backend, tier sizes, etcd endpoints |
| `Service` (headless) | `<cluster>-nodes` | per-pod DNS + gRPC / metrics ports |
| `StatefulSet` | `<cluster>-nodes` | `kvstore-node` replicas; per-pod PVC when NVMe tier is configured |

The reconcile loop is GET → CREATE-if-absent → PATCH-on-drift, scoped
to the spec subset the operator owns. Child resources carry an
`OwnerReference` back to the parent CR so deletion cascades.

Status (`.status.nodesActive`, `Ready` condition) is derived from the
StatefulSet's `ReadyReplicas`. The richer
`joining / draining / unreachable` breakdown will land once the
membership FSM exposes it over etcd (Phase H-2).

Still on the punch-list (Phase H-2+):
- In-cluster etcd StatefulSet when `byoEtcd: false`
  (placeholder DNS is written into the ConfigMap today).
- Control-plane StatefulSet (CP gRPC service).
- mTLS Secret material (cert-manager integration).
- DaemonSet flavour for labelled GPU hosts.

## Build & test

```bash
cd src/operator
go build ./...
go test ./internal/controller -v
```

The 11 controller tests run against the controller-runtime fake
client — no kube-apiserver or kind cluster required.

## Try it out

```bash
# 1. Apply the CRDs (regenerate with `make manifests` once Helm chart lands).
kubectl apply -f config/crd/bases/

# 2. Start the operator (or run out-of-cluster: go run ./cmd/manager).
kubectl apply -f config/deploy/operator.yaml

# 3. Create a cluster CR.
kubectl apply -f - <<'YAML'
apiVersion: kvcache.alluxio.io/v1alpha1
kind: KVCacheCluster
metadata:
  name: demo
  namespace: kvcache
spec:
  nodeReplicas: 3
  image: ghcr.io/alluxio/kvcache:dev
  nixlBackend: tcp
  tier:
    pinnedBytes: 32Gi
    dramBytes:   128Gi
    nvmePath:    /var/lib/kvcache/nvme.bin
    nvmeBytes:   1Ti
YAML

# 4. Watch the StatefulSet roll up.
kubectl -n kvcache get sts demo-nodes -w
```
