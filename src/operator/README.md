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
| `ServiceAccount` | `<cluster>-sa` | identity for `kvstore-node` and control-plane pods |
| `ConfigMap` | `<cluster>-config` | cluster identity, NIXL backend, tier sizes, etcd endpoints |
| `Service` (headless) | `<cluster>-nodes` | per-pod DNS + gRPC / metrics ports for kvstore-node |
| `StatefulSet` | `<cluster>-nodes` | `kvstore-node` replicas; per-pod PVC when NVMe tier is configured |
| `Service` (headless) | `<cluster>-etcd` | etcd client + peer ports — emitted unless `byoEtcd: true` |
| `StatefulSet` | `<cluster>-etcd` | 3-replica in-cluster etcd peer group; per-pod PVC for `/var/lib/etcd` |
| `Service` (headless) | `<cluster>-cp` | control-plane gRPC port |
| `StatefulSet` | `<cluster>-cp` | 3-replica control-plane, leader-elected through etcd |
| `Secret` (kubernetes.io/tls) | `<cluster>-mtls` | self-signed CA + leaf cert; mounted at `/etc/kvcache/tls/` on every pod |

A second controller reconciles `KVCacheTenant` CRs: it validates the
spec (`tenantID` hex, parseable quota quantities, allowed
`defaultPriority`, parent `clusterRef` in the same namespace) and
writes a `Validated` condition into `.status`. The real "push quotas
to nodes" path through the control-plane lands in Phase H-4.

Membership status (`.status.nodesActive` etc.) is sourced from the
in-cluster etcd's `/nodes/` prefix when reachable; the operator falls
back to the kvstore-node StatefulSet's `ReadyReplicas` when etcd is
slow / unreachable so a cold start still surfaces something useful.

The reconcile loop is GET → CREATE-if-absent → PATCH-on-drift, scoped
to the spec subset the operator owns. Child resources carry an
`OwnerReference` back to the parent CR so deletion cascades.

Status (`.status.nodesActive`, `Ready` condition) is derived from the
StatefulSet's `ReadyReplicas`. The richer
`joining / draining / unreachable` breakdown will land once the
membership FSM exposes it over etcd (Phase H-2).

Still on the punch-list (Phase H-4+):
- cert-manager integration (`useCertManager: true` switch that emits
  Certificate CRs instead of synthesising key bytes locally).
- mTLS cert rotation (today's Secret is generate-once).
- DaemonSet flavour for labelled GPU hosts.
- Real CP-side wiring that pushes validated tenant quotas to etcd —
  this controller is admission-side only.
- A `state` field in the membership FSM so the operator can split
  the `joining / draining / unreachable` counts (today: just
  `nodesActive`).

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
