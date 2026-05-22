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

Still on the punch-list (Phase H-5+):
- cert-manager integration (`useCertManager: true` switch that emits
  Certificate CRs instead of synthesising key bytes locally).
- DaemonSet flavour for labelled GPU hosts.
- A `state` field in the membership FSM so the operator can split
  the `joining / draining / unreachable` counts (today: just
  `nodesActive`).

Phase H-4 landed:
- mTLS leaf rotation (~90-day validity, regenerated when <1/3 of
  the lifetime remains; CA stable across rotations).
  `.status.mtlsCertNotAfter` surfaces the next expiry. Reconcile
  rotates via a 6h RequeueAfter tick.
- Operator-side `TenantPublisher` writes validated KVCacheTenant
  specs to `/kvcache/tenants/<cluster>/<tenant_id>` in the
  per-cluster etcd; kvstore-node and the CP watch that prefix for
  live quota updates. Both `Validated` and `Published` conditions
  surface on `KVCacheTenant.status.conditions`.

## Build & test

```bash
cd src/operator
go build ./...
go test ./internal/controller -v
```

The 39 controller tests run against the controller-runtime fake
client — no kube-apiserver or kind cluster required, ~3 seconds end
to end.

### Opt-in kind e2e

A separate, slower test suite (`src/operator/test/e2e/`, build tag
`e2e`) brings up a real kind cluster, applies the CRDs, runs the
operator in-process against the kind apiserver, and asserts:

- the eight-resource fan-out (SA / ConfigMap / nodes-Svc / nodes-STS
  + etcd-Svc / etcd-STS + cp-Svc / cp-STS) actually appears on a real
  apiserver;
- foreground cascade deletion through K8s' garbage collector removes
  child StatefulSets when the parent CR goes away — a path the fake
  client can't simulate.

Pods CrashLoopBackOff (kvstore-node `main.cpp` is a stub) — the
e2e only checks object shape, not workload liveness. Invocation:

```bash
make e2e-operator          # ~45s; requires docker + kind + kubectl
```

## Try it out

```bash
# 1. Apply the CRDs.
kubectl apply -f config/crd/

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
