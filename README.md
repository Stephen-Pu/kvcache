# KV Cache

> **Enterprise-grade, vendor-neutral KV Cache data layer for LLM inference.**
>
> 17.5× faster · 94% cheaper on 100K-token RAG · runtime safety-net that **refuses to lose to recompute**.

[![CI](https://github.com/Stephen-Pu/kvcache/actions/workflows/ci.yml/badge.svg)](https://github.com/Stephen-Pu/kvcache/actions)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![Go 1.22](https://img.shields.io/badge/Go-1.22+-blue.svg)](https://go.dev/)

---

## The number

For a single global-bank compliance workload (Llama-3.1-70B, 100K-token RAG prompt, 100 analysts, 50 queries/day each, $4/h H100):

| | Cold start | With KV Cache | Δ |
|---|---|---|---|
| End-to-end latency | ~525 s | ~30 s | **17.5×** |
| GPU·s per query | 4200 | 240 | 17.5× |
| Cost per query | $1.17 | $0.07 | **−94%** |
| **Annual cost / cluster** | **$8.5 M** | **$487 K** | **−$8 M/yr** |

<sub>**Assumptions:** Llama-3.1-70B, 8-way TP on 8× H100, fp16, 100K-token prompt with 90K shared prefix (system prompt + RAG). 95% cache hit rate at cluster steady state. Cold-start prefill at ~200 tok/s. Trace + math: v2.0 §13.7 (private design doc).<br>
**Your mileage depends primarily on** *(a)* **prefix-sharing rate across your workload** — this scenario assumes 90%+ of tokens shared via system prompt + RAG context, which is realistic for compliance / legal / customer-support but **not for ad-hoc chatbots**; and *(b)* **steady-state cache hit rate** (here 95%).</sub>

---

## Why this is different

Five things that make this **not just another prefix cache**:

### 1. KV-aware routing — cache location drives routing, not request origin

Most prefix caches route requests by **affinity** (whoever called gets the local cache). We route by **where the cache actually is**:

```
Request comes in to Node A. But cache for this prefix lives on Node B.
  → HRW(prefix_hash) ranks {B, C, A}
  → Overlap Score from peer bloom sketches: B has 6200 hit chunks
  → Route to B. Inter-node NIXL Pull (~35 ms) << recompute (~500 s).
```

This is the single most important design decision and the most visible
differentiator from vLLM/SGLang prefix cache (process-local) and Mooncake
(distributed but request-affinity-routed).

### 2. Server-Pull-Only NIXL — the prerequisite for real multi-tenancy

The data plane uses **NVIDIA NIXL** (UCX over IB/RoCE, GPUDirect RDMA,
GPUDirect Storage, NVLink, TCP fallback), but with one strict rule:

> **The server pulls. The client never pushes.**

Why: only the server-side scheduler can honor per-tenant quotas, priority
classes, and admission control. Client-initiated push is fundamentally
incompatible with multi-tenancy QoS. Most KV cache projects skip this and
ship "first-come-first-served" data planes; this project takes the
constraint seriously.

Concretely: a 3-queue (P0 / P1 / P2) **PriorityScheduler sits inside
`NixlWrapper`**, with reserved 20% / 75% / 5% bandwidth windows,
idle-credit lending for anti-starvation, and per-tenant round-robin
within each class. Every Pull goes through the scheduler's dispatcher
thread, so admission decisions actually throttle the data plane —
not just an in-memory bookkeeping exercise. Per-tenant FIFOs are
populated by hashing each caller's `tenant_id` at the C ABI
boundary, so two clients in the same class round-robin instead of
one starving the other. Admissions, forced admissions, and queue
depth are exposed as Prometheus counters / gauges, and `kv.lookup` /
`kv.fetch` / `nixl.scheduled_pull` spans are emitted through an
OTel-shaped tracing facade — with a built-in OTLP/HTTP exporter
(`kvcache_otlp` library) so spans land in any standard OTel
collector (Tempo / Jaeger / alloy / Honeycomb refinery) without
extra glue. An operator can answer "why is this particular Fetch
slow" — not just "how often are Fetches slow".

### 3. Five-tier storage with lazy promotion and cross-tenant eviction

```
   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
   │  T0 HBM  │  │ T1 Pinned│  │  T2 DRAM │  │  T3 NVMe │  │ T4 Cold  │
   │  GPU-own │←─│  cudaHost│←─│  pageable│←─│ io_uring │←─│  Alluxio │
   │          │  │  + NIXL  │  │  +  2Q   │  │   /SPDK  │  │ multi-cl │
   │          │  │          │  │  + Ghost │  │   + GDS  │  │   oud    │
   └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘
                                                                   │
                                                          ↓
                                            S3 / OSS / GCS / Azure Blob
```

- **Lazy promotion on access** (never T4→T0 direct; always via T1 staging)
- **2Q + Ghost Cache** in T2 (prevents scan pollution)
- **GDS for tiles > 16 MB** (NVMe → GPU direct, host CPU completely idle)
- **Cross-tenant eviction**: first penalize over-quota tenants, then descend
  by priority class
- **Cold tier via Alluxio EE** — no reinvented multi-cloud UFS

### 4. Runtime safety net — the cache refuses to lose to recompute

Every fetch is gated by a runtime check:

```c
if (fetch_estimate_ms >= recompute_estimate_ms × 0.5)
    return KV_E_SAFETY_NET;   // engine falls back to recompute
```

If the cache cannot beat recompute by 2× margin, it **steps aside** and
lets the engine recompute. This catches pathological scenarios (cross-AZ
T4 fetch for a short prefix where re-running prefill is faster) at
runtime instead of relying on offline policy tuning.

This is the kind of mechanism that turns "cache will always help" — a
hidden, often-wrong assumption — into a runtime-verified invariant.

### 5. Vendor-neutral + design-first

No lock-in on any axis:

| | This project | vLLM-cache | Mooncake | LMCache | NVIDIA Dynamo |
|---|---|---|---|---|---|
| GPU vendor lock-in | None | None | None | None | NVIDIA-only |
| Engine lock-in | None (vLLM/SGLang/TRT-LLM/AIBrix via single C ABI) | vLLM-only | vLLM-only | vLLM-only | NVIDIA-aligned |
| Cloud lock-in | None (cold tier via Alluxio multi-cloud) | N/A | Single cloud | N/A | Single cloud |
| Multi-tenant QoS | Hard (3D quota + priority + RBAC + audit) | None | Soft | None | Soft |
| Process model | Cross-process (server-pull, sidecar agent) | In-process | Cross-process | In-process | Cross-process |

The architecture isn't an accident: **6 first principles + 83 traceable
decisions**, each numbered (D-PERF-1, L1-PS-7, ...) so any code review
can ask "which decision does this implement?". The design documents
exist; the code follows them.

---

## Performance hot path — boring but disciplined

The above headlines mean nothing if the hot path doesn't honor them.
Specifically:

- **Zero-copy end to end** — engine writes into a Pinned slot that is
  simultaneously a NIXL-registered memory region; the server's NIXL Pull
  reads from the same physical pages. No bounce buffers, no extra memcpy.
- **io_uring + SPDK dual backend** for NVMe (configurable per node)
- **Pinned host memory** via `mmap MAP_LOCKED` / `mlock` so RDMA can DMA
  without page-pinning at I/O time
- **Hot path enterprise checks ≤ 1µs** (per `D-PERF-2`); RBAC decisions
  cached for 1s, quota decisions per-tenant local atomic counters

These are table-stakes for any serious data-plane, but worth saying:
**the engineering hygiene matches the architectural claims**.

---

## Quickstart

```bash
git clone https://github.com/Stephen-Pu/kvcache.git
cd kvcache

# macOS
brew install cmake ninja go python helm

# Ubuntu 22.04
sudo apt-get install cmake ninja-build g++ python3-venv golang-1.22

python3 -m venv .venv && source .venv/bin/activate
pip install cffi pytest

make all      # zero warnings, 207/207 tests pass, ~4 minutes cold start
```

Expected end of `make all`:

```
100% tests passed, 0 tests failed out of 207
...
src/adapters/vllm/tests/test_e2e_demo.py::test_prefix_reuse_across_two_requests PASSED
src/adapters/vllm/tests/test_e2e_demo.py::test_lookup_miss_returns_none PASSED
src/adapters/sglang/tests/test_sglang_backend.py::test_store_then_retrieve_round_trip PASSED
src/adapters/aibrix/tests/test_aibrix_backend.py::test_put_then_get_round_trip PASSED
... (12 more SGLang / AIBrix backend tests)
============================== 14 passed in 0.1s ===============================
```

Full setup including troubleshooting: [BUILD.md](./BUILD.md).

---

## Architecture (4-layer view)

```
┌─────────────────────────────────────────────────────────────┐
│  L4 Integration  │ ⑪ Engine adapters    ⑫ Ops & telemetry   │
├─────────────────────────────────────────────────────────────┤
│  L3 Service      │ ⑨ Multi-tenant QoS   ⑩ Security + audit  │
├─────────────────────────────────────────────────────────────┤
│  L2 Coordination │ ⑥ Routing + bloom    ⑦ Cluster           │
│                  │ ⑧ Replication (deferred — KV recomputable)│
├─────────────────────────────────────────────────────────────┤
│  L1 Engine       │ ① Locator   ② Prefix-reuse ART           │
│                  │ ③ Tiered storage  ④ Streaming ingest     │
│                  │ ⑤ Data plane (NIXL)                       │
└─────────────────────────────────────────────────────────────┘
```

Repository layout: [`src/`](./src/) is the entire source tree (English,
GitHub-released). Each subsystem has its own README pointing back to the
LLD section it implements.

---

## What works today (and what doesn't)

**Working end-to-end** (run `make all` to verify):

- 12 subsystems, 38 gtest binaries, **207 unit tests** — multi-thread
  ART stress, cross-instance TCP Pull, persistent ART round-trip,
  WAL-incremental ART durability with torn-write recovery,
  concurrent PriorityScheduler, ScheduledPull through the NIXL
  dispatcher, HttpEtcdClient error-path coverage, TRT-LLM C++
  backend round-trip, OTel-shaped trace facade, OTLP/HTTP exporter
  encoder, the kvstore-node `NodeRuntime` (TCP readiness probe +
  Prometheus `/metrics` + `/healthz`), and the `NodeData` gRPC
  service driven through a real `grpc::Server` (`Lookup`,
  `Reserve`, `Publish`, `Fetch`, `Seal`, `Release` over the wire).
  Live-etcd integration tests run opt-in via `ETCD_ENDPOINT=...`;
  live-OTel-collector ones run via `OTLP_ENDPOINT=...`.
- In-process headless backend — the Python demo runs the full LPM →
  fetch → tier promotion → seal → cross-request reuse flow.
- **Real BLAKE3** for prefix hashing, chunk identity, and HRW
  weights (BLAKE3-team reference C library, vendored via FetchContent).
- **Lock-free ART reads via Epoch-Based Reclamation** — readers walk
  with a single `std::atomic::load(acquire)` per descent; writers
  serialise among themselves but never block readers. Hits the LLD
  §9.1 p99 ≤ 10 µs budget, covered by a 4-reader + 1-writer × 300 ms
  stress test plus a targeted leaf-pinning contract test.
- **Real cross-process Pull over TCP** (`TcpBackend`) — two backend
  instances bind distinct ports, exchange opaque MR descriptors, and
  `Pull` moves bytes through a real socket. Future UCX / RDMA
  backends slot into the same `INixlBackend` interface.
- **Persistent ART with WAL-incremental durability** — every
  Insert / Remove is appended (and `fdatasync`'d) to an append-only
  WAL file before the in-memory tree mutates; periodic
  `Checkpoint()` writes a fresh whole-tree snapshot with BLAKE3-256
  body integrity and truncates the WAL. Boot replays
  `snapshot + WAL tail` instead of rebuilding from the seal log,
  so a node carrying 10M sealed chunks is up in ms, not minutes.
  Torn writes (partial fsync before crash) are detected by the
  per-record CRC32 and silently truncated at the last-good offset
  on the next replay.
- **PriorityScheduler on the NIXL data path** with per-tenant fair
  queueing. Tenant identity flows from `kv_ctx_open(tenant_id="...")`
  through FNV-1a-64 into per-tenant FIFOs, so two clients in the same
  priority class round-robin instead of starving each other.
- **Real etcd, two C++ backends**: `HttpEtcdClient` talks to the etcd
  v3 JSON gateway over libcurl (no grpc dep needed — runs out of the
  box on a dev laptop). `GrpcEtcdClient` talks to the canonical
  gRPC API using vendored etcd v3 protos at
  `third_party/etcd-proto/` — automatically enabled when
  `find_package(gRPC CONFIG)` succeeds, with the stub branch staying
  in place for builds without grpc installed. Both implement the
  same `IEtcdClient` surface (KV + Lease + polling Watch), so
  swapping is a one-line construction change. The Go side uses
  embedded etcd v3.5 in tests.
- Real Helm chart that renders a deployable K8s manifest.
- **K8s operator** with two reconcilers: `KVCacheCluster` emits the
  full nine-resource tree per cluster — kvstore-node `StatefulSet` +
  headless `Service` + `ServiceAccount` + `ConfigMap` + self-signed
  mTLS `Secret`; a 3-replica in-cluster etcd `StatefulSet` + headless
  `Service` (skipped when `byoEtcd: true`); and a 3-replica
  control-plane `StatefulSet` + headless `Service` wired against the
  same etcd. `kubectl apply -f cluster.yaml` brings up the entire
  data + control plane with mTLS material in place. The mTLS leaf
  cert auto-rotates (~90-day validity, regenerated when <1/3 of
  the lifetime remains; CA stays stable across rotations);
  `.status.mtlsCertNotAfter` surfaces the next expiry. `KVCacheTenant`
  CRs are validated by a second reconciler (hex tenant ID, parseable
  quota quantities, parent cluster exists), then published to the
  per-cluster etcd at `/kvcache/tenants/<cluster>/<tenant_id>` so the
  kvstore-node / control-plane processes can watch for live quota
  updates; both `Validated` and `Published` conditions surface on
  `.status.conditions`. Membership status is read from the
  in-cluster etcd's `/nodes/` prefix when reachable, with a
  StatefulSet-ReadyReplicas fallback when it isn't. Drift,
  idempotency, OwnerReference cascade, etcd-status override, and
  tenant validation paths are covered by 39 unit tests against the
  controller-runtime fake client, **plus an opt-in `make
  e2e-operator` target** that spins a real kind cluster, applies the
  CRDs, and verifies the full eight-resource fan-out + cascade
  delete against a real apiserver.
- 7-job CI on every push.

**Honestly not done yet** (called out so nobody is misled):

- **Real RDMA backends** (UCX / GPUDirect RDMA / GDS / NVLink) await
  RDMA-capable hardware (Mellanox CX-6/7 + IB or RoCE fabric). The
  `INixlBackend` interface is built so they slot in alongside
  `TcpBackend` without touching call sites.
- **Engine adapters** — vLLM, SGLang, AIBrix, and TRT-LLM all ship
  working adapters against the Core ABI. The three Python adapters
  (vLLM / SGLang / AIBrix) are ~50 LOC shells on top of a shared
  `kvcache_core` package that holds the `cffi` substrate; the C++
  adapter (TRT-LLM) is a static archive linking `libkvcache.{so,dylib}`
  directly. Each surface mirrors its engine's expected verbs —
  SGLang's `lookup / store / retrieve / drop`, AIBrix's
  `get / put / delete / exists`, TRT-LLM's `Lookup / Store /
  Retrieve / Drop`.
- **K8s operator** — the `cert-manager` integration (`useCertManager:
  true` opt-in that emits Certificate CRs instead of self-signing)
  and the `joining / draining / unreachable` membership breakdown
  (needs a `state` field in the membership FSM) are still on the
  punch-list. Today's reconcilers emit the full cluster tree, the
  self-signed mTLS Secret with auto-rotation, and push validated
  tenant quotas through to the per-cluster etcd.

This is an **honest MVP**: the architecture is complete and verified
end-to-end; production hardening is the next phase.

---

## Roadmap

**Next** (6–12 months):

- UCX / GDR / GDS / NVLink NIXL backends once RDMA hardware arrives
- Streaming-Watch path on `GrpcEtcdClient` (today both etcd clients
  poll; the bidirectional Watch stream is a clean follow-up against
  the vendored protos)
- mmap-backed persistent-ART node arena + copy-on-write replication
  (D-2 today is snapshot + record-level WAL — already incremental,
  but the tree itself still rebuilds in memory at boot)
- SPDK NVMe-oF for cross-node direct access
- AWS EFA / Azure InfiniBand / GCP TCPx certification
- SGLang / TRT-LLM / NVIDIA Dynamo / LMDeploy / TGI / DeepSpeed-MII adapters

**Phase 3** (12–24 months):

- FedRAMP / sovereign-cloud certification path
- SPIFFE internal identity
- Cross-cluster KV federation (if validated by Phase-2 customers)

---

## Contributing

Issues and PRs welcome. Before sending a PR, please:

1. `make all` passes locally with zero warnings
2. New code has a unit test
3. Any architectural change references an LLD decision ID
   (`D-PERF-1`, `L1-PS-7`, ...) — if the change predates the decision,
   propose the new decision first as an Issue

The design documents (HLD, LLD, v1.0–v2.0 discussion drafts) are not
in this repo; they are made available to active contributors on request.

## License

[Apache-2.0](./LICENSE). See the LICENSE file for the full text.

---

<sub>Built with care by [Stephen Pu](https://github.com/Stephen-Pu) (Alluxio).
Inspired by, and grateful to: vLLM, SGLang, Mooncake, LMCache, NVIDIA Dynamo,
Alluxio EE.</sub>
