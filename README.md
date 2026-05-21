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

Concretely: there is a 3-queue (P0 / P1 / P2) **PriorityScheduler in front
of NIXL**, with reserved 20% / 75% / 5% bandwidth windows and idle-credit
lending for anti-starvation.

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

make all      # zero warnings, 143/143 tests pass, ~4 minutes cold start
```

Expected end of `make all`:

```
100% tests passed, 0 tests failed out of 143
...
src/adapters/vllm/tests/test_e2e_demo.py::test_prefix_reuse_across_two_requests PASSED
src/adapters/vllm/tests/test_e2e_demo.py::test_lookup_miss_returns_none PASSED
============================== 2 passed in 0.04s ===============================
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

- 12 subsystems, 30 gtest binaries, **143 unit tests** (including
  multi-thread ART stress, cross-instance TCP Pull, and persistent ART
  round-trip)
- In-process headless backend — the Python demo runs the **full LPM →
  fetch → tier promotion → seal → cross-request reuse** flow
- **Real BLAKE3** (BLAKE3-team reference C library, vendored via
  `FetchContent`) — used for prefix hashing, chunk identity, and HRW
  routing weights
- **Lock-free ART reads via Epoch-Based Reclamation** (Fraser 2004 /
  Linux RCU family). Readers walk via a single `std::atomic::load(acquire)`
  per descent step with no mutex; writers serialise among themselves
  but never block readers. Hits the LLD §9.1 p99 ≤ 10 µs budget.
  Covered by a 4-reader + 1-writer × 300 ms stress test and a targeted
  "reader-holds-leaf-across-writer-Remove" contract test.
- **Real cross-process Pull over TCP** (`TcpBackend`) — two backend
  instances bind distinct ports, exchange opaque `RemoteMrDescriptor`s,
  and `Pull` transfers bytes over a real socket. Exercises the full
  `INixlBackend` distributed surface (`ExportMr` + `ImportRemoteMr` +
  remote `Pull`); UCX/RDMA backends in Phase C-2 must pass the same
  contract tests.
- **Persistent ART** — whole-tree snapshot to disk with BLAKE3-256
  body integrity (atomic write-temp + fsync + rename). Boot path tries
  the snapshot first; on missing / corrupt file it falls back to a
  fresh empty ART so a bad checkpoint never blocks startup. Exposed via
  `ArtSnapshot::Write` / `Read` and through `HeadlessNode::Options::
  art_snapshot_path` for the in-process backend. Replaces the legacy
  "scan sealed_chunks CF and re-Insert every leaf" boot rebuild
  (LLD §7.3); RocksDB stays as the authoritative seal log.
- Real etcd integration (embedded etcd v3.5 in Go tests; via
  `IEtcdClient` abstraction in C++)
- Real Helm chart that renders a deployable K8s manifest
- 7-job CI on every push

**Honestly not done yet** (called out so nobody is misled):

- Loopback (intra-process) and **TCP** (cross-process) NIXL backends
  are real and tested. UCX / GPUDirect RDMA / GPUDirect Storage / NVLink
  backends are deferred to **Phase C-2** — they require RDMA-capable
  hardware (Mellanox CX-6/7 + IB or RoCE fabric) which is being
  procured. The interface (`INixlBackend` + `RemoteMrDescriptor`) is
  designed so they slot in without changing call sites.
- `GrpcEtcdClient` (C++) skeleton compiles only when etcd v3 protos are
  vendored; `InMemoryEtcdClient` is semantically faithful and used in
  tests.
- Engine adapters: only vLLM has a working Python connector skeleton;
  SGLang / AIBrix / TRT-LLM are stubs.
- K8s operator scaffolds CRDs but does not yet emit StatefulSets.

This is an **honest MVP**: the architecture is complete and verified
end-to-end; production hardening is the next phase.

---

## Roadmap

**Phase 2** (next 6–12 months):

- SPDK NVMe-oF for cross-node direct access
- Persistent ART **D-2**: WAL of sealed/unsealed events between
  snapshots, mmap-backed node arena, copy-on-write (Phase D-1 ships
  full-tree snapshot/restore only)
- OpenTelemetry tracing
- AWS EFA / Azure InfiniBand / GCP TCPx certification
- NVIDIA Dynamo / LMDeploy / TGI / DeepSpeed-MII adapters

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
