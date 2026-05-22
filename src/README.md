# KV Cache — Source Tree

Implementation of the enterprise-grade, vendor-neutral KV Cache data layer.

- High-Level Design: `../KV_Cache_HLD_高阶架构设计.md`
- Low-Level Design: `../KV_Cache_LLD_详细设计.md`

> **Language policy**: all source code, comments, identifiers, and documents
> under `src/` must be in **English** (this tree is intended for GitHub release).

## Language matrix

| Component                    | Language       | Rationale                                                                 |
| ---------------------------- | -------------- | ------------------------------------------------------------------------- |
| `kvstore-node/`              | C++20          | Hot path ≤1µs; NIXL / io_uring / SPDK / RocksDB / CUDA are C/C++ native.  |
| `kvagent/`                   | C++20          | Shared `/dev/shm` ring with engine; latency-critical.                     |
| `core-abi/` (`libkvcache.so`)| C ABI / C++    | Stable ABI consumed by Python / Go / Rust bindings.                       |
| `control-plane/`             | Go             | Etcd / K8s ecosystem; not on hot path; lease-based leader election.       |
| `operator/`                  | Go             | Built with `operator-sdk` / `controller-runtime`.                         |
| `kvctl/`                     | Go             | Shares CP gRPC client code.                                               |
| `adapters/core/`             | Python         | Shared `cffi` wrapper over libkvcache.so; engine adapters depend on it.   |
| `adapters/vllm,sglang,aibrix`| Python         | Match host engine language; thin shells over `adapters/core/`.            |
| `adapters/trtllm/`           | C++            | TRT-LLM `KVCacheManager` backend is C++.                                  |
| `tests/unit/`                | C++ (gtest)    | Micro-benchmarks, hot-path assertions.                                    |
| `tests/e2e/`                 | Python (pytest)| End-to-end scenarios (100K RAG, multi-tenant, cross-node).                |

## Layout

```
src/
├── include/kvcache/         # Public C ABI headers (LLD §6.1.2)
├── core/                    # Shared C++ libs (Locator, hashing, gRPC protos)
├── kvstore-node/            # Data-plane node (subsystems ② ③ ④ ⑤ ⑥ ⑦ ⑨ ⑩ ⑫)
├── kvagent/                 # Sidecar (subsystem ⑪ engine-facing half)
├── core-abi/                # Implementation of libkvcache.so
├── control-plane/           # CP service (subsystem ⑦ CP half, ⑨ quota authority)
├── operator/                # K8s CRD + controller
├── kvctl/                   # Operator CLI (LLD §8.3)
├── adapters/                # Engine-specific adapters (LLD §6.1.4)
├── deploy/                  # Helm chart + CRDs (LLD §8.2)
└── tests/                   # Unit + E2E
```

## Build

Top-level: CMake (C/C++) and Go workspaces are independent.

```bash
# C/C++
cmake -S src -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# Go components
cd src/control-plane && go build ./...
cd src/kvctl        && go build ./...
cd src/operator     && go build ./...

# Python adapters
pip install -e src/adapters/core      # shared cffi substrate
pip install -e src/adapters/vllm
pip install -e src/adapters/sglang
pip install -e src/adapters/aibrix
```

## Subsystem ownership

| ID | Name                       | Location                              | LLD §  |
| -- | -------------------------- | ------------------------------------- | ------ |
| ①  | Identity & addressing      | `core/common/locator.*`               | §3.1   |
| ②  | Prefix-reuse engine        | `kvstore-node/src/prefix/`            | §3.2   |
| ③  | Tiered storage             | `kvstore-node/src/tier/`              | §3.3   |
| ④  | Streaming ingestion        | `kvstore-node/src/ingest/`            | §3.4   |
| ⑤  | Transport (NIXL)           | `kvstore-node/src/transport/`         | §3.5   |
| ⑥  | Routing & placement        | `kvstore-node/src/routing/`           | §4.2   |
| ⑦  | Cluster coordination       | `kvstore-node/src/cluster/` + `control-plane/` | §4.1 |
| ⑧  | Replication & persistence  | (MVP: skipped)                        | §4.3   |
| ⑨  | Multi-tenant QoS           | `kvstore-node/src/qos/` + `control-plane/internal/quota/` | §5.1 |
| ⑩  | Security & compliance      | `kvstore-node/src/security/`          | §5.2   |
| ⑪  | Engine integration         | `kvagent/` + `adapters/` + `core-abi/`| §6.1   |
| ⑫  | Observability & ops        | `kvstore-node/src/obs/` + `kvctl/`    | §6.2   |

Every scaffolded file carries a header comment with its LLD section reference
and a `TODO(<owner>):` marker. Replace the placeholders as each subsystem lands.
