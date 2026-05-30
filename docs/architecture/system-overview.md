# System Overview

End-to-end deployment topology: Control Plane, GPU Node Pool (co-located), and the multi-cloud Cold Tier.

---

```mermaid
flowchart LR
    %% =========================================================
    %% LEFT: Control Plane (3 replicas)
    %% =========================================================
    subgraph CP["🎛 Control Plane (3 replicas + Etcd 3 replicas)"]
        direction TB
        CP1["Membership Manager<br/>node FSM · failure detection<br/>Etcd lease 10s / renew 3s"]
        CP2["Tenant Manager<br/>3D quota · RBAC · audit · GDPR<br/>(L3 ⑨⑩)"]
        CP3["Routing Coordinator<br/>HRW + Bloom sketch broadcast<br/>(L2 ⑥)"]
        CP1 -.- CP2 -.- CP3
    end

    %% =========================================================
    %% MIDDLE: GPU Node Pool (co-located, multiple instances)
    %% =========================================================
    subgraph POOL["💻 GPU Node Pool — Co-located (D-DEPLOY-1)"]
        direction TB
        subgraph N1["GPU Node ①"]
            direction TB
            ENG["Engine Process<br/>vLLM · SGLang · TRT-LLM · AIBrix"]
            KAGENT["KVAgent (sidecar)<br/>local routing cache · bloom sketches · handle table"]
            KSTORE["KVStore Node<br/>② ART (lock-free reads, p99 < 10µs)<br/>RocksDB metadata · WAL incremental"]
            subgraph TIERS["5-Tier Storage (L1 ③)"]
                direction TB
                T0["T0 HBM (GPU-owned)"]
                T1["T1 Pinned (NIXL MR, zero-copy)"]
                T2["T2 DRAM (2Q + Ghost Cache)"]
                T3["T3 NVMe (io_uring / SPDK + GDS)"]
                T0 --> T1 --> T2 --> T3
            end
            ENG <-->|"shmem ring<br/>~2µs"| KAGENT
            KAGENT <-->|"NIXL Pull<br/>server-initiated"| KSTORE
            KSTORE --> TIERS
        end
        N2["GPU Node ② … ⓝ<br/>(same components)"]
    end

    %% =========================================================
    %% BOTTOM: T4 Cold Tier
    %% =========================================================
    subgraph CT["🌐 T4 Cold Tier — Multi-Cloud"]
        UFS["Pluggable Object UFS<br/>S3 · OSS · GCS · Azure Blob<br/>zstd-1 compression · SSE-S3 / SSE-KMS"]
    end

    %% =========================================================
    %% Connections
    %% =========================================================
    CP <==>|"gRPC + Etcd watch<br/>metadata · quota stream"| POOL
    POOL <==>|"cross-node NIXL Pull<br/>priority-scheduled<br/>(L2-RP-1 HRW routing)"| POOL
    POOL ==>|"demote · async write"| CT

    %% Styling
    classDef cpStyle fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000
    classDef poolStyle fill:#e1f5ff,stroke:#0288d1,stroke-width:2px,color:#000
    classDef nodeStyle fill:#e3f2fd,stroke:#0d47a1,stroke-width:1px,color:#000
    classDef tierStyle fill:#e8f5e9,stroke:#1b5e20,stroke-width:1px,color:#000
    classDef coldStyle fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000

    class CP cpStyle
    class CP1,CP2,CP3 cpStyle
    class POOL poolStyle
    class N1,N2 nodeStyle
    class ENG,KAGENT,KSTORE nodeStyle
    class TIERS,T0,T1,T2,T3 tierStyle
    class CT,UFS coldStyle
```

---

## Optimization goals

| Goal | Subject to |
|:---|:---|
| **Lookup p99 < 10 µs** | hot-path checks ≤ 1 µs (D-PERF-2) · lock-free ART reads (EBR) |
| **Tier latency << GPU recompute** | runtime safety-net (D-PERF-1) · GDS for tiles > 16 MB |
| **Hard multi-tenancy** | server-pull-only NIXL · 3D quota · 3 priority classes (P0/P1/P2) |
| **Data plane survives CP outage** | local routing cache · CP-independent (L2-CC-7) |
| **Zero-copy end-to-end** | Pinned slot ≡ NIXL MR · no bounce buffers |
| **No KV migration on rebalance** | KV is recomputable; ~1/N affected on join/leave (L2-RP-5) |

---

## Why **co-located** (and not Mooncake-style disaggregated pools)

| Aspect | Disaggregated pools (Mooncake) | Co-located (this design) |
|:---|:---|:---|
| **Scheduling complexity** | Central Conductor with 3 schedulers | Engine-local; CP only does metadata |
| **Disagg of prefill / decode** | First-class | Delegated to engine (vLLM v1 disagg, etc.) |
| **Cross-node KV motion** | Frequent (P-pool → D-pool) | Lazy (only on cache miss / membership change) |
| **Operational footprint** | 2 pod classes + Conductor + Store | 1 pod class (GPU node) + CP |
| **Idle CPU/DRAM utilization** | Wasted in storage-only nodes | Used (D-DEPLOY-1) |
| **Best fit for** | Single-tenant hyperscale | Multi-tenant enterprise |

The trade-off: we forgo central P/D coordination for **deployment simplicity and tenant isolation**.
For customers that need true disaggregation, the engine layer handles it (vLLM v1, SGLang, TRT-LLM all have disagg modes).

---

## Six core invariants

These hold across all scenarios (lookup / fetch / publish / eviction / membership change / network partition):

1. **Data plane never depends on Control Plane** — lookup/fetch/publish continue during CP outage
2. **Writes never block reads** — unsealed KV is not in ART; seal is atomic flip
3. **Zero-copy end to end** — engine writes into Pinned slot ≡ NIXL MR; server pulls same physical pages
4. **Server pulls, never client pushes** — NIXL Pull only; QoS lives server-side
5. **Per-node metadata strong-consistent; cross-node eventually consistent** — local RocksDB+ART atomic; cluster Bloom 30 s tick
6. **Cache always degrades to recompute** — D-PERF-1 safety-net; cache correctness never threatens system semantics

---

## Related

- [Integration & Transport Stack](./integration-stack.md) — engines down to hardware
- [Main README](../../README.md) — value proposition + quickstart
