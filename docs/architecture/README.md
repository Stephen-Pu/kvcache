# Architecture Diagrams

Two diagrams covering the kvcache system architecture.

| Diagram | Audience | Focus |
|:---|:---|:---|
| **[System Overview](./system-overview.md)** | Architects, ops | Deployment topology — Control Plane + GPU Node Pool + Cold Tier |
| **[Integration & Transport Stack](./integration-stack.md)** | Integrators, eng leads | Layered stack from engines down to hardware |

Both are authored in [Mermaid](https://mermaid.js.org/), which GitHub renders natively. To edit, open the `.md` file and modify the Mermaid block; no graphics tool needed.

For pixel-perfect alternatives (Inkscape / Excalidraw / drawio source), see the corresponding `.svg` / `.excalidraw` files in this directory once added.

---

## Quick view

### System overview (snapshot)

```
   ┌─ Control Plane (3×) ─┐         ┌─ GPU Node Pool ─┐         ┌─ T4 Cold ─┐
   │ Membership Manager   │ ◀────── │ Engine + KAgent │ ──────▶ │ S3 / OSS  │
   │ Tenant Manager       │  gRPC   │ + KVStore Node  │  Alluxio│ / GCS /   │
   │ Routing Coordinator  │         │ + 5-tier (HBM   │         │ Azure Blob│
   │ (3D quota · HRW)     │         │ /Pinned/DRAM/   │         │           │
   │                      │         │ NVMe/Cold)      │         │           │
   └──────────────────────┘         └────────┬────────┘         └───────────┘
                                             │
                                  cross-node NIXL Pull
                                  (server-initiated)
```

### Integration stack (snapshot)

```
Workflow:   vLLM | SGLang | TRT-LLM | AIBrix | (Dynamo P1)
              │
APIs:       C ABI | Python (cffi) | Go | Rust (P2)
              │
Core ABI:   lookup · reserve · publish · fetch · seal · release
              │
Engine:     ② Prefix-reuse ART  ③ 5-Tier Storage  ④ Streaming Ingest
              │
Transport:  NIXL  →  GDR · UCX · GDS · NVLink · TCP   (HIXL = Phase 2)
              │
Hardware:   GPU · NIC · NVMe · Object Storage
```

See linked documents for the rendered Mermaid versions with full annotations.
