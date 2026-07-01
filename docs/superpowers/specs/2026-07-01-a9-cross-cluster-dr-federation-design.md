# A9 — Cross-cluster KV federation: DR warm-standby

Status: **Design** (Phase-3; roadmap Gantt "Cross-cluster KV federation", 180d).
Date: 2026-07-01.
Scope: this spec covers **A9 only**. A10 (FedRAMP / sovereign-cloud) is a
separate spec that layers on the trust/topology choices made here.

## 1. Problem & goal

When a KV-cache cluster fails over, a cold standby forces every in-flight and
new request to **recompute** its KV prefixes at once — a thundering-herd that
spikes tail latency and GPU load exactly when the system is already degraded.

**Goal:** keep a standby cluster **warm** — pre-populated with the primary's
hot working set — so failover avoids the cold-start storm. KV is regenerable,
so this is a best-effort cache-warming problem, not a durable-replication one.

## 2. Requirements (decided)

| # | Decision | Rationale |
|---|----------|-----------|
| R1 | **Active/passive, 1:1** — one standby mirrors one primary; standby serves no client traffic until failover. | Simplest, cleanest RPO/RTO story. |
| R2 | **Replicate the warm working set only** — chunks in the hot tiers (T1/T2) at seal time, not the full corpus / cold tier. | Bounded WAN bandwidth; standby sized for the working set that matters at failover. |
| R3 | **Best-effort async, ~seconds RPO** — no synchronous ack on the primary write path. | Zero hot-path latency added to the primary; a few seconds of newest chunks simply recompute on failover. |
| R4 | **Shared SPIFFE trust domain** — standby replicator authenticates as an A11 internal workload. | Primary + standby are one operator/security boundary; reuses `VerifyInternalPeer`. |
| R5 | **Primary is oblivious** — no primary-side replication state or hot-path change; a standby can be attached/detached without touching the primary. | Operational safety; the DR mechanism can't destabilise the thing it protects. |

### Non-goals
- Not durable/strong replication (KV is a regenerable cache).
- Not active/active, not multi-standby, not capacity pooling, not a global
  management plane (those are separate federation modes, out of scope).
- Failover **cutover** (DNS/config repoint, promotion, fencing) is an
  ops/CP concern — noted where it touches the design, but the data-path spec
  does not implement it.

## 3. Architecture (Approach A — pull-based replica subscriber)

The standby drives everything; the primary only serves its existing read
surfaces. All new code lives on the standby side plus one small read RPC on
the primary.

```
  PRIMARY cluster (unchanged, oblivious)          STANDBY cluster (passive)
  ┌───────────────────────────────┐              ┌──────────────────────────────┐
  │ nodes seal KV                  │              │ ReplicationConsumer          │
  │  → EventStream (KV_EVENT_ADD)  │◀── subscribe─│   seed → watch → cursor      │
  │  → Subscribe RPC  (M-2)        │              │   warm-set filter            │
  │  → ReplicaFetch(locator) (NEW) │◀── fetch ────│   fetch worker pool          │
  │      resolves via G-1 index    │─── bytes ───▶│   → Reserve/Publish/Seal     │
  └───────────────────────────────┘   mTLS/SPIFFE└──────────────────────────────┘
       trust domain = one; standby replicator SVID gated by VerifyInternalPeer (A11)
```

## 4. Components (standby, new)

- **`ReplicationConsumer`** — seed-then-watch orchestrator, mirroring the
  established `NodeDirectory` / `IdentityWatcher` / `ClusterViewWatcher`
  pattern (seed on start, then live-watch, keep-last-good on error).
- **Warm-set filter** — admits only `ADD` events flagged in a hot tier at seal
  time. Requires a small additive field on `kv_event_t` (see §6).
- **Replication cursor** — persists the last-applied event `epoch` (already a
  field on `kv_event_t`) so a reconnect resumes from the cursor rather than
  re-seeding the whole set.
- **Fetch worker pool** — bounded queue; each worker dequeues a locator,
  issues `ReplicaFetch`, and commits locally. Bounded so the standby degrades
  gracefully (drops coldest-first) rather than growing unboundedly.

## 5. Data flow

1. Primary node seals a warm-tier chunk `C` → emits
   `KV_EVENT_ADD{locator, warm=true, epoch, node_id}`.
2. Standby `ReplicationConsumer` (subscribed via M-2 `Subscribe`) receives it;
   the warm-set filter passes; enqueue `C.locator`.
3. A fetch worker calls `ReplicaFetch(C.locator)` on the primary owner node
   (addressed via `node_id` from the event) and receives the chunk bytes.
4. The worker commits locally through the normal ingest path
   (`Reserve(locator) → write → Publish → Seal(tokens)`), so the standby's ART
   index + tiers populate exactly as if the chunk had been sealed locally.
5. The cursor advances to `C.epoch`.

On **failover**, ops repoints the engine adapters / `kvagent` to the standby
CP and promotes the standby to active. Because its hot tiers are already warm,
new traffic mostly hits, not recomputes.

## 6. Interfaces (new / changed)

- **`kv_event_t` — additive `warm` (tier) hint.** The event must tell the
  consumer whether the sealed chunk is in a hot tier, so the filter runs
  standby-side without a primary round-trip. Additive field; no ABI break for
  existing subscribers.
- **`ReplicaFetch(locator) → bytes` — new primary-side read RPC.** The event
  carries a *locator* (prefix_hash), not raw tokens, and the existing `Fetch`
  is by-handle-from-`Lookup` (which needs tokens). `ReplicaFetch` resolves a
  locator directly to its chunk bytes via the **G-1 content index**
  (`LocatorContentKey → chunk_path → leaf`). Read-only; gated by
  `VerifyInternalPeer` (A11). *(Rejected alternative: widen the replication
  event to carry the full token sequence — larger events, and duplicates the
  locator→chunk resolution the content index already does.)*
- **A11 extension (small):** accept a `replica` workload kind (or reuse
  `node`) in `VerifyInternalPeer` for the replication link.

## 7. Error handling & resilience

- **Link down:** keep cursor + last-good state; on reconnect, re-subscribe
  from the cursor `epoch`. If the primary's event buffer no longer covers the
  gap, fall back to a reconcile/re-seed pass.
- **Fetch failure / already-evicted (`NOT_FOUND`):** skip after bounded
  retries — a missing chunk just recomputes on failover. Never block the
  stream on one chunk.
- **Backpressure:** bounded fetch queue; when the standby can't keep up it
  drops the coldest queued chunks. Warmth degrades gracefully; the primary
  never stalls (it is oblivious).
- **Split-brain / dual-active:** out of scope for the data path; promotion
  fencing (e.g., a CP fencing token) is required at the ops layer — flagged,
  not implemented here.

## 8. Testing (fully local — no second physical cluster)

Two `HeadlessNode`s + an `InMemoryEtcdClient` in one process, following the
existing `cross_process_pull` / `node_data_service` integration patterns:

- Seal a mix of warm and sub-threshold chunks on the "primary" node.
- Run the `ReplicationConsumer` against the primary's in-process `EventStream`
  + `ReplicaFetch`.
- Assert the "standby" node's `Lookup` finds **all warm** chunks and **none**
  of the sub-threshold ones.
- Assert the cursor resumes correctly after a simulated disconnect (no
  duplicate commits, no missed warm chunks).
- Assert an evicted-before-fetch chunk is skipped cleanly (stream continues).
- Unit-test the warm-set filter, cursor advance/resume, and fetch-worker
  retry/drop in isolation.

## 9. Deferred / follow-on

- CP-side failover automation (promotion, fencing token, adapter re-point).
- Active/active and 1:N topologies (other federation modes).
- WAN transport hardening (compression on the replication link — could reuse
  the B2 codec / B3 cold-tier zstd; TLS is via the SPIFFE mTLS link).
- A10 (FedRAMP/sovereign) resolution of R4: Regulated Mode **forbids
  cross-boundary replication** (its BoundaryGuard denies an out-of-boundary
  target), so a DR standby is always in-boundary and the **shared SPIFFE trust
  domain (R4) stays correct**. Distinct-trust-domain, cross-boundary federation
  (SPIFFE federation bundle) is a separate, non-regulated future — not enabled
  by Regulated Mode. See
  [A10 §5](2026-07-01-a10-regulated-mode-design.md#5-a9--a10-reconciliation).

## 10. Reuse ledger (what already exists)

| Need | Reuse |
|------|-------|
| Event subscription | M-2 `Subscribe` / `EventStream`, `KV_EVENT_ADD` |
| Cross-node byte transfer | M-3 `Fetch` + `RemoteMrDescriptor` (transport for `ReplicaFetch`) |
| Locator → chunk resolution | G-1 content index (`LocatorContentKey`) |
| Local commit on standby | `HeadlessNode` `Reserve/Publish/Seal` ingest |
| Replication-link auth | A11 `VerifyInternalPeer` (internal workload SVID) |
| Seed-then-watch consumer shape | `NodeDirectory` / `IdentityWatcher` / `ClusterViewWatcher` |
