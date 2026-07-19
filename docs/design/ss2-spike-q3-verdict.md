# SS-2 Spine Spike — Q3 Verdict

> LLD §10 Q3 / SS-2 (`state_identity_t` + `ValuePolicy` spine). This document
> is the closing deliverable of the SS-2 spike (Tasks 1–6, `main` since
> `c969d11`) — the pass/fail verdict, the evidence, and an honest accounting
> of what was and was not built.

## The Q3 question

> Can one `ValuePolicy` interface cleanly cover both **A-class** state
> (KVCache: store-vs-recompute economics, cost-evictable) and **B-class**
> state (unconditional store, `NOT_EVICTABLE`) with:
> 1. no scattered `if (state_kind == ...)` branching on the hot path, and
> 2. all tests green?

## Verdict: PASS

### Evidence 1 — A/B contrast test (one registry, two opposite-semantics policies)

`src/tests/unit/policy/value_policy_ab_contrast_test.cpp`,
`ValuePolicyABContrast.OneRegistryCoversAandBWithOppositeSemantics`:

- One `ValuePolicyRegistry`, two policies registered with **one line each**:
  `reg.registerPolicy(SK_KV, std::make_unique<ValuePolicyKv>())` (A) and
  `reg.registerPolicy(SK_MEMORY, std::make_unique<ValuePolicyPersistentStub>())` (B).
- Same `StateIdentity` shape, opposite `state_kind` → opposite decisions on
  all three policy methods, asserted in one test:

  | Method | A (`SK_KV`, `ValuePolicyKv`) | B (`SK_MEMORY`, `ValuePolicyPersistentStub`) |
  |---|---|---|
  | `shouldStore` | `false` when recompute is cheaper than fetch | `true` unconditionally — irreplaceable, ignores economics |
  | `shouldEvict` | `kEvictable` — cost-driven eviction may proceed | `kNotEvictable` — demote-only, never discard |
  | `onMiss` | `kRecompute` | `kReplayFromPersist` |

- **Zero spine change**: the registry, the `ValuePolicy` interface, and the
  three hot-path call sites (Evidence 2, below) are identical whether the
  registry holds one policy or ten. Adding B required a new policy class and
  one `registerPolicy` line — nothing in `value_policy.h` or the hot path
  changed.

### Evidence 2 — no `if (state_kind)` on the hot path

The three seams inserted in Task 5 are flat `registry.of(kind).<method>()`
calls, not kind-conditional branches:

| Seam | File : function | Call |
|---|---|---|
| Store | `src/core-abi/src/headless_node.cpp` : `HeadlessNode::SealCommit` (line 599) | `policy_reg_.of(SK_KV).shouldStore(sid, cost)` |
| Miss (lookup) | `src/core-abi/src/headless_node.cpp` : `HeadlessNode::Lookup` (line 333) | `policy_reg_.of(SK_KV).onMiss(miss_sid)` |
| Miss (fetch) | `src/core-abi/src/headless_node.cpp` : `HeadlessNode::FetchWithPriority` (line 501) | `policy_reg_.of(SK_KV).onMiss(sid)` |
| Evict | `src/kvstore-node/src/tier/dram_tier.cpp` : `DramTier::IsNotEvictable` (line 157; called from `EvictToFit`, line 183) | `evict_policy_->shouldEvict(sid, kDramTierId)` |

Each site resolves the policy by `state_kind` through the registry (or a
policy pointer injected the same way) and calls one of the three interface
methods — there is no per-kind conditional logic inlined at the call site.
The kind-specific behavior lives entirely inside the policy implementation
(`ValuePolicyKv`, `ValuePolicyPersistentStub`), which is exactly the
separation Q3 asked for.

### Evidence 3 — full suite green

```
$ cd build && cmake --build . -j4 && ctest --output-on-failure
...
100% tests passed, 0 tests failed out of 527
```

527/527 — the full suite, including the four SS-2 policy tests
(`value_policy_registry_test.cpp`, `value_policy_kv_test.cpp`,
`value_policy_ab_contrast_test.cpp`) and every pre-existing test in the repo
(`headless_node`, `dram_tier`, tier manager, routing, etcd, TRT-LLM backend,
etc.). 15 additional tests are marked `Skipped` (external-endpoint /
hardware-gated integration tests: NVMe io_uring, real REST cold-tier, real
etcd HTTP/gRPC endpoints, OTLP HTTP) — unrelated to this spike and skipped
under the same conditions as before it.

**Q3 verdict: PASS.** One `ValuePolicy` interface + one
`ValuePolicyRegistry` admits both A-class and B-class economics, wired into
the real hot path with no scattered kind-checks, at 527/527 green.

---

## The honest finding (SS-3)

The store-vs-recompute economic decision — **D-PERF-1**, surfaced as
`KV_E_SAFETY_NET` — was **defined but never produced** in the codebase
before this spike:

- `KV_E_SAFETY_NET` was declared in `src/include/kvcache/kv_errors.h`
  (`= -9, /* D-PERF-1 trip: fetch slower than recompute */`) and stringified
  in `src/core-abi/src/kv_status.cpp` (`"safety-net trip (recompute
  faster)"`).
- No code path returned it. `HeadlessNode::SealCommit` stored the ingested
  slot **unconditionally** — there was no economic gate anywhere in the
  store path.

This spike is the **first implementation** of that gate:
`ValuePolicyKv::shouldStore` (`src/core/common/value_policy_kv.cpp`)
declines to store when `recompute_cost_ms` is known and cheaper than
`fetch_cost_ms`, and `SealCommit` now consults it (Evidence 2, Store row)
and returns `KV_E_SAFETY_NET` on decline.

**It is behavior-preserving today, not active.** The hot path currently
supplies a zeroed `CostModel{}` (no telemetry feeds it yet).
`ValuePolicyKv::shouldStore` treats an unknown/zero `recompute_cost_ms` as
"store" (the safe default), so the decline branch in `SealCommit` is
unreachable under today's inputs — the unconditional-store behavior from
before this spike is preserved exactly, which is what keeps the suite at
527/527 rather than requiring test rewrites.

**Production activation is gated on a follow-on**: real fetch/recompute
cost telemetry must feed the `CostModel` before `shouldStore` can actually
decline a store in production. Until that telemetry exists, do not describe
`KV_E_SAFETY_NET` / D-PERF-1 as an active, shipped runtime behavior — it is
implemented-but-dormant, wired for activation.

---

## Explicit deferrals

Not in scope for this spike; tracked as follow-on work:

- **§3.3.8.1 A→B promotion channel** — controlled promotion of a
  high-reuse A-class block to B-class persistence (`SIF_PERSISTENT`) is not
  implemented; the θ threshold for triggering promotion is unresolved
  (open item #31).
- **Other A-class `state_kind`s** — sandbox, embedding, and tool-result
  connectors are not registered; only `SK_KV` (A) and `SK_MEMORY` (B, stub)
  exist. This is the A-plane generalization proper (Phase 2).
- **The real ⑬ persistence engine** — `ValuePolicyPersistentStub::onMiss`
  returns `kReplayFromPersist` but there is no backing persistence/lineage
  engine (⑬/⑭) to actually replay from. It is a placeholder that proves the
  interface shape, not a working B-class fetch path.
- **Non-KV BLAKE3 `content_hash`** — KV keeps its existing 64B xxhash3 fast
  path unchanged; a BLAKE3 content hash for other state kinds is not wired.
- **`StateIdentity` across the FFI/proto/wire boundary** — this spike keeps
  `StateIdentity` C++-core-only; threading it across the C ABI / gRPC wire
  format is out of scope.

---

## Summary

| Question | Answer |
|---|---|
| Does one `ValuePolicy` interface cover A and B? | **Yes** — A/B contrast test, opposite semantics, one registry |
| Is the hot path free of `if (state_kind)`? | **Yes** — 4 call sites, all flat `registry.of(kind).<method>()` |
| Full suite green? | **Yes** — 527/527 (0 failed, 15 pre-existing skips) |
| Is D-PERF-1 / `KV_E_SAFETY_NET` a shipped, active runtime feature? | **No** — first-implemented here, behavior-preserving, dormant until cost telemetry lands (follow-on) |
