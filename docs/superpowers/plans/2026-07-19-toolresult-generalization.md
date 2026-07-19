# A-Plane Generalization: Tool-Result Memoization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the SS-2 spine generalizes to a **second real A-class `state_kind`** — tool-result memoization — at the policy + identity layer: register `SK_TOOL_RESULT` with an idempotent-only `ValuePolicyToolResult` and a tool-result `StateIdentity` builder, coexisting with KV (and the B-stub) in one registry with **zero spine change** and all tests green.

**Architecture:** Additive, C++-core-only, mirrors the SS-2 spike. A new `StateIdentityForToolResult(tool_id, args, idempotent)` (content-hash = `Fnv1a64(tool_id||args)`, `state_kind = SK_TOOL_RESULT`, `SIF_IDEMPOTENT` when idempotent) + a new `ValuePolicyToolResult` whose `shouldStore` enforces LLD §2.1c (non-idempotent tool results are never materialized) then applies the same economic gate as KV. Registered as the 3rd policy in `HeadlessNode`'s ctor. A 3-way contrast test (KV / tool-result / B-stub) is the generalization proof.

**Tech Stack:** C++20, CMake+Ninja, GoogleTest. Reuses `src/core/common/{state_identity.h,value_policy.h,hashing.h}` and the `ValuePolicyRegistry` from the SS-2 spike.

**Design source:** LLD `KV_Cache_Design/KV_Cache_LLD_详细设计.md` §2.1b (state_kinds/flags), §2.1c (L1-AD-10: `SK_TOOL_RESULT` only materializable when `SIF_IDEMPOTENT`; non-idempotent rejected by `ValuePolicy_ToolResult.shouldStore`), §3.3.8 per-state_kind policy table (tool-result: "幂等 且 调用贵 才存 / 成本驱动 / 重调幂等工具"). Grounded against the shipped SS-2 code (`ValuePolicyKv`, `ValuePolicyRegistry`, `StateIdentityFromLocator`).

## Global Constraints

- **Additive, C++-core-only.** No change to the frozen 64B `kv_locator_t`, the C ABI, wire, or FFI. New code in `src/core/common/`; namespace `kvcache::common`. Include convention: `#include "state_identity.h"` / `"value_policy.h"` / `"hashing.h"` (NOT `"common/..."`) — `core/common` is on the test include path.
- **Zero spine change.** Adding tool-result must not touch `ValuePolicy`, `ValuePolicyRegistry`, `StateIdentity`, or the hot-path seams — only a new policy class, a new identity builder, and one `registerPolicy` line. Any edit to the interface or a hot-path `if state_kind` is a generalization failure.
- **All existing tests stay green** (full suite, currently 527/527). Registering a 3rd policy is behavior-neutral for KV (the hot path still calls `of(SK_KV)`); tool-result has no ingest path yet, so nothing on the KV hot path changes.
- **LLD §2.1c is law:** `ValuePolicyToolResult::shouldStore` returns `false` when `SIF_IDEMPOTENT` is not set — a non-idempotent tool result must never be materialized (avoids replaying a side-effecting call's stale result).
- No new third-party dependency. Reuse `Fnv1a64` from `hashing.h` for `content_hash` (BLAKE3 for non-KV state kinds remains deferred per the SS-2 spike).
- TDD: failing test → run-fail → minimal impl → run-pass → commit, per step.

## File Structure

- `src/core/common/state_identity.h` — add `StateIdentityForToolResult(...)` (header-only, next to `StateIdentityFromLocator`). Task 1.
- `src/core/common/value_policy_tool_result.h` / `.cpp` — `ValuePolicyToolResult`. Task 2.
- `src/core-abi/src/headless_node.h` — register `SK_TOOL_RESULT` in the ctor (one line). Task 3.
- Tests: `state_identity_toolresult_test.cpp`, `value_policy_tool_result_test.cpp`, `value_policy_three_way_contrast_test.cpp`. Tasks 1–3.
- `README.md` capability matrix + `docs/design/ss2-spike-q3-verdict.md` — honest note (policy+identity landed; ingest connector deferred). Task 4.

## Out of scope (deferred)

- The engine-facing **ingest/lookup ABI path** for tool results (how a runtime actually stores/retrieves a tool result) — this plan proves the policy+identity layer, not a wired connector.
- Routing non-idempotent tool calls into B2 durable-execution lineage (that is B2 proper).
- BLAKE3 `content_hash` (KV keeps xxhash3; non-KV uses `Fnv1a64` here — production hash upgrade is the same deferral as SS-2).

---

### Task 1: `StateIdentityForToolResult` builder

**Files:**
- Modify: `src/core/common/state_identity.h`
- Test: `src/tests/unit/common/state_identity_toolresult_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`

**Interfaces:**
- Consumes: `state_kind_e::SK_TOOL_RESULT`, `si_flags_e::SIF_IDEMPOTENT`, `StateIdentity` (existing); `Fnv1a64` from `hashing.h`.
- Produces:
  ```cpp
  namespace kvcache::common {
  // Build a StateIdentity for a tool-result memoization entry.
  // content_hash = Fnv1a64(tool_id) folded with Fnv1a64(args) (LLD §2.1b:
  // hash(tool_id, params)); state_kind = SK_TOOL_RESULT; SIF_IDEMPOTENT set
  // iff `idempotent`. tenant_id_lo carries the tenant.
  StateIdentity StateIdentityForToolResult(uint64_t tenant_id_lo,
                                           std::string_view tool_id,
                                           std::string_view args,
                                           bool idempotent);
  }
  ```

- [ ] **Step 1: Write the failing test**

```cpp
// src/tests/unit/common/state_identity_toolresult_test.cpp
#include "state_identity.h"
#include <gtest/gtest.h>
#include <cstring>
using namespace kvcache::common;

TEST(StateIdentityToolResult, KindFlagsAndTenant) {
    StateIdentity id = StateIdentityForToolResult(7, "search", "q=cats", /*idempotent=*/true);
    EXPECT_EQ(id.version, 2u);
    EXPECT_EQ(id.state_kind, static_cast<uint16_t>(SK_TOOL_RESULT));
    EXPECT_EQ(id.tenant_id_lo, 7u);
    EXPECT_TRUE(id.flags & SIF_IDEMPOTENT);
    EXPECT_EQ(id.recipe_ref, 0u);
}

TEST(StateIdentityToolResult, NonIdempotentClearsFlag) {
    StateIdentity id = StateIdentityForToolResult(1, "send_email", "to=x", /*idempotent=*/false);
    EXPECT_FALSE(id.flags & SIF_IDEMPOTENT);
}

TEST(StateIdentityToolResult, ContentHashDedupsSameCallDistinguishesArgs) {
    auto a = StateIdentityForToolResult(1, "search", "q=cats", true);
    auto b = StateIdentityForToolResult(1, "search", "q=cats", true);   // same call
    auto c = StateIdentityForToolResult(1, "search", "q=dogs", true);   // different args
    EXPECT_EQ(std::memcmp(a.content_hash, b.content_hash, 32), 0) << "same (tool,args) → same hash (dedup)";
    EXPECT_NE(std::memcmp(a.content_hash, c.content_hash, 32), 0) << "different args → different hash";
}
```

- [ ] **Step 2: Run to verify fail** — `cmake --build build --target test_state_identity_toolresult && ./build/tests/unit/test_state_identity_toolresult` — FAIL (function missing).

- [ ] **Step 3: Implement in `state_identity.h`** (add below `StateIdentityFromLocator`):

```cpp
#include "hashing.h"   // Fnv1a64  (add to the existing includes)
// ...
inline StateIdentity StateIdentityForToolResult(uint64_t tenant_id_lo,
                                                std::string_view tool_id,
                                                std::string_view args,
                                                bool idempotent) {
    StateIdentity id{};
    id.version      = 2;
    id.state_kind   = SK_TOOL_RESULT;
    id.flags        = idempotent ? SIF_IDEMPOTENT : 0;
    id.tenant_id_lo = tenant_id_lo;
    // content_hash = hash(tool_id, args). Fnv1a64 each, write both into the
    // 32-byte content_hash (BLAKE3 is the deferred production hash).
    uint64_t h_tool = Fnv1a64(tool_id);
    uint64_t h_args = Fnv1a64(args);
    std::memcpy(id.content_hash,      &h_tool, sizeof(h_tool));   // [0..8)
    std::memcpy(id.content_hash + 8,  &h_args, sizeof(h_args));   // [8..16)
    id.recipe_ref = 0;
    return id;
}
```
*(Verify the existing `state_identity.h` include block + that adding `#include "hashing.h"` doesn't create a cycle — `hashing.h` is a leaf header. Confirm `Fnv1a64(std::string_view)` exists in `hashing.h`.)*

- [ ] **Step 4: Wire target + run** — add `test_state_identity_toolresult` to `src/tests/unit/CMakeLists.txt` mirroring `test_state_identity`. Run — PASS (3 tests). Confirm `test_state_identity` still green.

- [ ] **Step 5: Commit**
```bash
git add src/core/common/state_identity.h src/tests/unit/common/state_identity_toolresult_test.cpp src/tests/unit/CMakeLists.txt
git commit -m "feat(A-gen): StateIdentityForToolResult — content_hash=hash(tool_id,args) + SIF_IDEMPOTENT"
```

---

### Task 2: `ValuePolicyToolResult` — idempotent-only store gate (LLD §2.1c)

**Files:**
- Create: `src/core/common/value_policy_tool_result.h`, `.cpp`
- Test: `src/tests/unit/policy/value_policy_tool_result_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`

**Interfaces:**
- Consumes: `ValuePolicy`, `CostModel`, `EvictDecision`, `OnMissAction` (existing); `StateIdentity`, `SIF_IDEMPOTENT`.
- Produces: `class ValuePolicyToolResult final : public ValuePolicy` in `kvcache::common`.

- [ ] **Step 1: Write the failing test**

```cpp
// src/tests/unit/policy/value_policy_tool_result_test.cpp
#include "value_policy_tool_result.h"
#include "state_identity.h"
#include <gtest/gtest.h>
using namespace kvcache::common;

TEST(ValuePolicyToolResult, NonIdempotentIsNeverStored) {
    ValuePolicyToolResult p;
    StateIdentity id = StateIdentityForToolResult(1, "send_email", "to=x", /*idempotent=*/false);
    // LLD §2.1c: a non-idempotent tool result must never be materialized,
    // regardless of economics.
    EXPECT_FALSE(p.shouldStore(id, CostModel{.fetch_cost_ms = 1, .recompute_cost_ms = 9999}));
}

TEST(ValuePolicyToolResult, IdempotentUsesEconomicGate) {
    ValuePolicyToolResult p;
    StateIdentity id = StateIdentityForToolResult(1, "search", "q=cats", /*idempotent=*/true);
    EXPECT_TRUE(p.shouldStore(id, CostModel{}));                                   // unknown cost → store
    EXPECT_TRUE(p.shouldStore(id, CostModel{.fetch_cost_ms = 1, .recompute_cost_ms = 20})); // cheap fetch → store
    EXPECT_FALSE(p.shouldStore(id, CostModel{.fetch_cost_ms = 20, .recompute_cost_ms = 1})); // recompute cheaper → decline
}

TEST(ValuePolicyToolResult, EvictableAndRecomputeOnMiss) {
    ValuePolicyToolResult p;
    StateIdentity id = StateIdentityForToolResult(1, "search", "q=cats", true);
    EXPECT_EQ(p.shouldEvict(id, /*tier*/2), EvictDecision::kEvictable);   // A-class
    EXPECT_EQ(p.onMiss(id), OnMissAction::kRecompute);                    // re-call the idempotent tool
}
```

- [ ] **Step 2: Run to verify fail** — `cmake --build build --target test_value_policy_tool_result && ./build/tests/unit/test_value_policy_tool_result` — FAIL.

- [ ] **Step 3: Implement**

```cpp
// src/core/common/value_policy_tool_result.h
#pragma once
#include "value_policy.h"
namespace kvcache::common {
class ValuePolicyToolResult final : public ValuePolicy {
 public:
  bool          shouldStore(const StateIdentity&, const CostModel&) override;
  EvictDecision shouldEvict(const StateIdentity&, int tier) override;
  OnMissAction  onMiss(const StateIdentity&) override;
};
}  // namespace kvcache::common
```
```cpp
// src/core/common/value_policy_tool_result.cpp
#include "value_policy_tool_result.h"
namespace kvcache::common {
bool ValuePolicyToolResult::shouldStore(const StateIdentity& id, const CostModel& c) {
    // LLD §2.1c: only idempotent tool results may be materialized. A
    // side-effecting call's result must never be cached and replayed.
    if (!(id.flags & SIF_IDEMPOTENT)) return false;
    // Idempotent → same economic gate as KV (behavior-preserving default:
    // unknown recompute cost stores; otherwise store only if fetch is cheaper).
    if (c.recompute_cost_ms <= 0.0) return true;
    return c.fetch_cost_ms < c.recompute_cost_ms;
}
EvictDecision ValuePolicyToolResult::shouldEvict(const StateIdentity&, int) {
    return EvictDecision::kEvictable;   // A-class: cost-driven eviction may proceed
}
OnMissAction ValuePolicyToolResult::onMiss(const StateIdentity&) {
    return OnMissAction::kRecompute;    // re-call the idempotent tool
}
}  // namespace kvcache::common
```

- [ ] **Step 4: Wire target + run** — add `test_value_policy_tool_result` (sources: `value_policy_tool_result.cpp` + the test) to CMake. Run — PASS (3 tests).

- [ ] **Step 5: Commit**
```bash
git add src/core/common/value_policy_tool_result.h src/core/common/value_policy_tool_result.cpp \
        src/tests/unit/policy/value_policy_tool_result_test.cpp src/tests/unit/CMakeLists.txt
git commit -m "feat(A-gen): ValuePolicyToolResult — idempotent-only store gate (LLD §2.1c) + economic + evict/miss"
```

---

### Task 3: Register as the 3rd policy + 3-way contrast (the generalization proof) + full regression

**Files:**
- Modify: `src/core-abi/src/headless_node.h` (register `SK_TOOL_RESULT` in ctor)
- Test: `src/tests/unit/policy/value_policy_three_way_contrast_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`; `src/core-abi`/`kvstore-node` CMake only if `value_policy_tool_result.cpp` must link into a lib the node uses (mirror how `value_policy_kv.cpp` was linked into `kvcache_common`).

**Interfaces:**
- Consumes: Task 2 `ValuePolicyToolResult`, Task 1 `StateIdentityForToolResult`, existing `ValuePolicyKv` / `ValuePolicyPersistentStub` / `ValuePolicyRegistry`.
- Produces: `HeadlessNode`'s ctor registers `SK_TOOL_RESULT → ValuePolicyToolResult` alongside the existing `SK_KV`. Zero other spine change.

- [ ] **Step 1: Write the failing 3-way contrast test (the proof)**

```cpp
// src/tests/unit/policy/value_policy_three_way_contrast_test.cpp
#include "value_policy.h"
#include "value_policy_kv.h"
#include "value_policy_tool_result.h"
#include "value_policy_persistent_stub.h"
#include "state_identity.h"
#include <gtest/gtest.h>
using namespace kvcache::common;

// Generalization proof: THREE state_kinds — KV (A), tool-result (A, idempotent-gated),
// memory (B) — coexist in ONE registry, one registerPolicy line each, ZERO spine change,
// each with its own semantics. Adding the 3rd kind touched no interface, no hot path.
TEST(ThreeWayContrast, OneRegistryThreeKindsDistinctSemantics) {
    ValuePolicyRegistry reg;
    reg.registerPolicy(SK_KV,          std::make_unique<ValuePolicyKv>());
    reg.registerPolicy(SK_TOOL_RESULT, std::make_unique<ValuePolicyToolResult>());
    reg.registerPolicy(SK_MEMORY,      std::make_unique<ValuePolicyPersistentStub>());

    StateIdentity kv{}; kv.state_kind = SK_KV;
    StateIdentity tool_ok  = StateIdentityForToolResult(1, "search", "q=cats", /*idempotent=*/true);
    StateIdentity tool_bad = StateIdentityForToolResult(1, "send_email", "to=x", /*idempotent=*/false);
    StateIdentity mem{}; mem.state_kind = SK_MEMORY; mem.flags = SIF_PERSISTENT_B;

    CostModel cheap_recompute{.fetch_cost_ms = 10, .recompute_cost_ms = 1};
    // The distinguishing behavior of the NEW kind: non-idempotent tool result is refused
    // where KV (kind-agnostic on idempotency) would apply pure economics, and B stores unconditionally.
    EXPECT_FALSE(reg.of(SK_TOOL_RESULT).shouldStore(tool_bad, cheap_recompute)) << "non-idempotent → never store";
    EXPECT_FALSE(reg.of(SK_TOOL_RESULT).shouldStore(tool_ok,  cheap_recompute)) << "idempotent but recompute cheaper → decline";
    EXPECT_TRUE (reg.of(SK_TOOL_RESULT).shouldStore(tool_ok,  CostModel{}))     << "idempotent, unknown cost → store";
    EXPECT_FALSE(reg.of(SK_KV).shouldStore(kv, cheap_recompute))               << "KV: pure economics";
    EXPECT_TRUE (reg.of(SK_MEMORY).shouldStore(mem, cheap_recompute))          << "B: unconditional";

    // Evict/miss: both A-kinds evictable+recompute; B not-evictable+replay.
    EXPECT_EQ(reg.of(SK_TOOL_RESULT).shouldEvict(tool_ok, 2), EvictDecision::kEvictable);
    EXPECT_EQ(reg.of(SK_TOOL_RESULT).onMiss(tool_ok),         OnMissAction::kRecompute);
    EXPECT_EQ(reg.of(SK_MEMORY).shouldEvict(mem, 2),          EvictDecision::kNotEvictable);
}
```

- [ ] **Step 2: Run to verify fail** — `cmake --build build --target test_value_policy_three_way_contrast && ./build/tests/unit/test_value_policy_three_way_contrast` — FAIL (until `value_policy_tool_result` is linked / test wired).

- [ ] **Step 3: Register the policy** — in `src/core-abi/src/headless_node.h` (ctor, where `SK_KV` is registered ~line 213), add:
```cpp
policy_reg_.registerPolicy(
    kvcache::common::SK_TOOL_RESULT,
    std::make_unique<kvcache::common::ValuePolicyToolResult>());
```
Add `#include "value_policy_tool_result.h"` to `headless_node.h` (or wherever `value_policy_kv.h` is included). This is the ONLY production-code change — no hot-path edit.

- [ ] **Step 4: Run to verify pass + full regression** — `./build/tests/unit/test_value_policy_three_way_contrast` — PASS. Then the FULL suite: `cd build && cmake --build . -j4 && ctest --output-on-failure` — expect the prior green count (527) + the new tests, 0 failures (registering a 3rd policy is behavior-neutral for the KV hot path). Capture the summary line.

- [ ] **Step 5: Commit**
```bash
git add src/core-abi/src/headless_node.h \
        src/tests/unit/policy/value_policy_three_way_contrast_test.cpp src/tests/unit/CMakeLists.txt
git commit -m "feat(A-gen): register SK_TOOL_RESULT policy + 3-way contrast — generalization proven, zero spine change, full suite green"
```

---

### Task 4: Honest capability-matrix + verdict note (SS-3)

**Files:**
- Modify: `README.md` (capability matrix tool-result row)
- Modify: `docs/design/ss2-spike-q3-verdict.md` (a short generalization note)

- [ ] **Step 1: Update the README capability matrix — honestly.** The tool-result row currently reads `| Tool-result memoization | A | — | P2 · idempotent-only |`. The **policy + identity layer** now exists, but there is **no ingest connector** (engines can't yet store/retrieve tool results through the ABI). Do NOT flip it to `✅`. Reword the roadmap cell to reflect the partial landing, e.g.: `P2 · idempotent-only · policy + identity landed (SS-2 spine); engine ingest connector pending`. Keep the Shipped column `—`.

- [ ] **Step 2: Add a generalization note to the verdict doc** — a short subsection under the SS-2 verdict recording that the spine was exercised with a second real A-class kind (tool-result), idempotent-gated per §2.1c, registered with zero spine change; and that the ingest connector is the deferred remainder. This keeps the "never mark in-built as built" record accurate.

- [ ] **Step 3: Commit**
```bash
git add README.md docs/design/ss2-spike-q3-verdict.md
git commit -m "docs(A-gen): capability matrix + verdict — tool-result policy/identity landed, ingest connector deferred"
```

---

## Self-Review

**Spec coverage (LLD §2.1b/§2.1c/§3.3.8 tool-result row):** `StateIdentityForToolResult` (content_hash = hash(tool_id,args), SIF_IDEMPOTENT) → Task 1. `ValuePolicyToolResult` idempotent-only shouldStore (§2.1c) + economic + evict/miss (§3.3.8 row) → Task 2. Registration + generalization proof (one spine, 3 kinds) → Task 3. Honest disclosure → Task 4. Deferrals (ingest connector, non-idempotent→B2, BLAKE3) match the agreed scope.

**Placeholder scan:** every code step has real code; the "verify hashing.h include / no cycle" and "mirror value_policy_kv.cpp linkage" notes are grounding instructions (the successful SS-2 pattern), not TBDs. No "implement later" in a deliverable.

**Type consistency:** `StateIdentityForToolResult(uint64_t, string_view, string_view, bool) -> StateIdentity` (Task 1) is called by Tasks 2's + 3's tests. `ValuePolicyToolResult` implements the existing `ValuePolicy` 3-method interface unchanged (Task 2), registered via the existing `ValuePolicyRegistry::registerPolicy` (Task 3). `CostModel`/`EvictDecision`/`OnMissAction`/`SIF_IDEMPOTENT`/`SK_TOOL_RESULT` all consumed as defined in the shipped SS-2 code — no new spine types.

**Zero-spine-change invariant (the thesis):** the only production edit is one `registerPolicy` line + one include in `headless_node.h` (Task 3 Step 3). `ValuePolicy`, `ValuePolicyRegistry`, `StateIdentity`, and the three hot-path seams are untouched — verified by the fact that no task modifies `value_policy.h` or the seam sites. The 527-green gate (Task 3 Step 4) enforces behavior-neutrality.
