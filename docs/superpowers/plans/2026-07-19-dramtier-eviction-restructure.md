# DramTier Per-Kind Eviction Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the SS-2 verdict's #1 Phase-2 trap so a B-class `NOT_EVICTABLE` entry can safely live in DRAM: make `DramTier` carry each entry's real `state_kind`, dispatch the eviction policy **by that kind** (not a synthetic `SK_KV`), and restructure `EvictToFit` to **skip** non-evictable victims (walk toward the front) instead of `break` — all behavior-identical for the KV path (534 tests green).

**Architecture:** Additive + surgical. `DramTier::Entry` gains a `uint16_t state_kind` (default `SK_KV`); `Insert` gains a defaulted `state_kind` param (existing KV callers unchanged). `DramTier::Options` swaps its single `evict_policy` (`ValuePolicy*`) for a `policy_registry` (`ValuePolicyRegistry*`); the evict seam resolves `registry->of(entry.state_kind).shouldEvict(...)`. `EvictToFit`'s two eviction loops walk from the tail toward the front, skipping `NOT_EVICTABLE` entries and evicting the first evictable one (stop cleanly if a full pass finds none). For the KV path every entry is `SK_KV → kEvictable`, so the first evictable-from-tail is the tail — the exact victim/order as today.

**Tech Stack:** C++20, CMake+Ninja, GoogleTest. Modifies `src/kvstore-node/src/tier/dram_tier.{h,cpp}` + `src/core-abi/src/headless_node.cpp` (the `evict_policy` injection). Consumes the shipped SS-2 `ValuePolicy`/`ValuePolicyRegistry`.

**Design source:** the SS-2 verdict deferral `docs/design/ss2-spike-q3-verdict.md` ("DramTier eviction restructure (B-plane prerequisite)") — the two coupled gaps this plan closes. Confirmed 4 design decisions (Entry carries `state_kind` only, not full 128B StateIdentity; DramTier holds the registry; `Insert` gains a defaulted kind; `EvictToFit` walk-and-skip).

## Global Constraints

- **Behavior-identical for the KV path.** The full suite (currently 534/534) must stay green with the SAME victim selection and eviction order. KV entries default to `SK_KV`; `ValuePolicyKv::shouldEvict` returns `kEvictable`; the walk-and-skip evicts the tail (identical to today's `pop_back()`). The 534-green gate enforces this.
- **Additive-compatible signatures.** `Insert(key, data, n)` gains a trailing `uint16_t state_kind = SK_KV` — every existing call site compiles + behaves unchanged. `Entry::state_kind` defaults to `SK_KV`.
- **No infinite loop, ever.** The whole point: the walk must ADVANCE past a `NOT_EVICTABLE` entry (never re-inspect the same unadvanced tail). If a full pass finds nothing evictable, `EvictToFit` returns (capacity unsatisfied — the honest outcome), never spins.
- **The `state_kind`, not a full identity, is what the entry carries** (all policies ignore identity in `shouldEvict`). The seam builds a minimal `StateIdentity{state_kind = entry.state_kind}` for the `shouldEvict(StateIdentity, tier)` call.
- **Registry lifetime.** `DramTier` holds a non-owning `ValuePolicyRegistry*`; it must outlive the tier. `HeadlessNode` already declares `policy_reg_` before `tm_` (SS-2 Task-5 fix) so the registry outlives `DramTier` — preserve that ordering.
- No new third-party dependency. TDD: failing test → run-fail → minimal impl → run-pass → commit, per step.

## Out of scope (still deferred)

- Wiring a B-class entry into the DRAM **store** path (nothing inserts a `NOT_EVICTABLE` entry in production yet — this plan makes eviction *safe* for when that lands). Tests exercise it with a synthetic not-evictable policy.
- Passing the real kind from the tool-result / other store paths (needs those ingest paths; KV store path passes `SK_KV`).
- A demotion hand-off for `NOT_EVICTABLE` entries in DRAM (they simply aren't evicted; cross-tier demotion remains `TierManager`'s concern).

## File Structure

- `src/kvstore-node/src/tier/dram_tier.h` — `Entry::state_kind`; `Insert(...)` signature; `Options::policy_registry`; `IsNotEvictable(const Entry&)`. Tasks 1–3.
- `src/kvstore-node/src/tier/dram_tier.cpp` — thread kind through `Insert`; dispatch via registry; `EvictToFit` walk-and-skip. Tasks 1–3.
- `src/core-abi/src/headless_node.cpp` — pass `&policy_reg_` (registry) into `DramTier::Options` instead of `&policy_reg_.of(SK_KV)`. Task 2.
- Tests: `src/tests/unit/tier/dram_tier_evict_kind_test.cpp` (per-kind dispatch + walk-and-skip). Tasks 1–3.
- `docs/design/ss2-spike-q3-verdict.md` — flip the eviction-restructure deferral to landed. Task 4.

---

### Task 1: `Entry` carries `state_kind`; `Insert` threads it (additive, behavior-neutral)

**Files:**
- Modify: `src/kvstore-node/src/tier/dram_tier.h` (Entry struct + Insert decl + a test accessor)
- Modify: `src/kvstore-node/src/tier/dram_tier.cpp` (Insert impl)
- Test: `src/tests/unit/tier/dram_tier_evict_kind_test.cpp` (start it here)
- Modify: `src/tests/unit/CMakeLists.txt`

**Interfaces:**
- Consumes: `kvcache::common::SK_KV` (already included via `value_policy.h` in `dram_tier.h`).
- Produces: `Entry` gains `uint16_t state_kind = kvcache::common::SK_KV;`. `void Insert(const DramKey& key, const uint8_t* data, std::size_t n, uint16_t state_kind = kvcache::common::SK_KV);`. A test accessor `std::optional<uint16_t> KindOf(const DramKey&) const;`.

- [ ] **Step 1: Write the failing test**

```cpp
// src/tests/unit/tier/dram_tier_evict_kind_test.cpp
#include "tier/dram_tier.h"
#include <gtest/gtest.h>
using kvcache::node::tier::DramTier;
using kvcache::common::SK_KV;

static DramTier::Options SmallOpts() {   // adapt to the real Options fields you read
    DramTier::Options o;
    o.a1in_capacity  = 100;
    o.capacity_bytes = 200;
    return o;
}
TEST(DramTierKind, InsertRecordsKindDefaultKv) {
    DramTier t(SmallOpts());
    DramKey k{}; k.bytes[0] = 1;
    std::vector<uint8_t> v(10, 0xAA);
    t.Insert(k, v.data(), v.size());                      // default kind
    ASSERT_TRUE(t.KindOf(k).has_value());
    EXPECT_EQ(*t.KindOf(k), static_cast<uint16_t>(SK_KV));

    DramKey k2{}; k2.bytes[0] = 2;
    t.Insert(k2, v.data(), v.size(), /*state_kind=*/16);  // e.g. SK_MEMORY
    EXPECT_EQ(*t.KindOf(k2), 16u);
}
```
*(Read `dram_tier.h` for the real `Options` field names + `DramKey` shape before writing; adapt `SmallOpts`/`DramKey` construction to match. If a `KindOf` accessor feels too test-only, instead expose it under a clearly-commented test-support section — the eviction tests in Task 3 also rely on kind being observable.)*

- [ ] **Step 2: Run to verify fail** — `cmake --build build --target test_dram_tier_evict_kind && ./build/tests/unit/test_dram_tier_evict_kind` — FAIL (Insert has no kind param / no KindOf).

- [ ] **Step 3: Implement** — in `dram_tier.h`: add `uint16_t state_kind = kvcache::common::SK_KV;` to `struct Entry`; change the `Insert` decl to add `uint16_t state_kind = kvcache::common::SK_KV`; add `std::optional<uint16_t> KindOf(const DramKey&) const;` (+ `#include <optional>`). In `dram_tier.cpp` `Insert`: add `uint16_t state_kind` param; on the in-place replace path (`if (cur != index_.end())`) set `cur->second->state_kind = state_kind;`; on the two `push_front(Entry{...})` sites, add `state_kind` to the aggregate: `Entry{key, Queue::Am, std::vector<uint8_t>(data, data+n), state_kind}` (verify field order matches the struct). Implement `KindOf`:
```cpp
std::optional<uint16_t> DramTier::KindOf(const DramKey& key) const {
    std::lock_guard lk(mu_);
    auto it = index_.find(key);
    if (it == index_.end()) return std::nullopt;
    return it->second->state_kind;
}
```

- [ ] **Step 4: Run + regression** — `./build/tests/unit/test_dram_tier_evict_kind` PASS; then `cd build && ctest -R "dram|tier|node_data" --output-on-failure` — existing DRAM/tier tests green (Insert's new default param is behavior-neutral).

- [ ] **Step 5: Commit**
```bash
git add src/kvstore-node/src/tier/dram_tier.h src/kvstore-node/src/tier/dram_tier.cpp \
        src/tests/unit/tier/dram_tier_evict_kind_test.cpp src/tests/unit/CMakeLists.txt
git commit -m "feat(dram-evict): Entry carries state_kind; Insert threads it (default SK_KV, behavior-neutral)"
```

---

### Task 2: Dispatch eviction policy by entry kind via the registry

**Files:**
- Modify: `src/kvstore-node/src/tier/dram_tier.h` (`Options::policy_registry`; `IsNotEvictable(const Entry&)`)
- Modify: `src/kvstore-node/src/tier/dram_tier.cpp` (`IsNotEvictable`)
- Modify: `src/core-abi/src/headless_node.cpp` (pass the registry)
- Test: extend `dram_tier_evict_kind_test.cpp`

**Interfaces:**
- Consumes: Task 1 `Entry::state_kind`; `kvcache::common::ValuePolicyRegistry`, `StateIdentity`, `EvictDecision`.
- Produces: `Options::policy_registry` (`kvcache::common::ValuePolicyRegistry* = nullptr`) replaces the single `evict_policy`. `IsNotEvictable(const Entry&)` resolves the per-kind policy.

- [ ] **Step 1: Write the failing test** — register a 2-policy registry (SK_KV → an evictable policy; a test kind → a not-evictable policy), construct a `DramTier` with `opts.policy_registry = &reg`, and assert (via a small seam or the Task-3 eviction behavior) that an entry of the not-evictable kind reports not-evictable while an `SK_KV` entry does not. (If `IsNotEvictable` is private, assert through eviction behavior in Task 3; for Task 2 test the wiring compiles + a KV-only tier still evicts as before.) Use the real `ValuePolicyKv` + `ValuePolicyPersistentStub` (register stub under `SK_MEMORY`).

- [ ] **Step 2: Run to verify fail** — build — FAIL (`Options::evict_policy` renamed / `IsNotEvictable` signature changed).

- [ ] **Step 3: Implement**
  - `dram_tier.h`: replace `kvcache::common::ValuePolicy* evict_policy = nullptr;` in `Options` with `kvcache::common::ValuePolicyRegistry* policy_registry = nullptr;`. Store it as a member `registry_`. Change `bool IsNotEvictable() const;` → `bool IsNotEvictable(const Entry& e) const;`.
  - `dram_tier.cpp`:
```cpp
bool DramTier::IsNotEvictable(const Entry& e) const {
    if (!registry_ || !registry_->has(e.state_kind)) return false;   // no policy → evictable (as today)
    kvcache::common::StateIdentity sid{};
    sid.state_kind = e.state_kind;
    return registry_->of(e.state_kind).shouldEvict(sid, kDramTierId) ==
           kvcache::common::EvictDecision::kNotEvictable;
}
```
  - Update the two `EvictToFit` call sites from `IsNotEvictable()` to `IsNotEvictable(<the victim Entry under inspection>)` — **for this task keep the existing `break` semantics** (Task 3 does the walk-and-skip); pass the current tail entry (`a1in_.back()` / `am_.back()`) to `IsNotEvictable`.
  - `headless_node.cpp` `Init`: where it currently sets `tier_opts...evict_policy = &policy_reg_.of(SK_KV)`, change to `...policy_registry = &policy_reg_;` (pass the whole registry). Read the real injection site (SS-2 Task 5 added it) and update it.

- [ ] **Step 4: Run + regression** — the new test passes; `cd build && ctest -R "dram|tier|node_data|headless|seal|replica" --output-on-failure` — green (KV entries → `of(SK_KV)` → `kEvictable`; same victims; `break` still there but only trips for a genuinely not-evictable kind, which the KV path never produces).

- [ ] **Step 5: Commit**
```bash
git add src/kvstore-node/src/tier/dram_tier.h src/kvstore-node/src/tier/dram_tier.cpp \
        src/core-abi/src/headless_node.cpp src/tests/unit/tier/dram_tier_evict_kind_test.cpp
git commit -m "feat(dram-evict): dispatch shouldEvict by entry.state_kind via the registry (no more synthetic SK_KV)"
```

---

### Task 3: `EvictToFit` walk-and-skip restructure (the trap-closer)

**Files:**
- Modify: `src/kvstore-node/src/tier/dram_tier.cpp` (both `EvictToFit` loops)
- Test: extend `dram_tier_evict_kind_test.cpp` with the skip + no-hang cases

**Interfaces:**
- Consumes: Task 1/2 (`Entry::state_kind`, `IsNotEvictable(const Entry&)`, the registry).
- Produces: `EvictToFit` no longer `break`s on a not-evictable tail — it walks toward the front, skips not-evictable entries, evicts the first evictable one; a full pass with nothing evictable stops cleanly.

- [ ] **Step 1: Write the failing tests**

```cpp
// add to dram_tier_evict_kind_test.cpp
// Register SK_KV (evictable) + SK_MEMORY (ValuePolicyPersistentStub → NOT_EVICTABLE).
// Insert, tail→front: [evictable_a (tail), not_evictable (mid), evictable_b (front)].
// Force eviction (insert enough to exceed capacity). Assert: not_evictable SURVIVES,
// evictable_a (the tail) is dropped first — the walk skipped the non-evictable entry
// only if it sat at the tail; here evictable_a is the tail so it goes first, and if we
// then need more space the walk skips not_evictable and takes evictable_b.
TEST(DramTierKind, EvictionSkipsNotEvictableAndDropsEvictable) {
    /* build registry {SK_KV: ValuePolicyKv, SK_MEMORY: ValuePolicyPersistentStub};
       DramTier with policy_registry=&reg + small capacity; Insert entries with kinds;
       trigger eviction; EXPECT the SK_MEMORY entry still resident (KindOf/Get returns it)
       and an SK_KV entry evicted. */
}
TEST(DramTierKind, AllNotEvictableStopsCleanlyNoHang) {
    /* registry {SK_MEMORY: stub}; fill DRAM with only SK_MEMORY entries; insert one more
       to force eviction; EvictToFit must RETURN (no infinite loop) leaving capacity
       unsatisfied — assert the call completes and all not-evictable entries remain. */
}
```
*(The no-hang test is the critical one — it would spin forever under a naive `continue`. Keep it deterministic: a fixed small number of entries, a single Insert that would need eviction; assert it returns and the entries are all still present.)*

- [ ] **Step 2: Run to verify fail** — the skip test fails (current `break` stops the whole pass, so evictable_b past a not-evictable entry is never freed; and/or the not-evictable entry is at the tail and blocks everything).

- [ ] **Step 3: Restructure both loops** — replace each `while (over budget) { if (IsNotEvictable()) break; evict back(); }` with a walk that advances past non-evictable entries. For a `std::list`, iterate from the back toward the front to find the first evictable victim:
```cpp
// A1in loop (mirror for the Am/total loop):
while (!a1in_.empty() && a1in_bytes_used_ + incoming_bytes > a1in_capacity_) {
    // Walk from the tail toward the front for the first EVICTABLE entry.
    auto it = a1in_.end();          // reverse walk via --it
    bool found = false;
    while (it != a1in_.begin()) {
        --it;
        if (!IsNotEvictable(*it)) { found = true; break; }
    }
    if (!found) break;              // nothing evictable this pass → stop (capacity unsatisfied)
    const DramKey evicted_key = it->key;
    a1in_bytes_used_ -= it->data.size();
    GhostInsert(it->key);
    index_.erase(it->key);
    a1in_.erase(it);
    if (on_evict_) on_evict_(evicted_key);
}
```
Do the equivalent for the second loop (Am tail, falling back to A1in once Am is empty — preserve the existing "prefer Am tail" order, but walk-and-skip within it). **Crucial behavior-preservation:** when every entry is evictable (KV path), the reverse walk finds the tail immediately (`--it` from `end()` = last element), so the victim + order are IDENTICAL to today's `pop_back()`. Verify the erase-by-iterator + `index_`/byte-accounting exactly mirror the old `pop_back()` bookkeeping (GhostInsert, index_.erase, byte subtract, on_evict_). Remove the now-stale `break`-danger comments (the restructure they warned about is now done) and replace with a short note that the walk skips non-evictable entries and terminates when none are evictable.

- [ ] **Step 4: Run + FULL regression** — the two new tests pass (skip works; no-hang returns). Then the FULL suite: `cd build && cmake --build . -j4 && ctest --output-on-failure` — **534 + the new cases, 0 failures.** KV path victim/order unchanged (the 534-green is the proof). Capture the summary. **If any pre-existing DRAM/eviction test flips, the walk changed KV victim selection — fix until identical.**

- [ ] **Step 5: Commit**
```bash
git add src/kvstore-node/src/tier/dram_tier.cpp src/tests/unit/tier/dram_tier_evict_kind_test.cpp
git commit -m "feat(dram-evict): EvictToFit walk-and-skip — NOT_EVICTABLE entries survive, no infinite loop, KV victims unchanged"
```

---

### Task 4: Flip the SS-2 deferral to landed (SS-3 honesty) + final regression

**Files:**
- Modify: `docs/design/ss2-spike-q3-verdict.md` (the eviction-restructure deferral + evict-seam caveat)

- [ ] **Step 1: Update the verdict doc** — the "DramTier eviction restructure (B-plane prerequisite)" deferral and the "Evict-seam caveat" now describe **landed** work. Reword: the evict seam now dispatches by the entry's real `state_kind` (no synthetic `SK_KV`), and `EvictToFit` skips `NOT_EVICTABLE` victims (walk toward the front, terminates cleanly) — so a B-class entry can safely reside in DRAM eviction-wise. Keep honest about what remains: **nothing inserts a B-class entry into the DRAM store path yet** (that is the real B-plane ingest, still deferred); this closed the *eviction* half of the trap.

- [ ] **Step 2: Final full regression** — `cd build && cmake --build . -j4 && ctest --output-on-failure`; record the summary line in the doc.

- [ ] **Step 3: Commit**
```bash
git add docs/design/ss2-spike-q3-verdict.md
git commit -m "docs(dram-evict): SS-2 eviction-restructure deferral → landed; B ingest into DRAM store path still deferred"
```

---

## Self-Review

**Spec coverage (SS-2 verdict deferral (a)+(b)):** (a) per-victim real-kind dispatch → Task 1 (`Entry::state_kind` + `Insert` threading) + Task 2 (registry dispatch, drop synthetic `SK_KV`). (b) skip `NOT_EVICTABLE` instead of `break` → Task 3 (walk-and-skip + no-hang). Honest disclosure → Task 4. Deferrals (B ingest into DRAM store path, real per-kind from tool-result store, demotion hand-off) match the agreed scope.

**Placeholder scan:** the test bodies for Task 2/3 use prose sketches inside `/* ... */` for the registry/DramTier setup — the implementer fills them against the real `DramTier::Options`/`DramKey`/`Get` API (a concrete pointer, the SS-2 pattern), not a TBD. Every impl step shows real code or a precise edit. The "read the real EvictToFit bookkeeping and mirror it exactly" notes are grounding, not placeholders.

**Type consistency:** `Entry::state_kind` (uint16_t, Task 1) is read by `IsNotEvictable(const Entry&)` (Task 2) and the walk (Task 3). `Options::policy_registry` (`ValuePolicyRegistry*`, Task 2) is stored as `registry_` and used in `IsNotEvictable`. `Insert(key,data,n,state_kind=SK_KV)` (Task 1) — existing callers unchanged; `headless_node.cpp` store path continues to call the 3-arg form (default SK_KV). `shouldEvict(StateIdentity, int)` consumed as defined in SS-2.

**Behavior-preservation invariant (the gate):** every task keeps the KV path byte-identical — default `SK_KV`, `ValuePolicyKv::shouldEvict → kEvictable`, and the reverse-walk finding the tail first (= old `pop_back()`). Enforced by the 534-green full-suite gate in Task 3 Step 4 and Task 4 Step 2. The genuinely new behavior (skip + no-hang) is exercised only by the synthetic not-evictable-policy tests, which cannot affect the KV path.
