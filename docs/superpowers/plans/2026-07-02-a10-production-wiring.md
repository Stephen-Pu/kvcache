# A10 Regulated Mode — Production Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the already-built, already-reviewed A10 BoundaryGuard core into the two production call sites the whole-branch review flagged as unconnected — the cold-tier egress transport and node startup — so Regulated Mode actually enforces the perimeter at runtime instead of only in unit tests.

**Architecture:** Three integration seams, all additive and off-by-default. (1) The cold-tier factory (`CreateColdTier`) threads an optional `BoundaryGuard` + deny observer through `ColdTierOptions`; when present, the `native-rest` backend's HTTP transport is wrapped in the existing `GuardedHttpTransport` (via `RestColdTier::CreateWithTransport`) so every object-store request is boundary-checked before dialing. (2) `NodeRuntime`'s constructor runs `ValidateRegulatedMode` before binding its listeners and refuses to come up (`ok_=false`, error names the offending sink) when enabled and any configured sink is out of boundary. (3) `main.cpp` parses regulated-mode flags, builds one shared guard + a metric/audit deny observer, and passes them into both seams. When Regulated Mode is disabled, all three paths are byte-for-byte unchanged.

**Tech Stack:** C++20, CMake+Ninja, GoogleTest (FetchContent). Reuses the A10 core (`security/boundary_guard.h`, `boundary_policy_builder.h`, `regulated_mode.h`, `boundary_deny_observer.h`) and the cold-tier transport seam (`tier/rest_cold_tier.h`, `tier/guarded_transport.h`).

## Global Constraints

- C++20; CMake ≥ 3.28 in practice.
- No new third-party dependency; reuse the in-tree A10 core + cold-tier modules.
- **Off by default (spec R4):** when Regulated Mode is disabled (no guard threaded / `enabled=false`), the cold-tier factory and NodeRuntime behave exactly as before — no wrapping, no gate. Every existing test must still pass unchanged.
- **Fail-closed (spec R3/§8):** the startup gate refuses to boot on any out-of-boundary sink; a guarded transport denies (never dials) on any out-of-boundary or unparseable URL. No path fails open.
- Additive only: new fields on existing option structs default to "absent/disabled"; no ABI-version bump; no change to existing public signatures (only additions).
- Follow existing patterns: transport wrapping mirrors how `sigv4_transport` decorates in `RestColdTier::CreateWithTransport`; option plumbing mirrors existing `ColdTierOptions` / `NodeRuntime::Options` fields.
- Tests are gtest under `src/tests/unit/{tier,runtime}/`, wired in `src/tests/unit/CMakeLists.txt`.
- TDD: failing test → run-fail → minimal impl → run-pass → commit, per step.

## File Structure

- `src/kvstore-node/src/tier/cold_tier.h` / `.cpp` — add optional `guard` + `deny_observer` to `ColdTierOptions`; wrap the `native-rest` transport in `GuardedHttpTransport` when `guard` is set. Task 1.
- `src/kvstore-node/src/runtime/node_runtime.h` / `.cpp` — add `RegulatedModeConfig` to `NodeRuntime::Options`; run the startup gate in the constructor before binding. Task 2.
- `src/kvstore-node/src/main.cpp` — parse regulated-mode flags, build the shared guard + deny observer, pass into both seams; extract a testable `BuildRegulatedModeConfigFromFlags` helper. Task 3.
- `src/tests/unit/tier/cold_tier_guard_test.cpp`, `src/tests/unit/runtime/node_runtime_regulated_test.cpp`, `src/tests/unit/runtime/regulated_flags_test.cpp` — tests.
- `src/tests/unit/CMakeLists.txt`, `README.md` — wiring + phase log.

---

### Task 1: Thread the guard into the cold-tier factory; wrap the native-rest egress transport

**Files:**
- Modify: `src/kvstore-node/src/tier/cold_tier.h` (add two optional fields to `ColdTierOptions`)
- Modify: `src/kvstore-node/src/tier/cold_tier.cpp` (`CreateBaseColdTier` native-rest branch)
- Test: `src/tests/unit/tier/cold_tier_guard_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`

**Interfaces:**
- Consumes: `security::BoundaryGuard` (`security/boundary_guard.h`); `tier::GuardedHttpTransport` + `GuardedHttpTransport::DenyObserver` + `MakeCurlHttpTransport()` (`tier/guarded_transport.h`, `tier/rest_cold_tier.h`); `RestColdTier::CreateWithTransport(const Options&, std::shared_ptr<IHttpTransport>, std::string*)`.
- Produces: two new fields on `ColdTierOptions`:
  ```cpp
  // Optional Regulated-Mode egress guard. When set, the native-rest backend's
  // HTTP transport is wrapped so every object-store request is boundary-checked
  // before dialing. Unset (nullptr) => unchanged behavior (no wrapping).
  std::shared_ptr<const security::BoundaryGuard> guard;          // default nullptr
  tier::GuardedHttpTransport::DenyObserver       deny_observer;  // default empty
  ```

- [ ] **Step 1: Write the failing test**

```cpp
// src/tests/unit/tier/cold_tier_guard_test.cpp
#include "tier/cold_tier.h"
#include "security/boundary_guard.h"
#include <gtest/gtest.h>
#include <memory>
using namespace kvcache::node::tier;
using namespace kvcache::node::security;

// With a deny-all guard, a native-rest cold tier must refuse egress WITHOUT
// dialing — Get returns not-found/error and no network occurs (the guard
// short-circuits before libcurl). This proves the factory wrapped the transport.
TEST(ColdTierGuard, NativeRestWithDenyAllGuardBlocksEgress) {
    ColdTierOptions opts;
    opts.type = "native-rest";
    opts.rest.base_url = "https://out-of-boundary.example.com/bucket";
    // Empty allowlist + default_deny = air-gap: denies everything.
    opts.guard = std::make_shared<BoundaryGuard>(BoundaryPolicy{{}, /*default_deny=*/true});
    bool denied = false;
    opts.deny_observer = [&](const Endpoint& ep, std::string_view) {
        denied = true; EXPECT_EQ(ep.purpose, Purpose::kColdTier);
    };

    std::string err;
    auto tier = CreateColdTier(opts, &err);
    ASSERT_NE(tier, nullptr) << err;   // factory still constructs the tier

    // A read must be denied by the guard (no network). RestColdTier maps a
    // transport error to a miss/error; the key assertion is the observer fired
    // and the call returned quickly without a real DNS/connect.
    std::vector<uint8_t> out;
    DramKey k{};                       // any key; content irrelevant
    (void)tier->Get(k, &out, &err);    // adapt to the real IColdTier::Get signature
    EXPECT_TRUE(denied) << "guard must have denied the out-of-boundary egress";
}
```
*(Adapt `IColdTier::Get`'s real signature + `DramKey` construction by reading `tier/cold_tier.h` — mirror how `test_rest_cold_tier` calls Get. The invariant asserted is: the deny observer fired, proving the transport was guard-wrapped.)*

- [ ] **Step 2: Run to verify fail**

Run: `cmake --build build --target test_cold_tier_guard && ./build/tests/unit/test_cold_tier_guard`
Expected: FAIL to compile — `ColdTierOptions` has no `guard` / `deny_observer` field.

- [ ] **Step 3: Add the fields to `ColdTierOptions`**

In `cold_tier.h`, add the two members shown in **Interfaces → Produces** above. Add the needed includes (`<memory>`, `"security/boundary_guard.h"`, `"tier/guarded_transport.h"`). Keep them last in the struct so existing aggregate initialization is unaffected.

- [ ] **Step 4: Wrap the transport in the native-rest branch**

In `cold_tier.cpp` `CreateBaseColdTier`, replace the `native-rest` branch's `return RestColdTier::Create(ro, err);` with an explicit transport build that wraps when a guard is present:
```cpp
    if (opts.type == "native-rest") {
        RestColdTier::Options ro;
        ro.base_url             = opts.rest.base_url;
        ro.key_prefix           = opts.rest.key_prefix;
        ro.bearer_token         = opts.rest.bearer_token;
        ro.ca_pem_path          = opts.rest.ca_pem_path;
        ro.client_cert_pem_path = opts.rest.client_cert_pem_path;
        ro.client_key_pem_path  = opts.rest.client_key_pem_path;
        ro.timeout_ms           = opts.rest.timeout_ms;
        std::shared_ptr<IHttpTransport> transport = MakeCurlHttpTransport();
        if (opts.guard) {
            // Regulated Mode: boundary-check every request before dialing.
            transport = std::make_shared<GuardedHttpTransport>(
                std::move(transport), opts.guard, opts.deny_observer);
        }
        return RestColdTier::CreateWithTransport(ro, std::move(transport), err);
    }
```
*(This preserves prior behavior exactly when `opts.guard` is null: `MakeCurlHttpTransport()` + `CreateWithTransport` is what `RestColdTier::Create` already does internally — confirm by reading `RestColdTier::Create` in `rest_cold_tier.cpp` and match its transport construction so no knob is lost.)*

- [ ] **Step 5: Run to verify pass**

Run: `./build/tests/unit/test_cold_tier_guard`
Expected: PASS. Then confirm no regression in the existing cold-tier suite: `cd build && ctest -R "cold_tier|rest_cold|RestCold|ColdTier" --output-on-failure` — all still green (the null-guard path is unchanged).

- [ ] **Step 6: Commit**

```bash
git add src/kvstore-node/src/tier/cold_tier.h src/kvstore-node/src/tier/cold_tier.cpp \
        src/tests/unit/tier/cold_tier_guard_test.cpp src/tests/unit/CMakeLists.txt
git commit -m "feat(A10): thread BoundaryGuard into cold-tier factory (guard native-rest egress)"
```

---

### Task 2: Regulated-Mode startup gate in `NodeRuntime`

**Files:**
- Modify: `src/kvstore-node/src/runtime/node_runtime.h` (add `RegulatedModeConfig` to `Options`)
- Modify: `src/kvstore-node/src/runtime/node_runtime.cpp` (run the gate in the constructor before binding)
- Test: `src/tests/unit/runtime/node_runtime_regulated_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`

**Interfaces:**
- Consumes: `security::RegulatedModeConfig` + `security::ValidateRegulatedMode` + `security::ValidationResult` (`security/regulated_mode.h`); `security::Endpoint`/`Purpose`.
- Produces: a new field on `NodeRuntime::Options`:
  ```cpp
  // Regulated Mode (A10). When .enabled is true, NodeRuntime refuses to bind
  // (Ok()==false, error() names the offending sink) if any configured egress
  // sink is out of the declared boundary. Disabled (default) => unchanged.
  security::RegulatedModeConfig regulated;   // default: {enabled=false}
  ```

- [ ] **Step 1: Write the failing test**

```cpp
// src/tests/unit/runtime/node_runtime_regulated_test.cpp
#include "runtime/node_runtime.h"
#include <gtest/gtest.h>
using kvcache::node::runtime::NodeRuntime;
using namespace kvcache::node::security;

// Use port 0 so the OS picks a free port and the test never conflicts.
static NodeRuntime::Options BaseOpts() {
    NodeRuntime::Options o;
    o.grpc_port = 0; o.metrics_port = 0; o.skip_grpc_listener = true;
    return o;
}

TEST(NodeRuntimeRegulated, OutOfBoundarySinkRefusesToStart) {
    auto o = BaseOpts();
    o.regulated.enabled = true;
    o.regulated.allow_rules = {"*.svc.local"};
    o.regulated.configured_sinks = {
        {.host = "s3.public.io", .purpose = Purpose::kColdTier},  // out of boundary
    };
    NodeRuntime rt(o);
    EXPECT_FALSE(rt.Ok()) << "must refuse to bind when a sink is out of boundary";
    EXPECT_NE(rt.error().find("s3.public.io"), std::string::npos)
        << "error must name the offending sink";
}

TEST(NodeRuntimeRegulated, InBoundaryStartsOk) {
    auto o = BaseOpts();
    o.regulated.enabled = true;
    o.regulated.allow_rules = {"*.svc.local"};
    o.regulated.configured_sinks = {{.host = "s3.svc.local", .purpose = Purpose::kColdTier}};
    NodeRuntime rt(o);
    EXPECT_TRUE(rt.Ok()) << rt.error();
}

TEST(NodeRuntimeRegulated, DisabledIsUnchanged) {
    auto o = BaseOpts();                 // regulated.enabled = false
    o.regulated.configured_sinks = {{.host = "anywhere.evil.com", .purpose = Purpose::kColdTier}};
    NodeRuntime rt(o);
    EXPECT_TRUE(rt.Ok()) << "disabled Regulated Mode never blocks startup";
}
```

- [ ] **Step 2: Run to verify fail**

Run: `cmake --build build --target test_node_runtime_regulated && ./build/tests/unit/test_node_runtime_regulated`
Expected: FAIL to compile — `Options` has no `regulated` field.

- [ ] **Step 3: Add the field + run the gate in the constructor**

In `node_runtime.h`: add `#include "security/regulated_mode.h"` and the `regulated` field to `Options` (as in **Produces**).

In `node_runtime.cpp`, at the TOP of the `NodeRuntime::NodeRuntime(const Options& opts)` constructor body — BEFORE any socket bind — add the fail-closed gate:
```cpp
NodeRuntime::NodeRuntime(const Options& opts) : opts_(opts) {
    // A10 Regulated Mode: fail-closed startup gate. Refuse to bind if any
    // configured egress sink is out of the declared boundary.
    security::ValidationResult v = security::ValidateRegulatedMode(opts_.regulated);
    if (!v.ok) {
        ok_    = false;
        error_ = v.error;
        return;                 // do NOT bind listeners
    }
    // ... existing bind logic unchanged ...
}
```
*(Read the existing constructor first and insert the gate before the first `MakeListener`/bind call; keep the rest verbatim. Confirm `ok_`/`error_` are the real member names.)*

- [ ] **Step 4: Run to verify pass**

Run: `./build/tests/unit/test_node_runtime_regulated`
Expected: PASS (3 tests). Then regression: `cd build && ctest -R "node_runtime|NodeRuntime" --output-on-failure` — existing NodeRuntime tests still green (disabled path unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/kvstore-node/src/runtime/node_runtime.h src/kvstore-node/src/runtime/node_runtime.cpp \
        src/tests/unit/runtime/node_runtime_regulated_test.cpp src/tests/unit/CMakeLists.txt
git commit -m "feat(A10): Regulated Mode startup gate in NodeRuntime (fail-closed on out-of-boundary sink)"
```

---

### Task 3: `main.cpp` flag plumbing — build the shared guard + deny observer, feed both seams; README + full regression

**Files:**
- Modify: `src/kvstore-node/src/main.cpp`
- Create: `src/kvstore-node/src/runtime/regulated_flags.h` / `.cpp` (a testable flag→config helper, so `main.cpp` stays thin)
- Test: `src/tests/unit/runtime/regulated_flags_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`, `README.md`

**Interfaces:**
- Consumes: `security::RegulatedModeConfig`, `security::BuildPolicy`, `security::BoundaryGuard`, `security::MakeBoundaryDenyObserver` (from `security/boundary_deny_observer.h`); `ColdTierOptions`; `NodeRuntime::Options`.
- Produces:
  ```cpp
  namespace kvcache::node::runtime {
  // Parse the regulated-mode CLI surface into a RegulatedModeConfig. Pure +
  // testable (no globals). `argv`-style already-tokenized flags in/out.
  // Recognizes: --regulated-mode (bool), --boundary-allow <rule> (repeatable),
  // and derives configured_sinks from the passed egress endpoints.
  struct RegulatedFlags {
      bool enabled = false;
      std::vector<std::string> allow_rules;
  };
  security::RegulatedModeConfig
  BuildRegulatedModeConfig(const RegulatedFlags& flags,
                           const std::vector<security::Endpoint>& egress_sinks);
  }
  ```

- [ ] **Step 1: Write the failing test**

```cpp
// src/tests/unit/runtime/regulated_flags_test.cpp
#include "runtime/regulated_flags.h"
#include <gtest/gtest.h>
using namespace kvcache::node::runtime;
using namespace kvcache::node::security;

TEST(RegulatedFlags, BuildsConfigWithSinksAndRules) {
    RegulatedFlags f;
    f.enabled = true;
    f.allow_rules = {"*.svc.local", "10.0.0.0/8"};
    std::vector<Endpoint> sinks = {{.host = "s3.svc.local", .purpose = Purpose::kColdTier}};
    auto cfg = BuildRegulatedModeConfig(f, sinks);
    EXPECT_TRUE(cfg.enabled);
    EXPECT_EQ(cfg.allow_rules.size(), 2u);
    ASSERT_EQ(cfg.configured_sinks.size(), 1u);
    EXPECT_EQ(cfg.configured_sinks[0].host, "s3.svc.local");
    // Sanity: the resulting config validates in-boundary and rejects a bogus sink.
    EXPECT_TRUE(ValidateRegulatedMode(cfg).ok);
}

TEST(RegulatedFlags, DisabledProducesNoOpConfig) {
    RegulatedFlags f;                // enabled = false
    auto cfg = BuildRegulatedModeConfig(f, {{.host = "evil.com", .purpose = Purpose::kColdTier}});
    EXPECT_FALSE(cfg.enabled);
    EXPECT_TRUE(ValidateRegulatedMode(cfg).ok) << "disabled config is a no-op";
}
```

- [ ] **Step 2: Run to verify fail** — `cmake --build build --target test_regulated_flags && ./build/tests/unit/test_regulated_flags` — FAIL (header missing).

- [ ] **Step 3: Implement `regulated_flags.{h,cpp}`**

```cpp
// src/kvstore-node/src/runtime/regulated_flags.h
#pragma once
#include <string>
#include <vector>
#include "security/regulated_mode.h"
namespace kvcache::node::runtime {
struct RegulatedFlags { bool enabled = false; std::vector<std::string> allow_rules; };
security::RegulatedModeConfig
BuildRegulatedModeConfig(const RegulatedFlags& flags,
                         const std::vector<security::Endpoint>& egress_sinks);
}
```
```cpp
// src/kvstore-node/src/runtime/regulated_flags.cpp
#include "runtime/regulated_flags.h"
namespace kvcache::node::runtime {
security::RegulatedModeConfig
BuildRegulatedModeConfig(const RegulatedFlags& flags,
                         const std::vector<security::Endpoint>& egress_sinks) {
    security::RegulatedModeConfig cfg;
    cfg.enabled         = flags.enabled;
    cfg.allow_rules     = flags.allow_rules;
    cfg.configured_sinks = egress_sinks;
    // requirements (FIPS/KMS/audit) are deferred; left empty here. When those
    // controls land, push their RequirementCheck predicates onto cfg.requirements.
    return cfg;
}
}  // namespace kvcache::node::runtime
```

- [ ] **Step 4: Run to verify pass** — `./build/tests/unit/test_regulated_flags` — PASS.

- [ ] **Step 5: Wire into `main.cpp`** (build-verified, not unit-tested — it's glue)

Read `src/kvstore-node/src/main.cpp` and add, following its existing flag-parsing style:
- Parse `--regulated-mode` (sets `RegulatedFlags::enabled`) and repeatable `--boundary-allow <rule>` (appends to `allow_rules`).
- Assemble the node's egress sinks as `std::vector<security::Endpoint>` — at minimum the cold-tier `base_url` host (parse with `tier::HostFromUrl`) tagged `Purpose::kColdTier`; add etcd/telemetry endpoints if main already knows them, each with the matching `Purpose`.
- `auto cfg = BuildRegulatedModeConfig(flags, sinks);`
- Build the shared guard + observer when enabled:
  ```cpp
  std::shared_ptr<const security::BoundaryGuard> guard;
  tier::GuardedHttpTransport::DenyObserver deny_obs;
  if (cfg.enabled) {
      guard = std::make_shared<security::BoundaryGuard>(
          security::BuildPolicy(cfg.allow_rules, /*default_deny=*/true));
      deny_obs = security::MakeBoundaryDenyObserver(/*incr_metric=*/{}, /*audit=*/nullptr);
      // (wire the real metric increment + AuditLog* here if main owns them)
  }
  ```
- Pass `cfg` into `NodeRuntime::Options::regulated`, and `guard`/`deny_obs` into the `ColdTierOptions` used to build the cold tier.
- Ensure the node exits non-zero with the error if `NodeRuntime::Ok()` is false after construction (the gate refused). Confirm main already checks `Ok()`; if not, add the check.

- [ ] **Step 6: README + FULL regression**

Add an A10-production-wiring line to `README.md` under the A10 entry, matching the format of prior A-item phase-log entries (do NOT touch VERSION/badge). Then:
`cd build && cmake --build . -j4 && ctest --output-on-failure` — expect the prior green count + the new tests (`test_cold_tier_guard`, `test_node_runtime_regulated`, `test_regulated_flags`), 0 failures. Capture the summary line.

- [ ] **Step 7: Commit**

```bash
git add src/kvstore-node/src/main.cpp \
        src/kvstore-node/src/runtime/regulated_flags.h src/kvstore-node/src/runtime/regulated_flags.cpp \
        src/tests/unit/runtime/regulated_flags_test.cpp src/tests/unit/CMakeLists.txt README.md
git commit -m "feat(A10): main.cpp regulated-mode flags → shared guard + deny observer into both seams; full-suite green"
```

---

### Deferred / follow-on (unchanged from the A10 core plan)

- FIPS provider, `IKeyProvider`/KMS, hash-chained audit, region/account-tag rules — plug into `cfg.requirements` (FIPS/KMS/audit) and `Rule` (tags) when built.
- Per-dial runtime guarding of the OTHER seams (OTLP exporter, A9 replication dial once gRPC lands, etcd/gRPC peer dialer) — each mirrors Task 1's transport-wrap pattern. The startup gate (Task 2) already covers them at config-enumeration level once their endpoints are added to `configured_sinks` in Task 3.
- Real metric wiring for the deny observer (the `incr_metric` lambda) — connect to the counter registry if `main` owns it; left as an injectable in Task 3.

---

## Self-Review

**Spec coverage:** This plan completes the "wire it into production" gap the A10 whole-branch review flagged. §4 seams: the cold-tier `CurlHttpTransport` seam is now enforced at runtime (Task 1); the startup gate enumerates + checks all configured sinks and refuses to boot (Task 2, spec §4 "startup gate"); §3 Regulated-Mode boot behavior (refuse unless boundary active) is realized by the NodeRuntime gate. R4 (off by default, unchanged when disabled) is the invariant every task preserves (null guard / `enabled=false` → prior behavior). Deferred controls (§6) remain deferred via the empty `cfg.requirements` + the documented follow-on. The other §4 seams (telemetry/etcd/replication) are covered at the startup-gate level (add their endpoints to `configured_sinks`) with per-dial runtime wrapping left as a mechanical follow-on mirroring Task 1.

**Placeholder scan:** every code step has real code; the `main.cpp` step (Task 3 Step 5) is glue described concretely (which flags, which helper, which fields) and is build-verified rather than unit-tested because CLI `main` wiring isn't unit-testable — the testable part is extracted into `BuildRegulatedModeConfig` (Task 3 Steps 1–4). No TBDs in deliverables.

**Type consistency:** `ColdTierOptions::guard` (`shared_ptr<const security::BoundaryGuard>`) + `deny_observer` (`tier::GuardedHttpTransport::DenyObserver`) in Task 1 match what `main.cpp` builds in Task 3. `NodeRuntime::Options::regulated` (`security::RegulatedModeConfig`) in Task 2 is produced by `BuildRegulatedModeConfig` in Task 3. `security::Endpoint`/`Purpose`, `BuildPolicy`, `ValidateRegulatedMode`, `MakeBoundaryDenyObserver` are consumed exactly as the A10 core defines them. `MakeCurlHttpTransport`/`CreateWithTransport`/`GuardedHttpTransport` used per their real signatures (confirm while implementing).

**Simplicity:** the wiring is additive (optional fields, one gate call, one flag helper) — no restructuring of the cold-tier factory or NodeRuntime beyond the minimal insertion points. The disabled path is provably unchanged (null guard reproduces the exact prior transport construction; `enabled=false` returns `{ok=true}` immediately).
