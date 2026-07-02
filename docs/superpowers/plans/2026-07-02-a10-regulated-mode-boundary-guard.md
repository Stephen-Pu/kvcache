# A10 Regulated Mode — BoundaryGuard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the local, buildable core of A10 Regulated Mode — a fail-closed `BoundaryGuard` that decides whether a network endpoint is inside the operator-declared perimeter, a concrete guarded egress seam (cold-tier HTTP) that enforces it, and a Regulated-Mode startup gate that refuses to boot when any configured sink is out-of-boundary — all unit-testable with no cloud and no auditor.

**Architecture:** The centerpiece is a pure, thread-safe `BoundaryGuard::Check(Endpoint) -> Decision` (host-suffix/glob + CIDR matching, `default_deny`, empty-allowlist = air-gap). A `GuardedHttpTransport` decorator wraps the existing cold-tier `IHttpTransport` seam and `Check`s each URL's host before dialing (deny → error + observer callback), mirroring the established SigV4/metrics/encrypting decorator pattern. A `RegulatedMode` startup gate enumerates statically-configured egress endpoints, `Check`s each, and refuses to boot (fail-closed, naming the offending sink) on any deny; required-but-deferred controls (FIPS, KMS envelope, audit) are represented as injectable requirement predicates so the interface exists without building them yet.

**Tech Stack:** C++20, CMake+Ninja, GoogleTest (FetchContent), the existing `kvstore-node` security + tier modules (`security/audit.h`, `tier/rest_cold_tier.h`'s `IHttpTransport`).

**Spec:** `docs/superpowers/specs/2026-07-01-a10-regulated-mode-design.md`. This plan implements the **buildable local core** the spec calls out as unit-testable (§9): §4 BoundaryGuard (`Check`, rules, startup gate, fail-closed) and R2/R3. It **defers** (per the spec's own §6/§10/non-goals) FIPS provider integration, `IKeyProvider`/KMS, hash-chained audit, region/account-tag rule resolution, and the NIST control map (a doc artifact). Those are follow-ons; the requirement-predicate seam in Task 4 is where they later plug in.

## Global Constraints

- C++20; CMake ≥ 3.28 in practice (repo FetchContent uses a URL_HASH form 3.22 rejects).
- No new third-party dependency; use only the C++ standard library and in-tree modules.
- Default build unaffected: all new code is additive; Regulated Mode is **off by default** (spec R4) — when disabled, behavior is byte-for-byte unchanged (no guard constructed, no seam wrapping).
- **Fail-closed always** (spec R3/§4/§8): no code path fails open. A guard construction failure, an unresolvable endpoint, or an unparseable rule is a DENY / a refuse-to-boot, never an allow.
- Follow existing patterns: transport decorators mirror `tier/sigv4_transport.h`; tests are gtest under `src/tests/unit/security/` (new files) and `src/tests/unit/tier/`, wired in `src/tests/unit/CMakeLists.txt` following neighboring targets.
- New production sources compile into the existing `kvstore_node_core` / node security source list (grounded per task) — NOT into a new library.
- TDD: failing test → run-fail → minimal impl → run-pass → commit, per step.

## File Structure

- `src/kvstore-node/src/security/boundary_guard.h` / `.cpp` — `Endpoint`, `Purpose`, `Rule`, `BoundaryPolicy`, `Decision`, and `BoundaryGuard` (pure `Check`). Task 1.
- `src/kvstore-node/src/security/boundary_policy_builder.h` / `.cpp` — build a `BoundaryPolicy` from operator-supplied allow-rule strings (host-suffix/glob vs CIDR classification). Task 2.
- `src/kvstore-node/src/tier/guarded_transport.h` / `.cpp` — `GuardedHttpTransport`, an `IHttpTransport` decorator that `Check`s each URL host. Task 3.
- `src/kvstore-node/src/security/regulated_mode.h` / `.cpp` — `RegulatedModeConfig` + `ValidateRegulatedMode(...)` startup gate. Task 4.
- `src/tests/unit/security/boundary_guard_test.cpp`, `boundary_policy_builder_test.cpp`, `regulated_mode_test.cpp`; `src/tests/unit/tier/guarded_transport_test.cpp` — tests.
- `src/tests/unit/CMakeLists.txt` — wire the four test targets.
- Deferred (NOT in this plan; documented in spec §6/§10): FIPS provider, `IKeyProvider`/KMS, hash-chain audit, region/account-tag rules, gating seams beyond the cold-tier transport (OTLP exporter, A9 replication dial, etcd dialer — the startup gate covers them at config-enumeration level in Task 4; per-dial runtime decorators for each are a follow-on).

---

### Task 1: `BoundaryGuard::Check` — the pure fail-closed perimeter decision

**Files:**
- Create: `src/kvstore-node/src/security/boundary_guard.h`, `src/kvstore-node/src/security/boundary_guard.cpp`
- Test: `src/tests/unit/security/boundary_guard_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`

**Interfaces:**
- Consumes: nothing (pure; standard library only).
- Produces:
  ```cpp
  namespace kvcache::node::security {
  enum class Purpose { kColdTier, kKms, kTelemetry, kReplication, kEtcd, kOther };
  struct Endpoint { std::string host; uint16_t port = 0; Purpose purpose = Purpose::kOther; };
  // A rule matches by host suffix/glob OR by CIDR. Exactly one match mode per rule.
  struct Rule {
      std::string host_glob;   // e.g. "*.svc.cluster.local" or "s3.us-gov-west-1.amazonaws.com"; empty if CIDR rule
      std::string cidr;        // e.g. "10.0.0.0/8"; empty if host rule
      // optional: restrict this rule to one Purpose. Purpose::kOther means "any".
      Purpose purpose = Purpose::kOther;
  };
  struct BoundaryPolicy { std::vector<Rule> allow; bool default_deny = true; };
  struct Decision { bool allow = false; std::string reason; };
  class BoundaryGuard {
   public:
    explicit BoundaryGuard(BoundaryPolicy policy);
    // Pure, thread-safe (const, no mutable state). Never throws; any parse/
    // resolution failure is a DENY.
    Decision Check(const Endpoint& ep) const;
   private:
    BoundaryPolicy policy_;
  };
  // Helpers (also unit-tested):
  bool HostMatchesGlob(std::string_view host, std::string_view glob);  // '*' = one label-or-more suffix/prefix wildcard
  bool IpInCidr(std::string_view ip, std::string_view cidr);           // IPv4 only for MVP; malformed → false
  }
  ```

- [ ] **Step 1: Write the failing test**

```cpp
// src/tests/unit/security/boundary_guard_test.cpp
#include "security/boundary_guard.h"
#include <gtest/gtest.h>
using namespace kvcache::node::security;

static BoundaryGuard MakeGuard(std::vector<Rule> allow, bool default_deny = true) {
    return BoundaryGuard(BoundaryPolicy{std::move(allow), default_deny});
}

TEST(BoundaryGuard, HostSuffixGlobAllows) {
    auto g = MakeGuard({{.host_glob = "*.svc.cluster.local"}});
    EXPECT_TRUE(g.Check({.host = "node-1.svc.cluster.local", .port = 443}).allow);
    auto d = g.Check({.host = "evil.example.com", .port = 443});
    EXPECT_FALSE(d.allow);
    EXPECT_FALSE(d.reason.empty()) << "deny must explain why";
}

TEST(BoundaryGuard, ExactHostAllows) {
    auto g = MakeGuard({{.host_glob = "s3.us-gov-west-1.amazonaws.com"}});
    EXPECT_TRUE(g.Check({.host = "s3.us-gov-west-1.amazonaws.com"}).allow);
    EXPECT_FALSE(g.Check({.host = "s3.us-east-1.amazonaws.com"}).allow);
}

TEST(BoundaryGuard, CidrAllows) {
    auto g = MakeGuard({{.cidr = "10.0.0.0/8"}});
    EXPECT_TRUE(g.Check({.host = "10.5.6.7"}).allow);
    EXPECT_FALSE(g.Check({.host = "192.168.1.1"}).allow);
}

TEST(BoundaryGuard, EmptyAllowlistIsAirGap) {
    auto g = MakeGuard({});  // default_deny = true, no rules
    EXPECT_FALSE(g.Check({.host = "anything.local"}).allow);
    EXPECT_FALSE(g.Check({.host = "10.0.0.1"}).allow);
}

TEST(BoundaryGuard, PerPurposeRuleScoping) {
    // A rule scoped to kKms only allows KMS-purpose endpoints on that host.
    auto g = MakeGuard({{.host_glob = "kms.local", .purpose = Purpose::kKms}});
    EXPECT_TRUE(g.Check({.host = "kms.local", .purpose = Purpose::kKms}).allow);
    EXPECT_FALSE(g.Check({.host = "kms.local", .purpose = Purpose::kColdTier}).allow)
        << "purpose-scoped rule must not admit a different purpose";
}

TEST(BoundaryGuard, MalformedEndpointDenies) {
    auto g = MakeGuard({{.host_glob = "*.local"}});
    EXPECT_FALSE(g.Check({.host = ""}).allow) << "empty host is fail-closed deny";
}

TEST(HostMatchesGlob, SuffixWildcard) {
    EXPECT_TRUE(HostMatchesGlob("a.b.example.com", "*.example.com"));
    EXPECT_TRUE(HostMatchesGlob("example.com", "example.com"));
    EXPECT_FALSE(HostMatchesGlob("example.com.evil.net", "*.example.com"));
    EXPECT_FALSE(HostMatchesGlob("", "*.example.com"));
}

TEST(IpInCidr, Ipv4Ranges) {
    EXPECT_TRUE(IpInCidr("10.1.2.3", "10.0.0.0/8"));
    EXPECT_FALSE(IpInCidr("11.0.0.1", "10.0.0.0/8"));
    EXPECT_TRUE(IpInCidr("192.168.1.5", "192.168.1.0/24"));
    EXPECT_FALSE(IpInCidr("192.168.2.5", "192.168.1.0/24"));
    EXPECT_FALSE(IpInCidr("not-an-ip", "10.0.0.0/8")) << "malformed → false (fail-closed)";
    EXPECT_FALSE(IpInCidr("10.0.0.1", "garbage")) << "malformed cidr → false";
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cmake --build build --target test_boundary_guard && ./build/tests/unit/test_boundary_guard`
Expected: FAIL to compile — `boundary_guard.h` missing.

- [ ] **Step 3: Write the header**

```cpp
// src/kvstore-node/src/security/boundary_guard.h
#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
namespace kvcache::node::security {
enum class Purpose { kColdTier, kKms, kTelemetry, kReplication, kEtcd, kOther };
struct Endpoint { std::string host; uint16_t port = 0; Purpose purpose = Purpose::kOther; };
struct Rule { std::string host_glob; std::string cidr; Purpose purpose = Purpose::kOther; };
struct BoundaryPolicy { std::vector<Rule> allow; bool default_deny = true; };
struct Decision { bool allow = false; std::string reason; };

bool HostMatchesGlob(std::string_view host, std::string_view glob);
bool IpInCidr(std::string_view ip, std::string_view cidr);

class BoundaryGuard {
 public:
  explicit BoundaryGuard(BoundaryPolicy policy) : policy_(std::move(policy)) {}
  Decision Check(const Endpoint& ep) const;
 private:
  BoundaryPolicy policy_;
};
}  // namespace kvcache::node::security
```

- [ ] **Step 4: Implement `.cpp`**

```cpp
// src/kvstore-node/src/security/boundary_guard.cpp
#include "security/boundary_guard.h"
#include <array>
#include <cstdlib>
namespace kvcache::node::security {
namespace {
// A rule's purpose gate: kOther means "any purpose".
bool PurposeMatches(Purpose rule, Purpose ep) { return rule == Purpose::kOther || rule == ep; }
}  // namespace

bool HostMatchesGlob(std::string_view host, std::string_view glob) {
    if (host.empty() || glob.empty()) return false;
    if (glob.front() == '*') {
        // "*.example.com" matches any strict subdomain of example.com.
        std::string_view suffix = glob.substr(1);  // ".example.com"
        if (host.size() <= suffix.size()) return false;
        return host.substr(host.size() - suffix.size()) == suffix;
    }
    return host == glob;  // exact match
}

bool IpInCidr(std::string_view ip, std::string_view cidr) {
    auto parse_v4 = [](std::string_view s, uint32_t* out) -> bool {
        std::array<int, 4> oct{};
        int idx = 0; long cur = 0; bool any = false;
        for (size_t i = 0; i <= s.size(); ++i) {
            if (i == s.size() || s[i] == '.') {
                if (!any || cur < 0 || cur > 255 || idx > 3) return false;
                oct[idx++] = static_cast<int>(cur); cur = 0; any = false;
            } else if (s[i] >= '0' && s[i] <= '9') {
                cur = cur * 10 + (s[i] - '0'); any = true;
                if (cur > 255) return false;
            } else return false;
        }
        if (idx != 4) return false;
        *out = (uint32_t(oct[0]) << 24) | (uint32_t(oct[1]) << 16) |
               (uint32_t(oct[2]) << 8) | uint32_t(oct[3]);
        return true;
    };
    auto slash = cidr.find('/');
    if (slash == std::string_view::npos) return false;
    uint32_t net = 0, addr = 0;
    if (!parse_v4(cidr.substr(0, slash), &net)) return false;
    if (!parse_v4(ip, &addr)) return false;
    int bits = 0; auto pfx = cidr.substr(slash + 1);
    if (pfx.empty() || pfx.size() > 2) return false;
    for (char c : pfx) { if (c < '0' || c > '9') return false; bits = bits * 10 + (c - '0'); }
    if (bits < 0 || bits > 32) return false;
    uint32_t mask = bits == 0 ? 0u : (0xFFFFFFFFu << (32 - bits));
    return (net & mask) == (addr & mask);
}

Decision BoundaryGuard::Check(const Endpoint& ep) const {
    if (ep.host.empty()) return {false, "empty host (fail-closed)"};
    for (const auto& r : policy_.allow) {
        if (!PurposeMatches(r.purpose, ep.purpose)) continue;
        if (!r.host_glob.empty() && HostMatchesGlob(ep.host, r.host_glob))
            return {true, ""};
        if (!r.cidr.empty() && IpInCidr(ep.host, r.cidr))
            return {true, ""};
    }
    return {false, "endpoint '" + ep.host + "' not in allowlist (default-deny)"};
}
}  // namespace kvcache::node::security
```

- [ ] **Step 5: Wire the test target**

In `src/tests/unit/CMakeLists.txt`, add a `test_boundary_guard` target following the neighboring pattern for a `security/` unit that needs the node security sources (grep for how `audit_test` / `rbac` tests are declared; include `boundary_guard.cpp` in the target's sources or link the node core lib as those do).

- [ ] **Step 6: Run to verify pass**

Run: `cmake --build build --target test_boundary_guard && ./build/tests/unit/test_boundary_guard`
Expected: PASS (all cases).

- [ ] **Step 7: Commit**

```bash
git add src/kvstore-node/src/security/boundary_guard.h src/kvstore-node/src/security/boundary_guard.cpp \
        src/tests/unit/security/boundary_guard_test.cpp src/tests/unit/CMakeLists.txt
git commit -m "feat(A10): BoundaryGuard::Check — fail-closed perimeter decision (glob/CIDR/default-deny)"
```

---

### Task 2: `BoundaryPolicyBuilder` — build a policy from operator allow-rules

**Files:**
- Create: `src/kvstore-node/src/security/boundary_policy_builder.h`, `.cpp`
- Test: `src/tests/unit/security/boundary_policy_builder_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 1 `Rule`, `BoundaryPolicy`.
- Produces:
  ```cpp
  namespace kvcache::node::security {
  // Classify each allow-string into a Rule: a token containing '/' with a
  // trailing numeric prefix is treated as CIDR; otherwise a host glob/exact.
  // Empty input vector => empty allowlist (air-gap), default_deny stays true.
  BoundaryPolicy BuildPolicy(const std::vector<std::string>& allow_rules,
                             bool default_deny = true);
  }
  ```

- [ ] **Step 1: Write the failing test**

```cpp
// src/tests/unit/security/boundary_policy_builder_test.cpp
#include "security/boundary_policy_builder.h"
#include "security/boundary_guard.h"
#include <gtest/gtest.h>
using namespace kvcache::node::security;

TEST(BuildPolicy, ClassifiesCidrVsHostAndRoundTripsThroughGuard) {
    auto p = BuildPolicy({"*.svc.cluster.local", "10.0.0.0/8", "s3.gov.local"});
    ASSERT_EQ(p.allow.size(), 3u);
    EXPECT_TRUE(p.default_deny);
    BoundaryGuard g(p);
    EXPECT_TRUE(g.Check({.host = "n.svc.cluster.local"}).allow);
    EXPECT_TRUE(g.Check({.host = "10.9.9.9"}).allow);
    EXPECT_TRUE(g.Check({.host = "s3.gov.local"}).allow);
    EXPECT_FALSE(g.Check({.host = "8.8.8.8"}).allow);
}

TEST(BuildPolicy, EmptyIsAirGap) {
    auto p = BuildPolicy({});
    EXPECT_TRUE(p.allow.empty());
    EXPECT_TRUE(p.default_deny);
    BoundaryGuard g(p);
    EXPECT_FALSE(g.Check({.host = "anything"}).allow);
}
```

- [ ] **Step 2: Run to verify fail** — `cmake --build build --target test_boundary_policy_builder && ./build/tests/unit/test_boundary_policy_builder` — FAIL (header missing).

- [ ] **Step 3: Implement**

```cpp
// src/kvstore-node/src/security/boundary_policy_builder.h
#pragma once
#include <string>
#include <vector>
#include "security/boundary_guard.h"
namespace kvcache::node::security {
BoundaryPolicy BuildPolicy(const std::vector<std::string>& allow_rules, bool default_deny = true);
}
```

```cpp
// src/kvstore-node/src/security/boundary_policy_builder.cpp
#include "security/boundary_policy_builder.h"
namespace kvcache::node::security {
namespace {
// A token is CIDR if it has a '/' and the part after it is all digits.
bool LooksLikeCidr(const std::string& s) {
    auto slash = s.find('/');
    if (slash == std::string::npos || slash + 1 >= s.size()) return false;
    for (size_t i = slash + 1; i < s.size(); ++i)
        if (s[i] < '0' || s[i] > '9') return false;
    return true;
}
}  // namespace
BoundaryPolicy BuildPolicy(const std::vector<std::string>& allow_rules, bool default_deny) {
    BoundaryPolicy p; p.default_deny = default_deny;
    for (const auto& tok : allow_rules) {
        if (tok.empty()) continue;
        Rule r;
        if (LooksLikeCidr(tok)) r.cidr = tok; else r.host_glob = tok;
        p.allow.push_back(std::move(r));
    }
    return p;
}
}  // namespace kvcache::node::security
```

- [ ] **Step 4: Run to verify pass** — `./build/tests/unit/test_boundary_policy_builder` — PASS.

- [ ] **Step 5: Commit**

```bash
git add src/kvstore-node/src/security/boundary_policy_builder.h src/kvstore-node/src/security/boundary_policy_builder.cpp \
        src/tests/unit/security/boundary_policy_builder_test.cpp src/tests/unit/CMakeLists.txt
git commit -m "feat(A10): BoundaryPolicyBuilder — classify allow-rules into host/CIDR rules"
```

---

### Task 3: `GuardedHttpTransport` — enforce the guard on the cold-tier egress seam

**Files:**
- Create: `src/kvstore-node/src/tier/guarded_transport.h`, `.cpp`
- Test: `src/tests/unit/tier/guarded_transport_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`

**Interfaces:**
- Consumes: `tier::IHttpTransport` + `HttpResult` (from `tier/rest_cold_tier.h`); Task 1 `BoundaryGuard`, `Endpoint`, `Purpose`.
- Produces:
  ```cpp
  namespace kvcache::node::tier {
  // Decorator: on each Request, parse the URL host, Check{host, port, kColdTier}
  // against the guard; on DENY return an HttpResult with transport_err set
  // (never dials the inner transport) and invoke the deny observer; on ALLOW
  // delegate to the wrapped transport. Fail-closed: an unparseable URL is a DENY.
  class GuardedHttpTransport final : public IHttpTransport {
   public:
    using DenyObserver =
        std::function<void(const security::Endpoint&, std::string_view reason)>;
    GuardedHttpTransport(std::shared_ptr<IHttpTransport> inner,
                         std::shared_ptr<const security::BoundaryGuard> guard,
                         DenyObserver on_deny = {});
    HttpResult Request(const std::string& method, const std::string& url,
                       const std::vector<std::string>& headers,
                       const uint8_t* body, std::size_t n) override;
  };
  // Extract the host (no scheme, no port, no path) from an http(s) URL.
  // Returns empty on malformed input (caller treats empty as fail-closed deny).
  std::string HostFromUrl(std::string_view url, uint16_t* port_out);
  }
  ```

- [ ] **Step 1: Write the failing test**

```cpp
// src/tests/unit/tier/guarded_transport_test.cpp
#include "tier/guarded_transport.h"
#include "tier/rest_cold_tier.h"
#include "security/boundary_guard.h"
#include <gtest/gtest.h>
#include <memory>
using namespace kvcache::node::tier;
using namespace kvcache::node::security;

namespace {
// Records whether the inner transport was ever dialed.
struct SpyTransport : IHttpTransport {
    int calls = 0;
    HttpResult Request(const std::string&, const std::string&,
                       const std::vector<std::string>&, const uint8_t*, std::size_t) override {
        ++calls; return HttpResult{200, "ok", ""};
    }
};
}  // namespace

TEST(GuardedHttpTransport, AllowsInBoundaryHostAndDelegates) {
    auto spy = std::make_shared<SpyTransport>();
    auto guard = std::make_shared<BoundaryGuard>(BoundaryPolicy{{{.host_glob = "*.svc.local"}}, true});
    GuardedHttpTransport g(spy, guard);
    auto r = g.Request("GET", "https://store.svc.local/bucket/obj.kv", {}, nullptr, 0);
    EXPECT_EQ(r.status, 200);
    EXPECT_EQ(spy->calls, 1) << "in-boundary request must reach inner transport";
}

TEST(GuardedHttpTransport, BlocksOutOfBoundaryHostAndNeverDials) {
    auto spy = std::make_shared<SpyTransport>();
    auto guard = std::make_shared<BoundaryGuard>(BoundaryPolicy{{{.host_glob = "*.svc.local"}}, true});
    bool denied = false; std::string reason;
    GuardedHttpTransport g(spy, guard, [&](const Endpoint& ep, std::string_view why) {
        denied = true; reason = std::string(why); EXPECT_EQ(ep.purpose, Purpose::kColdTier);
    });
    auto r = g.Request("GET", "https://exfil.evil.com/x", {}, nullptr, 0);
    EXPECT_EQ(spy->calls, 0) << "out-of-boundary request must NOT dial";
    EXPECT_NE(r.transport_err, "") << "deny surfaces as transport_err (fail-closed)";
    EXPECT_TRUE(denied);
    EXPECT_FALSE(reason.empty());
}

TEST(GuardedHttpTransport, MalformedUrlIsDenied) {
    auto spy = std::make_shared<SpyTransport>();
    auto guard = std::make_shared<BoundaryGuard>(BoundaryPolicy{{{.host_glob = "*"}}, true});
    GuardedHttpTransport g(spy, guard);
    auto r = g.Request("GET", "not-a-url", {}, nullptr, 0);
    EXPECT_EQ(spy->calls, 0);
    EXPECT_NE(r.transport_err, "");
}

TEST(HostFromUrl, ParsesHostAndPort) {
    uint16_t port = 0;
    EXPECT_EQ(HostFromUrl("https://a.b.com:8443/x/y", &port), "a.b.com");
    EXPECT_EQ(port, 8443);
    port = 0;
    EXPECT_EQ(HostFromUrl("http://host.local/x", &port), "host.local");
    EXPECT_EQ(port, 80);
    EXPECT_EQ(HostFromUrl("garbage", &port), "");
}
```

- [ ] **Step 2: Run to verify fail** — `cmake --build build --target test_guarded_transport && ./build/tests/unit/test_guarded_transport` — FAIL (header missing).

- [ ] **Step 3: Implement the header**

```cpp
// src/kvstore-node/src/tier/guarded_transport.h
#pragma once
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include "tier/rest_cold_tier.h"       // IHttpTransport, HttpResult
#include "security/boundary_guard.h"   // BoundaryGuard, Endpoint, Purpose
namespace kvcache::node::tier {
std::string HostFromUrl(std::string_view url, uint16_t* port_out);
class GuardedHttpTransport final : public IHttpTransport {
 public:
  using DenyObserver = std::function<void(const security::Endpoint&, std::string_view)>;
  GuardedHttpTransport(std::shared_ptr<IHttpTransport> inner,
                       std::shared_ptr<const security::BoundaryGuard> guard,
                       DenyObserver on_deny = {})
      : inner_(std::move(inner)), guard_(std::move(guard)), on_deny_(std::move(on_deny)) {}
  HttpResult Request(const std::string& method, const std::string& url,
                     const std::vector<std::string>& headers,
                     const uint8_t* body, std::size_t n) override;
 private:
  std::shared_ptr<IHttpTransport> inner_;
  std::shared_ptr<const security::BoundaryGuard> guard_;
  DenyObserver on_deny_;
};
}  // namespace kvcache::node::tier
```

- [ ] **Step 4: Implement the `.cpp`**

```cpp
// src/kvstore-node/src/tier/guarded_transport.cpp
#include "tier/guarded_transport.h"
namespace kvcache::node::tier {
std::string HostFromUrl(std::string_view url, uint16_t* port_out) {
    uint16_t default_port = 0;
    std::string_view rest;
    if (url.substr(0, 8) == "https://") { default_port = 443; rest = url.substr(8); }
    else if (url.substr(0, 7) == "http://") { default_port = 80; rest = url.substr(7); }
    else { if (port_out) *port_out = 0; return ""; }
    auto slash = rest.find('/');
    std::string_view authority = slash == std::string_view::npos ? rest : rest.substr(0, slash);
    if (authority.empty()) { if (port_out) *port_out = 0; return ""; }
    auto colon = authority.find(':');
    std::string host{authority.substr(0, colon)};
    uint16_t port = default_port;
    if (colon != std::string_view::npos) {
        long p = 0; auto ps = authority.substr(colon + 1);
        if (ps.empty()) { if (port_out) *port_out = 0; return ""; }
        for (char c : ps) { if (c < '0' || c > '9') { if (port_out) *port_out = 0; return ""; }
                            p = p * 10 + (c - '0'); }
        if (p <= 0 || p > 65535) { if (port_out) *port_out = 0; return ""; }
        port = static_cast<uint16_t>(p);
    }
    if (host.empty()) { if (port_out) *port_out = 0; return ""; }
    if (port_out) *port_out = port;
    return host;
}

HttpResult GuardedHttpTransport::Request(const std::string& method, const std::string& url,
                                         const std::vector<std::string>& headers,
                                         const uint8_t* body, std::size_t n) {
    uint16_t port = 0;
    std::string host = HostFromUrl(url, &port);
    security::Endpoint ep{host, port, security::Purpose::kColdTier};
    security::Decision d = host.empty()
        ? security::Decision{false, "unparseable URL (fail-closed)"}
        : guard_->Check(ep);
    if (!d.allow) {
        if (on_deny_) on_deny_(ep, d.reason);
        HttpResult r; r.status = 0;
        r.transport_err = "boundary-denied: " + d.reason;
        return r;  // never dial
    }
    return inner_->Request(method, url, headers, body, n);
}
}  // namespace kvcache::node::tier
```

- [ ] **Step 5: Wire target + run** — add `test_guarded_transport` to `src/tests/unit/CMakeLists.txt` (needs `guarded_transport.cpp`, `boundary_guard.cpp`, and whatever `rest_cold_tier` needs for `HttpResult`/`IHttpTransport` — grep the existing `test_rest_cold_tier` target for the source set). Run: `./build/tests/unit/test_guarded_transport` — PASS.

- [ ] **Step 6: Commit**

```bash
git add src/kvstore-node/src/tier/guarded_transport.h src/kvstore-node/src/tier/guarded_transport.cpp \
        src/tests/unit/tier/guarded_transport_test.cpp src/tests/unit/CMakeLists.txt
git commit -m "feat(A10): GuardedHttpTransport — fail-closed boundary check on cold-tier egress"
```

---

### Task 4: Regulated Mode startup gate — refuse to boot on any out-of-boundary sink

**Files:**
- Create: `src/kvstore-node/src/security/regulated_mode.h`, `.cpp`
- Test: `src/tests/unit/security/regulated_mode_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 1 `BoundaryGuard`, `Endpoint`, `Decision`; Task 2 `BuildPolicy`.
- Produces:
  ```cpp
  namespace kvcache::node::security {
  // Injectable requirement check for a deferred control (FIPS / KMS / audit).
  // Returns std::nullopt if satisfied, or an error string naming what's missing.
  // This is the seam where the deferred §6 controls plug in later; for now the
  // caller supplies concrete checks (or none, in a non-strict test).
  using RequirementCheck = std::function<std::optional<std::string>()>;
  struct RegulatedModeConfig {
      bool enabled = false;                       // R4: off by default
      std::vector<std::string> allow_rules;       // operator allowlist (host globs / CIDRs)
      std::vector<Endpoint> configured_sinks;     // every statically-configured egress endpoint
      std::vector<RequirementCheck> requirements; // FIPS/KMS/audit presence (deferred controls)
  };
  struct ValidationResult { bool ok = false; std::string error; };
  // Fail-closed startup gate. If cfg.enabled is false → {ok=true} (no-op, unchanged behavior).
  // If enabled: build the guard, Check EVERY configured sink (refuse on first deny,
  // error names the offending sink + reason), then run EVERY requirement check
  // (refuse on first unmet). Returns {ok=true} only if all pass.
  ValidationResult ValidateRegulatedMode(const RegulatedModeConfig& cfg);
  }
  ```

- [ ] **Step 1: Write the failing test**

```cpp
// src/tests/unit/security/regulated_mode_test.cpp
#include "security/regulated_mode.h"
#include <gtest/gtest.h>
using namespace kvcache::node::security;

TEST(RegulatedMode, DisabledIsNoOp) {
    RegulatedModeConfig cfg;  // enabled = false
    cfg.configured_sinks = {{.host = "anywhere.evil.com", .purpose = Purpose::kColdTier}};
    auto r = ValidateRegulatedMode(cfg);
    EXPECT_TRUE(r.ok) << "disabled Regulated Mode never blocks";
}

TEST(RegulatedMode, AllInBoundaryBoots) {
    RegulatedModeConfig cfg;
    cfg.enabled = true;
    cfg.allow_rules = {"*.svc.local", "10.0.0.0/8"};
    cfg.configured_sinks = {
        {.host = "s3.svc.local", .purpose = Purpose::kColdTier},
        {.host = "10.0.1.2",     .purpose = Purpose::kEtcd},
    };
    auto r = ValidateRegulatedMode(cfg);
    EXPECT_TRUE(r.ok) << r.error;
}

TEST(RegulatedMode, OutOfBoundarySinkRefusesAndNamesIt) {
    RegulatedModeConfig cfg;
    cfg.enabled = true;
    cfg.allow_rules = {"*.svc.local"};
    cfg.configured_sinks = {
        {.host = "s3.svc.local",  .purpose = Purpose::kColdTier},
        {.host = "otel.public.io", .purpose = Purpose::kTelemetry},  // out of boundary
    };
    auto r = ValidateRegulatedMode(cfg);
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.error.find("otel.public.io"), std::string::npos)
        << "error must name the offending sink";
}

TEST(RegulatedMode, UnmetRequirementRefuses) {
    RegulatedModeConfig cfg;
    cfg.enabled = true;
    cfg.allow_rules = {"*"};
    cfg.configured_sinks = {};
    cfg.requirements = {
        []() -> std::optional<std::string> { return std::nullopt; },       // satisfied
        []() -> std::optional<std::string> { return "FIPS provider not active"; },  // missing
    };
    auto r = ValidateRegulatedMode(cfg);
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.error.find("FIPS"), std::string::npos);
}

TEST(RegulatedMode, EmptyAllowlistIsAirGapRefusesAnySink) {
    RegulatedModeConfig cfg;
    cfg.enabled = true;                 // no allow_rules => air-gap
    cfg.configured_sinks = {{.host = "s3.svc.local", .purpose = Purpose::kColdTier}};
    auto r = ValidateRegulatedMode(cfg);
    EXPECT_FALSE(r.ok) << "air-gap must deny every egress sink";
}
```

- [ ] **Step 2: Run to verify fail** — `cmake --build build --target test_regulated_mode && ./build/tests/unit/test_regulated_mode` — FAIL (header missing).

- [ ] **Step 3: Implement header**

```cpp
// src/kvstore-node/src/security/regulated_mode.h
#pragma once
#include <functional>
#include <optional>
#include <string>
#include <vector>
#include "security/boundary_guard.h"
namespace kvcache::node::security {
using RequirementCheck = std::function<std::optional<std::string>()>;
struct RegulatedModeConfig {
    bool enabled = false;
    std::vector<std::string> allow_rules;
    std::vector<Endpoint> configured_sinks;
    std::vector<RequirementCheck> requirements;
};
struct ValidationResult { bool ok = false; std::string error; };
ValidationResult ValidateRegulatedMode(const RegulatedModeConfig& cfg);
}  // namespace kvcache::node::security
```

- [ ] **Step 4: Implement `.cpp`**

```cpp
// src/kvstore-node/src/security/regulated_mode.cpp
#include "security/regulated_mode.h"
#include "security/boundary_policy_builder.h"
namespace kvcache::node::security {
namespace {
const char* PurposeName(Purpose p) {
    switch (p) {
        case Purpose::kColdTier: return "cold-tier";
        case Purpose::kKms: return "kms";
        case Purpose::kTelemetry: return "telemetry";
        case Purpose::kReplication: return "replication";
        case Purpose::kEtcd: return "etcd";
        default: return "other";
    }
}
}  // namespace
ValidationResult ValidateRegulatedMode(const RegulatedModeConfig& cfg) {
    if (!cfg.enabled) return {true, ""};   // R4: off by default → unchanged behavior
    BoundaryGuard guard(BuildPolicy(cfg.allow_rules, /*default_deny=*/true));
    for (const auto& sink : cfg.configured_sinks) {
        Decision d = guard.Check(sink);
        if (!d.allow) {
            return {false, std::string("Regulated Mode: configured ") +
                           PurposeName(sink.purpose) + " sink '" + sink.host +
                           "' is out of boundary: " + d.reason};
        }
    }
    for (const auto& req : cfg.requirements) {
        if (auto err = req()) return {false, "Regulated Mode: unmet requirement: " + *err};
    }
    return {true, ""};
}
}  // namespace kvcache::node::security
```

- [ ] **Step 5: Wire target + run** — add `test_regulated_mode` to `src/tests/unit/CMakeLists.txt` (sources: `regulated_mode.cpp`, `boundary_policy_builder.cpp`, `boundary_guard.cpp`). Run: `./build/tests/unit/test_regulated_mode` — PASS.

- [ ] **Step 6: Commit**

```bash
git add src/kvstore-node/src/security/regulated_mode.h src/kvstore-node/src/security/regulated_mode.cpp \
        src/tests/unit/security/regulated_mode_test.cpp src/tests/unit/CMakeLists.txt
git commit -m "feat(A10): Regulated Mode startup gate — fail-closed sink + requirement validation"
```

---

### Task 5: Wire the deny observer (metric + audit) and integrate; README + full regression

**Files:**
- Create: `src/kvstore-node/src/security/boundary_deny_observer.h` (a small factory that builds a `GuardedHttpTransport::DenyObserver` bound to the metrics counter + audit log)
- Test: `src/tests/unit/security/boundary_deny_observer_test.cpp`
- Modify: `src/tests/unit/CMakeLists.txt`; the node source list if needed; `README.md` (A10 phase-log entry)

**Interfaces:**
- Consumes: Task 3 `GuardedHttpTransport::DenyObserver`; the existing metrics facility and `security/audit.h` `AuditLog`/`AuditRecord`.
- Produces:
  ```cpp
  namespace kvcache::node::security {
  // Build a DenyObserver that (1) increments a boundary-deny counter and
  // (2) appends an audit record. Both sinks are optional (nullptr → skipped)
  // so the observer degrades gracefully and stays unit-testable.
  tier::GuardedHttpTransport::DenyObserver
  MakeBoundaryDenyObserver(std::function<void()> incr_metric, AuditLog* audit);
  }
  ```

- [ ] **Step 1: Ground the real APIs first.** Read `src/kvstore-node/src/security/audit.h` for the exact `AuditRecord` fields + `AuditLog::Append` signature, and grep the obs/metrics module (e.g. `src/kvstore-node/src/obs/` or wherever counters like `kv_*_total` are registered — look at how J-2 / MetricsColdTier register a counter) to learn the counter registration/increment API. The `kv_boundary_denied_total` counter (spec §4) uses that facility. Adapt the observer to the real APIs; the `incr_metric` param above is a thin lambda so the observer TU itself needs no metrics dependency.

- [ ] **Step 2: Write the failing test**

```cpp
// src/tests/unit/security/boundary_deny_observer_test.cpp
#include "security/boundary_deny_observer.h"
#include "security/boundary_guard.h"
#include <gtest/gtest.h>
using namespace kvcache::node::security;

TEST(BoundaryDenyObserver, IncrementsMetricAndToleratesNullAudit) {
    int metric = 0;
    auto obs = MakeBoundaryDenyObserver([&]{ ++metric; }, /*audit=*/nullptr);
    obs(Endpoint{.host = "evil.com", .purpose = Purpose::kColdTier}, "out of boundary");
    EXPECT_EQ(metric, 1) << "deny must bump the counter";
    // null audit must not crash.
}
```
*(If an `AuditLog` is straightforward to construct with a capturing consumer, add a second case asserting an `AuditRecord` with `Decision::Deny` and the reason in `message` is appended. If constructing `AuditLog` in a unit test is heavy, assert only the metric path and document that audit emission is covered by an integration point — do not fake it.)*

- [ ] **Step 3: Run to verify fail** — `cmake --build build --target test_boundary_deny_observer && ./build/tests/unit/test_boundary_deny_observer` — FAIL (header missing).

- [ ] **Step 4: Implement** the header + (if needed) `.cpp`. The observer calls `incr_metric()` if set, and if `audit` is non-null builds an `AuditRecord` (use the real fields from audit.h — set `decision` to the deny value, `message` to the reason, a synthetic system `cn`/`kind` for the boundary subsystem, `action`/`tenant_hash` to the closest neutral values) and calls `audit->Append(rec)`. Keep it minimal.

- [ ] **Step 5: Run to verify pass** — `./build/tests/unit/test_boundary_deny_observer` — PASS.

- [ ] **Step 6: README + full regression.** Add the A10 phase-log entry to `README.md` matching the format prior A-items used (do NOT touch VERSION/badge — release-time only). Then run the full suite: `cd build && cmake --build . -j4 && ctest --output-on-failure` — expect the prior green count + the new A10 tests, 0 failures. Capture the summary line.

- [ ] **Step 7: Commit**

```bash
git add src/kvstore-node/src/security/boundary_deny_observer.h \
        src/tests/unit/security/boundary_deny_observer_test.cpp \
        src/tests/unit/CMakeLists.txt README.md
# add the .cpp too if one was created
git commit -m "feat(A10): boundary deny observer (metric + audit) + README; full-suite green"
```

---

### Deferred / follow-on (NOT in this plan — matches spec §6/§10)

- **FIPS crypto mode** — OpenSSL FIPS provider load + validated-cipher enforcement + startup assertion. Plugs into Task 4 as a `RequirementCheck`.
- **`IKeyProvider` / KMS envelope** — replace plaintext `EncryptingColdTier::Options.key`; KMS endpoint is itself a guarded sink (`Purpose::kKms`). Plugs in as a `RequirementCheck` + a guarded KMS client.
- **Hash-chained tamper-evident audit** — extend `security/audit.cpp`.
- **Region/account-tag rules** — `Rule` gains a tag-match mode resolved from cloud metadata.
- **Gate the other seams at runtime** — OTLP exporter (J-2), A9 `ReplicationConsumer` dial + `ReplicaFetch` (once gRPC lands), etcd/gRPC peer dialer each get a guard check at dial time (Task 4's startup gate already covers them at config-enumeration level; per-dial decorators mirror Task 3's `GuardedHttpTransport`).
- **NIST 800-53 control map** — documentation artifact (spec §7), no code.

---

## Self-Review

**Spec coverage:** §2 R1 (one Regulated Mode profile) → `RegulatedModeConfig`/`ValidateRegulatedMode` (Task 4). R2 (configurable perimeter, empty ⇒ air-gap) → Task 2 `BuildPolicy` + Task 4 air-gap test. R3 (boundary guard centerpiece, fail-closed, startup + runtime) → Task 1 (`Check`), Task 4 (startup gate), Task 3 (runtime per-dial on the cold-tier seam). R4 (off by default, unchanged when disabled) → Task 4 `DisabledIsNoOp`. R5 (NIST map) → deferred (doc artifact). §4 BoundaryGuard type shape (`Endpoint`/`Purpose`/`BoundaryPolicy`/`Decision`/`Check`) → Task 1 verbatim. §4 seams → Task 3 implements the cold-tier seam; the other seams are enumerated in Task 4's startup gate and listed as per-dial follow-ons (matches the spec's "each adds a one-line Check" intent). §4 startup gate (refuse to boot, name offending sink) → Task 4. §6 (FIPS/KMS/audit) → interface-level `RequirementCheck` seam in Task 4; full impls deferred (matches spec "designed to the interface here but built as follow-ons"). §8 fail-closed semantics → Task 1 (empty host, malformed CIDR → deny), Task 3 (unparseable URL → deny), Task 4 (refuse on first deny). §9 testing (all local/unit) → Tasks 1–5 are all local gtest. **Gap:** the deny metric `kv_boundary_denied_total` + audit event (§4) land in Task 5; the audit-append assertion is best-effort pending the real `AuditLog` construction cost — flagged in Task 5 Step 2.

**Placeholder scan:** every code step carries real code; the "ground the real metrics/audit API" notes in Task 5 are grounding instructions (the same pattern the A9 plan used successfully), not TBDs. No "implement later" in a deliverable.

**Type consistency:** `Endpoint{host,port,purpose}`, `Purpose`, `BoundaryPolicy{allow,default_deny}`, `Decision{allow,reason}`, `Rule{host_glob,cidr,purpose}` defined in Task 1 and consumed unchanged by Tasks 2/3/4. `BuildPolicy(vector<string>, bool)` in Task 2 is called by Task 4. `GuardedHttpTransport::DenyObserver = function<void(const Endpoint&, string_view)>` in Task 3 is produced by `MakeBoundaryDenyObserver` in Task 5. `IHttpTransport`/`HttpResult` come verbatim from the existing `tier/rest_cold_tier.h`. `ValidateRegulatedMode`/`RegulatedModeConfig`/`ValidationResult` consistent across Task 4 + its tests.

**Scope honesty:** this plan is the fail-closed BoundaryGuard core + one enforced seam + startup gate — the buildable, unit-testable subset the spec §9 defines. FIPS/KMS/hash-audit/tag-rules/extra-seam-decorators are explicitly deferred with a plug-in seam (`RequirementCheck`, and the `GuardedHttpTransport` decorator pattern) so the follow-on is mechanical.
