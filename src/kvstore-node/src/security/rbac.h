// LLD §5.2 — 3-role RBAC + 6-step permission check + decision cache.
//
// Roles:
//   * kAdmin    — operator-level; can read/write tenant configs, cluster admin.
//   * kTenant   — per-tenant principal; can act on own tenant's data only.
//   * kInternal — internal component principal (CP, kvagent); broad data-plane
//                 rights but no tenant-config writes.
//
// Six-step permission check (LLD §5.2):
//   1. Identity present.
//   2. Identity kind matches one of the rule's allowed kinds.
//   3. Action permitted for the role.
//   4. Resource scope (tenant_id) matches identity's tenant.
//   5. Tenant not in `deletion_pending` state.
//   6. Decision is cached for 1 s under (identity, action, tenant) key.
//
// The cache (step 6) is the reason this module exists on the hot path — the
// LLD §D-PERF-2 budget is ≤1µs per call. The first call does the full 6 steps;
// subsequent calls within the TTL hit the cache.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include "security/mtls.h"

namespace kvcache::node::security {

enum class Action : uint8_t {
    kLookup       = 1,
    kReserve      = 2,
    kPublish      = 3,
    kFetch        = 4,
    kSeal         = 5,
    kRelease      = 6,
    kAdminRead    = 7,
    kAdminWrite   = 8,
    kClusterAdmin = 9,
};

const char* ActionName(Action a);

enum class Decision : uint8_t {
    kAllow = 0,
    kDeny  = 1,
};

class Rbac {
   public:
    struct Options {
        std::chrono::milliseconds cache_ttl{1000};
    };

    Rbac();
    explicit Rbac(const Options& opts);

    // Step 1-5: full evaluation. Step 6: cached for `cache_ttl` per
    // (cn, action, tenant_hash) triple.
    Decision Check(const Identity& peer,
                   Action          action,
                   uint64_t        target_tenant_hash,
                   bool            tenant_deletion_pending);

    // Stats — for metrics export and audit.
    uint64_t CacheHits() const noexcept {
        return cache_hits_.load(std::memory_order_relaxed);
    }
    uint64_t CacheMisses() const noexcept {
        return cache_misses_.load(std::memory_order_relaxed);
    }
    uint64_t Denials() const noexcept {
        return denials_.load(std::memory_order_relaxed);
    }

   private:
    Decision Evaluate(const Identity& peer, Action a, uint64_t target_tenant,
                      bool deletion_pending) const;

    struct CacheKey {
        std::string cn;
        Action      action;
        uint64_t    tenant;
        bool operator==(const CacheKey& o) const noexcept {
            return action == o.action && tenant == o.tenant && cn == o.cn;
        }
    };
    struct CacheKeyHash {
        std::size_t operator()(const CacheKey& k) const noexcept {
            std::size_t h = std::hash<std::string>{}(k.cn);
            h ^= std::hash<uint64_t>{}(k.tenant) + (h << 6) + (h >> 2);
            h ^= static_cast<std::size_t>(k.action) * 0x9e3779b97f4a7c15ULL;
            return h;
        }
    };
    struct CacheEntry {
        Decision                              decision;
        std::chrono::steady_clock::time_point expiry;
    };

    mutable std::mutex                                              mu_;
    std::unordered_map<CacheKey, CacheEntry, CacheKeyHash>          cache_;
    std::chrono::milliseconds                                       ttl_;
    std::atomic<uint64_t> cache_hits_{0};
    std::atomic<uint64_t> cache_misses_{0};
    std::atomic<uint64_t> denials_{0};
};

}  // namespace kvcache::node::security
