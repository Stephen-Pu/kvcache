// LLD §5.1 — Three-dimensional per-tenant quota.
//
// Dimensions:
//   * capacity_bytes — total cached bytes the tenant may hold.
//   * qps           — request-rate limit, computed over a 1-second window.
//   * bandwidth_bps — bytes-per-second egress / ingress over a 1-second window.
//
// Reserve never blocks: callers either succeed, get a quota error, or
// escalate via priority-based eviction (LLD §5.1).
//
// When CP is unreachable for > 1 h, set `inflated=true` to absorb
// authoritative-quota gaps via a 1.5× multiplier (LLD §4.1).
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace kvcache::node::qos {

struct QuotaLimits {
    uint64_t capacity_bytes = 0;
    uint32_t qps            = 0;
    uint64_t bandwidth_bps  = 0;
    bool     inflated       = false;
};

struct ReserveRequest {
    uint64_t bytes            = 0;
    uint32_t requests         = 1;
    uint64_t bandwidth_bytes  = 0;
};

enum class QuotaResult {
    kOk,
    kCapacityExceeded,
    kQpsExceeded,
    kBandwidthExceeded,
    kUnknownTenant,
};

class QuotaManager {
   public:
    struct Options {
        // Use milliseconds so tests can drive a sub-second window.
        std::chrono::milliseconds window{1000};
    };

    QuotaManager();  // uses default Options{}
    explicit QuotaManager(const Options& opts);

    void        SetLimits  (uint64_t tenant_hash, const QuotaLimits& limits);
    bool        RemoveTenant(uint64_t tenant_hash);
    QuotaLimits GetLimits  (uint64_t tenant_hash) const;

    QuotaResult Reserve(uint64_t tenant_hash, const ReserveRequest& req);
    void        Release(uint64_t tenant_hash, uint64_t bytes);

    uint64_t CapacityUsed   (uint64_t tenant_hash) const;
    uint32_t QpsWindow      (uint64_t tenant_hash) const;
    uint64_t BandwidthWindow(uint64_t tenant_hash) const;

   private:
    struct Counter {
        QuotaLimits             limits;
        std::atomic<uint64_t>   capacity_used{0};
        std::mutex              wmu;
        std::chrono::steady_clock::time_point window_start;
        uint32_t                qps_count = 0;
        uint64_t                bw_bytes  = 0;
    };

    void RollWindowLocked(Counter& c, std::chrono::steady_clock::time_point now);
    static double Inflation(const QuotaLimits& l) { return l.inflated ? 1.5 : 1.0; }

    mutable std::mutex                                       tenants_mu_;
    std::unordered_map<uint64_t, std::unique_ptr<Counter>>   tenants_;
    std::chrono::milliseconds                                window_;
};

}  // namespace kvcache::node::qos
