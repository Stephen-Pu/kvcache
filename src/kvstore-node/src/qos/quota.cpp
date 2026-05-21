// LLD §5.1 — QuotaManager implementation.
#include "qos/quota.h"

namespace kvcache::node::qos {

QuotaManager::QuotaManager() : QuotaManager(Options{}) {}
QuotaManager::QuotaManager(const Options& opts) : window_(opts.window) {}

void QuotaManager::SetLimits(uint64_t tenant_hash, const QuotaLimits& limits) {
    std::lock_guard lk(tenants_mu_);
    auto it = tenants_.find(tenant_hash);
    if (it == tenants_.end()) {
        auto c = std::make_unique<Counter>();
        c->limits = limits;
        c->window_start = std::chrono::steady_clock::now();
        tenants_.emplace(tenant_hash, std::move(c));
    } else {
        it->second->limits = limits;
    }
}

bool QuotaManager::RemoveTenant(uint64_t tenant_hash) {
    std::lock_guard lk(tenants_mu_);
    return tenants_.erase(tenant_hash) > 0;
}

QuotaLimits QuotaManager::GetLimits(uint64_t tenant_hash) const {
    std::lock_guard lk(tenants_mu_);
    auto it = tenants_.find(tenant_hash);
    if (it == tenants_.end()) return {};
    return it->second->limits;
}

void QuotaManager::RollWindowLocked(Counter& c,
                                     std::chrono::steady_clock::time_point now) {
    if (now - c.window_start >= window_) {
        c.window_start = now;
        c.qps_count = 0;
        c.bw_bytes  = 0;
    }
}

QuotaResult QuotaManager::Reserve(uint64_t tenant_hash, const ReserveRequest& req) {
    Counter* c = nullptr;
    {
        std::lock_guard lk(tenants_mu_);
        auto it = tenants_.find(tenant_hash);
        if (it == tenants_.end()) return QuotaResult::kUnknownTenant;
        c = it->second.get();
    }
    const auto inflation = Inflation(c->limits);

    // Capacity dimension (sticky, no window).
    const uint64_t cap_limit =
        static_cast<uint64_t>(c->limits.capacity_bytes * inflation);
    if (cap_limit > 0) {
        uint64_t cur = c->capacity_used.load(std::memory_order_acquire);
        bool ok = false;
        while (cur + req.bytes <= cap_limit) {
            if (c->capacity_used.compare_exchange_weak(cur, cur + req.bytes,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                ok = true;
                break;
            }
        }
        if (!ok) return QuotaResult::kCapacityExceeded;
    } else if (req.bytes > 0) {
        return QuotaResult::kCapacityExceeded;
    }

    // QPS + bandwidth dimensions (windowed).
    {
        std::lock_guard wlk(c->wmu);
        RollWindowLocked(*c, std::chrono::steady_clock::now());
        const uint32_t qps_limit = static_cast<uint32_t>(c->limits.qps * inflation);
        const uint64_t bw_limit  = static_cast<uint64_t>(c->limits.bandwidth_bps * inflation);

        if (qps_limit > 0 && c->qps_count + req.requests > qps_limit) {
            c->capacity_used.fetch_sub(req.bytes, std::memory_order_acq_rel);
            return QuotaResult::kQpsExceeded;
        }
        if (bw_limit > 0 && c->bw_bytes + req.bandwidth_bytes > bw_limit) {
            c->capacity_used.fetch_sub(req.bytes, std::memory_order_acq_rel);
            return QuotaResult::kBandwidthExceeded;
        }
        c->qps_count += req.requests;
        c->bw_bytes  += req.bandwidth_bytes;
    }
    return QuotaResult::kOk;
}

void QuotaManager::Release(uint64_t tenant_hash, uint64_t bytes) {
    std::lock_guard lk(tenants_mu_);
    auto it = tenants_.find(tenant_hash);
    if (it == tenants_.end()) return;
    uint64_t cur = it->second->capacity_used.load(std::memory_order_acquire);
    const uint64_t sub = (bytes > cur) ? cur : bytes;
    it->second->capacity_used.fetch_sub(sub, std::memory_order_acq_rel);
}

uint64_t QuotaManager::CapacityUsed(uint64_t tenant_hash) const {
    std::lock_guard lk(tenants_mu_);
    auto it = tenants_.find(tenant_hash);
    if (it == tenants_.end()) return 0;
    return it->second->capacity_used.load(std::memory_order_acquire);
}

uint32_t QuotaManager::QpsWindow(uint64_t tenant_hash) const {
    std::lock_guard lk(tenants_mu_);
    auto it = tenants_.find(tenant_hash);
    if (it == tenants_.end()) return 0;
    std::lock_guard wlk(it->second->wmu);
    return it->second->qps_count;
}

uint64_t QuotaManager::BandwidthWindow(uint64_t tenant_hash) const {
    std::lock_guard lk(tenants_mu_);
    auto it = tenants_.find(tenant_hash);
    if (it == tenants_.end()) return 0;
    std::lock_guard wlk(it->second->wmu);
    return it->second->bw_bytes;
}

}  // namespace kvcache::node::qos
