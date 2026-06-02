// LLD §4.2 — BloomView: tenant→AggregatedBloom cache + 30 s refresher.
#include "bloom_view/bloom_view.h"

#include <algorithm>
#include <utility>

namespace kvcache::agent::bloom_view {

void BloomView::RegisterTenant(std::string tenant_id) {
    std::unique_lock lk(mu_);
    if (std::find(tenants_.begin(), tenants_.end(), tenant_id) == tenants_.end()) {
        tenants_.push_back(std::move(tenant_id));
    }
}

void BloomView::UnregisterTenant(const std::string& tenant_id) {
    std::unique_lock lk(mu_);
    auto it = std::find(tenants_.begin(), tenants_.end(), tenant_id);
    if (it != tenants_.end()) tenants_.erase(it);
    sketches_.erase(tenant_id);
}

int BloomView::RefreshOnce() {
    // Snapshot the tenant set under the lock — Loader is slow (etcd).
    std::vector<std::string> snapshot;
    {
        std::shared_lock lk(mu_);
        snapshot = tenants_;
    }
    std::vector<TenantSketch> fresh;
    try {
        fresh = opts_.loader(snapshot);
    } catch (...) {
        std::unique_lock lk(mu_);
        ++refreshes_failed_;
        return -1;
    }

    // Build new sketches outside the lock — bit-vector copies can be MBs.
    std::unordered_map<std::string,
                       std::shared_ptr<kvcache::node::routing::AggregatedBloom>>
        next;
    for (auto& ts : fresh) {
        auto ab = std::make_shared<kvcache::node::routing::AggregatedBloom>(ts.params);
        // AggregatedBloom::Set takes ownership of the byte buffer.
        ab->Set(ts.params, std::move(ts.bits));
        next.emplace(ts.tenant_id, std::move(ab));
    }

    std::unique_lock lk(mu_);
    sketches_      = std::move(next);
    ++refreshes_ok_;
    last_refresh_at_ = std::chrono::steady_clock::now();
    return static_cast<int>(sketches_.size());
}

void BloomView::Start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;
    thread_ = std::thread([this] { RefreshLoop(); });
}

void BloomView::Stop() {
    if (!running_.exchange(false)) return;
    if (thread_.joinable()) thread_.join();
}

BloomView::~BloomView() { Stop(); }

void BloomView::RefreshLoop() {
    // Eager first refresh so the view has data before the first
    // 30 s tick elapses.
    (void)RefreshOnce();
    while (running_.load(std::memory_order_relaxed)) {
        // Sleep in small slices so Stop() takes effect quickly.
        const auto deadline = std::chrono::steady_clock::now() + opts_.refresh_interval;
        while (running_.load(std::memory_order_relaxed)
               && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!running_.load(std::memory_order_relaxed)) break;
        (void)RefreshOnce();
    }
}

bool BloomView::MaybeContains(const std::string& tenant_id,
                                std::span<const uint8_t> key) const {
    queries_.fetch_add(1, std::memory_order_relaxed);
    std::shared_ptr<kvcache::node::routing::AggregatedBloom> ab;
    {
        std::shared_lock lk(mu_);
        auto it = sketches_.find(tenant_id);
        if (it == sketches_.end()) {
            // No evidence-of-absence — be permissive. Caller's slow path
            // will resolve via NodeDirectory + HRW.
            return true;
        }
        ab = it->second;  // shared_ptr copy pins lifetime across the unlock.
    }
    if (ab->MaybeContains(key)) return true;
    answered_false_.fetch_add(1, std::memory_order_relaxed);
    return false;
}

BloomView::Stats BloomView::SnapshotStats() const {
    std::shared_lock lk(mu_);
    return Stats{
        .refreshes_ok     = refreshes_ok_,
        .refreshes_failed = refreshes_failed_,
        .queries          = queries_.load(std::memory_order_relaxed),
        .answered_false   = answered_false_.load(std::memory_order_relaxed),
        .tenants          = sketches_.size(),
        .last_refresh_at  = last_refresh_at_,
    };
}

}  // namespace kvcache::agent::bloom_view
