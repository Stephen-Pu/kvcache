// LLD §4.2 — LRU prefix → primary-node cache with TTL.
#include "routing_cache/routing_cache.h"

#include <utility>

namespace kvcache::agent::routing_cache {

void RoutingCache::Put(const Key& k, std::string node_id) {
    const auto now = std::chrono::steady_clock::now();
    const auto exp = now + opts_.ttl;

    std::unique_lock lk(mu_);
    ++puts_;
    auto it = idx_.find(k);
    if (it != idx_.end()) {
        // Update in place + promote to MRU.
        it->second->node_id    = std::move(node_id);
        it->second->expires_at = exp;
        lru_.splice(lru_.begin(), lru_, it->second);
        return;
    }
    // Insert at MRU.
    lru_.push_front(Entry{k, std::move(node_id), exp});
    idx_.emplace(k, lru_.begin());
    // Evict LRU until under capacity.
    while (idx_.size() > opts_.capacity) {
        const auto& victim = lru_.back();
        idx_.erase(victim.key);
        lru_.pop_back();
        ++evictions_;
    }
}

std::optional<std::string> RoutingCache::Get(const Key& k) {
    const auto now = std::chrono::steady_clock::now();
    std::unique_lock lk(mu_);  // promote requires write lock
    auto it = idx_.find(k);
    if (it == idx_.end()) {
        ++misses_;
        return std::nullopt;
    }
    if (it->second->expires_at < now) {
        // Expired — drop + miss.
        lru_.erase(it->second);
        idx_.erase(it);
        ++expirations_;
        ++misses_;
        return std::nullopt;
    }
    // Hit — promote to MRU and return.
    ++hits_;
    lru_.splice(lru_.begin(), lru_, it->second);
    return it->second->node_id;
}

void RoutingCache::Invalidate(const Key& k) {
    std::unique_lock lk(mu_);
    auto it = idx_.find(k);
    if (it == idx_.end()) return;
    lru_.erase(it->second);
    idx_.erase(it);
}

void RoutingCache::Clear() {
    std::unique_lock lk(mu_);
    idx_.clear();
    lru_.clear();
}

std::size_t RoutingCache::size() const {
    std::shared_lock lk(mu_);
    return idx_.size();
}

RoutingCache::Stats RoutingCache::SnapshotStats() const {
    std::shared_lock lk(mu_);
    return Stats{
        .puts        = puts_,
        .hits        = hits_,
        .misses      = misses_,
        .expirations = expirations_,
        .evictions   = evictions_,
        .size        = idx_.size(),
    };
}

}  // namespace kvcache::agent::routing_cache
