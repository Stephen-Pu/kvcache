// LLD §4.2 — Local per-prefix → primary-node cache.
//
// Phase A1.3 — the kvagent's local hot map: "for this (tenant, prefix
// fingerprint) which kvstore-node was the primary last time I asked?"
// Lets a Lookup short-circuit the full HRW + node-directory walk on
// repeat hits. Entries expire after a TTL (default 30s) so a quiet
// node leaving the cluster doesn't keep showing up forever; eviction
// is LRU once capacity is hit so a hot tenant doesn't starve a cold
// one of map slots.
//
// Concurrency: single shared_mutex around the LRU list + map. The
// hot path is Lookup (read-then-promote); we take a write lock to
// move the entry to MRU. A future cut can split the locks (Folly-
// style striped maps) but at ~1µs per Lookup that's premature.
//
// The cache is **lossy on purpose** — a stale entry triggers a retry
// through the slow path, not a hard error. Operators read
// ``cache_hits / (cache_hits + cache_misses)`` to spot churn.
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace kvcache::agent::routing_cache {

// Cache key: (tenant_id, prefix-fingerprint). The prefix fingerprint
// is whatever the caller's locator hashing produces — we hold it as
// opaque bytes so the cache doesn't have to know about BLAKE3 etc.
struct Key {
    std::string tenant_id;
    std::string prefix;  // opaque bytes; caller hashes if too long

    bool operator==(const Key& o) const noexcept {
        return tenant_id == o.tenant_id && prefix == o.prefix;
    }
};

struct KeyHash {
    std::size_t operator()(const Key& k) const noexcept {
        // FNV-1a over tenant || 0x1F || prefix. Good enough for the
        // hot path; collisions just degrade to a map probe.
        std::size_t h = 1469598103934665603ull;
        for (unsigned char c : k.tenant_id) {
            h ^= c; h *= 1099511628211ull;
        }
        h ^= 0x1F; h *= 1099511628211ull;
        for (unsigned char c : k.prefix) {
            h ^= c; h *= 1099511628211ull;
        }
        return h;
    }
};

class RoutingCache {
   public:
    struct Options {
        std::size_t capacity     = 4096;
        std::chrono::milliseconds ttl{30'000};  // 30 s default
    };

    RoutingCache() : RoutingCache(Options{}) {}
    explicit RoutingCache(Options opts) : opts_(std::move(opts)) {}

    // Insert (or update) a (key → node_id) mapping with an expiry
    // ``now + ttl``. Evicts the LRU entry when capacity is hit. The
    // node_id is whatever the caller resolved — opaque.
    void Put(const Key& k, std::string node_id);

    // Lookup. Returns the node_id on a fresh hit; std::nullopt on
    // miss OR expired. Promotes to MRU on hit (so eviction is real-LRU,
    // not insertion-FIFO).
    std::optional<std::string> Get(const Key& k);

    // Drop the entry for `k` (no-op if missing). Operators use this
    // when membership.Watch fires a delete for the previously-cached
    // primary; the next Lookup re-resolves through the slow path.
    void Invalidate(const Key& k);

    // Drop everything. Used on agent reload / large membership churn.
    void Clear();

    // Observability snapshot — atomic per-counter reads, mutex-free.
    struct Stats {
        uint64_t puts        = 0;
        uint64_t hits        = 0;
        uint64_t misses      = 0;
        uint64_t expirations = 0;
        uint64_t evictions   = 0;
        std::size_t size     = 0;
    };
    Stats SnapshotStats() const;

    std::size_t size() const;

   private:
    struct Entry {
        Key                       key;
        std::string               node_id;
        std::chrono::steady_clock::time_point expires_at;
    };

    Options opts_;
    mutable std::shared_mutex mu_;
    std::list<Entry>          lru_;  // front = MRU, back = LRU
    std::unordered_map<Key, std::list<Entry>::iterator, KeyHash> idx_;

    // Stats — bumped under mu_ to stay in lock-step with the entries.
    uint64_t puts_        = 0;
    uint64_t hits_        = 0;
    uint64_t misses_      = 0;
    uint64_t expirations_ = 0;
    uint64_t evictions_   = 0;
};

}  // namespace kvcache::agent::routing_cache
