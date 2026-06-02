// LLD §4.2 — Cluster-wide bloom-sketch view, 30 s refresh.
//
// Phase A1.4 — the kvagent's cached view of "which tenant prefixes
// exist somewhere in the cluster". Backed by per-tenant
// ``kvcache::node::routing::AggregatedBloom`` objects (the K-7
// publisher format on the wire). The agent polls a user-supplied
// Loader every 30 s; between polls, ``MaybeContains`` answers
// in-process from the cached sketches.
//
// The Loader indirection keeps this module free of an etcd dependency:
// tests pass a lambda; production wires a callback that calls
// ``EtcdClient::Get("/kvcache/bloom/aggregated/<tenant>")``. Same
// shape as the routing_cache: the data structure is the testable
// thing; the puller around it is composable plumbing.
//
// Concurrency: one std::shared_mutex around the tenant→AggregatedBloom
// map. Lookups (MaybeContains) take the shared lock; refresh installs
// new sketches atomically under the unique lock. We hold sketches by
// shared_ptr so a reader can keep using the old one while a refresh
// swap is in flight.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "routing/bloom_sketch.h"  // kvcache::node::routing::AggregatedBloom

namespace kvcache::agent::bloom_view {

// Wire snapshot for one tenant — matches the K-7 publisher payload.
struct TenantSketch {
    std::string                                  tenant_id;
    kvcache::node::routing::BloomParams          params;
    std::vector<uint8_t>                         bits;   // m_bits / 8 bytes
};

// Loader callback. Called from the refresh thread with the list of
// tenants the caller currently cares about (we don't crawl etcd blindly
// — the agent passes the tenants it serves). Returns the latest
// per-tenant sketches; missing entries are dropped from the view.
using Loader = std::function<std::vector<TenantSketch>(
    const std::vector<std::string>& tenants)>;

class BloomView {
   public:
    struct Options {
        std::chrono::milliseconds refresh_interval{30'000};  // 30 s
        Loader                    loader;                    // required
    };

    explicit BloomView(Options opts) : opts_(std::move(opts)) {}
    ~BloomView();

    BloomView(const BloomView&)            = delete;
    BloomView& operator=(const BloomView&) = delete;

    // Register / drop tenant interest. The refresh thread fetches
    // only registered tenants. Thread-safe.
    void RegisterTenant(std::string tenant_id);
    void UnregisterTenant(const std::string& tenant_id);

    // Synchronous refresh — calls Loader once with the current tenant
    // set + installs results. Used by tests and by the periodic loop.
    // Returns the number of sketches installed; -1 on Loader exception.
    int RefreshOnce();

    // Start / stop the periodic refresh loop in a background thread.
    // Calling Start() on a running view is a no-op.
    void Start();
    void Stop();

    // MaybeContains: false = definitely not in the cluster (anywhere);
    // true = maybe — caller falls back to NodeDirectory + HRW. Returns
    // true for unknown tenants too (no sketch ⇒ no evidence-of-absence).
    bool MaybeContains(const std::string& tenant_id,
                        std::span<const uint8_t> key) const;

    // Observability snapshot.
    struct Stats {
        uint64_t   refreshes_ok     = 0;
        uint64_t   refreshes_failed = 0;
        uint64_t   queries          = 0;
        uint64_t   answered_false   = 0;  // proved not-in-cluster
        std::size_t tenants         = 0;
        std::chrono::steady_clock::time_point last_refresh_at;
    };
    Stats SnapshotStats() const;

   private:
    void RefreshLoop();

    Options                                opts_;
    mutable std::shared_mutex              mu_;
    std::vector<std::string>               tenants_;
    std::unordered_map<std::string,
                       std::shared_ptr<kvcache::node::routing::AggregatedBloom>>
                                           sketches_;

    // Background refresher.
    std::atomic<bool>                      running_{false};
    std::thread                            thread_;

    // Stats — held under mu_ on writes; readers take shared lock.
    uint64_t                               refreshes_ok_     = 0;
    uint64_t                               refreshes_failed_ = 0;
    mutable std::atomic<uint64_t>          queries_{0};
    mutable std::atomic<uint64_t>          answered_false_{0};
    std::chrono::steady_clock::time_point  last_refresh_at_{};
};

}  // namespace kvcache::agent::bloom_view
