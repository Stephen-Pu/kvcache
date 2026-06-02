// LLD §4.1 / §4.2 — kvagent cluster-view watcher.
//
// Phase A1.8 — keeps the HrwResolver's node set fresh from the CP's
// published cluster view. The control-plane leader writes a JSON
// snapshot to etcd `/kvcache/cluster/view` (Phase K-2 ViewPublisher)
// on every membership change:
//
//   { "epoch": N, "leader_id": "...", "published_at_ns": ...,
//     "nodes": [ { "node_id": "...", "host": "...", ... }, ... ] }
//
// This watcher polls a Loader callback (etcd-free indirection, same
// pattern as BloomView) on a tick, parses the node_ids out of the
// snapshot, and calls HrwResolver::SetNodes. Between polls the
// resolver answers from the last-installed set.
//
// The Loader returns the raw JSON bytes of the view key (or an empty
// string when the key is absent — no leader / fresh cluster). Tests
// pass a lambda returning a canned snapshot; production wires a
// callback that does EtcdClient::Get(ViewKey).
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "router/hrw_resolver.h"

namespace kvcache::agent::router {

// The etcd key the CP publishes to — must match
// control-plane/internal/membership/view.go ViewKey.
inline constexpr const char* kClusterViewKey = "/kvcache/cluster/view";

// Loader: returns the raw JSON body at the view key, or "" if absent.
// nullopt signals a load error (etcd unreachable) — distinct from ""
// (key genuinely absent), so the watcher can keep the last-good set
// on a transient error instead of blanking the resolver.
using ViewLoader = std::function<std::optional<std::string>()>;

class ClusterViewWatcher {
   public:
    struct Options {
        std::chrono::milliseconds refresh_interval{30'000};  // 30 s
        ViewLoader loader;  // required
    };

    // The watcher updates `resolver` in place; `resolver` must outlive
    // the watcher (main.cpp owns both for the process lifetime).
    ClusterViewWatcher(HrwResolver& resolver, Options opts)
        : resolver_(resolver), opts_(std::move(opts)) {}
    ~ClusterViewWatcher();

    ClusterViewWatcher(const ClusterViewWatcher&)            = delete;
    ClusterViewWatcher& operator=(const ClusterViewWatcher&) = delete;

    // Parse a ClusterView JSON body → the list of node_ids (in the
    // order the snapshot listed them). Returns nullopt on malformed
    // JSON or a missing/!array "nodes" field. An empty "nodes" array
    // parses to an empty vector (a real "cluster has no nodes" state),
    // distinct from a parse failure. This is the testable unit.
    static std::optional<std::vector<std::string>> ParseNodeIds(
        const std::string& json_body);

    // Synchronous refresh: call Loader once, parse, apply to resolver.
    // Returns the node count installed, or -1 on load/parse error (in
    // which case the resolver's previous set is left untouched).
    int RefreshOnce();

    void Start();
    void Stop();

    struct Stats {
        uint64_t refreshes_ok     = 0;
        uint64_t refreshes_failed = 0;
        std::size_t last_node_count = 0;
    };
    Stats SnapshotStats() const;

   private:
    void RefreshLoop();

    HrwResolver&       resolver_;
    Options            opts_;
    std::atomic<bool>  running_{false};
    std::thread        thread_;

    mutable std::atomic<uint64_t> refreshes_ok_{0};
    mutable std::atomic<uint64_t> refreshes_failed_{0};
    std::atomic<std::size_t>      last_node_count_{0};
};

}  // namespace kvcache::agent::router
