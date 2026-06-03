// LLD §4.1 — etcd-backed ViewLoader for the ClusterViewWatcher.
//
// Phase A1.9 — closes the A1.8 gap: instead of reading the cluster view
// from a file a sidecar refreshes, the agent reads it straight from
// etcd via an IEtcdClient. MakeEtcdViewLoader adapts any IEtcdClient
// (InMemoryEtcdClient for the in-process demo + tests; GrpcEtcdClient
// for production, once the agent links the etcd proto + grpc) into the
// ClusterViewWatcher's ViewLoader callback.
//
// The loader maps etcd's Get semantics onto the ViewLoader contract:
//   * Get error (etcd unreachable)      → nullopt (watcher keeps
//                                          last-good node set)
//   * key absent (no leader / fresh)    → "" (watcher installs empty set)
//   * key present                       → the JSON body
//
// This keeps the watcher's keep-last-good-on-error behaviour intact
// across a flaky etcd connection.
#pragma once

#include <functional>
#include <string>

#include "cluster/etcd_client.h"   // kvcache::node::cluster::IEtcdClient
#include "router/cluster_view.h"   // ViewLoader, kClusterViewKey

namespace kvcache::agent::router {

// Build a ViewLoader that reads `key` from `etcd` on each call. `etcd`
// must outlive the returned callback (and thus the watcher) — main.cpp
// owns both for the process lifetime.
ViewLoader MakeEtcdViewLoader(kvcache::node::cluster::IEtcdClient& etcd,
                              std::string key = kClusterViewKey);

// Phase A1.11 — Watch-driven refresh. The A1.8 ClusterViewWatcher polls
// the view on a 30s tick; that bounds how stale routing can be after a
// node joins/drains. EtcdViewSubscription registers an etcd WatchPrefix
// on the view key and fires `on_change` on every Put/Delete, so the
// watcher's RefreshOnce runs within milliseconds of a CP publish. The
// poll stays as a safety net (missed events, watch reconnect gaps).
//
// `etcd` must outlive the subscription. Construct, then Start();
// Stop() (or the dtor) unsubscribes. The on_change callback runs on the
// etcd client's dispatch thread — keep it light (a RefreshOnce that
// itself does the etcd Get is fine).
class EtcdViewSubscription {
   public:
    EtcdViewSubscription(kvcache::node::cluster::IEtcdClient& etcd,
                         std::function<void()> on_change,
                         std::string key = kClusterViewKey);
    ~EtcdViewSubscription();

    EtcdViewSubscription(const EtcdViewSubscription&)            = delete;
    EtcdViewSubscription& operator=(const EtcdViewSubscription&) = delete;

    void Start();
    void Stop();

   private:
    kvcache::node::cluster::IEtcdClient&  etcd_;
    std::function<void()>                 on_change_;
    std::string                           key_;
    kvcache::node::cluster::WatchHandle   handle_ = 0;
    bool                                  watching_ = false;
};

}  // namespace kvcache::agent::router
