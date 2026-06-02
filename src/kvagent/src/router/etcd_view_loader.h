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

#include <string>

#include "cluster/etcd_client.h"   // kvcache::node::cluster::IEtcdClient
#include "router/cluster_view.h"   // ViewLoader, kClusterViewKey

namespace kvcache::agent::router {

// Build a ViewLoader that reads `key` from `etcd` on each call. `etcd`
// must outlive the returned callback (and thus the watcher) — main.cpp
// owns both for the process lifetime.
ViewLoader MakeEtcdViewLoader(kvcache::node::cluster::IEtcdClient& etcd,
                              std::string key = kClusterViewKey);

}  // namespace kvcache::agent::router
