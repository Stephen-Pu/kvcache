// Phase A1.9 — etcd-backed ViewLoader tests.
//
// Drives the adapter against a real in-process InMemoryEtcdClient
// (faithful etcd v3 semantics) end-to-end through the
// ClusterViewWatcher → HrwResolver chain. Covers: present key →
// nodes installed, absent key → empty set, and a live update (Put a
// new view, re-refresh, resolver follows).
#include "router/etcd_view_loader.h"

#include <gtest/gtest.h>

#include <string>

#include "cluster/etcd_client.h"
#include "router/cluster_view.h"
#include "router/hrw_resolver.h"

using kvcache::agent::router::ClusterViewWatcher;
using kvcache::agent::router::HrwResolver;
using kvcache::agent::router::MakeEtcdViewLoader;
using kvcache::agent::router::kClusterViewKey;
using kvcache::node::cluster::InMemoryEtcdClient;

static const char* kViewJson = R"({
  "epoch": 3,
  "leader_id": "cp-1",
  "nodes": [
    {"node_id": "node-a", "host": "10.0.0.1"},
    {"node_id": "node-b", "host": "10.0.0.2"}
  ]
})";

TEST(EtcdViewLoaderTest, ReadsViewFromEtcdIntoResolver) {
    InMemoryEtcdClient etcd;
    std::string err;
    ASSERT_TRUE(etcd.Put(kClusterViewKey, kViewJson, kvcache::node::cluster::kNoLease,
                         /*out_rev=*/nullptr, &err))
        << err;

    HrwResolver hrw;
    ClusterViewWatcher::Options opts;
    opts.loader = MakeEtcdViewLoader(etcd);
    ClusterViewWatcher w(hrw, opts);

    EXPECT_EQ(w.RefreshOnce(), 2);
    EXPECT_EQ(hrw.NodeCount(), 2u);
}

TEST(EtcdViewLoaderTest, AbsentKeyInstallsEmptySet) {
    InMemoryEtcdClient etcd;  // nothing written
    HrwResolver hrw;
    ClusterViewWatcher::Options opts;
    opts.loader = MakeEtcdViewLoader(etcd);
    ClusterViewWatcher w(hrw, opts);

    // Absent key → "" → empty node set (0 installed), NOT a -1 error.
    EXPECT_EQ(w.RefreshOnce(), 0);
    EXPECT_EQ(hrw.NodeCount(), 0u);
    EXPECT_EQ(w.SnapshotStats().refreshes_ok, 1u);
}

TEST(EtcdViewLoaderTest, LiveUpdateFollowedOnRefresh) {
    InMemoryEtcdClient etcd;
    std::string err;
    ASSERT_TRUE(etcd.Put(kClusterViewKey, kViewJson, kvcache::node::cluster::kNoLease,
                         nullptr, &err)) << err;

    HrwResolver hrw;
    ClusterViewWatcher::Options opts;
    opts.loader = MakeEtcdViewLoader(etcd);
    ClusterViewWatcher w(hrw, opts);
    ASSERT_EQ(w.RefreshOnce(), 2);

    // A node joins — leader rewrites the view.
    const char* kGrew = R"({"nodes":[
        {"node_id":"node-a"},{"node_id":"node-b"},{"node_id":"node-c"}]})";
    ASSERT_TRUE(etcd.Put(kClusterViewKey, kGrew, kvcache::node::cluster::kNoLease,
                         nullptr, &err)) << err;
    EXPECT_EQ(w.RefreshOnce(), 3);
    EXPECT_EQ(hrw.NodeCount(), 3u);
}
