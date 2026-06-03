// Phase A1.9 — etcd-backed ViewLoader tests.
//
// Drives the adapter against a real in-process InMemoryEtcdClient
// (faithful etcd v3 semantics) end-to-end through the
// ClusterViewWatcher → HrwResolver chain. Covers: present key →
// nodes installed, absent key → empty set, and a live update (Put a
// new view, re-refresh, resolver follows).
#include "router/etcd_view_loader.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

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

// Phase A1.11 — Watch subscription: a Put on the view key must fire the
// on_change callback (which in production calls watcher.RefreshOnce),
// so view changes propagate without waiting for the poll tick. Driven
// against the real in-process InMemoryEtcdClient.
TEST(EtcdViewSubscriptionTest, FiresOnChangeOnViewPut) {
    using kvcache::agent::router::EtcdViewSubscription;
    using kvcache::agent::router::kClusterViewKey;

    InMemoryEtcdClient etcd;
    std::atomic<int> fired{0};
    EtcdViewSubscription sub(etcd, [&] { fired.fetch_add(1, std::memory_order_relaxed); });
    sub.Start();

    std::string err;
    ASSERT_TRUE(etcd.Put(kClusterViewKey, kViewJson,
                         kvcache::node::cluster::kNoLease, nullptr, &err)) << err;

    // The watch callback runs on the etcd dispatch thread — poll for it.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (fired.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_GE(fired.load(), 1) << "view Put should fire the subscription";

    sub.Stop();
    // After Stop, a further Put must not fire.
    const int before = fired.load();
    ASSERT_TRUE(etcd.Put(kClusterViewKey, kViewJson,
                         kvcache::node::cluster::kNoLease, nullptr, &err)) << err;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(fired.load(), before) << "stopped subscription must not fire";
}

TEST(EtcdViewSubscriptionTest, EndToEndWatchUpdatesResolver) {
    using kvcache::agent::router::EtcdViewSubscription;
    using kvcache::agent::router::kClusterViewKey;

    InMemoryEtcdClient etcd;
    HrwResolver hrw;
    ClusterViewWatcher::Options opts;
    opts.loader = MakeEtcdViewLoader(etcd);
    ClusterViewWatcher w(hrw, opts);
    // The watcher's loop thread services RequestRefresh. The subscription
    // callback calls RequestRefresh (NOT RefreshOnce) — it must not
    // re-enter etcd.Get from the watch-dispatch thread, which holds the
    // InMemoryEtcdClient mutex and would deadlock. RequestRefresh only
    // signals; the loop thread does the Get.
    w.Start();
    EtcdViewSubscription sub(etcd, [&] { w.RequestRefresh(); });
    sub.Start();

    std::string err;
    ASSERT_TRUE(etcd.Put(kClusterViewKey, kViewJson,
                         kvcache::node::cluster::kNoLease, nullptr, &err)) << err;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (hrw.NodeCount() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(hrw.NodeCount(), 2u) << "watch-driven refresh should populate the resolver";
    sub.Stop();
    w.Stop();
}

// Phase A1.10 — the REAL cross-process client (HttpEtcdClient) is now
// linked into the agent. Against an unreachable endpoint its Create
// smoke-test fails fast, which is exactly the startup path main.cpp
// guards (etcd down → fall back to file/seed). Port 1 refuses
// connection ~instantly; a short dial_timeout caps the worst case.
TEST(EtcdViewLoaderTest, HttpClientCreateFailsOnUnreachableEndpoint) {
    kvcache::node::cluster::HttpEtcdClient::Options eo;
    eo.endpoint     = "http://127.0.0.1:1";  // nothing listens here
    eo.dial_timeout = std::chrono::milliseconds(500);
    std::string err;
    auto client = kvcache::node::cluster::HttpEtcdClient::Create(eo, &err);
    EXPECT_EQ(client, nullptr) << "Create must fail against a dead endpoint";
    EXPECT_FALSE(err.empty()) << "failure must carry a diagnostic";
}
