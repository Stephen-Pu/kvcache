// Phase Q-1 — NodeRegistrar + NodeDirectory tests against the
// in-memory etcd implementation.
//
// These tests verify the cluster-membership wiring end-to-end without
// touching the network: a registrar PUTs a leased key, a directory
// Watching the same prefix observes the upsert and refreshes the
// HrwRing; lease revoke / Stop() removes the entry; HRW Primary
// converges on the surviving node.
#include "cluster/etcd_client.h"
#include "cluster/node_directory.h"
#include "cluster/node_registrar.h"
#include "routing/hrw.h"

#include <gtest/gtest.h>
#include <chrono>
#include <string>
#include <thread>

using namespace kvcache::node::cluster;
using kvcache::node::routing::HrwRing;

namespace {

bool WaitFor(std::function<bool()> pred,
             std::chrono::milliseconds budget = std::chrono::seconds(2)) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

}  // namespace

// ---------------------------------------------------------------------------
// Value codec
// ---------------------------------------------------------------------------

TEST(NodeRegistrarCodec, RoundTrip) {
    const std::string encoded =
        EncodeNodeValue("node-a", "10.0.0.7", 7777);
    std::string host;
    uint16_t    port = 0;
    ASSERT_TRUE(DecodeNodeValue(encoded, &host, &port));
    EXPECT_EQ(host, "10.0.0.7");
    EXPECT_EQ(port, 7777u);
}

TEST(NodeRegistrarCodec, RejectsMalformed) {
    std::string host;
    uint16_t    port = 0;
    EXPECT_FALSE(DecodeNodeValue("", &host, &port));
    EXPECT_FALSE(DecodeNodeValue(R"({"node_id":"x"})", &host, &port));
    EXPECT_FALSE(DecodeNodeValue(R"({"host":"h","grpc_port":99999})",
                                  &host, &port));
}

// ---------------------------------------------------------------------------
// Registrar
// ---------------------------------------------------------------------------

TEST(NodeRegistrarTest, StartPutsLeasedKey) {
    InMemoryEtcdClient etcd;

    NodeRegistrar::Options o{};
    o.node_id        = "node-a";
    o.advertise_host = "127.0.0.1";
    o.grpc_port      = 7100;
    o.lease_ttl_seconds = 5;
    NodeRegistrar r(&etcd, o);

    std::string err;
    ASSERT_TRUE(r.Start(&err)) << err;
    EXPECT_TRUE(r.Running());
    EXPECT_NE(r.Lease(), kNoLease);

    auto kv = etcd.Get(r.Key(), &err);
    ASSERT_TRUE(kv.has_value()) << err;
    EXPECT_EQ(kv->lease, r.Lease());

    std::string host;
    uint16_t    port = 0;
    ASSERT_TRUE(DecodeNodeValue(kv->value, &host, &port));
    EXPECT_EQ(host, "127.0.0.1");
    EXPECT_EQ(port, 7100u);

    r.Stop();
    EXPECT_FALSE(r.Running());
    // After revoke the key is gone.
    auto after = etcd.Get(r.Key(), &err);
    EXPECT_FALSE(after.has_value());
}

TEST(NodeRegistrarTest, StartFailsOnIncompleteIdentity) {
    InMemoryEtcdClient etcd;
    NodeRegistrar::Options o{};  // node_id / host / port all default
    NodeRegistrar r(&etcd, o);
    std::string err;
    EXPECT_FALSE(r.Start(&err));
    EXPECT_NE(err.find("identity"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Directory
// ---------------------------------------------------------------------------

TEST(NodeDirectoryTest, SeedsThenObservesPuts) {
    InMemoryEtcdClient etcd;
    HrwRing ring;
    NodeDirectory dir(&etcd, &ring);
    std::string err;
    ASSERT_TRUE(dir.Start(&err)) << err;
    EXPECT_EQ(dir.NodeCount(), 0u);

    // Add node-a via a registrar — directory should observe via Watch.
    NodeRegistrar::Options oa{};
    oa.node_id = "node-a"; oa.advertise_host = "10.0.0.1"; oa.grpc_port = 7000;
    NodeRegistrar ra(&etcd, oa);
    ASSERT_TRUE(ra.Start(&err)) << err;

    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 1; }));
    auto ep = dir.Resolve("node-a");
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "10.0.0.1");
    EXPECT_EQ(ep->grpc_port, 7000u);
    EXPECT_EQ(ep->dial_target, "10.0.0.1:7000");
    EXPECT_EQ(ring.NodeCount(), 1u);

    // Add a second node, watch the ring expand.
    NodeRegistrar::Options ob{};
    ob.node_id = "node-b"; ob.advertise_host = "10.0.0.2"; ob.grpc_port = 7000;
    NodeRegistrar rb(&etcd, ob);
    ASSERT_TRUE(rb.Start(&err)) << err;
    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 2; }));
    EXPECT_EQ(ring.NodeCount(), 2u);

    // Removing one drops it from the ring.
    rb.Stop();
    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 1; }));
    EXPECT_EQ(ring.NodeCount(), 1u);
    EXPECT_FALSE(dir.Resolve("node-b").has_value());

    ra.Stop();
    dir.Stop();
}

// Phase K-3 — directory adopts the CP-published ClusterView snapshot
// in one go. PUTting a view with two nodes makes the table reflect
// those nodes BEFORE either registrar-published key exists; later
// putting a smaller view replaces (not merges) the table. A stale-
// epoch view from the same leader is dropped.
TEST(NodeDirectoryTest, AdoptsClusterViewSnapshot) {
    InMemoryEtcdClient etcd;
    HrwRing ring;
    NodeDirectory dir(&etcd, &ring);
    std::string err;
    ASSERT_TRUE(dir.Start(&err)) << err;
    EXPECT_EQ(dir.NodeCount(), 0u);

    // PUT a ClusterView with 2 nodes — the directory should adopt
    // it even though /kvcache/nodes/ has no registrar entries.
    const std::string v1 = R"({
        "epoch": 1,
        "leader_id": "cp-test",
        "published_at_ns": 0,
        "nodes": [
          {"node_id":"alpha","host":"10.0.0.1","grpc_port":7000},
          {"node_id":"beta", "host":"10.0.0.2","grpc_port":7001}
        ]
    })";
    ASSERT_TRUE(etcd.Put("/kvcache/cluster/view", v1, kNoLease,
                           nullptr, &err)) << err;
    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 2; }));
    {
        auto a = dir.Resolve("alpha");
        ASSERT_TRUE(a.has_value());
        EXPECT_EQ(a->dial_target, "10.0.0.1:7000");
    }

    // Stale-epoch view from the same leader: must be ignored.
    const std::string v_stale = R"({
        "epoch": 1, "leader_id": "cp-test",
        "nodes": [{"node_id":"ghost","host":"x","grpc_port":1}]
    })";
    ASSERT_TRUE(etcd.Put("/kvcache/cluster/view", v_stale, kNoLease,
                           nullptr, &err));
    // Negative check: NodeCount should NOT briefly flip to 1.
    // Sleep a beat so the watcher dispatched (in-memory etcd is
    // synchronous to PUT but the callback runs on a dispatch
    // thread).
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(dir.NodeCount(), 2u)
        << "stale-epoch view from same leader must be ignored";
    EXPECT_FALSE(dir.Resolve("ghost").has_value());

    // Fresh epoch with one node: table replaces wholesale.
    const std::string v2 = R"({
        "epoch": 7, "leader_id": "cp-test",
        "nodes": [{"node_id":"gamma","host":"10.0.0.3","grpc_port":7002}]
    })";
    ASSERT_TRUE(etcd.Put("/kvcache/cluster/view", v2, kNoLease,
                           nullptr, &err));
    ASSERT_TRUE(WaitFor([&] {
        auto ids = dir.NodeIds();
        return ids.size() == 1 && ids[0] == "gamma";
    }));
    EXPECT_FALSE(dir.Resolve("alpha").has_value());

    // New leader's epoch=1 must be accepted (epoch threshold resets
    // when leader_id changes).
    const std::string v_new_leader = R"({
        "epoch": 1, "leader_id": "cp-2",
        "nodes": [{"node_id":"delta","host":"10.0.0.4","grpc_port":7003}]
    })";
    ASSERT_TRUE(etcd.Put("/kvcache/cluster/view", v_new_leader, kNoLease,
                           nullptr, &err));
    ASSERT_TRUE(WaitFor([&] {
        auto ids = dir.NodeIds();
        return ids.size() == 1 && ids[0] == "delta";
    }));
    dir.Stop();
}

// Phase R-4 — chaos: N parallel registrars race the watch dispatcher.
//
// Each thread runs an independent NodeRegistrar against a shared
// etcd. The test asserts that NodeDirectory converges to ALL N
// nodes, then to 0 after every registrar Stops. Catches:
//
//   * Lost-update races between WatchPrefix callback delivery and
//     NodeDirectory's table mutations.
//   * Lock-ordering inversions between Registry-side keepalive
//     threads and Directory-side OnWatch.
//   * Off-by-one bugs in the rebuild-ring path when many concurrent
//     mutations land in a short window.
//
// N=10 is enough to surface races on macOS/Linux without making the
// test long. Each registrar lives in its own thread with its own
// keepalive loop.
TEST(NodeDirectoryTest, ConcurrentRegistrarsConverge) {
    constexpr int kN = 10;
    InMemoryEtcdClient etcd;
    HrwRing ring;
    NodeDirectory dir(&etcd, &ring);
    std::string err;
    ASSERT_TRUE(dir.Start(&err)) << err;

    std::vector<std::unique_ptr<NodeRegistrar>> regs;
    regs.reserve(kN);
    std::vector<std::thread>     starters;
    starters.reserve(kN);
    for (int i = 0; i < kN; ++i) {
        NodeRegistrar::Options o{};
        o.node_id        = "n" + std::to_string(i);
        o.advertise_host = "10.0.0." + std::to_string(i + 1);
        o.grpc_port      = static_cast<uint16_t>(8000 + i);
        regs.emplace_back(std::make_unique<NodeRegistrar>(&etcd, o));
    }
    // Race the Starts.
    for (auto& r : regs) {
        starters.emplace_back([&r] {
            std::string e;
            (void)r->Start(&e);
        });
    }
    for (auto& t : starters) t.join();

    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == kN; },
                          std::chrono::seconds(3)));
    EXPECT_EQ(ring.NodeCount(), kN);
    // Every id should be resolvable.
    for (int i = 0; i < kN; ++i) {
        EXPECT_TRUE(dir.Resolve("n" + std::to_string(i)).has_value())
            << "n" << i << " missing after concurrent register";
    }

    // Race the Stops (graceful unregister → LeaseRevoke).
    std::vector<std::thread> stoppers;
    stoppers.reserve(kN);
    for (auto& r : regs) {
        stoppers.emplace_back([&r] { r->Stop(); });
    }
    for (auto& t : stoppers) t.join();

    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 0; },
                          std::chrono::seconds(3)));
    EXPECT_EQ(ring.NodeCount(), 0u);
    dir.Stop();
}

// Phase R-3 — chaos: full leader handover under in-flight membership.
//
// Scenario the test pins down:
//   t0: Leader L1 publishes view {a}, NodeDirectory in view-mode.
//   t1: node-b registers via NodeRegistrar. View-mode is active so
//       the prefix watch is detached — the directory does NOT see
//       node-b yet. (R-3's "in-flight membership during view-mode".)
//   t2: Leader L1 dies — view key deleted. NodeDirectory reopens
//       the prefix watch, re-seeds via GetPrefix, picks up node-b.
//       Table converges to {a, b}.
//   t3: Leader L2 (different leader_id, epoch=1) publishes view {b}.
//       NodeDirectory adopts L2's view, detaches the prefix watch,
//       table flips to {b}.
//
// This exercises every state transition K-4 introduced + R-2's
// crash-style cleanup + K-3's new-leader epoch reset. If any one of
// them regresses, the test fails at a recognisable step.
TEST(NodeDirectoryTest, LeaderChurnHandoverConverges) {
    InMemoryEtcdClient::Options eo;
    eo.lease_sweep_interval = std::chrono::milliseconds(50);
    InMemoryEtcdClient etcd(eo);
    HrwRing ring;
    NodeDirectory dir(&etcd, &ring);
    std::string err;
    ASSERT_TRUE(dir.Start(&err)) << err;

    // ---- t0: L1 publishes {a} ----
    const std::string v_l1 = R"({
        "epoch": 1, "leader_id": "L1",
        "nodes": [{"node_id":"a","host":"10.0.0.1","grpc_port":7000}]
    })";
    auto l1_lease = etcd.LeaseGrant(/*ttl=*/1, &err);
    ASSERT_NE(l1_lease, kNoLease) << err;
    ASSERT_TRUE(etcd.Put("/kvcache/cluster/view", v_l1, l1_lease,
                          nullptr, &err));
    ASSERT_TRUE(WaitFor([&] {
        auto ids = dir.NodeIds();
        return ids.size() == 1 && ids[0] == "a";
    }));

    // ---- t1: node-b registers during view-mode (must be invisible) ----
    NodeRegistrar::Options ob{};
    ob.node_id = "b"; ob.advertise_host = "10.0.0.2"; ob.grpc_port = 7001;
    NodeRegistrar reg_b(&etcd, ob);
    ASSERT_TRUE(reg_b.Start(&err)) << err;
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(dir.NodeCount(), 1u)
        << "view-mode must hide prefix-watch events";

    // ---- t2: L1 dies (lease expires; sweeper deletes the view key) ----
    // NodeDirectory reopens the prefix watch + re-seeds via
    // GetPrefix. node-a in this test was NEVER in /kvcache/nodes/
    // (we manually PUT the view key without a matching registrar),
    // so re-seed picks up ONLY node-b. Table flips {a} → {b}
    // atomically; we don't see an interim {a,b}.
    ASSERT_TRUE(WaitFor([&] {
        auto ids = dir.NodeIds();
        return ids.size() == 1 && ids[0] == "b";
    }, std::chrono::seconds(3)));
    EXPECT_FALSE(dir.Resolve("a").has_value())
        << "node-a was only in the view (not the registrar prefix); "
           "re-seed must drop it";
    EXPECT_TRUE(dir.Resolve("b").has_value());

    // ---- t3: L2 takes over with a different leader_id ----
    // Epoch=1 is normally below L1's epoch=1 threshold but the
    // leader_id change resets the threshold (per K-3 semantics)
    // so L2's view is accepted.
    const std::string v_l2 = R"({
        "epoch": 1, "leader_id": "L2",
        "nodes": [
          {"node_id":"a","host":"10.0.0.1","grpc_port":7000},
          {"node_id":"b","host":"10.0.0.2","grpc_port":7001},
          {"node_id":"c","host":"10.0.0.3","grpc_port":7002}
        ]
    })";
    auto l2_lease = etcd.LeaseGrant(60, &err);  // long-lived
    ASSERT_TRUE(etcd.Put("/kvcache/cluster/view", v_l2, l2_lease,
                          nullptr, &err));
    ASSERT_TRUE(WaitFor([&] {
        auto ids = dir.NodeIds();
        return ids.size() == 3;
    }, std::chrono::seconds(2)));
    EXPECT_TRUE(dir.Resolve("c").has_value());

    reg_b.Stop();
    dir.Stop();
}

// Phase R-2 — chaos: a kvstore-node pod crashes mid-flight. The
// crash means NO graceful LeaseRevoke happens; the etcd lease just
// stops getting keepalive-d and eventually expires on its own.
// NodeDirectory should observe the delete event from the etcd
// sweeper and converge to the remaining membership within
// ~TTL + sweep_interval.
TEST(NodeDirectoryTest, CrashedNodeLeaseExpiryConverges) {
    // Tight sweeper interval so the test runs quickly without
    // changing the production default (100 ms).
    InMemoryEtcdClient::Options eo;
    eo.lease_sweep_interval = std::chrono::milliseconds(50);
    InMemoryEtcdClient etcd(eo);
    HrwRing ring;
    NodeDirectory dir(&etcd, &ring);
    std::string err;
    ASSERT_TRUE(dir.Start(&err)) << err;

    // Add a "graceful" peer via the registrar so we have something
    // to converge ONTO (the lease-expiry case must converge to
    // a CORRECT membership, not just to zero).
    NodeRegistrar::Options ok{};
    ok.node_id = "node-ok"; ok.advertise_host = "10.0.0.1"; ok.grpc_port = 7000;
    NodeRegistrar reg_ok(&etcd, ok);
    ASSERT_TRUE(reg_ok.Start(&err)) << err;

    // Simulate a crashed peer by putting a leased key WITHOUT a
    // matching NodeRegistrar — no keepalive thread, so the lease
    // expires after TTL on its own. TTL=1s + 50ms sweep = ~1.1s
    // convergence.
    auto crashed_lease = etcd.LeaseGrant(/*ttl_seconds=*/1, &err);
    ASSERT_NE(crashed_lease, kNoLease) << err;
    const std::string crashed_val =
        EncodeNodeValue("node-crashed", "10.0.0.99", 9999);
    ASSERT_TRUE(etcd.Put("/kvcache/nodes/node-crashed", crashed_val,
                          crashed_lease, nullptr, &err)) << err;

    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 2; }));
    ASSERT_TRUE(dir.Resolve("node-crashed").has_value());

    // Wait for the lease sweeper to expire the crashed entry.
    ASSERT_TRUE(WaitFor(
        [&] { return !dir.Resolve("node-crashed").has_value(); },
        std::chrono::seconds(3)));
    EXPECT_EQ(dir.NodeCount(), 1u)
        << "after lease expiry the table should hold only node-ok";
    EXPECT_TRUE(dir.Resolve("node-ok").has_value());
    EXPECT_EQ(ring.NodeCount(), 1u);

    reg_ok.Stop();
    dir.Stop();
}

// Phase K-4 — when ClusterView is active the prefix watch should be
// detached, so a subsequent /kvcache/nodes/ PUT no longer mutates the
// directory's table. Then deleting the view-key reopens the prefix
// watch and convergence resumes.
TEST(NodeDirectoryTest, ViewModeDetachesAndReattachesPrefixWatch) {
    InMemoryEtcdClient etcd;
    HrwRing ring;
    NodeDirectory dir(&etcd, &ring);
    std::string err;
    ASSERT_TRUE(dir.Start(&err)) << err;

    // Activate view-mode by publishing a ClusterView with one node.
    const std::string v = R"({
        "epoch": 1, "leader_id": "L1",
        "nodes": [{"node_id":"alpha","host":"10.0.0.1","grpc_port":7000}]
    })";
    ASSERT_TRUE(etcd.Put("/kvcache/cluster/view", v, kNoLease, nullptr, &err));
    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 1; }));

    // Now register a NEW node via the prefix path. With the prefix
    // watch detached, this PUT should NOT show up in the directory
    // (until either the view is updated to include it OR the view
    // is deleted, restoring prefix mode).
    NodeRegistrar::Options o{};
    o.node_id = "beta"; o.advertise_host = "10.0.0.2"; o.grpc_port = 7001;
    NodeRegistrar rb(&etcd, o);
    ASSERT_TRUE(rb.Start(&err)) << err;
    // Give any spurious events a window to fire — they shouldn't.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(dir.NodeCount(), 1u)
        << "view-mode must ignore prefix events; saw "
        << dir.NodeCount() << " entries";
    EXPECT_FALSE(dir.Resolve("beta").has_value());

    // Delete the view key — leader loss. Prefix watch should reopen
    // + re-seed, picking up the registrar entry that was published
    // while we were in view-mode.
    ASSERT_TRUE(etcd.Delete("/kvcache/cluster/view", &err));
    ASSERT_TRUE(WaitFor([&] {
        return dir.Resolve("beta").has_value();
    }));
    rb.Stop();
    dir.Stop();
}

TEST(NodeDirectoryTest, PrimaryConvergesOnSurvivingNode) {
    InMemoryEtcdClient etcd;
    HrwRing ring;
    NodeDirectory dir(&etcd, &ring);
    std::string err;
    ASSERT_TRUE(dir.Start(&err)) << err;

    NodeRegistrar::Options oa{};
    oa.node_id = "node-a"; oa.advertise_host = "10.0.0.1"; oa.grpc_port = 7000;
    NodeRegistrar ra(&etcd, oa);
    NodeRegistrar::Options ob{};
    ob.node_id = "node-b"; ob.advertise_host = "10.0.0.2"; ob.grpc_port = 7000;
    NodeRegistrar rb(&etcd, ob);
    ASSERT_TRUE(ra.Start(&err)) << err;
    ASSERT_TRUE(rb.Start(&err)) << err;
    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 2; }));

    const std::vector<uint8_t> key{1, 2, 3, 4, 5, 6, 7, 8};
    const std::string primary_two_nodes = ring.Primary(key);
    EXPECT_TRUE(primary_two_nodes == "node-a" || primary_two_nodes == "node-b");

    // Drop whichever one HRW picked; the survivor MUST become primary.
    if (primary_two_nodes == "node-a") {
        ra.Stop();
    } else {
        rb.Stop();
    }
    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 1; }));
    const std::string primary_one_node = ring.Primary(key);
    EXPECT_NE(primary_one_node, primary_two_nodes);
    EXPECT_FALSE(primary_one_node.empty());
}
