// Phase A1.8 — ClusterViewWatcher tests.
//
// Covers the JSON parser (the testable unit) + the RefreshOnce →
// HrwResolver wiring, including the keep-last-good-on-error semantics
// that matter for a flaky etcd.
#include "router/cluster_view.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "router/hrw_resolver.h"

using kvcache::agent::router::ClusterViewWatcher;
using kvcache::agent::router::HrwResolver;

// A snapshot matching control-plane/internal/membership/view.go's
// ClusterView JSON.
static const char* kViewJson = R"({
  "epoch": 7,
  "leader_id": "cp-host-123",
  "published_at_ns": 1717000000000000000,
  "nodes": [
    {"node_id": "node-a", "host": "10.0.0.1", "grpc_port": 7000},
    {"node_id": "node-b", "host": "10.0.0.2", "grpc_port": 7000},
    {"node_id": "node-c", "host": "10.0.0.3", "grpc_port": 7000}
  ]
})";

TEST(ClusterViewParseTest, ExtractsNodeIdsInOrder) {
    auto ids = ClusterViewWatcher::ParseNodeIds(kViewJson);
    ASSERT_TRUE(ids.has_value());
    ASSERT_EQ(ids->size(), 3u);
    EXPECT_EQ((*ids)[0], "node-a");
    EXPECT_EQ((*ids)[1], "node-b");
    EXPECT_EQ((*ids)[2], "node-c");
}

TEST(ClusterViewParseTest, EmptyBodyIsEmptySetNotError) {
    auto ids = ClusterViewWatcher::ParseNodeIds("");
    ASSERT_TRUE(ids.has_value());
    EXPECT_TRUE(ids->empty());
}

TEST(ClusterViewParseTest, EmptyNodesArrayParsesToEmptyVector) {
    auto ids = ClusterViewWatcher::ParseNodeIds(
        R"({"epoch":1,"leader_id":"x","nodes":[]})");
    ASSERT_TRUE(ids.has_value());
    EXPECT_TRUE(ids->empty());
}

TEST(ClusterViewParseTest, MalformedJsonReturnsNullopt) {
    EXPECT_FALSE(ClusterViewWatcher::ParseNodeIds("{not json").has_value());
    EXPECT_FALSE(ClusterViewWatcher::ParseNodeIds("[1,2,3]").has_value());  // not an object
}

TEST(ClusterViewParseTest, MissingOrWrongTypeNodesFieldIsNullopt) {
    EXPECT_FALSE(ClusterViewWatcher::ParseNodeIds(
        R"({"epoch":1})").has_value());  // no "nodes"
    EXPECT_FALSE(ClusterViewWatcher::ParseNodeIds(
        R"({"nodes":"oops"})").has_value());  // "nodes" not an array
}

TEST(ClusterViewParseTest, SkipsEntriesMissingNodeId) {
    auto ids = ClusterViewWatcher::ParseNodeIds(
        R"({"nodes":[{"node_id":"a"},{"host":"h"},{"node_id":""},{"node_id":"b"}]})");
    ASSERT_TRUE(ids.has_value());
    ASSERT_EQ(ids->size(), 2u);  // empty + missing dropped
    EXPECT_EQ((*ids)[0], "a");
    EXPECT_EQ((*ids)[1], "b");
}

TEST(ClusterViewParseTest, SkipsDrainingNodes) {
    // Phase A2.1 — a node published as DRAINING is excluded from the
    // routable set so the HRW resolver stops sending it new prefixes.
    auto ids = ClusterViewWatcher::ParseNodeIds(R"({"nodes":[
        {"node_id":"node-a","state":"active"},
        {"node_id":"node-b","state":"draining"},
        {"node_id":"node-c","state":"NODE_DRAINING"},
        {"node_id":"node-d"}
    ]})");
    ASSERT_TRUE(ids.has_value());
    ASSERT_EQ(ids->size(), 2u) << "draining + NODE_DRAINING must be skipped";
    EXPECT_EQ((*ids)[0], "node-a");
    EXPECT_EQ((*ids)[1], "node-d");  // no state field → kept
}

TEST(ClusterViewWatcherTest, RefreshOnceAppliesNodesToResolver) {
    HrwResolver hrw;
    EXPECT_EQ(hrw.NodeCount(), 0u);

    ClusterViewWatcher::Options opts;
    opts.loader = []() -> std::optional<std::string> { return std::string(kViewJson); };
    ClusterViewWatcher w(hrw, opts);

    int n = w.RefreshOnce();
    EXPECT_EQ(n, 3);
    EXPECT_EQ(hrw.NodeCount(), 3u);
    EXPECT_EQ(w.SnapshotStats().refreshes_ok, 1u);
    EXPECT_EQ(w.SnapshotStats().last_node_count, 3u);
}

TEST(ClusterViewWatcherTest, LoaderErrorKeepsLastGoodSet) {
    HrwResolver hrw;
    bool fail = false;
    ClusterViewWatcher::Options opts;
    opts.loader = [&fail]() -> std::optional<std::string> {
        if (fail) return std::nullopt;  // simulate etcd unreachable
        return std::string(kViewJson);
    };
    ClusterViewWatcher w(hrw, opts);

    ASSERT_EQ(w.RefreshOnce(), 3);
    EXPECT_EQ(hrw.NodeCount(), 3u);

    // Now the loader fails — the resolver must keep its 3 nodes.
    fail = true;
    EXPECT_EQ(w.RefreshOnce(), -1);
    EXPECT_EQ(hrw.NodeCount(), 3u) << "load error must not blank the resolver";
    EXPECT_EQ(w.SnapshotStats().refreshes_failed, 1u);
}

TEST(ClusterViewWatcherTest, MalformedJsonKeepsLastGoodSet) {
    HrwResolver hrw;
    std::string body(kViewJson);
    ClusterViewWatcher::Options opts;
    opts.loader = [&body]() -> std::optional<std::string> { return body; };
    ClusterViewWatcher w(hrw, opts);

    ASSERT_EQ(w.RefreshOnce(), 3);
    body = "{garbage";
    EXPECT_EQ(w.RefreshOnce(), -1);
    EXPECT_EQ(hrw.NodeCount(), 3u) << "parse error must not blank the resolver";
}

TEST(ClusterViewWatcherTest, UpdatedViewReplacesNodeSet) {
    HrwResolver hrw;
    std::string body(kViewJson);  // 3 nodes
    ClusterViewWatcher::Options opts;
    opts.loader = [&body]() -> std::optional<std::string> { return body; };
    ClusterViewWatcher w(hrw, opts);

    ASSERT_EQ(w.RefreshOnce(), 3);
    // A new view drops a node.
    body = R"({"nodes":[{"node_id":"node-a"},{"node_id":"node-b"}]})";
    EXPECT_EQ(w.RefreshOnce(), 2);
    EXPECT_EQ(hrw.NodeCount(), 2u);
}

// Phase A8+ — ParseNodes lifts per-node weight; absent weight defaults to 1.0,
// and a non-positive / wrong-typed weight also falls back to 1.0 (never
// silently zeroes a node out of routing).
TEST(ClusterViewParseTest, ParseNodesReadsWeightWithDefault) {
    auto nodes = ClusterViewWatcher::ParseNodes(
        R"({"nodes":[
              {"node_id":"heavy","weight":8.5},
              {"node_id":"plain"},
              {"node_id":"zero","weight":0},
              {"node_id":"bad","weight":"x"}
        ]})");
    ASSERT_TRUE(nodes.has_value());
    ASSERT_EQ(nodes->size(), 4u);
    EXPECT_EQ((*nodes)[0].first, "heavy");
    EXPECT_DOUBLE_EQ((*nodes)[0].second, 8.5);
    EXPECT_DOUBLE_EQ((*nodes)[1].second, 1.0) << "absent weight → 1.0";
    EXPECT_DOUBLE_EQ((*nodes)[2].second, 1.0) << "non-positive weight → 1.0";
    EXPECT_DOUBLE_EQ((*nodes)[3].second, 1.0) << "wrong-typed weight → 1.0";
}

// Back-compat: ParseNodeIds still projects just the ids (now delegates to
// ParseNodes), including the DRAINING skip.
TEST(ClusterViewParseTest, ParseNodeIdsStillProjectsIds) {
    auto ids = ClusterViewWatcher::ParseNodeIds(
        R"({"nodes":[{"node_id":"a","weight":3},{"node_id":"b","state":"DRAINING"}]})");
    ASSERT_TRUE(ids.has_value());
    ASSERT_EQ(ids->size(), 1u) << "draining node b skipped";
    EXPECT_EQ((*ids)[0], "a");
}
