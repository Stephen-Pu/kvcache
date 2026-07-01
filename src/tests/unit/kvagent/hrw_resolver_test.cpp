// Phase A1.7 — HrwResolver tests.
//
// Verifies the kvagent's slow-path resolver (HRW over the cluster node
// set): empty-set behaviour, deterministic primary selection, even-ish
// distribution across nodes, and minimal disruption when a node is
// added (the HRW property the whole design rests on). Also exercises
// the AsCallback adapter + end-to-end through RequestRouter.
#include "router/hrw_resolver.h"

#include <gtest/gtest.h>

#include <array>
#include <map>
#include <string>
#include <vector>

#include "bloom_view/bloom_view.h"
#include "router/router.h"
#include "routing_cache/routing_cache.h"

using namespace kvcache::agent;
using router::HrwResolver;

namespace {
std::array<uint8_t, 16> T(uint8_t b) { std::array<uint8_t, 16> a{}; a[0] = b; return a; }
std::array<uint8_t, 16> P(uint8_t b) { std::array<uint8_t, 16> a{}; a[15] = b; return a; }
}  // namespace

TEST(HrwResolverTest, EmptySetResolvesToNullopt) {
    HrwResolver r;
    EXPECT_EQ(r.NodeCount(), 0u);
    EXPECT_FALSE(r.Resolve(T(1), P(1)).has_value());
}

TEST(HrwResolverTest, ResolvesToOneOfTheNodesDeterministically) {
    HrwResolver r;
    r.SetNodes({"node-a", "node-b", "node-c"});
    ASSERT_EQ(r.NodeCount(), 3u);

    auto r1 = r.Resolve(T(7), P(42));
    ASSERT_TRUE(r1.has_value());
    EXPECT_TRUE(r1->node_id == "node-a" || r1->node_id == "node-b" ||
                r1->node_id == "node-c");
    EXPECT_EQ(r1->matched_tokens, 0u);  // agent doesn't know LPM length

    // Deterministic: same key → same primary.
    auto r2 = r.Resolve(T(7), P(42));
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r1->node_id, r2->node_id);
}

TEST(HrwResolverTest, DistributesAcrossNodes) {
    HrwResolver r;
    r.SetNodes({"node-a", "node-b", "node-c", "node-d"});
    std::map<std::string, int> hits;
    for (int i = 0; i < 256; ++i) {
        auto rr = r.Resolve(T(1), P(static_cast<uint8_t>(i)));
        ASSERT_TRUE(rr.has_value());
        ++hits[rr->node_id];
    }
    // All four nodes should get *some* traffic — HRW over 256 distinct
    // keys won't pile everything on one node. (Loose bound: each node
    // gets at least a handful; we're guarding against a broken hash
    // that collapses to a single node.)
    EXPECT_EQ(hits.size(), 4u) << "every node should receive some keys";
    for (const auto& [node, n] : hits) {
        EXPECT_GT(n, 10) << node << " got too few keys (" << n << ")";
    }
}

TEST(HrwResolverTest, AddingNodeMovesMinimalKeys) {
    // The core HRW property: adding a node only moves the ~1/N of keys
    // that now hash highest to the new node; the rest stay put.
    HrwResolver r;
    r.SetNodes({"node-a", "node-b", "node-c"});
    std::vector<std::string> before;
    before.reserve(256);
    for (int i = 0; i < 256; ++i) {
        before.push_back(r.Resolve(T(1), P(static_cast<uint8_t>(i)))->node_id);
    }
    // Add a 4th node.
    r.SetNodes({"node-a", "node-b", "node-c", "node-d"});
    int moved = 0;
    for (int i = 0; i < 256; ++i) {
        auto after = r.Resolve(T(1), P(static_cast<uint8_t>(i)))->node_id;
        if (after != before[i]) ++moved;
    }
    // Expectation ~256/4 = 64 keys move to node-d; everything else
    // stays. Allow generous slack (a consistent-hash ring would move
    // far more). Assert < 40% moved — a broken impl that reshuffles
    // everything would blow past this.
    EXPECT_LT(moved, 256 * 4 / 10) << "HRW should move ~1/N keys, moved " << moved;
    EXPECT_GT(moved, 0) << "adding a node should move *some* keys to it";
}

TEST(HrwResolverTest, AsCallbackDrivesRouterEndToEnd) {
    HrwResolver hrw;
    hrw.SetNodes({"node-x", "node-y"});

    routing_cache::RoutingCache cache;
    auto bloom = bloom_view::BloomView({
        .loader = [](const std::vector<std::string>&) {
            return std::vector<bloom_view::TenantSketch>{};  // permissive
        },
    });
    router::RequestRouter rt(cache, bloom, hrw.AsCallback());

    router::Request req;
    req.op = router::OpCode::kLookup;
    req.tenant_id = T(3);
    req.prefix_hash = P(9);
    auto resp = rt.Handle(req);
    EXPECT_EQ(resp.status, router::Status::kOk);
    EXPECT_EQ(resp.source, router::Source::kResolver);
    EXPECT_TRUE(resp.node_id == "node-x" || resp.node_id == "node-y");

    // Second lookup is served from the cache the resolver populated.
    auto resp2 = rt.Handle(req);
    EXPECT_EQ(resp2.source, router::Source::kCache);
    EXPECT_EQ(resp2.node_id, resp.node_id);
}

// Phase A8+ — a weight-0 node (zero-capacity / draining) is never chosen as
// primary while a positive-weight peer is present: eff = hash*weight = 0 loses.
TEST(HrwResolverTest, WeightZeroNodeNeverPrimary) {
    HrwResolver r;
    std::vector<std::pair<std::string, double>> nodes{{"live", 1.0}, {"dead", 0.0}};
    r.SetNodes(nodes);
    int dead = 0, live = 0;
    for (int i = 0; i < 200; ++i) {
        auto rr = r.Resolve(T(1), P(static_cast<uint8_t>(i)));
        ASSERT_TRUE(rr.has_value());
        if (rr->node_id == "dead") ++dead;
        else if (rr->node_id == "live") ++live;
    }
    EXPECT_EQ(dead, 0) << "a weight-0 node must never win the rendezvous hash";
    EXPECT_EQ(live, 200);
}

// Phase A8+ — a heavier node wins the rendezvous hash proportionally more.
TEST(HrwResolverTest, HeavierNodeWinsProportionally) {
    HrwResolver r;
    std::vector<std::pair<std::string, double>> nodes{{"light", 1.0}, {"heavy", 9.0}};
    r.SetNodes(nodes);
    int heavy = 0, light = 0;
    for (int i = 0; i < 200; ++i) {
        auto rr = r.Resolve(T(2), P(static_cast<uint8_t>(i)));
        ASSERT_TRUE(rr.has_value());
        if (rr->node_id == "heavy") ++heavy;
        else if (rr->node_id == "light") ++light;
    }
    EXPECT_GT(heavy, light * 2) << "9:1 weight → heavy must dominate (heavy="
                               << heavy << ", light=" << light << ")";
    EXPECT_GT(light, 0) << "the light node should still get some traffic";
}

// Phase A8+ — equal weights via the weighted overload match the uniform
// overload's placement exactly (weights don't skew when equal).
TEST(HrwResolverTest, EqualWeightsBehaveLikeUniform) {
    HrwResolver rw, ru;
    std::vector<std::pair<std::string, double>> weighted{
        {"n0", 1.0}, {"n1", 1.0}, {"n2", 1.0}};
    rw.SetNodes(weighted);
    ru.SetNodes(std::vector<std::string>{"n0", "n1", "n2"});
    for (int i = 0; i < 128; ++i) {
        auto a = rw.Resolve(T(5), P(static_cast<uint8_t>(i)));
        auto b = ru.Resolve(T(5), P(static_cast<uint8_t>(i)));
        ASSERT_TRUE(a.has_value() && b.has_value());
        EXPECT_EQ(a->node_id, b->node_id) << "equal weights must match uniform at i=" << i;
    }
}
