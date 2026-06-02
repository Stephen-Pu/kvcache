// Phase A1.4 — BloomView tests.
//
// Covers:
//   1. Unknown tenant returns true ("might exist") to keep callers
//      from incorrectly skipping the slow path.
//   2. Refresh installs sketches; MaybeContains returns true for keys
//      we inserted via a fake-loader, false for definitely-absent keys.
//   3. Loader exception bumps the refreshes_failed counter and does
//      not crash the refresher.
//   4. Start/Stop is idempotent and the background thread actually
//      runs RefreshOnce at least once.
//   5. Per-tenant scoping — a sketch for tenant A is invisible to a
//      query for tenant B.
#include "bloom_view/bloom_view.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>

using namespace kvcache::agent::bloom_view;
using kvcache::node::routing::BloomParams;
using kvcache::node::routing::LocalBloom;

namespace {

// Build a TenantSketch by inserting `keys` into a LocalBloom and
// snapshotting it — exercises the real sketch shape end-to-end.
TenantSketch MakeSketch(const std::string& tenant,
                         const std::vector<std::string>& keys) {
    BloomParams p = BloomParams::ForCapacity(/*expected_n=*/1024,
                                              /*target_fpr=*/0.001);
    LocalBloom b(p);
    for (const auto& k : keys) {
        b.Add({reinterpret_cast<const uint8_t*>(k.data()), k.size()});
    }
    return TenantSketch{tenant, p, b.Snapshot()};
}

std::span<const uint8_t> Bytes(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

}  // namespace

TEST(BloomViewTest, UnknownTenantIsPermissive) {
    BloomView bv({
        .loader = [](const std::vector<std::string>&) {
            return std::vector<TenantSketch>{};
        },
    });
    EXPECT_TRUE(bv.MaybeContains("never-seen", Bytes("anything")));
}

TEST(BloomViewTest, RefreshInstallsSketchesAndAnswersCorrectly) {
    BloomView bv({
        .loader = [](const std::vector<std::string>& tenants) {
            std::vector<TenantSketch> out;
            for (const auto& t : tenants) {
                if (t == "t1") {
                    out.push_back(MakeSketch("t1", {"k1", "k2", "k3"}));
                }
            }
            return out;
        },
    });
    bv.RegisterTenant("t1");
    ASSERT_EQ(bv.RefreshOnce(), 1);

    // Inserted keys: must report true.
    EXPECT_TRUE(bv.MaybeContains("t1", Bytes("k1")));
    EXPECT_TRUE(bv.MaybeContains("t1", Bytes("k2")));
    // Definitely-not-inserted: should report false with high probability.
    // Try several "absent" keys; at least one should report false
    // (the test's at-target FPR=0.001 gives us plenty of room).
    bool got_false = false;
    for (int i = 0; i < 50; ++i) {
        std::string absent = "absent-" + std::to_string(i);
        if (!bv.MaybeContains("t1", Bytes(absent))) {
            got_false = true;
            break;
        }
    }
    EXPECT_TRUE(got_false);
    auto s = bv.SnapshotStats();
    EXPECT_EQ(s.refreshes_ok, 1u);
    EXPECT_GE(s.queries, 1u);
    EXPECT_GE(s.answered_false, 1u);
}

TEST(BloomViewTest, LoaderExceptionBumpsFailedCounter) {
    BloomView bv({
        .loader = [](const std::vector<std::string>&) -> std::vector<TenantSketch> {
            throw std::runtime_error("etcd down");
        },
    });
    bv.RegisterTenant("t1");
    EXPECT_EQ(bv.RefreshOnce(), -1);
    auto s = bv.SnapshotStats();
    EXPECT_EQ(s.refreshes_failed, 1u);
    EXPECT_EQ(s.refreshes_ok, 0u);
}

TEST(BloomViewTest, StartStopRunsRefreshAtLeastOnce) {
    std::atomic<int> calls{0};
    BloomView bv({
        .refresh_interval = std::chrono::milliseconds(50),
        .loader = [&](const std::vector<std::string>&) {
            calls.fetch_add(1, std::memory_order_relaxed);
            return std::vector<TenantSketch>{};
        },
    });
    bv.RegisterTenant("t1");
    bv.Start();
    // Eager first refresh runs immediately; give a brief window so the
    // background thread can complete it.
    for (int i = 0; i < 100 && calls.load() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    bv.Stop();
    EXPECT_GE(calls.load(), 1);
    // Idempotent Stop.
    bv.Stop();
}

TEST(BloomViewTest, PerTenantScoping) {
    BloomView bv({
        .loader = [](const std::vector<std::string>& tenants) {
            std::vector<TenantSketch> out;
            for (const auto& t : tenants) {
                if (t == "t1") out.push_back(MakeSketch("t1", {"k-in-t1"}));
                if (t == "t2") out.push_back(MakeSketch("t2", {"k-in-t2"}));
            }
            return out;
        },
    });
    bv.RegisterTenant("t1");
    bv.RegisterTenant("t2");
    ASSERT_EQ(bv.RefreshOnce(), 2);

    EXPECT_TRUE(bv.MaybeContains("t1", Bytes("k-in-t1")));
    EXPECT_TRUE(bv.MaybeContains("t2", Bytes("k-in-t2")));
    // The sketch for the OTHER tenant should not see this key. There's
    // a Bloom-FPR floor here so try multiple to make the test robust.
    int absent_in_t2 = 0;
    for (int i = 0; i < 20; ++i) {
        // Vary the byte so we don't hit the same FPR bucket every probe.
        std::string k = "k-in-t1-" + std::to_string(i);
        if (!bv.MaybeContains("t2", Bytes(k))) ++absent_in_t2;
    }
    EXPECT_GT(absent_in_t2, 0);  // at least one definitively-absent answer
}
