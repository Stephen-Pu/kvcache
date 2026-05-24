#include "prefix/refcount.h"

#include <gtest/gtest.h>
#include <thread>
#include <vector>

using kvcache::node::prefix::Refcount;

TEST(RefcountTest, StartsAtZero) {
    Refcount rc;
    EXPECT_TRUE(rc.IsZero());
    EXPECT_EQ(rc.Load(), 0u);
}

TEST(RefcountTest, AcquireRelease) {
    Refcount rc;
    EXPECT_EQ(rc.Acquire(), 1u);
    EXPECT_EQ(rc.Acquire(), 2u);
    EXPECT_EQ(rc.Release(), 1u);
    EXPECT_EQ(rc.Release(), 0u);
    EXPECT_TRUE(rc.IsZero());
}

TEST(RefcountTest, TryAcquireIfNonZero_FailsAtZero) {
    Refcount rc;
    EXPECT_FALSE(rc.TryAcquireIfNonZero());
    EXPECT_TRUE(rc.IsZero());
}

TEST(RefcountTest, TryAcquireIfNonZero_SucceedsWhenHeld) {
    Refcount rc(1);
    EXPECT_TRUE(rc.TryAcquireIfNonZero());
    EXPECT_EQ(rc.Load(), 2u);
}

// Phase G-2 — TryEvict CAS semantics.
TEST(RefcountTest, TryEvict_SucceedsAtBaseline) {
    Refcount rc(1);
    EXPECT_TRUE(rc.TryEvict());
    EXPECT_TRUE(rc.IsZero());
}

TEST(RefcountTest, TryEvict_FailsWhenHolders) {
    Refcount rc(1);
    ASSERT_TRUE(rc.TryAcquireIfNonZero());  // bumps to 2 — a live holder
    EXPECT_FALSE(rc.TryEvict());
    EXPECT_EQ(rc.Load(), 2u);
}

TEST(RefcountTest, TryEvict_FailsAtZero) {
    Refcount rc;  // 0 already
    EXPECT_FALSE(rc.TryEvict());
    EXPECT_TRUE(rc.IsZero());
}

TEST(RefcountTest, TryEvict_RacesWithAcquireIfNonZero) {
    // Hammers TryEvict and TryAcquireIfNonZero concurrently from
    // baseline 1; either the evictor wins (count → 0, acquirer fails
    // ever after) or the acquirer wins (count ≥ 2, evictor fails).
    // We just assert no torn state — count is always 0 or ≥ 1, never
    // skipped past 0 spuriously.
    constexpr int kRounds = 5000;
    for (int round = 0; round < kRounds; ++round) {
        Refcount rc(1);
        std::atomic<bool> evict_won{false};
        std::atomic<bool> acquire_won{false};
        std::thread t1([&] {
            if (rc.TryEvict()) evict_won.store(true);
        });
        std::thread t2([&] {
            if (rc.TryAcquireIfNonZero()) acquire_won.store(true);
        });
        t1.join();
        t2.join();
        // Exactly one outcome consistent with linearisability:
        //  * evictor first: evict_won=true, acquire_won=false, count=0.
        //  * acquirer first: acquire_won=true, evict_won=false, count=2.
        if (evict_won.load()) {
            EXPECT_FALSE(acquire_won.load());
            EXPECT_EQ(rc.Load(), 0u);
        } else {
            EXPECT_TRUE(acquire_won.load());
            EXPECT_EQ(rc.Load(), 2u);
        }
    }
}

TEST(RefcountTest, ConcurrentAcquireRelease) {
    Refcount rc;
    constexpr int kThreads = 8;
    constexpr int kIters   = 10000;
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t) {
        ts.emplace_back([&]{
            for (int i = 0; i < kIters; ++i) {
                rc.Acquire();
                rc.Release();
            }
        });
    }
    for (auto& th : ts) th.join();
    EXPECT_TRUE(rc.IsZero());
}
