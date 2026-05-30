#include "prefix/refcount.h"

#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "log_sink.h"
#include "logging.h"

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

// Phase O-2 — Release()-under-flow now routes through kvcache::log.
// Drive Release() once with a zero counter and assert the sink captured
// an Error record from the "refcount" subsystem with the leaf address
// embedded in the message.
namespace {
class CapturingSink : public kvcache::log::sink::Logger {
   public:
    struct Rec { kvcache::log::sink::LogLevel level; std::string msg; };
    void Log(kvcache::log::sink::LogLevel level, const char*, int,
              const std::string& msg) override {
        std::lock_guard lk(mu_);
        recs_.push_back({level, msg});
    }
    std::vector<Rec> snapshot() {
        std::lock_guard lk(mu_); return recs_;
    }
   private:
    std::mutex mu_;
    std::vector<Rec> recs_;
};
}  // namespace

TEST(RefcountTest, UnderflowRoutesThroughLoggingFacade) {
    auto cap = std::make_shared<CapturingSink>();
    kvcache::log::sink::SetDefault(cap);
    kvcache::log::Init({.level = kvcache::log::Level::Trace});
    kvcache::log::sink::SetDefault(cap);  // Init() reinstalls Console

    Refcount rc;  // starts at 0
    // Under-flow: prev = 0, Release() returns wraparound (UINT32_MAX).
    EXPECT_EQ(rc.Release(), static_cast<uint32_t>(-1));

    auto recs = cap->snapshot();
    ASSERT_FALSE(recs.empty());
    EXPECT_EQ(recs[0].level, kvcache::log::sink::LogLevel::kError);
    EXPECT_NE(recs[0].msg.find("[refcount]"), std::string::npos);
    EXPECT_NE(recs[0].msg.find("under-flow"), std::string::npos);
}
