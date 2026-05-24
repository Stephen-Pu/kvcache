#include "transport/nixl_wrapper.h"

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

using namespace kvcache::node::transport;

namespace {
std::unique_ptr<INixlBackend> Loopback() {
    BackendOptions o; o.name = "loopback";
    std::string err;
    auto b = CreateBackend(o, &err);
    EXPECT_TRUE(b) << err;
    return b;
}
}  // namespace

TEST(NixlBackendTest, UnknownBackendFails) {
    BackendOptions o; o.name = "ucx";
    std::string err;
    auto b = CreateBackend(o, &err);
    EXPECT_EQ(b, nullptr);
    EXPECT_FALSE(err.empty());
}

TEST(NixlBackendTest, RegisterResolveUnregister) {
    auto b = Loopback();
    std::vector<uint8_t> buf(1024, 0xAB);
    std::string err;
    auto key = b->RegisterRegion(buf.data(), buf.size(), &err);
    ASSERT_NE(key, kInvalidMrKey) << err;

    void* addr = nullptr;
    std::size_t bytes = 0;
    ASSERT_TRUE(b->ResolveRegion(key, &addr, &bytes));
    EXPECT_EQ(addr, buf.data());
    EXPECT_EQ(bytes, buf.size());

    b->UnregisterRegion(key);
    EXPECT_FALSE(b->ResolveRegion(key, &addr, &bytes));
}

TEST(NixlBackendTest, PullCopiesBytes) {
    auto b = Loopback();
    std::vector<uint8_t> src(1024, 0xCD);
    std::vector<uint8_t> dst(1024, 0x00);
    std::string err;
    auto sk = b->RegisterRegion(src.data(), src.size(), &err);
    auto dk = b->RegisterRegion(dst.data(), dst.size(), &err);

    PullRequest req{dk, 0, sk, 0, 1024};
    auto cid = b->Pull(req, &err);
    ASSERT_NE(cid, kInvalidCompletionId) << err;
    EXPECT_TRUE(b->Wait(cid, 1000, &err));
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), 1024), 0);
}

TEST(NixlBackendTest, PullRejectsOutOfBounds) {
    auto b = Loopback();
    std::vector<uint8_t> src(100), dst(100);
    std::string err;
    auto sk = b->RegisterRegion(src.data(), src.size(), &err);
    auto dk = b->RegisterRegion(dst.data(), dst.size(), &err);
    PullRequest req{dk, 0, sk, 50, 100};  // src_off + bytes > 100
    EXPECT_EQ(b->Pull(req, &err), kInvalidCompletionId);
    EXPECT_FALSE(err.empty());
}

TEST(NixlWrapperTest, PullSyncRoundTrip) {
    NixlWrapper w(Loopback());
    std::vector<uint8_t> src(64, 0x55), dst(64, 0x00);
    std::string err;
    auto sk = w.Register(src.data(), src.size(), &err);
    auto dk = w.Register(dst.data(), dst.size(), &err);
    PullRequest r{dk, 0, sk, 0, 64};
    EXPECT_TRUE(w.PullSync(r, 1000, &err)) << err;
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), 64), 0);
}

// ---- Phase E-2: ScheduledPull ---------------------------------------------

TEST(NixlWrapperTest, ScheduledPullSingleCallerLoopback) {
    NixlWrapper w(Loopback());
    std::vector<uint8_t> src(256, 0x77), dst(256, 0x00);
    std::string err;
    auto sk = w.Register(src.data(), src.size(), &err);
    auto dk = w.Register(dst.data(), dst.size(), &err);
    PullRequest r{dk, 0, sk, 0, 256};
    EXPECT_TRUE(w.ScheduledPull(r, Priority::P1, kSystemTenantHash, 1000, &err))
        << err;
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), 256), 0);
}

TEST(NixlWrapperTest, ScheduledPullConcurrentCallersAllSucceed) {
    NixlWrapper w(Loopback());
    constexpr int kN = 32;
    std::vector<std::vector<uint8_t>> srcs(kN, std::vector<uint8_t>(64, 0));
    std::vector<std::vector<uint8_t>> dsts(kN, std::vector<uint8_t>(64, 0));
    for (int i = 0; i < kN; ++i) {
        for (auto& b : srcs[i]) b = static_cast<uint8_t>(i & 0xff);
    }

    std::vector<MrKey> sk(kN), dk(kN);
    {
        std::string err;
        for (int i = 0; i < kN; ++i) {
            sk[i] = w.Register(srcs[i].data(), srcs[i].size(), &err);
            dk[i] = w.Register(dsts[i].data(), dsts[i].size(), &err);
        }
    }

    std::atomic<int> ok_count{0};
    std::vector<std::thread> ts;
    for (int i = 0; i < kN; ++i) {
        ts.emplace_back([&, i] {
            std::string err;
            PullRequest r{dk[i], 0, sk[i], 0, 64};
            const auto prio = static_cast<Priority>(i % 3);
            // Spread across two synthetic tenants so per-tenant RR can do
            // something interesting under load.
            const uint64_t tenant = (i & 1) ? 0xAull : 0xBull;
            if (w.ScheduledPull(r, prio, tenant, 5000, &err)) {
                ++ok_count;
            }
        });
    }
    for (auto& t : ts) t.join();

    EXPECT_EQ(ok_count.load(), kN);
    for (int i = 0; i < kN; ++i) {
        EXPECT_EQ(std::memcmp(srcs[i].data(), dsts[i].data(), 64), 0)
            << "thread " << i;
    }
    // Scheduler ends quiescent.
    EXPECT_EQ(w.scheduler().QueueDepth(Priority::P0), 0u);
    EXPECT_EQ(w.scheduler().QueueDepth(Priority::P1), 0u);
    EXPECT_EQ(w.scheduler().QueueDepth(Priority::P2), 0u);
    EXPECT_EQ(w.scheduler().InFlightBytes(Priority::P0), 0u);
    EXPECT_EQ(w.scheduler().InFlightBytes(Priority::P1), 0u);
    EXPECT_EQ(w.scheduler().InFlightBytes(Priority::P2), 0u);
    // Every call went through the scheduler (no PullSync backdoor here),
    // so the per-priority counters must sum to kN.
    EXPECT_EQ(w.scheduler().NormalAdmissions() + w.scheduler().ForcedAdmissions(),
              static_cast<uint64_t>(kN));
}

// ---- Phase M-7: ScheduledPush --------------------------------------------

TEST(NixlWrapperTest, ScheduledPushRoutesThroughScheduler) {
    NixlWrapper w(Loopback());
    std::vector<uint8_t> src(256, 0xAA), dst(256, 0x00);
    std::string err;
    auto sk = w.Register(src.data(), src.size(), &err);
    auto dk = w.Register(dst.data(), dst.size(), &err);

    const auto admits_before =
        w.scheduler().NormalAdmissions() + w.scheduler().ForcedAdmissions();

    // Loopback's Push is a memcpy in the same direction as Pull, so the
    // observable side effect is the same: dst gets the src bytes. What
    // we're verifying here is that ScheduledPush walked through the
    // PriorityScheduler dispatcher, not a backend back-door.
    PushRequest r{sk, 0, dk, 0, 256};
    EXPECT_TRUE(w.ScheduledPush(r, Priority::P1, kSystemTenantHash, 1000, &err))
        << err;
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), 256), 0);

    const auto admits_after =
        w.scheduler().NormalAdmissions() + w.scheduler().ForcedAdmissions();
    EXPECT_EQ(admits_after - admits_before, 1u)
        << "ScheduledPush did not submit through the scheduler";

    // Scheduler ends quiescent.
    EXPECT_EQ(w.scheduler().QueueDepth(Priority::P1), 0u);
    EXPECT_EQ(w.scheduler().InFlightBytes(Priority::P1), 0u);
}

TEST(NixlWrapperTest, ScheduledPushMixedWithPullDrainsAll) {
    NixlWrapper w(Loopback());
    constexpr int kN = 24;  // half Pulls, half Pushes
    std::vector<std::vector<uint8_t>> srcs(kN, std::vector<uint8_t>(64, 0));
    std::vector<std::vector<uint8_t>> dsts(kN, std::vector<uint8_t>(64, 0));
    for (int i = 0; i < kN; ++i) {
        for (auto& b : srcs[i]) b = static_cast<uint8_t>((i * 7) & 0xff);
    }
    std::vector<MrKey> sk(kN), dk(kN);
    {
        std::string err;
        for (int i = 0; i < kN; ++i) {
            sk[i] = w.Register(srcs[i].data(), srcs[i].size(), &err);
            dk[i] = w.Register(dsts[i].data(), dsts[i].size(), &err);
        }
    }

    std::atomic<int> ok{0};
    std::vector<std::thread> ts;
    for (int i = 0; i < kN; ++i) {
        ts.emplace_back([&, i] {
            std::string err;
            const auto prio = static_cast<Priority>(i % 3);
            const uint64_t tenant = (i & 1) ? 0xAAull : 0xBBull;
            if (i & 1) {  // odd: Pull
                PullRequest r{dk[i], 0, sk[i], 0, 64};
                if (w.ScheduledPull(r, prio, tenant, 5000, &err)) ++ok;
            } else {       // even: Push
                PushRequest r{sk[i], 0, dk[i], 0, 64};
                if (w.ScheduledPush(r, prio, tenant, 5000, &err)) ++ok;
            }
        });
    }
    for (auto& t : ts) t.join();

    EXPECT_EQ(ok.load(), kN);
    for (int i = 0; i < kN; ++i) {
        EXPECT_EQ(std::memcmp(srcs[i].data(), dsts[i].data(), 64), 0)
            << "thread " << i;
    }
    EXPECT_EQ(w.scheduler().QueueDepth(Priority::P0), 0u);
    EXPECT_EQ(w.scheduler().QueueDepth(Priority::P1), 0u);
    EXPECT_EQ(w.scheduler().QueueDepth(Priority::P2), 0u);
    EXPECT_EQ(w.scheduler().NormalAdmissions() + w.scheduler().ForcedAdmissions(),
              static_cast<uint64_t>(kN));
}

TEST(NixlWrapperTest, ScheduledPullCustomSchedulerOpts) {
    // Construct with a non-default scheduler config; verify the wrapper
    // honours it (a small Pull still goes through, and the scheduler is
    // reachable via the public accessor).
    PriorityScheduler::Options so;
    so.total_window_bytes = 1 << 16;
    so.p0_pct = 50; so.p1_pct = 40; so.p2_pct = 10;
    NixlWrapper w(Loopback(), so);

    std::vector<uint8_t> src(32, 0xAB), dst(32, 0);
    std::string err;
    auto sk = w.Register(src.data(), src.size(), &err);
    auto dk = w.Register(dst.data(), dst.size(), &err);
    PullRequest r{dk, 0, sk, 0, 32};
    EXPECT_TRUE(w.ScheduledPull(r, Priority::P0, kSystemTenantHash, 1000, &err));
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), 32), 0);
    EXPECT_EQ(w.scheduler().Reserved(Priority::P0), (1u << 16) / 2);
}
