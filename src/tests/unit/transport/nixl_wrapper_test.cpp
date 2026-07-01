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
    // A genuinely-unknown name — not "ucx"/"tcp", which are real backends
    // (ucx when built with -DKVCACHE_ENABLE_UCX=ON, Phase A1).
    BackendOptions o; o.name = "nonexistent-backend";
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

// ---- Phase S-5: segmented ScheduledPull -----------------------------------

// A transfer larger than the segment size must still produce
// byte-correct output — segmentation is transparent to the caller.
TEST(NixlWrapperTest, SegmentedScheduledPullIsByteCorrect) {
    NixlWrapper w(Loopback());
    w.SetMaxSegmentBytes(256);  // force many segments

    constexpr std::size_t kBytes = 4096;  // 16 segments
    std::vector<uint8_t> src(kBytes), dst(kBytes, 0);
    for (std::size_t i = 0; i < kBytes; ++i) {
        src[i] = static_cast<uint8_t>((i * 13 + 5) & 0xff);
    }
    std::string err;
    auto sk = w.Register(src.data(), src.size(), &err);
    auto dk = w.Register(dst.data(), dst.size(), &err);
    PullRequest r{dk, 0, sk, 0, kBytes};
    EXPECT_TRUE(w.ScheduledPull(r, Priority::P1, kSystemTenantHash, 2000, &err))
        << err;
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), kBytes), 0)
        << "segmented pull must reassemble the full buffer";
}

// Segmentation honours dst/src offsets: pull bytes [1024,2048) of src
// into [1024,2048) of dst, leave the rest untouched.
TEST(NixlWrapperTest, SegmentedScheduledPullRespectsOffsets) {
    NixlWrapper w(Loopback());
    w.SetMaxSegmentBytes(128);

    constexpr std::size_t kBytes = 4096;
    std::vector<uint8_t> src(kBytes, 0xEE), dst(kBytes, 0x11);
    std::string err;
    auto sk = w.Register(src.data(), src.size(), &err);
    auto dk = w.Register(dst.data(), dst.size(), &err);
    // 1 KiB starting at offset 1024.
    PullRequest r{dk, 1024, sk, 1024, 1024};
    EXPECT_TRUE(w.ScheduledPull(r, Priority::P1, kSystemTenantHash, 2000, &err))
        << err;
    for (std::size_t i = 0; i < kBytes; ++i) {
        const uint8_t want = (i >= 1024 && i < 2048) ? 0xEE : 0x11;
        ASSERT_EQ(dst[i], want) << "byte " << i << " wrong after segmented pull";
    }
}

// Setting segment size to 0 disables segmentation (one scheduler item);
// the result is still correct.
TEST(NixlWrapperTest, ZeroSegmentSizeDisablesSegmentation) {
    NixlWrapper w(Loopback());
    w.SetMaxSegmentBytes(0);
    EXPECT_EQ(w.MaxSegmentBytes(), 0u);
    std::vector<uint8_t> src(1000, 0xAB), dst(1000, 0);
    std::string err;
    auto sk = w.Register(src.data(), src.size(), &err);
    auto dk = w.Register(dst.data(), dst.size(), &err);
    PullRequest r{dk, 0, sk, 0, 1000};
    EXPECT_TRUE(w.ScheduledPull(r, Priority::P1, kSystemTenantHash, 1000, &err))
        << err;
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), 1000), 0);
}

// ---- Phase S-6: concurrent + segmented stress (deadlock regression) -------

// Reproduces the notify-after-destroy deadlock fixed in S-6: many
// threads each issue segmented ScheduledPulls in a tight loop, so the
// dispatcher cycles submit→complete→destroy at a high rate. Before the
// fix (dispatcher notified pp->cv AFTER releasing pp->mu, allowing the
// woken caller to destroy the stack PendingXfer before the notify),
// this wedged within a fraction of a second. The test must finish all
// transfers — a hang here is the regression.
TEST(NixlWrapperTest, ConcurrentSegmentedStressDoesNotDeadlock) {
    NixlWrapper w(Loopback());
    w.SetMaxSegmentBytes(4096);  // tiny segments → max submit/complete churn

    constexpr int kThreads = 6;
    constexpr int kIters   = 40;
    constexpr std::size_t kBytes = 256 * 1024;  // 64 segments each

    std::vector<std::vector<uint8_t>> src(kThreads,
                                           std::vector<uint8_t>(kBytes, 0xC3));
    std::vector<std::vector<uint8_t>> dst(kThreads,
                                           std::vector<uint8_t>(kBytes, 0));
    std::vector<MrKey> sk(kThreads), dk(kThreads);
    {
        std::string err;
        for (int i = 0; i < kThreads; ++i) {
            sk[i] = w.Register(src[i].data(), src[i].size(), &err);
            dk[i] = w.Register(dst[i].data(), dst[i].size(), &err);
        }
    }

    std::atomic<int> ok_threads{0};
    std::vector<std::thread> ts;
    for (int i = 0; i < kThreads; ++i) {
        ts.emplace_back([&, i] {
            for (int it = 0; it < kIters; ++it) {
                PullRequest r{dk[i], 0, sk[i], 0, kBytes};
                std::string e;
                if (!w.ScheduledPull(r, Priority::P1,
                                      static_cast<uint64_t>(i), 5000, &e)) {
                    return;  // leave ok_threads unincremented → test fails
                }
            }
            ok_threads.fetch_add(1);
        });
    }
    for (auto& t : ts) t.join();

    EXPECT_EQ(ok_threads.load(), kThreads)
        << "a thread failed or the dispatcher deadlocked under "
           "concurrent segmented load";
    // Spot-check correctness of the last writer's buffer.
    for (std::size_t b = 0; b < kBytes; ++b) {
        ASSERT_EQ(dst[kThreads - 1][b], 0xC3) << "byte " << b << " wrong";
    }
}
