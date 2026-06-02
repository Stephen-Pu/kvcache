// Phase A1.2 — Doorbell tests.
//
// Covers:
//   1. Ring-then-Wait returns 1 promptly.
//   2. Wait with no ring returns 0 on timeout.
//   3. Multiple Rings before one Wait coalesce into a single wake.
//   4. Cross-thread: producer Rings, consumer Waits — observed wake.
#include "shmem_ring/doorbell.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

using namespace kvcache::agent::shmem_ring;

TEST(DoorbellTest, RingThenWaitReturnsOne) {
    std::string err;
    auto d = Doorbell::Create(&err);
    ASSERT_NE(d, nullptr) << err;
    ASSERT_TRUE(d->Ring());
    EXPECT_EQ(d->Wait(/*timeout_ms=*/100), 1);
}

TEST(DoorbellTest, WaitNoRingTimesOut) {
    std::string err;
    auto d = Doorbell::Create(&err);
    ASSERT_NE(d, nullptr);
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(d->Wait(/*timeout_ms=*/50), 0);
    const auto elapsed = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(elapsed, std::chrono::milliseconds(45));
}

TEST(DoorbellTest, MultipleRingsCoalesceToOneWake) {
    std::string err;
    auto d = Doorbell::Create(&err);
    ASSERT_NE(d, nullptr);
    for (int i = 0; i < 10; ++i) ASSERT_TRUE(d->Ring());
    EXPECT_EQ(d->Wait(/*timeout_ms=*/100), 1);
    // After draining the first wake, the second Wait must time out
    // because everything was coalesced.
    EXPECT_EQ(d->Wait(/*timeout_ms=*/20), 0);
}

TEST(DoorbellTest, CrossThreadProducerConsumer) {
    std::string err;
    auto d = Doorbell::Create(&err);
    ASSERT_NE(d, nullptr);
    std::thread t([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        d->Ring();
    });
    EXPECT_EQ(d->Wait(/*timeout_ms=*/1000), 1);
    t.join();
}
