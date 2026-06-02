// Phase A1.3 — RoutingCache tests.
//
// Covers:
//   1. Put then Get returns the stored node_id.
//   2. Get on miss returns nullopt and bumps misses.
//   3. TTL expiry drops + reports as miss + bumps expirations counter.
//   4. LRU eviction kicks in at capacity.
//   5. Invalidate drops the entry.
//   6. Concurrent Put/Get from multiple threads is safe (no crash,
//      stats counters add up).
#include "routing_cache/routing_cache.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace kvcache::agent::routing_cache;

namespace {
Key MakeKey(std::string t, std::string p) {
    return Key{std::move(t), std::move(p)};
}
}  // namespace

TEST(RoutingCacheTest, PutThenGet) {
    RoutingCache c({.capacity = 16, .ttl = std::chrono::seconds(10)});
    c.Put(MakeKey("t", "abc"), "node-1");
    auto got = c.Get(MakeKey("t", "abc"));
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "node-1");
    auto stats = c.SnapshotStats();
    EXPECT_EQ(stats.puts, 1u);
    EXPECT_EQ(stats.hits, 1u);
    EXPECT_EQ(stats.misses, 0u);
}

TEST(RoutingCacheTest, GetMissReturnsNullopt) {
    RoutingCache c;
    EXPECT_FALSE(c.Get(MakeKey("t", "xyz")).has_value());
    EXPECT_EQ(c.SnapshotStats().misses, 1u);
}

TEST(RoutingCacheTest, TtlExpiryDropsEntry) {
    RoutingCache c({.capacity = 16, .ttl = std::chrono::milliseconds(20)});
    c.Put(MakeKey("t", "k"), "n");
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_FALSE(c.Get(MakeKey("t", "k")).has_value());
    auto s = c.SnapshotStats();
    EXPECT_EQ(s.expirations, 1u);
    EXPECT_EQ(s.misses, 1u);
    EXPECT_EQ(s.size, 0u);
}

TEST(RoutingCacheTest, LruEvictionAtCapacity) {
    RoutingCache c({.capacity = 3, .ttl = std::chrono::seconds(10)});
    c.Put(MakeKey("t", "a"), "1");
    c.Put(MakeKey("t", "b"), "2");
    c.Put(MakeKey("t", "c"), "3");
    // Promote 'a' to MRU.
    (void)c.Get(MakeKey("t", "a"));
    // Insert 'd' — should evict 'b' (now LRU).
    c.Put(MakeKey("t", "d"), "4");
    EXPECT_TRUE(c.Get(MakeKey("t", "a")).has_value());
    EXPECT_FALSE(c.Get(MakeKey("t", "b")).has_value());  // evicted
    EXPECT_TRUE(c.Get(MakeKey("t", "c")).has_value());
    EXPECT_TRUE(c.Get(MakeKey("t", "d")).has_value());
    EXPECT_EQ(c.SnapshotStats().evictions, 1u);
}

TEST(RoutingCacheTest, InvalidateDropsEntry) {
    RoutingCache c;
    c.Put(MakeKey("t", "k"), "n");
    c.Invalidate(MakeKey("t", "k"));
    EXPECT_FALSE(c.Get(MakeKey("t", "k")).has_value());
    // Invalidating an absent key is fine.
    c.Invalidate(MakeKey("t", "missing"));
}

TEST(RoutingCacheTest, ConcurrentPutGetIsSafe) {
    RoutingCache c({.capacity = 256, .ttl = std::chrono::seconds(60)});
    constexpr int kThreads  = 8;
    constexpr int kPerThread = 2000;
    std::atomic<int> ops_done{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t) {
        ts.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                std::string p = "p" + std::to_string(t) + "-" + std::to_string(i & 31);
                c.Put(MakeKey("t", p), "n" + std::to_string(t));
                (void)c.Get(MakeKey("t", p));
                ops_done.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& th : ts) th.join();
    EXPECT_EQ(ops_done.load(), kThreads * kPerThread);
    auto s = c.SnapshotStats();
    EXPECT_EQ(s.puts, static_cast<uint64_t>(kThreads * kPerThread));
    EXPECT_LE(s.size, 256u);  // capped at capacity
}
