#include "tier/pinned_tier.h"

#include <gtest/gtest.h>
#include <cstring>
#include <vector>

#if KVCACHE_HAVE_CUDA
#include <cuda_runtime.h>
#endif

using kvcache::node::tier::PinnedTier;
using kvcache::node::tier::SlotDesc;

TEST(PinnedTierTest, RejectsBadOptions) {
    PinnedTier::Options bad;
    bad.pool_bytes = 100;
    bad.slot_bytes = 64;  // not a divisor of 100
    std::string err;
    EXPECT_EQ(PinnedTier::Create(bad, &err), nullptr);
    EXPECT_FALSE(err.empty());
}

TEST(PinnedTierTest, AcquireReleaseRoundTrip) {
    PinnedTier::Options o;
    o.pool_bytes = 4 * 4096;
    o.slot_bytes = 4096;
    o.use_mlock  = false;
    std::string err;
    auto t = PinnedTier::Create(o, &err);
    ASSERT_NE(t, nullptr) << err;
    EXPECT_EQ(t->SlotCount(), 4u);

    std::vector<SlotDesc> held;
    for (int i = 0; i < 4; ++i) {
        auto s = t->Acquire();
        ASSERT_TRUE(s.has_value());
        EXPECT_EQ(s->bytes, 4096u);
        held.push_back(*s);
    }
    EXPECT_FALSE(t->Acquire().has_value());

    for (auto& s : held) t->Release(s.id);
    EXPECT_TRUE(t->Acquire().has_value());
}

TEST(PinnedTierTest, SlotsAreWritable) {
    PinnedTier::Options o;
    o.pool_bytes = 4096;
    o.slot_bytes = 4096;
    o.use_mlock  = false;
    std::string err;
    auto t = PinnedTier::Create(o, &err);
    ASSERT_NE(t, nullptr);
    auto s = t->Acquire();
    ASSERT_TRUE(s.has_value());
    std::memset(s->addr, 0xAB, s->bytes);
    EXPECT_EQ(*static_cast<uint8_t*>(s->addr), 0xABu);
    t->Release(s->id);
}

TEST(PinnedTierTest, MrKeyComesFromCallback) {
    PinnedTier::Options o;
    o.pool_bytes = 4096;
    o.slot_bytes = 4096;
    o.use_mlock  = false;
    o.register_region = [](void*, uint64_t) -> uint32_t { return 0xDEADBEEF; };
    std::string err;
    auto t = PinnedTier::Create(o, &err);
    ASSERT_NE(t, nullptr);
    auto s = t->Acquire();
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(s->mr_key, 0xDEADBEEFu);
}

#if KVCACHE_HAVE_CUDA
// Phase A2 — when built with -DKVCACHE_ENABLE_CUDA=ON on a box with a GPU, the
// pool must be genuine cudaHostAlloc pinned+mapped host memory, not plain
// mmap. The distinguishing, hardware-observable property: cudaPointerGetAttributes
// reports it as a host allocation and cudaHostGetDevicePointer yields a valid
// device-side pointer (only possible for MAPPED pinned memory). A plain malloc
// / mmap pointer fails both. This is what makes zero-copy GPUDirect paths work.
TEST(PinnedTierCudaTest, PoolIsRealPinnedMappedHostMemory) {
    PinnedTier::Options o;
    o.pool_bytes = 2 * (16ull << 20);  // 2 slots
    o.slot_bytes = 16ull << 20;
    std::string err;
    auto t = PinnedTier::Create(o, &err);
    ASSERT_NE(t, nullptr) << err;
    auto s = t->Acquire();
    ASSERT_TRUE(s.has_value());

    // Host-writable like any slot.
    std::memset(s->addr, 0x5A, 4096);
    EXPECT_EQ(*static_cast<uint8_t*>(s->addr), 0x5Au);

    // It's registered pinned host memory (not pageable).
    cudaPointerAttributes attr{};
    ASSERT_EQ(cudaPointerGetAttributes(&attr, s->addr), cudaSuccess);
    EXPECT_EQ(attr.type, cudaMemoryTypeHost)
        << "pool must be CUDA host memory, got type " << attr.type;

    // MAPPED → a device pointer can be derived for zero-copy GPU access.
    void* dptr = nullptr;
    EXPECT_EQ(cudaHostGetDevicePointer(&dptr, s->addr, 0), cudaSuccess);
    EXPECT_NE(dptr, nullptr);

    t->Release(s->id);
}
#endif  // KVCACHE_HAVE_CUDA
