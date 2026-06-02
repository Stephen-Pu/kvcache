#include "tier/nvme_tier.h"

#include <gtest/gtest.h>
#include <cstring>
#include <filesystem>
#include <vector>

using kvcache::node::tier::DramKey;
using kvcache::node::tier::NvmeTier;

namespace {
DramKey Key(uint8_t b) {
    DramKey k{};
    k.bytes[0] = b;
    return k;
}
std::string TmpPath(const char* tag) {
    auto p = std::filesystem::temp_directory_path() /
             (std::string("kvcache_nvme_") + tag);
    std::filesystem::remove(p);
    return p.string();
}
}  // namespace

TEST(NvmeTierTest, RejectsBadOptions) {
    NvmeTier::Options o;
    o.path = TmpPath("bad");
    o.pool_bytes = 100;
    o.slot_bytes = 64;  // not a divisor
    std::string err;
    EXPECT_EQ(NvmeTier::Create(o, &err), nullptr);
    EXPECT_FALSE(err.empty());
}

TEST(NvmeTierTest, PutGetRoundTrip) {
    NvmeTier::Options o;
    o.path       = TmpPath("rt");
    o.pool_bytes = 4 * 4096;
    o.slot_bytes = 4096;
    std::string err;
    auto t = NvmeTier::Create(o, &err);
    ASSERT_NE(t, nullptr) << err;

    std::vector<uint8_t> data(1024, 0xCA);
    EXPECT_TRUE(t->Put(Key(1), data.data(), data.size(), &err)) << err;
    EXPECT_TRUE(t->Contains(Key(1)));

    std::vector<uint8_t> out;
    EXPECT_TRUE(t->Get(Key(1), &out, &err)) << err;
    EXPECT_EQ(out, data);
    std::filesystem::remove(o.path);
}

TEST(NvmeTierTest, OverwriteReusesSlot) {
    NvmeTier::Options o;
    o.path       = TmpPath("ow");
    o.pool_bytes = 4096;
    o.slot_bytes = 4096;
    std::string err;
    auto t = NvmeTier::Create(o, &err);
    ASSERT_NE(t, nullptr);

    std::vector<uint8_t> a(100, 1), b(200, 2);
    EXPECT_TRUE(t->Put(Key(1), a.data(), a.size(), &err));
    EXPECT_TRUE(t->Put(Key(1), b.data(), b.size(), &err));
    EXPECT_EQ(t->SlotsInUse(), 1u);

    std::vector<uint8_t> out;
    EXPECT_TRUE(t->Get(Key(1), &out, &err));
    EXPECT_EQ(out, b);
    std::filesystem::remove(o.path);
}

TEST(NvmeTierTest, EraseFreesSlot) {
    NvmeTier::Options o;
    o.path       = TmpPath("er");
    o.pool_bytes = 4096;
    o.slot_bytes = 4096;
    std::string err;
    auto t = NvmeTier::Create(o, &err);
    ASSERT_NE(t, nullptr);
    std::vector<uint8_t> v(50, 9);
    t->Put(Key(7), v.data(), v.size(), &err);
    EXPECT_EQ(t->SlotsInUse(), 1u);
    EXPECT_TRUE(t->Erase(Key(7)));
    EXPECT_EQ(t->SlotsInUse(), 0u);
    EXPECT_FALSE(t->Contains(Key(7)));
    std::filesystem::remove(o.path);
}

TEST(NvmeTierTest, FullPoolRejectsNewKeys) {
    NvmeTier::Options o;
    o.path       = TmpPath("full");
    o.pool_bytes = 4096;
    o.slot_bytes = 4096;
    std::string err;
    auto t = NvmeTier::Create(o, &err);
    ASSERT_NE(t, nullptr);
    std::vector<uint8_t> v(100, 1);
    EXPECT_TRUE(t->Put(Key(1), v.data(), v.size(), &err));
    EXPECT_FALSE(t->Put(Key(2), v.data(), v.size(), &err));
    EXPECT_FALSE(err.empty());
    std::filesystem::remove(o.path);
}

TEST(NvmeTierTest, GetMissingKeyFails) {
    NvmeTier::Options o;
    o.path       = TmpPath("miss");
    o.pool_bytes = 4096;
    o.slot_bytes = 4096;
    std::string err;
    auto t = NvmeTier::Create(o, &err);
    ASSERT_NE(t, nullptr);
    std::vector<uint8_t> out;
    EXPECT_FALSE(t->Get(Key(99), &out, &err));
    std::filesystem::remove(o.path);
}

// Phase B1 — io_uring path:
//
//   * UsingUring reports false by default (the blocking path is the
//     contract every existing test above already exercises).
//   * UringRoundTrip is gated on KVCACHE_ENABLE_URING + a non-zero
//     liburing being linked in. Asserts a full Put → Contains → Get
//     round-trip through the SQE submit/wait path and that the
//     payload comes back byte-identical.
//
// The gated test is skipped at runtime rather than `#if`-excluded so
// the file's TEST count is identical across build flavours — operators
// running ctest on a non-uring build see the test marked SKIPPED, not
// missing.

TEST(NvmeTierTest, UringDisabledByDefault) {
    NvmeTier::Options o;
    o.path       = TmpPath("uring-default");
    o.pool_bytes = 4 * 4096;
    o.slot_bytes = 4096;
    std::string err;
    auto t = NvmeTier::Create(o, &err);
    ASSERT_NE(t, nullptr) << err;
    EXPECT_FALSE(t->UsingUring());
    std::filesystem::remove(o.path);
}

TEST(NvmeTierTest, UringRoundTripWhenEnabled) {
#ifndef KVCACHE_ENABLE_URING
    GTEST_SKIP() << "built without KVCACHE_ENABLE_URING";
#else
    NvmeTier::Options o;
    o.path       = TmpPath("uring-rt");
    o.pool_bytes = 4 * 4096;
    o.slot_bytes = 4096;
    o.use_uring  = true;
    o.uring_queue_depth = 32;
    std::string err;
    auto t = NvmeTier::Create(o, &err);
    ASSERT_NE(t, nullptr) << err;
    ASSERT_TRUE(t->UsingUring())
        << "use_uring=true but UsingUring()=false; CMake wiring broken?";

    // Slightly varied payload so a memcpy bug shows up as a diff.
    std::vector<uint8_t> data(2048, 0);
    for (std::size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<uint8_t>(i * 37);
    }
    EXPECT_TRUE(t->Put(Key(42), data.data(), data.size(), &err)) << err;
    EXPECT_TRUE(t->Contains(Key(42)));

    std::vector<uint8_t> out;
    EXPECT_TRUE(t->Get(Key(42), &out, &err)) << err;
    EXPECT_EQ(out, data);
    std::filesystem::remove(o.path);
#endif
}
