// LLD §3.2 / §7.3 — ArtSnapshot round-trip and integrity tests.
//
// What we verify:
//   1. Empty tree round-trips cleanly.
//   2. Populated tree: every Insert is visible via Lookup on the restored
//      tree, with leaf payload byte-for-byte identical.
//   3. Restored tree's stat counters (LeafCount, NodeCount) match the
//      original.
//   4. The file format catches bit-flips: corrupting one body byte makes
//      Read fail with a checksum mismatch.
//   5. Magic / version mismatches are caught.
#include "prefix/art_snapshot.h"

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <random>
#include <vector>

#include "prefix/art_index.h"

namespace fs = std::filesystem;
using namespace kvcache::node::prefix;

namespace {

// Build a ChunkHash with a given byte pattern, useful for constructing
// distinct paths.
ChunkHash MakeHash(uint8_t b0, uint8_t b1 = 0, uint8_t b2 = 0,
                   uint8_t b3 = 0, uint8_t b4 = 0, uint8_t b5 = 0,
                   uint8_t b6 = 0, uint8_t b7 = 0) {
    return {b0, b1, b2, b3, b4, b5, b6, b7};
}

// Build a LeafData with deterministic content keyed by `seed`.
std::unique_ptr<LeafData> MakeLeaf(uint64_t seed) {
    auto d = std::make_unique<LeafData>();
    // Locator is a packed POD — write through bytes deterministically.
    auto* raw = reinterpret_cast<uint8_t*>(&d->locator);
    for (std::size_t i = 0; i < sizeof(d->locator); ++i) {
        raw[i] = static_cast<uint8_t>((seed * 31 + i) & 0xff);
    }
    d->tier_residency_bitmap = static_cast<uint32_t>(0x1 | (seed << 1));
    d->sealed_at_nanos       = seed * 1'000'000;
    d->last_access_nanos.store(seed * 999, std::memory_order_relaxed);
    d->bytes_total           = seed * 4096;
    d->refcount.Reset(static_cast<uint32_t>(seed % 7));
    return d;
}

// Compare two LeafData byte-by-byte over the locator + scalar fields.
::testing::AssertionResult LeafEqual(const LeafData& a, const LeafData& b) {
    if (std::memcmp(&a.locator, &b.locator, sizeof(a.locator)) != 0) {
        return ::testing::AssertionFailure() << "locator mismatch";
    }
    if (a.tier_residency_bitmap != b.tier_residency_bitmap)
        return ::testing::AssertionFailure() << "tier_residency_bitmap "
            << a.tier_residency_bitmap << " vs " << b.tier_residency_bitmap;
    if (a.sealed_at_nanos != b.sealed_at_nanos)
        return ::testing::AssertionFailure() << "sealed_at_nanos";
    if (a.last_access_nanos.load() != b.last_access_nanos.load())
        return ::testing::AssertionFailure() << "last_access_nanos";
    if (a.bytes_total != b.bytes_total)
        return ::testing::AssertionFailure() << "bytes_total";
    if (a.refcount.Load() != b.refcount.Load())
        return ::testing::AssertionFailure() << "refcount";
    return ::testing::AssertionSuccess();
}

// Per-test temp path so parallel ctest runs don't collide.
std::string TempPath(const std::string& name) {
    auto p = fs::temp_directory_path() / ("kvcache_art_snapshot_" + name +
                                          "_" + std::to_string(::getpid()));
    fs::remove(p);
    return p.string();
}

}  // namespace

TEST(ArtSnapshotTest, EmptyTreeRoundTrip) {
    ArtIndex art;
    std::string path = TempPath("empty");

    ArtSnapshot::WriteStats ws;
    std::string err;
    ASSERT_TRUE(ArtSnapshot::Write(art, path, &ws, &err)) << err;
    EXPECT_EQ(ws.leaves, 0u);
    EXPECT_EQ(ws.inner_nodes, 1u);  // root counts
    EXPECT_GT(ws.bytes_written, 0u);

    ArtSnapshot::ReadStats rs;
    auto restored = ArtSnapshot::Read(path, &rs, &err);
    ASSERT_NE(restored, nullptr) << err;
    EXPECT_EQ(rs.leaves, 0u);
    EXPECT_EQ(rs.inner_nodes, 1u);
    EXPECT_EQ(restored->LeafCount(), 0u);

    fs::remove(path);
}

TEST(ArtSnapshotTest, SingleLeafRoundTrip) {
    ArtIndex art;
    std::vector<ChunkHash> path1{MakeHash(0x10, 1, 2, 3, 4, 5, 6, 7)};
    auto leaf_in = MakeLeaf(42);
    LeafData expected;
    std::memcpy(&expected.locator, &leaf_in->locator,
                sizeof(expected.locator));
    expected.tier_residency_bitmap = leaf_in->tier_residency_bitmap;
    expected.sealed_at_nanos       = leaf_in->sealed_at_nanos;
    expected.last_access_nanos.store(leaf_in->last_access_nanos.load());
    expected.bytes_total           = leaf_in->bytes_total;
    expected.refcount.Reset(leaf_in->refcount.Load());

    ASSERT_EQ(art.Insert(path1, std::move(leaf_in)),
              ArtIndex::InsertResult::kInserted);

    std::string fpath = TempPath("single");
    std::string err;
    ASSERT_TRUE(ArtSnapshot::Write(art, fpath, nullptr, &err)) << err;

    auto restored = ArtSnapshot::Read(fpath, nullptr, &err);
    ASSERT_NE(restored, nullptr) << err;
    EXPECT_EQ(restored->LeafCount(), 1u);

    auto guard = restored->EnterRead();
    auto lr = restored->Lookup(path1, guard);
    ASSERT_EQ(lr.matched_chunks, 1u);
    ASSERT_NE(lr.leaf, nullptr);
    EXPECT_TRUE(LeafEqual(*lr.leaf, expected));

    fs::remove(fpath);
}

TEST(ArtSnapshotTest, ManyLeavesRoundTrip) {
    ArtIndex art;
    // 1024 distinct 2-chunk paths. Two-chunk paths exercise both the
    // inner-node branching and the terminal-leaf serialization.
    std::vector<std::vector<ChunkHash>> paths;
    std::mt19937_64 rng(7);
    for (int i = 0; i < 1024; ++i) {
        ChunkHash h0{};
        ChunkHash h1{};
        for (auto& b : h0) b = static_cast<uint8_t>(rng() & 0xff);
        for (auto& b : h1) b = static_cast<uint8_t>(rng() & 0xff);
        paths.push_back({h0, h1});
        auto leaf = MakeLeaf(static_cast<uint64_t>(i + 1));
        // Some paths may collide on slot 0 with mismatched edge tails;
        // skip the Insert if so — we still get a large random tree.
        auto r = art.Insert(paths.back(), std::move(leaf));
        if (r == ArtIndex::InsertResult::kPathConflict) {
            paths.pop_back();
        }
    }
    // The Node256-only MVP collides on slot h0[0] (256 distinct values)
    // for random hashes — birthday math caps the unique tree at ~256
    // paths. That's plenty to exercise the recursive serializer.
    ASSERT_GT(paths.size(), 200u);

    std::string fpath = TempPath("many");
    std::string err;
    ArtSnapshot::WriteStats ws;
    ASSERT_TRUE(ArtSnapshot::Write(art, fpath, &ws, &err)) << err;

    auto restored = ArtSnapshot::Read(fpath, nullptr, &err);
    ASSERT_NE(restored, nullptr) << err;
    EXPECT_EQ(restored->LeafCount(), art.LeafCount());
    EXPECT_EQ(restored->NodeCount(), art.NodeCount());

    auto g = restored->EnterRead();
    for (std::size_t i = 0; i < paths.size(); ++i) {
        auto lr = restored->Lookup(paths[i], g);
        ASSERT_EQ(lr.matched_chunks, paths[i].size()) << "path " << i;
        ASSERT_NE(lr.leaf, nullptr);
    }

    fs::remove(fpath);
}

TEST(ArtSnapshotTest, BodyChecksumMismatchRejected) {
    ArtIndex art;
    std::vector<ChunkHash> p1{MakeHash(0x01, 9, 8, 7, 6, 5, 4, 3)};
    art.Insert(p1, MakeLeaf(1));
    std::string fpath = TempPath("corrupt");
    std::string err;
    ASSERT_TRUE(ArtSnapshot::Write(art, fpath, nullptr, &err));

    // Flip one byte well past the header.
    {
        std::fstream f(fpath, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(80);
        char byte = 0;
        f.read(&byte, 1);
        byte ^= 0x55;
        f.seekp(80);
        f.write(&byte, 1);
    }
    auto restored = ArtSnapshot::Read(fpath, nullptr, &err);
    EXPECT_EQ(restored, nullptr);
    EXPECT_NE(err.find("checksum"), std::string::npos) << "err=" << err;

    fs::remove(fpath);
}

TEST(ArtSnapshotTest, BadMagicRejected) {
    std::string fpath = TempPath("badmagic");
    {
        std::ofstream f(fpath, std::ios::binary);
        const char garbage[80] = {'X','X','X','X', 0};
        f.write(garbage, sizeof(garbage));
    }
    std::string err;
    EXPECT_EQ(ArtSnapshot::Read(fpath, nullptr, &err), nullptr);
    EXPECT_NE(err.find("magic"), std::string::npos);
    fs::remove(fpath);
}

TEST(ArtSnapshotTest, UnsupportedVersionRejected) {
    ArtIndex art;
    std::string fpath = TempPath("badver");
    ASSERT_TRUE(ArtSnapshot::Write(art, fpath, nullptr, nullptr));

    // Bump the version field (at offset 4).
    {
        std::fstream f(fpath, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(4);
        uint32_t v = 9999;
        f.write(reinterpret_cast<char*>(&v), 4);
    }
    std::string err;
    EXPECT_EQ(ArtSnapshot::Read(fpath, nullptr, &err), nullptr);
    EXPECT_NE(err.find("version"), std::string::npos);
    fs::remove(fpath);
}

TEST(ArtSnapshotTest, MissingFileFailsCleanly) {
    std::string fpath = TempPath("nope");
    fs::remove(fpath);
    std::string err;
    EXPECT_EQ(ArtSnapshot::Read(fpath, nullptr, &err), nullptr);
    EXPECT_FALSE(err.empty());
}
