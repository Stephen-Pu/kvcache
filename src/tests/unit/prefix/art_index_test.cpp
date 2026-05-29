#include "prefix/art_index.h"

#include <gtest/gtest.h>
#include <memory>

using kvcache::node::prefix::ArtIndex;
using kvcache::node::prefix::ChunkHash;
using kvcache::node::prefix::LeafData;

namespace {

ChunkHash H(uint8_t a, uint8_t b = 0) {
    ChunkHash h{};
    h[0] = a;
    h[1] = b;
    return h;
}

std::unique_ptr<LeafData> MakeLeaf(uint64_t bytes) {
    auto l = std::make_unique<LeafData>();
    l->bytes_total = bytes;
    return l;
}

}  // namespace

TEST(ArtIndexTest, EmptyLookupReturnsZero) {
    ArtIndex art;
    auto g = art.EnterRead();
    std::vector<ChunkHash> path{H(1), H(2)};
    auto r = art.Lookup({path.data(), path.size()}, g);
    EXPECT_EQ(r.matched_chunks, 0u);
    EXPECT_EQ(r.leaf, nullptr);
}

TEST(ArtIndexTest, InsertThenExactLookup) {
    ArtIndex art;
    std::vector<ChunkHash> path{H(1), H(2), H(3)};
    auto res = art.Insert({path.data(), path.size()}, MakeLeaf(42));
    EXPECT_EQ(res, ArtIndex::InsertResult::kInserted);
    EXPECT_EQ(art.LeafCount(), 1u);

    auto g = art.EnterRead();
    auto r = art.Lookup({path.data(), path.size()}, g);
    EXPECT_EQ(r.matched_chunks, 3u);
    ASSERT_NE(r.leaf, nullptr);
    EXPECT_EQ(r.leaf->bytes_total, 42u);
}

TEST(ArtIndexTest, LongestPrefixMatch) {
    ArtIndex art;
    std::vector<ChunkHash> short_p{H(1), H(2)};
    art.Insert({short_p.data(), short_p.size()}, MakeLeaf(100));

    std::vector<ChunkHash> query{H(1), H(2), H(3), H(4)};
    auto g = art.EnterRead();
    auto r = art.Lookup({query.data(), query.size()}, g);
    EXPECT_EQ(r.matched_chunks, 2u);
    ASSERT_NE(r.leaf, nullptr);
    EXPECT_EQ(r.leaf->bytes_total, 100u);
}

TEST(ArtIndexTest, NoMatchOnDivergentFirstChunk) {
    ArtIndex art;
    std::vector<ChunkHash> stored{H(1), H(2)};
    art.Insert({stored.data(), stored.size()}, MakeLeaf(10));

    std::vector<ChunkHash> query{H(9), H(2)};
    auto g = art.EnterRead();
    auto r = art.Lookup({query.data(), query.size()}, g);
    EXPECT_EQ(r.matched_chunks, 0u);
}

TEST(ArtIndexTest, ReplaceUpdatesLeaf) {
    ArtIndex art;
    std::vector<ChunkHash> p{H(1)};
    art.Insert({p.data(), p.size()}, MakeLeaf(10));
    auto res = art.Insert({p.data(), p.size()}, MakeLeaf(20));
    EXPECT_EQ(res, ArtIndex::InsertResult::kReplaced);
    EXPECT_EQ(art.LeafCount(), 1u);

    auto g = art.EnterRead();
    auto r = art.Lookup({p.data(), p.size()}, g);
    ASSERT_NE(r.leaf, nullptr);
    EXPECT_EQ(r.leaf->bytes_total, 20u);
}

TEST(ArtIndexTest, RemoveLeaf) {
    ArtIndex art;
    std::vector<ChunkHash> p{H(1), H(2)};
    art.Insert({p.data(), p.size()}, MakeLeaf(10));
    EXPECT_TRUE(art.Remove({p.data(), p.size()}));
    EXPECT_EQ(art.LeafCount(), 0u);

    auto g = art.EnterRead();
    auto r = art.Lookup({p.data(), p.size()}, g);
    EXPECT_EQ(r.matched_chunks, 0u);
}

TEST(ArtIndexTest, ShallowAfterDeepAttachesEmbeddedLeaf) {
    // Phase D-5 — inserting "1" after "1,2" used to return kPathConflict
    // because the position at slot 1 was an Inner with children. Now the
    // shallow leaf attaches as an embedded_leaf on that Inner; both
    // paths resolve.
    ArtIndex art;
    std::vector<ChunkHash> deep{H(1), H(2)};
    std::vector<ChunkHash> shallow{H(1)};
    ASSERT_EQ(art.Insert({deep.data(), deep.size()}, MakeLeaf(10)),
                ArtIndex::InsertResult::kInserted);
    EXPECT_EQ(art.Insert({shallow.data(), shallow.size()}, MakeLeaf(20)),
                ArtIndex::InsertResult::kInserted);
    EXPECT_EQ(art.LeafCount(), 2u);

    auto g = art.EnterRead();
    auto r_deep = art.Lookup({deep.data(), deep.size()}, g);
    auto r_shallow = art.Lookup({shallow.data(), shallow.size()}, g);
    ASSERT_NE(r_deep.leaf, nullptr);
    ASSERT_NE(r_shallow.leaf, nullptr);
    EXPECT_EQ(r_deep.leaf->bytes_total, 10u);
    EXPECT_EQ(r_shallow.leaf->bytes_total, 20u);
    EXPECT_EQ(r_shallow.matched_chunks, 1u);
    EXPECT_EQ(r_deep.matched_chunks, 2u);
}

TEST(ArtIndexTest, DeepAfterShallowDemoteesLeafIntoInner) {
    // Phase D-5 — the inverse: Insert "1" then "1,2". The "1" leaf
    // sits at slot 1; the deeper insert promotes it into an Inner
    // whose embedded_leaf carries the original data, then attaches
    // "2" as a child.
    ArtIndex art;
    std::vector<ChunkHash> shallow{H(1)};
    std::vector<ChunkHash> deep{H(1), H(2)};
    ASSERT_EQ(art.Insert({shallow.data(), shallow.size()}, MakeLeaf(100)),
                ArtIndex::InsertResult::kInserted);
    EXPECT_EQ(art.Insert({deep.data(), deep.size()}, MakeLeaf(200)),
                ArtIndex::InsertResult::kInserted);
    EXPECT_EQ(art.LeafCount(), 2u);

    auto g = art.EnterRead();
    auto r_shallow = art.Lookup({shallow.data(), shallow.size()}, g);
    auto r_deep    = art.Lookup({deep.data(), deep.size()}, g);
    ASSERT_NE(r_shallow.leaf, nullptr);
    EXPECT_EQ(r_shallow.leaf->bytes_total, 100u);
    ASSERT_NE(r_deep.leaf, nullptr);
    EXPECT_EQ(r_deep.leaf->bytes_total, 200u);

    // LPM: querying the deep path also reveals that the shallow leaf
    // existed at depth 1 along the way — but the Lookup contract
    // returns the LONGEST match, which is the deep leaf.
    EXPECT_EQ(r_deep.matched_chunks, 2u);
}

// Phase D-4 — sibling chaining at a parent slot. Two ChunkHashes that
// share `h[0]` but differ on `h[1..7]` used to collide on the single
// slot pointer (kPathConflict). With chaining they coexist as separate
// chain entries distinguished by `edge_tail`.
TEST(ArtIndexTest, ChainSiblingsAtSameSlot) {
    ArtIndex art;
    // Five terminal paths whose first ChunkHash all share slot=1 (h[0])
    // but differ on h[1] (the first byte of edge_tail).
    auto mk = [](uint8_t tail0, uint8_t tail1) {
        std::vector<ChunkHash> p(1);
        p[0][0] = 1;                 // common slot
        p[0][1] = tail0;             // distinguishes the chain entry
        p[0][7] = tail1;             // anti-canonicalisation byte
        return p;
    };
    for (uint8_t i = 0; i < 5; ++i) {
        auto p = mk(i, /*tail1=*/static_cast<uint8_t>(0xA0 + i));
        EXPECT_EQ(art.Insert({p.data(), p.size()}, MakeLeaf(100 + i)),
                    ArtIndex::InsertResult::kInserted)
            << "chain sibling " << int(i)
            << " (h[1]=" << int(i) << ") must coexist";
    }
    EXPECT_EQ(art.LeafCount(), 5u);

    // Each sibling must lookup back to its own LeafData.
    auto g = art.EnterRead();
    for (uint8_t i = 0; i < 5; ++i) {
        auto p = mk(i, static_cast<uint8_t>(0xA0 + i));
        auto r = art.Lookup({p.data(), p.size()}, g);
        ASSERT_NE(r.leaf, nullptr)
            << "chain sibling " << int(i) << " disappeared";
        EXPECT_EQ(r.leaf->bytes_total, 100u + i)
            << "chain walk returned wrong leaf for sibling " << int(i);
    }

    // Looking up a sibling that was never inserted must miss, even
    // though its first byte matches the slot.
    {
        auto p = mk(/*tail0=*/99, /*tail1=*/0xFF);
        auto r = art.Lookup({p.data(), p.size()}, g);
        EXPECT_EQ(r.matched_chunks, 0u)
            << "chain walk must reject unmatched edge_tail";
    }
}

TEST(ArtIndexTest, RemoveLeafFromMiddleOfChainKeepsSiblings) {
    // Insert 3 chain siblings; remove the middle one; verify the head
    // and tail still resolve correctly. This exercises the chain
    // unlink path (parent's children[slot] -> next-survivor).
    ArtIndex art;
    auto mk = [](uint8_t tail0) {
        std::vector<ChunkHash> p(1);
        p[0][0] = 7;
        p[0][1] = tail0;
        return p;
    };
    auto p0 = mk(10);
    auto p1 = mk(20);
    auto p2 = mk(30);
    art.Insert({p0.data(), p0.size()}, MakeLeaf(700));
    art.Insert({p1.data(), p1.size()}, MakeLeaf(701));
    art.Insert({p2.data(), p2.size()}, MakeLeaf(702));
    ASSERT_EQ(art.LeafCount(), 3u);

    // Remove the middle entry.
    EXPECT_TRUE(art.Remove({p1.data(), p1.size()}));
    EXPECT_EQ(art.LeafCount(), 2u);

    auto g = art.EnterRead();
    auto r0 = art.Lookup({p0.data(), p0.size()}, g);
    auto r1 = art.Lookup({p1.data(), p1.size()}, g);
    auto r2 = art.Lookup({p2.data(), p2.size()}, g);
    ASSERT_NE(r0.leaf, nullptr);
    EXPECT_EQ(r0.leaf->bytes_total, 700u);
    EXPECT_EQ(r1.matched_chunks, 0u) << "removed sibling must miss";
    ASSERT_NE(r2.leaf, nullptr);
    EXPECT_EQ(r2.leaf->bytes_total, 702u);
}

TEST(ArtIndexTest, ChainSurvivesDenseFirstByteCollisions) {
    // The D-3-bench-revealing case: many leaves sharing the same
    // h[0] across distinct edge_tails. Pre-D-4 this returned
    // kPathConflict for all but the first. Now every Insert lands.
    ArtIndex art;
    constexpr std::size_t kCount = 64;
    for (std::size_t i = 0; i < kCount; ++i) {
        std::vector<ChunkHash> p(1);
        p[0][0] = 0x42;                          // common slot
        p[0][1] = static_cast<uint8_t>(i);       // unique tail
        p[0][2] = static_cast<uint8_t>(i * 17u); // tail noise
        EXPECT_EQ(art.Insert({p.data(), p.size()}, MakeLeaf(i)),
                    ArtIndex::InsertResult::kInserted)
            << "dense-collision entry " << i << " lost";
    }
    EXPECT_EQ(art.LeafCount(), kCount);
}
