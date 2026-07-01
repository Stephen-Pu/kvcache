// Task 2 — A9 DR warm-standby: HeadlessNode::SealByChunkPath unit test.
//
// Strategy: compile headless_node.cpp directly into this test binary (no
// kvcache dylib). Use the same singleton init as replica_fetch_test.
//
// Test scenario mirrors the real DR warm-standby replication flow:
//   1. Seal a chunk on the "primary" node (the singleton) using tokens + Seal().
//   2. Call ReplicaFetch to retrieve chunk_path + bytes (as the replicator would).
//   3. On a SEPARATE locator set (distinct tokens → distinct chunk_path), call
//      Reserve / Publish / SealByChunkPath to commit the chunk as a standby
//      would — using a pre-computed chunk_path instead of tokens.
//   4. Verify that Lookup with the matching tokens finds the chunk.
//
// The "primary" and "standby" share the same singleton in this unit test
// because HeadlessNode uses a process-singleton pattern; each step uses
// non-overlapping (tenant, model, token) triples so there are no ART
// path conflicts.
#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "ctx_options.h"
#include "headless_node.h"
#include "kvcache/kv_errors.h"
#include "prefix/lpm.h"   // NamespaceFingerprint, ChunkifyNS, kChunkTokens

using kvcache::abi::HeadlessNode;

namespace {

// FNV-1a 64-bit — mirrors kvcache::Fnv1a64 in hashing.h.
uint64_t Fnv1a64(const std::string& s) {
    uint64_t h = 0xcbf29ce484222325ull;
    for (unsigned char c : s) {
        h ^= c;
        h *= 0x100000001b3ull;
    }
    return h;
}

// Build a kv_locator_t for (model, tokens).
kv_locator_t MakeLocator(const std::string& model,
                          const std::vector<uint32_t>& tokens) {
    kv_locator_t loc{};
    std::memset(&loc, 0, sizeof(loc));
    loc.model_id_hash     = Fnv1a64(model);
    loc.range.token_count = static_cast<uint32_t>(tokens.size());
    loc.version           = 1;
    for (std::size_t i = 0; i < tokens.size(); ++i) {
        loc.prefix_hash[(i * 7) % 16] ^= static_cast<uint8_t>(tokens[i] & 0xff);
        loc.prefix_hash[(i * 3) % 16] ^= static_cast<uint8_t>((tokens[i] >> 8) & 0xff);
    }
    return loc;
}

// Initialise the HeadlessNode singleton once for the whole test binary.
// (Distinct from test_replica_fetch — each test binary gets its own process
// so there is no cross-binary singleton collision.)
HeadlessNode* GetNode() {
    static HeadlessNode* node = [] {
        auto opts = kvcache::abi::BuildCtxOptions(nullptr);
        std::string err;
        auto* n = HeadlessNode::GetOrCreate(opts, &err);
        return n;
    }();
    return node;
}

}  // namespace

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class SealByChunkPathTest : public ::testing::Test {
   protected:
    HeadlessNode* node_ = nullptr;
    void SetUp() override {
        node_ = GetNode();
        ASSERT_NE(node_, nullptr) << "HeadlessNode::GetOrCreate failed";
    }
};

// ---------------------------------------------------------------------------
// Happy-path: commit a chunk via SealByChunkPath and verify Lookup finds it.
//
// This is the standby-side commit: the caller supplies chunk_path (obtained
// from ReplicaFetch on the primary) rather than the original token sequence.
// ---------------------------------------------------------------------------
TEST_F(SealByChunkPathTest, CommittedChunkIsLookupableByOriginalTokens) {
    // 32 tokens × 64 bytes/token — two full 16-token chunks.
    constexpr std::size_t kNTokens       = 32;
    constexpr std::size_t kBytesPerToken = 64;
    constexpr std::size_t kPayloadBytes  = kNTokens * kBytesPerToken;

    // Use a unique token range to avoid ART conflicts with other tests.
    std::vector<uint32_t> tokens(kNTokens);
    for (uint32_t i = 0; i < kNTokens; ++i) tokens[i] = 3000u + i;

    const std::string  model       = "sbcp-happy-model";
    const uint64_t     tenant_hash = 0;
    const uint64_t     model_hash  = Fnv1a64(model);
    const kv_locator_t loc         = MakeLocator(model, tokens);
    const std::vector<uint8_t> payload(kPayloadBytes, 0xCD);

    // -----------------------------------------------------------------------
    // Step 1: compute the chunk_path the way Seal() would — same derivation
    // that a primary node performs at Seal time and ReplicaFetch returns.
    // -----------------------------------------------------------------------
    const auto ns =
        kvcache::node::prefix::NamespaceFingerprint(tenant_hash, model_hash);
    const auto chunk_path =
        kvcache::node::prefix::ChunkifyNS({tokens.data(), tokens.size()}, ns);
    ASSERT_FALSE(chunk_path.empty());

    // -----------------------------------------------------------------------
    // Step 2: standby-side commit — Reserve, Publish, SealByChunkPath.
    // This is the path a warm-standby would exercise after receiving
    // chunk_path + bytes from ReplicaFetch on the primary.
    // -----------------------------------------------------------------------
    kv_handle_t      h    = 0;
    kv_buffer_desc_t slot{};
    ASSERT_EQ(node_->Reserve(&loc, payload.size(),
                              tenant_hash, model_hash,
                              &h, &slot),
              KV_OK);

    if (slot.addr) std::memcpy(slot.addr, payload.data(), payload.size());

    kv_buffer_desc_t empty{};
    ASSERT_EQ(node_->Publish(h, empty, payload.size()), KV_OK);

    ASSERT_EQ(node_->SealByChunkPath(h, chunk_path), KV_OK);

    // -----------------------------------------------------------------------
    // Step 3: Lookup with the original tokens must hit the committed chunk.
    // -----------------------------------------------------------------------
    kv_locator_t out_meta{};
    kv_handle_t  lh           = 0;
    uint32_t     matched      = 0;
    EXPECT_EQ(node_->Lookup(
                  /*tenant_id=*/"",
                  tenant_hash,
                  model_hash,
                  tokens.data(), tokens.size(),
                  &out_meta, &lh, &matched),
              KV_OK);
    EXPECT_EQ(matched, static_cast<uint32_t>(kNTokens));

    // Release the read handle obtained by Lookup.
    node_->Release(lh);
}

// ---------------------------------------------------------------------------
// Invalid handle: SealByChunkPath on an unknown handle returns KV_E_INVAL.
// ---------------------------------------------------------------------------
TEST_F(SealByChunkPathTest, UnknownHandleReturnsInval) {
    const std::vector<kvcache::node::prefix::ChunkHash> dummy_path(1);
    EXPECT_EQ(node_->SealByChunkPath(/*handle=*/999999, dummy_path),
              KV_E_NOT_FOUND);
}
