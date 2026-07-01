// Task 1 — A9 DR warm-standby: HeadlessNode::ReplicaFetch unit test.
//
// Strategy: compile headless_node.cpp directly into this test binary (no
// kvcache dylib). Use BuildCtxOptions + HeadlessNode::GetOrCreate to
// initialise the singleton, then drive Reserve/Publish/Seal via the node
// methods directly, and call ReplicaFetch to verify the result.
//
// MakeLocator fills a kv_locator_t using the same derivation the C ABI uses
// internally:
//   tenant_id   = 16 zero bytes
//   model_id_hash = FNV-1a of model string (matches kv_abi.cpp Fnv1a64)
//   prefix_hash = XOR-fold of tokens (matches reserve_backpressure_test)
//   range.token_count = n, version = 1
//
// We call Reserve/Publish/Seal directly on HeadlessNode — no shmem ring,
// no gRPC — so evict_index_ and the DRAM tier are populated identically
// to a real ingest call. Then ReplicaFetch is verified for happy-path,
// miss-path, and null-out guard.
#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "ctx_options.h"    // BuildCtxOptions, HeadlessNode::Options
#include "headless_node.h"
#include "kvcache/kv_errors.h"

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

// Build a kv_locator_t for (model, tokens). The C ABI's internal Reserve call
// sets tenant_id bytes to zero (the locator is zero-init'd and the C ABI
// doesn't SHA-1 the tenant string into the locator bytes).
kv_locator_t MakeLocator(const std::string& model,
                          const std::vector<uint32_t>& tokens) {
    kv_locator_t loc{};
    std::memset(&loc, 0, sizeof(loc));
    loc.model_id_hash        = Fnv1a64(model);
    loc.range.token_count    = static_cast<uint32_t>(tokens.size());
    loc.version              = 1;
    for (std::size_t i = 0; i < tokens.size(); ++i) {
        loc.prefix_hash[(i * 7) % 16] ^= static_cast<uint8_t>(tokens[i] & 0xff);
        loc.prefix_hash[(i * 3) % 16] ^= static_cast<uint8_t>((tokens[i] >> 8) & 0xff);
    }
    return loc;
}

// Helper: Reserve → Publish → Seal a chunk into the node.
// Returns KV_OK on success. token_hash and model_hash are passed straight
// through to Reserve so the Seal lands on the right ART namespace.
int SealChunk(HeadlessNode* node,
              const kv_locator_t& loc,
              const std::vector<uint32_t>& tokens,
              const std::vector<uint8_t>& payload,
              uint64_t tenant_hash,
              uint64_t model_hash) {
    kv_handle_t    h    = 0;
    kv_buffer_desc_t slot{};
    int rc = node->Reserve(&loc, payload.size(), tenant_hash, model_hash,
                            &h, &slot);
    if (rc != KV_OK) return rc;
    if (slot.addr) std::memcpy(slot.addr, payload.data(), payload.size());
    kv_buffer_desc_t empty{};
    rc = node->Publish(h, empty, payload.size());
    if (rc != KV_OK) return rc;
    return node->Seal(h, tokens.data(), tokens.size());
}

// Initialise the HeadlessNode singleton once for the whole test binary.
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
// Test fixture: ensures the singleton is alive and provides helpers.
// ---------------------------------------------------------------------------
class ReplicaFetchTest : public ::testing::Test {
   protected:
    HeadlessNode* node_ = nullptr;
    void SetUp() override {
        node_ = GetNode();
        ASSERT_NE(node_, nullptr) << "HeadlessNode::GetOrCreate failed";
    }
};

// ---------------------------------------------------------------------------
// Happy-path: seal a chunk and verify ReplicaFetch returns it.
// ---------------------------------------------------------------------------
TEST_F(ReplicaFetchTest, ReturnsChunkPathAndBytesForSealedLocator) {
    // 32 tokens × 64 bytes/token = 2048 bytes.
    constexpr std::size_t kNTokens      = 32;
    constexpr std::size_t kBytesPerToken = 64;
    constexpr std::size_t kPayloadBytes  = kNTokens * kBytesPerToken;

    std::vector<uint32_t> tokens(kNTokens);
    for (uint32_t i = 0; i < kNTokens; ++i) tokens[i] = 5000u + i;

    const std::string model = "rf-happy-model";
    kv_locator_t loc = MakeLocator(model, tokens);

    const std::vector<uint8_t> payload(kPayloadBytes, 0xAB);
    const uint64_t tenant_hash = 0;              // zero → system bucket
    const uint64_t model_hash  = Fnv1a64(model);

    ASSERT_EQ(SealChunk(node_, loc, tokens, payload, tenant_hash, model_hash),
              KV_OK)
        << "Seal must succeed before ReplicaFetch can return data";

    HeadlessNode::ReplicaChunk rc;
    ASSERT_EQ(node_->ReplicaFetch(loc, &rc), KV_OK);
    EXPECT_FALSE(rc.chunk_path.empty())
        << "chunk_path must contain at least one hash for a sealed chunk";
    EXPECT_EQ(rc.bytes, payload)
        << "bytes must exactly equal the sealed payload";
}

// ---------------------------------------------------------------------------
// Miss-path: locator never sealed → KV_E_NOT_FOUND.
// ---------------------------------------------------------------------------
TEST_F(ReplicaFetchTest, NotFoundForUnsealedLocator) {
    const std::vector<uint32_t> tokens = {99001, 99002, 99003};
    kv_locator_t loc = MakeLocator("rf-miss-model", tokens);

    HeadlessNode::ReplicaChunk rc;
    EXPECT_EQ(node_->ReplicaFetch(loc, &rc), KV_E_NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Null-out guard: nullptr for out → KV_E_INVAL.
// ---------------------------------------------------------------------------
TEST_F(ReplicaFetchTest, NullOutReturnsInval) {
    const std::vector<uint32_t> tokens = {88001};
    kv_locator_t loc = MakeLocator("rf-inval-model", tokens);
    EXPECT_EQ(node_->ReplicaFetch(loc, nullptr), KV_E_INVAL);
}
