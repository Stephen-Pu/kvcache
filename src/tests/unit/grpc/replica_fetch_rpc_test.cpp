// Phase A9 / A11 — ReplicaFetch gRPC handler test.
//
// Verifies two things over a real grpc::Server loopback:
//
//   (a) A caller authorised as an internal peer (via the injectable
//       PeerVerifier seam) receives KV_OK, a non-empty chunk_path, and
//       the correct bytes after a chunk has been sealed into the backing
//       HeadlessNode.
//
//   (b) A caller that is NOT an internal peer (the real SPIFFE-based check
//       on an insecure channel that presents no cert) is rejected with
//       PERMISSION_DENIED.
//
// Auth seam: NodeDataServiceImpl::SetInternalPeerVerifier lets tests inject
// an always-allow or always-deny predicate. The insecure loopback channel
// carries no cert, so the real check always denies — that is case (b).
//
// Backend seam: NodeDataServiceImpl::SetReplicaFetchBackend decouples the
// grpc service library from HeadlessNode's hidden-visibility C++ symbols.
// The test wires in a lambda that calls node->ReplicaFetch directly;
// headless_node.cpp is compiled directly into this test binary so those
// C++ symbols are locally visible (same pattern as test_replica_fetch and
// test_replication_consumer).
#ifdef KVCACHE_HAVE_GRPC

#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>

#include <array>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "grpc/grpc_server.h"
#include "grpc/node_data_service.h"
#include "headless_node.h"   // compiled directly into this binary (not via dylib)
#include "ctx_options.h"
#include "kvcache/kv_abi.h"
#include "kvcache/kv_errors.h"
#include "node.grpc.pb.h"

using kvcache::node::grpc_server::GrpcServer;
using kvcache::node::grpc_server::NodeDataServiceImpl;
using kvcache::proto::NodeData;
using kvcache::abi::HeadlessNode;

namespace {

// FNV-1a 64-bit — matches HeadlessNode's internal hash and the C ABI.
uint64_t Fnv1a64(const std::string& s) {
    uint64_t h = 0xcbf29ce484222325ull;
    for (unsigned char c : s) {
        h ^= c;
        h *= 0x100000001b3ull;
    }
    return h;
}

// Build a kv_locator_t using the same derivation the C ABI uses internally:
// zero tenant_id, FNV model hash, XOR-fold prefix_hash from tokens.
kv_locator_t MakeLocator(const std::string& model,
                          const std::vector<uint32_t>& tokens) {
    kv_locator_t loc{};
    loc.model_id_hash     = Fnv1a64(model);
    loc.range.token_count = static_cast<uint32_t>(tokens.size());
    loc.version           = 1;
    for (std::size_t i = 0; i < tokens.size(); ++i) {
        loc.prefix_hash[(i * 7) % 16] ^= static_cast<uint8_t>(tokens[i] & 0xff);
        loc.prefix_hash[(i * 3) % 16] ^= static_cast<uint8_t>((tokens[i] >> 8) & 0xff);
    }
    return loc;
}

// Build the proto Locator that corresponds to MakeLocator above.
// tenant_id is 16 zero bytes (matching the zero-init in MakeLocator).
kvcache::proto::Locator MakeProtoLocator(const std::string& model,
                                          const std::vector<uint32_t>& tokens) {
    kvcache::proto::Locator loc;
    loc.set_tenant_id(std::string(16, '\0'));
    loc.set_model_id_hash(Fnv1a64(model));
    // prefix_hash: same XOR-fold as MakeLocator.
    std::string ph(16, '\0');
    for (std::size_t i = 0; i < tokens.size(); ++i) {
        ph[(i * 7) % 16] ^= static_cast<char>(tokens[i] & 0xff);
        ph[(i * 3) % 16] ^= static_cast<char>((tokens[i] >> 8) & 0xff);
    }
    loc.set_prefix_hash(ph);
    auto* r = loc.mutable_range();
    r->set_token_count(static_cast<uint32_t>(tokens.size()));
    loc.set_version(1);
    return loc;
}

// Reserve → Publish → Seal a chunk on the HeadlessNode singleton.
int SealChunkOnNode(HeadlessNode* node,
                    const kv_locator_t& loc,
                    const std::vector<uint32_t>& tokens,
                    const std::vector<uint8_t>& payload,
                    const std::string& model) {
    kv_handle_t      h    = 0;
    kv_buffer_desc_t slot{};
    const uint64_t tenant_hash = 0;
    const uint64_t model_hash  = Fnv1a64(model);
    int rc = node->Reserve(&loc, payload.size(), tenant_hash, model_hash,
                            &h, &slot);
    if (rc != KV_OK) return rc;
    if (slot.addr) std::memcpy(slot.addr, payload.data(), payload.size());
    kv_buffer_desc_t empty{};
    rc = node->Publish(h, empty, payload.size());
    if (rc != KV_OK) return rc;
    return node->Seal(h, tokens.data(), tokens.size());
}

// Initialise (or reuse) the process-wide HeadlessNode singleton.
HeadlessNode* GetNode() {
    static HeadlessNode* node = [] {
        auto opts = kvcache::abi::BuildCtxOptions(nullptr);
        std::string err;
        return HeadlessNode::GetOrCreate(opts, &err);
    }();
    return node;
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class ReplicaFetchRpcTest : public ::testing::Test {
   protected:
    void SetUp() override {
        node_ = GetNode();
        ASSERT_NE(node_, nullptr) << "HeadlessNode::GetOrCreate failed";

        // Open a bootstrap kv_ctx_t for NodeDataServiceImpl (Subscribe path).
        kv_ctx_config_t cfg{};
        cfg.abi_version = KVCACHE_ABI_VERSION;
        cfg.tenant_id   = "rfetch-rpc-test";
        cfg.model_id    = ::testing::UnitTest::GetInstance()
                              ->current_test_info()->name();
        ASSERT_EQ(kv_ctx_open(&cfg, &ctx_), KV_OK);
        ASSERT_NE(ctx_, nullptr);

        svc_ = std::make_unique<NodeDataServiceImpl>(ctx_);

        // Wire the ReplicaFetch backend to the in-process HeadlessNode.
        // node_ is a raw pointer to the singleton; the lambda captures it
        // by value (the singleton outlives the service).
        HeadlessNode* node = node_;
        svc_->SetReplicaFetchBackend(
            [node](const kv_locator_t& loc,
                   NodeDataServiceImpl::ReplicaChunk* out) -> int {
                HeadlessNode::ReplicaChunk hrc;
                int rv = node->ReplicaFetch(loc, &hrc);
                if (rv == KV_OK) {
                    // Convert HeadlessNode::prefix::ChunkHash (array<uint8_t,8>)
                    // to NodeDataServiceImpl::ChunkHash (same type alias).
                    out->chunk_path.clear();
                    for (const auto& h : hrc.chunk_path) {
                        NodeDataServiceImpl::ChunkHash ch;
                        static_assert(sizeof(ch) == sizeof(h),
                            "ChunkHash size mismatch");
                        std::memcpy(ch.data(), h.data(), ch.size());
                        out->chunk_path.push_back(ch);
                    }
                    out->bytes = std::move(hrc.bytes);
                }
                return rv;
            });

        // Install the always-allow verifier for the "internal peer" tests.
        // The default (null verifier) will be overridden per test as needed.
        svc_->SetInternalPeerVerifier(
            [](::grpc::ServerContext*) { return true; });

        GrpcServer::Options opts;
        opts.bind_host = "127.0.0.1";
        opts.port      = 0;
        server_ = std::make_unique<GrpcServer>(opts, svc_.get());
        ASSERT_TRUE(server_->Ok()) << server_->error();

        const std::string addr =
            "127.0.0.1:" + std::to_string(server_->BoundPort());
        auto ch = ::grpc::CreateChannel(addr, ::grpc::InsecureChannelCredentials());
        stub_ = NodeData::NewStub(ch);
    }

    void TearDown() override {
        stub_.reset();
        if (server_) server_->Stop();
        server_.reset();
        svc_.reset();
        if (ctx_) {
            kv_ctx_close(ctx_);
            ctx_ = nullptr;
        }
    }

    HeadlessNode*                        node_   = nullptr;
    kv_ctx_t*                            ctx_    = nullptr;
    std::unique_ptr<NodeDataServiceImpl> svc_;
    std::unique_ptr<GrpcServer>          server_;
    std::unique_ptr<NodeData::Stub>      stub_;
};

}  // namespace

// ---------------------------------------------------------------------------
// Case (a): internal/authorised peer — ReplicaFetch returns the sealed chunk.
// ---------------------------------------------------------------------------
TEST_F(ReplicaFetchRpcTest, InternalPeerGetsChunkPathAndBytes) {
    constexpr std::size_t kNTokens       = 32;
    constexpr std::size_t kBytesPerToken = 64;
    constexpr std::size_t kPayloadBytes  = kNTokens * kBytesPerToken;

    // Unique model+tokens so this test doesn't collide with others sharing the
    // singleton HeadlessNode.
    const std::string model = "rfetch-rpc-internal-model";
    std::vector<uint32_t> tokens(kNTokens);
    for (uint32_t i = 0; i < kNTokens; ++i) tokens[i] = 70000u + i;

    const kv_locator_t loc = MakeLocator(model, tokens);
    const std::vector<uint8_t> payload(kPayloadBytes, 0xAB);

    // Seal the chunk directly on the HeadlessNode singleton.
    ASSERT_EQ(SealChunkOnNode(node_, loc, tokens, payload, model), KV_OK)
        << "Seal must succeed before ReplicaFetch can return data";

    // The always-allow verifier was installed in SetUp.
    kvcache::proto::ReplicaFetchRequest req;
    *req.mutable_locator() = MakeProtoLocator(model, tokens);

    kvcache::proto::ReplicaFetchResponse resp;
    ::grpc::ClientContext ctx;
    auto st = stub_->ReplicaFetch(&ctx, req, &resp);

    ASSERT_TRUE(st.ok()) << "transport must succeed: " << st.error_message();
    EXPECT_EQ(resp.status(), KV_OK)
        << "application status must be KV_OK (0) for a sealed chunk";
    EXPECT_FALSE(resp.chunk_path().empty())
        << "chunk_path must contain at least one 8-byte hash";
    for (const auto& entry : resp.chunk_path()) {
        EXPECT_EQ(entry.size(), 8u) << "each chunk_path entry must be 8 bytes";
    }
    EXPECT_EQ(resp.data().size(), kPayloadBytes)
        << "data must be exactly the sealed payload size";
    for (std::size_t i = 0; i < kPayloadBytes; ++i) {
        ASSERT_EQ(static_cast<uint8_t>(resp.data()[i]), 0xABu)
            << "payload byte mismatch at index " << i;
    }
}

// ---------------------------------------------------------------------------
// Case (b): non-internal / unauthenticated peer → PERMISSION_DENIED.
// ---------------------------------------------------------------------------
TEST_F(ReplicaFetchRpcTest, NonInternalPeerIsDenied) {
    // Remove the always-allow verifier so the real SPIFFE check runs.
    // Over an insecure channel the auth context carries no cert properties
    // (no x509_common_name, no x509_subject_alternative_name), so
    // ClientCertInfo returns an empty CertInfo and VerifyInternalPeer
    // returns nullopt → PERMISSION_DENIED.
    svc_->SetInternalPeerVerifier(nullptr);

    const std::string model = "rfetch-rpc-denied-model";
    const std::vector<uint32_t> tokens = {80000u, 80001u, 80002u};

    kvcache::proto::ReplicaFetchRequest req;
    *req.mutable_locator() = MakeProtoLocator(model, tokens);

    kvcache::proto::ReplicaFetchResponse resp;
    ::grpc::ClientContext ctx;
    auto st = stub_->ReplicaFetch(&ctx, req, &resp);

    EXPECT_EQ(st.error_code(), ::grpc::StatusCode::PERMISSION_DENIED)
        << "non-internal peer must receive PERMISSION_DENIED; got: "
        << st.error_message();
}

#endif  // KVCACHE_HAVE_GRPC
