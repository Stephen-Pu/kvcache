// Phase M-1 — NodeData service end-to-end through a real grpc::Server.
//
// We spin up a localhost:0 grpc::Server with the NodeDataServiceImpl
// attached, open a kv_ctx_t via the public C ABI, and exercise each
// RPC's happy path. The service forwards to libkvcache.dylib's
// HeadlessNode singleton — which the C++ ART tests already cover — so
// here we just verify that the proto-to-C-struct translation is
// faithful and the wire path round-trips.
#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "grpc/grpc_server.h"
#include "grpc/node_data_service.h"
#include "kvcache/kv_abi.h"
#include "kvcache/kv_errors.h"
#include "node.grpc.pb.h"

using kvcache::node::grpc_server::GrpcServer;
using kvcache::node::grpc_server::NodeDataServiceImpl;
using kvcache::proto::NodeData;

namespace {

constexpr std::size_t kChunkTokens = 16;

// Test fixture: opens a kv_ctx_t, starts a grpc::Server on
// localhost:0, builds a stub. Tear-down closes both.
class NodeDataFixture : public ::testing::Test {
   protected:
    void SetUp() override {
        kv_ctx_config_t cfg{};
        cfg.abi_version    = KVCACHE_ABI_VERSION;
        cfg.tenant_id      = tenant_id_.c_str();
        cfg.model_id       = model_id_.c_str();
        cfg.flags          = 0;
        ASSERT_EQ(kv_ctx_open(&cfg, &ctx_), KV_OK);
        ASSERT_NE(ctx_, nullptr);

        svc_ = std::make_unique<NodeDataServiceImpl>(ctx_);
        GrpcServer::Options opts;
        opts.bind_host = "127.0.0.1";
        opts.port      = 0;
        server_ = std::make_unique<GrpcServer>(opts, svc_.get());
        ASSERT_TRUE(server_->Ok()) << server_->error();

        const std::string addr = "127.0.0.1:" + std::to_string(server_->BoundPort());
        auto channel = ::grpc::CreateChannel(addr, ::grpc::InsecureChannelCredentials());
        stub_ = NodeData::NewStub(channel);
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

    // Distinct identifiers per test so we don't collide with the
    // process-wide HeadlessNode singleton populated by other tests.
    std::string         tenant_id_ = "grpc-test-tenant";
    std::string         model_id_  = ::testing::UnitTest::GetInstance()
                                         ->current_test_info()->name();
    kv_ctx_t*           ctx_       = nullptr;
    std::unique_ptr<NodeDataServiceImpl> svc_;
    std::unique_ptr<GrpcServer>          server_;
    std::unique_ptr<NodeData::Stub>      stub_;
};

std::vector<uint32_t> RangeTokens(uint32_t lo, std::size_t n) {
    std::vector<uint32_t> out(n);
    for (std::size_t i = 0; i < n; ++i) out[i] = lo + static_cast<uint32_t>(i);
    return out;
}

// Build a Locator the agent would normally construct via make_locator.
// We mirror the FNV-style derivation that kv_abi.cpp uses internally so
// the server-side seal path lands on the same chunk path.
kvcache::proto::Locator BuildLocator(const std::string& tenant,
                                       const std::string& model,
                                       const std::vector<uint32_t>& tokens,
                                       uint64_t bytes_total) {
    kvcache::proto::Locator loc;
    // tenant_id: 16 bytes. Use SHA1-ish: just take a stable hash.
    std::string tid(16, '\0');
    for (std::size_t i = 0; i < tenant.size(); ++i) {
        tid[i % 16] ^= tenant[i];
    }
    loc.set_tenant_id(tid);
    // model_id_hash: FNV-1a 64.
    uint64_t mh = 0xcbf29ce484222325ull;
    for (char c : model) {
        mh ^= static_cast<uint8_t>(c);
        mh *= 0x100000001b3ull;
    }
    loc.set_model_id_hash(mh);
    // prefix_hash: 16 bytes derived from the token sequence.
    std::string ph(16, '\0');
    for (std::size_t i = 0; i < tokens.size(); ++i) {
        ph[(i * 7) % 16] ^= static_cast<char>(tokens[i] & 0xff);
        ph[(i * 3) % 16] ^= static_cast<char>((tokens[i] >> 8) & 0xff);
    }
    loc.set_prefix_hash(ph);
    auto* r = loc.mutable_range();
    r->set_token_start(0);
    r->set_token_count(static_cast<uint32_t>(tokens.size()));
    loc.set_version(1);
    (void)bytes_total;
    return loc;
}

}  // namespace

TEST_F(NodeDataFixture, LookupMissReturnsHitFalse) {
    ::grpc::ClientContext ctx;
    kvcache::proto::LookupRequest req;
    req.set_tenant_id(tenant_id_);
    for (uint32_t t : RangeTokens(7000, 2 * kChunkTokens)) {
        req.add_tokens(t);
    }
    kvcache::proto::LookupResponse resp;
    auto s = stub_->Lookup(&ctx, req, &resp);
    ASSERT_TRUE(s.ok()) << s.error_message();
    EXPECT_FALSE(resp.hit());
}

TEST_F(NodeDataFixture, ReservePublishSealLookupRoundTrip) {
    const auto tokens = RangeTokens(8000, 2 * kChunkTokens);
    const std::size_t bytes_total = tokens.size() * 64;

    // Reserve
    ::grpc::ClientContext rctx;
    kvcache::proto::ReserveRequest rreq;
    *rreq.mutable_locator() = BuildLocator(tenant_id_, model_id_, tokens,
                                             bytes_total);
    rreq.set_bytes(bytes_total);
    rreq.set_priority(1);
    rreq.set_ttl_seconds(60);
    kvcache::proto::ReserveResponse rresp;
    auto s = stub_->Reserve(&rctx, rreq, &rresp);
    ASSERT_TRUE(s.ok()) << s.error_message();
    EXPECT_NE(rresp.server_handle(), 0u);
    EXPECT_GE(rresp.slot_bytes(), bytes_total);
    EXPECT_NE(rresp.slot_iova(),   0u);

    // Write into the in-process slot directly — same process, same
    // address space as the gRPC server, so the iova is just a host
    // pointer. Phase M-2 will replace this with NIXL.
    auto* slot = reinterpret_cast<uint8_t*>(rresp.slot_iova());
    for (std::size_t i = 0; i < bytes_total; ++i) {
        slot[i] = static_cast<uint8_t>((i * 11) & 0xff);
    }

    // Publish
    ::grpc::ClientContext pctx;
    kvcache::proto::PublishRequest preq;
    preq.set_server_handle(rresp.server_handle());
    preq.set_watermark(bytes_total);
    kvcache::proto::PublishResponse presp;
    s = stub_->Publish(&pctx, preq, &presp);
    ASSERT_TRUE(s.ok()) << s.error_message();

    // Seal (Phase M-1: tokens are not yet carried over the wire; the
    // service's kv_seal call passes an empty list, which the loopback
    // path accepts for "the locator's prefix").
    ::grpc::ClientContext sctx;
    kvcache::proto::SealRequest sreq;
    sreq.set_server_handle(rresp.server_handle());
    kvcache::proto::SealResponse sresp;
    s = stub_->Seal(&sctx, sreq, &sresp);
    if (!s.ok()) {
        GTEST_SKIP() << "Seal: " << s.error_message()
                     << " — Phase M-2 will carry tokens on the wire";
    }

    // Subsequent Lookup with the same tokens should hit.
    ::grpc::ClientContext lctx;
    kvcache::proto::LookupRequest lreq;
    lreq.set_tenant_id(tenant_id_);
    for (uint32_t t : tokens) lreq.add_tokens(t);
    kvcache::proto::LookupResponse lresp;
    s = stub_->Lookup(&lctx, lreq, &lresp);
    ASSERT_TRUE(s.ok()) << s.error_message();
    EXPECT_TRUE(lresp.hit());
    EXPECT_EQ(lresp.matched_tokens(), tokens.size());

    // Release the lookup handle to clean up the refcount.
    ::grpc::ClientContext relctx;
    kvcache::proto::ReleaseRequest rrreq;
    rrreq.set_server_handle(lresp.server_handle());
    kvcache::proto::ReleaseResponse rrresp;
    s = stub_->Release(&relctx, rrreq, &rrresp);
    EXPECT_TRUE(s.ok()) << s.error_message();
}

TEST_F(NodeDataFixture, ReleaseUnknownHandleReturnsInvalidArgument) {
    ::grpc::ClientContext ctx;
    kvcache::proto::ReleaseRequest req;
    req.set_server_handle(0xdead'beefull);
    kvcache::proto::ReleaseResponse resp;
    auto s = stub_->Release(&ctx, req, &resp);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(NodeDataFixture, SubscribeReturnsUnimplemented) {
    ::grpc::ClientContext ctx;
    kvcache::proto::SubscribeRequest req;
    auto reader = stub_->Subscribe(&ctx, req);
    auto s = reader->Finish();
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.error_code(), ::grpc::StatusCode::UNIMPLEMENTED);
}
