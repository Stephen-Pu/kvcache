// Phase M-4 — cross-process Pull end-to-end through the gRPC NodeData
// service.
//
// Closes the loop M-3 B opened on the wire: the server-side
// HeadlessNode runs with the TcpBackend (selected via the
// KVCACHE_NIXL_BACKEND env var the C ABI honours at first ctx open
// since Phase M-4) so pinned-tier slots have real, exportable MR
// keys; the gRPC handler returns a non-empty
// `ReserveResponse.remote_mr_descriptor`; a separately-instantiated
// peer-side TcpBackend imports it and Pulls the slot contents over
// TCP into a local buffer; we verify the bytes match what the server
// wrote into the slot in the same process.
//
// Why two separate backends in one process?
//   * The TcpBackend protocol is symmetric — each instance listens on
//     its own port and the puller connects to the source's port.
//   * Running two distinct TcpBackend instances exercises the real
//     wire format (socket accept + GET request + bytes back) even
//     though both ends share the process address space.
//   * This test lives in its own test binary so the HeadlessNode
//     singleton can be initialised in TCP mode without colliding
//     with the loopback-mode fixture in test_node_data_service.
//
// Limitations:
//   * Only the Reserve → ExportMr → peer-import → Pull path is
//     exercised. The dst-side `FetchRequest.dst_remote_mr_descriptor`
//     hand-off still rides the Fetch handler path, which today drives
//     an in-process kv_fetch — wiring it through to a peer-Pull-from-
//     server flow is Phase M-5.
#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>
#include <openssl/sha.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "grpc/grpc_server.h"
#include "grpc/node_data_service.h"
#include "kvcache/kv_abi.h"
#include "kvcache/kv_errors.h"
#include "node.grpc.pb.h"
#include "transport/nixl_wrapper.h"

using kvcache::node::grpc_server::GrpcServer;
using kvcache::node::grpc_server::NodeDataServiceImpl;
using kvcache::node::transport::BackendOptions;
using kvcache::node::transport::CreateBackend;
using kvcache::node::transport::INixlBackend;
using kvcache::node::transport::kInvalidMrKey;
using kvcache::node::transport::PullRequest;
using kvcache::node::transport::RemoteMrDescriptor;
using kvcache::proto::NodeData;

namespace {

constexpr std::size_t kChunkTokens = 16;

kvcache::proto::Locator BuildLocator(const std::string& tenant,
                                       uint64_t model_hash,
                                       const std::vector<uint32_t>& tokens) {
    kvcache::proto::Locator loc;
    // Phase Q-5: SHA-1(tenant)[:16] matches the Python connector's
    // make_locator and the server's tenant-namespace derivation.
    uint8_t sha[20];
    SHA1(reinterpret_cast<const uint8_t*>(tenant.data()), tenant.size(), sha);
    std::string tid(reinterpret_cast<const char*>(sha), 16);
    loc.set_tenant_id(tid);
    loc.set_model_id_hash(model_hash);
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
    return loc;
}

std::vector<uint32_t> RangeTokens(uint32_t lo, std::size_t n) {
    std::vector<uint32_t> out(n);
    for (std::size_t i = 0; i < n; ++i) out[i] = lo + static_cast<uint32_t>(i);
    return out;
}

}  // namespace

// Phase M-4 — full Reserve → ExportMr → peer Pull round-trip.
TEST(CrossProcessPull, ReserveExportsDescriptorPeerPullsBytes) {
    // Force the C ABI to bring up the HeadlessNode singleton with the
    // TCP backend. The env var is read exactly once at the first
    // kv_ctx_open in this process (singleton is sticky), so the test
    // is order-independent within this binary.
    ASSERT_EQ(::setenv("KVCACHE_NIXL_BACKEND",  "tcp",       1), 0);
    ASSERT_EQ(::setenv("KVCACHE_NIXL_BIND_HOST", "127.0.0.1", 1), 0);
    ASSERT_EQ(::setenv("KVCACHE_NIXL_BIND_PORT", "0",         1), 0);

    kv_ctx_t* ctx = nullptr;
    {
        kv_ctx_config_t cfg{};
        cfg.abi_version = KVCACHE_ABI_VERSION;
        cfg.tenant_id   = "m4-tenant";
        cfg.model_id    = "m4-model";
        ASSERT_EQ(kv_ctx_open(&cfg, &ctx), KV_OK);
    }
    auto svc = std::make_unique<NodeDataServiceImpl>(ctx);
    GrpcServer::Options grpc_opts;
    grpc_opts.bind_host = "127.0.0.1";
    grpc_opts.port      = 0;
    auto server = std::make_unique<GrpcServer>(grpc_opts, svc.get());
    ASSERT_TRUE(server->Ok()) << server->error();
    auto channel = ::grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(server->BoundPort()),
        ::grpc::InsecureChannelCredentials());
    auto stub = NodeData::NewStub(channel);

    // ---- Reserve via gRPC: must return a non-empty descriptor now
    //      that the TCP backend's RegisterRegion is wired in to the
    //      pinned tier.
    const auto tokens             = RangeTokens(40000, 2 * kChunkTokens);
    const std::size_t bytes_total = tokens.size() * 64;
    ::grpc::ClientContext rctx;
    kvcache::proto::ReserveRequest rreq;
    *rreq.mutable_locator() = BuildLocator("m4-tenant", 0xc0ffee, tokens);
    rreq.set_bytes(bytes_total);
    kvcache::proto::ReserveResponse rresp;
    auto s = stub->Reserve(&rctx, rreq, &rresp);
    ASSERT_TRUE(s.ok()) << s.error_message();
    ASSERT_FALSE(rresp.remote_mr_descriptor().empty())
        << "TCP backend should produce a non-empty RemoteMrDescriptor "
           "now that pinned-tier slots are registered with it";
    ASSERT_NE(rresp.slot_iova(), 0u);

    // Write a known pattern into the slot — we'll Pull this from the
    // peer side and assert it round-trips.
    auto* slot = reinterpret_cast<uint8_t*>(rresp.slot_iova());
    for (std::size_t i = 0; i < bytes_total; ++i) {
        slot[i] = static_cast<uint8_t>((i * 31 + 7) & 0xff);
    }

    // ---- Peer side: distinct TcpBackend that imports the descriptor
    // and Pulls bytes out of the server's slot pool.
    BackendOptions pbo;
    pbo.name      = "tcp";
    pbo.bind_host = "127.0.0.1";
    pbo.bind_port = 0;
    std::string err;
    auto peer = CreateBackend(pbo, &err);
    ASSERT_NE(peer, nullptr) << err;

    // Import — peer learns server's listener host:port + pool MR id +
    // length straight off the wire.
    RemoteMrDescriptor desc;
    desc.opaque.assign(rresp.remote_mr_descriptor().begin(),
                        rresp.remote_mr_descriptor().end());
    auto peer_remote = peer->ImportRemoteMr(desc, &err);
    ASSERT_NE(peer_remote, kInvalidMrKey) << err;

    // Peer-local dst + registration.
    std::vector<uint8_t> dst(bytes_total, 0);
    auto peer_local = peer->RegisterRegion(dst.data(), dst.size(), &err);
    ASSERT_NE(peer_local, kInvalidMrKey) << err;

    // Pull from the freshly-issued slot. Slots are carved sequentially
    // from the start of the pinned pool; the first slot off a fresh
    // pool sits at offset 0, matching the bytes we just wrote.
    PullRequest pr{peer_local, /*dst_off=*/0,
                    peer_remote, /*src_off=*/0,
                    bytes_total};
    auto cid = peer->Pull(pr, &err);
    ASSERT_NE(cid, 0u) << err;
    ASSERT_TRUE(peer->Wait(cid, 2000, &err)) << err;

    EXPECT_EQ(std::memcmp(dst.data(), slot, bytes_total), 0);

    // Cleanup. Stop the gRPC server before tearing down the service
    // so in-flight handler threads aren't accessing freed state.
    stub.reset();
    server->Stop();
    server.reset();
    svc.reset();
    kv_ctx_close(ctx);
}

// Phase M-6 — full server-push round-trip through the gRPC Fetch
// handler. Engine registers its dst buffer with its own TcpBackend,
// exports the descriptor, and sends it via FetchRequest.
// dst_remote_mr_descriptor. The server-side gRPC handler imports the
// descriptor into the singleton's NIXL backend; HeadlessNode::Fetch
// detects the imported-remote dst (IsRemote == true) and dispatches
// via TcpBackend::Push — the server connects to the engine's
// listener and writes the bytes via PUT. We verify the engine's
// receive buffer matches the bytes the server staged via
// Reserve+Publish+Seal.
TEST(CrossProcessPull, FetchPushesBytesToEngine) {
    // Same TCP-mode singleton boot as the earlier test in this binary.
    // Setenv is idempotent across tests; the singleton is sticky.
    ASSERT_EQ(::setenv("KVCACHE_NIXL_BACKEND",   "tcp",       1), 0);
    ASSERT_EQ(::setenv("KVCACHE_NIXL_BIND_HOST", "127.0.0.1", 1), 0);
    ASSERT_EQ(::setenv("KVCACHE_NIXL_BIND_PORT", "0",         1), 0);

    kv_ctx_t* ctx = nullptr;
    {
        kv_ctx_config_t cfg{};
        cfg.abi_version = KVCACHE_ABI_VERSION;
        cfg.tenant_id   = "m6-tenant";
        cfg.model_id    = "m6-model";
        ASSERT_EQ(kv_ctx_open(&cfg, &ctx), KV_OK);
    }
    auto svc = std::make_unique<NodeDataServiceImpl>(ctx);
    GrpcServer::Options grpc_opts;
    grpc_opts.bind_host = "127.0.0.1";
    grpc_opts.port      = 0;
    auto server = std::make_unique<GrpcServer>(grpc_opts, svc.get());
    ASSERT_TRUE(server->Ok()) << server->error();
    auto channel = ::grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(server->BoundPort()),
        ::grpc::InsecureChannelCredentials());
    auto stub = NodeData::NewStub(channel);

    // Use small payloads — DRAM staging copies these in full, no need
    // to stress capacity for this test.
    const auto tokens             = RangeTokens(50000, 2 * kChunkTokens);
    const std::size_t bytes_total = 4096;

    // Reserve + write known pattern + Publish + Seal so the server
    // has actual bytes in DRAM that Fetch will resolve.
    ::grpc::ClientContext rctx;
    kvcache::proto::ReserveRequest rreq;
    *rreq.mutable_locator() = BuildLocator("m6-tenant", 0xb0bACAFE, tokens);
    rreq.set_bytes(bytes_total);
    kvcache::proto::ReserveResponse rresp;
    ASSERT_TRUE(stub->Reserve(&rctx, rreq, &rresp).ok());
    auto* slot = reinterpret_cast<uint8_t*>(rresp.slot_iova());
    for (std::size_t i = 0; i < bytes_total; ++i) {
        slot[i] = static_cast<uint8_t>((i * 53 + 11) & 0xff);
    }

    ::grpc::ClientContext pctx;
    kvcache::proto::PublishRequest preq;
    preq.set_server_handle(rresp.server_handle());
    preq.set_watermark(bytes_total);
    kvcache::proto::PublishResponse presp;
    ASSERT_TRUE(stub->Publish(&pctx, preq, &presp).ok());

    ::grpc::ClientContext sctx;
    kvcache::proto::SealRequest sreq;
    sreq.set_server_handle(rresp.server_handle());
    for (uint32_t t : tokens) sreq.add_tokens(t);
    kvcache::proto::SealResponse sresp;
    ASSERT_TRUE(stub->Seal(&sctx, sreq, &sresp).ok());

    // Lookup to mint a read handle.
    ::grpc::ClientContext lctx;
    kvcache::proto::LookupRequest lreq;
    lreq.set_tenant_id("m6-tenant");
    // Phase Q-5 — must match the model_id_hash used in BuildLocator
    // above; otherwise the ART namespace fingerprint differs and the
    // lookup walks an empty subtree.
    lreq.set_model_id_hash(0xb0bACAFE);
    for (uint32_t t : tokens) lreq.add_tokens(t);
    kvcache::proto::LookupResponse lresp;
    ASSERT_TRUE(stub->Lookup(&lctx, lreq, &lresp).ok());
    ASSERT_TRUE(lresp.hit());

    // ---- Engine side: separate TcpBackend, registers dst, exports.
    BackendOptions ebo;
    ebo.name      = "tcp";
    ebo.bind_host = "127.0.0.1";
    ebo.bind_port = 0;
    std::string err;
    auto engine = CreateBackend(ebo, &err);
    ASSERT_NE(engine, nullptr) << err;

    std::vector<uint8_t> engine_dst(bytes_total, 0);
    auto engine_local = engine->RegisterRegion(engine_dst.data(),
                                                engine_dst.size(), &err);
    ASSERT_NE(engine_local, kInvalidMrKey) << err;
    RemoteMrDescriptor engine_dst_desc;
    ASSERT_TRUE(engine->ExportMr(engine_local, &engine_dst_desc, &err)) << err;

    // ---- Issue Fetch with dst_remote_mr_descriptor = engine's
    //      exported dst. Server imports, sees IsRemote == true,
    //      drives a Push over TCP into engine_dst.
    ::grpc::ClientContext fctx;
    kvcache::proto::FetchRequest freq;
    freq.set_server_handle(lresp.server_handle());
    // dst_iova / dst_bytes are still needed by HeadlessNode for the
    // size/bound check (it uses dst.len). Pass the engine buffer's
    // address+size; they're consulted but the actual write is via
    // Push to the imported remote MR.
    freq.set_dst_iova(reinterpret_cast<uint64_t>(engine_dst.data()));
    freq.set_dst_bytes(engine_dst.size());
    freq.set_dst_remote_mr_descriptor(
        std::string(engine_dst_desc.opaque.begin(),
                     engine_dst_desc.opaque.end()));
    kvcache::proto::FetchResponse fresp;
    auto fs = stub->Fetch(&fctx, freq, &fresp);
    ASSERT_TRUE(fs.ok()) << fs.error_message();

    // Engine's dst should now hold the bytes the server wrote into
    // its slot before Sealing.
    EXPECT_EQ(std::memcmp(engine_dst.data(), slot, bytes_total), 0);

    stub.reset();
    server->Stop();
    server.reset();
    svc.reset();
    kv_ctx_close(ctx);
}
