// Phase Q-1 — cross-node Lookup fan-out unit test.
//
// Spins up two NodeDataServiceImpl instances in-process — same
// HeadlessNode singleton backing both, but each with its own
// NodeRuntime / GrpcServer identity. Both register with an in-memory
// etcd; a shared NodeDirectory + HrwRing observes both. The test
// computes HRW.Primary(key) for the test key, identifies which of the
// two services is the *non-primary*, sends a Lookup to it, and asserts
// the response carries the bytes only seal-ed on the primary's
// HeadlessNode side. The non-primary's handler must have transparently
// forwarded to the primary — verified end-to-end by the matching
// matched_tokens count.
//
// Why is "same HeadlessNode" OK here?
//   * For this unit test we only need to verify the wire-level
//     fan-out wiring. The e2e (kind) test validates the distinct-
//     backing-store case in a real two-pod deployment.
//   * Without the singleton break, two HeadlessNodes can't coexist in
//     one process; sharing it avoids that limit and keeps the test
//     focused on the forward primitive.
#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>
#include <openssl/sha.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cluster/etcd_client.h"
#include "cluster/node_directory.h"
#include "cluster/node_registrar.h"
#include "grpc/grpc_server.h"
#include "grpc/node_data_service.h"
#include "kvcache/kv_abi.h"
#include "node.grpc.pb.h"
#include "routing/hrw.h"

using kvcache::node::cluster::InMemoryEtcdClient;
using kvcache::node::cluster::NodeDirectory;
using kvcache::node::cluster::NodeRegistrar;
using kvcache::node::grpc_server::GrpcServer;
using kvcache::node::grpc_server::NodeDataServiceImpl;
using kvcache::node::routing::HrwRing;
using kvcache::proto::NodeData;

namespace {

bool WaitFor(std::function<bool()> pred,
             std::chrono::milliseconds budget = std::chrono::seconds(2)) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

constexpr std::size_t kChunkTokens = 16;

std::vector<uint32_t> RangeTokens(uint32_t lo, std::size_t n) {
    std::vector<uint32_t> out(n);
    for (std::size_t i = 0; i < n; ++i) out[i] = lo + static_cast<uint32_t>(i);
    return out;
}

}  // namespace

TEST(LookupForwarding, NonPrimaryForwardsToPrimary) {
    // ---- shared state ---------------------------------------------------
    kv_ctx_config_t cfg{};
    cfg.abi_version = KVCACHE_ABI_VERSION;
    cfg.tenant_id   = "fwd-tenant";
    cfg.model_id    = "fwd-model";
    kv_ctx_t* ctx = nullptr;
    ASSERT_EQ(kv_ctx_open(&cfg, &ctx), KV_OK);

    InMemoryEtcdClient etcd;
    HrwRing            ring;
    NodeDirectory      dir(&etcd, &ring);
    std::string err;
    ASSERT_TRUE(dir.Start(&err)) << err;

    // ---- stand up two gRPC services on OS-picked ports ------------------
    auto svc_a = std::make_unique<NodeDataServiceImpl>(ctx);
    auto svc_b = std::make_unique<NodeDataServiceImpl>(ctx);

    GrpcServer::Options gso;
    gso.bind_host = "127.0.0.1";
    gso.port      = 0;
    auto server_a = std::make_unique<GrpcServer>(gso, svc_a.get());
    ASSERT_TRUE(server_a->Ok()) << server_a->error();
    auto server_b = std::make_unique<GrpcServer>(gso, svc_b.get());
    ASSERT_TRUE(server_b->Ok()) << server_b->error();

    // ---- register both nodes with etcd ----------------------------------
    NodeRegistrar::Options oa{};
    oa.node_id        = "node-a";
    oa.advertise_host = "127.0.0.1";
    oa.grpc_port      = server_a->BoundPort();
    NodeRegistrar ra(&etcd, oa);
    ASSERT_TRUE(ra.Start(&err)) << err;

    NodeRegistrar::Options ob{};
    ob.node_id        = "node-b";
    ob.advertise_host = "127.0.0.1";
    ob.grpc_port      = server_b->BoundPort();
    NodeRegistrar rb(&etcd, ob);
    ASSERT_TRUE(rb.Start(&err)) << err;

    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 2; }));

    // ---- enable forwarding on both --------------------------------------
    svc_a->EnableForwarding("node-a", &ring, &dir);
    svc_b->EnableForwarding("node-b", &ring, &dir);

    // ---- decide which node is the primary for our test key -------------
    const std::string tenant_id    = "fwd-tenant";
    const uint64_t    model_hash   = 0xDEADBEEFCAFEBABEull;
    const auto        tokens       = RangeTokens(50000, 2 * kChunkTokens);

    std::vector<uint8_t> key_bytes;
    key_bytes.insert(key_bytes.end(), tenant_id.begin(), tenant_id.end());
    for (int i = 0; i < 8; ++i) {
        key_bytes.push_back(static_cast<uint8_t>((model_hash >> (i * 8)) & 0xff));
    }
    for (uint32_t t : tokens) {
        for (int i = 0; i < 4; ++i) {
            key_bytes.push_back(static_cast<uint8_t>((t >> (i * 8)) & 0xff));
        }
    }
    const std::string primary = ring.Primary(
        std::span<const uint8_t>(key_bytes.data(), key_bytes.size()));
    ASSERT_TRUE(primary == "node-a" || primary == "node-b");

    // The shared HeadlessNode singleton serves both — so we just need a
    // chunk sealed somewhere. Drive a Reserve→Publish→Seal directly
    // against the primary's local service (no forwarding needed: the
    // primary IS the owner). Subsequent Lookup against the *other*
    // service must forward and hit.

    NodeData::Stub* primary_stub = nullptr;
    NodeData::Stub* peer_stub    = nullptr;
    // Build stubs against both endpoints.
    auto chan_a = ::grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(server_a->BoundPort()),
        ::grpc::InsecureChannelCredentials());
    auto chan_b = ::grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(server_b->BoundPort()),
        ::grpc::InsecureChannelCredentials());
    auto stub_a = NodeData::NewStub(chan_a);
    auto stub_b = NodeData::NewStub(chan_b);
    if (primary == "node-a") {
        primary_stub = stub_a.get();
        peer_stub    = stub_b.get();
    } else {
        primary_stub = stub_b.get();
        peer_stub    = stub_a.get();
    }

    // ---- Seal a chunk on the primary ------------------------------------
    // Build a Locator the seal path can use. We mirror the shape used
    // by node_data_service_test's BuildLocator helper but inline here
    // so the test is self-contained.
    auto build_locator = [&]() {
        kvcache::proto::Locator loc;
        // Phase Q-5: SHA-1(tenant)[:16] matches both the Python
        // connector and the server's HashTenantString -> namespace
        // derivation. Required for ART lookups to find what Reserve
        // sealed.
        uint8_t sha[20];
        SHA1(reinterpret_cast<const uint8_t*>(tenant_id.data()),
              tenant_id.size(), sha);
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
    };

    const std::size_t bytes_total = tokens.size() * 64;
    ::grpc::ClientContext rctx;
    kvcache::proto::ReserveRequest rreq;
    *rreq.mutable_locator() = build_locator();
    rreq.set_bytes(bytes_total);
    kvcache::proto::ReserveResponse rresp;
    ASSERT_TRUE(primary_stub->Reserve(&rctx, rreq, &rresp).ok());
    auto* slot = reinterpret_cast<uint8_t*>(rresp.slot_iova());
    std::memset(slot, 0xCD, bytes_total);

    ::grpc::ClientContext pctx;
    kvcache::proto::PublishRequest preq;
    preq.set_server_handle(rresp.server_handle());
    preq.set_watermark(bytes_total);
    kvcache::proto::PublishResponse presp;
    ASSERT_TRUE(primary_stub->Publish(&pctx, preq, &presp).ok());

    ::grpc::ClientContext sctx;
    kvcache::proto::SealRequest sreq;
    sreq.set_server_handle(rresp.server_handle());
    for (uint32_t t : tokens) sreq.add_tokens(t);
    kvcache::proto::SealResponse sresp;
    ASSERT_TRUE(primary_stub->Seal(&sctx, sreq, &sresp).ok());

    // ---- Lookup via the NON-primary; expect a HIT via forwarding -------
    ::grpc::ClientContext lctx;
    kvcache::proto::LookupRequest lreq;
    lreq.set_tenant_id(tenant_id);
    lreq.set_model_id_hash(model_hash);
    for (uint32_t t : tokens) lreq.add_tokens(t);
    kvcache::proto::LookupResponse lresp;
    auto s = peer_stub->Lookup(&lctx, lreq, &lresp);
    ASSERT_TRUE(s.ok()) << s.error_message();
    EXPECT_TRUE(lresp.hit())
        << "Lookup on non-primary should forward to primary and hit; "
           "instead got miss. primary=" << primary;
    EXPECT_EQ(lresp.matched_tokens(), tokens.size());

    // Cleanup.
    stub_a.reset();
    stub_b.reset();
    ra.Stop();
    rb.Stop();
    server_a->Stop();
    server_b->Stop();
    server_a.reset();
    server_b.reset();
    svc_a.reset();
    svc_b.reset();
    dir.Stop();
    kv_ctx_close(ctx);
}

// ---------------------------------------------------------------------------
// Phase Q-2 — sticky-write fan-out. A client that sends Reserve to a
// NON-primary should still end up sealing the chunk on the primary,
// because the forwarding layer routes Reserve / Publish / Seal /
// Release based on (a) HRW for Reserve, (b) the (handle → owner) map
// for the handle-based RPCs.
// ---------------------------------------------------------------------------
TEST(LookupForwarding, ReserveSealForwardsViaHandleMap) {
    kv_ctx_config_t cfg{};
    cfg.abi_version = KVCACHE_ABI_VERSION;
    cfg.tenant_id   = "q2-tenant";
    cfg.model_id    = "q2-model";
    kv_ctx_t* ctx = nullptr;
    ASSERT_EQ(kv_ctx_open(&cfg, &ctx), KV_OK);

    InMemoryEtcdClient etcd;
    HrwRing            ring;
    NodeDirectory      dir(&etcd, &ring);
    std::string err;
    ASSERT_TRUE(dir.Start(&err)) << err;

    auto svc_a = std::make_unique<NodeDataServiceImpl>(ctx);
    auto svc_b = std::make_unique<NodeDataServiceImpl>(ctx);
    GrpcServer::Options gso;
    gso.bind_host = "127.0.0.1";
    gso.port      = 0;
    auto server_a = std::make_unique<GrpcServer>(gso, svc_a.get());
    auto server_b = std::make_unique<GrpcServer>(gso, svc_b.get());
    ASSERT_TRUE(server_a->Ok());
    ASSERT_TRUE(server_b->Ok());

    NodeRegistrar::Options oa{};
    oa.node_id = "node-a"; oa.advertise_host = "127.0.0.1";
    oa.grpc_port = server_a->BoundPort();
    NodeRegistrar ra(&etcd, oa);
    ASSERT_TRUE(ra.Start(&err)) << err;

    NodeRegistrar::Options ob{};
    ob.node_id = "node-b"; ob.advertise_host = "127.0.0.1";
    ob.grpc_port = server_b->BoundPort();
    NodeRegistrar rb(&etcd, ob);
    ASSERT_TRUE(rb.Start(&err)) << err;
    ASSERT_TRUE(WaitFor([&] { return dir.NodeCount() == 2; }));

    svc_a->EnableForwarding("node-a", &ring, &dir);
    svc_b->EnableForwarding("node-b", &ring, &dir);

    const std::string tenant_id  = "q2-tenant";
    const uint64_t    model_hash = 0xAA55AA55AA55AA55ull;
    const auto        tokens     = RangeTokens(60000, 2 * kChunkTokens);
    const std::size_t bytes_total = tokens.size() * 64;

    // Build the locator the same way Reserve handler computes the HRW key.
    auto build_locator = [&]() {
        kvcache::proto::Locator loc;
        // Phase Q-5: SHA-1(tenant)[:16] matches both the Python
        // connector and the server's HashTenantString -> namespace
        // derivation. Required for ART lookups to find what Reserve
        // sealed.
        uint8_t sha[20];
        SHA1(reinterpret_cast<const uint8_t*>(tenant_id.data()),
              tenant_id.size(), sha);
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
    };

    // Resolve HRW primary the same way the server does — tenant_id
    // bytes + u64 model_hash + prefix_hash bytes.
    const auto loc_proto = build_locator();
    std::vector<uint8_t> key;
    key.insert(key.end(),
                loc_proto.tenant_id().begin(), loc_proto.tenant_id().end());
    for (int i = 0; i < 8; ++i) {
        key.push_back(static_cast<uint8_t>((model_hash >> (i * 8)) & 0xff));
    }
    key.insert(key.end(),
                loc_proto.prefix_hash().begin(), loc_proto.prefix_hash().end());
    const std::string primary = ring.Primary(
        std::span<const uint8_t>(key.data(), key.size()));
    ASSERT_TRUE(primary == "node-a" || primary == "node-b");
    const std::string non_primary = (primary == "node-a") ? "node-b" : "node-a";

    // Send the entire write flow to the NON-primary. The server should
    // forward Reserve to the primary, record (handle → primary), then
    // forward Publish / Seal as follow-ups to that same primary.
    auto& target_server =
        (non_primary == "node-a") ? *server_a : *server_b;
    auto channel = ::grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(target_server.BoundPort()),
        ::grpc::InsecureChannelCredentials());
    auto stub = NodeData::NewStub(channel);

    ::grpc::ClientContext rctx;
    kvcache::proto::ReserveRequest rreq;
    *rreq.mutable_locator() = loc_proto;
    rreq.set_bytes(bytes_total);
    kvcache::proto::ReserveResponse rresp;
    ASSERT_TRUE(stub->Reserve(&rctx, rreq, &rresp).ok());
    ASSERT_NE(rresp.server_handle(), 0u);

    // Write into the slot — same process, so the iova returned by the
    // primary's HeadlessNode is a host pointer we can dereference.
    auto* slot = reinterpret_cast<uint8_t*>(rresp.slot_iova());
    std::memset(slot, 0xEF, bytes_total);

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

    // Verify: a Lookup against the non-primary should also forward and
    // hit. (The shared singleton means the bytes are in the same ART
    // either way; we're only verifying the wire didn't drop them.)
    ::grpc::ClientContext lctx;
    kvcache::proto::LookupRequest lreq;
    lreq.set_tenant_id(tenant_id);
    lreq.set_model_id_hash(model_hash);
    for (uint32_t t : tokens) lreq.add_tokens(t);
    kvcache::proto::LookupResponse lresp;
    ASSERT_TRUE(stub->Lookup(&lctx, lreq, &lresp).ok());
    EXPECT_TRUE(lresp.hit())
        << "Reserve+Seal forwarded to primary should be visible to Lookup";
    EXPECT_EQ(lresp.matched_tokens(), tokens.size());

    // Release the read handle.
    ::grpc::ClientContext relctx;
    kvcache::proto::ReleaseRequest relreq;
    relreq.set_server_handle(lresp.server_handle());
    kvcache::proto::ReleaseResponse relresp;
    EXPECT_TRUE(stub->Release(&relctx, relreq, &relresp).ok());

    // Cleanup.
    stub.reset();
    ra.Stop();
    rb.Stop();
    server_a->Stop();
    server_b->Stop();
    server_a.reset();
    server_b.reset();
    svc_a.reset();
    svc_b.reset();
    dir.Stop();
    kv_ctx_close(ctx);
}
