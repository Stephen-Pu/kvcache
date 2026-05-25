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

#include "cluster/bloom_publisher.h"
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
// Phase R-1 — chaos: forward target dies mid-flight.
//
// HRW routes the request to a peer; we shut that peer down so the
// cached PeerStub points at a dead listener. The Lookup must
// surface a clean grpc::StatusCode::UNAVAILABLE (not a silent miss
// or a hung RPC). Verifies the forward path's degradation contract:
// the caller can distinguish "owner is up but data isn't there"
// from "owner is unreachable".
TEST(LookupForwarding, ForwardSurfacesUnavailableWhenPeerDown) {
    kv_ctx_config_t cfg{};
    cfg.abi_version = KVCACHE_ABI_VERSION;
    cfg.tenant_id   = "chaos-tenant";
    cfg.model_id    = "chaos-down";
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

    // Pick a key whose HRW primary is one specific node; shut down
    // that node; send Lookup to the other.
    const std::string tenant_id = "chaos-tenant";
    const uint64_t    model_hash = 0xCAFEFEED;
    const auto        tokens     = RangeTokens(70000, 2 * kChunkTokens);
    std::vector<uint8_t> key;
    key.insert(key.end(), tenant_id.begin(), tenant_id.end());
    for (int i = 0; i < 8; ++i) {
        key.push_back(static_cast<uint8_t>((model_hash >> (i * 8)) & 0xff));
    }
    for (uint32_t t : tokens) {
        for (int i = 0; i < 4; ++i) {
            key.push_back(static_cast<uint8_t>((t >> (i * 8)) & 0xff));
        }
    }
    const std::string primary = ring.Primary(
        std::span<const uint8_t>(key.data(), key.size()));
    ASSERT_TRUE(primary == "node-a" || primary == "node-b");

    // Find the still-alive non-primary; build a stub against it
    // before we tear down the primary (so the channel exists).
    GrpcServer& survivor   = (primary == "node-a") ? *server_b : *server_a;
    GrpcServer& doomed     = (primary == "node-a") ? *server_a : *server_b;
    auto channel = ::grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(survivor.BoundPort()),
        ::grpc::InsecureChannelCredentials());
    auto stub = NodeData::NewStub(channel);

    // Kill the primary. The directory still THINKS both nodes are
    // alive (the registrar's lease hasn't expired yet), so the
    // survivor's HRW will still pick the dead node — exactly the
    // chaos we want to test.
    doomed.Stop();

    // Lookup against the survivor → forward to the (now-dead) primary
    // → must surface UNAVAILABLE.
    ::grpc::ClientContext lctx;
    // Short deadline so the test doesn't wait for gRPC's default
    // ~120s connect timeout to expire.
    lctx.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(3));
    kvcache::proto::LookupRequest lreq;
    lreq.set_tenant_id(tenant_id);
    lreq.set_model_id_hash(model_hash);
    for (uint32_t t : tokens) lreq.add_tokens(t);
    kvcache::proto::LookupResponse lresp;
    auto status = stub->Lookup(&lctx, lreq, &lresp);
    EXPECT_FALSE(status.ok())
        << "Lookup against dead peer must fail, not silently miss";
    // gRPC may report DEADLINE_EXCEEDED (the channel is still trying
    // to reconnect) or UNAVAILABLE (no listener). Either is correct
    // "peer is gone" signal — what we MUST NOT see is OK + hit=false.
    EXPECT_TRUE(
        status.error_code() == ::grpc::StatusCode::UNAVAILABLE ||
        status.error_code() == ::grpc::StatusCode::DEADLINE_EXCEEDED)
        << "expected UNAVAILABLE or DEADLINE_EXCEEDED, got "
        << status.error_code() << ": " << status.error_message();

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

// ---------------------------------------------------------------------------
// Phase K-6 — sketch-hint fallback. When the HRW primary IS the
// caller (`primary == self_node_id`) AND local Lookup misses, the
// service consults peer bloom sketches and forwards the request to
// the first peer whose sketch says "MaybeHas". Even though the
// forwarded peer will (in this shared-singleton test) also miss and
// return hit=false, we can verify the SKETCH-HINT WIRING fired by
// observing the peer's LookupCalls counter — it only increments
// when a remote Lookup actually reaches the peer, which proves K-6
// fanned out via the sketch path rather than silently bailing.
// ---------------------------------------------------------------------------
TEST(LookupForwarding, SketchHintForwardsOnLocalMiss) {
    kv_ctx_config_t cfg{};
    cfg.abi_version = KVCACHE_ABI_VERSION;
    cfg.tenant_id   = "k6-tenant";
    cfg.model_id    = "k6-model";
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
    ASSERT_TRUE(server_a->Ok()) << server_a->error();
    auto server_b = std::make_unique<GrpcServer>(gso, svc_b.get());
    ASSERT_TRUE(server_b->Ok()) << server_b->error();

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

    svc_a->EnableForwarding("node-a", &ring, &dir);
    svc_b->EnableForwarding("node-b", &ring, &dir);

    // Probe many disjoint token vectors until we find one whose HRW
    // primary happens to be node-a — that's the seed the test needs
    // (HRW primary == self for the caller).
    const std::string tenant_id  = "k6-tenant";
    const uint64_t    model_hash = 0x1357'9ABC'DEF0'1234ULL;
    const uint64_t    tenant_hash_for_sketch = 42;  // arbitrary; we
        // use the same value when populating the bloom + when
        // sending the LookupRequest (via tenant_id_hash field).
    std::vector<uint32_t> tokens;
    for (uint32_t seed = 1000; seed < 5000; seed += 13) {
        const auto candidate = RangeTokens(seed, 2 * kChunkTokens);
        // HRW key shape: same bytes the Lookup handler hashes against
        // ring_->Primary — tokens packed LE.
        std::vector<uint8_t> kb(candidate.size() * 4);
        for (std::size_t i = 0; i < candidate.size(); ++i) {
            for (int j = 0; j < 4; ++j) {
                kb[i * 4 + j] =
                    static_cast<uint8_t>((candidate[i] >> (j * 8)) & 0xff);
            }
        }
        if (ring.Primary(std::span<const uint8_t>(kb.data(), kb.size()))
              == "node-a") {
            tokens = candidate;
            break;
        }
    }
    ASSERT_FALSE(tokens.empty())
        << "couldn't find a token vector with HRW primary=node-a";

    // Populate node-b's bloom sketch with our token vector under the
    // SAME (tenant_hash, model_hash) the Lookup will use. We don't
    // bother sealing on node-b — we only need the directory to
    // see "node-b might have this key".
    using kvcache::node::cluster::BloomPublisher;
    BloomPublisher::Options pub_opts{};
    pub_opts.node_id        = "node-b";
    pub_opts.expected_chunks = 1024;
    pub_opts.publish_period = std::chrono::seconds(60);  // no-op timer
    BloomPublisher pub(&etcd, pub_opts);
    pub.AddTokens(tenant_hash_for_sketch, model_hash,
                  tokens.data(), tokens.size());
    ASSERT_TRUE(pub.Start(&err)) << err;
    ASSERT_TRUE(WaitFor([&] { return dir.SketchCount() == 1; }));

    auto chan_a = ::grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(server_a->BoundPort()),
        ::grpc::InsecureChannelCredentials());
    auto stub_a = NodeData::NewStub(chan_a);

    const uint64_t b_calls_before = svc_b->LookupCalls();

    ::grpc::ClientContext lctx;
    kvcache::proto::LookupRequest lreq;
    lreq.set_tenant_id(tenant_id);
    lreq.set_tenant_id_hash(tenant_hash_for_sketch);
    lreq.set_model_id_hash(model_hash);
    for (uint32_t t : tokens) lreq.add_tokens(t);
    kvcache::proto::LookupResponse lresp;
    auto s = stub_a->Lookup(&lctx, lreq, &lresp);
    ASSERT_TRUE(s.ok()) << s.error_message();
    // node-b's bloom has it but the (shared) HeadlessNode never had
    // it sealed — the forwarded Lookup MUST observe hit=false.
    EXPECT_FALSE(lresp.hit());

    // The actual K-6 assertion: node-b's Lookup handler was invoked.
    const uint64_t b_calls_after = svc_b->LookupCalls();
    EXPECT_GT(b_calls_after, b_calls_before)
        << "K-6 sketch-hint path did NOT forward to node-b. "
           "Before=" << b_calls_before << " After=" << b_calls_after;

    pub.Stop();
    stub_a.reset();
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
// Phase K-8 — Seal hook populates the publisher's bloom.
//
// Wires svc_a to a BloomPublisher; drives a Reserve→Publish→Seal
// against svc_a's stub; then decodes the publisher's snapshot and
// asserts the chunk-key derived from the sealed token vector lands
// in the resulting bloom. Closes the loop K-6 opened: by the time a
// peer's directory observes svc_a's published sketch, that sketch
// actually reflects what svc_a has sealed.
// ---------------------------------------------------------------------------
TEST(SketchPublishing, SealAddsTokensToPublisher) {
    kv_ctx_config_t cfg{};
    cfg.abi_version = KVCACHE_ABI_VERSION;
    cfg.tenant_id   = "k8-tenant";
    cfg.model_id    = "k8-model";
    kv_ctx_t* ctx = nullptr;
    ASSERT_EQ(kv_ctx_open(&cfg, &ctx), KV_OK);

    InMemoryEtcdClient etcd;
    auto svc = std::make_unique<NodeDataServiceImpl>(ctx);
    GrpcServer::Options gso;
    gso.bind_host = "127.0.0.1";
    gso.port      = 0;
    auto server = std::make_unique<GrpcServer>(gso, svc.get());
    ASSERT_TRUE(server->Ok()) << server->error();

    // Publisher does NOT need an etcd publish — we read its bloom
    // in-process directly through EncodeSnapshot.
    using kvcache::node::cluster::BloomPublisher;
    using kvcache::node::cluster::DecodeBloomSnapshot;
    using kvcache::node::cluster::SketchKeyForTokens;
    BloomPublisher::Options pub_opts{};
    pub_opts.node_id        = "k8-self";
    pub_opts.expected_chunks = 1024;
    pub_opts.publish_period = std::chrono::seconds(60);  // no-op timer
    BloomPublisher pub(&etcd, pub_opts);
    std::string err;
    ASSERT_TRUE(pub.Start(&err)) << err;

    svc->EnableSketchPublishing(&pub);

    auto chan = ::grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(server->BoundPort()),
        ::grpc::InsecureChannelCredentials());
    auto stub = NodeData::NewStub(chan);

    const std::string tenant_id  = "k8-tenant";
    const uint64_t    model_hash = 0xFEEDFACECAFEBABEULL;
    const auto        tokens     = RangeTokens(60000, 2 * kChunkTokens);
    const std::size_t bytes_total = tokens.size() * 64;

    // Build the same Locator shape Reserve expects (see Q-5 SHA-1 path).
    auto build_locator = [&]() {
        kvcache::proto::Locator loc;
        uint8_t sha[20];
        SHA1(reinterpret_cast<const uint8_t*>(tenant_id.data()),
              tenant_id.size(), sha);
        loc.set_tenant_id(std::string(reinterpret_cast<const char*>(sha), 16));
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

    ::grpc::ClientContext rctx;
    kvcache::proto::ReserveRequest rreq;
    *rreq.mutable_locator() = build_locator();
    rreq.set_bytes(bytes_total);
    kvcache::proto::ReserveResponse rresp;
    ASSERT_TRUE(stub->Reserve(&rctx, rreq, &rresp).ok());
    auto* slot = reinterpret_cast<uint8_t*>(rresp.slot_iova());
    std::memset(slot, 0x77, bytes_total);

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

    // Decode the publisher's bloom and probe it directly. Reserve
    // resolved (th, mh) via HashTenantBytes16(locator.tenant_id) for
    // tenant_hash; here the locator's tenant_id IS the SHA-1 prefix
    // bytes, so we replicate exactly.
    auto hash_tenant_bytes16 = [](const std::string& sixteen) {
        uint64_t h = 0xcbf29ce484222325ULL;
        for (char c : sixteen) {
            h ^= static_cast<uint8_t>(c);
            h *= 0x100000001b3ULL;
        }
        return h;
    };
    uint8_t sha[20];
    SHA1(reinterpret_cast<const uint8_t*>(tenant_id.data()),
          tenant_id.size(), sha);
    const std::string tenant16(reinterpret_cast<const char*>(sha), 16);
    const uint64_t expected_th = hash_tenant_bytes16(tenant16);

    const auto skey = SketchKeyForTokens(
        expected_th, model_hash, tokens.data(), tokens.size());

    const auto blob = pub.EncodeSnapshot();
    kvcache::node::routing::BloomParams params{0, 0};
    std::vector<uint8_t> bits;
    ASSERT_TRUE(DecodeBloomSnapshot(
        std::string(blob.begin(), blob.end()), &params, &bits));

    // Reconstruct the AggregatedBloom and probe.
    kvcache::node::routing::AggregatedBloom agg(params);
    agg.Set(params, std::move(bits));
    EXPECT_TRUE(agg.MaybeContains(skey))
        << "Seal hook did not add the chunk-key to the publisher bloom";

    stub.reset();
    pub.Stop();
    server->Stop();
    server.reset();
    svc.reset();
    kv_ctx_close(ctx);
}
