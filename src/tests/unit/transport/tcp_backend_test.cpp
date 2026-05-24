// TcpBackend: cross-instance Pull over a real TCP socket.
//
// We spin up two TcpBackend instances in the same process, bound to
// distinct OS-picked ports. Backend "server" registers a buffer with
// known content; backend "client" imports the server's descriptor,
// allocates a destination buffer, and issues a Pull. Verify the bytes
// transit through the real socket and land in client's destination.
//
// This exercises the full INixlBackend distributed surface: ExportMr +
// ImportRemoteMr + Pull-over-network. When a UCX or RDMA backend lands
// in Phase C-2 it must pass the same kinds of contract tests.
#include "transport/tcp_backend.h"
#include "transport/nixl_wrapper.h"

#include <gtest/gtest.h>
#include <chrono>
#include <cstring>
#include <numeric>
#include <thread>
#include <vector>

using namespace kvcache::node::transport;

namespace {
BackendOptions OnLocalhost(uint32_t port = 0) {
    BackendOptions o;
    o.name = "tcp";
    o.bind_host = "127.0.0.1";
    o.bind_port = port;
    return o;
}
}  // namespace

TEST(TcpBackendTest, ListenerBindsAndExposesPort) {
    TcpBackend b(OnLocalhost());
    ASSERT_TRUE(b.Ok());
    EXPECT_GT(b.ListenerPort(), 0u);
}

TEST(TcpBackendTest, RegisterAndResolveLocalMr) {
    TcpBackend b(OnLocalhost());
    ASSERT_TRUE(b.Ok());
    std::vector<uint8_t> buf(1024, 0xAB);
    std::string err;
    auto k = b.RegisterRegion(buf.data(), buf.size(), &err);
    ASSERT_NE(k, kInvalidMrKey) << err;

    void* addr = nullptr; std::size_t n = 0;
    EXPECT_TRUE(b.ResolveRegion(k, &addr, &n));
    EXPECT_EQ(addr, buf.data());
    EXPECT_EQ(n,    buf.size());

    b.UnregisterRegion(k);
    EXPECT_FALSE(b.ResolveRegion(k, &addr, &n));
}

TEST(TcpBackendTest, IntraInstancePullIsMemcpy) {
    TcpBackend b(OnLocalhost());
    ASSERT_TRUE(b.Ok());
    std::vector<uint8_t> src(256), dst(256, 0);
    std::iota(src.begin(), src.end(), 0);

    std::string err;
    auto sk = b.RegisterRegion(src.data(), src.size(), &err);
    auto dk = b.RegisterRegion(dst.data(), dst.size(), &err);
    PullRequest r{dk, 0, sk, 0, 256};
    auto cid = b.Pull(r, &err);
    ASSERT_NE(cid, kInvalidCompletionId) << err;
    EXPECT_TRUE(b.Wait(cid, 100, &err));
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), 256), 0);
}

TEST(TcpBackendTest, ExportImportRoundTripDescriptor) {
    TcpBackend b(OnLocalhost());
    ASSERT_TRUE(b.Ok());
    std::vector<uint8_t> buf(1024, 0xCD);
    std::string err;
    auto k = b.RegisterRegion(buf.data(), buf.size(), &err);
    ASSERT_NE(k, kInvalidMrKey);

    RemoteMrDescriptor desc;
    ASSERT_TRUE(b.ExportMr(k, &desc, &err)) << err;
    ASSERT_FALSE(desc.opaque.empty());

    // Reimporting in the SAME backend is allowed — the imported handle
    // refers to the same address but goes through the network path on
    // Pull. We use a separate test below for actual two-instance flow.
    auto imported = b.ImportRemoteMr(desc, &err);
    ASSERT_NE(imported, kInvalidMrKey) << err;

    void* addr = nullptr; std::size_t n = 0;
    EXPECT_TRUE(b.ResolveRegion(imported, &addr, &n));
    EXPECT_EQ(addr, nullptr);          // remote MR has no local address
    EXPECT_EQ(n,    buf.size());        // but its size IS reported
}

TEST(TcpBackendTest, CrossInstancePullOverSocket) {
    // Two backends, two ports, one process. The "server" backend holds
    // the source bytes; the "client" backend imports the descriptor and
    // pulls into its own dst buffer.
    TcpBackend server(OnLocalhost());
    TcpBackend client(OnLocalhost());
    ASSERT_TRUE(server.Ok());
    ASSERT_TRUE(client.Ok());
    ASSERT_NE(server.ListenerPort(), client.ListenerPort());

    // Server registers a buffer with a known pattern.
    constexpr std::size_t kBytes = 4096;
    std::vector<uint8_t> src(kBytes);
    for (std::size_t i = 0; i < kBytes; ++i) {
        src[i] = static_cast<uint8_t>(i & 0xff);
    }
    std::string err;
    auto server_key = server.RegisterRegion(src.data(), src.size(), &err);
    ASSERT_NE(server_key, kInvalidMrKey) << err;

    // Server exports the descriptor; in real life this would be shipped
    // over gRPC. Here we hand it directly to client.
    RemoteMrDescriptor desc;
    ASSERT_TRUE(server.ExportMr(server_key, &desc, &err)) << err;
    auto client_remote = client.ImportRemoteMr(desc, &err);
    ASSERT_NE(client_remote, kInvalidMrKey) << err;

    // Client allocates a destination and registers it locally.
    std::vector<uint8_t> dst(kBytes, 0);
    auto client_local = client.RegisterRegion(dst.data(), dst.size(), &err);
    ASSERT_NE(client_local, kInvalidMrKey);

    // Pull.
    PullRequest r{client_local, 0, client_remote, 0, kBytes};
    auto cid = client.Pull(r, &err);
    ASSERT_NE(cid, kInvalidCompletionId) << err;
    EXPECT_TRUE(client.Wait(cid, 1000, &err));

    // Verify bytes match.
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), kBytes), 0);
}

TEST(TcpBackendTest, CrossInstancePullPartialOffset) {
    TcpBackend server(OnLocalhost());
    TcpBackend client(OnLocalhost());

    // Source = 1024 bytes 0xEE, destination = 1024 bytes 0x00. Pull only
    // bytes [256, 256+512). Verify dst[0..256) == 0x00, dst[256..768) ==
    // 0xEE, dst[768..1024) == 0x00.
    std::vector<uint8_t> src(1024, 0xEE);
    std::vector<uint8_t> dst(1024, 0x00);
    std::string err;

    auto sk = server.RegisterRegion(src.data(), src.size(), &err);
    RemoteMrDescriptor desc;
    server.ExportMr(sk, &desc, &err);
    auto remote = client.ImportRemoteMr(desc, &err);
    auto dk = client.RegisterRegion(dst.data(), dst.size(), &err);

    PullRequest r{dk, 256, remote, 256, 512};
    auto cid = client.Pull(r, &err);
    ASSERT_NE(cid, kInvalidCompletionId) << err;
    EXPECT_TRUE(client.Wait(cid, 1000, &err));

    for (std::size_t i = 0; i < 256; ++i)        EXPECT_EQ(dst[i], 0x00);
    for (std::size_t i = 256; i < 768; ++i)      EXPECT_EQ(dst[i], 0xEE);
    for (std::size_t i = 768; i < 1024; ++i)     EXPECT_EQ(dst[i], 0x00);
}

TEST(TcpBackendTest, PullRejectsOutOfBoundsOffset) {
    TcpBackend server(OnLocalhost());
    TcpBackend client(OnLocalhost());

    std::vector<uint8_t> src(100), dst(100);
    std::string err;
    auto sk = server.RegisterRegion(src.data(), src.size(), &err);
    RemoteMrDescriptor desc;
    server.ExportMr(sk, &desc, &err);
    auto remote = client.ImportRemoteMr(desc, &err);
    auto dk = client.RegisterRegion(dst.data(), dst.size(), &err);

    // src_off + bytes = 50 + 100 = 150 > 100; should fail at the client's
    // pre-flight bounds check, before any socket I/O.
    PullRequest r{dk, 0, remote, 50, 100};
    EXPECT_EQ(client.Pull(r, &err), kInvalidCompletionId);
    EXPECT_FALSE(err.empty());
}

// Phase M-6 — Push: server has the bytes (src local), peer's dst is
// imported-remote. Server connects to peer's listener via the PUT op
// and deposits the bytes into peer's local MR.
TEST(TcpBackendTest, PushDepositsBytesIntoPeerMr) {
    TcpBackend server(OnLocalhost());
    TcpBackend peer(OnLocalhost());
    ASSERT_TRUE(server.Ok());
    ASSERT_TRUE(peer.Ok());

    constexpr std::size_t kBytes = 4096;
    std::vector<uint8_t> src(kBytes);
    for (std::size_t i = 0; i < kBytes; ++i) {
        src[i] = static_cast<uint8_t>((i * 37 + 5) & 0xff);
    }
    std::vector<uint8_t> dst(kBytes, 0);

    std::string err;
    // Peer registers its dst buffer and exports a descriptor pointing
    // at *its* listener.
    auto peer_local_dst = peer.RegisterRegion(dst.data(), dst.size(), &err);
    ASSERT_NE(peer_local_dst, kInvalidMrKey);
    RemoteMrDescriptor peer_dst_desc;
    ASSERT_TRUE(peer.ExportMr(peer_local_dst, &peer_dst_desc, &err)) << err;

    // Server imports the peer's descriptor → server-side remote-MR
    // key referring to peer's listener.
    auto server_remote_dst = server.ImportRemoteMr(peer_dst_desc, &err);
    ASSERT_NE(server_remote_dst, kInvalidMrKey) << err;
    EXPECT_TRUE(server.IsRemote(server_remote_dst));

    // Server registers its src bytes locally.
    auto server_local_src = server.RegisterRegion(src.data(), src.size(), &err);
    ASSERT_NE(server_local_src, kInvalidMrKey);
    EXPECT_FALSE(server.IsRemote(server_local_src));

    PushRequest r{server_local_src, 0, server_remote_dst, 0, kBytes};
    auto cid = server.Push(r, &err);
    ASSERT_NE(cid, kInvalidCompletionId) << err;
    EXPECT_TRUE(server.Wait(cid, 1000, &err));

    // Bytes landed in peer's dst buffer.
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), kBytes), 0);
}

TEST(TcpBackendTest, PushRejectsRemoteSrc) {
    TcpBackend a(OnLocalhost());
    TcpBackend b(OnLocalhost());
    std::vector<uint8_t> buf(64);
    std::string err;
    auto a_key = a.RegisterRegion(buf.data(), buf.size(), &err);
    RemoteMrDescriptor desc;
    a.ExportMr(a_key, &desc, &err);
    auto b_remote_of_a = b.ImportRemoteMr(desc, &err);

    // Push requires src_mr to be local — feeding it a remote key
    // must fail with a clear error, not crash.
    PushRequest r{b_remote_of_a, 0, b_remote_of_a, 0, 64};
    EXPECT_EQ(b.Push(r, &err), kInvalidCompletionId);
    EXPECT_NE(err.find("src_mr is not local"), std::string::npos);
}

TEST(TcpBackendTest, FactoryCreatesTcpBackend) {
    BackendOptions o = OnLocalhost();
    std::string err;
    auto b = CreateBackend(o, &err);
    ASSERT_NE(b, nullptr) << err;
    EXPECT_EQ(b->Name(), "tcp");
}
