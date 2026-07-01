// Phase A1 — UcxBackend tests.
//
// When built with -DKVCACHE_ENABLE_UCX=ON (KVCACHE_HAVE_UCX defined), these
// stand up two independent UcxBackends in one process — playing the roles of a
// remote "owner" node and a local "puller" node — and drive real one-sided RMA
// between them over whatever transport UCX auto-selects (shared memory / TCP on
// a plain box, IB/RoCE verbs on an RDMA host). The exact same code path a
// production RDMA deployment takes is exercised, minus the physical NIC.
//
// Without UCX compiled in, the factory must report "not compiled in".
#include "transport/ucx_backend.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "transport/nixl_wrapper.h"

using kvcache::node::transport::BackendOptions;
using kvcache::node::transport::CreateBackend;
using kvcache::node::transport::INixlBackend;
using kvcache::node::transport::kInvalidCompletionId;
using kvcache::node::transport::kInvalidMrKey;
using kvcache::node::transport::MrKey;
using kvcache::node::transport::PullRequest;
using kvcache::node::transport::PushRequest;
using kvcache::node::transport::RemoteMrDescriptor;

#if KVCACHE_HAVE_UCX

namespace {

std::vector<uint8_t> Pattern(std::size_t n, uint8_t seed) {
    std::vector<uint8_t> v(n);
    for (std::size_t i = 0; i < n; ++i) v[i] = static_cast<uint8_t>((i * 31 + seed) & 0xFF);
    return v;
}

// Owner exports a region; puller imports it. Returns (puller-side remote key).
MrKey ImportFrom(INixlBackend* owner, MrKey owner_mr, INixlBackend* puller,
                 std::string* err) {
    RemoteMrDescriptor desc;
    if (!owner->ExportMr(owner_mr, &desc, err)) return kInvalidMrKey;
    return puller->ImportRemoteMr(desc, err);
}

}  // namespace

TEST(UcxBackend, FactoryCreates) {
    BackendOptions o;
    o.name = "ucx";
    std::string err;
    auto b = CreateBackend(o, &err);
    ASSERT_NE(b, nullptr) << err;
    EXPECT_EQ(b->Name(), "ucx");
}

TEST(UcxBackend, RemoteGetRoundTrip) {
    BackendOptions o;
    o.name = "ucx";
    std::string err;
    auto owner = CreateBackend(o, &err);
    ASSERT_NE(owner, nullptr) << err;
    auto puller = CreateBackend(o, &err);
    ASSERT_NE(puller, nullptr) << err;

    constexpr std::size_t kN = 4096;
    auto src = Pattern(kN, 7);
    MrKey owner_mr = owner->RegisterRegion(src.data(), src.size(), &err);
    ASSERT_NE(owner_mr, kInvalidMrKey) << err;

    MrKey remote = ImportFrom(owner.get(), owner_mr, puller.get(), &err);
    ASSERT_NE(remote, kInvalidMrKey) << err;
    EXPECT_TRUE(puller->IsRemote(remote));

    std::vector<uint8_t> dst(kN, 0);
    MrKey dst_mr = puller->RegisterRegion(dst.data(), dst.size(), &err);
    ASSERT_NE(dst_mr, kInvalidMrKey) << err;
    EXPECT_FALSE(puller->IsRemote(dst_mr));

    PullRequest pr{dst_mr, 0, remote, 0, kN};
    auto cid = puller->Pull(pr, &err);
    ASSERT_NE(cid, kInvalidCompletionId) << err;
    ASSERT_TRUE(puller->Wait(cid, 5000, &err)) << err;
    EXPECT_EQ(dst, src) << "GET must land the owner's bytes in the puller buffer";
}

TEST(UcxBackend, RemoteGetSubRangeLeavesSurroundingUntouched) {
    BackendOptions o;
    o.name = "ucx";
    std::string err;
    auto owner = CreateBackend(o, &err);
    auto puller = CreateBackend(o, &err);
    ASSERT_NE(owner, nullptr);
    ASSERT_NE(puller, nullptr);

    constexpr std::size_t kN = 1024;
    auto src = Pattern(kN, 3);
    MrKey owner_mr = owner->RegisterRegion(src.data(), src.size(), &err);
    MrKey remote = ImportFrom(owner.get(), owner_mr, puller.get(), &err);
    ASSERT_NE(remote, kInvalidMrKey) << err;

    std::vector<uint8_t> dst(kN, 0xEE);
    MrKey dst_mr = puller->RegisterRegion(dst.data(), dst.size(), &err);
    // Pull [256,512) of src into dst[256,512).
    PullRequest pr{dst_mr, 256, remote, 256, 256};
    auto cid = puller->Pull(pr, &err);
    ASSERT_NE(cid, kInvalidCompletionId) << err;
    ASSERT_TRUE(puller->Wait(cid, 5000, &err)) << err;
    for (std::size_t i = 0; i < kN; ++i) {
        if (i >= 256 && i < 512) EXPECT_EQ(dst[i], src[i]) << "at " << i;
        else EXPECT_EQ(dst[i], 0xEE) << "surrounding byte clobbered at " << i;
    }
}

TEST(UcxBackend, RemotePutRoundTrip) {
    BackendOptions o;
    o.name = "ucx";
    std::string err;
    auto owner = CreateBackend(o, &err);   // holds the destination region
    auto pusher = CreateBackend(o, &err);  // has the bytes, pushes them
    ASSERT_NE(owner, nullptr);
    ASSERT_NE(pusher, nullptr);

    constexpr std::size_t kN = 2048;
    std::vector<uint8_t> target(kN, 0);
    MrKey owner_mr = owner->RegisterRegion(target.data(), target.size(), &err);
    MrKey remote = ImportFrom(owner.get(), owner_mr, pusher.get(), &err);
    ASSERT_NE(remote, kInvalidMrKey) << err;

    auto payload = Pattern(kN, 99);
    MrKey src_mr = pusher->RegisterRegion(payload.data(), payload.size(), &err);
    ASSERT_NE(src_mr, kInvalidMrKey) << err;

    PushRequest pr{src_mr, 0, remote, 0, kN};
    auto cid = pusher->Push(pr, &err);
    ASSERT_NE(cid, kInvalidCompletionId) << err;
    ASSERT_TRUE(pusher->Wait(cid, 5000, &err)) << err;
    EXPECT_EQ(target, payload) << "PUT must deposit bytes into the owner region";
}

TEST(UcxBackend, IntraProcessLocalPullIsMemcpy) {
    BackendOptions o;
    o.name = "ucx";
    std::string err;
    auto b = CreateBackend(o, &err);
    ASSERT_NE(b, nullptr) << err;

    constexpr std::size_t kN = 512;
    auto src = Pattern(kN, 5);
    std::vector<uint8_t> dst(kN, 0);
    MrKey src_mr = b->RegisterRegion(src.data(), src.size(), &err);
    MrKey dst_mr = b->RegisterRegion(dst.data(), dst.size(), &err);
    ASSERT_NE(src_mr, kInvalidMrKey);
    ASSERT_NE(dst_mr, kInvalidMrKey);

    PullRequest pr{dst_mr, 0, src_mr, 0, kN};  // both local
    auto cid = b->Pull(pr, &err);
    ASSERT_NE(cid, kInvalidCompletionId) << err;
    EXPECT_EQ(dst, src);
}

TEST(UcxBackend, OutOfBoundsRejected) {
    BackendOptions o;
    o.name = "ucx";
    std::string err;
    auto owner = CreateBackend(o, &err);
    auto puller = CreateBackend(o, &err);
    ASSERT_NE(owner, nullptr);
    ASSERT_NE(puller, nullptr);

    constexpr std::size_t kN = 256;
    auto src = Pattern(kN, 1);
    MrKey owner_mr = owner->RegisterRegion(src.data(), src.size(), &err);
    MrKey remote = ImportFrom(owner.get(), owner_mr, puller.get(), &err);
    std::vector<uint8_t> dst(kN, 0);
    MrKey dst_mr = puller->RegisterRegion(dst.data(), dst.size(), &err);

    PullRequest pr{dst_mr, 0, remote, 0, kN * 2};  // over remote length
    EXPECT_EQ(puller->Pull(pr, &err), kInvalidCompletionId);
}

#else  // !KVCACHE_HAVE_UCX

TEST(UcxBackend, NotCompiledInReportsError) {
    BackendOptions o;
    o.name = "ucx";
    std::string err;
    auto b = CreateBackend(o, &err);
    EXPECT_EQ(b, nullptr);
    EXPECT_FALSE(err.empty());
}

#endif  // KVCACHE_HAVE_UCX
