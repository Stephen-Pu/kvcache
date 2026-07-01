// KVZ-1 — KvTensorCodec rate-distortion + round-trip tests.
//
// KV-cache values are smooth across token positions, so the test data is a
// synthetic tensor whose per-element value drifts slowly token-to-token
// (plus a per-element offset) — representative of real K/V correlation,
// which is exactly what the delta+quant pipeline exploits.
#include "codec/kv_tensor_codec.h"

#include <gtest/gtest.h>

#include <cmath>
#include <vector>

using kvcache::codec::DecodeKvTensor;
using kvcache::codec::EncodeKvTensor;
using kvcache::codec::KvCodecParams;
using kvcache::codec::KvShape;

namespace {

// Smoothly-varying synthetic KV: value[t][e] = base(e) + slope(e)*t + small
// wiggle. Adjacent tokens differ by ~slope — small, so DPCM residuals are
// tiny relative to absolute magnitude.
std::vector<float> SmoothKv(uint32_t T, uint32_t E) {
    std::vector<float> v(static_cast<std::size_t>(T) * E);
    for (uint32_t e = 0; e < E; ++e) {
        const float base  = 0.5f * static_cast<float>((e % 17)) - 4.0f;
        const float slope = 0.01f * static_cast<float>((e % 7) + 1);
        for (uint32_t t = 0; t < T; ++t) {
            const float wiggle = 0.02f * std::sin(0.1f * (t + e));
            v[static_cast<std::size_t>(t) * E + e] =
                base + slope * static_cast<float>(t) + wiggle;
        }
    }
    return v;
}

double MaxAbsErr(const std::vector<float>& a, const std::vector<float>& b) {
    double m = 0.0;
    for (std::size_t i = 0; i < a.size(); ++i)
        m = std::max(m, std::fabs(static_cast<double>(a[i]) - b[i]));
    return m;
}

double ValueRange(const std::vector<float>& a) {
    float lo = a[0], hi = a[0];
    for (float x : a) { lo = std::min(lo, x); hi = std::max(hi, x); }
    return static_cast<double>(hi) - lo;
}

// B2 — KV with wildly heterogeneous per-channel magnitudes: channel e is
// scaled by 10^((e%5)-2), i.e. spanning ~0.01 … ~100 across channels, smooth
// along tokens. This is the realistic "outlier channel" case where a single
// per-token scale (driven by the biggest channel) crushes the small channels.
std::vector<float> HeteroKv(uint32_t T, uint32_t E) {
    std::vector<float> v(static_cast<std::size_t>(T) * E);
    for (uint32_t e = 0; e < E; ++e) {
        const double mag = std::pow(10.0, static_cast<double>(e % 5) - 2.0);
        const double base = mag * (0.3 + 0.1 * (e % 3));
        const double slope = mag * 0.01;
        for (uint32_t t = 0; t < T; ++t) {
            v[static_cast<std::size_t>(t) * E + e] =
                static_cast<float>(base + slope * t + 0.02 * mag * std::sin(0.1 * (t + e)));
        }
    }
    return v;
}

// Worst per-channel relative RMS error: max over channels of
// rms(orig-dec) / rms(orig). Per-channel quant keeps this ~uniform (~1/qmax);
// per-token quant lets it blow up toward 1.0 on the small-magnitude channels.
double WorstChannelRelErr(const std::vector<float>& a, const std::vector<float>& b,
                          uint32_t T, uint32_t E) {
    double worst = 0.0;
    for (uint32_t e = 0; e < E; ++e) {
        double se = 0.0, sref = 0.0;
        for (uint32_t t = 0; t < T; ++t) {
            const std::size_t i = static_cast<std::size_t>(t) * E + e;
            const double d = static_cast<double>(a[i]) - b[i];
            se += d * d;
            sref += static_cast<double>(a[i]) * a[i];
        }
        const double rel = std::sqrt(se / T) / (std::sqrt(sref / T) + 1e-12);
        worst = std::max(worst, rel);
    }
    return worst;
}

}  // namespace

TEST(KvTensorCodec, RoundTripWithinErrorBoundInt8) {
    const KvShape shape{128, 256};
    auto orig = SmoothKv(shape.n_tokens, shape.elems_per_token);
    std::string err;
    std::vector<uint8_t> blob;
    ASSERT_TRUE(EncodeKvTensor(orig.data(), shape, {8, true}, &blob, &err)) << err;

    std::vector<float> dec;
    KvShape got{};
    ASSERT_TRUE(DecodeKvTensor(blob.data(), blob.size(), &dec, &got, &err)) << err;
    EXPECT_EQ(got.n_tokens, shape.n_tokens);
    EXPECT_EQ(got.elems_per_token, shape.elems_per_token);
    ASSERT_EQ(dec.size(), orig.size());
    // int8 over a smooth tensor: max abs error well under 1% of the range.
    EXPECT_LT(MaxAbsErr(orig, dec), 0.01 * ValueRange(orig));
}

TEST(KvTensorCodec, HigherBitsLowerErrorRateDistortion) {
    const KvShape shape{64, 512};
    auto orig = SmoothKv(shape.n_tokens, shape.elems_per_token);
    std::string err;

    std::vector<uint8_t> b8, b4;
    ASSERT_TRUE(EncodeKvTensor(orig.data(), shape, {8, true}, &b8, &err)) << err;
    ASSERT_TRUE(EncodeKvTensor(orig.data(), shape, {4, true}, &b4, &err)) << err;

    std::vector<float> d8, d4;
    KvShape s{};
    ASSERT_TRUE(DecodeKvTensor(b8.data(), b8.size(), &d8, &s, &err)) << err;
    ASSERT_TRUE(DecodeKvTensor(b4.data(), b4.size(), &d4, &s, &err)) << err;

    const double e8 = MaxAbsErr(orig, d8);
    const double e4 = MaxAbsErr(orig, d4);
    EXPECT_LT(e8, e4) << "8-bit must reconstruct more accurately than 4-bit";
}

TEST(KvTensorCodec, CompressesBelowRawFp32) {
    const KvShape shape{256, 256};  // 64K elems × 4B = 256 KiB raw
    auto orig = SmoothKv(shape.n_tokens, shape.elems_per_token);
    const std::size_t raw = orig.size() * sizeof(float);
    std::string err;

    std::vector<uint8_t> b8;
    ASSERT_TRUE(EncodeKvTensor(orig.data(), shape, {8, true}, &b8, &err)) << err;
    // int8 quant alone is ~4x; even with per-token scale overhead the blob is
    // comfortably under half the raw fp32 size (more with the zstd stage).
    EXPECT_LT(b8.size(), raw / 2)
        << "int8 blob " << b8.size() << " vs raw " << raw;

    std::vector<uint8_t> b4;
    ASSERT_TRUE(EncodeKvTensor(orig.data(), shape, {4, true}, &b4, &err)) << err;
    EXPECT_LT(b4.size(), b8.size()) << "int4 must be smaller than int8";
}

// B2 — the headline result: on heterogeneous-magnitude channels, per-channel
// quantization reconstructs the small channels vastly better than the original
// per-token scheme, at the same bit-width and near-identical blob size.
TEST(KvTensorCodec, PerChannelBeatsPerTokenOnHeteroMagnitudes) {
    const KvShape shape{96, 320};
    auto orig = HeteroKv(shape.n_tokens, shape.elems_per_token);
    std::string err;

    std::vector<uint8_t> b_pc, b_pt;
    ASSERT_TRUE(EncodeKvTensor(orig.data(), shape, {8, true, /*per_channel=*/true},
                               &b_pc, &err)) << err;
    ASSERT_TRUE(EncodeKvTensor(orig.data(), shape, {8, true, /*per_channel=*/false},
                               &b_pt, &err)) << err;

    std::vector<float> d_pc, d_pt;
    KvShape s{};
    ASSERT_TRUE(DecodeKvTensor(b_pc.data(), b_pc.size(), &d_pc, &s, &err)) << err;
    ASSERT_TRUE(DecodeKvTensor(b_pt.data(), b_pt.size(), &d_pt, &s, &err)) << err;

    const double rel_pc = WorstChannelRelErr(orig, d_pc, shape.n_tokens, shape.elems_per_token);
    const double rel_pt = WorstChannelRelErr(orig, d_pt, shape.n_tokens, shape.elems_per_token);

    // Per-token destroys the small channels (worst relative error near 1.0);
    // per-channel keeps every channel faithful (a few % at int8).
    EXPECT_LT(rel_pc, 0.05) << "per-channel worst-channel rel err " << rel_pc;
    EXPECT_GT(rel_pt, 0.30) << "per-token should badly quantize small channels, got " << rel_pt;
    EXPECT_LT(rel_pc, rel_pt / 4.0)
        << "per-channel (" << rel_pc << ") must be far better than per-token (" << rel_pt << ")";

    // The R-D win costs essentially nothing in size (scales are E vs T fp32).
    const double ratio = static_cast<double>(b_pc.size()) / static_cast<double>(b_pt.size());
    EXPECT_LT(ratio, 1.15) << "per-channel blob " << b_pc.size() << " vs per-token " << b_pt.size();
}

// B2 back-compat: a per-token-encoded blob still decodes correctly (the flag
// bit selects the path), so old blobs remain readable.
TEST(KvTensorCodec, PerTokenModeStillRoundTrips) {
    const KvShape shape{48, 128};
    auto orig = SmoothKv(shape.n_tokens, shape.elems_per_token);
    std::string err;
    std::vector<uint8_t> blob;
    ASSERT_TRUE(EncodeKvTensor(orig.data(), shape, {8, true, /*per_channel=*/false},
                               &blob, &err)) << err;
    std::vector<float> dec; KvShape s{};
    ASSERT_TRUE(DecodeKvTensor(blob.data(), blob.size(), &dec, &s, &err)) << err;
    EXPECT_LT(MaxAbsErr(orig, dec), 0.01 * ValueRange(orig));
}

TEST(KvTensorCodec, DeltaOffStillRoundTrips) {
    const KvShape shape{32, 64};
    auto orig = SmoothKv(shape.n_tokens, shape.elems_per_token);
    std::string err;
    std::vector<uint8_t> blob;
    ASSERT_TRUE(EncodeKvTensor(orig.data(), shape, {8, false}, &blob, &err)) << err;
    std::vector<float> dec;
    KvShape s{};
    ASSERT_TRUE(DecodeKvTensor(blob.data(), blob.size(), &dec, &s, &err)) << err;
    EXPECT_LT(MaxAbsErr(orig, dec), 0.01 * ValueRange(orig));
}

TEST(KvTensorCodec, SingleTokenAndAllZero) {
    std::string err;
    // 1 token.
    std::vector<float> one(64, 3.5f);
    std::vector<uint8_t> blob;
    ASSERT_TRUE(EncodeKvTensor(one.data(), {1, 64}, {8, true}, &blob, &err)) << err;
    std::vector<float> dec; KvShape s{};
    ASSERT_TRUE(DecodeKvTensor(blob.data(), blob.size(), &dec, &s, &err)) << err;
    EXPECT_LT(MaxAbsErr(one, dec), 0.05);
    // All-zero tensor → scale defaults to 1, exact round-trip to zero.
    std::vector<float> zeros(128, 0.0f);
    ASSERT_TRUE(EncodeKvTensor(zeros.data(), {4, 32}, {8, true}, &blob, &err)) << err;
    ASSERT_TRUE(DecodeKvTensor(blob.data(), blob.size(), &dec, &s, &err)) << err;
    EXPECT_EQ(MaxAbsErr(zeros, dec), 0.0);
}

TEST(KvTensorCodec, RejectsBadParamsAndShape) {
    std::string err;
    std::vector<float> d(16, 1.0f);
    std::vector<uint8_t> blob;
    EXPECT_FALSE(EncodeKvTensor(d.data(), {4, 4}, {3, true}, &blob, &err));  // bad bits
    EXPECT_FALSE(EncodeKvTensor(d.data(), {0, 4}, {8, true}, &blob, &err));  // 0 tokens
    EXPECT_FALSE(EncodeKvTensor(d.data(), {4, 0}, {8, true}, &blob, &err));  // 0 elems
    EXPECT_FALSE(EncodeKvTensor(nullptr, {4, 4}, {8, true}, &blob, &err));   // null
}

TEST(KvTensorCodec, RejectsCorruptBlob) {
    std::string err;
    std::vector<float> orig = SmoothKv(8, 16);
    std::vector<uint8_t> blob;
    ASSERT_TRUE(EncodeKvTensor(orig.data(), {8, 16}, {8, true}, &blob, &err)) << err;
    std::vector<float> dec; KvShape s{};
    // Short blob.
    EXPECT_FALSE(DecodeKvTensor(blob.data(), 4, &dec, &s, &err));
    // Bad magic.
    auto bad = blob; bad[0] = 'X';
    EXPECT_FALSE(DecodeKvTensor(bad.data(), bad.size(), &dec, &s, &err));
}
