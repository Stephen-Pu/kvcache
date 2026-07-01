// LLD §3.4 / Roadmap Phase-2 — KvTensorCodec implementation.
#include "codec/kv_tensor_codec.h"

#include <cmath>
#include <cstring>

#if KVCACHE_HAVE_ZSTD
#include <zstd.h>
#endif

namespace kvcache::codec {

namespace {

constexpr char    kMagic[4] = {'K', 'V', 'T', '1'};
constexpr uint8_t kVersion  = 1;
constexpr uint8_t kFlagDelta      = 0x1;
constexpr uint8_t kFlagEntropy    = 0x2;   // payload is zstd-compressed
constexpr uint8_t kFlagPerChannel = 0x4;   // B2: scales are per-channel (E), not per-token (T)
// Header: magic(4) ver(1) bits(1) flags(1) rsvd(1) n_tokens(4) elems(4) = 16
constexpr std::size_t kHeaderSize = 16;

void PutU32LE(std::vector<uint8_t>* b, uint32_t v) {
    for (int i = 0; i < 4; ++i) b->push_back(static_cast<uint8_t>(v >> (8 * i)));
}
uint32_t GetU32LE(const uint8_t* p) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(p[i]) << (8 * i);
    return v;
}

// Packed quantized-int byte length for N values at `bits`.
std::size_t PackedLen(std::size_t n, int bits) {
    return bits == 8 ? n : (n + 1) / 2;
}

}  // namespace

bool EncodeKvTensor(const float* data, KvShape shape,
                    const KvCodecParams& params,
                    std::vector<uint8_t>* out, std::string* err) {
    if (params.bits != 8 && params.bits != 4) {
        if (err) *err = "kv_codec: bits must be 8 or 4";
        return false;
    }
    if (shape.n_tokens == 0 || shape.elems_per_token == 0) {
        if (err) *err = "kv_codec: empty shape";
        return false;
    }
    if (!data) {
        if (err) *err = "kv_codec: null data";
        return false;
    }
    const int qmax = params.bits == 8 ? 127 : 7;
    const std::size_t T = shape.n_tokens;
    const std::size_t E = shape.elems_per_token;
    const std::size_t N = T * E;
    const bool per_channel = params.per_channel;

    // scales layout: per-channel → E entries (one per element position);
    // per-token → T entries (one per token). See the header for why
    // per-channel wins on KV's heterogeneous channel magnitudes (B2).
    std::vector<float> scales(per_channel ? E : T, 1.0f);
    std::vector<int8_t> q(N, 0);
    std::vector<float> recon_prev(E, 0.0f);  // reconstructed previous token

    if (per_channel) {
        // Pass 0 — per-channel dynamic range from the OPEN-LOOP first
        // difference (data[t]-data[t-1]). The closed-loop residual is ~equal
        // in magnitude, so this is a safe (slightly conservative) per-channel
        // scale that must be fixed before any token is quantized.
        std::vector<float> amax(E, 0.0f);
        for (std::size_t t = 0; t < T; ++t) {
            const float* row = data + t * E;
            const float* prev = (params.delta && t > 0) ? data + (t - 1) * E : nullptr;
            for (std::size_t e = 0; e < E; ++e) {
                const float r = row[e] - (prev ? prev[e] : 0.0f);
                const float a = std::fabs(r);
                if (a > amax[e]) amax[e] = a;
            }
        }
        for (std::size_t e = 0; e < E; ++e)
            scales[e] = amax[e] > 0.0f ? amax[e] / static_cast<float>(qmax) : 1.0f;
        // Pass 1 — closed-loop DPCM quantize with the fixed per-channel scale.
        for (std::size_t t = 0; t < T; ++t) {
            const float* row = data + t * E;
            const bool predict = params.delta && t > 0;
            for (std::size_t e = 0; e < E; ++e) {
                const float pred = predict ? recon_prev[e] : 0.0f;
                long qi = std::lround((row[e] - pred) / scales[e]);
                if (qi > qmax) qi = qmax;
                if (qi < -qmax) qi = -qmax;
                q[t * E + e] = static_cast<int8_t>(qi);
                recon_prev[e] = pred + static_cast<float>(qi) * scales[e];
            }
        }
    } else {
        for (std::size_t t = 0; t < T; ++t) {
            const float* row = data + t * E;
            const bool predict = params.delta && t > 0;
            // Pass 1 — residual magnitude → per-token scale.
            float amax = 0.0f;
            for (std::size_t e = 0; e < E; ++e) {
                const float pred = predict ? recon_prev[e] : 0.0f;
                const float a = std::fabs(row[e] - pred);
                if (a > amax) amax = a;
            }
            const float scale = amax > 0.0f ? amax / static_cast<float>(qmax) : 1.0f;
            scales[t] = scale;
            // Pass 2 — quantize residual + reconstruct (closed-loop DPCM).
            for (std::size_t e = 0; e < E; ++e) {
                const float pred = predict ? recon_prev[e] : 0.0f;
                long qi = std::lround((row[e] - pred) / scale);
                if (qi > qmax) qi = qmax;
                if (qi < -qmax) qi = -qmax;
                q[t * E + e] = static_cast<int8_t>(qi);
                recon_prev[e] = pred + static_cast<float>(qi) * scale;
            }
        }
    }

    // Pack quantized ints.
    std::vector<uint8_t> packed;
    if (params.bits == 8) {
        packed.resize(N);
        for (std::size_t i = 0; i < N; ++i)
            packed[i] = static_cast<uint8_t>(q[i]);
    } else {  // int4: two signed nibbles per byte
        packed.resize((N + 1) / 2, 0);
        for (std::size_t i = 0; i < N; ++i) {
            const uint8_t nib = static_cast<uint8_t>(q[i]) & 0x0F;
            if (i % 2 == 0) packed[i / 2] |= nib;
            else            packed[i / 2] |= static_cast<uint8_t>(nib << 4);
        }
    }

    uint8_t flags = 0;
    if (params.delta) flags |= kFlagDelta;
    if (per_channel)  flags |= kFlagPerChannel;

    // Optional entropy stage.
    std::vector<uint8_t> payload;
#if KVCACHE_HAVE_ZSTD
    {
        const std::size_t bound = ZSTD_compressBound(packed.size());
        payload.resize(bound);
        const std::size_t r = ZSTD_compress(payload.data(), bound,
                                            packed.data(), packed.size(), 3);
        if (ZSTD_isError(r)) {
            if (err) *err = std::string("kv_codec: zstd: ") + ZSTD_getErrorName(r);
            return false;
        }
        payload.resize(r);
        flags |= kFlagEntropy;
    }
#else
    payload = std::move(packed);
#endif

    out->clear();
    out->reserve(kHeaderSize + T * sizeof(float) + payload.size());
    out->insert(out->end(), kMagic, kMagic + 4);
    out->push_back(kVersion);
    out->push_back(static_cast<uint8_t>(params.bits));
    out->push_back(flags);
    out->push_back(0);  // reserved
    PutU32LE(out, shape.n_tokens);
    PutU32LE(out, shape.elems_per_token);
    // Scales (fp32, native — same-build codec): E entries if per-channel,
    // else T. The kFlagPerChannel bit tells the decoder which.
    const auto* sb = reinterpret_cast<const uint8_t*>(scales.data());
    out->insert(out->end(), sb, sb + scales.size() * sizeof(float));
    out->insert(out->end(), payload.begin(), payload.end());
    return true;
}

bool DecodeKvTensor(const uint8_t* in, std::size_t n,
                    std::vector<float>* out, KvShape* shape, std::string* err) {
    if (n < kHeaderSize || std::memcmp(in, kMagic, 4) != 0) {
        if (err) *err = "kv_codec: bad magic / short blob";
        return false;
    }
    if (in[4] != kVersion) {
        if (err) *err = "kv_codec: unsupported version";
        return false;
    }
    const int bits = in[5];
    if (bits != 8 && bits != 4) {
        if (err) *err = "kv_codec: bad bits in header";
        return false;
    }
    const uint8_t flags = in[6];
    const bool delta       = flags & kFlagDelta;
    const bool entropy     = flags & kFlagEntropy;
    const bool per_channel = flags & kFlagPerChannel;
    const uint32_t T = GetU32LE(in + 8);
    const uint32_t E = GetU32LE(in + 12);
    if (T == 0 || E == 0) {
        if (err) *err = "kv_codec: empty shape in header";
        return false;
    }
    const std::size_t N = static_cast<std::size_t>(T) * E;
    const std::size_t n_scales = per_channel ? E : T;
    const std::size_t scales_bytes = n_scales * sizeof(float);
    if (n < kHeaderSize + scales_bytes) {
        if (err) *err = "kv_codec: truncated scales";
        return false;
    }
    std::vector<float> scales(n_scales);
    std::memcpy(scales.data(), in + kHeaderSize, scales_bytes);

    const uint8_t* payload = in + kHeaderSize + scales_bytes;
    const std::size_t payload_n = n - kHeaderSize - scales_bytes;
    const std::size_t packed_len = PackedLen(N, bits);

    std::vector<uint8_t> packed;
    if (entropy) {
#if KVCACHE_HAVE_ZSTD
        // dstCapacity derived from the header shape (not the blob) — a
        // corrupt size can't drive an unbounded alloc (C3 lesson).
        packed.resize(packed_len);
        const std::size_t r = ZSTD_decompress(packed.data(), packed_len,
                                              payload, payload_n);
        if (ZSTD_isError(r) || r != packed_len) {
            if (err) *err = "kv_codec: zstd decompress failed (corrupt blob?)";
            return false;
        }
#else
        if (err) *err = "kv_codec: entropy-coded blob needs a zstd build";
        return false;
#endif
    } else {
        if (payload_n < packed_len) {
            if (err) *err = "kv_codec: truncated payload";
            return false;
        }
        packed.assign(payload, payload + packed_len);
    }

    // Unpack quantized ints.
    std::vector<int8_t> q(N);
    if (bits == 8) {
        for (std::size_t i = 0; i < N; ++i)
            q[i] = static_cast<int8_t>(packed[i]);
    } else {
        for (std::size_t i = 0; i < N; ++i) {
            const uint8_t byte = packed[i / 2];
            const uint8_t nib = (i % 2 == 0) ? (byte & 0x0F) : (byte >> 4);
            q[i] = static_cast<int8_t>(nib & 0x8 ? static_cast<int>(nib) - 16
                                                 : static_cast<int>(nib));
        }
    }

    // Reconstruct (closed-loop DPCM along token axis).
    out->assign(N, 0.0f);
    std::vector<float> recon_prev(E, 0.0f);
    for (std::size_t t = 0; t < T; ++t) {
        const bool predict = delta && t > 0;
        for (std::size_t e = 0; e < E; ++e) {
            const float scale = per_channel ? scales[e] : scales[t];
            const float pred = predict ? recon_prev[e] : 0.0f;
            const float val = pred + static_cast<float>(q[t * E + e]) * scale;
            (*out)[t * E + e] = val;
            recon_prev[e] = val;
        }
    }
    shape->n_tokens = T;
    shape->elems_per_token = E;
    return true;
}

}  // namespace kvcache::codec
