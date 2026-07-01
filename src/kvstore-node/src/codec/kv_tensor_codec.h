// LLD §3.4 / Roadmap Phase-2 — KV-tensor compression (CacheGen-class).
//
// A LOSSY, tensor-aware codec for KV-cache values — distinct from the
// generic, lossless cold-tier blob zstd (CompressingColdTier, B3.1). KV
// values are strongly correlated across token positions (adjacent tokens'
// K/V vectors are similar), so the CacheGen-class pipeline is:
//
//   1. delta along the token axis  — closed-loop DPCM (predict each token
//      from the *reconstructed* previous token, so quantization error does
//      NOT accumulate / drift down the sequence),
//   2. symmetric quantization to int8 / int4 (the lossy step; `bits` is the
//      rate-distortion knob). Two scale granularities (Phase B2):
//        * per-channel (default): one scale per element-position, shared
//          across tokens. KV channels have very different magnitudes (outlier
//          channels are well-documented), so a per-token scale — driven by a
//          token's largest channel — over-coarsens its small channels.
//          Per-channel lets every channel use its full int range → markedly
//          lower reconstruction error at the same bit-width.
//        * per-token (opt-in via `per_channel=false`): the original scheme,
//          one scale per token. Kept for comparison / back-compat.
//   3. optional entropy coding (zstd) of the small quantized residuals —
//      gated on KVCACHE_HAVE_ZSTD; without it the quantized bytes are
//      stored raw (quantization still gives ~4x at int8 / ~8x at int4).
//
// The codec operates on a flat fp32 array laid out [n_tokens][elems_per_token]
// (a prod integration converts the engine's fp16/bf16 KV to fp32 at the
// boundary). Reconstruction is approximate, with per-element error bounded
// by the per-token quantization step (≈ max|residual| / qmax).
//
// This is a standalone library: it is NOT yet wired into the hot path —
// that needs an engine to supply the (tokens, layers, heads, dims, dtype)
// layout at the Core ABI boundary. Shipping the codec + its rate-distortion
// characterization is the self-contained slice.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace kvcache::codec {

struct KvShape {
    uint32_t n_tokens        = 0;
    uint32_t elems_per_token = 0;  // layers*heads*head_dim flattened
};

struct KvCodecParams {
    int  bits         = 8;     // 8 or 4 — the rate-distortion knob
    bool delta        = true;  // closed-loop DPCM along the token axis
    bool per_channel  = true;  // B2: per-channel scale (vs per-token if false)
};

// Encode `data` (n_tokens * elems_per_token fp32 values, row-major by token)
// into `*out`. Returns false + sets `*err` on bad shape / params.
bool EncodeKvTensor(const float* data, KvShape shape,
                    const KvCodecParams& params,
                    std::vector<uint8_t>* out, std::string* err);

// Decode a blob produced by EncodeKvTensor back into `*out` (fp32), filling
// `*shape`. Lossy: values are within the per-token quantization step of the
// originals. Returns false + sets `*err` on a malformed blob or a codec the
// build can't decode (e.g. zstd-entropy blob on a no-zstd build).
bool DecodeKvTensor(const uint8_t* in, std::size_t n,
                    std::vector<float>* out, KvShape* shape, std::string* err);

}  // namespace kvcache::codec
