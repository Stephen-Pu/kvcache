// LLD §3.2 — Longest-prefix-match.
#include "prefix/lpm.h"

#include <cstring>

#include "b3_facade.h"

namespace kvcache::node::prefix {

std::vector<ChunkHash> Chunkify(std::span<const uint32_t> tokens) {
    const std::size_t whole = tokens.size() / kChunkTokens;
    std::vector<ChunkHash> out;
    out.reserve(whole);

    // Each chunk hash = first 8 bytes of BLAKE3-128 over the chunk's raw token
    // bytes. We hash the tokens as little-endian 4-byte words (the wire layout
    // we use everywhere); this MUST stay consistent with seal-side hashing.
    for (std::size_t i = 0; i < whole; ++i) {
        const uint32_t* p = tokens.data() + i * kChunkTokens;
        const auto* raw = reinterpret_cast<const uint8_t*>(p);
        const auto digest = hash::Blake3_128({raw, kChunkTokens * sizeof(uint32_t)});
        ChunkHash h{};
        std::memcpy(h.data(), digest.data(), 8);
        out.push_back(h);
    }
    return out;
}

LpmOutcome LongestPrefixMatch(const ArtIndex& art,
                              std::span<const uint32_t> tokens,
                              const ArtIndex::ReaderGuard& guard) {
    LpmOutcome r{};
    const auto chunks = Chunkify(tokens);
    if (chunks.empty()) return r;
    const auto res = art.Lookup({chunks.data(), chunks.size()}, guard);
    r.matched_chunks = static_cast<uint32_t>(res.matched_chunks);
    r.matched_tokens = r.matched_chunks * kChunkTokens;
    r.leaf           = res.leaf;
    return r;
}

// Phase Q-5 — namespace fingerprint + namespace-prepended Chunkify +
// namespace-aware LPM.
ChunkHash NamespaceFingerprint(uint64_t tenant_hash, uint64_t model_hash) {
    uint8_t buf[16];
    std::memcpy(buf,     &tenant_hash, sizeof(tenant_hash));
    std::memcpy(buf + 8, &model_hash,  sizeof(model_hash));
    const auto digest = hash::Blake3_128({buf, sizeof(buf)});
    ChunkHash h{};
    std::memcpy(h.data(), digest.data(), 8);
    return h;
}

std::vector<ChunkHash> ChunkifyNS(std::span<const uint32_t> tokens,
                                    ChunkHash ns) {
    auto chunks = Chunkify(tokens);
    // Prepend ns. Cheaper than insert(begin) since we do it before
    // appending more — but the vector is sized N already, and N is
    // tiny (max ~32 chunks per request), so insert is fine here.
    chunks.insert(chunks.begin(), ns);
    return chunks;
}

LpmOutcome LongestPrefixMatchNS(const ArtIndex& art,
                                  std::span<const uint32_t> tokens,
                                  uint64_t tenant_hash,
                                  uint64_t model_hash,
                                  const ArtIndex::ReaderGuard& guard) {
    LpmOutcome r{};
    const auto ns = NamespaceFingerprint(tenant_hash, model_hash);
    const auto chunks = ChunkifyNS(tokens, ns);
    // chunks has at least 1 element (the namespace). With no token
    // chunks the path is just [ns] — that's an inner node, not a
    // leaf, so the lookup misses. Skip the call in that case so
    // ART::Lookup doesn't traverse for nothing.
    if (chunks.size() < 2) return r;
    const auto res = art.Lookup({chunks.data(), chunks.size()}, guard);
    // matched_chunks from ART includes the namespace; expose only the
    // real-chunk count to the caller. Saturate-at-zero: if ART matched
    // 0 (no namespace node) we still report 0.
    if (res.matched_chunks == 0) return r;
    r.matched_chunks = static_cast<uint32_t>(res.matched_chunks - 1);
    r.matched_tokens = r.matched_chunks * kChunkTokens;
    r.leaf           = res.leaf;
    return r;
}

}  // namespace kvcache::node::prefix
