// LLD §3.2 — Longest-prefix-match over 16-token chunks.
//
// This module bridges the engine-facing API (lookup over a raw token vector)
// and the ART (which is keyed on ChunkHash sequences).
//
//   tokens  (size N)
//     │
//     ▼ Chunkify  (16 tokens per chunk; tail < 16 tokens is dropped — only
//                  whole sealed chunks participate in LPM)
//   chunks (size ⌊N/16⌋)
//     │
//     ▼ Hash each chunk: ChunkHash = Blake3_128(chunk)[0..8]
//   chunk_hashes (size ⌊N/16⌋)
//     │
//     ▼ ArtIndex::Lookup
//   { matched_chunks, leaf* }
//
// Caller converts matched_chunks → matched_tokens = matched_chunks * 16.
#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "prefix/art_index.h"

namespace kvcache::node::prefix {

inline constexpr uint32_t kChunkTokens = 16;  // LLD §3.2

// Split a token sequence into 16-token chunks and hash each. Any tail < 16 is
// discarded — partial chunks are never sealed and therefore never indexed.
std::vector<ChunkHash> Chunkify(std::span<const uint32_t> tokens);

// Phase Q-5 — per-(tenant, model) namespace fingerprint.
//
// All ART paths are prepended with this fingerprint so different
// (tenant, model) pairs occupy disjoint subtrees of the same ART. Two
// requests with the same tokens but different (tenant, model) hash to
// different chunk paths -> isolated lookups. The fingerprint is
// BLAKE3-128 over (tenant_id_hash || model_id_hash), first 8 bytes.
//
// Callers MUST keep the (tenant_hash, model_hash) inputs stable
// between Seal-time and Lookup-time, or the lookup misses. The C ABI
// drives both from the same `kv_ctx_t` fields, so engine adapters
// don't need to recompute anything.
ChunkHash NamespaceFingerprint(uint64_t tenant_hash, uint64_t model_hash);

// Variant of Chunkify that prepends `ns` as the first chunk.
std::vector<ChunkHash> ChunkifyNS(std::span<const uint32_t> tokens,
                                    ChunkHash ns);

struct LpmOutcome {
    uint32_t matched_tokens = 0;   // = matched_chunks * 16
    uint32_t matched_chunks = 0;
    LeafData* leaf          = nullptr;  // valid while `guard` is held
};

// Convenience wrapper: builds the ChunkHash vector and runs ArtIndex::Lookup.
// The caller is responsible for holding `guard` for the entire time it
// dereferences `leaf`.
LpmOutcome LongestPrefixMatch(const ArtIndex& art,
                               std::span<const uint32_t> tokens,
                               const ArtIndex::ReaderGuard& guard);

// Phase Q-5 — namespace-scoped LPM. Prepends NamespaceFingerprint(
// tenant_hash, model_hash) before the token chunks so the walk is
// isolated to the (tenant, model) subtree. `matched_tokens` excludes
// the namespace chunk (it never represents real tokens). On a hit
// matched_chunks is at least 1 (the namespace plus ≥1 real chunk).
LpmOutcome LongestPrefixMatchNS(const ArtIndex& art,
                                  std::span<const uint32_t> tokens,
                                  uint64_t tenant_hash,
                                  uint64_t model_hash,
                                  const ArtIndex::ReaderGuard& guard);

}  // namespace kvcache::node::prefix
