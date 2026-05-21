// LLD §3.2 — Adaptive Radix Tree, in-memory prefix index.
//
// Keys are sequences of ChunkHash values. A ChunkHash is the first 8 bytes of
// BLAKE3-128(chunk_tokens), where each chunk is 16 tokens (LLD §3.2).
//
//   token stream  : t_0  t_1 ... t_{N-1}
//   chunk i       : tokens [i*16, (i+1)*16)
//   ChunkHash_i   : Blake3_128(chunk_i)[0..8]
//   ART key       : (ChunkHash_0, ChunkHash_1, ..., ChunkHash_{K-1})
//
// LPM semantics: Lookup(path) returns the deepest leaf such that the leaf's
// chunk-path is a prefix of `path`.
//
// Tree shape (this reference implementation):
//   * Every inner node has 256 child slots indexed by the FIRST byte of the
//     next ChunkHash on the path. This is the "Node256" specialization.
//   * The remaining 7 bytes of that ChunkHash live on the edge to the child
//     as an "edge label" (path compression).
//   * MVP: Node256-only — uses ~2 KiB per inner node. The Node4 / Node16 /
//     Node48 specializations from Leis et al. 2013 are deferred — see
//     TODO(perf-art-adaptive). Functional correctness is unchanged.
//
// Concurrency model (epoch-based, LLD §9.1 p99 ≤ 10µs):
//   * Readers walk the tree using `std::atomic<ArtNode*>` loads (acquire).
//     They do NOT take any mutex; the only synchronisation is the 2-atom
//     epoch-publish on EnterRead/ExitRead.
//   * Writers (Insert / Remove) serialise via a private mutex. They publish
//     new nodes with atomic stores (release) and push replaced nodes onto
//     the EpochManager's retire list. Readers that started before the swap
//     keep a valid pointer; the retired node is freed once their witness
//     advances past the retire epoch.
//   * No reader-vs-writer contention: writers never block readers.
//
// See prefix/epoch.h for the EBR primitives.
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>

#include "kvcache/kv_types.h"
#include "prefix/epoch.h"
#include "prefix/refcount.h"

namespace kvcache::node::prefix {

// 8-byte chunk hash — see header docblock for derivation.
using ChunkHash = std::array<uint8_t, 8>;

// LeafData is what the prefix engine attaches to each sealed prefix entry.
// Pointers handed out by Lookup are valid for the lifetime of the reader's
// ReaderGuard (see below).
struct LeafData {
    kv_locator_t                locator;
    uint32_t                    tier_residency_bitmap;
    Refcount                    refcount;
    uint64_t                    sealed_at_nanos;
    std::atomic<uint64_t>       last_access_nanos;
    uint64_t                    bytes_total;

    LeafData() noexcept : locator{}, tier_residency_bitmap{0},
                          sealed_at_nanos{0}, last_access_nanos{0},
                          bytes_total{0} {}
};

// Forward declarations of internal node types (defined in art_index.cpp).
struct ArtNode;
struct ArtInner256;
struct ArtLeaf;

class ArtIndex {
   public:
    ArtIndex();
    ~ArtIndex();
    ArtIndex(const ArtIndex&)            = delete;
    ArtIndex& operator=(const ArtIndex&) = delete;

    // ---- writer API (serialised by an internal writer mutex) ----

    enum class InsertResult {
        kInserted,        // new leaf created
        kReplaced,        // existing leaf at this exact path replaced
        kPathConflict,    // edge-label mismatch (should never happen with hashes)
    };

    InsertResult Insert(std::span<const ChunkHash> path,
                        std::unique_ptr<LeafData> leaf);

    // Remove a leaf at exactly `path`. The freed node is retired through the
    // EpochManager — concurrent readers that observed the leaf keep working
    // until they exit their guard.
    bool Remove(std::span<const ChunkHash> path);

    // ---- reader API ----

    // RAII guard wrapping an EpochManager EnterRead/ExitRead pair. Move-
    // constructible so callers can `auto g = art.EnterRead();`.
    class ReaderGuard {
       public:
        ReaderGuard() = default;
        explicit ReaderGuard(EpochManager& m) noexcept : guard_(m) {}

        ReaderGuard(const ReaderGuard&)            = delete;
        ReaderGuard& operator=(const ReaderGuard&) = delete;
        ReaderGuard(ReaderGuard&&) noexcept            = default;
        ReaderGuard& operator=(ReaderGuard&&) noexcept = default;

        bool active() const noexcept { return guard_.active(); }

       private:
        EpochGuard guard_;
    };

    // Acquire a reader guard. Every pointer returned by a subsequent Lookup
    // is valid until this guard is destroyed.
    ReaderGuard EnterRead() const;

    struct LookupResult {
        std::size_t matched_chunks{0};
        LeafData*   leaf{nullptr};
    };

    // Longest-prefix-match. The `guard` parameter pins reclamation while
    // the returned leaf pointer is in use; it is not actually consulted at
    // runtime — it exists only to make the lifetime contract explicit at
    // the type level.
    LookupResult Lookup(std::span<const ChunkHash> path,
                        const ReaderGuard& guard) const;

    // ---- stats ----
    std::size_t LeafCount() const noexcept {
        return leaf_count_.load(std::memory_order_acquire);
    }
    std::size_t NodeCount() const noexcept {
        return node_count_.load(std::memory_order_acquire);
    }

    // ---- maintenance ----
    // Run a pass of epoch-protected reclamation. Returns the number of
    // retired nodes freed. Typically called by a background sweeper or
    // by tests; never required for correctness.
    std::size_t RunReclaim() const { return epochs_.Reclaim(); }

    EpochManager& epoch_manager() const noexcept { return epochs_; }

   private:
    // The snapshot serializer reaches into the tree directly — it walks the
    // raw node graph under writer_mu_ and, on restore, builds a fresh tree
    // by allocating internal node types declared in art_index_internal.h.
    friend class ArtSnapshot;

    mutable std::mutex            writer_mu_;
    mutable EpochManager          epochs_;
    std::unique_ptr<ArtInner256>  root_;
    std::atomic<std::size_t>      leaf_count_{0};
    std::atomic<std::size_t>      node_count_{0};
};

}  // namespace kvcache::node::prefix
