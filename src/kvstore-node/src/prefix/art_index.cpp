// LLD §3.2 — ART implementation (Node256-only, epoch-based lock-free reads).
//
// Phase D-4 — per-slot sibling chaining. Each parent slot at index
// `h[0]` is the head of a linked list of nodes whose `edge_tail` (bytes
// 1..7 of the originating ChunkHash) differ. Walks at each level look
// up the chain entry whose edge_tail matches the incoming hash;
// inserts append to the chain tail when no match is found. This
// eliminates the pre-D-4 `kPathConflict` return path for the
// edge-tail-mismatch case — two ChunkHashes sharing only `h[0]` can
// now coexist instead of one losing.
//
// Threading model:
//   * Insert / Remove take `writer_mu_` (writers serialise among themselves).
//     They mutate the tree via atomic stores and retire replaced nodes
//     through the EpochManager.
//   * Lookup is wait-free: at each descent level we walk the per-slot
//     chain via `chain_next.load(acquire)`. No mutex.
//     The caller's EpochGuard (see ReaderGuard) pins reclamation for the
//     pointers it returns.
#include "prefix/art_index.h"
#include "prefix/art_index_internal.h"

#include <cstring>
#include <utility>

namespace kvcache::node::prefix {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

inline bool EdgeMatches(const ArtNode& child, const ChunkHash& h) {
    if (!child.edge_tail_valid) return false;
    return std::memcmp(child.edge_tail.data(), h.data() + 1, 7) == 0;
}

inline void SetEdge(ArtNode& child, const ChunkHash& h) {
    std::memcpy(child.edge_tail.data(), h.data() + 1, 7);
    child.edge_tail_valid = true;
}

// Phase D-4 — walk the per-slot sibling chain starting at ``head``,
// looking for the entry whose ``edge_tail`` matches ``h[1..7]``.
// Returns the matching node and (via out-param) its predecessor in the
// chain — ``*prev_out`` is nullptr when the head matches; ``*prev_out``
// is the chain tail when no match exists (so callers can append).
// Uses acquire loads so readers see a fully-published chain entry.
inline ArtNode* WalkChain(ArtNode* head, const ChunkHash& h,
                            ArtNode** prev_out) noexcept {
    ArtNode* prev = nullptr;
    ArtNode* cur  = head;
    while (cur) {
        if (EdgeMatches(*cur, h)) {
            if (prev_out) *prev_out = prev;
            return cur;
        }
        prev = cur;
        cur = cur->chain_next.load(std::memory_order_acquire);
    }
    if (prev_out) *prev_out = prev;
    return nullptr;
}

}  // namespace

// ---------------------------------------------------------------------------
// ArtIndex
// ---------------------------------------------------------------------------

ArtIndex::ArtIndex() : root_(std::make_unique<ArtInner256>()) {
    root_->edge_tail_valid = false;  // root has no incoming edge
}

ArtIndex::~ArtIndex() {
    // Force reclaim any retired nodes before walking the tree, since the
    // destructor will delete in-place via root_'s recursive dtor. Doing
    // this in the right order also catches application bugs (readers
    // surviving the index) — ForceReclaimAll runs deleters regardless of
    // active readers, which is the right thing at destruction.
    epochs_.ForceReclaimAll();
    // root_ unique_ptr dtor walks tree and deletes nodes.
}

ArtIndex::ReaderGuard ArtIndex::EnterRead() const {
    return ReaderGuard(epochs_);
}

ArtIndex::InsertResult ArtIndex::Insert(std::span<const ChunkHash> path,
                                        std::unique_ptr<LeafData> leaf) {
    if (path.empty()) return InsertResult::kPathConflict;

    std::lock_guard lk(writer_mu_);

    // Walk inner nodes, creating new ones as needed. With Phase D-4
    // chaining, each slot holds a sibling chain — multiple ChunkHashes
    // sharing `h[0]` but differing on `h[1..7]` coexist.
    ArtInner256* cur = root_.get();
    for (std::size_t i = 0; i + 1 < path.size(); ++i) {
        const ChunkHash& h = path[i];
        const uint8_t slot = h[0];
        ArtNode* head = cur->children[slot].load(std::memory_order_acquire);
        ArtNode* prev = nullptr;
        ArtNode* match = WalkChain(head, h, &prev);

        if (match) {
            if (match->tag == ArtNodeTag::Leaf) {
                // Existing terminal leaf at this exact (slot, edge_tail);
                // we'd need to demote it into an inner. Phase D-5 follow-on.
                return InsertResult::kPathConflict;
            }
            cur = static_cast<ArtInner256*>(match);
            continue;
        }
        // No matching chain entry — append a new Inner at the tail.
        auto* child = new ArtInner256();
        SetEdge(*child, h);
        // Publish with release so readers acquire-loading the link see a
        // fully-constructed node.
        if (prev) {
            prev->chain_next.store(child, std::memory_order_release);
        } else {
            cur->children[slot].store(child, std::memory_order_release);
        }
        node_count_.fetch_add(1, std::memory_order_relaxed);
        cur = child;
    }

    // Terminal hop: attach (or replace) a leaf in the chain at `last[0]`.
    const ChunkHash& last = path.back();
    const uint8_t slot = last[0];
    ArtNode* head = cur->children[slot].load(std::memory_order_acquire);
    ArtNode* prev = nullptr;
    ArtNode* match = WalkChain(head, last, &prev);

    if (match && match->tag == ArtNodeTag::Inner256) {
        // The matching chain entry is an inner — terminal leaf can't go
        // here without demoting it. Phase D-5 follow-on.
        return InsertResult::kPathConflict;
    }

    auto* new_leaf = new ArtLeaf();
    SetEdge(*new_leaf, last);
    new_leaf->data = std::move(leaf);

    if (match) {
        // Replace: splice the new leaf in, preserving the chain
        // continuation past the old entry. Then retire the old leaf
        // with its chain_next nulled so its recursive dtor doesn't
        // walk into the still-live chain.
        ArtNode* old = match;
        ArtNode* tail_link =
            old->chain_next.load(std::memory_order_relaxed);
        new_leaf->chain_next.store(tail_link, std::memory_order_relaxed);
        if (prev) {
            prev->chain_next.store(new_leaf, std::memory_order_release);
        } else {
            cur->children[slot].store(new_leaf, std::memory_order_release);
        }
        old->chain_next.store(nullptr, std::memory_order_relaxed);
        epochs_.Retire([old]() { delete old; });
        return InsertResult::kReplaced;
    }

    // Brand new leaf — append at the chain tail.
    if (prev) {
        prev->chain_next.store(new_leaf, std::memory_order_release);
    } else {
        cur->children[slot].store(new_leaf, std::memory_order_release);
    }
    leaf_count_.fetch_add(1, std::memory_order_relaxed);
    return InsertResult::kInserted;
}

bool ArtIndex::Remove(std::span<const ChunkHash> path) {
    if (path.empty()) return false;
    std::lock_guard lk(writer_mu_);

    ArtInner256* cur = root_.get();
    for (std::size_t i = 0; i + 1 < path.size(); ++i) {
        const ChunkHash& h = path[i];
        const uint8_t slot = h[0];
        ArtNode* head = cur->children[slot].load(std::memory_order_acquire);
        ArtNode* match = WalkChain(head, h, /*prev_out=*/nullptr);
        if (!match || match->tag != ArtNodeTag::Inner256) return false;
        cur = static_cast<ArtInner256*>(match);
    }

    const ChunkHash& last = path.back();
    const uint8_t slot = last[0];
    ArtNode* head = cur->children[slot].load(std::memory_order_acquire);
    ArtNode* prev = nullptr;
    ArtNode* match = WalkChain(head, last, &prev);
    if (!match || match->tag != ArtNodeTag::Leaf) return false;

    // Unlink from the chain (preserve the rest of the siblings).
    ArtNode* next = match->chain_next.load(std::memory_order_relaxed);
    if (prev) {
        prev->chain_next.store(next, std::memory_order_release);
    } else {
        cur->children[slot].store(next, std::memory_order_release);
    }
    // Null out the retired node's chain link so the recursive ~ArtNode
    // doesn't free the still-live sibling that took its place.
    match->chain_next.store(nullptr, std::memory_order_relaxed);
    ArtNode* old = match;
    epochs_.Retire([old]() { delete old; });
    leaf_count_.fetch_sub(1, std::memory_order_relaxed);
    return true;
}

ArtIndex::LookupResult ArtIndex::Lookup(std::span<const ChunkHash> path,
                                        const ReaderGuard& /*guard*/) const {
    // Lock-free walk. The guard parameter exists only to make the lifetime
    // contract explicit at the type level — the actual epoch publish was
    // done by guard's constructor.
    LookupResult best{0, nullptr};
    if (path.empty()) return best;

    const ArtInner256* cur = root_.get();
    for (std::size_t i = 0; i < path.size(); ++i) {
        const ChunkHash& h = path[i];
        const uint8_t slot = h[0];
        ArtNode* head = cur->children[slot].load(std::memory_order_acquire);
        ArtNode* match = WalkChain(head, h, /*prev_out=*/nullptr);
        if (!match) break;

        if (match->tag == ArtNodeTag::Leaf) {
            auto* lf = static_cast<const ArtLeaf*>(match);
            best.matched_chunks = i + 1;
            best.leaf           = lf->data.get();
            break;  // leaves terminal in MVP
        }
        cur = static_cast<const ArtInner256*>(match);
    }
    return best;
}

LeafData* ArtIndex::LookupByPath(std::span<const ChunkHash> path,
                                  const ReaderGuard& /*guard*/) const {
    if (path.empty()) return nullptr;

    const ArtInner256* cur = root_.get();
    for (std::size_t i = 0; i + 1 < path.size(); ++i) {
        const ChunkHash& h = path[i];
        const uint8_t slot = h[0];
        ArtNode* head = cur->children[slot].load(std::memory_order_acquire);
        ArtNode* match = WalkChain(head, h, /*prev_out=*/nullptr);
        if (!match || match->tag != ArtNodeTag::Inner256) return nullptr;
        cur = static_cast<const ArtInner256*>(match);
    }
    const ChunkHash& last = path.back();
    const uint8_t slot = last[0];
    ArtNode* head = cur->children[slot].load(std::memory_order_acquire);
    ArtNode* match = WalkChain(head, last, /*prev_out=*/nullptr);
    if (!match || match->tag != ArtNodeTag::Leaf) return nullptr;
    return static_cast<const ArtLeaf*>(match)->data.get();
}

}  // namespace kvcache::node::prefix
