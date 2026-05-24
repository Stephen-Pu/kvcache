// LLD §3.2 — ART implementation (Node256-only, epoch-based lock-free reads).
//
// Threading model:
//   * Insert / Remove take `writer_mu_` (writers serialise among themselves).
//     They mutate the tree via atomic stores and retire replaced nodes
//     through the EpochManager.
//   * Lookup is wait-free: a single atomic load per descent step, no mutex.
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

    // Walk inner nodes, creating new ones as needed.
    ArtInner256* cur = root_.get();
    for (std::size_t i = 0; i + 1 < path.size(); ++i) {
        const ChunkHash& h = path[i];
        const uint8_t slot = h[0];
        ArtNode* existing =
            cur->children[slot].load(std::memory_order_acquire);

        if (existing) {
            if (!EdgeMatches(*existing, h)) {
                return InsertResult::kPathConflict;
            }
            if (existing->tag == ArtNodeTag::Leaf) {
                // MVP: leaves are terminal. Splitting would require
                // re-attaching the leaf under a new inner node.
                return InsertResult::kPathConflict;
            }
            cur = static_cast<ArtInner256*>(existing);
        } else {
            auto* child = new ArtInner256();
            SetEdge(*child, h);
            // Publish with release so readers acquire-loading this slot
            // see a fully-constructed node.
            cur->children[slot].store(child, std::memory_order_release);
            node_count_.fetch_add(1, std::memory_order_relaxed);
            cur = child;
        }
    }

    // Terminal hop: attach (or replace) a leaf.
    const ChunkHash& last = path.back();
    const uint8_t slot = last[0];
    ArtNode* existing = cur->children[slot].load(std::memory_order_acquire);

    if (existing && !EdgeMatches(*existing, last)) {
        return InsertResult::kPathConflict;
    }
    if (existing && existing->tag == ArtNodeTag::Inner256) {
        return InsertResult::kPathConflict;
    }

    auto* new_leaf = new ArtLeaf();
    SetEdge(*new_leaf, last);
    new_leaf->data = std::move(leaf);

    if (existing && existing->tag == ArtNodeTag::Leaf) {
        // Replace: atomically swap, retire the old.
        cur->children[slot].store(new_leaf, std::memory_order_release);
        ArtNode* old = existing;
        epochs_.Retire([old]() { delete old; });
        return InsertResult::kReplaced;
    }

    // Brand new leaf.
    cur->children[slot].store(new_leaf, std::memory_order_release);
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
        ArtNode* child = cur->children[slot].load(std::memory_order_acquire);
        if (!child || !EdgeMatches(*child, h) ||
            child->tag != ArtNodeTag::Inner256) {
            return false;
        }
        cur = static_cast<ArtInner256*>(child);
    }

    const ChunkHash& last = path.back();
    const uint8_t slot = last[0];
    ArtNode* tail = cur->children[slot].load(std::memory_order_acquire);
    if (!tail || !EdgeMatches(*tail, last) || tail->tag != ArtNodeTag::Leaf) {
        return false;
    }
    cur->children[slot].store(nullptr, std::memory_order_release);
    ArtNode* old = tail;
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
        ArtNode* child = cur->children[slot].load(std::memory_order_acquire);
        if (!child || !EdgeMatches(*child, h)) break;

        if (child->tag == ArtNodeTag::Leaf) {
            auto* lf = static_cast<const ArtLeaf*>(child);
            best.matched_chunks = i + 1;
            best.leaf           = lf->data.get();
            break;  // leaves terminal in MVP
        }
        cur = static_cast<const ArtInner256*>(child);
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
        ArtNode* child = cur->children[slot].load(std::memory_order_acquire);
        if (!child || !EdgeMatches(*child, h) ||
            child->tag != ArtNodeTag::Inner256) {
            return nullptr;
        }
        cur = static_cast<const ArtInner256*>(child);
    }
    const ChunkHash& last = path.back();
    const uint8_t slot = last[0];
    ArtNode* tail = cur->children[slot].load(std::memory_order_acquire);
    if (!tail || !EdgeMatches(*tail, last) || tail->tag != ArtNodeTag::Leaf) {
        return nullptr;
    }
    return static_cast<const ArtLeaf*>(tail)->data.get();
}

}  // namespace kvcache::node::prefix
