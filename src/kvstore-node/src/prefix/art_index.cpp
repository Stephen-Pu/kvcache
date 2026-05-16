// LLD §3.2 — ART reference implementation (Node256-only, shared_mutex).
//
// See art_index.h for the design rationale and the two TODO replacements
// (Node4/16/48 adaptive specialization and epoch-based reclamation).
#include "prefix/art_index.h"

#include <cstring>
#include <mutex>  // std::unique_lock — not transitively included by <shared_mutex>

namespace kvcache::node::prefix {

// ---------------------------------------------------------------------------
// Internal node types
// ---------------------------------------------------------------------------

enum class ArtNodeTag : uint8_t { Inner256 = 0, Leaf = 1 };

struct ArtNode {
    ArtNodeTag tag;

    // Edge label = the 7 trailing bytes of the ChunkHash that selected this
    // node from its parent. (The first byte is the slot index in the parent.)
    std::array<uint8_t, 7> edge_tail{};
    bool                   edge_tail_valid = false;

    explicit ArtNode(ArtNodeTag t) : tag(t) {}
    virtual ~ArtNode() = default;
};

struct ArtInner256 : ArtNode {
    std::array<std::unique_ptr<ArtNode>, 256> children{};
    ArtInner256() : ArtNode(ArtNodeTag::Inner256) {}
};

struct ArtLeaf : ArtNode {
    std::unique_ptr<LeafData> data;
    ArtLeaf() : ArtNode(ArtNodeTag::Leaf) {}
};

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

// Sentinel pointer used by FindOrSlot to signal a first-byte collision with
// non-matching edge tail. With 8-byte BLAKE3 fragments such a collision has
// probability ~2^-56 per pair — effectively impossible — but we surface it
// rather than silently overwrite.
ArtNode* const kCollisionSentinel = reinterpret_cast<ArtNode*>(uintptr_t{1});

ArtNode* FindOrSlot(ArtInner256& parent, const ChunkHash& h, uint8_t* slot_out) {
    const uint8_t slot = h[0];
    *slot_out = slot;
    auto& child = parent.children[slot];
    if (!child) return nullptr;
    if (!EdgeMatches(*child, h)) return kCollisionSentinel;
    return child.get();
}

}  // namespace

// ---------------------------------------------------------------------------
// ArtIndex
// ---------------------------------------------------------------------------

ArtIndex::ArtIndex() : root_(std::make_unique<ArtInner256>()) {
    root_->edge_tail_valid = false;  // root has no incoming edge
}

ArtIndex::~ArtIndex() = default;

ArtIndex::ReaderGuard ArtIndex::EnterRead() const { return ReaderGuard(mu_); }

std::size_t ArtIndex::LeafCount() const noexcept {
    std::shared_lock lk(mu_);
    return leaf_count_;
}
std::size_t ArtIndex::NodeCount() const noexcept {
    std::shared_lock lk(mu_);
    return node_count_;
}

ArtIndex::InsertResult ArtIndex::Insert(std::span<const ChunkHash> path,
                                        std::unique_ptr<LeafData> leaf) {
    if (path.empty()) return InsertResult::kPathConflict;

    std::unique_lock lk(mu_);

    ArtInner256* cur = root_.get();
    for (std::size_t i = 0; i + 1 < path.size(); ++i) {
        const ChunkHash& h = path[i];
        uint8_t slot;
        ArtNode* existing = FindOrSlot(*cur, h, &slot);
        if (existing == kCollisionSentinel) return InsertResult::kPathConflict;

        if (!existing) {
            auto child = std::make_unique<ArtInner256>();
            SetEdge(*child, h);
            cur->children[slot] = std::move(child);
            ++node_count_;
            existing = cur->children[slot].get();
        } else if (existing->tag == ArtNodeTag::Leaf) {
            // Splitting an existing leaf into an inner-with-leaf is not
            // supported in this MVP — leaves are terminal. TODO(stephen):
            // implement split if streaming-write later seals partial chunks.
            return InsertResult::kPathConflict;
        }
        cur = static_cast<ArtInner256*>(existing);
    }

    // Final hop: attach a leaf.
    const ChunkHash& last = path.back();
    uint8_t slot;
    ArtNode* existing = FindOrSlot(*cur, last, &slot);
    if (existing == kCollisionSentinel) return InsertResult::kPathConflict;

    if (existing && existing->tag == ArtNodeTag::Leaf) {
        static_cast<ArtLeaf*>(existing)->data = std::move(leaf);
        return InsertResult::kReplaced;
    }
    if (existing && existing->tag == ArtNodeTag::Inner256) {
        // An inner node sits where we wanted to attach a leaf — would need
        // "leaf hanging off inner" semantics; not supported in MVP.
        return InsertResult::kPathConflict;
    }
    auto leaf_node = std::make_unique<ArtLeaf>();
    SetEdge(*leaf_node, last);
    leaf_node->data = std::move(leaf);
    cur->children[slot] = std::move(leaf_node);
    ++leaf_count_;
    return InsertResult::kInserted;
}

bool ArtIndex::Remove(std::span<const ChunkHash> path) {
    if (path.empty()) return false;
    std::unique_lock lk(mu_);

    // Walk inner nodes; remember the parent of the final leaf so we can detach
    // it. We do NOT prune empty inner chains in this MVP — periodic compaction
    // is a separate concern (TODO(stephen): pruner).
    ArtInner256* cur = root_.get();
    for (std::size_t i = 0; i + 1 < path.size(); ++i) {
        const ChunkHash& h = path[i];
        const uint8_t slot = h[0];
        auto& child = cur->children[slot];
        if (!child || !EdgeMatches(*child, h) ||
            child->tag != ArtNodeTag::Inner256) {
            return false;
        }
        cur = static_cast<ArtInner256*>(child.get());
    }
    const ChunkHash& last = path.back();
    const uint8_t slot = last[0];
    auto& tail = cur->children[slot];
    if (!tail || !EdgeMatches(*tail, last) || tail->tag != ArtNodeTag::Leaf) {
        return false;
    }
    tail.reset();
    --leaf_count_;
    return true;
}

ArtIndex::LookupResult ArtIndex::Lookup(std::span<const ChunkHash> path,
                                        const ReaderGuard&) const {
    LookupResult best{0, nullptr};
    if (path.empty()) return best;

    const ArtInner256* cur = root_.get();
    for (std::size_t i = 0; i < path.size(); ++i) {
        const ChunkHash& h = path[i];
        const uint8_t slot = h[0];
        const auto& child = cur->children[slot];
        if (!child || !EdgeMatches(*child, h)) break;

        if (child->tag == ArtNodeTag::Leaf) {
            auto* lf = static_cast<ArtLeaf*>(child.get());
            best.matched_chunks = i + 1;
            best.leaf           = lf->data.get();
            break;  // leaves are terminal in this MVP
        }
        cur = static_cast<const ArtInner256*>(child.get());
    }
    return best;
}

}  // namespace kvcache::node::prefix
