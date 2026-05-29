// Internal layout of ART nodes.
//
// Kept in a separate header (instead of in art_index.cpp) so that other
// translation units inside `prefix/` — currently the snapshot serializer in
// `art_snapshot.cpp` — can manipulate node objects directly without
// reflecting them through the public ArtIndex API.
//
// This header is NOT exported via `include/` and is not part of any public
// interface. Anything outside `kvcache::node::prefix` must go through
// art_index.h.
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>

#include "prefix/art_index.h"

namespace kvcache::node::prefix {

enum class ArtNodeTag : uint8_t { Inner256 = 0, Leaf = 1 };

struct ArtNode {
    ArtNodeTag             tag;
    std::array<uint8_t, 7> edge_tail{};
    bool                   edge_tail_valid = false;
    // Phase D-4 — sibling chain at the same parent-slot. Two
    // ChunkHashes with the same first byte but different bytes 1..7
    // share a parent slot but live as separate chain entries
    // distinguished by ``edge_tail``. Atomic so readers can walk the
    // list concurrently with writers; writers serialise through
    // ArtIndex::writer_mu_ and publish appends with release stores.
    // Pre-D-4 the tree could store only one (slot, edge_tail) pair
    // per parent slot — second hashes returned kPathConflict and
    // never landed; with chaining the conflict cap goes away.
    std::atomic<ArtNode*>  chain_next{nullptr};

    explicit ArtNode(ArtNodeTag t) : tag(t) {}
    // Recursive — deletes the rest of the chain when this node is
    // dropped. Safe at structure-destruction time (no readers) and
    // through EBR retires (whole chain link retires together when
    // the chain head is replaced).
    virtual ~ArtNode() {
        delete chain_next.load(std::memory_order_relaxed);
    }
};

struct ArtInner256 : ArtNode {
    // Child pointers are atomic so readers can walk them without locking.
    // Writers own these atoms (under ArtIndex::writer_mu_) and publish via
    // release stores.
    std::array<std::atomic<ArtNode*>, 256> children{};

    // Phase D-5 — "stop-here" data carried on an inner node. Set when
    // a terminal leaf at this exact path needs to coexist with deeper
    // children (e.g. Insert([A]) after Insert([A, B])), or when a
    // deeper Insert demotes a previously terminal leaf at this
    // position (e.g. Insert([A, B]) after Insert([A])). Atomic raw
    // pointer (not unique_ptr) so writers can atomic-swap on Replace
    // and readers see a fully-published store via acquire.
    // Memory: 8 bytes per Inner — cheap.
    std::atomic<LeafData*>  embedded_leaf{nullptr};

    ArtInner256() : ArtNode(ArtNodeTag::Inner256) {
        for (auto& c : children) c.store(nullptr, std::memory_order_relaxed);
    }

    // Recursive delete. Only safe when no readers remain — see ArtIndex
    // destructor and snapshot-load failure paths.
    ~ArtInner256() override {
        for (auto& c : children) {
            ArtNode* p = c.load(std::memory_order_relaxed);
            delete p;
        }
        // D-5: free the embedded leaf data (raw owned pointer).
        delete embedded_leaf.load(std::memory_order_relaxed);
    }
};

struct ArtLeaf : ArtNode {
    std::unique_ptr<LeafData> data;
    ArtLeaf() : ArtNode(ArtNodeTag::Leaf) {}
};

}  // namespace kvcache::node::prefix
