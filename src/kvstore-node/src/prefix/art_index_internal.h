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

    explicit ArtNode(ArtNodeTag t) : tag(t) {}
    virtual ~ArtNode() = default;
};

struct ArtInner256 : ArtNode {
    // Child pointers are atomic so readers can walk them without locking.
    // Writers own these atoms (under ArtIndex::writer_mu_) and publish via
    // release stores.
    std::array<std::atomic<ArtNode*>, 256> children{};

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
    }
};

struct ArtLeaf : ArtNode {
    std::unique_ptr<LeafData> data;
    ArtLeaf() : ArtNode(ArtNodeTag::Leaf) {}
};

}  // namespace kvcache::node::prefix
