// LLD §4.2 — Rendezvous (Highest Random Weight) hashing + Overlap Score.
//
// Why HRW instead of a consistent-hash ring?
//   * No "virtual node" tuning knob: every (key, node) pair has a single
//     deterministic weight; the top-K is just "sort and take K".
//   * Minimal disruption on node add/remove: only O(1/N) keys move.
//   * Trivial composition with cluster-state biases — we multiply the raw
//     weight by the node's TrafficWeight (see membership_fsm.h) and by an
//     "Overlap Score" describing how much of the prefix the node already
//     caches (per its bloom sketch).
//
// API:
//   * TopK(key_bytes, K) → vector<NodeId> in weight order.
//   * For inputs that benefit from prefix-locality, callers pass the
//     OverlapScoreFn so the routing decision can favor a node that already
//     holds a partial prefix (LLD §4.2 — "prefix that already has X% of
//     chunks elsewhere should keep landing there").
//
// The Node set is owned externally (typically by the cluster bootstrap layer
// watching etcd's /nodes/ prefix). HrwRing is read-mostly and copy-on-write.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

namespace kvcache::node::routing {

using NodeId = std::string;

struct NodeEntry {
    NodeId   id;
    double   traffic_weight = 1.0;  // from MembershipFsm::TrafficWeight()
};

// Caller-supplied: maps (NodeId, key_bytes) → [0.0, 1.0] overlap fraction.
// 0.0 means "no overlap"; 1.0 means "the node already holds the full
// prefix". The router multiplies the raw HRW weight by (1 + alpha *
// overlap), where alpha is configurable.
using OverlapScoreFn =
    std::function<double(const NodeId&, std::span<const uint8_t>)>;

class HrwRing {
   public:
    struct Options {
        // Weight given to overlap score. 0 disables overlap-biased routing.
        // LLD §4.2 default is small (0.2) so primary placement is still
        // driven by HRW hashing; overlap is a tie-breaker / promoter.
        double overlap_alpha = 0.2;
    };

    HrwRing();
    explicit HrwRing(const Options& opts);

    // Replace the current node set (copy-on-write). Thread-safe.
    void SetNodes(std::vector<NodeEntry> nodes);

    std::size_t NodeCount() const noexcept;

    // Top-K candidates for `key_bytes`, sorted by descending effective weight.
    // `overlap_fn` may be nullptr.
    std::vector<NodeId> TopK(std::span<const uint8_t> key_bytes, std::size_t k,
                              const OverlapScoreFn& overlap_fn = nullptr) const;

    // Convenience: the single primary node.
    NodeId Primary(std::span<const uint8_t> key_bytes,
                   const OverlapScoreFn& overlap_fn = nullptr) const;

   private:
    static uint64_t WeightHash(const NodeId& id, std::span<const uint8_t> key);

    mutable std::mutex                       mu_;
    std::shared_ptr<const std::vector<NodeEntry>> nodes_;
    double                                   overlap_alpha_;
};

}  // namespace kvcache::node::routing
