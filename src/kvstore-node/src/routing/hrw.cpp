// LLD §4.2 — HRW ring.
#include "routing/hrw.h"

#include <algorithm>
#include <cstring>

#include "blake3.h"  // kvcache::hash::Blake3_64 facade

namespace kvcache::node::routing {

HrwRing::HrwRing() : HrwRing(Options{}) {}

HrwRing::HrwRing(const Options& opts)
    : nodes_(std::make_shared<std::vector<NodeEntry>>()),
      overlap_alpha_(opts.overlap_alpha) {}

void HrwRing::SetNodes(std::vector<NodeEntry> nodes) {
    auto new_nodes = std::make_shared<std::vector<NodeEntry>>(std::move(nodes));
    std::lock_guard lk(mu_);
    nodes_ = std::move(new_nodes);
}

std::size_t HrwRing::NodeCount() const noexcept {
    std::lock_guard lk(mu_);
    return nodes_->size();
}

uint64_t HrwRing::WeightHash(const NodeId& id, std::span<const uint8_t> key) {
    // Concatenate node_id || key and hash. The strong-mix property of
    // BLAKE3 (FNV-1a placeholder) gives us independent uniform random
    // weights per (node, key) pair, which is the whole point of HRW.
    std::vector<uint8_t> buf;
    buf.reserve(id.size() + key.size());
    buf.insert(buf.end(), id.begin(), id.end());
    buf.insert(buf.end(), key.begin(), key.end());
    return kvcache::hash::Blake3_64({buf.data(), buf.size()});
}

std::vector<NodeId> HrwRing::TopK(std::span<const uint8_t> key,
                                    std::size_t k,
                                    const OverlapScoreFn& overlap_fn) const {
    std::shared_ptr<const std::vector<NodeEntry>> nodes;
    double alpha;
    {
        std::lock_guard lk(mu_);
        nodes = nodes_;
        alpha = overlap_alpha_;
    }
    if (nodes->empty() || k == 0) return {};

    struct Score { NodeId id; double effective; };
    std::vector<Score> scored;
    scored.reserve(nodes->size());

    for (const auto& n : *nodes) {
        // Convert hash to a [0, 1) double for stable composition with
        // traffic weight and overlap. Using top 53 bits gives full
        // double-precision uniformity.
        const uint64_t h = WeightHash(n.id, key);
        const double raw = static_cast<double>(h >> 11) / static_cast<double>(1ULL << 53);
        double eff = raw * n.traffic_weight;
        if (overlap_fn) {
            const double ov = overlap_fn(n.id, key);
            eff *= (1.0 + alpha * ov);
        }
        scored.push_back({n.id, eff});
    }

    if (scored.size() > k) {
        std::partial_sort(scored.begin(), scored.begin() + k, scored.end(),
                          [](const Score& a, const Score& b) {
                              return a.effective > b.effective;
                          });
        scored.resize(k);
    } else {
        std::sort(scored.begin(), scored.end(),
                  [](const Score& a, const Score& b) {
                      return a.effective > b.effective;
                  });
    }
    std::vector<NodeId> out;
    out.reserve(scored.size());
    for (auto& s : scored) out.push_back(std::move(s.id));
    return out;
}

NodeId HrwRing::Primary(std::span<const uint8_t> key,
                         const OverlapScoreFn& fn) const {
    auto v = TopK(key, 1, fn);
    return v.empty() ? NodeId{} : v.front();
}

}  // namespace kvcache::node::routing
