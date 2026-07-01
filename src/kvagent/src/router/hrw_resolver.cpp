// LLD §4.2 — HrwResolver implementation.
#include "router/hrw_resolver.h"

#include <cstring>

namespace kvcache::agent::router {

void HrwResolver::SetNodes(const std::vector<std::string>& node_ids) {
    std::vector<kvcache::node::routing::NodeEntry> entries;
    entries.reserve(node_ids.size());
    for (const auto& id : node_ids) {
        entries.push_back({id, /*traffic_weight=*/1.0});
    }
    ring_.SetNodes(std::move(entries));
}

void HrwResolver::SetNodes(
    const std::vector<std::pair<std::string, double>>& weighted) {
    std::vector<kvcache::node::routing::NodeEntry> entries;
    entries.reserve(weighted.size());
    for (const auto& [id, w] : weighted) {
        entries.push_back({id, w});
    }
    ring_.SetNodes(std::move(entries));
}

std::optional<ResolveResult> HrwResolver::Resolve(
    const std::array<uint8_t, 16>& tenant_id,
    const std::array<uint8_t, 16>& prefix_hash) const {
    if (ring_.NodeCount() == 0) return std::nullopt;

    // HRW key = tenant_id || prefix_hash (32 bytes). Same composition the
    // kvstore-node side hashes so both agree on the primary without
    // coordination.
    std::array<uint8_t, 32> key{};
    std::memcpy(key.data(),      tenant_id.data(),   16);
    std::memcpy(key.data() + 16, prefix_hash.data(), 16);

    kvcache::node::routing::NodeId primary =
        ring_.Primary({key.data(), key.size()}, /*overlap_fn=*/nullptr);
    if (primary.empty()) return std::nullopt;

    ResolveResult rr;
    rr.node_id = std::move(primary);
    rr.matched_tokens = 0;  // only the node knows the real LPM length
    return rr;
}

NodeResolver HrwResolver::AsCallback() {
    // Capture `this` — the HrwResolver must outlive the router. main.cpp
    // owns both for the process lifetime, and tests keep both on the
    // stack for the test body.
    return [this](const std::array<uint8_t, 16>& tenant_id,
                  const std::array<uint8_t, 16>& prefix_hash)
               -> std::optional<ResolveResult> {
        return Resolve(tenant_id, prefix_hash);
    };
}

}  // namespace kvcache::agent::router
