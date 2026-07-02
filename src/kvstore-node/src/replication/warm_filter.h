// Task 3 — A9 DR warm-standby: WarmSetFilter.
//
// Header-only predicate: decides which replication ADD events are worth
// mirroring to a standby node. Only ADD events whose chunk lives in a hot
// tier (tier index <= WarmPolicy::max_tier) are replicated.
//
// Tier ordering (kv_event_stream.h Tier enum):
//   Hbm(1) < Pinned(2) < Dram(3) < Nvme(4) < Cold(5)
// A tier is "hot" when its numeric value is in [Hbm .. max_tier].
#pragma once

#include "prefix/kv_event_stream.h"

namespace kvcache::node::replication {

// Policy knob: replicate ADD events from tiers with index <= max_tier.
// Default (max_tier = 1) = HBM-only replication.
struct WarmPolicy {
    int max_tier = 1;
};

// Returns true iff ev is an ADD event in a hot tier (Hbm..max_tier).
inline bool IsWarm(const prefix::Event& ev, const WarmPolicy& p) {
    if (ev.type != prefix::EventType::Add) {
        return false;
    }
    int tier_idx = static_cast<int>(ev.tier);
    return tier_idx >= static_cast<int>(prefix::Tier::Hbm) &&
           tier_idx <= p.max_tier;
}

}  // namespace kvcache::node::replication
