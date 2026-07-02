// Task 3 — A9 DR warm-standby: WarmSetFilter unit test.
//
// IsWarm(ev, policy) returns true iff the event is an ADD whose tier is
// in the hot range [Tier::Hbm .. Tier(max_tier)].
// The filter is header-only: no .cpp to compile; the test binary needs
// NODE_PREFIX_SRCS only for the kv_event_stream.h include chain.
#include "replication/warm_filter.h"

#include <gtest/gtest.h>

using kvcache::node::prefix::Event;
using kvcache::node::prefix::EventType;
using kvcache::node::prefix::Tier;
using kvcache::node::replication::IsWarm;
using kvcache::node::replication::WarmPolicy;

TEST(WarmFilter, AdmitsHotTierAddRejectsColdAndNonAdd) {
    WarmPolicy p{.max_tier = 1};  // T0 (Hbm) only

    // ADD in tier Hbm (1) — should be accepted
    Event add_hot{};
    add_hot.type = EventType::Add;
    add_hot.tier = Tier::Hbm;

    // ADD in tier Dram (3) — cold relative to max_tier=1, rejected
    Event add_cold{};
    add_cold.type = EventType::Add;
    add_cold.tier = Tier::Dram;

    // EVICT in tier Hbm (1) — wrong type, rejected
    Event evict{};
    evict.type = EventType::Evict;
    evict.tier = Tier::Hbm;

    EXPECT_TRUE(IsWarm(add_hot, p));
    EXPECT_FALSE(IsWarm(add_cold, p)) << "cold-tier chunk not replicated";
    EXPECT_FALSE(IsWarm(evict, p)) << "only ADD is replicated by the filter";
}

TEST(WarmFilter, MaxTierTwoAcceptsPinnedRejectsDram) {
    WarmPolicy p{.max_tier = 2};  // Hbm(1) + Pinned(2)

    Event add_pinned{};
    add_pinned.type = EventType::Add;
    add_pinned.tier = Tier::Pinned;

    Event add_dram{};
    add_dram.type = EventType::Add;
    add_dram.tier = Tier::Dram;

    EXPECT_TRUE(IsWarm(add_pinned, p));
    EXPECT_FALSE(IsWarm(add_dram, p));
}

TEST(WarmFilter, DefaultPolicyAcceptsHbmAndPinned) {
    WarmPolicy p{};  // default max_tier = 1

    Event add_hbm{};
    add_hbm.type = EventType::Add;
    add_hbm.tier = Tier::Hbm;

    Event add_pinned{};
    add_pinned.type = EventType::Add;
    add_pinned.tier = Tier::Pinned;

    EXPECT_TRUE(IsWarm(add_hbm, p));
    EXPECT_FALSE(IsWarm(add_pinned, p)) << "Pinned(2) exceeds default max_tier=1";
}
