// Task 2 — the production B policy + key projection.
#include "value_policy_persistent_wal.h"

#include <gtest/gtest.h>
#include <array>

#include "state_identity.h"
#include "value_policy.h"
#include "value_policy_kv.h"

using kvcache::common::CostModel;
using kvcache::common::EvictDecision;
using kvcache::common::OnMissAction;
using kvcache::common::SK_MEMORY;
using kvcache::common::StateIdentity;
using kvcache::common::StateKeyBytesFromIdentity;
using kvcache::common::ValuePolicyKv;
using kvcache::common::ValuePolicyPersistentWal;

TEST(StateKeyBytes, TakesFirst16OfContentHash) {
    StateIdentity id{};
    for (int i = 0; i < 32; ++i) id.content_hash[i] = static_cast<uint8_t>(i + 1);
    auto k = StateKeyBytesFromIdentity(id);
    ASSERT_EQ(k.size(), 16u);
    for (int i = 0; i < 16; ++i) EXPECT_EQ(k[i], static_cast<uint8_t>(i + 1));
}

TEST(ValuePolicyPersistentWal, IsEvictableAndReplaysFromPersist) {
    ValuePolicyPersistentWal p;
    StateIdentity id{};
    id.state_kind = SK_MEMORY;
    CostModel cost{};
    EXPECT_TRUE(p.shouldStore(id, cost));                                  // irreplaceable → always store
    EXPECT_EQ(p.shouldEvict(id, /*tier=*/0), EvictDecision::kEvictable);   // persist-first → demotable
    EXPECT_EQ(p.onMiss(id), OnMissAction::kReplayFromPersist);

    // Contrast with KV: opposite miss action, evictable both but for different reasons.
    ValuePolicyKv kv;
    EXPECT_EQ(kv.onMiss(id), OnMissAction::kRecompute);
}
