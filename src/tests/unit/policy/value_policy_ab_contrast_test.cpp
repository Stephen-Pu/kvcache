// SS-2 spine spike, Task 4 — the Q3 validation core.
//
// Q3: one ValuePolicy interface + one ValuePolicyRegistry admits two
// OPPOSITE-semantics policies (KV = A-class, PersistentStub = B-class),
// registered by one line each, with zero spine change. Same StateIdentity
// shape, different state_kind → opposite store/evict/miss decisions —
// proving "one spine, swap plugin, covers A and B".
#include "value_policy.h"
#include "value_policy_kv.h"
#include "value_policy_persistent_stub.h"

#include <gtest/gtest.h>

using namespace kvcache::common;

TEST(ValuePolicyABContrast, OneRegistryCoversAandBWithOppositeSemantics) {
    ValuePolicyRegistry reg;
    reg.registerPolicy(SK_KV,     std::make_unique<ValuePolicyKv>());              // A
    reg.registerPolicy(SK_MEMORY, std::make_unique<ValuePolicyPersistentStub>());  // B

    StateIdentity a{}; a.state_kind = SK_KV;
    StateIdentity b{}; b.state_kind = SK_MEMORY; b.flags = SIF_PERSISTENT_B;

    // shouldStore: A declines when recompute is cheaper; B stores unconditionally.
    EXPECT_FALSE(reg.of(SK_KV).shouldStore(a, CostModel{.fetch_cost_ms = 10, .recompute_cost_ms = 1}));
    EXPECT_TRUE(reg.of(SK_MEMORY).shouldStore(b, CostModel{.fetch_cost_ms = 10, .recompute_cost_ms = 1}))
        << "B is irreplaceable — stored regardless of economics";

    // shouldEvict: A is cost-evictable; B is NOT_EVICTABLE (demote-only).
    EXPECT_EQ(reg.of(SK_KV).shouldEvict(a, 2), EvictDecision::kEvictable);
    EXPECT_EQ(reg.of(SK_MEMORY).shouldEvict(b, 2), EvictDecision::kNotEvictable);

    // onMiss: A recomputes; B replays from persistence.
    EXPECT_EQ(reg.of(SK_KV).onMiss(a), OnMissAction::kRecompute);
    EXPECT_EQ(reg.of(SK_MEMORY).onMiss(b), OnMissAction::kReplayFromPersist);
}
