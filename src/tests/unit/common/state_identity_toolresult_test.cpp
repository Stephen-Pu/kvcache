// SS-2 spine spike, A-plane generalization Task 1 — StateIdentityForToolResult
// builder: second real A-class state_kind (SK_TOOL_RESULT) on the SS-2 spine.
#include "state_identity.h"
#include <gtest/gtest.h>
#include <cstring>
using namespace kvcache::common;

TEST(StateIdentityToolResult, KindFlagsAndTenant) {
    StateIdentity id = StateIdentityForToolResult(7, "search", "q=cats", /*idempotent=*/true);
    EXPECT_EQ(id.version, 2u);
    EXPECT_EQ(id.state_kind, static_cast<uint16_t>(SK_TOOL_RESULT));
    EXPECT_EQ(id.tenant_id_lo, 7u);
    EXPECT_TRUE(id.flags & SIF_IDEMPOTENT);
    EXPECT_EQ(id.recipe_ref, 0u);
}

TEST(StateIdentityToolResult, NonIdempotentClearsFlag) {
    StateIdentity id = StateIdentityForToolResult(1, "send_email", "to=x", /*idempotent=*/false);
    EXPECT_FALSE(id.flags & SIF_IDEMPOTENT);
}

TEST(StateIdentityToolResult, ContentHashDedupsSameCallDistinguishesArgs) {
    auto a = StateIdentityForToolResult(1, "search", "q=cats", true);
    auto b = StateIdentityForToolResult(1, "search", "q=cats", true);   // same call
    auto c = StateIdentityForToolResult(1, "search", "q=dogs", true);   // different args
    EXPECT_EQ(std::memcmp(a.content_hash, b.content_hash, 32), 0) << "same (tool,args) → same hash (dedup)";
    EXPECT_NE(std::memcmp(a.content_hash, c.content_hash, 32), 0) << "different args → different hash";
}
