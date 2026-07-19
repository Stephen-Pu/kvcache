// SS-2 spine spike, Task 1 — StateIdentity (128B) + KV projection.
#include "state_identity.h"

#include <gtest/gtest.h>

#include <cstring>

#include "kvcache/kv_types.h"

using namespace kvcache::common;

TEST(StateIdentity, IsExactly128Bytes) {
    static_assert(sizeof(StateIdentity) == 128, "");
    EXPECT_EQ(sizeof(StateIdentity), 128u);
}

TEST(StateIdentity, LocatorIsStillFrozen64) {
    // Guard: the spike must not have perturbed the frozen wire struct.
    EXPECT_EQ(sizeof(kv_locator_t), 64u);
}

TEST(StateIdentity, KvProjectionCarriesIdentityFields) {
    kv_locator_t loc{};
    std::memset(loc.tenant_id, 0xAB, sizeof(loc.tenant_id));
    loc.model_id_hash = 0x1122334455667788ull;
    std::memset(loc.prefix_hash, 0xCD, sizeof(loc.prefix_hash));
    loc.version = 1;

    StateIdentity id = StateIdentityFromLocator(loc);
    EXPECT_EQ(id.version, 2u);
    EXPECT_EQ(id.state_kind, static_cast<uint16_t>(SK_KV));
    EXPECT_EQ(id.recipe_ref, 0u) << "KV: identity≈lineage, trivial recipe";
    // content_hash starts with model_id_hash (LE 8 bytes) then prefix_hash (16 bytes).
    uint64_t got_model = 0;
    std::memcpy(&got_model, id.content_hash, sizeof(got_model));
    EXPECT_EQ(got_model, loc.model_id_hash);
    EXPECT_EQ(std::memcmp(id.content_hash + 8, loc.prefix_hash, 16), 0);
}
