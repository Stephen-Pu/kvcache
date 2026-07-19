// StateIdentity — 128-byte, C++-core-only superset of the frozen 64-byte
// kv_locator_t (SS-2 spine spike, Task 1).
//
// This header is intentionally NOT part of kvcache/kv_types.h (the C ABI /
// wire-format header). StateIdentity is a core-internal abstraction used to
// generalize the KV-specific locator toward other state kinds (sandbox,
// embedding, memory, exec-state, ...) without perturbing the frozen 64B
// wire struct or the FFI boundary.
#pragma once

#include <cstdint>
#include <cstring>

#include "kvcache/kv_types.h"  // kv_locator_t (frozen 64B, C ABI)

namespace kvcache::common {

// State kind discriminator (LLD spike, Q3). Group A = today's KV-adjacent
// kinds; group B = tomorrow's broader state kinds (memory, exec state).
enum state_kind_e : uint16_t {
    SK_KV          = 0,
    SK_SANDBOX     = 1,
    SK_EMBEDDING   = 2,
    SK_TOOL_RESULT = 3,
    SK_MEMORY      = 16,
    SK_EXEC_STATE  = 17,
    SK_MAX         = 18,
};

// StateIdentity-level flag bits (distinct from kv_locator_t::flags).
enum si_flags_e : uint16_t {
    SIF_IDEMPOTENT   = 1u << 0,
    SIF_PERSISTENT_B = 1u << 1,
    SIF_HAS_RECIPE   = 1u << 2,
};

// 128 bytes, C++-core-only superset of kv_locator_t (LLD §2.1b).
struct StateIdentity {
    uint32_t version;            // = 2
    uint16_t state_kind;         // state_kind_e
    uint16_t flags;              // si_flags_e bits
    uint64_t tenant_id_lo;       // low 8 bytes of tenant (KV: first 8 of tenant_id[16])
    uint8_t  content_hash[32];   // KV: model_id_hash(8) || prefix_hash(16) || pad
    uint64_t recipe_ref;         // 0 = trivial (identity ≈ lineage)
    uint8_t  shape[40];          // KV: layer/head/token range bytes
    uint8_t  reserved[32];
};

static_assert(sizeof(StateIdentity) == 128,
              "StateIdentity must be 128 bytes (LLD §2.1b)");

// KV projection: build a StateIdentity from the frozen 64B locator.
// state_kind = SK_KV, recipe_ref = 0 (identity ≈ lineage for KV chunks).
// Does NOT modify the locator.
inline StateIdentity StateIdentityFromLocator(const kv_locator_t& loc) {
    StateIdentity id{};
    id.version    = 2;
    id.state_kind = SK_KV;
    id.flags      = 0;

    // Low 8 bytes of the 16B tenant UUID.
    std::memcpy(&id.tenant_id_lo, loc.tenant_id, sizeof(id.tenant_id_lo));

    // content_hash = model_id_hash (8B) || prefix_hash (16B) || zero pad (8B).
    std::memcpy(id.content_hash, &loc.model_id_hash, sizeof(loc.model_id_hash));
    std::memcpy(id.content_hash + sizeof(loc.model_id_hash), loc.prefix_hash,
                sizeof(loc.prefix_hash));

    id.recipe_ref = 0;

    // shape = the range's raw bytes (16B for kv_range_t today), zero-padded.
    // Guard against a future kv_range_t growing past the 40B shape field.
    constexpr size_t kCopyLen =
        sizeof(loc.range) <= sizeof(id.shape) ? sizeof(loc.range) : sizeof(id.shape);
    std::memcpy(id.shape, &loc.range, kCopyLen);

    return id;
}

}  // namespace kvcache::common
