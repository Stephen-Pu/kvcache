// Phase ABI-1 — build a HeadlessNode::Options from defaults, environment
// overrides, and an optional kv_ctx_tuning_t (in that precedence order).
//
// Factored out of kv_abi.cpp into its own TU so the (pure, singleton-free)
// option-building logic is unit-testable without standing up the process-
// wide HeadlessNode singleton.
#pragma once

#include "headless_node.h"
#include "kvcache/kv_abi.h"

namespace kvcache::abi {

// Returns the headless-backend Options for a kv_ctx_open.
//   1. built-in defaults (demo / unit-test sizing)
//   2. KVCACHE_NIXL_* environment overrides
//   3. fields explicitly set in `tuning` (NULL = skip this layer)
// Later layers win over earlier ones.
HeadlessNode::Options BuildCtxOptions(const kv_ctx_tuning_t* tuning);

}  // namespace kvcache::abi
