// All methods are inline in the header; this TU exists so the class has a
// translation unit in kvcache_common (matching the value_policy_kv.cpp /
// value_policy_tool_result.cpp pattern) and a home for future non-trivial
// logic (e.g. per-tenant persistence-SLA gating).
#include "value_policy_persistent_wal.h"

namespace kvcache::common {
// (intentionally empty — see header)
}  // namespace kvcache::common
