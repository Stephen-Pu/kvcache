// ValuePolicyPersistentStub — B-class value policy STUB (SS-2 spine spike,
// Task 4, the Q3 validation core).
//
// This is a STUB, not a production policy: the ⑬ persistence engine that
// would actually back "replay from persist" is not built yet. Its only
// purpose is to prove that the single ValuePolicy interface + registry
// (Task 2) admits a state kind whose economics are the OPPOSITE of KV's
// (Task 3, ValuePolicyKv) — unconditional store, non-evictable, replay-on-miss
// instead of cost-gated store, cost-evictable, recompute-on-miss — with a
// one-line registerPolicy() call and zero change to the tiering
// engine/evictor/fetch-decider spine.
//
// Semantics (B-class, e.g. SK_MEMORY / SIF_PERSISTENT_B):
//   - shouldStore: always true. The state is irreplaceable (no cheap
//     recompute path), so it is stored unconditionally regardless of the
//     CostModel — unlike KV, which declines to store when recompute is
//     cheaper than fetch.
//   - shouldEvict: always kNotEvictable. A tier may demote this state to a
//     colder tier but must never discard it outright.
//   - onMiss: always kReplayFromPersist. This is a placeholder: real replay
//     requires the (not-yet-built) ⑬ persistence engine. Today this just
//     signals "go replay" to the fetch-decider; there is no backing store.
#pragma once

#include "value_policy.h"

namespace kvcache::common {

// STUB — not production. See file header for scope/limitations.
class ValuePolicyPersistentStub final : public ValuePolicy {
 public:
    bool shouldStore(const StateIdentity& /*id*/, const CostModel& /*cost*/) override {
        return true;  // irreplaceable — stored unconditionally, ignores economics
    }

    EvictDecision shouldEvict(const StateIdentity& /*id*/, int /*tier*/) override {
        return EvictDecision::kNotEvictable;  // demote-only, never discard
    }

    OnMissAction onMiss(const StateIdentity& /*id*/) override {
        return OnMissAction::kReplayFromPersist;  // stub: no ⑬ backing yet
    }
};

}  // namespace kvcache::common
