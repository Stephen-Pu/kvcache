// ValuePolicyPersistentWal — production B-class value policy (SK_MEMORY).
//
// Replaces ValuePolicyPersistentStub on a real node once the B-state WAL
// exists. Because StatePut writes the WAL (fsync) BEFORE staging to DRAM,
// every B entry resident in DRAM already has a durable copy, so its DRAM copy
// is safely reclaimable — eviction here is DEMOTION, not discard. A DRAM miss
// is served by replaying from the WAL.
//
//   shouldStore : true        — irreplaceable, always stored (ignores CostModel)
//   shouldEvict : kEvictable  — persist-first ⇒ dropping the DRAM copy is safe
//   onMiss      : kReplayFromPersist — replay the durable copy from the WAL
//
// (ValuePolicyPersistentStub, kNotEvictable, is retained as the pinned-entry
// exemplar + DramTier eviction-skip test fixture.)
#pragma once

#include "value_policy.h"

namespace kvcache::common {

class ValuePolicyPersistentWal final : public ValuePolicy {
 public:
    bool shouldStore(const StateIdentity& /*id*/, const CostModel& /*cost*/) override {
        return true;
    }
    EvictDecision shouldEvict(const StateIdentity& /*id*/, int /*tier*/) override {
        return EvictDecision::kEvictable;
    }
    OnMissAction onMiss(const StateIdentity& /*id*/) override {
        return OnMissAction::kReplayFromPersist;
    }
};

}  // namespace kvcache::common
