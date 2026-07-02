// Task 3 — A9 DR warm-standby: ReplicaSource abstraction.
//
// Decouples the chunk-fetch step inside ReplicationConsumer from the concrete
// in-process HeadlessNode::ReplicaFetch call.  A remote gRPC implementation
// will implement this interface in a later task without touching the consumer.
//
// Two concrete classes are provided here (header-only):
//   InProcessReplicaSource — wraps HeadlessNode& (today's behavior, zero overhead).
//   (Remote variant will be in a separate translation unit when added.)
//
// Namespace: kvcache::node::replication
#pragma once

#include "headless_node.h"      // kvcache::abi::HeadlessNode, ReplicaChunk
#include "kvcache/kv_types.h"   // kv_locator_t

namespace kvcache::node::replication {

// ---------------------------------------------------------------------------
// ReplicaSource — abstract fetch interface.
//
// Fetch() obtains the ReplicaChunk for `locator` and writes it to `out`.
// Returns KV_OK on success, KV_E_NOT_FOUND if the chunk is not available
// (e.g. evicted from the primary since the ADD event was emitted).
// ---------------------------------------------------------------------------
class ReplicaSource {
   public:
    virtual ~ReplicaSource() = default;

    // Fetch the chunk identified by `locator` and populate `out`.
    // Returns KV_OK on success, KV_E_NOT_FOUND if unavailable.
    virtual int Fetch(const kv_locator_t& locator,
                      abi::HeadlessNode::ReplicaChunk* out) = 0;
};

// ---------------------------------------------------------------------------
// InProcessReplicaSource — default in-process implementation.
//
// Delegates directly to HeadlessNode::ReplicaFetch; zero overhead over the
// previous direct call.  The referenced HeadlessNode must outlive this object.
// ---------------------------------------------------------------------------
class InProcessReplicaSource final : public ReplicaSource {
   public:
    explicit InProcessReplicaSource(abi::HeadlessNode& primary)
        : primary_(primary) {}

    int Fetch(const kv_locator_t& locator,
              abi::HeadlessNode::ReplicaChunk* out) override {
        return primary_.ReplicaFetch(locator, out);
    }

   private:
    abi::HeadlessNode& primary_;
};

}  // namespace kvcache::node::replication
