// Task 5 / Task 6 — A9 DR warm-standby: ReplicationConsumer.
//
// Ties together Tasks 1–4: for each incoming prefix::Event this class
//   1. warm-filters the event (IsWarm / WarmPolicy),
//   2. deduplicates via a monotonic epoch cursor (ReplicationCursor),
//   3. fetches the chunk from the primary (HeadlessNode::ReplicaFetch),
//   4. commits the chunk to the standby (Reserve + Publish + SealByChunkPath).
//
// ApplyEvent is the synchronous per-event step. Start()/Stop() wrap it with a
// live background subscribe loop that drives ApplyEvent on every event emitted
// by the primary's EventStream (via HeadlessNode::SubscribeEvents).
//
// Lifetime: primary and standby must outlive the ReplicationConsumer.
//
// Layer note: HeadlessNode lives in core-abi/ (one layer above kvstore-node/).
// This header forward-declares it and the .cpp includes headless_node.h.
#pragma once

#include <atomic>
#include <cstdint>
#include <thread>

#include "prefix/kv_event_stream.h"
#include "replication/replication_cursor.h"
#include "replication/warm_filter.h"

// Forward-declare HeadlessNode to avoid pulling core-abi headers into every
// kvstore-node translation unit that includes this header.
namespace kvcache::abi {
class HeadlessNode;
}

namespace kvcache::node::replication {

class ReplicationConsumer {
   public:
    struct Options {
        WarmPolicy warm;
    };

    // primary: source of truth — ReplicaFetch is called here.
    // standby: warm mirror — Reserve/Publish/SealByChunkPath are called here.
    // Both references are borrowed; they must outlive the consumer.
    ReplicationConsumer(abi::HeadlessNode& primary,
                        abi::HeadlessNode& standby,
                        Options opts);

    // Destructor: calls Stop() so the background thread is always joined.
    ~ReplicationConsumer();

    // Apply one event synchronously. Returns true iff a chunk was replicated
    // to the standby. Returns false (benign) for:
    //   - filtered events (non-ADD or cold tier),
    //   - duplicate epochs (already applied),
    //   - locator not found on primary (evicted since the event was emitted).
    // On a primary miss the cursor is still advanced to avoid infinite retry.
    bool ApplyEvent(const prefix::Event& ev);

    // The last epoch seen by the cursor (0 = nothing applied yet).
    uint64_t CursorEpoch() const;

    // Start the live subscribe/poll loop. Spawns a background thread that
    // subscribes to the primary's EventStream and calls ApplyEvent on each
    // incoming event. Idempotent — a second Start() while already running is
    // a no-op.
    void Start();

    // Stop the live subscribe/poll loop. Sets the stop flag, unsubscribes
    // from the primary (which wakes the poller thread), and joins the thread.
    // Idempotent — safe to call multiple times or from the destructor.
    void Stop();

    // The number of events for which ApplyEvent returned true (i.e. a chunk
    // was successfully replicated to the standby). Safe to read from any
    // thread at any time; after Stop() returns the value is final.
    uint64_t EventsApplied() const {
        return events_applied_.load(std::memory_order_acquire);
    }

   private:
    // Static member callback passed to HeadlessNode::SubscribeEvents.
    // Being a static member function it has access to all private fields.
    // `user` is always `this` — cast from void* inside the function.
    static void EventCbDispatch(const kv_event_t* raw_ev, void* user);

    abi::HeadlessNode& primary_;
    abi::HeadlessNode& standby_;
    Options            opts_;
    ReplicationCursor  cursor_;

    // Live-loop state. running_ guards Start() idempotency (CAS from false→true).
    std::atomic<bool>          running_{false};
    std::thread                thread_;
    // Subscription handle returned by primary_.SubscribeEvents; 0 = none.
    // Atomic so a concurrent Stop() (e.g. from the destructor while Start() is
    // still in progress on another thread) always sees the live sub_id and
    // can call UnsubscribeEvents rather than silently skipping it.
    // SubscriptionId is uint64_t (HeadlessNode typedef); spelled out here
    // because the header forward-declares HeadlessNode and cannot use its
    // nested types. The .cpp asserts the sizes match.
    std::atomic<uint64_t> sub_id_{0};

    // Count of events for which ApplyEvent returned true (a chunk was actually
    // replicated). Incremented inside EventCbDispatch after a successful Apply.
    // Read via EventsApplied() for test observability.
    std::atomic<uint64_t> events_applied_{0};
};

}  // namespace kvcache::node::replication
