// Task 5 / Task 6 — A9 DR warm-standby: ReplicationConsumer.
//
// Ties together Tasks 1–4: for each incoming prefix::Event this class
//   1. warm-filters the event (IsWarm / WarmPolicy),
//   2. deduplicates via a monotonic epoch cursor (ReplicationCursor),
//   3. fetches the chunk via a ReplicaSource (in-process or remote),
//   4. commits the chunk to the standby (Reserve + Publish + SealByChunkPath).
//
// ApplyEvent is the synchronous per-event step. Start()/Stop() wrap it with a
// live background subscribe loop that drives ApplyEvent on every event emitted
// by the primary's EventStream (via HeadlessNode::SubscribeEvents).
//
// Task 3 change: the fetch dependency is abstracted behind ReplicaSource.
// Two constructors are provided:
//
//   (HeadlessNode& primary, HeadlessNode& standby, Options)  [convenience]
//     Owns an InProcessReplicaSource wrapping `primary` for the fetch path;
//     also keeps the `primary` reference for Start()'s SubscribeEvents call.
//     All existing callers/tests use this form — behaviour is unchanged.
//
//   (ReplicaSource& source, HeadlessNode& standby, Options)  [remote path]
//     Uses an externally-owned ReplicaSource for fetches. Start()/Stop() are
//     NOT supported on this variant (the event-subscription path stays in-
//     process; remote event subscription is a follow-on task). Calling Start()
//     on a source-only consumer returns immediately (no-op) with no crash.
//
// Lifetime: all borrowed references must outlive the ReplicationConsumer.
//
// Layer note: HeadlessNode lives in core-abi/ (one layer above kvstore-node/).
// This header includes replica_source.h (which includes headless_node.h).
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>

#include "prefix/kv_event_stream.h"
#include "replication/replica_source.h"
#include "replication/replication_cursor.h"
#include "replication/warm_filter.h"

namespace kvcache::node::replication {

class ReplicationConsumer {
   public:
    struct Options {
        WarmPolicy warm;
    };

    // Convenience constructor (in-process path — today's default).
    //
    // primary: source of truth — chunk fetches go through an owned
    //          InProcessReplicaSource that calls primary.ReplicaFetch().
    //          Also used for Start()'s SubscribeEvents event stream.
    // standby: warm mirror — Reserve/Publish/SealByChunkPath are called here.
    // Both references are borrowed; they must outlive the consumer.
    ReplicationConsumer(abi::HeadlessNode& primary,
                        abi::HeadlessNode& standby,
                        Options opts);

    // Remote-fetch constructor (Task 3 seam — remote gRPC path).
    //
    // source:  externally-owned ReplicaSource used for chunk fetches.
    //          Must outlive the consumer.
    // standby: warm mirror — same as above.
    //
    // Start()/Stop(): the live event-subscription path requires a HeadlessNode
    // to call SubscribeEvents; this ctor has no such reference, so Start() is
    // a documented no-op. Remote event subscription is a follow-on task.
    ReplicationConsumer(ReplicaSource& source,
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
    //
    // NOTE: only functional when constructed via the (HeadlessNode& primary,
    // HeadlessNode& standby, Options) convenience ctor. When constructed via
    // (ReplicaSource&, HeadlessNode&, Options) this is a documented no-op:
    // remote event subscription is a follow-on task.
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

    // --- Fetch dependency (Task 3) -------------------------------------------
    //
    // owned_source_: non-null only when constructed via the (HeadlessNode&,
    //   HeadlessNode&, Options) convenience ctor. Lifetime matches the consumer.
    // source_: always valid — points to *owned_source_ (in-process ctor) or to
    //   the externally-owned ReplicaSource passed to the remote ctor.
    //   ApplyEvent always calls source_.Fetch() regardless of which ctor was used.
    std::unique_ptr<InProcessReplicaSource> owned_source_;
    ReplicaSource&                          source_;

    // --- Event-subscription dependency (in-process path only) ----------------
    //
    // primary_for_events_: non-null only when constructed via the convenience
    //   ctor (has an in-process primary for SubscribeEvents). Nullptr means
    //   Start() is a no-op.
    abi::HeadlessNode* primary_for_events_;

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
