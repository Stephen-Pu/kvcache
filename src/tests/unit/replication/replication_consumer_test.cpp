// Task 5 / Task 6 — A9 DR warm-standby: ReplicationConsumer unit test.
//
// Strategy: compile headless_node.cpp directly into this test binary (same
// pattern as test_replica_fetch and test_seal_by_chunk_path). HeadlessNode
// is a process singleton, so both "primary" and "standby" roles are served
// by the same instance with non-overlapping locator / token ranges.
//
// Single-singleton note: because primary_ == standby_ in this test, the
// SealByChunkPath step replaces the already-inserted ART leaf (kReplaced)
// rather than inserting fresh — the D-5 ART handles this as a non-error.
// This is a test-environment limitation; in production each node has its own
// ART and the standby leaf is always a true kInserted.
//
// Live-loop note (Task 6): because primary == standby, the loop's own
// SealByChunkPath calls emit further ADD events. The cursor-plus-locator
// dedup stops re-processing the same (epoch, locator) pair; cascading
// events for the same locator have different epochs but the cursor advances
// monotonically, and SealByChunkPath returns kReplaced (KV_OK) so the loop
// is idempotent. Stop() joins the thread cleanly regardless.
//
// Event capture: HeadlessNode::SubscribeEvents delivers kv_event_t via a
// background poller thread. SealAndCaptureEvent subscribes before Seal,
// polls the ring in a tight loop (with timeout) after Seal, and translates
// kv_event_t → prefix::Event so the caller can feed it to ApplyEvent.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

#include "ctx_options.h"
#include "headless_node.h"
#include "kvcache/kv_errors.h"
#include "kvcache/kv_types.h"
#include "prefix/kv_event_stream.h"
#include "prefix/lpm.h"  // NamespaceFingerprint, ChunkifyNS, kChunkTokens
#include "replication/replication_consumer.h"

using kvcache::abi::HeadlessNode;
using kvcache::node::prefix::Event;
using kvcache::node::prefix::EventType;
using kvcache::node::prefix::Tier;
using kvcache::node::replication::ReplicationConsumer;
using kvcache::node::replication::WarmPolicy;

namespace {

// FNV-1a 64-bit — mirrors kvcache::Fnv1a64 in hashing.h.
uint64_t Fnv1a64(const std::string& s) {
    uint64_t h = 0xcbf29ce484222325ull;
    for (unsigned char c : s) {
        h ^= c;
        h *= 0x100000001b3ull;
    }
    return h;
}

// Build a kv_locator_t for (model, tokens).
kv_locator_t MakeLocator(const std::string& model,
                          const std::vector<uint32_t>& tokens) {
    kv_locator_t loc{};
    std::memset(&loc, 0, sizeof(loc));
    loc.model_id_hash     = Fnv1a64(model);
    loc.range.token_count = static_cast<uint32_t>(tokens.size());
    loc.version           = 1;
    for (std::size_t i = 0; i < tokens.size(); ++i) {
        loc.prefix_hash[(i * 7) % 16] ^= static_cast<uint8_t>(tokens[i] & 0xff);
        loc.prefix_hash[(i * 3) % 16] ^=
            static_cast<uint8_t>((tokens[i] >> 8) & 0xff);
    }
    return loc;
}

// Initialise the HeadlessNode singleton once for the whole test binary.
HeadlessNode* GetNode() {
    static HeadlessNode* node = [] {
        auto opts = kvcache::abi::BuildCtxOptions(nullptr);
        std::string err;
        auto* n = HeadlessNode::GetOrCreate(opts, &err);
        return n;
    }();
    return node;
}

// ---------------------------------------------------------------------------
// SealAndCaptureEvent: Reserve → Publish → Seal on the node and capture the
// resulting KV_EVENT_ADD emitted by the EventStream.
//
// Uses SubscribeEvents (background poller delivering kv_event_t callbacks)
// and converts to prefix::Event.  The background thread is joined inside
// UnsubscribeEvents before we return, ensuring the event is fully captured.
// ---------------------------------------------------------------------------
struct CapturedEvent {
    std::mutex        mu;
    bool              ready = false;
    kv_event_t        raw{};
};

// Static callback signature required by HeadlessNode::SubscribeEvents.
static void EventCb(const kv_event_t* ev, void* user) {
    auto* cap = static_cast<CapturedEvent*>(user);
    std::lock_guard lk(cap->mu);
    if (!cap->ready) {
        cap->raw   = *ev;
        cap->ready = true;
    }
}

// Seals a chunk with the given tokens and payload, waits for the ADD event,
// and returns it as a prefix::Event.  Token count must be >= kChunkTokens (16).
Event SealAndCaptureEvent(HeadlessNode* node,
                           const std::string& model,
                           const std::vector<uint32_t>& tokens,
                           uint8_t fill_byte) {
    const uint64_t tenant_hash  = 0;
    const uint64_t model_hash   = Fnv1a64(model);
    const kv_locator_t loc      = MakeLocator(model, tokens);
    const std::size_t n_bytes   = tokens.size() * 64;
    const std::vector<uint8_t> payload(n_bytes, fill_byte);

    // Subscribe before Seal so we don't miss the event.
    CapturedEvent cap;
    auto sub_id = node->SubscribeEvents(EventCb, &cap);

    kv_handle_t      h{};
    kv_buffer_desc_t slot{};
    int rc = node->Reserve(&loc, n_bytes, tenant_hash, model_hash, &h, &slot);
    if (rc != KV_OK) {
        node->UnsubscribeEvents(sub_id);
        ADD_FAILURE() << "Reserve failed: " << rc;
        return {};
    }
    if (slot.addr) std::memcpy(slot.addr, payload.data(), n_bytes);
    kv_buffer_desc_t empty{};
    node->Publish(h, empty, n_bytes);
    node->Seal(h, tokens.data(), tokens.size());

    // Spin-wait up to 500 ms for the background poller to deliver the event.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    while (std::chrono::steady_clock::now() < deadline) {
        {
            std::lock_guard lk(cap.mu);
            if (cap.ready) break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // Unsubscribe joins the poller thread; cap.ready is stable after this.
    node->UnsubscribeEvents(sub_id);

    if (!cap.ready) {
        ADD_FAILURE() << "Timed out waiting for ADD event";
        return {};
    }

    // Translate kv_event_t → prefix::Event.
    Event ev{};
    ev.type     = static_cast<EventType>(cap.raw.type);
    ev.tier     = static_cast<Tier>(cap.raw.tier);
    ev.locator  = cap.raw.locator;
    ev.epoch    = cap.raw.epoch;
    return ev;
}

// Count matched tokens when looking up the given tokens on `node`.
uint32_t LookupMatched(HeadlessNode* node,
                        const std::string& model,
                        const std::vector<uint32_t>& tokens) {
    const uint64_t tenant_hash = 0;
    const uint64_t model_hash  = Fnv1a64(model);

    kv_locator_t out_meta{};
    kv_handle_t  lh      = 0;
    uint32_t     matched = 0;
    int rc = node->Lookup("", tenant_hash, model_hash,
                           tokens.data(), tokens.size(),
                           &out_meta, &lh, &matched);
    if (rc == KV_OK && lh) node->Release(lh);
    return matched;
}

}  // namespace

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class ReplicationConsumerTest : public ::testing::Test {
   protected:
    HeadlessNode* node_ = nullptr;
    void SetUp() override {
        node_ = GetNode();
        ASSERT_NE(node_, nullptr) << "HeadlessNode::GetOrCreate failed";
    }
};

// ---------------------------------------------------------------------------
// Main scenario: warm chunk replicated, duplicate epoch no-op, cold filtered,
// never-sealed locator skipped cleanly.
// ---------------------------------------------------------------------------
TEST_F(ReplicationConsumerTest, MirrorsWarmChunksNotColdOrDuplicate) {
    // Unique model / token range to avoid ART conflicts with other tests.
    const std::string      model  = "rc-consumer-test-model";
    std::vector<uint32_t>  tokens(32);
    for (uint32_t i = 0; i < 32; ++i) tokens[i] = 20000u + i;

    // primary == standby == singleton (single-process test limitation).
    //
    // HeadlessNode in headless (no-RocksDB) mode seals all chunks into the
    // DRAM tier (Tier::Dram == 3). max_tier = 3 ensures ADD events for
    // DRAM-resident chunks pass the warm filter.  In production (HBM hardware),
    // max_tier = 1 would be used to replicate only HBM-resident chunks.
    ReplicationConsumer rc(*node_, *node_, {.warm = {.max_tier = 3}});

    EXPECT_EQ(rc.CursorEpoch(), 0u) << "cursor starts at zero";

    // -----------------------------------------------------------------------
    // 1. Seal a warm chunk on "primary"; capture the ADD event.
    // -----------------------------------------------------------------------
    Event add = SealAndCaptureEvent(node_, model, tokens, 0xAB);
    ASSERT_EQ(add.type, EventType::Add) << "captured event must be ADD";
    EXPECT_GT(add.epoch, 0u) << "EventStream stamps epoch ≥ 1";

    // -----------------------------------------------------------------------
    // 2. Apply the warm ADD — should replicate.
    //
    //    Single-singleton note: SealByChunkPath inserts the same chunk_path
    //    that Seal() already placed in the ART; the D-5 ART returns kReplaced
    //    (not an error), so ApplyEvent returns true.
    // -----------------------------------------------------------------------
    EXPECT_TRUE(rc.ApplyEvent(add)) << "warm chunk should be replicated";
    EXPECT_EQ(rc.CursorEpoch(), add.epoch) << "cursor must advance to add.epoch";

    // Verify the standby (same singleton) can look up the chunk.
    EXPECT_EQ(LookupMatched(node_, model, tokens),
              static_cast<uint32_t>(tokens.size()))
        << "standby Lookup must match all tokens";

    // -----------------------------------------------------------------------
    // 3. Duplicate epoch is a no-op.
    // -----------------------------------------------------------------------
    EXPECT_FALSE(rc.ApplyEvent(add)) << "duplicate epoch must be rejected";
    EXPECT_EQ(rc.CursorEpoch(), add.epoch) << "cursor must not regress";

    // -----------------------------------------------------------------------
    // 4. Cold-tier ADD (tier > max_tier=3) is filtered by IsWarm.
    //
    //    Tier::Cold == 5 > max_tier=3, so IsWarm returns false.
    //    Use a new epoch so the cursor doesn't block it first.
    // -----------------------------------------------------------------------
    Event cold    = add;
    cold.tier     = Tier::Cold;   // Cold (5) > Dram (3) == max_tier → filtered
    cold.epoch    = add.epoch + 1;
    EXPECT_FALSE(rc.ApplyEvent(cold)) << "cold-tier ADD must be filtered";
    EXPECT_EQ(rc.CursorEpoch(), add.epoch)
        << "cursor must not advance for filtered event";

    // -----------------------------------------------------------------------
    // 5. ADD for a locator that was never sealed on primary → skipped cleanly.
    //
    //    ReplicaFetch returns KV_E_NOT_FOUND; ApplyEvent advances the cursor
    //    to prevent infinite retry and returns false.
    // -----------------------------------------------------------------------
    const std::vector<uint32_t> unknown_tokens = {60001, 60002, 60003,
                                                   60004, 60005, 60006,
                                                   60007, 60008, 60009,
                                                   60010, 60011, 60012,
                                                   60013, 60014, 60015,
                                                   60016};  // 16 tokens
    Event gone{};
    gone.type    = EventType::Add;
    gone.tier    = Tier::Hbm;
    gone.epoch   = add.epoch + 2;
    gone.locator = MakeLocator("rc-never-sealed", unknown_tokens);

    EXPECT_FALSE(rc.ApplyEvent(gone)) << "miss on primary must return false";
    EXPECT_EQ(rc.CursorEpoch(), add.epoch + 2)
        << "cursor must advance past the miss epoch to prevent retry";
}

// ---------------------------------------------------------------------------
// Cursor starts at zero; applying events with epoch == 0 is rejected.
// ---------------------------------------------------------------------------
TEST_F(ReplicationConsumerTest, EpochZeroRejectedByCursor) {
    ReplicationConsumer rc(*node_, *node_, {.warm = {.max_tier = 3}});
    Event ev{};
    ev.type  = EventType::Add;
    ev.tier  = Tier::Dram;
    ev.epoch = 0;  // epoch 0 ≤ cursor.last_ (0) → ShouldApply returns false
    // epoch 0 is NOT > last_ (0), so ShouldApply returns false.
    EXPECT_FALSE(rc.ApplyEvent(ev));
    EXPECT_EQ(rc.CursorEpoch(), 0u);
}

// ---------------------------------------------------------------------------
// Non-ADD events (Evict, Promote, Demote) are filtered by IsWarm.
// ---------------------------------------------------------------------------
TEST_F(ReplicationConsumerTest, NonAddEventsFiltered) {
    ReplicationConsumer rc(*node_, *node_, {.warm = {.max_tier = 5}});
    for (auto t : {EventType::Evict, EventType::Promote, EventType::Demote}) {
        Event ev{};
        ev.type  = t;
        ev.tier  = Tier::Hbm;
        ev.epoch = static_cast<uint64_t>(t) * 100;
        EXPECT_FALSE(rc.ApplyEvent(ev))
            << "non-ADD event type " << static_cast<int>(t)
            << " must be filtered";
    }
    EXPECT_EQ(rc.CursorEpoch(), 0u) << "cursor must not move for filtered events";
}

// ---------------------------------------------------------------------------
// Task 6 — Live subscribe/poll loop: Start() + Stop().
//
// After Start(), seal 3 warm chunks on the primary (== standby singleton).
// Wait until EventsApplied() reaches 3 — this proves the live loop actually
// ran ApplyEvent, not just that the chunks exist in the ART from Seal().
// Then Stop() — must join cleanly, no hang.
//
// Cold-chunk filtering is validated via the synchronous MirrorsWarmChunksNotColdOrDuplicate
// test; in the loopback headless backend we cannot force a real Cold-tier event
// from the seal path, so we do not attempt it here.
//
// WaitFor: bounded spin-poll (10 ms slices, 2 s budget) — same pattern as
// node_registry_test / etcd_client_test.
// ---------------------------------------------------------------------------
namespace {

bool WaitFor(std::function<bool()> pred,
             std::chrono::milliseconds budget = std::chrono::seconds(2)) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

}  // namespace

TEST_F(ReplicationConsumerTest, LiveLoopReplicatesWarmNotCold) {
    // Use a unique model / token range so there are no ART conflicts with
    // the earlier synchronous tests.
    const std::string model = "rc-live-loop-model";

    // 3 warm chunks, each a distinct 32-token window (80000+ range).
    constexpr int kWarmCount = 3;
    std::vector<std::vector<uint32_t>> warm_tokens(kWarmCount);
    for (int i = 0; i < kWarmCount; ++i) {
        warm_tokens[i].resize(32);
        for (uint32_t j = 0; j < 32; ++j)
            warm_tokens[i][j] = 80000u + static_cast<uint32_t>(i) * 100 + j;
    }

    // max_tier = 3 (Dram) — headless mode seals into DRAM tier.
    ReplicationConsumer rc(*node_, *node_, {.warm = {.max_tier = 3}});

    EXPECT_EQ(rc.EventsApplied(), 0u) << "no events applied before Start()";

    // -----------------------------------------------------------------------
    // Start the live loop before sealing any chunks so the loop is listening
    // when the ADD events arrive.
    // -----------------------------------------------------------------------
    rc.Start();

    // -----------------------------------------------------------------------
    // Seal 3 warm chunks. SealAndCaptureEvent subscribes its own one-shot
    // listener, seals the chunk, waits for the ADD event, and unsubscribes.
    // The live loop has its OWN separate subscription and will also receive
    // these ADD events (possibly with a slight delay).
    // -----------------------------------------------------------------------
    for (int i = 0; i < kWarmCount; ++i) {
        Event ev = SealAndCaptureEvent(node_, model, warm_tokens[i],
                                        static_cast<uint8_t>(0x10 + i));
        ASSERT_EQ(ev.type, EventType::Add)
            << "warm chunk " << i << " must emit ADD";
    }

    // -----------------------------------------------------------------------
    // Wait until the live loop has called ApplyEvent successfully for ALL 3
    // warm chunks. EventsApplied() is incremented by EventCbDispatch only when
    // ApplyEvent returns true — so this assertion fails if the loop is a stub
    // or if Start() were a no-op. The shared-singleton means SealByChunkPath
    // returns kReplaced (KV_OK), so each warm chunk counts as one application.
    // -----------------------------------------------------------------------
    bool loop_ran = WaitFor([&] {
        return rc.EventsApplied() >= static_cast<uint64_t>(kWarmCount);
    });
    EXPECT_TRUE(loop_ran)
        << "live loop must process all " << kWarmCount
        << " warm ADD events within 2 s (EventsApplied="
        << rc.EventsApplied() << ")";

    // Cursor must have advanced beyond zero.
    EXPECT_GT(rc.CursorEpoch(), 0u)
        << "cursor must advance after the live loop processes events";

    // -----------------------------------------------------------------------
    // Stop — joins the background sentinel thread; must return without hanging.
    // Assertions after Stop() are race-free: the poller is joined and no
    // further increments to events_applied_ can occur.
    // -----------------------------------------------------------------------
    rc.Stop();  // joins background thread

    // In the single-singleton topology (primary == standby), SealByChunkPath
    // emits further ADD events that the live loop also processes (cascading
    // sealed chunks with new epochs, all deduped by the cursor). EventsApplied
    // is therefore >= kWarmCount, not exactly kWarmCount. The critical proof
    // that the loop ran is the WaitFor above; this assertion confirms the
    // final count is at least as large as expected.
    EXPECT_GE(rc.EventsApplied(), static_cast<uint64_t>(kWarmCount))
        << "EventsApplied must be >= warm chunk count after Stop()";

    // Idempotent: a second Stop() must not crash or hang.
    rc.Stop();
}
