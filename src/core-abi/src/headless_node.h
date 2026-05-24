// In-process backend for the Core ABI — wires the public C ABI directly to
// the in-tree node subsystems (no shmem ring, no gRPC). This is the demo /
// single-process / unit-test path; the Step-7+ cross-process path will keep
// the same ABI but talk to a real kvagent over shmem.
//
// One HeadlessNode is owned by the library. Multiple kv_ctx_t may share it
// (tenant/model lives in the ctx, not in the node).
#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ingest/mutable_buffer.h"
#include "ingest/seal.h"
#include "ingest/watermark.h"
#include "kvcache/kv_types.h"
#include "meta/rocksdb_store.h"
#include "prefix/art_index.h"
#include "prefix/art_wal.h"
#include "prefix/kv_event_stream.h"
#include "prefix/lpm.h"
#include "tier/tier_manager.h"
#include "transport/nixl_wrapper.h"
#include "transport/priority_scheduler.h"

namespace kvcache::abi {

class HeadlessNode {
   public:
    struct Options {
        node::tier::TierManager::Options tier;
        // RocksDB is optional in headless mode — if `rocksdb_path` is empty,
        // the seal path skips RocksDB and only updates ART + EventStream.
        // This keeps the demo path working on dev laptops without rocksdb.
        std::string rocksdb_path;
        // NIXL backend selection. "loopback" is the unit-test / demo default.
        std::string nixl_backend = "loopback";
        // Phase M-4 — used when nixl_backend == "tcp". The TcpBackend
        // binds a listener so peer backends can connect and Pull bytes
        // from the pinned-tier slot pool. 0 = OS-picked port.
        std::string nixl_bind_host = "127.0.0.1";
        uint32_t    nixl_bind_port = 0;
        // Optional ART snapshot file. If set and the file exists at Init
        // time, the in-memory ART is restored from it — faster than the
        // RocksDB sealed-chunks scan. If the file is missing or invalid,
        // Init falls back to a fresh empty ART (and logs to err). If the
        // path is empty, no snapshot work happens.
        std::string art_snapshot_path;
        // Optional ART WAL file (Phase D-2). When set, every Insert /
        // Remove is appended (and fsynced) to this file before the
        // in-memory ART mutates, and the file is replayed on top of
        // the snapshot at Init. Together with art_snapshot_path this
        // gives incremental durability: boot is O(snapshot + WAL tail)
        // instead of O(every-ever-seen-chunk).
        std::string art_wal_path;
    };

    static HeadlessNode* GetOrCreate(const Options& opts, std::string* err);
    static void          Shutdown();

    // Hot-path ABI operations. Status codes match include/kvcache/kv_errors.h.

    int Lookup(const char* tenant_id, uint64_t model_id_hash,
               const uint32_t* tokens, std::size_t n,
               kv_locator_t* out_meta,
               kv_handle_t*  out_handle,
               uint32_t*     out_matched_tokens);

    int Reserve(const kv_locator_t* locator, std::size_t bytes,
                kv_handle_t* out_handle, kv_buffer_desc_t* out_slot);

    int Publish(kv_handle_t handle, kv_buffer_desc_t src, uint64_t watermark);

    // `tenant_hash` plumbs the caller's tenant identity through to the
    // PriorityScheduler so per-tenant round-robin actually kicks in. Pass
    // 0 (kSystemTenantHash) for system traffic that should share a single
    // bucket — see kv_abi.cpp for the FNV-1a string→u64 hash the C ABI uses.
    int Fetch(kv_handle_t handle, uint64_t tenant_hash,
              const kv_range_t* ranges, std::size_t n_ranges,
              kv_buffer_desc_t dst,
              kv_completion_t* out_completion);

    int Wait(kv_completion_t cid, uint32_t timeout_ms);
    int Seal(kv_handle_t handle, const uint32_t* tokens, std::size_t n_tokens);
    int Release(kv_handle_t handle);

    // Persist the in-memory ART to `path` (atomic write-temp + rename).
    // Caller-driven — typical use is on graceful drain, or on a periodic
    // checkpoint timer. Returns true on success; sets *err otherwise.
    bool WriteArtSnapshot(const std::string& path, std::string* err);

    // Event subscription (Phase M-2). The poller thread invokes
    // `cb(event, user)` on each Add / Evict / Promote / Demote event
    // observed by the node's EventStream. One subscription per
    // SubscriptionId — UnsubscribeEvents() cancels and joins.
    using SubscriptionId = uint64_t;
    using EventCallback  = void (*)(const kv_event_t* ev, void* user);
    SubscriptionId SubscribeEvents(EventCallback cb, void* user);
    void           UnsubscribeEvents(SubscriptionId id);

    // Phase M-3 B — expose the NIXL backend so the gRPC layer can
    // Export / Import RemoteMrDescriptors on the same backend that
    // owns the registered pinned-tier MR. Returns nullptr if Init
    // failed (no backend was created).
    node::transport::INixlBackend* backend() {
        return nixl_ ? nixl_->backend() : nullptr;
    }

    // Phase G-2 — explicit destructor so the sweeper joins before
    // the ART / events fields it touches are destroyed.
    ~HeadlessNode();

   private:
    HeadlessNode() = default;

    bool Init(const Options& opts, std::string* err);

    // Per-handle bookkeeping. Two flavors of handle live here:
    //
    //   kIngestHandle  : created by Reserve; owns a MutableBuffer slot.
    //   kReadHandle    : created by Lookup; references a sealed ART leaf and
    //                     holds a refcount on it.
    enum class HandleKind { kIngest, kRead };
    struct HandleState {
        HandleKind kind;
        kv_locator_t locator;
        // For ingest:
        uint64_t ingest_handle = 0;
        // For read:
        node::prefix::LeafData* leaf = nullptr;
    };

    mutable std::mutex mu_;
    std::unordered_map<kv_handle_t, HandleState> handles_;
    std::atomic<kv_handle_t> next_handle_{1};

    // Subsystems.
    std::unique_ptr<node::tier::TierManager>           tm_;
    std::unique_ptr<node::meta::RocksdbStore>          rocks_;     // optional
    std::unique_ptr<node::ingest::MutableBufferPool>   buffers_;
    std::unique_ptr<node::ingest::WatermarkTracker>    wm_;
    std::unique_ptr<node::prefix::ArtIndex>            art_;
    // Optional wrapper that funnels Insert / Remove through a WAL +
    // periodic snapshot (Phase D-2). When `art_wal_path` is unset in
    // Options the wrapper is nullptr and writes go straight to art_.
    std::unique_ptr<node::prefix::ArtWal>              art_wal_;
    std::unique_ptr<node::prefix::EventStream>         events_;
    std::unique_ptr<node::transport::NixlWrapper>      nixl_;

    // ----- event subscription bookkeeping (Phase M-2) -----
    struct EventSub {
        EventCallback cb   = nullptr;
        void*         user = nullptr;
        // Subscriber ring handle inside `events_`.
        uint64_t      ring_handle = 0;
        // Per-subscription poller. Joined inside UnsubscribeEvents.
        std::thread   poller;
        std::atomic<bool> stop{false};
    };
    mutable std::mutex                                            sub_mu_;
    std::unordered_map<SubscriptionId, std::unique_ptr<EventSub>> subs_;
    std::atomic<SubscriptionId>                                   next_sub_{1};

    // ----- DRAM-eviction → ART-prune bridge (Phase G-1) -----
    //
    // At Seal time we remember the (DramKey -> chunk_path) mapping for
    // every leaf inserted into ART. When DramTier evicts bytes for a
    // key, the callback consults this map, removes the corresponding
    // leaf from ART (so future Lookups miss instead of hitting on a
    // dead pointer), and publishes a KV_EVENT_EVICT.
    //
    // Refcount semantics: leaves carry an initial Acquire() at Seal,
    // representing the ART-owned reference. The callback decrements
    // that ref before removing — if other holders are still in flight
    // (refcount > 1), the removal is skipped and the leaf becomes a
    // "Lookup hits, Fetch misses" zombie until the next eviction
    // round retries it (Phase G-2 will add a sweeper).
    struct DramKeyHasher {
        std::size_t operator()(const node::tier::DramKey& k) const noexcept {
            return node::tier::DramKeyHash{}(k);
        }
    };
    mutable std::mutex                                                mu_evict_;
    std::unordered_map<node::tier::DramKey,
                       std::vector<node::prefix::ChunkHash>,
                       DramKeyHasher>                                 evict_index_;

    void OnDramEvict(const node::tier::DramKey& key);

    // ----- Refcount-deferred eviction sweep (Phase G-2) -----
    //
    // When OnDramEvict fires but the leaf still has outstanding readers
    // (refcount > 1), we cannot safely Remove from ART without leaving
    // dangling handles. The path is queued here and a background
    // sweeper retries each entry: as soon as TryEvict succeeds, the
    // leaf is removed and a KV_EVENT_EVICT is published. Entries are
    // dropped from the queue when either (a) the eviction succeeds or
    // (b) the path no longer points at the same leaf (a fresh Seal
    // replaced it).
    struct DeferredEvict {
        std::vector<node::prefix::ChunkHash> path;
        // Identity of the leaf we deferred. If a later Seal replaces
        // the leaf at this path, the new leaf has a different
        // address; the sweeper drops the entry without acting.
        node::prefix::LeafData* leaf = nullptr;
        // Locator captured at defer time so the EVICT event we
        // eventually publish carries something useful for subscribers.
        kv_locator_t locator{};
    };
    mutable std::mutex                       mu_defer_;
    std::vector<DeferredEvict>               deferred_evicts_;
    std::condition_variable                  defer_cv_;
    std::atomic<bool>                        sweeper_stop_{false};
    std::thread                              sweeper_;

    // True if the leaf was claimed and removed; false if it was
    // deferred or already gone. Publishes the EVICT event on success.
    bool TryEvictNow(std::span<const node::prefix::ChunkHash> path);
    void SweeperLoop();
};

}  // namespace kvcache::abi
