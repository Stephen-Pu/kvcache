// In-process backend for the Core ABI — wires the public C ABI directly to
// the in-tree node subsystems (no shmem ring, no gRPC). This is the demo /
// single-process / unit-test path; the Step-7+ cross-process path will keep
// the same ABI but talk to a real kvagent over shmem.
//
// One HeadlessNode is owned by the library. Multiple kv_ctx_t may share it
// (tenant/model lives in the ctx, not in the node).
#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "ingest/mutable_buffer.h"
#include "ingest/seal.h"
#include "ingest/watermark.h"
#include "kvcache/kv_types.h"
#include "meta/rocksdb_store.h"
#include "prefix/art_index.h"
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
        // Optional ART snapshot file. If set and the file exists at Init
        // time, the in-memory ART is restored from it — faster than the
        // RocksDB sealed-chunks scan. If the file is missing or invalid,
        // Init falls back to a fresh empty ART (and logs to err). If the
        // path is empty, no snapshot work happens.
        std::string art_snapshot_path;
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

    int Fetch(kv_handle_t handle,
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
    std::unique_ptr<node::prefix::EventStream>         events_;
    std::unique_ptr<node::transport::NixlWrapper>      nixl_;
    std::unique_ptr<node::transport::PriorityScheduler> sched_;
};

}  // namespace kvcache::abi
