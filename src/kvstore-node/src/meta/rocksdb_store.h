// LLD §2.3 — Node-local metadata store, backed by RocksDB.
//
// Single RocksDB instance per node. Four column families:
//
//   "default"             — node-level configuration (cluster id, node id,
//                           NIXL transport selection, schema version, the
//                           monotonic cluster_epoch counter).
//   "sealed_chunks"       — sealed KV chunks: key = SealedChunkKey (40B),
//                           value = SealedChunkValue (variable).
//   "tenant_quota_state"  — last-known per-tenant quota and stale GC bookkeeping;
//                           key = tenant_id (16B), value = TenantQuotaSnapshot.
//   "audit_buffer_overflow" — audit events that did not fit in the in-memory
//                           ring buffer and were spilled here; key = epoch+seq,
//                           value = serialized audit event.
//
// All keys are big-endian where ordering matters (RocksDB's lexicographic
// comparator yields the natural numeric order).
//
// Crash recovery (LLD §7.3): on node boot, the prefix engine scans the
// "sealed_chunks" CF via NewSealedChunkIterator() and rebuilds the in-memory
// ART before state transitions to JOINING.

#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "kvcache/kv_types.h"

namespace rocksdb {
class DB;
class ColumnFamilyHandle;
}  // namespace rocksdb

namespace kvcache::node::meta {

// ---------------------------------------------------------------------------
// Column family names — stable identifiers, must never change without a
// schema-version bump in the "default" CF.
// ---------------------------------------------------------------------------
inline constexpr std::string_view kCfDefault              = "default";
inline constexpr std::string_view kCfSealedChunks         = "sealed_chunks";
inline constexpr std::string_view kCfTenantQuotaState     = "tenant_quota_state";
inline constexpr std::string_view kCfAuditBufferOverflow  = "audit_buffer_overflow";

inline constexpr uint32_t kSchemaVersion = 1;

// ---------------------------------------------------------------------------
// SealedChunkKey — 40 bytes:
//   tenant_id      : 16B
//   model_id_hash  :  8B  (big-endian)
//   prefix_hash    : 16B
//
// This is exactly the "identity" portion of a kv_locator_t (LLD §2.1).
// Range fields are intentionally excluded — all sub-ranges of one prefix
// collide on the same key, and per-range coverage is encoded in the value.
// ---------------------------------------------------------------------------
struct SealedChunkKey {
    std::array<uint8_t, 40> bytes;

    static SealedChunkKey From(const kv_locator_t& loc);
    std::string_view View() const noexcept {
        return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    }
};

struct SealedChunkValue {
    uint32_t version;                  // == kSchemaVersion
    uint64_t sealed_at_nanos;
    uint64_t bytes_total;
    uint32_t tier_residency_bitmap;    // bit per Tier (events.proto)
    kv_range_t range_covered;
    std::vector<uint8_t> opaque_meta;  // forward-compat extension space

    std::string Serialize() const;
    static std::optional<SealedChunkValue> Deserialize(std::string_view bytes);
};

struct TenantQuotaSnapshot {
    uint64_t capacity_used_bytes;
    uint32_t qps_window_count;
    uint64_t bandwidth_window_bytes;
    uint64_t last_reconcile_unix_nanos;

    std::string Serialize() const;
    static std::optional<TenantQuotaSnapshot> Deserialize(std::string_view);
};

// ---------------------------------------------------------------------------
// RocksdbStore
// ---------------------------------------------------------------------------

class RocksdbStore {
   public:
    struct Options {
        std::string path;
        bool        create_if_missing = true;
        bool        use_direct_io     = false;
        uint64_t    block_cache_bytes = 64ull << 20;
        // TODO(stephen): RateLimiter, WriteBufferManager, Statistics, etc.
    };

    static std::unique_ptr<RocksdbStore> Open(const Options& opts, std::string* err);

    RocksdbStore() = default;
    ~RocksdbStore();
    RocksdbStore(const RocksdbStore&) = delete;
    RocksdbStore& operator=(const RocksdbStore&) = delete;

    // ---------- sealed_chunks CF ----------

    // Atomic seal commit: writes the SealedChunkValue and advances the cluster
    // epoch counter in the "default" CF, in a single WriteBatch. This is the
    // boundary that makes a streaming-write visible to future lookups.
    bool PutSealedChunkAtomic(const SealedChunkKey& key,
                              const SealedChunkValue& value,
                              uint64_t new_epoch,
                              std::string* err);

    std::optional<SealedChunkValue> GetSealedChunk(const SealedChunkKey& key,
                                                    std::string* err);

    bool DeleteSealedChunk(const SealedChunkKey& key, std::string* err);

    class SealedChunkIterator;
    std::unique_ptr<SealedChunkIterator> NewSealedChunkIterator();

    // ---------- tenant_quota_state CF ----------

    bool PutQuotaState(std::span<const uint8_t, 16> tenant_id,
                       const TenantQuotaSnapshot& snap, std::string* err);
    std::optional<TenantQuotaSnapshot> GetQuotaState(
        std::span<const uint8_t, 16> tenant_id, std::string* err);

    // ---------- audit_buffer_overflow CF ----------

    bool AppendAuditOverflow(std::span<const uint8_t> event_bytes, std::string* err);

    // ---------- default CF ----------

    bool PutConfig(std::string_view key, std::string_view value, std::string* err);
    std::optional<std::string> GetConfig(std::string_view key, std::string* err);

    uint64_t CurrentEpoch() const noexcept;

   private:
    // These handles are only touched when KVCACHE_HAVE_ROCKSDB is defined.
    // The facade build (no rocksdb linked) keeps the fields present so the
    // class layout is stable across translation units.
    [[maybe_unused]] rocksdb::DB* db_ = nullptr;
    [[maybe_unused]] rocksdb::ColumnFamilyHandle* cf_default_ = nullptr;
    [[maybe_unused]] rocksdb::ColumnFamilyHandle* cf_sealed_  = nullptr;
    [[maybe_unused]] rocksdb::ColumnFamilyHandle* cf_quota_   = nullptr;
    [[maybe_unused]] rocksdb::ColumnFamilyHandle* cf_audit_   = nullptr;

    mutable uint64_t cached_epoch_ = 0;
};

class RocksdbStore::SealedChunkIterator {
   public:
    virtual ~SealedChunkIterator() = default;
    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual SealedChunkKey   Key() const = 0;
    virtual SealedChunkValue Value(std::string* err) const = 0;
};

}  // namespace kvcache::node::meta
