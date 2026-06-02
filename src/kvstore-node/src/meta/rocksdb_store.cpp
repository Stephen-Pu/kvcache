// LLD §2.3 — RocksDB-backed metadata store.
//
// When KVCACHE_HAVE_ROCKSDB is defined this TU links real rocksdb. Otherwise
// it compiles to a facade whose accessors return errors; this keeps developer
// builds green on machines that don't have rocksdb installed.
#include "meta/rocksdb_store.h"

#include <atomic>
#include <cstring>

#if defined(KVCACHE_HAVE_ROCKSDB)
#include <memory>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/write_buffer_manager.h>
#endif

namespace kvcache::node::meta {

#if defined(KVCACHE_HAVE_ROCKSDB)
// These helpers are only referenced inside the KVCACHE_HAVE_ROCKSDB branch
// below; gate them out of the facade build to silence -Wunused-function.
namespace {

inline std::string BeU64(uint64_t v) {
    std::string s(8, '\0');
    for (int i = 7; i >= 0; --i) { s[i] = static_cast<char>(v & 0xff); v >>= 8; }
    return s;
}
inline uint64_t ParseBeU64(std::string_view s) {
    if (s.size() != 8) return 0;
    uint64_t v = 0;
    for (size_t i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint8_t>(s[i]);
    return v;
}

constexpr std::string_view kKeyClusterEpoch  = "cluster_epoch";
constexpr std::string_view kKeySchemaVersion = "schema_version";

}  // namespace
#endif  // KVCACHE_HAVE_ROCKSDB

// ---------------------------------------------------------------------------
// SealedChunkKey
// ---------------------------------------------------------------------------

SealedChunkKey SealedChunkKey::From(const kv_locator_t& loc) {
    SealedChunkKey k{};
    std::memcpy(k.bytes.data() + 0, loc.tenant_id, 16);
    uint64_t m = loc.model_id_hash;
    for (int i = 7; i >= 0; --i) {
        k.bytes[16 + i] = static_cast<uint8_t>(m & 0xff);
        m >>= 8;
    }
    std::memcpy(k.bytes.data() + 24, loc.prefix_hash, 16);
    return k;
}

// ---------------------------------------------------------------------------
// SealedChunkValue
// ---------------------------------------------------------------------------

std::string SealedChunkValue::Serialize() const {
    // Layout (host-endian; never read across architectures):
    //   [version u32][sealed_at u64][bytes_total u64][tier_bitmap u32]
    //   [range 16B][opaque_len u32][opaque bytes...]
    std::string out;
    out.reserve(4 + 8 + 8 + 4 + 16 + 4 + opaque_meta.size());
    auto put = [&](const void* p, size_t n) {
        out.append(reinterpret_cast<const char*>(p), n);
    };
    put(&version,               sizeof(version));
    put(&sealed_at_nanos,       sizeof(sealed_at_nanos));
    put(&bytes_total,           sizeof(bytes_total));
    put(&tier_residency_bitmap, sizeof(tier_residency_bitmap));
    put(&range_covered,         sizeof(range_covered));
    uint32_t olen = static_cast<uint32_t>(opaque_meta.size());
    put(&olen, sizeof(olen));
    if (olen) put(opaque_meta.data(), olen);
    return out;
}

std::optional<SealedChunkValue> SealedChunkValue::Deserialize(std::string_view in) {
    constexpr size_t kFixed = 4 + 8 + 8 + 4 + 16 + 4;
    if (in.size() < kFixed) return std::nullopt;
    SealedChunkValue v{};
    size_t off = 0;
    auto get = [&](void* p, size_t n) {
        std::memcpy(p, in.data() + off, n);
        off += n;
    };
    get(&v.version,               sizeof(v.version));
    if (v.version != kSchemaVersion) return std::nullopt;
    get(&v.sealed_at_nanos,       sizeof(v.sealed_at_nanos));
    get(&v.bytes_total,           sizeof(v.bytes_total));
    get(&v.tier_residency_bitmap, sizeof(v.tier_residency_bitmap));
    get(&v.range_covered,         sizeof(v.range_covered));
    uint32_t olen;
    get(&olen, sizeof(olen));
    if (in.size() < off + olen) return std::nullopt;
    v.opaque_meta.assign(in.data() + off, in.data() + off + olen);
    return v;
}

// ---------------------------------------------------------------------------
// TenantQuotaSnapshot
// ---------------------------------------------------------------------------

std::string TenantQuotaSnapshot::Serialize() const {
    std::string out;
    out.reserve(28);
    auto put = [&](const void* p, size_t n) {
        out.append(reinterpret_cast<const char*>(p), n);
    };
    put(&capacity_used_bytes,       sizeof(capacity_used_bytes));
    put(&qps_window_count,          sizeof(qps_window_count));
    put(&bandwidth_window_bytes,    sizeof(bandwidth_window_bytes));
    put(&last_reconcile_unix_nanos, sizeof(last_reconcile_unix_nanos));
    return out;
}

std::optional<TenantQuotaSnapshot>
TenantQuotaSnapshot::Deserialize(std::string_view in) {
    if (in.size() < 28) return std::nullopt;
    TenantQuotaSnapshot s{};
    size_t off = 0;
    auto get = [&](void* p, size_t n) {
        std::memcpy(p, in.data() + off, n);
        off += n;
    };
    get(&s.capacity_used_bytes,       sizeof(s.capacity_used_bytes));
    get(&s.qps_window_count,          sizeof(s.qps_window_count));
    get(&s.bandwidth_window_bytes,    sizeof(s.bandwidth_window_bytes));
    get(&s.last_reconcile_unix_nanos, sizeof(s.last_reconcile_unix_nanos));
    return s;
}

// ===========================================================================
// RocksdbStore — real implementation behind KVCACHE_HAVE_ROCKSDB
// ===========================================================================

#if defined(KVCACHE_HAVE_ROCKSDB)

namespace {
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::DB;
using rocksdb::ReadOptions;
using rocksdb::Slice;
using rocksdb::Status;
using rocksdb::WriteBatch;
using rocksdb::WriteOptions;
}  // namespace

class RocksdbIteratorImpl final : public RocksdbStore::SealedChunkIterator {
   public:
    explicit RocksdbIteratorImpl(rocksdb::Iterator* it) : it_(it) {
        if (it_) it_->SeekToFirst();
    }
    ~RocksdbIteratorImpl() override { delete it_; }

    bool Valid() const override { return it_ && it_->Valid(); }
    void Next()  override        { if (it_) it_->Next(); }

    SealedChunkKey Key() const override {
        SealedChunkKey k{};
        const auto s = it_->key();
        if (s.size() == k.bytes.size()) {
            std::memcpy(k.bytes.data(), s.data(), k.bytes.size());
        }
        return k;
    }
    SealedChunkValue Value(std::string* err) const override {
        const auto s = it_->value();
        auto v = SealedChunkValue::Deserialize({s.data(), s.size()});
        if (!v) { if (err) *err = "sealed_chunks: value deserialize failed"; return {}; }
        return *v;
    }

   private:
    rocksdb::Iterator* it_;
};

std::unique_ptr<RocksdbStore> RocksdbStore::Open(const Options& opts, std::string* err) {
    auto store = std::make_unique<RocksdbStore>();
    rocksdb::Options db_opts;
    db_opts.create_if_missing                       = opts.create_if_missing;
    db_opts.create_missing_column_families          = true;
    db_opts.use_direct_reads                        = opts.use_direct_io;
    db_opts.use_direct_io_for_flush_and_compaction  = opts.use_direct_io;

    // Phase B10 — production tuning. Each shared object is stashed on
    // the store so it outlives db_ (rocksdb keeps raw pointers to them).
    if (opts.enable_statistics) {
        store->stats_ = rocksdb::CreateDBStatistics();
        db_opts.statistics = store->stats_;
    }
    if (opts.rate_limit_bytes_per_sec > 0) {
        store->rate_limiter_.reset(rocksdb::NewGenericRateLimiter(
            static_cast<int64_t>(opts.rate_limit_bytes_per_sec)));
        db_opts.rate_limiter = store->rate_limiter_;
    }
    if (opts.write_buffer_manager_bytes > 0) {
        store->wbm_ = std::make_shared<rocksdb::WriteBufferManager>(
            static_cast<size_t>(opts.write_buffer_manager_bytes));
        db_opts.write_buffer_manager = store->wbm_;
    }

    // Shared block cache → BlockBasedTableOptions applied to every CF.
    rocksdb::ColumnFamilyOptions cf_tmpl;
    if (opts.block_cache_bytes > 0) {
        store->block_cache_ = rocksdb::NewLRUCache(
            static_cast<size_t>(opts.block_cache_bytes));
        rocksdb::BlockBasedTableOptions table_opts;
        table_opts.block_cache = store->block_cache_;
        cf_tmpl.table_factory.reset(
            rocksdb::NewBlockBasedTableFactory(table_opts));
    }

    std::vector<ColumnFamilyDescriptor> cfs = {
        {std::string(kCfDefault),             cf_tmpl},
        {std::string(kCfSealedChunks),        cf_tmpl},
        {std::string(kCfTenantQuotaState),    cf_tmpl},
        {std::string(kCfAuditBufferOverflow), cf_tmpl},
    };
    std::vector<ColumnFamilyHandle*> handles;
    // rocksdb 11.x: DB::Open takes std::unique_ptr<DB>*. Open into a
    // local unique_ptr then release into the raw db_ member (the dtor
    // does the matching delete).
    std::unique_ptr<DB> db_holder;
    Status s = DB::Open(db_opts, opts.path, cfs, &handles, &db_holder);
    if (!s.ok()) { if (err) *err = s.ToString(); return nullptr; }
    store->db_ = db_holder.release();
    store->cf_default_ = handles[0];
    store->cf_sealed_  = handles[1];
    store->cf_quota_   = handles[2];
    store->cf_audit_   = handles[3];

    // Initialize schema_version + cluster_epoch on first open.
    std::string sv;
    s = store->db_->Get(ReadOptions(), store->cf_default_,
                        Slice(kKeySchemaVersion.data(), kKeySchemaVersion.size()), &sv);
    if (s.IsNotFound()) {
        WriteBatch wb;
        std::string sv_bytes(reinterpret_cast<const char*>(&kSchemaVersion),
                             sizeof(kSchemaVersion));
        wb.Put(store->cf_default_, Slice(kKeySchemaVersion.data(), kKeySchemaVersion.size()),
               sv_bytes);
        wb.Put(store->cf_default_, Slice(kKeyClusterEpoch.data(), kKeyClusterEpoch.size()),
               BeU64(0));
        s = store->db_->Write(WriteOptions(), &wb);
        if (!s.ok()) { if (err) *err = s.ToString(); return nullptr; }
        store->cached_epoch_ = 0;
    } else if (!s.ok()) {
        if (err) *err = s.ToString();
        return nullptr;
    } else {
        std::string e;
        store->db_->Get(ReadOptions(), store->cf_default_,
                        Slice(kKeyClusterEpoch.data(), kKeyClusterEpoch.size()), &e);
        store->cached_epoch_ = ParseBeU64(e);
    }
    return store;
}

RocksdbStore::~RocksdbStore() {
    if (db_) {
        for (auto* h : {cf_default_, cf_sealed_, cf_quota_, cf_audit_}) {
            if (h) db_->DestroyColumnFamilyHandle(h);
        }
        delete db_;
    }
}

bool RocksdbStore::PutSealedChunkAtomic(const SealedChunkKey& key,
                                        const SealedChunkValue& value,
                                        uint64_t new_epoch,
                                        std::string* err) {
    WriteBatch wb;
    auto val = value.Serialize();
    wb.Put(cf_sealed_,
           Slice(reinterpret_cast<const char*>(key.bytes.data()), key.bytes.size()),
           val);
    wb.Put(cf_default_,
           Slice(kKeyClusterEpoch.data(), kKeyClusterEpoch.size()),
           BeU64(new_epoch));
    Status s = db_->Write(WriteOptions(), &wb);
    if (!s.ok()) { if (err) *err = s.ToString(); return false; }
    cached_epoch_ = new_epoch;
    return true;
}

std::optional<SealedChunkValue>
RocksdbStore::GetSealedChunk(const SealedChunkKey& key, std::string* err) {
    std::string val;
    Status s = db_->Get(ReadOptions(), cf_sealed_,
                        Slice(reinterpret_cast<const char*>(key.bytes.data()),
                              key.bytes.size()),
                        &val);
    if (s.IsNotFound()) return std::nullopt;
    if (!s.ok()) { if (err) *err = s.ToString(); return std::nullopt; }
    return SealedChunkValue::Deserialize(val);
}

bool RocksdbStore::DeleteSealedChunk(const SealedChunkKey& key, std::string* err) {
    Status s = db_->Delete(WriteOptions(), cf_sealed_,
                           Slice(reinterpret_cast<const char*>(key.bytes.data()),
                                 key.bytes.size()));
    if (!s.ok()) { if (err) *err = s.ToString(); return false; }
    return true;
}

std::unique_ptr<RocksdbStore::SealedChunkIterator>
RocksdbStore::NewSealedChunkIterator() {
    return std::make_unique<RocksdbIteratorImpl>(
        db_->NewIterator(ReadOptions(), cf_sealed_));
}

bool RocksdbStore::PutQuotaState(std::span<const uint8_t, 16> tenant_id,
                                 const TenantQuotaSnapshot& snap, std::string* err) {
    auto val = snap.Serialize();
    Status s = db_->Put(WriteOptions(), cf_quota_,
                        Slice(reinterpret_cast<const char*>(tenant_id.data()), 16), val);
    if (!s.ok()) { if (err) *err = s.ToString(); return false; }
    return true;
}

std::optional<TenantQuotaSnapshot>
RocksdbStore::GetQuotaState(std::span<const uint8_t, 16> tenant_id, std::string* err) {
    std::string val;
    Status s = db_->Get(ReadOptions(), cf_quota_,
                        Slice(reinterpret_cast<const char*>(tenant_id.data()), 16), &val);
    if (s.IsNotFound()) return std::nullopt;
    if (!s.ok()) { if (err) *err = s.ToString(); return std::nullopt; }
    return TenantQuotaSnapshot::Deserialize(val);
}

bool RocksdbStore::AppendAuditOverflow(std::span<const uint8_t> event_bytes,
                                        std::string* err) {
    static std::atomic<uint64_t> seq{0};
    uint64_t s = seq.fetch_add(1, std::memory_order_relaxed);
    std::string key = BeU64(cached_epoch_) + BeU64(s);
    Status st = db_->Put(WriteOptions(), cf_audit_, key,
                         Slice(reinterpret_cast<const char*>(event_bytes.data()),
                               event_bytes.size()));
    if (!st.ok()) { if (err) *err = st.ToString(); return false; }
    return true;
}

bool RocksdbStore::PutConfig(std::string_view key, std::string_view value,
                             std::string* err) {
    Status s = db_->Put(WriteOptions(), cf_default_, Slice(key.data(), key.size()),
                        Slice(value.data(), value.size()));
    if (!s.ok()) { if (err) *err = s.ToString(); return false; }
    return true;
}

std::optional<std::string>
RocksdbStore::GetConfig(std::string_view key, std::string* err) {
    std::string val;
    Status s = db_->Get(ReadOptions(), cf_default_, Slice(key.data(), key.size()), &val);
    if (s.IsNotFound()) return std::nullopt;
    if (!s.ok()) { if (err) *err = s.ToString(); return std::nullopt; }
    return val;
}

uint64_t RocksdbStore::CurrentEpoch() const noexcept { return cached_epoch_; }

std::string RocksdbStore::StatsString() const {
    if (!stats_) return {};  // statistics were not enabled at Open
    return stats_->ToString();
}

#else  // !KVCACHE_HAVE_ROCKSDB

// Facade: every accessor errors. Lets the tree compile/link without rocksdb.

std::unique_ptr<RocksdbStore> RocksdbStore::Open(const Options&, std::string* err) {
    if (err) *err = "RocksDB not compiled in (define KVCACHE_HAVE_ROCKSDB)";
    return nullptr;
}
RocksdbStore::~RocksdbStore() = default;
bool RocksdbStore::PutSealedChunkAtomic(const SealedChunkKey&, const SealedChunkValue&,
                                        uint64_t, std::string* err) {
    if (err) *err = "RocksDB not compiled in"; return false;
}
std::optional<SealedChunkValue> RocksdbStore::GetSealedChunk(const SealedChunkKey&,
                                                              std::string*) { return std::nullopt; }
bool RocksdbStore::DeleteSealedChunk(const SealedChunkKey&, std::string*) { return false; }
std::unique_ptr<RocksdbStore::SealedChunkIterator>
RocksdbStore::NewSealedChunkIterator() { return nullptr; }
bool RocksdbStore::PutQuotaState(std::span<const uint8_t, 16>,
                                 const TenantQuotaSnapshot&, std::string*) { return false; }
std::optional<TenantQuotaSnapshot> RocksdbStore::GetQuotaState(
    std::span<const uint8_t, 16>, std::string*) { return std::nullopt; }
bool RocksdbStore::AppendAuditOverflow(std::span<const uint8_t>, std::string*) { return false; }
bool RocksdbStore::PutConfig(std::string_view, std::string_view, std::string*) { return false; }
std::optional<std::string> RocksdbStore::GetConfig(std::string_view, std::string*) { return std::nullopt; }
uint64_t RocksdbStore::CurrentEpoch() const noexcept { return cached_epoch_; }
std::string RocksdbStore::StatsString() const { return {}; }

#endif  // KVCACHE_HAVE_ROCKSDB

}  // namespace kvcache::node::meta
