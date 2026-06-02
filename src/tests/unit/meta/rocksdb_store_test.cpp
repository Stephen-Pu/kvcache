// rocksdb_store_test.cpp
//
// Exercises serialization and the KVCACHE_HAVE_ROCKSDB facade. Full RocksDB
// integration tests (open / write / replay) require linking against rocksdb;
// they are gated on the same compile flag and skipped otherwise.
#include "meta/rocksdb_store.h"

#include <gtest/gtest.h>

using kvcache::node::meta::RocksdbStore;
using kvcache::node::meta::SealedChunkKey;
using kvcache::node::meta::SealedChunkValue;
using kvcache::node::meta::TenantQuotaSnapshot;
using kvcache::node::meta::kSchemaVersion;

TEST(SealedChunkKeyTest, FromLocatorIsDeterministic) {
    kv_locator_t loc{};
    for (int i = 0; i < 16; ++i) loc.tenant_id[i]  = static_cast<uint8_t>(i);
    for (int i = 0; i < 16; ++i) loc.prefix_hash[i] = static_cast<uint8_t>(0xA0 + i);
    loc.model_id_hash = 0x0123456789ABCDEFull;

    auto k1 = SealedChunkKey::From(loc);
    auto k2 = SealedChunkKey::From(loc);
    EXPECT_EQ(k1.bytes, k2.bytes);
    // model_id_hash encoded big-endian starting at offset 16.
    EXPECT_EQ(k1.bytes[16], 0x01u);
    EXPECT_EQ(k1.bytes[23], 0xEFu);
}

TEST(SealedChunkValueTest, RoundTrip) {
    SealedChunkValue v{};
    v.version               = kSchemaVersion;
    v.sealed_at_nanos       = 1700000000000000000ull;
    v.bytes_total           = 4096;
    v.tier_residency_bitmap = 0b00110;  // pinned + dram
    v.range_covered         = {0, 80, 0, 64, 0, 16};
    v.opaque_meta           = {1, 2, 3, 4};

    auto s = v.Serialize();
    auto d = SealedChunkValue::Deserialize(s);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->bytes_total, v.bytes_total);
    EXPECT_EQ(d->tier_residency_bitmap, v.tier_residency_bitmap);
    EXPECT_EQ(d->opaque_meta, v.opaque_meta);
}

TEST(SealedChunkValueTest, RejectsWrongVersion) {
    SealedChunkValue v{};
    v.version = 999;
    auto s = v.Serialize();
    auto d = SealedChunkValue::Deserialize(s);
    EXPECT_FALSE(d.has_value());
}

TEST(TenantQuotaSnapshotTest, RoundTrip) {
    TenantQuotaSnapshot q{
        .capacity_used_bytes       = 100ull << 30,
        .qps_window_count          = 1234,
        .bandwidth_window_bytes    = 500ull << 20,
        .last_reconcile_unix_nanos = 1700000000000000000ull,
    };
    auto s = q.Serialize();
    auto d = TenantQuotaSnapshot::Deserialize(s);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->capacity_used_bytes,       q.capacity_used_bytes);
    EXPECT_EQ(d->qps_window_count,          q.qps_window_count);
    EXPECT_EQ(d->bandwidth_window_bytes,    q.bandwidth_window_bytes);
    EXPECT_EQ(d->last_reconcile_unix_nanos, q.last_reconcile_unix_nanos);
}

#if defined(KVCACHE_HAVE_ROCKSDB)

#include <filesystem>

TEST(RocksdbStoreTest, OpenAndPutSealedChunkRoundTrip) {
    auto path = std::filesystem::temp_directory_path() / "kvcache_rocks_test";
    std::filesystem::remove_all(path);

    RocksdbStore::Options opts;
    opts.path = path.string();

    std::string err;
    auto store = RocksdbStore::Open(opts, &err);
    ASSERT_NE(store, nullptr) << err;
    EXPECT_EQ(store->CurrentEpoch(), 0u);

    kv_locator_t loc{};
    loc.model_id_hash = 1;
    SealedChunkValue v{};
    v.version = kSchemaVersion;
    v.bytes_total = 1024;

    ASSERT_TRUE(store->PutSealedChunkAtomic(SealedChunkKey::From(loc), v, 1, &err)) << err;
    EXPECT_EQ(store->CurrentEpoch(), 1u);

    auto got = store->GetSealedChunk(SealedChunkKey::From(loc), &err);
    ASSERT_TRUE(got.has_value()) << err;
    EXPECT_EQ(got->bytes_total, 1024u);
}

// Phase B10 — open with the full production tuning surface and verify
// (a) it still round-trips, (b) the Statistics object is live + dumps
// after writes. Exercises the rate-limiter / WBM / block-cache / stats
// code paths that were unreachable before B10.
TEST(RocksdbStoreTest, ProductionTuningOpensAndStatsDump) {
    auto path = std::filesystem::temp_directory_path() / "kvcache_rocks_tuned";
    std::filesystem::remove_all(path);

    RocksdbStore::Options opts;
    opts.path                       = path.string();
    opts.block_cache_bytes          = 8ull << 20;    // 8 MiB
    opts.rate_limit_bytes_per_sec   = 16ull << 20;   // 16 MiB/s
    opts.write_buffer_manager_bytes = 32ull << 20;   // 32 MiB total memtable
    opts.enable_statistics          = true;

    std::string err;
    auto store = RocksdbStore::Open(opts, &err);
    ASSERT_NE(store, nullptr) << err;

    // A few writes so the statistics counters move off zero.
    for (uint64_t i = 1; i <= 8; ++i) {
        kv_locator_t loc{};
        loc.model_id_hash = i;
        SealedChunkValue v{};
        v.version = kSchemaVersion;
        v.bytes_total = i * 256;
        ASSERT_TRUE(store->PutSealedChunkAtomic(SealedChunkKey::From(loc), v, i, &err)) << err;
    }
    EXPECT_EQ(store->CurrentEpoch(), 8u);

    // StatsString must be non-empty and look like a rocksdb dump.
    const std::string stats = store->StatsString();
    EXPECT_FALSE(stats.empty());
    // rocksdb's ToString() always contains ticker lines like
    // "rocksdb.block.cache." — assert one well-known token is present.
    EXPECT_NE(stats.find("rocksdb."), std::string::npos)
        << "stats dump didn't look like rocksdb output";
}

TEST(RocksdbStoreTest, StatsStringEmptyWhenDisabled) {
    auto path = std::filesystem::temp_directory_path() / "kvcache_rocks_nostats";
    std::filesystem::remove_all(path);
    RocksdbStore::Options opts;
    opts.path              = path.string();
    opts.enable_statistics = false;
    std::string err;
    auto store = RocksdbStore::Open(opts, &err);
    ASSERT_NE(store, nullptr) << err;
    EXPECT_TRUE(store->StatsString().empty());
}

#else

TEST(RocksdbStoreFacadeTest, OpenReturnsErrorWithoutRocksdb) {
    RocksdbStore::Options opts;
    opts.path = "/tmp/should-not-be-created";
    std::string err;
    auto s = RocksdbStore::Open(opts, &err);
    EXPECT_EQ(s, nullptr);
    EXPECT_FALSE(err.empty());
}

#endif
