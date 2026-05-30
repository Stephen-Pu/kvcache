// ArtWal — Phase D-2 unit tests.
//
// Coverage:
//   1. Empty WAL → empty ART; reopen-after-close is a no-op.
//   2. Insert N → close → reopen → ART has N leaves, looked up via the
//      same paths.
//   3. Snapshot + WAL replay: Insert M → Checkpoint → Insert K → close
//      → reopen → ART has M+K leaves (M from snapshot, K from WAL).
//   4. Torn-write recovery: corrupt the last WAL byte → reopen → ART
//      reflects every record up to but not including the torn one,
//      with no crash.
//   5. Checkpoint truncates the WAL on disk.
//   6. Concurrent insertion under one writer is correct (mutex
//      ordering: WAL append before ART mutate).
//   7. fsync_each_write=false short-circuits but still produces a
//      replay-able WAL.
#include "prefix/art_wal.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>

#include "log_sink.h"
#include <vector>

namespace fs = std::filesystem;
using namespace kvcache::node::prefix;

namespace {

// Tempdir per test → each test gets its own snapshot + WAL pair so
// parallel ctest runs (and reruns within the same binary) don't
// collide.
struct TempPair {
    std::string root;
    std::string snap;
    std::string wal;

    TempPair() {
        root = (fs::temp_directory_path() /
                  ("kvcache_artwal_" + std::to_string(::getpid()) + "_" +
                   std::to_string(reinterpret_cast<uintptr_t>(this)))).string();
        fs::create_directories(root);
        snap = root + "/snapshot.bin";
        wal  = root + "/wal.bin";
    }
    ~TempPair() {
        fs::remove_all(root);
    }
};

ChunkHash MakeHash(uint8_t b0, uint8_t b1 = 0, uint8_t b2 = 0,
                    uint8_t b3 = 0, uint8_t b4 = 0, uint8_t b5 = 0,
                    uint8_t b6 = 0, uint8_t b7 = 0) {
    return {b0, b1, b2, b3, b4, b5, b6, b7};
}

std::unique_ptr<LeafData> MakeLeaf(uint64_t seed) {
    auto d = std::make_unique<LeafData>();
    auto* raw = reinterpret_cast<uint8_t*>(&d->locator);
    for (std::size_t i = 0; i < sizeof(d->locator); ++i) {
        raw[i] = static_cast<uint8_t>((seed * 31 + i) & 0xff);
    }
    d->tier_residency_bitmap = static_cast<uint32_t>(0x1 | (seed << 1));
    d->sealed_at_nanos       = seed * 1'000'000;
    d->last_access_nanos.store(seed * 999, std::memory_order_relaxed);
    d->bytes_total           = seed * 4096;
    d->refcount.Reset(static_cast<uint32_t>(seed % 7));
    return d;
}

// Two-chunk path. All paths sharing the same `kind` route through the
// same root Inner256 (h0 is identical) and differentiate at the leaf
// slot via `idx` in h1[0]. The MVP ART (Node256-only, terminal leaves)
// rejects sibling Inner256s with mismatched edge tails, so we keep
// h0[1..7] fixed inside a single test.
std::vector<ChunkHash> PathFor(uint8_t kind, uint8_t idx) {
    return {MakeHash(0xC0 + kind, 1, 2, 3, 4, 5, 6, 7),
              MakeHash(idx,         9, 8, 7, 6, 5, 4, 3)};
}

}  // namespace

TEST(ArtWalTest, EmptyOpenAndReopenIsNoop) {
    TempPair t;
    ArtWal::Options o{t.snap, t.wal, true};
    std::string err;
    auto w = ArtWal::Open(o, &err);
    ASSERT_NE(w, nullptr);
    EXPECT_EQ(w->art().LeafCount(), 0u);
    EXPECT_EQ(w->records_replayed(), 0u);
    w.reset();

    auto w2 = ArtWal::Open(o, &err);
    ASSERT_NE(w2, nullptr);
    EXPECT_EQ(w2->art().LeafCount(), 0u);
}

TEST(ArtWalTest, InsertThenReopenRecoversEveryLeaf) {
    TempPair t;
    ArtWal::Options o{t.snap, t.wal, true};
    std::string err;
    auto w = ArtWal::Open(o, &err);
    ASSERT_NE(w, nullptr);
    constexpr int kN = 20;
    std::vector<std::vector<ChunkHash>> paths;
    for (int i = 0; i < kN; ++i) {
        paths.push_back(PathFor(/*kind=*/1, static_cast<uint8_t>(i)));
        ASSERT_EQ(w->Insert(paths.back(), MakeLeaf(uint64_t(i + 1))),
                    ArtIndex::InsertResult::kInserted);
    }
    EXPECT_EQ(w->art().LeafCount(),     static_cast<std::size_t>(kN));
    EXPECT_EQ(w->wal_record_count(),    static_cast<std::size_t>(kN));
    w.reset();

    auto w2 = ArtWal::Open(o, &err);
    ASSERT_NE(w2, nullptr) << err;
    EXPECT_EQ(w2->art().LeafCount(),    static_cast<std::size_t>(kN));
    EXPECT_EQ(w2->records_replayed(),   static_cast<std::size_t>(kN));
    auto g = w2->art().EnterRead();
    for (const auto& p : paths) {
        auto r = w2->art().Lookup(p, g);
        EXPECT_EQ(r.matched_chunks, p.size());
        EXPECT_NE(r.leaf, nullptr);
    }
}

TEST(ArtWalTest, SnapshotPlusWalReplayCoversBoth) {
    TempPair t;
    ArtWal::Options o{t.snap, t.wal, true};
    std::string err;
    auto w = ArtWal::Open(o, &err);
    ASSERT_NE(w, nullptr);

    constexpr int kM = 8, kK = 5;
    std::vector<std::vector<ChunkHash>> snap_paths, wal_paths;
    for (int i = 0; i < kM; ++i) {
        snap_paths.push_back(PathFor(/*kind=*/2, static_cast<uint8_t>(i)));
        ASSERT_EQ(w->Insert(snap_paths.back(), MakeLeaf(uint64_t(i + 100))),
                    ArtIndex::InsertResult::kInserted);
    }
    ASSERT_TRUE(w->Checkpoint(t.snap, &err)) << err;
    EXPECT_EQ(w->wal_record_count(), 0u);

    for (int i = 0; i < kK; ++i) {
        wal_paths.push_back(PathFor(/*kind=*/3, static_cast<uint8_t>(i)));
        ASSERT_EQ(w->Insert(wal_paths.back(), MakeLeaf(uint64_t(i + 200))),
                    ArtIndex::InsertResult::kInserted);
    }
    EXPECT_EQ(w->wal_record_count(), static_cast<std::size_t>(kK));
    w.reset();

    auto w2 = ArtWal::Open(o, &err);
    ASSERT_NE(w2, nullptr) << err;
    EXPECT_EQ(w2->art().LeafCount(), static_cast<std::size_t>(kM + kK));
    EXPECT_EQ(w2->records_replayed(), static_cast<std::size_t>(kK));
    auto g = w2->art().EnterRead();
    for (const auto& p : snap_paths) {
        EXPECT_EQ(w2->art().Lookup(p, g).matched_chunks, p.size());
    }
    for (const auto& p : wal_paths) {
        EXPECT_EQ(w2->art().Lookup(p, g).matched_chunks, p.size());
    }
}

TEST(ArtWalTest, TornLastRecordIsSkippedSilently) {
    TempPair t;
    ArtWal::Options o{t.snap, t.wal, true};
    std::string err;
    auto w = ArtWal::Open(o, &err);
    ASSERT_NE(w, nullptr);
    constexpr int kN = 5;
    std::vector<std::vector<ChunkHash>> paths;
    for (int i = 0; i < kN; ++i) {
        paths.push_back(PathFor(/*kind=*/4, static_cast<uint8_t>(i)));
        ASSERT_EQ(w->Insert(paths.back(), MakeLeaf(uint64_t(i + 300))),
                    ArtIndex::InsertResult::kInserted);
    }
    w.reset();

    // Append garbage onto the end of the WAL — simulates a torn write.
    {
        FILE* f = std::fopen(t.wal.c_str(), "ab");
        ASSERT_NE(f, nullptr);
        const char garbage[] = "\xAA\xAA\xAA\xAA\xAA";
        std::fwrite(garbage, 1, sizeof(garbage), f);
        std::fclose(f);
    }

    auto w2 = ArtWal::Open(o, &err);
    ASSERT_NE(w2, nullptr) << err;
    // Every committed record made it; only the trailing garbage was
    // skipped (and the file truncated back to a clean boundary).
    EXPECT_EQ(w2->art().LeafCount(),  static_cast<std::size_t>(kN));
    EXPECT_EQ(w2->records_replayed(), static_cast<std::size_t>(kN));
}

TEST(ArtWalTest, CheckpointTruncatesWalOnDisk) {
    TempPair t;
    ArtWal::Options o{t.snap, t.wal, true};
    std::string err;
    auto w = ArtWal::Open(o, &err);
    ASSERT_NE(w, nullptr);
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(w->Insert(PathFor(/*kind=*/5, static_cast<uint8_t>(i)),
                              MakeLeaf(uint64_t(i + 400))),
                    ArtIndex::InsertResult::kInserted);
    }
    const auto before = fs::file_size(t.wal);
    EXPECT_GT(before, 0u);
    ASSERT_TRUE(w->Checkpoint(t.snap, &err)) << err;
    EXPECT_EQ(fs::file_size(t.wal), 0u);
    EXPECT_EQ(w->wal_record_count(), 0u);
}

TEST(ArtWalTest, ConcurrentInsertsAreSerialisedSafely) {
    TempPair t;
    ArtWal::Options o{t.snap, t.wal, true};
    std::string err;
    auto w = ArtWal::Open(o, &err);
    ASSERT_NE(w, nullptr);

    constexpr int kThreads = 4;
    constexpr int kPerThread = 25;
    std::vector<std::thread> ts;
    std::atomic<int> ok{0};
    for (int tid = 0; tid < kThreads; ++tid) {
        ts.emplace_back([&, tid] {
            for (int i = 0; i < kPerThread; ++i) {
                auto path = PathFor(/*kind=*/static_cast<uint8_t>(0x10 + tid),
                                       static_cast<uint8_t>(i));
                if (w->Insert(path, MakeLeaf(uint64_t(tid * 1000 + i))) ==
                    ArtIndex::InsertResult::kInserted) {
                    ++ok;
                }
            }
        });
    }
    for (auto& th : ts) th.join();
    EXPECT_EQ(ok.load(), kThreads * kPerThread);
    EXPECT_EQ(w->wal_record_count(),
                static_cast<std::size_t>(kThreads * kPerThread));
    EXPECT_EQ(w->art().LeafCount(),
                static_cast<std::size_t>(kThreads * kPerThread));
}

TEST(ArtWalTest, NoFsyncStillProducesReplayableWal) {
    TempPair t;
    ArtWal::Options o{t.snap, t.wal, /*fsync_each_write=*/false};
    std::string err;
    auto w = ArtWal::Open(o, &err);
    ASSERT_NE(w, nullptr);
    for (int i = 0; i < 7; ++i) {
        ASSERT_EQ(w->Insert(PathFor(/*kind=*/6, static_cast<uint8_t>(i)),
                              MakeLeaf(uint64_t(i + 500))),
                    ArtIndex::InsertResult::kInserted);
    }
    w.reset();
    auto w2 = ArtWal::Open(o, &err);
    ASSERT_NE(w2, nullptr) << err;
    EXPECT_EQ(w2->records_replayed(), 7u);
}

TEST(ArtWalTest, NoWalPathIsTransparentPassthrough) {
    // Construct without snapshot or WAL — every mutation goes straight
    // to the in-memory ArtIndex with no I/O.
    ArtWal::Options o{};
    std::string err;
    auto w = ArtWal::Open(o, &err);
    ASSERT_NE(w, nullptr);
    auto path = PathFor(/*kind=*/7, 1);
    ASSERT_EQ(w->Insert(path, MakeLeaf(42)),
                ArtIndex::InsertResult::kInserted);
    EXPECT_EQ(w->wal_record_count(), 0u);
    EXPECT_EQ(w->art().LeafCount(),   1u);
}

// Phase O-2.2: torn-tail recovery is no longer silent — Warn fires on the
// "art_wal" subsystem so the operator can correlate boot-time WAL
// truncation with the prior unclean shutdown.
namespace {
class CapturingSink : public kvcache::log::sink::Logger {
   public:
    struct Rec { kvcache::log::sink::LogLevel level; std::string msg; };
    void Log(kvcache::log::sink::LogLevel level, const char*, int,
              const std::string& msg) override {
        std::lock_guard lk(mu_);
        recs_.push_back({level, msg});
    }
    std::vector<Rec> snapshot() {
        std::lock_guard lk(mu_); return recs_;
    }
   private:
    std::mutex mu_;
    std::vector<Rec> recs_;
};
}  // namespace

TEST(ArtWalTest, TornTailLogsWarnViaFacade) {
    auto cap = std::make_shared<CapturingSink>();
    kvcache::log::sink::SetDefault(cap);

    TempPair t;
    ArtWal::Options o{t.snap, t.wal, true};
    std::string err;
    auto w = ArtWal::Open(o, &err);
    ASSERT_NE(w, nullptr);
    ASSERT_EQ(w->Insert(PathFor(4, 1), MakeLeaf(42)),
                ArtIndex::InsertResult::kInserted);
    w.reset();
    {
        FILE* f = std::fopen(t.wal.c_str(), "ab");
        ASSERT_NE(f, nullptr);
        const char garbage[] = "\xAA\xAA\xAA\xAA\xAA";
        std::fwrite(garbage, 1, sizeof(garbage), f);
        std::fclose(f);
    }
    auto w2 = ArtWal::Open(o, &err);
    ASSERT_NE(w2, nullptr);

    auto recs = cap->snapshot();
    bool found = false;
    for (const auto& r : recs) {
        if (r.level == kvcache::log::sink::LogLevel::kWarn &&
            r.msg.find("[art_wal]") != std::string::npos &&
            r.msg.find("torn tail") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "expected an [art_wal] torn-tail Warn";
}

TEST(ArtWalTest, CorruptSnapshotLogsWarnAndContinuesWithEmptyArt) {
    auto cap = std::make_shared<CapturingSink>();
    kvcache::log::sink::SetDefault(cap);

    TempPair t;
    // Write a garbage snapshot file so Read() fails parse-side.
    {
        FILE* f = std::fopen(t.snap.c_str(), "wb");
        ASSERT_NE(f, nullptr);
        const char junk[] = "not a valid snapshot header";
        std::fwrite(junk, 1, sizeof(junk), f);
        std::fclose(f);
    }
    ArtWal::Options o{t.snap, t.wal, true};
    std::string err;
    auto w = ArtWal::Open(o, &err);
    ASSERT_NE(w, nullptr) << err;
    // Empty ART — boot fell back to fresh.
    EXPECT_EQ(w->art().LeafCount(), 0u);

    auto recs = cap->snapshot();
    bool found = false;
    for (const auto& r : recs) {
        if (r.level == kvcache::log::sink::LogLevel::kWarn &&
            r.msg.find("[art_wal]") != std::string::npos &&
            r.msg.find("snapshot ignored") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "expected an [art_wal] snapshot-ignored Warn";
}
