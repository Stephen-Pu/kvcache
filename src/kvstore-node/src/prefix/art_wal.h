// Persistent ART with WAL-backed incremental durability (Phase D-2).
//
// Phase D-1 shipped whole-tree snapshot/restore: every mutator was
// in-memory only, and durability waited for the next `Checkpoint`. On a
// node carrying tens of millions of sealed chunks, a crash between
// checkpoints would lose hours of inserts.
//
// D-2 wraps the ART in a write-ahead-log facade. Every Insert / Remove
// is appended to a single append-only WAL file and fsynced before the
// in-memory ART is mutated. Boot is now O(WAL tail) instead of O(N
// chunks), because the snapshot covers the cold mass and the WAL only
// needs to replay records taken since the most recent snapshot.
//
// On-disk format (little-endian, packed; one record per mutation):
//
//   record_len  u32   total bytes of this record, INCLUDING header
//   op          u8    1 = INSERT, 2 = REMOVE
//   path_len    u8    chunk count (1..255)
//   path        u8[8 * path_len]
//   [INSERT only] LeafData payload (same encoding as ArtSnapshot's
//                                    per-leaf serialisation):
//     locator         u8[64]
//     tier_residency  u32
//     refcount        u32
//     sealed_at_nanos u64
//     last_access     u64
//     bytes_total     u64
//   crc32       u32   IEEE polynomial over [op .. last byte before crc]
//
// A torn write (partial fsync before crash) is detected at boot via the
// length prefix + CRC mismatch and silently truncated — the in-memory
// ART simply reflects every fully-committed record up to that point.
//
// Checkpoint atomicity: ArtSnapshot::Write writes a tmp file and
// renames it into place; then we truncate the WAL to zero. A crash
// between rename and truncate leaves the old WAL on disk, but since
// Insert is idempotent (same path → kReplaced) and Remove of a
// non-existent key is a no-op, replay is safe under any crash window.
#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>

#include "prefix/art_index.h"

namespace kvcache::node::prefix {

class ArtWal {
   public:
    // Open a persistent ART. `snapshot_path` and `wal_path` may both be
    // empty (no persistence — equivalent to a bare ArtIndex). If
    // snapshot_path exists it's loaded first; then any records in
    // wal_path are replayed on top. Missing-file is fine; only a
    // CRC-mismatched record (or unparseable file) sets `*err`, and
    // even then the wrapper returns a usable ArtWal that reflects
    // every record up to the first torn write.
    struct Options {
        std::string snapshot_path;
        std::string wal_path;
        // When true, every Insert/Remove fsync's the WAL before
        // mutating the in-memory ART (the safe production default).
        // Tests that don't care about crash durability can flip this
        // off to speed up high-volume insert loops by ~10×.
        bool fsync_each_write = true;
    };

    static std::unique_ptr<ArtWal> Open(const Options& opts, std::string* err);

    ~ArtWal();

    ArtWal(const ArtWal&)            = delete;
    ArtWal& operator=(const ArtWal&) = delete;

    // ---- writer API (mirrors ArtIndex) ----------------------------------

    ArtIndex::InsertResult Insert(std::span<const ChunkHash> path,
                                    std::unique_ptr<LeafData> leaf,
                                    std::string* err = nullptr);

    bool Remove(std::span<const ChunkHash> path,
                 std::string* err = nullptr);

    // ---- reader API: forward to the wrapped ArtIndex -------------------

    ArtIndex&       art()       noexcept { return *art_; }
    const ArtIndex& art() const noexcept { return *art_; }

    // Take a fresh whole-tree snapshot at `new_snapshot_path`, then
    // truncate the WAL. Atomic vs concurrent mutators (acquires the
    // same writer mutex as Insert/Remove).
    //
    // `new_snapshot_path` may equal `opts.snapshot_path` to overwrite
    // the existing snapshot — the rename is the publish boundary.
    bool Checkpoint(const std::string& new_snapshot_path,
                     std::string* err = nullptr);

    // Number of records replayed at Open time. Useful for diagnostics
    // and for tests that want to assert a snapshot/WAL split.
    std::size_t records_replayed() const noexcept { return records_replayed_; }

    // Number of records currently in the WAL on disk (i.e. mutations
    // since the most recent Checkpoint). Tests use this to verify
    // truncation.
    std::size_t wal_record_count() const noexcept;

   private:
    ArtWal() = default;

    bool AppendInsertLocked(std::span<const ChunkHash> path,
                              const LeafData& leaf, std::string* err);
    bool AppendRemoveLocked(std::span<const ChunkHash> path, std::string* err);

    Options                   opts_;
    std::unique_ptr<ArtIndex> art_;
    mutable std::mutex        mu_;          // serialises mutators + Checkpoint
    int                       wal_fd_ = -1;
    std::size_t               records_replayed_ = 0;
    std::size_t               wal_records_      = 0;
};

}  // namespace kvcache::node::prefix
