// LLD §3.2 / §7.3 — Persistent ART snapshot.
//
// At node shutdown (or on a periodic timer), serialize the in-memory ART to
// a single on-disk file. On boot, restore the ART by reading that file
// directly — much faster than the legacy "scan the sealed_chunks column
// family in RocksDB and re-Insert every leaf" rebuild.
//
// Phase D-1 scope (this file):
//   * Whole-tree serialization to a single file. Write-temp + fsync + rename
//     for atomic publication.
//   * BLAKE3-256 integrity over the body so a torn or bit-rotted file is
//     rejected cleanly rather than silently mis-loading.
//   * No WAL, no incremental updates, no mmap. The expectation is that the
//     caller writes a fresh snapshot on demand; if the process crashes
//     between writes, the boot path falls back to the RocksDB rebuild.
//
// Phase D-2+ (not in this file): WAL of sealed/unsealed events between
// snapshots, persistent arena with mmap'd nodes, copy-on-write.
//
// On-disk format (all little-endian, packed):
//
//   Header (72 bytes):
//     magic           u8[4]   "PART"
//     version         u32     = kSnapshotVersion (currently 1)
//     flags           u32     reserved, must be 0
//     reserved        u32     reserved, must be 0
//     leaf_count      u64     number of leaves serialized
//     node_count      u64     number of inner nodes serialized
//     body_bytes      u64     size of the body payload
//     body_blake3     u8[32]  BLAKE3-256 over the body
//
//   Body: recursive descent starting at the root (which is always an
//   ArtInner256 with edge_tail_valid=false).
//
//     Inner256 record:
//       tag             u8      = 0
//       edge_valid      u8      0 or 1
//       edge_tail       u8[7]
//       child_count     u16     number of non-null children (0..256)
//       repeated child_count times:
//         slot          u8      which of 256 slots
//         [recursive child record]
//
//     Leaf record:
//       tag             u8      = 1
//       edge_valid      u8
//       edge_tail       u8[7]
//       locator         u8[64]  kv_locator_t POD
//       tier_residency  u32
//       refcount        u32     snapshotted Refcount value
//       sealed_at_nanos u64
//       last_access     u64
//       bytes_total     u64
#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace kvcache::node::prefix {

class ArtIndex;

// Phase D-5 — bumped from 2 to 3. v3 adds an ``embedded_leaf`` block
// on every Inner256 (has_embedded byte + LeafData payload when present),
// so an Inner can carry "stop-here" data alongside its children. v1/v2
// snapshots are incompatible — load fails with ``unsupported version``.
// The WAL is rebuildable from current state, so operators upgrade by
// checkpointing fresh and discarding old files.
inline constexpr uint32_t kSnapshotVersion = 3;
inline constexpr uint8_t  kSnapshotMagic[4] = {'P', 'A', 'R', 'T'};
inline constexpr std::size_t kSnapshotHeaderBytes = 72;

class ArtSnapshot {
   public:
    struct WriteStats {
        uint64_t bytes_written = 0;
        uint64_t leaves        = 0;
        uint64_t inner_nodes   = 0;
    };

    // Serialize `art` to `path` atomically (writes to path+".tmp", fsyncs,
    // renames into place). Holds the ART's writer mutex for the walk —
    // concurrent readers continue lock-free; concurrent writers wait.
    static bool Write(const ArtIndex& art, const std::string& path,
                      WriteStats* stats, std::string* err);

    struct ReadStats {
        uint64_t leaves      = 0;
        uint64_t inner_nodes = 0;
    };

    // Read a snapshot from `path` and return a freshly-constructed ArtIndex
    // populated from it. Verifies magic, version, and BLAKE3-256 body
    // checksum before touching memory; on any failure returns nullptr with
    // *err set.
    static std::unique_ptr<ArtIndex> Read(const std::string& path,
                                           ReadStats* stats,
                                           std::string* err);
};

}  // namespace kvcache::node::prefix
