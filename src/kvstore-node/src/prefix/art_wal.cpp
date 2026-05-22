// Persistent ART with WAL-backed incremental durability — implementation.
//
// See art_wal.h for the on-disk format. The implementation is small by
// design: one mutex serialises mutators (matches ArtIndex's own writer
// discipline); one fd holds the WAL; record framing is a length-prefix
// + CRC32 so a torn write is recoverable at boot.
#include "prefix/art_wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <utility>
#include <vector>

#include "prefix/art_snapshot.h"

namespace kvcache::node::prefix {

namespace {

constexpr uint8_t kOpInsert = 1;
constexpr uint8_t kOpRemove = 2;

// Per-leaf wire payload: 64-byte locator + 4 + 4 + 8 + 8 + 8 = 96 bytes.
// Matches the encoding ArtSnapshot uses, so a future "merge WAL into
// snapshot in-place" optimisation can reuse the same parser.
constexpr std::size_t kLeafPayloadBytes = 64 + 4 + 4 + 8 + 8 + 8;

// ---- byte append helpers (little-endian) --------------------------------

void PutU8 (std::vector<uint8_t>& b, uint8_t v)  { b.push_back(v); }
void PutU16(std::vector<uint8_t>& b, uint16_t v) {
    for (int i = 0; i < 2; ++i) b.push_back(static_cast<uint8_t>(v >> (i * 8)));
}
void PutU32(std::vector<uint8_t>& b, uint32_t v) {
    for (int i = 0; i < 4; ++i) b.push_back(static_cast<uint8_t>(v >> (i * 8)));
}
void PutU64(std::vector<uint8_t>& b, uint64_t v) {
    for (int i = 0; i < 8; ++i) b.push_back(static_cast<uint8_t>(v >> (i * 8)));
}
void PutBytes(std::vector<uint8_t>& b, const void* data, std::size_t n) {
    const auto* p = static_cast<const uint8_t*>(data);
    b.insert(b.end(), p, p + n);
}

// ---- CRC32 (IEEE 802.3 polynomial, software fallback) -------------------

uint32_t Crc32(const uint8_t* p, std::size_t n) {
    static uint32_t table[256];
    static bool inited = false;
    if (!inited) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c >> 1) ^ (0xEDB88320u & -(c & 1));
            table[i] = c;
        }
        inited = true;
    }
    uint32_t c = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < n; ++i) {
        c = table[(c ^ p[i]) & 0xFF] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

// ---- record builders -----------------------------------------------------

void EncodeLeaf(std::vector<uint8_t>& out, const LeafData& leaf) {
    PutBytes(out, &leaf.locator, sizeof(leaf.locator));
    PutU32(out, leaf.tier_residency_bitmap);
    PutU32(out, leaf.refcount.Load());
    PutU64(out, leaf.sealed_at_nanos);
    PutU64(out, leaf.last_access_nanos.load(std::memory_order_acquire));
    PutU64(out, leaf.bytes_total);
}

std::vector<uint8_t> EncodeRecord(uint8_t op,
                                    std::span<const ChunkHash> path,
                                    const LeafData* leaf) {
    // Body = op(1) + path_len(1) + path(8*N) + [leaf(96)?] + crc(4)
    const std::size_t body_no_crc =
        1 + 1 + 8 * path.size() + (op == kOpInsert ? kLeafPayloadBytes : 0);
    const std::size_t total = 4 + body_no_crc + 4;
    std::vector<uint8_t> rec;
    rec.reserve(total);
    PutU32(rec, static_cast<uint32_t>(total));
    PutU8 (rec, op);
    PutU8 (rec, static_cast<uint8_t>(path.size()));
    for (const auto& h : path) PutBytes(rec, h.data(), h.size());
    if (op == kOpInsert) {
        // INSERT records carry the leaf payload.
        EncodeLeaf(rec, *leaf);
    }
    // CRC covers everything written so far EXCEPT the 4-byte length prefix.
    const uint32_t crc = Crc32(rec.data() + 4, rec.size() - 4);
    PutU32(rec, crc);
    return rec;
}

// ---- record reader -------------------------------------------------------

// Decode one record from `p`/`end`. On success, `*cursor` advances past
// it. On corrupt-or-torn, returns false and *cursor is unchanged (caller
// truncates the WAL there). LeafData is filled when op==kOpInsert.
struct Record {
    uint8_t                op = 0;
    std::vector<ChunkHash> path;
    LeafData               leaf;  // valid only when op == kOpInsert
};

bool ParseLeafBytes(const uint8_t* p, std::size_t n, LeafData* leaf) {
    if (n < kLeafPayloadBytes) return false;
    std::memcpy(&leaf->locator, p, sizeof(leaf->locator));
    p += sizeof(leaf->locator);
    auto load32 = [&](uint32_t* dst) {
        *dst = uint32_t(p[0]) | (uint32_t(p[1]) << 8) |
               (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
        p += 4;
    };
    auto load64 = [&](uint64_t* dst) {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= uint64_t(p[i]) << (i * 8);
        *dst = v;
        p += 8;
    };
    uint32_t refc = 0;
    load32(&leaf->tier_residency_bitmap);
    load32(&refc);
    load64(&leaf->sealed_at_nanos);
    uint64_t last_access = 0;
    load64(&last_access);
    leaf->last_access_nanos.store(last_access, std::memory_order_relaxed);
    load64(&leaf->bytes_total);
    leaf->refcount.Reset(refc);
    return true;
}

bool DecodeRecord(const uint8_t*& cursor, const uint8_t* end, Record* out) {
    if (end - cursor < 4) return false;
    uint32_t total = uint32_t(cursor[0]) | (uint32_t(cursor[1]) << 8) |
                       (uint32_t(cursor[2]) << 16) | (uint32_t(cursor[3]) << 24);
    if (total < 4 + 1 + 1 + 4 || static_cast<std::size_t>(end - cursor) < total) {
        return false;
    }
    const uint8_t* body = cursor + 4;
    const std::size_t body_len = total - 4 - 4;
    const uint32_t got_crc =
        uint32_t(body[body_len])       | (uint32_t(body[body_len + 1]) << 8) |
        (uint32_t(body[body_len + 2]) << 16) |
        (uint32_t(body[body_len + 3]) << 24);
    const uint32_t want_crc = Crc32(body, body_len);
    if (got_crc != want_crc) return false;

    const uint8_t  op       = body[0];
    const uint8_t  path_len = body[1];
    if (op != kOpInsert && op != kOpRemove) return false;
    if (path_len == 0) return false;
    const std::size_t needed = 2 + 8 * std::size_t(path_len) +
                                  (op == kOpInsert ? kLeafPayloadBytes : 0);
    if (body_len != needed) return false;

    out->op = op;
    out->path.resize(path_len);
    const uint8_t* p = body + 2;
    for (std::size_t i = 0; i < path_len; ++i) {
        std::memcpy(out->path[i].data(), p, 8);
        p += 8;
    }
    if (op == kOpInsert) {
        if (!ParseLeafBytes(p, kLeafPayloadBytes, &out->leaf)) return false;
    }
    cursor += total;
    return true;
}

}  // namespace

// ---- ArtWal --------------------------------------------------------------

std::unique_ptr<ArtWal> ArtWal::Open(const Options& opts, std::string* err) {
    auto self  = std::unique_ptr<ArtWal>(new ArtWal());
    self->opts_ = opts;

    // 1. Snapshot — restore or build fresh.
    if (!opts.snapshot_path.empty()) {
        struct ::stat st{};
        if (::stat(opts.snapshot_path.c_str(), &st) == 0) {
            std::string snap_err;
            auto restored = ArtSnapshot::Read(opts.snapshot_path, nullptr,
                                                &snap_err);
            if (restored) {
                self->art_ = std::move(restored);
            } else if (err) {
                *err = "snapshot ignored: " + snap_err;  // non-fatal
            }
        }
    }
    if (!self->art_) self->art_ = std::make_unique<ArtIndex>();

    // 2. WAL — replay every record we can verify, then keep the fd open
    //    for the rest of this ArtWal's life.
    if (!opts.wal_path.empty()) {
        // Read existing contents (may be empty / nonexistent).
        std::ifstream in(opts.wal_path, std::ios::binary);
        std::vector<uint8_t> buf;
        if (in) {
            buf.assign(std::istreambuf_iterator<char>(in),
                        std::istreambuf_iterator<char>());
        }
        const uint8_t* cur = buf.data();
        const uint8_t* end = buf.data() + buf.size();
        std::size_t replay_ok = 0;
        while (cur < end) {
            Record r;
            if (!DecodeRecord(cur, end, &r)) break;  // torn → stop
            if (r.op == kOpInsert) {
                auto leaf = std::make_unique<LeafData>();
                std::memcpy(&leaf->locator, &r.leaf.locator,
                              sizeof(leaf->locator));
                leaf->tier_residency_bitmap = r.leaf.tier_residency_bitmap;
                leaf->sealed_at_nanos       = r.leaf.sealed_at_nanos;
                leaf->last_access_nanos.store(
                    r.leaf.last_access_nanos.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
                leaf->bytes_total           = r.leaf.bytes_total;
                leaf->refcount.Reset(r.leaf.refcount.Load());
                self->art_->Insert(r.path, std::move(leaf));
            } else {
                self->art_->Remove(r.path);
            }
            ++replay_ok;
        }
        self->records_replayed_ = replay_ok;
        self->wal_records_       = replay_ok;

        // If the replay loop stopped before EOF, truncate the file at
        // the last-good offset so subsequent appends sit on a clean
        // boundary. Otherwise we'd be writing past garbage.
        const std::size_t good_bytes =
            static_cast<std::size_t>(cur - buf.data());
        if (good_bytes < buf.size()) {
            // Reopen with truncation point; safe to ignore failures
            // here — the boot path proceeds with the good prefix.
            ::truncate(opts.wal_path.c_str(),
                        static_cast<off_t>(good_bytes));
        }

        // Now open the fd append-only for future writes.
        self->wal_fd_ = ::open(opts.wal_path.c_str(),
                                  O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (self->wal_fd_ < 0) {
            if (err) *err = std::string("open wal: ") + std::strerror(errno);
            return nullptr;
        }
    }
    return self;
}

ArtWal::~ArtWal() {
    if (wal_fd_ >= 0) ::close(wal_fd_);
}

ArtIndex::InsertResult ArtWal::Insert(std::span<const ChunkHash> path,
                                       std::unique_ptr<LeafData> leaf,
                                       std::string* err) {
    std::lock_guard<std::mutex> lk(mu_);
    if (wal_fd_ >= 0) {
        if (!AppendInsertLocked(path, *leaf, err)) {
            // Refuse to mutate the in-memory ART if the durability path
            // failed — otherwise readers would see a leaf that's not on
            // disk, violating the recovery contract.
            return ArtIndex::InsertResult::kPathConflict;
        }
    }
    return art_->Insert(path, std::move(leaf));
}

bool ArtWal::Remove(std::span<const ChunkHash> path, std::string* err) {
    std::lock_guard<std::mutex> lk(mu_);
    if (wal_fd_ >= 0) {
        if (!AppendRemoveLocked(path, err)) return false;
    }
    return art_->Remove(path);
}

bool ArtWal::AppendInsertLocked(std::span<const ChunkHash> path,
                                  const LeafData& leaf, std::string* err) {
    const auto rec = EncodeRecord(kOpInsert, path, &leaf);
    const ssize_t w = ::write(wal_fd_, rec.data(), rec.size());
    if (w != static_cast<ssize_t>(rec.size())) {
        if (err) *err = std::string("write wal: ") + std::strerror(errno);
        return false;
    }
    if (opts_.fsync_each_write) {
#if defined(__linux__)
        if (::fdatasync(wal_fd_) != 0) {
#else
        if (::fsync(wal_fd_) != 0) {
#endif
            if (err) *err = std::string("fsync wal: ") + std::strerror(errno);
            return false;
        }
    }
    ++wal_records_;
    return true;
}

bool ArtWal::AppendRemoveLocked(std::span<const ChunkHash> path,
                                  std::string* err) {
    LeafData unused{};  // unused for REMOVE
    (void)unused;
    const auto rec = EncodeRecord(kOpRemove, path, nullptr);
    const ssize_t w = ::write(wal_fd_, rec.data(), rec.size());
    if (w != static_cast<ssize_t>(rec.size())) {
        if (err) *err = std::string("write wal: ") + std::strerror(errno);
        return false;
    }
    if (opts_.fsync_each_write) {
#if defined(__linux__)
        if (::fdatasync(wal_fd_) != 0) {
#else
        if (::fsync(wal_fd_) != 0) {
#endif
            if (err) *err = std::string("fsync wal: ") + std::strerror(errno);
            return false;
        }
    }
    ++wal_records_;
    return true;
}

bool ArtWal::Checkpoint(const std::string& new_snapshot_path,
                          std::string* err) {
    std::lock_guard<std::mutex> lk(mu_);
    // 1. Write the snapshot — ArtSnapshot::Write does write-tmp + fsync
    //    + rename, so this step alone is atomic.
    if (!ArtSnapshot::Write(*art_, new_snapshot_path, nullptr, err)) {
        return false;
    }
    // 2. Truncate the WAL. A crash between (1) and (2) leaves the old
    //    WAL on disk; replay-after-snapshot is idempotent because Insert
    //    of an identical leaf returns kReplaced and Remove of an absent
    //    key returns false — so the in-memory ART converges either way.
    if (wal_fd_ >= 0) {
        if (::ftruncate(wal_fd_, 0) != 0) {
            if (err) *err = std::string("truncate wal: ") + std::strerror(errno);
            return false;
        }
        if (::lseek(wal_fd_, 0, SEEK_SET) == static_cast<off_t>(-1)) {
            if (err) *err = std::string("lseek wal: ") + std::strerror(errno);
            return false;
        }
        wal_records_ = 0;
    }
    opts_.snapshot_path = new_snapshot_path;
    return true;
}

std::size_t ArtWal::wal_record_count() const noexcept {
    std::lock_guard<std::mutex> lk(mu_);
    return wal_records_;
}

}  // namespace kvcache::node::prefix
