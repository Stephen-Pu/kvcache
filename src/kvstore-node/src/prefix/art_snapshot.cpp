// LLD §3.2 / §7.3 — Persistent ART snapshot implementation.
//
// See art_snapshot.h for the on-disk format. The walk is recursive and
// runs under ArtIndex::writer_mu_ to guarantee a point-in-time consistent
// snapshot (readers continue without blocking; concurrent writers wait).
#include "prefix/art_snapshot.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <vector>

#include "b3_facade.h"
#include "prefix/art_index.h"
#include "prefix/art_index_internal.h"

namespace kvcache::node::prefix {

namespace {

// Tags in the on-disk format. Match ArtNodeTag but spelled out so a format
// change does not silently cascade from the internal enum.
constexpr uint8_t kTagInner = 0;
constexpr uint8_t kTagLeaf  = 1;

// ---- raw byte append helpers (little-endian) -----------------------------

void PutU8(std::vector<uint8_t>& b, uint8_t v) { b.push_back(v); }

void PutU16(std::vector<uint8_t>& b, uint16_t v) {
    b.push_back(static_cast<uint8_t>(v));
    b.push_back(static_cast<uint8_t>(v >> 8));
}

void PutU32(std::vector<uint8_t>& b, uint32_t v) {
    for (int i = 0; i < 4; ++i)
        b.push_back(static_cast<uint8_t>(v >> (i * 8)));
}

void PutU64(std::vector<uint8_t>& b, uint64_t v) {
    for (int i = 0; i < 8; ++i)
        b.push_back(static_cast<uint8_t>(v >> (i * 8)));
}

void PutBytes(std::vector<uint8_t>& b, const void* data, std::size_t n) {
    auto* p = static_cast<const uint8_t*>(data);
    b.insert(b.end(), p, p + n);
}

// ---- byte readers --------------------------------------------------------

struct Cursor {
    const uint8_t* p;
    const uint8_t* end;
    bool           failed = false;

    bool Need(std::size_t n) {
        if (failed) return false;
        if (static_cast<std::size_t>(end - p) < n) {
            failed = true;
            return false;
        }
        return true;
    }
    uint8_t U8() {
        if (!Need(1)) return 0;
        return *p++;
    }
    uint16_t U16() {
        if (!Need(2)) return 0;
        uint16_t v = uint16_t(p[0]) | (uint16_t(p[1]) << 8);
        p += 2;
        return v;
    }
    uint32_t U32() {
        if (!Need(4)) return 0;
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) v |= uint32_t(p[i]) << (i * 8);
        p += 4;
        return v;
    }
    uint64_t U64() {
        if (!Need(8)) return 0;
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= uint64_t(p[i]) << (i * 8);
        p += 8;
        return v;
    }
    void Bytes(void* out, std::size_t n) {
        if (!Need(n)) return;
        std::memcpy(out, p, n);
        p += n;
    }
};

// ---- recursive serializer (writer) ---------------------------------------

void SerializeNode(const ArtNode* node, std::vector<uint8_t>& out,
                   ArtSnapshot::WriteStats& stats) {
    if (node->tag == ArtNodeTag::Inner256) {
        const auto* inner = static_cast<const ArtInner256*>(node);
        stats.inner_nodes++;

        // Pre-scan to compute child_count and collect non-null slots.
        // 256 children at most → a 256-byte local buffer is fine.
        uint8_t  slots[256];
        ArtNode* kids[256];
        uint16_t n = 0;
        for (int i = 0; i < 256; ++i) {
            ArtNode* c = inner->children[i].load(std::memory_order_relaxed);
            if (c) {
                slots[n] = static_cast<uint8_t>(i);
                kids[n]  = c;
                ++n;
            }
        }

        PutU8(out, kTagInner);
        PutU8(out, inner->edge_tail_valid ? 1 : 0);
        PutBytes(out, inner->edge_tail.data(), 7);
        PutU16(out, n);
        for (uint16_t i = 0; i < n; ++i) {
            PutU8(out, slots[i]);
            SerializeNode(kids[i], out, stats);
        }
    } else {
        const auto* leaf = static_cast<const ArtLeaf*>(node);
        stats.leaves++;

        PutU8(out, kTagLeaf);
        PutU8(out, leaf->edge_tail_valid ? 1 : 0);
        PutBytes(out, leaf->edge_tail.data(), 7);

        const LeafData& d = *leaf->data;
        // kv_locator_t is a packed POD of exactly 64 bytes — memcpy is the
        // wire encoding (LLD §2.1: little-endian, packed).
        PutBytes(out, &d.locator, sizeof(d.locator));
        PutU32(out, d.tier_residency_bitmap);
        PutU32(out, d.refcount.Load());
        PutU64(out, d.sealed_at_nanos);
        PutU64(out, d.last_access_nanos.load(std::memory_order_acquire));
        PutU64(out, d.bytes_total);
    }
}

// ---- recursive deserializer (reader) -------------------------------------

ArtNode* DeserializeNode(Cursor& c, ArtSnapshot::ReadStats& stats) {
    uint8_t tag = c.U8();
    if (c.failed) return nullptr;

    if (tag == kTagInner) {
        auto inner = std::make_unique<ArtInner256>();
        inner->edge_tail_valid = c.U8() != 0;
        c.Bytes(inner->edge_tail.data(), 7);
        uint16_t n = c.U16();
        if (n > 256) {
            c.failed = true;
            return nullptr;
        }
        stats.inner_nodes++;
        for (uint16_t i = 0; i < n; ++i) {
            uint8_t slot = c.U8();
            ArtNode* child = DeserializeNode(c, stats);
            if (c.failed || !child) {
                // inner's dtor cascades delete of any already-attached
                // children, so the allocations above are not leaked.
                return nullptr;
            }
            // Slots are strictly increasing in our writer; but the format
            // does not require it, so just store at the named slot. If two
            // children name the same slot the second wins (malformed file
            // — caller detects via leaf_count / inner_count verification).
            inner->children[slot].store(child, std::memory_order_relaxed);
        }
        return inner.release();
    }

    if (tag == kTagLeaf) {
        auto leaf = std::make_unique<ArtLeaf>();
        leaf->edge_tail_valid = c.U8() != 0;
        c.Bytes(leaf->edge_tail.data(), 7);

        auto data = std::make_unique<LeafData>();
        c.Bytes(&data->locator, sizeof(data->locator));
        data->tier_residency_bitmap = c.U32();
        uint32_t refc                = c.U32();
        data->sealed_at_nanos        = c.U64();
        data->last_access_nanos.store(c.U64(), std::memory_order_relaxed);
        data->bytes_total            = c.U64();
        data->refcount.Reset(refc);

        leaf->data = std::move(data);
        stats.leaves++;
        return leaf.release();
    }

    c.failed = true;
    return nullptr;
}

}  // namespace

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

bool ArtSnapshot::Write(const ArtIndex& art, const std::string& path,
                        WriteStats* stats, std::string* err) {
    auto fail = [&](const std::string& m) {
        if (err) *err = m;
        return false;
    };

    WriteStats local{};
    std::vector<uint8_t> body;
    body.reserve(64 * 1024);  // most snapshots fit in a few KiB-MiB

    {
        // Hold writer mutex for the walk to get a point-in-time view.
        std::lock_guard<std::mutex> lk(art.writer_mu_);
        SerializeNode(art.root_.get(), body, local);
    }

    // Header
    auto digest = kvcache::hash::Blake3_256(
        std::span<const uint8_t>(body.data(), body.size()));

    std::vector<uint8_t> header;
    header.reserve(kSnapshotHeaderBytes);
    PutBytes(header, kSnapshotMagic, 4);
    PutU32(header, kSnapshotVersion);
    PutU32(header, 0);  // flags
    PutU32(header, 0);  // reserved
    PutU64(header, local.leaves);
    PutU64(header, local.inner_nodes);
    PutU64(header, static_cast<uint64_t>(body.size()));
    PutBytes(header, digest.data(), 32);
    if (header.size() != kSnapshotHeaderBytes) {
        return fail("internal: header size mismatch");
    }

    // Atomic publish: write to tmp, fsync, rename.
    std::string tmp = path + ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return fail(std::string("open(") + tmp + "): " + std::strerror(errno));

    auto write_all = [&](const uint8_t* p, std::size_t n) {
        while (n > 0) {
            ssize_t w = ::write(fd, p, n);
            if (w < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            p += w;
            n -= w;
        }
        return true;
    };

    if (!write_all(header.data(), header.size()) ||
        !write_all(body.data(),   body.size())) {
        ::close(fd);
        ::unlink(tmp.c_str());
        return fail(std::string("write: ") + std::strerror(errno));
    }
#if defined(__linux__)
    if (::fdatasync(fd) != 0) {
        ::close(fd);
        ::unlink(tmp.c_str());
        return fail(std::string("fdatasync: ") + std::strerror(errno));
    }
#else
    if (::fsync(fd) != 0) {
        ::close(fd);
        ::unlink(tmp.c_str());
        return fail(std::string("fsync: ") + std::strerror(errno));
    }
#endif
    ::close(fd);

    if (::rename(tmp.c_str(), path.c_str()) != 0) {
        ::unlink(tmp.c_str());
        return fail(std::string("rename: ") + std::strerror(errno));
    }

    local.bytes_written = header.size() + body.size();
    if (stats) *stats = local;
    return true;
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

std::unique_ptr<ArtIndex> ArtSnapshot::Read(const std::string& path,
                                              ReadStats* stats,
                                              std::string* err) {
    auto fail = [&](const std::string& m) -> std::unique_ptr<ArtIndex> {
        if (err) *err = m;
        return nullptr;
    };

    std::ifstream in(path, std::ios::binary);
    if (!in) return fail("open: " + path);

    std::vector<uint8_t> buf((std::istreambuf_iterator<char>(in)),
                              std::istreambuf_iterator<char>());
    if (buf.size() < kSnapshotHeaderBytes) {
        return fail("file too small for header");
    }

    Cursor c{buf.data(), buf.data() + buf.size()};

    uint8_t magic[4];
    c.Bytes(magic, 4);
    if (std::memcmp(magic, kSnapshotMagic, 4) != 0) {
        return fail("bad magic");
    }
    uint32_t version = c.U32();
    if (version != kSnapshotVersion) {
        return fail("unsupported version " + std::to_string(version));
    }
    uint32_t flags = c.U32();
    (void)c.U32();  // reserved
    if (flags != 0) return fail("unknown flags set");

    uint64_t expect_leaves = c.U64();
    uint64_t expect_inner  = c.U64();
    uint64_t body_bytes    = c.U64();
    uint8_t  expect_digest[32];
    c.Bytes(expect_digest, 32);

    if (c.failed) return fail("header truncated");

    if (buf.size() - kSnapshotHeaderBytes != body_bytes) {
        return fail("body_bytes mismatch (file truncated or padded)");
    }

    // Verify body checksum before allocating anything.
    auto got_digest = kvcache::hash::Blake3_256(
        std::span<const uint8_t>(buf.data() + kSnapshotHeaderBytes, body_bytes));
    if (std::memcmp(got_digest.data(), expect_digest, 32) != 0) {
        return fail("body checksum mismatch");
    }

    // Body is trusted; now walk it.
    Cursor body_c{buf.data() + kSnapshotHeaderBytes,
                   buf.data() + buf.size()};

    ReadStats local{};
    auto art = std::make_unique<ArtIndex>();
    // Replace the freshly-built empty root with the deserialized root.
    // The empty root will be freed by unique_ptr move-assignment.
    ArtNode* root_raw = DeserializeNode(body_c, local);
    if (body_c.failed || !root_raw) {
        delete root_raw;
        return fail("body truncated or malformed");
    }
    if (root_raw->tag != ArtNodeTag::Inner256) {
        delete root_raw;
        return fail("root is not Inner256");
    }
    if (body_c.p != body_c.end) {
        delete root_raw;
        return fail("trailing bytes after body");
    }
    if (local.leaves != expect_leaves || local.inner_nodes != expect_inner) {
        delete root_raw;
        return fail("leaf/inner count mismatch — body malformed");
    }

    art->root_.reset(static_cast<ArtInner256*>(root_raw));
    art->leaf_count_.store(local.leaves, std::memory_order_release);
    // ArtIndex::node_count_ counts inner nodes *created by Insert* — the
    // root is not counted there. Subtract it here to keep the post-restore
    // counter byte-for-byte equivalent to the writer's view.
    art->node_count_.store(local.inner_nodes > 0 ? local.inner_nodes - 1 : 0,
                            std::memory_order_release);

    if (stats) *stats = local;
    return art;
}

}  // namespace kvcache::node::prefix
