// LLD §6.1.3 — MPMC SQ/CQ rings in /dev/shm with header versioning.
//
// Phase A1.1 — engine-facing shared-memory ring. One pair (SQ engine→
// agent, CQ agent→engine) lives in two `/dev/shm` files per registered
// engine. Lock-free MPMC via Vyukov bounded-queue: each slot carries
// an atomic ``seq`` tracking whether it is empty (next producer owns
// it) or full (next consumer owns it). Producers/consumers cmpxchg
// the head/tail cursor to claim a slot; the seq check makes
// claim+publish the only memory-visible step.
//
// File layout (single mmap):
//
//   offset 0    Header (cache-line padded, fixed 64 B)
//     magic           uint32   "KVSQ" / "KVCQ"
//     version         uint32   on-disk format
//     slot_count      uint32   power-of-two; mask = slot_count - 1
//     slot_bytes      uint32   payload bytes per slot (excludes seq)
//     reserved[6]     uint32   future use; zero today
//   offset 64   Cursors (two cache lines so producer + consumer don't
//               share a line and thrash)
//     head_pos        atomic<uint64>   producer cursor (monotonic)
//     pad
//     tail_pos        atomic<uint64>   consumer cursor (monotonic)
//     pad
//   offset 192  Slot[slot_count]  (cache-line aligned each)
//     atomic<uint64> seq  +  uint8 payload[slot_bytes]
//     rounded to next 64 B.
//
// The ring is symmetric — same layout for SQ and CQ — only the magic
// differs. kvagent owns the file lifecycle (Create); engines Open.
// slot_bytes is fixed at create time and validated at open time.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace kvcache::agent::shmem_ring {

inline constexpr uint32_t kSqMagic        = 0x5153564B;  // "KVSQ"
inline constexpr uint32_t kCqMagic        = 0x5143564B;  // "KVCQ"
inline constexpr uint32_t kCurrentVersion = 1;
inline constexpr std::size_t kCacheLine   = 64;

enum class RingKind : uint32_t {
    Sq = kSqMagic,
    Cq = kCqMagic,
};

struct alignas(kCacheLine) RingHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t slot_count;
    uint32_t slot_bytes;
    uint32_t reserved[6];
    char     _pad[kCacheLine - 10 * sizeof(uint32_t)];
};
static_assert(sizeof(RingHeader) == kCacheLine, "RingHeader must fit one cache line");

struct alignas(kCacheLine) RingCursors {
    std::atomic<uint64_t> head_pos;
    char _pad1[kCacheLine - sizeof(std::atomic<uint64_t>)];
    std::atomic<uint64_t> tail_pos;
    char _pad2[kCacheLine - sizeof(std::atomic<uint64_t>)];
};
static_assert(sizeof(RingCursors) == 2 * kCacheLine, "RingCursors must be 2 cache lines");

class SqCq {
   public:
    struct OpenOptions {
        std::string path;                 // /dev/shm/... path
        RingKind    kind        = RingKind::Sq;
        uint32_t    slot_count  = 1024;   // pow-of-2; ignored on Open
        uint32_t    slot_bytes  = 256;    // payload bytes per slot
    };

    static std::unique_ptr<SqCq> Create(const OpenOptions& opts, std::string* err);
    static std::unique_ptr<SqCq> Open(const OpenOptions& opts, std::string* err);

    ~SqCq();
    SqCq(const SqCq&)            = delete;
    SqCq& operator=(const SqCq&) = delete;

    // try_push: copy `payload` (<= slot_bytes) into the next free slot
    // and advance head. Returns true on success, false when ring full.
    bool try_push(std::span<const uint8_t> payload) noexcept;

    // try_pop: copy the next ready slot's payload into *out (resized
    // to slot_bytes). Returns true on success, false when ring empty.
    bool try_pop(std::vector<uint8_t>* out) noexcept;

    // pop_for: spin-with-backoff up to timeout_ns. Use the doorbell
    // (A1.2) instead in production; this is for tests + degraded paths.
    bool pop_for(std::vector<uint8_t>* out, uint64_t timeout_ns) noexcept;

    uint64_t head_pos() const noexcept;
    uint64_t tail_pos() const noexcept;
    std::size_t approx_size() const noexcept;
    uint32_t slot_count() const noexcept { return hdr_->slot_count; }
    uint32_t slot_bytes() const noexcept { return hdr_->slot_bytes; }

   private:
    SqCq() = default;

    std::size_t slot_stride() const noexcept;
    std::atomic<uint64_t>& slot_seq(uint64_t pos) const noexcept;
    uint8_t*    slot_payload(uint64_t pos) const noexcept;

    int          fd_         = -1;
    void*        map_base_   = nullptr;
    std::size_t  map_bytes_  = 0;
    RingHeader*  hdr_        = nullptr;
    RingCursors* cursors_    = nullptr;
    void*        slot_base_  = nullptr;
    uint64_t     mask_       = 0;
    uint32_t     slot_bytes_ = 0;  // cached from header — avoid a load on hot path
    std::string  path_;
    bool         owned_      = false;  // we Create'd it; unlink on dtor
};

}  // namespace kvcache::agent::shmem_ring
