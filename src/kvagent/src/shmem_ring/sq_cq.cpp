// LLD §6.1.3 — MPMC SQ/CQ rings, Vyukov implementation.
#include "shmem_ring/sq_cq.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <thread>

namespace kvcache::agent::shmem_ring {

namespace {

// Slot layout helper — each slot holds `seq (8B atomic) + payload`
// rounded up to the next cache line so adjacent slots don't false-share.
constexpr std::size_t SlotStride(uint32_t slot_bytes) noexcept {
    const std::size_t raw = sizeof(std::atomic<uint64_t>) + slot_bytes;
    return (raw + kCacheLine - 1) & ~(kCacheLine - 1);
}

bool IsPow2(uint32_t v) noexcept { return v > 0 && (v & (v - 1)) == 0; }

constexpr std::size_t kSlotBaseOffset = sizeof(RingHeader) + sizeof(RingCursors);

}  // namespace

std::size_t SqCq::slot_stride() const noexcept {
    return SlotStride(slot_bytes_);
}

std::atomic<uint64_t>& SqCq::slot_seq(uint64_t pos) const noexcept {
    auto* base = static_cast<uint8_t*>(slot_base_) + (pos & mask_) * slot_stride();
    return *reinterpret_cast<std::atomic<uint64_t>*>(base);
}

uint8_t* SqCq::slot_payload(uint64_t pos) const noexcept {
    return static_cast<uint8_t*>(slot_base_) + (pos & mask_) * slot_stride()
         + sizeof(std::atomic<uint64_t>);
}

// ----- Create -----------------------------------------------------------

std::unique_ptr<SqCq> SqCq::Create(const OpenOptions& opts, std::string* err) {
    if (!IsPow2(opts.slot_count)) {
        if (err) *err = "slot_count must be a power of 2";
        return nullptr;
    }
    if (opts.slot_bytes == 0) {
        if (err) *err = "slot_bytes must be > 0";
        return nullptr;
    }
    if (opts.path.empty()) {
        if (err) *err = "path must be non-empty";
        return nullptr;
    }

    const std::size_t map_bytes =
        kSlotBaseOffset + static_cast<std::size_t>(opts.slot_count) * SlotStride(opts.slot_bytes);

    // O_CREAT | O_TRUNC: a fresh ring on every Create. The agent is the
    // single source of truth for the ring's lifecycle.
    int fd = ::open(opts.path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        if (err) *err = std::string("open ") + opts.path + ": " + std::strerror(errno);
        return nullptr;
    }
    if (::ftruncate(fd, static_cast<off_t>(map_bytes)) != 0) {
        if (err) *err = std::string("ftruncate: ") + std::strerror(errno);
        ::close(fd);
        ::unlink(opts.path.c_str());
        return nullptr;
    }
    void* base = ::mmap(nullptr, map_bytes, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        if (err) *err = std::string("mmap: ") + std::strerror(errno);
        ::close(fd);
        ::unlink(opts.path.c_str());
        return nullptr;
    }

    auto self = std::unique_ptr<SqCq>(new SqCq());
    self->fd_         = fd;
    self->map_base_   = base;
    self->map_bytes_  = map_bytes;
    self->hdr_        = reinterpret_cast<RingHeader*>(base);
    self->cursors_    = reinterpret_cast<RingCursors*>(
                          static_cast<uint8_t*>(base) + sizeof(RingHeader));
    self->slot_base_  = static_cast<uint8_t*>(base) + kSlotBaseOffset;
    self->mask_       = opts.slot_count - 1;
    self->slot_bytes_ = opts.slot_bytes;
    self->path_       = opts.path;
    self->owned_      = true;

    // Initialise Vyukov slot seqs: slot k starts at seq=k so the first
    // producer at pos=k finds (seq - pos == 0) and can claim it. With
    // ftruncate-zeroed memory + slot 0's seq already at 0, this loop
    // is what makes positions 1..N-1 producible. Forgetting this
    // initialisation looks like "ring is full after one push" (the
    // very bug this comment exists to head off next time).
    new (&self->cursors_->head_pos) std::atomic<uint64_t>(0);
    new (&self->cursors_->tail_pos) std::atomic<uint64_t>(0);
    for (uint64_t k = 0; k < opts.slot_count; ++k) {
        new (&self->slot_seq(k)) std::atomic<uint64_t>(k);
    }
    self->hdr_->slot_count = opts.slot_count;
    self->hdr_->slot_bytes = opts.slot_bytes;
    self->hdr_->version    = kCurrentVersion;
    std::atomic_thread_fence(std::memory_order_release);
    self->hdr_->magic      = static_cast<uint32_t>(opts.kind);
    return self;
}

// ----- Open -------------------------------------------------------------

std::unique_ptr<SqCq> SqCq::Open(const OpenOptions& opts, std::string* err) {
    if (opts.path.empty()) {
        if (err) *err = "path must be non-empty";
        return nullptr;
    }
    int fd = ::open(opts.path.c_str(), O_RDWR, 0);
    if (fd < 0) {
        if (err) *err = std::string("open ") + opts.path + ": " + std::strerror(errno);
        return nullptr;
    }
    struct ::stat st{};
    if (::fstat(fd, &st) != 0 || st.st_size < static_cast<off_t>(sizeof(RingHeader))) {
        if (err) *err = "ring file too small or fstat failed";
        ::close(fd);
        return nullptr;
    }
    const std::size_t map_bytes = static_cast<std::size_t>(st.st_size);
    void* base = ::mmap(nullptr, map_bytes, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        if (err) *err = std::string("mmap: ") + std::strerror(errno);
        ::close(fd);
        return nullptr;
    }
    auto* hdr = reinterpret_cast<RingHeader*>(base);
    if (hdr->magic != static_cast<uint32_t>(opts.kind)) {
        if (err) *err = "ring magic mismatch (wrong kind or stale file)";
        ::munmap(base, map_bytes);
        ::close(fd);
        return nullptr;
    }
    if (hdr->version != kCurrentVersion) {
        if (err) *err = "ring version mismatch";
        ::munmap(base, map_bytes);
        ::close(fd);
        return nullptr;
    }
    if (opts.slot_count != 0 && hdr->slot_count != opts.slot_count) {
        if (err) *err = "ring slot_count mismatch";
        ::munmap(base, map_bytes);
        ::close(fd);
        return nullptr;
    }
    if (opts.slot_bytes != 0 && hdr->slot_bytes != opts.slot_bytes) {
        if (err) *err = "ring slot_bytes mismatch";
        ::munmap(base, map_bytes);
        ::close(fd);
        return nullptr;
    }
    if (!IsPow2(hdr->slot_count)) {
        if (err) *err = "on-disk slot_count is not pow-of-2";
        ::munmap(base, map_bytes);
        ::close(fd);
        return nullptr;
    }

    auto self = std::unique_ptr<SqCq>(new SqCq());
    self->fd_         = fd;
    self->map_base_   = base;
    self->map_bytes_  = map_bytes;
    self->hdr_        = hdr;
    self->cursors_    = reinterpret_cast<RingCursors*>(
                          static_cast<uint8_t*>(base) + sizeof(RingHeader));
    self->slot_base_  = static_cast<uint8_t*>(base) + kSlotBaseOffset;
    self->mask_       = hdr->slot_count - 1;
    self->slot_bytes_ = hdr->slot_bytes;
    self->path_       = opts.path;
    self->owned_      = false;
    return self;
}

SqCq::~SqCq() {
    if (map_base_) ::munmap(map_base_, map_bytes_);
    if (fd_ >= 0)  ::close(fd_);
    if (owned_ && !path_.empty()) ::unlink(path_.c_str());
}

// ----- try_push / try_pop / pop_for ------------------------------------

bool SqCq::try_push(std::span<const uint8_t> payload) noexcept {
    if (payload.size() > slot_bytes_) return false;

    uint64_t pos = cursors_->head_pos.load(std::memory_order_relaxed);
    for (;;) {
        auto& seq = slot_seq(pos);
        const uint64_t s = seq.load(std::memory_order_acquire);
        const int64_t  diff = static_cast<int64_t>(s) - static_cast<int64_t>(pos);
        if (diff == 0) {
            // Slot is free for this position; try to claim head.
            if (cursors_->head_pos.compare_exchange_weak(
                    pos, pos + 1,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                // We own the slot — write payload then publish.
                auto* dst = slot_payload(pos);
                std::memcpy(dst, payload.data(), payload.size());
                // Stamp the "filled" seq; consumers wait for seq == pos + 1.
                seq.store(pos + 1, std::memory_order_release);
                return true;
            }
            // CAS lost — pos was reloaded; retry.
        } else if (diff < 0) {
            // Slot still holds an unread value from the previous lap →
            // the ring is full.
            return false;
        } else {
            // Producer raced ahead; reload pos and retry.
            pos = cursors_->head_pos.load(std::memory_order_relaxed);
        }
    }
}

bool SqCq::try_pop(std::vector<uint8_t>* out) noexcept {
    if (!out) return false;
    uint64_t pos = cursors_->tail_pos.load(std::memory_order_relaxed);
    for (;;) {
        auto& seq = slot_seq(pos);
        const uint64_t s = seq.load(std::memory_order_acquire);
        const int64_t  diff = static_cast<int64_t>(s) - static_cast<int64_t>(pos + 1);
        if (diff == 0) {
            // A producer has filled this slot for us.
            if (cursors_->tail_pos.compare_exchange_weak(
                    pos, pos + 1,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                out->assign(slot_payload(pos), slot_payload(pos) + slot_bytes_);
                // Stamp "free for next lap" — producer waits for
                // seq == (pos + slot_count) on the next round-trip.
                seq.store(pos + mask_ + 1, std::memory_order_release);
                return true;
            }
        } else if (diff < 0) {
            // No producer yet — empty.
            return false;
        } else {
            pos = cursors_->tail_pos.load(std::memory_order_relaxed);
        }
    }
}

bool SqCq::pop_for(std::vector<uint8_t>* out, uint64_t timeout_ns) noexcept {
    // Coarse spin-with-backoff; production callers attach a doorbell.
    if (try_pop(out)) return true;
    const auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::nanoseconds(timeout_ns);
    int backoff = 64;
    while (std::chrono::steady_clock::now() < deadline) {
        for (int i = 0; i < backoff; ++i) {
            if (try_pop(out)) return true;
        }
        std::this_thread::yield();
        if (backoff < 8192) backoff <<= 1;
    }
    return false;
}

// ----- observability ----------------------------------------------------

uint64_t SqCq::head_pos() const noexcept {
    return cursors_->head_pos.load(std::memory_order_relaxed);
}
uint64_t SqCq::tail_pos() const noexcept {
    return cursors_->tail_pos.load(std::memory_order_relaxed);
}
std::size_t SqCq::approx_size() const noexcept {
    const uint64_t h = head_pos();
    const uint64_t t = tail_pos();
    return h >= t ? static_cast<std::size_t>(h - t) : 0;
}

}  // namespace kvcache::agent::shmem_ring
