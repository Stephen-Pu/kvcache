// LLD §3.3 T1 — Pinned tier implementation.
#include "tier/pinned_tier.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <sys/mman.h>
#include <unistd.h>

#include "logging.h"

namespace kvcache::node::tier {

std::unique_ptr<PinnedTier> PinnedTier::Create(const Options& opts, std::string* err) {
    if (opts.pool_bytes == 0 || opts.slot_bytes == 0 ||
        opts.pool_bytes < opts.slot_bytes) {
        if (err) *err = "pinned_tier: invalid pool/slot sizes";
        return nullptr;
    }
    if (opts.pool_bytes % opts.slot_bytes != 0) {
        if (err) *err = "pinned_tier: pool_bytes must be a multiple of slot_bytes";
        return nullptr;
    }
    auto t = std::unique_ptr<PinnedTier>(new PinnedTier());
    t->pool_bytes_ = opts.pool_bytes;
    t->slot_bytes_ = opts.slot_bytes;
    t->slot_count_ = static_cast<uint32_t>(opts.pool_bytes / opts.slot_bytes);

    // Anonymous mmap; optionally locked into RAM so RDMA can use it.
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_LOCKED
    if (opts.use_mlock) flags |= MAP_LOCKED;
#endif
    void* p = ::mmap(nullptr, t->pool_bytes_, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (p == MAP_FAILED) {
        if (err) *err = std::string("pinned_tier: mmap failed: ") + std::strerror(errno);
        return nullptr;
    }
    t->base_ = p;

    // Best-effort: if MAP_LOCKED was not honored, fall back to mlock().
    if (opts.use_mlock) {
        if (::mlock(p, t->pool_bytes_) == 0) {
            t->mlocked_ = true;
        } else {
            // Phase O-2: mlock failure is non-fatal — the caller may not
            // have CAP_IPC_LOCK or may be exceeding ulimit -l — but the
            // tier silently runs without page pinning, which the operator
            // needs to know about (RDMA registration may pay a faulting
            // cost on first touch, and benchmarks won't match the spec).
            // Warn-level: degraded-but-running.
            char buf[128];
            std::snprintf(buf, sizeof(buf),
                          "mlock(%zu bytes) failed: %s — tier running "
                          "without page pinning (check ulimit -l / "
                          "CAP_IPC_LOCK)",
                          t->pool_bytes_, std::strerror(errno));
            ::kvcache::log::Get("pinned_tier").Warn(buf);
        }
    }

    // Register the entire pool with NIXL via the caller-supplied callback.
    if (opts.register_region) {
        t->mr_key_ = opts.register_region(t->base_, t->pool_bytes_);
    }

    // Initialize the free-list as the full set of slot indices.
    t->free_stack_.reserve(t->slot_count_);
    for (uint32_t i = t->slot_count_; i > 0; --i) {
        t->free_stack_.push_back(i - 1);
    }
    return t;
}

PinnedTier::~PinnedTier() {
    if (base_) {
        if (mlocked_) ::munlock(base_, pool_bytes_);
        ::munmap(base_, pool_bytes_);
    }
}

std::optional<SlotDesc> PinnedTier::Acquire() {
    std::lock_guard lk(mu_);
    if (free_stack_.empty()) return std::nullopt;
    uint32_t idx = free_stack_.back();
    free_stack_.pop_back();
    in_use_.fetch_add(1, std::memory_order_relaxed);

    SlotDesc d{};
    d.addr   = static_cast<uint8_t*>(base_) + idx * slot_bytes_;
    d.bytes  = slot_bytes_;
    d.mr_key = mr_key_;
    d.id.v   = idx;
    return d;
}

void PinnedTier::Release(SlotId id) {
    if (!id.valid() || id.v >= slot_count_) return;
    std::lock_guard lk(mu_);
    free_stack_.push_back(id.v);
    in_use_.fetch_sub(1, std::memory_order_relaxed);
}

}  // namespace kvcache::node::tier
